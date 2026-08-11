#!/usr/bin/env python3
"""
Redis同步服务

支持一对多Redis实例的持续同步服务。
"""

import logging
import hashlib
import signal
import sys
import time
import threading
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
import redis
from pathlib import Path

from .config import load_and_validate_service_config
from .connection_manager import (
    RedisConnectionManager,
    assert_distinct_redis_databases,
)
from .key_sync import (
    MAX_DUMP_BATCH_BYTES,
    _is_absttl_compatibility_error,
    _is_restore_compatibility_error,
    _sync_key_fallback,
    capture_dump_with_preflight,
    restore_dump_with_deadline,
)
from .migration_orchestrator import MigrationOrchestrator, MigrationConfig, MigrationType
from .sync_filters import (
    KeySyncFilter,
    build_atomic_filtered_delete_command,
    source_state_in_dynamic_scope,
)
from .web_ui import WebUI
from .unified_incremental_service import UnifiedIncrementalService


SourceFingerprint = Tuple[bytes, Optional[int], bool]
DEFAULT_SERVICE_STOP_TIMEOUT = 40.0
COOPERATIVE_STOP_GRACE = 1.0


@dataclass
class SyncTarget:
    """同步目标配置"""
    name: str
    host: str
    port: int
    password: Optional[str] = None
    db: int = 0
    ssl: bool = False
    enabled: bool = True
    connection_config: Optional[Dict[str, Any]] = None


@dataclass
class SyncStats:
    """同步统计信息"""
    total_synced: int = 0
    total_failed: int = 0
    last_sync_time: Optional[float] = None
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    is_healthy: bool = True
    failure_timestamps: List[float] = field(default_factory=list, repr=False)


@dataclass(frozen=True)
class CapturedSourceState:
    """One immutable source state reused for every target in a sync round."""

    key: bytes
    dump_data: bytes
    expires_at_ms: Optional[int]
    fingerprint: SourceFingerprint
    expiry_is_exact: bool = False


@dataclass
class SourceChangeSet:
    """A bounded set of source changes plus the snapshot used to detect them."""

    current_snapshot: Dict[bytes, SourceFingerprint]
    upserts: List[bytes]
    deletions: List[bytes]
    captured_upserts: Dict[bytes, CapturedSourceState] = field(default_factory=dict)


class RedisSyncService:
    """Redis同步服务"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logging()
        
        # 服务状态
        self.running = False
        self.shutdown_event = threading.Event()
        self._lifecycle_lock = threading.RLock()
        self._starting = False
        self._starting_thread_id = None
        self._start_complete = threading.Event()
        self._start_complete.set()
        self._stop_complete = threading.Event()
        self._stop_complete.set()
        self._stopped = False
        
        # 连接管理器和编排器
        self.source_conn = None
        self.target_connections: Dict[str, RedisConnectionManager] = {}
        self.orchestrators: Dict[str, MigrationOrchestrator] = {}
        self._unavailable_targets: Dict[str, Dict[str, Any]] = {}
        self._target_next_recovery: Dict[str, float] = {}
        self._source_pexpiretime_supported: Optional[bool] = None
        
        # 统计信息
        self.stats: Dict[str, SyncStats] = {}
        
        # 线程池
        self.executor = ThreadPoolExecutor(
            max_workers=self.config['service']['performance']['max_workers']
        )
        self._executor_futures = set()
        self._executor_futures_lock = threading.Lock()

        # 同步任务
        self.sync_tasks: List[threading.Thread] = []

        # 统一增量同步服务
        self.incremental_service: Optional[UnifiedIncrementalService] = None

        # Web UI
        self.web_ui = None
        if self.config.get('web_ui', {}).get('enabled', True):
            web_config = self.config.get('web_ui', {})
            self.web_ui = WebUI(
                self,
                host=web_config.get('host', '127.0.0.1'),
                port=web_config.get('port', 8080)
            )

        # 记录启动时间
        self.start_time = time.time()

        self._sync_key_filter = KeySyncFilter.from_config(
            self.config.get('sync', {}).get('filters')
        )
        self._source_snapshot: Optional[
            Dict[bytes, SourceFingerprint]
        ] = None
        self._target_pending_deletions: Dict[
            str, Dict[bytes, None]
        ] = {}
        self._realtime_bootstrap_snapshot: Optional[
            Dict[bytes, SourceFingerprint]
        ] = None
        self._realtime_baseline_established = False
        self._alignment_futures = set()
        self._alignment_futures_lock = threading.Lock()

        self.logger.info("Redis同步服务初始化完成")
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            return load_and_validate_service_config(self.config_path)
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def _setup_logging(self) -> logging.Logger:
        """设置日志"""
        log_config = self.config['service']['logging']

        # 配置 root logger，这样所有模块的日志都会输出
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, log_config['level']))

        # 清除已有的 handlers
        root_logger.handlers.clear()

        # 文件处理器
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_config['file'],
            maxBytes=log_config['max_size'],
            backupCount=log_config['backup_count']
        )
        file_handler.setFormatter(logging.Formatter(log_config['format']))
        root_logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(log_config['format']))
        root_logger.addHandler(console_handler)

        # 返回服务专用的 logger
        logger = logging.getLogger('redis-sync-service')

        return logger
    
    def _setup_signal_handlers(self):
        """设置信号处理器"""
        def signal_handler(signum, frame):
            self.logger.info(f"接收到信号 {signum}，开始优雅关闭...")
            self.running = False
            self.shutdown_event.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    @staticmethod
    def _redis_connection_kwargs(config: Dict[str, Any]) -> Dict[str, Any]:
        allowed = {
            'host', 'port', 'username', 'password', 'db', 'ssl',
            'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile',
            'socket_timeout', 'socket_connect_timeout', 'socket_keepalive',
            'health_check_interval', 'client_name',
        }
        params = {key: config[key] for key in allowed if key in config}
        max_connections = config.get('connection_pool_max_connections')
        if max_connections is not None:
            params['max_connections'] = int(max_connections)
        params['decode_responses'] = False
        return params
    
    def _connect_source(self) -> bool:
        """连接源Redis"""
        try:
            source_config = self.config['source']
            conn_params = self._redis_connection_kwargs(source_config)
            conn_params.pop('decode_responses', None)
            connection_manager = RedisConnectionManager(
                retry_config=self.config['service'].get('retry'),
                shutdown_event=self.shutdown_event,
            )
            self.source_conn = connection_manager.connect_source(**conn_params)
            self.logger.info(f"源Redis连接成功: {source_config['host']}:{source_config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"源Redis连接失败: {e}")
            return False
    
    def _prepare_target_resources(self, target_config: Dict[str, Any]):
        """Connect and initialize a target without exposing it to live fan-out."""
        conn_manager = RedisConnectionManager(
            retry_config=self.config['service'].get('retry'),
            shutdown_event=self.shutdown_event,
        )
        try:
            conn_manager.set_source_client(
                self.source_conn,
                self._redis_connection_kwargs(self.config['source']),
            )
            target_params = self._redis_connection_kwargs(target_config)
            target_params.pop('decode_responses', None)
            conn_manager.connect_target(**target_params)
            self._assert_distinct_target_database(
                conn_manager.target_client, target_config
            )

            orchestrator = MigrationOrchestrator(
                conn_manager,
                shutdown_event=self.shutdown_event,
            )
            scan_count = self.config['service']['performance'].get(
                'scan_count', 10000
            )
            orchestrator.initialize_handlers(scan_count=scan_count)
            return conn_manager, orchestrator
        except Exception:
            conn_manager.close()
            raise

    def _assert_distinct_target_database(
        self, target_client: redis.Redis, target_config: Dict[str, Any]
    ) -> None:
        """Use server identity and a marker as guards against endpoint aliases."""
        source_db = int(self.config.get('source', {}).get('db', 0))
        target_db = int(target_config.get('db', 0))
        if self.source_conn is None:
            return
        assert_distinct_redis_databases(
            self.source_conn,
            target_client,
            source_db,
            target_db,
        )

    def _activate_target(
        self,
        target_config: Dict[str, Any],
        conn_manager: RedisConnectionManager,
        orchestrator: MigrationOrchestrator,
    ) -> None:
        target_name = target_config['name']
        stats = self.stats.setdefault(target_name, SyncStats())
        self.target_connections[target_name] = conn_manager
        self.orchestrators[target_name] = orchestrator
        self._unavailable_targets.pop(target_name, None)
        stats.is_healthy = True
        stats.last_error = None

    def _record_target_connection_failure(
        self, target_config: Dict[str, Any], error: BaseException
    ) -> None:
        target_name = target_config['name']
        stats = self.stats.setdefault(target_name, SyncStats())
        self.target_connections.pop(target_name, None)
        self.orchestrators.pop(target_name, None)
        self._unavailable_targets[target_name] = dict(target_config)
        self._record_target_failure(stats, error, force_unhealthy=True)
        self.logger.error("目标Redis连接失败 %s: %s", target_name, error)

    def _record_target_failure(
        self,
        stats: SyncStats,
        error: Any,
        *,
        failed_operations: int = 0,
        force_unhealthy: bool = False,
        now: Optional[float] = None,
    ) -> int:
        """Record failures inside the configured sliding failover window."""
        failover = self.config.get('service', {}).get('failover', {})
        failure_window = max(1.0, float(failover.get('failure_window', 300)))
        current = time.monotonic() if now is None else float(now)
        cutoff = current - failure_window
        stats.failure_timestamps = [
            timestamp
            for timestamp in stats.failure_timestamps
            if timestamp >= cutoff
        ]
        stats.failure_timestamps.append(current)
        stats.consecutive_failures = len(stats.failure_timestamps)
        stats.total_failed += max(0, int(failed_operations))
        stats.last_error = str(error)
        max_failures = max(1, int(failover.get('max_failures', 5)))
        if force_unhealthy or stats.consecutive_failures >= max_failures:
            stats.is_healthy = False
        return stats.consecutive_failures

    @staticmethod
    def _reset_target_failures(stats: SyncStats) -> None:
        stats.failure_timestamps.clear()
        stats.consecutive_failures = 0
        stats.last_error = None

    def _connect_target(self, target_config: Dict[str, Any]) -> bool:
        target_name = target_config['name']
        try:
            conn_manager, orchestrator = self._prepare_target_resources(target_config)
            self._activate_target(target_config, conn_manager, orchestrator)
            self.logger.info(
                "目标Redis连接成功: %s (%s:%s)",
                target_name,
                target_config['host'],
                target_config['port'],
            )
            return True
        except Exception as e:
            self._record_target_connection_failure(target_config, e)
            return False

    def _connect_targets(self) -> bool:
        """Connect every enabled target while keeping healthy ones available."""
        for target_config in self.config['targets']:
            if self._service_is_stopping():
                break
            if target_config.get('enabled', True):
                self._connect_target(target_config)
        return bool(self.target_connections)

    def _recover_target_connection(self, target_name: str) -> bool:
        target_config = self._unavailable_targets.get(target_name)
        if target_config is None:
            return target_name in self.orchestrators
        return self._connect_target(target_config)

    def _target_recovery_is_due(
        self, target_name: str, *, now: Optional[float] = None
    ) -> bool:
        """Reserve one failover attempt after the configured per-target delay."""
        failover = self.config.get('service', {}).get('failover', {})
        if not failover.get('enabled', True):
            return False

        current = time.monotonic() if now is None else float(now)
        deadlines = getattr(self, '_target_next_recovery', None)
        if deadlines is None:
            deadlines = self._target_next_recovery = {}
        deadline = deadlines.get(target_name)
        delay = max(0.0, float(failover.get('recovery_delay', 60)))
        if deadline is None:
            deadlines[target_name] = current + delay
            return delay == 0
        if current < deadline:
            return False

        deadlines[target_name] = current + delay
        return True

    def _clear_target_recovery_deadline(self, target_name: str) -> None:
        deadlines = getattr(self, '_target_next_recovery', None)
        if deadlines is not None:
            deadlines.pop(target_name, None)
    
    def _create_sync_config(self) -> MigrationConfig:
        """创建同步配置"""
        sync_config = self.config['sync']

        if sync_config['mode'] == 'full':
            migration_type = MigrationType.FULL
        elif sync_config['mode'] == 'incremental':
            migration_type = MigrationType.INCREMENTAL
        else:  # hybrid
            migration_type = MigrationType.FULL  # 先全量，后面会启动增量

        # 获取full_sync配置
        full_sync_config = sync_config.get('full_sync', {})

        kt = full_sync_config.get('key_types')
        key_types = kt if isinstance(kt, list) else None

        filters = sync_config.get('filters') or {}
        inc = filters.get('include_patterns')
        exc = filters.get('exclude_patterns')

        config = MigrationConfig(
            migration_type=migration_type,
            key_pattern=full_sync_config.get('key_pattern', '*'),
            batch_size=full_sync_config.get('batch_size', 1000),
            scan_count=full_sync_config.get('scan_count', 10000),
            preserve_ttl=full_sync_config.get('preserve_ttl', True),
            verify_migration=(
                full_sync_config.get('verify_migration', True)
                and sync_config['mode'] == 'full'
            ),
            verify_mode=full_sync_config.get('verify_mode', 'full'),
            verify_sample_size=full_sync_config.get('verify_sample_size', 100),
            full_strategy=full_sync_config.get('strategy', 'scan'),
            clear_target=full_sync_config.get('clear_target', False),
            overwrite_existing=full_sync_config.get('overwrite_existing', True),
            key_types=key_types,
            include_patterns=inc if isinstance(inc, list) else None,
            exclude_patterns=exc if isinstance(exc, list) else None,
            filter_min_ttl=int(filters.get('min_ttl') or 0),
            filter_max_key_size=int(filters.get('max_key_size') or 0),
        )

        return config
    
    def _recovery_sync_config(self, *, clear_target: Optional[bool] = None):
        config = self._create_sync_config()
        incremental_config = self.config['sync'].get('incremental_sync', {})
        config.migration_type = MigrationType.FULL
        # The received RDB is a consistency boundary. Copying current key state
        # is idempotent with the key-state command backlog that follows it.
        config.full_strategy = 'scan'
        config.overwrite_existing = True
        config.verify_migration = False
        config.key_pattern = incremental_config.get('key_pattern', config.key_pattern)
        key_types = incremental_config.get('key_types')
        if isinstance(key_types, list):
            config.key_types = key_types
        if clear_target is not None:
            config.clear_target = clear_target
        return config

    def _perform_full_sync(
        self,
        target_name: str,
        migration_config: Optional[MigrationConfig] = None,
    ) -> bool:
        """执行全量同步"""
        try:
            start_time = time.time()
            orchestrator = self.orchestrators[target_name]
            config = migration_config or self._create_sync_config()

            self.logger.info(f"🚀 开始全量同步: {target_name} (时间戳: {start_time:.2f})")

            # 记录开始迁移前的时间
            before_migrate = time.time()
            result = orchestrator.migrate(config)
            after_migrate = time.time()

            # 计算实际迁移耗时
            actual_migrate_time = after_migrate - before_migrate

            # 调试日志
            self.logger.debug(f"迁移结果: success={result.get('success')}, stats keys={list(result.get('statistics', {}).keys())}")
            self.logger.debug(f"{target_name} 实际迁移耗时: {actual_migrate_time:.2f}秒")

            if result['success']:
                stats = result.get('statistics', {})
                # 使用实际迁移时间，而不是从函数开始的时间
                elapsed = actual_migrate_time
                migrated_keys = stats.get('migrated_keys', 0)

                # 获取验证时间（如果有）
                verification_time = result.get('verification_time', 0)

                self.stats[target_name].total_synced += migrated_keys
                self.stats[target_name].last_sync_time = time.time()
                self._reset_target_failures(self.stats[target_name])
                self.stats[target_name].is_healthy = True

                # 单行日志，避免多行显示问题
                speed = migrated_keys / elapsed if elapsed > 0 else 0

                # 如果有验证时间，显示详细信息
                if verification_time > 0:
                    migrate_only_time = elapsed - verification_time
                    migrate_speed = migrated_keys / migrate_only_time if migrate_only_time > 0 else 0
                    self.logger.info(
                        f"✅ {target_name} 全量同步完成 - "
                        f"键数: {migrated_keys}, "
                        f"总耗时: {elapsed:.2f}秒 (迁移: {migrate_only_time:.2f}秒, 验证: {verification_time:.2f}秒), "
                        f"迁移速度: {migrate_speed:.0f} 键/秒"
                    )
                else:
                    self.logger.info(
                        f"✅ {target_name} 全量同步完成 - "
                        f"键数: {migrated_keys}, "
                        f"耗时: {elapsed:.2f}秒, "
                        f"速度: {speed:.0f} 键/秒"
                    )
                return True
            else:
                errs = result.get('errors') or []
                raise Exception('; '.join(errs) if errs else '未知错误')
                
        except Exception as e:
            # An incomplete full copy must be recovered with another full copy.
            self._record_target_failure(
                self.stats[target_name],
                e,
                failed_operations=1,
                force_unhealthy=True,
            )
            
            self.logger.error(f"全量同步失败: {target_name}, 错误: {e}")
            return False
    
    def _perform_incremental_sync(self, target_name: str) -> bool:
        """执行增量同步"""
        try:
            orchestrator = self.orchestrators[target_name]
            
            # 创建增量同步配置
            inc_config = self.config['sync']['incremental_sync']
            filters = self.config['sync'].get('filters') or {}
            inc = filters.get('include_patterns')
            exc = filters.get('exclude_patterns')
            config = MigrationConfig(
                migration_type=MigrationType.INCREMENTAL,
                key_pattern=inc_config.get('key_pattern', '*'),
                max_changes_per_sync=inc_config.get('max_changes_per_sync', 10000),
                continuous_sync=False,  # 一次性增量同步
                include_patterns=inc if isinstance(inc, list) else None,
                exclude_patterns=exc if isinstance(exc, list) else None,
                filter_min_ttl=int(filters.get('min_ttl') or 0),
                filter_max_key_size=int(filters.get('max_key_size') or 0),
            )
            
            result = orchestrator.migrate(config)

            if result['success']:
                stats = result.get('statistics', {})
                changed_keys = stats.get('changed_keys', 0)
                synced_keys = stats.get('synced_keys', 0)
                failed_keys = stats.get('failed_keys', 0)

                if changed_keys > 0:
                    self.stats[target_name].total_synced += synced_keys
                    self.logger.info(f"✅ 增量同步完成: {target_name}")
                    self.logger.info(f"   变更键数: {changed_keys}")
                    self.logger.info(f"   同步成功: {synced_keys}")
                    self.logger.info(f"   同步失败: {failed_keys}")
                else:
                    self.logger.debug(f"✓ 增量同步: {target_name}, 无变更")

                self.stats[target_name].last_sync_time = time.time()
                self._reset_target_failures(self.stats[target_name])
                self.stats[target_name].is_healthy = True

                return True
            else:
                errs = result.get('errors') or []
                error_msg = '; '.join(errs) if errs else '未知错误'
                self.logger.error(f"❌ 增量同步失败: {target_name}, 错误: {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            self._record_target_failure(
                self.stats[target_name], e, failed_operations=1
            )
            if not self.stats[target_name].is_healthy:
                self.logger.error(f"⚠️  目标 {target_name} 标记为不健康，连续失败次数: {self.stats[target_name].consecutive_failures}")

            self.logger.error(f"❌ 增量同步失败: {target_name}, 错误: {e}", exc_info=True)
            return False

    @staticmethod
    def _key_bytes(key: Any) -> bytes:
        if isinstance(key, bytes):
            return key
        if isinstance(key, memoryview):
            return key.tobytes()
        return str(key).encode("utf-8", errors="surrogateescape")

    def _source_has_pexpiretime(self, source: redis.Redis, probe_key: bytes) -> bool:
        """Probe PEXPIRETIME outside MULTI so Redis < 7 cannot abort a transaction."""
        supported = getattr(self, '_source_pexpiretime_supported', None)
        if supported is not None:
            return supported
        try:
            source.execute_command('PEXPIRETIME', probe_key)
        except redis.ResponseError:
            supported = False
        else:
            supported = True
        self._source_pexpiretime_supported = supported
        return supported

    def _pipeline_batch_size(self) -> int:
        """Return the bounded batch size for service-owned Redis pipelines."""
        configured = (
            self.config.get('service', {})
            .get('performance', {})
            .get('pipeline_batch_size', 100)
        )
        return max(1, min(int(configured), 200))

    def _payload_bounded_source_groups(self, source, keys):
        """Plan source DUMP groups without materializing their payloads."""
        candidates = list(keys)
        if len(candidates) <= 1:
            return [candidates] if candidates else []

        sizes = []
        batch_size = self._pipeline_batch_size()
        try:
            for offset in range(0, len(candidates), batch_size):
                chunk = candidates[offset:offset + batch_size]
                pipe = source.pipeline(transaction=False)
                for key in chunk:
                    pipe.execute_command('MEMORY', 'USAGE', key)
                raw_sizes = pipe.execute(raise_on_error=False)
                if len(raw_sizes) != len(chunk):
                    raise RuntimeError(
                        "MEMORY USAGE response count mismatch: "
                        f"{len(raw_sizes)} != {len(chunk)}"
                    )
                sizes.extend(raw_sizes)
        except Exception as error:
            self.logger.warning(
                "MEMORY USAGE batch planning failed; capturing keys singly: %s",
                error,
            )
            return [[key] for key in candidates]

        groups = []
        current_group = []
        current_bytes = 0

        def flush_group():
            nonlocal current_group
            nonlocal current_bytes
            if current_group:
                groups.append(current_group)
                current_group = []
                current_bytes = 0

        for key, raw_size in zip(candidates, sizes):
            if (
                isinstance(raw_size, BaseException)
                or isinstance(raw_size, bool)
                or not isinstance(raw_size, int)
                or raw_size <= 0
            ):
                flush_group()
                groups.append([key])
                continue
            if current_group and (
                len(current_group) >= batch_size
                or current_bytes + raw_size > MAX_DUMP_BATCH_BYTES
            ):
                flush_group()
            current_group.append(key)
            current_bytes += raw_size
            if (
                len(current_group) >= batch_size
                or current_bytes >= MAX_DUMP_BATCH_BYTES
            ):
                flush_group()
        flush_group()
        return groups

    @staticmethod
    def _snapshot_entry_size(key: bytes, fingerprint) -> int:
        size = sys.getsizeof(key) + sys.getsizeof(fingerprint)
        size += sum(
            sys.getsizeof(value) for value in fingerprint if value is not None
        )
        return size

    @classmethod
    def _snapshot_size(cls, snapshot) -> int:
        return sys.getsizeof(snapshot) + sum(
            cls._snapshot_entry_size(key, fingerprint)
            for key, fingerprint in snapshot.items()
        )

    def _retained_snapshot_size(self) -> int:
        snapshots = (
            getattr(self, '_source_snapshot', None),
            getattr(self, '_realtime_bootstrap_snapshot', None),
        )
        seen = set()
        total = 0
        for snapshot in snapshots:
            if snapshot is None or id(snapshot) in seen:
                continue
            seen.add(id(snapshot))
            total += self._snapshot_size(snapshot)
        total += self._target_pending_deletions_size(
            getattr(self, '_target_pending_deletions', {})
        )
        return total

    @staticmethod
    def _target_pending_deletions_size(pending_by_target) -> int:
        size = sys.getsizeof(pending_by_target)
        for target_name, pending in pending_by_target.items():
            size += sys.getsizeof(target_name) + sys.getsizeof(pending)
            size += sum(sys.getsizeof(key) for key in pending)
        return size

    def _snapshot_memory_limit(self) -> int:
        configured = (
            self.config.get('service', {})
            .get('performance', {})
            .get('memory_limit')
        )
        return int(configured) if configured is not None else 0

    def _build_source_snapshot(self) -> Dict[bytes, SourceFingerprint]:
        """Scan the managed keyspace and fingerprint values plus absolute expiry."""
        if self._service_is_stopping():
            raise RuntimeError("service is stopping")
        inc_config = self.config['sync']['incremental_sync']
        key_pattern = inc_config.get('key_pattern', '*')
        key_types = inc_config.get('key_types')
        scan_count = self.config['service']['performance'].get('scan_count', 10000)
        pipeline_batch_size = self._pipeline_batch_size()
        source = self.source_conn
        if source is None and self.orchestrators:
            source = next(iter(self.orchestrators.values())).connection_manager.source_client
        if source is None:
            raise RuntimeError("source Redis is not connected")
        snapshot: Dict[bytes, SourceFingerprint] = {}
        memory_limit = self._snapshot_memory_limit()
        retained_snapshot_size = (
            self._retained_snapshot_size() if memory_limit else 0
        )
        change_list_size = 2 * sys.getsizeof([])
        snapshot_payload_size = 0
        if (
            memory_limit
            and retained_snapshot_size
            + sys.getsizeof(snapshot)
            + change_list_size
            > memory_limit
        ):
            raise MemoryError(
                "source snapshot memory_limit exceeded before the next SCAN"
            )
        cursor = 0

        while True:
            if self._service_is_stopping():
                raise RuntimeError("service is stopping")
            cursor, raw_keys = source.scan(
                cursor=cursor,
                match=key_pattern,
                count=scan_count,
            )
            keys = [self._key_bytes(key) for key in raw_keys]

            if key_types and keys:
                typed_keys = []
                for offset in range(0, len(keys), pipeline_batch_size):
                    if self._service_is_stopping():
                        raise RuntimeError("service is stopping")
                    chunk = keys[offset:offset + pipeline_batch_size]
                    pipe = source.pipeline(transaction=False)
                    for key in chunk:
                        pipe.type(key)
                    raw_types = pipe.execute(raise_on_error=False)
                    for value in raw_types:
                        if isinstance(value, BaseException):
                            raise value
                    typed_keys.extend(
                        key
                        for key, key_type in zip(chunk, raw_types)
                        if (
                            key_type.decode("ascii")
                            if isinstance(key_type, bytes)
                            else str(key_type)
                        ) in key_types
                    )
                keys = typed_keys

            if self._sync_key_filter and keys:
                keys = [
                    self._key_bytes(key)
                    for key in self._sync_key_filter.filter_batch(source, keys)
                ]

            for chunk in self._payload_bounded_source_groups(source, keys):
                if self._service_is_stopping():
                    raise RuntimeError("service is stopping")
                has_pexpiretime = self._source_has_pexpiretime(source, chunk[0])
                pipe = source.pipeline(transaction=True)
                for key in chunk:
                    pipe.dump(key)
                    pipe.pttl(key)
                    if has_pexpiretime:
                        pipe.execute_command('PEXPIRETIME', key)
                pttl_sample_started_ns = time.monotonic_ns()
                raw = pipe.execute(raise_on_error=False)
                observed_at_ms = int(time.time() * 1000)
                elapsed_ms = (
                    max(0, time.monotonic_ns() - pttl_sample_started_ns)
                    + 999_999
                ) // 1_000_000
                stride = 3 if has_pexpiretime else 2
                for index, key in enumerate(chunk):
                    dump_data = raw[index * stride]
                    pttl = raw[index * stride + 1]
                    pexpiretime = raw[index * stride + 2] if has_pexpiretime else None
                    if isinstance(dump_data, Exception):
                        raise dump_data
                    if isinstance(pttl, Exception):
                        raise pttl
                    if dump_data is None:
                        continue
                    ttl_ms = int(pttl)
                    if ttl_ms == -2 or ttl_ms == 0:
                        continue
                    if ttl_ms < -2:
                        raise ValueError(f"无效的 PTTL 响应: {ttl_ms}")
                    expires_at = None
                    expiry_is_exact = False
                    if ttl_ms > 0:
                        remaining_ttl_ms = ttl_ms - elapsed_ms
                        if remaining_ttl_ms <= 0:
                            continue
                        if not isinstance(pexpiretime, Exception):
                            try:
                                absolute_ms = int(pexpiretime)
                            except (TypeError, ValueError):
                                absolute_ms = -1
                            if absolute_ms > 0:
                                expires_at = absolute_ms
                                expiry_is_exact = True
                        if expires_at is None:
                            expires_at = observed_at_ms + remaining_ttl_ms
                    fingerprint = (
                        hashlib.sha256(dump_data).digest(),
                        expires_at,
                        expiry_is_exact,
                    )
                    if memory_limit and key in snapshot:
                        snapshot_payload_size -= self._snapshot_entry_size(
                            key, snapshot[key]
                        )
                    snapshot[key] = fingerprint
                    if memory_limit:
                        snapshot_payload_size += self._snapshot_entry_size(
                            key, fingerprint
                        )
                        estimated_size = (
                            retained_snapshot_size
                            + sys.getsizeof(snapshot)
                            + snapshot_payload_size
                            + change_list_size
                        )
                        if estimated_size > memory_limit:
                            snapshot.pop(key, None)
                            raise MemoryError(
                                "source snapshot memory_limit exceeded: "
                                f"estimated {estimated_size} bytes > "
                                f"configured {memory_limit} bytes"
                            )

            if cursor == 0:
                break

        return snapshot

    @staticmethod
    def _fingerprints_equal(
        left: Optional[SourceFingerprint],
        right: Optional[SourceFingerprint],
    ) -> bool:
        if left is None or right is None or left[0] != right[0]:
            return False
        left_expiry, right_expiry = left[1], right[1]
        if left_expiry is None or right_expiry is None:
            return left_expiry is right_expiry
        left_exact = len(left) >= 3 and left[2]
        right_exact = len(right) >= 3 and right[2]
        if left_exact and right_exact:
            return left_expiry == right_expiry
        # Redis < 7 lacks PEXPIRETIME. The fallback combines local wall time
        # and PTTL, which can jitter slightly between scans.
        return abs(left_expiry - right_expiry) <= 1000

    @staticmethod
    def _full_reconciliation_baseline(
        snapshot: Dict[bytes, SourceFingerprint],
        target_keys=(),
        *,
        in_place: bool = False,
    ) -> Dict[bytes, SourceFingerprint]:
        """Retain deletion candidates while forcing every surviving key to replay."""
        if in_place:
            baseline = snapshot
            for key in baseline:
                fingerprint = baseline[key]
                baseline[key] = (b"", fingerprint[1], fingerprint[2])
        else:
            baseline = {
                key: (b"", fingerprint[1], fingerprint[2])
                for key, fingerprint in snapshot.items()
            }
        for key in target_keys:
            baseline.setdefault(key, (b"", None, False))
        return baseline

    def _scan_source_for_changes(self) -> Optional[SourceChangeSet]:
        """Detect exact value/TTL changes and deletions from source snapshots."""
        try:
            current = self._build_source_snapshot()
            previous = self._source_snapshot or {}
            max_changes = int(
                self.config['sync']['incremental_sync'].get(
                    'max_changes_per_sync', 10000
                )
            )
            limit = max_changes if max_changes > 0 else None
            deletions: List[bytes] = []
            upserts: List[bytes] = []
            memory_limit = self._snapshot_memory_limit()
            snapshot_memory = 0
            if memory_limit:
                snapshot_memory = (
                    self._retained_snapshot_size()
                    + self._snapshot_size(current)
                )

            def check_change_list_memory() -> None:
                if (
                    memory_limit
                    and snapshot_memory
                    + sys.getsizeof(deletions)
                    + sys.getsizeof(upserts)
                    > memory_limit
                ):
                    raise MemoryError(
                        "source snapshot memory_limit exceeded while tracking changes"
                    )

            deletion_iter = (
                key for key in previous if key not in current
            )
            upsert_iter = (
                key
                for key, fingerprint in current.items()
                if not self._fingerprints_equal(previous.get(key), fingerprint)
            )

            def take_changes(iterator, destination, count=None) -> int:
                taken = 0
                while count is None or taken < count:
                    try:
                        key = next(iterator)
                    except StopIteration:
                        break
                    destination.append(key)
                    taken += 1
                    check_change_list_memory()
                return taken

            if limit is None:
                take_changes(deletion_iter, deletions)
                take_changes(upsert_iter, upserts)
            else:
                prefer = getattr(self, '_change_selection_turn', 'deletion')
                deletion_quota = limit // 2
                upsert_quota = limit // 2
                if limit % 2:
                    if prefer == 'upsert':
                        upsert_quota += 1
                    else:
                        deletion_quota += 1

                take_changes(deletion_iter, deletions, deletion_quota)
                take_changes(upsert_iter, upserts, upsert_quota)
                remaining = limit - len(deletions) - len(upserts)
                if remaining:
                    if len(deletions) < deletion_quota:
                        take_changes(upsert_iter, upserts, remaining)
                    elif len(upserts) < upsert_quota:
                        take_changes(deletion_iter, deletions, remaining)

                if deletions or upserts:
                    self._change_selection_turn = (
                        'deletion' if prefer == 'upsert' else 'upsert'
                    )

            change_count = len(deletions) + len(upserts)
            if change_count:
                self.logger.info(
                    "Detected %s source changes (%s upserts, %s deletions)",
                    change_count,
                    len(upserts),
                    len(deletions),
                )
            return SourceChangeSet(
                current_snapshot=current,
                upserts=upserts,
                deletions=deletions,
            )
        except Exception as e:
            self.logger.error("Source change scan failed: %s", e, exc_info=True)
            return None

    def _capture_source_states(
        self,
        source_client,
        keys: List[bytes],
        *,
        capture_memory_limit: int = 0,
    ) -> Tuple[Dict[bytes, CapturedSourceState], List[bytes]]:
        """Atomically capture payload, expiry and dynamic filter state."""
        captured: Dict[bytes, CapturedSourceState] = {}
        invalid = []
        if not keys:
            return captured, invalid

        incremental_config = self.config['sync']['incremental_sync']
        key_types = incremental_config.get('key_types')
        min_ttl = self._sync_key_filter.min_ttl if self._sync_key_filter else 0
        max_key_size = (
            self._sync_key_filter.max_key_size if self._sync_key_filter else 0
        )
        needs_type = bool(key_types)
        needs_memory = max_key_size > 0
        pipeline_batch_size = self._pipeline_batch_size()
        captured_payload_size = 0

        if (
            capture_memory_limit
            and sys.getsizeof(captured) > capture_memory_limit
        ):
            raise MemoryError(
                "source snapshot memory_limit exceeded before capturing upserts"
            )

        def record_state(
            key,
            dump_data,
            expires_at_ms,
            expiry_is_exact,
        ):
            nonlocal captured_payload_size
            fingerprint = (
                hashlib.sha256(dump_data).digest(),
                expires_at_ms,
                expiry_is_exact,
            )
            state = CapturedSourceState(
                key=key,
                dump_data=dump_data,
                expires_at_ms=expires_at_ms,
                fingerprint=fingerprint,
                expiry_is_exact=expiry_is_exact,
            )
            captured[key] = state
            captured_payload_size += (
                sys.getsizeof(state) + sys.getsizeof(dump_data)
            )
            if (
                capture_memory_limit
                and sys.getsizeof(captured) + captured_payload_size
                > capture_memory_limit
            ):
                captured.pop(key, None)
                raise MemoryError(
                    "source snapshot memory_limit exceeded while capturing "
                    "upsert payloads"
                )

        if needs_memory:
            for key in keys:
                state = capture_dump_with_preflight(
                    source_client,
                    key,
                    preserve_ttl=True,
                    key_types=key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                )
                if state is None:
                    invalid.append(key)
                    continue
                record_state(
                    key,
                    state.dump_data,
                    state.expires_at_ms,
                    getattr(state, 'expiry_is_exact', False),
                )
            return captured, invalid

        has_pexpiretime = self._source_has_pexpiretime(
            source_client, keys[0]
        )
        stride = 2 + int(has_pexpiretime) + int(needs_type)

        for offset in range(0, len(keys), pipeline_batch_size):
            chunk = keys[offset:offset + pipeline_batch_size]
            source_pipe = source_client.pipeline(transaction=True)
            for key in chunk:
                source_pipe.dump(key)
                source_pipe.pttl(key)
                if has_pexpiretime:
                    source_pipe.execute_command('PEXPIRETIME', key)
                if needs_type:
                    source_pipe.type(key)
                if needs_memory:
                    source_pipe.execute_command('MEMORY', 'USAGE', key)
            pttl_sample_started_ns = time.monotonic_ns()
            raw = source_pipe.execute(raise_on_error=False)
            observed_at_ms = int(time.time() * 1000)
            elapsed_ms = (
                max(0, time.monotonic_ns() - pttl_sample_started_ns)
                + 999_999
            ) // 1_000_000

            for index, key in enumerate(chunk):
                base = index * stride
                dump_data = raw[base]
                pttl = raw[base + 1]
                next_index = base + 2
                pexpiretime = raw[next_index] if has_pexpiretime else None
                next_index += int(has_pexpiretime)
                key_type = raw[next_index] if needs_type else None
                next_index += int(needs_type)
                memory_size = raw[next_index] if needs_memory else None
                for response in (
                    dump_data,
                    pttl,
                    key_type,
                    memory_size,
                ):
                    if isinstance(response, BaseException):
                        raise response

                try:
                    ttl_ms = int(pttl)
                except (TypeError, ValueError) as error:
                    raise ValueError(
                        f"无效的 PTTL 响应: {pttl!r}"
                    ) from error
                if dump_data is None or ttl_ms in (-2, 0):
                    invalid.append(key)
                    continue
                if ttl_ms < -2:
                    raise ValueError(f"无效的 PTTL 响应: {ttl_ms}")

                expires_at_ms = None
                expiry_is_exact = False
                remaining_ttl_ms = ttl_ms
                if ttl_ms > 0:
                    try:
                        absolute_ms = int(pexpiretime)
                    except (TypeError, ValueError):
                        absolute_ms = -1
                    remaining_ttl_ms = ttl_ms - elapsed_ms
                    if remaining_ttl_ms <= 0:
                        invalid.append(key)
                        continue
                    if absolute_ms > 0:
                        expires_at_ms = absolute_ms
                        expiry_is_exact = True
                    else:
                        expires_at_ms = observed_at_ms + remaining_ttl_ms

                if not source_state_in_dynamic_scope(
                    remaining_ttl_ms,
                    key_type=key_type,
                    memory_size=memory_size,
                    key_types=key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                ):
                    invalid.append(key)
                    continue

                record_state(
                    key,
                    dump_data,
                    expires_at_ms,
                    expiry_is_exact,
                )

        return captured, invalid

    def _prepare_change_set_for_delivery(self, changes: SourceChangeSet) -> None:
        """Bind selected upserts to one state shared by every target."""
        if not changes.upserts:
            changes.captured_upserts = {}
            return
        source = self.source_conn
        if source is None and self.orchestrators:
            source = next(
                iter(self.orchestrators.values())
            ).connection_manager.source_client
        if source is None:
            raise RuntimeError("source Redis is not connected")

        memory_limit = self._snapshot_memory_limit()
        capture_memory_limit = 0
        if memory_limit:
            retained_size = self._retained_snapshot_size()
            base_size = (
                retained_size
                + self._snapshot_size(changes.current_snapshot)
                + sys.getsizeof(changes.upserts)
                + sys.getsizeof(changes.deletions)
            )
            capture_memory_limit = memory_limit - base_size
            if capture_memory_limit <= 0:
                raise MemoryError(
                    "source snapshot memory_limit exceeded before capturing upserts"
                )

        captured, invalid = self._capture_source_states(
            source,
            changes.upserts,
            capture_memory_limit=capture_memory_limit,
        )
        changes.captured_upserts = captured
        changes.upserts = [
            key for key in changes.upserts if key in captured
        ]
        deletion_set = set(changes.deletions)
        previously_managed = self._source_snapshot or {}
        for key in invalid:
            changes.current_snapshot.pop(key, None)
            if key in previously_managed and key not in deletion_set:
                changes.deletions.append(key)
                deletion_set.add(key)
        for key, state in captured.items():
            changes.current_snapshot[key] = state.fingerprint

    def _commit_change_set(self, changes: SourceChangeSet) -> None:
        committed = self._source_snapshot
        if committed is None:
            committed = {}
            self._source_snapshot = committed
        for key in changes.deletions:
            committed.pop(key, None)
        for key in changes.upserts:
            fingerprint = changes.current_snapshot.get(key)
            if fingerprint is None:
                committed.pop(key, None)
            else:
                committed[key] = fingerprint

    def _sync_keys_to_target(
        self,
        target_name: str,
        keys: List[bytes],
        deleted_keys: Optional[List[bytes]] = None,
        prepared_states: Optional[Dict[bytes, CapturedSourceState]] = None,
    ) -> bool:
        """Apply a source change set to one target without advancing on failure."""
        if deleted_keys is None:
            deleted_keys = []
        stats = self.stats[target_name]
        synced = 0
        skipped_deletions = 0
        total_operations = len(keys) + len(deleted_keys)
        try:
            orchestrator = self.orchestrators[target_name]
            source_client = orchestrator.connection_manager.source_client
            target_client = orchestrator.connection_manager.target_client
            pipeline_batch_size = self._pipeline_batch_size()
            key_types = self.config['sync']['incremental_sync'].get('key_types')
            min_ttl = self._sync_key_filter.min_ttl if self._sync_key_filter else 0
            max_key_size = (
                self._sync_key_filter.max_key_size if self._sync_key_filter else 0
            )
            if prepared_states is None:
                source_states, invalid_keys = self._capture_source_states(
                    source_client, keys
                )
            else:
                source_states = prepared_states
                invalid_keys = [key for key in keys if key not in source_states]
            invalid_key_set = set(invalid_keys)

            for offset in range(0, len(keys), pipeline_batch_size):
                chunk = keys[offset:offset + pipeline_batch_size]
                entries = []
                vanished = [key for key in chunk if key in invalid_key_set]
                for key in chunk:
                    state = source_states.get(key)
                    if state is not None:
                        entries.append(
                            (
                                key,
                                state.expires_at_ms,
                                state.dump_data,
                                state.expiry_is_exact,
                            )
                        )

                target_pipe = target_client.pipeline(transaction=False)
                target_operations = []
                for key, expires_at, dump_data, expiry_is_exact in entries:
                    if expires_at is None:
                        target_pipe.restore(key, 0, dump_data, replace=True)
                    else:
                        target_pipe.restore(
                            key,
                            expires_at,
                            dump_data,
                            replace=True,
                            absttl=True,
                        )
                    target_operations.append(
                        (
                            'restore',
                            key,
                            expires_at,
                            dump_data,
                            expiry_is_exact,
                        )
                    )
                results = (
                    target_pipe.execute(raise_on_error=False)
                    if target_operations
                    else []
                )
                if len(results) != len(target_operations):
                    raise RuntimeError(
                        "目标 Pipeline 返回数量与请求数量不一致: "
                        f"{len(results)} != {len(target_operations)}"
                    )
                for target_operation, result in zip(target_operations, results):
                    (
                        operation,
                        key,
                        expires_at,
                        dump_data,
                        expiry_is_exact,
                    ) = target_operation
                    if not isinstance(result, Exception):
                        continue
                    if (
                        operation == 'restore'
                        and isinstance(result, redis.ResponseError)
                        and _is_restore_compatibility_error(result)
                    ):
                        if (
                            expires_at is not None
                            and _is_absttl_compatibility_error(result)
                        ):
                            try:
                                restored = restore_dump_with_deadline(
                                    target_client,
                                    key,
                                    dump_data,
                                    expires_at,
                                    overwrite=True,
                                    prefer_absttl=False,
                                    expires_at_is_exact=expiry_is_exact,
                                )
                            except redis.ResponseError as legacy_error:
                                if not _is_restore_compatibility_error(legacy_error):
                                    raise
                            else:
                                if not restored:
                                    raise RuntimeError(
                                        f"旧版目标 RESTORE 跳过了键 {key!r}"
                                    )
                                continue
                        fallback_pttl = (
                            -1
                            if expires_at is None
                            else max(
                                1,
                                expires_at - int(time.time() * 1000),
                            )
                        )
                        if _sync_key_fallback(
                            source_client,
                            target_client,
                            key,
                            fallback_pttl,
                            True,
                            overwrite=True,
                            expires_at_ms=expires_at,
                            expected_dump=dump_data,
                            key_types=key_types,
                            min_ttl=min_ttl,
                            max_key_size=max_key_size,
                            expires_at_is_exact=expiry_is_exact,
                        ):
                            continue
                        raise RuntimeError(
                            f"源状态在 RESTORE 兼容降级前发生变化: {key!r}"
                        )
                    raise result
                synced += len(target_operations)
                deleted, skipped = self._delete_target_change_keys(
                    target_name, vanished
                )
                synced += deleted
                skipped_deletions += skipped

            for offset in range(0, len(deleted_keys), pipeline_batch_size):
                requested_chunk = deleted_keys[
                    offset:offset + pipeline_batch_size
                ]
                deleted, skipped = self._delete_target_change_keys(
                    target_name, requested_chunk
                )
                synced += deleted
                skipped_deletions += skipped

        except Exception as e:
            stats.total_synced += synced
            failed = max(
                0, total_operations - synced - skipped_deletions
            )
            self._record_target_failure(
                stats, e, failed_operations=failed
            )
            self.logger.error(
                "Sync to target %s failed: %s", target_name, e, exc_info=True
            )
            return False

        stats.total_synced += synced
        stats.last_sync_time = time.time()
        self._reset_target_failures(stats)
        stats.is_healthy = True
        return True

    def _delete_target_change_keys(
        self,
        target_name: str,
        keys: List[bytes],
        *,
        key_types_override=None,
    ) -> Tuple[int, int]:
        """Atomically recheck dynamic target scope and delete one batch."""
        selected = (
            list(self._sync_key_filter.filter_names(keys))
            if self._sync_key_filter
            else list(keys)
        )
        skipped = len(keys) - len(selected)
        if not selected:
            return 0, skipped
        target = self.orchestrators[target_name].connection_manager.target_client
        key_types = key_types_override
        if key_types is None:
            key_types = (
                self.config.get('sync', {})
                .get('incremental_sync', {})
                .get('key_types')
            )
        min_ttl = self._sync_key_filter.min_ttl if self._sync_key_filter else 0
        max_key_size = (
            self._sync_key_filter.max_key_size if self._sync_key_filter else 0
        )
        if key_types or min_ttl > 0 or max_key_size > 0:
            command = build_atomic_filtered_delete_command(
                selected,
                key_types=key_types,
                min_ttl=min_ttl,
                max_key_size=max_key_size,
            )
            deleted = int(target.execute_command(*command))
            if deleted < 0 or deleted > len(selected):
                raise RuntimeError(
                    f"目标原子删除返回无效数量: {deleted}"
                )
            return deleted, skipped + len(selected) - deleted

        pipe = target.pipeline(transaction=False)
        for key in selected:
            pipe.delete(key)
        results = pipe.execute()
        if len(results) != len(selected):
            raise RuntimeError(
                "目标 DEL Pipeline 返回数量与请求数量不一致: "
                f"{len(results)} != {len(selected)}"
            )
        for result in results:
            if isinstance(result, BaseException):
                raise result
        return len(selected), skipped

    def _perform_unified_incremental_sync(self) -> bool:
        """执行统一增量同步（扫描一次，同步到所有目标）"""
        try:
            # 1. 扫描一次源Redis
            changes = self._scan_source_for_changes()
            if changes is None:
                return False
            self._prepare_change_set_for_delivery(changes)

            # 2. 并行同步到所有健康的目标
            healthy_targets = [
                name for name, stats in self.stats.items()
                if stats.is_healthy
            ] if hasattr(self, 'stats') else []

            pending_by_target = getattr(
                self, '_target_pending_deletions', {}
            )
            has_global_changes = bool(changes.upserts or changes.deletions)
            has_healthy_pending = any(
                pending_by_target.get(target_name)
                for target_name in healthy_targets
            )
            if not has_global_changes and not has_healthy_pending:
                if self._source_snapshot is None:
                    self._source_snapshot = changes.current_snapshot
                self.logger.debug("✓ 统一增量同步: 无变更")
                return True

            if not healthy_targets:
                self.logger.warning("⚠️  没有健康的目标可以同步")
                return False

            change_count = len(changes.upserts) + len(changes.deletions)
            self.logger.info(f"⇉ 并行同步 {change_count} 个键到 {len(healthy_targets)} 个目标")

            # 使用线程池并行同步
            futures = []
            for target_name in healthy_targets:
                pending = pending_by_target.get(target_name, {})
                pending_deletions = [
                    key for key in pending
                    if key not in changes.current_snapshot
                ]
                pending_upserts = any(
                    key in pending for key in changes.upserts
                )
                if (
                    not has_global_changes
                    and not pending_deletions
                    and not pending_upserts
                ):
                    continue
                future = self.executor.submit(
                    self._sync_change_set_to_target,
                    target_name,
                    changes.upserts,
                    changes.deletions,
                    pending_deletions,
                    changes.captured_upserts,
                )
                self._track_executor_future(future)
                futures.append((target_name, pending_deletions, future))

            # 等待所有同步完成
            success_count = 0
            for target_name, pending_deletions, future in futures:
                try:
                    if future.result():
                        success_count += 1
                        pending = pending_by_target.get(target_name)
                        if pending is not None:
                            for key in pending_deletions:
                                pending.pop(key, None)
                            for key in changes.upserts:
                                pending.pop(key, None)
                            if not pending:
                                pending_by_target.pop(target_name, None)
                        self.logger.info(f"  ✓ {target_name}: 同步完成")
                    else:
                        self.logger.warning(f"  ✗ {target_name}: 同步失败")
                except Exception as e:
                    self.logger.error(f"  ✗ {target_name}: {e}")

            all_succeeded = success_count == len(futures)
            if has_global_changes and all_succeeded:
                self._commit_change_set(changes)
            self.logger.info(
                f"✓ 统一同步完成: {success_count}/{len(futures)} 个目标成功"
            )
            return all_succeeded

        except Exception as e:
            self.logger.error(f"❌ 统一增量同步失败: {e}", exc_info=True)
            return False

    def _sync_change_set_to_target(
        self,
        target_name: str,
        upserts: List[bytes],
        deletions: List[bytes],
        pending_deletions: List[bytes],
        prepared_states: Optional[Dict[bytes, CapturedSourceState]] = None,
    ) -> bool:
        """Apply global changes and this target's reconciliation deletions."""
        if (upserts or deletions) and not self._sync_keys_to_target(
            target_name, upserts, deletions, prepared_states
        ):
            return False
        if pending_deletions and not self._sync_keys_to_target(
            target_name, [], pending_deletions
        ):
            return False
        return True

    def _unified_sync_coordinator(self):
        """统一同步协调器（扫描一次源，同步到所有目标）"""
        self.logger.info("🚀 启动统一同步协调器")

        # 根据模式执行初始同步
        sync_mode = self.config['sync']['mode']
        self.logger.info(f"📋 同步模式: {sync_mode}")

        inc_config = self.config['sync'].get('incremental_sync', {})
        inc_method = inc_config.get(
            'method', inc_config.get('change_detection_method', 'scan')
        )
        psync_owns_initial_alignment = (
            sync_mode == 'hybrid'
            and inc_config.get('enabled', True)
            and inc_method in {'sync', 'psync'}
        )

        if psync_owns_initial_alignment:
            # Starting PSYNC first gives the initial full copy an exact stream
            # boundary. Its FULLRESYNC callback preserves target-owned keys while
            # the capture worker retains every concurrent source change.
            self.logger.info(
                "🔄 HYBRID 实时模式由首次 FULLRESYNC 执行受保护的全量对齐"
            )

        # 全量同步阶段（并行执行到所有目标）
        if sync_mode in ['full', 'hybrid'] and not psync_owns_initial_alignment:
            if (
                sync_mode == 'hybrid'
                and inc_config.get('enabled', True)
                and inc_method == 'scan'
            ):
                try:
                    pre_full_snapshot = self._build_source_snapshot()
                    # A live SCAN can observe an intermediate value or miss a
                    # key that is deleted and recreated during the copy.
                    self._source_snapshot = self._full_reconciliation_baseline(
                        pre_full_snapshot, in_place=True
                    )
                except Exception as e:
                    self.logger.error(
                        "Failed to capture pre-full source snapshot: %s", e,
                        exc_info=True,
                    )
                    self.running = False
                    self.shutdown_event.set()
                    return

            self.logger.info("🔄 开始全量同步阶段")
            target_count = len(self.target_connections)
            self.logger.info(f"   同步目标数: {target_count}")
            self.logger.info(f"   优化模式: 并行全量同步")

            # 使用线程池并行执行全量同步
            full_sync_start = time.time()
            with ThreadPoolExecutor(max_workers=min(target_count, 8)) as executor:
                futures = {}
                submit_times = {}
                for target_name in self.target_connections.keys():
                    submit_time = time.time()
                    self.logger.info(f"  → 启动全量同步: {target_name} (提交时间: {submit_time:.2f})")
                    future = executor.submit(self._perform_full_sync, target_name)
                    futures[future] = target_name
                    submit_times[target_name] = submit_time

                # 等待所有同步完成
                for future in futures:
                    target_name = futures[future]
                    try:
                        success = future.result()
                        complete_time = time.time()
                        wait_time = complete_time - submit_times[target_name]
                        if success:
                            self.logger.info(f"  ✅ {target_name} 全量同步完成 (从提交到完成: {wait_time:.2f}秒)")
                        else:
                            self.logger.error(f"  ❌ {target_name} 全量同步失败")
                    except Exception as e:
                        self.logger.error(f"  ❌ {target_name} 全量同步异常: {e}")

            full_sync_total = time.time() - full_sync_start
            self.logger.info(f"✅ 全量同步阶段完成，总耗时: {full_sync_total:.2f}秒")

            if (
                sync_mode == 'hybrid'
                and inc_config.get('enabled', True)
                and inc_method == 'scan'
                and self._source_snapshot is not None
            ):
                reconciliation_config = self._recovery_sync_config(
                    clear_target=False
                )
                for target_name, stats in self.stats.items():
                    if not stats.is_healthy or target_name not in self.orchestrators:
                        continue
                    try:
                        self._arm_scan_reconciliation(
                            target_name,
                            reconciliation_config,
                            include_target_only=bool(
                                self.config['sync'].get('full_sync', {}).get(
                                    'clear_target', False
                                )
                            ),
                        )
                    except Exception as exc:
                        self._record_target_failure(
                            stats, exc, force_unhealthy=True
                        )
                        self.logger.error(
                            "目标 %s 的 post-full 对账扫描失败: %s",
                            target_name,
                            exc,
                        )

            self.logger.info("✅ 全量同步阶段完成")

            if sync_mode == 'full' and any(
                not stats.is_healthy for stats in self.stats.values()
            ):
                self.logger.error("全量模式存在失败目标，服务停止并保留失败状态")
                self.running = False
                self.shutdown_event.set()
                return

        # 增量同步阶段
        if sync_mode in ['incremental', 'hybrid']:
            inc_config = self.config['sync']['incremental_sync']
            if inc_config.get('enabled', True):
                inc_method = inc_config.get(
                    'method', inc_config.get('change_detection_method', 'scan')
                )

                # 检查增量同步模式
                if inc_method in ['sync', 'psync']:
                    # 使用 SYNC/PSYNC 实时复制模式
                    self.logger.info(f"🚀 使用 {inc_method.upper()} 实时复制模式")
                    self._start_realtime_replication(inc_method, inc_config)
                else:
                    # 使用 SCAN 轮询模式
                    sync_interval = inc_config.get('interval', 5)
                    target_count = len(self.target_connections)

                    self.logger.info(f"⏱️  增量同步间隔: {sync_interval} 秒")
                    self.logger.info(f"✨ 优化模式：扫描一次源，并行同步到 {target_count} 个目标")

                    sync_count = 0
                    while self.running and not self.shutdown_event.is_set():
                        try:
                            sync_count += 1

                            # 检查不健康的目标
                            unhealthy_targets = [
                                name for name, stats in self.stats.items()
                                if not stats.is_healthy
                            ]

                            if unhealthy_targets:
                                self.logger.warning(f"⚠️  发现 {len(unhealthy_targets)} 个不健康的目标: {unhealthy_targets}")

                                # 尝试恢复不健康的目标
                                for target_name in unhealthy_targets:
                                    if not self._target_recovery_is_due(target_name):
                                        continue
                                    self.logger.info(f"🔄 尝试恢复目标: {target_name}")
                                    if not self._recover_target_connection(target_name):
                                        self.logger.warning(
                                            "  ✗ 目标 %s 重新连接失败", target_name
                                        )
                                        continue
                                    recovery_config = self._recovery_sync_config(
                                        clear_target=False
                                    )
                                    try:
                                        self._clear_managed_target_scope(
                                            target_name, recovery_config
                                        )
                                    except Exception as exc:
                                        self._record_target_failure(
                                            self.stats[target_name],
                                            exc,
                                            force_unhealthy=True,
                                        )
                                        self.logger.error(
                                            "目标 %s 恢复前清理失败: %s",
                                            target_name,
                                            exc,
                                        )
                                        continue
                                    if self._perform_full_sync(
                                        target_name, recovery_config
                                    ):
                                        try:
                                            self._arm_scan_reconciliation(
                                                target_name, recovery_config
                                            )
                                        except Exception as exc:
                                            self._record_target_failure(
                                                self.stats[target_name],
                                                exc,
                                                force_unhealthy=True,
                                            )
                                            self.logger.error(
                                                "目标 %s 恢复后对账扫描失败: %s",
                                                target_name,
                                                exc,
                                            )
                                            continue
                                        self.logger.info(f"  ✓ 目标 {target_name} 恢复成功")
                                        self.stats[target_name].is_healthy = True
                                        self._reset_target_failures(
                                            self.stats[target_name]
                                        )
                                        self._clear_target_recovery_deadline(target_name)
                                    else:
                                        self.logger.warning(f"  ✗ 目标 {target_name} 恢复失败")

                            # 统一执行增量同步
                            self.logger.info(f"🔄 [{sync_count}] 开始统一增量同步")
                            self._perform_unified_incremental_sync()

                            # 等待下次同步
                            self.logger.info(f"⏳ 等待 {sync_interval} 秒后进行下次同步...")
                            if self.shutdown_event.wait(sync_interval):
                                break

                        except Exception as e:
                            self.logger.error(f"❌ 统一同步协调器异常: {e}", exc_info=True)
                            if self.shutdown_event.wait(sync_interval):
                                break

        self.logger.info("🛑 统一同步协调器结束")

    def _configured_target(self, target_name: str) -> Optional[Dict[str, Any]]:
        unavailable = self._unavailable_targets.get(target_name)
        if unavailable is not None:
            return dict(unavailable)
        for target_config in self.config.get('targets', []):
            if target_config.get('name') == target_name:
                return dict(target_config)
        return None

    def _record_realtime_target_success(self, target_name: str) -> None:
        stats = self.stats.setdefault(target_name, SyncStats())
        stats.total_synced += 1
        stats.last_sync_time = time.time()
        self._reset_target_failures(stats)
        stats.is_healthy = True

    def _deactivate_realtime_target(
        self,
        target_name: str,
        *,
        force: bool = False,
        record_failure: bool = True,
    ) -> bool:
        """Remove a failed target so healthy targets can keep committing."""
        target_config = self._configured_target(target_name)
        if target_config is None:
            return False

        stats = self.stats.setdefault(target_name, SyncStats())
        failures = stats.consecutive_failures
        if record_failure:
            failures = self._record_target_failure(
                stats,
                'real-time replication delivery failed',
                failed_operations=1,
                force_unhealthy=force,
            )
        elif force:
            stats.is_healthy = False
        if stats.is_healthy:
            self.logger.warning(
                "目标 %s 实时交付失败 (%s)，未达到摘除阈值",
                target_name,
                failures,
            )
            return False

        manager = None
        if self.incremental_service is not None:
            manager = self.incremental_service.unregister_target(target_name)
        if manager is None:
            manager = self.target_connections.pop(target_name, None)
        self.orchestrators.pop(target_name, None)
        self._unavailable_targets[target_name] = target_config

        if manager is not None:
            try:
                manager.close()
            except Exception:
                pass
        self.logger.error("目标 %s 已从实时分发中摘除，等待全量恢复", target_name)
        return True

    def _managed_target_key_batches(
        self,
        target_name: str,
        config: MigrationConfig,
    ):
        target = self.orchestrators[target_name].connection_manager.target_client
        cursor = 0
        allowed_types = set(config.key_types or [])
        pipeline_batch_size = self._pipeline_batch_size()
        while True:
            cursor, raw_keys = target.scan(
                cursor=cursor,
                match=config.key_pattern,
                count=max(1, config.scan_count),
            )
            keys = [self._key_bytes(key) for key in raw_keys]
            if self._sync_key_filter:
                keys = list(self._sync_key_filter.filter_batch(target, keys))
            if allowed_types and keys:
                typed_keys = []
                for offset in range(0, len(keys), pipeline_batch_size):
                    chunk = keys[offset:offset + pipeline_batch_size]
                    pipe = target.pipeline(transaction=False)
                    for key in chunk:
                        pipe.type(key)
                    raw_types = pipe.execute()
                    typed_keys.extend(
                        key
                        for key, key_type in zip(chunk, raw_types)
                        if (
                            key_type.decode('ascii')
                            if isinstance(key_type, bytes)
                            else str(key_type)
                        ) in allowed_types
                    )
                keys = typed_keys
            for offset in range(0, len(keys), pipeline_batch_size):
                yield keys[offset:offset + pipeline_batch_size]
            if cursor == 0:
                break

    def _clear_managed_target_scope(
        self, target_name: str, config: MigrationConfig
    ) -> None:
        for chunk in self._managed_target_key_batches(target_name, config):
            self._delete_target_change_keys(
                target_name,
                list(chunk),
                key_types_override=config.key_types,
            )

    def _delete_target_keys(
        self, target_name: str, keys, *, key_types_override=None
    ) -> None:
        batch_size = self._pipeline_batch_size()
        chunk = []

        def delete_chunk() -> None:
            self._delete_target_change_keys(
                target_name,
                list(chunk),
                key_types_override=key_types_override,
            )

        for key in keys:
            chunk.append(key)
            if len(chunk) >= batch_size:
                delete_chunk()
                chunk = []
        if chunk:
            delete_chunk()

    def _arm_scan_reconciliation(
        self,
        target_name: str,
        config: MigrationConfig,
        *,
        include_target_only: bool = True,
    ) -> None:
        """Force a post-full replay and optionally prune target-only keys."""
        snapshot = self._source_snapshot
        if snapshot is None:
            snapshot = {}
        baseline = self._full_reconciliation_baseline(
            snapshot, in_place=True
        )
        self._source_snapshot = baseline
        if include_target_only:
            memory_limit = self._snapshot_memory_limit()
            pending_by_target = getattr(
                self, '_target_pending_deletions', None
            )
            if pending_by_target is None:
                pending_by_target = {}
                self._target_pending_deletions = pending_by_target
            retained_size = (
                self._retained_snapshot_size() if memory_limit else 0
            )
            pending = pending_by_target.get(target_name)
            for batch in self._managed_target_key_batches(target_name, config):
                for key in batch:
                    if key in baseline or (pending is not None and key in pending):
                        continue
                    created_pending = pending is None
                    before_outer_size = sys.getsizeof(pending_by_target)
                    if created_pending:
                        pending = {}
                        pending_by_target[target_name] = pending
                    before_pending_size = sys.getsizeof(pending)
                    pending[key] = None
                    added_size = sys.getsizeof(pending) - before_pending_size
                    added_size += sys.getsizeof(key)
                    if created_pending:
                        added_size += (
                            sys.getsizeof(pending_by_target)
                            - before_outer_size
                            + sys.getsizeof(target_name)
                            + before_pending_size
                        )
                    if (
                        memory_limit
                        and retained_size + added_size > memory_limit
                    ):
                        pending.pop(key, None)
                        if created_pending:
                            pending_by_target.pop(target_name, None)
                            pending = None
                        raise MemoryError(
                            "source snapshot memory_limit exceeded while "
                            "tracking target-only keys"
                        )
                    retained_size += added_size

    def _align_realtime_target(
        self,
        target_name: str,
        *,
        prune_managed_scope: bool = True,
        bootstrap_previous=None,
        bootstrap_current=None,
        bootstrap_clear_target: bool = False,
    ) -> bool:
        if self._service_is_stopping():
            return False
        config = self._recovery_sync_config(
            clear_target=bootstrap_clear_target
        )
        try:
            if not bootstrap_clear_target:
                if prune_managed_scope:
                    self._clear_managed_target_scope(target_name, config)
                else:
                    previous = bootstrap_previous or {}
                    current = bootstrap_current or {}
                    self._delete_target_keys(
                        target_name,
                        (key for key in previous if key not in current),
                        key_types_override=config.key_types,
                    )
            # With bootstrap_clear_target, the full migration performs FLUSHDB
            # after PSYNC capture starts and preserves the configured semantics.
        except Exception as exc:
            self._record_target_failure(
                self.stats.setdefault(target_name, SyncStats()),
                exc,
                failed_operations=1,
                force_unhealthy=True,
            )
            self.logger.error("清理目标 %s 的同步范围失败: %s", target_name, exc)
            return False
        return self._perform_full_sync(target_name, config)

    def _apply_replication_snapshot(
        self, _rdb_data: bytes, *, force_recovery: bool = False
    ) -> bool:
        """Align active targets at FULLRESYNC before applying buffered commands."""
        if self._service_is_stopping():
            return False
        target_names = list(self.target_connections)
        is_bootstrap = (
            not force_recovery
            and not getattr(self, '_realtime_baseline_established', False)
        )
        bootstrap_previous = None
        bootstrap_current = None
        bootstrap_clear_target = bool(
            is_bootstrap
            and self.config['sync'].get('full_sync', {}).get(
                'clear_target', False
            )
        )
        if is_bootstrap:
            previous = getattr(self, '_realtime_bootstrap_snapshot', None) or {}
            if previous:
                bootstrap_previous = previous
                bootstrap_current = self._build_source_snapshot()
        if not target_names:
            self._realtime_baseline_established = True
            self._realtime_bootstrap_snapshot = None
            return True

        executor = ThreadPoolExecutor(
            max_workers=min(len(target_names), 8),
            thread_name_prefix="redis-fullresync-alignment",
        )
        future_targets = {
            executor.submit(
                    self._align_realtime_target,
                    name,
                    prune_managed_scope=not is_bootstrap,
                    bootstrap_previous=bootstrap_previous,
                    bootstrap_current=bootstrap_current,
                    bootstrap_clear_target=bootstrap_clear_target,
                ): name
            for name in target_names
        }
        self._track_alignment_futures(future_targets)
        pending = set(future_targets)
        try:
            while pending:
                if self._service_is_stopping():
                    for future in pending:
                        future.cancel()
                    return False
                done, pending = wait(
                    pending,
                    timeout=0.05,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    target_name = future_targets[future]
                    if self._service_is_stopping():
                        for remaining in pending:
                            remaining.cancel()
                        return False
                    try:
                        aligned = future.result()
                    except Exception as exc:
                        self.logger.error(
                            "FULLRESYNC 目标对齐失败 %s: %s", target_name, exc
                        )
                        self._record_target_failure(
                            self.stats.setdefault(target_name, SyncStats()),
                            exc,
                            failed_operations=1,
                            force_unhealthy=True,
                        )
                        self._deactivate_realtime_target(
                            target_name, force=True, record_failure=False
                        )
                    else:
                        if not aligned:
                            self._deactivate_realtime_target(
                                target_name, force=True, record_failure=False
                            )
        finally:
            try:
                executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:  # Python 3.7/3.8 compatibility
                executor.shutdown(wait=False)
        self._realtime_baseline_established = True
        self._realtime_bootstrap_snapshot = None
        # Failed targets are recovered from a fresh full copy later. Keeping the
        # source stream moving prevents one outage from stalling healthy targets.
        return True

    def _track_alignment_futures(self, future_targets) -> None:
        lock = getattr(self, '_alignment_futures_lock', None)
        if lock is None:
            lock = self._alignment_futures_lock = threading.Lock()
        active = getattr(self, '_alignment_futures', None)
        if active is None:
            active = self._alignment_futures = set()
        with lock:
            active.update(future_targets)

        def discard(completed) -> None:
            with lock:
                active.discard(completed)

        for future in future_targets:
            future.add_done_callback(discard)

    def _wait_for_alignment_futures(self, timeout: float) -> bool:
        lock = getattr(self, '_alignment_futures_lock', None)
        active = getattr(self, '_alignment_futures', None)
        if lock is None or active is None:
            return True
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            with lock:
                pending = {future for future in active if not future.done()}
            if not pending:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            wait(pending, timeout=min(0.05, remaining))

    def _track_executor_future(self, future) -> None:
        add_done_callback = getattr(future, 'add_done_callback', None)
        if not callable(add_done_callback):
            return
        lock = getattr(self, '_executor_futures_lock', None)
        if lock is None:
            lock = self._executor_futures_lock = threading.Lock()
        active = getattr(self, '_executor_futures', None)
        if active is None:
            active = self._executor_futures = set()
        with lock:
            active.add(future)

        def discard(completed) -> None:
            with lock:
                active.discard(completed)

        add_done_callback(discard)

    def _pending_executor_futures(self):
        lock = getattr(self, '_executor_futures_lock', None)
        active = getattr(self, '_executor_futures', None)
        if lock is None or active is None:
            return set()
        with lock:
            return {future for future in active if not future.done()}

    def _apply_database_resync(self) -> bool:
        return self._apply_replication_snapshot(b'', force_recovery=True)

    def _recover_realtime_target(self, target_name: str) -> bool:
        if self._service_is_stopping():
            return False
        target_config = self._configured_target(target_name)
        if target_config is None or self.incremental_service is None:
            return False
        try:
            manager, orchestrator = self._prepare_target_resources(target_config)
        except Exception as exc:
            if self._service_is_stopping():
                return False
            self._record_target_connection_failure(target_config, exc)
            return False

        success = False
        registered = False
        with self.incremental_service.command_barrier():
            if not self._service_is_stopping():
                self.incremental_service.register_target(target_name, manager)
                registered = True
                self.orchestrators[target_name] = orchestrator
                try:
                    success = self._align_realtime_target(
                        target_name, prune_managed_scope=True
                    )
                    if success and not self._service_is_stopping():
                        self._activate_target(target_config, manager, orchestrator)
                        self._reset_target_failures(self.stats[target_name])
                    else:
                        success = False
                        self.incremental_service.unregister_target(target_name)
                        registered = False
                        self.orchestrators.pop(target_name, None)
                except Exception as exc:
                    self.logger.error("目标 %s 实时恢复失败: %s", target_name, exc)
                    self.incremental_service.unregister_target(target_name)
                    registered = False
                    self.orchestrators.pop(target_name, None)
        if not success:
            if registered:
                self.incremental_service.unregister_target(target_name)
                self.orchestrators.pop(target_name, None)
            self._unavailable_targets[target_name] = target_config
            try:
                manager.close()
            except Exception:
                pass
        return success

    def _service_is_stopping(self) -> bool:
        if getattr(self, '_stopped', False):
            return True
        event = getattr(self, 'shutdown_event', None)
        return event is not None and event.is_set()

    def _start_realtime_replication(self, mode: str, config: Dict[str, Any]):
        """启动实时复制（SYNC/PSYNC）"""
        try:
            realtime_config = dict(config)
            filters = dict(self.config['sync'].get('filters') or {})
            filters.update(realtime_config.get('filters') or {})
            realtime_config['filters'] = filters
            realtime_config['source_db'] = int(self.config['source'].get('db', 0))
            realtime_config['pipeline_batch_size'] = self._pipeline_batch_size()
            realtime_config.setdefault('apply_mode', 'key_state')
            realtime_config['materialize_snapshot'] = False
            realtime_config['snapshot_callback'] = self._apply_replication_snapshot
            realtime_config['database_resync_callback'] = self._apply_database_resync
            realtime_config['target_failure_callback'] = (
                self._deactivate_realtime_target
            )
            realtime_config['target_success_callback'] = (
                self._record_realtime_target_success
            )

            self.incremental_service = UnifiedIncrementalService(
                mode=mode,
                source_conn=self.source_conn,
                target_connections=self.target_connections,
                config=realtime_config,
            )

            worker = threading.Thread(
                target=self.incremental_service.start,
                name=f'{mode}-replication-service',
                daemon=True,
            )
            worker.start()

            recovery_delay = max(
                1.0,
                float(self.config['service']['failover'].get('recovery_delay', 60)),
            )
            while self.running and not self.shutdown_event.is_set():
                now = time.monotonic()
                for target_name in list(self._unavailable_targets):
                    if not self._target_recovery_is_due(target_name, now=now):
                        continue
                    self.logger.info("尝试恢复实时目标: %s", target_name)
                    if self._recover_realtime_target(target_name):
                        self._clear_target_recovery_deadline(target_name)

                if not worker.is_alive():
                    self.logger.error("%s 实时复制线程已退出", mode.upper())
                    self.running = False
                    self.shutdown_event.set()
                    break
                self.shutdown_event.wait(min(1.0, recovery_delay))

            if self.incremental_service:
                self.incremental_service.stop()
            worker.join(timeout=10)

        except Exception as e:
            self.logger.error(f"❌ {mode.upper()} 复制失败: {e}", exc_info=True)
            self.running = False
            self.shutdown_event.set()

    def start(self) -> bool:
        """启动同步服务"""
        lifecycle_lock = getattr(self, '_lifecycle_lock', None)
        if lifecycle_lock is None:
            lifecycle_lock = self._lifecycle_lock = threading.RLock()
        with lifecycle_lock:
            if getattr(self, '_stopped', False) or self.running:
                return False
            if getattr(self, '_starting', False):
                return False
            self._starting = True
            self._starting_thread_id = threading.get_ident()
            self._start_complete = getattr(
                self, '_start_complete', threading.Event()
            )
            self._start_complete.clear()
            self.shutdown_event.clear()
        self.logger.info("启动Redis同步服务...")
        started = False
        try:
            self._setup_signal_handlers()
            if self._service_is_stopping() or not self._connect_source():
                return False
            if self._service_is_stopping() or not self._connect_targets():
                return False
            if self._service_is_stopping():
                return False
            if not self.target_connections:
                self.logger.error("没有可用的目标Redis实例")
                return False

            if self.web_ui:
                try:
                    self.web_ui.start()
                except Exception as e:
                    self.logger.warning(f"Web UI启动失败: {e}")
            if self._service_is_stopping():
                return False

            with lifecycle_lock:
                if getattr(self, '_stopped', False) or self.shutdown_event.is_set():
                    return False
                self.running = True
                coordinator_thread = threading.Thread(
                    target=self._unified_sync_coordinator,
                    name="unified-sync-coordinator",
                    daemon=True,
                )
                coordinator_thread.start()
                self.sync_tasks.append(coordinator_thread)
                started = True

            self.logger.info(f"✅ Redis同步服务启动成功")
            self.logger.info(f"   同步目标数: {len(self.target_connections)}")
            self.logger.info(f"   优化模式: 统一扫描 + 并行分发")
            return True
        finally:
            with lifecycle_lock:
                self._starting = False
                self._starting_thread_id = None
                self._start_complete.set()
                stopped_elsewhere = getattr(self, '_stopped', False)
            if not started and not stopped_elsewhere:
                self.stop()
    
    def stop(self, timeout: float = DEFAULT_SERVICE_STOP_TIMEOUT):
        """Stop every service component within one shared deadline."""
        deadline = time.monotonic() + max(0.0, float(timeout))

        def remaining() -> float:
            return max(0.0, deadline - time.monotonic())

        shutdown_errors = []
        lifecycle_lock = getattr(self, '_lifecycle_lock', None)
        if lifecycle_lock is None:
            lifecycle_lock = self._lifecycle_lock = threading.RLock()
        if not lifecycle_lock.acquire(timeout=remaining()):
            message = "获取停止生命周期锁超时"
            self.logger.error("Redis同步服务停止不完整: %s", message)
            raise RuntimeError(message)
        try:
            stop_complete = getattr(self, '_stop_complete', None)
            if stop_complete is None:
                stop_complete = self._stop_complete = threading.Event()
                stop_complete.set()
            if getattr(self, '_stopped', False):
                wait_for_stop = not stop_complete.is_set()
                owns_stop = False
            else:
                self._stopped = True
                self.running = False
                self.shutdown_event.set()
                stop_complete.clear()
                wait_for_stop = False
                owns_stop = True
            starting_elsewhere = (
                getattr(self, '_starting', False)
                and getattr(self, '_starting_thread_id', None)
                != threading.get_ident()
            )
            start_complete = getattr(self, '_start_complete', None)
        finally:
            lifecycle_lock.release()
        if not owns_stop:
            if wait_for_stop:
                if not stop_complete.wait(remaining()):
                    message = "等待现有停止流程超时"
                    self.logger.error("Redis同步服务停止不完整: %s", message)
                    raise RuntimeError(message)
            return
        if starting_elsewhere and start_complete is not None:
            if not start_complete.wait(remaining()):
                shutdown_errors.append("启动流程未退出")

        self.logger.info("停止Redis同步服务...")
        try:
            incremental_service = self.incremental_service
            if incremental_service:
                try:
                    try:
                        incremental_service.stop(timeout=0)
                    except TypeError:
                        incremental_service.stop()
                except Exception as e:
                    shutdown_errors.append(f"实时复制服务停止失败: {e}")

            current = threading.current_thread()
            sync_tasks = [
                thread for thread in list(self.sync_tasks) if thread is not current
            ]
            grace_deadline = min(
                deadline, time.monotonic() + COOPERATIVE_STOP_GRACE
            )
            for thread in sync_tasks:
                thread.join(
                    timeout=max(0.0, grace_deadline - time.monotonic())
                )

            # Closing Redis pools after the cooperative grace interrupts SCAN,
            # target writes, and replication reads that are still blocked in I/O.
            managers = list(self.target_connections.values())
            self.target_connections.clear()
            orchestrators = getattr(self, 'orchestrators', None)
            if orchestrators is not None:
                orchestrators.clear()
            for conn_manager in managers:
                try:
                    conn_manager.close()
                except Exception:
                    pass

            source_conn = self.source_conn
            self.source_conn = None
            if source_conn:
                try:
                    source_conn.close()
                except Exception:
                    pass
                try:
                    source_conn.connection_pool.disconnect()
                except Exception:
                    pass

            alive_sync_tasks = []
            for thread in sync_tasks:
                thread.join(timeout=remaining())
                is_alive = getattr(thread, 'is_alive', None)
                if callable(is_alive) and is_alive():
                    alive_sync_tasks.append(
                        getattr(thread, 'name', repr(thread))
                    )
            if alive_sync_tasks:
                shutdown_errors.append(
                    "同步协调线程未退出: " + ", ".join(alive_sync_tasks)
                )

            if not self._wait_for_alignment_futures(remaining()):
                shutdown_errors.append("FULLRESYNC 目标对齐线程未退出")

            if incremental_service is not None:
                wait_stopped = getattr(incremental_service, 'wait_stopped', None)
                if callable(wait_stopped):
                    try:
                        replication_stopped = bool(wait_stopped(remaining()))
                    except Exception as exc:
                        shutdown_errors.append(f"确认实时复制线程失败: {exc}")
                    else:
                        if not replication_stopped:
                            shutdown_errors.append("实时复制线程未退出")

            pending_executor_futures = self._pending_executor_futures()
            for future in pending_executor_futures:
                future.cancel()
            try:
                self.executor.shutdown(wait=False, cancel_futures=True)
            except TypeError:  # Python 3.7/3.8 compatibility
                self.executor.shutdown(wait=False)
            still_running_futures = {
                future
                for future in pending_executor_futures
                if not future.done()
            }
            if still_running_futures:
                shutdown_errors.append(
                    "服务线程池仍有 %s 个任务未退出"
                    % len(still_running_futures)
                )

            if self.web_ui:
                try:
                    try:
                        web_stopped = self.web_ui.stop(timeout=remaining())
                    except TypeError:
                        web_stopped = self.web_ui.stop()
                    if web_stopped is False:
                        shutdown_errors.append("Web UI 线程未退出")
                except Exception as e:
                    shutdown_errors.append(f"Web UI停止失败: {e}")

            if shutdown_errors:
                message = "; ".join(shutdown_errors)
                self.logger.error("Redis同步服务停止不完整: %s", message)
                raise RuntimeError(message)
            self.logger.info("Redis同步服务已停止")
        finally:
            stop_complete.set()
    
    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        targets = {
            name: {
                'healthy': stats.is_healthy,
                'total_synced': stats.total_synced,
                'total_failed': stats.total_failed,
                'last_sync_time': stats.last_sync_time,
                'last_error': stats.last_error,
                'consecutive_failures': stats.consecutive_failures
            }
            for name, stats in self.stats.items()
        }
        replication = None
        incremental_service = getattr(self, 'incremental_service', None)
        if incremental_service is not None:
            status_callback = getattr(
                incremental_service, 'get_replication_status', None
            )
            if callable(status_callback):
                try:
                    replication = status_callback()
                except Exception as exc:
                    replication = {
                        'running': False,
                        'healthy': False,
                        'last_error': f'failed to read replication status: {exc}',
                    }
        targets_healthy = bool(targets) and all(
            target['healthy'] for target in targets.values()
        )
        replication_healthy = (
            replication is None or replication.get('healthy') is True
        )
        return {
            'running': self.running,
            'healthy': bool(
                self.running and targets_healthy and replication_healthy
            ),
            'replication': replication,
            'targets': targets,
        }
    
    def run(self):
        """运行服务（阻塞）"""
        if not self.start():
            sys.exit(1)
        
        try:
            # 主循环 - 监控和统计
            while self.running:
                if self.shutdown_event.wait(60):
                    break
                
                status = self.get_status()
                healthy_targets = sum(1 for target in status['targets'].values() if target['healthy'])
                total_targets = len(status['targets'])
                
                self.logger.info(f"服务状态: 健康目标 {healthy_targets}/{total_targets}")
                
        except KeyboardInterrupt:
            self.logger.info("接收到中断信号")
        finally:
            self.stop()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Redis同步服务')
    parser.add_argument('--config', '-c', default='config.yaml', help='配置文件路径')
    
    args = parser.parse_args()
    
    # 检查配置文件是否存在
    if not Path(args.config).exists():
        print(f"配置文件不存在: {args.config}")
        sys.exit(1)
    
    # 创建并运行服务
    service = RedisSyncService(args.config)
    service.run()


if __name__ == '__main__':
    main()
