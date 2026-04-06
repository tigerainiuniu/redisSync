"""
Redis迁移编排器

协调不同迁移策略（包括SYNC、SCAN和REPLCONF）的主要类。
为Redis数据迁移和同步提供统一接口。
"""

import logging
import time
import threading
from enum import Enum
from typing import Optional, Dict, Any, Callable, List
from dataclasses import dataclass

from .connection_manager import RedisConnectionManager
from .sync_handler import SyncHandler
from .scan_handler import ScanHandler
from .replconf_handler import ReplConfHandler
from .full_migration_handler import FullMigrationHandler
from .incremental_migration_handler import IncrementalMigrationHandler
from .exceptions import MigrationError, ConfigurationError

logger = logging.getLogger(__name__)


class MigrationStrategy(Enum):
    """迁移策略选项。"""
    SCAN = "scan"
    SYNC = "sync"
    PSYNC = "psync"
    HYBRID = "hybrid"
    FULL = "full"  # 全量迁移
    INCREMENTAL = "incremental"  # 增量迁移


class MigrationType(Enum):
    """迁移类型。"""
    FULL = "full"  # 全量迁移
    INCREMENTAL = "incremental"  # 增量迁移


@dataclass
class MigrationConfig:
    """迁移操作的配置。"""
    strategy: MigrationStrategy = MigrationStrategy.SCAN
    migration_type: MigrationType = MigrationType.FULL
    batch_size: int = 100
    scan_count: int = 1000
    preserve_ttl: bool = True
    overwrite_existing: bool = False
    key_pattern: str = "*"
    key_type: Optional[str] = None
    key_types: Optional[List[str]] = None  # 支持多种键类型
    enable_replication: bool = False
    replication_port: int = 6380
    replication_timeout: int = 30
    progress_callback: Optional[Callable[[int, int], None]] = None
    verify_migration: bool = True
    verify_mode: str = "fast"  # 验证模式: fast（快速）, full（完整）
    verify_sample_size: Optional[int] = 100  # 验证采样数量
    max_retries: int = 3
    retry_delay: float = 1.0

    # 全量迁移特定配置
    clear_target: bool = False  # 是否清空目标数据库
    full_strategy: str = "scan"  # 全量迁移子策略: scan, sync, dump_restore

    # 增量迁移特定配置
    sync_interval: int = 60  # 增量同步间隔（秒）
    max_changes_per_sync: int = 10000  # 每次同步的最大变更数
    since_timestamp: Optional[float] = None  # 起始时间戳
    change_callback: Optional[Callable] = None  # 变更回调函数
    continuous_sync: bool = False  # 是否启用持续同步

    # sync.filters（glob 包含/排除、TTL、内存上限）
    include_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
    filter_min_ttl: int = 0
    filter_max_key_size: int = 0


def key_filter_from_migration_config(config: MigrationConfig):
    """从 MigrationConfig 构建与 sync.filters 一致的键过滤器（无规则时返回 None）。"""
    from .sync_filters import KeySyncFilter

    return KeySyncFilter.from_config(
        {
            "include_patterns": config.include_patterns,
            "exclude_patterns": config.exclude_patterns,
            "min_ttl": config.filter_min_ttl,
            "max_key_size": config.filter_max_key_size,
        }
    )


class MigrationOrchestrator:
    """使用各种策略编排Redis迁移。"""

    def __init__(self, connection_manager: RedisConnectionManager):
        """
        初始化迁移编排器。

        参数:
            connection_manager: Redis连接管理器实例
        """
        self.connection_manager = connection_manager
        self.sync_handler: Optional[SyncHandler] = None
        self.scan_handler: Optional[ScanHandler] = None
        self.replconf_handler: Optional[ReplConfHandler] = None
        self.full_migration_handler: Optional[FullMigrationHandler] = None
        self.incremental_migration_handler: Optional[IncrementalMigrationHandler] = None
        self._stop_replication = threading.Event()
        self._replication_thread: Optional[threading.Thread] = None
        
    def initialize_handlers(self, scan_count: int = 10000):
        """初始化迁移处理器。"""
        if not self.connection_manager.source_client or not self.connection_manager.target_client:
            raise RuntimeError("源和目标Redis客户端必须已连接")

        self.sync_handler = SyncHandler(
            self.connection_manager.source_client,
            self.connection_manager.target_client
        )

        self.scan_handler = ScanHandler(
            self.connection_manager.source_client,
            self.connection_manager.target_client
        )

        self.replconf_handler = ReplConfHandler(
            self.connection_manager.source_client,
            self.connection_manager.target_client
        )

        self.full_migration_handler = FullMigrationHandler(
            self.connection_manager.source_client,
            self.connection_manager.target_client
        )

        self.incremental_migration_handler = IncrementalMigrationHandler(
            self.connection_manager.source_client,
            self.connection_manager.target_client,
            scan_count=scan_count,
        )

        logger.info("迁移处理器已初始化（scan_count=%s）", scan_count)
    
    def migrate(self, config: MigrationConfig) -> Dict[str, Any]:
        """
        根据配置执行迁移。

        参数:
            config: 迁移配置

        返回:
            迁移结果和统计信息
        """
        if not self.sync_handler or not self.scan_handler or not self.replconf_handler:
            self.initialize_handlers()

        logger.info(f"开始迁移，策略: {config.strategy.value}，类型: {config.migration_type.value}")

        start_time = time.time()
        results = {
            'strategy': config.strategy.value,
            'migration_type': config.migration_type.value,
            'start_time': start_time,
            'end_time': None,
            'duration': None,
            'success': False,
            'statistics': {},
            'errors': []
        }

        migration_ok = True
        try:
            # 根据迁移类型选择处理方式
            if config.migration_type == MigrationType.FULL:
                full_res = self._perform_full_migration(config)
                results['statistics'] = full_res.get('statistics', {})
                migration_ok = bool(full_res.get('success', True))
                if not migration_ok:
                    err = full_res.get('error', '全量迁移失败')
                    results['errors'].append(str(err))
            elif config.migration_type == MigrationType.INCREMENTAL:
                inc_res = self._perform_incremental_migration(config)
                results['statistics'] = inc_res
                if isinstance(inc_res, dict) and 'success' in inc_res:
                    migration_ok = bool(inc_res.get('success'))
                    if not migration_ok:
                        results['errors'].append(str(inc_res.get('error', '增量迁移失败')))
            else:
                raise ConfigurationError(
                    f"不支持的迁移类型: {config.migration_type!r}（仅支持 full / incremental）"
                )

            # 如果需要，验证迁移（仅全量且主迁移已成功）
            if (
                migration_ok
                and config.verify_migration
                and config.migration_type == MigrationType.FULL
            ):
                verify_start = time.time()
                logger.info("🔍 开始验证迁移...")
                verification_results = self._verify_migration(config)
                verify_elapsed = time.time() - verify_start
                results['verification'] = verification_results
                results['verification_time'] = verify_elapsed

                if verification_results.get('success', False):
                    logger.info(f"✅ 迁移验证通过 (耗时: {verify_elapsed:.2f}秒)")
                else:
                    logger.warning(f"⚠️  迁移验证失败 (耗时: {verify_elapsed:.2f}秒)")

            results['success'] = migration_ok and not results['errors']
            if results['success']:
                logger.info("迁移成功完成")
            else:
                logger.error("迁移未完成: %s", results['errors'])
            
        except Exception as e:
            error_msg = f"Migration failed: {e}"
            logger.error(error_msg)
            results['errors'].append(error_msg)
            results['success'] = False
        
        finally:
            end_time = time.time()
            results['end_time'] = end_time
            results['duration'] = end_time - start_time
            
            # Stop replication if running
            if self._replication_thread and self._replication_thread.is_alive():
                self.stop_replication()
        
        return results

    def _perform_full_migration(self, config: MigrationConfig) -> Dict[str, Any]:
        """执行全量迁移。"""
        if not self.full_migration_handler:
            raise RuntimeError("全量迁移处理器未初始化")

        logger.info(f"开始全量迁移，子策略: {config.full_strategy}")

        # 准备键类型列表
        key_types = None
        if config.key_type:
            key_types = [config.key_type]
        elif config.key_types:
            key_types = config.key_types

        return self.full_migration_handler.perform_full_migration(
            strategy=config.full_strategy,
            clear_target=config.clear_target,
            preserve_ttl=config.preserve_ttl,
            batch_size=config.batch_size,
            scan_count=config.scan_count,
            progress_callback=config.progress_callback,
            key_pattern=config.key_pattern,
            key_types=key_types,
            key_filter=key_filter_from_migration_config(config),
        )

    def _perform_incremental_migration(self, config: MigrationConfig) -> Dict[str, Any]:
        """执行增量迁移。"""
        if not self.incremental_migration_handler:
            raise RuntimeError("增量迁移处理器未初始化")

        logger.info("开始增量迁移")

        # 准备键类型列表
        key_types = None
        if config.key_type:
            key_types = [config.key_type]
        elif config.key_types:
            key_types = config.key_types

        if config.continuous_sync:
            # 启动持续增量同步
            success = self.incremental_migration_handler.start_incremental_sync(
                sync_interval=config.sync_interval,
                key_pattern=config.key_pattern,
                key_types=key_types,
                change_callback=config.change_callback,
                max_changes_per_sync=config.max_changes_per_sync,
                key_filter=key_filter_from_migration_config(config),
            )

            return {
                'continuous_sync_started': success,
                'sync_interval': config.sync_interval,
                'max_changes_per_sync': config.max_changes_per_sync
            }
        else:
            # 执行一次性增量同步
            result = self.incremental_migration_handler.perform_incremental_sync(
                key_pattern=config.key_pattern,
                key_types=key_types,
                since_timestamp=config.since_timestamp,
                max_changes=config.max_changes_per_sync,
                key_filter=key_filter_from_migration_config(config),
            )

            return result

    def _migrate_with_scan(self, config: MigrationConfig) -> Dict[str, Any]:
        """Migrate using SCAN strategy."""
        logger.info("Performing SCAN-based migration")
        
        return self.scan_handler.migrate_all_keys(
            pattern=config.key_pattern,
            key_type=config.key_type,
            batch_size=config.batch_size,
            scan_count=config.scan_count,
            preserve_ttl=config.preserve_ttl,
            overwrite=config.overwrite_existing,
            progress_callback=config.progress_callback,
            key_filter=key_filter_from_migration_config(config),
        )
    
    def _migrate_with_sync(self, config: MigrationConfig) -> Dict[str, Any]:
        """Migrate using SYNC strategy."""
        logger.info("Performing SYNC-based migration")

        # 握手与 SYNC 必须在同一条连接上，由 SyncHandler 在取到连接后完成
        success = self.sync_handler.perform_full_sync(
            progress_callback=config.progress_callback,
            clear_target=config.clear_target,
            replication_handshake=config.enable_replication,
            listening_port=config.replication_port,
            hold_connection_for_stream=config.enable_replication,
        )
        
        if not success:
            raise RuntimeError("Full synchronization failed")
        
        # Start continuous replication if enabled
        if config.enable_replication:
            self._start_replication_stream(config)
        
        return {
            'sync_completed': True,
            'replication_enabled': config.enable_replication
        }
    
    def _migrate_with_psync(self, config: MigrationConfig) -> Dict[str, Any]:
        """Migrate using PSYNC strategy."""
        logger.info("Performing PSYNC-based migration")

        # 握手与 PSYNC 在同一条连接上；勿再单独走 ReplConfHandler 以免换连接
        success = self.sync_handler.perform_psync(
            progress_callback=config.progress_callback,
            clear_target=config.clear_target,
            replication_handshake=config.enable_replication,
            listening_port=config.replication_port,
            hold_connection_for_stream=config.enable_replication,
        )
        
        if not success:
            raise RuntimeError("Partial synchronization failed")
        
        # Start continuous replication if enabled
        if config.enable_replication:
            self._start_replication_stream(config)
        
        return {
            'psync_completed': True,
            'replication_enabled': config.enable_replication
        }
    
    def _migrate_with_hybrid(self, config: MigrationConfig) -> Dict[str, Any]:
        """Migrate using hybrid strategy (SYNC + SCAN for verification)."""
        logger.info("Performing hybrid migration (SYNC + SCAN verification)")
        
        results = {}
        
        # First, try SYNC migration
        try:
            sync_results = self._migrate_with_sync(config)
            results['sync_results'] = sync_results
        except Exception as e:
            logger.warning(f"SYNC migration failed, falling back to SCAN: {e}")
            scan_results = self._migrate_with_scan(config)
            results['scan_results'] = scan_results
            return results
        
        # Then, verify with SCAN comparison
        logger.info("Verifying SYNC migration with SCAN comparison")
        comparison_results = self.scan_handler.compare_keys(
            pattern=config.key_pattern,
            sample_size=10000  # Limit comparison for performance
        )
        results['comparison_results'] = comparison_results
        
        # If significant mismatches, perform SCAN migration for missing keys
        missing_ratio = comparison_results.get('missing_in_target', 0) / max(comparison_results.get('total_compared', 1), 1)
        if missing_ratio > 0.01:  # More than 1% missing
            logger.warning(f"Found {missing_ratio:.2%} missing keys, performing supplementary SCAN migration")
            scan_results = self._migrate_with_scan(config)
            results['supplementary_scan_results'] = scan_results
        
        return results
    
    _REPLICATION_FORWARD_CMDS = frozenset(
        {
            b"SET",
            b"DEL",
            b"EXPIRE",
            b"LPUSH",
            b"RPUSH",
            b"SADD",
            b"ZADD",
            b"HSET",
        }
    )

    def _start_replication_stream(self, config: MigrationConfig):
        """在同一条 SYNC/PSYNC 连接上启动持续复制流（bytes 透传，避免非 UTF-8 损坏）。"""
        self._stop_replication.clear()

        def replication_worker():
            def handle_command(command: bytes, args: List[bytes]):
                try:
                    if command.upper() in self._REPLICATION_FORWARD_CMDS:
                        self.connection_manager.target_client.execute_command(
                            command, *args
                        )
                        logger.debug(
                            "Replicated command: %s (%d args)",
                            command.decode("utf-8", errors="replace"),
                            len(args),
                        )
                except Exception as e:
                    logger.error(
                        "Failed to replicate command %r: %s",
                        command.decode("utf-8", errors="replace"),
                        e,
                    )

            ok = self.sync_handler.start_replication_stream(
                callback=handle_command,
                stop_event=self._stop_replication,
            )
            if not ok:
                logger.warning(
                    "持续复制流未启动：未持有 SYNC/PSYNC 后的连接（请确认 hold_connection_for_stream）"
                )
            else:
                logger.info("持续复制流已退出（停止信号或连接结束）")

        self._replication_thread = threading.Thread(
            target=replication_worker, daemon=True, name="replication-stream"
        )
        self._replication_thread.start()
        logger.info("Replication stream worker started")
    
    def stop_replication(self):
        """Stop continuous replication."""
        if self._replication_thread and self._replication_thread.is_alive():
            self._stop_replication.set()
            self._replication_thread.join(timeout=5)
            logger.info("Replication stream stopped")
    
    def _verify_migration(self, config: MigrationConfig) -> Dict[str, Any]:
        """Verify migration by comparing source and target."""
        try:
            # 获取验证配置（从config或使用默认值）
            verify_mode = config.verify_mode
            sample_size = config.verify_sample_size or 100

            # 确定是否使用快速模式
            use_fast_mode = (verify_mode == 'fast')

            logger.info(f"验证模式: {verify_mode}, 采样数量: {sample_size}")

            comparison_results = self.scan_handler.compare_keys(
                pattern=config.key_pattern,
                sample_size=sample_size,
                use_fast_mode=use_fast_mode
            )

            total_compared = comparison_results.get('total_compared', 0)
            matching_keys = comparison_results.get('matching_keys', 0)

            success_rate = matching_keys / max(total_compared, 1)
            verification_success = success_rate >= 0.95  # 95% success threshold

            return {
                'success': verification_success,
                'success_rate': success_rate,
                'total_compared': total_compared,
                'matching_keys': matching_keys,
                'mode': verify_mode,
                'details': comparison_results
            }

        except Exception as e:
            logger.error(f"Migration verification failed: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status."""
        status = {
            'replication_active': self._replication_thread and self._replication_thread.is_alive(),
            'source_connected': self.connection_manager.source_client is not None,
            'target_connected': self.connection_manager.target_client is not None,
        }
        
        if self.replconf_handler:
            status['replication_config'] = self.replconf_handler.get_current_config()
            status['replication_lag'] = self.replconf_handler.get_replication_lag()
        
        if self.connection_manager.source_client:
            status['source_info'] = self.connection_manager.get_source_info()
        
        if self.connection_manager.target_client:
            status['target_info'] = self.connection_manager.get_target_info()
        
        return status
    
    def start_incremental_sync(self, config: MigrationConfig) -> bool:
        """启动增量同步。"""
        if not self.incremental_migration_handler:
            self.initialize_handlers()

        # 准备键类型列表
        key_types = None
        if config.key_type:
            key_types = [config.key_type]
        elif config.key_types:
            key_types = config.key_types

        return self.incremental_migration_handler.start_incremental_sync(
            sync_interval=config.sync_interval,
            key_pattern=config.key_pattern,
            key_types=key_types,
            change_callback=config.change_callback,
            max_changes_per_sync=config.max_changes_per_sync,
            key_filter=key_filter_from_migration_config(config),
        )

    def stop_incremental_sync(self) -> Dict[str, Any]:
        """停止增量同步。"""
        if not self.incremental_migration_handler:
            return {'error': '增量迁移处理器未初始化'}

        return self.incremental_migration_handler.stop_incremental_sync()

    def get_incremental_stats(self) -> Dict[str, Any]:
        """获取增量同步统计信息。"""
        if not self.incremental_migration_handler:
            return {'error': '增量迁移处理器未初始化'}

        return self.incremental_migration_handler.get_incremental_stats()

    def get_full_migration_progress(self) -> Dict[str, Any]:
        """获取全量迁移进度。"""
        if not self.full_migration_handler:
            return {'error': '全量迁移处理器未初始化'}

        return self.full_migration_handler.get_migration_progress()

    def set_incremental_checkpoint(self, checkpoint_data: Dict[str, Any]):
        """设置增量同步检查点。"""
        if not self.incremental_migration_handler:
            self.initialize_handlers()

        self.incremental_migration_handler.set_sync_checkpoint(checkpoint_data)

    def get_incremental_checkpoint(self) -> Dict[str, Any]:
        """获取增量同步检查点。"""
        if not self.incremental_migration_handler:
            return {'error': '增量迁移处理器未初始化'}

        return self.incremental_migration_handler.get_sync_checkpoint()

    def cleanup(self):
        """清理资源。"""
        # 停止增量同步
        if self.incremental_migration_handler:
            self.incremental_migration_handler.stop_incremental_sync()

        # 停止复制
        self.stop_replication()

        # 关闭连接
        self.connection_manager.close_connections()

        logger.info("迁移编排器已清理")
