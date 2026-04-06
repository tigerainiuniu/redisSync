#!/usr/bin/env python3
"""
Redis同步服务

支持一对多Redis实例的持续同步服务。
"""

import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
import redis
from pathlib import Path

from .config import load_and_validate_service_config
from .connection_manager import RedisConnectionManager
from .migration_orchestrator import MigrationOrchestrator, MigrationConfig, MigrationType
from .sync_filters import KeySyncFilter
from .web_ui import WebUI
from .unified_incremental_service import UnifiedIncrementalService


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


class RedisSyncService:
    """Redis同步服务"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logging()
        
        # 服务状态
        self.running = False
        self.shutdown_event = threading.Event()
        
        # 连接管理器和编排器
        self.source_conn = None
        self.target_connections: Dict[str, RedisConnectionManager] = {}
        self.orchestrators: Dict[str, MigrationOrchestrator] = {}
        
        # 统计信息
        self.stats: Dict[str, SyncStats] = {}
        
        # 线程池
        self.executor = ThreadPoolExecutor(
            max_workers=self.config['service']['performance']['max_workers']
        )

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
                host=web_config.get('host', '0.0.0.0'),
                port=web_config.get('port', 8080)
            )

        # 记录启动时间
        self.start_time = time.time()

        self._sync_key_filter = KeySyncFilter.from_config(
            self.config.get('sync', {}).get('filters')
        )

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
            self.stop()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _connect_source(self) -> bool:
        """连接源Redis"""
        try:
            source_config = self.config['source']
            # 构建连接参数
            conn_params = {
                'host': source_config['host'],
                'port': source_config['port'],
                'password': source_config.get('password'),
                'db': source_config.get('db', 0),
                'ssl': source_config.get('ssl', False),
                'socket_timeout': source_config.get('socket_timeout', 30),
                'socket_connect_timeout': source_config.get('socket_connect_timeout', 30),
                'decode_responses': False,
            }

            if source_config.get('socket_keepalive', False):
                conn_params['socket_keepalive'] = True

            self.source_conn = redis.Redis(**conn_params)
            
            # 测试连接
            self.source_conn.ping()
            self.logger.info(f"源Redis连接成功: {source_config['host']}:{source_config['port']}")
            return True
            
        except Exception as e:
            self.logger.error(f"源Redis连接失败: {e}")
            return False
    
    def _connect_targets(self) -> bool:
        """连接所有目标Redis"""
        success = True
        shared_source_client = None
        shared_source_config = None

        for target_config in self.config['targets']:
            if not target_config.get('enabled', True):
                continue

            target_name = target_config['name']

            try:
                conn_manager = RedisConnectionManager()

                if shared_source_client is None:
                    conn_manager.connect_source(
                        host=self.config['source']['host'],
                        port=self.config['source']['port'],
                        password=self.config['source'].get('password'),
                        db=self.config['source'].get('db', 0),
                        ssl=self.config['source'].get('ssl', False),
                    )
                    shared_source_client = conn_manager.source_client
                    shared_source_config = conn_manager._source_config
                else:
                    conn_manager.set_source_client(
                        shared_source_client, shared_source_config
                    )

                conn_manager.connect_target(
                    host=target_config['host'],
                    port=target_config['port'],
                    password=target_config.get('password'),
                    db=target_config.get('db', 0),
                    ssl=target_config.get('ssl', False),
                )

                orchestrator = MigrationOrchestrator(conn_manager)

                scan_count = self.config['service']['performance'].get('scan_count', 10000)
                orchestrator.initialize_handlers(scan_count=scan_count)

                self.target_connections[target_name] = conn_manager
                self.orchestrators[target_name] = orchestrator
                self.stats[target_name] = SyncStats()
                
                self.logger.info(f"目标Redis连接成功: {target_name} ({target_config['host']}:{target_config['port']})")
                
            except Exception as e:
                self.logger.error(f"目标Redis连接失败 {target_name}: {e}")
                success = False
        
        return success
    
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
            verify_migration=full_sync_config.get('verify_migration', True),
            verify_mode=full_sync_config.get('verify_mode', 'fast'),
            verify_sample_size=full_sync_config.get('verify_sample_size', 100),
            full_strategy=full_sync_config.get('strategy', 'scan'),
            clear_target=full_sync_config.get('clear_target', True),
            key_types=key_types,
            include_patterns=inc if isinstance(inc, list) else None,
            exclude_patterns=exc if isinstance(exc, list) else None,
            filter_min_ttl=int(filters.get('min_ttl') or 0),
            filter_max_key_size=int(filters.get('max_key_size') or 0),
        )

        return config
    
    def _perform_full_sync(self, target_name: str) -> bool:
        """执行全量同步"""
        try:
            start_time = time.time()
            orchestrator = self.orchestrators[target_name]
            config = self._create_sync_config()

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
                self.stats[target_name].consecutive_failures = 0
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
            self.stats[target_name].total_failed += 1
            self.stats[target_name].last_error = str(e)
            self.stats[target_name].consecutive_failures += 1
            
            # 检查是否需要标记为不健康
            max_failures = self.config['service']['failover']['max_failures']
            if self.stats[target_name].consecutive_failures >= max_failures:
                self.stats[target_name].is_healthy = False
                self.logger.error(f"目标 {target_name} 标记为不健康，连续失败次数: {self.stats[target_name].consecutive_failures}")
            
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
                self.stats[target_name].consecutive_failures = 0
                self.stats[target_name].is_healthy = True

                return True
            else:
                errs = result.get('errors') or []
                error_msg = '; '.join(errs) if errs else '未知错误'
                self.logger.error(f"❌ 增量同步失败: {target_name}, 错误: {error_msg}")
                raise Exception(error_msg)

        except Exception as e:
            self.stats[target_name].total_failed += 1
            self.stats[target_name].last_error = str(e)
            self.stats[target_name].consecutive_failures += 1

            # 检查是否需要标记为不健康
            max_failures = self.config['service']['failover']['max_failures']
            if self.stats[target_name].consecutive_failures >= max_failures:
                self.stats[target_name].is_healthy = False
                self.logger.error(f"⚠️  目标 {target_name} 标记为不健康，连续失败次数: {self.stats[target_name].consecutive_failures}")

            self.logger.error(f"❌ 增量同步失败: {target_name}, 错误: {e}", exc_info=True)
            return False

    def _scan_source_for_changes(self) -> List[str]:
        """统一扫描源Redis，检测变更的键（只扫描一次）"""
        try:
            inc_config = self.config['sync']['incremental_sync']
            key_pattern = inc_config.get('key_pattern', '*')
            max_changes = inc_config.get('max_changes_per_sync', 10000)

            # 使用第一个orchestrator来检测变更（它们共享同一个源）
            first_orchestrator = next(iter(self.orchestrators.values()))

            # 获取上次同步时间
            last_sync_time = min(
                (stats.last_sync_time for stats in self.stats.values() if stats.last_sync_time),
                default=time.time() - 30  # 默认30秒前
            )

            self.logger.debug(f"🔍 扫描源Redis检测变更（上次同步: {time.time() - last_sync_time:.1f}秒前）")

            # 调用增量处理器检测变更
            changed_keys = first_orchestrator.incremental_migration_handler._detect_changes_by_idle_time(
                key_pattern=key_pattern,
                key_types=inc_config.get('key_types'),
                since_timestamp=last_sync_time,
                max_changes=max_changes
            )

            if self._sync_key_filter and changed_keys:
                src = first_orchestrator.connection_manager.source_client
                changed_keys = self._sync_key_filter.filter_batch(
                    src, list(changed_keys)
                )

            if changed_keys:
                self.logger.info(f"✓ 检测到 {len(changed_keys)} 个变更键")
            else:
                self.logger.debug(f"✓ 无变更键")

            return changed_keys

        except Exception as e:
            self.logger.error(f"❌ 扫描源Redis失败: {e}", exc_info=True)
            return []

    def _sync_keys_to_target(self, target_name: str, keys: List[str]) -> bool:
        """将指定的键同步到目标（优化版：使用Pipeline批量操作）"""
        if not keys:
            return True

        try:
            orchestrator = self.orchestrators[target_name]
            source_client = orchestrator.connection_manager.source_client
            target_client = orchestrator.connection_manager.target_client

            synced = 0
            failed = 0

            # 超大批量处理（小数据集一次性处理）
            batch_size = min(len(keys), 500)  # 最大500个键/批
            total_batches = (len(keys) + batch_size - 1) // batch_size

            for batch_idx in range(0, len(keys), batch_size):
                batch_keys = keys[batch_idx:batch_idx + batch_size]
                current_batch = batch_idx // batch_size + 1

                try:
                    # 使用无事务Pipeline批量读取（更快）
                    pipe = source_client.pipeline(transaction=False)
                    for key in batch_keys:
                        pipe.dump(key)
                        pipe.pttl(key)

                    results = pipe.execute()

                    # 快速解析（使用元组而不是字典）
                    entries = []
                    for i in range(0, len(results), 2):
                        dump_data = results[i]
                        ttl = results[i + 1]

                        if dump_data is not None:
                            entries.append((
                                batch_keys[i // 2],
                                ttl if ttl > 0 else 0,
                                dump_data
                            ))

                    # 使用无事务Pipeline批量写入（更快）
                    if entries:
                        pipe = target_client.pipeline(transaction=False)
                        for key, ttl, data in entries:
                            pipe.restore(key, ttl, data, replace=True)

                        pipe.execute()
                        synced += len(entries)

                except Exception as e:
                    failed += len(batch_keys)
                    self.logger.warning(
                        f"⚠️  批次 {current_batch}/{total_batches} 同步失败: {e}"
                    )

            # 更新统计
            self.stats[target_name].total_synced += synced
            self.stats[target_name].total_failed += failed
            self.stats[target_name].last_sync_time = time.time()

            if failed == 0:
                self.stats[target_name].consecutive_failures = 0
                self.stats[target_name].is_healthy = True

            self.logger.info(
                f"✅ {target_name}: 同步完成 - 成功 {synced} 个，失败 {failed} 个"
            )

            return True

        except Exception as e:
            self.logger.error(f"❌ 同步到目标 {target_name} 失败: {e}", exc_info=True)
            self.stats[target_name].total_failed += len(keys)
            self.stats[target_name].consecutive_failures += 1

            max_failures = self.config['service']['failover']['max_failures']
            if self.stats[target_name].consecutive_failures >= max_failures:
                self.stats[target_name].is_healthy = False

            return False

    def _perform_unified_incremental_sync(self) -> bool:
        """执行统一增量同步（扫描一次，同步到所有目标）"""
        try:
            # 1. 扫描一次源Redis
            changed_keys = self._scan_source_for_changes()

            if not changed_keys:
                self.logger.debug("✓ 统一增量同步: 无变更")
                return True

            # 2. 并行同步到所有健康的目标
            healthy_targets = [
                name for name, stats in self.stats.items()
                if stats.is_healthy
            ]

            if not healthy_targets:
                self.logger.warning("⚠️  没有健康的目标可以同步")
                return False

            self.logger.info(f"⇉ 并行同步 {len(changed_keys)} 个键到 {len(healthy_targets)} 个目标")

            # 使用线程池并行同步
            futures = []
            for target_name in healthy_targets:
                future = self.executor.submit(
                    self._sync_keys_to_target,
                    target_name,
                    changed_keys
                )
                futures.append((target_name, future))

            # 等待所有同步完成
            success_count = 0
            for target_name, future in futures:
                try:
                    if future.result(timeout=300):  # 5分钟超时
                        success_count += 1
                        self.logger.info(f"  ✓ {target_name}: 同步完成")
                    else:
                        self.logger.warning(f"  ✗ {target_name}: 同步失败")
                except Exception as e:
                    self.logger.error(f"  ✗ {target_name}: {e}")

            self.logger.info(f"✓ 统一同步完成: {success_count}/{len(healthy_targets)} 个目标成功")
            return success_count > 0

        except Exception as e:
            self.logger.error(f"❌ 统一增量同步失败: {e}", exc_info=True)
            return False

    def _unified_sync_coordinator(self):
        """统一同步协调器（扫描一次源，同步到所有目标）"""
        self.logger.info("🚀 启动统一同步协调器")

        # 根据模式执行初始同步
        sync_mode = self.config['sync']['mode']
        self.logger.info(f"📋 同步模式: {sync_mode}")

        # 全量同步阶段（并行执行到所有目标）
        if sync_mode in ['full', 'hybrid']:
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

            self.logger.info("✅ 全量同步阶段完成")

        # 增量同步阶段
        if sync_mode in ['incremental', 'hybrid']:
            inc_config = self.config['sync']['incremental_sync']
            if inc_config.get('enabled', True):
                inc_method = inc_config.get('method', 'scan')

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
                                recovery_delay = self.config['service']['failover']['recovery_delay']

                                # 尝试恢复不健康的目标
                                for target_name in unhealthy_targets:
                                    self.logger.info(f"🔄 尝试恢复目标: {target_name}")
                                    if self._perform_full_sync(target_name):
                                        self.logger.info(f"  ✓ 目标 {target_name} 恢复成功")
                                        self.stats[target_name].is_healthy = True
                                        self.stats[target_name].consecutive_failures = 0
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

    def _start_realtime_replication(self, mode: str, config: Dict[str, Any]):
        """启动实时复制（SYNC/PSYNC）"""
        try:
            # 创建统一增量同步服务
            self.incremental_service = UnifiedIncrementalService(
                mode=mode,
                source_conn=self.source_conn,
                target_connections=self.target_connections,
                config=config
            )

            # 启动服务
            self.incremental_service.start()

            # 等待直到服务停止
            while self.running and not self.shutdown_event.is_set():
                self.shutdown_event.wait(1)

            # 停止服务
            if self.incremental_service:
                self.incremental_service.stop()

        except Exception as e:
            self.logger.error(f"❌ {mode.upper()} 复制失败: {e}", exc_info=True)

    def start(self) -> bool:
        """启动同步服务"""
        self.logger.info("启动Redis同步服务...")
        
        # 设置信号处理器
        self._setup_signal_handlers()
        
        # 连接Redis实例
        if not self._connect_source():
            return False
        
        if not self._connect_targets():
            return False
        
        if not self.target_connections:
            self.logger.error("没有可用的目标Redis实例")
            return False
        
        # 启动Web UI
        if self.web_ui:
            try:
                self.web_ui.start()
            except Exception as e:
                self.logger.warning(f"Web UI启动失败: {e}")

        # 启动统一同步协调器
        self.running = True

        # 启动一个统一的同步协调器线程（而不是为每个目标启动线程）
        coordinator_thread = threading.Thread(
            target=self._unified_sync_coordinator,
            name="unified-sync-coordinator",
            daemon=True
        )
        coordinator_thread.start()
        self.sync_tasks.append(coordinator_thread)

        self.logger.info(f"✅ Redis同步服务启动成功")
        self.logger.info(f"   同步目标数: {len(self.target_connections)}")
        self.logger.info(f"   优化模式: 统一扫描 + 并行分发")
        return True
    
    def stop(self):
        """停止同步服务"""
        self.logger.info("停止Redis同步服务...")
        
        self.running = False
        self.shutdown_event.set()
        
        # 等待所有同步任务结束
        for thread in self.sync_tasks:
            thread.join(timeout=30)
        
        # 关闭连接
        for conn_manager in self.target_connections.values():
            try:
                conn_manager.close()
            except Exception:
                pass

        if self.source_conn:
            try:
                self.source_conn.close()
            except Exception:
                pass
        
        # 关闭线程池
        self.executor.shutdown(wait=True)

        # 停止Web UI
        if self.web_ui:
            try:
                self.web_ui.stop()
            except Exception as e:
                self.logger.warning(f"Web UI停止失败: {e}")

        self.logger.info("Redis同步服务已停止")
    
    def get_status(self) -> Dict[str, Any]:
        """获取服务状态"""
        return {
            'running': self.running,
            'targets': {
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
        }
    
    def run(self):
        """运行服务（阻塞）"""
        if not self.start():
            sys.exit(1)
        
        try:
            # 主循环 - 监控和统计
            while self.running:
                time.sleep(60)  # 每分钟输出一次状态
                
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
