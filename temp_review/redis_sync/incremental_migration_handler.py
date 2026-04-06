"""
增量迁移处理器

实现Redis的增量数据迁移，支持基于时间戳、键变更监控和复制流的增量同步。
可以检测和同步源Redis中的数据变更到目标Redis。
"""

import redis
import logging
import time
import threading
from typing import Optional, Callable, Dict, Any, List, Set
from datetime import datetime
from collections import defaultdict

from .exceptions import MigrationError
from .key_sync import sync_key_with_dump_restore
from .sync_filters import KeySyncFilter
logger = logging.getLogger(__name__)


class IncrementalMigrationHandler:
    """处理Redis增量迁移的核心类。"""

    def __init__(self, source_client: redis.Redis, target_client: redis.Redis, scan_count: int = 10000):
        """
        初始化增量迁移处理器。

        参数:
            source_client: 源Redis客户端
            target_client: 目标Redis客户端
            scan_count: SCAN命令的COUNT参数（默认10000）
        """
        self.source_client = source_client
        self.target_client = target_client
        self.scan_count = scan_count  # 可配置的SCAN count
        self.last_sync_time = None
        self.sync_checkpoint = None
        self.is_monitoring = False
        self.monitor_thread = None
        self.stop_event = threading.Event()
        self._stats_lock = threading.Lock()

        # 增量迁移统计
        self.incremental_stats = {
            'start_time': None,
            'last_sync_time': None,
            'total_changes': 0,
            'successful_changes': 0,
            'failed_changes': 0,
            'change_types': defaultdict(int),
            'sync_intervals': []
        }
    
    def start_incremental_sync(self,
                              sync_interval: int = 60,
                              key_pattern: str = "*",
                              key_types: Optional[List[str]] = None,
                              change_callback: Optional[Callable] = None,
                              max_changes_per_sync: int = 10000,
                              key_filter: Optional[KeySyncFilter] = None) -> bool:
        """
        启动增量同步。
        
        参数:
            sync_interval: 同步间隔（秒）
            key_pattern: 键模式过滤
            key_types: 键类型过滤
            change_callback: 变更回调函数
            max_changes_per_sync: 每次同步的最大变更数
            
        返回:
            是否成功启动
        """
        if self.is_monitoring:
            logger.warning("增量同步已在运行")
            return False
        
        logger.info(f"启动增量同步，间隔: {sync_interval}秒")
        
        self.incremental_stats['start_time'] = datetime.now()
        self.last_sync_time = time.time()
        self.is_monitoring = True
        self.stop_event.clear()
        
        # 启动监控线程
        self.monitor_thread = threading.Thread(
            target=self._incremental_sync_worker,
            args=(
                sync_interval,
                key_pattern,
                key_types,
                change_callback,
                max_changes_per_sync,
                key_filter,
            ),
            daemon=True
        )
        self.monitor_thread.start()
        
        return True
    
    def stop_incremental_sync(self) -> Dict[str, Any]:
        """
        停止增量同步。
        
        返回:
            同步统计信息
        """
        if not self.is_monitoring:
            logger.warning("增量同步未在运行")
            return self.incremental_stats
        
        logger.info("停止增量同步")
        self.stop_event.set()
        self.is_monitoring = False
        
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)
        
        return self.incremental_stats
    
    def perform_incremental_sync(self,
                                key_pattern: str = "*",
                                key_types: Optional[List[str]] = None,
                                since_timestamp: Optional[float] = None,
                                max_changes: int = 10000,
                                key_filter: Optional[KeySyncFilter] = None) -> Dict[str, Any]:
        """
        执行一次增量同步。

        参数:
            key_pattern: 键模式过滤
            key_types: 键类型过滤
            since_timestamp: 起始时间戳
            max_changes: 最大变更数

        返回:
            同步结果
        """
        start_time = time.time()
        sync_timestamp = since_timestamp or self.last_sync_time or start_time

        logger.info("=" * 60)
        logger.info("开始执行增量同步")
        logger.info(f"  键模式: {key_pattern}")
        logger.info(f"  键类型过滤: {key_types}")
        logger.info(f"  上次同步时间: {sync_timestamp} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(sync_timestamp))})")
        logger.info(f"  当前时间: {start_time} ({time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))})")
        logger.info(f"  时间差: {start_time - sync_timestamp:.2f} 秒")
        logger.info(f"  最大变更数: {max_changes}")

        try:
            # 检测变更的键
            logger.info("开始检测变更的键...")
            changed_keys = self._detect_changed_keys(
                key_pattern, key_types, sync_timestamp, max_changes
            )

            if key_filter and changed_keys:
                changed_keys = key_filter.filter_batch(
                    self.source_client, list(changed_keys)
                )

            if not changed_keys:
                logger.info("✓ 未检测到键变更")
                logger.info("=" * 60)
                return {
                    'success': True,
                    'changed_keys': 0,
                    'synced_keys': 0,
                    'failed_keys': 0,
                    'duration': time.time() - start_time
                }

            logger.info(f"✓ 检测到 {len(changed_keys)} 个变更的键:")
            for i, key in enumerate(changed_keys[:10], 1):  # 只显示前10个
                logger.info(f"  {i}. {key}")
            if len(changed_keys) > 10:
                logger.info(f"  ... 还有 {len(changed_keys) - 10} 个键")

            # 同步变更的键
            logger.info("开始同步变更的键...")
            sync_result = self._sync_changed_keys(changed_keys)

            logger.info(f"✓ 同步完成:")
            logger.info(f"  成功: {sync_result['synced']} 个")
            logger.info(f"  失败: {sync_result['failed']} 个")

            with self._stats_lock:
                self.incremental_stats['total_changes'] += len(changed_keys)
                self.incremental_stats['successful_changes'] += sync_result['synced']
                self.incremental_stats['failed_changes'] += sync_result['failed']
                self.incremental_stats['last_sync_time'] = datetime.now()

            duration = time.time() - start_time
            with self._stats_lock:
                self.incremental_stats['sync_intervals'].append(duration)
                if len(self.incremental_stats['sync_intervals']) > 1000:
                    self.incremental_stats['sync_intervals'] = self.incremental_stats['sync_intervals'][-500:]

            # 更新最后同步时间
            self.last_sync_time = start_time

            logger.info(f"✓ 增量同步完成，耗时: {duration:.2f} 秒")
            logger.info("=" * 60)

            return {
                'success': True,
                'changed_keys': len(changed_keys),
                'synced_keys': sync_result['synced'],
                'failed_keys': sync_result['failed'],
                'duration': duration,
                'sync_timestamp': start_time
            }

        except Exception as e:
            logger.error(f"✗ 增量同步失败: {e}", exc_info=True)
            logger.info("=" * 60)
            return {
                'success': False,
                'error': str(e),
                'duration': time.time() - start_time
            }
    
    def _incremental_sync_worker(self,
                                sync_interval: int,
                                key_pattern: str,
                                key_types: Optional[List[str]],
                                change_callback: Optional[Callable],
                                max_changes_per_sync: int,
                                key_filter: Optional[KeySyncFilter]):
        """增量同步工作线程。"""
        logger.info("增量同步工作线程启动")
        
        while not self.stop_event.is_set():
            try:
                # 执行增量同步
                result = self.perform_incremental_sync(
                    key_pattern,
                    key_types,
                    None,
                    max_changes_per_sync,
                    key_filter=key_filter,
                )
                
                # 调用变更回调
                if change_callback and result['success']:
                    try:
                        change_callback(result)
                    except Exception as e:
                        logger.error(f"变更回调执行失败: {e}")
                
                # 等待下次同步
                self.stop_event.wait(sync_interval)
                
            except Exception as e:
                logger.error(f"增量同步工作线程出错: {e}")
                self.stop_event.wait(sync_interval)
        
        logger.info("增量同步工作线程停止")
    
    def _detect_changed_keys(self,
                           key_pattern: str,
                           key_types: Optional[List[str]],
                           since_timestamp: float,
                           max_changes: int) -> List[str]:
        """
        检测变更的键。

        这里使用多种策略来检测变更：
        1. 比较键的最后修改时间（如果Redis支持）
        2. 比较键的值哈希
        3. 扫描所有键并与目标比较
        """
        changed_keys = []

        try:
            logger.info("策略1: 使用OBJECT IDLETIME检测最近活跃的键")
            # 策略1: 使用OBJECT IDLETIME检测最近活跃的键
            idle_changes = self._detect_changes_by_idle_time(
                key_pattern, key_types, since_timestamp, max_changes
            )
            changed_keys.extend(idle_changes)
            logger.info(f"  空闲时间检测到 {len(idle_changes)} 个变更")

            # 如果检测到的变更不够，使用策略2: 值比较
            if len(changed_keys) < max_changes:
                logger.info("策略2: 使用值比较检测变更")
                remaining_limit = max_changes - len(changed_keys)
                additional_changes = self._detect_changes_by_comparison(
                    key_pattern, key_types, remaining_limit, set(changed_keys)
                )
                changed_keys.extend(additional_changes)
                logger.info(f"  值比较检测到 {len(additional_changes)} 个额外变更")

            logger.info(f"总共检测到 {len(changed_keys)} 个变更的键")
            return changed_keys[:max_changes]

        except Exception as e:
            logger.error(f"✗ 检测键变更失败: {e}", exc_info=True)
            return []
    
    def _detect_changes_by_idle_time(self,
                                   key_pattern: str,
                                   key_types: Optional[List[str]],
                                   since_timestamp: float,
                                   max_changes: int) -> List[str]:
        """
        通过空闲时间检测变更的键（使用SCAN避免阻塞 + Pipeline批量检测）

        OBJECT IDLETIME返回键自上次访问以来的秒数。
        如果idle_time小，说明最近被访问/修改过。

        注意：使用SCAN而不是KEYS，避免在大数据量时阻塞Redis
        """
        changed_keys = []
        current_time = time.time()
        # 计算时间差（秒）
        time_diff = current_time - since_timestamp

        logger.debug(f"🔍 检测变更：时间差={time_diff:.1f}秒")

        try:
            cursor = 0
            scan_count = min(1000, self.scan_count)

            while len(changed_keys) < max_changes:
                cursor, keys = self.source_client.scan(
                    cursor=cursor,
                    match=key_pattern,
                    count=scan_count,
                )
                if not keys:
                    if cursor == 0:
                        break
                    continue

                batch = list(keys)

                if key_types:
                    pipe = self.source_client.pipeline(transaction=False)
                    for key in batch:
                        pipe.type(key)
                    types = pipe.execute()
                    filtered = []
                    for i, kt in enumerate(types):
                        kt_s = kt.decode() if isinstance(kt, bytes) else kt
                        if kt_s in key_types:
                            filtered.append(batch[i])
                    batch = filtered

                if not batch:
                    if cursor == 0:
                        break
                    continue

                pipe_batch = 1000
                for off in range(0, len(batch), pipe_batch):
                    sub = batch[off : off + pipe_batch]
                    pipe = self.source_client.pipeline(transaction=False)
                    for key in sub:
                        pipe.object("idletime", key)
                    idle_times = pipe.execute()
                    for i, idle_time in enumerate(idle_times):
                        if idle_time is None:
                            continue
                        if idle_time <= time_diff + 5:
                            key = sub[i]
                            key_str = key.decode() if isinstance(key, bytes) else key
                            changed_keys.append(key_str)
                            logger.debug(
                                "✓ 变更键: %s, idle=%s秒", key_str, idle_time
                            )
                            if len(changed_keys) >= max_changes:
                                break
                    if len(changed_keys) >= max_changes:
                        break

                if len(changed_keys) >= max_changes:
                    break
                if cursor == 0:
                    break

            logger.info("✅ 检测到 %s 个变更键（流式 SCAN，未一次性加载全库键名）", len(changed_keys))
            return changed_keys

        except Exception as e:
            logger.error(f"❌ 检测变更失败: {e}", exc_info=True)
            return []
    
    def _detect_changes_by_comparison(self,
                                    key_pattern: str,
                                    key_types: Optional[List[str]],
                                    max_changes: int,
                                    exclude_keys: Set[str]) -> List[str]:
        """
        通过值比较检测变更的键。

        这个方法会扫描所有匹配的键，并比较源和目标的值。
        如果键不存在于目标或值不同，则认为是变更。
        """
        changed_keys = []

        cursor = 0
        scanned_count = 0

        logger.debug(f"开始值比较检测，排除键数: {len(exclude_keys)}")

        while len(changed_keys) < max_changes and scanned_count < 50000:
            try:
                cursor, keys = self.source_client.scan(
                    cursor=cursor,
                    match=key_pattern,
                    count=self.scan_count // 2  # 比较模式使用较小的count
                )

                for key in keys:
                    scanned_count += 1
                    key_str = key.decode() if isinstance(key, bytes) else key

                    # 跳过已经检测过的键
                    if key_str in exclude_keys:
                        continue

                    try:
                        # 检查键类型
                        if key_types:
                            key_type = self.source_client.type(key)
                            if isinstance(key_type, bytes):
                                key_type = key_type.decode()
                            if key_type not in key_types:
                                continue

                        # 比较源和目标的值
                        is_different, reason = self._is_key_different(key)
                        if is_different:
                            changed_keys.append(key_str)
                            logger.debug(f"检测到变更键: {key_str}, 原因: {reason}")

                            if len(changed_keys) >= max_changes:
                                break

                    except Exception as e:
                        logger.debug(f"比较键 {key} 失败: {e}")
                        continue

                if cursor == 0:
                    break

            except Exception as e:
                logger.error(f"比较扫描时出错: {e}")
                break

        logger.info(f"通过值比较检测到 {len(changed_keys)} 个变更的键（扫描了{scanned_count}个键）")
        return changed_keys
    
    def _is_key_different(self, key) -> tuple:
        """
        检查键在源和目标中是否不同。

        返回:
            (is_different, reason): 是否不同和原因
        """
        try:
            # 检查键是否存在于目标
            if not self.target_client.exists(key):
                return (True, "目标中不存在")

            # 检查类型是否相同
            source_type = self.source_client.type(key)
            target_type = self.target_client.type(key)

            if isinstance(source_type, bytes):
                source_type = source_type.decode()
            if isinstance(target_type, bytes):
                target_type = target_type.decode()

            if source_type != target_type:
                return (True, f"类型不同: {source_type} vs {target_type}")

            # 根据类型比较值
            if source_type == 'string':
                source_val = self.source_client.get(key)
                target_val = self.target_client.get(key)
                if source_val != target_val:
                    return (True, "值不同")

            elif source_type == 'hash':
                source_hash = self.source_client.hgetall(key)
                target_hash = self.target_client.hgetall(key)
                if source_hash != target_hash:
                    return (True, "哈希值不同")

            elif source_type == 'list':
                source_list = self.source_client.lrange(key, 0, -1)
                target_list = self.target_client.lrange(key, 0, -1)
                if source_list != target_list:
                    return (True, "列表值不同")

            elif source_type == 'set':
                source_set = self.source_client.smembers(key)
                target_set = self.target_client.smembers(key)
                if source_set != target_set:
                    return (True, "集合值不同")

            elif source_type == 'zset':
                source_zset = self.source_client.zrange(key, 0, -1, withscores=True)
                target_zset = self.target_client.zrange(key, 0, -1, withscores=True)
                if source_zset != target_zset:
                    return (True, "有序集合值不同")

            else:
                # 对于其他类型，假设不同
                return (True, f"未知类型: {source_type}")

            # 值相同
            return (False, "值相同")

        except Exception as e:
            logger.debug(f"比较键 {key} 时出错: {e}")
            return (True, f"比较出错: {str(e)}")  # 出错时假设不同，需要同步
    
    def _sync_changed_keys(self, changed_keys: List[str]) -> Dict[str, int]:
        """同步变更的键。"""
        synced_count = 0
        failed_count = 0

        logger.info(f"开始同步 {len(changed_keys)} 个变更的键")

        for i, key in enumerate(changed_keys, 1):
            try:
                logger.debug(f"  [{i}/{len(changed_keys)}] 同步键: {key}")
                if self._sync_single_key(key):
                    synced_count += 1
                    logger.debug(f"    ✓ 同步成功")
                else:
                    failed_count += 1
                    logger.warning(f"    ✗ 同步失败")
            except Exception as e:
                logger.error(f"    ✗ 同步键 {key} 失败: {e}", exc_info=True)
                failed_count += 1

        logger.info(f"同步完成: 成功 {synced_count}, 失败 {failed_count}")
        return {'synced': synced_count, 'failed': failed_count}
    
    def _sync_single_key(self, key: str) -> bool:
        """使用 DUMP/RESTORE 原子同步单个键（含流等类型）。"""
        try:
            src_exists = bool(self.source_client.exists(key))
            sync_key_with_dump_restore(
                self.source_client,
                self.target_client,
                key,
                overwrite=True,
            )
            with self._stats_lock:
                if not src_exists:
                    self.incremental_stats["change_types"]["deleted"] += 1
                else:
                    self.incremental_stats["change_types"]["updated"] += 1
            return True

        except Exception as e:
            logger.error("      ✗ 同步键 %s 失败: %s", key, e, exc_info=True)
            return False

    def get_incremental_stats(self) -> Dict[str, Any]:
        """获取增量迁移统计信息。"""
        with self._stats_lock:
            stats = {
                **{k: v for k, v in self.incremental_stats.items() if k != "change_types"},
                "change_types": dict(self.incremental_stats["change_types"]),
            }
            stats['is_monitoring'] = self.is_monitoring
            stats['last_sync_timestamp'] = self.last_sync_time
        
        if stats['start_time']:
            stats['running_time'] = datetime.now() - stats['start_time']
            stats['running_time_seconds'] = stats['running_time'].total_seconds()
        
        if stats['sync_intervals']:
            stats['avg_sync_duration'] = sum(stats['sync_intervals']) / len(stats['sync_intervals'])
            stats['total_syncs'] = len(stats['sync_intervals'])
        
        return stats
    
    def set_sync_checkpoint(self, checkpoint_data: Dict[str, Any]):
        """设置同步检查点。"""
        self.sync_checkpoint = checkpoint_data
        self.last_sync_time = checkpoint_data.get('timestamp', time.time())
        logger.info(f"设置同步检查点: {checkpoint_data}")
    
    def get_sync_checkpoint(self) -> Dict[str, Any]:
        """获取同步检查点。"""
        return {
            'timestamp': self.last_sync_time,
            'checkpoint_data': self.sync_checkpoint,
            'stats': self.get_incremental_stats()
        }
