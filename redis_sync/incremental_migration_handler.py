"""
增量迁移处理器

实现Redis的增量数据迁移，支持基于时间戳、键变更监控和复制流的增量同步。
可以检测和同步源Redis中的数据变更到目标Redis。
"""

import redis
import logging
import time
import threading
from typing import Optional, Callable, Dict, Any, List, Set, Union
from datetime import datetime
from collections import defaultdict

from .exceptions import MigrationError
from .key_sync import (
    _is_restore_compatibility_error,
    _sync_key_fallback,
    capture_dump_with_preflight,
    redis_values_equal,
    restore_dump_with_deadline,
    sync_key_with_dump_restore,
)
from .sync_filters import KeySyncFilter, build_atomic_filtered_delete_command
logger = logging.getLogger(__name__)
Key = Union[str, bytes]
MAX_PIPELINE_KEYS = 200


def _key_chunks(keys, batch_size: int = MAX_PIPELINE_KEYS):
    for offset in range(0, len(keys), batch_size):
        yield keys[offset:offset + batch_size]


class IncrementalMigrationHandler:
    """处理Redis增量迁移的核心类。"""

    SCAN_MAX_RETRIES = 3
    SCAN_RETRY_DELAY = 0.1
    THREAD_JOIN_TIMEOUT = 10

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
        self._monitor_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._detection_turn_lock = threading.Lock()
        self._prefer_deletions_next = False

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
        with self._monitor_lock:
            active_thread = self.monitor_thread
            if self.is_monitoring or (
                active_thread is not None and active_thread.is_alive()
            ):
                logger.warning("增量同步已在运行")
                return False

            logger.info(f"启动增量同步，间隔: {sync_interval}秒")

            self.incremental_stats['start_time'] = datetime.now()
            self.last_sync_time = time.time()
            self.stop_event.clear()
            self.is_monitoring = True

            # Assign the worker before start so its finalizer can identify
            # whether it still owns the lifecycle state.
            worker = threading.Thread(
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
            self.monitor_thread = worker
            try:
                worker.start()
            except Exception:
                self.monitor_thread = None
                self.is_monitoring = False
                self.stop_event.set()
                raise
        
        return True
    
    def stop_incremental_sync(self) -> Dict[str, Any]:
        """
        停止增量同步。
        
        返回:
            同步统计信息
        """
        with self._monitor_lock:
            worker = self.monitor_thread
            if not self.is_monitoring and not (
                worker is not None and worker.is_alive()
            ):
                logger.warning("增量同步未在运行")
                return self.incremental_stats

            logger.info("停止增量同步")
            self.stop_event.set()

        if (
            worker is not None
            and worker.is_alive()
            and worker is not threading.current_thread()
        ):
            worker.join(timeout=self.THREAD_JOIN_TIMEOUT)

        with self._monitor_lock:
            if worker is not None and worker.is_alive():
                # Keep ownership and the stop event intact. A subsequent start
                # must not revive this worker or create a second one.
                self.is_monitoring = True
                logger.error("增量同步工作线程未在超时时间内停止")
            else:
                if self.monitor_thread is worker:
                    self.monitor_thread = None
                self.is_monitoring = False
        
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
            detected_changes = self._detect_changed_keys(
                key_pattern,
                key_types,
                sync_timestamp,
                max_changes,
                key_filter=key_filter,
                return_deletions=True,
            )
            if (
                isinstance(detected_changes, tuple)
                and len(detected_changes) == 2
            ):
                changed_keys, deletion_candidates = detected_changes
            else:
                # Preserve compatibility with callers/tests replacing the legacy
                # detector with a plain list-returning function.
                changed_keys = detected_changes
                deletion_candidates = set()

            if not changed_keys:
                logger.info("✓ 未检测到键变更")
                logger.info("=" * 60)
                with self._stats_lock:
                    self.incremental_stats['last_sync_time'] = datetime.now()
                self.last_sync_time = start_time
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
            sync_result = self._sync_changed_keys(
                changed_keys,
                deletion_candidates=deletion_candidates,
                key_types=key_types,
                key_filter=key_filter,
            )

            logger.info(f"✓ 同步完成:")
            logger.info(f"  成功: {sync_result['synced']} 个")
            logger.info(f"  失败: {sync_result['failed']} 个")

            with self._stats_lock:
                self.incremental_stats['total_changes'] += len(changed_keys)
                self.incremental_stats['successful_changes'] += sync_result['synced']
                self.incremental_stats['failed_changes'] += sync_result['failed']

            duration = time.time() - start_time
            with self._stats_lock:
                self.incremental_stats['sync_intervals'].append(duration)
                if len(self.incremental_stats['sync_intervals']) > 1000:
                    self.incremental_stats['sync_intervals'] = self.incremental_stats['sync_intervals'][-500:]

            success = sync_result['failed'] == 0
            if success:
                with self._stats_lock:
                    self.incremental_stats['last_sync_time'] = datetime.now()
                self.last_sync_time = start_time

            if success:
                logger.info(f"✓ 增量同步完成，耗时: {duration:.2f} 秒")
            else:
                logger.error(
                    "✗ 增量同步有 %s 个键失败，保留原同步检查点",
                    sync_result['failed'],
                )
            logger.info("=" * 60)

            result = {
                'success': success,
                'changed_keys': len(changed_keys),
                'synced_keys': sync_result['synced'],
                'failed_keys': sync_result['failed'],
                'duration': duration,
                'sync_timestamp': start_time,
            }
            if not success:
                result['error'] = f"{sync_result['failed']} 个键同步失败"
            return result

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

        try:
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
        finally:
            with self._monitor_lock:
                if self.monitor_thread is threading.current_thread():
                    self.monitor_thread = None
                    self.is_monitoring = False
            logger.info("增量同步工作线程停止")
    
    def _detect_changed_keys(self,
                           key_pattern: str,
                           key_types: Optional[List[str]],
                           since_timestamp: float,
                           max_changes: int,
                           key_filter: Optional[KeySyncFilter] = None,
                           return_deletions: bool = False):
        """
        检测变更的键。

        这里使用多种策略来检测变更：
        1. 比较键的最后修改时间（如果Redis支持）
        2. 比较键的值哈希
        3. 扫描所有键并与目标比较
        """
        try:
            if max_changes <= 0:
                return ([], set()) if return_deletions else []

            with self._detection_turn_lock:
                prefer_deletions = self._prefer_deletions_next
                self._prefer_deletions_next = not prefer_deletions

            # IDLETIME measures reads as well as writes and can consume the
            # change limit with unchanged hot keys. Compare serialized state
            # instead so every reported key represents a real divergence.
            logger.info("使用 DUMP/PTTL 比较检测变更")
            if max_changes == 1:
                upsert_budget = 0 if prefer_deletions else 1
                deletion_budget = 1 - upsert_budget
            else:
                smaller_budget = max_changes // 2
                larger_budget = max_changes - smaller_budget
                if prefer_deletions:
                    upsert_budget, deletion_budget = smaller_budget, larger_budget
                else:
                    upsert_budget, deletion_budget = larger_budget, smaller_budget

            changed_keys = self._detect_changes_by_comparison(
                key_pattern, key_types, upsert_budget, set(), key_filter=key_filter
            ) if upsert_budget else []
            deleted_keys = self._detect_target_only_keys(
                key_pattern,
                key_types,
                deletion_budget,
                set(changed_keys),
                key_filter=key_filter,
            ) if deletion_budget else []

            remaining_limit = max_changes - len(changed_keys) - len(deleted_keys)
            if remaining_limit > 0 and len(changed_keys) < upsert_budget:
                extra_deletions = self._detect_target_only_keys(
                    key_pattern,
                    key_types,
                    remaining_limit,
                    set(changed_keys) | set(deleted_keys),
                    key_filter=key_filter,
                )
                deleted_keys.extend(extra_deletions)
                remaining_limit -= len(extra_deletions)
            if remaining_limit > 0 and len(deleted_keys) < deletion_budget:
                extra_changes = self._detect_changes_by_comparison(
                    key_pattern,
                    key_types,
                    remaining_limit,
                    set(changed_keys) | set(deleted_keys),
                    key_filter=key_filter,
                )
                changed_keys.extend(extra_changes)

            logger.info("  值比较检测到 %s 个变更", len(changed_keys))
            logger.info("  检测到 %s 个源端已删除键", len(deleted_keys))
            changed_keys.extend(deleted_keys)

            selected_changes = changed_keys[:max_changes]
            logger.info(f"总共检测到 {len(selected_changes)} 个变更的键")
            if return_deletions:
                selected_set = set(selected_changes)
                return selected_changes, {
                    key for key in deleted_keys if key in selected_set
                }
            return selected_changes

        except Exception as e:
            logger.error(f"✗ 检测键变更失败: {e}", exc_info=True)
            raise MigrationError(f"检测键变更失败: {e}") from e
    
    def _detect_changes_by_idle_time(self,
                                   key_pattern: str,
                                   key_types: Optional[List[str]],
                                   since_timestamp: float,
                                   max_changes: int,
                                   key_filter: Optional[KeySyncFilter] = None) -> List[Key]:
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
                cursor, keys = self._scan_page(cursor, key_pattern, scan_count)
                if not keys:
                    if cursor == 0:
                        break
                    continue

                batch = list(keys)

                if key_types:
                    filtered = []
                    for chunk in _key_chunks(batch):
                        pipe = self.source_client.pipeline(transaction=False)
                        for key in chunk:
                            pipe.type(key)
                        types = pipe.execute()
                        for key, key_type in zip(chunk, types):
                            normalized = (
                                key_type.decode()
                                if isinstance(key_type, bytes)
                                else key_type
                            )
                            if normalized in key_types:
                                filtered.append(key)
                    batch = filtered

                if not batch:
                    if cursor == 0:
                        break
                    continue

                if key_filter:
                    batch = list(key_filter.filter_batch(self.source_client, batch))
                    if not batch:
                        if cursor == 0:
                            break
                        continue

                for sub in _key_chunks(batch):
                    pipe = self.source_client.pipeline(transaction=False)
                    for key in sub:
                        pipe.object("idletime", key)
                    idle_times = pipe.execute()
                    for i, idle_time in enumerate(idle_times):
                        if idle_time is None:
                            continue
                        if idle_time <= time_diff + 5:
                            key = sub[i]
                            changed_keys.append(key)
                            logger.debug(
                                "✓ 变更键: %r, idle=%s秒", key, idle_time
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
            raise MigrationError(f"IDLETIME 扫描失败: {e}") from e
    
    def _detect_changes_by_comparison(self,
                                    key_pattern: str,
                                    key_types: Optional[List[str]],
                                    max_changes: int,
                                    exclude_keys: Set[Key],
                                    key_filter: Optional[KeySyncFilter] = None) -> List[Key]:
        """
        通过值比较检测变更的键。

        这个方法会扫描所有匹配的键，并比较源和目标的值。
        如果键不存在于目标或值不同，则认为是变更。
        """
        changed_keys = []
        selected_keys = set(exclude_keys)

        cursor = 0
        scanned_count = 0

        logger.debug(f"开始值比较检测，排除键数: {len(exclude_keys)}")

        while len(changed_keys) < max_changes:
            try:
                cursor, keys = self._scan_page(
                    cursor,
                    key_pattern,
                    max(1, self.scan_count // 2),
                )

                page_seen = set()
                candidates = []
                for key in keys:
                    if key in page_seen or key in selected_keys:
                        continue
                    page_seen.add(key)
                    candidates.append(key)
                if key_filter and candidates:
                    candidates = list(
                        key_filter.filter_batch(self.source_client, candidates)
                    )

                for key in candidates:
                    scanned_count += 1

                    # 检查键类型
                    if key_types:
                        key_type = self.source_client.type(key)
                        if isinstance(key_type, bytes):
                            key_type = key_type.decode()
                        if key_type not in key_types:
                            continue

                    # 比较源和目标的值与过期状态
                    is_different, reason = self._is_key_different(key)
                    if is_different:
                        changed_keys.append(key)
                        selected_keys.add(key)
                        logger.debug("检测到变更键: %r, 原因: %s", key, reason)

                        if len(changed_keys) >= max_changes:
                            break

                if cursor == 0:
                    break

            except Exception as e:
                logger.error(f"比较扫描时出错: {e}")
                raise MigrationError(f"值比较 SCAN 失败: {e}") from e

        logger.info(f"通过值比较检测到 {len(changed_keys)} 个变更的键（扫描了{scanned_count}个键）")
        return changed_keys

    def _detect_target_only_keys(
        self,
        key_pattern: str,
        key_types: Optional[List[str]],
        max_changes: int,
        exclude_keys: Set[Key],
        key_filter: Optional[KeySyncFilter] = None,
    ) -> List[Key]:
        """Find target keys that disappeared from the managed source keyspace."""
        if max_changes <= 0:
            return []
        deleted = []
        selected_keys = set(exclude_keys)
        cursor = 0
        while len(deleted) < max_changes:
            cursor, raw_keys = self._scan_target_page(
                cursor, key_pattern, max(1, self.scan_count)
            )
            keys = []
            page_seen = set()
            for key in raw_keys:
                if key in page_seen or key in selected_keys:
                    continue
                page_seen.add(key)
                keys.append(key)
            if key_filter:
                keys = list(key_filter.filter_batch(self.target_client, list(keys)))
            if key_types and keys:
                typed_keys = []
                for chunk in _key_chunks(keys):
                    pipe = self.target_client.pipeline(transaction=False)
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
                            key_type.decode()
                            if isinstance(key_type, bytes)
                            else str(key_type)
                        ) in key_types
                    )
                keys = typed_keys
            if keys:
                stride = 2 if key_types else 1
                source_allowed = []
                for chunk in _key_chunks(keys):
                    pipe = self.source_client.pipeline(transaction=False)
                    for key in chunk:
                        pipe.exists(key)
                        if key_types:
                            pipe.type(key)
                    raw_source = pipe.execute(raise_on_error=False)
                    for index, key in enumerate(chunk):
                        source_exists = raw_source[index * stride]
                        source_type = (
                            raw_source[index * stride + 1] if key_types else None
                        )
                        if isinstance(source_exists, BaseException):
                            raise source_exists
                        if isinstance(source_type, BaseException):
                            raise source_type
                        allowed = bool(source_exists)
                        if allowed and key_types:
                            normalized = (
                                source_type.decode()
                                if isinstance(source_type, bytes)
                                else str(source_type)
                            )
                            allowed = normalized in key_types
                        if allowed:
                            source_allowed.append(key)

                if key_filter and source_allowed:
                    source_allowed = list(
                        key_filter.filter_batch(self.source_client, source_allowed)
                    )
                allowed_set = set(source_allowed)
                for key in keys:
                    if key not in allowed_set:
                        deleted.append(key)
                        selected_keys.add(key)
                        if len(deleted) >= max_changes:
                            break
            if cursor == 0:
                break
        return deleted
    
    def _is_key_different(self, key) -> tuple:
        """
        检查键在源和目标中是否不同。

        返回:
            (is_different, reason): 是否不同和原因
        """
        if not self.target_client.exists(key):
            return (True, "目标中不存在")

        if not redis_values_equal(self.source_client, self.target_client, key):
            return (True, "序列化值不同")

        source_ttl = int(self.source_client.pttl(key))
        target_ttl = int(self.target_client.pttl(key))
        if source_ttl <= 0 or target_ttl <= 0:
            if source_ttl != target_ttl:
                return (True, f"过期状态不同: {source_ttl} vs {target_ttl}")
        elif abs(source_ttl - target_ttl) > 2000:
            return (True, f"TTL不同: {source_ttl} vs {target_ttl}")
        return (False, "值和TTL相同")

    def _source_key_in_scope(
        self,
        key: Key,
        key_types: Optional[List[str]],
        key_filter: Optional[KeySyncFilter],
    ) -> bool:
        """Re-evaluate dynamic source filters immediately before a delete."""
        if not self.source_client.exists(key):
            return False
        if key_types:
            key_type = self.source_client.type(key)
            normalized = (
                key_type.decode() if isinstance(key_type, bytes) else str(key_type)
            )
            if normalized not in key_types:
                return False
        if key_filter:
            return bool(key_filter.filter_batch(self.source_client, [key]))
        return True

    def _capture_scoped_source_key(
        self,
        key: Key,
        key_types: Optional[List[str]],
        key_filter: Optional[KeySyncFilter],
    ):
        """Atomically capture one source value and its dynamic filter state."""
        if key_filter and not key_filter.name_allowed(key):
            return False, None, None, False
        captured = capture_dump_with_preflight(
            self.source_client,
            key,
            preserve_ttl=True,
            key_types=key_types,
            min_ttl=key_filter.min_ttl if key_filter else 0,
            max_key_size=key_filter.max_key_size if key_filter else 0,
        )
        if captured is None:
            return False, None, None, False
        return (
            True,
            captured.dump_data,
            captured.expires_at_ms,
            captured.expiry_is_exact,
        )

    def _sync_deletion_candidate(
        self,
        key: Key,
        key_types: Optional[List[str]],
        key_filter: Optional[KeySyncFilter],
        _repair_depth: int = 0,
    ) -> bool:
        """Delete an out-of-scope key, repairing a concurrent re-entry."""
        if key_filter and not key_filter.name_allowed(key):
            return True
        if self._source_key_in_scope(key, key_types, key_filter):
            if _repair_depth >= 1:
                return False
            return self._sync_single_key(
                key, key_types, key_filter, _repair_depth=_repair_depth + 1
            )

        delete_command = build_atomic_filtered_delete_command(
            [key],
            key_types=key_types,
            min_ttl=key_filter.min_ttl if key_filter else 0,
            max_key_size=key_filter.max_key_size if key_filter else 0,
        )
        deleted = self.target_client.execute_command(*delete_command)

        # A source key can be recreated or move back into a dynamic TTL/size
        # filter while the target DEL is in flight. Recheck and restore it now.
        if self._source_key_in_scope(key, key_types, key_filter):
            if _repair_depth >= 1:
                return False
            return self._sync_single_key(
                key, key_types, key_filter, _repair_depth=_repair_depth + 1
            )

        if deleted:
            with self._stats_lock:
                self.incremental_stats["change_types"]["deleted"] += int(deleted)
        return True

    def _sync_changed_keys(
        self,
        changed_keys: List[Key],
        deletion_candidates: Optional[Set[Key]] = None,
        key_types: Optional[List[str]] = None,
        key_filter: Optional[KeySyncFilter] = None,
    ) -> Dict[str, int]:
        """同步变更的键。"""
        synced_count = 0
        failed_count = 0
        deletion_candidates = set(deletion_candidates or ())

        logger.info(f"开始同步 {len(changed_keys)} 个变更的键")

        for i, key in enumerate(changed_keys, 1):
            try:
                logger.debug(f"  [{i}/{len(changed_keys)}] 同步键: {key}")
                if key in deletion_candidates:
                    success = self._sync_deletion_candidate(
                        key, key_types, key_filter
                    )
                else:
                    success = self._sync_single_key(key, key_types, key_filter)
                if success:
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
    
    def _sync_single_key(
        self,
        key: Key,
        key_types: Optional[List[str]] = None,
        key_filter: Optional[KeySyncFilter] = None,
        *,
        _repair_depth: int = 0,
    ) -> bool:
        """使用 DUMP/RESTORE 原子同步单个键（含流等类型）。"""
        try:
            if key_types or key_filter:
                in_scope, dump_data, expires_at_ms, expiry_is_exact = (
                    self._capture_scoped_source_key(key, key_types, key_filter)
                )
                if not in_scope:
                    return self._sync_deletion_candidate(
                        key,
                        key_types,
                        key_filter,
                        _repair_depth=_repair_depth,
                    )
                try:
                    restored = restore_dump_with_deadline(
                        self.target_client,
                        key,
                        dump_data,
                        expires_at_ms,
                        overwrite=True,
                        expires_at_is_exact=expiry_is_exact,
                    )
                except redis.ResponseError as error:
                    if not _is_restore_compatibility_error(error):
                        raise
                    fallback_pttl = (
                        -1
                        if expires_at_ms is None
                        else max(
                            1,
                            expires_at_ms - int(time.time() * 1000),
                        )
                    )
                    restored = _sync_key_fallback(
                        self.source_client,
                        self.target_client,
                        key,
                        fallback_pttl,
                        True,
                        overwrite=True,
                        expires_at_ms=expires_at_ms,
                        expected_dump=dump_data,
                        key_types=key_types,
                        min_ttl=key_filter.min_ttl if key_filter else 0,
                        max_key_size=key_filter.max_key_size if key_filter else 0,
                        expires_at_is_exact=expiry_is_exact,
                    )
                if not restored:
                    return False
                if not self._source_key_in_scope(key, key_types, key_filter):
                    return self._sync_deletion_candidate(
                        key,
                        key_types,
                        key_filter,
                        _repair_depth=_repair_depth,
                    )
                with self._stats_lock:
                    self.incremental_stats["change_types"]["updated"] += 1
                return True

            sync_key_with_dump_restore(
                self.source_client,
                self.target_client,
                key,
                overwrite=True,
            )
            with self._stats_lock:
                if not self.source_client.exists(key):
                    self.incremental_stats["change_types"]["deleted"] += 1
                else:
                    self.incremental_stats["change_types"]["updated"] += 1
            return True

        except Exception as e:
            logger.error("      ✗ 同步键 %s 失败: %s", key, e, exc_info=True)
            return False

    def _scan_page(self, cursor: int, pattern: str, count: int):
        """读取一页 SCAN；持续失败时向上层返回明确错误。"""
        last_error = None
        for attempt in range(1, self.SCAN_MAX_RETRIES + 1):
            try:
                return self.source_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=count,
                )
            except Exception as e:
                last_error = e
                if attempt < self.SCAN_MAX_RETRIES:
                    logger.warning(
                        "增量 SCAN 失败，准备重试 (%s/%s): %s",
                        attempt,
                        self.SCAN_MAX_RETRIES,
                        e,
                    )
                    if self.SCAN_RETRY_DELAY > 0:
                        time.sleep(self.SCAN_RETRY_DELAY * attempt)
        raise MigrationError(
            f"增量 SCAN 连续失败 {self.SCAN_MAX_RETRIES} 次: {last_error}"
        ) from last_error

    def _scan_target_page(self, cursor: int, pattern: str, count: int):
        last_error = None
        for attempt in range(1, self.SCAN_MAX_RETRIES + 1):
            try:
                return self.target_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=count,
                )
            except Exception as e:
                last_error = e
                if attempt < self.SCAN_MAX_RETRIES and self.SCAN_RETRY_DELAY > 0:
                    time.sleep(self.SCAN_RETRY_DELAY * attempt)
        raise MigrationError(
            f"目标 SCAN 连续失败 {self.SCAN_MAX_RETRIES} 次: {last_error}"
        ) from last_error

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
