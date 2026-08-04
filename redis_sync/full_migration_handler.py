"""
全量迁移处理器

实现Redis的全量数据迁移，支持多种全量迁移策略。
包括基于SYNC的RDB快照迁移和基于SCAN的完整键迁移。
"""

import redis
import logging
import time
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime

from .exceptions import MigrationError, SyncError
from .key_sync import (
    _is_absttl_compatibility_error,
    _is_busykey_error,
    _is_restore_compatibility_error,
    _sync_key_fallback,
    restore_dump_with_deadline,
    source_supports_pexpiretime,
)
from .sync_filters import (
    KeySyncFilter,
    build_atomic_filtered_delete_command,
    source_state_in_dynamic_scope,
)
from .utils import ProgressTracker, format_bytes, format_duration

logger = logging.getLogger(__name__)
MAX_PIPELINE_KEYS = 200


def _bounded_pipeline_batch_size(batch_size: int) -> int:
    return max(1, min(int(batch_size), MAX_PIPELINE_KEYS))


def _type_str(key_type) -> str:
    if isinstance(key_type, bytes):
        return key_type.decode()
    return str(key_type)


class FullMigrationHandler:
    """处理Redis全量迁移的核心类。"""

    SUPPORTED_STRATEGIES = frozenset({"scan", "sync", "dump_restore"})
    SCAN_MAX_RETRIES = 3
    SCAN_RETRY_DELAY = 0.1
    
    def __init__(self, source_client: redis.Redis, target_client: redis.Redis):
        """
        初始化全量迁移处理器。
        
        参数:
            source_client: 源Redis客户端
            target_client: 目标Redis客户端
        """
        self.source_client = source_client
        self.target_client = target_client
        self.migration_start_time = None
        self.migration_stats = {
            'total_keys': 0,
            'migrated_keys': 0,
            'failed_keys': 0,
            'skipped_keys': 0,
            'total_bytes': 0,
            'start_time': None,
            'end_time': None
        }

    def _filter_keys_by_types(
        self,
        keys: List[bytes],
        key_types: List[str],
        batch_size: int,
    ) -> List[bytes]:
        filtered_keys = []
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        for offset in range(0, len(keys), pipeline_batch_size):
            chunk = keys[offset:offset + pipeline_batch_size]
            pipe = self.source_client.pipeline(transaction=False)
            for key in chunk:
                pipe.type(key)
            raw_types = pipe.execute(raise_on_error=False)
            for value in raw_types:
                if isinstance(value, BaseException):
                    raise value
            filtered_keys.extend(
                key
                for key, key_type in zip(chunk, raw_types)
                if _type_str(key_type) in key_types
            )
        return filtered_keys
    
    def perform_full_migration(self,
                              strategy: str = "scan",
                              clear_target: bool = False,
                              preserve_ttl: bool = True,
                              batch_size: int = 1000,
                              scan_count: int = 10000,
                              progress_callback: Optional[Callable[[int, int], None]] = None,
                              key_pattern: str = "*",
                              key_types: Optional[List[str]] = None,
                              key_filter: Optional[KeySyncFilter] = None,
                              overwrite_existing: bool = False) -> Dict[str, Any]:
        """
        执行全量迁移。

        参数:
            strategy: 迁移策略 ("scan", "sync", "dump_restore")
            clear_target: 是否清空目标数据库
            preserve_ttl: 是否保持TTL
            batch_size: 批处理大小（每批处理的键数）
            scan_count: SCAN命令的COUNT参数（每次SCAN返回的键数）
            progress_callback: 进度回调函数
            key_pattern: 键模式过滤
            key_types: 键类型过滤列表
            overwrite_existing: 是否覆盖目标端已有键

        返回:
            迁移结果统计
        """
        logger.info(f"开始全量迁移，策略: {strategy}")
        self.migration_start_time = time.time()
        self.migration_stats = {
            'total_keys': 0,
            'migrated_keys': 0,
            'failed_keys': 0,
            'skipped_keys': 0,
            'total_bytes': 0,
            'start_time': datetime.now(),
            'end_time': None,
        }
        self._active_full_params = {
            'preserve_ttl': preserve_ttl,
            'batch_size': batch_size,
            'scan_count': scan_count,
            'progress_callback': progress_callback,
            'key_pattern': key_pattern,
            'key_types': key_types,
            'overwrite_existing': overwrite_existing,
        }
        self._key_filter = key_filter

        try:
            try:
                if strategy not in self.SUPPORTED_STRATEGIES:
                    raise MigrationError(f"不支持的迁移策略: {strategy}")

                # 清空目标数据库（如果需要）
                if clear_target:
                    self._clear_target_database()

                # 根据策略执行迁移
                if strategy == "scan":
                    result = self._migrate_with_scan(
                        preserve_ttl, batch_size, scan_count, progress_callback,
                        key_pattern, key_types, overwrite_existing
                    )
                elif strategy == "sync":
                    result = self._migrate_with_sync(progress_callback)
                elif strategy == "dump_restore":
                    result = self._migrate_with_dump_restore(
                        preserve_ttl, batch_size, scan_count, progress_callback,
                        key_pattern, key_types, overwrite_existing
                    )

                if self.migration_stats['failed_keys'] > 0:
                    raise MigrationError(
                        f"全量迁移有 {self.migration_stats['failed_keys']} 个键失败"
                    )

                self.migration_stats['end_time'] = datetime.now()
                duration = time.time() - self.migration_start_time

                logger.info(f"全量迁移完成，耗时: {format_duration(duration)}")

                return {
                    'success': True,
                    'strategy': strategy,
                    'duration': duration,
                    'statistics': self.migration_stats,
                    'details': result
                }

            except Exception as e:
                logger.error(f"全量迁移失败: {e}")
                self.migration_stats['end_time'] = datetime.now()
                return {
                    'success': False,
                    'strategy': strategy,
                    'duration': time.time() - self.migration_start_time,
                    'statistics': self.migration_stats,
                    'error': str(e)
                }
        finally:
            self._key_filter = None
            self._active_full_params = None
    
    def _clear_target_database(self):
        """清空目标数据库。"""
        logger.info("清空目标数据库")
        try:
            self.target_client.flushdb()
            logger.info("目标数据库已清空")
        except Exception as e:
            logger.error(f"清空目标数据库失败: {e}")
            raise MigrationError(f"清空目标数据库失败: {e}")
    
    def _migrate_with_scan(self,
                          preserve_ttl: bool,
                          batch_size: int,
                          scan_count: int,
                          progress_callback: Optional[Callable],
                          key_pattern: str,
                          key_types: Optional[List[str]],
                          overwrite_existing: bool = False) -> Dict[str, Any]:
        """使用SCAN策略进行全量迁移。"""
        logger.info(f"使用SCAN策略进行全量迁移（scan_count={scan_count}）")
        
        # 估算总键数
        total_keys = self._estimate_key_count(key_pattern, key_types)
        self.migration_stats['total_keys'] = total_keys
        
        progress_tracker = ProgressTracker(total_keys, "SCAN全量迁移")
        migrated_count = 0
        failed_count = 0
        skipped_count = 0
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        
        cursor = 0
        while True:
            # 扫描键（使用scan_count参数）
            cursor, keys = self._scan_page(cursor, key_pattern, scan_count)
                
            if keys:
                if getattr(self, "_key_filter", None):
                    keys = self._key_filter.filter_names(list(keys))

                # 迁移这批键
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for offset in range(0, len(keys), pipeline_batch_size):
                    batch = keys[offset:offset + pipeline_batch_size]
                    batch_result = self._migrate_key_batch(
                        batch, preserve_ttl, overwrite_existing
                    )
                    migrated_count += batch_result['migrated']
                    failed_count += batch_result['failed']
                    skipped_count += batch_result['skipped']

                # 更新进度
                progress_tracker.update(len(keys))
                if progress_callback:
                    progress_callback(
                        migrated_count + failed_count + skipped_count, total_keys
                    )

            if cursor == 0:
                break
        
        self.migration_stats['migrated_keys'] = migrated_count
        self.migration_stats['failed_keys'] = failed_count
        self.migration_stats['skipped_keys'] = skipped_count
        
        return {
            'migrated_keys': migrated_count,
            'failed_keys': failed_count,
            'skipped_keys': skipped_count,
            'total_processed': migrated_count + failed_count + skipped_count
        }
    
    def _migrate_with_sync(self, progress_callback: Optional[Callable]) -> Dict[str, Any]:
        """Execute the supported key-level fallback for the SYNC option.

        The tool does not load an RDB into the target Redis data directory.
        Opening a SYNC stream only to discard that RDB also occupies a source
        pool connection while the fallback needs another one, which deadlocks
        multi-target service runs at the documented pool minimum.
        """
        params = getattr(self, '_active_full_params', None)
        if not params:
            raise SyncError("内部错误：缺少全量迁移上下文，无法执行 SYNC 回退")

        logger.warning(
            "SYNC RDB 不能由客户端直接载入目标实例；直接使用 "
            "SCAN+DUMP/RESTORE 执行等价全量复制"
        )
        result = self._migrate_with_scan(
            params['preserve_ttl'],
            params['batch_size'],
            params['scan_count'],
            progress_callback or params.get('progress_callback'),
            params['key_pattern'],
            params['key_types'],
            params['overwrite_existing'],
        )
        return {
            **result,
            'rdb_size': 0,
            'fallback': 'scan_dump_restore',
        }
    
    def _migrate_with_dump_restore(self,
                                  preserve_ttl: bool,
                                  batch_size: int,
                                  scan_count: int,
                                  progress_callback: Optional[Callable],
                                  key_pattern: str,
                                  key_types: Optional[List[str]],
                                  overwrite_existing: bool = False) -> Dict[str, Any]:
        """使用DUMP/RESTORE策略进行全量迁移。"""
        logger.info(f"使用DUMP/RESTORE策略进行全量迁移（scan_count={scan_count}）")
        
        # 估算总键数
        total_keys = self._estimate_key_count(key_pattern, key_types)
        self.migration_stats['total_keys'] = total_keys
        
        progress_tracker = ProgressTracker(total_keys, "DUMP/RESTORE全量迁移")
        migrated_count = 0
        failed_count = 0
        skipped_count = 0
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        
        cursor = 0
        while True:
            # 扫描键（使用scan_count参数）
            cursor, keys = self._scan_page(cursor, key_pattern, scan_count)
                
            if keys:
                if getattr(self, "_key_filter", None):
                    keys = self._key_filter.filter_names(list(keys))

                # 使用DUMP/RESTORE迁移这批键
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for offset in range(0, len(keys), pipeline_batch_size):
                    batch = keys[offset:offset + pipeline_batch_size]
                    batch_result = self._dump_restore_batch(
                        batch, preserve_ttl, overwrite_existing
                    )
                    migrated_count += batch_result['migrated']
                    failed_count += batch_result['failed']
                    skipped_count += batch_result['skipped']

                # 更新进度
                progress_tracker.update(len(keys))
                if progress_callback:
                    progress_callback(
                        migrated_count + failed_count + skipped_count, total_keys
                    )

            if cursor == 0:
                break
        
        self.migration_stats['migrated_keys'] = migrated_count
        self.migration_stats['failed_keys'] = failed_count
        self.migration_stats['skipped_keys'] = skipped_count
        
        return {
            'migrated_keys': migrated_count,
            'failed_keys': failed_count,
            'skipped_keys': skipped_count,
            'total_processed': migrated_count + failed_count + skipped_count
        }
    
    def _apply_rdb_data(self, rdb_data: bytes, progress_callback: Optional[Callable]):
        """
        SYNC 返回的 RDB 无法在独立进程中安全写入目标实例数据目录时，
        改为从源端执行 SCAN+DUMP/RESTORE（与全量 scan 策略等价，保证目标有数据）。
        """
        params = getattr(self, '_active_full_params', None)
        if not params:
            raise SyncError("内部错误：缺少全量迁移上下文，无法处理 RDB")

        logger.warning(
            "已接收 RDB 流 (%s)，不在本进程内解析 RDB 文件；"
            "改用 SCAN+DUMP/RESTORE 从源库复制到目标",
            format_bytes(len(rdb_data)),
        )
        _ = rdb_data
        self._migrate_with_scan(
            params['preserve_ttl'],
            params['batch_size'],
            params['scan_count'],
            progress_callback or params.get('progress_callback'),
            params['key_pattern'],
            params['key_types'],
            params['overwrite_existing'],
        )
    
    def _migrate_key_batch(
        self,
        keys: List[bytes],
        preserve_ttl: bool,
        overwrite_existing: bool = False,
    ) -> Dict[str, int]:
        """迁移一批键（优化：使用DUMP/RESTORE Pipeline批量处理）。"""
        # 直接使用DUMP/RESTORE批量处理（最快）
        return self._dump_restore_batch(keys, preserve_ttl, overwrite_existing)
    
    def _dump_restore_batch(
        self,
        keys: List[bytes],
        preserve_ttl: bool,
        overwrite_existing: bool = False,
    ) -> Dict[str, int]:
        """Migrate keys through internally bounded Redis pipelines."""
        totals = {'migrated': 0, 'failed': 0, 'skipped': 0}
        candidates = list(keys)
        active_params = getattr(self, '_active_full_params', None) or {}
        pipeline_batch_size = _bounded_pipeline_batch_size(
            active_params.get('batch_size', MAX_PIPELINE_KEYS)
        )
        for offset in range(0, len(candidates), pipeline_batch_size):
            result = self._dump_restore_chunk(
                candidates[offset:offset + pipeline_batch_size],
                preserve_ttl,
                overwrite_existing,
            )
            for field in totals:
                totals[field] += result[field]
        return totals

    def _dump_restore_chunk(
        self,
        keys: List[bytes],
        preserve_ttl: bool,
        overwrite_existing: bool = False,
    ) -> Dict[str, int]:
        """Run one DUMP/RESTORE pipeline chunk of at most 200 keys."""
        migrated = 0
        failed = 0
        skipped = 0
        candidates = list(keys)

        if not overwrite_existing and candidates:
            try:
                pipe = self.target_client.pipeline(transaction=False)
                for key in candidates:
                    pipe.exists(key)
                exists_results = pipe.execute(raise_on_error=False)
            except Exception as e:
                logger.error("批量检查目标键失败: %s", e)
                return {'migrated': 0, 'failed': len(keys), 'skipped': 0}

            pending = []
            for key, exists in zip(candidates, exists_results):
                if isinstance(exists, Exception):
                    failed += 1
                elif exists:
                    skipped += 1
                else:
                    pending.append(key)
            candidates = pending

        if not candidates:
            return {'migrated': migrated, 'failed': failed, 'skipped': skipped}

        active_params = getattr(self, '_active_full_params', None) or {}
        key_types = active_params.get('key_types')
        key_filter = getattr(self, '_key_filter', None)
        min_ttl = key_filter.min_ttl if key_filter else 0
        max_key_size = key_filter.max_key_size if key_filter else 0
        needs_type = bool(key_types)
        needs_memory = max_key_size > 0
        has_dynamic_filters = bool(key_types or min_ttl > 0 or max_key_size > 0)

        try:
            # Keep payload, TTL, type and memory usage in one source state.
            has_pexpiretime = preserve_ttl and source_supports_pexpiretime(
                self.source_client, candidates[0]
            )
            pipe = self.source_client.pipeline(transaction=True)
            for key in candidates:
                pipe.dump(key)
                pipe.pttl(key)
                if has_pexpiretime:
                    pipe.execute_command('PEXPIRETIME', key)
                if needs_type:
                    pipe.type(key)
                if needs_memory:
                    pipe.execute_command('MEMORY', 'USAGE', key)
            pttl_sample_started_ns = time.monotonic_ns()
            results = pipe.execute(raise_on_error=False)
            observed_at_ms = int(time.time() * 1000)
            elapsed_ms = (
                max(0, time.monotonic_ns() - pttl_sample_started_ns)
                + 999_999
            ) // 1_000_000
        except Exception as e:
            logger.error("DUMP/PTTL批量读取失败: %s", e)
            return {
                'migrated': migrated,
                'failed': failed + len(candidates),
                'skipped': skipped,
            }

        adjusted_operations = []
        stride = (
            2
            + int(has_pexpiretime)
            + int(needs_type)
            + int(needs_memory)
        )

        def add_filtered_deletion(key) -> None:
            nonlocal skipped
            if not overwrite_existing:
                skipped += 1
                return
            operation_name = 'filtered_delete' if has_dynamic_filters else 'delete'
            adjusted_operations.append((operation_name, key, None, None))

        for index, key in enumerate(candidates):
            base = index * stride
            dump_data = results[base]
            pttl = results[base + 1]
            next_index = base + 2
            pexpiretime = results[next_index] if has_pexpiretime else None
            next_index += int(has_pexpiretime)
            key_type = results[next_index] if needs_type else None
            next_index += int(needs_type)
            memory_size = results[next_index] if needs_memory else None
            # PEXPIRETIME may be denied even after a successful capability
            # probe; PTTL from the same EXEC remains a valid fallback.
            responses = (dump_data, pttl, key_type, memory_size)
            if any(isinstance(response, BaseException) for response in responses):
                failed += 1
                continue
            try:
                pttl_value = int(pttl)
            except (TypeError, ValueError):
                failed += 1
                continue
            if dump_data is None or pttl_value in (0, -2):
                add_filtered_deletion(key)
                continue
            if pttl_value < -2:
                failed += 1
                continue

            expires_at_ms = None
            remaining_ttl_ms = pttl_value
            if pttl_value > 0:
                try:
                    absolute_ms = int(pexpiretime)
                except (TypeError, ValueError):
                    absolute_ms = -1
                if preserve_ttl and absolute_ms > 0:
                    expires_at_ms = absolute_ms
                    remaining_ttl_ms = absolute_ms - observed_at_ms
                else:
                    remaining_ttl_ms = pttl_value - elapsed_ms
                    if preserve_ttl:
                        expires_at_ms = observed_at_ms + remaining_ttl_ms
                if remaining_ttl_ms <= 0:
                    add_filtered_deletion(key)
                    continue

            if not source_state_in_dynamic_scope(
                remaining_ttl_ms,
                key_type=key_type,
                memory_size=memory_size,
                key_types=key_types,
                min_ttl=min_ttl,
                max_key_size=max_key_size,
            ):
                add_filtered_deletion(key)
                continue
            adjusted_operations.append(
                ('restore', key, expires_at_ms, dump_data)
            )

        if not adjusted_operations:
            return {'migrated': migrated, 'failed': failed, 'skipped': skipped}

        try:
            pipe = self.target_client.pipeline(transaction=False)
            for operation, key, expires_at_ms, dump_data in adjusted_operations:
                if operation == 'delete':
                    pipe.delete(key)
                elif operation == 'filtered_delete':
                    pipe.execute_command(
                        *build_atomic_filtered_delete_command(
                            [key],
                            key_types=key_types,
                            min_ttl=min_ttl,
                            max_key_size=max_key_size,
                        )
                    )
                elif expires_at_ms is None:
                    pipe.restore(
                        key,
                        0,
                        dump_data,
                        replace=overwrite_existing,
                    )
                else:
                    pipe.restore(
                        key,
                        expires_at_ms,
                        dump_data,
                        replace=overwrite_existing,
                        absttl=True,
                    )
            write_results = pipe.execute(raise_on_error=False)
        except Exception as e:
            logger.error("DUMP/RESTORE批量写入失败: %s", e)
            failed += len(adjusted_operations)
        else:
            for operation, result in zip(adjusted_operations, write_results):
                operation_name, key, _expires_at_ms, _dump_data = operation
                if not isinstance(result, Exception):
                    migrated += 1
                    continue
                if (
                    not overwrite_existing
                    and isinstance(result, redis.ResponseError)
                    and _is_busykey_error(result)
                ):
                    skipped += 1
                    continue
                if (
                    operation_name == 'restore'
                    and isinstance(result, redis.ResponseError)
                    and _is_restore_compatibility_error(result)
                ):
                    if (
                        _expires_at_ms is not None
                        and _is_absttl_compatibility_error(result)
                    ):
                        try:
                            fallback_result = restore_dump_with_deadline(
                                self.target_client,
                                key,
                                _dump_data,
                                _expires_at_ms,
                                overwrite=overwrite_existing,
                                prefer_absttl=False,
                            )
                        except redis.ResponseError as legacy_error:
                            if not _is_restore_compatibility_error(legacy_error):
                                logger.error(
                                    "旧版目标 RESTORE 失败 key=%r: %s",
                                    key,
                                    legacy_error,
                                )
                                failed += 1
                                continue
                        except Exception as fallback_error:
                            logger.error(
                                "旧版目标 RESTORE 失败 key=%r: %s",
                                key,
                                fallback_error,
                            )
                            failed += 1
                            continue
                        else:
                            if fallback_result:
                                migrated += 1
                            else:
                                skipped += 1
                            continue
                    fallback_pttl = (
                        -1
                        if _expires_at_ms is None
                        else max(
                            1,
                            _expires_at_ms - int(time.time() * 1000),
                        )
                    )
                    try:
                        fallback_result = _sync_key_fallback(
                            self.source_client,
                            self.target_client,
                            key,
                            fallback_pttl,
                            preserve_ttl,
                            overwrite=overwrite_existing,
                            expires_at_ms=_expires_at_ms,
                            expected_dump=_dump_data,
                            key_types=key_types,
                            min_ttl=min_ttl,
                            max_key_size=max_key_size,
                        )
                    except Exception as fallback_error:
                        logger.error(
                            "RESTORE兼容降级失败 key=%r: %s",
                            key,
                            fallback_error,
                        )
                        failed += 1
                    else:
                        if fallback_result:
                            migrated += 1
                        else:
                            skipped += 1
                    continue
                failed += 1

        return {'migrated': migrated, 'failed': failed, 'skipped': skipped}

    def _scan_page(self, cursor: int, pattern: str, count: int):
        """读取一页 SCAN；持续故障在有限重试后终止迁移。"""
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
                        "SCAN失败，准备重试 (%s/%s): %s",
                        attempt,
                        self.SCAN_MAX_RETRIES,
                        e,
                    )
                    if self.SCAN_RETRY_DELAY > 0:
                        time.sleep(self.SCAN_RETRY_DELAY * attempt)
        raise MigrationError(
            f"SCAN连续失败 {self.SCAN_MAX_RETRIES} 次: {last_error}"
        ) from last_error
    
    def _estimate_key_count(self, pattern: str, key_types: Optional[List[str]]) -> int:
        """估算匹配的键数量。"""
        try:
            if pattern == "*" and not key_types:
                return self.source_client.dbsize()
            
            # 采样估算
            sample_size = 1000
            cursor = 0
            sampled_keys = 0
            matching_keys = 0
            
            while sampled_keys < sample_size:
                cursor, keys = self._scan_page(
                    cursor,
                    pattern,
                    min(100, sample_size - sampled_keys),
                )
                
                for key in keys:
                    sampled_keys += 1
                    
                    # 检查类型过滤
                    if key_types:
                        try:
                            key_type = self.source_client.type(key).decode()
                            if key_type in key_types:
                                matching_keys += 1
                        except Exception:
                            pass
                    else:
                        matching_keys += 1
                    
                    if sampled_keys >= sample_size:
                        break
                
                if sampled_keys >= sample_size:
                    break
                if cursor == 0:
                    break
            
            if sampled_keys == 0:
                return 0
            
            # 基于采样估算总数
            total_keys = self.source_client.dbsize()
            match_ratio = matching_keys / sampled_keys
            estimated_count = int(total_keys * match_ratio)
            
            logger.info(f"估算匹配键数: {estimated_count}")
            return estimated_count
            
        except MigrationError:
            raise
        except Exception as e:
            logger.error(f"估算键数失败: {e}")
            return 0
    
    def get_migration_progress(self) -> Dict[str, Any]:
        """获取迁移进度信息。"""
        if not self.migration_start_time:
            return {'status': 'not_started'}
        
        elapsed_time = time.time() - self.migration_start_time
        total_keys = self.migration_stats['total_keys']
        migrated_keys = self.migration_stats['migrated_keys']
        
        progress_percentage = (migrated_keys / max(total_keys, 1)) * 100
        
        return {
            'status': 'in_progress' if self.migration_stats['end_time'] is None else 'completed',
            'total_keys': total_keys,
            'migrated_keys': migrated_keys,
            'failed_keys': self.migration_stats['failed_keys'],
            'progress_percentage': progress_percentage,
            'elapsed_time': elapsed_time,
            'estimated_remaining_time': self._estimate_remaining_time(elapsed_time, migrated_keys, total_keys)
        }
    
    def _estimate_remaining_time(self, elapsed_time: float, completed: int, total: int) -> Optional[float]:
        """估算剩余时间。"""
        if completed <= 0 or elapsed_time <= 0:
            return None
        
        rate = completed / elapsed_time
        remaining = total - completed
        
        if rate <= 0:
            return None
        
        return remaining / rate
