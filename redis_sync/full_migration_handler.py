"""
全量迁移处理器

实现Redis的全量数据迁移，支持多种全量迁移策略。
包括基于SYNC的RDB快照迁移和基于SCAN的完整键迁移。
"""

import redis
import logging
import time
import threading
from typing import Optional, Callable, Dict, Any, List
from datetime import datetime

from .exceptions import MigrationError, SyncError
from .key_sync import (
    CapturedDumpState,
    MAX_DUMP_BATCH_BYTES,
    _execute_pipeline_allowing_errors,
    _is_absttl_compatibility_error,
    _is_busykey_error,
    _is_restore_compatibility_error,
    _sync_key_fallback,
    restore_dump_with_deadline,
)
from .sync_filters import (
    KeySyncFilter,
    build_atomic_filtered_delete_command,
    redis_glob_match,
    source_state_in_dynamic_scope,
)
from .utils import ProgressTracker, format_bytes, format_duration

logger = logging.getLogger(__name__)
MAX_PIPELINE_KEYS = 200


ATOMIC_SOURCE_CAPTURE_LUA = b"""
local preserve_ttl = tonumber(ARGV[1]) == 1
local min_ttl_ms = tonumber(ARGV[2]) or 0
local max_key_size = tonumber(ARGV[3]) or 0
local batch_budget = tonumber(ARGV[4]) or 0
local type_count = tonumber(ARGV[5]) or 0
local now_ms = -1
if preserve_ttl then
    local now = redis.pcall('TIME')
    if type(now) == 'table' and not now['err'] and #now >= 2 then
        now_ms = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
    end
end
local captured_payload_bytes = 0
local result = {}

for _, key in ipairs(KEYS) do
    local pttl = redis.call('PTTL', key)
    if pttl == -2 or pttl == 0 then
        result[#result + 1] = {0}
    else
        local allowed = true
        local key_type = ''
        if type_count > 0 then
            local type_reply = redis.call('TYPE', key)
            key_type = type_reply
            if type(type_reply) == 'table' then
                key_type = type_reply['ok']
            end
            allowed = false
            for index = 1, type_count do
                if key_type == ARGV[5 + index] then
                    allowed = true
                    break
                end
            end
        end

        if allowed and min_ttl_ms > 0 and pttl > 0 and pttl < min_ttl_ms then
            allowed = false
        end

        local memory_reply = redis.pcall('MEMORY', 'USAGE', key)
        local memory_error = type(memory_reply) == 'table' and memory_reply['err']
        local memory_size = -1
        if not memory_error and memory_reply then
            memory_size = tonumber(memory_reply)
        end

        if memory_error then
            result[#result + 1] = {4, tostring(memory_error)}
        elseif not memory_error and not memory_reply then
            result[#result + 1] = {0}
        elseif allowed and max_key_size > 0 and memory_size > max_key_size then
            result[#result + 1] = {1}
        elseif not allowed then
            result[#result + 1] = {1}
        elseif memory_size > 0 and batch_budget > 0 and captured_payload_bytes > 0
                and captured_payload_bytes + memory_size > batch_budget then
            result[#result + 1] = {3}
        else
            local dump_data = redis.call('DUMP', key)
            if not dump_data then
                result[#result + 1] = {0}
            elseif batch_budget > 0 and captured_payload_bytes > 0
                    and captured_payload_bytes + string.len(dump_data) > batch_budget then
                result[#result + 1] = {3}
            else
                local expires_at_ms = -1
                if preserve_ttl and pttl > 0 and now_ms > 0 then
                    expires_at_ms = now_ms + pttl
                end
                captured_payload_bytes = captured_payload_bytes + string.len(dump_data)
                result[#result + 1] = {
                    2, dump_data, pttl, expires_at_ms, key_type, memory_size
                }
            end
        end
    end
end

return result
""".strip()


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
    
    def __init__(
        self,
        source_client: redis.Redis,
        target_client: redis.Redis,
        stop_event: Optional[threading.Event] = None,
    ):
        """
        初始化全量迁移处理器。
        
        参数:
            source_client: 源Redis客户端
            target_client: 目标Redis客户端
        """
        self.source_client = source_client
        self.target_client = target_client
        self.stop_event = stop_event
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

    def _raise_if_cancelled(self) -> None:
        if self.stop_event is not None and self.stop_event.is_set():
            raise MigrationError("全量迁移已取消")

    def _filter_keys_by_types(
        self,
        keys: List[bytes],
        key_types: List[str],
        batch_size: int,
    ) -> List[bytes]:
        filtered_keys = []
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        for offset in range(0, len(keys), pipeline_batch_size):
            self._raise_if_cancelled()
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
                self._raise_if_cancelled()
                if strategy not in self.SUPPORTED_STRATEGIES:
                    raise MigrationError(f"不支持的迁移策略: {strategy}")

                # 清空目标数据库（如果需要）
                if clear_target:
                    self._raise_if_cancelled()
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
        total_keys = self._estimate_key_count(
            key_pattern,
            key_types,
            getattr(self, "_key_filter", None),
        )
        self.migration_stats['total_keys'] = total_keys
        
        progress_tracker = ProgressTracker(total_keys, "SCAN全量迁移")
        migrated_count = 0
        failed_count = 0
        skipped_count = 0
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        
        cursor = 0
        while True:
            self._raise_if_cancelled()
            # 扫描键（使用scan_count参数）
            cursor, keys = self._scan_page(cursor, key_pattern, scan_count)
            self._raise_if_cancelled()
                
            if keys:
                if getattr(self, "_key_filter", None):
                    keys = self._key_filter.filter_names(list(keys))

                # 迁移这批键
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for offset in range(0, len(keys), pipeline_batch_size):
                    self._raise_if_cancelled()
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
        total_keys = self._estimate_key_count(
            key_pattern,
            key_types,
            getattr(self, "_key_filter", None),
        )
        self.migration_stats['total_keys'] = total_keys
        
        progress_tracker = ProgressTracker(total_keys, "DUMP/RESTORE全量迁移")
        migrated_count = 0
        failed_count = 0
        skipped_count = 0
        pipeline_batch_size = _bounded_pipeline_batch_size(batch_size)
        
        cursor = 0
        while True:
            self._raise_if_cancelled()
            # 扫描键（使用scan_count参数）
            cursor, keys = self._scan_page(cursor, key_pattern, scan_count)
            self._raise_if_cancelled()
                
            if keys:
                if getattr(self, "_key_filter", None):
                    keys = self._key_filter.filter_names(list(keys))

                # 使用DUMP/RESTORE迁移这批键
                if not keys:
                    if cursor == 0:
                        break
                    continue
                for offset in range(0, len(keys), pipeline_batch_size):
                    self._raise_if_cancelled()
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
            self._raise_if_cancelled()
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
        """Capture pre-sized key groups and write bounded RESTORE pipelines."""
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
            for index, key in enumerate(candidates):
                if index >= len(exists_results):
                    failed += 1
                    continue
                exists = exists_results[index]
                if isinstance(exists, BaseException):
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
        has_dynamic_filters = bool(key_types or min_ttl > 0 or max_key_size > 0)

        adjusted_operations = []
        queued_payload_bytes = 0

        def flush_operations() -> None:
            nonlocal adjusted_operations
            nonlocal queued_payload_bytes
            nonlocal migrated
            nonlocal failed
            nonlocal skipped
            if not adjusted_operations:
                return
            self._raise_if_cancelled()
            result = self._write_dump_operations(
                adjusted_operations,
                preserve_ttl=preserve_ttl,
                overwrite_existing=overwrite_existing,
                key_types=key_types,
                min_ttl=min_ttl,
                max_key_size=max_key_size,
            )
            migrated += result['migrated']
            failed += result['failed']
            skipped += result['skipped']
            adjusted_operations = []
            queued_payload_bytes = 0

        def queue_operation(operation) -> None:
            nonlocal queued_payload_bytes
            payload = operation[3]
            payload_size = len(payload) if payload is not None else 0
            if adjusted_operations and (
                len(adjusted_operations) >= MAX_PIPELINE_KEYS
                or queued_payload_bytes + payload_size > MAX_DUMP_BATCH_BYTES
            ):
                flush_operations()
            adjusted_operations.append(operation)
            queued_payload_bytes += payload_size
            if (
                len(adjusted_operations) >= MAX_PIPELINE_KEYS
                or queued_payload_bytes >= MAX_DUMP_BATCH_BYTES
            ):
                flush_operations()

        def add_filtered_deletion(key) -> None:
            nonlocal skipped
            if not overwrite_existing:
                skipped += 1
                return
            operation_name = 'filtered_delete' if has_dynamic_filters else 'delete'
            queue_operation((operation_name, key, None, None, False))

        def consume_captured_batch(batch) -> None:
            nonlocal failed
            for key, captured in batch:
                if isinstance(captured, BaseException):
                    logger.error("DUMP/PTTL读取失败 key=%r: %s", key, captured)
                    failed += 1
                    continue
                if captured is None:
                    add_filtered_deletion(key)
                    continue
                queue_operation(
                    (
                        'restore',
                        key,
                        captured.expires_at_ms,
                        captured.dump_data,
                        captured.expiry_is_exact,
                    )
                )

        group = candidates
        self._raise_if_cancelled()
        capture_batches = self._capture_dump_group(
            group,
            preserve_ttl=preserve_ttl,
            key_types=key_types,
            min_ttl=min_ttl,
            max_key_size=max_key_size,
        )
        captured_count = 0
        while True:
            try:
                captured_batch = next(capture_batches)
            except StopIteration:
                break
            except Exception as error:
                logger.error(
                    "DUMP/PTTL批量读取失败 keys=%d: %s",
                    len(group) - captured_count,
                    error,
                )
                failed += max(0, len(group) - captured_count)
                break

            captured_count += len(captured_batch)
            consume_captured_batch(captured_batch)
            # Release this EVAL response before requesting the deferred round.
            flush_operations()
            del captured_batch

        self._raise_if_cancelled()
        flush_operations()
        return {'migrated': migrated, 'failed': failed, 'skipped': skipped}

    def _capture_dump_group(
        self,
        keys: List[bytes],
        *,
        preserve_ttl: bool,
        key_types: Optional[List[str]],
        min_ttl: int,
        max_key_size: int,
    ):
        """Yield each atomically captured round before requesting deferred keys."""
        allowed_types = list(key_types or [])
        pending = list(keys)

        while pending:
            self._raise_if_cancelled()
            pipe = self.source_client.pipeline(transaction=False)
            pipe.execute_command(
                'EVAL',
                ATOMIC_SOURCE_CAPTURE_LUA,
                len(pending),
                *pending,
                int(bool(preserve_ttl)),
                max(0, int(min_ttl)) * 1000,
                max(0, int(max_key_size)),
                MAX_DUMP_BATCH_BYTES,
                len(allowed_types),
                *allowed_types,
            )
            sample_started_ns = time.monotonic_ns()
            raw = _execute_pipeline_allowing_errors(pipe)
            elapsed_ms = (
                max(0, time.monotonic_ns() - sample_started_ns) + 999_999
            ) // 1_000_000

            if len(raw) != 1:
                raise RuntimeError(
                    f"source capture response count mismatch: {len(raw)} != 1"
                )
            replies = raw[0]
            if isinstance(replies, BaseException):
                raise replies
            if not isinstance(replies, (list, tuple)):
                raise RuntimeError(
                    f"invalid source capture response: {replies!r}"
                )
            if len(replies) != len(pending):
                raise RuntimeError(
                    "source capture key count mismatch: "
                    f"{len(replies)} != {len(pending)}"
                )

            deferred = []
            captured_batch = []
            dump_data = None
            reply = None
            for key, reply in zip(pending, replies):
                if not isinstance(reply, (list, tuple)) or not reply:
                    captured_batch.append(
                        (
                            key,
                            RuntimeError(
                                f"invalid source capture row for {key!r}: {reply!r}"
                            ),
                        )
                    )
                    continue
                try:
                    status = int(reply[0])
                except (TypeError, ValueError):
                    captured_batch.append(
                        (
                            key,
                            RuntimeError(
                                f"invalid source capture status for {key!r}: {reply!r}"
                            ),
                        )
                    )
                    continue

                if status == 3:
                    deferred.append(key)
                    continue
                if status == 4:
                    detail = reply[1] if len(reply) > 1 else "MEMORY USAGE failed"
                    captured_batch.append((key, RuntimeError(str(detail))))
                    continue
                if status in (0, 1):
                    captured_batch.append((key, None))
                    continue
                if status != 2 or len(reply) != 6:
                    captured_batch.append(
                        (
                            key,
                            RuntimeError(
                                f"invalid source capture row for {key!r}: {reply!r}"
                            ),
                        )
                    )
                    continue

                dump_data, pttl, expires_at, key_type, memory_size = reply[1:]
                try:
                    ttl_ms = int(pttl)
                    absolute_ms = int(expires_at)
                    memory_bytes = int(memory_size)
                except (TypeError, ValueError):
                    captured_batch.append(
                        (
                            key,
                            ValueError(
                                f"invalid source capture values for {key!r}: {reply!r}"
                            ),
                        )
                    )
                    continue
                remaining_ttl_ms = ttl_ms
                if ttl_ms > 0:
                    remaining_ttl_ms -= elapsed_ms
                    if remaining_ttl_ms <= 0:
                        captured_batch.append((key, None))
                        continue
                if not source_state_in_dynamic_scope(
                    remaining_ttl_ms,
                    key_type=key_type,
                    memory_size=memory_bytes,
                    key_types=allowed_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                ):
                    captured_batch.append((key, None))
                    continue
                expires_at_ms = None
                expiry_is_exact = False
                if preserve_ttl and ttl_ms > 0:
                    if absolute_ms > 0:
                        expires_at_ms = absolute_ms
                        expiry_is_exact = True
                    else:
                        expires_at_ms = (
                            int(time.time() * 1000) + remaining_ttl_ms
                        )
                captured_memory = (
                    memory_bytes if memory_bytes >= 0 else None
                )
                captured_batch.append(
                    (
                        key,
                        CapturedDumpState(
                            dump_data=dump_data,
                            pttl_ms=ttl_ms,
                            expires_at_ms=expires_at_ms,
                            remaining_ttl_ms=remaining_ttl_ms,
                            key_type=key_type or None,
                            memory_size=captured_memory,
                            expiry_is_exact=expiry_is_exact,
                        ),
                    )
                )

            if deferred and not captured_batch:
                raise RuntimeError("source capture made no progress")
            del raw, replies, reply, dump_data
            yield captured_batch
            del captured_batch
            pending = deferred

    def _write_dump_operations(
        self,
        operations,
        *,
        preserve_ttl: bool,
        overwrite_existing: bool,
        key_types: Optional[List[str]],
        min_ttl: int,
        max_key_size: int,
    ) -> Dict[str, int]:
        """Write one payload-bounded target pipeline and handle fallbacks."""
        migrated = 0
        failed = 0
        skipped = 0

        try:
            pipe = self.target_client.pipeline(transaction=False)
            for (
                operation,
                key,
                expires_at_ms,
                dump_data,
                _expiry_is_exact,
            ) in operations:
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
            failed += len(operations)
        else:
            for index, operation in enumerate(operations):
                (
                    operation_name,
                    key,
                    _expires_at_ms,
                    _dump_data,
                    _expiry_is_exact,
                ) = operation
                if index >= len(write_results):
                    failed += 1
                    continue
                result = write_results[index]
                if not isinstance(result, BaseException):
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
                                expires_at_is_exact=_expiry_is_exact,
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
                            expires_at_is_exact=_expiry_is_exact,
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

    def _scan_page(self, cursor: int, pattern: Optional[str], count: int):
        """读取一页 SCAN；持续故障在有限重试后终止迁移。"""
        last_error = None
        for attempt in range(1, self.SCAN_MAX_RETRIES + 1):
            self._raise_if_cancelled()
            try:
                scan_args = {'cursor': cursor, 'count': count}
                if pattern is not None:
                    scan_args['match'] = pattern
                return self.source_client.scan(**scan_args)
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
                        delay = self.SCAN_RETRY_DELAY * attempt
                        if self.stop_event is not None:
                            if self.stop_event.wait(delay):
                                self._raise_if_cancelled()
                        else:
                            time.sleep(delay)
        raise MigrationError(
            f"SCAN连续失败 {self.SCAN_MAX_RETRIES} 次: {last_error}"
        ) from last_error
    
    def _estimate_key_count(
        self,
        pattern: str,
        key_types: Optional[List[str]],
        key_filter: Optional[KeySyncFilter] = None,
    ) -> int:
        """估算匹配的键数量。"""
        try:
            self._raise_if_cancelled()
            if pattern == "*" and not key_types and not key_filter:
                return self.source_client.dbsize()

            # SCAN MATCH only returns matching keys, so it cannot be used as
            # the denominator of a selectivity sample. Sample the whole DB and
            # apply all name filters locally instead.
            sample_size = 1000
            cursor = 0
            sampled_keys = []
            sampled_key_set = set()
            completed_scan = False

            while len(sampled_keys) < sample_size:
                self._raise_if_cancelled()
                cursor, keys = self._scan_page(
                    cursor,
                    None,
                    min(100, sample_size - len(sampled_keys)),
                )

                page_truncated = False
                for key_index, key in enumerate(keys):
                    if key in sampled_key_set:
                        continue
                    sampled_key_set.add(key)
                    sampled_keys.append(key)
                    if len(sampled_keys) >= sample_size:
                        page_truncated = key_index + 1 < len(keys)
                        break

                if len(sampled_keys) >= sample_size:
                    completed_scan = cursor == 0 and not page_truncated
                    break
                if cursor == 0:
                    completed_scan = True
                    break

            if not sampled_keys:
                return 0

            matching_keys = [
                key
                for key in sampled_keys
                if redis_glob_match(key, pattern)
            ]
            if key_filter and matching_keys:
                matching_keys = key_filter.filter_batch(
                    self.source_client,
                    matching_keys,
                )
            if key_types and matching_keys:
                matching_keys = self._filter_keys_by_types(
                    matching_keys,
                    key_types,
                    MAX_PIPELINE_KEYS,
                )

            if completed_scan:
                exact_count = len(matching_keys)
                logger.info("精确匹配键数: %s", exact_count)
                return exact_count

            # 基于采样估算总数
            total_keys = self.source_client.dbsize()
            match_ratio = len(matching_keys) / len(sampled_keys)
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
