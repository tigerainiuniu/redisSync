"""
键级同步：优先使用 DUMP/RESTORE 在目标端原子替换，避免 delete+写入 的中间空窗期。
"""

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Union

import redis

from .sync_filters import source_state_in_dynamic_scope

logger = logging.getLogger(__name__)

KeyType = Union[str, bytes]
_PEXPIRETIME_CACHE_ATTR = "_redis_sync_pexpiretime_supported"
MAX_DUMP_BATCH_BYTES = 16 * 1024 * 1024


class SourceStateChangedError(RuntimeError):
    """The source no longer matches the immutable state selected for delivery."""


@dataclass(frozen=True)
class CapturedDumpState:
    """One atomically captured source value after dynamic-scope preflight."""

    dump_data: Any
    pttl_ms: int
    expires_at_ms: Optional[int]
    remaining_ttl_ms: int
    key_type: Any = None
    memory_size: Any = None
    expiry_is_exact: bool = False


def _is_busykey_error(error: redis.ResponseError) -> bool:
    message = str(error).lower()
    return "busykey" in message or "target key name already exists" in message


def _is_restore_compatibility_error(error: redis.ResponseError) -> bool:
    """Return whether RESTORE failed because the target protocol is older."""
    message = str(error).lower()
    if "dump payload version or checksum are wrong" in message:
        return True
    if "syntax error" in message:
        return True
    return (
        "restore" in message
        and (
            "unknown command" in message
            or "wrong number of arguments" in message
            or "unsupported" in message
        )
    )


def _is_absttl_compatibility_error(error: redis.ResponseError) -> bool:
    """Return whether RESTORE failed because ABSTTL is not supported."""
    message = str(error).lower()
    return (
        "syntax error" in message
        or "wrong number of arguments" in message
        or "unknown argument" in message
        or "unsupported" in message
    )


def _is_missing_key_error(error: redis.ResponseError) -> bool:
    return "no such key" in str(error).lower()


def _temporary_key() -> bytes:
    return b"__redis_sync_tmp__:" + uuid.uuid4().hex.encode("ascii")


def expiry_deadline_from_pttl(
    pttl_ms: int,
    sample_started_ns: int,
) -> Optional[int]:
    """Convert a sampled relative TTL into a conservative Unix-ms deadline."""
    elapsed_ns = max(0, time.monotonic_ns() - sample_started_ns)
    remaining_ms = int(pttl_ms) - (elapsed_ns + 999_999) // 1_000_000
    if remaining_ms <= 0:
        return None
    return int(time.time() * 1000) + remaining_ms


def source_supports_pexpiretime(source: redis.Redis, probe_key: KeyType) -> bool:
    """Probe Redis 7 PEXPIRETIME outside MULTI and cache the result per client."""
    cached = getattr(source, _PEXPIRETIME_CACHE_ATTR, None)
    if type(cached) is bool:
        return cached
    try:
        response = source.execute_command("PEXPIRETIME", probe_key)
    except (AttributeError, redis.ResponseError):
        supported = False
    else:
        supported = type(response) is int
    cache_source_pexpiretime_support(source, supported)
    return supported


def cache_source_pexpiretime_support(source: redis.Redis, supported: bool) -> None:
    """Update the per-client capability cache without requiring mutable clients."""
    try:
        setattr(source, _PEXPIRETIME_CACHE_ATTR, bool(supported))
    except Exception:
        pass


def is_pexpiretime_capability_error(error: BaseException) -> bool:
    """Return whether a cached-positive PEXPIRETIME capability became invalid."""
    if isinstance(error, redis.exceptions.ExecAbortError):
        # PEXPIRETIME is the only optional command queued by these captures. A
        # retry without it will either succeed or expose any unrelated error.
        return True
    if not isinstance(error, redis.ResponseError):
        return False
    message = str(error).lower()
    return "pexpiretime" in message and any(
        marker in message
        for marker in (
            "unknown command",
            "noperm",
            "permission",
            "unsupported",
            "not allowed",
        )
    )


def _execute_pipeline_allowing_errors(pipe):
    """Execute redis-py pipelines while keeping lightweight test doubles usable."""
    try:
        return pipe.execute(raise_on_error=False)
    except TypeError as error:
        if "raise_on_error" not in str(error):
            raise
        return pipe.execute()


def _known_integer(value: Any) -> Optional[int]:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value


def _preflight_out_of_scope(
    pttl: Any,
    *,
    key_type: Any,
    memory_size: Any,
    key_types: Iterable[Union[str, bytes]],
    min_ttl: int,
    max_key_size: int,
) -> bool:
    """Evaluate only concrete WATCH replies; mocks/older clients fall through."""
    ttl_value = _known_integer(pttl)
    if ttl_value is not None:
        if ttl_value in (-2, 0):
            return True
        if ttl_value < -2:
            raise ValueError(f"invalid PTTL response: {ttl_value}")
        if min_ttl > 0 and 0 < ttl_value < int(min_ttl) * 1000:
            return True

    allowed_types = tuple(key_types or ())
    if allowed_types and isinstance(key_type, (str, bytes)):
        if not source_state_in_dynamic_scope(
            ttl_value if ttl_value is not None else -1,
            key_type=key_type,
            key_types=allowed_types,
        ):
            return True

    size_value = _known_integer(memory_size)
    if size_value is not None and max_key_size > 0:
        if size_value > int(max_key_size):
            return True
    return False


def capture_dump_with_preflight(
    source: redis.Redis,
    key: KeyType,
    *,
    preserve_ttl: bool = True,
    key_types: Optional[Iterable[Union[str, bytes]]] = None,
    min_ttl: int = 0,
    max_key_size: int = 0,
    max_watch_retries: int = 3,
    _pexpiretime_override: Optional[bool] = None,
) -> Optional[CapturedDumpState]:
    """Capture one DUMP after a WATCH-protected size/scope preflight.

    When ``max_key_size`` is active, a key already above the limit is rejected
    before Redis is asked to serialize it.  TYPE, TTL and MEMORY are sampled
    again in EXEC, so a concurrent change either aborts WATCH or is rejected by
    the second scope check.
    """

    allowed_key_types = tuple(key_types or ())
    needs_type = bool(allowed_key_types)
    needs_memory = int(max_key_size) > 0
    has_pexpiretime = preserve_ttl and (
        source_supports_pexpiretime(source, key)
        if _pexpiretime_override is None
        else bool(_pexpiretime_override)
    )

    for attempt in range(max(1, int(max_watch_retries))):
        pipe = source.pipeline(transaction=True)
        watch_supported = needs_memory and callable(getattr(pipe, "watch", None))
        try:
            if watch_supported:
                pipe.watch(key)
                preflight_pttl = pipe.pttl(key)
                preflight_type = pipe.type(key) if needs_type else None
                preflight_memory = pipe.execute_command("MEMORY", "USAGE", key)
                if _preflight_out_of_scope(
                    preflight_pttl,
                    key_type=preflight_type,
                    memory_size=preflight_memory,
                    key_types=allowed_key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                ):
                    return None
                pipe.multi()

            pipe.dump(key)
            pipe.pttl(key)
            if has_pexpiretime:
                pipe.execute_command("PEXPIRETIME", key)
            if needs_type:
                pipe.type(key)
            if needs_memory:
                pipe.execute_command("MEMORY", "USAGE", key)
            sample_started_ns = time.monotonic_ns()
            raw = _execute_pipeline_allowing_errors(pipe)
        except redis.WatchError:
            if attempt + 1 >= max(1, int(max_watch_retries)):
                raise SourceStateChangedError(
                    f"source key changed during capture: {key!r}"
                )
            continue
        except Exception as error:
            if has_pexpiretime and is_pexpiretime_capability_error(error):
                cache_source_pexpiretime_support(source, False)
                return capture_dump_with_preflight(
                    source,
                    key,
                    preserve_ttl=preserve_ttl,
                    key_types=allowed_key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                    max_watch_retries=max_watch_retries,
                    _pexpiretime_override=False,
                )
            raise
        finally:
            reset = getattr(pipe, "reset", None)
            if callable(reset):
                try:
                    reset()
                except Exception:
                    pass

        expected = 2 + int(has_pexpiretime) + int(needs_type) + int(needs_memory)
        if len(raw) != expected:
            raise RuntimeError(
                f"source capture response count mismatch: {len(raw)} != {expected}"
            )
        response_index = 0
        dump_data = raw[response_index]
        response_index += 1
        pttl = raw[response_index]
        response_index += 1
        pexpiretime = raw[response_index] if has_pexpiretime else None
        response_index += int(has_pexpiretime)
        key_type = raw[response_index] if needs_type else None
        response_index += int(needs_type)
        memory_size = raw[response_index] if needs_memory else None

        if isinstance(pexpiretime, BaseException):
            if isinstance(pexpiretime, redis.ResponseError):
                cache_source_pexpiretime_support(source, False)
                return capture_dump_with_preflight(
                    source,
                    key,
                    preserve_ttl=preserve_ttl,
                    key_types=allowed_key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                    max_watch_retries=max_watch_retries,
                    _pexpiretime_override=False,
                )
            raise pexpiretime

        for value in (dump_data, pttl, key_type, memory_size):
            if isinstance(value, BaseException):
                raise value
        if dump_data is None:
            return None
        try:
            ttl_ms = int(pttl)
        except (TypeError, ValueError) as error:
            raise ValueError(f"invalid PTTL response: {pttl!r}") from error
        if ttl_ms in (-2, 0):
            return None
        if ttl_ms < -2:
            raise ValueError(f"invalid PTTL response: {ttl_ms}")
        if needs_memory and memory_size is None:
            raise RuntimeError(f"MEMORY USAGE returned null for existing key {key!r}")

        observed_at_ms = int(time.time() * 1000)
        expires_at_ms = None
        expiry_is_exact = False
        remaining_ttl_ms = ttl_ms
        if ttl_ms > 0:
            try:
                absolute_ms = int(pexpiretime)
            except (TypeError, ValueError):
                absolute_ms = -1
            elapsed_ms = (
                max(0, time.monotonic_ns() - sample_started_ns) + 999_999
            ) // 1_000_000
            remaining_ttl_ms = ttl_ms - elapsed_ms
            if remaining_ttl_ms <= 0:
                return None
            if preserve_ttl and absolute_ms > 0:
                expires_at_ms = absolute_ms
                expiry_is_exact = True
            else:
                if preserve_ttl:
                    expires_at_ms = observed_at_ms + remaining_ttl_ms

        if not source_state_in_dynamic_scope(
            remaining_ttl_ms,
            key_type=key_type,
            memory_size=memory_size,
            key_types=allowed_key_types,
            min_ttl=min_ttl,
            max_key_size=max_key_size,
        ):
            return None
        return CapturedDumpState(
            dump_data=dump_data,
            pttl_ms=ttl_ms,
            expires_at_ms=expires_at_ms,
            remaining_ttl_ms=remaining_ttl_ms,
            key_type=key_type,
            memory_size=memory_size,
            expiry_is_exact=expiry_is_exact,
        )

    raise SourceStateChangedError(f"source key capture did not complete: {key!r}")


def _restore_dump_via_temporary_key(
    target: redis.Redis,
    key: KeyType,
    dump_data: Any,
    expires_at_ms: int,
    *,
    overwrite: bool,
    expires_at_is_exact: bool,
) -> bool:
    """Restore for Redis < 5 without exposing or mutating a partial value."""
    temporary_key = _temporary_key()
    temporary_may_exist = False
    try:
        target.restore(temporary_key, 0, dump_data, replace=False)
        temporary_may_exist = True

        if not target.pexpireat(temporary_key, int(expires_at_ms)):
            if (
                expires_at_is_exact
                or expires_at_ms > int(time.time() * 1000)
            ):
                raise RuntimeError(
                    f"RESTORE 临时键在设置过期时间前消失: {temporary_key!r}"
                )
            if overwrite:
                target.delete(key)
                return True
            return False

        try:
            if overwrite:
                target.rename(temporary_key, key)
            elif not target.renamenx(temporary_key, key):
                return False
        except redis.ResponseError as error:
            # The temporary key can expire between PEXPIREAT and RENAME.
            if not _is_missing_key_error(error):
                raise
            if (
                not expires_at_is_exact
                and expires_at_ms > int(time.time() * 1000)
            ):
                raise
            if overwrite:
                target.delete(key)
                return True
            return False

        temporary_may_exist = False
        return True
    finally:
        if temporary_may_exist:
            try:
                target.delete(temporary_key)
            except Exception as cleanup_error:
                logger.warning(
                    "清理 RESTORE 临时键失败 key=%r temp=%r: %s",
                    key,
                    temporary_key,
                    cleanup_error,
                )


def restore_dump_with_deadline(
    target: redis.Redis,
    key: KeyType,
    dump_data: Any,
    expires_at_ms: Optional[int],
    *,
    overwrite: bool = True,
    prefer_absttl: bool = True,
    expires_at_is_exact: bool = False,
) -> bool:
    """Restore a DUMP payload without adding target-bound network delay to TTL."""
    if expires_at_ms is None:
        try:
            target.restore(key, 0, dump_data, replace=overwrite)
        except redis.ResponseError as error:
            if not overwrite and _is_busykey_error(error):
                return False
            raise
        return True

    expires_at_ms = int(expires_at_ms)
    if prefer_absttl:
        try:
            target.restore(
                key,
                expires_at_ms,
                dump_data,
                replace=overwrite,
                absttl=True,
            )
            return True
        except redis.ResponseError as error:
            if not overwrite and _is_busykey_error(error):
                return False
            if not _is_absttl_compatibility_error(error):
                raise

    return _restore_dump_via_temporary_key(
        target,
        key,
        dump_data,
        expires_at_ms,
        overwrite=overwrite,
        expires_at_is_exact=expires_at_is_exact,
    )


def redis_values_equal(
    source: redis.Redis,
    target: redis.Redis,
    key: KeyType,
    *,
    key_type: Any = None,
) -> bool:
    """Compare a Redis value across versions without trusting DUMP encoding."""
    if source.dump(key) == target.dump(key):
        return True

    if key_type is None:
        source_type = source.type(key)
        target_type = target.type(key)
        source_type = (
            source_type.decode("ascii", errors="replace")
            if isinstance(source_type, bytes)
            else str(source_type)
        )
        target_type = (
            target_type.decode("ascii", errors="replace")
            if isinstance(target_type, bytes)
            else str(target_type)
        )
        if source_type != target_type:
            return False
        key_type = source_type
    elif isinstance(key_type, bytes):
        key_type = key_type.decode("ascii", errors="replace")
    else:
        key_type = str(key_type)

    if key_type == "string":
        return source.get(key) == target.get(key)
    if key_type == "list":
        return source.lrange(key, 0, -1) == target.lrange(key, 0, -1)
    if key_type == "set":
        return source.smembers(key) == target.smembers(key)
    if key_type == "zset":
        return source.zrange(key, 0, -1, withscores=True) == target.zrange(
            key, 0, -1, withscores=True
        )
    if key_type == "hash":
        return source.hgetall(key) == target.hgetall(key)

    # Stream DUMP data also contains consumer group and pending-entry metadata;
    # module values have no portable type-level representation.
    return False


def sync_key_with_dump_restore(
    source: redis.Redis,
    target: redis.Redis,
    key: KeyType,
    *,
    overwrite: bool = True,
    preserve_ttl: bool = True,
    key_types: Optional[Iterable[Union[str, bytes]]] = None,
    min_ttl: int = 0,
    max_key_size: int = 0,
) -> bool:
    """
    使用 DUMP + RESTORE(replace=True) 同步单个键。

    若源端键不存在，则删除目标端同名键。

    返回:
        True 表示已迁移或已删除目标键；False 表示跳过（目标已存在且 overwrite=False）。
    """
    try:
        if not source.exists(key):
            if overwrite:
                target.delete(key)
                return True
            return False

        if not overwrite and target.exists(key):
            return False

        allowed_key_types = tuple(key_types or ())
        needs_type = bool(allowed_key_types)
        captured_state = None
        if int(max_key_size) > 0:
            captured_state = capture_dump_with_preflight(
                source,
                key,
                preserve_ttl=preserve_ttl,
                key_types=allowed_key_types,
                min_ttl=min_ttl,
                max_key_size=max_key_size,
            )
            if captured_state is None:
                if not source.exists(key):
                    if overwrite:
                        target.delete(key)
                        return True
                return False
            dump_data = captured_state.dump_data
            pttl = captured_state.remaining_ttl_ms
            pexpiretime = captured_state.expires_at_ms
            key_type = captured_state.key_type
            memory_size = captured_state.memory_size
            pttl_sample_started_ns = None
        else:
            # MULTI/EXEC keeps payload, TTL and type in one source state. Retry
            # once without Redis 7's optional PEXPIRETIME if a cached-positive
            # capability becomes invalid after a restart or ACL change.
            has_pexpiretime = preserve_ttl and source_supports_pexpiretime(
                source, key
            )
            while True:
                pipe = source.pipeline(transaction=True)
                try:
                    pipe.dump(key)
                    pipe.pttl(key)
                    if has_pexpiretime:
                        pipe.execute_command("PEXPIRETIME", key)
                    if needs_type:
                        pipe.type(key)
                    pttl_sample_started_ns = time.monotonic_ns()
                    results = _execute_pipeline_allowing_errors(pipe)
                except Exception as error:
                    if has_pexpiretime and is_pexpiretime_capability_error(error):
                        cache_source_pexpiretime_support(source, False)
                        has_pexpiretime = False
                        continue
                    raise
                finally:
                    reset = getattr(pipe, "reset", None)
                    if callable(reset):
                        try:
                            reset()
                        except Exception:
                            pass

                expected = 2 + int(has_pexpiretime) + int(needs_type)
                if len(results) != expected:
                    raise RuntimeError(
                        "source capture response count mismatch: "
                        f"{len(results)} != {expected}"
                    )
                result_index = 0
                dump_data = results[result_index]
                result_index += 1
                pttl = results[result_index]
                result_index += 1
                pexpiretime = (
                    results[result_index] if has_pexpiretime else None
                )
                result_index += int(has_pexpiretime)
                key_type = results[result_index] if needs_type else None
                memory_size = None
                if isinstance(pexpiretime, BaseException):
                    if has_pexpiretime and isinstance(
                        pexpiretime, redis.ResponseError
                    ):
                        cache_source_pexpiretime_support(source, False)
                        has_pexpiretime = False
                        continue
                    raise pexpiretime
                break

        for value in (dump_data, pttl, key_type, memory_size):
            if isinstance(value, BaseException):
                raise value

        if dump_data is None:
            if overwrite:
                target.delete(key)
                return True
            return False

        try:
            p = int(pttl)
        except (TypeError, ValueError) as e:
            raise ValueError(f"无效的 PTTL 响应: {pttl!r}") from e

        if captured_state is None and p > 0:
            elapsed_ms = (
                max(0, time.monotonic_ns() - pttl_sample_started_ns) + 999_999
            ) // 1_000_000
            p = max(0, p - elapsed_ms)

        # DUMP 成功后键仍可能在 PTTL 前到期。0/-2 不能映射为 RESTORE 的
        # TTL=0，否则会把已到期键恢复成永久键。
        if p in (0, -2):
            if overwrite:
                target.delete(key)
                return True
            return False
        if p < -2:
            raise ValueError(f"无效的 PTTL 响应: {p}")
        if not source_state_in_dynamic_scope(
            p,
            key_type=key_type,
            memory_size=memory_size,
            key_types=allowed_key_types,
            min_ttl=min_ttl,
            max_key_size=max_key_size,
        ):
            return False

        expires_at_ms = None
        expires_at_is_exact = False
        if preserve_ttl and p > 0:
            if captured_state is not None:
                expires_at_ms = captured_state.expires_at_ms
                expires_at_is_exact = captured_state.expiry_is_exact
            else:
                try:
                    absolute_ms = int(pexpiretime)
                except (TypeError, ValueError):
                    absolute_ms = -1
                if absolute_ms > 0:
                    expires_at_ms = absolute_ms
                    expires_at_is_exact = True
                else:
                    expires_at_ms = int(time.time() * 1000) + p
            if expires_at_ms is None:
                if overwrite:
                    target.delete(key)
                    return True
                return False
        try:
            return restore_dump_with_deadline(
                target,
                key,
                dump_data,
                expires_at_ms,
                overwrite=overwrite,
                expires_at_is_exact=expires_at_is_exact,
            )
        except redis.ResponseError as e:
            if not _is_restore_compatibility_error(e):
                raise
            logger.warning(
                "RESTORE 兼容性错误，回退到类型级复制: key=%r err=%s",
                key,
                e,
            )
            return _sync_key_fallback(
                source,
                target,
                key,
                pttl,
                preserve_ttl,
                overwrite=overwrite,
                expires_at_ms=expires_at_ms,
                expected_dump=dump_data,
                key_types=allowed_key_types,
                min_ttl=min_ttl,
                max_key_size=max_key_size,
                expires_at_is_exact=expires_at_is_exact,
            )

    except Exception as e:
        logger.error("键同步失败 key=%r: %s", key, e)
        raise


def _sync_key_fallback(
    source: redis.Redis,
    target: redis.Redis,
    key: KeyType,
    pttl: Any,
    preserve_ttl: bool = True,
    *,
    overwrite: bool = True,
    expires_at_ms: Optional[int] = None,
    pttl_sample_started_ns: Any = None,
    expected_dump: Any = None,
    key_types: Optional[Iterable[Union[str, bytes]]] = None,
    min_ttl: int = 0,
    max_key_size: int = 0,
    expires_at_is_exact: bool = False,
) -> bool:
    """在临时键上完成兼容复制，再原子替换正式键。

    类型级读取由 WATCH 保护，并在 EXEC 中重新采样 DUMP、TTL 和动态
    过滤字段。这样写入临时键的值一定对应调用方捕获的 DUMP 状态。
    """

    if preserve_ttl and pttl is not None:
        p = int(pttl)
        if p > 0:
            if expires_at_ms is None:
                if pttl_sample_started_ns is None:
                    expires_at_ms = int(time.time() * 1000) + p
                else:
                    expires_at_ms = expiry_deadline_from_pttl(
                        p,
                        pttl_sample_started_ns,
                    )
            if expires_at_ms is None:
                if overwrite:
                    target.delete(key)
                    return True
                return False
        elif p == -1:
            expires_at_ms = None
        elif p in (0, -2):
            if overwrite:
                target.delete(key)
                return True
            return False
        else:
            raise ValueError(f"无效的 PTTL 响应: {p}")

    allowed_key_types = tuple(key_types or ())
    needs_type = bool(allowed_key_types)
    needs_memory = int(max_key_size) > 0
    has_pexpiretime = preserve_ttl and source_supports_pexpiretime(source, key)
    captured = None
    watch_failures = 0

    while watch_failures < 3:
        pipe = source.pipeline()
        try:
            pipe.watch(key)
            key_type_b = pipe.type(key)
            key_type = (
                key_type_b.decode("ascii", errors="replace")
                if isinstance(key_type_b, bytes)
                else str(key_type_b)
            )

            if needs_memory:
                preflight_pttl = pipe.pttl(key)
                preflight_memory = pipe.execute_command("MEMORY", "USAGE", key)
                for response in (preflight_pttl, preflight_memory):
                    if isinstance(response, BaseException):
                        raise response
                if _preflight_out_of_scope(
                    preflight_pttl,
                    key_type=key_type_b,
                    memory_size=preflight_memory,
                    key_types=allowed_key_types,
                    min_ttl=min_ttl,
                    max_key_size=max_key_size,
                ):
                    raise SourceStateChangedError(
                        f"源键在兼容降级读取前移出过滤范围: {key!r}"
                    )

            if key_type == "string":
                value = pipe.get(key)
                values = None
            elif key_type == "list":
                value = None
                values = pipe.lrange(key, 0, -1)
            elif key_type == "set":
                value = None
                values = pipe.smembers(key)
            elif key_type == "zset":
                value = None
                values = pipe.zrange(key, 0, -1, withscores=True)
            elif key_type == "hash":
                value = None
                values = pipe.hgetall(key)
            elif key_type == "stream":
                raise RuntimeError(
                    "RESTORE 失败后不对 stream 执行降级复制，"
                    "以免丢失 consumer group/PEL 元数据"
                )
            elif key_type == "none":
                raise SourceStateChangedError(
                    f"源键在兼容降级前消失: {key!r}"
                )
            else:
                raise RuntimeError(
                    f"RESTORE 失败且键类型 {key_type!r} 不支持完整降级复制"
                )

            pipe.multi()
            pipe.dump(key)
            pipe.pttl(key)
            if has_pexpiretime:
                pipe.execute_command("PEXPIRETIME", key)
            if needs_type:
                pipe.type(key)
            if needs_memory:
                pipe.execute_command("MEMORY", "USAGE", key)
            validation_started_ns = time.monotonic_ns()
            raw = _execute_pipeline_allowing_errors(pipe)
        except redis.WatchError:
            watch_failures += 1
            if watch_failures >= 3:
                raise SourceStateChangedError(
                    f"源键在兼容降级读取期间持续变化: {key!r}"
                )
            continue
        except Exception as error:
            if has_pexpiretime and is_pexpiretime_capability_error(error):
                cache_source_pexpiretime_support(source, False)
                has_pexpiretime = False
                continue
            raise
        finally:
            try:
                pipe.reset()
            except Exception:
                pass

        result_index = 0
        current_dump = raw[result_index]
        result_index += 1
        current_pttl = raw[result_index]
        result_index += 1
        current_pexpiretime = raw[result_index] if has_pexpiretime else None
        result_index += int(has_pexpiretime)
        current_type = raw[result_index] if needs_type else key_type_b
        result_index += int(needs_type)
        current_memory = raw[result_index] if needs_memory else None
        if isinstance(current_pexpiretime, BaseException):
            if has_pexpiretime and isinstance(
                current_pexpiretime, redis.ResponseError
            ):
                cache_source_pexpiretime_support(source, False)
                has_pexpiretime = False
                continue
            raise current_pexpiretime
        for response in (
            current_dump,
            current_pttl,
            current_type,
            current_memory,
        ):
            if isinstance(response, BaseException):
                raise response

        try:
            current_pttl_value = int(current_pttl)
        except (TypeError, ValueError) as error:
            raise ValueError(
                f"无效的 PTTL 响应: {current_pttl!r}"
            ) from error
        if current_dump is None or current_pttl_value in (0, -2):
            raise SourceStateChangedError(
                f"源键在兼容降级前消失或到期: {key!r}"
            )
        if current_pttl_value < -2:
            raise ValueError(f"无效的 PTTL 响应: {current_pttl_value}")

        elapsed_ms = (
            max(0, time.monotonic_ns() - validation_started_ns) + 999_999
        ) // 1_000_000
        remaining_pttl = current_pttl_value
        if current_pttl_value > 0:
            remaining_pttl = current_pttl_value - elapsed_ms
            if remaining_pttl <= 0:
                raise SourceStateChangedError(
                    f"源键在兼容降级前到期: {key!r}"
                )

        current_deadline = None
        deadline_is_exact = False
        if preserve_ttl and current_pttl_value > 0:
            try:
                absolute_ms = int(current_pexpiretime)
            except (TypeError, ValueError):
                absolute_ms = -1
            if absolute_ms > 0:
                current_deadline = absolute_ms
                deadline_is_exact = True
            else:
                current_deadline = expiry_deadline_from_pttl(
                    current_pttl_value,
                    validation_started_ns,
                )
            if current_deadline is None:
                raise SourceStateChangedError(
                    f"源键在兼容降级前到期: {key!r}"
                )

        if not source_state_in_dynamic_scope(
            remaining_pttl,
            key_type=current_type,
            memory_size=current_memory,
            key_types=allowed_key_types,
            min_ttl=min_ttl,
            max_key_size=max_key_size,
        ):
            raise SourceStateChangedError(
                f"源键在兼容降级前移出过滤范围: {key!r}"
            )
        if expected_dump is not None and current_dump != expected_dump:
            raise SourceStateChangedError(
                f"源键在兼容降级前内容发生变化: {key!r}"
            )
        if preserve_ttl:
            if (expires_at_ms is None) != (current_deadline is None):
                raise SourceStateChangedError(
                    f"源键在兼容降级前过期策略发生变化: {key!r}"
                )
            if expires_at_ms is not None:
                allowed_drift_ms = (
                    0 if deadline_is_exact and expires_at_is_exact else 1000
                )
                if abs(int(expires_at_ms) - int(current_deadline)) > allowed_drift_ms:
                    raise SourceStateChangedError(
                        f"源键在兼容降级前过期时间发生变化: {key!r}"
                    )

        captured = (key_type, value, values)
        break

    if captured is None:
        raise SourceStateChangedError(f"未捕获到稳定的源键状态: {key!r}")
    key_type, value, values = captured

    if key_type == "string" and value is None:
        raise SourceStateChangedError(
            f"源键在兼容降级读取期间消失: {key!r}"
        )
    if key_type != "string" and not values:
        raise SourceStateChangedError(
            f"源键在兼容降级读取期间变为空: {key!r}"
        )

    temporary_key = _temporary_key()
    temporary_may_exist = True
    try:
        if key_type == "string":
            target.set(temporary_key, value)
        elif key_type == "list":
            target.rpush(temporary_key, *values)
        elif key_type == "set":
            target.sadd(temporary_key, *values)
        elif key_type == "zset":
            target.zadd(temporary_key, mapping=dict(values))
        elif key_type == "hash":
            target.hset(temporary_key, mapping=values)

        if expires_at_ms is not None:
            if not target.pexpireat(temporary_key, int(expires_at_ms)):
                if (
                    expires_at_is_exact
                    or expires_at_ms > int(time.time() * 1000)
                ):
                    raise RuntimeError(
                        f"降级复制临时键在设置过期时间前消失: {temporary_key!r}"
                    )
                if overwrite:
                    target.delete(key)
                    return True
                return False

        try:
            if overwrite:
                target.rename(temporary_key, key)
            elif not target.renamenx(temporary_key, key):
                return False
        except redis.ResponseError as error:
            if expires_at_ms is None or not _is_missing_key_error(error):
                raise
            if (
                not expires_at_is_exact
                and expires_at_ms > int(time.time() * 1000)
            ):
                raise
            if overwrite:
                target.delete(key)
                return True
            return False
        temporary_may_exist = False
        return True
    finally:
        if temporary_may_exist:
            try:
                target.delete(temporary_key)
            except Exception as cleanup_error:
                logger.warning(
                    "清理降级复制临时键失败 key=%r temp=%r: %s",
                    key,
                    temporary_key,
                    cleanup_error,
                )
