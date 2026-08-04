"""
同步键过滤：与 config.yaml 中 sync.filters 对齐（包含/排除 glob、最小 TTL、最大内存占用）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import redis

logger = logging.getLogger(__name__)

Key = Union[str, bytes]
Pattern = Union[str, bytes]


ATOMIC_FILTERED_DELETE_LUA = b"""
local min_ttl = tonumber(ARGV[1]) or 0
local max_key_size = tonumber(ARGV[2]) or 0
local type_count = tonumber(ARGV[3]) or 0
local deleted = 0

for _, key in ipairs(KEYS) do
    local allowed = redis.call('EXISTS', key) == 1
    if allowed and type_count > 0 then
        local type_reply = redis.call('TYPE', key)
        local actual_type = type_reply
        if type(type_reply) == 'table' then
            actual_type = type_reply['ok']
        end
        allowed = false
        for index = 1, type_count do
            if actual_type == ARGV[3 + index] then
                allowed = true
                break
            end
        end
    end
    if allowed and min_ttl > 0 then
        local ttl = redis.call('TTL', key)
        allowed = ttl == -1 or ttl >= min_ttl
    end
    if allowed and max_key_size > 0 then
        local key_size = redis.call('MEMORY', 'USAGE', key)
        allowed = not key_size or key_size <= max_key_size
    end
    if allowed then
        deleted = deleted + redis.call('DEL', key)
    end
end

return deleted
""".strip()


def build_atomic_filtered_delete_command(
    keys: List[Key],
    *,
    key_types: Optional[Iterable[Union[str, bytes]]] = None,
    min_ttl: int = 0,
    max_key_size: int = 0,
) -> List[Any]:
    """Build one atomic target-side scope check and delete command."""
    if not keys:
        raise ValueError("atomic filtered delete requires at least one key")
    normalized_types = sorted(
        {
            value.lower() if isinstance(value, bytes) else str(value).lower().encode("utf-8")
            for value in (key_types or ())
        }
    )
    return [
        b"EVAL",
        ATOMIC_FILTERED_DELETE_LUA,
        str(len(keys)).encode("ascii"),
        *keys,
        str(max(0, int(min_ttl))).encode("ascii"),
        str(max(0, int(max_key_size))).encode("ascii"),
        str(len(normalized_types)).encode("ascii"),
        *normalized_types,
    ]


def source_state_in_dynamic_scope(
    pttl_ms: Any,
    *,
    key_type: Any = None,
    memory_size: Any = None,
    key_types: Optional[Iterable[Union[str, bytes]]] = None,
    min_ttl: int = 0,
    max_key_size: int = 0,
) -> bool:
    """Check value-dependent filters against one atomically sampled source state."""
    try:
        ttl = int(pttl_ms)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid PTTL response: {pttl_ms!r}") from exc
    if ttl in (-2, 0):
        return False
    if ttl < -2:
        raise ValueError(f"invalid PTTL response: {ttl}")

    normalized_types = {
        value.lower() if isinstance(value, bytes) else str(value).lower().encode("utf-8")
        for value in (key_types or ())
    }
    if normalized_types:
        normalized_type = (
            key_type.lower()
            if isinstance(key_type, bytes)
            else str(key_type).lower().encode("utf-8")
        )
        if normalized_type not in normalized_types:
            return False

    minimum_ttl_ms = max(0, int(min_ttl)) * 1000
    if minimum_ttl_ms and ttl > 0 and ttl < minimum_ttl_ms:
        return False

    maximum_size = max(0, int(max_key_size))
    if maximum_size and memory_size is not None:
        try:
            if int(memory_size) > maximum_size:
                return False
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid MEMORY USAGE response: {memory_size!r}"
            ) from exc
    return True


def _redis_bytes(value: Union[str, bytes]) -> bytes:
    return value if isinstance(value, bytes) else value.encode("utf-8")


@lru_cache(maxsize=512)
def _compile_redis_glob(pattern: bytes) -> Tuple[Tuple[Any, ...], ...]:
    """Tokenize the byte-oriented glob syntax used by Redis stringmatchlen()."""
    tokens: List[Tuple[Any, ...]] = []
    index = 0
    pattern_length = len(pattern)

    while index < pattern_length:
        current = pattern[index]
        if current == ord("*"):
            if not tokens or tokens[-1][0] != "star":
                tokens.append(("star",))
            index += 1
            continue
        if current == ord("?"):
            tokens.append(("any",))
            index += 1
            continue
        if current == ord("\\") and index + 1 < pattern_length:
            tokens.append(("literal", pattern[index + 1]))
            index += 2
            continue
        if current != ord("["):
            tokens.append(("literal", current))
            index += 1
            continue

        index += 1
        negate = index < pattern_length and pattern[index] == ord("^")
        if negate:
            index += 1
        ranges = []
        while index < pattern_length and pattern[index] != ord("]"):
            if pattern[index] == ord("\\") and index + 1 < pattern_length:
                value = pattern[index + 1]
                ranges.append((value, value))
                index += 2
                continue
            start = pattern[index]
            if index + 2 < pattern_length and pattern[index + 1] == ord("-"):
                end = pattern[index + 2]
                ranges.append((min(start, end), max(start, end)))
                index += 3
                continue
            ranges.append((start, start))
            index += 1
        if index < pattern_length and pattern[index] == ord("]"):
            index += 1
        tokens.append(("class", negate, tuple(ranges)))

    return tuple(tokens)


def redis_glob_match(key: Key, pattern: Pattern) -> bool:
    """Match a Redis key using case-sensitive, binary-safe Redis glob rules."""
    value = _redis_bytes(key)
    tokens = _compile_redis_glob(_redis_bytes(pattern))
    previous = [False] * (len(value) + 1)
    previous[0] = True

    for token in tokens:
        current = [False] * (len(value) + 1)
        kind = token[0]
        if kind == "star":
            current[0] = previous[0]
            for value_index in range(1, len(value) + 1):
                current[value_index] = (
                    previous[value_index] or current[value_index - 1]
                )
        else:
            for value_index, byte in enumerate(value, start=1):
                if not previous[value_index - 1]:
                    continue
                if kind == "any":
                    current[value_index] = True
                elif kind == "literal":
                    current[value_index] = byte == token[1]
                else:
                    matched = any(start <= byte <= end for start, end in token[2])
                    current[value_index] = not matched if token[1] else matched
        previous = current

    return previous[-1]


@dataclass
class KeySyncFilter:
    """键级过滤；名称匹配与 Redis SCAN 的 glob 语义一致。"""

    include_patterns: Optional[List[Pattern]] = None
    exclude_patterns: Optional[List[Pattern]] = None
    min_ttl: int = 0
    max_key_size: int = 0

    @classmethod
    def from_config(cls, cfg: Optional[Dict[str, Any]]) -> Optional[KeySyncFilter]:
        if not cfg or not isinstance(cfg, dict):
            return None
        inc = cfg.get("include_patterns")
        exc = cfg.get("exclude_patterns")
        min_ttl = int(cfg.get("min_ttl") or 0)
        max_key_size = int(cfg.get("max_key_size") or 0)
        if isinstance(inc, list) and len(inc) == 0:
            inc = None
        if isinstance(exc, list) and len(exc) == 0:
            exc = None
        if inc is None and exc is None and min_ttl <= 0 and max_key_size <= 0:
            return None
        return cls(
            include_patterns=inc if isinstance(inc, list) else None,
            exclude_patterns=exc if isinstance(exc, list) else None,
            min_ttl=min_ttl,
            max_key_size=max_key_size,
        )

    def name_allowed(self, key: Key) -> bool:
        if self.exclude_patterns:
            for pat in self.exclude_patterns:
                if redis_glob_match(key, pat):
                    return False
        if self.include_patterns:
            return any(
                redis_glob_match(key, pattern)
                for pattern in self.include_patterns
            )
        return True

    def filter_names(self, keys: List[Key]) -> List[Key]:
        return [k for k in keys if self.name_allowed(k)]

    def filter_batch(self, client: redis.Redis, keys: List[Key]) -> List[Key]:
        """先做名称过滤，再按需批量查 TTL / MEMORY USAGE。"""
        keys = self.filter_names(keys)
        if not keys:
            return []
        if self.min_ttl <= 0 and self.max_key_size <= 0:
            return keys

        out: List[Key] = []
        batch_size = 200
        for i in range(0, len(keys), batch_size):
            chunk = keys[i : i + batch_size]
            pipe = client.pipeline(transaction=False)
            for k in chunk:
                pipe.ttl(k)
                if self.max_key_size > 0:
                    pipe.execute_command("MEMORY", "USAGE", k)
            try:
                raw = pipe.execute()
            except Exception as e:
                raise RuntimeError(f"TTL/MEMORY 批量查询失败: {e}") from e

            idx = 0
            for k in chunk:
                ttl = raw[idx]
                idx += 1
                mem = None
                if self.max_key_size > 0:
                    mem = raw[idx]
                    idx += 1
                if isinstance(ttl, Exception):
                    raise RuntimeError(f"读取键 {k!r} 的 TTL 失败: {ttl}")
                if mem is not None and isinstance(mem, Exception):
                    raise RuntimeError(f"读取键 {k!r} 的内存占用失败: {mem}")
                if not self._ttl_ok(ttl):
                    continue
                if not self._size_ok(mem):
                    continue
                out.append(k)
        return out

    def _ttl_ok(self, ttl: Any) -> bool:
        if self.min_ttl <= 0:
            return True
        try:
            t = int(ttl)
        except (TypeError, ValueError):
            return True
        if t == -1:
            return True
        if t == -2:
            return False
        return t >= self.min_ttl

    def _size_ok(self, mem: Any) -> bool:
        if self.max_key_size <= 0:
            return True
        if mem is None:
            return True
        try:
            m = int(mem)
            return m <= self.max_key_size
        except (TypeError, ValueError):
            return True
