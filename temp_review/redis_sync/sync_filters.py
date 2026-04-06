"""
同步键过滤：与 config.yaml 中 sync.filters 对齐（包含/排除 glob、最小 TTL、最大内存占用）。
"""

from __future__ import annotations

import fnmatch
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

import redis

logger = logging.getLogger(__name__)

Key = Union[str, bytes]


def _key_text(key: Key) -> str:
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="surrogateescape")
    return key


@dataclass
class KeySyncFilter:
    """键级过滤；与 Redis SCAN 风格 glob 一致（fnmatch）。"""

    include_patterns: Optional[List[str]] = None
    exclude_patterns: Optional[List[str]] = None
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
        name = _key_text(key)
        if self.exclude_patterns:
            for pat in self.exclude_patterns:
                if fnmatch.fnmatchcase(name, pat):
                    return False
        if self.include_patterns:
            return any(fnmatch.fnmatchcase(name, p) for p in self.include_patterns)
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
                logger.warning("TTL/MEMORY 批量查询失败，本批仅按名称过滤: %s", e)
                out.extend(chunk)
                continue

            idx = 0
            for k in chunk:
                ttl = raw[idx]
                idx += 1
                mem = None
                if self.max_key_size > 0:
                    mem = raw[idx]
                    idx += 1
                if isinstance(ttl, Exception):
                    ttl = -2
                if mem is not None and isinstance(mem, Exception):
                    mem = None
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
