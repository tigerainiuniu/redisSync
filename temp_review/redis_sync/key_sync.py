"""
键级同步：优先使用 DUMP/RESTORE 在目标端原子替换，避免 delete+写入 的中间空窗期。
"""

import logging
from typing import Any, Union

import redis

logger = logging.getLogger(__name__)

KeyType = Union[str, bytes]


def sync_key_with_dump_restore(
    source: redis.Redis,
    target: redis.Redis,
    key: KeyType,
    *,
    overwrite: bool = True,
    preserve_ttl: bool = True,
) -> bool:
    """
    使用 DUMP + RESTORE(replace=True) 同步单个键。

    若源端键不存在，则删除目标端同名键。

    返回:
        True 表示已迁移或已删除目标键；False 表示跳过（目标已存在且 overwrite=False）。
    """
    try:
        if not source.exists(key):
            target.delete(key)
            return True

        if not overwrite and target.exists(key):
            return False

        pipe = source.pipeline(transaction=False)
        pipe.dump(key)
        pipe.pttl(key)
        results = pipe.execute()
        dump_data, pttl = results[0], results[1]

        if isinstance(dump_data, Exception):
            raise dump_data
        if isinstance(pttl, Exception):
            pttl = 0

        if dump_data is None:
            target.delete(key)
            return True

        if preserve_ttl:
            p = int(pttl) if pttl is not None else -1
            ttl_ms = p if p > 0 else 0
        else:
            ttl_ms = 0
        try:
            target.restore(key, ttl_ms, dump_data, replace=True)
        except redis.ResponseError as e:
            logger.warning("RESTORE 失败，回退到类型级复制: key=%r err=%s", key, e)
            return _sync_key_fallback(source, target, key, pttl, preserve_ttl)

        return True

    except Exception as e:
        logger.error("键同步失败 key=%r: %s", key, e)
        raise


def _sync_key_fallback(
    source: redis.Redis,
    target: redis.Redis,
    key: KeyType,
    pttl: Any,
    preserve_ttl: bool = True,
) -> bool:
    """RESTORE 不支持或失败时的回退路径（仍尽量避免长时间空键）。"""
    key_type_b = source.type(key)
    key_type = (
        key_type_b.decode() if isinstance(key_type_b, bytes) else str(key_type_b)
    )

    ttl_sec = None
    if preserve_ttl and pttl is not None:
        p = int(pttl)
        if p > 0:
            ttl_sec = max(1, p // 1000) if p >= 1000 else 1
        elif p == -1:
            ttl_sec = None
        elif p == -2:
            target.delete(key)
            return True

    if key_type == "string":
        val = source.get(key)
        if val is None:
            target.delete(key)
            return True
        target.set(key, val)
    elif key_type == "list":
        values = source.lrange(key, 0, -1)
        target.delete(key)
        if values:
            target.rpush(key, *values)
    elif key_type == "set":
        values = source.smembers(key)
        target.delete(key)
        if values:
            target.sadd(key, *values)
    elif key_type == "zset":
        values = source.zrange(key, 0, -1, withscores=True)
        target.delete(key)
        if values:
            target.zadd(key, mapping=dict(values))
    elif key_type == "hash":
        values = source.hgetall(key)
        target.delete(key)
        if values:
            target.hset(key, mapping=values)
    elif key_type == "stream":
        entries = source.xrange(key)
        target.delete(key)
        for entry_id, fields in entries or []:
            target.xadd(key, fields, id=entry_id)
    else:
        logger.warning("不支持的键类型: %s key=%r", key_type, key)
        return False

    if ttl_sec and ttl_sec > 0:
        target.expire(key, ttl_sec)
    return True
