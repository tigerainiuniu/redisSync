from unittest.mock import MagicMock

import pytest

from redis_sync.sync_filters import (
    ATOMIC_FILTERED_DELETE_LUA,
    KeySyncFilter,
    build_atomic_filtered_delete_command,
    redis_glob_match,
)


def test_from_config_returns_none_when_empty():
    assert KeySyncFilter.from_config(None) is None
    assert KeySyncFilter.from_config({}) is None
    assert KeySyncFilter.from_config({"include_patterns": []}) is None


def test_from_config_min_ttl_only():
    f = KeySyncFilter.from_config({"min_ttl": 10})
    assert f is not None
    assert f.min_ttl == 10


def test_atomic_filtered_delete_command_encodes_scope_and_keys():
    command = build_atomic_filtered_delete_command(
        [b"one", b"two"],
        key_types=["zset", b"string"],
        min_ttl=10,
        max_key_size=100,
    )

    assert command == [
        b"EVAL",
        ATOMIC_FILTERED_DELETE_LUA,
        b"2",
        b"one",
        b"two",
        b"10",
        b"100",
        b"2",
        b"string",
        b"zset",
    ]


def test_name_allowed_include_exclude():
    f = KeySyncFilter(
        include_patterns=["user:*"],
        exclude_patterns=["user:admin*"],
    )
    assert f.name_allowed("user:1")
    assert not f.name_allowed("user:admin")
    assert not f.name_allowed("other")


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        ("h[^e]llo", "hallo", True),
        ("h[^e]llo", "hello", False),
        ("h[!x]llo", "h!llo", True),
        ("h[!x]llo", "hxllo", True),
        ("h[!x]llo", "hallo", False),
        ("range:[a-c]", "range:b", True),
        (r"literal\*", "literal*", True),
        (r"literal\*", "literal-anything", False),
        (b"bin:\xff?", b"bin:\xff\x00", True),
        (b"bin:\xff?", b"bin:\xfe\x00", False),
    ],
)
def test_redis_glob_match_uses_redis_semantics(pattern, key, expected):
    assert redis_glob_match(key, pattern) is expected


def test_filter_batch_min_ttl():
    client = MagicMock()
    pipe = MagicMock()
    client.pipeline.return_value = pipe
    pipe.execute.return_value = [30]

    f = KeySyncFilter(min_ttl=10)
    out = f.filter_batch(client, [b"k1"])
    assert out == [b"k1"]

    pipe.execute.return_value = [3]
    out2 = f.filter_batch(client, [b"k1"])
    assert out2 == []


def test_key_filter_from_migration_config():
    from redis_sync.migration_orchestrator import MigrationConfig, key_filter_from_migration_config

    cfg = MigrationConfig(
        include_patterns=["a*"],
        exclude_patterns=None,
        filter_min_ttl=0,
        filter_max_key_size=0,
    )
    kf = key_filter_from_migration_config(cfg)
    assert kf is not None
    assert kf.name_allowed("abc")
    assert not kf.name_allowed("z")
