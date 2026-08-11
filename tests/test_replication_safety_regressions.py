from unittest.mock import MagicMock

import pytest
import redis
import yaml

import redis_sync.key_sync as key_sync_module
from redis_sync.config import load_and_validate_service_config
from redis_sync.exceptions import ConfigurationError
from redis_sync.key_sync import SourceStateChangedError, _sync_key_fallback
from redis_sync.key_sync import sync_key_with_dump_restore
from redis_sync.unified_incremental_service import UnifiedIncrementalService


class OversizedSourcePipeline:
    def __init__(self):
        self.calls = []

    def watch(self, key):
        self.calls.append(("watch", key))

    def pttl(self, key):
        self.calls.append(("pttl", key))
        return -1

    def type(self, key):
        self.calls.append(("type", key))
        return b"list"

    def execute_command(self, command, subcommand, key):
        assert (command, subcommand) == ("MEMORY", "USAGE")
        self.calls.append(("memory", key))
        return 101

    def multi(self):
        self.calls.append(("multi",))

    def dump(self, key):
        self.calls.append(("dump", key))
        raise AssertionError("DUMP ran before the size preflight rejected the key")

    def lrange(self, key, start, end):
        self.calls.append(("lrange", key, start, end))
        raise AssertionError("LRANGE materialized an oversized list")

    def reset(self):
        self.calls.append(("reset",))


class OversizedSource:
    def __init__(self):
        self.watched = OversizedSourcePipeline()

    def exists(self, _key):
        return True

    def execute_command(self, *_args):
        raise redis.ResponseError("unknown command PEXPIRETIME")

    def pipeline(self, transaction=True):
        return self.watched


def test_key_sync_rejects_oversized_key_before_dump():
    source = OversizedSource()

    assert not sync_key_with_dump_restore(
        source, MagicMock(), b"large", max_key_size=100
    )
    assert not any(call[0] == "dump" for call in source.watched.calls)
    assert ("memory", b"large") in source.watched.calls


def test_unified_capture_rejects_oversized_key_before_dump():
    source = OversizedSource()
    service = UnifiedIncrementalService(
        "psync",
        source,
        {},
        {"apply_mode": "key_state", "filters": {"max_key_size": 100}},
    )
    service._command_keys = lambda _command: [b"large"]
    try:
        assert service._prepare_key_state([b"SET", b"large", b"value"]) == (
            "keys",
            [(b"large", None, None, None, False)],
        )
        assert not any(call[0] == "dump" for call in source.watched.calls)
        assert ("memory", b"large") in source.watched.calls
    finally:
        service.stop()


def test_type_fallback_rejects_oversized_list_before_lrange():
    source = OversizedSource()

    with pytest.raises(SourceStateChangedError, match="移出过滤范围"):
        _sync_key_fallback(
            source,
            MagicMock(),
            b"large-list",
            -1,
            max_key_size=100,
        )
    assert not any(call[0] == "lrange" for call in source.watched.calls)
    assert ("memory", b"large-list") in source.watched.calls


def test_exact_source_expiry_ignores_fast_local_wall_clock(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.return_value = 102_500
    source.pipeline.return_value.execute.return_value = [b"dump", 2_500, 102_500]
    target = MagicMock()
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 1_000_000.0)

    assert sync_key_with_dump_restore(source, target, b"expiring")
    target.restore.assert_called_once_with(
        b"expiring", 102_500, b"dump", replace=True, absttl=True
    )
    target.delete.assert_not_called()


def _service_config(idle_timeout):
    return {
        "source": {"host": "source.example", "port": 6379},
        "targets": [
            {"name": "target", "host": "target.example", "port": 6379}
        ],
        "sync": {
            "mode": "incremental",
            "incremental_sync": {
                "method": "psync",
                "target_connection_idle_timeout": idle_timeout,
            },
        },
        "service": {},
    }


def test_target_connection_idle_timeout_is_validated(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(_service_config(12.5)), encoding="utf-8")
    loaded = load_and_validate_service_config(str(path))
    assert loaded["sync"]["incremental_sync"][
        "target_connection_idle_timeout"
    ] == 12.5

    path.write_text(yaml.safe_dump(_service_config(0)), encoding="utf-8")
    with pytest.raises(ConfigurationError, match="target_connection_idle_timeout"):
        load_and_validate_service_config(str(path))
