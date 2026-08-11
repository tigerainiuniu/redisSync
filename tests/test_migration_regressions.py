import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest
import redis

import redis_sync.full_migration_handler as full_migration_module
import redis_sync.incremental_migration_handler as incremental_migration_module
import redis_sync.key_sync as key_sync_module
from redis_sync.full_migration_handler import FullMigrationHandler
from redis_sync.incremental_migration_handler import IncrementalMigrationHandler
from redis_sync.key_sync import sync_key_with_dump_restore
from redis_sync.scan_handler import ScanHandler
from redis_sync.sync_filters import KeySyncFilter


def _pipeline(results=None, error=None):
    pipe = MagicMock()
    if error is not None:
        pipe.execute.side_effect = error
    else:
        pipe.execute.return_value = results or []
    return pipe


def _watched_pipeline(results, *, key_type=b"string", value=None, values=None):
    pipe = _pipeline(results)
    pipe.type.return_value = key_type
    if key_type == b"string":
        pipe.get.return_value = value
    elif key_type == b"list":
        pipe.lrange.return_value = values
    elif key_type == b"set":
        pipe.smembers.return_value = values
    elif key_type == b"zset":
        pipe.zrange.return_value = values
    elif key_type == b"hash":
        pipe.hgetall.return_value = values
    return pipe


def test_full_migration_validates_strategy_before_flush():
    source = MagicMock()
    target = MagicMock()

    result = FullMigrationHandler(source, target).perform_full_migration(
        strategy="invalid",
        clear_target=True,
    )

    assert result["success"] is False
    target.flushdb.assert_not_called()


def test_full_migration_does_not_clear_target_by_default():
    source = MagicMock()
    source.dbsize.return_value = 0
    source.scan.return_value = (0, [])
    target = MagicMock()

    result = FullMigrationHandler(source, target).perform_full_migration()

    assert result["success"] is True
    target.flushdb.assert_not_called()


def test_full_migration_stops_before_next_batch_when_cancelled():
    stop_event = threading.Event()
    source = MagicMock()
    source.dbsize.return_value = 2
    source.scan.return_value = (0, [b"first", b"second"])
    handler = FullMigrationHandler(source, MagicMock(), stop_event=stop_event)
    migrated_batches = []

    def migrate_batch(keys, *_args):
        migrated_batches.append(list(keys))
        stop_event.set()
        return {"migrated": len(keys), "failed": 0, "skipped": 0}

    handler._migrate_key_batch = migrate_batch

    result = handler.perform_full_migration(batch_size=1)

    assert result["success"] is False
    assert "取消" in result["error"]
    assert migrated_batches == [[b"first"]]


def test_full_migration_scan_retry_wait_is_interruptible():
    stop_event = threading.Event()
    source = MagicMock()

    def fail_scan(**_kwargs):
        stop_event.set()
        raise redis.ConnectionError("scan blocked")

    source.scan.side_effect = fail_scan
    handler = FullMigrationHandler(source, MagicMock(), stop_event=stop_event)
    handler.SCAN_RETRY_DELAY = 60

    started = time.monotonic()
    result = handler.perform_full_migration(key_pattern="managed:*")

    assert time.monotonic() - started < 0.2
    assert result["success"] is False
    assert "取消" in result["error"]
    assert source.scan.call_count == 1


def test_sync_strategy_uses_scan_fallback_without_opening_replication_stream():
    source = MagicMock()
    source.dbsize.return_value = 0
    source.scan.return_value = (0, [])
    target = MagicMock()

    result = FullMigrationHandler(source, target).perform_full_migration(
        strategy="sync",
        clear_target=False,
    )

    assert result["success"] is True
    assert result["details"]["fallback"] == "scan_dump_restore"
    source.connection_pool.get_connection.assert_not_called()


def test_estimate_key_count_stops_after_empty_scan_cycle():
    source = MagicMock()
    source.scan.return_value = (0, [])
    handler = FullMigrationHandler(source, MagicMock())
    handler.SCAN_RETRY_DELAY = 0

    assert handler._estimate_key_count("missing:*", None) == 0
    assert source.scan.call_count == 1


def test_estimate_key_count_samples_full_db_before_applying_pattern():
    source = MagicMock()

    def scan(**kwargs):
        if "match" in kwargs:
            return 0, [b"tenant:1"]
        return 0, [b"tenant:1", b"other:1"]

    source.scan.side_effect = scan
    source.dbsize.return_value = 1000
    handler = FullMigrationHandler(source, MagicMock())

    assert handler._estimate_key_count("tenant:*", None) == 1
    assert source.scan.call_args.kwargs == {"cursor": 0, "count": 100}


def test_estimate_key_count_applies_name_filter_to_completed_sample():
    source = MagicMock()
    source.scan.return_value = (
        0,
        [b"tenant:public", b"tenant:private", b"other:public"],
    )
    handler = FullMigrationHandler(source, MagicMock())
    key_filter = KeySyncFilter(exclude_patterns=[b"*:private"])

    assert handler._estimate_key_count("tenant:*", None, key_filter) == 1


def test_estimate_key_count_applies_dynamic_filters_to_sample():
    source = MagicMock()
    source.scan.return_value = (0, [b"small", b"large"])
    source.pipeline.return_value = _pipeline([-1, 50, -1, 150])
    handler = FullMigrationHandler(source, MagicMock())
    key_filter = KeySyncFilter(max_key_size=100)

    assert handler._estimate_key_count("*", None, key_filter) == 1


def test_estimate_key_count_does_not_treat_truncated_last_page_as_exact():
    source = MagicMock()
    source.scan.return_value = (
        0,
        [f"tenant:{index}".encode() for index in range(1000)] + [b"other"],
    )
    source.dbsize.return_value = 2000
    handler = FullMigrationHandler(source, MagicMock())

    assert handler._estimate_key_count("tenant:*", None) == 2000
    source.dbsize.assert_called_once_with()


def test_full_migration_stops_after_scan_retry_limit():
    source = MagicMock()
    source.dbsize.return_value = 1
    source.scan.side_effect = redis.ConnectionError("source unavailable")
    handler = FullMigrationHandler(source, MagicMock())
    handler.SCAN_RETRY_DELAY = 0

    result = handler.perform_full_migration(clear_target=False)

    assert result["success"] is False
    assert "SCAN" in result["error"]
    assert source.scan.call_count == handler.SCAN_MAX_RETRIES


def test_full_migration_reports_restore_response_failure():
    source = MagicMock()
    source.dbsize.return_value = 1
    source.scan.return_value = (0, [b"key"])
    source.pipeline.return_value = _pipeline([b"dump", -1])

    target = MagicMock()
    target.pipeline.return_value = _pipeline([redis.ResponseError("restore failed")])

    result = FullMigrationHandler(source, target).perform_full_migration(
        clear_target=False,
        overwrite_existing=True,
    )

    assert result["success"] is False
    assert result["statistics"]["migrated_keys"] == 0
    assert result["statistics"]["failed_keys"] == 1


def test_incremental_comparison_accepts_equal_value_with_different_dump_format():
    source = MagicMock()
    source.dump.return_value = b"new-version-dump"
    source.type.return_value = b"string"
    source.get.return_value = b"same-value"
    source.pttl.return_value = -1
    target = MagicMock()
    target.exists.return_value = True
    target.dump.return_value = b"old-version-dump"
    target.type.return_value = b"string"
    target.get.return_value = b"same-value"
    target.pttl.return_value = -1

    assert IncrementalMigrationHandler(source, target)._is_key_different(
        b"key"
    ) == (False, "值和TTL相同")


def test_full_verification_accepts_equal_value_with_different_dump_format():
    source = MagicMock()
    source.type.return_value = b"string"
    source.dump.return_value = b"new-version-dump"
    source.get.return_value = b"same-value"
    source.ttl.return_value = -1
    target = MagicMock()
    target.exists.return_value = True
    target.type.return_value = b"string"
    target.dump.return_value = b"old-version-dump"
    target.get.return_value = b"same-value"
    target.ttl.return_value = -1

    comparison = ScanHandler(source, target)._compare_single_key(b"key")

    assert comparison == {
        "exists_in_target": True,
        "types_match": True,
        "values_match": True,
        "ttl_match": True,
    }


def test_cross_version_stream_verification_remains_conservative():
    source = MagicMock()
    source.type.return_value = b"stream"
    source.dump.return_value = b"new-version-stream-dump"
    source.ttl.return_value = -1
    target = MagicMock()
    target.exists.return_value = True
    target.type.return_value = b"stream"
    target.dump.return_value = b"old-version-stream-dump"
    target.ttl.return_value = -1

    comparison = ScanHandler(source, target)._compare_single_key(b"stream")

    assert comparison["types_match"] is True
    assert comparison["values_match"] is False
    assert comparison["ttl_match"] is True


@pytest.mark.parametrize("strategy", ["scan", "dump_restore"])
def test_full_migration_fails_closed_when_type_filter_query_fails(strategy):
    source = MagicMock()
    source.scan.return_value = (0, [b"key"])
    source.type.return_value = b"hash"
    source.pipeline.return_value = _pipeline(
        [b"dump", -1, redis.ResponseError("NOPERM TYPE")]
    )
    target = MagicMock()
    target_pipe = _pipeline([0])
    target.pipeline.return_value = target_pipe
    handler = FullMigrationHandler(source, target)
    handler._estimate_key_count = MagicMock(return_value=1)

    result = handler.perform_full_migration(
        strategy=strategy,
        clear_target=False,
        key_types=["hash"],
    )

    assert result["success"] is False
    target_pipe.restore.assert_not_called()
    target_pipe.delete.assert_not_called()
    target_pipe.execute_command.assert_not_called()


def test_full_batch_skips_existing_key_without_overwrite():
    source = MagicMock()
    target = MagicMock()
    exists_pipe = _pipeline([1])
    target.pipeline.return_value = exists_pipe

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"],
        preserve_ttl=True,
        overwrite_existing=False,
    )

    assert result == {"migrated": 0, "failed": 0, "skipped": 1}
    source.pipeline.assert_not_called()
    exists_pipe.exists.assert_called_once_with(b"key")


def test_full_batch_overwrites_only_when_enabled():
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", -1])
    target = MagicMock()
    write_pipe = _pipeline([b"OK"])
    target.pipeline.return_value = write_pipe

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.restore.assert_called_once_with(b"key", 0, b"dump", replace=True)


def test_full_batch_deducts_pipeline_delay_from_pttl(monkeypatch):
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    write_pipe = _pipeline([b"OK"])
    target.pipeline.return_value = write_pipe
    monotonic_values = iter([1_000_000_000, 1_100_000_000])
    monkeypatch.setattr(
        full_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(full_migration_module.time, "time", lambda: 100.0)

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"expiring"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.restore.assert_called_once_with(
        b"expiring", 100_150, b"dump", replace=True, absttl=True
    )


def test_full_batch_deletes_key_that_expires_before_write(monkeypatch):
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", 100])
    target = MagicMock()
    write_pipe = _pipeline([1])
    target.pipeline.return_value = write_pipe
    monotonic_values = iter([1_000_000_000, 1_101_000_000])
    monkeypatch.setattr(
        full_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"expiring"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.delete.assert_called_once_with(b"expiring")
    write_pipe.restore.assert_not_called()


def test_full_batch_uses_single_key_fallback_for_restore_compatibility(monkeypatch):
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", -1])
    target = MagicMock()
    target.pipeline.return_value = _pipeline(
        [redis.ResponseError("ERR DUMP payload version or checksum are wrong")]
    )
    fallback = MagicMock(return_value=True)
    monkeypatch.setattr(full_migration_module, "_sync_key_fallback", fallback)

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    fallback.assert_called_once_with(
        source,
        target,
        b"key",
        -1,
        True,
        overwrite=True,
        expires_at_ms=None,
        expected_dump=b"dump",
        key_types=None,
        min_ttl=0,
        max_key_size=0,
        expires_at_is_exact=False,
    )


@pytest.mark.parametrize(
    ("results", "key_types", "key_filter"),
    [
        ([b"dump", -1, b"list"], ["string"], None),
        ([b"dump", 10_000], None, KeySyncFilter(min_ttl=60)),
        ([b"dump", -1, 100], None, KeySyncFilter(max_key_size=10)),
    ],
    ids=["wrong-type", "low-ttl", "oversized"],
)
def test_full_batch_atomically_rechecks_dynamic_scope_before_restore(
    results, key_types, key_filter
):
    source = MagicMock()
    source.pipeline.return_value = _pipeline(results)
    target = MagicMock()
    write_pipe = _pipeline([1])
    target.pipeline.return_value = write_pipe
    handler = FullMigrationHandler(source, target)
    handler._active_full_params = {"key_types": key_types}
    handler._key_filter = key_filter

    result = handler._dump_restore_batch(
        [b"key"], preserve_ttl=True, overwrite_existing=True
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.restore.assert_not_called()
    command = write_pipe.execute_command.call_args.args
    assert command[0] == b"EVAL"
    assert b"key" in command


@pytest.mark.parametrize("strategy", ["scan", "dump_restore"])
def test_full_migration_defers_all_dynamic_filters_to_atomic_capture(strategy):
    source = MagicMock()
    target = MagicMock()
    handler = FullMigrationHandler(source, target)
    handler._estimate_key_count = MagicMock(return_value=1)
    handler._scan_page = MagicMock(return_value=(0, [b"moved-into-scope"]))
    handler._filter_keys_by_types = MagicMock(
        side_effect=AssertionError("transaction-external type filter was used")
    )
    key_filter = MagicMock(spec=KeySyncFilter)
    key_filter.min_ttl = 10
    key_filter.max_key_size = 100
    key_filter.filter_names.side_effect = lambda keys: list(keys)
    key_filter.filter_batch.side_effect = AssertionError(
        "transaction-external value filter was used"
    )
    handler._dump_restore_batch = MagicMock(
        return_value={"migrated": 1, "failed": 0, "skipped": 0}
    )

    result = handler.perform_full_migration(
        strategy=strategy,
        key_types=["string"],
        key_filter=key_filter,
        overwrite_existing=True,
    )

    assert result["success"] is True
    handler._filter_keys_by_types.assert_not_called()
    key_filter.filter_names.assert_called_once_with([b"moved-into-scope"])
    key_filter.filter_batch.assert_not_called()
    handler._dump_restore_batch.assert_called_once_with(
        [b"moved-into-scope"], True, True
    )


@pytest.mark.parametrize("strategy", ["scan", "dump_restore"])
def test_full_migration_keeps_name_filters_before_atomic_capture(strategy):
    handler = FullMigrationHandler(MagicMock(), MagicMock())
    handler._estimate_key_count = MagicMock(return_value=1)
    handler._scan_page = MagicMock(return_value=(0, [b"excluded:key"]))
    handler._dump_restore_batch = MagicMock(
        return_value={"migrated": 0, "failed": 0, "skipped": 0}
    )

    result = handler.perform_full_migration(
        strategy=strategy,
        key_filter=KeySyncFilter(include_patterns=[b"managed:*"]),
        overwrite_existing=True,
    )

    assert result["success"] is True
    handler._dump_restore_batch.assert_not_called()


def test_full_batch_uses_exact_source_pexpiretime(monkeypatch):
    source = MagicMock()
    source.execute_command.return_value = 102_500
    source.pipeline.return_value = _pipeline([b"dump", 2500, 102_500])
    target = MagicMock()
    write_pipe = _pipeline([b"OK"])
    target.pipeline.return_value = write_pipe
    monotonic_values = iter([0, 100_000_000])
    monkeypatch.setattr(
        full_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(full_migration_module.time, "time", lambda: 100.0)

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"], preserve_ttl=True, overwrite_existing=True
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.restore.assert_called_once_with(
        b"key", 102_500, b"dump", replace=True, absttl=True
    )


def test_full_batch_pexpiretime_error_falls_back_to_pttl(monkeypatch):
    source = MagicMock()
    source.execute_command.return_value = 102_500
    source.pipeline.side_effect = [
        _pipeline([b"dump", 250, redis.ResponseError("NOPERM PEXPIRETIME")]),
        _pipeline([b"dump", 250]),
    ]
    target = MagicMock()
    write_pipe = _pipeline([b"OK"])
    target.pipeline.return_value = write_pipe
    monotonic_values = iter([0, 0, 100_000_000])
    monkeypatch.setattr(
        full_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(full_migration_module.time, "time", lambda: 100.0)

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"], preserve_ttl=True, overwrite_existing=True
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.restore.assert_called_once_with(
        b"key", 100_150, b"dump", replace=True, absttl=True
    )


def test_full_batch_old_target_reuses_captured_dump_and_deadline(monkeypatch):
    source = MagicMock()
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.pipeline.return_value = _pipeline([redis.ResponseError("ERR syntax error")])
    target.pexpireat.return_value = True
    monkeypatch.setattr(full_migration_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(full_migration_module.time, "time", lambda: 100.0)
    source_fallback = MagicMock(side_effect=AssertionError("unexpected source reread"))
    monkeypatch.setattr(
        full_migration_module,
        "_sync_key_fallback",
        source_fallback,
    )

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"], preserve_ttl=True, overwrite_existing=True
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    temporary_key = target.restore.call_args.args[0]
    target.restore.assert_called_once_with(temporary_key, 0, b"dump", replace=False)
    target.pexpireat.assert_called_once_with(temporary_key, 100_250)
    target.rename.assert_called_once_with(temporary_key, b"key")
    source_fallback.assert_not_called()


def test_full_batch_old_target_receives_exact_expiry_provenance(monkeypatch):
    source = MagicMock()
    source.execute_command.return_value = 102_500
    source.pipeline.return_value = _pipeline([b"dump", 2500, 102_500])
    target = MagicMock()
    target.pipeline.return_value = _pipeline(
        [redis.ResponseError("ERR syntax error")]
    )
    legacy_restore = MagicMock(return_value=True)
    monkeypatch.setattr(
        full_migration_module,
        "restore_dump_with_deadline",
        legacy_restore,
    )
    monotonic_values = iter([0, 100_000_000])
    monkeypatch.setattr(
        full_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"key"], preserve_ttl=True, overwrite_existing=True
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    legacy_restore.assert_called_once_with(
        target,
        b"key",
        b"dump",
        102_500,
        overwrite=True,
        prefer_absttl=False,
        expires_at_is_exact=True,
    )


@pytest.mark.parametrize("pttl", [0, -2])
def test_key_sync_deletes_target_when_key_expires_between_dump_and_pttl(pttl):
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.return_value = _pipeline([b"dump", pttl])
    target = MagicMock()

    assert sync_key_with_dump_restore(source, target, b"expiring") is True

    target.delete.assert_called_once_with(b"expiring")
    target.restore.assert_not_called()


def test_key_sync_deducts_elapsed_time_from_pttl(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    monotonic_values = iter([1_000_000_000, 1_100_000_000])
    monkeypatch.setattr(
        key_sync_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    assert sync_key_with_dump_restore(source, target, b"expiring") is True

    target.restore.assert_called_once_with(
        b"expiring", 100_150, b"dump", replace=True, absttl=True
    )


def test_key_sync_uses_exact_source_deadline_while_pttl_is_alive(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.return_value = 102_500
    source.pipeline.return_value = _pipeline([b"dump", 2500, 102_500])
    target = MagicMock()
    monotonic_values = iter([0, 100_000_000])
    monkeypatch.setattr(
        key_sync_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    assert sync_key_with_dump_restore(source, target, b"expiring") is True

    target.restore.assert_called_once_with(
        b"expiring", 102_500, b"dump", replace=True, absttl=True
    )


def test_key_sync_old_target_restores_temp_with_fixed_deadline(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.return_value = True
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    assert sync_key_with_dump_restore(source, target, b"key") is True

    temporary_key = target.restore.call_args_list[1].args[0]
    assert temporary_key.startswith(b"__redis_sync_tmp__:")
    assert target.restore.call_args_list == [
        call(b"key", 100_250, b"dump", replace=True, absttl=True),
        call(temporary_key, 0, b"dump", replace=False),
    ]
    target.pexpireat.assert_called_once_with(temporary_key, 100_250)
    target.rename.assert_called_once_with(temporary_key, b"key")


def test_key_sync_legacy_expiry_failure_preserves_old_target(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.side_effect = redis.ResponseError("NOPERM PEXPIREAT")
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    with pytest.raises(redis.ResponseError, match="NOPERM"):
        sync_key_with_dump_restore(source, target, b"key")

    temporary_key = target.restore.call_args_list[1].args[0]
    assert target.delete.call_args_list == [call(temporary_key)]
    assert call(b"key") not in target.delete.call_args_list
    target.rename.assert_not_called()


def test_key_sync_missing_temp_before_pexpireat_deadline_preserves_old_target(
    monkeypatch,
):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.return_value = False
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    with pytest.raises(RuntimeError, match="临时键"):
        sync_key_with_dump_restore(source, target, b"key")

    temporary_key = target.restore.call_args_list[1].args[0]
    assert target.delete.call_args_list == [call(temporary_key)]
    assert call(b"key") not in target.delete.call_args_list


def test_key_sync_missing_temp_at_pexpireat_after_deadline_deletes_old_target(
    monkeypatch,
):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.return_value = False
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    wall_clock = iter([100.0, 101.0])
    monkeypatch.setattr(key_sync_module.time, "time", lambda: next(wall_clock))

    assert sync_key_with_dump_restore(source, target, b"key") is True

    temporary_key = target.restore.call_args_list[1].args[0]
    assert target.delete.call_args_list == [call(b"key"), call(temporary_key)]


def test_key_sync_missing_legacy_temp_before_deadline_preserves_old_target(
    monkeypatch,
):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.return_value = True
    target.rename.side_effect = redis.ResponseError("ERR no such key")
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    with pytest.raises(redis.ResponseError, match="no such key"):
        sync_key_with_dump_restore(source, target, b"key")

    temporary_key = target.restore.call_args_list[1].args[0]
    assert target.delete.call_args_list == [call(temporary_key)]
    assert call(b"key") not in target.delete.call_args_list


def test_key_sync_old_target_renamenx_race_skips_and_cleans_temp(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.return_value = _pipeline([b"dump", 250])
    target = MagicMock()
    target.exists.return_value = False
    target.restore.side_effect = [redis.ResponseError("ERR syntax error"), b"OK"]
    target.pexpireat.return_value = True
    target.renamenx.return_value = False
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    assert sync_key_with_dump_restore(
        source, target, b"key", overwrite=False
    ) is False

    temporary_key = target.restore.call_args_list[1].args[0]
    target.renamenx.assert_called_once_with(temporary_key, b"key")
    assert target.delete.call_args_list == [call(temporary_key)]
    assert call(b"key") not in target.delete.call_args_list


def test_key_sync_type_fallback_uses_absolute_expiry(monkeypatch):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.side_effect = [
        _pipeline([b"dump", 250]),
        _watched_pipeline([b"dump", 250], value=b"value"),
    ]
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError(
        "ERR DUMP payload version or checksum are wrong"
    )
    target.pexpireat.return_value = True
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    monkeypatch.setattr(key_sync_module.time, "time", lambda: 100.0)

    assert sync_key_with_dump_restore(source, target, b"key") is True

    temporary_key = target.set.call_args.args[0]
    target.pexpireat.assert_called_once_with(temporary_key, 100_250)
    target.rename.assert_called_once_with(temporary_key, b"key")


def test_key_sync_type_fallback_missing_temp_after_deadline_deletes_old_target(
    monkeypatch,
):
    source = MagicMock()
    source.exists.return_value = True
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    source.pipeline.side_effect = [
        _pipeline([b"dump", 250]),
        _watched_pipeline([b"dump", 250], value=b"value"),
    ]
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError(
        "ERR DUMP payload version or checksum are wrong"
    )
    target.pexpireat.return_value = False
    monkeypatch.setattr(key_sync_module.time, "monotonic_ns", lambda: 0)
    wall_clock = iter([100.0, 100.0, 101.0])
    monkeypatch.setattr(key_sync_module.time, "time", lambda: next(wall_clock))

    assert sync_key_with_dump_restore(source, target, b"key") is True

    temporary_key = target.set.call_args.args[0]
    assert target.delete.call_args_list == [call(b"key"), call(temporary_key)]


def test_key_sync_rejects_invalid_negative_pttl():
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.return_value = _pipeline([b"dump", -3])

    with pytest.raises(ValueError, match="PTTL"):
        sync_key_with_dump_restore(source, MagicMock(), b"key")


@pytest.mark.parametrize(
    "source_state",
    ["missing-before-read", "missing-after-read", "expired-after-dump"],
)
def test_key_sync_does_not_delete_target_for_vanished_source_without_overwrite(
    source_state,
):
    source = MagicMock()
    target = MagicMock()
    target.exists.return_value = False
    if source_state == "missing-before-read":
        source.exists.return_value = False
    else:
        source.exists.return_value = True
        source.pipeline.return_value = _pipeline(
            [None, -2] if source_state == "missing-after-read" else [b"dump", 0]
        )

    assert sync_key_with_dump_restore(
        source, target, b"vanished", overwrite=False
    ) is False

    target.delete.assert_not_called()
    target.restore.assert_not_called()


@pytest.mark.parametrize(
    "message",
    [
        "OOM command not allowed when used memory > 'maxmemory'",
        "NOPERM this user has no permissions to run the 'restore' command",
        "READONLY You can't write against a read only replica",
    ],
)
def test_key_sync_does_not_fallback_for_operational_restore_errors(message):
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.return_value = _pipeline([b"dump", -1])
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError(message)

    with pytest.raises(redis.ResponseError, match=message.split()[0]):
        sync_key_with_dump_restore(source, target, b"key")

    source.type.assert_not_called()
    target.delete.assert_not_called()
    target.set.assert_not_called()
    target.rpush.assert_not_called()
    target.sadd.assert_not_called()
    target.zadd.assert_not_called()
    target.hset.assert_not_called()


def test_key_sync_compatibility_fallback_builds_temp_key_then_renames():
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.side_effect = [
        _pipeline([b"dump", -1]),
        _watched_pipeline([b"dump", -1], value=b"new-value"),
    ]
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError(
        "ERR DUMP payload version or checksum are wrong"
    )

    assert sync_key_with_dump_restore(source, target, b"key") is True

    temporary_key = target.set.call_args.args[0]
    assert temporary_key.startswith(b"__redis_sync_tmp__:")
    target.set.assert_called_once_with(temporary_key, b"new-value")
    target.rename.assert_called_once_with(temporary_key, b"key")
    assert call(b"key") not in target.delete.call_args_list


def test_key_sync_fallback_rejects_source_change_before_target_temp_write():
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.side_effect = [
        _pipeline([b"captured-dump", -1]),
        _watched_pipeline([b"changed-dump", -1], value=b"changed-value"),
    ]
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError(
        "ERR DUMP payload version or checksum are wrong"
    )

    with pytest.raises(key_sync_module.SourceStateChangedError):
        sync_key_with_dump_restore(source, target, b"key")

    target.set.assert_not_called()
    target.rename.assert_not_called()


def test_key_sync_failed_compatibility_fallback_keeps_old_target_value():
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.side_effect = [
        _pipeline([b"dump", -1]),
        _watched_pipeline(
            [b"dump", -1], key_type=b"list", values=[b"new-value"]
        ),
    ]
    target = MagicMock()
    target.restore.side_effect = redis.ResponseError("ERR syntax error")
    target.rpush.side_effect = redis.ResponseError("OOM temporary write failed")

    with pytest.raises(redis.ResponseError, match="OOM"):
        sync_key_with_dump_restore(source, target, b"key")

    temporary_key = target.rpush.call_args.args[0]
    target.delete.assert_called_once_with(temporary_key)
    assert call(b"key") not in target.delete.call_args_list
    target.rename.assert_not_called()
    target.renamenx.assert_not_called()


def test_full_batch_does_not_restore_expired_dump_as_persistent():
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", 0])
    target = MagicMock()
    write_pipe = _pipeline([1])
    target.pipeline.return_value = write_pipe

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"expiring"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 1, "failed": 0, "skipped": 0}
    write_pipe.delete.assert_called_once_with(b"expiring")
    write_pipe.restore.assert_not_called()


def test_scan_handler_preserves_binary_keys():
    source = MagicMock()
    source.scan.return_value = (0, [b"\xff\x00key"])
    handler = ScanHandler(source, MagicMock())
    handler.SCAN_RETRY_DELAY = 0

    assert list(handler.scan_keys()) == [[b"\xff\x00key"]]


def test_scan_handler_fails_closed_when_type_filter_query_fails():
    source = MagicMock()
    source.scan.return_value = (0, [b"key"])
    source.type.side_effect = redis.ResponseError("NOPERM TYPE")

    with pytest.raises(Exception, match="checking type"):
        list(ScanHandler(source, MagicMock()).scan_keys(key_type="hash"))


def test_fast_comparison_fails_closed_when_both_type_queries_are_denied():
    source = MagicMock()
    source.scan.return_value = (0, [b"key"])
    source.pipeline.return_value = _pipeline(
        [redis.ResponseError("NOPERM source TYPE")]
    )
    target = MagicMock()
    target.pipeline.return_value = _pipeline(
        [1, redis.ResponseError("NOPERM target TYPE")]
    )

    result = ScanHandler(source, target).compare_keys(use_fast_mode=True)

    assert result["total_compared"] == 1
    assert result["matching_keys"] == 0
    assert result["missing_in_target"] == 0
    assert len(result["errors"]) == 1
    assert "source TYPE" in result["errors"][0]
    assert "target TYPE" in result["errors"][0]


def test_scan_handler_does_not_force_replace_when_overwrite_is_disabled():
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.return_value = _pipeline([b"dump", -1])
    target = MagicMock()
    target.exists.side_effect = [False, False, True]
    target.restore.side_effect = redis.ResponseError("BUSYKEY")
    handler = ScanHandler(source, target)

    assert handler._migrate_single_key(b"key", overwrite=False) is False
    target.restore.assert_called_once_with(b"key", 0, b"dump", replace=False)


def test_incremental_detection_preserves_binary_keys():
    source = MagicMock()
    source.scan.return_value = (0, [b"\xff\x00key"])
    source.pipeline.return_value = _pipeline([0])
    handler = IncrementalMigrationHandler(source, MagicMock())
    handler.SCAN_RETRY_DELAY = 0

    changed = handler._detect_changes_by_idle_time(
        "*",
        None,
        0,
        10,
    )

    assert changed == [b"\xff\x00key"]


def test_incremental_failure_returns_false_and_keeps_checkpoint():
    handler = IncrementalMigrationHandler(MagicMock(), MagicMock())
    handler.last_sync_time = 123.0
    handler._detect_changed_keys = MagicMock(return_value=[b"key"])
    handler._sync_changed_keys = MagicMock(return_value={"synced": 0, "failed": 1})

    result = handler.perform_incremental_sync()

    assert result["success"] is False
    assert result["failed_keys"] == 1
    assert handler.last_sync_time == 123.0


def test_incremental_scan_failure_is_reported_after_retry_limit():
    source = MagicMock()
    source.scan.side_effect = redis.ConnectionError("source unavailable")
    handler = IncrementalMigrationHandler(source, MagicMock())
    handler.SCAN_RETRY_DELAY = 0

    result = handler.perform_incremental_sync(since_timestamp=1.0)

    assert result["success"] is False
    assert "SCAN" in result["error"]
    assert source.scan.call_count == handler.SCAN_MAX_RETRIES


def test_incremental_value_comparison_supports_streams_and_ttl_changes():
    source = MagicMock()
    target = MagicMock()
    source.dump.return_value = b"stream-dump"
    target.dump.return_value = b"stream-dump"
    source.pttl.return_value = 10_000
    target.pttl.return_value = 4_000
    target.exists.return_value = True

    different, reason = IncrementalMigrationHandler(
        source, target
    )._is_key_different(b"stream")

    assert different is True
    assert "TTL" in reason


def test_incremental_target_scan_failure_preserves_checkpoint():
    source = MagicMock()
    source.scan.return_value = (0, [])
    target = MagicMock()
    target.scan.side_effect = redis.ConnectionError("target unavailable")
    handler = IncrementalMigrationHandler(source, target)
    handler.SCAN_RETRY_DELAY = 0
    handler.last_sync_time = 123.0

    result = handler.perform_incremental_sync(since_timestamp=123.0)

    assert result["success"] is False
    assert handler.last_sync_time == 123.0
    assert target.scan.call_count == handler.SCAN_MAX_RETRIES


def test_incremental_target_only_detection_preserves_value_filtered_keys():
    source = MagicMock()
    target = MagicMock()
    target.scan.return_value = (0, [b"outside-size-scope"])
    target.pipeline.return_value = _pipeline([-1, 100])
    handler = IncrementalMigrationHandler(source, target)

    deleted = handler._detect_target_only_keys(
        "*",
        None,
        10,
        set(),
        KeySyncFilter(max_key_size=1),
    )

    assert deleted == []
    source.pipeline.assert_not_called()


def test_incremental_change_tuple_when_upserts_fill_limit():
    handler = IncrementalMigrationHandler(MagicMock(), MagicMock())
    handler._detect_changes_by_comparison = MagicMock(return_value=[b"upsert"])
    handler._detect_target_only_keys = MagicMock()

    detected = handler._detect_changed_keys(
        "*", None, 0, 1, return_deletions=True
    )

    assert detected == ([b"upsert"], set())
    handler._detect_target_only_keys.assert_not_called()


def test_incremental_limit_one_alternates_upserts_and_deletions_across_cycles():
    handler = IncrementalMigrationHandler(MagicMock(), MagicMock())
    handler._detect_changes_by_comparison = MagicMock(return_value=[b"hot-upsert"])
    handler._detect_target_only_keys = MagicMock(return_value=[b"stale-target"])

    first = handler._detect_changed_keys("*", None, 0, 1, return_deletions=True)
    second = handler._detect_changed_keys("*", None, 0, 1, return_deletions=True)
    third = handler._detect_changed_keys("*", None, 0, 1, return_deletions=True)

    assert first == ([b"hot-upsert"], set())
    assert second == ([b"stale-target"], {b"stale-target"})
    assert third == ([b"hot-upsert"], set())
    assert handler._detect_changes_by_comparison.call_count == 2
    assert handler._detect_target_only_keys.call_count == 1


def test_incremental_comparison_scan_duplicates_do_not_consume_change_budget():
    source = MagicMock()
    source.scan.side_effect = [
        (1, [b"duplicate"]),
        (0, [b"duplicate", b"later"]),
    ]
    handler = IncrementalMigrationHandler(source, MagicMock(), scan_count=4)
    handler._is_key_different = MagicMock(return_value=(True, "different"))

    changed = handler._detect_changes_by_comparison("*", None, 2, set())

    assert changed == [b"duplicate", b"later"]


def test_incremental_comparison_only_retains_selected_keys_across_pages():
    source = MagicMock()
    source.scan.side_effect = [
        (1, [b"changes-later"]),
        (0, [b"changes-later", b"later"]),
    ]
    handler = IncrementalMigrationHandler(source, MagicMock(), scan_count=4)
    comparisons = {b"changes-later": 0}

    def is_different(key):
        if key == b"changes-later":
            comparisons[key] += 1
            return comparisons[key] == 2, "changed on second page"
        return True, "different"

    handler._is_key_different = is_different

    changed = handler._detect_changes_by_comparison("*", None, 2, set())

    assert changed == [b"changes-later", b"later"]


def test_incremental_target_scan_duplicates_do_not_consume_deletion_budget():
    source = MagicMock()
    source.pipeline.side_effect = [_pipeline([0]), _pipeline([0])]
    target = MagicMock()
    target.scan.side_effect = [
        (1, [b"duplicate"]),
        (0, [b"duplicate", b"later"]),
    ]
    handler = IncrementalMigrationHandler(source, target, scan_count=2)

    deleted = handler._detect_target_only_keys("*", None, 2, set())

    assert deleted == [b"duplicate", b"later"]


def test_incremental_target_scan_only_retains_selected_keys_across_pages():
    source = MagicMock()
    source.pipeline.side_effect = [
        _pipeline([1]),
        _pipeline([0, 0]),
    ]
    target = MagicMock()
    target.scan.side_effect = [
        (1, [b"deleted-later"]),
        (0, [b"deleted-later", b"later"]),
    ]
    handler = IncrementalMigrationHandler(source, target, scan_count=2)

    deleted = handler._detect_target_only_keys("*", None, 2, set())

    assert deleted == [b"deleted-later", b"later"]


def test_incremental_zero_change_limit_returns_compatible_empty_tuple():
    handler = IncrementalMigrationHandler(MagicMock(), MagicMock())
    handler._detect_changes_by_comparison = MagicMock(return_value=[])
    handler._detect_target_only_keys = MagicMock()

    detected = handler._detect_changed_keys(
        "*", None, 0, 0, return_deletions=True
    )

    assert detected == ([], set())
    handler._detect_target_only_keys.assert_not_called()


def test_incremental_stop_timeout_keeps_old_worker_and_stop_event():
    source = MagicMock()
    handler = IncrementalMigrationHandler(source, MagicMock())
    handler.THREAD_JOIN_TIMEOUT = 0.01
    entered = threading.Event()
    release = threading.Event()
    worker = None

    def blocked_sync(*_args, **_kwargs):
        entered.set()
        release.wait(timeout=2)
        return {"success": True}

    handler.perform_incremental_sync = blocked_sync
    try:
        assert handler.start_incremental_sync(sync_interval=60) is True
        assert entered.wait(timeout=1)
        worker = handler.monitor_thread

        handler.stop_incremental_sync()

        assert worker is not None and worker.is_alive()
        assert handler.is_monitoring is True
        assert handler.stop_event.is_set()
        assert handler.start_incremental_sync(sync_interval=60) is False
        assert handler.monitor_thread is worker
        assert handler.stop_event.is_set()
    finally:
        release.set()
        if worker is not None:
            worker.join(timeout=1)

    assert worker is not None and not worker.is_alive()
    assert handler.is_monitoring is False
    assert handler.monitor_thread is None


def test_incremental_max_key_size_preflight_skips_dump_for_oversized_key():
    class OversizedPipeline:
        def __init__(self):
            self.dump_calls = 0
            self.reset_calls = 0

        def watch(self, _key):
            return self

        def pttl(self, _key):
            return -1

        def execute_command(self, command, subcommand, _key):
            assert (command, subcommand) == ("MEMORY", "USAGE")
            return 101

        def multi(self):
            return self

        def dump(self, _key):
            self.dump_calls += 1
            return self

        def execute(self, **_kwargs):
            raise AssertionError("oversized key reached DUMP transaction")

        def reset(self):
            self.reset_calls += 1

    source = MagicMock()
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    watched = OversizedPipeline()
    source.pipeline.return_value = watched
    handler = IncrementalMigrationHandler(source, MagicMock())

    captured = handler._capture_scoped_source_key(
        b"oversized",
        None,
        KeySyncFilter(max_key_size=100),
    )

    assert captured == (False, None, None, False)
    assert watched.dump_calls == 0
    assert watched.reset_calls == 1


@pytest.mark.parametrize(
    ("key", "key_types", "key_filter", "sample"),
    [
        (b"wrong-type", ["string"], None, [b"dump", -1, b"list"]),
        (b"short-ttl", None, KeySyncFilter(min_ttl=10), [b"dump", 9_000]),
        (b"too-large", None, KeySyncFilter(max_key_size=100), [b"dump", -1, 101]),
    ],
)
def test_incremental_atomic_source_capture_rejects_dynamic_filter_misses(
    key, key_types, key_filter, sample
):
    source = MagicMock()
    source.pipeline.return_value = _pipeline(sample)
    handler = IncrementalMigrationHandler(source, MagicMock())

    in_scope, dump_data, expires_at, expiry_is_exact = (
        handler._capture_scoped_source_key(
            key, key_types, key_filter
        )
    )

    assert (in_scope, dump_data, expires_at, expiry_is_exact) == (
        False,
        None,
        None,
        False,
    )


def test_incremental_atomic_source_capture_rechecks_name_filter():
    source = MagicMock()
    target = MagicMock()
    handler = IncrementalMigrationHandler(source, target)
    key_filter = KeySyncFilter(include_patterns=[b"managed:*"])

    captured = handler._capture_scoped_source_key(
        b"excluded", None, key_filter
    )

    assert captured == (False, None, None, False)
    assert handler._sync_single_key(b"excluded", None, key_filter) is True
    source.pipeline.assert_not_called()
    target.execute_command.assert_not_called()
    target.delete.assert_not_called()


def test_incremental_atomic_capture_pexpiretime_error_falls_back_to_pttl(
    monkeypatch,
):
    source = MagicMock()
    source.execute_command.return_value = 102_500
    source.pipeline.side_effect = [
        _pipeline(
            [b"dump", 250, redis.ResponseError("NOPERM PEXPIRETIME"), b"string"]
        ),
        _pipeline([b"dump", 250, b"string"]),
    ]
    monotonic_values = iter([0, 0, 100_000_000])
    monkeypatch.setattr(
        incremental_migration_module.time,
        "monotonic_ns",
        lambda: next(monotonic_values),
    )
    monkeypatch.setattr(incremental_migration_module.time, "time", lambda: 100.0)

    captured = IncrementalMigrationHandler(
        source, MagicMock()
    )._capture_scoped_source_key(b"key", ["string"], None)

    assert captured == (True, b"dump", 100_150, False)


def test_incremental_upsert_moved_out_of_scope_uses_filtered_target_delete():
    key = b"grew-after-detection"
    source = MagicMock()
    source.exists.return_value = True
    source.pipeline.side_effect = [
        _pipeline([b"dump", -1, 101]),
        _pipeline([-1, 101]),
        _pipeline([-1, 101]),
    ]
    target = MagicMock()
    target.execute_command.return_value = 1
    handler = IncrementalMigrationHandler(source, target)

    with patch(
        "redis_sync.incremental_migration_handler.restore_dump_with_deadline"
    ) as restore:
        assert handler._sync_single_key(
            key, None, KeySyncFilter(max_key_size=100)
        ) is True

    restore.assert_not_called()
    assert target.execute_command.call_args.args[0] == b"EVAL"
    target.delete.assert_not_called()


def test_incremental_filtered_upsert_missing_at_capture_never_plain_deletes_target():
    key = b"expired-during-capture"
    source = MagicMock()
    source.exists.return_value = False
    source.pipeline.return_value = _pipeline([None, -2, None])
    target = MagicMock()
    target.execute_command.return_value = 1
    handler = IncrementalMigrationHandler(source, target)

    assert handler._sync_single_key(
        key, None, KeySyncFilter(max_key_size=100)
    ) is True

    assert target.execute_command.call_args.args[0] == b"EVAL"
    target.delete.assert_not_called()


def test_incremental_filtered_upsert_repairs_scope_exit_after_restore():
    key = b"changed-after-capture"
    source = MagicMock()
    source.pipeline.return_value = _pipeline([b"dump", -1, 50])
    target = MagicMock()
    target.execute_command.return_value = 1
    handler = IncrementalMigrationHandler(source, target)
    handler._source_key_in_scope = MagicMock(side_effect=[False, False, False])

    with patch(
        "redis_sync.incremental_migration_handler.restore_dump_with_deadline",
        return_value=True,
    ) as restore:
        assert handler._sync_single_key(
            key, None, KeySyncFilter(max_key_size=100)
        ) is True

    restore.assert_called_once_with(
        target,
        key,
        b"dump",
        None,
        overwrite=True,
        expires_at_is_exact=False,
    )
    assert target.execute_command.call_args.args[0] == b"EVAL"
    target.delete.assert_not_called()


def test_incremental_filtered_compatibility_fallback_keeps_captured_identity():
    key = b"changed-before-legacy-fallback"
    source = MagicMock()
    target = MagicMock()
    handler = IncrementalMigrationHandler(source, target)
    handler._capture_scoped_source_key = MagicMock(
        return_value=(True, b"captured-dump", None, False)
    )
    key_filter = KeySyncFilter(min_ttl=10, max_key_size=100)

    with patch(
        "redis_sync.incremental_migration_handler.restore_dump_with_deadline",
        side_effect=redis.ResponseError(
            "ERR DUMP payload version or checksum are wrong"
        ),
    ), patch(
        "redis_sync.incremental_migration_handler._sync_key_fallback",
        side_effect=key_sync_module.SourceStateChangedError("source changed"),
    ) as fallback:
        assert handler._sync_single_key(key, ["string"], key_filter) is False

    fallback.assert_called_once_with(
        source,
        target,
        key,
        -1,
        True,
        overwrite=True,
        expires_at_ms=None,
        expected_dump=b"captured-dump",
        key_types=["string"],
        min_ttl=10,
        max_key_size=100,
        expires_at_is_exact=False,
    )
    target.set.assert_not_called()
    target.rename.assert_not_called()


def test_incremental_filtered_legacy_fallback_receives_exact_expiry(monkeypatch):
    key = b"exact-expiry"
    source = MagicMock()
    target = MagicMock()
    handler = IncrementalMigrationHandler(source, target)
    handler._capture_scoped_source_key = MagicMock(
        return_value=(True, b"captured-dump", 102_500, True)
    )
    handler._source_key_in_scope = MagicMock(return_value=True)
    monkeypatch.setattr(incremental_migration_module.time, "time", lambda: 999.0)

    with patch(
        "redis_sync.incremental_migration_handler.restore_dump_with_deadline",
        side_effect=redis.ResponseError(
            "ERR DUMP payload version or checksum are wrong"
        ),
    ) as restore, patch(
        "redis_sync.incremental_migration_handler._sync_key_fallback",
        return_value=True,
    ) as fallback:
        assert handler._sync_single_key(
            key, ["string"], KeySyncFilter(min_ttl=10)
        ) is True

    restore.assert_called_once_with(
        target,
        key,
        b"captured-dump",
        102_500,
        overwrite=True,
        expires_at_is_exact=True,
    )
    assert fallback.call_args.kwargs["expires_at_is_exact"] is True


def test_incremental_value_filter_deletion_candidate_is_deleted_not_copied():
    key = b"left-size-filter"
    source = MagicMock()
    source.exists.return_value = True
    target = MagicMock()
    source.pipeline.return_value = _pipeline([-1, 101])
    target.execute_command.return_value = 1
    key_filter = KeySyncFilter(max_key_size=100)
    handler = IncrementalMigrationHandler(source, target)
    handler._detect_changed_keys = MagicMock(return_value=([key], {key}))

    with patch(
        "redis_sync.incremental_migration_handler.sync_key_with_dump_restore"
    ) as sync_key:
        result = handler.perform_incremental_sync(key_filter=key_filter)

    assert result["success"] is True
    delete_command = target.execute_command.call_args.args
    assert delete_command[0] == b"EVAL"
    assert delete_command[2:4] == (b"1", key)
    assert delete_command[4:7] == (b"0", b"100", b"0")
    target.delete.assert_not_called()
    sync_key.assert_not_called()


def test_incremental_deletion_candidate_reentry_is_resynced_after_delete():
    key = b"reentered-size-filter"
    source = MagicMock()
    source.exists.return_value = True
    target = MagicMock()
    source.pipeline.side_effect = [
        _pipeline([-1, 101]),
        _pipeline([-1, 50]),
        _pipeline([b"dump", -1, 50]),
        _pipeline([-1, 50]),
    ]
    target.execute_command.return_value = 1
    key_filter = KeySyncFilter(max_key_size=100)
    handler = IncrementalMigrationHandler(source, target)
    handler._detect_changed_keys = MagicMock(return_value=([key], {key}))

    with patch(
        "redis_sync.incremental_migration_handler.sync_key_with_dump_restore",
        return_value=True,
    ) as sync_key:
        result = handler.perform_incremental_sync(key_filter=key_filter)

    assert result["success"] is True
    assert target.execute_command.call_args.args[0] == b"EVAL"
    target.delete.assert_not_called()
    sync_key.assert_not_called()
    target.restore.assert_called_once_with(key, 0, b"dump", replace=True)


def _atomic_delete_result(command, *, key_type=b"string", ttl=-1, size=1):
    key_count = int(command[2])
    argument_offset = 3 + key_count
    min_ttl = int(command[argument_offset])
    max_key_size = int(command[argument_offset + 1])
    type_count = int(command[argument_offset + 2])
    allowed_types = command[argument_offset + 3:argument_offset + 3 + type_count]
    type_allowed = not allowed_types or key_type in allowed_types
    ttl_allowed = min_ttl <= 0 or ttl == -1 or ttl >= min_ttl
    size_allowed = max_key_size <= 0 or size is None or size <= max_key_size
    return int(type_allowed and ttl_allowed and size_allowed)


@pytest.mark.parametrize(
    (
        "key_types",
        "key_filter",
        "target_scan_state",
        "source_scan_state",
        "target_state",
    ),
    [
        (["string"], None, [b"string"], [0, b"none"], {"key_type": b"list"}),
        (None, KeySyncFilter(min_ttl=10), [30], [0], {"ttl": 9}),
        (None, KeySyncFilter(max_key_size=100), [-1, 50], [0], {"size": 101}),
    ],
)
def test_incremental_deletion_atomically_preserves_target_moved_out_of_scope(
    key_types,
    key_filter,
    target_scan_state,
    source_scan_state,
    target_state,
):
    key = b"moved-out-after-scan"
    source = MagicMock()
    source.exists.return_value = False
    source.pipeline.return_value = _pipeline(source_scan_state)
    target = MagicMock()
    target.scan.return_value = (0, [key])
    target.pipeline.return_value = _pipeline(target_scan_state)
    target.execute_command.side_effect = lambda *command: _atomic_delete_result(
        command, **target_state
    )
    handler = IncrementalMigrationHandler(source, target)

    candidates = handler._detect_target_only_keys(
        "*", key_types, 1, set(), key_filter
    )
    assert candidates == [key]
    assert handler._sync_deletion_candidate(key, key_types, key_filter) is True

    assert target.execute_command.call_args.args[0] == b"EVAL"
    target.delete.assert_not_called()
    assert handler.incremental_stats["change_types"]["deleted"] == 0


class _BatchRecordingPipeline:
    def __init__(self, client, transaction):
        self.client = client
        self.transaction = transaction
        self.operations = []

    def _add(self, command, key, *args):
        self.operations.append((command, key, args))
        return self

    def type(self, key):
        return self._add("type", key)

    def object(self, subcommand, key):
        assert subcommand == "idletime"
        return self._add("object", key)

    def exists(self, key):
        return self._add("exists", key)

    def dump(self, key):
        return self._add("dump", key)

    def pttl(self, key):
        return self._add("pttl", key)

    def execute_command(self, command, subcommand, key):
        assert (command, subcommand) == ("MEMORY", "USAGE")
        return self._add("memory", key)

    def restore(self, key, ttl, dump_data, replace=False):
        return self._add("restore", key, ttl, dump_data, replace)

    def delete(self, key):
        return self._add("delete", key)

    def execute(self, **_kwargs):
        self.client.pipeline_calls.append(
            (self.transaction, list(self.operations))
        )
        results = []
        for command, key, _args in self.operations:
            if command == "type":
                results.append(b"string")
            elif command == "object":
                results.append(0)
            elif command == "exists":
                results.append(int(key in self.client.existing_keys))
            elif command == "dump":
                results.append(b"dump:" + key)
            elif command == "pttl":
                results.append(-1)
            elif command == "memory":
                results.append(len(b"dump:" + key))
            elif command == "delete":
                results.append(1)
            else:
                results.append(b"OK")
        return results


class _BatchRecordingRedis:
    def __init__(self, keys=(), existing_keys=()):
        self.keys = list(keys)
        self.existing_keys = set(existing_keys)
        self.pipeline_calls = []

    def scan(self, cursor=0, match="*", count=1000):
        return 0, list(self.keys)

    def pipeline(self, transaction=False):
        return _BatchRecordingPipeline(self, transaction)

    def execute_command(self, *_args):
        raise redis.ResponseError("unknown command PEXPIRETIME")


def _recorded_batch_sizes(client, command):
    return [
        sum(operation[0] == command for operation in operations)
        for _transaction, operations in client.pipeline_calls
        if any(operation[0] == command for operation in operations)
    ]


@pytest.mark.parametrize(
    ("method_name", "worker_name"),
    [
        ("_migrate_with_scan", "_migrate_key_batch"),
        ("_migrate_with_dump_restore", "_dump_restore_batch"),
    ],
)
def test_full_scan_honors_configured_worker_batch_without_type_prefilter(
    method_name, worker_name
):
    keys = [f"key-{index}".encode() for index in range(205)]
    source = _BatchRecordingRedis(keys)
    handler = FullMigrationHandler(source, _BatchRecordingRedis())
    handler._estimate_key_count = MagicMock(return_value=len(keys))
    worker = MagicMock(
        side_effect=lambda chunk, *_args: {
            "migrated": len(chunk),
            "failed": 0,
            "skipped": 0,
        }
    )
    setattr(handler, worker_name, worker)

    result = getattr(handler, method_name)(
        True, 73, 1000, None, "*", ["string"], True
    )

    assert result["migrated_keys"] == len(keys)
    assert _recorded_batch_sizes(source, "type") == []
    assert [len(call_args.args[0]) for call_args in worker.call_args_list] == [
        73, 73, 59
    ]


def test_direct_full_dump_restore_batch_is_hard_capped_at_200_keys():
    keys = [f"key-{index}".encode() for index in range(405)]
    source = _BatchRecordingRedis(keys)
    target = _BatchRecordingRedis(existing_keys=keys)

    handler = FullMigrationHandler(source, target)
    handler._active_full_params = {"key_types": ["string"]}

    result = handler._dump_restore_batch(
        keys,
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 405, "failed": 0, "skipped": 0}
    assert _recorded_batch_sizes(source, "dump") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "pttl") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "type") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "memory") == [200, 200, 5]
    assert _recorded_batch_sizes(target, "restore") == [200, 200, 5]


def test_full_dump_restore_flushes_target_pipeline_at_payload_budget(monkeypatch):
    source = MagicMock()
    source.pipeline.side_effect = [
        _pipeline([6, 6]),
        _pipeline([b"123456", -1]),
        _pipeline([b"abcdef", -1]),
    ]
    target = MagicMock()
    first_write = _pipeline([b"OK"])
    second_write = _pipeline([b"OK"])
    target.pipeline.side_effect = [first_write, second_write]
    monkeypatch.setattr(full_migration_module, "MAX_DUMP_BATCH_BYTES", 10)

    result = FullMigrationHandler(source, target)._dump_restore_batch(
        [b"first", b"second"],
        preserve_ttl=True,
        overwrite_existing=True,
    )

    assert result == {"migrated": 2, "failed": 0, "skipped": 0}
    first_write.restore.assert_called_once_with(
        b"first", 0, b"123456", replace=True
    )
    second_write.restore.assert_called_once_with(
        b"second", 0, b"abcdef", replace=True
    )


def test_incremental_idletime_type_and_object_pipelines_are_hard_capped():
    keys = [f"key-{index}".encode() for index in range(405)]
    source = _BatchRecordingRedis(keys)
    handler = IncrementalMigrationHandler(source, _BatchRecordingRedis())

    changed = handler._detect_changes_by_idle_time(
        "*", ["string"], time.time(), 500
    )

    assert changed == keys
    assert _recorded_batch_sizes(source, "type") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "object") == [200, 200, 5]


def test_incremental_target_only_pipelines_are_hard_capped():
    keys = [f"key-{index}".encode() for index in range(405)]
    source = _BatchRecordingRedis()
    target = _BatchRecordingRedis(keys)
    handler = IncrementalMigrationHandler(source, target)

    deleted = handler._detect_target_only_keys(
        "*", ["string"], 500, set()
    )

    assert deleted == keys
    assert _recorded_batch_sizes(target, "type") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "exists") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "type") == [200, 200, 5]


def test_scan_verification_type_prefilter_is_hard_capped():
    keys = [f"key-{index}".encode() for index in range(405)]
    source = _BatchRecordingRedis(keys)

    batches = list(
        ScanHandler(source, _BatchRecordingRedis())._verification_batches(
            "*", ["string"], None
        )
    )

    assert batches == [keys]
    assert _recorded_batch_sizes(source, "type") == [200, 200, 5]


def test_scan_fast_compare_pipelines_are_hard_capped():
    keys = [f"key-{index}".encode() for index in range(405)]
    source = _BatchRecordingRedis(keys, existing_keys=keys)
    target = _BatchRecordingRedis(keys, existing_keys=keys)

    result = ScanHandler(source, target).compare_keys(use_fast_mode=True)

    assert result["total_compared"] == 405
    assert result["matching_keys"] == 405
    assert _recorded_batch_sizes(target, "exists") == [200, 200, 5]
    assert _recorded_batch_sizes(target, "type") == [200, 200, 5]
    assert _recorded_batch_sizes(source, "type") == [200, 200, 5]
