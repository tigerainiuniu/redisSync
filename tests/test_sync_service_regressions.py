import hashlib
import logging
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis

import redis_sync.sync_service as sync_service_module
from redis_sync.migration_orchestrator import MigrationConfig
from redis_sync.sync_filters import KeySyncFilter
from redis_sync.sync_service import RedisSyncService, SourceChangeSet, SyncStats


class FakeSourcePipeline:
    def __init__(self, source):
        self.source = source
        self.operations = []

    def dump(self, key):
        self.operations.append(("dump", key))
        return self

    def pttl(self, key):
        self.operations.append(("pttl", key))
        return self

    def type(self, key):
        self.operations.append(("type", key))
        return self

    def execute_command(self, command, *arguments):
        key = arguments[-1]
        operation = command.lower()
        if operation == "memory":
            operation = "memory"
        self.operations.append((operation, key))
        return self

    def execute(self, **kwargs):
        self.source.pipeline_operations.append(list(self.operations))
        results = []
        for command, key in self.operations:
            value = self.source.values.get(key)
            if command == "dump":
                results.append(value[0] if value else None)
            elif command == "pttl":
                results.append(value[1] if value else -2)
            elif command == "pexpiretime":
                results.append(value[2] if value and len(value) > 2 else -1)
            elif command == "memory":
                results.append(
                    value[4]
                    if value and len(value) > 4
                    else len(value[0]) if value and value[0] is not None else None
                )
            else:
                results.append(
                    value[3]
                    if value and len(value) > 3
                    else b"string" if value else b"none"
                )
        return results


class FakeSource:
    def __init__(self, values):
        self.values = values
        self.pipeline_transactions = []
        self.pipeline_operations = []
        self.pexpiretime_probes = []

    def scan(self, cursor=0, match="*", count=1000):
        return 0, list(self.values)

    def pipeline(self, transaction=False):
        self.pipeline_transactions.append(transaction)
        return FakeSourcePipeline(self)

    def execute_command(self, command, key):
        assert command.upper() == "PEXPIRETIME"
        self.pexpiretime_probes.append(key)
        value = self.values.get(key)
        result = value[2] if value and len(value) > 2 else -1
        if isinstance(result, BaseException):
            raise result
        return result


class RecordingTargetPipeline:
    def __init__(self, target):
        self.target = target
        self.operations = []

    def restore(self, key, ttl, dump_data, replace=False, absttl=False):
        self.operations.append(
            ("restore", key, ttl, dump_data, replace, absttl)
        )
        return self

    def delete(self, key):
        self.operations.append(("delete", key))
        return self

    def type(self, key):
        self.operations.append(("type", key))
        return self

    def ttl(self, key):
        self.operations.append(("ttl", key))
        return self

    def execute_command(self, command, subcommand, key):
        assert (command.upper(), subcommand.upper()) == ("MEMORY", "USAGE")
        self.operations.append(("memory", key))
        return self

    def execute(self, **_kwargs):
        self.target.pipeline_operations.append(list(self.operations))
        self.target.operations.extend(self.operations)
        results = []
        for operation in self.operations:
            command, key = operation[:2]
            if command == "type":
                results.append(self.target.key_types.get(key, b"string"))
            elif command == "ttl":
                results.append(self.target.key_ttls.get(key, -1))
            elif command == "memory":
                results.append(self.target.key_memory.get(key))
            else:
                results.append(True)
        return results


class RecordingTarget:
    def __init__(self):
        self.operations = []
        self.pipeline_operations = []
        self.eval_batches = []
        self.before_eval = None
        self.scan_keys = []
        self.key_types = {}
        self.key_ttls = {}
        self.key_memory = {}

    def pipeline(self, transaction=False):
        assert transaction is False
        return RecordingTargetPipeline(self)

    def scan(self, cursor=0, match="*", count=1000):
        return 0, list(self.scan_keys)

    def execute_command(self, command, _script, key_count, *arguments):
        assert command == b"EVAL"
        count = int(key_count)
        keys = arguments[:count]
        self.eval_batches.append(list(keys))
        if callable(self.before_eval):
            self.before_eval()
        filter_args = arguments[count:]
        min_ttl = int(filter_args[0])
        max_key_size = int(filter_args[1])
        type_count = int(filter_args[2])
        allowed_types = set(filter_args[3:3 + type_count])
        deleted = 0
        for key in keys:
            exists = (
                key in self.scan_keys
                or key in self.key_types
                or key in self.key_ttls
                or key in self.key_memory
            )
            allowed = exists
            if allowed and allowed_types:
                allowed = self.key_types.get(key, b"string") in allowed_types
            if allowed and min_ttl > 0:
                ttl = self.key_ttls.get(key, -1)
                allowed = ttl == -1 or ttl >= min_ttl
            if allowed and max_key_size > 0:
                memory = self.key_memory.get(key)
                allowed = memory is None or memory <= max_key_size
            if allowed:
                self.operations.append(("delete", key))
                deleted += 1
        return deleted


class FailOnSecondTargetPipeline(RecordingTargetPipeline):
    def execute(self, **_kwargs):
        if len(self.target.pipeline_operations) == 1:
            self.target.pipeline_operations.append(list(self.operations))
            raise RuntimeError("second target batch failed")
        return super().execute()


class FailOnSecondTarget(RecordingTarget):
    def pipeline(self, transaction=False):
        assert transaction is False
        return FailOnSecondTargetPipeline(self)


def make_service(source, previous=None):
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "sync": {
            "incremental_sync": {
                "key_pattern": "*",
                "key_types": None,
                "max_changes_per_sync": 100,
            }
        },
        "service": {
            "performance": {"scan_count": 100},
            "failover": {"max_failures": 2},
        },
    }
    service.source_conn = source
    service.orchestrators = {}
    service._sync_key_filter = None
    service._source_snapshot = previous
    service._target_pending_deletions = {}
    service.logger = logging.getLogger("test-sync-service")
    return service


def test_service_sync_config_falls_back_to_full_verification():
    service = make_service(FakeSource({}))
    service.config["sync"]["mode"] = "full"
    service.config["sync"]["full_sync"] = {}

    assert service._create_sync_config().verify_mode == "full"


def make_realtime_alignment_service(source, target, bootstrap_snapshot):
    service = make_service(source)
    service.config["sync"].update(
        mode="hybrid",
        full_sync={"clear_target": False},
    )
    service.target_connections = {"target": object()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service._realtime_bootstrap_snapshot = bootstrap_snapshot
    service._realtime_baseline_established = False
    service._perform_full_sync = MagicMock(return_value=True)
    service._deactivate_realtime_target = MagicMock()
    return service


def test_fullresync_snapshot_stop_does_not_wait_for_alignment_executor():
    service = make_realtime_alignment_service(
        FakeSource({}), RecordingTarget(), None
    )
    service.shutdown_event = threading.Event()
    alignment_started = threading.Event()
    allow_alignment_exit = threading.Event()
    results = []

    def blocked_alignment(*_args, **_kwargs):
        alignment_started.set()
        allow_alignment_exit.wait(1)
        return False

    service._align_realtime_target = blocked_alignment
    callback = threading.Thread(
        target=lambda: results.append(
            service._apply_replication_snapshot(b"snapshot")
        )
    )
    callback.start()
    assert alignment_started.wait(1)

    started = time.monotonic()
    service.shutdown_event.set()
    callback.join(timeout=0.5)

    assert time.monotonic() - started < 0.5
    assert not callback.is_alive()
    assert results == [False]
    assert service._wait_for_alignment_futures(0) is False

    allow_alignment_exit.set()
    assert service._wait_for_alignment_futures(1) is True


def test_snapshot_detects_binary_key_update_and_deletion():
    old = hashlib.sha256(b"old").digest()
    source = FakeSource({b"\xffbinary": (b"new", -1)})
    service = make_service(
        source,
        previous={b"\xffbinary": (old, None), b"deleted": (old, None)},
    )

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.upserts == [b"\xffbinary"]
    assert changes.deletions == [b"deleted"]
    assert source.pipeline_transactions == [True]


def test_snapshot_uses_configured_bounded_dump_batches():
    source = FakeSource(
        {
            b"one": (b"1", -1),
            b"two": (b"2", -1),
            b"three": (b"3", -1),
        }
    )
    service = make_service(source)
    service.config["service"]["performance"]["pipeline_batch_size"] = 2

    snapshot = service._build_source_snapshot()

    assert set(snapshot) == {b"one", b"two", b"three"}
    assert source.pipeline_transactions == [False, False, True, True]
    assert [len(operations) for operations in source.pipeline_operations] == [2, 1, 6, 3]


def test_snapshot_dump_groups_honor_payload_budget(monkeypatch):
    source = FakeSource(
        {
            b"first": (b"123456", -1, -1, b"string", 6),
            b"second": (b"abcdef", -1, -1, b"string", 6),
        }
    )
    service = make_service(source)
    monkeypatch.setattr(sync_service_module, "MAX_DUMP_BATCH_BYTES", 10)

    snapshot = service._build_source_snapshot()

    assert set(snapshot) == {b"first", b"second"}
    dump_batches = [
        [operation for operation in batch if operation[0] == "dump"]
        for batch in source.pipeline_operations
        if any(operation[0] == "dump" for operation in batch)
    ]
    assert [len(batch) for batch in dump_batches] == [1, 1]


def test_pipeline_batch_size_is_capped_for_programmatic_config():
    service = make_service(FakeSource({}))
    service.config["service"]["performance"]["pipeline_batch_size"] = 10_000

    assert service._pipeline_batch_size() == 200


def test_snapshot_type_lookups_use_configured_pipeline_batch_size():
    source = FakeSource(
        {
            f"key-{index}".encode(): (
                f"dump-{index}".encode(),
                -1,
                -1,
                b"string",
            )
            for index in range(5)
        }
    )
    service = make_service(source)
    service.config["service"]["performance"]["pipeline_batch_size"] = 2
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]

    snapshot = service._build_source_snapshot()

    assert len(snapshot) == 5
    assert source.pipeline_transactions == [
        False, False, False,
        False, False, False,
        True, True, True,
    ]
    assert [len(batch) for batch in source.pipeline_operations] == [
        2, 2, 1,
        2, 2, 1,
        6, 6, 3,
    ]


def test_type_pipeline_error_aborts_snapshot_without_inventing_deletions():
    old = hashlib.sha256(b"old").digest()
    previous = {b"key": (old, None)}
    source = FakeSource(
        {
            b"key": (
                b"new",
                -1,
                -1,
                redis.ResponseError("NOPERM TYPE"),
            )
        }
    )
    service = make_service(source, previous=previous)
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]

    assert service._scan_source_for_changes() is None
    assert service._source_snapshot == previous


def test_redis6_expiry_fallback_jitter_does_not_repeat_upsert(monkeypatch):
    dump_data = b"same-value"
    source = FakeSource(
        {
            b"key": (
                dump_data,
                5000,
                redis.ResponseError("unknown command PEXPIRETIME"),
            )
        }
    )
    previous = {
        b"key": (hashlib.sha256(dump_data).digest(), 105_000, False),
    }
    service = make_service(source, previous=previous)
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.5)
    monkeypatch.setattr(sync_service_module.time, "monotonic_ns", lambda: 0)

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.upserts == []
    assert changes.deletions == []
    assert source.pexpiretime_probes == [b"key"]
    assert source.pipeline_operations == [[("dump", b"key"), ("pttl", b"key")]]


def test_redis7_exact_expiry_change_is_not_hidden_by_fallback_tolerance(
    monkeypatch,
):
    dump_data = b"same-value"
    source = FakeSource({b"key": (dump_data, 5000, 105_500)})
    previous = {
        b"key": (hashlib.sha256(dump_data).digest(), 105_000, True),
    }
    service = make_service(source, previous=previous)
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.5)

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.upserts == [b"key"]


def test_redis6_snapshot_drops_key_that_expires_during_pipeline_delay(monkeypatch):
    source = FakeSource(
        {
            b"key": (
                b"dump",
                500,
                redis.ResponseError("unknown command PEXPIRETIME"),
            )
        }
    )
    service = make_service(source)
    monotonic_clock = iter((0, 1_000_000_000))
    monkeypatch.setattr(
        sync_service_module.time, "monotonic_ns", lambda: next(monotonic_clock)
    )
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.0)

    assert service._build_source_snapshot() == {}


def test_redis7_snapshot_uses_pttl_for_liveness(monkeypatch):
    source = FakeSource({b"key": (b"dump", 500, 999_999)})
    service = make_service(source)
    monotonic_clock = iter((0, 1_000_000_000))
    monkeypatch.setattr(
        sync_service_module.time, "monotonic_ns", lambda: next(monotonic_clock)
    )
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.0)

    assert service._build_source_snapshot() == {}


def test_source_snapshot_rejects_invalid_pttl():
    service = make_service(FakeSource({b"key": (b"dump", -3)}))

    with pytest.raises(ValueError, match="PTTL"):
        service._build_source_snapshot()


def test_snapshot_memory_limit_aborts_without_advancing_previous_state():
    previous = {b"old": (hashlib.sha256(b"old").digest(), None, False)}
    service = make_service(
        FakeSource({b"new": (b"new", -1)}),
        previous=previous,
    )
    service.config["service"]["performance"]["memory_limit"] = 1

    assert service._scan_source_for_changes() is None
    assert service._source_snapshot is previous


def test_snapshot_memory_limit_does_not_double_count_duplicate_scan_key():
    class DuplicateScanSource(FakeSource):
        def __init__(self):
            super().__init__({b"duplicate": (b"value", -1)})
            self.scan_calls = 0

        def scan(self, cursor=0, match="*", count=1000):
            self.scan_calls += 1
            return (1, [b"duplicate"]) if self.scan_calls == 1 else (0, [b"duplicate"])

    source = DuplicateScanSource()
    service = make_service(source)
    fingerprint = (
        hashlib.sha256(b"value").digest(), None, False
    )
    expected = {b"duplicate": fingerprint}
    service.config["service"]["performance"]["memory_limit"] = (
        service._retained_snapshot_size()
        + service._snapshot_size(expected)
        + 2 * sys.getsizeof([])
    )

    assert service._build_source_snapshot() == expected
    assert source.scan_calls == 2


def test_snapshot_memory_limit_counts_realtime_bootstrap_snapshot():
    previous = {}
    bootstrap = {
        b"bootstrap": (hashlib.sha256(b"bootstrap").digest(), None, False)
    }
    service = make_service(FakeSource({}), previous=previous)
    service._realtime_bootstrap_snapshot = bootstrap
    service.config["service"]["performance"]["memory_limit"] = (
        service._snapshot_size(previous)
        + sys.getsizeof({})
        + 2 * sys.getsizeof([])
    )

    with pytest.raises(MemoryError, match="memory_limit"):
        service._build_source_snapshot()

    assert service._scan_source_for_changes() is None
    assert service._source_snapshot is previous

    service.target_connections = {}
    service._realtime_baseline_established = False
    with pytest.raises(MemoryError, match="memory_limit"):
        service._apply_replication_snapshot(b"snapshot")
    assert service._realtime_bootstrap_snapshot is bootstrap


def test_change_scan_splits_even_budget_between_deletions_and_upserts():
    previous = {
        b"first": (b"1", None),
        b"second": (b"2", None),
        b"changed": (b"old", None),
    }
    current = {b"changed": (b"new", None)}
    service = make_service(FakeSource({}), previous=previous)
    service.config["sync"]["incremental_sync"]["max_changes_per_sync"] = 2
    service._build_source_snapshot = lambda: current

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.deletions == [b"first"]
    assert changes.upserts == [b"changed"]


def test_change_scan_alternates_odd_budget_between_change_classes():
    previous = {
        b"deleted": (b"old", None),
        b"changed": (b"old", None),
    }
    current = {b"changed": (b"new", None)}
    service = make_service(FakeSource({}), previous=previous)
    service.config["sync"]["incremental_sync"]["max_changes_per_sync"] = 1
    service._build_source_snapshot = lambda: current

    first = service._scan_source_for_changes()
    second = service._scan_source_for_changes()

    assert first is not None and second is not None
    assert (first.deletions, first.upserts) == ([b"deleted"], [])
    assert (second.deletions, second.upserts) == ([], [b"changed"])


def test_post_full_reconciliation_replays_aba_keys_and_keeps_deletions():
    same_dump = b"value-a"
    digest = hashlib.sha256(same_dump).digest()
    pre_full = {
        b"aba": (digest, None, False),
        b"deleted-during-full": (digest, None, False),
    }
    source = FakeSource({b"aba": (same_dump, -1)})
    service = make_service(
        source,
        previous=RedisSyncService._full_reconciliation_baseline(pre_full),
    )

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.upserts == [b"aba"]
    assert changes.deletions == [b"deleted-during-full"]


def test_post_full_reconciliation_deletes_key_seen_only_on_target():
    service = make_service(
        FakeSource({}),
        previous=RedisSyncService._full_reconciliation_baseline(
            {}, target_keys=[b"transient-during-full"]
        ),
    )

    changes = service._scan_source_for_changes()

    assert changes is not None
    assert changes.upserts == []
    assert changes.deletions == [b"transient-during-full"]


def test_first_realtime_snapshot_preserves_target_only_keys():
    source = FakeSource({b"source-key": (b"source-value", -1)})
    target = RecordingTarget()
    target.scan_keys = [b"target-only"]
    digest = hashlib.sha256(b"source-value").digest()
    service = make_realtime_alignment_service(
        source,
        target,
        {b"source-key": (digest, None, False)},
    )

    assert service._apply_replication_snapshot(b"snapshot") is True

    assert target.operations == []
    assert service._realtime_baseline_established is True
    assert service._realtime_bootstrap_snapshot is None
    service._perform_full_sync.assert_called_once()
    migration_config = service._perform_full_sync.call_args.args[1]
    assert migration_config.clear_target is False


def test_first_realtime_snapshot_removes_source_key_deleted_during_full_copy():
    target = RecordingTarget()
    target.scan_keys = [b"target-only", b"deleted-during-full"]
    service = make_realtime_alignment_service(
        FakeSource({}),
        target,
        {b"deleted-during-full": (b"old-digest", None, False)},
    )

    assert service._apply_replication_snapshot(b"snapshot") is True

    assert target.operations == [("delete", b"deleted-during-full")]
    assert all(operation[1] != b"target-only" for operation in target.operations)


def test_first_realtime_snapshot_rechecks_type_atomically_before_delete():
    key = b"changed-after-bootstrap-scan"
    target = RecordingTarget()
    target.scan_keys = [key]
    target.key_types[key] = b"string"
    target.before_eval = lambda: target.key_types.update({key: b"list"})
    service = make_realtime_alignment_service(
        FakeSource({}),
        target,
        {key: (b"old-digest", None, False)},
    )
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]

    assert service._apply_replication_snapshot(b"snapshot") is True

    assert target.eval_batches == [[key]]
    assert ("delete", key) not in target.operations


def test_bootstrap_deletions_are_streamed_before_previous_is_fully_consumed():
    target = RecordingTarget()

    class StreamingPrevious(dict):
        def __iter__(self):
            for index, key in enumerate(super().__iter__()):
                if index >= 2:
                    assert target.pipeline_operations == [
                        [("delete", b"one"), ("delete", b"two")]
                    ]
                yield key

    previous = StreamingPrevious(
        (key, (b"digest", None, False))
        for key in (b"one", b"two", b"three", b"four")
    )
    service = make_realtime_alignment_service(
        FakeSource({}), target, previous
    )
    service.config["service"]["performance"]["pipeline_batch_size"] = 2

    assert service._apply_replication_snapshot(b"snapshot") is True

    assert [operation[1] for operation in target.operations] == [
        b"one", b"two", b"three", b"four"
    ]


def test_each_realtime_target_receives_complete_bootstrap_deletion_stream():
    previous = {
        key: (b"digest", None, False)
        for key in (b"one", b"two", b"three")
    }
    first = RecordingTarget()
    second = RecordingTarget()
    service = make_service(FakeSource({}))
    service.config["sync"].update(
        mode="hybrid", full_sync={"clear_target": False}
    )
    service.target_connections = {"first": object(), "second": object()}
    service.orchestrators = {
        "first": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=first
            )
        ),
        "second": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=second
            )
        ),
    }
    service.stats = {"first": SyncStats(), "second": SyncStats()}
    service._realtime_bootstrap_snapshot = previous
    service._realtime_baseline_established = False
    service._perform_full_sync = MagicMock(return_value=True)
    service._deactivate_realtime_target = MagicMock()

    assert service._apply_replication_snapshot(b"snapshot") is True

    expected = [b"one", b"two", b"three"]
    assert [operation[1] for operation in first.operations] == expected
    assert [operation[1] for operation in second.operations] == expected


def test_recovered_scan_target_arms_replay_and_target_only_deletion():
    digest = hashlib.sha256(b"same").digest()
    service = make_service(
        FakeSource({}),
        previous={b"same-as-checkpoint": (digest, None, False)},
    )
    service._managed_target_key_batches = lambda _name, _config: iter(
        [[b"transient-on-recovered-target"]]
    )

    service._arm_scan_reconciliation("target", object())

    assert service._source_snapshot == {
        b"same-as-checkpoint": (b"", None, False),
    }
    assert service._target_pending_deletions == {
        "target": {b"transient-on-recovered-target": None}
    }


def test_scan_reconciliation_mutates_checkpoint_in_place():
    checkpoint = {
        b"source": (hashlib.sha256(b"source").digest(), None, False)
    }
    service = make_service(FakeSource({}), previous=checkpoint)
    service._managed_target_key_batches = lambda *_args: iter(())

    service._arm_scan_reconciliation("target", object())

    assert service._source_snapshot is checkpoint
    assert checkpoint == {b"source": (b"", None, False)}


def test_scan_reconciliation_rolls_back_target_only_key_over_memory_limit():
    checkpoint = {
        b"source": (hashlib.sha256(b"source").digest(), None, False)
    }
    service = make_service(FakeSource({}), previous=checkpoint)
    marker_snapshot = {b"source": (b"", None, False)}
    service.config["service"]["performance"]["memory_limit"] = (
        service._snapshot_size(marker_snapshot)
    )
    service._managed_target_key_batches = lambda *_args: iter(
        [[b"target-only"]]
    )

    with pytest.raises(MemoryError, match="memory_limit"):
        service._arm_scan_reconciliation("target", object())

    assert service._source_snapshot is checkpoint
    assert checkpoint == marker_snapshot
    assert service._target_pending_deletions == {}


def test_first_hybrid_scan_preserves_target_only_keys_when_clear_is_disabled():
    digest = hashlib.sha256(b"source-value").digest()
    service = make_service(
        FakeSource({}),
        previous={b"source-before-full": (digest, None, False)},
    )
    service._managed_target_key_batches = lambda _name, _config: iter(
        [[b"target-only"]]
    )

    service._arm_scan_reconciliation(
        "target", object(), include_target_only=False
    )

    assert service._source_snapshot == {
        b"source-before-full": (b"", None, False),
    }


@pytest.mark.parametrize(
    ("clear_target", "expected_include_target_only"),
    [(False, False), (True, True)],
)
def test_hybrid_scan_coordinator_maps_clear_target_to_reconciliation_scope(
    clear_target, expected_include_target_only
):
    service = make_service(FakeSource({b"source": (b"value", -1)}))
    service.config["sync"] = {
        "mode": "hybrid",
        "full_sync": {"clear_target": clear_target},
        "incremental_sync": {
            "enabled": True,
            "method": "scan",
            "interval": 0,
            "key_pattern": "*",
            "key_types": None,
        },
    }
    service.target_connections = {"target": object()}
    service.orchestrators = {"target": object()}
    service.stats = {"target": SyncStats()}
    service.running = True
    service.shutdown_event = sync_service_module.threading.Event()
    service._perform_full_sync = MagicMock(return_value=True)
    reconciliation_calls = []

    def arm_reconciliation(
        target_name, migration_config, *, include_target_only=True
    ):
        reconciliation_calls.append(
            (target_name, migration_config.clear_target, include_target_only)
        )
        service.running = False

    service._arm_scan_reconciliation = arm_reconciliation

    service._unified_sync_coordinator()

    assert reconciliation_calls == [
        ("target", False, expected_include_target_only)
    ]


def test_hybrid_scan_stops_when_pre_full_baseline_cannot_be_captured():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "sync": {
            "mode": "hybrid",
            "incremental_sync": {"enabled": True, "method": "scan"},
        }
    }
    service.logger = logging.getLogger("test-pre-full-baseline-failure")
    service.running = True
    service.shutdown_event = sync_service_module.threading.Event()
    service._build_source_snapshot = lambda: (_ for _ in ()).throw(
        RuntimeError("injected snapshot failure")
    )

    service._unified_sync_coordinator()

    assert service.running is False
    assert service.shutdown_event.is_set()


def test_hybrid_scan_reuses_pre_full_snapshot_as_reconciliation_baseline():
    baseline = {
        b"source": (hashlib.sha256(b"source").digest(), None, False)
    }
    service = make_service(FakeSource({}))
    service.config["sync"] = {
        "mode": "hybrid",
        "full_sync": {"clear_target": False},
        "incremental_sync": {
            "enabled": True,
            "method": "scan",
            "interval": 0,
            "key_pattern": "*",
            "key_types": None,
        },
    }
    service.target_connections = {"target": object()}
    service.orchestrators = {"target": object()}
    service.stats = {"target": SyncStats(is_healthy=False)}
    service.running = False
    service.shutdown_event = sync_service_module.threading.Event()
    service._build_source_snapshot = lambda: baseline
    service._perform_full_sync = MagicMock(return_value=True)

    service._unified_sync_coordinator()

    assert service._source_snapshot is baseline
    assert baseline == {b"source": (b"", None, False)}


def test_hybrid_psync_starts_stream_before_any_live_full_copy():
    service = make_service(FakeSource({}))
    service.config["sync"] = {
        "mode": "hybrid",
        "full_sync": {"clear_target": False},
        "incremental_sync": {
            "enabled": True,
            "method": "psync",
            "key_pattern": "*",
            "key_types": None,
        },
    }
    service.target_connections = {"target": object()}
    service.stats = {"target": SyncStats()}
    service.running = True
    service.shutdown_event = sync_service_module.threading.Event()
    events = []

    def capture_baseline():
        events.append("baseline")
        return {b"source-key": (b"digest", None, False)}

    def perform_full_sync(_target_name):
        events.append("full")
        return True

    service._build_source_snapshot = capture_baseline
    service._perform_full_sync = perform_full_sync
    service._start_realtime_replication = lambda *_args: events.append("realtime")

    service._unified_sync_coordinator()

    assert events == ["realtime"]
    assert getattr(service, "_realtime_bootstrap_snapshot", None) is None


def test_hybrid_psync_cannot_copy_transient_key_before_stream_boundary():
    service = make_service(FakeSource({}))
    service.config["sync"] = {
        "mode": "hybrid",
        "full_sync": {"clear_target": False},
        "incremental_sync": {
            "enabled": True,
            "method": "psync",
            "key_pattern": "*",
            "key_types": None,
        },
    }
    service.target_connections = {"target": object()}
    service.stats = {"target": SyncStats()}
    service.running = True
    service.shutdown_event = sync_service_module.threading.Event()
    copied_before_psync = []

    def unsafe_live_full(_target_name):
        copied_before_psync.append(b"created-and-deleted-before-psync")
        return True

    service._perform_full_sync = unsafe_live_full
    service._build_source_snapshot = MagicMock(
        side_effect=AssertionError("PSYNC bootstrap must not scan before connecting")
    )
    service._start_realtime_replication = lambda *_args: None

    service._unified_sync_coordinator()

    assert copied_before_psync == []
    service._build_source_snapshot.assert_not_called()


def test_deferred_psync_alignment_preserves_preexisting_target_only_keys():
    source = FakeSource({b"source-key": (b"source-value", -1)})
    target = RecordingTarget()
    target.scan_keys = [b"target-only"]
    service = make_realtime_alignment_service(source, target, None)

    assert service._apply_replication_snapshot(b"snapshot") is True

    assert target.operations == []
    service._perform_full_sync.assert_called_once()
    migration_config = service._perform_full_sync.call_args.args[1]
    assert migration_config.clear_target is False


def test_deferred_psync_alignment_honors_explicit_clear_target():
    service = make_realtime_alignment_service(FakeSource({}), RecordingTarget(), None)
    service.config["sync"]["full_sync"]["clear_target"] = True

    assert service._apply_replication_snapshot(b"snapshot") is True

    service._perform_full_sync.assert_called_once()
    migration_config = service._perform_full_sync.call_args.args[1]
    assert migration_config.clear_target is True


def test_scan_target_recovery_clears_then_full_syncs_then_arms_reconciliation():
    service = make_service(FakeSource({}))
    service.config["sync"] = {
        "mode": "incremental",
        "full_sync": {"clear_target": True},
        "incremental_sync": {
            "enabled": True,
            "method": "scan",
            "interval": 0,
            "key_pattern": "*",
            "key_types": None,
        },
    }
    service.target_connections = {}
    service.orchestrators = {"target": object()}
    service.stats = {"target": SyncStats(is_healthy=False)}
    service.running = True
    service.shutdown_event = sync_service_module.threading.Event()
    service._target_next_recovery = {}
    service._target_recovery_is_due = lambda _name: True
    events = []

    def recover(target_name):
        events.append(("recover", target_name))
        return True

    def clear_scope(target_name, migration_config):
        events.append(("clear", target_name, migration_config.clear_target))

    def full_sync(target_name, migration_config):
        events.append(("full", target_name, migration_config.clear_target))
        return True

    def arm(target_name, migration_config):
        events.append(("arm", target_name, migration_config.clear_target))

    def finish_iteration():
        events.append(("incremental",))
        service.running = False
        service.shutdown_event.set()
        return True

    service._recover_target_connection = recover
    service._clear_managed_target_scope = clear_scope
    service._perform_full_sync = full_sync
    service._arm_scan_reconciliation = arm
    service._perform_unified_incremental_sync = finish_iteration

    service._unified_sync_coordinator()

    assert events == [
        ("recover", "target"),
        ("clear", "target", False),
        ("full", "target", False),
        ("arm", "target", False),
        ("incremental",),
    ]
    assert service.stats["target"].is_healthy is True


class ImmediateFuture:
    def __init__(self, value):
        self.value = value

    def result(self):
        return self.value


class ImmediateExecutor:
    def submit(self, func, *args):
        return ImmediateFuture(func(*args))


def test_checkpoint_commits_only_after_every_healthy_target_succeeds():
    previous = {b"key": (b"old", None)}
    current = {
        b"key": (hashlib.sha256(b"new").digest(), None, False)
    }
    service = make_service(FakeSource({b"key": (b"new", -1)}), previous=previous)
    service.stats = {"ok": SyncStats(), "bad": SyncStats()}
    service.executor = ImmediateExecutor()
    service._scan_source_for_changes = lambda: SourceChangeSet(
        dict(current), [b"key"], []
    )
    service._sync_keys_to_target = (
        lambda name, keys, deleted, prepared: name == "ok"
    )

    assert service._perform_unified_incremental_sync() is False
    assert service._source_snapshot == {b"key": (b"", None, False)}
    assert service._source_snapshot is previous

    service._sync_keys_to_target = lambda name, keys, deleted, prepared: True
    assert service._perform_unified_incremental_sync() is True
    assert service._source_snapshot == current
    assert service._source_snapshot is previous


@pytest.mark.parametrize(
    "change", ["new_deleted", "value_reverted", "deletion_recreated"]
)
def test_partial_scan_delivery_reconciles_source_returning_to_checkpoint(
    monkeypatch, change
):
    key = b"key"
    original = {} if change == "new_deleted" else {key: (b"old", -1)}
    previous = {
        k: (hashlib.sha256(value[0]).digest(), None, False)
        for k, value in original.items()
    }
    changed = {} if change == "deletion_recreated" else {key: (b"new", -1)}
    source = FakeSource(changed)
    service = make_service(source, previous=previous)
    good, bad = RecordingTarget(), RecordingTarget()
    service.orchestrators = {
        name: SimpleNamespace(connection_manager=SimpleNamespace(
            source_client=source, target_client=target,
        )) for name, target in [("good", good), ("bad", bad)]
    }
    service.stats = {name: SyncStats() for name in service.orchestrators}
    service.executor = ImmediateExecutor()
    with monkeypatch.context() as patch:
        patch.setattr(
            bad, "pipeline",
            MagicMock(side_effect=redis.ConnectionError("transient outage")),
        )
        assert service._perform_unified_incremental_sync() is False

    assert good.operations
    good.operations.clear()
    source.values = original
    assert service._perform_unified_incremental_sync() is True
    if change == "new_deleted":
        assert good.operations == [("delete", key)]
        assert service._source_snapshot == {}
    else:
        assert good.operations == [("restore", key, 0, b"old", True, False)]
        assert service._source_snapshot[key][0] == hashlib.sha256(b"old").digest()


def test_scan_partial_pipeline_failure_keeps_all_attempted_keys_for_reconciliation():
    source = FakeSource({b"first": (b"new", -1), b"second": (b"new", -1)})
    target = FailOnSecondTarget()
    service = make_service(source, previous={})
    service.config["service"]["performance"]["pipeline_batch_size"] = 1
    service.orchestrators = {"target": SimpleNamespace(connection_manager=SimpleNamespace(
        source_client=source, target_client=target,
    ))}
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()

    assert service._perform_unified_incremental_sync() is False
    assert target.operations == [("restore", b"first", 0, b"new", True, False)]
    source.values = {}
    assert service._perform_unified_incremental_sync() is True
    assert ("delete", b"first") in target.operations
    assert service._source_snapshot == {}


@pytest.mark.parametrize("previous", [None, {}])
def test_pending_marker_memory_limit_aborts_before_target_writes(previous):
    key = b"new-key"
    service = make_service(FakeSource({key: (b"value", -1)}), previous=previous)
    service.config["sync"]["mode"] = "incremental"
    service.running = True
    service.stats = {"target": SyncStats()}
    service.executor = MagicMock()
    changes = service._scan_source_for_changes()
    service._prepare_change_set_for_delivery(changes)
    service._scan_source_for_changes = lambda: changes
    service._prepare_change_set_for_delivery = lambda _changes: None
    retained_size = service._retained_snapshot_size()
    if previous is None:
        retained_size += sys.getsizeof({})
    base_size = (
        retained_size
        + service._snapshot_size(changes.current_snapshot)
        + sys.getsizeof(changes.upserts)
        + sys.getsizeof(changes.deletions)
        + sys.getsizeof(changes.captured_upserts)
        + sum(
            sys.getsizeof(state) + sys.getsizeof(state.dump_data)
            for state in changes.captured_upserts.values()
        )
    )
    pending_size = service._snapshot_size({key: (b"", None, False)})
    service.config["service"]["performance"]["memory_limit"] = (
        base_size + pending_size - sys.getsizeof({}) - 1
    )

    assert service._perform_unified_incremental_sync() is False
    service.executor.submit.assert_not_called()
    assert service._source_snapshot == {}
    status = service.get_status()
    assert status["healthy"] is False
    assert "marking pending keys" in status["scan"]["last_error"]


def test_scan_status_tracks_source_failure_and_successful_recovery(monkeypatch):
    source = FakeSource({})
    service = make_service(source, previous={})
    service.config["sync"]["mode"] = "incremental"
    service.config["sync"]["incremental_sync"]["method"] = "scan"
    service.running = True
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()

    assert service.get_status()["healthy"] is False
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.0)
    assert service._perform_unified_incremental_sync() is True
    assert service.get_status()["healthy"] is True
    assert service.get_status()["scan"]["last_success_time"] == 100.0

    monkeypatch.setattr(sync_service_module.time, "time", lambda: 200.0)
    with monkeypatch.context() as patch:
        patch.setattr(
            source, "scan",
            MagicMock(side_effect=redis.ConnectionError("source unavailable")),
        )
        for _ in range(3):
            assert service._perform_unified_incremental_sync() is False
    status = service.get_status()
    assert status["healthy"] is False
    assert status["targets"]["target"]["healthy"] is True
    assert status["scan"]["last_error"] == "source unavailable"
    assert status["scan"]["last_attempt_time"] == 200.0
    assert status["scan"]["last_success_time"] == 100.0
    assert status["scan"]["in_progress"] is False

    monkeypatch.setattr(sync_service_module.time, "time", lambda: 300.0)
    assert service._perform_unified_incremental_sync() is True
    status = service.get_status()
    assert status["healthy"] is True
    assert status["scan"]["last_error"] is None
    assert status["scan"]["last_success_time"] == 300.0


@pytest.mark.parametrize("stage", ["snapshot", "capture"])
def test_scan_status_reports_source_read_failures_before_delivery(monkeypatch, stage):
    service = make_service(FakeSource({b"key": (b"value", -1)}), previous={})
    service.config["sync"]["mode"] = "hybrid"
    service.running = True
    service.stats = {"target": SyncStats()}
    service.executor = MagicMock()
    if stage == "snapshot":
        service.config["service"]["performance"]["memory_limit"] = 1
        expected_error = "memory_limit"
    else:
        monkeypatch.setattr(
            service, "_capture_source_states",
            MagicMock(side_effect=redis.ConnectionError("capture unavailable")),
        )
        expected_error = "capture unavailable"

    assert service._perform_unified_incremental_sync() is False
    service.executor.submit.assert_not_called()
    assert service._source_snapshot == {}
    status = service.get_status()
    assert status["healthy"] is False
    assert expected_error in status["scan"]["last_error"]
    assert status["scan"]["last_success_time"] is None
    assert status["scan"]["in_progress"] is False


@pytest.mark.parametrize("mode,method", [
    ("full", "scan"), ("incremental", "psync"), ("hybrid", "sync"),
])
def test_scan_status_does_not_affect_other_sync_modes(mode, method):
    service = make_service(FakeSource({}))
    service.config["sync"]["mode"] = mode
    service.config["sync"]["incremental_sync"]["method"] = method
    service.running = True
    service.stats = {"target": SyncStats()}

    status = service.get_status()
    assert status["healthy"] is True
    assert status["scan"] is None


def test_delivery_reuses_one_captured_payload_and_commits_its_fingerprint():
    previous = {b"key": (b"old", None, False)}
    source = FakeSource({b"key": (b"delivered-state", -1)})
    service = make_service(source, previous=previous)
    targets = {"first": RecordingTarget(), "second": RecordingTarget()}
    service.orchestrators = {
        name: SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
        for name, target in targets.items()
    }
    service.stats = {name: SyncStats() for name in targets}
    service.executor = ImmediateExecutor()
    scanned = {
        b"key": (hashlib.sha256(b"scanned-state").digest(), None, False)
    }
    service._scan_source_for_changes = lambda: SourceChangeSet(
        dict(scanned), [b"key"], []
    )

    assert service._perform_unified_incremental_sync() is True

    expected_restore = (
        "restore", b"key", 0, b"delivered-state", True, False
    )
    assert targets["first"].operations == [expected_restore]
    assert targets["second"].operations == [expected_restore]
    assert len(source.pipeline_operations) == 1
    delivered_fingerprint = (
        hashlib.sha256(b"delivered-state").digest(), None, False
    )
    assert service._source_snapshot == {b"key": delivered_fingerprint}

    source.values[b"key"] = (b"scanned-state", -1)
    next_changes = RedisSyncService._scan_source_for_changes(service)
    assert next_changes is not None
    assert next_changes.upserts == [b"key"]


def test_delivery_capture_max_size_preflight_never_dumps_oversized_key():
    class OversizedPipeline:
        def __init__(self):
            self.dump_calls = 0

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
            return None

    source = MagicMock()
    source.execute_command.side_effect = redis.ResponseError("unknown command")
    watched = OversizedPipeline()
    source.pipeline.return_value = watched
    service = make_service(source)
    service._sync_key_filter = KeySyncFilter(max_key_size=100)

    captured, invalid = service._capture_source_states(
        source,
        [b"oversized"],
    )

    assert captured == {}
    assert invalid == [b"oversized"]
    assert watched.dump_calls == 0


def test_delivery_capture_uses_pttl_for_liveness(monkeypatch):
    source = FakeSource({b"key": (b"dump", 500, 999_999)})
    service = make_service(source)
    monotonic_clock = iter((0, 1_000_000_000))
    monkeypatch.setattr(
        sync_service_module.time, "monotonic_ns", lambda: next(monotonic_clock)
    )
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.0)

    captured, invalid = service._capture_source_states(source, [b"key"])

    assert captured == {}
    assert invalid == [b"key"]


@pytest.mark.parametrize(
    ("source_state", "key_types", "key_filter"),
    [
        ((b"dump", -1, -1, b"list"), ["string"], None),
        ((b"dump", 10_000, -1, b"string"), None, KeySyncFilter(min_ttl=60)),
        (
            (b"dump", -1, -1, b"string", 100),
            None,
            KeySyncFilter(max_key_size=10),
        ),
    ],
    ids=["wrong-type", "low-ttl", "oversized"],
)
def test_delivery_capture_removes_previously_managed_key_that_left_scope(
    source_state, key_types, key_filter
):
    key = b"managed"
    previous = {key: (b"old", None, False)}
    source = FakeSource({key: source_state})
    target = RecordingTarget()
    target.scan_keys = [key]
    target.key_types[key] = b"string"
    target.key_ttls[key] = -1
    target.key_memory[key] = 5
    service = make_service(source, previous=previous)
    service.config["sync"]["incremental_sync"]["key_types"] = key_types
    service._sync_key_filter = key_filter
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()
    service._scan_source_for_changes = lambda: SourceChangeSet(
        {key: (b"scanned", None, False)}, [key], []
    )

    assert service._perform_unified_incremental_sync() is True
    assert ("delete", key) in target.operations
    assert all(operation[0] != "restore" for operation in target.operations)
    assert service._source_snapshot == {}


def test_new_key_that_leaves_scope_does_not_delete_unmanaged_target_value():
    key = b"not-yet-managed"
    source = FakeSource({key: (b"dump", -1, -1, b"list")})
    target = RecordingTarget()
    target.scan_keys = [key]
    target.key_types[key] = b"string"
    service = make_service(source, previous={})
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()
    service._scan_source_for_changes = lambda: SourceChangeSet(
        {key: (b"scanned", None, False)}, [key], []
    )

    assert service._perform_unified_incremental_sync() is True
    assert target.operations == []
    assert service._source_snapshot == {}


def test_delivery_capture_preserves_target_value_outside_dynamic_scope():
    key = b"external-now"
    source = FakeSource({key: (b"dump", -1, -1, b"list")})
    target = RecordingTarget()
    target.scan_keys = [key]
    target.key_types[key] = b"list"
    service = make_service(
        source, previous={key: (b"old", None, False)}
    )
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()
    service._scan_source_for_changes = lambda: SourceChangeSet(
        {key: (b"scanned", None, False)}, [key], []
    )

    assert service._perform_unified_incremental_sync() is True
    assert ("delete", key) not in target.operations
    assert service._source_snapshot == {}


def test_capture_payload_memory_limit_fails_before_any_target_write():
    key = b"large-payload"
    previous = {key: (b"old", None, False)}
    source = FakeSource({key: (b"x" * 4096, -1)})
    target = RecordingTarget()
    service = make_service(source, previous=previous)
    service.config["sync"]["mode"] = "incremental"
    service.running = True
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()
    changes = SourceChangeSet(
        {key: (b"scanned", None, False)}, [key], []
    )
    service._scan_source_for_changes = lambda: changes
    base_size = (
        service._retained_snapshot_size()
        + service._snapshot_size(changes.current_snapshot)
        + sys.getsizeof(changes.upserts)
        + sys.getsizeof(changes.deletions)
    )
    service.config["service"]["performance"]["memory_limit"] = (
        base_size + sys.getsizeof({}) + 1
    )

    assert service._perform_unified_incremental_sync() is False
    assert target.operations == []
    assert service._source_snapshot is previous
    status = service.get_status()
    assert status["healthy"] is False
    assert "capturing" in status["scan"]["last_error"]


def test_target_only_reconciliation_deletion_is_not_fanned_out():
    service = make_service(FakeSource({}), previous={})
    small = RecordingTarget()
    small.scan_keys = [b"shared"]
    small.key_memory = {b"shared": 5}
    large = RecordingTarget()
    large.scan_keys = [b"shared"]
    large.key_memory = {b"shared": 100}
    service.orchestrators = {
        "small": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=small
            )
        ),
        "large": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=large
            )
        ),
    }
    service.stats = {"small": SyncStats(), "large": SyncStats()}
    service.executor = ImmediateExecutor()
    service._sync_key_filter = KeySyncFilter(max_key_size=10)
    config = MigrationConfig(key_pattern="*", scan_count=100)

    service._arm_scan_reconciliation("small", config)
    service._arm_scan_reconciliation("large", config)

    assert service._perform_unified_incremental_sync() is True
    assert ("delete", b"shared") in small.operations
    assert ("delete", b"shared") not in large.operations
    assert service._source_snapshot == {}
    assert service._target_pending_deletions == {}


def test_source_deletion_rechecks_each_targets_dynamic_scope_before_del():
    previous = {b"shared": (hashlib.sha256(b"old").digest(), None, False)}
    service = make_service(FakeSource({}), previous=previous)
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]
    service._sync_key_filter = KeySyncFilter(min_ttl=60, max_key_size=10)
    targets = {}
    target_settings = {
        "managed": (b"string", -1, 5),
        "large": (b"string", -1, 100),
        "short_ttl": (b"string", 10, 5),
        "wrong_type": (b"hash", -1, 5),
    }
    for name, (key_type, ttl, memory) in target_settings.items():
        target = RecordingTarget()
        target.key_types[b"shared"] = key_type
        target.key_ttls[b"shared"] = ttl
        target.key_memory[b"shared"] = memory
        targets[name] = target
    service.orchestrators = {
        name: SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=target
            )
        )
        for name, target in targets.items()
    }
    service.stats = {name: SyncStats() for name in targets}
    service.executor = ImmediateExecutor()

    assert service._perform_unified_incremental_sync() is True
    assert ("delete", b"shared") in targets["managed"].operations
    for name in ("large", "short_ttl", "wrong_type"):
        assert ("delete", b"shared") not in targets[name].operations
    assert service._source_snapshot == {}


def test_source_deletion_type_recheck_failure_does_not_advance_checkpoint():
    previous = {b"shared": (hashlib.sha256(b"old").digest(), None, False)}
    service = make_service(FakeSource({}), previous=previous)
    service.config["sync"]["incremental_sync"]["key_types"] = ["string"]
    target = MagicMock()
    target.execute_command.side_effect = RuntimeError("TYPE failed")
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=target
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()

    assert service._perform_unified_incremental_sync() is False
    assert service._source_snapshot is previous
    assert b"shared" in service._source_snapshot


def test_failed_target_only_deletion_remains_pending_for_retry():
    service = make_service(FakeSource({}), previous={})
    target = MagicMock()
    target_pipe = MagicMock()
    target_pipe.delete.return_value = target_pipe
    target_pipe.execute.side_effect = RuntimeError("injected DEL failure")
    target.pipeline.return_value = target_pipe
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=service.source_conn, target_client=target
            )
        )
    }
    service.stats = {"target": SyncStats()}
    service.executor = ImmediateExecutor()
    service._target_pending_deletions = {"target": {b"target-only": None}}

    assert service._perform_unified_incremental_sync() is False
    target_pipe.delete.assert_called_once_with(b"target-only")
    assert service._source_snapshot == {}
    assert service._target_pending_deletions == {
        "target": {b"target-only": None}
    }


def test_initial_no_change_baseline_adopts_current_snapshot():
    current = {b"key": (b"digest", None, False)}
    service = make_service(FakeSource({}))
    service._scan_source_for_changes = lambda: SourceChangeSet(current, [], [])

    assert service._perform_unified_incremental_sync() is True
    assert service._source_snapshot is current


def test_fullresync_alignment_failure_is_recorded_once_and_removes_target():
    service = make_service(FakeSource({}))
    service.config["sync"].update(
        mode="hybrid",
        full_sync={"clear_target": False},
    )
    service.config["targets"] = [{"name": "target"}]
    manager = MagicMock()
    orchestrator = SimpleNamespace(
        connection_manager=manager,
        migrate=MagicMock(return_value={"success": False, "errors": ["boom"]}),
    )
    service.target_connections = {"target": manager}
    service.orchestrators = {"target": orchestrator}
    service.stats = {"target": SyncStats()}
    service._unavailable_targets = {}
    service.incremental_service = None
    service._realtime_baseline_established = True
    service._realtime_bootstrap_snapshot = None
    service._clear_managed_target_scope = lambda *_args: None

    assert service._apply_replication_snapshot(b"snapshot") is True

    stats = service.stats["target"]
    assert stats.total_failed == 1
    assert len(stats.failure_timestamps) == 1
    assert stats.is_healthy is False
    assert "target" not in service.target_connections
    assert "target" not in service.orchestrators
    assert "target" in service._unavailable_targets


class FailingTargetPipeline:
    def restore(self, *args, **kwargs):
        return self

    def delete(self, *args, **kwargs):
        return self

    def execute(self):
        raise RuntimeError("target write failed")


class FailingTarget:
    def pipeline(self, transaction=False):
        return FailingTargetPipeline()


def test_target_failure_does_not_advance_target_timestamp():
    source = FakeSource({b"key": (b"dump", -1)})
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=FailingTarget(),
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is False
    assert service.stats["target"].last_sync_time is None
    assert service.stats["target"].consecutive_failures == 1


def test_target_sync_rejects_invalid_pttl_without_writing_permanent_key():
    source = FakeSource({b"key": (b"dump", -3)})
    target = RecordingTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is False
    assert target.operations == []
    assert service.stats["target"].total_failed == 1


def test_target_sync_uses_configured_pipeline_batches():
    source = FakeSource(
        {
            b"one": (b"1", -1),
            b"two": (b"2", -1),
            b"three": (b"3", -1),
        }
    )
    target = RecordingTarget()
    service = make_service(source)
    service.config["service"]["performance"]["pipeline_batch_size"] = 2
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target(
        "target", [b"one", b"two", b"three"]
    ) is True

    assert source.pipeline_transactions == [True, True]
    assert [len(batch) for batch in target.pipeline_operations] == [2, 1]
    assert [operation[1] for operation in target.operations] == [
        b"one",
        b"two",
        b"three",
    ]


def test_target_second_batch_failure_records_only_confirmed_work():
    source = FakeSource(
        {
            b"one": (b"1", -1),
            b"two": (b"2", -1),
            b"three": (b"3", -1),
        }
    )
    target = FailOnSecondTarget()
    service = make_service(source)
    service.config["service"]["performance"]["pipeline_batch_size"] = 2
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target(
        "target", [b"one", b"two", b"three"]
    ) is False

    stats = service.stats["target"]
    assert stats.total_synced == 2
    assert stats.total_failed == 1
    assert stats.last_sync_time is None
    assert stats.consecutive_failures == 1


def test_target_deletions_use_configured_pipeline_batch_size():
    source = FakeSource({})
    target = RecordingTarget()
    service = make_service(source)
    service.config["service"]["performance"]["pipeline_batch_size"] = 2
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    deleted_keys = [f"deleted-{index}".encode() for index in range(5)]

    assert service._sync_keys_to_target(
        "target", [], deleted_keys=deleted_keys
    ) is True

    assert [len(batch) for batch in target.pipeline_operations] == [2, 2, 1]
    assert all(
        operation[0] == "delete"
        for batch in target.pipeline_operations
        for operation in batch
    )


def test_managed_scope_type_and_delete_pipelines_use_configured_batch_size():
    target = RecordingTarget()
    target.scan_keys = [f"key-{index}".encode() for index in range(5)]
    service = make_service(FakeSource({}))
    service.config["service"]["performance"]["pipeline_batch_size"] = 2
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(target_client=target)
        )
    }

    service._clear_managed_target_scope(
        "target",
        MigrationConfig(key_pattern="*", key_types=["string"], scan_count=100),
    )

    assert [len(batch) for batch in target.pipeline_operations] == [2, 2, 1]
    assert [batch[0][0] for batch in target.pipeline_operations] == [
        "type", "type", "type"
    ]
    assert [len(batch) for batch in target.eval_batches] == [2, 2, 1]


def test_managed_scope_clear_rechecks_type_atomically_before_delete():
    key = b"changed-after-scan"
    target = RecordingTarget()
    target.scan_keys = [key]
    target.key_types[key] = b"string"
    target.before_eval = lambda: target.key_types.update({key: b"list"})
    service = make_service(FakeSource({}))
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(target_client=target)
        )
    }

    service._clear_managed_target_scope(
        "target",
        MigrationConfig(key_pattern="*", key_types=["string"], scan_count=100),
    )

    assert target.eval_batches == [[key]]
    assert ("delete", key) not in target.operations


def test_managed_scope_preserves_target_keys_outside_value_filters():
    target = MagicMock()
    target.scan.return_value = (0, [b"outside-size-scope"])
    target.pipeline.return_value.execute.return_value = [-1, 100]
    service = make_service(FakeSource({}))
    service._sync_key_filter = KeySyncFilter(max_key_size=1)
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(target_client=target)
        )
    }

    batches = list(
        service._managed_target_key_batches(
            "target",
            MigrationConfig(key_pattern="*", scan_count=100),
        )
    )

    assert batches == []


def test_redis6_target_write_keeps_pexpiretime_outside_transaction(monkeypatch):
    clock = iter((100.0, 101.0, 101.0))
    monkeypatch.setattr(sync_service_module.time, "time", lambda: next(clock))
    monkeypatch.setattr(sync_service_module.time, "monotonic_ns", lambda: 0)
    source = FakeSource(
        {
            b"key": (
                b"dump",
                2500,
                redis.ResponseError("unknown command PEXPIRETIME"),
            )
        }
    )
    target = RecordingTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is True

    assert target.operations == [
        ("restore", b"key", 102_500, b"dump", True, True)
    ]
    assert source.pexpiretime_probes == [b"key"]
    assert source.pipeline_operations == [[("dump", b"key"), ("pttl", b"key")]]


def test_redis6_target_write_deletes_key_expired_during_source_read(monkeypatch):
    source = FakeSource(
        {
            b"key": (
                b"dump",
                500,
                redis.ResponseError("unknown command PEXPIRETIME"),
            )
        }
    )
    target = RecordingTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    monotonic_clock = iter((0, 1_000_000_000))
    monkeypatch.setattr(
        sync_service_module.time, "monotonic_ns", lambda: next(monotonic_clock)
    )
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 100.0)

    assert service._sync_keys_to_target("target", [b"key"]) is True
    assert target.operations == [("delete", b"key")]


@pytest.mark.parametrize(
    "target_time",
    [
        101.0,
        103.0,
    ],
    ids=["local-clock-before-deadline", "local-clock-after-deadline"],
)
def test_target_write_does_not_use_process_clock_to_predelete(
    monkeypatch, target_time
):
    clock = iter((100.0, target_time, target_time))
    monkeypatch.setattr(sync_service_module.time, "time", lambda: next(clock))
    # PTTL decides liveness; PEXPIRETIME remains the exact target deadline.
    monotonic_clock = iter((0, 100_000_000))
    monkeypatch.setattr(
        sync_service_module.time, "monotonic_ns", lambda: next(monotonic_clock)
    )
    source = FakeSource({b"key": (b"dump", 2500, 102_500)})
    target = RecordingTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is True
    assert target.operations == [
        ("restore", b"key", 102_500, b"dump", True, True)
    ]


def test_scan_sync_old_target_receives_exact_expiry_provenance(monkeypatch):
    class LegacyPipeline(RecordingTargetPipeline):
        def execute(self, **_kwargs):
            self.target.pipeline_operations.append(list(self.operations))
            self.target.operations.extend(self.operations)
            return [redis.ResponseError("ERR syntax error")]

    class LegacyTarget(RecordingTarget):
        def pipeline(self, transaction=False):
            assert transaction is False
            return LegacyPipeline(self)

    source = FakeSource({b"key": (b"dump", 2500, 102_500)})
    target = LegacyTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }
    legacy_restore = MagicMock(return_value=True)
    monkeypatch.setattr(
        sync_service_module,
        "restore_dump_with_deadline",
        legacy_restore,
    )
    monkeypatch.setattr(sync_service_module.time, "monotonic_ns", lambda: 0)

    assert service._sync_keys_to_target("target", [b"key"]) is True
    legacy_restore.assert_called_once_with(
        target,
        b"key",
        b"dump",
        102_500,
        overwrite=True,
        prefer_absttl=False,
        expires_at_is_exact=True,
    )


def test_target_pipeline_delay_does_not_extend_absolute_deadline(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(sync_service_module.time, "time", lambda: clock[0])
    monkeypatch.setattr(sync_service_module.time, "monotonic_ns", lambda: 0)

    class DelayedPipeline(RecordingTargetPipeline):
        def execute(self, **kwargs):
            clock[0] += 1
            return super().execute(**kwargs)

    class DelayedTarget(RecordingTarget):
        def pipeline(self, transaction=False):
            assert transaction is False
            return DelayedPipeline(self)

    source = FakeSource(
        {
            b"key": (
                b"dump",
                2500,
                redis.ResponseError("unknown command PEXPIRETIME"),
            )
        }
    )
    target = DelayedTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is True
    assert target.operations == [
        ("restore", b"key", 102_500, b"dump", True, True)
    ]


def test_target_sync_rejects_invalid_negative_pttl():
    source = FakeSource({b"key": (b"dump", -3)})
    target = RecordingTarget()
    service = make_service(source)
    service.stats = {"target": SyncStats()}
    service.orchestrators = {
        "target": SimpleNamespace(
            connection_manager=SimpleNamespace(
                source_client=source,
                target_client=target,
            )
        )
    }

    assert service._sync_keys_to_target("target", [b"key"]) is False
    assert "PTTL" in service.stats["target"].last_error
    assert target.operations == []
