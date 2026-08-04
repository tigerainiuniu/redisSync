import threading
import time

import pytest
import redis

import redis_sync.unified_incremental_service as unified_incremental_service
from redis_sync.sync_filters import KeySyncFilter
from redis_sync.unified_incremental_service import UnifiedIncrementalService


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

    def execute_command(self, *args):
        if args[0] == "PEXPIRETIME":
            self.operations.append(("pexpiretime", args[1]))
        else:
            assert args[:2] == ("MEMORY", "USAGE")
            self.operations.append(("memory", args[2]))
        return self

    def execute(self):
        self.source.pipeline_executions += 1
        self.source.pipeline_batches.append(list(self.operations))
        result = []
        for operation, key in self.operations:
            state = self.source.states.get(key, (None, -2, None))
            dump_value, ttl = state[:2]
            memory_size = state[2] if len(state) > 2 else None
            if operation == "dump":
                result.append(dump_value)
            elif operation == "pttl":
                result.append(ttl)
            elif operation == "type":
                result.append(state[3] if len(state) > 3 else b"string")
            elif operation == "pexpiretime":
                result.append(self.source.pexpiretimes.get(key, -1))
            else:
                result.append(memory_size)
        return result


class FakeSource:
    def __init__(self, states=None):
        self.states = states or {}
        self.connection_pool = type(
            "SourcePool", (), {"connection_kwargs": {"db": 0}}
        )()
        self.getkeys_calls = []
        self.pipeline_executions = 0
        self.pipeline_batches = []
        self.pexpiretimes = None

    def command_getkeys(self, *args):
        command = list(args)
        self.getkeys_calls.append(command)
        name = command[0].upper()
        if name in {b"SET", b"INCR", b"LPUSH", b"DEL"}:
            return [command[1]]
        return []

    def execute_command(self, *args):
        if args and args[0] == "PEXPIRETIME":
            if self.pexpiretimes is None:
                raise redis.ResponseError("unknown command PEXPIRETIME")
            return self.pexpiretimes.get(args[1], -1)
        raise AssertionError("command_getkeys API should be preferred")

    def pipeline(self, transaction=True):
        assert transaction is True
        return FakeSourcePipeline(self)


class FakeTargetConnection:
    def __init__(self, delay=0, failures=0):
        self.sent = []
        self.last_command = None
        self.delay = delay
        self.failures = failures
        self.disconnected = False
        self._active = 0
        self.max_active = 0
        self._lock = threading.Lock()
        self.target_client = None

    def send_command(self, *args):
        self.last_command = args
        self.sent.append(args)

    def read_response(self):
        with self._lock:
            self._active += 1
            self.max_active = max(self.max_active, self._active)
        try:
            if self.delay and self.last_command[0].upper() == b"SET":
                time.sleep(self.delay)
            if self.failures:
                self.failures -= 1
                raise RuntimeError("injected target failure")
            if self.last_command[0].upper() == b"EVAL":
                return self._execute_filtered_delete()
            return b"OK"
        finally:
            with self._lock:
                self._active -= 1

    def disconnect(self):
        self.disconnected = True

    def _execute_filtered_delete(self):
        command = self.last_command
        key_count = int(command[2])
        keys = command[3:3 + key_count]
        argument_offset = 3 + key_count
        min_ttl = int(command[argument_offset])
        max_key_size = int(command[argument_offset + 1])
        type_count = int(command[argument_offset + 2])
        allowed_types = set(
            command[argument_offset + 3:argument_offset + 3 + type_count]
        )
        callback = getattr(self.target_client, "before_eval", None)
        if callable(callback):
            callback()
        deleted = 0
        for key in keys:
            if key not in self.target_client.scan_keys:
                continue
            ttl, memory_size, key_type = self.target_client.states.get(
                key, (-1, None, b"string")
            )
            if allowed_types and key_type not in allowed_types:
                continue
            if min_ttl > 0 and ttl != -1 and ttl < min_ttl:
                continue
            if (
                max_key_size > 0
                and memory_size is not None
                and memory_size > max_key_size
            ):
                continue
            self.target_client.scan_keys.remove(key)
            self.target_client.states.pop(key, None)
            deleted += 1
        return deleted


class FakeTargetPool:
    def __init__(self, db=0, delay=0, failures=0):
        self.connection_kwargs = {"db": db}
        self.connection = FakeTargetConnection(delay=delay, failures=failures)
        self.released = []

    def get_connection(self, name=None):
        return self.connection

    def release(self, connection):
        self.released.append(connection)


class FakeTargetClient:
    def __init__(self, pool, scan_keys=None, states=None):
        self.connection_pool = pool
        self.scan_keys = list(scan_keys or [])
        self.states = states or {}
        self.scan_calls = []
        self.pipeline_batches = []
        self.pipeline_operations = []

    def scan(self, cursor=0, count=None):
        self.scan_calls.append((cursor, count))
        return 0, list(self.scan_keys)

    def pipeline(self, transaction=False):
        assert transaction is False
        return FakeTargetTypePipeline(self)


class FakeTargetTypePipeline:
    def __init__(self, client):
        self.client = client
        self.keys = []
        self.operations = []

    def type(self, key):
        self.keys.append(key)
        self.operations.append(("type", key))
        return self

    def ttl(self, key):
        self.keys.append(key)
        self.operations.append(("ttl", key))
        return self

    def execute_command(self, *args):
        assert args[:2] == ("MEMORY", "USAGE")
        self.keys.append(args[2])
        self.operations.append(("memory", args[2]))
        return self

    def execute(self):
        self.client.pipeline_batches.append(list(self.keys))
        self.client.pipeline_operations.append(list(self.operations))
        result = []
        for operation, key in self.operations:
            ttl, memory_size, key_type = self.client.states.get(
                key, (-1, None, b"string")
            )
            if operation == "ttl":
                result.append(ttl)
            elif operation == "memory":
                result.append(memory_size)
            else:
                result.append(key_type)
        return result


def target_manager(db=0, delay=0, failures=0, scan_keys=None, states=None):
    pool = FakeTargetPool(db=db, delay=delay, failures=failures)
    client = FakeTargetClient(pool, scan_keys=scan_keys, states=states)
    pool.connection.target_client = client
    manager = type("TargetManager", (), {"target_client": client})()
    return manager, pool


def service_for(config=None, source=None, targets=None):
    runtime_config = {"apply_mode": "key_state"}
    runtime_config.update(config or {})
    service = UnifiedIncrementalService(
        "psync",
        source or FakeSource(),
        targets or {},
        runtime_config,
    )
    service.running = True
    return service


def test_psync_mode_passes_capture_max_size_to_handler(monkeypatch):
    captured = {}

    class StubHandler:
        replication_thread = None
        last_error = None

        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start_replication(self, _callback):
            return True

        def stop_replication(self):
            return None

    monkeypatch.setattr(
        unified_incremental_service,
        "PSyncIncrementalHandler",
        StubHandler,
    )
    service = service_for(config={"capture_max_size": 123456})
    service.shutdown_event.set()
    try:
        service._start_psync_mode()
        assert captured["capture_max_size"] == 123456
    finally:
        service.stop()


def test_stop_before_start_keeps_unified_service_stopped():
    service = UnifiedIncrementalService(
        "scan", FakeSource(), {}, {"apply_mode": "direct"}
    )

    service.stop()

    assert service.start() is False
    assert service.running is False
    assert service.shutdown_event.is_set()


def test_stop_during_psync_handler_construction_never_starts_handler(monkeypatch):
    constructing = threading.Event()
    release_constructor = threading.Event()
    started = []

    class BlockingHandler:
        replication_thread = None
        last_error = None

        def __init__(self, **_kwargs):
            constructing.set()
            assert release_constructor.wait(2)

        def start_replication(self, _callback):
            started.append(True)
            return True

        def stop_replication(self):
            return None

    monkeypatch.setattr(
        unified_incremental_service, "PSyncIncrementalHandler", BlockingHandler
    )
    service = UnifiedIncrementalService(
        "psync", FakeSource(), {}, {"apply_mode": "key_state"}
    )
    worker = threading.Thread(target=service.start)
    worker.start()
    assert constructing.wait(1)

    service.stop()
    release_constructor.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert started == []
    assert service.running is False
    assert service.shutdown_event.is_set()


def test_psync_direct_mode_is_rejected_before_ambiguous_retries():
    manager, _pool = target_manager(db=0)
    with pytest.raises(ValueError, match="ambiguous replication write"):
        UnifiedIncrementalService(
            "psync",
            FakeSource(),
            {"target": manager},
            {"apply_mode": "direct"},
        )


def test_direct_mode_rejects_key_filters_that_cannot_split_multi_key_commands():
    manager, _pool = target_manager()
    with pytest.raises(ValueError, match="direct apply_mode"):
        UnifiedIncrementalService(
            "psync",
            FakeSource(),
            {"target": manager},
            {
                "apply_mode": "direct",
                "filters": {"include_patterns": ["public:*"]},
            },
        )


def test_scan_mode_can_still_construct_direct_one_shot_service():
    manager, _pool = target_manager()
    service = UnifiedIncrementalService(
        "scan", FakeSource(), {"target": manager}, {"apply_mode": "direct"}
    )
    try:
        assert service.apply_mode == "direct"
    finally:
        service.stop()


def test_unified_service_defaults_to_key_state_apply_mode():
    service = UnifiedIncrementalService("scan", FakeSource(), {}, {})
    try:
        assert service.apply_mode == "key_state"
    finally:
        service.stop()


def test_nonzero_command_dedup_window_is_rejected():
    with pytest.raises(ValueError, match="command_dedup_window"):
        UnifiedIncrementalService(
            "psync",
            FakeSource(),
            {},
            {"command_dedup_window": 1},
        )


def test_key_state_honors_incremental_key_pattern_and_key_types():
    manager, pool = target_manager()
    source = FakeSource(
        {
            b"managed:string": (b"string-dump", -1, None, b"string"),
            b"managed:list": (b"list-dump", -1, None, b"list"),
            b"other:string": (b"other-dump", -1, None, b"string"),
        }
    )
    service = service_for(
        source=source,
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "key_pattern": "managed:*",
            "key_types": ["string"],
        },
    )
    try:
        assert service._on_command_received([b"SET", b"managed:string", b"v"])
        assert service._on_command_received([b"LPUSH", b"managed:list", b"v"])
        assert service._on_command_received([b"SET", b"other:string", b"v"])
        assert pool.connection.sent[0] == (
            b"RESTORE", b"managed:string", b"0", b"string-dump", b"REPLACE"
        )
        assert pool.connection.sent[1][0] == b"EVAL"
        assert pool.connection.sent[1][2:4] == (b"1", b"managed:list")
    finally:
        service.stop()


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        ("h[^e]llo", b"hallo", True),
        ("h[^e]llo", b"hello", False),
        ("h[!x]llo", b"h!llo", True),
        ("h[!x]llo", b"hxllo", True),
        ("h[!x]llo", b"hallo", False),
        ("range:[a-c]", b"range:b", True),
        (r"literal\*", b"literal*", True),
        (r"literal\*", b"literal-anything", False),
        (b"bin:\xff?", b"bin:\xff\x00", True),
        (b"bin:\xff?", b"bin:\xfe\x00", False),
    ],
)
def test_full_and_realtime_filters_share_redis_glob_semantics(
    pattern, key, expected
):
    full_filter = KeySyncFilter(include_patterns=[pattern])
    service = service_for(
        config={
            "apply_mode": "key_state",
            "filters": {"include_patterns": [pattern]},
        }
    )
    try:
        assert full_filter.name_allowed(key) is expected
        assert service._key_name_allowed(key) is expected
    finally:
        service.stop()


def test_realtime_key_pattern_uses_redis_glob_semantics():
    service = service_for(
        config={"apply_mode": "key_state", "key_pattern": "h[^e]llo"}
    )
    try:
        assert service._key_name_allowed(b"hallo")
        assert not service._key_name_allowed(b"hello")
    finally:
        service.stop()


def test_snapshot_callback_uses_the_command_barrier():
    callback_entered = threading.Event()
    service = service_for(
        config={
            "snapshot_callback": lambda _data: callback_entered.set() or True,
        }
    )
    worker = None
    try:
        with service.command_barrier():
            worker = threading.Thread(target=service._snapshot_callback, args=(b"rdb",))
            worker.start()
            assert not callback_entered.wait(0.05)
        assert callback_entered.wait(1)
    finally:
        if worker is not None:
            worker.join(timeout=1)
        service.stop()


def test_key_state_mode_turns_non_idempotent_write_into_restore():
    manager, pool = target_manager(db=7)
    source = FakeSource({b"counter": (b"serialized-current-value", 2500)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state", "source_db": 0},
    )
    started_at_ms = int(time.time() * 1000)
    try:
        assert service._on_command_received([b"INCR", b"counter"])
        assert len(pool.connection.sent) == 1
        restore = pool.connection.sent[0]
        assert restore[:2] == (b"RESTORE", b"counter")
        assert started_at_ms < int(restore[2]) <= int(time.time() * 1000) + 2500
        assert restore[3:] == (
            b"serialized-current-value",
            b"REPLACE",
            b"ABSTTL",
        )
        assert not any(command[0] == b"INCR" for command in pool.connection.sent)
    finally:
        service.stop()


def test_command_key_lookup_falls_back_without_redis_command_info_parser():
    class LegacySource(FakeSource):
        command_getkeys = None

        def execute_command(self, *args):
            assert args == ("COMMAND GETKEYS", b"SET", b"key", b"value")
            return ["key"]

    service = service_for(source=LegacySource())
    try:
        assert service._command_keys([b"SET", b"key", b"value"]) == [b"key"]
    finally:
        service.stop()


def test_command_key_lookup_preserves_binary_keys_via_raw_connection():
    binary_key = b"binary:\xffkey"

    class RawCommandConnection:
        def __init__(self):
            self.sent = None

        def send_command(self, *args, **kwargs):
            assert kwargs == {"check_health": False}
            self.sent = args

        def read_response(self, **kwargs):
            assert kwargs == {"disable_decoding": True}
            return [binary_key]

    class RawCommandPool:
        connection_kwargs = {"db": 0}

        def __init__(self):
            self.connection = RawCommandConnection()
            self.released = []

        def get_connection(self, command_name=None):
            assert command_name == "COMMAND"
            return self.connection

        def release(self, connection):
            self.released.append(connection)

    source = FakeSource()
    source.connection_pool = RawCommandPool()
    service = service_for(source=source)
    try:
        command = [b"SET", binary_key, b"value"]
        assert service._command_keys(command) == [binary_key]
        assert source.connection_pool.connection.sent == (
            "COMMAND",
            "GETKEYS",
            *command,
        )
        assert source.connection_pool.released == [
            source.connection_pool.connection
        ]
    finally:
        service.stop()


def test_command_key_lookup_normalizes_bytes_like_results_without_stringifying():
    class BytesLikeSource(FakeSource):
        def command_getkeys(self, *_args):
            return [
                bytearray(b"bytearray-key"),
                memoryview(b"memoryview-key"),
                "text-key",
            ]

    source = BytesLikeSource()
    source.connection_pool = None
    service = service_for(source=source)
    try:
        assert service._command_keys([b"DEL", b"ignored"]) == [
            b"bytearray-key",
            b"memoryview-key",
            b"text-key",
        ]
    finally:
        service.stop()


def test_command_key_lookup_releases_raw_connection_after_response_error():
    class FailingConnection:
        def send_command(self, *_args, **_kwargs):
            pass

        def read_response(self, **_kwargs):
            raise ValueError("bad response")

    class RawCommandPool:
        connection_kwargs = {"db": 0}

        def __init__(self):
            self.connection = FailingConnection()
            self.released = []

        def get_connection(self, _command_name=None):
            return self.connection

        def release(self, connection):
            self.released.append(connection)

    source = FakeSource()
    source.connection_pool = RawCommandPool()
    service = service_for(source=source)
    try:
        with pytest.raises(RuntimeError, match="COMMAND GETKEYS rejected"):
            service._command_keys([b"SET", b"key", b"value"])
        assert source.connection_pool.released == [
            source.connection_pool.connection
        ]
    finally:
        service.stop()


def test_stop_interrupts_blocked_command_key_lookup_and_releases_connection():
    entered_read = threading.Event()
    disconnected = threading.Event()

    class BlockingConnection:
        def send_command(self, *_args, **_kwargs):
            pass

        def read_response(self, **_kwargs):
            entered_read.set()
            if not disconnected.wait(2):
                raise AssertionError("source connection was not interrupted")
            raise ConnectionError("connection closed")

        def disconnect(self):
            disconnected.set()

    class RawCommandPool:
        connection_kwargs = {"db": 0}

        def __init__(self):
            self.connection = BlockingConnection()
            self.released = []

        def get_connection(self, _command_name=None):
            return self.connection

        def release(self, connection):
            self.released.append(connection)

    source = FakeSource()
    source.connection_pool = RawCommandPool()
    service = service_for(source=source)
    errors = []

    def lookup():
        try:
            service._command_keys([b"SET", b"key", b"value"])
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=lookup)
    worker.start()
    assert entered_read.wait(1)
    service.stop()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert disconnected.is_set()
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert source.connection_pool.released == [source.connection_pool.connection]


def test_command_key_lookup_does_not_send_when_stop_wins_before_registration():
    acquiring = threading.Event()
    return_connection = threading.Event()

    class CommandConnection:
        def __init__(self):
            self.sent = False

        def send_command(self, *_args, **_kwargs):
            self.sent = True

    class SlowCommandPool:
        connection_kwargs = {"db": 0}

        def __init__(self):
            self.connection = CommandConnection()
            self.released = []

        def get_connection(self, _command_name=None):
            acquiring.set()
            assert return_connection.wait(2)
            return self.connection

        def release(self, connection):
            self.released.append(connection)

    source = FakeSource()
    source.connection_pool = SlowCommandPool()
    service = service_for(source=source)
    errors = []

    def lookup():
        try:
            service._command_keys([b"SET", b"key", b"value"])
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=lookup)
    worker.start()
    assert acquiring.wait(1)
    service.stop()
    return_connection.set()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert not source.connection_pool.connection.sent
    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert source.connection_pool.released == [source.connection_pool.connection]


def test_command_key_lookup_rejects_non_key_result_values():
    class InvalidSource(FakeSource):
        def command_getkeys(self, *_args):
            return [123]

    source = InvalidSource()
    source.connection_pool = None
    service = service_for(source=source)
    try:
        with pytest.raises(RuntimeError, match="invalid key of type int"):
            service._command_keys([b"DEL", b"ignored"])
    finally:
        service.stop()


def test_key_state_retry_uses_absolute_expiry_without_extending_ttl(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(
        unified_incremental_service.time, "time", lambda: clock[0]
    )
    monkeypatch.setattr(
        unified_incremental_service.time, "monotonic_ns", lambda: 0
    )
    healthy, healthy_pool = target_manager()
    flaky, flaky_pool = target_manager(failures=1)
    source = FakeSource({b"key": (b"dump", 2500)})
    successful_targets = []
    service = service_for(
        source=source,
        targets={"healthy": healthy, "flaky": flaky},
        config={
            "apply_mode": "key_state",
            "target_success_callback": successful_targets.append,
        },
    )
    try:
        assert not service._on_command_received([b"SET", b"key", b"value"])
        clock[0] += 1
        assert service._on_command_received([b"SET", b"key", b"value"])

        assert source.pipeline_executions == 1
        assert successful_targets == ["healthy", "flaky"]
        assert healthy_pool.connection.sent == [
            (
                b"RESTORE",
                b"key",
                b"102500",
                b"dump",
                b"REPLACE",
                b"ABSTTL",
            )
        ]
        assert flaky_pool.connection.sent == [
            (
                b"RESTORE",
                b"key",
                b"102500",
                b"dump",
                b"REPLACE",
                b"ABSTTL",
            ),
            (
                b"RESTORE",
                b"key",
                b"102500",
                b"dump",
                b"REPLACE",
                b"ABSTTL",
            ),
        ]
    finally:
        service.stop()


def test_key_state_uses_exact_pexpiretime_despite_source_rtt(monkeypatch):
    manager, pool = target_manager()
    source = FakeSource({b"key": (b"dump", 2500)})
    source.pexpiretimes = {b"key": 102_500}
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    monotonic_clock = iter((0, 10_000_000_000))
    monkeypatch.setattr(
        unified_incremental_service.time,
        "monotonic_ns",
        lambda: next(monotonic_clock),
    )
    monkeypatch.setattr(
        unified_incremental_service.time,
        "time",
        lambda: 100.0,
    )
    try:
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent == [
            (
                b"RESTORE",
                b"key",
                b"102500",
                b"dump",
                b"REPLACE",
                b"ABSTTL",
            )
        ]
    finally:
        service.stop()


def test_key_state_target_send_delay_does_not_change_deadline(monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(
        unified_incremental_service.time,
        "time",
        lambda: clock[0],
    )
    monkeypatch.setattr(
        unified_incremental_service.time,
        "monotonic_ns",
        lambda: 0,
    )
    manager, pool = target_manager()
    original_send = pool.connection.send_command

    def delayed_send(*args):
        clock[0] += 1
        original_send(*args)

    monkeypatch.setattr(pool.connection, "send_command", delayed_send)
    service = service_for(
        source=FakeSource({b"key": (b"dump", 2500)}),
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    try:
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent == [
            (
                b"RESTORE",
                b"key",
                b"102500",
                b"dump",
                b"REPLACE",
                b"ABSTTL",
            )
        ]
    finally:
        service.stop()


def test_key_state_deletes_key_that_expires_during_source_pipeline(monkeypatch):
    manager, pool = target_manager()
    source = FakeSource({b"key": (b"dump", 500)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    monotonic_clock = iter((0, 1_000_000_000))
    monkeypatch.setattr(
        unified_incremental_service.time,
        "monotonic_ns",
        lambda: next(monotonic_clock),
    )
    monkeypatch.setattr(
        unified_incremental_service.time, "time", lambda: 100.0
    )
    try:
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent == [(b"DEL", b"key")]
    finally:
        service.stop()


def test_key_state_restore_compatibility_fallback_keeps_target_active(monkeypatch):
    manager, pool = target_manager()
    source = FakeSource(
        {
            b"first": (b"newer-dump", -1),
            b"second": (b"compatible-dump", -1),
        }
    )
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    original_read_response = pool.connection.read_response
    compatibility_errors = 1

    def read_response():
        nonlocal compatibility_errors
        if (
            compatibility_errors
            and pool.connection.last_command[0].upper() == b"RESTORE"
        ):
            compatibility_errors -= 1
            raise redis.ResponseError(
                "ERR DUMP payload version or checksum are wrong"
            )
        return original_read_response()

    fallback_calls = []

    def fallback(source_client, target_client, key, *args, **options):
        fallback_calls.append((source_client, target_client, key, args, options))
        return True

    monkeypatch.setattr(pool.connection, "read_response", read_response)
    monkeypatch.setattr(
        unified_incremental_service, "_sync_key_fallback", fallback
    )
    failed_targets = []
    service.target_failure_callback = failed_targets.append

    try:
        assert service._on_command_received([b"SET", b"first", b"value"])
        assert service._on_command_received([b"SET", b"second", b"value"])

        assert failed_targets == []
        assert list(service.target_connections) == ["target"]
        assert fallback_calls == [
            (
                source,
                manager.target_client,
                b"first",
                (-1, True),
                {
                    "overwrite": True,
                    "expires_at_ms": None,
                    "expected_dump": b"newer-dump",
                    "key_types": set(),
                    "min_ttl": 0,
                    "max_key_size": 0,
                },
            )
        ]
        assert pool.connection.sent == [
            (b"RESTORE", b"first", b"0", b"newer-dump", b"REPLACE"),
            (b"RESTORE", b"second", b"0", b"compatible-dump", b"REPLACE"),
        ]
    finally:
        service.stop()


@pytest.mark.parametrize("pttl", [0, -2])
def test_key_state_pttl_zero_or_missing_deletes_stale_target_key(pttl):
    manager, pool = target_manager()
    source = FakeSource({b"key": (b"dump-that-must-not-be-restored", pttl)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    try:
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent == [(b"DEL", b"key")]
    finally:
        service.stop()


def test_key_state_rejects_invalid_negative_pttl():
    manager, pool = target_manager()
    service = service_for(
        source=FakeSource({b"key": (b"dump", -3)}),
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    try:
        assert not service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent == []
    finally:
        service.stop()


def test_key_state_rejects_invalid_pttl_without_touching_target():
    manager, pool = target_manager()
    source = FakeSource({b"key": (b"dump", -3)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state"},
    )
    try:
        with pytest.raises(ValueError, match="PTTL"):
            service._prepare_key_state([b"SET", b"key", b"value"])
        assert pool.connection.sent == []
    finally:
        service.stop()


def test_key_state_capture_caps_source_pipeline_batches_at_200():
    keys = [f"key:{index}".encode() for index in range(405)]
    source = FakeSource({key: (b"dump", -1) for key in keys})
    service = service_for(
        source=source,
        config={
            "apply_mode": "key_state",
            "pipeline_batch_size": 10_000,
        },
    )
    service._command_keys = lambda _command: keys
    try:
        operation, states = service._prepare_key_state([b"MSET"])

        assert operation == "keys"
        assert [state[0] for state in states] == keys
        assert [len(batch) // 2 for batch in source.pipeline_batches] == [200, 200, 5]
    finally:
        service.stop()


@pytest.mark.parametrize(
    ("filters", "state"),
    [
        ({"min_ttl": 2}, (b"dump", 1999)),
        ({"max_key_size": 100}, (b"dump", -1, 101)),
    ],
)
def test_key_state_value_filters_delete_previously_synced_target_key(filters, state):
    manager, pool = target_manager(scan_keys=[b"key"])
    source = FakeSource({b"key": state})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state", "filters": filters},
    )
    try:
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert pool.connection.sent[0][0] == b"EVAL"
        assert pool.connection.sent[0][2:4] == (b"1", b"key")
        assert manager.target_client.scan_keys == []
    finally:
        service.stop()


def test_key_state_filtered_missing_state_preserves_target_moved_out_after_prepare():
    key = b"changed-after-prepare"
    manager, pool = target_manager(
        scan_keys=[key],
        states={key: (-1, 50, b"string")},
    )
    manager.target_client.before_eval = lambda: manager.target_client.states.update(
        {key: (-1, 101, b"string")}
    )
    source = FakeSource({key: (b"source-dump", -1, 101)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "filters": {"max_key_size": 100},
        },
    )
    try:
        assert service._on_command_received([b"SET", key, b"value"])
        assert pool.connection.sent[0][0] == b"EVAL"
        assert manager.target_client.scan_keys == [key]
    finally:
        service.stop()


def test_key_state_filtered_expired_state_rechecks_target_before_delete():
    key = b"expired-before-apply"
    manager, pool = target_manager(
        scan_keys=[key],
        states={key: (-1, 50, b"string")},
    )
    manager.target_client.before_eval = lambda: manager.target_client.states.update(
        {key: (-1, 101, b"string")}
    )
    service = service_for(
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "filters": {"max_key_size": 100},
        },
    )
    try:
        assert service._apply_key_state_to_target(
            "target", manager, "keys", [(key, b"dump", 0)]
        )
        assert pool.connection.sent[0][0] == b"EVAL"
        assert manager.target_client.scan_keys == [key]
    finally:
        service.stop()


def test_key_state_filtered_flushdb_only_deletes_managed_keys():
    manager, pool = target_manager(
        scan_keys=[b"managed:one", b"private:keep", b"managed:two"]
    )
    service = service_for(
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "filters": {"include_patterns": ["managed:*"]},
        },
    )
    try:
        assert service._on_command_received([b"FLUSHDB"])
        assert len(pool.connection.sent) == 1
        command = pool.connection.sent[0]
        assert command[0] == b"EVAL"
        assert command[2] == b"2"
        assert command[3:5] == (b"managed:one", b"managed:two")
        assert manager.target_client.scan_keys == [b"private:keep"]
        assert all(command[0] != b"FLUSHDB" for command in pool.connection.sent)
    finally:
        service.stop()


def test_key_state_filtered_flushdb_preserves_keys_outside_value_filters():
    keys = [b"managed", b"short-lived", b"oversized"]
    manager, pool = target_manager(
        scan_keys=keys,
        states={
            b"managed": (-1, 100, b"string"),
            b"short-lived": (9, 50, b"string"),
            b"oversized": (-1, 101, b"string"),
        },
    )
    service = service_for(
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "filters": {"min_ttl": 10, "max_key_size": 100},
        },
    )
    try:
        assert service._on_command_received([b"FLUSHDB"])
        assert pool.connection.sent[0][0] == b"EVAL"
        assert manager.target_client.scan_keys == [b"short-lived", b"oversized"]
        assert manager.target_client.pipeline_operations == []
    finally:
        service.stop()


def test_key_state_filtered_flushdb_caps_type_and_delete_batches_at_200():
    keys = [f"managed:{index}".encode() for index in range(405)]
    manager, pool = target_manager(scan_keys=keys)
    service = service_for(
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "key_types": ["string"],
            "pipeline_batch_size": 10_000,
        },
    )
    try:
        assert service.pipeline_batch_size == 200
        assert service._on_command_received([b"FLUSHDB"])
        delete_commands = [
            command for command in pool.connection.sent if command[0] == b"EVAL"
        ]
        assert [int(command[2]) for command in delete_commands] == [200, 200, 5]
        assert manager.target_client.scan_keys == []
    finally:
        service.stop()


@pytest.mark.parametrize("flush_command", [b"FLUSHDB", b"FLUSHALL"])
@pytest.mark.parametrize(
    ("config", "changed_state"),
    [
        ({"key_types": ["string"]}, (-1, 50, b"list")),
        ({"filters": {"min_ttl": 10}}, (9, 50, b"string")),
        ({"filters": {"max_key_size": 100}}, (-1, 101, b"string")),
    ],
)
def test_key_state_filtered_flush_rechecks_dynamic_scope_inside_delete_script(
    flush_command, config, changed_state
):
    key = b"changed-after-scan"
    manager, pool = target_manager(
        scan_keys=[key],
        states={key: (-1, 50, b"string")},
    )
    manager.target_client.before_eval = lambda: manager.target_client.states.update(
        {key: changed_state}
    )
    service_config = {"apply_mode": "key_state"}
    service_config.update(config)
    service = service_for(
        targets={"target": manager},
        config=service_config,
    )
    try:
        assert service._on_command_received([flush_command])
        assert pool.connection.sent[0][0] == b"EVAL"
        assert manager.target_client.scan_keys == [key]
    finally:
        service.stop()


def test_key_state_flushall_applies_to_source_db_even_when_stream_selected_another_db():
    manager, pool = target_manager(db=7)
    service = service_for(
        targets={"target": manager},
        config={"apply_mode": "key_state", "source_db": 0},
    )
    try:
        assert service._on_command_received([b"SELECT", b"4"])
        assert service._on_command_received([b"FLUSHALL"])
        assert pool.connection.sent == [(b"FLUSHDB",)]
    finally:
        service.stop()


def test_key_state_move_reconciles_both_sides_of_source_db_boundary():
    manager, pool = target_manager()
    source = FakeSource({b"moved-in": (b"current-dump", -1)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state", "source_db": 0},
    )
    try:
        assert service._on_command_received([b"MOVE", b"moved-out", b"1"])
        assert service._on_command_received([b"SELECT", b"1"])
        assert service._on_command_received([b"MOVE", b"moved-in", b"0"])
        assert pool.connection.sent == [
            (b"DEL", b"moved-out"),
            (b"RESTORE", b"moved-in", b"0", b"current-dump", b"REPLACE"),
        ]
    finally:
        service.stop()


def test_key_state_copy_into_source_db_reconciles_destination_and_copy_out_is_ignored():
    manager, pool = target_manager()
    source = FakeSource({b"copied-in": (b"copied-dump", -1)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state", "source_db": 0},
    )
    try:
        assert service._on_command_received([b"SELECT", b"2"])
        assert service._on_command_received(
            [b"COPY", b"remote-source", b"copied-in", b"DB", b"0", b"REPLACE"]
        )
        assert service._on_command_received([b"SELECT", b"0"])
        assert service._on_command_received(
            [b"COPY", b"local-source", b"copied-out", b"DB", b"2"]
        )
        assert pool.connection.sent == [
            (b"RESTORE", b"copied-in", b"0", b"copied-dump", b"REPLACE")
        ]
    finally:
        service.stop()


def test_key_state_swapdb_invokes_database_resync_callback():
    manager, pool = target_manager()
    callbacks = []
    service = service_for(
        targets={"target": manager},
        config={
            "apply_mode": "key_state",
            "source_db": 0,
            "database_resync_callback": lambda: callbacks.append("resync"),
        },
    )
    try:
        assert service._on_command_received([b"SWAPDB", b"0", b"3"])
        assert callbacks == ["resync"]
        assert pool.connection.sent == []
    finally:
        service.stop()


def test_failed_target_is_removed_while_healthy_target_keeps_receiving_key_state():
    healthy, healthy_pool = target_manager()
    failing, failing_pool = target_manager(failures=1)
    source = FakeSource(
        {
            b"first": (b"first-dump", -1),
            b"second": (b"second-dump", -1),
        }
    )
    failed_targets = []
    service = service_for(
        source=source,
        targets={"healthy": healthy, "failing": failing},
        config={"apply_mode": "key_state"},
    )

    def remove_failed_target(target_name):
        failed_targets.append(target_name)
        service.unregister_target(target_name)

    service.target_failure_callback = remove_failed_target
    try:
        assert service._on_command_received([b"SET", b"first", b"value"])
        assert failed_targets == ["failing"]
        assert list(service.target_connections) == ["healthy"]

        assert service._on_command_received([b"SET", b"second", b"value"])
        assert healthy_pool.connection.sent == [
            (b"RESTORE", b"first", b"0", b"first-dump", b"REPLACE"),
            (b"RESTORE", b"second", b"0", b"second-dump", b"REPLACE"),
        ]
        assert failing_pool.connection.sent == [
            (b"RESTORE", b"first", b"0", b"first-dump", b"REPLACE")
        ]
        assert failing_pool.connection.disconnected
    finally:
        service.stop()


def test_key_state_mode_tracks_select_and_ignores_other_source_databases():
    manager, pool = target_manager()
    source = FakeSource({b"key": (b"dump", -1)})
    service = service_for(
        source=source,
        targets={"target": manager},
        config={"apply_mode": "key_state", "source_db": 0},
    )
    try:
        assert service._on_command_received([b"SELECT", b"1"])
        assert service._on_command_received([b"SET", b"key", b"value"])
        assert source.getkeys_calls == []
        assert pool.connection.sent == []
    finally:
        service.stop()


def test_target_timeout_waits_for_inflight_command_before_next_command():
    manager, pool = target_manager(delay=0.05)
    service = service_for(
        targets={"target": manager},
        config={"target_command_timeout": 0.01},
    )
    results = []
    def deliver(command):
        with service._command_lock:
            return service._sync_command_to_targets(command)
    try:
        first = threading.Thread(
            target=lambda: results.append(
                deliver([b"SET", b"key", b"first"])
            )
        )
        second = threading.Thread(
            target=lambda: results.append(
                deliver([b"SET", b"key", b"second"])
            )
        )
        first.start()
        time.sleep(0.005)
        second.start()
        first.join(timeout=2)
        second.join(timeout=2)
        assert not first.is_alive() and not second.is_alive()
        assert results == [True, True]
        assert pool.connection.max_active == 1
        assert pool.connection.sent == [
            (b"SET", b"key", b"first"),
            (b"SET", b"key", b"second"),
        ]
    finally:
        service.stop()
