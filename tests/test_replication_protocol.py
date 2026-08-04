import os
import socket
import threading
import time
import tracemalloc

import pytest

import redis_sync.psync_incremental_handler as psync_module
import redis_sync.redis_protocol as protocol_module
from redis_sync.psync_incremental_handler import (
    PSyncIncrementalHandler,
    _DiskBackedCapture,
)
from redis_sync.redis_protocol import (
    MAX_REPLICATION_BUFFER_SIZE,
    ReplicationProtocolError,
    ReplicationStreamReader,
)
from redis_sync.sync_handler import SyncHandler


def resp_command(*parts: bytes) -> bytes:
    frame = f"*{len(parts)}\r\n".encode()
    for part in parts:
        frame += f"${len(part)}\r\n".encode() + part + b"\r\n"
    return frame


class ChunkSocket:
    def __init__(self, chunks, timeout_when_empty=False):
        self.chunks = list(chunks)
        self.timeout_when_empty = timeout_when_empty
        self.timeout = None

    def recv(self, size):
        if not self.chunks:
            if self.timeout_when_empty:
                raise socket.timeout()
            return b""
        data = self.chunks.pop(0)
        if len(data) > size:
            self.chunks.insert(0, data[size:])
            data = data[:size]
        return data

    def settimeout(self, timeout):
        self.timeout = timeout


def test_replication_reader_rejects_oversized_socket_buffer():
    with pytest.raises(ValueError, match="buffer_size"):
        ReplicationStreamReader(
            ChunkSocket([]),
            buffer_size=MAX_REPLICATION_BUFFER_SIZE + 1,
        )


def test_replication_reader_parses_split_bulk_body_without_reparsing_headers(
    monkeypatch,
):
    payload = b"x" * 4096
    frame = resp_command(b"SET", b"key", payload)
    payload_start = frame.index(payload)
    chunks = [frame[: payload_start + 1]] + [
        frame[offset : offset + 31]
        for offset in range(payload_start + 1, len(frame), 31)
    ]
    calls = []
    original = protocol_module._IncrementalArrayCommandParser._parse_header

    def record_header(data, cursor, prefix, label):
        parsed = original(data, cursor, prefix, label)
        if parsed is not None:
            calls.append((cursor, prefix, label))
        return parsed

    monkeypatch.setattr(
        protocol_module._IncrementalArrayCommandParser,
        "_parse_header",
        staticmethod(record_header),
    )
    reader = ReplicationStreamReader(ChunkSocket(chunks), buffer_size=64)

    command, consumed = reader.peek_command()

    assert command == [b"SET", b"key", payload]
    assert consumed == len(frame)
    assert [prefix for _cursor, prefix, _label in calls] == [b"*", b"$", b"$", b"$"]


def test_incremental_parser_materializes_large_bulk_with_one_payload_copy():
    payload_size = 8 * 1024 * 1024
    prefix = (
        b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$"
        + str(payload_size).encode("ascii")
        + b"\r\n"
    )
    pending = bytearray(prefix + b"x" * payload_size)
    parser = protocol_module._IncrementalArrayCommandParser()
    assert parser.parse(pending) is None
    pending.extend(b"\r\n")

    tracemalloc.start()
    try:
        command, consumed = parser.parse(pending)
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert command[:2] == [b"SET", b"key"]
    assert len(command[2]) == payload_size
    assert command[2][:1] == b"x"
    assert command[2][-1:] == b"x"
    assert consumed == len(pending)
    assert peak < payload_size * 3 // 2


def test_replication_reader_rejects_oversized_bulk_from_declaration(monkeypatch):
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_BULK_SIZE", 8)
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_FRAME_SIZE", 64)
    reader = ReplicationStreamReader(ChunkSocket([b"*1\r\n$9\r\n"]))

    with pytest.raises(ReplicationProtocolError, match="bulk length"):
        reader.peek_command()


def test_replication_reader_rejects_oversized_frame_from_declaration(monkeypatch):
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_BULK_SIZE", 32)
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_FRAME_SIZE", 20)
    reader = ReplicationStreamReader(ChunkSocket([b"*1\r\n$16\r\n"]))

    with pytest.raises(ReplicationProtocolError, match="frame"):
        reader.peek_command()


def test_replication_reader_rejects_oversized_initial_pending_buffer(monkeypatch):
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_PENDING_SIZE", 16)

    with pytest.raises(ReplicationProtocolError, match="pending buffer"):
        ReplicationStreamReader(ChunkSocket([]), initial_buffer=b"x" * 17)


def test_replication_reader_default_buffer_isolation_and_explicit_ownership():
    initial = bytearray(b"pending")
    isolated = ReplicationStreamReader(
        ChunkSocket([]), initial_buffer=initial
    )

    initial[:] = b"changed"
    assert isolated.pending_bytes == b"pending"
    assert isolated._buffer is not initial

    adopted = bytearray(b"adopted")
    owner = ReplicationStreamReader(
        ChunkSocket([]),
        initial_buffer=adopted,
        adopt_initial_buffer=True,
    )
    assert owner._buffer is adopted

    transferred = owner.take_buffer_ownership()
    assert transferred is adopted
    assert owner.pending_size == 0
    assert owner._buffer is not adopted

    with pytest.raises(TypeError, match="requires a bytearray"):
        ReplicationStreamReader(
            ChunkSocket([]),
            initial_buffer=b"bytes",
            adopt_initial_buffer=True,
        )


def test_replication_reader_adoption_does_not_copy_large_pending_buffer():
    pending = bytearray(b"x" * (8 * 1024 * 1024))

    tracemalloc.start()
    try:
        reader = ReplicationStreamReader(
            ChunkSocket([]),
            initial_buffer=pending,
            adopt_initial_buffer=True,
        )
        transferred = reader.take_buffer_ownership()
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert transferred is pending
    assert peak < 1024 * 1024


def test_replication_reader_rejects_accumulated_pending_buffer(monkeypatch):
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_PENDING_SIZE", 16)
    reader = ReplicationStreamReader(ChunkSocket([b"+" + b"x" * 16]))

    with pytest.raises(ReplicationProtocolError, match="pending buffer"):
        reader.read_response()


def test_replication_reader_rejects_oversized_rdb_header(monkeypatch):
    monkeypatch.setattr(protocol_module, "_MAX_RESP_HEADER_SIZE", 8)
    reader = ReplicationStreamReader(ChunkSocket([b"$123456789"]))

    with pytest.raises(ReplicationProtocolError, match="header"):
        reader.read_rdb()


def test_replication_reader_rejects_excessive_argument_count(monkeypatch):
    monkeypatch.setattr(protocol_module, "MAX_REPLICATION_ARGUMENTS", 2)
    reader = ReplicationStreamReader(ChunkSocket([b"*3\r\n"]))

    with pytest.raises(ReplicationProtocolError, match="array length"):
        reader.peek_command()


class FeedSocket(ChunkSocket):
    def __init__(self, chunks):
        super().__init__([])
        self.chunks = list(chunks)
        self.closed = False
        self.condition = threading.Condition()

    def recv(self, size):
        with self.condition:
            deadline = time.monotonic() + (self.timeout or 0.1)
            while not self.chunks and not self.closed:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise socket.timeout()
                self.condition.wait(remaining)
            if self.closed:
                return b""
            data = self.chunks.pop(0)
            if len(data) > size:
                self.chunks.insert(0, data[size:])
                data = data[:size]
            return data

    def feed(self, data):
        with self.condition:
            self.chunks.append(data)
            self.condition.notify_all()

    def close(self):
        with self.condition:
            self.closed = True
            self.condition.notify_all()


class ResetWhenEmptySocket(ChunkSocket):
    def recv(self, size):
        if self.chunks:
            return super().recv(size)
        raise ConnectionResetError("injected capture reset")


class ResetAfterCRSocket(ChunkSocket):
    def __init__(self):
        super().__init__([b"\r"])

    def recv(self, size):
        if self.chunks:
            return super().recv(size)
        raise ConnectionResetError("injected reset")


class FakeConnection:
    def __init__(self, sock):
        self._sock = sock
        self.sent = []
        self.disconnected = False

    def send_command(self, *args, **kwargs):
        assert kwargs.get("check_health") is False
        self.sent.append(args)

    def disconnect(self):
        self.disconnected = True
        close = getattr(self._sock, "close", None)
        if close is not None:
            close()

    def read_response(self):
        raise AssertionError("replication must not use redis-py's parser")


class FakePool:
    def __init__(self, connection):
        self.connection = connection
        self.released = []

    def get_connection(self, name=None):
        return self.connection

    def release(self, connection):
        self.released.append(connection)


class SequencePool:
    def __init__(self, connections):
        self.connections = list(connections)
        self.requests = []
        self.released = []

    def get_connection(self, name=None):
        self.requests.append(name)
        return self.connections.pop(0)

    def release(self, connection):
        self.released.append(connection)


def fake_source(connection):
    source = type("Source", (), {})()
    source.connection_pool = FakePool(connection)
    source.info = lambda section=None: {
        "role": "master",
        "master_repl_offset": 0,
    }
    return source


def fake_source_with_pool(pool):
    source = type("Source", (), {})()
    source.connection_pool = pool
    return source


def test_reader_keeps_coalesced_command_after_fixed_length_rdb():
    rdb = b"REDIS0012payload"
    command = resp_command(b"SET", b"key", b"value")
    wire = b"$" + str(len(rdb)).encode() + b"\r\n" + rdb + command
    reader = ReplicationStreamReader(ChunkSocket([wire]))

    assert reader.read_rdb() == rdb
    parsed, consumed = reader.peek_command()
    assert parsed == [b"SET", b"key", b"value"]
    assert consumed == len(command)


def test_reader_ignores_bare_lf_keepalive_before_psync_response():
    reader = ReplicationStreamReader(
        ChunkSocket([b"\n+FULLRESYNC replid 42\r\n"])
    )

    assert reader.read_response() == b"FULLRESYNC replid 42"


def test_reader_ignores_bare_lf_keepalive_before_rdb_header():
    rdb = b"REDIS0012payload"
    wire = b"\n$" + str(len(rdb)).encode() + b"\r\n" + rdb
    reader = ReplicationStreamReader(ChunkSocket([wire]))

    assert reader.read_rdb() == rdb


def test_reader_accepts_optional_snapshot_crlf_before_coalesced_command():
    rdb = b"REDIS0012payload"
    command = resp_command(b"SET", b"key", b"value")
    wire = b"$" + str(len(rdb)).encode() + b"\r\n" + rdb + b"\r\n" + command
    reader = ReplicationStreamReader(ChunkSocket([wire]))

    assert reader.read_rdb() == rdb
    parsed, consumed = reader.peek_command()
    assert parsed == [b"SET", b"key", b"value"]
    assert consumed == len(command)


def test_reader_accepts_split_optional_snapshot_crlf_without_collecting_rdb():
    rdb = b"REDIS0012payload"
    command = resp_command(b"PING")
    header = b"$" + str(len(rdb)).encode() + b"\r\n"
    reader = ReplicationStreamReader(
        ChunkSocket([header + rdb + b"\r", b"\n" + command])
    )

    assert reader.read_rdb(collect=False) == b""
    parsed, consumed = reader.peek_command()
    assert parsed == [b"PING"]
    assert consumed == len(command)


def test_reader_supports_diskless_eof_rdb_split_across_reads():
    marker = b"0123456789012345678901234567890123456789"
    rdb = b"REDIS0012payload-with-boundary-prefix-012345"
    command = resp_command(b"PING")
    header = b"$EOF:" + marker + b"\r\n"
    reader = ReplicationStreamReader(
        ChunkSocket([header + rdb[:10], rdb[10:] + marker[:12], marker[12:] + command])
    )

    assert reader.read_rdb() == rdb
    assert reader.peek_command()[0] == [b"PING"]


def test_psync_does_not_commit_partial_frame_and_preserves_it_for_resume():
    command = resp_command(b"SET", b"k", b"v")
    cut = 7
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 100
    handler._received_offset = 100

    first_connection = FakeConnection(ChunkSocket([command[:cut]]))
    assert handler._receive_command_stream(first_connection, lambda _: True) is False
    assert handler.replication_offset == 100
    assert handler.received_offset == 100 + cut
    assert handler.pending_buffer == command[:cut]

    second_connection = FakeConnection(ChunkSocket([command[cut:]]))
    resumed_reader = ReplicationStreamReader(
        second_connection._sock,
        initial_buffer=handler.pending_buffer,
        on_socket_read=handler._record_received,
    )
    delivered = []

    def callback(frame):
        delivered.append(frame)
        handler.running = False
        return True

    assert handler._receive_command_stream(
        second_connection, callback, reader=resumed_reader
    ) is True
    assert delivered == [[b"SET", b"k", b"v"]]
    assert handler.replication_offset == 100 + len(command)
    assert handler.received_offset == 100 + len(command)
    assert handler.pending_buffer == b""


def test_psync_timeout_reads_pending_length_without_copying_frame():
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 100

    class TimeoutReader:
        def __init__(self):
            self.pending_size_calls = 0
            self.pending_bytes_calls = 0

        def peek_command(self):
            handler.running = False
            raise socket.timeout()

        @property
        def pending_size(self):
            self.pending_size_calls += 1
            return 7

        @property
        def pending_bytes(self):
            self.pending_bytes_calls += 1
            return b"partial"

    reader = TimeoutReader()
    connection = FakeConnection(ChunkSocket([]))

    assert handler._receive_command_stream(
        connection,
        lambda _command: True,
        reader=reader,
        _manage_ack=False,
    ) is True
    assert handler.received_offset == 107
    assert handler.pending_buffer == b"partial"
    assert reader.pending_size_calls == 1
    assert reader.pending_bytes_calls == 1


def test_psync_callback_retries_do_not_copy_pending_frame():
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 100

    class NoWaitEvent:
        @staticmethod
        def wait(_timeout):
            return False

    class CountingReader:
        def __init__(self):
            self.pending_bytes_calls = 0
            self.commits = []
            self._pending_size = 12

        @staticmethod
        def peek_command():
            return [b"SET", b"key", b"value"], 12

        @property
        def pending_size(self):
            return self._pending_size

        @property
        def pending_bytes(self):
            self.pending_bytes_calls += 1
            return b""

        def commit(self, consumed):
            self.commits.append(consumed)
            self._pending_size -= consumed

    handler._stop_event = NoWaitEvent()
    reader = CountingReader()
    connection = FakeConnection(ChunkSocket([]))
    callback_calls = []

    def retry_then_succeed(_command):
        callback_calls.append(True)
        if len(callback_calls) < 4:
            return False
        handler.running = False
        return True

    assert handler._receive_command_stream(
        connection,
        retry_then_succeed,
        reader=reader,
        _manage_ack=False,
        _retry_callbacks=True,
    ) is True
    assert len(callback_calls) == 4
    assert reader.commits == [12]
    assert reader.pending_bytes_calls == 1
    assert handler.received_offset == 112


def test_psync_callback_failure_leaves_frame_uncommitted_and_unacked():
    command = resp_command(b"INCR", b"counter")
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 40
    handler._received_offset = 40
    connection = FakeConnection(ChunkSocket([command], timeout_when_empty=True))

    assert handler._receive_command_stream(connection, lambda _: False) is False
    assert handler.replication_offset == 40
    assert handler.pending_buffer == command
    ack_offsets = [
        parts[2]
        for parts in connection.sent
        if parts[:2] == ("REPLCONF", "ACK")
    ]
    assert ack_offsets
    assert set(ack_offsets) == {40}


def test_psync_ack_heartbeat_runs_while_command_callback_is_blocked():
    command = resp_command(b"SET", b"key", b"value")
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 70
    handler._received_offset = 70
    connection = FakeConnection(ChunkSocket([command], timeout_when_empty=True))

    def slow_callback(_command):
        time.sleep(0.045)
        handler.running = False
        return True

    assert handler._receive_command_stream(connection, slow_callback) is True
    ack_offsets = [
        parts[2]
        for parts in connection.sent
        if parts[:2] == ("REPLCONF", "ACK")
    ]
    assert len(ack_offsets) >= 3
    assert set(ack_offsets) == {70}


def test_psync_partial_rdb_terminator_is_not_counted_as_backlog():
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 50
    handler._received_offset = 50
    connection = FakeConnection(ChunkSocket([b"\r"]))
    reader = ReplicationStreamReader(
        connection._sock,
        allow_leading_crlf=True,
    )

    assert handler._receive_command_stream(
        connection, lambda _command: True, reader=reader
    ) is False
    assert handler.received_offset == 50
    assert handler.pending_buffer == b""


def test_psync_reset_after_partial_rdb_terminator_keeps_backlog_offset():
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True
    handler.replication_offset = 50
    handler._received_offset = 50
    connection = FakeConnection(ResetAfterCRSocket())
    reader = ReplicationStreamReader(
        connection._sock,
        allow_leading_crlf=True,
    )

    assert handler._receive_command_stream(
        connection, lambda _command: True, reader=reader
    ) is False
    assert handler.received_offset == 50
    assert handler.pending_buffer == b""


def test_psync_getack_replies_after_committing_request_frame():
    command = resp_command(b"REPLCONF", b"GETACK", b"*")
    handler = PSyncIncrementalHandler(object(), ack_interval=10)
    handler.running = True
    handler.replication_offset = 12
    handler._received_offset = 12
    connection = FakeConnection(ChunkSocket([command], timeout_when_empty=True))

    def callback(_command):
        handler.running = False
        return True

    assert handler._receive_command_stream(connection, callback) is True
    committed = 12 + len(command)
    assert any(
        parts == ("REPLCONF", "ACK", committed)
        for parts in connection.sent
    )


def test_psync_resume_requests_the_byte_after_the_last_received_offset():
    command = resp_command(b"SET", b"k", b"v")
    cut = 7
    wire = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+CONTINUE replid\r\n"
        + command[cut:]
    )
    connection = FakeConnection(ChunkSocket([wire], timeout_when_empty=True))
    handler = PSyncIncrementalHandler(fake_source(connection), ack_interval=0.01)
    handler.running = True
    handler.replication_id = "replid"
    handler.replication_offset = 100
    handler._received_offset = 100 + cut
    handler._pending_buffer = command[:cut]
    delivered = []

    def callback(frame):
        delivered.append(frame)
        handler.running = False
        return True

    handler._replication_loop(callback)

    assert ("PSYNC", "replid", 100 + cut + 1) in connection.sent
    assert delivered == [[b"SET", b"k", b"v"]]
    assert handler.replication_offset == 100 + len(command)
    assert handler.received_offset == 100 + len(command)


def test_psync_pending_assembly_adopts_prefetch_and_extends_pending_in_place():
    handler = PSyncIncrementalHandler(object())
    prefetched = bytearray(b"prefetched")

    assembled = handler._assemble_pending_buffer(prefetched)

    assert assembled is prefetched
    assert handler._pending_buffer == bytearray()

    pending = bytearray(b"partial-")
    handler._pending_buffer = pending
    next_prefetch = bytearray(b"remainder")

    assembled = handler._assemble_pending_buffer(next_prefetch)

    assert assembled is pending
    assert assembled == b"partial-remainder"
    assert next_prefetch == bytearray()
    assert handler._pending_buffer == bytearray()


def test_psync_pending_assembly_rejects_limit_before_transferring_ownership(
    monkeypatch,
):
    monkeypatch.setattr(psync_module, "MAX_REPLICATION_PENDING_SIZE", 16)
    handler = PSyncIncrementalHandler(object())
    handler.replication_offset = 100
    handler._received_offset = 112
    pending = bytearray(b"old-pending!")
    prefetched = bytearray(b"new-data")
    handler._pending_buffer = pending

    with pytest.raises(ReplicationProtocolError, match="pending buffer"):
        handler._assemble_pending_buffer(prefetched)

    assert handler._pending_buffer is pending
    assert handler.pending_buffer == b"old-pending!"
    assert prefetched == b"new-data"
    assert handler.replication_offset == 100
    assert handler.received_offset == 112


def test_psync_fullresync_prefetch_assembly_has_no_large_buffer_copy():
    handler = PSyncIncrementalHandler(object())
    prefetched = bytearray(b"x" * (8 * 1024 * 1024))

    tracemalloc.start()
    try:
        initial_buffer = handler._assemble_pending_buffer(prefetched)
        reader = ReplicationStreamReader(
            ChunkSocket([]),
            initial_buffer=initial_buffer,
            adopt_initial_buffer=True,
        )
        _current, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert initial_buffer is prefetched
    assert reader._buffer is prefetched
    assert peak < 1024 * 1024


def test_psync_does_not_count_optional_rdb_crlf_in_replication_offset():
    rdb = b"REDIS0012snapshot"
    command = resp_command(b"PING")
    wire = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+FULLRESYNC replid 50\r\n"
        + b"$"
        + str(len(rdb)).encode()
        + b"\r\n"
        + rdb
        + b"\r\n"
        + command
    )
    connection = FakeConnection(ChunkSocket([wire], timeout_when_empty=True))
    handler = PSyncIncrementalHandler(
        fake_source(connection),
        snapshot_callback=lambda _: True,
        ack_interval=0.01,
    )
    handler.running = True

    def callback(_frame):
        handler.running = False
        return True

    handler._replication_loop(callback)

    assert handler.replication_offset == 50 + len(command)
    assert handler.received_offset == 50 + len(command)


def test_psync_ack_heartbeat_runs_during_snapshot_callback():
    rdb = b"REDIS0012snapshot"
    wire = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+FULLRESYNC replid 50\r\n"
        + b"$"
        + str(len(rdb)).encode()
        + b"\r\n"
        + rdb
    )
    connection = FakeConnection(ChunkSocket([wire], timeout_when_empty=True))
    handler = PSyncIncrementalHandler(
        fake_source(connection),
        ack_interval=0.01,
    )
    handler.running = True

    def apply_snapshot(_rdb):
        time.sleep(0.045)
        handler.running = False
        return True

    handler.snapshot_callback = apply_snapshot
    handler._replication_loop(lambda _frame: True)

    ack_offsets = [
        parts[2]
        for parts in connection.sent
        if parts[:2] == ("REPLCONF", "ACK")
    ]
    assert len(ack_offsets) >= 3
    assert set(ack_offsets) == {0}


def test_psync_drains_and_replays_commands_during_snapshot_callback():
    rdb = b"REDIS0012snapshot"
    command = resp_command(b"SET", b"during-snapshot", b"value")
    handshake_and_rdb = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+FULLRESYNC replid 80\r\n"
        + b"$"
        + str(len(rdb)).encode()
        + b"\r\n"
        + rdb
    )
    connection = FakeConnection(
        ChunkSocket(
            [handshake_and_rdb, command],
            timeout_when_empty=True,
        )
    )
    delivered = []
    handler = PSyncIncrementalHandler(
        fake_source(connection),
        snapshot_callback=lambda _rdb: time.sleep(0.04) or True,
        ack_interval=0.01,
    )
    handler.running = True

    def callback(frame):
        delivered.append(frame)
        handler.running = False
        return True

    handler._replication_loop(callback)

    assert delivered == [[b"SET", b"during-snapshot", b"value"]]
    assert handler.replication_offset == 80 + len(command)


def test_disk_backed_capture_reclaims_consumed_prefix_and_rolls_when_needed():
    capture = _DiskBackedCapture(max_memory_size=32)
    try:
        for _ in range(20):
            capture.append(b"x" * 16)
            assert capture.read(16) == b"x" * 16
            assert capture.stored_size == 0
        assert not capture.rolled_to_disk

        payload = b"y" * 96
        capture.append(payload)
        assert capture.rolled_to_disk
        capture.seal()
        assert capture.read() == payload
        assert capture.stored_size == 0
        assert capture.read() == b""
    finally:
        capture.close()


def test_disk_backed_capture_limit_rejects_append_and_cleans_all_segments(
    monkeypatch, tmp_path
):
    original_named_temporary_file = psync_module.tempfile.NamedTemporaryFile

    def temporary_file_in_test_directory(**kwargs):
        return original_named_temporary_file(dir=str(tmp_path), **kwargs)

    monkeypatch.setattr(
        psync_module.tempfile,
        "NamedTemporaryFile",
        temporary_file_in_test_directory,
    )
    capture = _DiskBackedCapture(max_memory_size=4, max_total_size=8)
    capture.append(b"abcdefgh")
    assert capture.stored_size == 8
    assert list(tmp_path.iterdir())

    with pytest.raises(
        ReplicationProtocolError,
        match="capture pending data exceeds configured limit: 9 > 8 bytes",
    ):
        capture.append(b"i")

    assert capture.stored_size == 0
    assert list(capture._segments) == []
    assert list(tmp_path.iterdir()) == []
    with pytest.raises(ReplicationProtocolError, match="configured limit"):
        capture.read()
    capture.close()


def test_capture_limit_disconnects_drain_and_cleans_spooled_data(
    monkeypatch, tmp_path
):
    original_named_temporary_file = psync_module.tempfile.NamedTemporaryFile

    def temporary_file_in_test_directory(**kwargs):
        return original_named_temporary_file(dir=str(tmp_path), **kwargs)

    monkeypatch.setattr(
        psync_module.tempfile,
        "NamedTemporaryFile",
        temporary_file_in_test_directory,
    )
    connection = FakeConnection(
        ChunkSocket([b"abcd", b"efgh", b"i"], timeout_when_empty=True)
    )
    handler = PSyncIncrementalHandler(
        object(),
        buffer_size=4,
        ack_interval=0.01,
        capture_max_size=8,
    )
    handler.CAPTURE_MEMORY_SIZE = 4

    capture_state = handler._start_snapshot_stream_capture(connection)
    capture_state[3].join(timeout=1)

    capture, _stop_event, errors, thread, _connection = capture_state
    assert not thread.is_alive()
    assert connection.disconnected
    assert len(errors) == 1
    assert "capture pending data exceeds configured limit: 9 > 8 bytes" in str(
        errors[0]
    )
    assert capture.stored_size == 0
    assert list(capture._segments) == []
    assert list(tmp_path.iterdir()) == []
    handler._stop_snapshot_stream_capture(capture_state)
    capture.close()


def test_disk_backed_capture_closes_sealed_files_until_they_are_consumed():
    capture = _DiskBackedCapture(max_memory_size=32)
    paths = []
    try:
        payload = b"z" * (32 * 5 + 1)
        capture.append(payload)

        segments = list(capture._segments)
        sealed = segments[:-1]
        paths = [segment.path for segment in sealed]
        assert len(sealed) == 5
        assert all(segment.sealed for segment in sealed)
        assert all(segment.file is None for segment in sealed)
        assert all(path is not None and os.path.exists(path) for path in paths)
        assert sum(segment.file is not None for segment in segments) == 1

        capture.seal()
        assert capture.read() == payload
        assert capture.stored_size == 0
        assert all(not os.path.exists(path) for path in paths)
    finally:
        capture.close()
        for path in paths:
            if path is not None and os.path.exists(path):
                os.unlink(path)


def test_disk_backed_capture_cleans_failed_persistence_and_drains_source(
    monkeypatch, tmp_path
):
    capture = _DiskBackedCapture(max_memory_size=4)
    capture.append(b"abcd")
    incomplete_path = tmp_path / "incomplete.spool"

    class FailingPersistedFile:
        def __init__(self):
            self.name = str(incomplete_path)
            self._file = open(self.name, "w+b")

        def write(self, data):
            self._file.write(bytes(data[:1]))
            raise OSError("injected persistence failure")

        def close(self):
            self._file.close()

    monkeypatch.setattr(
        psync_module.tempfile,
        "NamedTemporaryFile",
        lambda **_kwargs: FailingPersistedFile(),
    )

    persistence_error = None
    try:
        capture.append(b"e")
    except OSError as exc:
        persistence_error = exc

    assert persistence_error is not None
    assert "persistence failure" in str(persistence_error)
    assert not incomplete_path.exists()

    capture.seal(persistence_error)
    assert capture.read() == b"abcd"
    try:
        capture.read()
    except OSError as exc:
        assert exc is persistence_error
    else:
        raise AssertionError("capture did not report its persistence failure")
    capture.close()


def test_disk_backed_capture_retries_transient_unlink_failure(monkeypatch):
    capture = _DiskBackedCapture(max_memory_size=32)
    payload = b"u" * 65
    capture.append(payload)
    paths = [segment.path for segment in capture._segments if segment.path]
    assert len(paths) == 2

    original_unlink = psync_module.os.unlink
    failed_once = False

    def flaky_unlink(path):
        nonlocal failed_once
        if not failed_once:
            failed_once = True
            raise PermissionError("injected sharing violation")
        return original_unlink(path)

    monkeypatch.setattr(psync_module.os, "unlink", flaky_unlink)
    try:
        capture.seal()
        assert capture.read(32) == payload[:32]
        assert os.path.exists(paths[0])
        assert paths[0] in capture._pending_unlinks

        assert capture.read(32) == payload[32:64]
        assert not os.path.exists(paths[0])
        assert paths[0] not in capture._pending_unlinks
        assert capture.read() == payload[64:]
        assert capture.read() == b""
        assert not capture._pending_unlinks
    finally:
        capture.close()
        for path in paths:
            if os.path.exists(path):
                original_unlink(path)


def test_psync_capture_preserves_prefetch_spool_live_order_and_retries_locally():
    rdb = b"REDIS0012snapshot"
    commands = [
        resp_command(b"SET", b"prefetched", b"a"),
        resp_command(b"SET", b"spooled-b", b"b" * 128),
        resp_command(b"SET", b"spooled-c", b"c" * 128),
        resp_command(b"SET", b"live-d", b"d"),
    ]
    handshake_rdb_and_prefetch = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+FULLRESYNC replid 80\r\n"
        + b"$"
        + str(len(rdb)).encode()
        + b"\r\n"
        + rdb
        + commands[0]
    )
    sock = FeedSocket([handshake_rdb_and_prefetch, commands[1], commands[2]])
    connection = FakeConnection(sock)
    pool = SequencePool([connection])
    handler = PSyncIncrementalHandler(
        fake_source_with_pool(pool),
        buffer_size=32,
        snapshot_callback=lambda _rdb: True,
        ack_interval=0.01,
    )
    handler.CAPTURE_MEMORY_SIZE = 64
    handler.running = True
    delivered = []
    first_attempt = True

    def callback(frame):
        nonlocal first_attempt
        if first_attempt:
            first_attempt = False
            return False
        delivered.append(frame)
        if frame[1] == b"spooled-c":
            sock.feed(commands[3])
        if frame[1] == b"live-d":
            handler.running = False
        return True

    handler._replication_loop(callback)

    assert [frame[1] for frame in delivered] == [
        b"prefetched",
        b"spooled-b",
        b"spooled-c",
        b"live-d",
    ]
    assert pool.requests == ["PSYNC"]
    assert handler.replication_offset == 80 + sum(map(len, commands))


def test_psync_capture_partial_frame_resumes_after_last_buffered_byte():
    rdb = b"REDIS0012snapshot"
    command = resp_command(b"SET", b"partial", b"value")
    cut = 11
    first_wire = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+FULLRESYNC replid 50\r\n"
        + b"$"
        + str(len(rdb)).encode()
        + b"\r\n"
        + rdb
    )
    second_wire = (
        b"+PONG\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+OK\r\n"
        b"+CONTINUE replid\r\n"
        + command[cut:]
    )
    first = FakeConnection(ResetWhenEmptySocket([first_wire, command[:cut]]))
    second = FakeConnection(ChunkSocket([second_wire], timeout_when_empty=True))
    pool = SequencePool([first, second])
    handler = PSyncIncrementalHandler(
        fake_source_with_pool(pool),
        snapshot_callback=lambda _rdb: True,
        ack_interval=0.01,
    )
    handler.RETRY_DELAY = 0
    handler.running = True
    delivered = []

    def callback(frame):
        delivered.append(frame)
        handler.running = False
        return True

    handler._replication_loop(callback)

    assert delivered == [[b"SET", b"partial", b"value"]]
    assert ("PSYNC", "replid", 50 + cut + 1) in second.sent
    assert handler.replication_offset == 50 + len(command)
    assert handler.last_error is None


def test_capture_write_failure_disconnects_and_records_error(monkeypatch):
    class FailingSpool:
        _rolled = False

        def seek(self, _position):
            return 0

        def write(self, _data):
            raise OSError("injected disk failure")

        def close(self):
            pass

    monkeypatch.setattr(
        psync_module.tempfile,
        "SpooledTemporaryFile",
        lambda **_kwargs: FailingSpool(),
    )
    connection = FakeConnection(ChunkSocket([b"backlog"], timeout_when_empty=True))
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)

    capture_state = handler._start_snapshot_stream_capture(connection)
    capture_state[3].join(timeout=1)

    assert not capture_state[3].is_alive()
    assert connection.disconnected
    assert isinstance(capture_state[2][0], RuntimeError)
    assert "disk failure" in str(capture_state[2][0])
    handler._stop_snapshot_stream_capture(capture_state)
    capture_state[0].close()


def test_capture_is_closed_after_stubborn_worker_eventually_exits(
    monkeypatch, tmp_path
):
    monkeypatch.setattr(psync_module, "_WORKER_JOIN_TIMEOUT", 0.01)
    original_named_temporary_file = psync_module.tempfile.NamedTemporaryFile

    def temporary_file_in_test_directory(**kwargs):
        return original_named_temporary_file(dir=str(tmp_path), **kwargs)

    monkeypatch.setattr(
        psync_module.tempfile,
        "NamedTemporaryFile",
        temporary_file_in_test_directory,
    )

    class StubbornSocket:
        def __init__(self):
            self.blocked = threading.Event()
            self.allow_exit = threading.Event()
            self.sent_data = False

        def settimeout(self, _timeout):
            pass

        def recv(self, _size):
            if not self.sent_data:
                self.sent_data = True
                return b"abcdefgh"
            self.blocked.set()
            self.allow_exit.wait(1)
            return b""

    connection = FakeConnection(StubbornSocket())
    handler = PSyncIncrementalHandler(object(), buffer_size=8)
    handler.CAPTURE_MEMORY_SIZE = 4
    capture_state = None

    with pytest.raises(RuntimeError, match="did not stop promptly"):
        with handler._captured_replication_stream(connection) as state:
            capture_state = state
            assert connection._sock.blocked.wait(1)
            assert list(tmp_path.iterdir())

    assert capture_state is not None
    assert capture_state[3].is_alive()
    connection._sock.allow_exit.set()

    deadline = time.monotonic() + 1
    while list(tmp_path.iterdir()) and time.monotonic() < deadline:
        time.sleep(0.01)

    capture_state[3].join(timeout=1)
    assert not capture_state[3].is_alive()
    assert list(tmp_path.iterdir()) == []


def test_stop_during_pool_acquire_never_starts_handshake_or_snapshot():
    class BlockingPool:
        def __init__(self, connection):
            self.connection = connection
            self.started = threading.Event()
            self.allow = threading.Event()
            self.released = []

        def get_connection(self, _name):
            self.started.set()
            self.allow.wait(1)
            return self.connection

        def release(self, connection):
            self.released.append(connection)

    connection = FakeConnection(ChunkSocket([]))
    pool = BlockingPool(connection)
    handler = PSyncIncrementalHandler(fake_source_with_pool(pool))
    snapshots = []

    assert handler.start_replication(
        lambda _frame: True,
        snapshot_callback=lambda data: snapshots.append(data),
    )
    assert pool.started.wait(1)
    stop_thread = threading.Thread(target=handler.stop_replication)
    stop_thread.start()
    time.sleep(0.02)
    assert handler.start_replication(lambda _frame: True) is False
    pool.allow.set()
    stop_thread.join(timeout=2)

    assert not stop_thread.is_alive()
    assert connection.sent == []
    assert snapshots == []
    assert pool.released == [connection]


def test_blocked_ack_worker_defers_pool_release_until_worker_exits(monkeypatch):
    monkeypatch.setattr(psync_module, "_WORKER_JOIN_TIMEOUT", 0.01)

    class BlockingAckConnection:
        def __init__(self):
            self.started = threading.Event()
            self.allow = threading.Event()
            self.disconnected = False

        def send_command(self, *parts, **kwargs):
            assert kwargs.get("check_health") is False
            if parts[:2] == ("REPLCONF", "ACK"):
                self.started.set()
                self.allow.wait(1)

        def disconnect(self):
            self.disconnected = True

    connection = BlockingAckConnection()
    pool = FakePool(connection)
    handler = PSyncIncrementalHandler(object(), ack_interval=0.01)
    handler.running = True

    with handler._ack_heartbeat(connection) as errors:
        assert connection.started.wait(1)

    handler.running = False
    handler._active_connection = connection
    handler._disconnect_and_release(pool, connection)

    assert errors
    assert connection.disconnected
    assert handler._active_connection is None
    assert pool.released == []

    connection.allow.set()
    deadline = time.monotonic() + 1
    while not pool.released and time.monotonic() < deadline:
        time.sleep(0.01)
    assert pool.released == [connection]


def test_sync_handler_reads_eof_snapshot_and_prefetched_command_without_parser_mix():
    marker = b"a" * 40
    rdb = b"REDIS0012snapshot"
    command = resp_command(b"SET", b"k", b"v")
    wire = (
        b"+FULLRESYNC replid 5\r\n"
        + b"$EOF:"
        + marker
        + b"\r\n"
        + rdb
        + marker
        + command
    )
    connection = FakeConnection(ChunkSocket([wire], timeout_when_empty=True))
    handler = SyncHandler(fake_source(connection), object())
    snapshots = []

    assert handler.perform_psync(
        hold_connection_for_stream=True,
        snapshot_callback=lambda data: snapshots.append(data) or True,
    )
    assert snapshots == [rdb]

    delivered = []
    stop = type("Stop", (), {"is_set": lambda self: bool(delivered)})()
    assert handler.start_replication_stream(
        lambda command_name, args: delivered.append([command_name, *args]) or True,
        stop_event=stop,
        ack_interval=0.01,
    )
    assert delivered == [[b"SET", b"k", b"v"]]


def test_sync_handler_returns_false_when_stream_disconnects():
    connection = FakeConnection(ChunkSocket([]))
    handler = SyncHandler(fake_source(connection), object())
    assert handler.start_replication_stream(
        lambda command, args: True,
        connection=connection,
        reader=ReplicationStreamReader(connection._sock),
        ack_interval=0.01,
    ) is False


def test_sync_handler_getack_replies_after_committing_request_frame():
    command = resp_command(b"REPLCONF", b"GETACK", b"*")
    connection = FakeConnection(ChunkSocket([command], timeout_when_empty=True))
    handler = SyncHandler(fake_source(connection), object())
    handler.replication_offset = 9
    delivered = []
    stop = type("Stop", (), {"is_set": lambda self: bool(delivered)})()

    assert handler.start_replication_stream(
        lambda command_name, args: delivered.append([command_name, *args]) or True,
        stop_event=stop,
        connection=connection,
        reader=ReplicationStreamReader(connection._sock),
        ack_interval=10,
    )

    committed = 9 + len(command)
    assert any(
        parts == ("REPLCONF", "ACK", committed)
        for parts in connection.sent
    )
