"""
hold_connection_for_stream + start_replication_stream 集成测试（伪造连接，无需真实 Redis）。
防止重构后「宣称启用持续复制但实际未挂接连接」的回归。
"""

import inspect
import threading
import time
import socket

from redis_sync.sync_handler import SyncHandler


def test_sync_handler_snapshot_entrypoints_preserve_target_by_default():
    assert inspect.signature(SyncHandler.perform_full_sync).parameters[
        "clear_target"
    ].default is False
    assert inspect.signature(SyncHandler.perform_psync).parameters[
        "clear_target"
    ].default is False
    assert inspect.signature(SyncHandler._apply_rdb_data).parameters[
        "clear_target"
    ].default is False


class SyncHandlerNoMigrate(SyncHandler):
    """不触发 SCAN 全库，仅验证连接挂接与读流。"""

    def _apply_rdb_data(self, rdb_data, progress_callback=None, clear_target=True):
        return True


class FakePool:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self, name=None):
        return self._conn

    def release(self, conn):
        pass


def resp_command(*parts):
    value = f"*{len(parts)}\r\n".encode()
    for part in parts:
        value += f"${len(part)}\r\n".encode() + part + b"\r\n"
    return value


class FakeSocket:
    def __init__(self, wire):
        self.wire = bytearray(wire)

    def recv(self, size):
        if not self.wire:
            raise socket.timeout()
        data = bytes(self.wire[:size])
        del self.wire[:size]
        return data

    def settimeout(self, timeout):
        self.timeout = timeout


class FakeConnectionForSyncStream:
    """Serve a coalesced RDB and SET frame from one raw socket buffer."""

    def __init__(self, psync_header=False):
        rdb = b"REDIS0012fake_rdb_payload"
        header = b"+FULLRESYNC replid 41\r\n" if psync_header else b""
        self._sock = FakeSocket(
            header + b"$" + str(len(rdb)).encode() + b"\r\n" + rdb
            + resp_command(b"SET", b"stream_key", b"\xffbinary-value")
        )
        self.sent = []
        self.disconnected = False

    def send_command(self, *args, **kwargs):
        assert kwargs.get("check_health") is False
        self.sent.append(args)

    def disconnect(self):
        self.disconnected = True


def test_full_sync_hold_then_stream_delivers_command_bytes():
    fc = FakeConnectionForSyncStream(psync_header=True)
    source = type("SC", (), {})()
    source.connection_pool = FakePool(fc)
    source.info = lambda section=None: {"role": "master"}
    target = type("TC", (), {})()
    target.flushdb = lambda: None
    target.execute_command = lambda *a, **k: True

    handler = SyncHandlerNoMigrate(source, target)

    assert handler.perform_full_sync(
        hold_connection_for_stream=True,
        replication_handshake=False,
    )

    assert handler._replication_stream_connection is fc, "成功 SYNC 后应暂存连接供读流"
    assert ("PSYNC", "?", -1) in fc.sent
    assert not any(command[0] == "SYNC" for command in fc.sent)

    received = []

    def cb(cmd: bytes, args: list):
        received.append((cmd, args))

    stop = threading.Event()

    def run_stream():
        handler.start_replication_stream(cb, stop_event=stop)

    th = threading.Thread(target=run_stream, daemon=True)
    th.start()
    time.sleep(0.15)
    stop.set()
    th.join(timeout=2.0)
    assert not th.is_alive(), "复制流线程应随 stop_event 退出"

    assert handler._replication_stream_connection is None, "读流结束后应已消费并释放暂存连接"

    assert len(received) == 1
    cmd, args = received[0]
    assert cmd == b"SET"
    assert args == [b"stream_key", b"\xffbinary-value"]


class FakeConnectionForPsyncContinue:
    def __init__(self):
        self._sock = FakeSocket(
            b"+CONTINUE\r\n" + resp_command(b"PING")
        )
        self.sent = []
        self.disconnected = False

    def send_command(self, *args, **kwargs):
        assert kwargs.get("check_health") is False
        self.sent.append(args)

    def disconnect(self):
        self.disconnected = True


def test_psync_continue_hold_then_stream():
    fc = FakeConnectionForPsyncContinue()
    source = type("SC", (), {})()
    source.connection_pool = FakePool(fc)
    target = type("TC", (), {})()

    handler = SyncHandlerNoMigrate(source, target)

    assert handler.perform_psync(
        hold_connection_for_stream=True,
        replication_handshake=False,
    )

    received = []

    def cb(cmd: bytes, args: list):
        received.append((cmd, args))

    stop = threading.Event()

    th = threading.Thread(
        target=lambda: handler.start_replication_stream(cb, stop_event=stop),
        daemon=True,
    )
    th.start()
    time.sleep(0.15)
    stop.set()
    th.join(timeout=2.0)
    assert not th.is_alive()

    assert len(received) == 1
    assert received[0][0] == b"PING"


def test_full_sync_without_hold_releases_connection_no_pending_stream():
    fc = FakeConnectionForSyncStream()
    source = type("SC", (), {})()
    source.connection_pool = FakePool(fc)
    source.info = lambda section=None: {"role": "master"}
    target = type("TC", (), {})()
    target.flushdb = lambda: None

    handler = SyncHandlerNoMigrate(source, target)

    assert handler.perform_full_sync(hold_connection_for_stream=False)
    assert handler._replication_stream_connection is None

    def cb(cmd: bytes, args: list):
        pass

    assert handler.start_replication_stream(cb) is False
