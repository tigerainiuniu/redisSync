"""
hold_connection_for_stream + start_replication_stream 集成测试（伪造连接，无需真实 Redis）。
防止重构后「宣称启用持续复制但实际未挂接连接」的回归。
"""

import threading
import time

import redis

from redis_sync.sync_handler import SyncHandler


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


class FakeConnectionForSyncStream:
    """SYNC: 首包 RDB bytes；读流: 一条 SET，之后 Timeout 直到 stop_event。"""

    def __init__(self):
        self.n = 0

    def send_command(self, *args):
        pass

    def read_response(self):
        self.n += 1
        if self.n == 1:
            return b"REDIS0012fake_rdb_payload"
        if self.n == 2:
            return [b"SET", b"stream_key", b"\xffbinary-value"]
        raise redis.TimeoutError()


def test_full_sync_hold_then_stream_delivers_command_bytes():
    fc = FakeConnectionForSyncStream()
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
        self.n = 0

    def send_command(self, *args):
        pass

    def read_response(self):
        self.n += 1
        if self.n == 1:
            return b"+CONTINUE\r\n"
        if self.n == 2:
            return [b"PING"]
        raise redis.TimeoutError()


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
