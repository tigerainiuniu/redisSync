"""PSYNC-based real-time replication."""

from __future__ import annotations

import logging
import os
import socket
import stat
import tempfile
import threading
import time
from collections import deque
from contextlib import contextmanager
from typing import Callable, List, Optional, Tuple

import redis

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

from .redis_protocol import (
    MAX_REPLICATION_BUFFER_SIZE,
    MAX_REPLICATION_PENDING_SIZE,
    ReplicationConnectionClosed,
    ReplicationProtocolError,
    ReplicationStreamReader,
    SnapshotRequiredError,
    is_replconf_getack,
)


logger = logging.getLogger(__name__)
_WORKER_JOIN_TIMEOUT = 0.5
_SPOOL_PREFIX = "redis-sync-repl-"
_SPOOL_SUFFIX = ".spool"
_SPOOL_CREATION_GRACE_SECONDS = 60
_LEGACY_SPOOL_STALE_AGE_SECONDS = 24 * 60 * 60
_ACTIVE_SPOOL_PATHS = set()
_ACTIVE_SPOOL_PATHS_LOCK = threading.Lock()


def _spool_owner_pid(name: str) -> Optional[int]:
    if not name.startswith(_SPOOL_PREFIX) or not name.endswith(_SPOOL_SUFFIX):
        return None
    owner = name[len(_SPOOL_PREFIX):].split("-", 1)[0]
    return int(owner) if owner.isdigit() else None


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _register_active_spool(path: str) -> None:
    with _ACTIVE_SPOOL_PATHS_LOCK:
        _ACTIVE_SPOOL_PATHS.add(os.path.abspath(path))


def _unregister_active_spool(path: str) -> None:
    with _ACTIVE_SPOOL_PATHS_LOCK:
        _ACTIVE_SPOOL_PATHS.discard(os.path.abspath(path))


def _cleanup_stale_spool_files(
    directory: Optional[str] = None,
    *,
    now: Optional[float] = None,
    legacy_stale_age: float = _LEGACY_SPOOL_STALE_AGE_SECONDS,
) -> List[str]:
    """Remove crash leftovers while preserving every live process's spool."""
    directory = tempfile.gettempdir() if directory is None else directory
    current_time = time.time() if now is None else float(now)
    removed = []
    try:
        entries = list(os.scandir(directory))
    except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
        return removed

    for entry in entries:
        name = entry.name
        if not name.startswith(_SPOOL_PREFIX) or not name.endswith(_SPOOL_SUFFIX):
            continue
        path = os.path.abspath(entry.path)
        with _ACTIVE_SPOOL_PATHS_LOCK:
            if path in _ACTIVE_SPOOL_PATHS:
                continue
        try:
            entry_stat = entry.stat(follow_symlinks=False)
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if not stat.S_ISREG(entry_stat.st_mode):
            continue

        age = max(0.0, current_time - entry_stat.st_mtime)
        owner_pid = _spool_owner_pid(name)
        if owner_pid is not None:
            owner_alive = _pid_is_alive(owner_pid)
            if fcntl is None and owner_alive:
                continue
            # Protect the cross-process window between file creation and flock.
            # Older files must still prove liveness through the lock because a
            # PID may have been reused after a crash.
            if owner_alive and age < _SPOOL_CREATION_GRACE_SECONDS:
                continue
        elif age < float(legacy_stale_age):
            continue

        flags = os.O_RDONLY
        flags |= getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = None
        try:
            descriptor = os.open(path, flags)
            opened_stat = os.fstat(descriptor)
            if not stat.S_ISREG(opened_stat.st_mode):
                continue
            if fcntl is not None:
                try:
                    fcntl.flock(
                        descriptor,
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                except (BlockingIOError, PermissionError, OSError):
                    continue
            os.unlink(path)
            removed.append(path)
        except (FileNotFoundError, PermissionError, OSError):
            continue
        finally:
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
    return removed


class _ReplicationStopped(Exception):
    pass


class _CaptureSegment:
    def __init__(self, size_limit: int):
        self.file = tempfile.SpooledTemporaryFile(
            max_size=size_limit,
            mode="w+b",
        )
        self.read_position = 0
        self.write_position = 0
        self.sealed = False
        self.path: Optional[str] = None
        self.lease_file = None


class _DiskBackedCapture:
    """A blocking stream whose consumed disk segments are deleted promptly."""

    def __init__(
        self,
        max_memory_size: int = 8 * 1024 * 1024,
        max_total_size: int = 1024 * 1024 * 1024,
    ):
        self._segment_size = max(1, int(max_memory_size))
        self._max_total_size = int(max_total_size)
        if self._max_total_size < 1:
            raise ValueError("replication capture max_total_size must be positive")
        self._segments = deque()
        self._condition = threading.Condition()
        self._buffered_size = 0
        self._ever_rolled = False
        self._sealed = False
        self._closed = False
        self._error: Optional[BaseException] = None
        self._pending_unlinks = set()

    @property
    def error(self) -> Optional[BaseException]:
        with self._condition:
            return self._error

    @property
    def rolled_to_disk(self) -> bool:
        with self._condition:
            return self._ever_rolled or any(
                segment.path is not None
                or bool(getattr(segment.file, "_rolled", False))
                for segment in self._segments
            )

    @property
    def stored_size(self) -> int:
        with self._condition:
            return sum(segment.write_position for segment in self._segments)

    def _new_segment(self) -> _CaptureSegment:
        segment = _CaptureSegment(self._segment_size)
        self._segments.append(segment)
        return segment

    def _seal_full_segment(self, segment: _CaptureSegment) -> None:
        source = segment.file
        if source is None:
            raise OSError("replication spool segment has no writable file")

        persisted = None
        path = None
        registered = False
        lease_locked = False
        try:
            persisted = tempfile.NamedTemporaryFile(
                prefix=f"{_SPOOL_PREFIX}{os.getpid()}-",
                suffix=_SPOOL_SUFFIX,
                mode="w+b",
                delete=False,
            )
            path = persisted.name
            _register_active_spool(path)
            registered = True
            if fcntl is not None and callable(getattr(persisted, "fileno", None)):
                fcntl.flock(persisted.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                lease_locked = True
            source.seek(0)
            remaining = segment.write_position
            while remaining:
                amount = min(1024 * 1024, remaining)
                chunk = source.read(amount)
                if len(chunk) != amount:
                    raise OSError(
                        "short replication spool persistence read: "
                        f"{len(chunk)}/{amount} bytes"
                    )
                written = persisted.write(chunk)
                if written != len(chunk):
                    raise OSError(
                        "short replication spool persistence write: "
                        f"{written}/{len(chunk)} bytes"
                    )
                remaining -= len(chunk)
            persisted.flush()
        except Exception:
            if persisted is not None:
                try:
                    persisted.close()
                except Exception:
                    pass
            if registered and path is not None:
                _unregister_active_spool(path)
            if path is not None:
                self._unlink_or_defer(path, incomplete=True)
            raise

        segment.file = None
        segment.path = path
        if lease_locked:
            # Keep the file descriptor and flock alive until the segment is
            # consumed. Readers use a separate descriptor in ``segment.file``.
            segment.lease_file = persisted
            persisted = None
        else:
            persisted.close()
            persisted = None
        segment.sealed = True
        self._ever_rolled = True
        try:
            source.close()
        except Exception:
            logger.warning("failed to close in-memory replication spool", exc_info=True)

    @staticmethod
    def _open_segment_for_read(segment: _CaptureSegment):
        if segment.file is None:
            if segment.path is None:
                raise OSError("replication spool segment has no readable file")
            segment.file = open(segment.path, "rb")
        return segment.file

    def _unlink_or_defer(self, path: str, incomplete: bool = False) -> None:
        try:
            os.unlink(path)
        except FileNotFoundError:
            self._pending_unlinks.discard(path)
        except Exception:
            self._pending_unlinks.add(path)
            logger.warning(
                "failed to remove %sreplication spool %s; cleanup will retry",
                "incomplete " if incomplete else "",
                path,
                exc_info=True,
            )
        else:
            self._pending_unlinks.discard(path)

    def _retry_pending_unlinks(self, final: bool = False) -> None:
        for path in tuple(self._pending_unlinks):
            try:
                os.unlink(path)
            except FileNotFoundError:
                self._pending_unlinks.discard(path)
            except Exception:
                continue
            else:
                self._pending_unlinks.discard(path)
        if final and self._pending_unlinks:
            logger.warning(
                "replication spool files remain after close: %s",
                ", ".join(sorted(self._pending_unlinks)),
            )

    def _dispose_segment(self, segment: _CaptureSegment) -> None:
        file_obj = segment.file
        segment.file = None
        if file_obj is not None:
            try:
                file_obj.close()
            except Exception:
                logger.warning("failed to close replication spool segment", exc_info=True)

        path = segment.path
        segment.path = None
        if path is not None:
            self._unlink_or_defer(path)
            _unregister_active_spool(path)
        lease_file = segment.lease_file
        segment.lease_file = None
        if lease_file is not None:
            try:
                lease_file.close()
            except Exception:
                logger.warning("failed to close replication spool lease", exc_info=True)

    def append(self, data: bytes) -> None:
        if not data:
            return
        with self._condition:
            if self._closed or self._sealed:
                raise RuntimeError("replication capture is already sealed")
            if self._buffered_size + len(data) > self._max_total_size:
                error = ReplicationProtocolError(
                    "replication capture pending data exceeds configured limit: "
                    f"{self._buffered_size + len(data)} > "
                    f"{self._max_total_size} bytes"
                )
                self._error = error
                self._closed = True
                self._sealed = True
                while self._segments:
                    self._dispose_segment(self._segments.popleft())
                self._buffered_size = 0
                self._retry_pending_unlinks(final=True)
                self._condition.notify_all()
                raise error
            remaining = memoryview(data)
            while remaining:
                segment = (
                    self._segments[-1]
                    if self._segments and not self._segments[-1].sealed
                    else self._new_segment()
                )
                capacity = self._segment_size - segment.write_position
                if capacity == 0:
                    self._seal_full_segment(segment)
                    continue
                amount = min(capacity, len(remaining))
                segment.file.seek(segment.write_position)
                written = segment.file.write(remaining[:amount])
                if written != amount:
                    raise OSError(
                        f"short replication spool write: {written}/{amount} bytes"
                    )
                segment.write_position += written
                self._buffered_size += written
                remaining = remaining[written:]
                if remaining and segment.write_position == self._segment_size:
                    self._seal_full_segment(segment)
            self._condition.notify_all()

    def seal(self, error: Optional[BaseException] = None) -> None:
        with self._condition:
            if error is not None and self._error is None:
                self._error = error
            self._sealed = True
            self._condition.notify_all()

    def read(self, size: int = -1) -> bytes:
        with self._condition:
            self._retry_pending_unlinks()
            while (
                self._buffered_size == 0
                and not self._sealed
                and not self._closed
            ):
                self._condition.wait()

            if self._closed:
                if self._error is not None:
                    raise self._error
                raise ValueError("read from closed replication capture")

            if self._buffered_size > 0:
                requested = (
                    self._buffered_size
                    if size is None or size < 0
                    else min(size, self._buffered_size)
                )
                chunks = []
                remaining = requested
                while remaining and self._segments:
                    segment = self._segments[0]
                    available = segment.write_position - segment.read_position
                    amount = min(remaining, available)
                    segment_file = self._open_segment_for_read(segment)
                    segment_file.seek(segment.read_position)
                    chunk = segment_file.read(amount)
                    if len(chunk) != amount:
                        raise OSError(
                            f"short replication spool read: {len(chunk)}/{amount} bytes"
                        )
                    chunks.append(chunk)
                    segment.read_position += len(chunk)
                    self._buffered_size -= len(chunk)
                    remaining -= len(chunk)

                    if segment.read_position == segment.write_position:
                        if (
                            segment.sealed
                            or len(self._segments) > 1
                            or self._sealed
                        ):
                            self._segments.popleft()
                            self._dispose_segment(segment)
                        else:
                            segment.file.seek(0)
                            segment.file.truncate(0)
                            segment.read_position = 0
                            segment.write_position = 0
                return b"".join(chunks)

            if self._error is not None:
                raise self._error
            return b""

    def close(self) -> None:
        with self._condition:
            if self._closed:
                self._retry_pending_unlinks(final=True)
                return
            self._closed = True
            self._sealed = True
            self._condition.notify_all()
            while self._segments:
                self._dispose_segment(self._segments.popleft())
            self._buffered_size = 0
            self._retry_pending_unlinks(final=True)


def _resp_ok(response) -> bool:
    return response == b"OK" or response == "OK"


def _parse_psync_header(response) -> Tuple[str, Optional[str], Optional[int]]:
    if isinstance(response, bytes):
        text = response.decode("latin-1")
    elif isinstance(response, str):
        text = response
    else:
        raise ReplicationProtocolError(f"invalid PSYNC response {response!r}")

    parts = text.strip().lstrip("+").split()
    if len(parts) >= 3 and parts[0] == "FULLRESYNC":
        try:
            return "FULLRESYNC", parts[1], int(parts[2])
        except ValueError as exc:
            raise ReplicationProtocolError("invalid FULLRESYNC offset") from exc
    if parts and parts[0] == "CONTINUE":
        return "CONTINUE", parts[1] if len(parts) > 1 else None, None
    raise ReplicationProtocolError(f"unexpected PSYNC response {text!r}")


class PSyncIncrementalHandler:
    """Consume a Redis replication stream with committed-offset ACKs."""

    CAPTURE_MEMORY_SIZE = 8 * 1024 * 1024
    CAPTURE_MAX_SIZE = 1024 * 1024 * 1024
    MAX_RETRIES = 999
    RETRY_DELAY = 5.0

    def __init__(
        self,
        source_client: redis.Redis,
        buffer_size: int = 8192,
        snapshot_callback: Optional[Callable[[bytes], object]] = None,
        listening_port: int = 6380,
        ack_interval: float = 1.0,
        materialize_snapshot: bool = True,
        capture_max_size: Optional[int] = None,
    ):
        removed_spools = _cleanup_stale_spool_files()
        if removed_spools:
            logger.info(
                "removed %s stale replication spool file(s)",
                len(removed_spools),
            )
        self.source_client = source_client
        self.buffer_size = int(buffer_size)
        if not 1 <= self.buffer_size <= MAX_REPLICATION_BUFFER_SIZE:
            raise ValueError(
                "buffer_size must be between 1 and "
                f"{MAX_REPLICATION_BUFFER_SIZE} bytes"
            )
        self.snapshot_callback = snapshot_callback
        self.listening_port = listening_port
        self.ack_interval = max(0.01, float(ack_interval))
        self.materialize_snapshot = bool(materialize_snapshot)
        self.capture_max_size = int(
            self.CAPTURE_MAX_SIZE
            if capture_max_size is None
            else capture_max_size
        )
        if self.capture_max_size < 1:
            raise ValueError("capture_max_size must be positive")

        self.running = False
        self.replication_thread: Optional[threading.Thread] = None
        self.replication_id: Optional[str] = None
        # Public offset is the last byte whose complete command was delivered.
        self.replication_offset: int = -1
        # Received bytes may be ahead of the committed offset.  Their exact
        # contents stay in _pending_buffer across a reconnect.
        self._received_offset: int = -1
        self._pending_buffer = bytearray()
        self.last_error: Optional[BaseException] = None

        self._stop_event = threading.Event()
        self._active_connection = None
        self._connection_lock = threading.Lock()
        self._ack_send_lock = threading.Lock()
        self._connection_workers = {}
        self._deferred_releases = set()

    @property
    def received_offset(self) -> int:
        return self._received_offset

    @property
    def pending_buffer(self) -> bytes:
        return bytes(self._pending_buffer)

    def _take_pending_buffer(self) -> bytearray:
        pending = self._pending_buffer
        self._pending_buffer = bytearray()
        if isinstance(pending, bytearray):
            return pending
        return bytearray(pending)

    def _assemble_pending_buffer(self, prefetched: bytearray) -> bytearray:
        pending_size = len(self._pending_buffer)
        if pending_size + len(prefetched) > MAX_REPLICATION_PENDING_SIZE:
            raise ReplicationProtocolError(
                "replication pending buffer exceeds replication limit"
            )
        pending = self._take_pending_buffer()
        if not pending:
            return prefetched
        try:
            pending.extend(prefetched)
            prefetched.clear()
        except Exception:
            del pending[pending_size:]
            self._pending_buffer = pending
            raise
        return pending

    @staticmethod
    def _take_reader_buffer(reader) -> bytearray:
        take_ownership = getattr(reader, 'take_buffer_ownership', None)
        if callable(take_ownership):
            return take_ownership()
        return bytearray(reader.pending_bytes)

    def start_replication(
        self,
        command_callback: Callable[[List[bytes]], object],
        snapshot_callback: Optional[Callable[[bytes], object]] = None,
    ):
        """Start background replication.

        A FULLRESYNC must be applied by ``snapshot_callback``.  Returning
        ``False`` from either callback leaves the corresponding data
        uncommitted.  Legacy callbacks that return ``None`` count as success.
        """

        if self.running or (
            self.replication_thread is not None
            and self.replication_thread.is_alive()
        ):
            logger.warning("PSYNC replication is already running")
            return False
        if snapshot_callback is not None:
            self.snapshot_callback = snapshot_callback

        self.running = True
        self.last_error = None
        self._stop_event.clear()
        self.replication_thread = threading.Thread(
            target=self._replication_loop,
            args=(command_callback,),
            name="psync-replication",
            daemon=True,
        )
        self.replication_thread.start()
        return True

    def stop_replication(self, timeout: float = 10.0) -> bool:
        """Stop replication and report whether its worker actually exited."""

        self.running = False
        self._stop_event.set()
        with self._connection_lock:
            connection = self._active_connection
        if connection is not None:
            try:
                connection.disconnect()
            except Exception:
                pass
        return self.wait_stopped(timeout)

    def wait_stopped(self, timeout: float = 10.0) -> bool:
        """Wait for the replication worker without claiming a timed-out stop."""
        thread = self.replication_thread
        if thread is None:
            return True
        if thread is threading.current_thread():
            return False
        thread.join(timeout=max(0.0, float(timeout)))
        stopped = not thread.is_alive()
        if not stopped:
            logger.warning(
                "PSYNC replication thread remains alive after %.1fs stop timeout",
                float(timeout),
            )
        return stopped

    def _set_active_connection(self, connection) -> bool:
        with self._connection_lock:
            if not self.running or self._stop_event.is_set():
                return False
            self._active_connection = connection
            return True

    def _register_connection_worker(self, connection, thread) -> None:
        with self._connection_lock:
            self._connection_workers.setdefault(id(connection), set()).add(thread)

    def _unregister_connection_worker(self, connection, thread) -> None:
        with self._connection_lock:
            workers = self._connection_workers.get(id(connection))
            if workers is None:
                return
            workers.discard(thread)
            if not workers:
                self._connection_workers.pop(id(connection), None)

    def _release_connection_after_workers(self, pool, connection, workers) -> None:
        connection_id = id(connection)

        def release_when_idle() -> None:
            try:
                for worker in workers:
                    worker.join()
            finally:
                with self._connection_lock:
                    self._connection_workers.pop(connection_id, None)
                    self._deferred_releases.discard(connection_id)
            try:
                pool.release(connection)
            except Exception:
                pass

        threading.Thread(
            target=release_when_idle,
            name="psync-deferred-connection-release",
            daemon=True,
        ).start()

    def _disconnect_and_release(self, pool, connection) -> None:
        if connection is None:
            return
        connection_id = id(connection)
        with self._connection_lock:
            if self._active_connection is connection:
                self._active_connection = None
            workers = [
                thread
                for thread in self._connection_workers.get(connection_id, set())
                if thread.is_alive()
            ]
            already_deferred = connection_id in self._deferred_releases
            if workers and not already_deferred:
                self._deferred_releases.add(connection_id)
        try:
            connection.disconnect()
        except Exception:
            pass
        if already_deferred:
            return
        if workers:
            self._release_connection_after_workers(pool, connection, workers)
            return
        try:
            pool.release(connection)
        except Exception:
            pass

    def _read_ok(self, connection, reader, *command) -> None:
        connection.send_command(*command, check_health=False)
        response = reader.read_response()
        if not _resp_ok(response):
            raise ReplicationProtocolError(
                f"{' '.join(map(str, command))} returned {response!r}"
            )

    def _perform_handshake(self, connection, reader) -> None:
        connection.send_command("PING", check_health=False)
        pong = reader.read_response()
        if pong not in (b"PONG", "PONG", True):
            raise ReplicationProtocolError(f"PING returned {pong!r}")
        self._read_ok(
            connection,
            reader,
            "REPLCONF",
            "listening-port",
            self.listening_port,
        )
        self._read_ok(connection, reader, "REPLCONF", "capa", "eof")
        self._read_ok(connection, reader, "REPLCONF", "capa", "psync2")

    def _apply_snapshot(self, rdb_data: bytes) -> None:
        callback = self.snapshot_callback
        if callback is None:
            raise SnapshotRequiredError(
                "FULLRESYNC requires a snapshot_callback; the RDB was not discarded"
            )
        result = callback(rdb_data)
        if result is False:
            raise RuntimeError("snapshot callback reported failure")

    def _apply_snapshot_with_retry(self, rdb_data: bytes) -> None:
        while self.running and not self._stop_event.is_set():
            try:
                self._apply_snapshot(rdb_data)
                return
            except SnapshotRequiredError:
                raise
            except Exception as exc:
                self.last_error = exc
                logger.error("snapshot callback failed: %s", exc)
                if self._stop_event.wait(
                    max(0.05, min(self.ack_interval, 1.0))
                ):
                    raise _ReplicationStopped
        raise _ReplicationStopped

    def _record_received(self, size: int) -> None:
        if self._received_offset < 0:
            raise ReplicationProtocolError("received stream bytes before base offset")
        self._received_offset += size

    def _start_snapshot_stream_capture(self, connection):
        """Keep draining the socket while callbacks consume a disk-backed stream."""
        capture = _DiskBackedCapture(
            self.CAPTURE_MEMORY_SIZE,
            self.capture_max_size,
        )
        stop_event = threading.Event()
        errors = []

        def drain() -> None:
            capture_error = None
            try:
                try:
                    connection._sock.settimeout(min(self.ack_interval, 0.1))
                except Exception:
                    pass
                while not stop_event.is_set():
                    try:
                        data = connection._sock.recv(self.buffer_size)
                    except socket.timeout:
                        continue
                    except Exception as exc:
                        capture_error = ReplicationConnectionClosed(
                            f"replication socket read failed: {exc}"
                        )
                        errors.append(capture_error)
                        return
                    if not data:
                        capture_error = ReplicationConnectionClosed(
                            "replication connection closed during snapshot alignment"
                        )
                        errors.append(capture_error)
                        return
                    try:
                        capture.append(data)
                    except Exception as exc:
                        capture_error = RuntimeError(
                            f"failed to spool replication backlog: {exc}"
                        )
                        errors.append(capture_error)
                        try:
                            connection.disconnect()
                        except Exception:
                            pass
                        capture.seal(capture_error)
                        capture.close()
                        return
            finally:
                capture.seal(capture_error)
                self._unregister_connection_worker(
                    connection, threading.current_thread()
                )

        thread = threading.Thread(
            target=drain,
            name="psync-snapshot-stream-capture",
            daemon=True,
        )
        self._register_connection_worker(connection, thread)
        thread.start()
        return capture, stop_event, errors, thread, connection

    @staticmethod
    def _stop_snapshot_stream_capture(capture_state) -> None:
        _capture, stop_event, _errors, thread, connection = capture_state
        stop_event.set()
        thread.join(timeout=_WORKER_JOIN_TIMEOUT)
        forced_disconnect = thread.is_alive()
        if thread.is_alive():
            try:
                connection.disconnect()
            except Exception:
                pass
            thread.join(timeout=_WORKER_JOIN_TIMEOUT)
        if thread.is_alive():
            raise RuntimeError("snapshot stream capture did not stop promptly")
        if forced_disconnect:
            raise ReplicationConnectionClosed(
                "snapshot stream capture required disconnect to stop"
            )

    @staticmethod
    def _close_capture_after_worker(capture, thread) -> None:
        def close_when_idle() -> None:
            thread.join()
            capture.close()

        threading.Thread(
            target=close_when_idle,
            name="psync-deferred-capture-close",
            daemon=True,
        ).start()

    @contextmanager
    def _captured_replication_stream(self, connection):
        capture_state = self._start_snapshot_stream_capture(connection)
        capture = capture_state[0]
        try:
            yield capture_state
        finally:
            try:
                self._stop_snapshot_stream_capture(capture_state)
            finally:
                if capture_state[3].is_alive():
                    self._close_capture_after_worker(capture, capture_state[3])
                else:
                    capture.close()

    def _ensure_running(self) -> None:
        if not self.running or self._stop_event.is_set():
            raise _ReplicationStopped

    @contextmanager
    def _ack_heartbeat(self, connection):
        """Send committed-offset ACKs independently of reads and callbacks."""
        stop_event = threading.Event()
        errors = []

        def send_acks() -> None:
            try:
                while self.running and not stop_event.is_set():
                    try:
                        self._send_replconf_ack(connection)
                    except Exception as exc:
                        errors.append(exc)
                        self.last_error = exc
                        try:
                            connection.disconnect()
                        except Exception:
                            pass
                        return
                    if stop_event.wait(self.ack_interval):
                        return
            finally:
                self._unregister_connection_worker(
                    connection, threading.current_thread()
                )

        thread = threading.Thread(
            target=send_acks,
            name="psync-ack-heartbeat",
            daemon=True,
        )
        self._register_connection_worker(connection, thread)
        thread.start()
        try:
            yield errors
        finally:
            stop_event.set()
            thread.join(timeout=_WORKER_JOIN_TIMEOUT)
            forced_disconnect = thread.is_alive()
            if forced_disconnect:
                try:
                    connection.disconnect()
                except Exception:
                    pass
                thread.join(timeout=_WORKER_JOIN_TIMEOUT)
                errors.append(
                    ReplicationConnectionClosed(
                        "ACK heartbeat required disconnect to stop"
                    )
                )
            if thread.is_alive():
                errors.append(RuntimeError("ACK heartbeat did not stop promptly"))

    def _replication_loop(self, command_callback: Callable[[List[bytes]], object]):
        retry_count = 0
        max_retries = self.MAX_RETRIES
        retry_delay = self.RETRY_DELAY

        while self.running and retry_count < max_retries:
            if retry_count and self._stop_event.wait(retry_delay):
                break

            pool = self.source_client.connection_pool
            connection = None
            try:
                connection = pool.get_connection("PSYNC")
                if not self._set_active_connection(connection):
                    self._disconnect_and_release(pool, connection)
                    connection = None
                    break
                reader = ReplicationStreamReader(
                    connection._sock, buffer_size=self.buffer_size
                )
                self._ensure_running()
                self._perform_handshake(connection, reader)
                self._ensure_running()

                last_received_offset = self._received_offset
                if last_received_offset < 0:
                    last_received_offset = self.replication_offset
                if self.replication_id and last_received_offset >= 0:
                    connection.send_command(
                        "PSYNC", self.replication_id, last_received_offset + 1,
                        check_health=False,
                    )
                else:
                    connection.send_command("PSYNC", "?", -1, check_health=False)

                kind, replid, initial_offset = _parse_psync_header(
                    reader.read_response()
                )
                self._ensure_running()
                if kind == "FULLRESYNC":
                    if replid is None or initial_offset is None:
                        raise ReplicationProtocolError(
                            "FULLRESYNC omitted replication metadata"
                        )
                    # The old history can no longer be resumed. Until the new
                    # snapshot commits, heartbeat with offset zero only.
                    self.replication_id = None
                    self.replication_offset = -1
                    self._received_offset = -1
                    self._pending_buffer = bytearray()

                with self._ack_heartbeat(connection) as ack_errors:
                    if kind == "FULLRESYNC":
                        rdb_data = reader.read_rdb(
                            collect=self.materialize_snapshot
                        )
                    else:
                        if replid is not None:
                            self.replication_id = replid
                        if self._received_offset < 0:
                            self._received_offset = max(
                                self.replication_offset, 0
                            )
                    prefetched = reader.take_buffer_ownership()
                    self._ensure_running()

                    with self._captured_replication_stream(
                        connection
                    ) as capture_state:
                        captured_stream, _, capture_errors, _, _ = capture_state
                        if kind == "FULLRESYNC":
                            self._ensure_running()
                            self._apply_snapshot_with_retry(rdb_data)
                            self._ensure_running()
                            self.replication_id = replid
                            self.replication_offset = initial_offset
                            self._received_offset = initial_offset

                        initial_buffer = self._assemble_pending_buffer(prefetched)
                        stream_reader = ReplicationStreamReader(
                            connection._sock,
                            buffer_size=self.buffer_size,
                            initial_buffer=initial_buffer,
                            initial_stream=captured_stream,
                            allow_leading_crlf=(kind == "FULLRESYNC"),
                            adopt_initial_buffer=True,
                        )
                        self._received_offset = (
                            self.replication_offset
                            + stream_reader.pending_size
                        )
                        retry_count = 0
                        # The PSYNC handshake (and FULLRESYNC callback, when
                        # present) has recovered the stream. Do not leave a
                        # previous connection failure visible as current state.
                        self.last_error = None
                        stream_ok = self._receive_command_stream(
                            connection,
                            command_callback,
                            reader=stream_reader,
                            _manage_ack=False,
                            _retry_callbacks=True,
                        )
                    if capture_errors:
                        self.last_error = capture_errors[0]
                        if self.running:
                            stream_ok = False
                    if ack_errors:
                        raise ack_errors[0]
                    if self.running and not stream_ok:
                        retry_count += 1

            except _ReplicationStopped:
                pass
            except SnapshotRequiredError as exc:
                self.last_error = exc
                logger.error("PSYNC stopped: %s", exc)
                self.running = False
            except Exception as exc:
                self.last_error = exc
                if self.running:
                    logger.error("PSYNC replication failed: %s", exc, exc_info=True)
                    retry_count += 1
            finally:
                self._disconnect_and_release(pool, connection)

        self.running = False

    def _skip_rdb_data(self, connection):
        """Legacy helper retained as an explicit snapshot operation."""

        reader = ReplicationStreamReader(
            connection._sock, buffer_size=self.buffer_size
        )
        rdb_data = reader.read_rdb(collect=self.materialize_snapshot)
        self._apply_snapshot(rdb_data)
        return reader.take_buffer()

    def _receive_command_stream(
        self,
        connection,
        command_callback: Callable[[List[bytes]], object],
        reader: Optional[ReplicationStreamReader] = None,
        _manage_ack: bool = True,
        _retry_callbacks: bool = False,
    ) -> bool:
        """Deliver complete frames and ACK only their committed offsets."""

        if reader is None:
            initial_buffer = self._take_pending_buffer()
            reader = ReplicationStreamReader(
                connection._sock,
                buffer_size=self.buffer_size,
                initial_buffer=initial_buffer,
                adopt_initial_buffer=True,
            )

        if _manage_ack:
            with self._ack_heartbeat(connection) as ack_errors:
                result = self._receive_command_stream(
                    connection,
                    command_callback,
                    reader=reader,
                    _manage_ack=False,
                    _retry_callbacks=_retry_callbacks,
                )
            if ack_errors:
                self.last_error = ack_errors[0]
                return False
            return result

        try:
            connection._sock.settimeout(self.ack_interval)
        except Exception:
            pass

        while self.running:
            try:
                command, consumed = reader.peek_command()
                self._received_offset = (
                    self.replication_offset + reader.pending_size
                )
                while True:
                    callback_error = None
                    try:
                        delivered = command_callback(command)
                    except Exception as exc:
                        delivered = False
                        callback_error = exc
                        logger.error("replication command callback failed: %s", exc)
                    if delivered is not False:
                        self.last_error = None
                        break

                    self.last_error = callback_error or RuntimeError(
                        "replication command callback reported failure"
                    )
                    if not _retry_callbacks:
                        self._pending_buffer = self._take_reader_buffer(reader)
                        return False
                    if not self.running or self._stop_event.wait(
                        max(0.05, min(self.ack_interval, 1.0))
                    ):
                        self._pending_buffer = self._take_reader_buffer(reader)
                        return True

                reader.commit(consumed)
                self.replication_offset += consumed
                self._received_offset = (
                    self.replication_offset + reader.pending_size
                )
                if is_replconf_getack(command):
                    self._send_replconf_ack(connection)

            except socket.timeout:
                self._received_offset = (
                    self.replication_offset + reader.pending_size
                )
            except ReplicationConnectionClosed as exc:
                self.last_error = exc
                reader.discard_partial_snapshot_terminator()
                self._received_offset = (
                    self.replication_offset + reader.pending_size
                )
                self._pending_buffer = self._take_reader_buffer(reader)
                return False
            except Exception as exc:
                self.last_error = exc
                self._received_offset = (
                    self.replication_offset + reader.pending_size
                )
                self._pending_buffer = self._take_reader_buffer(reader)
                logger.error("replication command stream failed: %s", exc)
                return False

        self._pending_buffer = self._take_reader_buffer(reader)
        return True

    def _send_replconf_ack(self, connection):
        committed_offset = max(self.replication_offset, 0)
        with self._ack_send_lock:
            connection.send_command(
                "REPLCONF", "ACK", committed_offset, check_health=False
            )
