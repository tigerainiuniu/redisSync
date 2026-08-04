"""Redis SYNC/PSYNC snapshot and replication stream handling."""

from __future__ import annotations

import logging
import socket
import threading
from contextlib import contextmanager
from typing import Any, Callable, Dict, List, Optional, Tuple

import redis

from .redis_protocol import (
    ReplicationConnectionClosed,
    ReplicationProtocolError,
    ReplicationStreamReader,
    is_replconf_getack,
)


logger = logging.getLogger(__name__)
_WORKER_JOIN_TIMEOUT = 0.5


def _repl_ok(response: Any) -> bool:
    return response in (b"OK", "OK", True)


def run_replication_handshake_on_connection(
    connection: Any,
    listening_port: int,
    reader: Optional[ReplicationStreamReader] = None,
) -> bool:
    """Send PING and REPLCONF on one connection using one read buffer."""

    reader = reader or ReplicationStreamReader(connection._sock)
    try:
        connection.send_command("PING", check_health=False)
        pong = reader.read_response()
        if pong not in (b"PONG", "PONG", True):
            logger.error("Handshake PING failed: %r", pong)
            return False

        connection.send_command(
            "REPLCONF", "listening-port", listening_port, check_health=False
        )
        if not _repl_ok(reader.read_response()):
            logger.error("REPLCONF listening-port failed")
            return False

        connection.send_command("REPLCONF", "capa", "eof", check_health=False)
        if not _repl_ok(reader.read_response()):
            logger.error("REPLCONF capa eof failed")
            return False

        connection.send_command("REPLCONF", "capa", "psync2", check_health=False)
        if not _repl_ok(reader.read_response()):
            logger.error("REPLCONF capa psync2 failed")
            return False
        return True
    except Exception as exc:
        logger.error("Replication handshake failed: %s", exc)
        return False


def parse_psync_response(
    response: Any,
) -> Optional[Tuple[str, Optional[str], Optional[int]]]:
    """Parse ``FULLRESYNC`` and PSYNC2 ``CONTINUE`` response variants."""

    try:
        if isinstance(response, (list, tuple)):
            if not response:
                return None
            first = response[0]
            kind = first.decode("latin-1") if isinstance(first, bytes) else str(first)
            if kind == "FULLRESYNC" and len(response) >= 3:
                rid = (
                    response[1].decode("latin-1")
                    if isinstance(response[1], bytes)
                    else str(response[1])
                )
                return "FULLRESYNC", rid, int(response[2])
            if kind == "CONTINUE":
                rid = None
                off = None
                if len(response) >= 2:
                    rid = (
                        response[1].decode("latin-1")
                        if isinstance(response[1], bytes)
                        else str(response[1])
                    )
                if len(response) >= 3:
                    off = int(response[2])
                return "CONTINUE", rid, off
            return None

        if isinstance(response, bytes):
            text = response.decode("latin-1")
        elif isinstance(response, str):
            text = response
        else:
            return None
        parts = text.strip().lstrip("+").split()
        if len(parts) >= 3 and parts[0] == "FULLRESYNC":
            return "FULLRESYNC", parts[1], int(parts[2])
        if parts and parts[0] == "CONTINUE":
            rid = parts[1] if len(parts) > 1 else None
            off = int(parts[2]) if len(parts) > 2 else None
            return "CONTINUE", rid, off
    except (ValueError, TypeError, UnicodeError):
        return None
    return None


class SyncHandler:
    """Perform a snapshot transfer and optionally retain its command stream."""

    def __init__(self, source_client: redis.Redis, target_client: redis.Redis):
        self.source_client = source_client
        self.target_client = target_client
        self.replication_id: Optional[str] = None
        self.replication_offset: int = 0
        self._replication_stream_connection: Any = None
        self._replication_stream_reader: Optional[ReplicationStreamReader] = None
        self._replication_pending_buffer = b""
        self._ack_send_lock = threading.Lock()
        self._connection_worker_lock = threading.Lock()
        self._connection_workers = {}
        self._deferred_releases = set()

    def _register_connection_worker(self, connection: Any, thread) -> None:
        with self._connection_worker_lock:
            self._connection_workers.setdefault(id(connection), set()).add(thread)

    def _unregister_connection_worker(self, connection: Any, thread) -> None:
        with self._connection_worker_lock:
            workers = self._connection_workers.get(id(connection))
            if workers is None:
                return
            workers.discard(thread)
            if not workers:
                self._connection_workers.pop(id(connection), None)

    @contextmanager
    def _ack_heartbeat(self, connection: Any, interval: float):
        stop_event = threading.Event()
        errors = []

        def send_acks() -> None:
            try:
                while not stop_event.is_set():
                    try:
                        self._send_ack(connection)
                    except Exception as exc:
                        errors.append(exc)
                        try:
                            connection.disconnect()
                        except Exception:
                            pass
                        return
                    if stop_event.wait(interval):
                        return
            finally:
                self._unregister_connection_worker(
                    connection, threading.current_thread()
                )

        thread = threading.Thread(
            target=send_acks,
            name="sync-ack-heartbeat",
            daemon=True,
        )
        self._register_connection_worker(connection, thread)
        thread.start()
        try:
            yield errors
        finally:
            stop_event.set()
            thread.join(timeout=_WORKER_JOIN_TIMEOUT)
            if thread.is_alive():
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

    def _disconnect_and_release(self, connection: Any) -> None:
        if connection is None:
            return
        connection_id = id(connection)
        with self._connection_worker_lock:
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
            pool = self.source_client.connection_pool

            def release_when_idle() -> None:
                try:
                    for worker in workers:
                        worker.join()
                finally:
                    with self._connection_worker_lock:
                        self._connection_workers.pop(connection_id, None)
                        self._deferred_releases.discard(connection_id)
                try:
                    pool.release(connection)
                except Exception:
                    pass

            threading.Thread(
                target=release_when_idle,
                name="sync-deferred-connection-release",
                daemon=True,
            ).start()
            return
        try:
            self.source_client.connection_pool.release(connection)
        except Exception:
            pass

    def _store_stream_connection(
        self, connection: Any, reader: ReplicationStreamReader
    ) -> None:
        if self._replication_stream_connection is not None:
            self._disconnect_and_release(self._replication_stream_connection)
        self._replication_stream_connection = connection
        self._replication_stream_reader = reader

    def _apply_received_snapshot(
        self,
        rdb_data: bytes,
        snapshot_callback: Optional[Callable[[bytes], object]],
        progress_callback: Optional[Callable[[int, int], None]],
        clear_target: bool,
    ) -> bool:
        if snapshot_callback is not None:
            try:
                return snapshot_callback(rdb_data) is not False
            except Exception as exc:
                logger.error("Snapshot callback failed: %s", exc)
                return False

        # Preserve subclasses that already provide a real snapshot applier.
        if type(self)._apply_rdb_data is not SyncHandler._apply_rdb_data:
            return self._apply_rdb_data(
                rdb_data, progress_callback, clear_target=clear_target
            )

        logger.error(
            "Received a replication snapshot but no snapshot_callback was provided"
        )
        return False

    def perform_full_sync(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        chunk_size: int = 8192,
        clear_target: bool = False,
        replication_handshake: bool = False,
        listening_port: int = 6380,
        hold_connection_for_stream: bool = False,
        snapshot_callback: Optional[Callable[[bytes], object]] = None,
        materialize_snapshot: bool = True,
    ) -> bool:
        if hold_connection_for_stream:
            # Legacy SYNC does not return the exact offset at which its RDB was
            # created. PSYNC's FULLRESYNC header does, so a retained stream must
            # use that handshake to avoid skipping writes around the snapshot.
            return self.perform_psync(
                progress_callback=progress_callback,
                clear_target=clear_target,
                replication_handshake=replication_handshake,
                listening_port=listening_port,
                hold_connection_for_stream=True,
                snapshot_callback=snapshot_callback,
                materialize_snapshot=materialize_snapshot,
            )
        del chunk_size
        connection = None
        try:
            source_info = self.source_client.info("replication")
            snapshot_offset = int(
                source_info.get("master_repl_offset", 0) or 0
            )
            self.replication_offset = 0
            connection = self.source_client.connection_pool.get_connection("SYNC")
            reader = ReplicationStreamReader(connection._sock)
            if replication_handshake and not run_replication_handshake_on_connection(
                connection, listening_port, reader=reader
            ):
                return False

            connection.send_command("SYNC", check_health=False)
            with self._ack_heartbeat(connection, 1.0) as ack_errors:
                rdb_data = reader.read_rdb(collect=materialize_snapshot)
                if not self._apply_received_snapshot(
                    rdb_data,
                    snapshot_callback,
                    progress_callback,
                    clear_target,
                ):
                    return False
                self.replication_offset = snapshot_offset
            if ack_errors:
                raise ack_errors[0]
            if hold_connection_for_stream:
                self._store_stream_connection(connection, reader)
                connection = None
            return True
        except Exception as exc:
            logger.error("Full synchronization failed: %s", exc)
            return False
        finally:
            self._disconnect_and_release(connection)

    def perform_psync(
        self,
        replication_id: Optional[str] = None,
        offset: int = -1,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        clear_target: bool = False,
        replication_handshake: bool = False,
        listening_port: int = 6380,
        hold_connection_for_stream: bool = False,
        snapshot_callback: Optional[Callable[[bytes], object]] = None,
        materialize_snapshot: bool = True,
    ) -> bool:
        connection = None
        try:
            connection = self.source_client.connection_pool.get_connection("PSYNC")
            reader = ReplicationStreamReader(connection._sock)
            if replication_handshake and not run_replication_handshake_on_connection(
                connection, listening_port, reader=reader
            ):
                return False

            if replication_id:
                connection.send_command(
                    "PSYNC", replication_id, offset, check_health=False
                )
            else:
                connection.send_command("PSYNC", "?", -1, check_health=False)

            parsed = parse_psync_response(reader.read_response())
            if parsed is None:
                logger.error("Invalid PSYNC response")
                return False
            kind, rid, server_offset = parsed

            if kind == "FULLRESYNC":
                if rid is None or server_offset is None:
                    return False
                self.replication_id = None
                self.replication_offset = 0
                with self._ack_heartbeat(connection, 1.0) as ack_errors:
                    rdb_data = reader.read_rdb(collect=materialize_snapshot)
                    if not self._apply_received_snapshot(
                        rdb_data,
                        snapshot_callback,
                        progress_callback,
                        clear_target,
                    ):
                        return False
                    self.replication_id = rid
                    self.replication_offset = int(server_offset)
                if ack_errors:
                    raise ack_errors[0]
            else:
                if rid is not None:
                    self.replication_id = rid
                if server_offset is not None:
                    self.replication_offset = int(server_offset)
                elif replication_id is not None and offset >= 0:
                    self.replication_offset = offset - 1

            if hold_connection_for_stream:
                self._store_stream_connection(connection, reader)
                connection = None
            return True
        except Exception as exc:
            logger.error("Partial synchronization failed: %s", exc)
            return False
        finally:
            self._disconnect_and_release(connection)

    def _apply_rdb_data(
        self,
        rdb_data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        clear_target: bool = False,
    ) -> bool:
        del rdb_data, progress_callback, clear_target
        logger.error("RDB application is not implemented; provide snapshot_callback")
        return False

    def _parse_and_restore_rdb(
        self,
        rdb_data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> bool:
        del rdb_data, progress_callback
        logger.error("RDB parsing is not implemented")
        return False

    def _send_ack(self, connection: Any) -> None:
        with self._ack_send_lock:
            connection.send_command(
                "REPLCONF",
                "ACK",
                max(self.replication_offset, 0),
                check_health=False,
            )

    def start_replication_stream(
        self,
        callback: Callable[[bytes, List[bytes]], object],
        stop_event: Optional[Any] = None,
        connection: Optional[Any] = None,
        reader: Optional[ReplicationStreamReader] = None,
        ack_interval: float = 1.0,
    ) -> bool:
        """Read, deliver, commit, and ACK commands on the retained connection."""

        if connection is None:
            connection = self._replication_stream_connection
            self._replication_stream_connection = None
            if reader is None:
                reader = self._replication_stream_reader
            self._replication_stream_reader = None
        if connection is None:
            logger.error("Replication stream has no retained SYNC/PSYNC connection")
            return False
        if reader is None:
            reader = ReplicationStreamReader(
                connection._sock, initial_buffer=self._replication_pending_buffer
            )

        interval = max(0.01, float(ack_interval))
        stream_ok = True
        try:
            try:
                connection._sock.settimeout(interval)
            except Exception:
                pass
            with self._ack_heartbeat(connection, interval) as ack_errors:
                while True:
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        command, consumed = reader.peek_command()
                        delivered = callback(command[0], command[1:])
                        if delivered is False:
                            stream_ok = False
                            break
                        reader.commit(consumed)
                        self.replication_offset += consumed
                        self._replication_pending_buffer = reader.pending_bytes
                        if is_replconf_getack(command):
                            self._send_ack(connection)
                    except (socket.timeout, redis.TimeoutError):
                        pass
                    except ReplicationConnectionClosed as exc:
                        reader.discard_partial_snapshot_terminator()
                        logger.error("Replication stream disconnected: %s", exc)
                        stream_ok = False
                        break
                    except Exception as exc:
                        logger.error("Error reading replication stream: %s", exc)
                        stream_ok = False
                        break
            if ack_errors:
                logger.error("Failed to ACK replication stream: %s", ack_errors[0])
                stream_ok = False
        finally:
            self._replication_pending_buffer = reader.pending_bytes
            self._disconnect_and_release(connection)
        return stream_ok

    def _handle_replication_command(
        self, response: Any, callback: Callable[[bytes, List[bytes]], object]
    ):
        if isinstance(response, list) and response:
            command = response[0]
            command_b = command if isinstance(command, bytes) else str(command).encode()
            args_b = [
                arg
                if isinstance(arg, bytes)
                else arg.tobytes()
                if isinstance(arg, memoryview)
                else str(arg).encode()
                for arg in response[1:]
            ]
            return callback(command_b, args_b)
        return None

    def get_replication_info(self) -> Dict[str, Any]:
        return {
            "replication_id": self.replication_id,
            "replication_offset": self.replication_offset,
            "source_info": self.source_client.info("replication"),
            "target_info": self.target_client.info("replication"),
        }
