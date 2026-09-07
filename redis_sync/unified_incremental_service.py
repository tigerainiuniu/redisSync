#!/usr/bin/env python3
"""Unified SCAN, SYNC, and PSYNC incremental service."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Set, Tuple

import redis

from .connection_manager import RedisConnectionManager
from .key_sync import (
    _is_absttl_compatibility_error,
    _is_restore_compatibility_error,
    _sync_key_fallback,
    cache_source_pexpiretime_support,
    capture_dump_with_preflight,
    is_pexpiretime_capability_error,
    restore_dump_with_deadline,
    source_supports_pexpiretime,
)
from .psync_incremental_handler import PSyncIncrementalHandler
from .sync_filters import build_atomic_filtered_delete_command, redis_glob_match
from .sync_handler import SyncHandler


logger = logging.getLogger(__name__)
MAX_PIPELINE_KEYS = 200
DEFAULT_TARGET_CONNECTION_IDLE_TIMEOUT = 60.0


def _bounded_pipeline_batch_size(value) -> int:
    return max(1, min(int(value), MAX_PIPELINE_KEYS))


_DEFAULT_SKIP_COMMANDS = {
    "PING",
    "REPLCONF",
    "INFO",
    "CONFIG",
    "MONITOR",
    "SUBSCRIBE",
    "PSUBSCRIBE",
    "UNSUBSCRIBE",
    "PUNSUBSCRIBE",
}


class UnifiedIncrementalService:
    """Run one ordered replication stream and fan it out to all targets."""

    def __init__(
        self,
        mode: str,
        source_conn,
        target_connections: Dict[str, RedisConnectionManager],
        config: Dict[str, Any],
    ):
        self.mode = mode.lower()
        if self.mode == "sync":
            logger.warning(
                "SYNC streaming lacks an exact snapshot offset; using PSYNC FULLRESYNC"
            )
            self.mode = "psync"
        if self.mode not in {"scan", "sync", "psync"}:
            raise ValueError(f"unsupported incremental mode: {mode}")

        self.source_conn = source_conn
        self.source_client = (
            source_conn.source_client
            if hasattr(source_conn, "source_client")
            else source_conn
        )
        self.target_connections = target_connections
        self.config = config
        self.pipeline_batch_size = _bounded_pipeline_batch_size(
            config.get("pipeline_batch_size", 100)
        )
        self.running = False
        self.shutdown_event = threading.Event()
        self.handler = None
        self._lifecycle_lock = threading.Lock()
        self._stopped = False

        self._stats_lock = threading.Lock()
        self._replication_state_lock = threading.Lock()
        self._command_lock = threading.RLock()
        self._delivery_lock = threading.Lock()
        self.stats = {
            "mode": self.mode,
            "commands_received": 0,
            "commands_synced": 0,
            "commands_failed": 0,
            "commands_skipped": 0,
            "commands_duplicated": 0,
            "start_time": None,
            "last_command_time": None,
            "command_types": {},
        }

        self.command_dedup_window = float(
            config.get("command_dedup_window", 0) or 0
        )
        if self.command_dedup_window != 0:
            raise ValueError(
                "command_dedup_window must be 0 because replication streams "
                "may contain legitimate identical commands"
            )
        self.target_command_timeout = float(
            config.get("target_command_timeout", 5) or 5
        )
        self.target_connection_idle_timeout = float(
            config.get(
                "target_connection_idle_timeout",
                DEFAULT_TARGET_CONNECTION_IDLE_TIMEOUT,
            )
            or DEFAULT_TARGET_CONNECTION_IDLE_TIMEOUT
        )
        if self.target_connection_idle_timeout <= 0:
            raise ValueError("target_connection_idle_timeout must be positive")
        self.apply_mode = str(config.get("apply_mode", "key_state")).lower()
        if self.apply_mode not in {"direct", "key_state"}:
            raise ValueError(f"unsupported replication apply_mode: {self.apply_mode}")
        if self.mode in {"sync", "psync"} and self.apply_mode == "direct":
            raise ValueError(
                "direct apply_mode cannot safely retry an ambiguous replication "
                "write; use key_state"
            )

        source_kwargs = getattr(
            getattr(self.source_client, "connection_pool", None),
            "connection_kwargs",
            {},
        )
        self.source_db = int(config.get("source_db", source_kwargs.get("db", 0)) or 0)
        self._selected_db = 0

        filters = config.get("filters") or {}
        skip = config.get(
            "skip_commands",
            filters.get("command_exclude", filters.get("skip_commands")),
        )
        include = config.get(
            "include_commands",
            filters.get("command_include", filters.get("include_commands")),
        )
        self.skip_commands = {
            str(item).upper() for item in (skip or _DEFAULT_SKIP_COMMANDS)
        }
        self.skip_commands.discard("SELECT")
        self.include_commands = (
            {str(item).upper() for item in include} if include else None
        )
        self.command_filter = config.get("command_filter") or filters.get(
            "command_filter"
        )
        self.key_filter = config.get("key_filter") or filters.get("key_filter")
        self.target_failure_callback = config.get("target_failure_callback")
        self.target_success_callback = config.get("target_success_callback")
        self.database_resync_callback = config.get("database_resync_callback")
        self.key_pattern = self._compile_patterns(
            config.get("key_pattern") or "*"
        )[0]
        configured_key_types = config.get("key_types")
        if configured_key_types is None and config.get("key_type"):
            configured_key_types = [config["key_type"]]
        if isinstance(configured_key_types, (str, bytes)):
            configured_key_types = [configured_key_types]
        self.key_types = {
            (
                item.decode("ascii", errors="replace").lower()
                if isinstance(item, bytes)
                else str(item).lower()
            )
            for item in (configured_key_types or [])
        }
        self.include_patterns = self._compile_patterns(
            filters.get("include_patterns") or ["*"]
        )
        self.exclude_patterns = self._compile_patterns(
            filters.get("exclude_patterns") or []
        )
        self.filter_min_ttl = int(filters.get("min_ttl") or 0)
        self.filter_max_key_size = int(filters.get("max_key_size") or 0)
        self._has_key_filters = bool(
            self.key_pattern != b"*"
            or self.key_types
            or self.exclude_patterns
            or self.include_patterns != [b"*"]
            or callable(self.key_filter)
            or self.filter_min_ttl > 0
            or self.filter_max_key_size > 0
        )
        if self.apply_mode == "direct" and self._has_key_filters:
            raise ValueError(
                "direct apply_mode does not support key filters on multi-key "
                "commands; use key_state"
            )

        self.executor = ThreadPoolExecutor(
            max_workers=max(1, min(8, len(target_connections) or 1)),
            thread_name_prefix="redis-replication-target",
        )
        self._target_locks = {
            name: threading.Lock() for name in target_connections
        }
        self._target_stream_connections: Dict[str, Any] = {}
        self._target_connection_db: Dict[str, int] = {}
        self._target_connection_last_used: Dict[str, float] = {}
        self._target_connections_in_use: Set[str] = set()
        self._target_inflight_futures: Dict[str, Any] = {}
        self._cancelled_targets: Set[str] = set()
        self._target_connections_lock = threading.Lock()
        self._connection_reaper_thread: Optional[threading.Thread] = None
        self._source_command_connections: Dict[int, Any] = {}
        self._source_command_connections_lock = threading.Lock()

        # A failed fan-out may have succeeded on a subset of targets.  Retain
        # those acknowledgements so retrying a non-idempotent command does not
        # execute it twice on targets that already accepted it.
        self._pending_delivery_key: Optional[Tuple[int, Tuple[bytes, ...]]] = None
        self._pending_delivery_success: Set[str] = set()
        self._pending_prepared_state = None

        self._callback_count = 0
        self._callback_started_at: Optional[float] = None
        self._callback_started_monotonic: Optional[float] = None
        self._last_progress_time: Optional[float] = None
        self._last_callback_error: Optional[str] = None

    @staticmethod
    def _compile_patterns(patterns) -> List[bytes]:
        values = patterns if isinstance(patterns, (list, tuple, set)) else [patterns]
        return [
            item if isinstance(item, bytes) else str(item).encode("utf-8")
            for item in values
        ]

    def start(self):
        with self._lifecycle_lock:
            if self._stopped:
                return False
            if self.running:
                return True
            self.running = True
            self.shutdown_event.clear()
            self._start_target_connection_reaper()
        with self._stats_lock:
            self.stats["start_time"] = time.time()

        if self.mode == "scan":
            return True
        if self.mode == "sync":
            self._start_sync_mode()
        else:
            self._start_psync_mode()
        return not self._stopped

    def stop(self, timeout: float = 1.0) -> bool:
        with self._lifecycle_lock:
            if self._stopped:
                return self.wait_stopped(0)
            self._stopped = True
            self.running = False
            self.shutdown_event.set()
            handler = self.handler
        self._interrupt_target_stream_connections()
        self._interrupt_source_command_connections()
        handler_stopped = True
        if handler and hasattr(handler, "stop_replication"):
            try:
                result = handler.stop_replication(timeout=timeout)
            except TypeError:
                result = handler.stop_replication()
            handler_stopped = result is not False
        try:
            self.executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:  # Python 3.7/3.8 compatibility
            self.executor.shutdown(wait=False)
        self._print_stats()
        return handler_stopped and self.wait_stopped(0)

    def wait_stopped(self, timeout: float = 10.0) -> bool:
        handler = self.handler
        if handler is None:
            return True
        wait_callback = getattr(handler, "wait_stopped", None)
        if callable(wait_callback):
            return bool(wait_callback(timeout))
        thread = getattr(handler, "replication_thread", None)
        if thread is None:
            return True
        if thread is threading.current_thread():
            return False
        thread.join(timeout=max(0.0, float(timeout)))
        return not thread.is_alive()

    def _snapshot_callback(self, rdb_data: bytes) -> bool:
        self._begin_replication_callback()
        succeeded = False
        failure_message = "replication snapshot callback reported failure"
        try:
            callback = self.config.get("snapshot_callback")
            if not callable(callback):
                failure_message = (
                    "FULLRESYNC requires config['snapshot_callback']; "
                    "snapshot retained as failure"
                )
                logger.error(failure_message)
                return False
            with self._command_lock:
                result = callback(rdb_data) is not False
                if result:
                    self._selected_db = 0
                    with self._delivery_lock:
                        self._pending_delivery_key = None
                        self._pending_delivery_success.clear()
                        self._pending_prepared_state = None
                    succeeded = True
                return result
        except Exception as exc:
            failure_message = str(exc)
            logger.error("snapshot callback failed: %s", exc, exc_info=True)
            return False
        finally:
            self._finish_replication_callback(succeeded, failure_message)

    def _start_sync_mode(self):
        target_client = next(
            (
                manager.target_client
                for manager in self.target_connections.values()
                if manager and manager.target_client
            ),
            self.source_client,
        )
        self.handler = SyncHandler(self.source_client, target_client)
        snapshot_callback = self.config.get("snapshot_callback")
        ok = self.handler.perform_full_sync(
            clear_target=False,
            hold_connection_for_stream=True,
            materialize_snapshot=bool(
                self.config.get("materialize_snapshot", True)
            ),
            snapshot_callback=(
                self._snapshot_callback if callable(snapshot_callback) else None
            ),
        )
        if not ok:
            self.running = False
            return
        ok = self.handler.start_replication_stream(
            lambda command, args: self._on_command_received([command, *args]),
            stop_event=self.shutdown_event,
            ack_interval=float(self.config.get("ack_interval", 1) or 1),
        )
        if not ok and self.running:
            logger.error("SYNC replication stream disconnected")
            self.running = False

    def _start_psync_mode(self):
        snapshot_callback = self.config.get("snapshot_callback")
        handler = PSyncIncrementalHandler(
            source_client=self.source_client,
            buffer_size=int(self.config.get("buffer_size", 8192)),
            snapshot_callback=(
                self._snapshot_callback if callable(snapshot_callback) else None
            ),
            listening_port=int(self.config.get("listening_port", 6380)),
            ack_interval=float(self.config.get("ack_interval", 1) or 1),
            materialize_snapshot=bool(
                self.config.get("materialize_snapshot", True)
            ),
            capture_max_size=int(
                self.config.get("capture_max_size", 1024 * 1024 * 1024)
            ),
        )
        with self._lifecycle_lock:
            if self._stopped or not self.running:
                return
            self.handler = handler
            handler.start_replication(self._on_command_received)
        while self.running and not self.shutdown_event.wait(0.2):
            thread = handler.replication_thread
            if thread is not None and not thread.is_alive():
                if handler.last_error:
                    logger.error("PSYNC stopped: %s", handler.last_error)
                self.running = False
                break

    def _on_command_received(self, command: List[bytes]) -> bool:
        """Track callback progress and commit only after every target succeeds."""
        self._begin_replication_callback()
        delivered = False
        failure_message = "replication command callback reported failure"
        try:
            delivered = self._process_command_received(command)
            return delivered
        except Exception as exc:
            failure_message = str(exc)
            raise
        finally:
            self._finish_replication_callback(delivered, failure_message)

    def _begin_replication_callback(self) -> None:
        """Start timing before a callback waits on the ordered command barrier."""
        started_at = time.time()
        started_monotonic = time.monotonic()
        with self._replication_state_lock:
            if self._callback_count == 0:
                self._callback_started_at = started_at
                self._callback_started_monotonic = started_monotonic
            self._callback_count += 1

    def _finish_replication_callback(
        self, succeeded: bool, failure_message: str
    ) -> None:
        with self._replication_state_lock:
            if succeeded:
                self._last_progress_time = time.time()
                self._last_callback_error = None
            else:
                self._last_callback_error = failure_message
            self._callback_count = max(0, self._callback_count - 1)
            if self._callback_count == 0:
                self._callback_started_at = None
                self._callback_started_monotonic = None

    def _process_command_received(self, command: List[bytes]) -> bool:
        """Return true only after every target has committed this command."""

        with self._command_lock:
            if not self.running or not command:
                return False
            command = [
                item if isinstance(item, bytes) else str(item).encode("utf-8")
                for item in command
            ]
            cmd_name = command[0].decode("utf-8", errors="replace").upper()
            with self._stats_lock:
                self.stats["commands_received"] += 1
                self.stats["last_command_time"] = time.time()
                self.stats["command_types"][cmd_name] = (
                    self.stats["command_types"].get(cmd_name, 0) + 1
                )

            if cmd_name == "SELECT":
                if len(command) != 2:
                    return self._record_failure("invalid SELECT frame")
                try:
                    selected_db = int(command[1])
                except ValueError:
                    return self._record_failure("invalid SELECT database")
                if self.apply_mode == "key_state":
                    self._selected_db = selected_db
                    return self._record_success()
                delivered = self._sync_command_to_targets(command)
                if delivered:
                    self._selected_db = selected_db
                return delivered

            if self._should_skip_command(cmd_name, command):
                with self._stats_lock:
                    self.stats["commands_skipped"] += 1
                return True

            if self.apply_mode == "key_state":
                if not self._command_affects_source_db(cmd_name, command):
                    with self._stats_lock:
                        self.stats["commands_skipped"] += 1
                    return True
                return self._sync_key_state_to_targets(command)
            if self._has_key_filters:
                try:
                    keys = self._command_keys(command)
                except Exception as exc:
                    return self._record_failure(
                        f"failed to evaluate replication key filter: {exc}"
                    )
                if keys and any(not self._key_allowed(key) for key in keys):
                    with self._stats_lock:
                        self.stats["commands_skipped"] += 1
                    return True
            return self._sync_command_to_targets(command)

    def _record_success(self) -> bool:
        with self._stats_lock:
            self.stats["commands_synced"] += 1
        return True

    def _record_failure(self, message: str) -> bool:
        logger.error(message)
        with self._stats_lock:
            self.stats["commands_failed"] += 1
        return False

    def _should_skip_command(self, cmd_name: str, command: List[bytes]) -> bool:
        if cmd_name in self.skip_commands:
            return True
        if self.include_commands is not None and cmd_name not in self.include_commands:
            return True
        if callable(self.command_filter):
            return self.command_filter(command, self._selected_db) is False
        return False

    @staticmethod
    def _database_argument(value: bytes) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _command_affects_source_db(
        self, cmd_name: str, command: List[bytes]
    ) -> bool:
        if cmd_name == "FLUSHALL":
            return True
        if cmd_name == "SWAPDB" and len(command) >= 3:
            databases = {
                self._database_argument(command[1]),
                self._database_argument(command[2]),
            }
            return self.source_db in databases
        if cmd_name == "MOVE" and len(command) >= 3:
            destination = self._database_argument(command[2])
            return self._selected_db == self.source_db or destination == self.source_db
        if cmd_name == "COPY" and len(command) >= 3:
            destination_db = self._selected_db
            for index in range(3, len(command) - 1):
                if command[index].upper() == b"DB":
                    parsed = self._database_argument(command[index + 1])
                    if parsed is not None:
                        destination_db = parsed
                    break
            return destination_db == self.source_db
        return self._selected_db == self.source_db

    def _key_name_allowed(self, key: bytes, database: Optional[int] = None) -> bool:
        if not redis_glob_match(key, self.key_pattern):
            return False
        included = any(
            redis_glob_match(key, pattern) for pattern in self.include_patterns
        )
        excluded = any(
            redis_glob_match(key, pattern) for pattern in self.exclude_patterns
        )
        if not included or excluded:
            return False
        if callable(self.key_filter):
            filter_db = self._selected_db if database is None else database
            return self.key_filter(key, filter_db) is not False
        return True

    def _key_allowed(self, key: bytes) -> bool:
        return self._key_name_allowed(key)

    @contextmanager
    def command_barrier(self):
        """Pause command delivery while a target is brought to a known baseline."""
        with self._command_lock:
            yield

    def register_target(self, target_name: str, target_conn) -> None:
        """Register a target while the caller holds ``command_barrier``."""
        with self._command_lock:
            self.target_connections[target_name] = target_conn
            self._target_locks.setdefault(target_name, threading.Lock())
            with self._target_connections_lock:
                self._cancelled_targets.discard(target_name)
            with self._delivery_lock:
                # A recovered target with the same name is a new delivery
                # endpoint. It must acknowledge the pending frame itself.
                self._pending_delivery_success.discard(target_name)

    def unregister_target(self, target_name: str):
        """Remove a failed target from active fan-out and close its stream socket."""
        with self._command_lock:
            target_conn = self.target_connections.pop(target_name, None)
            if target_conn is not None:
                self._interrupt_target_stream_connection(
                    target_name,
                    target_conn=target_conn,
                )
            return target_conn

    def _delivery_identity(self, command: List[bytes]):
        return self._selected_db, tuple(command)

    def _begin_delivery(self, command: List[bytes]):
        identity = self._delivery_identity(command)
        with self._delivery_lock:
            if self._pending_delivery_key != identity:
                self._pending_delivery_key = identity
                self._pending_delivery_success.clear()
                self._pending_prepared_state = None
            active_names = list(self.target_connections)
            for name in active_names:
                self._target_locks.setdefault(name, threading.Lock())
            remaining = [
                name for name in active_names
                if name not in self._pending_delivery_success
            ]
        return identity, remaining

    def _finish_target_delivery(self, identity, target_name: str) -> None:
        with self._delivery_lock:
            if self._pending_delivery_key == identity:
                self._pending_delivery_success.add(target_name)

    def _complete_delivery(self, identity) -> bool:
        with self._delivery_lock:
            active_names = set(self.target_connections)
            complete = (
                self._pending_delivery_key == identity
                and bool(active_names)
                and active_names.issubset(self._pending_delivery_success)
            )
            if complete:
                self._pending_delivery_key = None
                self._pending_delivery_success.clear()
                self._pending_prepared_state = None
        if complete:
            return self._record_success()
        with self._stats_lock:
            self.stats["commands_failed"] += 1
        return False

    def _wait_target_future(
        self,
        target_name: str,
        future,
        submitted_at: float,
    ) -> bool:
        deadline = submitted_at + self.target_command_timeout
        while True:
            if future.done():
                try:
                    return future.result() is not False
                except Exception as exc:
                    logger.error("target %s command failed: %s", target_name, exc)
                    return False
            remaining = deadline - time.monotonic()
            if remaining <= 0 or self.shutdown_event.is_set():
                reason = (
                    "service shutdown"
                    if self.shutdown_event.is_set()
                    else f"{self.target_command_timeout:.1f}s deadline"
                )
                logger.error(
                    "target %s interrupted after %s",
                    target_name,
                    reason,
                )
                self._interrupt_target_stream_connection(
                    target_name,
                    future=future,
                )
                future.cancel()
                return False
            try:
                return future.result(timeout=min(0.05, remaining)) is not False
            except FuturesTimeoutError:
                continue
            except Exception as exc:
                logger.error("target %s command failed: %s", target_name, exc)
                return False

    def _fan_out(self, identity, remaining, function, *args) -> bool:
        futures = {}
        for name in remaining:
            start_gate = threading.Event()

            def run_registered_target(
                target_name=name,
                target_conn=self.target_connections[name],
                gate=start_gate,
            ):
                gate.wait()
                return function(target_name, target_conn, *args)

            submitted_at = time.monotonic()
            future = self.executor.submit(run_registered_target)
            futures[name] = (future, submitted_at)
            with self._target_connections_lock:
                self._target_inflight_futures[name] = future
            future.add_done_callback(
                lambda completed, target=name: self._finish_target_future(
                    target, completed
                )
            )
            # A worker may acquire its raw connection immediately. Publish the
            # owning future first so stop/timeout always defer pool release
            # until that worker has actually finished with the connection.
            start_gate.set()
        failed_targets = []
        for target_name, (future, submitted_at) in futures.items():
            if self._wait_target_future(target_name, future, submitted_at):
                self._finish_target_delivery(identity, target_name)
                callback = self.target_success_callback
                if callable(callback):
                    try:
                        callback(target_name)
                    except Exception as exc:
                        logger.error(
                            "target success callback failed for %s: %s",
                            target_name,
                            exc,
                        )
            else:
                failed_targets.append(target_name)
        for target_name in failed_targets:
            callback = self.target_failure_callback
            if callable(callback):
                try:
                    callback(target_name)
                except Exception as exc:
                    logger.error(
                        "target failure callback failed for %s: %s",
                        target_name,
                        exc,
                    )
        return self._complete_delivery(identity)

    def _sync_command_to_targets(self, command: List[bytes]) -> bool:
        identity, remaining = self._begin_delivery(command)
        return self._fan_out(
            identity, remaining, self._sync_command_to_target, command, self._selected_db
        )

    def _command_keys(self, command: List[bytes]) -> List[bytes]:
        connection = None
        pool = getattr(self.source_client, "connection_pool", None)
        try:
            get_connection = getattr(pool, "get_connection", None)
            release = getattr(pool, "release", None)
            if callable(get_connection) and callable(release):
                # Redis' reply is binary-safe, while redis-py's COMMAND response
                # callback decodes GETKEYS results even when decode_responses is
                # disabled. Reading from Connection bypasses that callback.
                connection = get_connection("COMMAND")
                if not self._register_source_command_connection(connection):
                    raise RuntimeError("replication service is stopping")
                connection.send_command(
                    "COMMAND", "GETKEYS", *command, check_health=False
                )
                keys = connection.read_response(disable_decoding=True)
            else:
                command_getkeys = getattr(
                    self.source_client, "command_getkeys", None
                )
                if callable(command_getkeys):
                    keys = command_getkeys(*command)
                else:
                    keys = self.source_client.execute_command(
                        "COMMAND GETKEYS", *command
                    )
        except Exception as exc:
            raise RuntimeError(
                f"COMMAND GETKEYS rejected {command[0]!r}: {exc}"
            ) from exc
        finally:
            if connection is not None:
                self._unregister_source_command_connection(connection)
                try:
                    pool.release(connection)
                except Exception:
                    logger.warning(
                        "failed to release COMMAND GETKEYS connection",
                        exc_info=True,
                    )
        if keys is None:
            return []
        if not isinstance(keys, (list, tuple)):
            raise RuntimeError(
                f"COMMAND GETKEYS returned invalid response {keys!r}"
            )
        normalized = []
        for key in keys:
            if isinstance(key, bytes):
                normalized.append(key)
            elif isinstance(key, (bytearray, memoryview)):
                normalized.append(bytes(key))
            elif isinstance(key, str):
                normalized.append(key.encode("utf-8"))
            else:
                raise RuntimeError(
                    "COMMAND GETKEYS returned an invalid key "
                    f"of type {type(key).__name__}"
                )
        return normalized

    def _register_source_command_connection(self, connection) -> bool:
        with self._source_command_connections_lock:
            if self.shutdown_event.is_set() or self._stopped:
                return False
            self._source_command_connections[id(connection)] = connection
            return True

    def _unregister_source_command_connection(self, connection) -> None:
        with self._source_command_connections_lock:
            self._source_command_connections.pop(id(connection), None)

    def _interrupt_source_command_connections(self) -> None:
        with self._source_command_connections_lock:
            connections = list(self._source_command_connections.values())
            self._source_command_connections.clear()
            # The owner unregisters before returning the connection to the pool.
            # Keep the lock until disconnect completes so a late disconnect can
            # never race with another borrower reusing that connection.
            for connection in connections:
                try:
                    connection.disconnect()
                except Exception:
                    pass

    def _capture_key_states(self, keys: List[bytes], has_pexpiretime: bool):
        states = []
        stride = (
            2
            + bool(has_pexpiretime)
            + bool(self.key_types)
            + bool(self.filter_max_key_size > 0)
        )
        try:
            for offset in range(0, len(keys), self.pipeline_batch_size):
                chunk = keys[offset:offset + self.pipeline_batch_size]
                pipeline = self.source_client.pipeline(transaction=True)
                for key in chunk:
                    pipeline.dump(key)
                    pipeline.pttl(key)
                    if has_pexpiretime:
                        pipeline.execute_command("PEXPIRETIME", key)
                    if self.key_types:
                        pipeline.type(key)
                    if self.filter_max_key_size > 0:
                        pipeline.execute_command("MEMORY", "USAGE", key)
                pttl_sample_started_ns = time.monotonic_ns()
                raw = pipeline.execute()
                observed_at_ms = int(time.time() * 1000)
                elapsed_ms = (
                    max(0, time.monotonic_ns() - pttl_sample_started_ns)
                    + 999_999
                ) // 1_000_000
                for index, key in enumerate(chunk):
                    base = index * stride
                    dump_value = raw[base]
                    ttl_value = raw[base + 1]
                    expiry_value = raw[base + 2] if has_pexpiretime else None
                    next_index = base + 2 + bool(has_pexpiretime)
                    key_type = raw[next_index] if self.key_types else None
                    next_index += bool(self.key_types)
                    memory_size = (
                        raw[next_index]
                        if self.filter_max_key_size > 0
                        else None
                    )
                    if isinstance(expiry_value, BaseException):
                        if (
                            has_pexpiretime
                            and isinstance(expiry_value, redis.ResponseError)
                        ):
                            cache_source_pexpiretime_support(
                                self.source_client, False
                            )
                            return self._capture_key_states(keys, False)
                        raise expiry_value
                    for value in (
                        dump_value,
                        ttl_value,
                        key_type,
                        memory_size,
                    ):
                        if isinstance(value, BaseException):
                            raise value
                    ttl = int(ttl_value)
                    if dump_value is None or ttl in (-2, 0):
                        states.append((key, None, None, None, False))
                        continue
                    if ttl < -2:
                        raise ValueError(
                            f"invalid PTTL response for {key!r}: {ttl}"
                        )
                    expires_at_ms = None
                    remaining_ttl_ms = ttl
                    if ttl > 0:
                        remaining_ttl_ms = ttl - elapsed_ms
                        if remaining_ttl_ms <= 0:
                            states.append((key, None, None, None, False))
                            continue
                        try:
                            absolute_ms = int(expiry_value)
                        except (TypeError, ValueError):
                            absolute_ms = -1
                        if absolute_ms > 0:
                            # The source absolute deadline is transferred as-is,
                            # but source liveness is determined by PTTL only.
                            expires_at_ms = absolute_ms
                            expiry_is_exact = True
                        else:
                            expires_at_ms = observed_at_ms + remaining_ttl_ms
                            expiry_is_exact = False
                    else:
                        expiry_is_exact = False
                    if self.key_types:
                        normalized_type = (
                            key_type.decode("ascii", errors="replace").lower()
                            if isinstance(key_type, bytes)
                            else str(key_type).lower()
                        )
                        if normalized_type not in self.key_types:
                            states.append((key, None, None, None, False))
                            continue
                    if (
                        self.filter_min_ttl > 0
                        and 0 < remaining_ttl_ms < self.filter_min_ttl * 1000
                    ):
                        states.append((key, None, None, None, False))
                        continue
                    if (
                        self.filter_max_key_size > 0
                        and memory_size is not None
                        and int(memory_size) > self.filter_max_key_size
                    ):
                        states.append((key, None, None, None, False))
                        continue
                    states.append(
                        (
                            key,
                            dump_value,
                            expires_at_ms,
                            remaining_ttl_ms,
                            expiry_is_exact,
                        )
                    )
        except Exception as error:
            if has_pexpiretime and is_pexpiretime_capability_error(error):
                cache_source_pexpiretime_support(self.source_client, False)
                return self._capture_key_states(keys, False)
            raise
        return states

    def _prepare_key_state(self, command: List[bytes]):
        cmd_name = command[0].decode("utf-8", errors="replace").upper()
        # Pub/sub and script/function registries are outside the mirrored DB.
        # GETKEYS reports an error for these legitimate keyless stream frames.
        if cmd_name in {
            "MULTI", "EXEC", "DISCARD", "PUBLISH", "SPUBLISH", "FUNCTION", "SCRIPT",
        }:
            return "noop", []
        if cmd_name in {"FLUSHDB", "FLUSHALL"}:
            return "flushdb", []
        if cmd_name == "SWAPDB":
            return "resync", []

        if cmd_name == "MOVE":
            keys = [command[1]] if len(command) >= 2 else []
        elif cmd_name == "COPY":
            keys = [command[2]] if len(command) >= 3 else []
        else:
            keys = self._command_keys(command)

        keys = [key for key in keys if self._key_allowed(key)]
        if not keys:
            return "noop", []
        if self.filter_max_key_size > 0:
            states = []
            for key in keys:
                captured = capture_dump_with_preflight(
                    self.source_client,
                    key,
                    preserve_ttl=True,
                    key_types=self.key_types,
                    min_ttl=self.filter_min_ttl,
                    max_key_size=self.filter_max_key_size,
                )
                if captured is None:
                    states.append((key, None, None, None, False))
                else:
                    states.append(
                        (
                            key,
                            captured.dump_data,
                            captured.expires_at_ms,
                            captured.remaining_ttl_ms,
                            captured.expiry_is_exact,
                        )
                    )
            return "keys", states
        has_pexpiretime = source_supports_pexpiretime(
            self.source_client,
            keys[0],
        )
        return "keys", self._capture_key_states(keys, has_pexpiretime)

    def _sync_key_state_to_targets(self, command: List[bytes]) -> bool:
        identity, remaining = self._begin_delivery(command)
        with self._delivery_lock:
            prepared = self._pending_prepared_state
        if prepared is None:
            try:
                prepared = self._prepare_key_state(command)
            except Exception as exc:
                return self._record_failure(f"failed to capture source key state: {exc}")
            with self._delivery_lock:
                if self._pending_delivery_key == identity:
                    self._pending_prepared_state = prepared
        operation, states = prepared
        if operation == "noop":
            with self._delivery_lock:
                self._pending_delivery_success.update(self.target_connections)
            return self._complete_delivery(identity)
        if operation == "resync":
            callback = self.database_resync_callback
            if not callable(callback):
                return self._record_failure(
                    "SWAPDB requires a database_resync_callback in key_state mode"
                )
            try:
                if callback() is False:
                    return self._record_failure("database resync callback reported failure")
            except Exception as exc:
                return self._record_failure(f"database resync callback failed: {exc}")
            with self._delivery_lock:
                self._pending_delivery_success.update(self.target_connections)
            return self._complete_delivery(identity)
        return self._fan_out(
            identity,
            remaining,
            self._apply_key_state_to_target,
            operation,
            states,
        )

    def _target_base_db(self, target_conn: RedisConnectionManager) -> int:
        client = target_conn.target_client
        kwargs = getattr(getattr(client, "connection_pool", None), "connection_kwargs", {})
        return int(kwargs.get("db", 0) or 0)

    def _get_target_stream_connection(self, target_name, target_conn):
        with self._target_connections_lock:
            if self.shutdown_event.is_set() or not self.running:
                raise RuntimeError("replication service is stopping")
            if (
                target_name in self._cancelled_targets
                or self.target_connections.get(target_name) is not target_conn
            ):
                raise RuntimeError(f"target {target_name} is no longer active")
            connection = self._target_stream_connections.get(target_name)
            if connection is not None:
                return connection
        pool = target_conn.target_client.connection_pool
        connection = pool.get_connection("REPLICATION")
        with self._target_connections_lock:
            if (
                self.shutdown_event.is_set()
                or not self.running
                or target_name in self._cancelled_targets
                or self.target_connections.get(target_name) is not target_conn
            ):
                try:
                    connection.disconnect()
                except Exception:
                    pass
                try:
                    pool.release(connection)
                except Exception:
                    pass
                raise RuntimeError(f"target {target_name} is no longer active")
            existing = self._target_stream_connections.get(target_name)
            if existing is not None:
                try:
                    pool.release(connection)
                except Exception:
                    pass
                return existing
            self._target_stream_connections[target_name] = connection
            self._target_connection_db[target_name] = self._target_base_db(target_conn)
            self._target_connection_last_used[target_name] = time.monotonic()
            return connection

    @contextmanager
    def _target_connection_activity(self, target_name: str):
        with self._target_connections_lock:
            self._target_connections_in_use.add(target_name)
        try:
            yield
        finally:
            with self._target_connections_lock:
                self._target_connections_in_use.discard(target_name)
                if target_name in self._target_stream_connections:
                    self._target_connection_last_used[target_name] = time.monotonic()

    def _drop_target_stream_connection(self, target_name, target_conn) -> None:
        with self._target_connections_lock:
            connection = self._target_stream_connections.pop(target_name, None)
            self._target_connection_db.pop(target_name, None)
            self._target_connection_last_used.pop(target_name, None)
        if connection is None:
            return
        try:
            connection.disconnect()
        except Exception:
            pass
        try:
            target_conn.target_client.connection_pool.release(connection)
        except Exception:
            pass

    @staticmethod
    def _send_target_command(connection, command: List[bytes]):
        connection.send_command(*command)
        return connection.read_response()

    def _delete_target_key_in_scope(self, connection, key: bytes):
        if self.key_types or self.filter_min_ttl > 0 or self.filter_max_key_size > 0:
            command = build_atomic_filtered_delete_command(
                [key],
                key_types=self.key_types,
                min_ttl=self.filter_min_ttl,
                max_key_size=self.filter_max_key_size,
            )
        else:
            command = [b"DEL", key]
        return self._send_target_command(connection, command)

    def _select_target_db(self, target_name, connection, database: int) -> None:
        if self._target_connection_db.get(target_name) == database:
            return
        response = self._send_target_command(
            connection, [b"SELECT", str(database).encode("ascii")]
        )
        if response not in (b"OK", "OK", True):
            raise redis.ResponseError(f"SELECT {database} returned {response!r}")
        self._target_connection_db[target_name] = database

    def _sync_command_to_target(
        self,
        target_name: str,
        target_conn: RedisConnectionManager,
        command: List[bytes],
        selected_db: int,
    ) -> bool:
        lock = self._target_locks[target_name]
        with lock:
            with self._target_connection_activity(target_name):
                try:
                    if not target_conn or not target_conn.target_client:
                        return False
                    connection = self._get_target_stream_connection(target_name, target_conn)
                    cmd_name = command[0].upper()
                    if cmd_name != b"SELECT":
                        self._select_target_db(target_name, connection, selected_db)
                    response = self._send_target_command(connection, command)
                    if cmd_name == b"SELECT":
                        self._target_connection_db[target_name] = int(command[1])
                    return not isinstance(response, BaseException)
                except Exception as exc:
                    logger.error("target %s execution failed: %s", target_name, exc)
                    self._drop_target_stream_connection(target_name, target_conn)
                    return False

    def _apply_key_state_to_target(
        self,
        target_name: str,
        target_conn: RedisConnectionManager,
        operation: str,
        states,
    ) -> bool:
        lock = self._target_locks[target_name]
        with lock:
            with self._target_connection_activity(target_name):
                try:
                    if not target_conn or not target_conn.target_client:
                        return False
                    connection = self._get_target_stream_connection(target_name, target_conn)
                    target_db = self._target_base_db(target_conn)
                    self._select_target_db(target_name, connection, target_db)
                    if operation == "flushdb":
                        self._flush_target_scope(connection, target_conn)
                        return True
                    for state in states:
                        key, dump_value, expires_at_ms = state[:3]
                        if len(state) >= 5:
                            remaining_ttl_ms = state[3]
                            expiry_is_exact = bool(state[4])
                        else:
                            remaining_ttl_ms = None
                            expiry_is_exact = (
                                bool(state[3]) if len(state) == 4 else False
                            )
                        if dump_value is None:
                            self._delete_target_key_in_scope(connection, key)
                        elif expires_at_ms is not None and expires_at_ms <= 0:
                            self._delete_target_key_in_scope(connection, key)
                        else:
                            if expires_at_ms is None:
                                restore_command = [
                                    b"RESTORE",
                                    key,
                                    b"0",
                                    dump_value,
                                    b"REPLACE",
                                ]
                            else:
                                # Redis evaluates ABSTTL against its own clock.
                                # A skewed sync-process clock must not pre-delete
                                # a source key whose sampled PTTL was positive.
                                restore_command = [
                                    b"RESTORE",
                                    key,
                                    str(expires_at_ms).encode("ascii"),
                                    dump_value,
                                    b"REPLACE",
                                    b"ABSTTL",
                                ]
                            try:
                                self._send_target_command(
                                    connection,
                                    restore_command,
                                )
                            except redis.ResponseError as exc:
                                if (
                                    expires_at_ms is not None
                                    and _is_absttl_compatibility_error(exc)
                                ):
                                    try:
                                        restored = restore_dump_with_deadline(
                                            target_conn.target_client,
                                            key,
                                            dump_value,
                                            expires_at_ms,
                                            overwrite=True,
                                            prefer_absttl=False,
                                            expires_at_is_exact=expiry_is_exact,
                                        )
                                    except redis.ResponseError as legacy_exc:
                                        exc = legacy_exc
                                    else:
                                        if not restored:
                                            raise RuntimeError(
                                                f"legacy RESTORE skipped key {key!r}"
                                            )
                                        continue
                                if not _is_restore_compatibility_error(exc):
                                    raise
                                logger.warning(
                                    "target %s RESTORE compatibility fallback for key=%r: %s",
                                    target_name,
                                    key,
                                    exc,
                                )
                                fallback_pttl = (
                                    -1
                                    if expires_at_ms is None
                                    else (
                                        max(1, int(remaining_ttl_ms))
                                        if remaining_ttl_ms is not None
                                        else max(
                                            1,
                                            expires_at_ms - int(time.time() * 1000),
                                        )
                                    )
                                )
                                if not _sync_key_fallback(
                                    self.source_client,
                                    target_conn.target_client,
                                    key,
                                    fallback_pttl,
                                    True,
                                    overwrite=True,
                                    expires_at_ms=expires_at_ms,
                                    expected_dump=dump_value,
                                    key_types=self.key_types,
                                    min_ttl=self.filter_min_ttl,
                                    max_key_size=self.filter_max_key_size,
                                    expires_at_is_exact=expiry_is_exact,
                                ):
                                    raise RuntimeError(
                                        f"RESTORE compatibility fallback skipped key {key!r}"
                                    )
                    return True
                except Exception as exc:
                    logger.error("target %s state apply failed: %s", target_name, exc)
                    self._drop_target_stream_connection(target_name, target_conn)
                    return False

    def _flush_target_scope(self, connection, target_conn) -> None:
        """Apply source FLUSH* without deleting keys outside the sync filters."""
        if not self._has_key_filters:
            self._send_target_command(connection, [b"FLUSHDB"])
            return

        target_client = target_conn.target_client
        cursor = 0
        while True:
            cursor, raw_keys = target_client.scan(cursor=cursor, count=1000)
            keys = [
                key if isinstance(key, bytes) else str(key).encode("utf-8")
                for key in raw_keys
            ]
            managed_keys = [
                key for key in keys
                if self._key_name_allowed(key, database=self.source_db)
            ]
            for offset in range(0, len(managed_keys), self.pipeline_batch_size):
                chunk = managed_keys[offset:offset + self.pipeline_batch_size]
                self._send_target_command(
                    connection,
                    build_atomic_filtered_delete_command(
                        chunk,
                        key_types=self.key_types,
                        min_ttl=self.filter_min_ttl,
                        max_key_size=self.filter_max_key_size,
                    ),
                )
            if cursor == 0:
                break

    @staticmethod
    def _disconnect_and_release_target_connection(target_conn, connection) -> None:
        try:
            connection.disconnect()
        except Exception:
            pass
        try:
            target_conn.target_client.connection_pool.release(connection)
        except Exception:
            pass

    def _finish_target_future(self, target_name: str, future) -> None:
        with self._target_connections_lock:
            if self._target_inflight_futures.get(target_name) is future:
                self._target_inflight_futures.pop(target_name, None)

    def _release_target_connection_after_future(
        self,
        target_conn,
        connection,
        future,
    ) -> None:
        if future is None or future.done():
            self._disconnect_and_release_target_connection(target_conn, connection)
            return
        future.add_done_callback(
            lambda _completed: self._disconnect_and_release_target_connection(
                target_conn, connection
            )
        )

    def _interrupt_target_stream_connection(
        self,
        target_name: str,
        *,
        target_conn=None,
        future=None,
    ) -> None:
        """Cancel one target and detach its raw socket without taking its lock."""
        with self._target_connections_lock:
            self._cancelled_targets.add(target_name)
            connection = self._target_stream_connections.pop(target_name, None)
            self._target_connection_db.pop(target_name, None)
            self._target_connection_last_used.pop(target_name, None)
            if target_conn is None:
                target_conn = self.target_connections.get(target_name)
            if future is None:
                future = self._target_inflight_futures.get(target_name)
        if connection is None or target_conn is None:
            return
        try:
            connection.disconnect()
        except Exception:
            pass
        self._release_target_connection_after_future(
            target_conn,
            connection,
            future,
        )

    def _start_target_connection_reaper(self) -> None:
        thread = self._connection_reaper_thread
        if thread is not None and thread.is_alive():
            return

        def reap_loop() -> None:
            interval = max(
                0.1,
                min(5.0, self.target_connection_idle_timeout / 2),
            )
            while not self.shutdown_event.wait(interval):
                self._reap_idle_target_connections()

        self._connection_reaper_thread = threading.Thread(
            target=reap_loop,
            name="redis-replication-target-reaper",
            daemon=True,
        )
        self._connection_reaper_thread.start()

    def _reap_idle_target_connections(self, now: Optional[float] = None) -> int:
        """Bound the lifetime of cached raw sockets, including CLOSE-WAIT FDs."""
        now = time.monotonic() if now is None else float(now)
        expired = []
        with self._target_connections_lock:
            for target_name, connection in list(
                self._target_stream_connections.items()
            ):
                if target_name in self._target_connections_in_use:
                    continue
                last_used = self._target_connection_last_used.get(target_name, now)
                if now - last_used < self.target_connection_idle_timeout:
                    continue
                target_conn = self.target_connections.get(target_name)
                self._target_stream_connections.pop(target_name, None)
                self._target_connection_db.pop(target_name, None)
                self._target_connection_last_used.pop(target_name, None)
                if target_conn is not None:
                    expired.append((target_conn, connection))
        for target_conn, connection in expired:
            self._disconnect_and_release_target_connection(target_conn, connection)
        return len(expired)

    def _interrupt_target_stream_connections(self):
        """Disconnect in-flight target sockets without waiting on target locks."""
        with self._target_connections_lock:
            active = []
            for target_name, connection in self._target_stream_connections.items():
                target_conn = self.target_connections.get(target_name)
                if target_conn is not None:
                    active.append(
                        (
                            target_conn,
                            connection,
                            self._target_inflight_futures.get(target_name),
                        )
                    )
            self._cancelled_targets.update(self.target_connections)
            self._target_stream_connections.clear()
            self._target_connection_db.clear()
            self._target_connection_last_used.clear()
        for target_conn, connection, future in active:
            try:
                connection.disconnect()
            except Exception:
                pass
            self._release_target_connection_after_future(
                target_conn,
                connection,
                future,
            )
        return active

    def _print_stats(self):
        snapshot = self.get_stats()
        if snapshot.get("start_time"):
            logger.info(
                "%s replication: received=%s synced=%s failed=%s skipped=%s",
                self.mode.upper(),
                snapshot["commands_received"],
                snapshot["commands_synced"],
                snapshot["commands_failed"],
                snapshot["commands_skipped"],
            )

    def get_replication_status(self) -> Dict[str, Any]:
        """Return liveness, stream offsets and callback-stall diagnostics."""
        now = time.time()
        now_monotonic = time.monotonic()
        with self._replication_state_lock:
            callback_in_flight = self._callback_count > 0
            callback_started_at = self._callback_started_at
            callback_started_monotonic = self._callback_started_monotonic
            last_progress_time = self._last_progress_time
            callback_error = self._last_callback_error
        callback_duration = None
        if callback_in_flight and callback_started_monotonic is not None:
            callback_duration = max(
                0.0, now_monotonic - callback_started_monotonic
            )
        stalled = bool(
            callback_duration is not None
            and callback_duration >= self.target_command_timeout
        )

        handler = self.handler
        handler_running = (
            bool(getattr(handler, "running"))
            if handler is not None and hasattr(handler, "running")
            else None
        )
        handler_error_value = getattr(handler, "last_error", None)
        handler_error = (
            str(handler_error_value) if handler_error_value is not None else None
        )
        committed_offset = getattr(handler, "replication_offset", None)
        replication_id = getattr(handler, "replication_id", None)
        baseline_established = bool(
            self.mode != "psync"
            or (
                replication_id
                and isinstance(committed_offset, int)
                and committed_offset >= 0
            )
        )
        try:
            received_offset = getattr(handler, "received_offset", None)
        except Exception:
            received_offset = None
        pending_offset_bytes = None
        if isinstance(committed_offset, int) and isinstance(received_offset, int):
            pending_offset_bytes = max(0, received_offset - committed_offset)

        with self._target_connections_lock:
            open_target_connections = len(self._target_stream_connections)
        active_target_count = len(self.target_connections)
        last_error = callback_error or handler_error
        service_running = bool(
            self.running
            and not self.shutdown_event.is_set()
            and not self._stopped
        )
        healthy = bool(
            service_running
            and active_target_count > 0
            and handler_running is not False
            and baseline_established
            and not stalled
            and last_error is None
        )
        return {
            "mode": self.mode,
            "running": service_running,
            "healthy": healthy,
            "handler_running": handler_running,
            "last_error": last_error,
            "handler_error": handler_error,
            "baseline_established": baseline_established,
            "committed_offset": committed_offset,
            "received_offset": received_offset,
            "pending_offset_bytes": pending_offset_bytes,
            "callback_in_flight": callback_in_flight,
            "callback_started_at": callback_started_at,
            "callback_duration": callback_duration,
            "stalled": stalled,
            "last_progress_time": last_progress_time,
            "last_progress_ago": (
                max(0.0, now - last_progress_time)
                if last_progress_time is not None
                else None
            ),
            "active_target_count": active_target_count,
            "open_target_connections": open_target_connections,
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._stats_lock:
            stats = {
                **{key: value for key, value in self.stats.items() if key != "command_types"},
                "command_types": dict(self.stats["command_types"]),
            }
        if stats["start_time"]:
            stats["duration"] = time.time() - stats["start_time"]
        if stats["last_command_time"]:
            stats["last_command_ago"] = time.time() - stats["last_command_time"]
        return stats
