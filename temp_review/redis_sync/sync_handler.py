"""
Redis SYNC命令处理器

使用SYNC命令实现Redis复制，用于完整数据库同步。
此处理器可以执行初始同步和持续复制。
"""

import redis
import logging
import time
from typing import Optional, Callable, Dict, Any, Tuple, List

logger = logging.getLogger(__name__)


def _repl_ok(response: Any) -> bool:
    return response == b"OK" or response == "OK"


def run_replication_handshake_on_connection(connection: Any, listening_port: int) -> bool:
    """在同一条连接上发送 PING + REPLCONF（与 PSYNC/SYNC 共用连接）。"""
    try:
        connection.send_command("PING")
        pong = connection.read_response()
        if pong not in (b"PONG", "PONG", True):
            logger.error("Handshake PING failed: %r", pong)
            return False

        connection.send_command("REPLCONF", "listening-port", listening_port)
        if not _repl_ok(connection.read_response()):
            logger.error("REPLCONF listening-port failed")
            return False

        connection.send_command("REPLCONF", "capa", "eof")
        if not _repl_ok(connection.read_response()):
            logger.error("REPLCONF capa eof failed")
            return False

        connection.send_command("REPLCONF", "capa", "psync2")
        if not _repl_ok(connection.read_response()):
            logger.error("REPLCONF capa psync2 failed")
            return False

        return True
    except Exception as e:
        logger.error("Replication handshake on connection failed: %s", e)
        return False


def parse_psync_response(
    response: Any,
) -> Optional[Tuple[str, Optional[str], Optional[int]]]:
    """
    解析 PSYNC 首包。Redis 通常为简单字符串行 +FULLRESYNC / +CONTINUE；
    redis-py 可能返回 bytes、str，少数版本可能为 list。
    返回 (FULLRESYNC|CONTINUE, repl_id, offset)，解析失败返回 None。
    """
    if isinstance(response, list) and len(response) >= 1:
        head = response[0]
        kind = head.decode() if isinstance(head, bytes) else str(head)
        if kind == "FULLRESYNC" and len(response) >= 3:
            rid = response[1].decode() if isinstance(response[1], bytes) else str(response[1])
            off = int(response[2])
            return ("FULLRESYNC", rid, off)
        if kind == "CONTINUE":
            rid = None
            off = None
            if len(response) >= 2:
                rid = response[1].decode() if isinstance(response[1], bytes) else str(response[1])
            if len(response) >= 3:
                off = int(response[2])
            return ("CONTINUE", rid, off)
        return None

    if isinstance(response, (bytes, str)):
        if isinstance(response, bytes):
            text = response.decode("latin-1")
        else:
            text = response
        text = text.strip()
        if text.startswith("+"):
            text = text[1:]
        parts: List[str] = text.split()
        if not parts:
            return None
        if parts[0] == "FULLRESYNC" and len(parts) >= 3:
            return ("FULLRESYNC", parts[1], int(parts[2]))
        if parts[0] == "CONTINUE":
            rid = parts[1] if len(parts) > 1 else None
            off = int(parts[2]) if len(parts) > 2 else None
            return ("CONTINUE", rid, off)
        return None

    return None


class SyncHandler:
    """处理Redis SYNC命令进行完整复制。"""

    def __init__(self, source_client: redis.Redis, target_client: redis.Redis):
        self.source_client = source_client
        self.target_client = target_client
        self.replication_id: Optional[str] = None
        self.replication_offset: int = 0
        # 完成 SYNC/PSYNC 后暂不 release，供 start_replication_stream 消费
        self._replication_stream_connection: Any = None

    def _store_stream_connection(self, connection: Any) -> None:
        if self._replication_stream_connection is not None:
            try:
                self.source_client.connection_pool.release(
                    self._replication_stream_connection
                )
            except Exception:
                pass
        self._replication_stream_connection = connection

    def perform_full_sync(
        self,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        chunk_size: int = 8192,
        clear_target: bool = True,
        replication_handshake: bool = False,
        listening_port: int = 6380,
        hold_connection_for_stream: bool = False,
    ) -> bool:
        try:
            logger.info("Starting full synchronization using SYNC command")

            source_info = self.source_client.info("replication")
            logger.info("Source Redis role: %s", source_info.get("role", "unknown"))

            connection = self.source_client.connection_pool.get_connection("SYNC")
            try:
                if replication_handshake:
                    if not run_replication_handshake_on_connection(
                        connection, listening_port
                    ):
                        return False

                connection.send_command("SYNC")

                response = connection.read_response()
                if isinstance(response, bytes):
                    rdb_data = response
                    logger.info("Received RDB data: %s bytes", len(rdb_data))
                    success = self._apply_rdb_data(
                        rdb_data, progress_callback, clear_target=clear_target
                    )
                    if not success:
                        return False
                    if hold_connection_for_stream:
                        self._store_stream_connection(connection)
                        connection = None
                    logger.info("Full synchronization completed successfully")
                    return True

                logger.error("Unexpected SYNC response: %s", response)
                return False

            finally:
                if connection is not None:
                    self.source_client.connection_pool.release(connection)

        except Exception as e:
            logger.error("Full synchronization failed: %s", e)
            return False

    def perform_psync(
        self,
        replication_id: Optional[str] = None,
        offset: int = -1,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        clear_target: bool = True,
        replication_handshake: bool = False,
        listening_port: int = 6380,
        hold_connection_for_stream: bool = False,
    ) -> bool:
        try:
            logger.info(
                "Starting partial synchronization using PSYNC %s %s",
                replication_id or "?",
                offset,
            )

            connection = self.source_client.connection_pool.get_connection("PSYNC")
            try:
                if replication_handshake:
                    if not run_replication_handshake_on_connection(
                        connection, listening_port
                    ):
                        return False

                if replication_id:
                    connection.send_command("PSYNC", replication_id, offset)
                else:
                    connection.send_command("PSYNC", "?", -1)

                response = connection.read_response()
                parsed = parse_psync_response(response)
                if parsed is None:
                    logger.error("Invalid PSYNC response format: %r", response)
                    return False

                kind, rid, off = parsed
                if kind == "FULLRESYNC":
                    if rid is None or off is None:
                        logger.error("FULLRESYNC missing replid/offset: %r", response)
                        return False
                    self.replication_id = rid
                    self.replication_offset = int(off)
                    logger.info(
                        "Full resync required. New replication ID: %s",
                        self.replication_id,
                    )

                    rdb_response = connection.read_response()
                    if isinstance(rdb_response, bytes):
                        ok = self._apply_rdb_data(
                            rdb_response,
                            progress_callback,
                            clear_target=clear_target,
                        )
                        if not ok:
                            return False
                        if hold_connection_for_stream:
                            self._store_stream_connection(connection)
                            connection = None
                        return True
                    logger.error(
                        "FULLRESYNC 后未收到 RDB bulk 响应: %r", rdb_response
                    )
                    return False

                if kind == "CONTINUE":
                    if rid is not None:
                        self.replication_id = rid
                    if off is not None:
                        self.replication_offset = int(off)
                    logger.info("Partial resync continuing")
                    if hold_connection_for_stream:
                        self._store_stream_connection(connection)
                        connection = None
                    return True

                logger.error("Unknown PSYNC parsed kind: %s", kind)
                return False

            finally:
                if connection is not None:
                    self.source_client.connection_pool.release(connection)

        except Exception as e:
            logger.error("Partial synchronization failed: %s", e)
            return False

    def _apply_rdb_data(
        self,
        rdb_data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None,
        clear_target: bool = True,
    ) -> bool:
        try:
            logger.info("Applying RDB data to target Redis")

            if clear_target:
                self.target_client.flushdb()

            return self._parse_and_restore_rdb(rdb_data, progress_callback)

        except Exception as e:
            logger.error("Failed to apply RDB data: %s", e)
            return False

    def _parse_and_restore_rdb(
        self,
        rdb_data: bytes,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> bool:
        try:
            logger.info("Parsing RDB data and restoring keys")
            _ = rdb_data
            logger.warning(
                "RDB parsing not fully implemented. Using SCAN-based migration."
            )

            from .scan_handler import ScanHandler

            scan_handler = ScanHandler(self.source_client, self.target_client)
            stats = scan_handler.migrate_all_keys(progress_callback=progress_callback)
            if not isinstance(stats, dict):
                return False
            return stats.get("failed_keys", 0) == 0

        except Exception as e:
            logger.error("Failed to parse and restore RDB: %s", e)
            return False

    def start_replication_stream(
        self,
        callback: Callable[[bytes, List[bytes]], None],
        stop_event: Optional[Any] = None,
        connection: Optional[Any] = None,
    ) -> bool:
        """
        在已完成 SYNC/PSYNC 的同一条连接上读取复制流。
        未显式传入 connection 时，会消费 perform_full_sync/perform_psync 在
        hold_connection_for_stream=True 时暂存的连接。
        """
        if connection is None:
            connection = self._replication_stream_connection
            self._replication_stream_connection = None
        if connection is None:
            logger.error(
                "复制流需要 SYNC/PSYNC 后的专用连接："
                "请设置 hold_connection_for_stream=True，或显式传入 connection。"
            )
            return False

        try:
            logger.info("Starting replication stream on provided connection")
            try:
                while True:
                    if stop_event and stop_event.is_set():
                        break
                    try:
                        response = connection.read_response()
                        if response:
                            self._handle_replication_command(response, callback)
                    except redis.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error("Error reading replication stream: %s", e)
                        break
            finally:
                try:
                    self.source_client.connection_pool.release(connection)
                except Exception:
                    pass
            return True
        except Exception as e:
            logger.error("Failed to start replication stream: %s", e)
            return False

    def _handle_replication_command(
        self, response: Any, callback: Callable[[bytes, List[bytes]], None]
    ):
        try:
            if isinstance(response, list) and len(response) > 0:
                cmd = response[0]
                command_b = cmd if isinstance(cmd, bytes) else str(cmd).encode("utf-8")
                args_b: List[bytes] = []
                for arg in response[1:]:
                    if isinstance(arg, bytes):
                        args_b.append(arg)
                    elif isinstance(arg, memoryview):
                        args_b.append(arg.tobytes())
                    else:
                        args_b.append(str(arg).encode("utf-8"))
                callback(command_b, args_b)
        except Exception as e:
            logger.error("Error handling replication command: %s", e)

    def get_replication_info(self) -> Dict[str, Any]:
        return {
            "replication_id": self.replication_id,
            "replication_offset": self.replication_offset,
            "source_info": self.source_client.info("replication"),
            "target_info": self.target_client.info("replication"),
        }
