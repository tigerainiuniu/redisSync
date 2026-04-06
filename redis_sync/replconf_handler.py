"""
Redis REPLCONF命令处理器

使用REPLCONF命令实现Redis复制配置。
处理复制握手、ACK和配置管理。

复制握手（PING + REPLCONF*）必须在同一条 TCP 连接上顺序发送，与后续 PSYNC 一致。
"""

import redis
import logging
import time
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


def _resp_ok(response) -> bool:
    return response == b"OK" or response == "OK"


class ReplConfHandler:
    """处理Redis REPLCONF命令进行复制配置。"""

    def __init__(self, source_client: redis.Redis, target_client: Optional[redis.Redis] = None):
        self.source_client = source_client
        self.target_client = target_client
        self.replication_id: Optional[str] = None
        self.replication_offset: int = 0
        self.listening_port: Optional[int] = None
        self.capabilities: Dict[str, str] = {}

    def perform_replication_handshake(
        self,
        listening_port: int = 6380,
        capabilities: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        在同一条连接上完成复制握手：PING → REPLCONF listening-port → REPLCONF capa*。
        """
        pool = self.source_client.connection_pool
        connection = pool.get_connection("REPLICATION_HANDSHAKE")
        try:
            logger.info("Starting replication handshake (single connection)")

            connection.send_command("PING")
            pong = connection.read_response()
            if pong not in (b"PONG", "PONG", True):
                logger.error("PING failed on handshake connection: %r", pong)
                return False
            logger.debug("PING successful")

            connection.send_command("REPLCONF", "listening-port", listening_port)
            r = connection.read_response()
            if not _resp_ok(r):
                logger.error("REPLCONF listening-port failed: %r", r)
                return False
            self.listening_port = listening_port
            logger.debug("REPLCONF listening-port %s ok", listening_port)

            merged: Dict[str, str] = {"eof": "", "psync2": ""}
            if capabilities:
                merged.update(capabilities)

            for cap, val in merged.items():
                if val:
                    connection.send_command("REPLCONF", "capa", f"{cap}:{val}")
                else:
                    connection.send_command("REPLCONF", "capa", cap)
                r2 = connection.read_response()
                if not _resp_ok(r2):
                    logger.error("REPLCONF capa %s failed: %r", cap, r2)
                    return False
                self.capabilities[cap] = val
                logger.debug("REPLCONF capa %s ok", cap)

            logger.info("Replication handshake completed successfully")
            return True

        except Exception as e:
            logger.error("Replication handshake failed: %s", e)
            return False
        finally:
            pool.release(connection)

    def send_replconf_ack(self, offset: int) -> bool:
        try:
            connection = self.source_client.connection_pool.get_connection("REPLCONF")
            try:
                connection.send_command("REPLCONF", "ACK", offset)
                logger.debug("REPLCONF ACK %s sent", offset)
                self.replication_offset = offset
                return True
            finally:
                self.source_client.connection_pool.release(connection)
        except Exception as e:
            logger.error("REPLCONF ACK command failed: %s", e)
            return False

    def get_replconf_getack(self) -> Optional[int]:
        try:
            connection = self.source_client.connection_pool.get_connection("REPLCONF")
            try:
                connection.send_command("REPLCONF", "GETACK", "*")
                response = connection.read_response()

                if isinstance(response, list) and len(response) >= 3:
                    if (response[0] == b"REPLCONF" or response[0] == "REPLCONF") and (
                        response[1] == b"ACK" or response[1] == "ACK"
                    ):
                        offset = int(response[2])
                        logger.debug("REPLCONF GETACK returned offset: %s", offset)
                        return offset

                logger.error("Invalid REPLCONF GETACK response: %s", response)
                return None
            finally:
                self.source_client.connection_pool.release(connection)
        except Exception as e:
            logger.error("REPLCONF GETACK command failed: %s", e)
            return None

    def configure_replication_timeout(self, timeout: int) -> bool:
        try:
            connection = self.source_client.connection_pool.get_connection("REPLCONF")
            try:
                connection.send_command("REPLCONF", "timeout", timeout)
                response = connection.read_response()
                if _resp_ok(response):
                    logger.debug("REPLCONF timeout %s successful", timeout)
                    return True
                logger.error("REPLCONF timeout failed: %s", response)
                return False
            finally:
                self.source_client.connection_pool.release(connection)
        except Exception as e:
            logger.error("REPLCONF timeout command failed: %s", e)
            return False

    def get_master_replication_info(self) -> Dict[str, Any]:
        try:
            info = self.source_client.info("replication")

            repl_info = {
                "role": info.get("role", "unknown"),
                "master_replid": info.get("master_replid", ""),
                "master_replid2": info.get("master_replid2", ""),
                "master_repl_offset": info.get("master_repl_offset", 0),
                "second_repl_offset": info.get("second_repl_offset", -1),
                "repl_backlog_active": info.get("repl_backlog_active", 0),
                "repl_backlog_size": info.get("repl_backlog_size", 0),
                "repl_backlog_first_byte_offset": info.get(
                    "repl_backlog_first_byte_offset", 0
                ),
                "repl_backlog_histlen": info.get("repl_backlog_histlen", 0),
                "connected_slaves": info.get("connected_slaves", 0),
            }

            slave_info = []
            slave_count = info.get("connected_slaves", 0)
            for i in range(slave_count):
                slave_key = f"slave{i}"
                if slave_key in info:
                    slave_data = info[slave_key]
                    if isinstance(slave_data, bytes):
                        text = slave_data.decode("utf-8", errors="replace")
                    else:
                        text = str(slave_data)
                    slave_dict = {}
                    for item in text.split(","):
                        if "=" in item:
                            k, v = item.split("=", 1)
                            slave_dict[k] = v
                    slave_info.append(slave_dict)

            repl_info["slaves"] = slave_info
            return repl_info

        except Exception as e:
            logger.error("Failed to get master replication info: %s", e)
            return {}

    def wait_for_replication_sync(
        self, target_offset: int, timeout: int = 30, check_interval: float = 0.1
    ) -> bool:
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_offset = self.get_replconf_getack()
            if current_offset is not None and current_offset >= target_offset:
                logger.info("Replication sync reached target offset: %s", target_offset)
                return True

            time.sleep(check_interval)

        logger.warning("Replication sync timeout waiting for offset: %s", target_offset)
        return False

    def get_replication_lag(self) -> Optional[float]:
        try:
            master_info = self.get_master_replication_info()
            master_offset = master_info.get("master_repl_offset", 0)

            current_offset = self.get_replconf_getack()
            if current_offset is None:
                return None

            offset_diff = master_offset - current_offset
            if offset_diff <= 0:
                return 0.0

            estimated_lag = offset_diff / 1000.0
            return max(0.0, estimated_lag)

        except Exception as e:
            logger.error("Failed to calculate replication lag: %s", e)
            return None

    def get_current_config(self) -> Dict[str, Any]:
        return {
            "replication_id": self.replication_id,
            "replication_offset": self.replication_offset,
            "listening_port": self.listening_port,
            "capabilities": self.capabilities.copy(),
            "master_info": self.get_master_replication_info(),
        }
