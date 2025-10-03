"""
Redis REPLCONF命令处理器

使用REPLCONF命令实现Redis复制配置。
处理复制握手、ACK和配置管理。
"""

import redis
import logging
import time
import socket
from typing import Optional, Dict, Any, Tuple

logger = logging.getLogger(__name__)


class ReplConfHandler:
    """处理Redis REPLCONF命令进行复制配置。"""
    
    def __init__(self, source_client: redis.Redis, target_client: Optional[redis.Redis] = None):
        """
        初始化REPLCONF处理器。

        参数:
            source_client: 源Redis客户端 (主节点)
            target_client: 目标Redis客户端 (可选，用于从节点操作)
        """
        self.source_client = source_client
        self.target_client = target_client
        self.replication_id: Optional[str] = None
        self.replication_offset: int = 0
        self.listening_port: Optional[int] = None
        self.capabilities: Dict[str, str] = {}
        
    def perform_replication_handshake(self, 
                                    listening_port: int = 6380,
                                    capabilities: Optional[Dict[str, str]] = None) -> bool:
        """
        Perform complete replication handshake with master.
        
        Args:
            listening_port: Port this replica is listening on
            capabilities: Replication capabilities to announce
            
        Returns:
            True if handshake successful, False otherwise
        """
        try:
            logger.info("Starting replication handshake")
            
            # Step 1: Send PING
            if not self._send_ping():
                return False
            
            # Step 2: Send REPLCONF listening-port
            if not self._send_replconf_listening_port(listening_port):
                return False
            
            # Step 3: Send REPLCONF capabilities
            if capabilities:
                for capability, value in capabilities.items():
                    if not self._send_replconf_capability(capability, value):
                        return False
            
            # Step 4: Send REPLCONF capa (common capabilities)
            default_capabilities = {
                'eof': '',  # Support RDB EOF marker
                'psync2': '',  # Support PSYNC2
            }
            
            for capability, value in default_capabilities.items():
                if not self._send_replconf_capability(capability, value):
                    return False
            
            logger.info("Replication handshake completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Replication handshake failed: {e}")
            return False
    
    def _send_ping(self) -> bool:
        """Send PING command during handshake."""
        try:
            response = self.source_client.ping()
            if response:
                logger.debug("PING successful")
                return True
            else:
                logger.error("PING failed")
                return False
        except Exception as e:
            logger.error(f"PING command failed: {e}")
            return False
    
    def _send_replconf_listening_port(self, port: int) -> bool:
        """Send REPLCONF listening-port command."""
        try:
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                connection.send_command('REPLCONF', 'listening-port', port)
                response = connection.read_response()
                
                if response == b'OK' or response == 'OK':
                    logger.debug(f"REPLCONF listening-port {port} successful")
                    self.listening_port = port
                    return True
                else:
                    logger.error(f"REPLCONF listening-port failed: {response}")
                    return False
                    
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"REPLCONF listening-port command failed: {e}")
            return False
    
    def _send_replconf_capability(self, capability: str, value: str = '') -> bool:
        """Send REPLCONF capa command for a specific capability."""
        try:
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                if value:
                    connection.send_command('REPLCONF', 'capa', f"{capability}:{value}")
                else:
                    connection.send_command('REPLCONF', 'capa', capability)
                    
                response = connection.read_response()
                
                if response == b'OK' or response == 'OK':
                    logger.debug(f"REPLCONF capa {capability} successful")
                    self.capabilities[capability] = value
                    return True
                else:
                    logger.error(f"REPLCONF capa {capability} failed: {response}")
                    return False
                    
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"REPLCONF capa {capability} command failed: {e}")
            return False
    
    def send_replconf_ack(self, offset: int) -> bool:
        """
        Send REPLCONF ACK command to acknowledge replication offset.
        
        Args:
            offset: Replication offset to acknowledge
            
        Returns:
            True if ACK sent successfully, False otherwise
        """
        try:
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                connection.send_command('REPLCONF', 'ACK', offset)
                # ACK command doesn't expect a response in normal replication
                logger.debug(f"REPLCONF ACK {offset} sent")
                self.replication_offset = offset
                return True
                
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"REPLCONF ACK command failed: {e}")
            return False
    
    def get_replconf_getack(self) -> Optional[int]:
        """
        Send REPLCONF GETACK command to get current replication offset.
        
        Returns:
            Current replication offset or None if failed
        """
        try:
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                connection.send_command('REPLCONF', 'GETACK', '*')
                response = connection.read_response()
                
                if isinstance(response, list) and len(response) >= 3:
                    # Response format: ['REPLCONF', 'ACK', offset]
                    if (response[0] == b'REPLCONF' or response[0] == 'REPLCONF') and \
                       (response[1] == b'ACK' or response[1] == 'ACK'):
                        offset = int(response[2])
                        logger.debug(f"REPLCONF GETACK returned offset: {offset}")
                        return offset
                
                logger.error(f"Invalid REPLCONF GETACK response: {response}")
                return None
                
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"REPLCONF GETACK command failed: {e}")
            return None
    
    def configure_replication_timeout(self, timeout: int) -> bool:
        """
        Configure replication timeout using REPLCONF.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            True if configured successfully, False otherwise
        """
        try:
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                connection.send_command('REPLCONF', 'timeout', timeout)
                response = connection.read_response()
                
                if response == b'OK' or response == 'OK':
                    logger.debug(f"REPLCONF timeout {timeout} successful")
                    return True
                else:
                    logger.error(f"REPLCONF timeout failed: {response}")
                    return False
                    
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"REPLCONF timeout command failed: {e}")
            return False
    
    def get_master_replication_info(self) -> Dict[str, Any]:
        """
        Get replication information from master.
        
        Returns:
            Dictionary containing replication information
        """
        try:
            info = self.source_client.info('replication')
            
            repl_info = {
                'role': info.get('role', 'unknown'),
                'master_replid': info.get('master_replid', ''),
                'master_replid2': info.get('master_replid2', ''),
                'master_repl_offset': info.get('master_repl_offset', 0),
                'second_repl_offset': info.get('second_repl_offset', -1),
                'repl_backlog_active': info.get('repl_backlog_active', 0),
                'repl_backlog_size': info.get('repl_backlog_size', 0),
                'repl_backlog_first_byte_offset': info.get('repl_backlog_first_byte_offset', 0),
                'repl_backlog_histlen': info.get('repl_backlog_histlen', 0),
                'connected_slaves': info.get('connected_slaves', 0)
            }
            
            # Add slave information if available
            slave_info = []
            slave_count = info.get('connected_slaves', 0)
            for i in range(slave_count):
                slave_key = f'slave{i}'
                if slave_key in info:
                    slave_data = info[slave_key]
                    # Parse slave info string: ip=127.0.0.1,port=6380,state=online,offset=123,lag=0
                    slave_dict = {}
                    for item in slave_data.split(','):
                        if '=' in item:
                            key, value = item.split('=', 1)
                            slave_dict[key] = value
                    slave_info.append(slave_dict)
            
            repl_info['slaves'] = slave_info
            
            return repl_info
            
        except Exception as e:
            logger.error(f"Failed to get master replication info: {e}")
            return {}
    
    def wait_for_replication_sync(self, 
                                 target_offset: int,
                                 timeout: int = 30,
                                 check_interval: float = 0.1) -> bool:
        """
        Wait for replication to reach a specific offset.
        
        Args:
            target_offset: Target replication offset to wait for
            timeout: Maximum time to wait in seconds
            check_interval: Interval between checks in seconds
            
        Returns:
            True if target offset reached, False if timeout
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            current_offset = self.get_replconf_getack()
            if current_offset is not None and current_offset >= target_offset:
                logger.info(f"Replication sync reached target offset: {target_offset}")
                return True
            
            time.sleep(check_interval)
        
        logger.warning(f"Replication sync timeout waiting for offset: {target_offset}")
        return False
    
    def get_replication_lag(self) -> Optional[float]:
        """
        Calculate replication lag in seconds.
        
        Returns:
            Replication lag in seconds or None if unable to calculate
        """
        try:
            master_info = self.get_master_replication_info()
            master_offset = master_info.get('master_repl_offset', 0)
            
            current_offset = self.get_replconf_getack()
            if current_offset is None:
                return None
            
            # This is a simplified lag calculation
            # In practice, you'd need to consider the rate of replication
            offset_diff = master_offset - current_offset
            
            # Estimate lag based on typical replication speed
            # This is a rough approximation
            if offset_diff <= 0:
                return 0.0
            
            # Assume average command size and replication speed
            estimated_lag = offset_diff / 1000.0  # Very rough estimate
            return max(0.0, estimated_lag)
            
        except Exception as e:
            logger.error(f"Failed to calculate replication lag: {e}")
            return None
    
    def get_current_config(self) -> Dict[str, Any]:
        """Get current REPLCONF configuration."""
        return {
            'replication_id': self.replication_id,
            'replication_offset': self.replication_offset,
            'listening_port': self.listening_port,
            'capabilities': self.capabilities.copy(),
            'master_info': self.get_master_replication_info()
        }
