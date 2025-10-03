"""
Redis连接管理器

处理源和目标Redis实例的连接，支持身份验证、SSL和连接池。
支持自动重连和健康检查，适用于跨境远距离传输场景。
"""

import redis
import logging
import time
from typing import Optional, Dict, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class RedisConnectionManager:
    """管理源和目标Redis实例的连接，支持自动重连和健康检查。"""

    def __init__(self, retry_config: Optional[Dict[str, Any]] = None):
        self.source_client: Optional[redis.Redis] = None
        self.target_client: Optional[redis.Redis] = None
        self._source_config: Optional[Dict[str, Any]] = None
        self._target_config: Optional[Dict[str, Any]] = None

        # 重试配置（适用于跨境传输）
        self.retry_config = retry_config or {
            'max_attempts': 5,      # 增加重试次数
            'backoff_factor': 2,    # 指数退避
            'max_delay': 60,        # 最大延迟60秒
            'initial_delay': 1      # 初始延迟1秒
        }

        # 连接健康状态
        self._last_ping_time = {}
        self._connection_failures = {}
    
    def _create_connection_config(self,
                                 host: str,
                                 port: int,
                                 password: Optional[str],
                                 db: int,
                                 ssl: bool,
                                 ssl_cert_reqs: Optional[str],
                                 ssl_ca_certs: Optional[str],
                                 ssl_certfile: Optional[str],
                                 ssl_keyfile: Optional[str],
                                 connection_pool_kwargs: Optional[Dict[str, Any]],
                                 **kwargs) -> Dict[str, Any]:
        """创建Redis连接配置（针对跨境传输优化）"""
        config = {
            "host": host,
            "port": port,
            "password": password,
            "db": db,
            "ssl": ssl,
            "ssl_cert_reqs": ssl_cert_reqs,
            "ssl_ca_certs": ssl_ca_certs,
            "ssl_certfile": ssl_certfile,
            "ssl_keyfile": ssl_keyfile,
            "decode_responses": False,

            # 跨境传输优化配置
            "socket_timeout": 60,           # 增加超时时间到60秒
            "socket_connect_timeout": 30,   # 连接超时30秒
            "socket_keepalive": True,       # 启用TCP keepalive
            # 注意：socket_keepalive_options 在某些系统上可能不支持，已移除
            "retry_on_timeout": True,       # 超时自动重试
            "health_check_interval": 30,    # 每30秒健康检查
            **kwargs
        }

        if connection_pool_kwargs:
            config.update(connection_pool_kwargs)

        # 移除None值
        return {k: v for k, v in config.items() if v is not None}

    def _connect_with_retry(self, config: Dict[str, Any], name: str) -> redis.Redis:
        """带重试机制的连接（指数退避）"""
        max_attempts = self.retry_config['max_attempts']
        backoff_factor = self.retry_config['backoff_factor']
        max_delay = self.retry_config['max_delay']
        initial_delay = self.retry_config['initial_delay']

        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"尝试连接 {name} (第 {attempt}/{max_attempts} 次)")

                # 禁用 CLIENT SETINFO（腾讯云 Redis 不支持）
                config['client_name'] = None
                config['lib_name'] = None
                config['lib_version'] = None

                client = redis.Redis(**config)
                # 测试连接
                client.ping()

                logger.info(f"✅ 成功连接到 {name} ({config['host']}:{config['port']})")

                # 重置失败计数
                self._connection_failures[name] = 0
                self._last_ping_time[name] = time.time()

                return client

            except (redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError,
                    redis.exceptions.ResponseError) as e:
                last_error = e
                self._connection_failures[name] = self._connection_failures.get(name, 0) + 1

                if attempt < max_attempts:
                    # 计算退避延迟（指数退避）
                    delay = min(initial_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    logger.warning(
                        f"⚠️  连接 {name} 失败 (第 {attempt}/{max_attempts} 次): {e}"
                        f"\n   {delay:.1f}秒后重试..."
                    )
                    time.sleep(delay)
                else:
                    logger.error(f"❌ 连接 {name} 失败，已达最大重试次数: {e}")

            except Exception as e:
                last_error = e
                logger.error(f"❌ 连接 {name} 时发生未预期的错误: {e}")
                break

        # 所有重试都失败
        raise ConnectionError(f"无法连接到 {name}: {last_error}")

    def connect_source(self,
                      host: str = "localhost",
                      port: int = 6379,
                      password: Optional[str] = None,
                      db: int = 0,
                      ssl: bool = False,
                      ssl_cert_reqs: Optional[str] = None,
                      ssl_ca_certs: Optional[str] = None,
                      ssl_certfile: Optional[str] = None,
                      ssl_keyfile: Optional[str] = None,
                      connection_pool_kwargs: Optional[Dict[str, Any]] = None,
                      **kwargs) -> redis.Redis:
        """
        连接到源Redis实例（带重试机制）。

        参数:
            host: Redis主机地址
            port: Redis端口
            password: Redis密码
            db: Redis数据库编号
            ssl: 启用SSL连接
            ssl_cert_reqs: SSL证书要求
            ssl_ca_certs: SSL CA证书文件路径
            ssl_certfile: SSL证书文件路径
            ssl_keyfile: SSL密钥文件路径
            connection_pool_kwargs: 额外的连接池参数
            **kwargs: 额外的Redis客户端参数

        返回:
            Redis客户端实例
        """
        config = self._create_connection_config(
            host, port, password, db, ssl,
            ssl_cert_reqs, ssl_ca_certs, ssl_certfile, ssl_keyfile,
            connection_pool_kwargs, **kwargs
        )

        self._source_config = config
        self.source_client = self._connect_with_retry(config, f"源Redis {host}:{port}")
        return self.source_client
    
    def connect_target(self,
                      host: str = "localhost",
                      port: int = 6379,
                      password: Optional[str] = None,
                      db: int = 0,
                      ssl: bool = False,
                      ssl_cert_reqs: Optional[str] = None,
                      ssl_ca_certs: Optional[str] = None,
                      ssl_certfile: Optional[str] = None,
                      ssl_keyfile: Optional[str] = None,
                      connection_pool_kwargs: Optional[Dict[str, Any]] = None,
                      **kwargs) -> redis.Redis:
        """
        连接到目标Redis实例。

        参数:
            host: Redis主机地址
            port: Redis端口
            password: Redis密码
            db: Redis数据库编号
            ssl: 启用SSL连接
            ssl_cert_reqs: SSL证书要求
            ssl_ca_certs: SSL CA证书文件路径
            ssl_certfile: SSL证书文件路径
            ssl_keyfile: SSL密钥文件路径
            connection_pool_kwargs: 额外的连接池参数
            **kwargs: 额外的Redis客户端参数

        返回:
            Redis客户端实例
        """
        config = self._create_connection_config(
            host, port, password, db, ssl,
            ssl_cert_reqs, ssl_ca_certs, ssl_certfile, ssl_keyfile,
            connection_pool_kwargs, **kwargs
        )

        self._target_config = config
        self.target_client = self._connect_with_retry(config, f"目标Redis {host}:{port}")
        return self.target_client
    
    def connect_from_url(self, source_url: str, target_url: str) -> tuple[redis.Redis, redis.Redis]:
        """
        Connect to Redis instances using URLs.
        
        Args:
            source_url: Source Redis URL (redis://host:port/db)
            target_url: Target Redis URL (redis://host:port/db)
            
        Returns:
            Tuple of (source_client, target_client)
        """
        try:
            self.source_client = redis.from_url(source_url, decode_responses=False)
            self.source_client.ping()
            logger.info(f"Connected to source Redis from URL: {source_url}")
        except Exception as e:
            logger.error(f"Failed to connect to source Redis from URL: {e}")
            raise
            
        try:
            self.target_client = redis.from_url(target_url, decode_responses=False)
            self.target_client.ping()
            logger.info(f"Connected to target Redis from URL: {target_url}")
        except Exception as e:
            logger.error(f"Failed to connect to target Redis from URL: {e}")
            raise
            
        return self.source_client, self.target_client
    
    def get_source_info(self) -> Dict[str, Any]:
        """Get source Redis instance information."""
        if not self.source_client:
            raise RuntimeError("Source client not connected")
        return self.source_client.info()
    
    def get_target_info(self) -> Dict[str, Any]:
        """Get target Redis instance information."""
        if not self.target_client:
            raise RuntimeError("Target client not connected")
        return self.target_client.info()
    
    def check_connection_health(self, client: redis.Redis, name: str) -> bool:
        """
        检查Redis连接健康状态

        参数:
            client: Redis客户端
            name: 连接名称（用于日志）

        返回:
            True if healthy, False otherwise
        """
        try:
            client.ping()
            self._last_ping_time[name] = time.time()
            return True
        except (redis.exceptions.ConnectionError,
                redis.exceptions.TimeoutError) as e:
            logger.warning(f"⚠️  {name} 健康检查失败: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ {name} 健康检查异常: {e}")
            return False

    def reconnect_if_needed(self, client_type: str = 'source') -> Optional[redis.Redis]:
        """
        如果连接断开，尝试重新连接

        参数:
            client_type: 'source' 或 'target'

        返回:
            重新连接的客户端，如果失败则返回None
        """
        if client_type == 'source':
            client = self.source_client
            config = self._source_config
            name = f"源Redis {config.get('host')}:{config.get('port')}"
        else:
            client = self.target_client
            config = self._target_config
            name = f"目标Redis {config.get('host')}:{config.get('port')}"

        if not config:
            logger.error(f"❌ 无法重连 {name}: 配置不存在")
            return None

        # 检查当前连接
        if client and self.check_connection_health(client, name):
            return client

        # 需要重连
        logger.warning(f"🔄 {name} 连接断开，尝试重新连接...")

        try:
            new_client = self._connect_with_retry(config, name)

            if client_type == 'source':
                self.source_client = new_client
            else:
                self.target_client = new_client

            logger.info(f"✅ {name} 重新连接成功")
            return new_client

        except Exception as e:
            logger.error(f"❌ {name} 重新连接失败: {e}")
            return None

    def execute_with_retry(self,
                          client: redis.Redis,
                          operation: callable,
                          *args,
                          client_type: str = 'source',
                          **kwargs) -> Any:
        """
        执行Redis操作，失败时自动重试和重连

        参数:
            client: Redis客户端
            operation: 要执行的操作（函数）
            client_type: 'source' 或 'target'
            *args, **kwargs: 传递给operation的参数

        返回:
            操作结果
        """
        max_attempts = self.retry_config['max_attempts']
        backoff_factor = self.retry_config['backoff_factor']
        initial_delay = self.retry_config['initial_delay']

        last_error = None

        for attempt in range(1, max_attempts + 1):
            try:
                return operation(*args, **kwargs)

            except (redis.exceptions.ConnectionError,
                    redis.exceptions.TimeoutError) as e:
                last_error = e
                logger.warning(f"⚠️  操作失败 (第 {attempt}/{max_attempts} 次): {e}")

                if attempt < max_attempts:
                    # 尝试重连
                    new_client = self.reconnect_if_needed(client_type)
                    if new_client:
                        client = new_client
                        # 更新operation中的client引用（如果是方法调用）
                        if hasattr(operation, '__self__'):
                            operation = getattr(new_client, operation.__name__)

                    # 退避延迟
                    delay = min(initial_delay * (backoff_factor ** (attempt - 1)),
                               self.retry_config['max_delay'])
                    logger.info(f"   {delay:.1f}秒后重试...")
                    time.sleep(delay)

            except Exception as e:
                last_error = e
                logger.error(f"❌ 操作失败（非网络错误）: {e}")
                raise

        # 所有重试都失败
        raise last_error

    def close_connections(self):
        """Close all Redis connections."""
        if self.source_client:
            try:
                self.source_client.close()
                logger.info("已关闭源Redis连接")
            except:
                pass
        if self.target_client:
            try:
                self.target_client.close()
                logger.info("已关闭目标Redis连接")
            except:
                pass

    def close(self):
        """Alias for close_connections"""
        self.close_connections()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()
