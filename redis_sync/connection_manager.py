"""
Redis连接管理器

处理源和目标Redis实例的连接，支持身份验证、SSL和连接池。
支持自动重连和健康检查，适用于跨境远距离传输场景。
"""

import redis
import inspect
import logging
import secrets
import threading
import time
from typing import Optional, Dict, Any, Tuple

from .exceptions import RedisConnectionError

logger = logging.getLogger(__name__)


def assert_distinct_redis_databases(
    source_client: redis.Redis,
    target_client: redis.Redis,
    source_db: int,
    target_db: int,
    *,
    allow_marker: bool = True,
) -> None:
    """Verify Redis identity even when endpoint aliases or INFO ACLs hide it."""
    if int(source_db) != int(target_db):
        return

    try:
        source_info = source_client.info('server')
        target_info = target_client.info('server')
    except redis.RedisError:
        source_info = target_info = {}
    source_run_id = source_info.get('run_id') or source_info.get(b'run_id')
    target_run_id = target_info.get('run_id') or target_info.get(b'run_id')
    if source_run_id and target_run_id:
        if source_run_id == target_run_id:
            raise ValueError(
                "目标与 source 的 Redis run_id 和 db 相同，同步会造成源数据清理"
            )
    if not allow_marker:
        return

    marker_key = b"__redis_sync_identity__:" + secrets.token_hex(16).encode("ascii")
    marker_value = secrets.token_bytes(32)
    marker_created = False
    try:
        marker_created = bool(
            target_client.set(marker_key, marker_value, nx=True, px=10_000)
        )
        if not marker_created:
            raise ValueError("Redis 同库标记发生冲突，请重试")
        if source_client.get(marker_key) == marker_value:
            raise ValueError(
                "source 与 target 指向同一 Redis 数据库，同步会造成源数据清理"
            )
        if not target_client.exists(marker_key):
            raise ValueError(
                "Redis 同库身份标记在校验完成前失效，请重试"
            )
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError(
            "Redis 同库身份校验失败；需要 INFO 权限，或目标 SET/EXISTS/DEL "
            "与源 GET 权限"
        ) from exc
    finally:
        if marker_created:
            try:
                target_client.delete(marker_key)
            except Exception:
                pass


class RedisConnectionManager:
    """管理源和目标Redis实例的连接，支持自动重连和健康检查。"""

    def __init__(
        self,
        retry_config: Optional[Dict[str, Any]] = None,
        shutdown_event: Optional[threading.Event] = None,
    ):
        self.source_client: Optional[redis.Redis] = None
        self.target_client: Optional[redis.Redis] = None
        self._source_config: Optional[Dict[str, Any]] = None
        self._target_config: Optional[Dict[str, Any]] = None
        self._owns_source_client = False
        self._owns_target_client = False
        self._close_lock = threading.Lock()
        self._closed = False
        self.shutdown_event = shutdown_event

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

    @staticmethod
    def _close_client(client: Optional[redis.Redis]) -> None:
        """Best-effort cleanup for a client that will no longer be managed."""
        if client is None:
            return
        try:
            client.close()
        except Exception:
            pass
        try:
            client.connection_pool.disconnect()
        except Exception:
            pass

    def _stopping_unlocked(self) -> bool:
        return self._closed or (
            self.shutdown_event is not None and self.shutdown_event.is_set()
        )

    def _is_stopping(self) -> bool:
        with self._close_lock:
            return self._stopping_unlocked()

    def _raise_if_stopping(self, name: str) -> None:
        if self._is_stopping():
            raise RedisConnectionError(f"连接 {name} 的重试已停止")

    def _install_client(
        self,
        client_type: str,
        new_client: redis.Redis,
        config: Optional[Dict[str, Any]],
        *,
        owned: bool,
    ) -> redis.Redis:
        """Atomically transfer a connected client into manager ownership."""
        if client_type not in {"source", "target"}:
            raise ValueError("client_type must be source or target")

        client_attr = f"{client_type}_client"
        config_attr = f"_{client_type}_config"
        owned_attr = f"_owns_{client_type}_client"
        previous = None
        with self._close_lock:
            if self._stopping_unlocked():
                accepted = False
            else:
                current = getattr(self, client_attr)
                if getattr(self, owned_attr) and current is not new_client:
                    previous = current
                setattr(self, client_attr, new_client)
                setattr(self, config_attr, config)
                setattr(self, owned_attr, owned)
                accepted = True

        if not accepted:
            if owned:
                self._close_client(new_client)
            raise RedisConnectionError("连接管理器已停止，拒绝挂载新连接")
        if previous is not None:
            self._close_client(previous)
        return new_client

    def _install_client_pair(
        self,
        new_source: redis.Redis,
        new_target: redis.Redis,
    ) -> Tuple[redis.Redis, redis.Redis]:
        """Atomically replace both URL-created clients."""
        previous_source = None
        previous_target = None
        with self._close_lock:
            if self._stopping_unlocked():
                accepted = False
            else:
                if self._owns_source_client and self.source_client is not new_source:
                    previous_source = self.source_client
                if self._owns_target_client and self.target_client is not new_target:
                    previous_target = self.target_client
                self.source_client = new_source
                self.target_client = new_target
                self._source_config = None
                self._target_config = None
                self._owns_source_client = True
                self._owns_target_client = True
                accepted = True

        if not accepted:
            self._close_client(new_source)
            if new_target is not new_source:
                self._close_client(new_target)
            raise RedisConnectionError("连接管理器已停止，拒绝挂载新连接")
        if previous_source is not None:
            self._close_client(previous_source)
        if previous_target is not None and previous_target is not previous_source:
            self._close_client(previous_target)
        return new_source, new_target
    
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
            self._raise_if_stopping(name)
            client = None
            try:
                logger.info(f"尝试连接 {name} (第 {attempt}/{max_attempts} 次)")

                # 禁用 CLIENT SETINFO（腾讯云 Redis 不支持）
                redis_parameters = inspect.signature(redis.Redis).parameters
                if 'client_name' in redis_parameters:
                    config['client_name'] = None
                if 'lib_name' in redis_parameters:
                    config['lib_name'] = None
                if 'lib_version' in redis_parameters:
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
                self._close_client(client)
                last_error = e
                self._connection_failures[name] = self._connection_failures.get(name, 0) + 1

                if attempt < max_attempts:
                    # 计算退避延迟（指数退避）
                    delay = min(initial_delay * (backoff_factor ** (attempt - 1)), max_delay)
                    logger.warning(
                        f"⚠️  连接 {name} 失败 (第 {attempt}/{max_attempts} 次): {e}"
                        f"\n   {delay:.1f}秒后重试..."
                    )
                    if self.shutdown_event is not None:
                        if self.shutdown_event.wait(delay):
                            raise RedisConnectionError(
                                f"连接 {name} 的重试已停止"
                            )
                    else:
                        time.sleep(delay)
                else:
                    logger.error(f"❌ 连接 {name} 失败，已达最大重试次数: {e}")

            except Exception as e:
                self._close_client(client)
                last_error = e
                logger.error(f"❌ 连接 {name} 时发生未预期的错误: {e}")
                break

        # 所有重试都失败
        raise RedisConnectionError(f"无法连接到 {name}: {last_error}")

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

        new_client = self._connect_with_retry(config, f"源Redis {host}:{port}")
        return self._install_client(
            "source", new_client, config, owned=True
        )
    
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

        new_client = self._connect_with_retry(config, f"目标Redis {host}:{port}")
        return self._install_client(
            "target", new_client, config, owned=True
        )

    def set_source_client(
        self,
        client: redis.Redis,
        config: Optional[Dict[str, Any]] = None,
        *,
        owned: bool = False,
    ) -> None:
        """复用已有源 Redis 客户端（一对多场景避免重复建连）。"""
        self._install_client("source", client, config, owned=owned)

    def set_target_client(
        self,
        client: redis.Redis,
        config: Optional[Dict[str, Any]] = None,
        *,
        owned: bool = False,
    ) -> None:
        self._install_client("target", client, config, owned=owned)

    def connect_from_url(self, source_url: str, target_url: str):
        """
        Connect to Redis instances using URLs.
        
        Args:
            source_url: Source Redis URL (redis://host:port/db)
            target_url: Target Redis URL (redis://host:port/db)
            
        Returns:
            Tuple of (source_client, target_client)
        """
        self._raise_if_stopping("Redis URL")
        new_source = None
        new_target = None
        try:
            new_source = redis.from_url(source_url, decode_responses=False)
            new_source.ping()
            logger.info("Connected to source Redis from URL (credentials hidden)")
        except Exception as e:
            self._close_client(new_source)
            logger.error(f"Failed to connect to source Redis from URL: {e}")
            raise
            
        try:
            self._raise_if_stopping("目标 Redis URL")
            new_target = redis.from_url(target_url, decode_responses=False)
            new_target.ping()
            logger.info("Connected to target Redis from URL (credentials hidden)")
        except Exception as e:
            self._close_client(new_target)
            self._close_client(new_source)
            logger.error(f"Failed to connect to target Redis from URL: {e}")
            raise

        return self._install_client_pair(new_source, new_target)
    
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
        if client_type not in {'source', 'target'}:
            raise ValueError("client_type must be source or target")
        with self._close_lock:
            if self._stopping_unlocked():
                return None
            if client_type == 'source':
                client = self.source_client
                config = self._source_config
            else:
                client = self.target_client
                config = self._target_config

        if not config:
            logger.error(f"❌ 无法重连 {client_type}: 配置不存在")
            return None

        name = f"{'源' if client_type == 'source' else '目标'}Redis {config.get('host')}:{config.get('port')}"

        # 检查当前连接
        if client and self.check_connection_health(client, name):
            with self._close_lock:
                current = (
                    self.source_client
                    if client_type == 'source'
                    else self.target_client
                )
                if not self._stopping_unlocked() and current is client:
                    return client
                return None

        # 需要重连
        logger.warning(f"🔄 {name} 连接断开，尝试重新连接...")

        try:
            new_client = self._connect_with_retry(config, name)
            self._install_client(client_type, new_client, config, owned=True)

            logger.info(f"✅ {name} 重新连接成功")
            return new_client

        except Exception as e:
            logger.error(f"❌ {name} 重新连接失败: {e}")
            return None

    def execute_with_retry(self,
                          client: redis.Redis,
                          operation,
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
        with self._close_lock:
            source_client = self.source_client if self._owns_source_client else None
            target_client = self.target_client if self._owns_target_client else None
            self._closed = True
            self.source_client = None
            self.target_client = None
            self._source_config = None
            self._target_config = None
            self._owns_source_client = False
            self._owns_target_client = False
        if source_client is not None:
            self._close_client(source_client)
            logger.info("已关闭源Redis连接")
        if target_client is not None:
            self._close_client(target_client)
            logger.info("已关闭目标Redis连接")

    def close(self):
        """Alias for close_connections"""
        self.close_connections()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connections()
