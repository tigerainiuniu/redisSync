"""
Redis Sync工具的自定义异常。

为不同错误条件定义特定的异常类型。
"""


class RedisSyncError(Exception):
    """Redis Sync工具的基础异常。"""
    pass


class ConnectionError(RedisSyncError):
    """Redis连接失败时抛出。"""
    pass


class ConfigurationError(RedisSyncError):
    """配置无效时抛出。"""
    pass


class MigrationError(RedisSyncError):
    """迁移操作失败时抛出。"""
    pass


class ReplicationError(RedisSyncError):
    """复制操作失败时抛出。"""
    pass


class SyncError(RedisSyncError):
    """SYNC命令失败时抛出。"""
    pass


class ScanError(RedisSyncError):
    """SCAN操作失败时抛出。"""
    pass


class ReplConfError(RedisSyncError):
    """REPLCONF命令失败时抛出。"""
    pass


class VerificationError(RedisSyncError):
    """迁移验证失败时抛出。"""
    pass


class TimeoutError(RedisSyncError):
    """操作超时时抛出。"""
    pass


class DataIntegrityError(RedisSyncError):
    """数据完整性检查失败时抛出。"""
    pass
