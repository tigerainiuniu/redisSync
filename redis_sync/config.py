"""
Redis Sync工具的配置管理。

处理从文件、环境变量和命令行参数加载配置。
"""

import ipaddress
import os
import socket
import yaml
import logging
import math
from typing import Dict, Any, Mapping, Optional
from dataclasses import dataclass, asdict
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from .exceptions import ConfigurationError
from .redis_protocol import MAX_REPLICATION_BUFFER_SIZE

logger = logging.getLogger(__name__)

MAX_SCAN_COUNT = 100_000


@dataclass
class RedisConfig:
    """Redis连接配置。"""
    host: str = "localhost"
    port: int = 6379
    password: Optional[str] = None
    db: int = 0
    ssl: bool = False
    ssl_cert_reqs: Optional[str] = None
    ssl_ca_certs: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_keyfile: Optional[str] = None
    url: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典，排除None值。"""
        return {k: v for k, v in asdict(self).items() if v is not None}


@dataclass
class MigrationSettings:
    """迁移操作设置。"""
    strategy: str = "scan"
    batch_size: int = 100
    scan_count: int = 1000
    preserve_ttl: bool = True
    overwrite_existing: bool = False
    key_pattern: str = "*"
    key_type: Optional[str] = None
    enable_replication: bool = False
    replication_port: int = 6380
    replication_timeout: int = 30
    verify_migration: bool = True
    max_retries: int = 3
    retry_delay: float = 1.0


@dataclass
class LoggingConfig:
    """Logging configuration."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Optional[str] = None
    console: bool = True
    colored: bool = True


_CONFIG_SECTIONS = frozenset({'source', 'target', 'migration', 'logging'})
_REDIS_FIELDS = frozenset(RedisConfig.__dataclass_fields__)
_MIGRATION_FIELDS = frozenset(MigrationSettings.__dataclass_fields__)
_LOGGING_FIELDS = frozenset(LoggingConfig.__dataclass_fields__)
_MIGRATION_STRATEGIES = frozenset({
    'scan', 'sync', 'dump_restore', 'full', 'incremental'
})
_REDIS_KEY_TYPES = frozenset({'string', 'list', 'set', 'zset', 'hash', 'stream'})
_LOG_LEVELS = frozenset({'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'})


def _format_unknown_fields(fields) -> str:
    return ', '.join(sorted((repr(field) for field in fields)))


def _validate_known_fields(
    section_name: str,
    values: Mapping[str, Any],
    allowed_fields,
) -> None:
    unknown = set(values) - allowed_fields
    if unknown:
        raise ConfigurationError(
            f"{section_name} 包含未知字段: {_format_unknown_fields(unknown)}"
        )


def _validate_section(
    data: Mapping[str, Any],
    section_name: str,
    allowed_fields,
) -> Dict[str, Any]:
    values = data.get(section_name, {})
    if not isinstance(values, Mapping):
        raise ConfigurationError(f"{section_name} 必须是映射")
    _validate_known_fields(section_name, values, allowed_fields)
    return dict(values)


def _validate_boolean(name: str, value: Any) -> bool:
    if not isinstance(value, bool):
        raise ConfigurationError(f"{name} 必须是布尔值")
    return value


def _validate_integer(
    name: str,
    value: Any,
    *,
    minimum: int,
    maximum: Optional[int] = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{name} 必须是整数")
    if value < minimum or (maximum is not None and value > maximum):
        if maximum is None:
            raise ConfigurationError(f"{name} 必须大于等于 {minimum}")
        raise ConfigurationError(f"{name} 必须在 {minimum} 到 {maximum} 之间")
    return value


def _validate_number(name: str, value: Any, *, minimum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{name} 必须是数字")
    if (isinstance(value, float) and not math.isfinite(value)) or value < minimum:
        raise ConfigurationError(f"{name} 必须大于等于 {minimum:g}")
    return value


def _validate_string(
    name: str,
    value: Any,
    *,
    allow_none: bool = False,
    nonempty: bool = False,
) -> Optional[str]:
    if value is None and allow_none:
        return None
    if not isinstance(value, str):
        expected = "字符串或 null" if allow_none else "字符串"
        raise ConfigurationError(f"{name} 必须是{expected}")
    if nonempty and not value.strip():
        raise ConfigurationError(f"{name} 必须是非空字符串")
    return value


def _validate_redis_url(name: str, value: Any) -> Optional[str]:
    url = _validate_string(name, value, allow_none=True, nonempty=True)
    if url is None:
        return None

    try:
        parsed = urlparse(url)
        port = parsed.port
    except ValueError as exc:
        raise ConfigurationError(f"{name} 不是有效的 Redis URL: {exc}") from exc

    if parsed.scheme not in {'redis', 'rediss', 'unix'}:
        raise ConfigurationError(
            f"{name} 必须使用 redis://、rediss:// 或 unix:// 协议"
        )
    if parsed.scheme == 'unix':
        if not parsed.path:
            raise ConfigurationError(f"{name} 的 Unix socket 路径不能为空")
    elif not parsed.hostname:
        raise ConfigurationError(f"{name} 的主机不能为空")
    if port is not None and not 1 <= port <= 65535:
        raise ConfigurationError(f"{name} 的端口必须在 1 到 65535 之间")

    query_db = parse_qs(parsed.query, keep_blank_values=True).get('db')
    db_value = query_db[-1] if query_db else None
    if db_value is None and parsed.scheme != 'unix' and parsed.path not in {'', '/'}:
        db_value = unquote(parsed.path.lstrip('/'))
    if db_value is not None:
        try:
            db = int(db_value)
        except (TypeError, ValueError):
            raise ConfigurationError(f"{name} 中的 db 必须是非负整数")
        if db < 0:
            raise ConfigurationError(f"{name} 中的 db 必须是非负整数")
    return url


def _validate_redis_section(section_name: str, values: Dict[str, Any]) -> None:
    defaults = RedisConfig()
    values['host'] = _validate_string(
        f'{section_name}.host', values.get('host', defaults.host), nonempty=True
    )
    values['port'] = _validate_integer(
        f'{section_name}.port', values.get('port', defaults.port),
        minimum=1, maximum=65535,
    )
    values['db'] = _validate_integer(
        f'{section_name}.db', values.get('db', defaults.db), minimum=0
    )
    values['ssl'] = _validate_boolean(
        f'{section_name}.ssl', values.get('ssl', defaults.ssl)
    )
    values['url'] = _validate_redis_url(
        f'{section_name}.url', values.get('url', defaults.url)
    )
    for field_name in (
        'password', 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
    ):
        if field_name in values:
            values[field_name] = _validate_string(
                f'{section_name}.{field_name}', values[field_name], allow_none=True
            )


def _validate_migration_section(values: Dict[str, Any]) -> None:
    defaults = MigrationSettings()
    strategy = _validate_string(
        'migration.strategy', values.get('strategy', defaults.strategy), nonempty=True
    )
    strategy = strategy.lower()
    if strategy not in _MIGRATION_STRATEGIES:
        valid = ', '.join(sorted(_MIGRATION_STRATEGIES))
        raise ConfigurationError(
            f"migration.strategy 必须是以下值之一: {valid}"
        )
    values['strategy'] = strategy

    for field_name in ('batch_size', 'replication_timeout', 'max_retries'):
        values[field_name] = _validate_integer(
            f'migration.{field_name}',
            values.get(field_name, getattr(defaults, field_name)),
            minimum=1,
        )
    values['scan_count'] = _validate_integer(
        'migration.scan_count',
        values.get('scan_count', defaults.scan_count),
        minimum=1,
        maximum=MAX_SCAN_COUNT,
    )
    values['replication_port'] = _validate_integer(
        'migration.replication_port',
        values.get('replication_port', defaults.replication_port),
        minimum=1,
        maximum=65535,
    )
    values['retry_delay'] = _validate_number(
        'migration.retry_delay',
        values.get('retry_delay', defaults.retry_delay),
        minimum=0,
    )

    for field_name in (
        'preserve_ttl', 'overwrite_existing', 'enable_replication', 'verify_migration'
    ):
        values[field_name] = _validate_boolean(
            f'migration.{field_name}',
            values.get(field_name, getattr(defaults, field_name)),
        )
    if values['enable_replication']:
        raise ConfigurationError(
            "migration.enable_replication 不适用于一次性配置；"
            "常驻复制请使用服务配置 sync.incremental_sync.method=psync"
        )

    values['key_pattern'] = _validate_string(
        'migration.key_pattern',
        values.get('key_pattern', defaults.key_pattern),
        nonempty=True,
    )
    key_type = _validate_string(
        'migration.key_type',
        values.get('key_type', defaults.key_type),
        allow_none=True,
        nonempty=True,
    )
    if key_type is not None:
        key_type = key_type.lower()
    if key_type is not None and key_type not in _REDIS_KEY_TYPES:
        valid = ', '.join(sorted(_REDIS_KEY_TYPES))
        raise ConfigurationError(f"migration.key_type 必须是 null 或以下值之一: {valid}")
    values['key_type'] = key_type


def _validate_logging_section(values: Dict[str, Any]) -> None:
    defaults = LoggingConfig()
    level = _validate_string(
        'logging.level', values.get('level', defaults.level), nonempty=True
    )
    normalized_level = level.upper()
    if normalized_level not in _LOG_LEVELS:
        raise ConfigurationError(f"logging.level 不是有效日志级别: {level!r}")
    values['level'] = normalized_level
    values['format'] = _validate_string(
        'logging.format', values.get('format', defaults.format)
    )
    values['file'] = _validate_string(
        'logging.file', values.get('file', defaults.file), allow_none=True
    )
    for field_name in ('console', 'colored'):
        values[field_name] = _validate_boolean(
            f'logging.{field_name}',
            values.get(field_name, getattr(defaults, field_name)),
        )


@dataclass
class Config:
    """Main configuration class."""
    source: RedisConfig
    target: RedisConfig
    migration: MigrationSettings
    logging: LoggingConfig
    
    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> 'Config':
        """Create Config from dictionary."""
        if not isinstance(data, Mapping):
            raise ConfigurationError("配置顶层必须是映射")
        _validate_known_fields('配置顶层', data, _CONFIG_SECTIONS)

        source = _validate_section(data, 'source', _REDIS_FIELDS)
        target = _validate_section(data, 'target', _REDIS_FIELDS)
        migration = _validate_section(data, 'migration', _MIGRATION_FIELDS)
        logging_config = _validate_section(data, 'logging', _LOGGING_FIELDS)
        _validate_redis_section('source', source)
        _validate_redis_section('target', target)
        _validate_migration_section(migration)
        _validate_logging_section(logging_config)

        return cls(
            source=RedisConfig(**source),
            target=RedisConfig(**target),
            migration=MigrationSettings(**migration),
            logging=LoggingConfig(**logging_config)
        )
    
    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            if not isinstance(data, Mapping):
                raise ConfigurationError(f"配置文件内容为空或格式错误: {config_path}")

            service_sections = {'targets', 'sync', 'service'}.intersection(data)
            if service_sections:
                raise ConfigurationError(
                    "检测到常驻服务配置格式（source/targets/sync/service）。"
                    "请通过 python -m redis_sync --config <path> 启动常驻服务；"
                    "redis-sync CLI 请使用 redis-sync init 生成单目标配置。"
                )
            return cls.from_dict(data)
        except ConfigurationError:
            raise
        except Exception as e:
            raise ConfigurationError(f"加载配置文件失败 {config_path}: {e}") from e
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        def _safe_int(env_name: str, default: str) -> int:
            raw = os.getenv(env_name, default)
            try:
                return int(raw)
            except (TypeError, ValueError):
                raise ConfigurationError(
                    f"环境变量 {env_name} 的值 '{raw}' 不是有效整数"
                )

        def _safe_bool(env_name: str, default: str) -> bool:
            raw = os.getenv(env_name, default)
            normalized = str(raw).strip().lower()
            if normalized in {'true', '1', 'yes', 'on'}:
                return True
            if normalized in {'false', '0', 'no', 'off'}:
                return False
            raise ConfigurationError(
                f"环境变量 {env_name} 的值 '{raw}' 不是有效布尔值"
            )

        source_config = RedisConfig(
            host=os.getenv('REDIS_SOURCE_HOST', 'localhost'),
            port=_safe_int('REDIS_SOURCE_PORT', '6379'),
            password=os.getenv('REDIS_SOURCE_PASSWORD'),
            db=_safe_int('REDIS_SOURCE_DB', '0'),
            ssl=_safe_bool('REDIS_SOURCE_SSL', 'false'),
            url=os.getenv('REDIS_SOURCE_URL')
        )
        
        target_config = RedisConfig(
            host=os.getenv('REDIS_TARGET_HOST', 'localhost'),
            port=_safe_int('REDIS_TARGET_PORT', '6379'),
            password=os.getenv('REDIS_TARGET_PASSWORD'),
            db=_safe_int('REDIS_TARGET_DB', '0'),
            ssl=_safe_bool('REDIS_TARGET_SSL', 'false'),
            url=os.getenv('REDIS_TARGET_URL')
        )
        
        migration_config = MigrationSettings(
            strategy=os.getenv('MIGRATION_STRATEGY', 'scan'),
            batch_size=_safe_int('MIGRATION_BATCH_SIZE', '100'),
            scan_count=_safe_int('MIGRATION_SCAN_COUNT', '1000'),
            preserve_ttl=_safe_bool('MIGRATION_PRESERVE_TTL', 'true'),
            overwrite_existing=_safe_bool('MIGRATION_OVERWRITE', 'false'),
            key_pattern=os.getenv('MIGRATION_KEY_PATTERN', '*'),
            key_type=os.getenv('MIGRATION_KEY_TYPE'),
            enable_replication=_safe_bool('MIGRATION_ENABLE_REPLICATION', 'false'),
            verify_migration=_safe_bool('MIGRATION_VERIFY', 'true')
        )
        
        logging_config = LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            file=os.getenv('LOG_FILE'),
            console=_safe_bool('LOG_CONSOLE', 'true'),
            colored=_safe_bool('LOG_COLORED', 'true')
        )
        
        return cls.from_dict({
            'source': asdict(source_config),
            'target': asdict(target_config),
            'migration': asdict(migration_config),
            'logging': asdict(logging_config),
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'source': asdict(self.source),
            'target': asdict(self.target),
            'migration': asdict(self.migration),
            'logging': asdict(self.logging)
        }
    
    def save_to_file(self, config_path: str):
        """Save configuration to YAML file."""
        try:
            config_dir = Path(config_path).parent
            config_dir.mkdir(parents=True, exist_ok=True)
            
            with open(config_path, 'w') as f:
                yaml.dump(self.to_dict(), f, default_flow_style=False, indent=2)
            
            logger.info(f"Configuration saved to {config_path}")
        except Exception as e:
            logger.error(f"Failed to save config to {config_path}: {e}")
            raise


def setup_logging(config: LoggingConfig):
    """Setup logging based on configuration."""
    try:
        import colorlog
        colorlog_available = True
    except ImportError:
        colorlog_available = False

    # Set logging level
    level = getattr(logging, config.level.upper(), logging.INFO)

    # Create formatters
    if config.colored and config.console and colorlog_available:
        console_formatter = colorlog.ColoredFormatter(
            '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            log_colors={
                'DEBUG': 'cyan',
                'INFO': 'green',
                'WARNING': 'yellow',
                'ERROR': 'red',
                'CRITICAL': 'red,bg_white',
            }
        )
    else:
        console_formatter = logging.Formatter(config.format)

    file_formatter = logging.Formatter(config.format)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Add console handler
    if config.console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

    # Add file handler
    if config.file:
        try:
            log_dir = Path(config.file).parent
            log_dir.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(config.file)
            file_handler.setLevel(level)
            file_handler.setFormatter(file_formatter)
            root_logger.addHandler(file_handler)
        except Exception as e:
            logger.error(f"Failed to setup file logging: {e}")

    # Set specific logger levels
    logging.getLogger('redis').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def create_sample_config() -> str:
    """Create a sample configuration file content."""
    sample_config = {
        'source': {
            'host': 'localhost',
            'port': 6379,
            'password': None,
            'db': 0,
            'ssl': False
        },
        'target': {
            'host': 'localhost',
            'port': 6380,
            'password': None,
            'db': 0,
            'ssl': False
        },
        'migration': {
            'strategy': 'scan',
            'batch_size': 100,
            'scan_count': 1000,
            'preserve_ttl': True,
            'overwrite_existing': False,
            'key_pattern': '*',
            'key_type': None,
            'enable_replication': False,
            'replication_port': 6380,
            'replication_timeout': 30,
            'verify_migration': True,
            'max_retries': 3,
            'retry_delay': 1.0
        },
        'logging': {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'file': None,
            'console': True,
            'colored': True
        }
    }
    
    return yaml.dump(sample_config, default_flow_style=False, indent=2)


def load_config(config_path: Optional[str] = None, 
               use_env: bool = True,
               create_default: bool = False) -> Config:
    """
    Load configuration from various sources.
    
    Args:
        config_path: Path to configuration file
        use_env: Whether to use environment variables as fallback
        create_default: Whether to create default config if none found
        
    Returns:
        Configuration object
    """
    # Try to load from file first
    if config_path and os.path.exists(config_path):
        return Config.from_file(config_path)
    
    # Try environment variables
    if use_env:
        return Config.from_env()
    
    # Create default configuration
    if create_default:
        logger.info("Using default configuration")
        return Config(
            source=RedisConfig(),
            target=RedisConfig(port=6380),
            migration=MigrationSettings(),
            logging=LoggingConfig()
        )
    
    raise ConfigurationError("No valid configuration found")


def load_and_validate_service_config(config_path: str) -> Dict[str, Any]:
    """
    加载 redis-sync 服务 YAML（一对多）并校验必选结构。

    与 CLI 使用的 Config dataclass 并存：服务进程统一经此入口加载，避免散落解析逻辑。
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except Exception as e:
        raise ConfigurationError(f"无法读取配置文件 {config_path}: {e}") from e

    if not isinstance(data, dict):
        raise ConfigurationError("配置文件顶层必须是 YAML 映射")

    for section in ("source", "targets", "sync", "service"):
        if section not in data:
            raise ConfigurationError(f"缺少必选配置段: {section}")

    targets = data["targets"]
    if not isinstance(targets, list) or len(targets) == 0:
        raise ConfigurationError("targets 必须是非空列表")

    def _mapping(section_name: str, value: Any) -> Dict[str, Any]:
        if not isinstance(value, dict):
            raise ConfigurationError(f"{section_name} 必须是 YAML 映射")
        return value

    def _known_fields(
        section_name: str, section: Dict[str, Any], allowed_fields
    ) -> None:
        unknown = set(section) - set(allowed_fields)
        if unknown:
            rendered = ", ".join(sorted(repr(field) for field in unknown))
            raise ConfigurationError(
                f"{section_name} 包含未知字段: {rendered}"
            )

    _known_fields(
        "配置顶层",
        data,
        {
            "source", "targets", "sync", "service", "notifications",
            "security", "web_ui",
        },
    )

    def _integer(name: str, value: Any, minimum: int, maximum: int) -> int:
        if isinstance(value, bool):
            raise ConfigurationError(f"{name} 必须是整数")
        if isinstance(value, int):
            parsed = value
        elif isinstance(value, float):
            if not math.isfinite(value) or not value.is_integer():
                raise ConfigurationError(f"{name} 必须是整数")
            parsed = int(value)
        elif isinstance(value, str):
            stripped = value.strip()
            digits = stripped[1:] if stripped[:1] in {"+", "-"} else stripped
            if not digits or any(character not in "0123456789" for character in digits):
                raise ConfigurationError(f"{name} 必须是整数")
            parsed = int(stripped, 10)
        else:
            raise ConfigurationError(f"{name} 必须是整数")
        if not minimum <= parsed <= maximum:
            raise ConfigurationError(
                f"{name} 必须在 {minimum} 到 {maximum} 之间"
            )
        return parsed

    def _number(
        name: str, value: Any, minimum: float, maximum: float
    ) -> float:
        if isinstance(value, bool):
            raise ConfigurationError(f"{name} 必须是数字")
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            raise ConfigurationError(f"{name} 必须是数字")
        if not minimum <= parsed <= maximum:
            raise ConfigurationError(
                f"{name} 必须在 {minimum} 到 {maximum} 之间"
            )
        return parsed

    def _boolean(name: str, value: Any) -> bool:
        if not isinstance(value, bool):
            raise ConfigurationError(f"{name} 必须是布尔值")
        return value

    def _nullable_string(name: str, value: Any) -> Optional[str]:
        if value is not None and not isinstance(value, str):
            raise ConfigurationError(f"{name} 必须是字符串或 null")
        return value

    def _string_list(name: str, value: Any, *, allow_none: bool = True):
        if value is None and allow_none:
            return None
        if not isinstance(value, list) or any(
            not isinstance(item, str) or not item for item in value
        ):
            suffix = "null 或字符串列表" if allow_none else "字符串列表"
            raise ConfigurationError(f"{name} 必须是{suffix}")
        return value

    def _key_scope(section_name: str, section: Dict[str, Any]) -> None:
        pattern = section.get("key_pattern", "*")
        if not isinstance(pattern, str) or not pattern:
            raise ConfigurationError(f"{section_name}.key_pattern 必须是非空字符串")
        section["key_pattern"] = pattern
        key_types = _string_list(
            f"{section_name}.key_types", section.get("key_types")
        )
        if key_types is None:
            section["key_types"] = None
            return
        allowed_key_types = {"string", "list", "set", "zset", "hash", "stream"}
        normalized = [item.strip().lower() for item in key_types]
        invalid = sorted(set(normalized) - allowed_key_types)
        if invalid:
            raise ConfigurationError(
                f"{section_name}.key_types 包含不支持的 Redis 类型: "
                f"{', '.join(invalid)}"
            )
        section["key_types"] = normalized

    def _connection_options(section_name: str, section: Dict[str, Any]) -> None:
        for field in (
            "username", "password", "client_name", "ssl_cert_reqs",
            "ssl_ca_certs", "ssl_certfile", "ssl_keyfile",
        ):
            if field in section:
                section[field] = _nullable_string(
                    f"{section_name}.{field}", section[field]
                )
        for field in ("socket_timeout", "socket_connect_timeout"):
            if field in section:
                section[field] = _number(
                    f"{section_name}.{field}", section[field], 0.001, 10**9
                )
        if "health_check_interval" in section:
            section["health_check_interval"] = _integer(
                f"{section_name}.health_check_interval",
                section["health_check_interval"],
                0,
                10**9,
            )
        if "socket_keepalive" in section:
            section["socket_keepalive"] = _boolean(
                f"{section_name}.socket_keepalive", section["socket_keepalive"]
            )

    address_cache: Dict[str, set] = {}

    def _host_addresses(host: Any) -> set:
        normalized = str(host).strip().lower().rstrip(".")
        if normalized in address_cache:
            return address_cache[normalized]
        addresses = {normalized}
        if normalized.endswith((".example", ".invalid", ".test")):
            address_cache[normalized] = addresses
            return addresses
        try:
            for info in socket.getaddrinfo(
                normalized, None, type=socket.SOCK_STREAM
            ):
                addresses.add(str(info[4][0]).split("%", 1)[0].lower())
        except (OSError, UnicodeError):
            pass
        address_cache[normalized] = addresses
        return addresses

    def _same_redis_endpoint(left, right) -> bool:
        left_host, left_port, left_db = left
        right_host, right_port, right_db = right
        if left_port != right_port or left_db != right_db:
            return False
        return bool(_host_addresses(left_host) & _host_addresses(right_host))

    source = _mapping("source", data["source"])
    connection_fields = {
        "host", "port", "username", "password", "db", "ssl",
        "ssl_cert_reqs", "ssl_ca_certs", "ssl_certfile", "ssl_keyfile",
        "socket_timeout", "socket_connect_timeout", "socket_keepalive",
        "health_check_interval", "client_name",
        "connection_pool_max_connections",
    }
    _known_fields("source", source, connection_fields)
    if not isinstance(source.get("host"), str) or not source["host"].strip():
        raise ConfigurationError("source.host 必须是非空字符串")
    source["port"] = _integer("source.port", source.get("port", 6379), 1, 65535)
    source["db"] = _integer("source.db", source.get("db", 0), 0, 2**31 - 1)
    source["ssl"] = _boolean("source.ssl", source.get("ssl", False))
    _connection_options("source", source)
    source_endpoint = (
        str(source["host"]).strip().lower(),
        source["port"],
        source["db"],
    )

    enabled_count = 0
    target_names = set()
    target_endpoints = []
    for index, target in enumerate(targets):
        target = _mapping(f"targets[{index}]", target)
        _known_fields(
            f"targets[{index}]", target, connection_fields | {"name", "enabled"}
        )
        name = target.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ConfigurationError(f"targets[{index}].name 不能为空")
        if name in target_names:
            raise ConfigurationError(f"目标名称重复: {name}")
        target_names.add(name)
        target["port"] = _integer(
            f"targets[{index}].port", target.get("port", 6379), 1, 65535
        )
        target["db"] = _integer(
            f"targets[{index}].db", target.get("db", 0), 0, 2**31 - 1
        )
        target["ssl"] = _boolean(
            f"targets[{index}].ssl", target.get("ssl", False)
        )
        _connection_options(f"targets[{index}]", target)
        configured_target_pool = target.get("connection_pool_max_connections")
        if configured_target_pool is not None:
            target["connection_pool_max_connections"] = _integer(
                f"targets[{index}].connection_pool_max_connections",
                configured_target_pool,
                1,
                2**31 - 1,
            )
        target["enabled"] = _boolean(
            f"targets[{index}].enabled", target.get("enabled", True)
        )
        if "host" in target and (
            not isinstance(target["host"], str) or not target["host"].strip()
        ):
            raise ConfigurationError(f"targets[{index}].host 必须是非空字符串")
        if target["enabled"]:
            enabled_count += 1
            if not target.get("host"):
                raise ConfigurationError(f"targets[{index}].host 不能为空")
            endpoint = (
                str(target["host"]).strip().lower(),
                target["port"],
                target["db"],
            )
            if _same_redis_endpoint(endpoint, source_endpoint):
                raise ConfigurationError(
                    f"targets[{index}] 与 source 指向同一 Redis 数据库，"
                    "同步会造成源数据清理或复制自循环"
                )
            if any(
                _same_redis_endpoint(endpoint, existing)
                for existing in target_endpoints
            ):
                raise ConfigurationError(
                    f"targets[{index}] 与另一个 enabled 目标指向同一 Redis 数据库"
                )
            target_endpoints.append(endpoint)
    if enabled_count == 0:
        raise ConfigurationError("至少需要一个 enabled=true 的目标")

    sync = _mapping("sync", data["sync"])
    _known_fields(
        "sync", sync, {"mode", "full_sync", "incremental_sync", "filters"}
    )
    mode = str(sync.get("mode", "")).lower()
    if mode not in {"full", "incremental", "hybrid"}:
        raise ConfigurationError("sync.mode 必须是 full、incremental 或 hybrid")
    sync["mode"] = mode
    full_sync = sync.setdefault("full_sync", {})
    full_sync = _mapping("sync.full_sync", full_sync)
    _known_fields(
        "sync.full_sync",
        full_sync,
        {
            "strategy", "batch_size", "scan_count", "preserve_ttl",
            "clear_target", "overwrite_existing", "verify_migration",
            "verify_mode", "verify_sample_size", "key_pattern", "key_types",
        },
    )
    full_strategy = str(full_sync.get("strategy", "scan")).lower()
    if full_strategy not in {"scan", "sync", "dump_restore"}:
        raise ConfigurationError(
            "sync.full_sync.strategy 必须是 scan、sync 或 dump_restore"
        )
    full_sync["strategy"] = full_strategy
    _key_scope("sync.full_sync", full_sync)
    for field, default, maximum in (
        ("batch_size", 1000, 10**9),
        ("scan_count", 10000, MAX_SCAN_COUNT),
    ):
        full_sync[field] = _integer(
            f"sync.full_sync.{field}", full_sync.get(field, default), 1, maximum
        )
    for field, default in (
        ("preserve_ttl", True),
        ("clear_target", False),
        ("overwrite_existing", True),
        ("verify_migration", True),
    ):
        full_sync[field] = _boolean(
            f"sync.full_sync.{field}", full_sync.get(field, default)
        )
    verify_mode = str(full_sync.get("verify_mode", "full")).lower()
    if verify_mode not in {"fast", "full"}:
        raise ConfigurationError(
            "sync.full_sync.verify_mode 必须是 fast 或 full"
        )
    full_sync["verify_mode"] = verify_mode
    full_sync["verify_sample_size"] = _integer(
        "sync.full_sync.verify_sample_size",
        full_sync.get("verify_sample_size", 100),
        1,
        10**9,
    )
    incremental = sync.setdefault("incremental_sync", {})
    incremental = _mapping("sync.incremental_sync", incremental)
    _known_fields(
        "sync.incremental_sync",
        incremental,
        {
            "enabled", "method", "change_detection_method", "apply_mode",
            "interval", "max_changes_per_sync", "key_pattern", "key_types",
            "command_dedup_window", "target_command_timeout", "buffer_size",
            "target_connection_idle_timeout", "capture_max_size", "listening_port",
            "ack_interval", "skip_commands", "include_commands", "filters",
        },
    )
    method = str(
        incremental.get(
            "method", incremental.get("change_detection_method", "scan")
        )
    ).lower()
    if method == "idle_time":
        logger.warning(
            "sync.incremental_sync.method=idle_time 已废弃，按 scan 处理"
        )
        method = "scan"
    if method == "sync":
        logger.warning(
            "sync.incremental_sync.method=sync 使用 PSYNC 全量握手以获取精确 offset"
        )
        method = "psync"
    if method not in {"scan", "sync", "psync"}:
        raise ConfigurationError(
            "sync.incremental_sync.method 必须是 scan、sync 或 psync"
        )
    incremental["method"] = method
    incremental["enabled"] = _boolean(
        "sync.incremental_sync.enabled", incremental.get("enabled", True)
    )
    if mode in {"incremental", "hybrid"} and not incremental["enabled"]:
        raise ConfigurationError(
            f"sync.mode={mode} 时 sync.incremental_sync.enabled 必须为 true"
        )
    apply_mode = str(incremental.get("apply_mode", "key_state")).lower()
    if apply_mode not in {"direct", "key_state"}:
        raise ConfigurationError(
            "sync.incremental_sync.apply_mode 必须是 direct 或 key_state"
        )
    incremental["apply_mode"] = apply_mode
    if apply_mode == "direct":
        raise ConfigurationError(
            "sync.incremental_sync.apply_mode=direct 不能保证单 Redis DB 的"
            "复制边界；常驻服务必须使用 key_state"
        )
    _key_scope("sync.incremental_sync", incremental)
    incremental["interval"] = _integer(
        "sync.incremental_sync.interval",
        incremental.get("interval", 30),
        1,
        10**9,
    )
    incremental["max_changes_per_sync"] = _integer(
        "sync.incremental_sync.max_changes_per_sync",
        incremental.get("max_changes_per_sync", 10000),
        1,
        10**9,
    )
    try:
        dedup_window = float(incremental.get("command_dedup_window", 0) or 0)
    except (TypeError, ValueError):
        raise ConfigurationError(
            "sync.incremental_sync.command_dedup_window 必须为 0"
        )
    if dedup_window != 0:
        raise ConfigurationError(
            "sync.incremental_sync.command_dedup_window 必须为 0；"
            "复制流中的合法重复命令不能按时间窗口去重"
        )
    incremental["command_dedup_window"] = 0
    if "filters" in incremental:
        raise ConfigurationError(
            "sync.incremental_sync.filters 不受支持；键和命令过滤请统一配置在 "
            "sync.filters"
        )
    incremental["target_command_timeout"] = _number(
        "sync.incremental_sync.target_command_timeout",
        incremental.get("target_command_timeout", 5),
        0.001,
        10**9,
    )
    incremental["target_connection_idle_timeout"] = _number(
        "sync.incremental_sync.target_connection_idle_timeout",
        incremental.get("target_connection_idle_timeout", 60),
        0.001,
        10**9,
    )
    incremental["buffer_size"] = _integer(
        "sync.incremental_sync.buffer_size",
        incremental.get("buffer_size", 8192),
        1,
        MAX_REPLICATION_BUFFER_SIZE,
    )
    incremental["capture_max_size"] = _integer(
        "sync.incremental_sync.capture_max_size",
        incremental.get("capture_max_size", 1024 * 1024 * 1024),
        1,
        2**63 - 1,
    )
    incremental["listening_port"] = _integer(
        "sync.incremental_sync.listening_port",
        incremental.get("listening_port", 6380),
        1,
        65535,
    )
    incremental["ack_interval"] = _number(
        "sync.incremental_sync.ack_interval",
        incremental.get("ack_interval", 1),
        0.01,
        10**9,
    )
    for field in ("skip_commands", "include_commands"):
        if field in incremental:
            incremental[field] = _string_list(
                f"sync.incremental_sync.{field}",
                incremental[field],
                allow_none=False,
            )
    if mode == "hybrid" and (
        incremental["key_pattern"] != full_sync["key_pattern"]
        or incremental["key_types"] != full_sync["key_types"]
    ):
        raise ConfigurationError(
            "sync.mode=hybrid 时 full_sync 与 incremental_sync 的 "
            "key_pattern/key_types 必须一致"
        )

    if mode in {"incremental", "hybrid"} and method in {"sync", "psync"}:
        for index, target in enumerate(targets):
            if not target["enabled"]:
                continue
            target_pool_size = target.get("connection_pool_max_connections")
            if target_pool_size is not None and target_pool_size < 2:
                raise ConfigurationError(
                    f"targets[{index}].connection_pool_max_connections 至少需要为 2 "
                    "（实时写连接与目标扫描/恢复各需一条）"
                )

    configured_pool_size = source.get("connection_pool_max_connections")
    if configured_pool_size is not None:
        required_connections = enabled_count
        if mode in {"incremental", "hybrid"} and method in {"sync", "psync"}:
            required_connections += 1
        pool_size = _integer(
            "source.connection_pool_max_connections",
            configured_pool_size,
            1,
            2**31 - 1,
        )
        if pool_size < required_connections:
            raise ConfigurationError(
                "source.connection_pool_max_connections 至少需要为 "
                f"{required_connections}（当前启用目标与复制流所需连接数）"
            )
        source["connection_pool_max_connections"] = pool_size
    sync.setdefault("filters", {})
    filters = _mapping("sync.filters", sync["filters"])
    _known_fields(
        "sync.filters",
        filters,
        {
            "include_patterns", "exclude_patterns", "min_ttl",
            "max_key_size", "command_include", "command_exclude",
            "include_commands", "skip_commands",
        },
    )
    for field in ("include_patterns", "exclude_patterns"):
        filters[field] = _string_list(
            f"sync.filters.{field}", filters.get(field)
        )
    for field in (
        "command_include",
        "command_exclude",
        "include_commands",
        "skip_commands",
    ):
        if field in filters:
            filters[field] = _string_list(
                f"sync.filters.{field}", filters[field], allow_none=False
            )
    filters["min_ttl"] = _integer(
        "sync.filters.min_ttl", filters.get("min_ttl", 0), 0, 10**9
    )
    filters["max_key_size"] = _integer(
        "sync.filters.max_key_size",
        filters.get("max_key_size", 0),
        0,
        2**63 - 1,
    )

    service = _mapping("service", data["service"])
    _known_fields(
        "service",
        service,
        {"name", "logging", "monitoring", "performance", "retry", "failover"},
    )
    logging_config = service.setdefault("logging", {})
    logging_config = _mapping("service.logging", logging_config)
    _known_fields(
        "service.logging",
        logging_config,
        {"level", "file", "max_size", "backup_count", "format"},
    )
    logging_config.setdefault("level", "INFO")
    logging_config.setdefault("file", "redis-sync.log")
    logging_config.setdefault("max_size", 104857600)
    logging_config.setdefault("backup_count", 5)
    logging_config.setdefault(
        "format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    if str(logging_config["level"]).upper() not in {
        "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"
    }:
        raise ConfigurationError("service.logging.level 不是有效日志级别")
    logging_config["level"] = str(logging_config["level"]).upper()
    if not isinstance(logging_config["file"], str) or not logging_config["file"]:
        raise ConfigurationError("service.logging.file 必须是非空字符串")
    if not isinstance(logging_config["format"], str) or not logging_config["format"]:
        raise ConfigurationError("service.logging.format 必须是非空字符串")
    logging_config["max_size"] = _integer(
        "service.logging.max_size", logging_config["max_size"], 1, 2**63 - 1
    )
    logging_config["backup_count"] = _integer(
        "service.logging.backup_count", logging_config["backup_count"], 0, 10**9
    )

    performance = service.setdefault("performance", {})
    performance = _mapping("service.performance", performance)
    _known_fields(
        "service.performance",
        performance,
        {
            "max_workers", "scan_count", "queue_size", "memory_limit",
            "pipeline_batch_size",
        },
    )
    performance.setdefault("max_workers", 8)
    performance.setdefault("scan_count", 10000)
    performance.setdefault("pipeline_batch_size", 100)
    performance["max_workers"] = _integer(
        "service.performance.max_workers",
        performance["max_workers"],
        1,
        1024,
    )
    performance["scan_count"] = _integer(
        "service.performance.scan_count",
        performance["scan_count"],
        1,
        MAX_SCAN_COUNT,
    )
    performance["pipeline_batch_size"] = _integer(
        "service.performance.pipeline_batch_size",
        performance["pipeline_batch_size"],
        1,
        200,
    )
    for field in ("queue_size", "memory_limit"):
        if field in performance:
            performance[field] = _integer(
                f"service.performance.{field}",
                performance[field],
                1,
                2**63 - 1,
            )

    monitoring = service.setdefault("monitoring", {})
    monitoring = _mapping("service.monitoring", monitoring)
    _known_fields(
        "service.monitoring",
        monitoring,
        {"enabled", "metrics_interval", "health_check_interval"},
    )
    monitoring["enabled"] = _boolean(
        "service.monitoring.enabled", monitoring.get("enabled", False)
    )
    for field, default in (
        ("metrics_interval", 60),
        ("health_check_interval", 30),
    ):
        monitoring[field] = _integer(
            f"service.monitoring.{field}", monitoring.get(field, default), 1, 10**9
        )

    retry = service.setdefault("retry", {})
    retry = _mapping("service.retry", retry)
    _known_fields(
        "service.retry",
        retry,
        {"max_attempts", "backoff_factor", "max_delay", "initial_delay"},
    )
    retry.setdefault("max_attempts", 5)
    retry.setdefault("backoff_factor", 2)
    retry.setdefault("max_delay", 60)
    retry.setdefault("initial_delay", 1)
    retry["max_attempts"] = _integer(
        "service.retry.max_attempts", retry["max_attempts"], 1, 10**9
    )
    for field in ("backoff_factor", "max_delay", "initial_delay"):
        retry[field] = _number(
            f"service.retry.{field}", retry[field], 0, 10**9
        )

    failover = service.setdefault("failover", {})
    failover = _mapping("service.failover", failover)
    _known_fields(
        "service.failover",
        failover,
        {"enabled", "max_failures", "failure_window", "recovery_delay"},
    )
    failover["enabled"] = _boolean(
        "service.failover.enabled", failover.get("enabled", True)
    )
    failover.setdefault("max_failures", 5)
    failover.setdefault("recovery_delay", 60)
    failover["max_failures"] = _integer(
        "service.failover.max_failures", failover["max_failures"], 1, 10**9
    )
    failover["recovery_delay"] = _integer(
        "service.failover.recovery_delay", failover["recovery_delay"], 0, 10**9
    )
    failover["failure_window"] = _integer(
        "service.failover.failure_window",
        failover.get("failure_window", 300),
        1,
        10**9,
    )

    web_ui = data.setdefault("web_ui", {})
    web_ui = _mapping("web_ui", web_ui)
    _known_fields("web_ui", web_ui, {"enabled", "host", "port", "debug"})
    web_ui["enabled"] = _boolean("web_ui.enabled", web_ui.get("enabled", True))
    web_ui["debug"] = _boolean("web_ui.debug", web_ui.get("debug", False))
    host = web_ui.get("host", "127.0.0.1")
    if not isinstance(host, str) or not host.strip():
        raise ConfigurationError("web_ui.host 必须是非空字符串")
    web_ui["host"] = host
    web_ui["port"] = _integer(
        "web_ui.port", web_ui.get("port", 8080), 1, 65535
    )

    security = data.setdefault("security", {})
    security = _mapping("security", security)
    _known_fields(
        "security", security, {"auth_enabled", "api_key", "allowed_ips", "encryption"}
    )
    security["auth_enabled"] = _boolean(
        "security.auth_enabled", security.get("auth_enabled", False)
    )
    if "allowed_ips" in security:
        security["allowed_ips"] = _string_list(
            "security.allowed_ips", security["allowed_ips"], allow_none=False
        )
    api_key = security.get("api_key")
    if api_key is not None and not isinstance(api_key, str):
        raise ConfigurationError("security.api_key 必须是字符串或 null")
    if security["auth_enabled"] and (not api_key or not api_key.strip()):
        raise ConfigurationError(
            "security.auth_enabled=true 时 security.api_key 必须是非空字符串"
        )

    normalized_host = host.strip().lower().rstrip(".")
    try:
        bind_address = ipaddress.ip_address(normalized_host.split("%", 1)[0])
        loopback_bind = bind_address.is_loopback
    except ValueError:
        loopback_bind = normalized_host == "localhost"

    if web_ui["enabled"] and not loopback_bind and not security["auth_enabled"]:
        allowed_ips = security.get("allowed_ips")
        if not allowed_ips:
            raise ConfigurationError(
                "Web UI 监听非本机地址时必须启用 API key 认证，"
                "或将 security.allowed_ips 限定为非空 loopback 网段列表"
            )
        for allowed_ip in allowed_ips:
            allowed_text = allowed_ip.strip()
            try:
                network = ipaddress.ip_network(allowed_text, strict=False)
            except ValueError as exc:
                raise ConfigurationError(
                    "匿名外部 Web UI 的 security.allowed_ips 只能包含有效的 "
                    "loopback IP/CIDR"
                ) from exc
            if allowed_text == "*" or not network.is_loopback:
                raise ConfigurationError(
                    "匿名外部 Web UI 的 security.allowed_ips 只能包含 "
                    "loopback IP/CIDR"
                )

    encryption = security.setdefault("encryption", {})
    encryption = _mapping("security.encryption", encryption)
    _known_fields("security.encryption", encryption, {"enabled", "key"})
    encryption["enabled"] = _boolean(
        "security.encryption.enabled", encryption.get("enabled", False)
    )
    encryption_key = encryption.get("key")
    if encryption_key is not None and not isinstance(encryption_key, str):
        raise ConfigurationError("security.encryption.key 必须是字符串或 null")

    return data
