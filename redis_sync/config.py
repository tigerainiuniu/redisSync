"""
Redis Sync工具的配置管理。

处理从文件、环境变量和命令行参数加载配置。
"""

import os
import yaml
import logging
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

logger = logging.getLogger(__name__)


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


@dataclass
class Config:
    """Main configuration class."""
    source: RedisConfig
    target: RedisConfig
    migration: MigrationSettings
    logging: LoggingConfig
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Config':
        """Create Config from dictionary."""
        return cls(
            source=RedisConfig(**data.get('source', {})),
            target=RedisConfig(**data.get('target', {})),
            migration=MigrationSettings(**data.get('migration', {})),
            logging=LoggingConfig(**data.get('logging', {}))
        )
    
    @classmethod
    def from_file(cls, config_path: str) -> 'Config':
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                data = yaml.safe_load(f)
            return cls.from_dict(data)
        except Exception as e:
            logger.error(f"Failed to load config from {config_path}: {e}")
            raise
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Load configuration from environment variables."""
        source_config = RedisConfig(
            host=os.getenv('REDIS_SOURCE_HOST', 'localhost'),
            port=int(os.getenv('REDIS_SOURCE_PORT', '6379')),
            password=os.getenv('REDIS_SOURCE_PASSWORD'),
            db=int(os.getenv('REDIS_SOURCE_DB', '0')),
            ssl=os.getenv('REDIS_SOURCE_SSL', 'false').lower() == 'true',
            url=os.getenv('REDIS_SOURCE_URL')
        )
        
        target_config = RedisConfig(
            host=os.getenv('REDIS_TARGET_HOST', 'localhost'),
            port=int(os.getenv('REDIS_TARGET_PORT', '6379')),
            password=os.getenv('REDIS_TARGET_PASSWORD'),
            db=int(os.getenv('REDIS_TARGET_DB', '0')),
            ssl=os.getenv('REDIS_TARGET_SSL', 'false').lower() == 'true',
            url=os.getenv('REDIS_TARGET_URL')
        )
        
        migration_config = MigrationSettings(
            strategy=os.getenv('MIGRATION_STRATEGY', 'scan'),
            batch_size=int(os.getenv('MIGRATION_BATCH_SIZE', '100')),
            scan_count=int(os.getenv('MIGRATION_SCAN_COUNT', '1000')),
            preserve_ttl=os.getenv('MIGRATION_PRESERVE_TTL', 'true').lower() == 'true',
            overwrite_existing=os.getenv('MIGRATION_OVERWRITE', 'false').lower() == 'true',
            key_pattern=os.getenv('MIGRATION_KEY_PATTERN', '*'),
            key_type=os.getenv('MIGRATION_KEY_TYPE'),
            enable_replication=os.getenv('MIGRATION_ENABLE_REPLICATION', 'false').lower() == 'true',
            verify_migration=os.getenv('MIGRATION_VERIFY', 'true').lower() == 'true'
        )
        
        logging_config = LoggingConfig(
            level=os.getenv('LOG_LEVEL', 'INFO'),
            file=os.getenv('LOG_FILE'),
            console=os.getenv('LOG_CONSOLE', 'true').lower() == 'true',
            colored=os.getenv('LOG_COLORED', 'true').lower() == 'true'
        )
        
        return cls(
            source=source_config,
            target=target_config,
            migration=migration_config,
            logging=logging_config
        )
    
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
        try:
            return Config.from_file(config_path)
        except Exception as e:
            logger.warning(f"Failed to load config from file: {e}")
    
    # Try environment variables
    if use_env:
        try:
            return Config.from_env()
        except Exception as e:
            logger.warning(f"Failed to load config from environment: {e}")
    
    # Create default configuration
    if create_default:
        logger.info("Using default configuration")
        return Config(
            source=RedisConfig(),
            target=RedisConfig(port=6380),
            migration=MigrationSettings(),
            logging=LoggingConfig()
        )
    
    raise RuntimeError("No valid configuration found")
