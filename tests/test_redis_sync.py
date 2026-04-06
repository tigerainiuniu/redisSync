from redis_sync.exceptions import RedisConnectionError, RedisTimeoutError, RedisSyncError
from redis_sync.redis_protocol import parse_resp_array_command
from redis_sync.utils import validate_redis_key
from redis_sync.config import load_and_validate_service_config
import tempfile
import os
import yaml


def test_custom_exceptions_not_builtin():
    assert issubclass(RedisConnectionError, RedisSyncError)
    assert RedisConnectionError is not ConnectionError
    assert issubclass(RedisTimeoutError, RedisSyncError)
    assert RedisTimeoutError is not TimeoutError


def test_parse_resp_array_command():
    raw = b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n"
    cmd, rest = parse_resp_array_command(raw)
    assert cmd == [b"GET", b"foo"]
    assert rest == b""

    partial = b"*2\r\n$3\r\nGET\r\n"
    cmd2, rest2 = parse_resp_array_command(partial)
    assert cmd2 is None
    assert rest2 == partial


def test_validate_redis_key_allows_binary_like_string():
    assert validate_redis_key("a\x00b") is True
    assert validate_redis_key("") is False


def test_load_and_validate_service_config_minimal():
    data = {
        "source": {"host": "127.0.0.1", "port": 6379},
        "targets": [{"name": "t1", "host": "127.0.0.1", "port": 6380}],
        "sync": {"mode": "hybrid"},
        "service": {"logging": {"level": "INFO", "file": "x.log", "max_size": 1, "backup_count": 1, "format": "%(message)s"}},
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yaml.safe_dump(data, f)
        path = f.name
    try:
        loaded = load_and_validate_service_config(path)
        assert loaded["source"]["host"] == "127.0.0.1"
        assert len(loaded["targets"]) == 1
    finally:
        os.unlink(path)
