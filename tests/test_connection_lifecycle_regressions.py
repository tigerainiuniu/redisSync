from unittest.mock import MagicMock

import pytest
import redis

import redis_sync.connection_manager as connection_manager_module
from redis_sync.connection_manager import RedisConnectionManager
from redis_sync.exceptions import RedisConnectionError


def _client_with_ping(result=None, error=None):
    client = MagicMock()
    if error is not None:
        client.ping.side_effect = error
    else:
        client.ping.return_value = result
    return client


def _retry_config(attempts):
    return {
        "max_attempts": attempts,
        "backoff_factor": 1,
        "max_delay": 0,
        "initial_delay": 0,
    }


def test_connect_retry_closes_every_failed_client(monkeypatch):
    clients = [
        _client_with_ping(error=redis.ConnectionError("down"))
        for _ in range(3)
    ]
    monkeypatch.setattr(
        connection_manager_module.redis,
        "Redis",
        MagicMock(side_effect=clients),
    )
    manager = RedisConnectionManager(_retry_config(3))

    with pytest.raises(RedisConnectionError):
        manager.connect_target(host="target", port=6379)

    for client in clients:
        client.close.assert_called_once_with()
        client.connection_pool.disconnect.assert_called_once_with()


def test_connect_retry_closes_failures_before_returning_success(monkeypatch):
    failed = [
        _client_with_ping(error=redis.TimeoutError("slow")),
        _client_with_ping(error=redis.ResponseError("NOAUTH")),
    ]
    connected = _client_with_ping(result=True)
    monkeypatch.setattr(
        connection_manager_module.redis,
        "Redis",
        MagicMock(side_effect=[*failed, connected]),
    )
    manager = RedisConnectionManager(_retry_config(3))

    assert manager.connect_target(host="target", port=6379) is connected

    for client in failed:
        client.close.assert_called_once_with()
        client.connection_pool.disconnect.assert_called_once_with()
    connected.close.assert_not_called()
    connected.connection_pool.disconnect.assert_not_called()


def test_connect_from_url_target_failure_cleans_both_owned_clients(monkeypatch):
    source = _client_with_ping(result=True)
    target = _client_with_ping(error=redis.ConnectionError("target down"))
    monkeypatch.setattr(
        connection_manager_module.redis,
        "from_url",
        MagicMock(side_effect=[source, target]),
    )
    manager = RedisConnectionManager()

    with pytest.raises(redis.ConnectionError, match="target down"):
        manager.connect_from_url("redis://source/0", "redis://target/0")

    for client in (source, target):
        client.close.assert_called_once_with()
        client.connection_pool.disconnect.assert_called_once_with()
    assert manager.source_client is None
    assert manager.target_client is None
    assert manager._owns_source_client is False
    assert manager._owns_target_client is False


def test_reconnect_preserves_shared_client_and_owns_replacement(monkeypatch):
    shared = _client_with_ping(error=redis.ConnectionError("stale"))
    replacement = _client_with_ping(result=True)
    manager = RedisConnectionManager(_retry_config(1))
    manager.set_source_client(
        shared,
        {"host": "source", "port": 6379},
        owned=False,
    )
    monkeypatch.setattr(manager, "_connect_with_retry", MagicMock(return_value=replacement))

    assert manager.reconnect_if_needed("source") is replacement

    shared.close.assert_not_called()
    shared.connection_pool.disconnect.assert_not_called()
    assert manager.source_client is replacement
    assert manager._owns_source_client is True
    manager.close()
    replacement.close.assert_called_once_with()
    replacement.connection_pool.disconnect.assert_called_once_with()


def test_reconnect_closes_owned_client_before_replacement(monkeypatch):
    old = _client_with_ping(error=redis.ConnectionError("stale"))
    replacement = _client_with_ping(result=True)
    manager = RedisConnectionManager(_retry_config(1))
    manager.set_target_client(
        old,
        {"host": "target", "port": 6379},
        owned=True,
    )
    monkeypatch.setattr(manager, "_connect_with_retry", MagicMock(return_value=replacement))

    assert manager.reconnect_if_needed("target") is replacement

    old.close.assert_called_once_with()
    old.connection_pool.disconnect.assert_called_once_with()
    assert manager.target_client is replacement
    assert manager._owns_target_client is True


def test_close_is_idempotent_and_detaches_owned_clients():
    source = _client_with_ping(result=True)
    target = _client_with_ping(result=True)
    manager = RedisConnectionManager()
    manager.set_source_client(source, owned=True)
    manager.set_target_client(target, owned=True)

    manager.close()
    manager.close()

    for client in (source, target):
        client.close.assert_called_once_with()
        client.connection_pool.disconnect.assert_called_once_with()
    assert manager.source_client is None
    assert manager.target_client is None
    assert manager._owns_source_client is False
    assert manager._owns_target_client is False
