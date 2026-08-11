import threading
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


def _start_call(call):
    outcome = []

    def run():
        try:
            outcome.append(("result", call()))
        except BaseException as exc:
            outcome.append(("error", exc))

    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


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


def test_owned_setters_close_replaced_clients_immediately():
    old_source = _client_with_ping(result=True)
    old_target = _client_with_ping(result=True)
    new_source = _client_with_ping(result=True)
    new_target = _client_with_ping(result=True)
    manager = RedisConnectionManager()
    manager.set_source_client(old_source, owned=True)
    manager.set_target_client(old_target, owned=True)

    manager.set_source_client(new_source, owned=True)
    manager.set_target_client(new_target, owned=True)

    old_source.close.assert_called_once_with()
    old_source.connection_pool.disconnect.assert_called_once_with()
    old_target.close.assert_called_once_with()
    old_target.connection_pool.disconnect.assert_called_once_with()
    new_source.close.assert_not_called()
    new_target.close.assert_not_called()

    manager.close()
    new_source.close.assert_called_once_with()
    new_target.close.assert_called_once_with()


def test_connect_retry_stops_during_backoff(monkeypatch):
    shutdown_event = threading.Event()
    failed = _client_with_ping(error=redis.ConnectionError("down"))

    def create_client(**_kwargs):
        shutdown_event.set()
        return failed

    monkeypatch.setattr(connection_manager_module.redis, "Redis", create_client)
    manager = RedisConnectionManager(
        {
            "max_attempts": 10,
            "backoff_factor": 2,
            "max_delay": 60,
            "initial_delay": 60,
        },
        shutdown_event=shutdown_event,
    )

    with pytest.raises(RedisConnectionError, match="重试已停止"):
        manager.connect_target(host="target", port=6379)

    failed.close.assert_called_once_with()
    failed.connection_pool.disconnect.assert_called_once_with()


def test_connect_retry_does_not_start_after_shutdown(monkeypatch):
    shutdown_event = threading.Event()
    shutdown_event.set()
    constructor = MagicMock()
    monkeypatch.setattr(connection_manager_module.redis, "Redis", constructor)
    manager = RedisConnectionManager(
        _retry_config(3), shutdown_event=shutdown_event
    )

    with pytest.raises(RedisConnectionError, match="重试已停止"):
        manager.connect_source(host="source", port=6379)

    constructor.assert_not_called()


@pytest.mark.parametrize(
    ("method_name", "client_attr"),
    [
        ("connect_source", "source_client"),
        ("connect_target", "target_client"),
    ],
)
def test_connect_does_not_publish_client_after_concurrent_close(
    monkeypatch, method_name, client_attr
):
    started = threading.Event()
    release = threading.Event()
    connected = _client_with_ping(result=True)
    manager = RedisConnectionManager(_retry_config(1))

    def delayed_connect(*_args, **_kwargs):
        started.set()
        assert release.wait(2)
        return connected

    monkeypatch.setattr(manager, "_connect_with_retry", delayed_connect)
    method = getattr(manager, method_name)
    thread, outcome = _start_call(lambda: method(host="redis.example"))
    assert started.wait(1)

    manager.close()
    release.set()
    thread.join(2)

    assert not thread.is_alive()
    assert outcome[0][0] == "error"
    assert isinstance(outcome[0][1], RedisConnectionError)
    assert getattr(manager, client_attr) is None
    connected.close.assert_called_once_with()
    connected.connection_pool.disconnect.assert_called_once_with()


@pytest.mark.parametrize(
    ("method_name", "client_attr"),
    [
        ("connect_source", "source_client"),
        ("connect_target", "target_client"),
    ],
)
def test_connect_rejects_success_when_shutdown_arrives_during_ping(
    monkeypatch, method_name, client_attr
):
    started = threading.Event()
    release = threading.Event()
    shutdown_event = threading.Event()
    connected = _client_with_ping()

    def delayed_ping():
        started.set()
        assert release.wait(2)
        return True

    connected.ping.side_effect = delayed_ping
    monkeypatch.setattr(connection_manager_module.redis, "Redis", lambda **_kwargs: connected)
    manager = RedisConnectionManager(
        _retry_config(1), shutdown_event=shutdown_event
    )
    method = getattr(manager, method_name)
    thread, outcome = _start_call(lambda: method(host="redis.example"))
    assert started.wait(1)

    shutdown_event.set()
    release.set()
    thread.join(2)

    assert not thread.is_alive()
    assert outcome[0][0] == "error"
    assert isinstance(outcome[0][1], RedisConnectionError)
    assert getattr(manager, client_attr) is None
    connected.close.assert_called_once_with()
    connected.connection_pool.disconnect.assert_called_once_with()


def test_connect_from_url_does_not_publish_pair_after_concurrent_close(monkeypatch):
    started = threading.Event()
    release = threading.Event()
    source = _client_with_ping(result=True)
    target = _client_with_ping()

    def delayed_ping():
        started.set()
        assert release.wait(2)
        return True

    target.ping.side_effect = delayed_ping
    monkeypatch.setattr(
        connection_manager_module.redis,
        "from_url",
        MagicMock(side_effect=[source, target]),
    )
    manager = RedisConnectionManager()
    thread, outcome = _start_call(
        lambda: manager.connect_from_url(
            "redis://source/0", "redis://target/0"
        )
    )
    assert started.wait(1)

    manager.close()
    release.set()
    thread.join(2)

    assert not thread.is_alive()
    assert outcome[0][0] == "error"
    assert isinstance(outcome[0][1], RedisConnectionError)
    assert manager.source_client is None
    assert manager.target_client is None
    for client in (source, target):
        client.close.assert_called_once_with()
        client.connection_pool.disconnect.assert_called_once_with()


@pytest.mark.parametrize("client_type", ["source", "target"])
def test_reconnect_does_not_publish_replacement_after_concurrent_close(
    monkeypatch, client_type
):
    started = threading.Event()
    release = threading.Event()
    shared = _client_with_ping(error=redis.ConnectionError("stale"))
    replacement = _client_with_ping(result=True)
    manager = RedisConnectionManager(_retry_config(1))
    setter = getattr(manager, f"set_{client_type}_client")
    setter(shared, {"host": client_type, "port": 6379}, owned=False)

    def delayed_connect(*_args, **_kwargs):
        started.set()
        assert release.wait(2)
        return replacement

    monkeypatch.setattr(manager, "_connect_with_retry", delayed_connect)
    thread, outcome = _start_call(
        lambda: manager.reconnect_if_needed(client_type)
    )
    assert started.wait(1)

    manager.close()
    release.set()
    thread.join(2)

    assert not thread.is_alive()
    assert outcome == [("result", None)]
    assert getattr(manager, f"{client_type}_client") is None
    shared.close.assert_not_called()
    shared.connection_pool.disconnect.assert_not_called()
    replacement.close.assert_called_once_with()
    replacement.connection_pool.disconnect.assert_called_once_with()
