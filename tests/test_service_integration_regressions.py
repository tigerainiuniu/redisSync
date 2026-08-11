import logging
import threading
import time
from contextlib import nullcontext
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis
import yaml

import redis_sync.sync_service as sync_service_module
from redis_sync.config import load_and_validate_service_config
from redis_sync.connection_manager import (
    RedisConnectionManager,
    assert_distinct_redis_databases,
)
from redis_sync.exceptions import ConfigurationError
from redis_sync.migration_orchestrator import (
    MigrationConfig,
    MigrationOrchestrator,
    MigrationStrategy,
)
from redis_sync.sync_service import RedisSyncService


def test_realtime_service_injects_key_state_runtime_config(monkeypatch):
    captured = {}

    class FakeIncrementalService:
        def __init__(self, mode, source_conn, target_connections, config):
            captured.update(
                mode=mode,
                source_conn=source_conn,
                target_connections=target_connections,
                config=config,
            )

        def start(self):
            captured["started"] = True

        def stop(self):
            captured["stopped"] = True

    monkeypatch.setattr(
        sync_service_module,
        "UnifiedIncrementalService",
        FakeIncrementalService,
    )

    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "source": {"db": 7},
        "sync": {
            "filters": {
                "include_patterns": ["tenant:*"],
                "exclude_patterns": ["*:base-temp"],
            }
        },
        "service": {"failover": {"recovery_delay": 1}},
    }
    service.source_conn = object()
    service.target_connections = {"target-a": object()}
    service._unavailable_targets = {}
    service.running = False
    service.shutdown_event = threading.Event()
    service.logger = logging.getLogger("test-realtime-runtime-config")

    requested_config = {
        "filters": {
            "exclude_patterns": ["*:runtime-temp"],
            "command_include": ["SET", "DEL"],
        }
    }
    service._start_realtime_replication("psync", requested_config)

    runtime_config = captured["config"]
    assert captured["mode"] == "psync"
    assert captured["source_conn"] is service.source_conn
    assert captured["target_connections"] is service.target_connections
    assert runtime_config["apply_mode"] == "key_state"
    assert runtime_config["source_db"] == 7
    assert runtime_config["materialize_snapshot"] is False
    assert runtime_config["filters"] == {
        "include_patterns": ["tenant:*"],
        "exclude_patterns": ["*:runtime-temp"],
        "command_include": ["SET", "DEL"],
    }
    assert runtime_config["snapshot_callback"].__self__ is service
    assert (
        runtime_config["snapshot_callback"].__func__
        is RedisSyncService._apply_replication_snapshot
    )
    assert (
        runtime_config["target_failure_callback"].__func__
        is RedisSyncService._deactivate_realtime_target
    )
    assert (
        runtime_config["target_success_callback"].__func__
        is RedisSyncService._record_realtime_target_success
    )
    assert "snapshot_callback" not in requested_config
    assert captured["started"] is True
    assert captured["stopped"] is True


def test_stop_stops_incremental_service_before_joining_workers():
    events = []

    class FakeIncrementalService:
        def stop(self, timeout):
            events.append(("incremental-stop", timeout))

    class FakeWorker:
        def join(self, timeout):
            events.append(("worker-join", timeout))

        def is_alive(self):
            return False

    class FakeExecutor:
        def shutdown(self, wait, cancel_futures=False):
            events.append(("executor-shutdown", wait, cancel_futures))

    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-service-stop-order")
    service.running = True
    service.shutdown_event = threading.Event()
    service.incremental_service = FakeIncrementalService()
    service.sync_tasks = [FakeWorker()]
    service.target_connections = {}
    service.source_conn = None
    service.executor = FakeExecutor()
    service.web_ui = None

    service.stop()

    assert events[0] == ("incremental-stop", 0)
    assert events[1][0] == "worker-join"
    assert 0 < events[1][1] <= 1
    assert ("executor-shutdown", False, True) in events
    assert service.running is False
    assert service.shutdown_event.is_set()


def test_stop_closes_connections_before_final_replication_exit_check():
    events = []

    class FakeManager:
        def close(self):
            events.append("connection-close")

    class FakeIncrementalService:
        def stop(self, timeout):
            events.append(("incremental-stop", timeout))
            return False

        def wait_stopped(self, timeout):
            events.append(("replication-wait", timeout))
            return "connection-close" in events

    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-service-final-stop-check")
    service.running = True
    service.shutdown_event = threading.Event()
    service.incremental_service = FakeIncrementalService()
    service.sync_tasks = []
    service.target_connections = {"target": FakeManager()}
    service.orchestrators = {}
    service.source_conn = None
    service.executor = MagicMock()
    service.web_ui = None

    service.stop()

    assert events[:2] == [("incremental-stop", 0), "connection-close"]
    assert events[2][0] == "replication-wait"
    assert 0 < events[2][1] <= 40


def test_stop_reports_replication_thread_that_remains_alive():
    class StuckIncrementalService:
        def stop(self):
            return False

        def wait_stopped(self, _timeout):
            return False

    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-service-incomplete-stop")
    service.running = True
    service.shutdown_event = threading.Event()
    service.incremental_service = StuckIncrementalService()
    service.sync_tasks = []
    service.target_connections = {}
    service.orchestrators = {}
    service.source_conn = None
    service.executor = MagicMock()
    service.web_ui = None

    with pytest.raises(RuntimeError, match="实时复制线程未退出"):
        service.stop()


def test_stop_waits_for_inflight_start_and_prevents_service_resurrection():
    connect_started = threading.Event()
    release_connect = threading.Event()
    source = MagicMock()
    executor = MagicMock()
    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-start-stop-race")
    service.running = False
    service.shutdown_event = threading.Event()
    service.incremental_service = None
    service.sync_tasks = []
    service.target_connections = {}
    service.orchestrators = {}
    service.source_conn = None
    service.executor = executor
    service.web_ui = None
    service._setup_signal_handlers = MagicMock()

    def connect_source():
        connect_started.set()
        assert release_connect.wait(2)
        service.source_conn = source
        return True

    service._connect_source = connect_source
    service._connect_targets = MagicMock(return_value=True)
    start_result = []
    starter = threading.Thread(target=lambda: start_result.append(service.start()))
    stopper = threading.Thread(target=service.stop)

    starter.start()
    assert connect_started.wait(1)
    stopper.start()
    release_connect.set()
    starter.join(timeout=2)
    stopper.join(timeout=2)

    assert not starter.is_alive()
    assert not stopper.is_alive()
    assert start_result == [False]
    assert service.running is False
    assert service.shutdown_event.is_set()
    assert service.sync_tasks == []
    service._connect_targets.assert_not_called()
    source.close.assert_called_once_with()
    source.connection_pool.disconnect.assert_called_once_with()
    executor.shutdown.assert_called_once_with(wait=False, cancel_futures=True)


def test_stop_uses_one_deadline_for_all_coordinator_joins():
    join_timeouts = []

    class StuckWorker:
        def __init__(self, name):
            self.name = name

        def join(self, timeout):
            join_timeouts.append(timeout)
            time.sleep(timeout)

        def is_alive(self):
            return True

    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-service-shared-stop-deadline")
    service.running = True
    service.shutdown_event = threading.Event()
    service.incremental_service = None
    service.sync_tasks = [StuckWorker("one"), StuckWorker("two")]
    service.target_connections = {}
    service.orchestrators = {}
    service.source_conn = None
    service.executor = MagicMock()
    service.web_ui = None

    started_at = time.monotonic()
    with pytest.raises(RuntimeError, match="同步协调线程未退出"):
        service.stop(timeout=0.05)
    elapsed = time.monotonic() - started_at

    assert elapsed < 0.2
    assert sum(join_timeouts) <= 0.06
    service.executor.shutdown.assert_called_once_with(
        wait=False, cancel_futures=True
    )


def test_concurrent_stop_wait_uses_its_own_bounded_deadline():
    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-concurrent-service-stop-deadline")
    service._lifecycle_lock = threading.RLock()
    service._stop_complete = threading.Event()
    service._stopped = True

    started_at = time.monotonic()
    with pytest.raises(RuntimeError, match="等待现有停止流程超时"):
        service.stop(timeout=0.02)

    assert time.monotonic() - started_at < 0.2


def test_realtime_recovery_closes_prepared_target_when_stop_wins():
    prepared = threading.Event()
    release_prepare = threading.Event()
    manager = MagicMock()
    orchestrator = MagicMock()
    incremental = MagicMock()
    incremental.command_barrier.return_value = nullcontext()
    service = RedisSyncService.__new__(RedisSyncService)
    service.logger = logging.getLogger("test-stop-during-realtime-recovery")
    service.running = True
    service.shutdown_event = threading.Event()
    service._stopped = False
    service.incremental_service = incremental
    service.config = {"targets": [{"name": "target-a"}]}
    service._unavailable_targets = {"target-a": {"name": "target-a"}}
    service.orchestrators = {}
    service._align_realtime_target = MagicMock(return_value=True)

    def prepare(_config):
        prepared.set()
        assert release_prepare.wait(2)
        return manager, orchestrator

    service._prepare_target_resources = prepare
    result = []
    worker = threading.Thread(
        target=lambda: result.append(service._recover_realtime_target("target-a"))
    )
    worker.start()
    assert prepared.wait(1)
    service._stopped = True
    service.running = False
    service.shutdown_event.set()
    release_prepare.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert result == [False]
    incremental.register_target.assert_not_called()
    service._align_realtime_target.assert_not_called()
    manager.close.assert_called_once_with()


def test_connection_manager_closes_only_owned_clients_and_disconnects_pool():
    shared_source = MagicMock()
    owned_target = MagicMock()
    manager = RedisConnectionManager()
    manager.set_source_client(shared_source, owned=False)
    manager.set_target_client(owned_target, owned=True)

    manager.close_connections()

    shared_source.close.assert_not_called()
    shared_source.connection_pool.disconnect.assert_not_called()
    owned_target.close.assert_called_once_with()
    owned_target.connection_pool.disconnect.assert_called_once_with()


def test_connected_redis_run_id_blocks_host_alias_to_same_database():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {"source": {"db": 3}}
    service.logger = logging.getLogger("test-run-id-database-identity")
    service.source_conn = MagicMock()
    service.source_conn.info.return_value = {"run_id": "same-process"}
    target = MagicMock()
    target.info.return_value = {"run_id": "same-process"}

    with pytest.raises(ValueError, match=r"run_id 和 db 相同"):
        service._assert_distinct_target_database(target, {"db": 3})


def test_different_proxy_run_ids_still_probe_for_shared_keyspace():
    shared = {}
    source = MagicMock()
    target = MagicMock()
    source.info.return_value = {"run_id": "proxy-node-a"}
    target.info.return_value = {"run_id": "proxy-node-b"}

    def set_marker(key, value, nx=False, px=None):
        if nx and key in shared:
            return False
        shared[key] = value
        return True

    target.set.side_effect = set_marker
    source.get.side_effect = shared.get
    target.exists.side_effect = lambda key: int(key in shared)
    target.delete.side_effect = lambda key: int(shared.pop(key, None) is not None)

    with pytest.raises(ValueError, match="指向同一 Redis 数据库"):
        assert_distinct_redis_databases(source, target, 0, 0)

    target.set.assert_called_once()
    target.delete.assert_called_once()
    assert shared == {}


def test_connected_redis_run_id_check_tolerates_info_acl_denial():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {"source": {"db": 0}}
    service.logger = logging.getLogger("test-run-id-acl-denial")
    service.source_conn = MagicMock()
    service.source_conn.info.side_effect = redis.ResponseError("NOPERM INFO")

    service._assert_distinct_target_database(MagicMock(), {"db": 0})


def test_connected_redis_identity_marker_expiry_fails_closed():
    source = MagicMock()
    target = MagicMock()
    source.info.side_effect = redis.ResponseError("NOPERM INFO")
    target.set.return_value = True
    source.get.return_value = None
    target.exists.return_value = 0

    with pytest.raises(ValueError, match="身份标记在校验完成前失效"):
        assert_distinct_redis_databases(source, target, 0, 0)

    target.delete.assert_called_once()


def test_realtime_service_constructor_failure_stops_outer_service(monkeypatch):
    class ConstructorFailure:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("injected constructor failure")

    monkeypatch.setattr(
        sync_service_module,
        "UnifiedIncrementalService",
        ConstructorFailure,
    )
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "source": {"db": 0},
        "sync": {"filters": {}},
        "service": {"failover": {"recovery_delay": 1}},
    }
    service.source_conn = object()
    service.target_connections = {}
    service._unavailable_targets = {}
    service.running = True
    service.shutdown_event = threading.Event()
    service.logger = logging.getLogger("test-realtime-constructor-failure")

    service._start_realtime_replication("psync", {})

    assert service.running is False
    assert service.shutdown_event.is_set()


def test_failover_recovery_gate_honors_enabled_and_per_target_delay():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "service": {"failover": {"enabled": False, "recovery_delay": 10}}
    }
    service._target_next_recovery = {}

    assert not service._target_recovery_is_due("target-a", now=100)
    assert service._target_next_recovery == {}

    service.config["service"]["failover"]["enabled"] = True
    assert not service._target_recovery_is_due("target-a", now=100)
    assert not service._target_recovery_is_due("target-a", now=109.999)
    assert not service._target_recovery_is_due("target-b", now=101)
    assert service._target_recovery_is_due("target-a", now=110)
    assert service._target_recovery_is_due("target-b", now=111)

    service._clear_target_recovery_deadline("target-a")
    assert not service._target_recovery_is_due("target-a", now=111)
    assert service._target_recovery_is_due("target-a", now=121)


def test_source_connection_uses_service_retry_configuration(monkeypatch):
    captured = {}
    source_client = object()

    class FakeConnectionManager:
        def __init__(self, retry_config=None, shutdown_event=None):
            captured["retry_config"] = retry_config
            captured["shutdown_event"] = shutdown_event

        def connect_source(self, **kwargs):
            captured["connection_kwargs"] = kwargs
            return source_client

    monkeypatch.setattr(
        sync_service_module, "RedisConnectionManager", FakeConnectionManager
    )
    service = RedisSyncService.__new__(RedisSyncService)
    retry_config = {
        "max_attempts": 4,
        "backoff_factor": 2,
        "max_delay": 9,
        "initial_delay": 0.25,
    }
    service.config = {
        "source": {
            "host": "source.example",
            "port": 6380,
            "password": "secret",
            "db": 3,
        },
        "service": {"retry": retry_config},
    }
    service.shutdown_event = threading.Event()
    service.logger = logging.getLogger("test-source-connect-retry")

    assert service._connect_source() is True
    assert service.source_conn is source_client
    assert captured["retry_config"] is retry_config
    assert captured["shutdown_event"] is service.shutdown_event
    assert captured["connection_kwargs"] == {
        "host": "source.example",
        "port": 6380,
        "password": "secret",
        "db": 3,
    }


def test_failover_failure_count_uses_sliding_window():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "service": {
            "failover": {
                "max_failures": 3,
                "failure_window": 10,
            }
        }
    }
    stats = sync_service_module.SyncStats()

    assert service._record_target_failure(stats, "first", now=100) == 1
    assert service._record_target_failure(stats, "second", now=109) == 2
    assert service._record_target_failure(stats, "third", now=111) == 2
    assert stats.is_healthy is True
    assert service._record_target_failure(stats, "fourth", now=112) == 3
    assert stats.is_healthy is False

    service._reset_target_failures(stats)
    assert stats.consecutive_failures == 0
    assert stats.failure_timestamps == []
    assert stats.last_error is None


def test_realtime_target_is_removed_only_after_failure_threshold():
    service = RedisSyncService.__new__(RedisSyncService)
    target_config = {
        "name": "target-a",
        "host": "target.example",
        "port": 6379,
    }
    manager = MagicMock()
    incremental_service = MagicMock()
    service.config = {
        "targets": [target_config],
        "service": {
            "failover": {"max_failures": 3, "failure_window": 60}
        },
    }
    service.logger = logging.getLogger("test-realtime-failure-threshold")
    service.incremental_service = incremental_service
    service.target_connections = {"target-a": manager}
    service.orchestrators = {"target-a": object()}
    service._unavailable_targets = {}
    service.stats = {"target-a": sync_service_module.SyncStats()}
    incremental_service.unregister_target.side_effect = (
        lambda name: service.target_connections.pop(name, None)
    )

    assert service._deactivate_realtime_target("target-a") is False
    assert service._deactivate_realtime_target("target-a") is False
    incremental_service.unregister_target.assert_not_called()
    assert "target-a" in service.target_connections

    assert service._deactivate_realtime_target("target-a") is True
    incremental_service.unregister_target.assert_called_once_with("target-a")
    assert "target-a" not in service.target_connections
    assert "target-a" in service._unavailable_targets
    assert service.stats["target-a"].total_failed == 3
    assert service.stats["target-a"].is_healthy is False


def test_realtime_target_success_updates_status_and_resets_failures(monkeypatch):
    service = RedisSyncService.__new__(RedisSyncService)
    stats = sync_service_module.SyncStats(
        total_synced=4,
        last_error="transient",
        consecutive_failures=2,
        failure_timestamps=[10.0, 11.0],
    )
    service.stats = {"target-a": stats}
    service.running = True
    monkeypatch.setattr(sync_service_module.time, "time", lambda: 123.0)

    service._record_realtime_target_success("target-a")

    status = service.get_status()["targets"]["target-a"]
    assert status["total_synced"] == 5
    assert status["last_sync_time"] == 123.0
    assert status["consecutive_failures"] == 0
    assert status["last_error"] is None
    assert status["healthy"] is True


def test_service_status_reports_replication_stall_despite_green_target_stats():
    service = RedisSyncService.__new__(RedisSyncService)
    service.running = True
    service.stats = {"target-a": sync_service_module.SyncStats(is_healthy=True)}
    service.incremental_service = SimpleNamespace(
        get_replication_status=lambda: {
            "running": True,
            "healthy": False,
            "stalled": True,
            "callback_in_flight": True,
            "callback_duration": 12.0,
            "last_error": None,
        }
    )

    status = service.get_status()

    assert status["running"] is True
    assert status["targets"]["target-a"]["healthy"] is True
    assert status["replication"]["stalled"] is True
    assert status["healthy"] is False


def test_successful_realtime_recovery_resets_failure_window():
    service = RedisSyncService.__new__(RedisSyncService)
    target_config = {
        "name": "target-a",
        "host": "target.example",
        "port": 6379,
    }
    manager = MagicMock()
    orchestrator = MagicMock()
    incremental_service = MagicMock()
    incremental_service.command_barrier.return_value = nullcontext()
    service.incremental_service = incremental_service
    service.config = {"targets": [target_config]}
    service._unavailable_targets = {"target-a": target_config}
    service.target_connections = {}
    service.orchestrators = {}
    service.stats = {
        "target-a": sync_service_module.SyncStats(
            last_error="old failure",
            consecutive_failures=2,
            is_healthy=False,
            failure_timestamps=[100.0, 101.0],
        )
    }
    service._prepare_target_resources = MagicMock(
        return_value=(manager, orchestrator)
    )
    service._align_realtime_target = MagicMock(return_value=True)

    assert service._recover_realtime_target("target-a") is True

    service._align_realtime_target.assert_called_once_with(
        "target-a", prune_managed_scope=True
    )
    stats = service.stats["target-a"]
    assert stats.is_healthy is True
    assert stats.last_error is None
    assert stats.consecutive_failures == 0
    assert stats.failure_timestamps == []


def test_scan_coordinator_does_not_recover_when_failover_is_disabled():
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "sync": {
            "mode": "incremental",
            "incremental_sync": {
                "enabled": True,
                "method": "scan",
                "interval": 0,
            },
        },
        "service": {
            "failover": {
                "enabled": False,
                "max_failures": 1,
                "recovery_delay": 60,
            }
        },
    }
    service.logger = logging.getLogger("test-disabled-scan-failover")
    service.running = True
    service.shutdown_event = threading.Event()
    service.target_connections = {}
    service.stats = {"target-a": sync_service_module.SyncStats(is_healthy=False)}
    service._target_next_recovery = {}
    service._recover_target_connection = MagicMock(return_value=True)

    def finish_iteration():
        service.running = False
        service.shutdown_event.set()
        return False

    service._perform_unified_incremental_sync = finish_iteration

    service._unified_sync_coordinator()

    service._recover_target_connection.assert_not_called()


def test_realtime_coordinator_does_not_recover_when_failover_is_disabled(
    monkeypatch,
):
    started = threading.Event()
    stopped = threading.Event()

    class BlockingIncrementalService:
        def __init__(self, *args, **kwargs):
            pass

        def start(self):
            started.set()
            stopped.wait(1)

        def stop(self):
            stopped.set()

    monkeypatch.setattr(
        sync_service_module,
        "UnifiedIncrementalService",
        BlockingIncrementalService,
    )
    service = RedisSyncService.__new__(RedisSyncService)
    service.config = {
        "source": {"db": 0},
        "sync": {"filters": {}},
        "service": {
            "failover": {"enabled": False, "recovery_delay": 60}
        },
    }
    service.logger = logging.getLogger("test-disabled-realtime-failover")
    service.source_conn = object()
    service.target_connections = {}
    service._unavailable_targets = {"target-a": {"name": "target-a"}}
    service._target_next_recovery = {}
    service._recover_realtime_target = MagicMock(return_value=True)
    service.running = True
    service.shutdown_event = threading.Event()

    def request_stop():
        assert started.wait(1)
        service.running = False
        service.shutdown_event.set()

    stopper = threading.Thread(target=request_stop)
    stopper.start()
    service._start_realtime_replication("psync", {})
    stopper.join(timeout=1)

    assert not stopper.is_alive()
    service._recover_realtime_target.assert_not_called()


def test_full_migration_maps_sync_strategy_and_forwards_overwrite():
    handler = MagicMock()
    handler.perform_full_migration.return_value = {"success": True}
    orchestrator = MigrationOrchestrator(SimpleNamespace())
    orchestrator.full_migration_handler = handler

    result = orchestrator._perform_full_migration(
        MigrationConfig(
            strategy=MigrationStrategy.SYNC,
            overwrite_existing=True,
        )
    )

    assert result == {"success": True}
    kwargs = handler.perform_full_migration.call_args.kwargs
    assert kwargs["strategy"] == "sync"
    assert kwargs["overwrite_existing"] is True


def test_orchestrator_rejects_programmatic_replication_mode():
    orchestrator = MigrationOrchestrator(SimpleNamespace())

    with pytest.raises(ConfigurationError, match=r"enable_replication.*一次性"):
        orchestrator.migrate(MigrationConfig(enable_replication=True))


def _write_service_config(tmp_path, config):
    path = tmp_path / "redis-sync.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(path)


def _service_config():
    return {
        "source": {"host": "source.example", "port": 6379},
        "targets": [
            {"name": "target-a", "host": "target-a.example", "port": 6379}
        ],
        "sync": {"mode": "full"},
        "service": {},
    }


def test_service_config_rejects_unknown_sync_mode(tmp_path):
    config = _service_config()
    config["sync"]["mode"] = "streaming"

    with pytest.raises(ConfigurationError, match=r"sync\.mode"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_duplicate_target_names(tmp_path):
    config = _service_config()
    config["targets"].append(
        {"name": "target-a", "host": "target-b.example", "port": 6380}
    )

    with pytest.raises(ConfigurationError, match="目标名称重复: target-a"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_realtime_service_config_requires_pool_slot_per_target_and_stream(tmp_path):
    config = _service_config()
    config["source"]["connection_pool_max_connections"] = 2
    config["targets"].append(
        {"name": "target-b", "host": "target-b.example", "port": 6380}
    )
    config["sync"] = {
        "mode": "hybrid",
        "incremental_sync": {"method": "psync"},
    }

    with pytest.raises(ConfigurationError, match=r"至少需要为 3"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_normalizes_legacy_idle_time_to_scan(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "hybrid",
        "incremental_sync": {"method": "idle_time"},
    }

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["sync"]["incremental_sync"]["method"] == "scan"


def test_service_config_normalizes_sync_streaming_to_psync(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "incremental",
        "incremental_sync": {"method": "sync"},
    }

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["sync"]["incremental_sync"]["method"] == "psync"


def test_service_config_rejects_oversized_replication_buffer(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "incremental",
        "incremental_sync": {
            "method": "psync",
            "buffer_size": 16 * 1024 * 1024 + 1,
        },
    }

    with pytest.raises(ConfigurationError, match=r"buffer_size"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_defaults_replication_capture_limit(tmp_path):
    loaded = load_and_validate_service_config(
        _write_service_config(tmp_path, _service_config())
    )

    assert (
        loaded["sync"]["incremental_sync"]["capture_max_size"]
        == 1024 * 1024 * 1024
    )


def test_service_config_rejects_nonpositive_replication_capture_limit(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "incremental",
        "incremental_sync": {
            "method": "psync",
            "capture_max_size": 0,
        },
    }

    with pytest.raises(ConfigurationError, match=r"capture_max_size"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_defaults_reserved_monitoring_to_disabled(tmp_path):
    loaded = load_and_validate_service_config(
        _write_service_config(tmp_path, _service_config())
    )

    assert loaded["service"]["monitoring"]["enabled"] is False


@pytest.mark.parametrize(
    ("configure", "message"),
    [
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"key_types": "string"}}
            ),
            r"key_types",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"key_pattern": ["managed:*"]}}
            ),
            r"key_pattern",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"apply_mode": "eventual"}}
            ),
            r"apply_mode",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"apply_mode": "direct"}}
            ),
            r"direct.*key_state",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"filters": {"min_ttl": -1}}}
            ),
            r"incremental_sync\.filters",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"target_command_timeout": 0}}
            ),
            r"target_command_timeout",
        ),
        (
            lambda config: config["sync"].update(
                {"filters": {"include_patterns": "managed:*"}}
            ),
            r"include_patterns",
        ),
        (
            lambda config: config["sync"].update(
                {"incremental_sync": {"command_dedup_window": 10}}
            ),
            r"command_dedup_window",
        ),
    ],
)
def test_service_config_rejects_inconsistent_realtime_scope(
    tmp_path, configure, message
):
    config = _service_config()
    config["sync"]["mode"] = "incremental"
    configure(config)

    with pytest.raises(ConfigurationError, match=message):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_disabled_incremental_mode(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "incremental",
        "incremental_sync": {"enabled": False},
    }

    with pytest.raises(ConfigurationError, match=r"enabled 必须为 true"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_hybrid_service_config_requires_same_full_and_incremental_scope(tmp_path):
    config = _service_config()
    config["sync"] = {
        "mode": "hybrid",
        "full_sync": {"key_pattern": "tenant:*"},
        "incremental_sync": {"key_pattern": "*"},
    }

    with pytest.raises(ConfigurationError, match=r"key_pattern/key_types 必须一致"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_source_as_target(tmp_path):
    config = _service_config()
    config["targets"][0].update(
        host=config["source"]["host"],
        port=config["source"]["port"],
        db=0,
    )

    with pytest.raises(ConfigurationError, match=r"与 source 指向同一"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_resolved_host_alias_to_source(tmp_path):
    config = _service_config()
    config["source"]["host"] = "localhost"
    config["targets"][0]["host"] = "127.0.0.1"

    with pytest.raises(ConfigurationError, match=r"与 source 指向同一"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_duplicate_target_endpoint(tmp_path):
    config = _service_config()
    config["targets"].append(
        {
            "name": "target-b",
            "host": "target-a.example",
            "port": 6379,
            "db": 0,
        }
    )

    with pytest.raises(ConfigurationError, match=r"另一个 enabled 目标"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_realtime_service_rejects_single_connection_target_pool(tmp_path):
    config = _service_config()
    config["targets"][0]["connection_pool_max_connections"] = 1
    config["sync"] = {
        "mode": "incremental",
        "incremental_sync": {"method": "psync"},
    }

    with pytest.raises(ConfigurationError, match=r"至少需要为 2"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_quoted_false_for_destructive_boolean(tmp_path):
    config = _service_config()
    config["sync"]["full_sync"] = {"clear_target": "false"}

    with pytest.raises(ConfigurationError, match=r"clear_target 必须是布尔值"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_defaults_to_preserving_target_data(tmp_path):
    loaded = load_and_validate_service_config(
        _write_service_config(tmp_path, _service_config())
    )

    assert loaded["sync"]["full_sync"]["clear_target"] is False


def test_service_config_defaults_pipeline_batch_size(tmp_path):
    loaded = load_and_validate_service_config(
        _write_service_config(tmp_path, _service_config())
    )

    assert loaded["service"]["performance"]["pipeline_batch_size"] == 100


@pytest.mark.parametrize(
    "field",
    [
        "username",
        "password",
        "client_name",
        "ssl_cert_reqs",
        "ssl_ca_certs",
        "ssl_certfile",
        "ssl_keyfile",
    ],
)
@pytest.mark.parametrize("section", ["source", "target"])
def test_service_config_rejects_non_string_connection_options(
    tmp_path, section, field
):
    config = _service_config()
    if section == "source":
        config["source"][field] = 123
        expected = rf"source\.{field}"
    else:
        config["targets"][0][field] = 123
        expected = rf"targets\[0\]\.{field}"

    with pytest.raises(ConfigurationError, match=expected):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_accepts_null_optional_connection_strings(tmp_path):
    config = _service_config()
    fields = (
        "username",
        "password",
        "client_name",
        "ssl_cert_reqs",
        "ssl_ca_certs",
        "ssl_certfile",
        "ssl_keyfile",
    )
    for field in fields:
        config["source"][field] = None
        config["targets"][0][field] = None

    loaded = load_and_validate_service_config(
        _write_service_config(tmp_path, config)
    )

    assert all(loaded["source"][field] is None for field in fields)
    assert all(loaded["targets"][0][field] is None for field in fields)


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("source", "port", 6379.5),
        ("source", "db", 1.5),
        ("target", "port", 6380.5),
    ],
)
def test_service_config_rejects_fractional_integer_fields(
    tmp_path, section, field, value
):
    config = _service_config()
    if section == "target":
        config["targets"][0][field] = value
        expected = rf"targets\[0\]\.{field}"
    else:
        config[section][field] = value
        expected = rf"{section}\.{field}"

    with pytest.raises(ConfigurationError, match=expected):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_fractional_service_integer(tmp_path):
    config = _service_config()
    config["service"]["performance"] = {"max_workers": 1.5}

    with pytest.raises(
        ConfigurationError, match=r"service\.performance\.max_workers"
    ):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_bounds_pipeline_batch_size(tmp_path):
    config = _service_config()
    config["service"]["performance"] = {"pipeline_batch_size": 201}

    with pytest.raises(
        ConfigurationError,
        match=r"service\.performance\.pipeline_batch_size",
    ):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_typo_in_destructive_full_sync_option(tmp_path):
    config = _service_config()
    config["sync"]["full_sync"] = {"cleer_target": False}

    with pytest.raises(ConfigurationError, match=r"full_sync.*cleer_target"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_rejects_top_level_web_ui_typo(tmp_path):
    config = _service_config()
    config["web_iu"] = {"enabled": False}

    with pytest.raises(ConfigurationError, match=r"配置顶层.*web_iu"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


@pytest.mark.parametrize("api_key", [None, ""], ids=["null-key", "empty-key"])
def test_service_config_allows_external_web_bind_with_loopback_only_acl(
    tmp_path, api_key
):
    config = _service_config()
    config["web_ui"] = {"enabled": True, "host": "0.0.0.0"}
    config["security"] = {
        "auth_enabled": False,
        "api_key": api_key,
        "allowed_ips": ["127.0.0.1", "::1"],
    }

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["web_ui"]["host"] == "0.0.0.0"
    assert loaded["security"]["allowed_ips"] == ["127.0.0.1", "::1"]


def test_service_config_allows_external_web_bind_with_api_key(tmp_path):
    config = _service_config()
    config["web_ui"] = {"enabled": True, "host": "0.0.0.0"}
    config["security"] = {
        "auth_enabled": True,
        "api_key": "secret",
        "allowed_ips": ["10.0.0.0/8"],
    }

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["security"]["auth_enabled"] is True


@pytest.mark.parametrize(
    "allowed_ips",
    [
        None,
        [],
        ["*"],
        ["10.0.0.0/8"],
        ["127.0.0.1", "not-a-network"],
        ["127.0.0.1", "   "],
    ],
    ids=[
        "missing",
        "empty",
        "wildcard",
        "non-loopback",
        "invalid",
        "blank",
    ],
)
def test_service_config_rejects_unprotected_external_web_bind(
    tmp_path, allowed_ips
):
    config = _service_config()
    config["web_ui"] = {"enabled": True, "host": "0.0.0.0"}
    config["security"] = {"auth_enabled": False}
    if allowed_ips is not None:
        config["security"]["allowed_ips"] = allowed_ips

    with pytest.raises(ConfigurationError, match=r"Web UI|allowed_ips"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_service_config_ignores_web_exposure_policy_when_web_is_disabled(tmp_path):
    config = _service_config()
    config["web_ui"] = {"enabled": False, "host": "0.0.0.0"}

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["web_ui"]["enabled"] is False


def test_service_config_normalizes_and_validates_key_types(tmp_path):
    config = _service_config()
    config["sync"]["full_sync"] = {"key_types": ["STRING", "Hash"]}
    config["sync"]["incremental_sync"] = {"key_types": ["sTrEaM"]}

    loaded = load_and_validate_service_config(_write_service_config(tmp_path, config))

    assert loaded["sync"]["full_sync"]["key_types"] == ["string", "hash"]
    assert loaded["sync"]["incremental_sync"]["key_types"] == ["stream"]

    config["sync"]["full_sync"]["key_types"] = ["bitmap"]
    with pytest.raises(ConfigurationError, match=r"不支持的 Redis 类型"):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


@pytest.mark.parametrize(
    ("configure", "message"),
    [
        (
            lambda config: config.update({"web_ui": {"enabled": "false"}}),
            r"web_ui\.enabled",
        ),
        (
            lambda config: config.update(
                {"security": {"auth_enabled": "false"}}
            ),
            r"security\.auth_enabled",
        ),
        (
            lambda config: config["service"].update(
                {"retry": {"max_attempts": "many"}}
            ),
            r"service\.retry\.max_attempts",
        ),
        (
            lambda config: config["service"].update(
                {"logging": {"backup_count": -1}}
            ),
            r"service\.logging\.backup_count",
        ),
    ],
)
def test_service_config_rejects_runtime_type_errors(
    tmp_path, configure, message
):
    config = _service_config()
    configure(config)

    with pytest.raises(ConfigurationError, match=message):
        load_and_validate_service_config(_write_service_config(tmp_path, config))


def test_empty_managed_keyspace_verifies_successfully():
    orchestrator = MigrationOrchestrator(SimpleNamespace())
    orchestrator.scan_handler = MagicMock()
    orchestrator.scan_handler.compare_keys.return_value = {
        "total_compared": 0,
        "matching_keys": 0,
        "errors": [],
    }

    result = orchestrator._verify_migration(MigrationConfig())

    assert result["success"] is True
    assert result["total_compared"] == 0
