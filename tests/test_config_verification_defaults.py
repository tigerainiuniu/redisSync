from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import yaml

from redis_sync.config import Config, load_and_validate_service_config
from redis_sync.exceptions import ConfigurationError
from redis_sync.migration_orchestrator import MigrationConfig, MigrationOrchestrator
from redis_sync.scan_handler import ScanHandler


MAX_SCAN_COUNT = 100_000


def _service_config():
    return {
        "source": {"host": "source.example", "port": 6379},
        "targets": [
            {"name": "target-a", "host": "target.example", "port": 6379}
        ],
        "sync": {"mode": "full"},
        "service": {},
    }


def _load_service_config(tmp_path, config):
    path = tmp_path / "redis-sync.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return load_and_validate_service_config(str(path))


def test_default_verification_rejects_same_type_different_string_values():
    source = MagicMock()
    source.scan.return_value = (0, [b"foo"])
    source.type.return_value = b"string"
    source.dump.return_value = b"source-dump"
    source.get.return_value = b"A"
    source.ttl.return_value = -1

    target = MagicMock()
    target.exists.return_value = 1
    target.type.return_value = b"string"
    target.dump.return_value = b"target-dump"
    target.get.return_value = b"B"
    target.ttl.return_value = -1

    orchestrator = MigrationOrchestrator(SimpleNamespace())
    orchestrator.scan_handler = ScanHandler(source, target)

    result = orchestrator._verify_migration(MigrationConfig())

    assert result["mode"] == "full"
    assert result["success"] is False
    assert result["details"]["value_mismatches"] == 1


def test_explicit_fast_verification_remains_a_structure_check():
    source = MagicMock()
    source.scan.return_value = (0, [b"foo"])
    source.pipeline.return_value.execute.return_value = [b"string"]
    source.get.return_value = b"A"

    target = MagicMock()
    target.pipeline.return_value.execute.return_value = [1, b"string"]
    target.get.return_value = b"B"

    orchestrator = MigrationOrchestrator(SimpleNamespace())
    orchestrator.scan_handler = ScanHandler(source, target)

    result = orchestrator._verify_migration(MigrationConfig(verify_mode="fast"))

    assert result["mode"] == "fast"
    assert result["success"] is True
    source.get.assert_not_called()
    target.get.assert_not_called()


def test_service_config_defaults_to_full_verification(tmp_path):
    loaded = _load_service_config(tmp_path, _service_config())

    assert loaded["sync"]["full_sync"]["verify_mode"] == "full"


def test_legacy_scan_count_accepts_hard_limit():
    loaded = Config.from_dict({"migration": {"scan_count": MAX_SCAN_COUNT}})

    assert loaded.migration.scan_count == MAX_SCAN_COUNT


def test_legacy_scan_count_rejects_value_above_hard_limit():
    with pytest.raises(ConfigurationError, match=r"migration\.scan_count"):
        Config.from_dict({"migration": {"scan_count": MAX_SCAN_COUNT + 1}})


def test_environment_scan_count_uses_hard_limit(monkeypatch):
    monkeypatch.setenv("MIGRATION_SCAN_COUNT", str(MAX_SCAN_COUNT))
    assert Config.from_env().migration.scan_count == MAX_SCAN_COUNT

    monkeypatch.setenv("MIGRATION_SCAN_COUNT", str(MAX_SCAN_COUNT + 1))
    with pytest.raises(ConfigurationError, match=r"migration\.scan_count"):
        Config.from_env()


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("sync", "full_sync"),
        ("service", "performance"),
    ],
)
def test_service_scan_counts_accept_hard_limit(tmp_path, section, field):
    config = _service_config()
    config[section][field] = {"scan_count": MAX_SCAN_COUNT}

    loaded = _load_service_config(tmp_path, config)

    assert loaded[section][field]["scan_count"] == MAX_SCAN_COUNT


@pytest.mark.parametrize(
    ("section", "field", "message"),
    [
        ("sync", "full_sync", r"sync\.full_sync\.scan_count"),
        ("service", "performance", r"service\.performance\.scan_count"),
    ],
)
def test_service_scan_counts_reject_value_above_hard_limit(
    tmp_path, section, field, message
):
    config = _service_config()
    config[section][field] = {"scan_count": MAX_SCAN_COUNT + 1}

    with pytest.raises(ConfigurationError, match=message):
        _load_service_config(tmp_path, config)
