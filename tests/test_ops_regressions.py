import base64
import json
import socket
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from click.testing import CliRunner

import redis_sync.cli as cli_module
from redis_sync.cli import _connect_redis_instances
from redis_sync.config import (
    Config,
    LoggingConfig,
    MigrationSettings,
    RedisConfig,
    load_config,
)
from redis_sync.exceptions import ConfigurationError
from redis_sync.web_ui import WebUI, WebUIHandler, _ConcurrentHTTPServer


def _make_web_handler(config, client_ip='127.0.0.1', headers=None, path='/api/status'):
    handler = object.__new__(WebUIHandler)
    handler.sync_service = SimpleNamespace(config=config)
    handler.client_address = (client_ip, 12345)
    handler.headers = headers or {}
    handler.path = path
    responses = []
    handler._send_response = lambda status, content, content_type, **kwargs: responses.append(
        (status, content, content_type, kwargs.get('headers'))
    )
    return handler, responses


def test_web_routes_enforce_allowed_ips_and_api_key():
    config = {
        'security': {
            'auth_enabled': True,
            'api_key': 'expected-key',
            'allowed_ips': ['127.0.0.0/8'],
        }
    }

    denied_ip, denied_ip_responses = _make_web_handler(
        config,
        client_ip='192.0.2.10',
        headers={'X-API-Key': 'expected-key'},
    )
    denied_ip.do_GET()
    assert denied_ip_responses[0][0] == 403

    missing_key, missing_key_responses = _make_web_handler(config)
    missing_key.do_GET()
    assert missing_key_responses[0][0] == 401

    allowed, allowed_responses = _make_web_handler(
        config,
        headers={'X-API-Key': 'expected-key'},
    )
    routed = []
    allowed._serve_status_api = lambda: routed.append(True)
    allowed.do_GET()
    assert routed == [True]
    assert allowed_responses == []


@pytest.mark.parametrize(
    'authorization',
    [
        'Bearer expected-key',
        'Basic ' + base64.b64encode(b'operator:expected-key').decode('ascii'),
    ],
    ids=['bearer', 'basic'],
)
def test_web_api_accepts_bearer_and_basic_api_key(authorization):
    config = {
        'security': {
            'auth_enabled': True,
            'api_key': 'expected-key',
            'allowed_ips': ['127.0.0.1'],
        }
    }
    handler, responses = _make_web_handler(
        config,
        headers={'Authorization': authorization},
    )
    routed = []
    handler._serve_status_api = lambda: routed.append(True)

    handler.do_GET()

    assert routed == [True]
    assert responses == []


@pytest.mark.parametrize(
    'authorization',
    [
        'Bearer wrong-key',
        'Basic ' + base64.b64encode(b'operator:wrong-key').decode('ascii'),
        'Basic not-valid-base64!',
    ],
    ids=['bearer', 'basic', 'malformed-basic'],
)
def test_web_api_rejects_invalid_authorization_with_basic_challenge(authorization):
    config = {
        'security': {
            'auth_enabled': True,
            'api_key': 'expected-key',
        }
    }
    handler, responses = _make_web_handler(
        config,
        headers={'Authorization': authorization},
    )

    handler.do_GET()

    assert responses[0][0] == 401
    assert responses[0][3] == {'WWW-Authenticate': 'Basic realm="Redis Sync"'}


def test_config_api_recursively_redacts_secrets_without_mutating_config():
    config = {
        'source': {
            'host': 'source.example',
            'password': 'source-secret',
            'url': 'redis://user:source-url-secret@source.example/0',
        },
        'targets': [{'name': 'target-1', 'password': 'target-secret'}],
        'security': {
            'auth_enabled': True,
            'api_key': 'api-secret',
            'encryption': {'key': 'encryption-secret'},
        },
        'notifications': {
            'email': {'password': 'mail-secret'},
            'webhook': {'url': 'https://hooks.example/TOKEN'},
        },
        'plugins': [
            {
                'name': 'nested-plugin',
                'credentials': 'credential-bundle',
                'settings': {
                    'access-token': 'nested-token',
                    'database_dsn': 'redis://nested-secret',
                },
            }
        ],
    }
    handler, responses = _make_web_handler(config)

    handler._serve_config_api()

    assert responses[0][0] == 200
    payload = json.loads(responses[0][1])
    assert payload['source']['host'] == 'source.example'
    assert payload['source']['password'] == '***'
    assert payload['source']['url'] == '***'
    assert payload['targets'][0]['password'] == '***'
    assert payload['security']['api_key'] == '***'
    assert payload['security']['encryption']['key'] == '***'
    assert payload['notifications']['email']['password'] == '***'
    assert payload['notifications']['webhook']['url'] == '***'
    assert payload['plugins'][0]['name'] == 'nested-plugin'
    assert payload['plugins'][0]['credentials'] == '***'
    assert payload['plugins'][0]['settings']['access-token'] == '***'
    assert payload['plugins'][0]['settings']['database_dsn'] == '***'
    assert config['security']['api_key'] == 'api-secret'
    assert config['plugins'][0]['settings']['access-token'] == 'nested-token'


def test_reload_endpoint_reports_not_implemented():
    handler, responses = _make_web_handler({})

    handler._handle_reload()

    assert responses[0][0] == 501
    assert json.loads(responses[0][1])['success'] is False


def test_web_server_partial_request_does_not_block_api_or_shutdown():
    service = SimpleNamespace(
        config={},
        start_time=time.time(),
        get_status=lambda: {'running': True, 'targets': {}},
    )
    web_ui = WebUI(
        service,
        host='127.0.0.1',
        port=0,
        request_timeout=0.5,
    )
    stalled_client = None

    try:
        web_ui.start()
        address = web_ui.server.server_address
        stalled_client = socket.create_connection(address, timeout=1)
        stalled_client.sendall(b'GET /api/status HTTP/1.1\r\nHost: localhost\r\n')

        with socket.create_connection(address, timeout=1) as healthy_client:
            healthy_client.sendall(
                b'GET /api/status HTTP/1.1\r\n'
                b'Host: localhost\r\n'
                b'Connection: close\r\n\r\n'
            )
            response = b''
            while True:
                chunk = healthy_client.recv(4096)
                if not chunk:
                    break
                response += chunk

        assert response.startswith(b'HTTP/1.0 200 OK')
        assert b'"running": true' in response

        started_at = time.monotonic()
        web_ui.stop()
        assert time.monotonic() - started_at < 2
    finally:
        if stalled_client is not None:
            stalled_client.close()
        web_ui.stop()


def test_web_ui_stop_uses_one_deadline_and_tracks_all_shutdown_threads(capsys):
    shutdown_started = threading.Event()
    shutdown_release = threading.Event()
    close_started = threading.Event()
    close_release = threading.Event()
    server_thread_release = threading.Event()
    shutdown_calls = []
    close_calls = []

    class BlockingServer:
        def shutdown(self):
            shutdown_calls.append(True)
            shutdown_started.set()
            shutdown_release.wait()

        def server_close(self):
            close_calls.append(True)
            close_started.set()
            close_release.wait()

    server_thread = threading.Thread(
        target=server_thread_release.wait,
        daemon=True,
    )
    server_thread.start()
    web_ui = WebUI(object())
    web_ui.server = BlockingServer()
    web_ui.server_thread = server_thread

    try:
        started_at = time.monotonic()
        assert web_ui.stop(timeout=0) is False
        assert time.monotonic() - started_at < 0.1
        assert capsys.readouterr().out == "Web UI停止未完成\n"
        assert shutdown_started.wait(1)
        assert close_started.wait(1)

        shutdown_release.set()
        assert web_ui.stop(timeout=0.1) is False
        assert shutdown_calls == [True]
        assert close_calls == [True]

        close_release.set()
        assert web_ui.stop(timeout=0.1) is False

        server_thread_release.set()
        capsys.readouterr()
        assert web_ui.stop(timeout=1) is True
        assert capsys.readouterr().out == "Web UI已停止\n"
        assert web_ui.server is None
        assert web_ui.server_thread is None
        assert web_ui.stop(timeout=0) is True
    finally:
        shutdown_release.set()
        close_release.set()
        server_thread_release.set()
        server_thread.join(timeout=1)


def test_web_ui_stop_before_start_is_idempotent():
    web_ui = WebUI(object())

    assert web_ui.stop(timeout=0) is True
    assert web_ui.stop(timeout=0) is True


def test_web_server_rejects_requests_above_concurrency_limit(monkeypatch):
    accepted = []

    class FakeRequest:
        def __init__(self):
            self.sent = b''
            self.closed = False

        def sendall(self, payload):
            self.sent += payload

        def shutdown(self, _how):
            pass

        def close(self):
            self.closed = True

    monkeypatch.setattr(
        ThreadingHTTPServer,
        'process_request',
        lambda _server, request, _address: accepted.append(request),
    )
    server = _ConcurrentHTTPServer(
        ('127.0.0.1', 0),
        WebUIHandler,
        max_workers=1,
        bind_and_activate=False,
    )
    first = FakeRequest()
    overloaded = FakeRequest()

    try:
        server.process_request(first, ('127.0.0.1', 10001))
        server.process_request(overloaded, ('127.0.0.1', 10002))

        assert accepted == [first]
        assert overloaded.sent.startswith(
            b'HTTP/1.1 503 Service Unavailable'
        )
        assert overloaded.closed is True
    finally:
        server._request_slots.release()
        server.server_close()


def test_web_dashboard_uses_composite_service_health():
    script = object.__new__(WebUIHandler)._get_javascript()

    assert "status.running && status.healthy === true" in script
    assert "statusDot.className = 'status-dot degraded'" in script
    assert "statusText.textContent = '运行异常'" in script


def test_systemd_unit_restarts_after_clean_internal_exit():
    unit_path = Path(__file__).resolve().parents[1] / 'redis-sync.service'
    unit = unit_path.read_text(encoding='utf-8')

    assert 'Restart=always' in unit
    assert 'Restart=on-failure' not in unit


class _FakeRedisClient:
    def __init__(self, identity):
        self.identity = identity
        self.ping_count = 0

    def ping(self):
        self.ping_count += 1
        return True

    def info(self, section=None):
        return {'run_id': repr(self.identity)}


class _FakeConnectionManager:
    def __init__(self):
        self.source_client = None
        self.target_client = None

    def connect_source(self, **kwargs):
        self.source_client = _FakeRedisClient(
            ('host', kwargs['host'], kwargs['port'])
        )
        return self.source_client

    def set_source_client(self, client, config=None, owned=False):
        self.source_client = client

    def connect_target(self, **kwargs):
        self.target_client = _FakeRedisClient(
            ('host', kwargs['host'], kwargs['port'])
        )
        return self.target_client

    def set_target_client(self, client, config=None, owned=False):
        self.target_client = client


def _cli_config(source, target, pattern='*'):
    return Config(
        source=source,
        target=target,
        migration=MigrationSettings(key_pattern=pattern),
        logging=LoggingConfig(console=False, colored=False),
    )


def test_target_url_does_not_replace_host_configured_source(monkeypatch):
    url_clients = {}

    def fake_from_url(url, **kwargs):
        client = _FakeRedisClient(('url', url))
        url_clients[url] = client
        return client

    monkeypatch.setattr(cli_module.redis, 'from_url', fake_from_url)
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(host='source.example', port=6390),
        RedisConfig(url='redis://target.example:6380/0'),
    )

    _connect_redis_instances(manager, config)

    assert manager.source_client.identity == ('host', 'source.example', 6390)
    assert manager.target_client is url_clients['redis://target.example:6380/0']
    assert manager.target_client.ping_count == 1


def test_source_url_does_not_connect_hardcoded_target(monkeypatch):
    def fake_from_url(url, **kwargs):
        return _FakeRedisClient(('url', url))

    monkeypatch.setattr(cli_module.redis, 'from_url', fake_from_url)
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(url='redis://source.example:6379/0'),
        RedisConfig(host='target.example', port=6385),
    )

    _connect_redis_instances(manager, config)

    assert manager.source_client.identity == ('url', 'redis://source.example:6379/0')
    assert manager.source_client.ping_count == 1
    assert manager.target_client.identity == ('host', 'target.example', 6385)


def test_cli_rejects_source_and_target_pointing_to_same_database():
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(url='redis://same.example:6379/3'),
        RedisConfig(host='same.example', port=6379, db=3),
    )

    with pytest.raises(ValueError, match='同一 Redis 数据库'):
        _connect_redis_instances(manager, config)

    assert manager.source_client is None
    assert manager.target_client is None


def test_cli_rejects_host_aliases_resolving_to_same_database(monkeypatch):
    addresses = {
        'redis.internal': '127.0.0.1',
        '127.0.0.1': '127.0.0.1',
    }

    def fake_getaddrinfo(host, port, **kwargs):
        return [
            (
                cli_module.socket.AF_INET,
                cli_module.socket.SOCK_STREAM,
                6,
                '',
                (addresses[host], port),
            )
        ]

    monkeypatch.setattr(cli_module.socket, 'getaddrinfo', fake_getaddrinfo)
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(host='redis.internal', port=6379, db=2),
        RedisConfig(host='127.0.0.1', port=6379, db=2),
    )

    with pytest.raises(ValueError, match='同一 Redis 数据库'):
        _connect_redis_instances(manager, config)

    assert manager.source_client is None
    assert manager.target_client is None


def test_cli_endpoint_check_falls_back_to_normalized_host_on_dns_failure(monkeypatch):
    def fail_resolution(*args, **kwargs):
        raise cli_module.socket.gaierror('lookup failed')

    monkeypatch.setattr(cli_module.socket, 'getaddrinfo', fail_resolution)
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(host='Source.Invalid', port=6379),
        RedisConfig(host='target.invalid', port=6379),
    )

    _connect_redis_instances(manager, config)

    assert manager.source_client.identity == ('host', 'Source.Invalid', 6379)
    assert manager.target_client.identity == ('host', 'target.invalid', 6379)


def test_cli_endpoint_check_handles_distinct_unix_socket_urls(monkeypatch):
    url_clients = {}

    def fake_from_url(url, **kwargs):
        client = _FakeRedisClient(('url', url))
        url_clients[url] = client
        return client

    monkeypatch.setattr(cli_module.redis, 'from_url', fake_from_url)
    manager = _FakeConnectionManager()
    config = _cli_config(
        RedisConfig(url='unix:///tmp/source.sock'),
        RedisConfig(url='unix:///tmp/target.sock'),
    )

    _connect_redis_instances(manager, config)

    assert manager.source_client is url_clients['unix:///tmp/source.sock']
    assert manager.target_client is url_clients['unix:///tmp/target.sock']


def test_omitted_cli_pattern_preserves_configured_pattern(monkeypatch):
    config = _cli_config(RedisConfig(), RedisConfig(port=6380), pattern='user:*')
    monkeypatch.setattr(cli_module, 'load_config', lambda **kwargs: config)
    monkeypatch.setattr(cli_module, 'setup_logging', lambda config: None)

    result = CliRunner().invoke(cli_module.cli, ['migrate', '--dry-run'])

    assert result.exit_code == 0, result.output
    assert 'Key Pattern: user:*' in result.output
    assert config.migration.key_pattern == 'user:*'


def test_explicit_cli_pattern_overrides_configured_pattern(monkeypatch):
    config = _cli_config(RedisConfig(), RedisConfig(port=6380), pattern='user:*')
    monkeypatch.setattr(cli_module, 'load_config', lambda **kwargs: config)
    monkeypatch.setattr(cli_module, 'setup_logging', lambda config: None)

    result = CliRunner().invoke(
        cli_module.cli, ['migrate', '--pattern', 'order:*', '--dry-run']
    )

    assert result.exit_code == 0, result.output
    assert 'Key Pattern: order:*' in result.output
    assert config.migration.key_pattern == 'order:*'


@pytest.mark.parametrize(
    ('arguments', 'option'),
    [
        (['--migration-type', 'full', '--continuous'], '--continuous'),
        (['--migration-type', 'full', '--sync-interval', '1'], '--sync-interval'),
        (['--migration-type', 'full', '--max-changes', '1'], '--max-changes'),
        (['--migration-type', 'incremental', '--clear-target'], '--clear-target'),
        (
            ['--migration-type', 'incremental', '--full-strategy', 'scan'],
            '--full-strategy',
        ),
        (
            ['--migration-type', 'incremental', '--strategy', 'scan'],
            '--strategy',
        ),
        (
            ['--migration-type', 'full', '--strategy', 'incremental'],
            '--strategy',
        ),
        (
            ['--strategy', 'scan', '--full-strategy', 'dump_restore'],
            '--full-strategy',
        ),
        (['--migration-type', 'incremental', '--batch-size', '10'], '--batch-size'),
        (['--migration-type', 'incremental', '--overwrite'], '--overwrite'),
        (['--migration-type', 'incremental', '--no-ttl'], '--no-ttl'),
        (['--migration-type', 'incremental', '--no-verify'], '--no-verify'),
    ],
)
def test_migrate_rejects_options_ignored_by_selected_mode(
    monkeypatch, arguments, option
):
    config = _cli_config(RedisConfig(), RedisConfig(port=6380))
    monkeypatch.setattr(cli_module, 'load_config', lambda **kwargs: config)
    monkeypatch.setattr(cli_module, 'setup_logging', lambda config: None)

    result = CliRunner().invoke(
        cli_module.cli, ['migrate', *arguments, '--dry-run']
    )

    assert result.exit_code == 2
    assert option in result.output


def test_compare_rejects_nonpositive_sample_size(monkeypatch):
    config = _cli_config(RedisConfig(), RedisConfig(port=6380))
    monkeypatch.setattr(cli_module, 'load_config', lambda **kwargs: config)
    monkeypatch.setattr(cli_module, 'setup_logging', lambda config: None)

    result = CliRunner().invoke(
        cli_module.cli, ['compare', '--sample-size', '0']
    )

    assert result.exit_code == 2
    assert 'x>=1' in result.output


def test_invalid_environment_value_is_not_replaced_with_local_defaults(monkeypatch):
    monkeypatch.setenv('REDIS_SOURCE_HOST', 'source.example')
    monkeypatch.setenv('REDIS_TARGET_HOST', 'target.example')
    monkeypatch.setenv('MIGRATION_BATCH_SIZE', 'not-an-integer')

    with pytest.raises(ConfigurationError, match='MIGRATION_BATCH_SIZE'):
        load_config(use_env=True, create_default=True)


@pytest.mark.parametrize(
    ('name', 'value', 'message'),
    [
        ('REDIS_SOURCE_PORT', '70000', r'source\.port'),
        ('REDIS_TARGET_DB', '-1', r'target\.db'),
        ('MIGRATION_STRATEGY', 'psync', r'migration\.strategy'),
        ('MIGRATION_KEY_TYPE', 'module', r'migration\.key_type'),
        ('LOG_LEVEL', 'TRACE', r'logging\.level'),
    ],
)
def test_environment_config_uses_the_same_strict_validation(
    monkeypatch, name, value, message
):
    monkeypatch.setenv(name, value)

    with pytest.raises(ConfigurationError, match=message):
        Config.from_env()


def test_environment_config_normalizes_strategy_key_type_and_log_level(monkeypatch):
    monkeypatch.setenv('MIGRATION_STRATEGY', 'SCAN')
    monkeypatch.setenv('MIGRATION_KEY_TYPE', 'HASH')
    monkeypatch.setenv('LOG_LEVEL', 'debug')
    monkeypatch.setenv('REDIS_SOURCE_SSL', 'yes')

    config = Config.from_env()

    assert config.migration.strategy == 'scan'
    assert config.migration.key_type == 'hash'
    assert config.logging.level == 'DEBUG'
    assert config.source.ssl is True


def test_service_config_file_has_clear_cli_error(tmp_path):
    service_config = {
        'source': {
            'host': '127.0.0.1',
            'port': 6379,
            'socket_timeout': 60,
        },
        'targets': [{'name': 'target-1', 'host': '127.0.0.1', 'port': 6380}],
        'sync': {'mode': 'hybrid'},
        'service': {'logging': {'level': 'INFO'}},
    }
    config_path = tmp_path / 'service.yaml'
    config_path.write_text(yaml.safe_dump(service_config), encoding='utf-8')

    with pytest.raises(ConfigurationError, match='常驻服务配置格式') as exc_info:
        Config.from_file(str(config_path))

    assert 'python -m redis_sync' in str(exc_info.value)


def test_cli_config_from_dict_preserves_defaults_and_does_not_mutate_input():
    data = {
        'source': {'url': 'redis://source.example:6379/2'},
        'target': {'host': 'target.example', 'port': 6380},
        'migration': {'key_type': None, 'retry_delay': 0},
        'logging': {'level': 'debug'},
    }

    config = Config.from_dict(data)

    assert config.source.url == 'redis://source.example:6379/2'
    assert config.target.db == 0
    assert config.migration.batch_size == 100
    assert config.migration.retry_delay == 0
    assert config.logging.level == 'DEBUG'
    assert data['logging']['level'] == 'debug'


@pytest.mark.parametrize(
    ('data', 'message'),
    [
        (None, '配置顶层必须是映射'),
        ([], '配置顶层必须是映射'),
        ({'source': []}, 'source 必须是映射'),
        ({'target': None}, 'target 必须是映射'),
        ({'migration': 'scan'}, 'migration 必须是映射'),
        ({'logging': True}, 'logging 必须是映射'),
    ],
)
def test_cli_config_rejects_non_mapping_data_and_sections(data, message):
    with pytest.raises(ConfigurationError, match=message):
        Config.from_dict(data)


@pytest.mark.parametrize(
    'data',
    [
        {'surce': {}},
        {'source': {'socket_timeout': 30}},
        {'target': {'name': 'target-a'}},
        {'migration': {'continuous_sync': True}},
        {'logging': {'datefmt': '%Y-%m-%d'}},
    ],
    ids=['top-level', 'source', 'target', 'migration', 'logging'],
)
def test_cli_config_rejects_unknown_fields(data):
    with pytest.raises(ConfigurationError, match='未知字段'):
        Config.from_dict(data)


@pytest.mark.parametrize(
    ('section', 'field'),
    [
        ('source', 'ssl'),
        ('target', 'ssl'),
        ('migration', 'preserve_ttl'),
        ('migration', 'overwrite_existing'),
        ('migration', 'enable_replication'),
        ('migration', 'verify_migration'),
        ('logging', 'console'),
        ('logging', 'colored'),
    ],
)
def test_cli_config_rejects_string_pseudo_booleans(section, field):
    with pytest.raises(ConfigurationError, match=rf'{section}\.{field}.*布尔值'):
        Config.from_dict({section: {field: 'false'}})


def test_cli_config_rejects_unsupported_replication_switch():
    with pytest.raises(ConfigurationError, match=r'enable_replication.*一次性'):
        Config.from_dict({'migration': {'enable_replication': True}})


@pytest.mark.parametrize(
    ('data', 'message'),
    [
        ({'source': {'host': ''}}, r'source\.host'),
        ({'source': {'host': 123}}, r'source\.host'),
        ({'target': {'port': 0}}, r'target\.port'),
        ({'target': {'port': 65536}}, r'target\.port'),
        ({'source': {'port': '6379'}}, r'source\.port'),
        ({'source': {'db': -1}}, r'source\.db'),
        ({'source': {'url': 'https://redis.example/0'}}, r'source\.url'),
        ({'source': {'url': 'redis:///0'}}, r'source\.url'),
        ({'source': {'url': 'redis://redis.example/not-a-db'}}, r'source\.url'),
        ({'target': {'password': 123}}, r'target\.password'),
    ],
)
def test_cli_config_rejects_invalid_redis_settings(data, message):
    with pytest.raises(ConfigurationError, match=message):
        Config.from_dict(data)


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('batch_size', 0),
        ('batch_size', '100'),
        ('batch_size', True),
        ('scan_count', 0),
        ('replication_port', 65536),
        ('replication_timeout', 0),
        ('max_retries', 0),
        ('retry_delay', -0.1),
        ('retry_delay', float('inf')),
    ],
)
def test_cli_config_rejects_invalid_migration_numeric_settings(field, value):
    with pytest.raises(ConfigurationError, match=rf'migration\.{field}'):
        Config.from_dict({'migration': {field: value}})


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('strategy', 'hybrid'),
        ('strategy', 1),
        ('key_pattern', ''),
        ('key_pattern', None),
        ('key_type', ''),
        ('key_type', 'module'),
    ],
)
def test_cli_config_rejects_invalid_migration_text_settings(field, value):
    with pytest.raises(ConfigurationError, match=rf'migration\.{field}'):
        Config.from_dict({'migration': {field: value}})


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('level', 'TRACE'),
        ('level', 20),
        ('format', None),
        ('file', 123),
    ],
)
def test_cli_config_rejects_invalid_logging_strings(field, value):
    with pytest.raises(ConfigurationError, match=rf'logging\.{field}'):
        Config.from_dict({'logging': {field: value}})


def test_cli_config_from_file_applies_strict_validation(tmp_path):
    config_path = tmp_path / 'cli.yaml'
    config_path.write_text(
        yaml.safe_dump({'migration': {'preserve_ttl': 'false'}}),
        encoding='utf-8',
    )

    with pytest.raises(ConfigurationError, match=r'migration\.preserve_ttl.*布尔值'):
        Config.from_file(str(config_path))
