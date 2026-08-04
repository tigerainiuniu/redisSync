"""
Redis Sync工具的命令行界面。

为Redis迁移和同步操作提供全面的CLI。
"""

import json
import socket
import sys
import time

import click
import redis
from tqdm import tqdm
from urllib.parse import parse_qs, urlparse

from .config import Config, setup_logging, create_sample_config, load_config
from .connection_manager import (
    RedisConnectionManager,
    assert_distinct_redis_databases,
)
from .migration_orchestrator import MigrationOrchestrator, MigrationConfig, MigrationStrategy


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='配置文件路径')
@click.option('--verbose', '-v', is_flag=True, help='启用详细日志')
@click.option('--quiet', '-q', is_flag=True, help='除错误外抑制输出')
@click.pass_context
def cli(ctx, config, verbose, quiet):
    """Redis Sync - 全面的Redis迁移和同步工具。"""
    ctx.ensure_object(dict)
    
    # Load configuration
    try:
        if config:
            ctx.obj['config'] = Config.from_file(config)
        else:
            ctx.obj['config'] = load_config(use_env=True, create_default=True)
    except Exception as e:
        click.echo(f"Error loading configuration: {e}", err=True)
        sys.exit(1)
    
    # Adjust logging level based on flags
    if verbose:
        ctx.obj['config'].logging.level = 'DEBUG'
    elif quiet:
        ctx.obj['config'].logging.level = 'ERROR'
        ctx.obj['config'].logging.console = False
    
    # Setup logging
    setup_logging(ctx.obj['config'].logging)


@cli.command()
@click.option('--output', '-o', type=click.Path(), default='redis-sync-config.yaml',
              help='输出配置文件路径')
def init(output):
    """初始化示例配置文件。"""
    try:
        config_content = create_sample_config()
        
        with open(output, 'w') as f:
            f.write(config_content)
        
        click.echo(f"Sample configuration created at: {output}")
        click.echo("Please edit the configuration file to match your Redis instances.")
        
    except Exception as e:
        click.echo(f"Error creating configuration: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--strategy', '-s',
              type=click.Choice([
                  'scan', 'sync', 'dump_restore', 'full', 'incremental'
              ]),
              help='迁移策略（全量迁移使用 scan、sync 或 dump_restore）')
@click.option('--migration-type',
              type=click.Choice(['full', 'incremental']),
              default=None, help='迁移类型：全量或增量')
@click.option('--full-strategy',
              type=click.Choice(['scan', 'sync', 'dump_restore']),
              default=None, help='全量迁移子策略')
@click.option('--pattern', '-p', default=None, help='要迁移的键模式')
@click.option('--key-type', '-t',
              type=click.Choice(['string', 'list', 'set', 'zset', 'hash', 'stream']),
              help='按键类型过滤')
@click.option('--batch-size', '-b', type=click.IntRange(min=1), help='处理批大小')
@click.option('--overwrite', is_flag=True, help='覆盖现有键')
@click.option('--no-ttl', is_flag=True, help='不保持TTL值')
@click.option('--no-verify', is_flag=True, help='跳过迁移验证')
@click.option('--enable-replication', is_flag=True, help='启用持续复制')
@click.option('--clear-target', is_flag=True, help='清空目标数据库（仅全量迁移）')
@click.option('--sync-interval', type=click.IntRange(min=1), default=None,
              help='增量同步间隔（秒）')
@click.option('--max-changes', type=click.IntRange(min=1), default=None,
              help='每次同步的最大变更数')
@click.option('--continuous', is_flag=True, help='启用持续增量同步')
@click.option('--include', multiple=True, help='键名包含模式（glob），可多次指定')
@click.option('--exclude', multiple=True, help='键名排除模式（glob），可多次指定')
@click.option('--min-ttl', type=click.IntRange(min=0), default=None,
              help='最小TTL过滤（秒）')
@click.option('--max-key-size', type=click.IntRange(min=0), default=None,
              help='最大键内存过滤（字节）')
@click.option('--dry-run', is_flag=True, help='显示将要迁移的内容而不实际执行')
@click.pass_context
def migrate(ctx, strategy, migration_type, full_strategy, pattern, key_type, batch_size,
           overwrite, no_ttl, no_verify, enable_replication, clear_target,
           sync_interval, max_changes, continuous,
           include, exclude, min_ttl, max_key_size, dry_run):
    """从源Redis实例迁移数据到目标Redis实例。"""
    config = ctx.obj['config']
    requested_strategy = strategy
    requested_full_strategy = full_strategy

    # 使用CLI选项覆盖配置
    if strategy:
        config.migration.strategy = strategy
    if pattern is not None:
        config.migration.key_pattern = pattern
    if key_type:
        config.migration.key_type = key_type
    if batch_size is not None:
        config.migration.batch_size = batch_size
    if overwrite:
        config.migration.overwrite_existing = True
    if no_ttl:
        config.migration.preserve_ttl = False
    if no_verify:
        config.migration.verify_migration = False
    if enable_replication or config.migration.enable_replication:
        raise click.UsageError(
            "--enable-replication 不适用于一次性 migrate 命令；"
            "请在常驻服务配置 sync.incremental_sync.method 中使用 psync"
        )

    configured_strategy = config.migration.strategy
    if migration_type is None and configured_strategy in {'full', 'incremental'}:
        migration_type = configured_strategy
    migration_type = (
        migration_type
        or getattr(config.migration, 'migration_type', None)
        or 'full'
    )

    if migration_type == 'full':
        if requested_strategy == 'incremental':
            raise click.UsageError(
                "--strategy incremental 与 --migration-type full 冲突"
            )
        if (
            requested_strategy in {'scan', 'sync', 'dump_restore'}
            and requested_full_strategy is not None
            and requested_strategy != requested_full_strategy
        ):
            raise click.UsageError(
                "--strategy 与 --full-strategy 指定了不同的全量迁移策略"
            )
        if continuous:
            raise click.UsageError("--continuous 仅适用于增量迁移")
        if sync_interval is not None:
            raise click.UsageError("--sync-interval 仅适用于增量迁移")
        if max_changes is not None:
            raise click.UsageError("--max-changes 仅适用于增量迁移")
    else:
        if requested_strategy not in {None, 'incremental'}:
            raise click.UsageError(
                f"--strategy {requested_strategy} 与 "
                "--migration-type incremental 冲突"
            )
        if requested_full_strategy is not None:
            raise click.UsageError("--full-strategy 仅适用于全量迁移")
        if clear_target:
            raise click.UsageError("--clear-target 仅适用于全量迁移")
        if batch_size is not None:
            raise click.UsageError("--batch-size 不受增量迁移支持")
        if overwrite:
            raise click.UsageError("--overwrite 不受增量迁移支持")
        if no_ttl:
            raise click.UsageError(
                "--no-ttl 不受增量迁移支持；增量迁移始终同步源键 TTL"
            )
        if no_verify:
            raise click.UsageError("--no-verify 不受增量迁移支持")

    if full_strategy is None:
        full_strategy = getattr(config.migration, 'full_strategy', None)
    if full_strategy is None and configured_strategy in {
        'scan', 'sync', 'dump_restore'
    }:
        full_strategy = configured_strategy
    if full_strategy is None:
        if configured_strategy in {'full', 'incremental'}:
            full_strategy = 'scan'
        else:
            raise click.UsageError(
                f"策略 {configured_strategy!r} 不能作为全量迁移子策略；"
                "请指定 --full-strategy scan、sync 或 dump_restore"
            )
    if sync_interval is None:
        sync_interval = getattr(config.migration, 'sync_interval', None)
        if sync_interval is None:
            sync_interval = 60
    if max_changes is None:
        max_changes = getattr(config.migration, 'max_changes_per_sync', None)
        if max_changes is None:
            max_changes = 10000
    if min_ttl is None:
        min_ttl = getattr(config.migration, 'filter_min_ttl', None)
        if min_ttl is None:
            min_ttl = 0
    if max_key_size is None:
        max_key_size = getattr(config.migration, 'filter_max_key_size', None)
        if max_key_size is None:
            max_key_size = 0
    
    if dry_run:
        click.echo("DRY RUN MODE - No actual migration will be performed")
        _show_migration_plan(config)
        return
    
    # Perform migration
    try:
        with RedisConnectionManager() as conn_manager:
            # Connect to Redis instances
            _connect_redis_instances(
                conn_manager, config, require_identity_probe=True
            )
            
            # Initialize orchestrator
            orchestrator = MigrationOrchestrator(conn_manager)
            
            # 创建迁移配置
            from .migration_orchestrator import MigrationType

            migration_config = MigrationConfig(
                strategy=MigrationStrategy(config.migration.strategy),
                migration_type=MigrationType(migration_type),
                batch_size=config.migration.batch_size,
                scan_count=config.migration.scan_count,
                preserve_ttl=config.migration.preserve_ttl,
                overwrite_existing=config.migration.overwrite_existing,
                key_pattern=config.migration.key_pattern,
                key_type=config.migration.key_type,
                enable_replication=config.migration.enable_replication,
                verify_migration=config.migration.verify_migration,
                progress_callback=_create_progress_callback(),
                # 全量迁移选项
                clear_target=clear_target,
                full_strategy=full_strategy,
                # 增量迁移选项
                sync_interval=sync_interval,
                max_changes_per_sync=max_changes,
                continuous_sync=continuous,
                # 过滤选项
                include_patterns=list(include) if include else None,
                exclude_patterns=list(exclude) if exclude else None,
                filter_min_ttl=min_ttl,
                filter_max_key_size=max_key_size,
            )
            
            # 执行迁移
            click.echo(f"开始迁移，策略: {config.migration.strategy}，类型: {migration_type}")

            if migration_type == 'incremental' and continuous:
                # 持续增量同步
                success = orchestrator.start_incremental_sync(migration_config)
                if success:
                    click.echo("持续增量同步已启动。按Ctrl+C停止。")
                    try:
                        while True:
                            time.sleep(10)
                            stats = orchestrator.get_incremental_stats()
                            if stats.get('total_changes', 0) > 0:
                                click.echo(f"增量统计: 总变更{stats['total_changes']}, "
                                         f"成功{stats['successful_changes']}, "
                                         f"失败{stats['failed_changes']}")
                    except KeyboardInterrupt:
                        click.echo("\n停止增量同步...")
                        stop_result = orchestrator.stop_incremental_sync()
                        click.echo(f"增量同步已停止: {stop_result}")
                else:
                    click.echo("启动持续增量同步失败", err=True)
                    sys.exit(1)
            else:
                # 常规迁移
                results = orchestrator.migrate(migration_config)

                # 显示结果
                _display_migration_results(results)

                if not results['success']:
                    sys.exit(1)

    except Exception as e:
        click.echo(f"Migration failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--pattern', '-p', default=None, help='Key pattern to compare')
@click.option(
    '--sample-size', '-n', type=click.IntRange(min=1),
    help='Limit comparison to sample size',
)
@click.option('--output', '-o', type=click.Path(), help='Save comparison results to file')
@click.pass_context
def compare(ctx, pattern, sample_size, output):
    """Compare keys between source and target Redis instances."""
    config = ctx.obj['config']
    if pattern is None:
        pattern = config.migration.key_pattern
    
    try:
        with RedisConnectionManager() as conn_manager:
            # Connect to Redis instances
            _connect_redis_instances(conn_manager, config)
            
            # Initialize orchestrator
            orchestrator = MigrationOrchestrator(conn_manager)
            orchestrator.initialize_handlers()
            
            # Perform comparison
            click.echo(f"Comparing keys with pattern: {pattern}")
            results = orchestrator.scan_handler.compare_keys(
                pattern=pattern,
                sample_size=sample_size
            )
            
            # Display results
            _display_comparison_results(results)
            
            # Save to file if requested
            if output:
                with open(output, 'w') as f:
                    json.dump(results, f, indent=2)
                click.echo(f"Comparison results saved to: {output}")
                
    except Exception as e:
        click.echo(f"Comparison failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def status(ctx):
    """Show current migration and replication status."""
    config = ctx.obj['config']
    
    try:
        with RedisConnectionManager() as conn_manager:
            # Connect to Redis instances
            _connect_redis_instances(conn_manager, config)
            
            # Initialize orchestrator
            orchestrator = MigrationOrchestrator(conn_manager)
            orchestrator.initialize_handlers()
            
            # Get status
            status_info = orchestrator.get_migration_status()
            
            # Display status
            _display_status(status_info)
            
    except Exception as e:
        click.echo(f"Failed to get status: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def info(ctx):
    """Show Redis instance information."""
    config = ctx.obj['config']
    
    try:
        with RedisConnectionManager() as conn_manager:
            # Connect to Redis instances
            _connect_redis_instances(conn_manager, config)
            
            # Get Redis info
            source_info = conn_manager.get_source_info()
            target_info = conn_manager.get_target_info()
            
            # Display info
            click.echo("=== Source Redis Info ===")
            _display_redis_info(source_info)
            
            click.echo("\n=== Target Redis Info ===")
            _display_redis_info(target_info)
            
    except Exception as e:
        click.echo(f"Failed to get Redis info: {e}", err=True)
        sys.exit(1)


def _connect_redis_instances(
    conn_manager: RedisConnectionManager,
    config: Config,
    *,
    require_identity_probe: bool = False,
):
    """Connect to source and target Redis instances."""
    if _redis_configs_share_endpoint(config.source, config.target):
        raise ValueError(
            "source 与 target 指向同一 Redis 数据库，迁移可能清空源数据"
        )

    # Connect to source
    if config.source.url:
        source_client = redis.from_url(
            config.source.url, decode_responses=False
        )
        source_client.ping()
        conn_manager.set_source_client(source_client, owned=True)
    else:
        conn_manager.connect_source(**config.source.to_dict())
    
    # Connect to target
    if config.target.url:
        target_client = redis.from_url(
            config.target.url, decode_responses=False
        )
        target_client.ping()
        conn_manager.set_target_client(target_client, owned=True)
    else:
        conn_manager.connect_target(**config.target.to_dict())

    source_db = _redis_config_endpoint(config.source)[2]
    target_db = _redis_config_endpoint(config.target)[2]
    assert_distinct_redis_databases(
        conn_manager.source_client,
        conn_manager.target_client,
        source_db,
        target_db,
        allow_marker=require_identity_probe,
    )


def _redis_config_endpoint(config):
    """Return the effective host/port/db tuple before opening a connection."""
    if config.url:
        parsed = urlparse(config.url)
        query = parse_qs(parsed.query)
        if parsed.scheme == "unix":
            db = int(query["db"][-1]) if query.get("db") else 0
            return (f"unix:{parsed.path}", 0, db)
        if query.get("db"):
            db = int(query["db"][-1])
        elif parsed.path and parsed.path != "/":
            db = int(parsed.path.strip("/") or 0)
        else:
            db = 0
        return ((parsed.hostname or "").lower(), parsed.port or 6379, db)
    return (str(config.host).strip().lower(), int(config.port), int(config.db))


def _redis_configs_share_endpoint(source, target):
    """Check host aliases as well as literal endpoint equality."""
    source_host, source_port, source_db = _redis_config_endpoint(source)
    target_host, target_port, target_db = _redis_config_endpoint(target)
    if source_port != target_port or source_db != target_db:
        return False
    if source_host == target_host:
        return True
    if source_host.startswith("unix:") or target_host.startswith("unix:"):
        return False

    def _addresses(host, port):
        try:
            return {
                address[4][0].lower()
                for address in socket.getaddrinfo(
                    host, port, type=socket.SOCK_STREAM
                )
            }
        except OSError:
            return set()

    source_addresses = _addresses(source_host, source_port)
    target_addresses = _addresses(target_host, target_port)
    return bool(source_addresses and source_addresses.intersection(target_addresses))


def _show_migration_plan(config: Config):
    """Show migration plan for dry run."""
    click.echo(f"Migration Strategy: {config.migration.strategy}")
    click.echo(f"Key Pattern: {config.migration.key_pattern}")
    click.echo(f"Key Type Filter: {config.migration.key_type or 'All types'}")
    click.echo(f"Batch Size: {config.migration.batch_size}")
    click.echo(f"Preserve TTL: {config.migration.preserve_ttl}")
    click.echo(f"Overwrite Existing: {config.migration.overwrite_existing}")
    click.echo(f"Verify Migration: {config.migration.verify_migration}")
    click.echo(f"Enable Replication: {config.migration.enable_replication}")


def _create_progress_callback():
    """Create a progress callback using tqdm."""
    pbar = None
    
    def progress_callback(current: int, total: int):
        nonlocal pbar
        if pbar is None:
            pbar = tqdm(total=total, desc="Migrating keys", unit="keys")
        pbar.update(current - pbar.n)
        if current >= total:
            pbar.close()
            pbar = None
    
    return progress_callback


def _display_migration_results(results: dict):
    """Display migration results."""
    click.echo(f"\n=== Migration Results ===")
    click.echo(f"Strategy: {results['strategy']}")
    click.echo(f"Success: {results['success']}")
    click.echo(f"Duration: {results['duration']:.2f} seconds")
    
    if 'statistics' in results:
        stats = results['statistics']
        if isinstance(stats, dict):
            for key, value in stats.items():
                if isinstance(value, (int, float)):
                    click.echo(f"{key.replace('_', ' ').title()}: {value}")
    
    if 'verification' in results:
        verification = results['verification']
        click.echo(f"\n=== Verification Results ===")
        click.echo(f"Success: {verification.get('success', False)}")
        if 'success_rate' in verification:
            click.echo(f"Success Rate: {verification['success_rate']:.2%}")
    
    if results.get('errors'):
        click.echo(f"\n=== Errors ===")
        for error in results['errors'][:5]:  # Show first 5 errors
            click.echo(f"- {error}")


def _display_comparison_results(results: dict):
    """Display comparison results."""
    click.echo(f"\n=== Comparison Results ===")
    click.echo(f"Total Compared: {results.get('total_compared', 0)}")
    click.echo(f"Matching Keys: {results.get('matching_keys', 0)}")
    click.echo(f"Missing in Target: {results.get('missing_in_target', 0)}")
    click.echo(f"Value Mismatches: {results.get('value_mismatches', 0)}")
    click.echo(f"TTL Mismatches: {results.get('ttl_mismatches', 0)}")
    click.echo(f"Type Mismatches: {results.get('type_mismatches', 0)}")
    
    if results.get('errors'):
        click.echo(f"\nErrors: {len(results['errors'])}")


def _display_status(status: dict):
    """Display migration status."""
    click.echo(f"=== Migration Status ===")
    click.echo(f"Source Connected: {status.get('source_connected', False)}")
    click.echo(f"Target Connected: {status.get('target_connected', False)}")
    click.echo(f"Replication Active: {status.get('replication_active', False)}")
    
    if 'replication_lag' in status and status['replication_lag'] is not None:
        click.echo(f"Replication Lag: {status['replication_lag']:.2f} seconds")


def _display_redis_info(info: dict):
    """Display Redis instance information."""
    important_keys = [
        'redis_version', 'role', 'connected_clients', 'used_memory_human',
        'keyspace_hits', 'keyspace_misses', 'total_commands_processed'
    ]
    
    for key in important_keys:
        if key in info:
            click.echo(f"{key.replace('_', ' ').title()}: {info[key]}")


def main():
    """Main entry point."""
    cli()


if __name__ == '__main__':
    main()
