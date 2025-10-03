"""
Redis Sync工具的命令行界面。

为Redis迁移和同步操作提供全面的CLI。
"""

import click
import sys
import time
import json
from typing import Optional
from pathlib import Path
from tqdm import tqdm

from .config import Config, setup_logging, create_sample_config, load_config
from .connection_manager import RedisConnectionManager
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
              type=click.Choice(['scan', 'sync', 'psync', 'hybrid', 'full', 'incremental']),
              help='迁移策略')
@click.option('--migration-type',
              type=click.Choice(['full', 'incremental']),
              default='full', help='迁移类型：全量或增量')
@click.option('--full-strategy',
              type=click.Choice(['scan', 'sync', 'dump_restore']),
              default='scan', help='全量迁移子策略')
@click.option('--pattern', '-p', default='*', help='要迁移的键模式')
@click.option('--key-type', '-t',
              type=click.Choice(['string', 'list', 'set', 'zset', 'hash', 'stream']),
              help='按键类型过滤')
@click.option('--batch-size', '-b', type=int, help='处理批大小')
@click.option('--overwrite', is_flag=True, help='覆盖现有键')
@click.option('--no-ttl', is_flag=True, help='不保持TTL值')
@click.option('--no-verify', is_flag=True, help='跳过迁移验证')
@click.option('--enable-replication', is_flag=True, help='启用持续复制')
@click.option('--clear-target', is_flag=True, help='清空目标数据库（仅全量迁移）')
@click.option('--sync-interval', type=int, default=60, help='增量同步间隔（秒）')
@click.option('--max-changes', type=int, default=10000, help='每次同步的最大变更数')
@click.option('--continuous', is_flag=True, help='启用持续增量同步')
@click.option('--dry-run', is_flag=True, help='显示将要迁移的内容而不实际执行')
@click.pass_context
def migrate(ctx, strategy, migration_type, full_strategy, pattern, key_type, batch_size,
           overwrite, no_ttl, no_verify, enable_replication, clear_target,
           sync_interval, max_changes, continuous, dry_run):
    """从源Redis实例迁移数据到目标Redis实例。"""
    config = ctx.obj['config']

    # 使用CLI选项覆盖配置
    if strategy:
        config.migration.strategy = strategy
    if migration_type:
        # 设置迁移类型
        pass  # 将在创建MigrationConfig时处理
    if pattern:
        config.migration.key_pattern = pattern
    if key_type:
        config.migration.key_type = key_type
    if batch_size:
        config.migration.batch_size = batch_size
    if overwrite:
        config.migration.overwrite_existing = True
    if no_ttl:
        config.migration.preserve_ttl = False
    if no_verify:
        config.migration.verify_migration = False
    if enable_replication:
        config.migration.enable_replication = True
    
    if dry_run:
        click.echo("DRY RUN MODE - No actual migration will be performed")
        _show_migration_plan(config)
        return
    
    # Perform migration
    try:
        with RedisConnectionManager() as conn_manager:
            # Connect to Redis instances
            _connect_redis_instances(conn_manager, config)
            
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
                continuous_sync=continuous
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
            
            if not results['success']:
                sys.exit(1)
                
    except Exception as e:
        click.echo(f"Migration failed: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--pattern', '-p', default='*', help='Key pattern to compare')
@click.option('--sample-size', '-n', type=int, help='Limit comparison to sample size')
@click.option('--output', '-o', type=click.Path(), help='Save comparison results to file')
@click.pass_context
def compare(ctx, pattern, sample_size, output):
    """Compare keys between source and target Redis instances."""
    config = ctx.obj['config']
    
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


def _connect_redis_instances(conn_manager: RedisConnectionManager, config: Config):
    """Connect to source and target Redis instances."""
    # Connect to source
    if config.source.url:
        source_client = conn_manager.source_client = conn_manager.connect_from_url(
            config.source.url, config.target.url or "redis://localhost:6380"
        )[0]
    else:
        source_client = conn_manager.connect_source(**config.source.to_dict())
    
    # Connect to target
    if config.target.url:
        if not config.source.url:
            target_client = conn_manager.target_client = conn_manager.connect_from_url(
                "redis://localhost:6379", config.target.url
            )[1]
    else:
        target_client = conn_manager.connect_target(**config.target.to_dict())


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
