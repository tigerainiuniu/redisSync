#!/usr/bin/env python3
"""
Redis同步服务启动脚本

使用方法:
    python run_sync_service.py
    python run_sync_service.py --config custom_config.yaml
"""

import sys
import argparse
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from redis_sync.sync_service import RedisSyncService


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Redis同步服务 - 支持一对多Redis实例的持续同步',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
示例:
    # 使用默认配置文件启动
    python run_sync_service.py
    
    # 使用自定义配置文件启动
    python run_sync_service.py --config /path/to/config.yaml
    
    # 检查配置文件
    python run_sync_service.py --check-config
        '''
    )
    
    parser.add_argument(
        '--config', '-c',
        default='config.yaml',
        help='配置文件路径 (默认: config.yaml)'
    )
    
    parser.add_argument(
        '--check-config',
        action='store_true',
        help='检查配置文件并退出'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='Redis Sync Service 1.0.0'
    )
    
    args = parser.parse_args()
    
    # 检查配置文件是否存在
    config_path = Path(args.config)
    if not config_path.exists():
        print(f"❌ 配置文件不存在: {config_path}")
        print(f"💡 请创建配置文件或使用 --config 指定正确的路径")
        
        # 如果是默认配置文件，提供创建提示
        if args.config == 'config.yaml':
            print(f"💡 你可以复制示例配置文件:")
            print(f"   cp config.yaml.example config.yaml")
        
        sys.exit(1)
    
    try:
        # 创建服务实例
        service = RedisSyncService(str(config_path))
        
        # 如果只是检查配置，则输出配置信息并退出
        if args.check_config:
            print("✅ 配置文件检查通过")
            print(f"📁 配置文件: {config_path.absolute()}")
            
            config = service.config
            
            # 显示源Redis配置
            source = config['source']
            print(f"📡 源Redis: {source['host']}:{source['port']} (DB: {source.get('db', 0)})")
            
            # 显示目标Redis配置
            targets = config['targets']
            enabled_targets = [t for t in targets if t.get('enabled', True)]
            print(f"🎯 目标Redis数量: {len(enabled_targets)}")
            
            for target in enabled_targets:
                print(f"   - {target['name']}: {target['host']}:{target['port']} (DB: {target.get('db', 0)})")
            
            # 显示同步配置
            sync_config = config['sync']
            print(f"🔄 同步模式: {sync_config['mode']}")
            
            if sync_config.get('incremental_sync', {}).get('enabled', True):
                interval = sync_config['incremental_sync'].get('interval', 30)
                print(f"⏱️  增量同步间隔: {interval}秒")
            
            # 显示Web UI配置
            web_ui = config.get('web_ui', {})
            if web_ui.get('enabled', True):
                host = web_ui.get('host', '127.0.0.1')
                port = web_ui.get('port', 8080)
                print(f"🌐 Web管理界面: http://{host}:{port}")
            
            return
        
        # 显示启动信息
        print("🚀 启动Redis同步服务...")
        print(f"📁 配置文件: {config_path.absolute()}")
        
        # 显示Web UI地址
        web_ui = service.config.get('web_ui', {})
        if web_ui.get('enabled', True):
            host = web_ui.get('host', '127.0.0.1')
            port = web_ui.get('port', 8080)
            if host == '0.0.0.0':
                print(f"🌐 Web管理界面: http://localhost:{port}")
            else:
                print(f"🌐 Web管理界面: http://{host}:{port}")
        
        print("📝 按 Ctrl+C 停止服务")
        print("-" * 50)
        
        # 运行服务
        service.run()
        
    except KeyboardInterrupt:
        print("\n👋 服务已停止")
    except Exception as e:
        print(f"❌ 服务启动失败: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
