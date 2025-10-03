#!/usr/bin/env python3
"""
统一增量同步服务

支持三种增量同步模式：
1. SCAN 模式：使用 SCAN + IDLETIME 轮询检测变更
2. SYNC 模式：使用 SYNC 命令接收 RDB + 命令流
3. PSYNC 模式：使用 PSYNC 命令接收 RDB + 命令流（支持部分同步）

用户可以通过配置文件自由选择模式
"""

import logging
import threading
import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
import redis

from .connection_manager import RedisConnectionManager
from .psync_incremental_handler import PSyncIncrementalHandler

logger = logging.getLogger(__name__)


class UnifiedIncrementalService:
    """统一增量同步服务，支持 SCAN/SYNC/PSYNC 三种模式"""
    
    def __init__(self,
                 mode: str,
                 source_conn,  # redis.Redis 或 RedisConnectionManager
                 target_connections: Dict[str, RedisConnectionManager],
                 config: Dict[str, Any]):
        """
        初始化统一增量同步服务

        参数:
            mode: 增量同步模式 ("scan", "sync", "psync")
            source_conn: 源Redis连接（redis.Redis 或 RedisConnectionManager）
            target_connections: 目标Redis连接管理器字典
            config: 增量同步配置
        """
        self.mode = mode.lower()
        self.source_conn = source_conn
        self.target_connections = target_connections
        self.config = config

        # 获取源Redis客户端
        if hasattr(source_conn, 'source_client'):
            # RedisConnectionManager
            self.source_client = source_conn.source_client
        else:
            # redis.Redis
            self.source_client = source_conn
        
        # 验证模式
        if self.mode not in ['scan', 'sync', 'psync']:
            raise ValueError(f"不支持的增量同步模式: {mode}，支持的模式: scan, sync, psync")
        
        # 运行状态
        self.running = False
        self.shutdown_event = threading.Event()
        
        # 统计信息
        self.stats = {
            'mode': self.mode,
            'commands_received': 0,
            'commands_synced': 0,
            'commands_failed': 0,
            'commands_skipped': 0,
            'commands_duplicated': 0,  # 去重的命令数
            'start_time': None,
            'last_command_time': None,
            'command_types': {}  # 统计每种命令的数量
        }

        # 命令去重：记录最近处理的命令（使用 LRU 缓存）
        from collections import OrderedDict
        self.recent_commands = OrderedDict()
        self.max_recent_commands = 1000  # 最多记录 1000 个最近命令
        self.command_dedup_window = 5  # 5秒内的重复命令会被去重
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=8)
        
        # 模式特定的处理器
        self.handler = None
        
        logger.info(f"统一增量同步服务初始化完成，模式: {self.mode.upper()}")
    
    def start(self):
        """启动增量同步服务"""
        if self.running:
            logger.warning("⚠️  增量同步服务已在运行")
            return

        self.running = True
        self.stats['start_time'] = time.time()

        logger.info("=" * 60)
        logger.info(f"🚀 启动增量同步服务 - {self.mode.upper()} 模式")
        logger.info("=" * 60)
        logger.info(f"📊 目标数量: {len(self.target_connections)}")
        for target_name in self.target_connections.keys():
            logger.info(f"  ➡️  {target_name}")
        logger.info("=" * 60)

        # 根据模式启动对应的服务
        if self.mode == 'scan':
            logger.info("📡 使用 SCAN 模式...")
            self._start_scan_mode()
        elif self.mode == 'sync':
            logger.info("📡 使用 SYNC 模式...")
            self._start_sync_mode()
        elif self.mode == 'psync':
            logger.info("📡 使用 PSYNC 实时复制模式...")
            self._start_psync_mode()
    
    def stop(self):
        """停止增量同步服务"""
        if not self.running:
            logger.debug("⚠️  增量同步服务未运行")
            return

        logger.info("=" * 60)
        logger.info(f"🛑 停止增量同步服务 - {self.mode.upper()} 模式...")
        logger.info("=" * 60)

        self.running = False
        self.shutdown_event.set()

        # 停止处理器
        if self.handler:
            logger.info("🛑 停止处理器...")
            if hasattr(self.handler, 'stop_replication'):
                self.handler.stop_replication()
                logger.info("✅ PSYNC 处理器已停止")

        # 关闭线程池
        logger.info("🛑 关闭线程池...")
        self.executor.shutdown(wait=True)
        logger.info("✅ 线程池已关闭")

        # 打印统计信息
        self._print_stats()

        logger.info("=" * 60)
        logger.info("✅ 增量同步服务已停止")
        logger.info("=" * 60)
    
    def _start_scan_mode(self):
        """启动 SCAN 模式（IDLETIME 轮询）"""
        logger.info("📋 SCAN 模式说明:")
        logger.info("  - 使用 SCAN 命令遍历所有键（不阻塞）")
        logger.info("  - 使用 OBJECT IDLETIME 检测变更")
        logger.info("  - Pipeline 批量操作")
        logger.info("  - 轮询间隔: {} 秒".format(self.config.get('interval', 5)))
        logger.info("=" * 60)
        
        # SCAN 模式由主服务的 _perform_unified_incremental_sync 处理
        # 这里只是标记模式，实际逻辑在 sync_service.py 中
        logger.info("✅ SCAN 模式已启动（由主服务协调）")
    
    def _start_sync_mode(self):
        """启动 SYNC 模式"""
        logger.info("📋 SYNC 模式说明:")
        logger.info("  1. 伪装成 Redis 从库")
        logger.info("  2. 发送 SYNC 命令")
        logger.info("  3. 接收 RDB（全量）→ 跳过（已完成全量同步）")
        logger.info("  4. 持续接收命令流（增量）")
        logger.info("  5. 实时转发到所有目标（并行）")
        logger.info("=" * 60)

        try:
            # 创建 SYNC 处理器
            from .sync_handler import SyncHandler

            # 获取连接
            connection = self.source_client.connection_pool.get_connection('SYNC')
            
            try:
                # 发送 SYNC 命令
                logger.info("📤 发送 SYNC 命令...")
                connection.send_command('SYNC')
                
                # 读取 RDB 响应
                logger.info("📥 接收 RDB 数据...")
                response = connection.read_response()
                
                if isinstance(response, bytes):
                    logger.info(f"✅ 接收到 RDB 数据: {len(response)} 字节")
                    logger.info("⏭️  跳过 RDB（已完成全量同步）")
                    
                    # 开始接收命令流
                    logger.info("🔄 开始接收实时命令流...")
                    self._receive_command_stream_sync(connection)
                else:
                    logger.error(f"❌ 意外的 SYNC 响应: {response}")
                    
            finally:
                self.source_client.connection_pool.release(connection)
                
        except Exception as e:
            logger.error(f"❌ SYNC 模式失败: {e}", exc_info=True)
    
    def _start_psync_mode(self):
        """启动 PSYNC 模式"""
        logger.info("📋 PSYNC 模式说明:")
        logger.info("  1. 伪装成 Redis 从库")
        logger.info("  2. 执行 REPLCONF 握手")
        logger.info("  3. 发送 PSYNC 命令")
        logger.info("  4. 接收 RDB（全量）→ 跳过（已完成全量同步）")
        logger.info("  5. 持续接收命令流（增量）")
        logger.info("  6. 实时转发到所有目标（并行）")
        logger.info("=" * 60)

        try:
            # 创建 PSYNC 处理器
            buffer_size = self.config.get('buffer_size', 8192)
            logger.info(f"📊 配置参数:")
            logger.info(f"  - Buffer 大小: {buffer_size} 字节")
            logger.info(f"  - 目标数量: {len(self.target_connections)}")
            logger.info("=" * 60)

            logger.info("🔧 创建 PSYNC 处理器...")
            self.handler = PSyncIncrementalHandler(
                source_client=self.source_client,
                buffer_size=buffer_size
            )
            logger.info("✅ PSYNC 处理器创建成功")

            # 启动 PSYNC 复制
            logger.info("🚀 启动 PSYNC 复制...")
            logger.info("📡 命令回调函数: _on_command_received")
            self.handler.start_replication(self._on_command_received)

            logger.info("=" * 60)
            logger.info("✅ PSYNC 模式已启动")
            logger.info("📡 等待接收命令流...")
            logger.info("=" * 60)

            # 等待直到服务停止
            while self.running and not self.shutdown_event.is_set():
                self.shutdown_event.wait(1)

        except Exception as e:
            logger.error("=" * 60)
            logger.error(f"❌ PSYNC 模式失败: {e}", exc_info=True)
            logger.error("=" * 60)
    
    def _receive_command_stream_sync(self, connection):
        """接收 SYNC 命令流"""
        buffer = b''
        buffer_size = self.config.get('buffer_size', 8192)
        
        while self.running:
            try:
                # 接收数据
                data = connection._sock.recv(buffer_size)
                if not data:
                    logger.warning("⚠️  连接关闭")
                    break
                
                buffer += data
                
                # 解析命令
                while buffer:
                    command, remaining = self._parse_redis_command(buffer)
                    if command is None:
                        # 数据不完整，等待更多数据
                        break
                    
                    buffer = remaining
                    
                    # 处理命令
                    if command:
                        self._on_command_received(command)
                
            except Exception as e:
                if self.running:
                    logger.error(f"❌ 接收命令流失败: {e}")
                break
    
    def _parse_redis_command(self, data: bytes) -> tuple:
        """解析 Redis 协议命令"""
        if not data or data[0:1] != b'*':
            return None, data
        
        try:
            # 查找第一个\r\n
            crlf_pos = data.find(b'\r\n')
            if crlf_pos == -1:
                return None, data
            
            # 解析数组长度
            array_len = int(data[1:crlf_pos])
            pos = crlf_pos + 2
            
            # 解析每个元素
            elements = []
            for _ in range(array_len):
                if pos >= len(data) or data[pos:pos+1] != b'$':
                    return None, data
                
                # 查找长度
                crlf_pos = data.find(b'\r\n', pos)
                if crlf_pos == -1:
                    return None, data
                
                elem_len = int(data[pos+1:crlf_pos])
                pos = crlf_pos + 2
                
                # 读取元素数据
                if pos + elem_len + 2 > len(data):
                    return None, data
                
                element = data[pos:pos+elem_len]
                elements.append(element)
                pos += elem_len + 2
            
            return elements, data[pos:]
            
        except Exception as e:
            logger.error(f"❌ 解析命令失败: {e}")
            return None, data
    
    def _on_command_received(self, command: List[bytes]):
        """接收到命令时的回调"""
        try:
            if not self.running:
                logger.debug("⚠️  服务已停止，忽略命令")
                return

            if not command:
                logger.warning("⚠️  收到空命令，跳过")
                return

            self.stats['commands_received'] += 1
            self.stats['last_command_time'] = time.time()

            # 解析命令
            cmd_name = command[0].decode('utf-8', errors='ignore').upper()

            # 统计命令类型
            self.stats['command_types'][cmd_name] = self.stats['command_types'].get(cmd_name, 0) + 1

            logger.info(f"🔍 解析命令: {cmd_name} (参数数量: {len(command)})")

            # 过滤不需要同步的命令
            if self._should_skip_command(cmd_name):
                self.stats['commands_skipped'] += 1
                logger.info(f"⏭️  跳过命令: {cmd_name} (已跳过 {self.stats['commands_skipped']} 个)")
                return

            # 命令去重检查
            if self._is_duplicate_command(command):
                self.stats['commands_duplicated'] += 1
                logger.warning(f"🔁 检测到重复命令，已去重 (总去重: {self.stats['commands_duplicated']} 个)")
                return

            # 记录命令（INFO 级别，便于调试）
            cmd_str = ' '.join([c.decode('utf-8', errors='ignore') for c in command[:5]])  # 只显示前5个参数
            if len(command) > 5:
                cmd_str += f" ... (共{len(command)}个参数)"
            logger.info(f"📨 接收命令 #{self.stats['commands_received']}: {cmd_str}")

            # 并行同步到所有目标
            logger.debug(f"🔄 开始同步到 {len(self.target_connections)} 个目标...")
            start_time = time.time()
            self._sync_command_to_targets(command)
            elapsed = time.time() - start_time
            logger.debug(f"✅ 命令同步完成，耗时 {elapsed*1000:.1f}ms")

        except Exception as e:
            logger.error(f"❌ 处理命令失败: {e}", exc_info=True)
    
    def _should_skip_command(self, cmd_name: str) -> bool:
        """判断是否应该跳过该命令"""
        skip_commands = {
            'PING', 'REPLCONF', 'SELECT', 'INFO', 'CONFIG',
            'MONITOR', 'SUBSCRIBE', 'PSUBSCRIBE', 'UNSUBSCRIBE', 'PUNSUBSCRIBE'
        }
        return cmd_name in skip_commands

    def _is_duplicate_command(self, command: List[bytes]) -> bool:
        """检查是否是重复命令（基于命令内容和时间窗口）"""
        import hashlib

        # 生成命令的哈希值
        cmd_hash = hashlib.md5(b''.join(command)).hexdigest()

        current_time = time.time()

        # 检查是否在时间窗口内已经处理过相同命令
        if cmd_hash in self.recent_commands:
            last_time = self.recent_commands[cmd_hash]
            if current_time - last_time < self.command_dedup_window:
                # 更新时间（保持在缓存中）
                self.recent_commands[cmd_hash] = current_time
                return True  # 是重复命令

        # 记录命令
        self.recent_commands[cmd_hash] = current_time

        # 限制缓存大小（LRU）
        if len(self.recent_commands) > self.max_recent_commands:
            # 删除最旧的命令
            self.recent_commands.popitem(last=False)

        return False  # 不是重复命令

    def _sync_command_to_targets(self, command: List[bytes]):
        """并行同步命令到所有目标"""
        try:
            logger.debug(f"📤 准备同步到 {len(self.target_connections)} 个目标")

            futures = []
            for target_name, target_conn in self.target_connections.items():
                logger.debug(f"  ➡️  提交任务到 {target_name}")
                future = self.executor.submit(
                    self._sync_command_to_target,
                    target_name,
                    target_conn,
                    command
                )
                futures.append((target_name, future))

            logger.debug(f"📋 已提交 {len(futures)} 个同步任务，等待完成...")

            # 等待所有目标完成
            success_count = 0
            failed_targets = []

            for target_name, future in futures:
                try:
                    result = future.result(timeout=5)
                    if result:
                        success_count += 1
                        logger.info(f"✅ 同步到 {target_name} 成功")
                    else:
                        failed_targets.append(target_name)
                        logger.warning(f"⚠️  同步到 {target_name} 返回 False")
                except TimeoutError:
                    failed_targets.append(target_name)
                    logger.error(f"❌ 同步到 {target_name} 超时（5秒）")
                    self.stats['commands_failed'] += 1
                except Exception as e:
                    failed_targets.append(target_name)
                    logger.error(f"❌ 同步到 {target_name} 失败: {e}", exc_info=True)
                    self.stats['commands_failed'] += 1

            if success_count == len(self.target_connections):
                self.stats['commands_synced'] += 1
                logger.info(f"📊 命令同步完成: ✅ 全部成功 ({success_count}/{len(self.target_connections)})")
            else:
                logger.warning(f"📊 命令同步完成: ⚠️  部分失败 ({success_count}/{len(self.target_connections)})，失败目标: {', '.join(failed_targets)}")

        except Exception as e:
            logger.error(f"❌ 同步命令到目标时发生异常: {e}", exc_info=True)
    
    def _sync_command_to_target(self,
                                target_name: str,
                                target_conn: RedisConnectionManager,
                                command: List[bytes]) -> bool:
        """同步命令到单个目标"""
        try:
            logger.debug(f"  🎯 [{target_name}] 开始执行命令")

            # 检查连接
            if not target_conn or not target_conn.target_client:
                logger.error(f"  ❌ [{target_name}] 目标连接不存在")
                return False

            target_client = target_conn.target_client

            # 将 bytes 转换为 str
            cmd_args = [c.decode('utf-8', errors='ignore') if isinstance(c, bytes) else c
                       for c in command]

            logger.debug(f"  🔧 [{target_name}] 执行命令: {cmd_args[0]} (参数数量: {len(cmd_args)})")

            # 执行命令
            start_time = time.time()
            result = target_client.execute_command(*cmd_args)
            elapsed = time.time() - start_time

            logger.debug(f"  ✅ [{target_name}] 命令执行成功，耗时 {elapsed*1000:.1f}ms，结果: {result}")
            return True

        except redis.ConnectionError as e:
            logger.error(f"  ❌ [{target_name}] 连接错误: {e}")
            return False
        except redis.TimeoutError as e:
            logger.error(f"  ❌ [{target_name}] 超时错误: {e}")
            return False
        except Exception as e:
            logger.error(f"  ❌ [{target_name}] 执行命令失败: {e}", exc_info=True)
            return False
    
    def _print_stats(self):
        """打印统计信息"""
        if not self.stats['start_time']:
            return

        duration = time.time() - self.stats['start_time']

        logger.info("=" * 60)
        logger.info(f"{self.mode.upper()} 模式同步统计")
        logger.info("=" * 60)
        logger.info(f"运行时间: {duration:.1f}秒")
        logger.info(f"接收命令: {self.stats['commands_received']}")
        logger.info(f"跳过命令: {self.stats.get('commands_skipped', 0)}")
        logger.info(f"同步成功: {self.stats['commands_synced']}")
        logger.info(f"同步失败: {self.stats['commands_failed']}")

        # 显示命令类型统计
        if self.stats.get('command_types'):
            logger.info("")
            logger.info("📊 命令类型统计:")
            for cmd_type, count in sorted(self.stats['command_types'].items(), key=lambda x: x[1], reverse=True):
                logger.info(f"  {cmd_type}: {count}")

        if duration > 0 and self.stats['commands_received'] > 0:
            logger.info(f"平均速度: {self.stats['commands_received']/duration:.1f} 命令/秒")

        if self.stats['last_command_time']:
            last_cmd_ago = time.time() - self.stats['last_command_time']
            logger.info(f"最后命令: {last_cmd_ago:.1f}秒前")

        logger.info("=" * 60)
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        stats = self.stats.copy()
        
        if stats['start_time']:
            stats['duration'] = time.time() - stats['start_time']
        
        if stats['last_command_time']:
            stats['last_command_ago'] = time.time() - stats['last_command_time']
        
        return stats

