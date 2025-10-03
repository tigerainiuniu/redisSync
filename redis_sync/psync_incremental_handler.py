"""
PSYNC实时增量同步处理器

使用Redis PSYNC协议实现实时增量同步，类似RedisShake的实现方式。
这是最快速、最实时的增量同步方法。
"""

import redis
import logging
import threading
import time
import socket
import struct
from typing import Optional, Callable, List, Dict, Any
from queue import Queue, Empty

logger = logging.getLogger(__name__)


class PSyncIncrementalHandler:
    """使用PSYNC协议实现实时增量同步"""
    
    def __init__(self, source_client: redis.Redis, buffer_size: int = 8192):
        """
        初始化PSYNC增量处理器
        
        参数:
            source_client: 源Redis客户端
            buffer_size: 缓冲区大小
        """
        self.source_client = source_client
        self.buffer_size = buffer_size
        self.running = False
        self.replication_thread: Optional[threading.Thread] = None
        self.command_queue: Queue = Queue(maxsize=10000)
        self.replication_id: Optional[str] = None
        self.replication_offset: int = -1
        
    def start_replication(self, command_callback: Callable[[List[bytes]], None]):
        """
        启动PSYNC实时复制

        参数:
            command_callback: 接收到命令时的回调函数，参数为命令列表
        """
        if self.running:
            logger.warning("⚠️  PSYNC复制已在运行")
            return

        self.running = True

        logger.info("=" * 60)
        logger.info("🚀 启动 PSYNC 实时复制")
        logger.info("=" * 60)
        logger.info(f"📊 Buffer 大小: {self.buffer_size} 字节")
        logger.info(f"📊 回调函数: {command_callback.__name__ if hasattr(command_callback, '__name__') else 'lambda'}")
        try:
            host = self.source_client.connection_pool.connection_kwargs.get('host', 'unknown')
            port = self.source_client.connection_pool.connection_kwargs.get('port', 'unknown')
            logger.info(f"📊 源 Redis: {host}:{port}")
        except:
            logger.info(f"📊 源 Redis: (无法获取连接信息)")
        logger.info("=" * 60)

        # 启动复制线程
        logger.info("🔧 创建后台复制线程...")
        self.replication_thread = threading.Thread(
            target=self._replication_loop,
            args=(command_callback,),
            name="psync-replication",
            daemon=True
        )
        self.replication_thread.start()
        logger.info(f"✅ 后台复制线程已启动: {self.replication_thread.name}")
        logger.info("=" * 60)
    
    def stop_replication(self):
        """停止PSYNC复制"""
        if not self.running:
            return
        
        self.running = False
        
        if self.replication_thread:
            self.replication_thread.join(timeout=10)
        
        logger.info("🛑 PSYNC实时复制已停止")
    
    def _replication_loop(self, command_callback: Callable[[List[bytes]], None]):
        """PSYNC复制主循环（带自动重连）"""
        import time

        retry_count = 0
        max_retries = 999  # 几乎无限重试
        retry_delay = 5  # 秒

        logger.info("=" * 60)
        logger.info("🔄 进入 PSYNC 复制主循环")
        logger.info(f"📊 最大重试次数: {max_retries}")
        logger.info(f"📊 重试延迟: {retry_delay} 秒")
        logger.info("=" * 60)

        while self.running and retry_count < max_retries:
            try:
                if retry_count > 0:
                    logger.info("=" * 60)
                    logger.info(f"🔄 尝试重新连接 PSYNC... (第 {retry_count} 次)")
                    logger.info(f"⏱️  等待 {retry_delay} 秒...")
                    logger.info("=" * 60)
                    time.sleep(retry_delay)
                else:
                    logger.info("=" * 60)
                    logger.info("🔄 开始第一次连接...")
                    logger.info("=" * 60)

                # 执行复制握手
                logger.info("📡 步骤1: 执行复制握手...")
                if not self._perform_handshake():
                    logger.error("❌ PSYNC握手失败")
                    retry_count += 1
                    continue

                logger.info("✅ 复制握手成功")

                # 执行PSYNC命令
                logger.info("📡 步骤2: 获取 PSYNC 连接...")
                connection = self.source_client.connection_pool.get_connection('PSYNC')
                logger.info("✅ PSYNC 连接已获取")

                try:
                    # 发送PSYNC命令
                    if self.replication_id and self.replication_offset >= 0:
                        # 部分同步
                        logger.info(f"📡 发送 PSYNC {self.replication_id} {self.replication_offset}")
                        connection.send_command('PSYNC', self.replication_id, self.replication_offset)
                    else:
                        # 全量同步
                        logger.info("📡 发送 PSYNC ? -1")
                        connection.send_command('PSYNC', '?', '-1')

                    # 读取响应
                    response = connection.read_response()
                    logger.info(f"📥 PSYNC响应: {response} (类型: {type(response)})")

                    # 处理响应（兼容 bytes 和 str）
                    if isinstance(response, bytes):
                        response_str = response.decode('utf-8', errors='ignore')
                    elif isinstance(response, str):
                        response_str = response
                    else:
                        logger.error(f"❌ 未知的PSYNC响应类型: {type(response)}")
                        retry_count += 1
                        continue

                    if response_str.startswith('FULLRESYNC'):
                        # 全量同步
                        parts = response_str.split()
                        self.replication_id = parts[1]
                        self.replication_offset = int(parts[2])
                        logger.info(f"📦 开始全量同步: repl_id={self.replication_id}, offset={self.replication_offset}")

                        # 接收RDB数据（跳过，因为我们已经做过全量同步）
                        self._skip_rdb_data(connection)

                    elif response_str.startswith('CONTINUE'):
                        # 部分同步
                        logger.info("⚡ 继续部分同步")
                    else:
                        logger.error(f"❌ 未知的PSYNC响应: {response_str}")
                        retry_count += 1
                        continue

                    # 开始接收增量命令流
                    logger.info("🔄 开始接收实时命令流...")
                    retry_count = 0  # 重置重试计数（连接成功）
                    self._receive_command_stream(connection, command_callback)

                    # 如果正常退出（连接关闭），尝试重连
                    if self.running:
                        logger.warning("⚠️  连接断开，准备重连...")
                        retry_count += 1

                finally:
                    try:
                        self.source_client.connection_pool.release(connection)
                    except:
                        pass

            except Exception as e:
                if self.running:
                    logger.error(f"❌ PSYNC复制异常: {e}", exc_info=True)
                    retry_count += 1
                else:
                    break

        if retry_count >= max_retries:
            logger.error(f"❌ PSYNC 重连失败次数过多 ({max_retries} 次)，停止重连")
        else:
            logger.info("✅ PSYNC 复制已停止")
    
    def _perform_handshake(self) -> bool:
        """执行复制握手"""
        try:
            # 1. PING
            logger.info("📡 步骤1: 发送 PING...")
            if not self.source_client.ping():
                logger.error("❌ PING失败")
                return False
            logger.info("✅ PING 成功")

            # 2. REPLCONF listening-port
            connection = self.source_client.connection_pool.get_connection('REPLCONF')
            try:
                logger.info("📡 步骤2: 发送 REPLCONF listening-port 6380...")
                connection.send_command('REPLCONF', 'listening-port', '6380')
                response = connection.read_response()
                logger.info(f"📥 响应: {response} (类型: {type(response)})")

                # 兼容 bytes 和 str 类型
                if response not in [b'OK', 'OK']:
                    logger.error(f"❌ REPLCONF listening-port失败: {response}")
                    return False
                logger.info("✅ REPLCONF listening-port 成功")

                # 3. REPLCONF capa eof
                logger.info("📡 步骤3: 发送 REPLCONF capa eof...")
                connection.send_command('REPLCONF', 'capa', 'eof')
                response = connection.read_response()
                logger.info(f"📥 响应: {response} (类型: {type(response)})")

                if response not in [b'OK', 'OK']:
                    logger.error(f"❌ REPLCONF capa eof失败: {response}")
                    return False
                logger.info("✅ REPLCONF capa eof 成功")

                # 4. REPLCONF capa psync2
                logger.info("📡 步骤4: 发送 REPLCONF capa psync2...")
                connection.send_command('REPLCONF', 'capa', 'psync2')
                response = connection.read_response()
                logger.info(f"📥 响应: {response} (类型: {type(response)})")

                if response not in [b'OK', 'OK']:
                    logger.error(f"❌ REPLCONF capa psync2失败: {response}")
                    return False
                logger.info("✅ REPLCONF capa psync2 成功")

                logger.info("✅ 复制握手完成")
                return True

            finally:
                self.source_client.connection_pool.release(connection)

        except Exception as e:
            logger.error(f"❌ 复制握手失败: {e}", exc_info=True)
            return False
    
    def _skip_rdb_data(self, connection):
        """跳过RDB数据（因为已经做过全量同步）"""
        try:
            # 设置 socket 超时（300秒，足够传输大的 RDB）
            connection._sock.settimeout(300)
            logger.debug("📊 Socket 超时设置为 300 秒（用于 RDB 传输）")

            # 读取RDB大小（逐字节读取，避免读取过多）
            size_line = b''
            first_byte = connection._sock.recv(1)
            if not first_byte:
                logger.warning("⚠️  读取RDB大小时连接断开（第一个字节为空）")
                return

            size_line += first_byte
            logger.debug(f"📊 RDB 第一个字节: {first_byte.hex()} ('{chr(first_byte[0]) if 32 <= first_byte[0] < 127 else '?'}')")

            if first_byte[0] == ord('$'):
                # Bulk string格式: $<size>\r\n<data>
                # 逐字节读取直到 \r\n
                logger.debug("📡 读取 RDB 大小...")
                while not size_line.endswith(b'\r\n'):
                    byte = connection._sock.recv(1)
                    if not byte:
                        logger.error("❌ 读取RDB大小时连接断开")
                        return
                    size_line += byte

                # 解析大小
                size_str = size_line[1:-2]  # 去掉 $ 和 \r\n
                rdb_size = int(size_str)

                logger.info(f"📦 RDB 大小: {rdb_size} 字节 ({rdb_size / 1024 / 1024:.2f} MB)")
                logger.info(f"🔄 开始跳过 RDB 数据...")

                # 跳过RDB数据
                remaining = rdb_size
                skipped = 0
                start_time = time.time()
                last_log_time = start_time

                while remaining > 0:
                    chunk_size = min(remaining, self.buffer_size)
                    chunk = connection._sock.recv(chunk_size)
                    if not chunk:
                        logger.warning(f"⚠️  跳过RDB数据时连接断开，已跳过 {skipped}/{rdb_size} 字节")
                        break
                    remaining -= len(chunk)
                    skipped += len(chunk)

                    # 每跳过 10MB 或每 5 秒记录一次进度
                    current_time = time.time()
                    if skipped % (10 * 1024 * 1024) < self.buffer_size or current_time - last_log_time >= 5:
                        elapsed = current_time - start_time
                        speed = skipped / elapsed / 1024 / 1024 if elapsed > 0 else 0
                        logger.info(f"📊 已跳过 {skipped}/{rdb_size} 字节 ({skipped*100//rdb_size}%)，速度: {speed:.2f} MB/s")
                        last_log_time = current_time

                elapsed = time.time() - start_time
                speed = skipped / elapsed / 1024 / 1024 if elapsed > 0 else 0
                logger.info(f"✅ RDB数据已跳过: {skipped}/{rdb_size} 字节，耗时 {elapsed:.1f} 秒，平均速度: {speed:.2f} MB/s")

                # 重要：不要读取额外的数据，让 _receive_command_stream 处理命令流
                logger.debug("📡 RDB 跳过完成，准备接收命令流")

            else:
                logger.warning(f"⚠️  意外的 RDB 第一个字节: {first_byte.hex()} (期望 '$' = 0x24)")

        except socket.timeout:
            logger.error(f"❌ 跳过RDB数据超时（300秒）")
        except Exception as e:
            logger.error(f"❌ 跳过RDB数据失败: {e}", exc_info=True)
    
    def _receive_command_stream(self, connection, command_callback: Callable[[List[bytes]], None]):
        """接收并处理命令流（带 REPLCONF ACK 心跳）"""
        buffer = b''
        command_count = 0
        last_log_time = time.time()
        last_data_time = time.time()
        last_ack_time = time.time()  # 新增：上次发送 ACK 的时间
        data_received_count = 0
        ack_interval = 1.0  # 新增：ACK 心跳间隔（1 秒）

        logger.info("=" * 60)
        logger.info("📡 开始监听命令流（带 REPLCONF ACK 心跳）...")
        logger.info(f"📊 Socket 超时设置: 1 秒（用于心跳）")
        logger.info(f"📊 Buffer 大小: {self.buffer_size} 字节")
        logger.info(f"📊 ACK 心跳间隔: {ack_interval} 秒")
        logger.info(f"📊 回调函数: {command_callback.__name__ if hasattr(command_callback, '__name__') else 'lambda'}")
        logger.info("=" * 60)

        while self.running:
            try:
                # 设置较短的超时，以便定期发送 ACK
                connection._sock.settimeout(ack_interval)  # 1秒超时

                elapsed_since_last = time.time() - last_data_time
                logger.debug(f"⏳ 等待接收数据... (距上次数据 {elapsed_since_last:.1f} 秒)")

                data = connection._sock.recv(self.buffer_size)
                data_received_count += 1

                if not data:
                    elapsed = time.time() - last_data_time
                    logger.warning("=" * 60)
                    logger.warning(f"⚠️  连接关闭（recv返回空数据）")
                    logger.warning(f"📊 距上次数据: {elapsed:.1f} 秒")
                    logger.warning(f"📊 已接收数据次数: {data_received_count}")
                    logger.warning(f"📊 已处理命令数: {command_count}")
                    logger.warning("=" * 60)
                    break

                last_data_time = time.time()

                # 新增：更新复制偏移量（重要！）
                self.replication_offset += len(data)

                logger.info(f"📥 收到数据 #{data_received_count}: {len(data)} 字节 (offset: {self.replication_offset})")
                logger.debug(f"📊 数据前 50 字节（hex）: {data[:50].hex()}")
                logger.debug(f"📊 数据前 50 字节（repr）: {repr(data[:50])}")

                buffer += data
                logger.debug(f"📊 当前 buffer 大小: {len(buffer)} 字节")

                # 解析命令
                parsed_count = 0
                while buffer:
                    command, remaining = self._parse_redis_command(buffer)
                    if command is None:
                        # 数据不完整，等待更多数据
                        logger.info(f"⏸️  数据不完整，等待更多数据（当前 buffer: {len(buffer)} 字节）")
                        logger.info(f"📊 Buffer 前 100 字节（hex）: {buffer[:100].hex()}")
                        logger.info(f"📊 Buffer 前 100 字节（repr）: {repr(buffer[:100])}")
                        break

                    buffer = remaining
                    parsed_count += 1

                    # 处理命令
                    if command:
                        try:
                            command_count += 1

                            # 记录命令详情
                            cmd_name = command[0].decode('utf-8', errors='ignore') if command else 'UNKNOWN'
                            cmd_args_str = ' '.join([c.decode('utf-8', errors='ignore')[:50] for c in command[:3]])
                            if len(command) > 3:
                                cmd_args_str += f" ... (共{len(command)}个参数)"

                            logger.info(f"🔧 解析命令 #{command_count}: {cmd_name} {cmd_args_str}")
                            logger.debug(f"📊 命令完整内容: {[c.decode('utf-8', errors='ignore') for c in command]}")

                            # 调用回调
                            logger.info(f"📞 调用回调函数处理命令 #{command_count}...")
                            command_callback(command)
                            logger.debug(f"✅ 回调函数执行完成")

                            # 每10秒或每100个命令记录一次
                            current_time = time.time()
                            if current_time - last_log_time >= 10 or command_count % 100 == 0:
                                logger.info(f"📊 已处理 {command_count} 个命令")
                                last_log_time = current_time

                        except Exception as e:
                            logger.error(f"❌ 命令回调失败: {e}", exc_info=True)

                if parsed_count > 0:
                    logger.info(f"📊 本次接收解析了 {parsed_count} 个命令，剩余 buffer: {len(buffer)} 字节")

            except socket.timeout:
                # 超时是正常的，用于发送 ACK
                elapsed = time.time() - last_data_time
                logger.debug(f"⏱️  接收超时（正常），距上次数据: {elapsed:.1f} 秒")
            except Exception as e:
                if self.running:
                    logger.error("=" * 60)
                    logger.error(f"❌ 接收命令流失败: {e}", exc_info=True)
                    logger.error("=" * 60)
                break

            # 新增：定期发送 REPLCONF ACK 心跳（重要！）
            current_time = time.time()
            if current_time - last_ack_time >= ack_interval:
                try:
                    self._send_replconf_ack(connection)
                    last_ack_time = current_time
                except Exception as e:
                    logger.error(f"❌ 发送 REPLCONF ACK 失败: {e}")
                    break  # ACK 发送失败，断开连接重试

        logger.info("=" * 60)
        logger.info(f"✅ 命令流接收结束")
        logger.info(f"📊 总接收数据次数: {data_received_count}")
        logger.info(f"📊 总处理命令数: {command_count}")
        logger.info(f"📊 最终复制偏移量: {self.replication_offset}")
        logger.info("=" * 60)
    
    def _send_replconf_ack(self, connection):
        """
        发送 REPLCONF ACK 心跳

        格式: *3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$<len>\r\n<offset>\r\n
        """
        try:
            offset_str = str(self.replication_offset)
            cmd = f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n${len(offset_str)}\r\n{offset_str}\r\n"

            connection._sock.sendall(cmd.encode())
            logger.debug(f"💓 发送 REPLCONF ACK {self.replication_offset}")

        except Exception as e:
            logger.error(f"❌ 发送 REPLCONF ACK 失败: {e}")
            raise  # 抛出异常，触发重连

    def _parse_redis_command(self, data: bytes) -> tuple[Optional[List[bytes]], bytes]:
        """
        解析Redis协议命令

        返回: (命令列表, 剩余数据)
        """
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
            logger.error(f"解析命令失败: {e}")
            return None, data

