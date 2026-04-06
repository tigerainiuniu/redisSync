# Redis Sync Service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

一个高性能、支持一对多Redis实例持续同步的服务，专为跨境远距离传输优化。

## 🌟 主要特性

- 🔄 **持续同步**：支持长期运行的后台同步服务
- 🎯 **一对多同步**：一个源Redis可以同步到多个目标Redis
- ⚡ **性能优化**：统一扫描 + 并行分发，多目标场景性能提升67%
- 🌍 **跨境优化**：专门针对跨境远距离传输优化，支持自动重试和重连
- 📊 **全量+增量**：支持全量同步、增量同步和混合模式
- 🌐 **Web管理界面**：内置Web界面实时监控同步状态
- ⚙️ **灵活配置**：YAML配置文件，支持复杂的同步策略
- 🛡️ **故障恢复**：自动故障检测和恢复机制
- 📝 **详细日志**：完整的操作日志和性能监控
- 🔐 **安全连接**：支持SSL/TLS和密码认证

## 📖 目录

- [快速开始](#-快速开始)
- [安装](#-安装)
- [配置说明](#-配置说明)
- [性能优化](#-多目标同步优化)
- [跨境传输](#-跨境远距离传输支持)
- [Web管理界面](#-web管理界面)
- [故障排除](#-故障排除)
- [贡献指南](#-贡献)
- [许可证](#-许可证)

## 📦 安装

### 方式1: 使用 pip（推荐）

```bash
# 克隆仓库
git clone https://github.com/tigerainiuniu/redisSync.git
cd redisSync

# 安装依赖
pip install -r requirements.txt

# 或使用 setup.py 安装
pip install -e .
```

### 方式2: 使用 Docker（即将支持）

```bash
docker pull yourusername/redis-sync:latest
docker run -v ./config.yaml:/app/config.yaml redis-sync
```

### 依赖要求

- Python 3.7+
- redis-py >= 4.0.0
- PyYAML >= 5.4.0

## 🚀 快速开始

### 1. 准备配置文件

```bash
# 复制示例配置文件
cp config.yaml.example config.yaml

# 编辑配置文件
vim config.yaml
```

配置示例：
```yaml
source:
  host: "source-redis.example.com"
  port: 6379
  password: "your-password"

targets:
  - name: "target1"
    host: "target1-redis.example.com"
    port: 6379
    enabled: true
```

### 2. 启动同步服务

```bash
# 启动服务
python run_sync_service.py

# 使用自定义配置文件
python run_sync_service.py --config /path/to/config.yaml

# 检查配置文件
python run_sync_service.py --check-config
```

### 3. 访问Web管理界面

打开浏览器访问：http://localhost:8080

查看实时同步状态、统计信息和目标健康状况。

## 📋 配置说明

### 基本配置结构

```yaml
# 源Redis配置
source:
  host: "localhost"
  port: 6379
  password: null
  db: 0

# 目标Redis配置（支持多个）
targets:
  - name: "target1"
    host: "localhost"
    port: 6380
    enabled: true
  
  - name: "target2"
    host: "192.168.1.100"
    port: 6379
    enabled: false

# 同步配置
sync:
  mode: "hybrid"  # full, incremental, hybrid
  
  full_sync:
    strategy: "scan"
    batch_size: 1000
    preserve_ttl: true
    
  incremental_sync:
    enabled: true
    interval: 30  # 秒
    max_changes_per_sync: 10000

# Web界面配置
web_ui:
  enabled: true
  host: "0.0.0.0"
  port: 8080
```

## 🔧 同步模式

### 全量同步 (Full Sync)
- **scan**: 使用SCAN命令逐键迁移
- **sync**: 使用SYNC命令RDB快照迁移
- **dump_restore**: 使用DUMP/RESTORE命令序列化迁移

### 增量同步 (Incremental Sync)
- 基于空闲时间检测变更
- 支持持续监控和同步
- 可配置同步间隔和批量大小

### 混合模式 (Hybrid)
- 先执行全量同步
- 然后启动增量同步
- 适合大多数场景

## ⚡ 多目标同步优化

### 统一扫描 + 并行分发

当配置多个目标Redis时，系统使用优化的同步策略：

**扫描一次源，并行同步到所有目标**

```
源Redis
  ↓ SCAN 一次 ← 只扫描1次！
  ↓ 检测变更键
  ├─→ 目标1 (并行)
  ├─→ 目标2 (并行)
  └─→ 目标3 (并行)
```

**性能提升**（3个目标场景）：
- ✅ SCAN次数减少 **67%**
- ✅ 网络流量减少 **67%**
- ✅ 源Redis负载减少 **67%**
- ✅ 并行同步更快

## 🌍 跨境远距离传输支持

### 内置优化机制

本项目专门针对跨境远距离传输进行了优化：

#### 1. **自动重试机制**（指数退避）
- 连接失败自动重试（默认5次）
- 智能退避策略：1s → 2s → 4s → 8s → 16s
- 适应网络抖动和临时故障

#### 2. **自动重连机制**
- 检测到连接断开自动重连
- 每30秒健康检查
- 操作失败时自动重连并重试

#### 3. **TCP Keepalive**
- 保持长连接活跃
- 防止被防火墙/NAT关闭
- 及时发现连接断开

#### 4. **超时优化**
- 默认60秒socket超时（适应高延迟）
- 30秒连接超时
- 可根据网络情况调整

### 跨境传输配置示例

```yaml
source:
  host: "remote-redis.example.com"
  port: 6379
  password: "your-password"

  # 跨境优化配置
  socket_timeout: 60           # 60秒超时
  socket_connect_timeout: 30   # 30秒连接超时
  socket_keepalive: true       # 启用keepalive

sync:
  full_sync:
    batch_size: 500            # 减小批量大小
  incremental_sync:
    interval: 10               # 10秒同步间隔
    max_changes_per_sync: 5000

service:
  retry:
    max_attempts: 5            # 5次重试
    backoff_factor: 2          # 指数退避
    max_delay: 60              # 最大60秒延迟
    initial_delay: 1           # 初始1秒延迟

  failover:
    enabled: true
    max_failures: 10           # 增加容错次数
    recovery_delay: 120        # 2分钟恢复延迟
```

详细说明请查看：[跨境传输优化说明.md](跨境传输优化说明.md)

## 🌐 Web管理界面

Web界面提供以下功能：

- 📊 **实时监控**：查看同步状态和统计信息
- 🎯 **目标管理**：监控各个目标Redis的健康状态
- 📈 **性能指标**：同步速度、成功率等指标
- ⚙️ **配置查看**：查看当前配置信息

访问地址：http://localhost:8080

## 🛠️ 高级功能

### 故障恢复
- 自动检测目标Redis连接失败
- 支持自动重试和恢复
- 可配置最大失败次数和恢复延迟

### 过滤功能
```yaml
filters:
  include_patterns:
    - "user:*"
    - "session:*"
  exclude_patterns:
    - "temp:*"
    - "cache:*"
  min_ttl: 60
  max_key_size: 104857600
```

### 安全配置
```yaml
source:
  host: "redis.example.com"
  port: 6380
  password: "your-password"
  ssl: true
  ssl_cert_reqs: "required"
```

## 📝 使用示例

### 启动服务
```bash
# 基本启动
python run_sync_service.py

# 指定配置文件
python run_sync_service.py --config production.yaml

# 检查配置
python run_sync_service.py --check-config
```

### 监控服务
```bash
# 查看日志
tail -f redis-sync.log

# 访问Web界面
curl http://localhost:8080/api/status
```

## 🔍 故障排除

### 常见问题

#### 1. 连接失败
```bash
# 检查Redis是否运行
redis-cli -h <host> -p <port> -a <password> ping

# 验证配置
python run_sync_service.py --check-config
```

#### 2. 同步生效很慢

**问题**：修改源数据后，目标库很久才更新

**解决方案**：减小同步间隔
```yaml
sync:
  incremental_sync:
    interval: 5  # 从30秒改为5秒（推荐）
```

**效果对比**：
- 间隔30秒：延迟30-60秒
- 间隔5秒：延迟5-10秒（推荐）
- 间隔1秒：延迟1-2秒（高资源消耗）

修改后重启服务：
```bash
# 停止服务 (Ctrl+C)
python run_sync_service.py
```

#### 3. 增量同步不工作

**检查步骤**：
```bash
# 1. 查看日志
tail -f redis-sync.log | grep "增量同步"

# 2. 确认配置
grep -A 5 "incremental_sync:" config.yaml

# 3. 检查Redis连接
redis-cli -h <source-host> -p <port> ping
redis-cli -h <target-host> -p <port> ping
```

#### 4. 内存使用过高
```yaml
sync:
  full_sync:
    batch_size: 500  # 减小批量大小
service:
  performance:
    memory_limit: 536870912  # 限制为512MB
```

### 日志分析
```bash
# 查看错误日志
grep ERROR redis-sync.log

# 查看同步统计
grep "同步完成" redis-sync.log

# 实时监控
tail -f redis-sync.log | grep "🔄"

# 查看增量同步详情
tail -f redis-sync.log | grep "变更键数"
```

## 📊 性能优化

### 快速优化（提升同步速度）

**修改同步间隔**（最有效）：
```yaml
sync:
  incremental_sync:
    interval: 5  # 推荐：5秒
```

**增加并发**：
```yaml
service:
  performance:
    max_workers: 8  # 增加工作线程
```

**优化批量大小**：
```yaml
sync:
  full_sync:
    batch_size: 2000  # 增加批量大小
  incremental_sync:
    max_changes_per_sync: 20000
```

### 不同场景的推荐配置

#### 高实时性场景
```yaml
sync:
  mode: "hybrid"
  incremental_sync:
    interval: 5  # 5秒同步
    max_changes_per_sync: 5000
service:
  performance:
    max_workers: 8
```

#### 平衡性能场景
```yaml
sync:
  mode: "hybrid"
  incremental_sync:
    interval: 10  # 10秒同步
    max_changes_per_sync: 10000
service:
  performance:
    max_workers: 4
```

#### 低资源消耗场景
```yaml
sync:
  mode: "hybrid"
  incremental_sync:
    interval: 60  # 60秒同步
    max_changes_per_sync: 5000
service:
  performance:
    max_workers: 2
  logging:
    level: "WARNING"  # 减少日志
```

### 监控指标
- 同步延迟：修改到同步的时间
- 同步速度：键/秒
- 内存使用：进程内存占用
- CPU使用：进程CPU占用
- 错误率：失败次数/总次数

## ❓ 常见问题 FAQ

### Q1: 使用的是SCAN还是SYNC命令？
**A**: 使用 **SCAN** 命令。
- 全量同步：使用SCAN逐键扫描（默认策略）
- 增量同步：使用SCAN + OBJECT IDLETIME检测变更
- 优点：不阻塞Redis，对生产环境友好
- 如需使用SYNC：修改配置 `strategy: "sync"`

### Q2: 如何加快同步速度？
**A**: 修改配置文件中的同步间隔：
```yaml
sync:
  incremental_sync:
    interval: 5  # 从30改为5秒
```
然后重启服务。

### Q3: 支持多少个目标Redis？
**A**: 理论上无限制，实际取决于：
- 源Redis的性能
- 网络带宽
- 服务器资源
建议：不超过10个目标

### Q4: 如何验证同步是否正常？
**A**:
```bash
# 1. 查看日志
tail -f redis-sync.log

# 2. 访问Web界面
http://localhost:8080

# 3. 手动测试
redis-cli -h <source> SET test:key "value"
# 等待同步间隔时间
redis-cli -h <target> GET test:key
```

### Q5: 服务异常退出怎么办？
**A**:
```bash
# 1. 查看错误日志
tail -100 redis-sync.log | grep ERROR

# 2. 检查配置
python run_sync_service.py --check-config

# 3. 检查Redis连接
redis-cli -h <host> -p <port> -a <password> ping
```

## 🤝 贡献

欢迎贡献代码、报告问题或提出建议！

### 如何贡献

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 开发指南

```bash
# 克隆仓库
git clone https://github.com/yourusername/redisSync.git
cd redisSync

# 安装开发依赖
pip install -r requirements.txt
pip install -e .

# 运行测试（如果有）
python -m pytest

# 代码风格检查
flake8 redis_sync/
```

### 报告问题

如果发现bug或有功能建议，请[创建Issue](https://github.com/yourusername/redisSync/issues)。

请包含：
- 问题描述
- 复现步骤
- 预期行为
- 实际行为
- 环境信息（Python版本、Redis版本等）
- 相关日志

## 📞 支持

如果遇到问题：

1. **查看文档**：仔细阅读本README
2. **查看日志**：`tail -f redis-sync.log`
3. **检查配置**：`python run_sync_service.py --check-config`
4. **Web界面**：http://localhost:8080
5. **提交Issue**：[GitHub Issues](https://github.com/yourusername/redisSync/issues)

## 🗺️ 路线图

- [ ] Docker支持
- [ ] 更多同步策略（基于时间戳、版本号等）
- [ ] 数据压缩传输
- [ ] 更详细的性能监控
- [ ] 支持Redis Cluster
- [ ] 双向同步支持
- [ ] 图形化配置工具

## 🙏 致谢

感谢所有贡献者和使用者！

特别感谢：
- [redis-py](https://github.com/redis/redis-py) - Redis Python客户端
- 所有提供反馈和建议的用户

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

Copyright (c) 2025 redisSync Contributors

## ⭐ Star History

如果这个项目对你有帮助，请给个 Star ⭐️

[![Star History Chart](https://api.star-history.com/svg?repos=yourusername/redisSync&type=Date)](https://star-history.com/#yourusername/redisSync&Date)
