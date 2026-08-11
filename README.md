# Redis Sync Service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

一个高性能、支持一对多Redis实例持续同步的服务，专为跨境远距离传输优化。

## 🌟 主要特性

- 🔄 **持续同步**：支持长期运行的后台同步服务
- 🎯 **一对多同步**：一个源Redis可以同步到多个目标Redis
- ⚡ **性能优化**：单条复制流或统一扫描，并行分发到多个目标
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
- [生产部署](#-生产部署systemd)
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

### 方式2: Docker 五实例回归环境

```bash
cd docker/redis-five
docker compose up -d
cd ../..
python3 scripts/e2e_five_redis.py
docker compose -f docker/redis-five/docker-compose.yml down
```

该 Compose 文件启动 1 个源 Redis 和 4 个目标 Redis，用于本地回归；同步服务本身仍通过 Python 启动。

### 依赖要求

- Python 3.7+
- redis-py >= 4.5.0
- Click >= 8.0.0
- PyYAML >= 6.0
- colorlog >= 6.0.0
- tqdm >= 4.64.0

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

## 🏭 生产部署（systemd）

仓库内的 `redis-sync.service` 使用专用账号、固定目录和虚拟环境：

- 程序目录：`/opt/redis-sync`
- 配置文件：`/etc/redis-sync/config.yaml`
- 应用日志：`/var/log/redis-sync/redis-sync.log`
- systemd 日志：`journalctl -u redis-sync`

```bash
# 创建专用账号并安装程序
sudo useradd --system --home-dir /opt/redis-sync --shell /usr/sbin/nologin redis-sync
sudo install -d -m 0755 -o root -g root /opt/redis-sync
sudo git clone https://github.com/tigerainiuniu/redisSync.git /opt/redis-sync
sudo python3 -m venv /opt/redis-sync/.venv
sudo /opt/redis-sync/.venv/bin/pip install -r /opt/redis-sync/requirements.txt

# 安装配置；编辑后将 service.logging.file 设为
# /var/log/redis-sync/redis-sync.log
sudo install -d -m 0750 -o root -g redis-sync /etc/redis-sync
sudo install -d -m 0750 -o redis-sync -g redis-sync /var/log/redis-sync
sudo install -m 0640 -o root -g redis-sync \
  /opt/redis-sync/config.yaml.example /etc/redis-sync/config.yaml
sudo editor /etc/redis-sync/config.yaml

# 校验配置并启动
sudo -u redis-sync /opt/redis-sync/.venv/bin/python \
  /opt/redis-sync/run_sync_service.py \
  --config /etc/redis-sync/config.yaml --check-config
sudo install -m 0644 /opt/redis-sync/redis-sync.service /etc/systemd/system/redis-sync.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-sync
```

```bash
systemctl status redis-sync
journalctl -u redis-sync -f
sudo systemctl restart redis-sync
sudo systemctl stop redis-sync
```

修改 `redis-sync.service` 后执行 `sudo systemctl daemon-reload`。更新代码或依赖后先运行配置检查，再重启服务。
示例 unit 使用 `Restart=always`，因此复制线程异常退出后即使进程返回成功状态也会自动拉起；执行 `systemctl stop redis-sync` 属于 systemd 主动停止，不会触发自动重启。

## 📋 配置说明

### 基本配置结构

```yaml
# 源Redis配置
source:
  host: "localhost"
  port: 6379
  password: null
  db: 0
  # 至少为启用目标数；使用 sync/psync 时再预留 1 条复制流连接
  connection_pool_max_connections: 50

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
    scan_count: 10000
    preserve_ttl: true
    clear_target: false
    overwrite_existing: true
    verify_migration: true
    verify_mode: "full"
    
  incremental_sync:
    enabled: true
    method: "psync"
    apply_mode: "key_state"
    target_command_timeout: 5  # 单个目标写入的硬截止时间（秒）
    target_connection_idle_timeout: 60  # 空闲实时写连接回收时间（秒）
    capture_max_size: 1073741824  # FULLRESYNC 对齐期间复制流缓存总上限，默认 1 GiB

# Web API 访问控制
security:
  auth_enabled: true
  api_key: "replace-with-a-secret"
  allowed_ips:
    - "127.0.0.1"
    - "10.0.0.0/8"

# Web界面配置
web_ui:
  enabled: true
  host: "127.0.0.1"
  port: 8080
```

## 🔧 同步模式

### 全量同步 (Full Sync)
- **scan**: 使用SCAN命令逐键迁移
- **sync**: 兼容别名；因客户端不直接装载 RDB，实际执行 SCAN+DUMP/RESTORE
- **dump_restore**: 使用DUMP/RESTORE命令序列化迁移
- `clear_target: false` 保留目标端仅有的键；`clear_target: true` 在复制前对目标数据库执行 `FLUSHDB`，包括过滤范围外的键
- 验证默认使用 `verify_mode: full`，对验证样本比较键的存在性、类型、值和 TTL。显式设置 `fast` 时仅检查存在性和类型结构，同类型但值不同的键会被视为匹配
- `migration.scan_count`、`sync.full_sync.scan_count` 与 `service.performance.scan_count` 建议设置为 5000-20000，硬上限为 100000；更大的 SCAN 页会被配置校验拒绝
- 源端每次最多对 200 个键执行单次原子 `EVAL`：脚本先检查 `PTTL` 和 `MEMORY USAGE`，配置类型过滤时再检查 `TYPE`，最后对合格键执行 `DUMP`；超限键不会被 DUMP 物化，实际 DUMP payload 按约 16 MiB 分轮返回，并在请求下一轮前写入目标。单个键可超过分轮值，但仍受 `max_key_size` 约束
- 源端 Redis ACL 需允许 `EVAL`、`PTTL`、`MEMORY USAGE` 和 `DUMP`，使用类型过滤时还需允许 `TYPE`。允许 `TIME` 时使用源端绝对过期时间；`TIME` 受限时按响应耗时扣减 TTL 后生成保守截止时间。目标端 RESTORE 管道同样限制为最多 200 键、累计约 16 MiB

### 增量同步 (Incremental Sync)
- **psync（推荐）**：使用 Redis PSYNC 复制流；无法续传时接收 RDB 快照并重新对齐目标
- **sync**：兼容别名，常驻服务会使用与 `psync` 相同的 PSYNC 复制流
- **scan**：定时扫描，以键的序列化内容和绝对过期时间指纹检测更新、删除和 TTL 变化
- 常驻服务默认且必须使用 `apply_mode: key_state`，按源端当前键状态执行 `RESTORE`/`DEL`，使重试保持幂等并支持键过滤；配置 `direct` 会在启动校验时被拒绝
- `command_dedup_window` 必须为 `0`；复制流允许连续出现相同的合法写命令，按内容去重会造成数据丢失
- `target_command_timeout` 默认为 `5` 秒，是单个目标写入的硬截止时间。超时时服务会断开该目标的底层连接并继续向其他健康目标分发
- TTL 以绝对毫秒时间写入目标；源 Redis、同步主机和目标 Redis 的系统时钟需保持同步，时钟偏差会等量影响过期时间
- 目标不支持 `RESTORE ABSTTL` 时使用临时键、`PEXPIREAT` 和原子重命名保持截止时间；DUMP 格式不兼容时，字符串及常见容器类型会在 `WATCH` 保护下按类型复制，源状态变化会触发重试。stream 不走类型级降级，以保留 consumer group/PEL 语义

### 混合模式 (Hybrid)
- 使用 `psync`/`sync` 时先建立 PSYNC 流和首次 `FULLRESYNC` 边界，再执行全量 DUMP/RESTORE 对齐，最后回放对齐期间捕获的复制命令，避免“先全量、后建流”遗漏并发写入
- 捕获数据会从内存转存磁盘，内存与磁盘中的未消费复制流合计受 `capture_max_size` 限制；默认值为 `1073741824`（1 GiB），超限会中止本次复制而不静默丢弃命令
- 磁盘缓存使用系统临时目录下的 `redis-sync-repl-*.spool` 文件；启动复制时会根据进程锁清理崩溃遗留文件，并保留正在被其他实例使用的文件
- 首次对齐时，`clear_target: false` 保留目标端仅有的键；`clear_target: true` 会在复制流开始捕获后对每个目标数据库执行 `FLUSHDB`
- 使用 `scan` 时先记录源快照，再执行全量迁移和对账，然后进入轮询。故障目标恢复时只重建受管范围，过滤范围外的目标键会保留

## ⚡ 多目标同步优化

### 单流读取 + 并行分发

当配置多个目标Redis时，PSYNC/SYNC 模式只读取一条源复制流，再将变更并行分发到健康目标。SCAN 轮询模式同样只构建一次源快照。

**读取一次源，并行同步到所有目标**

```
源Redis
  ↓ PSYNC 复制流 / SCAN 快照
  ↓ 解析变更
  ├─→ 目标1 (并行)
  ├─→ 目标2 (并行)
  └─→ 目标3 (并行)
```

目标写入相互隔离；单个目标失败时会从活动分发集合摘除，健康目标继续同步。故障目标恢复后，服务先在命令屏障内执行受管范围的全量对齐，再重新加入实时分发。

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
    method: "psync"
    apply_mode: "key_state"

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

## 🌐 Web管理界面

Web界面提供以下功能：

- 📊 **实时监控**：查看同步状态和统计信息
- 🎯 **目标管理**：监控各个目标Redis的健康状态
- 📈 **性能指标**：同步速度、成功率等指标
- ⚙️ **配置查看**：查看当前配置信息

访问地址：http://localhost:8080

启用 `security.auth_enabled` 后，页面和 API 都会执行 IP/CIDR 与 API key 校验。以下三种认证方式等价：

```bash
curl -H 'X-API-Key: replace-with-a-secret' http://localhost:8080/api/status
curl -H 'Authorization: Bearer replace-with-a-secret' http://localhost:8080/api/status
curl -u 'redis-sync:replace-with-a-secret' http://localhost:8080/api/status
```

Basic Auth 的用户名可自行设置，密码位必须是 `security.api_key`。`/api/config` 会递归隐藏密码、token、API key、URL/DSN 等敏感字段。

`/api/status` 的顶层 `running` 表示服务进程正在工作，`healthy` 还会综合目标可用性和复制流状态。`replication` 字段包含 `baseline_established`、`committed_offset`、`received_offset`、`pending_offset_bytes`、`callback_duration`、`stalled` 和 `last_error`，可用于区分进程存活、FULLRESYNC 基线尚未建立、复制积压和目标写入阻塞。Web 页面会将 `running: true`、`healthy: false` 显示为“运行异常”。

默认的 `web_ui.host: 127.0.0.1` 仅监听本机。监听 `0.0.0.0`、`::` 或其他非 loopback 地址时，配置校验要求满足以下任一条件：

- 启用 `security.auth_enabled` 并设置非空 `security.api_key`。
- 保持认证关闭，但 `security.allowed_ips` 必须是非空列表，且全部为 loopback IP/CIDR（例如 `127.0.0.0/8`、`::1/128`）。此模式用于兼容仅由本机反向代理访问的部署。

远程访问建议由 TLS 反向代理转发到 `127.0.0.1:8080`，同时启用 API key 认证。Web 服务直接对外监听时不要配置空白名单、`*`、非 loopback 或无效 CIDR 作为匿名访问范围。

内置 Web 服务最多同时处理 32 个请求，超过上限时立即返回 `503 Service Unavailable`。它适合状态查看和管理接口，不承担公网入口的连接治理；远程入口应由反向代理配置连接数、请求速率和总请求时限。

### 连接池容量

`source.connection_pool_max_connections` 至少应等于启用目标数。若 `sync.mode` 为 `incremental`/`hybrid` 且 `incremental_sync.method` 为 `sync`/`psync`，还需为复制流增加 1 条连接。例如 4 个启用目标使用 PSYNC 时，最小值为 5；配置校验会拒绝更小的值。

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

#### 2. 轮询同步生效很慢

**问题**：修改源数据后，目标库很久才更新

**解决方案**：新部署使用 `method: psync`。若明确使用 `scan` 轮询，可减小同步间隔：
```yaml
sync:
  incremental_sync:
    method: "scan"
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
    pipeline_batch_size: 50  # 减小单次 Pipeline 键数
    scan_count: 2000         # 减小单次 SCAN 返回规模
    memory_limit: 536870912  # 前后两份 SCAN 指纹快照的估算上限
```

超过 `memory_limit` 时本轮 SCAN 会失败且不推进检查点；它不是操作系统级进程内存硬限制。
`queue_size`、内置指标采集、通知和配置加密字段仅为配置兼容保留，
当前版本不启动对应的队列、指标、通知或加密组件。

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

**轮询模式修改同步间隔**：
```yaml
sync:
  incremental_sync:
    method: "scan"
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
    method: "psync"
    apply_mode: "key_state"
service:
  performance:
    max_workers: 8
```

#### 平衡性能场景
```yaml
sync:
  mode: "hybrid"
  incremental_sync:
    method: "scan"
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
    method: "scan"
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
**A**: 由全量和增量配置分别决定。
- 全量同步默认 `full_sync.strategy: scan`，也支持 `sync` 和 `dump_restore`
- 常驻增量同步推荐 `incremental_sync.method: psync`；`sync` 是 PSYNC 兼容别名，也支持轮询 `scan`
- `strategy` 只控制全量迁移；`method` 控制常驻增量同步

### Q2: 如何加快同步速度？
**A**: 对实时性要求高时使用 PSYNC：
```yaml
sync:
  incremental_sync:
    method: "psync"
    apply_mode: "key_state"
```
SCAN 轮询模式才通过 `interval` 调整检测周期。

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
git clone https://github.com/tigerainiuniu/redisSync.git
cd redisSync

# 安装开发依赖
pip install -r requirements.txt
pip install -e .

# 运行完整测试
python3 -m pytest tests -q

# 基础静态校验
python3 -m compileall -q redis_sync run_sync_service.py scripts
python3 setup.py check

# 代码风格检查
flake8 redis_sync/
```

### 报告问题

如果发现bug或有功能建议，请[创建Issue](https://github.com/tigerainiuniu/redisSync/issues)。

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
5. **提交Issue**：[GitHub Issues](https://github.com/tigerainiuniu/redisSync/issues)

## 🗺️ 路线图

- [x] Docker 五实例本地回归环境
- [ ] 同步服务容器镜像
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

[![Star History Chart](https://api.star-history.com/svg?repos=tigerainiuniu/redisSync&type=Date)](https://star-history.com/#tigerainiuniu/redisSync&Date)
