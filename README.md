# Redis Sync Service

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.7+](https://img.shields.io/badge/python-3.7+-blue.svg)](https://www.python.org/downloads/)

Redis Sync Service 用于将一个 Redis 数据库持续同步到一个或多个目标数据库。服务以普通 Redis 客户端运行，不改变现有 Redis 拓扑。

目前支持：

- `full`、`incremental` 和 `hybrid` 三种运行模式
- 基于 PSYNC 复制流或 SCAN 轮询的增量同步
- 一个源端向多个目标端分发，目标故障相互隔离
- 键名、类型、TTL 和键大小过滤
- Redis TLS 连接，以及带访问控制的 Web 状态页和 API

同步以单个 Redis 逻辑数据库为单位。当前版本不处理 Redis Cluster 分片，也不提供双向同步和冲突合并。

## 安装

运行环境：

- Python 3.7+
- redis-py 4.5+
- Click 8.0+
- PyYAML 6.0+
- colorlog 6.0+
- tqdm 4.64+

从源码安装：

```bash
git clone https://github.com/tigerainiuniu/redisSync.git
cd redisSync

python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

`pip install -e .` 会额外安装项目的命令行入口；常驻同步服务仍由 `run_sync_service.py` 启动。

## 快速开始

复制示例配置：

```bash
cp config.yaml.example config.yaml
```

下面是一份最小的 PSYNC 混合同步配置。示例只有一个目标，因此源连接池至少需要 2 条连接，目标连接池至少需要 2 条连接。

```yaml
source:
  host: "source-redis.example.com"
  port: 6379
  password: "source-password"
  db: 0
  connection_pool_max_connections: 2

targets:
  - name: "target-1"
    host: "target-redis.example.com"
    port: 6379
    password: "target-password"
    db: 0
    connection_pool_max_connections: 2
    enabled: true

sync:
  mode: "hybrid"
  full_sync:
    strategy: "scan"
    clear_target: false
    overwrite_existing: true
  incremental_sync:
    enabled: true
    method: "psync"
    apply_mode: "key_state"
    command_dedup_window: 0

service:
  logging:
    level: "INFO"
    file: "redis-sync.log"

web_ui:
  enabled: true
  host: "127.0.0.1"
  port: 8080
```

先检查配置，再启动服务：

```bash
python3 run_sync_service.py --config config.yaml --check-config
python3 run_sync_service.py --config config.yaml
```

Web 状态页默认地址为 `http://127.0.0.1:8080`。完整字段和注释见 [config.yaml.example](config.yaml.example)。

## 配置

配置文件分为以下几部分：

| 配置段 | 用途 |
| --- | --- |
| `source` | 源 Redis 连接、TLS、超时和连接池 |
| `targets` | 目标 Redis 列表；`enabled: false` 的目标不会启动 |
| `sync.full_sync` | 全量策略、批次、目标清理和校验 |
| `sync.incremental_sync` | PSYNC 或 SCAN 增量同步参数 |
| `sync.filters` | 键名、类型、TTL、大小和命令过滤 |
| `service` | 日志、并发、重试、故障恢复和资源预算 |
| `security` | Web API 的 IP/CIDR 和 API key 限制 |
| `web_ui` | Web 服务的监听地址和端口 |

配置加载器会检查字段名、取值范围和跨字段约束。部署前应执行一次 `--check-config`。

### 同步模式

```yaml
sync:
  mode: "hybrid"  # full, incremental, hybrid

  full_sync:
    strategy: "scan"         # scan, sync, dump_restore
    batch_size: 200
    scan_count: 10000
    preserve_ttl: true
    clear_target: false
    overwrite_existing: true
    verify_migration: true
    verify_mode: "full"       # full, fast

  incremental_sync:
    enabled: true
    method: "psync"           # psync, sync, scan
    apply_mode: "key_state"
    interval: 30               # 仅 scan 轮询使用
    max_changes_per_sync: 10000 # 仅 scan 轮询使用
    command_dedup_window: 0
    target_command_timeout: 5
    target_connection_idle_timeout: 60
    capture_max_size: 1073741824
```

`full` 只执行一次全量同步；`incremental` 跳过独立的 full 阶段，但 PSYNC 首次连接或无法续传时仍会通过 `FULLRESYNC` 对齐目标；`hybrid` 先建立一致性边界并完成全量对齐，再进入增量同步。

### 过滤范围

```yaml
sync:
  full_sync:
    key_pattern: "*"
    key_types: null
  incremental_sync:
    key_pattern: "*"
    key_types: null
  filters:
    include_patterns:
      - "user:*"
      - "session:*"
    exclude_patterns:
      - "session:temporary:*"
    min_ttl: 60
    max_key_size: 104857600
```

`hybrid` 模式要求 `full_sync` 和 `incremental_sync` 的 `key_pattern`、`key_types` 一致。恢复故障目标时，服务只重建受管范围，范围外的目标键会保留。

`clear_target: true` 会在复制前对目标数据库执行 `FLUSHDB`，不受过滤范围限制。生产环境启用前应单独确认这一行为。

### 连接池

源端连接池的最小容量为启用目标数。使用 `incremental` 或 `hybrid` 且增量方法为 `psync`/`sync` 时，还要为复制流增加 1 条连接。例如 4 个启用目标需要至少 5 条源连接。

PSYNC 实时同步下，如果显式设置目标连接池，每个启用目标至少需要 2 条连接：一条用于实时写入，一条用于扫描或恢复。配置检查会拒绝低于这些下限的设置。

### TLS 和网络参数

源端和目标端支持相同的 TLS、连接超时和 socket 参数：

```yaml
source:
  host: "redis.example.com"
  port: 6380
  password: "redis-password"
  ssl: true
  ssl_cert_reqs: "required"
  socket_timeout: 60
  socket_connect_timeout: 30
  socket_keepalive: true

service:
  retry:
    max_attempts: 5
    initial_delay: 1
    backoff_factor: 2
    max_delay: 60
  failover:
    enabled: true
    max_failures: 5
    failure_window: 300
    recovery_delay: 60
```

`health_check_interval` 是 redis-py 对空闲连接的检查间隔：连接空闲超过该时间后，在下一条命令前检查连接。它不是后台巡检任务。链路不稳定时，应根据实际 RTT、最长可接受阻塞时间和 Redis 服务端超时设置调整上述参数。

## 同步语义

### 全量同步

`scan` 和 `dump_restore` 当前都使用 SCAN 枚举键，并通过 DUMP/RESTORE 写入目标，两者只是配置名称不同。`sync` 是兼容名称；客户端不直接将 RDB 装载到目标，实际也走相同路径。

全量读取每组最多处理 200 个键。每轮在源端通过一次原子 `EVAL` 检查 `PTTL`，配置类型过滤时检查 `TYPE`，随后检查 `MEMORY USAGE`，符合条件后才执行 `DUMP`。单轮返回的 DUMP payload 预算约为 16 MiB；达到预算后会分轮读取并先写入上一轮结果。单个键可以超过 16 MiB，但仍受 `sync.filters.max_key_size` 限制。

目标端 RESTORE 管道同样限制为每组最多 200 个键、累计约 16 MiB。`full_sync.batch_size` 大于 200 不会继续扩大实际管道批次。

TTL 默认保留。源端允许 `TIME` 时使用源 Redis 的绝对时间计算截止时间；`TIME` 不可用时，服务会扣除请求耗时并生成较保守的截止时间。

当 `sync.mode: full` 且 `verify_migration: true` 时，同步结束后执行抽样验证：

- `verify_mode: full` 比较键是否存在、类型、值和 TTL。
- `verify_mode: fast` 只检查存在性和类型，不比较值。

`hybrid` 的首次对齐和故障恢复不执行这项抽样验证，它们依靠复制边界和后续命令回放完成衔接。

### 增量同步

`sync.incremental_sync.method` 支持：

- `psync`：读取 Redis PSYNC 复制流。断线后尝试续传；源端复制 backlog 已不包含所需 offset 时会收到 `FULLRESYNC`，随后重新对齐目标。
- `sync`：兼容名称，常驻服务按 `psync` 处理。
- `scan`：定时构建源端快照，根据键的序列化内容和绝对过期时间指纹识别新增、修改、删除和 TTL 变化。

常驻服务要求 `apply_mode: key_state`。每次变更都按源端当前键状态执行 `RESTORE` 或 `DEL`，便于安全重试并保持过滤语义；`direct` 会在配置检查时被拒绝。

PSYNC 只同步数据库中的键。复制流中的 `PUBLISH`、`SPUBLISH`、`FUNCTION` 和 `SCRIPT` 会跳过，后续写入照常处理；订阅消息、函数库和脚本缓存不在同步范围内。

SCAN 向目标写入前，会将本轮涉及的键标记为待确认。所有参与本轮同步的目标成功后才提交指纹；部分写入失败时保留标记。即使源键在重试前被删除或改回旧值，后续轮询仍会补做删除或覆盖。

`command_dedup_window` 必须为 `0`。复制流可以连续出现内容相同的合法写命令，按命令内容做时间窗口去重会丢失变更。

PSYNC 实时写入中，`target_command_timeout` 是单个目标写入的硬截止时间，默认 5 秒。超时后服务会断开该目标的底层连接，其他健康目标继续同步。空闲实时写连接在 `target_connection_idle_timeout` 到期后回收。

TTL 以绝对毫秒时间写入目标。源 Redis、同步主机和目标 Redis 的系统时钟应保持同步，时钟偏差会直接反映到过期时间上。

目标不支持 `RESTORE ABSTTL` 时，服务使用临时键、`PEXPIREAT` 和原子重命名保留截止时间。DUMP 格式不兼容时，字符串和常见容器类型会在 `WATCH` 保护下按类型复制；Stream 键不走该降级路径，以免丢失 consumer group 和 PEL 语义。

### 混合同步和恢复

`hybrid` 使用 `psync`/`sync` 时，服务先建立复制流和首次 `FULLRESYNC` 边界，再执行全量 DUMP/RESTORE 对齐，最后回放对齐期间捕获的命令。这样不会在“全量结束、复制流建立”之间留下写入空档。

对齐期间尚未消费的复制流会从内存转存到系统临时目录中的 `redis-sync-repl-*.spool` 文件。内存和磁盘缓存总量受 `capture_max_size` 限制，默认 1 GiB；超过上限会中止本次复制。服务启动时会清理确认未被其他进程占用的遗留 spool 文件。

使用 PSYNC 时，源 Redis 的复制 backlog 应覆盖预期的断线窗口。backlog 不足不会静默跳过数据，但会触发新的 `FULLRESYNC` 和目标重建。

使用 `scan` 时，服务先记录源快照，再进行全量迁移和对账，然后进入轮询。

PSYNC 多目标同步共享一条源复制流，SCAN 增量同步每轮共享一份检测快照。`full` 模式仍会按目标分别执行源端扫描和迁移。增量写入按目标并发执行；单个目标失败后会从活动分发集合中摘除，恢复时先在命令屏障内完成受管范围的全量对齐，再重新加入实时分发。可承载的目标数取决于源端连接池、链路 RTT、带宽、同步主机资源和目标写入能力，应按实际数据量压测。

### Redis ACL

全量同步要求源端允许 `SCAN`、`EVAL`、`PTTL`、`MEMORY USAGE` 和 `DUMP`；配置类型过滤时还需要 `TYPE`。`TIME` 可选，权限受限时会使用前述保守 TTL 计算。

PSYNC 模式还需要源端账号具备复制握手和读取复制流所需权限。目标端账号需要允许实际写入涉及的 `RESTORE`、`DEL`、`PEXPIREAT`、`RENAME` 和兼容路径使用的 `RENAMENX`。`clear_target: true` 需要 `FLUSHDB`，`overwrite_existing: false` 需要 `EXISTS`。启用动态过滤删除时，目标端还会通过 `EVAL` 调用 `EXISTS`、`TYPE`、`TTL`、`MEMORY USAGE` 和 `DEL`。建议在上线前使用与服务相同的 Redis 账号跑一轮全量和增量测试。

## systemd 部署

仓库中的 [redis-sync.service](redis-sync.service) 使用以下路径：

- 程序目录：`/opt/redis-sync`
- 配置文件：`/etc/redis-sync/config.yaml`
- 应用日志：`/var/log/redis-sync/redis-sync.log`
- systemd 日志：`journalctl -u redis-sync`

安装服务：

```bash
sudo useradd --system --home-dir /opt/redis-sync --shell /usr/sbin/nologin redis-sync
sudo install -d -m 0755 -o root -g root /opt/redis-sync
sudo git clone https://github.com/tigerainiuniu/redisSync.git /opt/redis-sync
sudo python3 -m venv /opt/redis-sync/.venv
sudo /opt/redis-sync/.venv/bin/pip install -r /opt/redis-sync/requirements.txt

sudo install -d -m 0750 -o root -g redis-sync /etc/redis-sync
sudo install -d -m 0750 -o redis-sync -g redis-sync /var/log/redis-sync
sudo install -m 0640 -o root -g redis-sync \
  /opt/redis-sync/config.yaml.example /etc/redis-sync/config.yaml
sudo editor /etc/redis-sync/config.yaml
```

将 `service.logging.file` 设置为 `/var/log/redis-sync/redis-sync.log`，然后检查配置并启动：

```bash
sudo -u redis-sync /opt/redis-sync/.venv/bin/python \
  /opt/redis-sync/run_sync_service.py \
  --config /etc/redis-sync/config.yaml --check-config

sudo install -m 0644 /opt/redis-sync/redis-sync.service \
  /etc/systemd/system/redis-sync.service
sudo systemctl daemon-reload
sudo systemctl enable --now redis-sync
```

常用管理命令：

```bash
systemctl status redis-sync
journalctl -u redis-sync -f
sudo systemctl restart redis-sync
sudo systemctl stop redis-sync
```

unit 使用 `Restart=always`。同步线程异常导致进程退出时，systemd 会重新拉起服务；`systemctl stop redis-sync` 是主动停止，不会触发重启。修改 unit 后需要执行 `sudo systemctl daemon-reload`。

配置放在 `/etc/redis-sync`，更新代码时不会被仓库文件覆盖。更新后先重新安装依赖并检查现有配置，再重启服务：

```bash
cd /opt/redis-sync
sudo git pull --ff-only
sudo /opt/redis-sync/.venv/bin/pip install -r requirements.txt
sudo -u redis-sync /opt/redis-sync/.venv/bin/python \
  run_sync_service.py --config /etc/redis-sync/config.yaml --check-config
sudo systemctl restart redis-sync
```

## Web 状态和 API

默认 `web_ui.host: 127.0.0.1` 只监听本机。需要启用认证时，可配置 API key 和允许访问的网段：

```yaml
security:
  auth_enabled: true
  api_key: "replace-with-a-secret"
  allowed_ips:
    - "127.0.0.1"
    - "10.0.0.0/8"

web_ui:
  enabled: true
  host: "127.0.0.1"
  port: 8080
```

页面和 API 使用同一套 IP/CIDR 与 API key 校验。API key 可通过以下任一方式传递：

```bash
curl -H 'X-API-Key: replace-with-a-secret' http://127.0.0.1:8080/api/status
curl -H 'Authorization: Bearer replace-with-a-secret' http://127.0.0.1:8080/api/status
curl -u 'redis-sync:replace-with-a-secret' http://127.0.0.1:8080/api/status
```

Basic Auth 的用户名没有固定值，密码必须是 `security.api_key`。`/api/config` 会递归隐藏密码、token、API key、URL 和 DSN 等敏感字段。

`/api/status` 的 `running` 表示服务进程正在工作，`healthy` 还会检查目标可用性，以及当前使用的复制流或 SCAN 轮询状态。`replication` 中的主要字段包括：

- `baseline_established`：首次 FULLRESYNC 基线是否已建立
- `committed_offset`、`received_offset`：已提交和已接收的复制 offset
- `pending_offset_bytes`：尚未提交的复制流字节数
- `callback_duration`：最近一次目标应用耗时
- `stalled`、`last_error`：复制阻塞和最近错误

SCAN 增量同步通过 `scan` 返回以下字段，其他同步方式下该字段为 `null`：

- `in_progress`：是否正在执行一轮同步
- `last_attempt_time`、`last_success_time`：最近一轮开始时间、最近成功时间，均为 Unix 秒时间戳；尚未发生时为 `null`
- `last_error`：最近一轮的错误，成功后清空
- `healthy`：已完成至少一轮同步，且最近一轮没有报错、服务仍在运行

首次 SCAN 完成前、源端扫描失败、读取待同步数据失败或内存预算超限时，整体 `healthy` 为 `false`；后续轮询成功后恢复。没有键变更的一轮也算成功。

`running: true`、`healthy: false` 会在页面上显示为“运行异常”。

监听 `0.0.0.0`、`::` 或其他非 loopback 地址时，配置检查要求满足以下条件之一：

- 启用 `security.auth_enabled` 并设置非空 `security.api_key`。
- 保持认证关闭，但 `security.allowed_ips` 必须是非空列表，且只能包含 loopback IP/CIDR。这种配置用于同机反向代理访问。

对外访问时，建议由 TLS 反向代理转发到 `127.0.0.1:8080`，同时启用 API key。内置 Web 服务最多同时处理 32 个请求，超出后返回 `503 Service Unavailable`；连接数、请求速率和总请求时限应由反向代理控制。

## 调优和排障

参数需要结合同步方式、数据规模和链路情况调整：

| 参数 | 说明 |
| --- | --- |
| `incremental_sync.method` | 低延迟持续同步使用 `psync`；不具备复制流条件时使用 `scan` |
| `incremental_sync.interval` | 仅影响 SCAN 轮询；间隔越短，源端扫描频率越高 |
| `full_sync.batch_size` | 实际批次最多 200 键；大于 200 不会扩大管道 |
| `service.performance.pipeline_batch_size` | 取值 1-200，默认 100 |
| `full_sync.scan_count` / `service.performance.scan_count` | 硬上限 100000；根据键大小、源端负载和链路 RTT 压测调整 |
| `service.performance.max_workers` | 控制 SCAN 增量同步的目标并发，默认 8；其他同步路径使用内部最多 8 个工作线程 |
| `incremental_sync.capture_max_size` | FULLRESYNC 对齐期间未消费复制流的内存与磁盘总上限 |
| `service.performance.memory_limit` | SCAN 快照、待确认键、变更列表及暂存 DUMP 数据的估算预算，不是操作系统级进程内存上限 |

`queue_size`、内置指标采集、通知和配置加密字段仅为配置兼容保留，当前版本不会启动对应的队列、指标、通知或加密组件。

遇到问题时先收集以下信息：

```bash
# 配置检查
python3 run_sync_service.py --config config.yaml --check-config

# Redis 连通性
redis-cli -h SOURCE_HOST -p 6379 PING
redis-cli -h TARGET_HOST -p 6379 PING

# 前台运行时的应用日志
tail -f redis-sync.log

# systemd 部署日志
journalctl -u redis-sync -n 200 --no-pager

# Web 状态
curl -H 'X-API-Key: API_KEY' http://127.0.0.1:8080/api/status
```

常见情况：

- PSYNC 频繁出现 `FULLRESYNC`：检查网络断开记录和源端 `INFO replication`，确认复制 backlog 能覆盖预期断线时间。
- SCAN 模式延迟偏高：检查 `incremental_sync.interval`。缩短间隔会增加源 Redis 的扫描负载。
- SCAN 内存预算超限：缩小受管键范围，或在评估主机内存后提高 `memory_limit`。降低 `scan_count` 和 `pipeline_batch_size` 只能缓解瞬时峰值；超限时本轮扫描失败且不会推进检查点。
- 单个目标反复恢复：检查该目标的写入延迟、`target_command_timeout`、连接池和服务日志；健康目标在此期间仍会继续同步。
- 配置检查失败：按错误信息修正未知字段、连接池下限、混合模式过滤范围或 Web 外部监听限制。

提交问题时请附上已脱敏的配置、Python/Redis 版本、复现步骤和相关日志：[GitHub Issues](https://github.com/tigerainiuniu/redisSync/issues)。

## 开发和测试

安装开发依赖并运行本地检查：

```bash
python3 -m pip install -e '.[dev]'
python3 -m pytest tests -q
python3 -m compileall -q redis_sync run_sync_service.py scripts
python3 setup.py check
flake8 redis_sync/
```

仓库提供一个由 1 个源 Redis 和 4 个目标 Redis 组成的本地回归环境：

```bash
cd docker/redis-five
docker compose up -d
cd ../..
python3 scripts/e2e_five_redis.py
docker compose -f docker/redis-five/docker-compose.yml down
```

提交代码前请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。功能建议和缺陷报告使用 [GitHub Issues](https://github.com/tigerainiuniu/redisSync/issues)。

## 许可证

项目使用 [MIT License](LICENSE)。

Copyright (c) 2025 redisSync Contributors
