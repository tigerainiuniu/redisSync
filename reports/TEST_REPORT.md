# Redis 同步项目 — 测试报告

**生成时间**：2026-04-06（本轮复测）  
**项目路径**：仓库根目录（原 `temp_review` 内容已提升至根目录）  
**执行者**：自动化测试（Docker Compose / pytest / compileall / 配置检查 / E2E）

---

## 1. 摘要

| 类别 | 结果 | 说明 |
|------|------|------|
| Docker Compose（`docker/redis-five`） | **通过** | 5 容器均为 `healthy` |
| 单元/集成测试（pytest） | **通过** | 16/16，0 失败、0 跳过、0 错误 |
| 语法编译（compileall） | **通过** | `redis_sync` 与 `run_sync_service.py` |
| 配置检查（`--check-config`） | **通过** | `config.docker.yaml` 解析与源/目标列表正确 |
| 五 Redis E2E（`scripts/e2e_five_redis.py`） | **通过** | 源写入 → 对 4 目标各一轮全量 SCAN → 逐键 GET 校验 |

**结论**：**本轮单测、配置检查与 Docker 五实例 E2E 全部通过。**

### 端口说明（重要）

此前宿主机 **6379** 已被占用，`redis-five-src` 无法绑定。仓库已将 **源 Redis 宿主机端口改为 6378**（容器内仍为 6379），与 `config.docker.yaml`、`e2e_five_redis.py` 默认参数一致；目标仍为 **6380–6383**。若你本地没有占用 6379，也可自行把 compose 改回 `6379:6379` 并同步改配置与脚本参数。

---

## 2. 环境与命令

- **Python**：3.13.2  
- **pytest**：9.0.2  
- **Docker**：可用（`docker info` 正常）

### 2.1 Docker

```bash
cd docker/redis-five
docker compose up -d --force-recreate
docker compose ps
```

**结果**：`redis-five-src` → `0.0.0.0:6378->6379`；`t1`–`t4` → `6380`–`6383`，均为 `healthy`。

### 2.2 pytest

```bash
python3 -m pytest tests/ -v --tb=short --junitxml=reports/junit.xml
```

（于仓库根目录执行。）

**退出码**：0（约 0.45s）

### 2.3 compileall

```bash
python3 -m compileall -q redis_sync run_sync_service.py
```

**退出码**：0

### 2.4 配置检查

```bash
python3 run_sync_service.py --config docker/redis-five/config.docker.yaml --check-config
```

**退出码**：0；源 `127.0.0.1:6378`，目标 4 个端口 6380–6383。

### 2.5 E2E

```bash
python3 scripts/e2e_five_redis.py
```

**退出码**：0；四个目标均输出 `OK e2e:string` / `e2e:counter` / `e2e:bin`，结尾 `全部目标校验通过。`

---

## 3. 机器可读产物

- **JUnit XML**：`reports/junit.xml`

---

## 4. 可选后续

长驻混合同步 + Web：`python3 run_sync_service.py --config docker/redis-five/config.docker.yaml`（默认 Web `http://127.0.0.1:18080`）。

停止容器：`cd docker/redis-five && docker compose down`。

---

## 5. 可维护性说明

将源端口从 6379 改为 **6378** 后，与常见本机 Redis 冲突概率更低，E2E 默认与 Compose 一致，减少「容器起不来 / 连错库」类问题。若需在 CI 固定 6379，应保证 job 内无其他服务占用该端口，或继续使用 6378 映射策略。
