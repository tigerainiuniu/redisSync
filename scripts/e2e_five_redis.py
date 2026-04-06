#!/usr/bin/env python3
"""
五 Redis Docker 联调冒烟：先写源库，再对每个目标跑一轮全量 SCAN 迁移并校验。

前置:
  cd docker/redis-five && docker compose up -d
  等待 healthcheck 就绪（约数秒）

在仓库根目录执行:
  python3 scripts/e2e_five_redis.py
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import redis

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from redis_sync.connection_manager import RedisConnectionManager
from redis_sync.migration_orchestrator import (
    MigrationConfig,
    MigrationOrchestrator,
    MigrationStrategy,
    MigrationType,
)


def wait_ping(r: redis.Redis, label: str, attempts: int = 30) -> None:
    for i in range(attempts):
        try:
            if r.ping():
                return
        except redis.ConnectionError:
            pass
        time.sleep(0.5)
    raise SystemExit(f"无法在约 {attempts * 0.5:.0f}s 内连上 {label}")


def main() -> None:
    p = argparse.ArgumentParser(description="五 Redis 全量同步冒烟")
    p.add_argument("--host", default="127.0.0.1", help="Redis 宿主机地址")
    p.add_argument("--source-port", type=int, default=6378)
    p.add_argument(
        "--target-ports",
        default="6380,6381,6382,6383",
        help="目标端口列表，逗号分隔",
    )
    args = p.parse_args()
    target_ports = [int(x.strip()) for x in args.target_ports.split(",") if x.strip()]

    src = redis.Redis(
        host=args.host, port=args.source_port, decode_responses=False, socket_timeout=30
    )
    wait_ping(src, f"源 {args.host}:{args.source_port}")

    for port in target_ports:
        c = redis.Redis(host=args.host, port=port, decode_responses=False, socket_timeout=30)
        wait_ping(c, f"目标 {args.host}:{port}")

    src.flushdb()
    for port in target_ports:
        redis.Redis(host=args.host, port=port, decode_responses=False).flushdb()

    test_keys = {
        b"e2e:string": b"hello-docker-five",
        b"e2e:counter": b"42",
        b"e2e:bin": bytes(range(256)),
    }
    for k, v in test_keys.items():
        src.set(k, v)

    cfg = MigrationConfig(
        strategy=MigrationStrategy.SCAN,
        migration_type=MigrationType.FULL,
        full_strategy="scan",
        clear_target=True,
        key_pattern="*",
        batch_size=100,
        scan_count=1000,
        preserve_ttl=True,
        verify_migration=False,
    )

    failed = 0
    for port in target_ports:
        name = f"target-{port}"
        print(f"\n=== 全量迁移 → {args.host}:{port} ===")
        mgr = RedisConnectionManager()
        try:
            mgr.connect_source(host=args.host, port=args.source_port, db=0)
            mgr.connect_target(host=args.host, port=port, db=0)
            orch = MigrationOrchestrator(mgr)
            orch.initialize_handlers(scan_count=1000)
            res = orch.migrate(cfg)
            if not res.get("success"):
                print("失败:", res.get("errors"))
                failed += 1
                continue
        finally:
            mgr.close_connections()

        tgt = redis.Redis(host=args.host, port=port, decode_responses=False)
        for k, expect in test_keys.items():
            got = tgt.get(k)
            if got != expect:
                print(f"  键 {k!r} 不一致: 期望 {len(expect)}B, 得到 {len(got) if got else 0}B")
                failed += 1
            else:
                print(f"  OK {k.decode('utf-8', errors='replace')}")

    if failed:
        raise SystemExit(f"结束: {failed} 项检查失败")
    print("\n全部目标校验通过。")


if __name__ == "__main__":
    main()
