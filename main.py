"""
CloudFront Kinesis Log Consumer — 入口

用法：
  python main.py                             # 正常启动（持续消费）
  python main.py --list-checkpoints          # 查看当前消费位点
  python main.py --backfill \
    --start-time 2026-03-01T00:00:00+00:00 \
    --end-time   2026-03-02T00:00:00+00:00   # 补跑历史数据（单次运行，完成后自动退出）
                                              # 使用独立 backfill checkpoint，不影响正常消费位点
  python main.py --debug-format \
    --start-time 2026-03-17T08:00:00+00:00 \
    --limit 20                               # 调试：输出处理后的日志 JSON，不写 Doris，不更新 checkpoint

K8S 分布式部署（基于 Redis 分布式锁）：
  每个 Pod 启动后自动抢占空闲 shard 的分布式锁，无需手动配置序号。
  Pod 扩缩容时锁自动释放和重新分配，支持随时灵活扩缩容，无漏消费/重复消费风险。
  需在 config.yaml 中配置 redis 连接信息，或通过环境变量注入：
    CF_REDIS__HOST=redis-host
    CF_REDIS__PORT=6379
    CF_REDIS__PASSWORD=secret

  Redis 同时用于 checkpoint 持久化和分布式锁，两者共用同一连接。
  未配置 Redis 时以单实例模式启动（打印 WARNING），checkpoint 降级为内存存储（重启后丢失）。
  已配置 Redis 但连接失败时直接退出，避免多 Pod 无锁运行导致重复消费。
  backfill/debug 模式下不连接 Redis，不读写 checkpoint，不使用分布式锁，可独立运行。
"""
import argparse
import json
import logging
import signal
import sys
from datetime import datetime, timezone

import redis

import config as cfg_module
from checkpoint import Checkpoint
from consumer import StreamConsumer, run_debug_format
from lock import ShardLockManager
from transformer import Transformer
from writer import DorisWriter

cfg_module.setup_logging()
logger = logging.getLogger(__name__)


def _build_redis_client(cfg: dict) -> redis.Redis | None:
    r_cfg = cfg.get("redis")
    if not r_cfg or not r_cfg.get("host"):
        return None
    client = redis.Redis(
        host=r_cfg["host"],
        port=r_cfg.get("port", 6379),
        password=r_cfg.get("password") or None,
        db=r_cfg.get("db", 0),
        decode_responses=False,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    client.ping()
    logger.info(f"Redis connected: {r_cfg['host']}:{r_cfg.get('port', 6379)}")
    return client


def main():
    parser = argparse.ArgumentParser(description="CloudFront Kinesis Log Consumer")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--list-checkpoints", action="store_true")
    parser.add_argument("--backfill", action="store_true")
    parser.add_argument("--start-time")
    parser.add_argument("--end-time")
    parser.add_argument("--debug-format", action="store_true")
    parser.add_argument("--limit", type=int, default=10)
    args = parser.parse_args()

    cfg = cfg_module.load(args.config)

    stream_name = cfg["kinesis"]["stream_name"]

    if args.list_checkpoints:
        try:
            rc = _build_redis_client(cfg)
        except Exception as e:
            logger.error(f"Redis connection failed: {e}")
            sys.exit(1)
        ckpt = Checkpoint(rc, stream_name)
        print(f"\n=== {stream_name} ===")
        print(json.dumps(ckpt.all(), indent=2))
        return

    if args.debug_format:
        run_debug_format(cfg, at_timestamp=args.start_time, limit=args.limit)
        return

    backfill_end: datetime | None = None
    if args.backfill:
        if not args.start_time:
            parser.error("--backfill 需要 --start-time")
        if args.end_time:
            backfill_end = datetime.fromisoformat(args.end_time)
            # 统一转为 UTC aware，与 Kinesis ApproximateArrivalTimestamp 保持一致
            if backfill_end.tzinfo is None:
                backfill_end = backfill_end.replace(tzinfo=timezone.utc)
        cfg["kinesis"]["initial_position"] = "AT_TIMESTAMP"
        cfg["kinesis"]["initial_timestamp"] = args.start_time
        logger.info(f"Backfill mode: {args.start_time} -> {args.end_time or 'now'}")

    # Redis 是正常消费模式的依赖（checkpoint 持久化 + 分布式锁）
    # backfill/debug 模式不读写 checkpoint、不需要分布式锁，跳过 Redis 连接
    redis_client: redis.Redis | None = None
    redis_cfg = cfg.get("redis", {})
    if not args.backfill and not args.debug_format:
        if redis_cfg.get("host"):
            try:
                redis_client = _build_redis_client(cfg)
            except Exception as e:
                logger.error(f"Redis connection failed: {e}")
                sys.exit(1)
        else:
            logger.warning(
                "Redis not configured, running in single-instance mode. "
                "Checkpoint will NOT persist across restarts. "
                "DO NOT deploy multiple pods without Redis."
            )

    # 正常消费模式下启用分布式锁；backfill/debug 模式无需抢锁
    lock_manager: ShardLockManager | None = None
    if redis_client:
        lock_manager = ShardLockManager(
            redis_client,
            stream_name=stream_name,
            lock_ttl=redis_cfg.get("lock_ttl", 30),
            heartbeat_interval=redis_cfg.get("heartbeat_interval", 10.0),
        )

    # backfill/debug 模式传 None，Checkpoint 降级为内存存储（不会实际读写）
    ckpt = Checkpoint(redis_client, stream_name)
    transformer = Transformer(cfg)
    writer = DorisWriter(cfg)
    consumer = StreamConsumer(
        cfg, ckpt, transformer, writer,
        lock_manager=lock_manager,
        backfill=args.backfill,
        backfill_end=backfill_end,
    )

    def _shutdown(sig, frame):
        logger.info("Shutting down...")
        consumer.stop()
        if lock_manager:
            lock_manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    mode = "distributed" if lock_manager else "single-instance"
    logger.info(f"Starting consumer [{mode}] for stream: {stream_name}")
    consumer.run()
    if lock_manager:
        lock_manager.stop()
    logger.info("Consumer finished")


if __name__ == "__main__":
    main()
