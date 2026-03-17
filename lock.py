"""
Redis 分布式锁管理（shard 级别）。

每个 shard 对应一个 Redis key：
  cf:shard-lock:<stream_name>:<shard_id>
值为当前 worker 的唯一 ID（UUID），用于续期和释放时的身份校验。

Worker 心跳 key：
  cf:worker:<stream_name>:<worker_id>
用于统计在线 Pod 数量，实现 rebalance 时的公平份额计算。

锁的生命周期：
  - 抢锁：SET NX PX <ttl_ms>，成功则持有
  - 续期：每隔 heartbeat_interval 秒重置 TTL，只有持有者才能续期
  - 释放：Lua 脚本原子校验 + 删除，防止误删他人的锁
  - 自动过期：Pod 崩溃后 TTL 到期，其他 Pod 自动接管

Rebalance（主动让出）：
  每次 rebalance 时计算公平份额 = ceil(总 shard 数 / 在线 Pod 数)。
  若本 Pod 持有的 shard 数超过份额，主动释放多余的锁，让新 Pod 有机会抢到。
  这样扩容后无需重启老 Pod，新 Pod 自然接管部分 shard。
"""
import math
import logging
import threading
import uuid

import redis

logger = logging.getLogger(__name__)


class ShardLockManager:
    _RELEASE_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    else
        return 0
    end
    """

    def __init__(self, redis_client: redis.Redis, stream_name: str,
                 lock_ttl: int = 30, heartbeat_interval: float = 10.0):
        self._redis = redis_client
        self._stream_name = stream_name
        self._lock_ttl_ms = lock_ttl * 1000
        self._worker_ttl_ms = lock_ttl * 3 * 1000  # worker 心跳 TTL 略长于锁 TTL
        self._heartbeat_interval = heartbeat_interval
        self._worker_id = str(uuid.uuid4())
        self._held: set[str] = set()
        self._stop_heartbeat = threading.Event()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="shard-lock-heartbeat"
        )
        self._heartbeat_thread.start()
        # 注册 worker 心跳
        self._redis.set(self._worker_key(), self._worker_id, px=self._worker_ttl_ms)
        logger.info(f"ShardLockManager started, worker_id={self._worker_id}")

    def _key(self, shard_id: str) -> str:
        return f"cf:shard-lock:{self._stream_name}:{shard_id}"

    def _worker_key(self) -> str:
        return f"cf:worker:{self._stream_name}:{self._worker_id}"

    def online_worker_count(self) -> int:
        """统计当前在线的 worker 数量（通过 Redis key 扫描）。"""
        try:
            keys = self._redis.keys(f"cf:worker:{self._stream_name}:*")
            return max(len(keys), 1)
        except Exception as e:
            logger.warning(f"Failed to count online workers: {e}")
            return 1

    def fair_share(self, total_shards: int) -> int:
        """计算本 Pod 应持有的公平 shard 份额 = ceil(总 shard 数 / 在线 Pod 数)。"""
        return math.ceil(total_shards / self.online_worker_count())

    def try_acquire(self, shard_id: str) -> bool:
        """尝试抢占 shard 锁，成功返回 True。"""
        ok = self._redis.set(self._key(shard_id), self._worker_id, px=self._lock_ttl_ms, nx=True)
        if ok:
            self._held.add(shard_id)
            logger.info(f"Acquired lock for shard {shard_id}")
        return bool(ok)

    def release(self, shard_id: str):
        """释放 shard 锁（仅释放自己持有的，Lua 脚本原子校验）。"""
        if shard_id not in self._held:
            return
        self._redis.eval(self._RELEASE_SCRIPT, 1, self._key(shard_id), self._worker_id)
        self._held.discard(shard_id)
        logger.info(f"Released lock for shard {shard_id}")

    def release_all(self):
        """释放本 Pod 持有的所有 shard 锁。"""
        for shard_id in list(self._held):
            self.release(shard_id)

    def held_count(self) -> int:
        return len(self._held)

    def held_shards(self) -> list[str]:
        """返回当前持有锁的 shard ID 列表（有序，用于 rebalance 时确定性地选择释放目标）。"""
        return sorted(self._held)

    def is_held(self, shard_id: str) -> bool:
        return shard_id in self._held

    def _heartbeat_loop(self):
        """定期续期所有持有的锁和 worker 心跳，防止 TTL 到期。"""
        while not self._stop_heartbeat.wait(self._heartbeat_interval):
            # 续期 worker 心跳
            try:
                self._redis.pexpire(self._worker_key(), self._worker_ttl_ms)
            except Exception as e:
                logger.error(f"Worker heartbeat failed: {e}")

            # 续期 shard 锁
            for shard_id in list(self._held):
                key = self._key(shard_id)
                try:
                    current = self._redis.get(key)
                    if current and current.decode() == self._worker_id:
                        self._redis.pexpire(key, self._lock_ttl_ms)
                    else:
                        logger.warning(f"Lost lock for shard {shard_id}, removing from held set")
                        self._held.discard(shard_id)
                except Exception as e:
                    logger.error(f"Heartbeat failed for shard {shard_id}: {e}")

    def stop(self):
        """停止心跳线程，释放所有锁，注销 worker 心跳。"""
        self._stop_heartbeat.set()
        self.release_all()
        try:
            self._redis.delete(self._worker_key())
        except Exception:
            pass
