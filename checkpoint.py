"""
消费位点管理。

优先使用 Redis 存储（推荐）：
  key:   cf:checkpoint:<stream_name>:<shard_id>
  value: SequenceNumber 字符串

  优点：Pod 重启或重新调度后位点不丢失，多 Pod 共享，与分布式锁复用同一 Redis 实例。

降级为内存存储（无 Redis 时）：
  位点仅保存在进程内存中，Pod 重启后丢失，会从 initial_position 重新消费。
  仅适用于单实例、可接受少量重复消费的场景。
"""
import logging

import redis as redis_lib

logger = logging.getLogger(__name__)


class Checkpoint:
    def __init__(self, redis_client: redis_lib.Redis | None, stream_name: str):
        self._redis = redis_client
        self._stream_name = stream_name
        self._mem: dict[str, str] = {}  # 无 Redis 时的内存降级存储

    @property
    def redis_client(self) -> redis_lib.Redis | None:
        """暴露底层 Redis 客户端，供需要直接操作 Redis 的场景使用（如 backfill checkpoint）。"""
        return self._redis

    def _key(self, shard_id: str) -> str:
        return f"cf:checkpoint:{self._stream_name}:{shard_id}"

    def get(self, shard_id: str) -> str | None:
        """获取指定 shard 的最后消费位点，不存在时返回 None。"""
        if self._redis is None:
            return self._mem.get(shard_id)
        try:
            val = self._redis.get(self._key(shard_id))
            return val.decode() if val else None
        except Exception as e:
            logger.warning(f"Checkpoint get failed for {shard_id}: {e}")
            return None

    def save(self, shard_id: str, seq: str):
        """更新指定 shard 的消费位点。"""
        if self._redis is None:
            self._mem[shard_id] = seq
            return
        try:
            self._redis.set(self._key(shard_id), seq)
        except Exception as e:
            logger.error(f"Checkpoint save failed for {shard_id}: {e}")

    def delete(self, shard_id: str):
        """删除指定 shard 的位点（checkpoint 过期或 shard 关闭时调用）。"""
        if self._redis is None:
            self._mem.pop(shard_id, None)
            return
        try:
            self._redis.delete(self._key(shard_id))
        except Exception as e:
            logger.warning(f"Checkpoint delete failed for {shard_id}: {e}")

    def all(self) -> dict[str, str]:
        """返回当前 stream 所有 shard 的位点。"""
        if self._redis is None:
            return dict(self._mem)
        try:
            pattern = f"cf:checkpoint:{self._stream_name}:*"
            prefix_len = len(f"cf:checkpoint:{self._stream_name}:")
            result: dict[str, str] = {}
            cursor = 0
            while True:
                cursor, keys = self._redis.scan(cursor, match=pattern, count=100)
                if keys:
                    values = self._redis.mget(keys)
                    for k, v in zip(keys, values):
                        if v is not None:
                            result[k.decode()[prefix_len:]] = v.decode()
                if cursor == 0:
                    break
            return result
        except Exception as e:
            logger.warning(f"Checkpoint all() failed: {e}")
            return {}
