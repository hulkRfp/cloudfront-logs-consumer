"""
Kinesis stream 消费主循环。

StreamConsumer 负责：
  - 定期刷新 shard 列表，感知 shard 扩缩容（split/merge）
  - 通过 ShardLockManager 动态抢占/释放 shard（分布式模式）
  - 拉取记录 -> Transformer 解析 -> DorisWriter 写入 -> Checkpoint 更新
  - shard CLOSED 后 NextShardIterator 返回 null，自动结束该 shard 的消费
  - 补跑模式（backfill）：
      * 用 FROM_TIMESTAMP 拿到所有相关 shard（含 CLOSED 的 parent shard）
      * 每批写入后保存独立的 backfill checkpoint（key 前缀 cf:backfill-checkpoint:），
        中断后重启可从断点续跑，不污染正常消费位点
      * 超过 end_time 的记录自动跳过，该 shard 标记完成
      * 所有 shard 完成后自动退出，清理 backfill checkpoint，不影响正常消费的 checkpoint

run_debug_format 用于调试：
  从指定时间点拉取有限条日志，经 Transformer 处理后打印 JSON，不写 Doris，不更新 checkpoint。
"""
import base64
import json
import logging
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

from checkpoint import Checkpoint
from lock import ShardLockManager
from transformer import Transformer
from writer import DorisWriter

logger = logging.getLogger(__name__)


class StreamConsumer:
    def __init__(self, stream_cfg: dict, checkpoint: Checkpoint,
                 transformer: Transformer, writer: DorisWriter,
                 lock_manager: ShardLockManager | None = None,
                 backfill: bool = False,
                 backfill_end: datetime | None = None):
        k = stream_cfg["kinesis"]
        self.stream_name: str = k["stream_name"]
        self.region: str = k["region"]
        self.batch_size: int = k.get("batch_size", 5000)
        self.poll_active: float = k.get("poll_interval_active", 1.0)
        self.poll_idle: float = k.get("poll_interval_idle", 30.0)
        self.shard_refresh: float = k.get("shard_refresh_interval", 60)
        self.initial_position: str = k.get("initial_position", "LATEST").upper()
        self.initial_timestamp: str | None = k.get("initial_timestamp")

        self.lock_manager = lock_manager
        self.backfill = backfill
        self.backfill_end = backfill_end

        self.checkpoint = checkpoint
        self.transformer = transformer
        self.writer = writer
        self.client = boto3.client("kinesis", region_name=self.region)
        self._running = False
        # backfill 专用 checkpoint 前缀，与正常消费位点隔离
        self._bf_ckpt_prefix = f"cf:backfill-checkpoint:{self.stream_name}:"

    def run(self):
        self._running = True
        iterators: dict[str, str] = {}
        closed_shards: set[str] = set()
        last_refresh = 0.0
        # 记录每个 shard 当前处理到的最新 seq，flush 成功后批量更新 checkpoint
        # 多 shard 共享同一 writer，flush 时 buffer 里可能有多个 shard 的数据，
        # 必须对所有涉及的 shard 一起更新 checkpoint，避免部分 shard 数据落盘但位点未推进
        pending_seqs: dict[str, str] = {}

        while self._running:
            if time.monotonic() - last_refresh >= self.shard_refresh:
                try:
                    if self.backfill:
                        # backfill 模式：用 FROM_TIMESTAMP 拿到所有相关 shard（含 CLOSED 的 parent shard）
                        # CLOSED shard 里可能有补跑时间段内的历史数据，不能跳过
                        start_ts = (
                            datetime.fromisoformat(self.initial_timestamp)
                            if self.initial_timestamp else None
                        )
                        shards = self._list_shards(start_time=start_ts)
                        # backfill 时同时消费 OPEN 和 CLOSED shard；EXPIRED shard 不会出现在列表中
                        open_ids = {s["ShardId"] for s in shards}
                    else:
                        shards = self._list_shards()
                        open_ids = {
                            s["ShardId"] for s in shards
                            if "EndingSequenceNumber" not in s.get("SequenceNumberRange", {})
                        }
                    self._rebalance_shards(open_ids, iterators, closed_shards)
                    last_refresh = time.monotonic()
                except Exception as e:
                    logger.error(f"[{self.stream_name}] Failed to refresh shards: {e}")

            any_records = False
            for shard_id in list(iterators):
                last_seq = ""  # 本轮该 shard 最后处理的 SequenceNumber，records 为空时保持空字符串
                if self.lock_manager and not self.lock_manager.is_held(shard_id):
                    logger.warning(f"[{self.stream_name}:{shard_id}] Lock lost, releasing shard")
                    iterators.pop(shard_id, None)
                    continue

                try:
                    records, next_iter, lag = self._get_records(iterators[shard_id])
                except ClientError as e:
                    code = e.response["Error"]["Code"]
                    if code == "ResourceNotFoundException":
                        # shard 已进入 EXPIRED 状态（retention period 到期），数据不可访问
                        # 按需模式下正常的 split/merge 不会触发此错误（CLOSED shard 仍可读）
                        # 此处仅处理极端情况：消费严重滞后导致 shard 过期
                        logger.info(f"[{self.stream_name}:{shard_id}] Shard expired (retention exceeded), releasing")
                        closed_shards.add(shard_id)
                        iterators.pop(shard_id, None)
                        if self.lock_manager:
                            self.lock_manager.release(shard_id)
                    elif code in ("ExpiredIteratorException", "InvalidArgumentException"):
                        logger.warning(f"[{self.stream_name}:{shard_id}] Iterator expired, refreshing")
                        iterators[shard_id] = self._get_iterator(shard_id)
                    else:
                        logger.error(f"[{self.stream_name}:{shard_id}] GetRecords error: {e}")
                    continue
                except Exception as e:
                    logger.error(f"[{self.stream_name}:{shard_id}] GetRecords failed: {e}")
                    try:
                        iterators[shard_id] = self._get_iterator(shard_id)
                    except Exception as ie:
                        logger.error(f"[{self.stream_name}:{shard_id}] Re-fetch iterator failed: {ie}")
                    continue

                if records:
                    any_records = True
                    last_seq, shard_done = self._process(shard_id, records)
                    if self.backfill:
                        if shard_done:
                            # 该 shard 已消费到 end_time，flush 剩余 buffer 后标记完成
                            logger.info(f"[{self.stream_name}:{shard_id}] Reached end_time, done")
                            self.writer.flush()
                            self._bf_ckpt_delete(shard_id)
                            closed_shards.add(shard_id)
                            iterators.pop(shard_id, None)
                            continue
                        # 只有实际写入了记录才保存 backfill checkpoint
                        if last_seq:
                            self._bf_ckpt_save(shard_id, last_seq)
                        if self.writer.should_flush():
                            self.writer.flush()
                    else:
                        # 攒够 batch_size 或超过 flush_interval 才 flush，减少小文件
                        # pending_seqs 记录本轮所有 shard 的最新 seq，flush 后批量更新 checkpoint
                        # 避免多 shard 共享 writer 时只更新触发 flush 的那个 shard 的位点
                        if last_seq:
                            pending_seqs[shard_id] = last_seq
                        if self.writer.should_flush():
                            self.writer.flush()
                            for sid, seq in list(pending_seqs.items()):
                                self.checkpoint.save(sid, seq)
                            pending_seqs.clear()
                    if lag and lag > 60000:
                        logger.warning(f"[{self.stream_name}:{shard_id}] {lag}ms behind")
                elif self.writer.should_flush():
                    # 无新数据时兜底 flush（超过 flush_interval）
                    self.writer.flush()
                    if not self.backfill:
                        for sid, seq in list(pending_seqs.items()):
                            self.checkpoint.save(sid, seq)
                        pending_seqs.clear()

                if next_iter is None:
                    logger.info(f"[{self.stream_name}:{shard_id}] Shard closed")
                    # shard 关闭时强制 flush 剩余 buffer，确保数据不丢
                    if self.writer.has_pending():
                        self.writer.flush()
                        if not self.backfill:
                            for sid, seq in list(pending_seqs.items()):
                                self.checkpoint.save(sid, seq)
                            pending_seqs.clear()
                    if self.backfill:
                        self._bf_ckpt_delete(shard_id)
                    closed_shards.add(shard_id)
                    iterators.pop(shard_id, None)
                    if self.lock_manager:
                        self.lock_manager.release(shard_id)
                else:
                    iterators[shard_id] = next_iter

            if self.backfill and len(iterators) == 0:
                logger.info(f"[{self.stream_name}] Backfill complete, flushing and exiting")
                self.writer.flush()
                self._running = False
                break

            # backfill 模式全速追赶，不等待；正常模式按 poll_interval 控制拉取频率
            if not self.backfill:
                time.sleep(self.poll_active if any_records else self.poll_idle)

    def stop(self):
        self._running = False
        try:
            self.writer.flush()
        except Exception:
            pass

    def _rebalance_shards(self, open_ids: set[str], iterators: dict[str, str], closed_shards: set[str]):
        """
        根据当前 open shard 列表重新分配：
        - 无锁模式：直接持有所有 shard（单实例）
        - 分布式锁模式：
          1. 计算公平份额，若持有超额则主动让出，触发 rebalance
          2. 对所有未在本地消费的 shard 尝试抢锁
          一个 Pod 可持有多个 shard，shard 数 > Pod 数时自动均摊。
        """
        if self.lock_manager:
            total = len(open_ids)
            share = self.lock_manager.fair_share(total)
            held = self.lock_manager.held_count()
            if held > share:
                # 主动让出超额的 shard，held_shards() 返回有序列表保证确定性
                excess = held - share
                to_release = self.lock_manager.held_shards()[:excess]
                for sid in to_release:
                    logger.info(
                        f"[{self.stream_name}] Rebalancing: releasing shard {sid} "
                        f"(held={held}, share={share}, workers={self.lock_manager.online_worker_count()})"
                    )
                    iterators.pop(sid, None)
                    self.lock_manager.release(sid)

        for sid in open_ids:
            if sid in iterators or sid in closed_shards:
                continue
            if self.lock_manager:
                if self.lock_manager.try_acquire(sid):
                    logger.info(f"[{self.stream_name}] Acquired shard: {sid}")
                    iterators[sid] = self._get_iterator(sid)
                # 抢锁失败说明其他 Pod 已持有，跳过；下次 rebalance 仍会重试（应对 Pod 下线场景）
            else:
                logger.info(f"[{self.stream_name}] New shard: {sid}")
                iterators[sid] = self._get_iterator(sid)

        # 移除已不在 open 列表中的 shard（缩容/合并），释放对应的锁
        for sid in list(iterators):
            if sid not in open_ids:
                logger.info(f"[{self.stream_name}] Shard {sid} no longer open, releasing")
                iterators.pop(sid, None)
                if self.lock_manager:
                    self.lock_manager.release(sid)

    def _process(self, shard_id: str, records: list) -> tuple[str, bool]:
        """
        处理一批记录，返回 (last_seq, shard_done)。
        last_seq：本批最后一条记录的 SequenceNumber（无论成功或跳过均推进）；
                  空字符串表示本批没有处理任何记录（不应发生，仅防御性处理）。
        shard_done=True 表示遇到超过 backfill_end 的记录，该 shard 可以停止消费。
        """
        last_seq = ""
        for r in records:
            # backfill 模式下检查记录时间是否超过 end_time
            if self.backfill and self.backfill_end:
                approx_ts = r.get("ApproximateArrivalTimestamp")
                if approx_ts and approx_ts > self.backfill_end:
                    logger.info(
                        f"[{self.stream_name}:{shard_id}] Record timestamp {approx_ts} "
                        f"exceeds end_time {self.backfill_end}, stopping shard"
                    )
                    # 返回已写入的最后一条 seq（last_seq 为空表示本批没有写入任何记录）
                    return last_seq, True
            try:
                raw = base64.b64decode(r["Data"]).decode("utf-8", errors="replace")
                parsed = self.transformer.parse(raw)
                if parsed:
                    self.writer.write(parsed)
            except Exception as e:
                logger.warning(
                    f"[{self.stream_name}:{shard_id}] Skipping record seq={r.get('SequenceNumber', '?')}: {e}"
                )
            last_seq = r["SequenceNumber"]  # 无论成功或跳过，均推进到当前记录的 seq
        return last_seq, False

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=2, max=30),
           before_sleep=before_sleep_log(logger, logging.WARNING), reraise=True)
    def _get_records(self, iterator: str) -> tuple:
        resp = self.client.get_records(ShardIterator=iterator, Limit=self.batch_size)
        return resp.get("Records", []), resp.get("NextShardIterator"), resp.get("MillisBehindLatest")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10),
           before_sleep=before_sleep_log(logger, logging.WARNING), reraise=True)
    def _list_shards(self, start_time: datetime | None = None) -> list[dict]:
        """
        列出 stream 的 shard 列表。

        正常模式：只返回 OPEN shard（默认行为）。
        backfill 模式：传入 start_time，使用 FROM_TIMESTAMP filter，
          返回该时间点之后所有有效 shard，包含 CLOSED（已停止写入但数据未过期）的 parent shard，
          确保补跑时不遗漏 split/merge 前的历史数据。
        """
        shards = []
        if start_time:
            # FROM_TIMESTAMP 返回在该时间点之后存在的所有 shard（含 CLOSED，不含 EXPIRED）
            kwargs: dict = {
                "StreamName": self.stream_name,
                "ShardFilter": {
                    "Type": "FROM_TIMESTAMP",
                    "Timestamp": start_time,
                },
            }
        else:
            kwargs = {"StreamName": self.stream_name}
        while True:
            resp = self.client.list_shards(**kwargs)
            shards.extend(resp.get("Shards", []))
            if not (next_token := resp.get("NextToken")):
                break
            # 翻页时只传 NextToken，不能同时传 StreamName/ShardFilter
            kwargs = {"NextToken": next_token}
        return shards

    def _get_iterator(self, shard_id: str) -> str:
        # backfill 模式：优先从 backfill checkpoint 续跑（支持中断恢复）
        # 正常模式：从正常 checkpoint 续跑
        if self.backfill:
            seq = self._bf_ckpt_get(shard_id)
        else:
            seq = self.checkpoint.get(shard_id)
        if seq:
            try:
                resp = self.client.get_shard_iterator(
                    StreamName=self.stream_name, ShardId=shard_id,
                    ShardIteratorType="AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=seq,
                )
                logger.info(f"[{self.stream_name}:{shard_id}] Resuming from {'backfill ' if self.backfill else ''}checkpoint")
                return resp["ShardIterator"]
            except ClientError as e:
                code = e.response["Error"]["Code"]
                if code in ("InvalidArgumentException", "ResourceNotFoundException"):
                    logger.warning(
                        f"[{self.stream_name}:{shard_id}] Checkpoint invalid/expired ({code}), "
                        f"falling back to {self.initial_position}"
                    )
                    if self.backfill:
                        self._bf_ckpt_delete(shard_id)
                    else:
                        self.checkpoint.delete(shard_id)
                else:
                    raise
        return self._initial_iterator(shard_id)

    def _initial_iterator(self, shard_id: str) -> str:
        if self.initial_position == "AT_TIMESTAMP":
            if not self.initial_timestamp:
                raise ValueError("initial_timestamp required when initial_position=AT_TIMESTAMP")
            resp = self.client.get_shard_iterator(
                StreamName=self.stream_name, ShardId=shard_id,
                ShardIteratorType="AT_TIMESTAMP",
                Timestamp=datetime.fromisoformat(self.initial_timestamp),
            )
        else:
            resp = self.client.get_shard_iterator(
                StreamName=self.stream_name, ShardId=shard_id,
                ShardIteratorType=self.initial_position,
            )
        logger.info(f"[{self.stream_name}:{shard_id}] Starting from {self.initial_position}")
        return resp["ShardIterator"]

    # ── backfill 专用 checkpoint（Redis 存储，key 与正常消费位点隔离）──────────

    def _bf_ckpt_get(self, shard_id: str) -> str | None:
        """读取 backfill checkpoint，无 Redis 时返回 None（每次从头跑）。"""
        redis = self.checkpoint._redis
        if redis is None:
            return None
        try:
            val = redis.get(self._bf_ckpt_prefix + shard_id)
            return val.decode() if val else None
        except Exception as e:
            logger.warning(f"[{self.stream_name}:{shard_id}] Backfill checkpoint get failed: {e}")
            return None

    def _bf_ckpt_save(self, shard_id: str, seq: str):
        """保存 backfill checkpoint。"""
        redis = self.checkpoint._redis
        if redis is None:
            return
        try:
            redis.set(self._bf_ckpt_prefix + shard_id, seq)
        except Exception as e:
            logger.error(f"[{self.stream_name}:{shard_id}] Backfill checkpoint save failed: {e}")

    def _bf_ckpt_delete(self, shard_id: str):
        """shard 完成后清理 backfill checkpoint。"""
        redis = self.checkpoint._redis
        if redis is None:
            return
        try:
            redis.delete(self._bf_ckpt_prefix + shard_id)
        except Exception as e:
            logger.warning(f"[{self.stream_name}:{shard_id}] Backfill checkpoint delete failed: {e}")


def run_debug_format(cfg: dict, at_timestamp: str | None, limit: int):
    """
    从指定时间点（或 TRIM_HORIZON）拉取最多 limit 条日志，
    经 Transformer 处理后以 JSON 格式打印到控制台，不写 Doris，不更新 checkpoint。
    """
    stream_name = cfg["kinesis"]["stream_name"]
    region = cfg["kinesis"]["region"]
    transformer = Transformer(cfg)
    client = boto3.client("kinesis", region_name=region)

    shards: list[dict] = []
    if at_timestamp:
        # FROM_TIMESTAMP 返回该时间点之后所有有效 shard（含 CLOSED），避免遗漏历史数据
        kwargs: dict = {
            "StreamName": stream_name,
            "ShardFilter": {"Type": "FROM_TIMESTAMP", "Timestamp": datetime.fromisoformat(at_timestamp)},
        }
    else:
        kwargs = {"StreamName": stream_name}
    while True:
        resp = client.list_shards(**kwargs)
        shards.extend(resp.get("Shards", []))
        if not (next_token := resp.get("NextToken")):
            break
        kwargs = {"NextToken": next_token}

    # debug 模式消费所有相关 shard（含 CLOSED）
    open_shards = [s["ShardId"] for s in shards]

    collected: list[dict] = []
    for shard_id in sorted(open_shards):
        if len(collected) >= limit:
            break
        if at_timestamp:
            resp = client.get_shard_iterator(
                StreamName=stream_name, ShardId=shard_id,
                ShardIteratorType="AT_TIMESTAMP",
                Timestamp=datetime.fromisoformat(at_timestamp),
            )
        else:
            resp = client.get_shard_iterator(
                StreamName=stream_name, ShardId=shard_id,
                ShardIteratorType="TRIM_HORIZON",
            )
        iterator = resp["ShardIterator"]

        for _ in range(10):
            if len(collected) >= limit:
                break
            fetch_resp = client.get_records(ShardIterator=iterator, Limit=min(100, limit - len(collected)))
            records = fetch_resp.get("Records", [])
            iterator = fetch_resp.get("NextShardIterator", "")
            for r in records:
                if len(collected) >= limit:
                    break
                try:
                    raw = base64.b64decode(r["Data"]).decode("utf-8", errors="replace")
                    if parsed := transformer.parse(raw):
                        collected.append(parsed)
                except Exception as e:
                    logger.warning(f"[debug] Skipping record: {e}")
            if not iterator or not records:
                break

    print(json.dumps(collected, ensure_ascii=False, indent=2))
    logger.info(f"[debug] Printed {len(collected)} records from stream: {stream_name}")
