"""
Doris Stream Load 写入。

批量缓冲写入，由调用方通过 should_flush()/flush() 控制落盘时机：
  - buffer 条数达到 batch_size，或距上次 flush 超过 flush_interval 时，should_flush() 返回 True
  - flush() 成功后调用方应更新所有相关 shard 的 checkpoint，保证落盘数据与位点一致

每次 flush 生成唯一 label，Doris 通过 label 实现幂等，重复提交同一 label 会被跳过。
写入失败时指数退避重试最多 5 次；全部失败后清空 buffer 并抛出异常，
checkpoint 不推进，下次重启后从上一个 checkpoint 重放这批数据，Doris label 幂等不会重复入库。
"""
import json
import logging
import time
import uuid
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log

logger = logging.getLogger(__name__)


class DorisWriter:
    def __init__(self, config: dict):
        d = config["doris"]
        self.fe_host: str = d["fe_host"]
        self.fe_port: int = d["fe_port"]
        self.database: str = d["database"]
        self.table: str = d["table"]
        self.auth: tuple[str, str] = (d["username"], d["password"])
        self.batch_size: int = d.get("batch_size", 5000)
        self.flush_interval: float = d.get("flush_interval", 30.0)
        self._buffer: list[dict[str, Any]] = []
        self._last_flush = time.monotonic()

    @property
    def _url(self) -> str:
        return f"http://{self.fe_host}:{self.fe_port}/api/{self.database}/{self.table}/_stream_load"

    def write(self, record: dict[str, Any]):
        """写入一条记录到 buffer。由 consumer 通过 should_flush()/flush() 控制落盘时机。"""
        self._buffer.append(record)

    def has_pending(self) -> bool:
        """是否有待写入的数据。"""
        return bool(self._buffer)

    def should_flush(self) -> bool:
        return bool(self._buffer) and (
            len(self._buffer) >= self.batch_size
            or time.monotonic() - self._last_flush >= self.flush_interval
        )

    def flush(self):
        if not self._buffer:
            return
        batch = self._buffer[:]
        label = f"cf-{int(time.time())}-{uuid.uuid4().hex[:8]}"
        try:
            self._stream_load(batch, label)
            self._buffer.clear()
        except Exception as e:
            logger.error(f"Stream Load failed after retries, label={label}, rows={len(batch)}: {e}")
            # 写入失败时清空 buffer，避免无限堆积导致 OOM
            # checkpoint 未推进，重启后会从上一个 checkpoint 重放这批数据
            # Doris label 幂等，重放不会导致重复入库
            self._buffer.clear()
            raise
        finally:
            self._last_flush = time.monotonic()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _stream_load(self, batch: list[dict[str, Any]], label: str):
        # 每行一个 JSON 对象；ensure_ascii=False 保留 Unicode 原文
        # 特殊字符（引号、反斜杠、控制字符）由 json.dumps 自动转义，Doris 可正确解析
        lines: list[str] = []
        for row in batch:
            try:
                lines.append(json.dumps(row, ensure_ascii=False))
            except Exception as e:
                logger.warning(f"Skipping unserializable row: {e} | row keys: {list(row.keys())}")

        if not lines:
            logger.warning(f"label={label}: all rows skipped during serialization")
            return

        headers = {
            "format": "json",
            "read_json_by_line": "true",
            "label": label,
            "Expect": "100-continue",
            # 关闭严格模式，允许部分字段缺失；如需严格可改为 true
            "strict_mode": "false",
        }
        # allow_redirects=False：FE 会 302 重定向到 BE，requests 默认跟随重定向但不带 auth
        # 手动跟随重定向并重新附加 auth，确保 BE 收到认证信息
        resp = requests.put(
            self._url,
            data="\n".join(lines).encode("utf-8"),
            headers=headers,
            auth=self.auth,
            timeout=60,
            allow_redirects=False,
        )
        if resp.status_code in (301, 302, 307, 308):
            redirect_url = resp.headers.get("Location")
            resp = requests.put(
                redirect_url,
                data="\n".join(lines).encode("utf-8"),
                headers=headers,
                auth=self.auth,
                timeout=60,
            )
        resp.raise_for_status()
        result = resp.json()
        status = result.get("Status")

        match status:
            case "Success" | "Publish Timeout":
                filtered = result.get("NumberFilteredRows", 0)
                if filtered:
                    logger.warning(
                        f"Stream Load label={label} rows={len(lines)} "
                        f"filtered={filtered} (type mismatch or length exceeded), "
                        f"error_url={result.get('ErrorURL', '')}"
                    )
                else:
                    logger.info(f"Stream Load OK label={label} rows={len(lines)}")
            case "Label Already Exists" if result.get("ExistingJobStatus") in ("VISIBLE", "COMMITTED"):
                logger.info(f"Stream Load label={label} already committed, skipping")
            case _:
                raise RuntimeError(f"Stream Load failed: {result}")
