"""
字段处理：解析 CloudFront tab 分隔日志，执行重命名、删除、新增、扩展字段合并。
特殊字符（SQL 注入、XSS、Unicode 等）均安全保留，由 Stream Load JSON 格式传输。
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote

logger = logging.getLogger(__name__)

# CloudFront 实时日志中 "-" 表示空值
_EMPTY = "-"

# URL 解码字段默认值
_DEFAULT_URL_DECODE_FIELDS = {"uri_query", "referer", "user_agent"}


class Transformer:
    def __init__(self, config: dict):
        t = config["transform"]
        self.source_fields: list[str] = t["source_fields"] or []
        self.target_fields: list[str] = t["target_fields"] or []
        self.rename: dict[str, str] = t.get("rename") or {}
        self.drop_fields: list[str] = t.get("drop_fields") or []
        self.add_fields: dict[str, str] = t.get("add_fields") or {}
        self.url_decode_fields: list[str] = t.get("url_decode_fields") or list(_DEFAULT_URL_DECODE_FIELDS)
        self._explicit: set[str] = {f for f in self.target_fields if f != "extras"}
        # 类型转换字段集合提升为实例变量，避免每条记录重建 set
        # 数值类型转换字段，从配置读取，避免写死
        self._int_fields: frozenset[str] = frozenset(t.get("int_fields") or [])
        self._float_fields: frozenset[str] = frozenset(t.get("float_fields") or [])
        self._strip_query_fields: frozenset[str] = frozenset(t.get("strip_query_fields") or [])
        # Cookie 字段提取配置
        cookie_cfg = t.get("cookie_fields") or {}
        self._cookie_source_field: str = cookie_cfg.get("source_field", "")
        self._cookie_mappings: dict[str, str] = cookie_cfg.get("mappings") or {}
        self._cookie_url_decode: bool = cookie_cfg.get("url_decode", False)
        self._cookie_default: Any = cookie_cfg.get("default_value", None)

    def parse(self, raw_line: str) -> dict[str, Any] | None:
        """
        解析一行 tab 分隔的 CloudFront 日志，返回处理后的字典。
        任何解析异常都会向上抛出，由调用方决定是否跳过。
        """
        line = raw_line.strip()
        if not line or line.startswith("#"):
            return None

        parts = line.split("\t")

        # 1. 按 source_fields 顺序映射原始值
        record: dict[str, Any] = {
            field: (None if (val := parts[i] if i < len(parts) else None) is None or val == _EMPTY else val)
            for i, field in enumerate(self.source_fields)
        }

        # 2. 重命名
        renamed: dict[str, Any] = {self.rename.get(k, k): v for k, v in record.items()}

        # 3. 删除
        for field in self.drop_fields:
            renamed.pop(field, None)

        # 4. 新增字段
        for field, expr in self.add_fields.items():
            renamed[field] = self._resolve_expr(expr)

        # 5. URL 解码（保留原始特殊字符，不做任何转义或过滤）
        for field in self.url_decode_fields:
            if renamed.get(field):
                renamed[field] = _url_decode_safe(renamed[field])

        # 5b. 去除查询参数（截取 ? 之前的纯路径）
        for field in self._strip_query_fields:
            if (v := renamed.get(field)):
                renamed[field] = v.split("?", 1)[0]

        # 5c. Cookie 字段提取：从指定字段解析 cookie 字符串，提取指定 key 的值为独立字段
        if self._cookie_source_field and self._cookie_mappings:
            cookie_raw = renamed.get(self._cookie_source_field)
            cookie_dict = _parse_cookie(cookie_raw) if cookie_raw else {}
            for cookie_key, output_field in self._cookie_mappings.items():
                value = cookie_dict.get(cookie_key, self._cookie_default)
                if value is not None and self._cookie_url_decode:
                    value = _url_decode_safe(value)
                renamed[output_field] = value

        # 6. 类型转换
        renamed = self._convert_types(renamed)

        # 7. 多余字段归入 extras（JSON 字符串）
        final: dict[str, Any] = {}
        extras: dict[str, Any] = {}
        for k, v in renamed.items():
            (final if k in self._explicit else extras)[k] = v

        if "extras" in self.target_fields:
            # ensure_ascii=False 保留 Unicode；特殊字符由 JSON 转义保证安全
            final["extras"] = json.dumps(extras, ensure_ascii=False) if extras else None

        return final

    def _resolve_expr(self, expr: str) -> Any:
        if expr.startswith("literal:"):
            return expr[len("literal:"):]
        if expr.startswith("env:"):
            return os.environ.get(expr[len("env:"):])
        return expr

    def _convert_types(self, record: dict[str, Any]) -> dict[str, Any]:
        """类型转换：时间格式、数值字段"""
        if log_time := record.get("log_time"):
            try:
                # CloudFront 实时日志 timestamp 为 unix 浮点秒（如 1773800098.640）
                ts = float(log_time)
                record["log_time"] = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                # 兼容 ISO 格式（如 2026-03-18T08:00:00Z）
                record["log_time"] = str(log_time).replace("T", " ").replace("Z", "")

        for f in self._int_fields:
            if (v := record.get(f)) is not None:
                try:
                    record[f] = int(v)
                except (ValueError, TypeError):
                    record[f] = None

        for f in self._float_fields:
            if (v := record.get(f)) is not None:
                try:
                    record[f] = float(v)
                except (ValueError, TypeError):
                    record[f] = None

        return record


def _url_decode_safe(value: str) -> str:
    """
    对 URL 编码的字段做解码，支持多层编码（CloudFront 实时日志会对
    cs-uri-query 等字段做额外一层 URL 编码，导致需要多次解码）。
    最多解码 5 次，防止异常数据导致无限循环。解码失败时返回原始值。
    """
    try:
        result = value
        for _ in range(5):
            decoded = unquote(result, encoding="utf-8", errors="replace")
            if decoded == result:
                break
            result = decoded
        return result
    except Exception:
        return value


def _parse_cookie(cookie_str: str) -> dict[str, str]:
    """
    解析 HTTP Cookie 字符串为字典。
    支持标准格式：key1=value1; key2=value2
    也兼容无空格分隔：key1=value1;key2=value2
    值中可能包含 '='（如 Base64），仅按第一个 '=' 分割。
    """
    result: dict[str, str] = {}
    if not cookie_str or cookie_str == _EMPTY:
        return result
    for pair in cookie_str.split(";"):
        pair = pair.strip()
        if not pair:
            continue
        if "=" in pair:
            key, value = pair.split("=", 1)
            result[key.strip()] = value.strip()
        else:
            # 无值的 cookie（如 flag 类型），值设为空字符串
            result[pair.strip()] = ""
    return result

