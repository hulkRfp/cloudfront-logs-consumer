"""
配置加载与日志初始化。

支持两种配置来源，环境变量优先级高于 YAML 文件：
  1. config.yaml（默认路径，可通过 --config 指定）
  2. 环境变量：格式 CF_<SECTION>__<KEY>，双下划线表示层级
     例：CF_DORIS__PASSWORD=secret  ->  cfg["doris"]["password"] = "secret"
         CF_KINESIS__STREAM_NAME=my-stream
         CF_REDIS__HOST=redis-host
"""
import json
import logging
import sys

import yaml

_ENV_PREFIX = "CF_"


# ──────────────────────────────────────────────
# JSON 结构化日志
# ──────────────────────────────────────────────

class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(level: int = logging.INFO):
    """初始化 JSON 格式日志，输出到 stdout。"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)


# ──────────────────────────────────────────────
# 配置加载
# ──────────────────────────────────────────────

def load(path: str) -> dict:
    """加载 YAML 配置文件，并用环境变量覆盖对应字段。"""
    with open(path) as f:
        cfg = yaml.safe_load(f)
    return _apply_env_overrides(cfg)


def _apply_env_overrides(cfg: dict) -> dict:
    import os
    logger = logging.getLogger(__name__)
    for env_key, env_val in os.environ.items():
        if not env_key.startswith(_ENV_PREFIX):
            continue
        name = env_key[len(_ENV_PREFIX):]
        if "__" not in name:
            continue
        section, key = name.split("__", 1)
        section = section.lower()
        key = key.lower()
        if section not in cfg or not isinstance(cfg[section], dict):
            continue
        original = cfg[section].get(key)
        cfg[section][key] = _cast(env_val, original)
        logger.debug(f"Config override: {env_key} -> cfg[{section}][{key}]")
    return cfg


def _cast(value: str, original) -> object:
    """按原始值类型转换环境变量字符串，原始值为 None 时保持字符串。"""
    if original is None:
        return value
    t = type(original)
    if t is bool:
        return value.lower() in ("1", "true", "yes")
    try:
        return t(value)
    except (ValueError, TypeError):
        return value
