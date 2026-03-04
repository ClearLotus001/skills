"""公共工具模块。

集中维护脚本间复用的方法，避免重复定义。
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
import sys
from datetime import datetime, timezone
from io import TextIOBase
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


# ---------------------------------------------------------------------------
# 日志重定向
# ---------------------------------------------------------------------------

class TeeLogger(TextIOBase):
    """将输出同时写到原始流和日志文件。"""

    def __init__(self, log_path: Path, original_stream: Any) -> None:
        super().__init__()
        self._original = original_stream
        self._log_file = log_path.open("a", encoding="utf-8", errors="replace")

    def write(self, msg: str) -> int:
        if msg:
            try:
                self._original.write(msg)
            except Exception:  # noqa: BLE001
                pass
            self._log_file.write(msg)
            self._log_file.flush()
        return len(msg) if msg else 0

    def flush(self) -> None:
        try:
            self._original.flush()
        except Exception:  # noqa: BLE001
            pass
        self._log_file.flush()

    def close(self) -> None:
        self._log_file.close()

    @property
    def encoding(self) -> str:  # type: ignore[override]
        return "utf-8"


def setup_file_logging(log_path: str | Path) -> None:
    """将 stdout/stderr 双写到日志文件，同时保留终端输出。"""
    path = Path(log_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    # 每次运行先清空旧日志
    path.write_text("", encoding="utf-8")
    sys.stdout = TeeLogger(path, sys.stdout)  # type: ignore[assignment]
    sys.stderr = TeeLogger(path, sys.stderr)  # type: ignore[assignment]


# ---------------------------------------------------------------------------
# 时间
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    """返回当前 UTC ISO 时间字符串。"""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 文件写入
# ---------------------------------------------------------------------------

def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """原子写入 JSON 文件（先写临时文件，再替换）。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.flush()
        temp_path = Path(tmp.name)
    temp_path.replace(path)


def atomic_write_text(path: Path, content: str) -> None:
    """原子写入文本文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=path.parent) as tmp:
        tmp.write(content)
        tmp.flush()
        temp_path = Path(tmp.name)
    temp_path.replace(path)


def file_sha256(path: Path) -> str:
    """计算文件 SHA-256 摘要。"""
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# 严重级别与分类
# ---------------------------------------------------------------------------

def severity_key(value: Any) -> str:
    """将严重级别标准化为内部 key。"""
    v = str(value or "").strip().lower()
    if v in {"error", "错误"}:
        return "error"
    if v in {"warn", "warning", "警告"}:
        return "warn"
    if v in {"info", "信息"}:
        return "info"
    return "info"


def severity_rank(level: str) -> int:
    """返回严重级别排序权重（越小越严重）。"""
    table = {"error": 0, "warn": 1, "info": 2}
    return table.get(severity_key(level), 9)


def severity_label_zh(level: Any) -> str:
    """将内部严重级别映射为中文标签。"""
    table = {"error": "错误", "warn": "警告", "info": "信息"}
    return table.get(severity_key(level), "信息")


def category_key(value: Any) -> str:
    """将分类标准化为内部 key。"""
    v = str(value or "").strip().lower()
    if v in {"local", "局部", "本地"}:
        return "local"
    if v in {"relation", "关联"}:
        return "relation"
    if v in {"global", "全局"}:
        return "global"
    return "local"


def category_label_zh(value: Any) -> str:
    """将内部分类映射为中文标签。"""
    table = {"local": "局部", "relation": "关联", "global": "全局"}
    return table.get(category_key(value), "局部")


# ---------------------------------------------------------------------------
# 数据集配置
# ---------------------------------------------------------------------------

def dataset_configs(rules: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """读取 rules 中的 datasets 映射（仅支持对象格式）。"""
    raw = rules.get("datasets", {})
    result: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                result[str(k)] = v
    return result


# ---------------------------------------------------------------------------
# 路径与文件匹配
# ---------------------------------------------------------------------------

def normalize_path_text(value: str) -> str:
    """路径标准化：转小写并统一为正斜杠。"""
    return str(value).replace("\\", "/").strip().lower()


def file_matches(file_item: dict[str, Any], expected_file: str, file_pattern: str) -> bool:
    """判断清单文件项是否匹配给定文件名或通配符。"""
    file_name = normalize_path_text(str(file_item.get("name", "")))
    file_path = normalize_path_text(str(file_item.get("path", "")))

    if expected_file:
        expected_norm = normalize_path_text(expected_file)
        return file_name == expected_norm or file_path.endswith(f"/{expected_norm}") or file_path == expected_norm

    if file_pattern:
        pattern = normalize_path_text(file_pattern)
        return fnmatch.fnmatch(file_name, pattern) or fnmatch.fnmatch(file_path, pattern)

    return True


# ---------------------------------------------------------------------------
# 稳定 issue_id
# ---------------------------------------------------------------------------

def stable_issue_id(rule_id: str, file_name: str, sheet: str, row: int, column: str, actual: str) -> str:
    """基于规则与定位信息生成稳定的 16 位 issue_id。"""
    raw = f"{rule_id}|{file_name}|{sheet}|{row}|{column}|{actual}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def stable_issue_id_simple(rule_id: str, detail: str) -> str:
    """简化版 issue_id（用于无具体行列定位的场景）。"""
    raw = f"{rule_id}|{detail}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


# ---------------------------------------------------------------------------
# 值处理
# ---------------------------------------------------------------------------

def value_text(value: Any) -> str:
    """将任意值转为可展示字符串。"""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def is_empty(value: Any) -> bool:
    """判断值是否为空（None 或仅空白字符串）。"""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


# ---------------------------------------------------------------------------
# 序列化
# ---------------------------------------------------------------------------

def json_friendly(value: Any) -> Any:
    """将 datetime/date/time 转为可 JSON 序列化的字符串。"""
    from datetime import date, datetime, time
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    return value
