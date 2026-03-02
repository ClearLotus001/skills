"""公共工具模块 — 消除跨脚本重复定义。

所有脚本应从此模块导入以下公共函数，而非各自定义。
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any


# ---------------------------------------------------------------------------
# 时间
# ---------------------------------------------------------------------------

def utc_now_iso() -> str:
    """返回 UTC 时间的 ISO 格式字符串。"""
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# 文件 I/O
# ---------------------------------------------------------------------------

def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """原子写入 JSON 文件（先写临时文件再 replace）。"""
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
    """计算文件的 SHA-256 哈希。"""
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


# ---------------------------------------------------------------------------
# 严重级别 / 类别
# ---------------------------------------------------------------------------

def severity_key(value: Any) -> str:
    """将中英文严重级别统一为英文 key。"""
    v = str(value or "").strip().lower()
    if v in {"error", "错误"}:
        return "error"
    if v in {"warn", "warning", "警告"}:
        return "warn"
    if v in {"info", "信息"}:
        return "info"
    return "info"


def severity_rank(level: str) -> int:
    """严重级别排序权重（越小越严重）。"""
    table = {"error": 0, "warn": 1, "info": 2}
    return table.get(severity_key(level), 9)


def severity_label_zh(level: Any) -> str:
    """英文严重级别 → 中文标签。"""
    table = {"error": "错误", "warn": "警告", "info": "信息"}
    return table.get(severity_key(level), "信息")


def category_key(value: Any) -> str:
    """将中英文类别统一为英文 key。"""
    v = str(value or "").strip().lower()
    if v in {"local", "局部", "本地"}:
        return "local"
    if v in {"relation", "关联"}:
        return "relation"
    if v in {"global", "全局"}:
        return "global"
    return "local"


def category_label_zh(value: Any) -> str:
    """英文类别 → 中文标签。"""
    table = {"local": "局部", "relation": "关联", "global": "全局"}
    return table.get(category_key(value), "局部")


# ---------------------------------------------------------------------------
# 数据集配置解析（统一为一个函数名）
# ---------------------------------------------------------------------------

def dataset_configs(rules: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """从 rules 中提取 datasets 映射。支持 dict 和 list 两种格式。"""
    raw = rules.get("datasets", {})
    result: dict[str, dict[str, Any]] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            if isinstance(v, dict):
                result[str(k)] = v
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            ds_id = item.get("id")
            if ds_id:
                result[str(ds_id)] = item
    return result


# ---------------------------------------------------------------------------
# 路径与文件匹配
# ---------------------------------------------------------------------------

def normalize_path_text(value: str) -> str:
    """统一路径格式为小写正斜杠。"""
    return str(value).replace("\\", "/").strip().lower()


def file_matches(file_item: dict[str, Any], expected_file: str, file_pattern: str) -> bool:
    """判断 manifest 文件条目是否匹配给定的文件名或通配符。"""
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
# 稳定问题 ID
# ---------------------------------------------------------------------------

def stable_issue_id(rule_id: str, file_name: str, sheet: str, row: int, column: str, actual: str) -> str:
    """基于规则与位置生成确定性的 16 位十六进制 issue ID。"""
    raw = f"{rule_id}|{file_name}|{sheet}|{row}|{column}|{actual}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def stable_issue_id_simple(rule_id: str, detail: str) -> str:
    """简化版稳定 issue ID（用于不涉及具体位置的全局规则）。"""
    raw = f"{rule_id}|{detail}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


# ---------------------------------------------------------------------------
# 值处理
# ---------------------------------------------------------------------------

def value_text(value: Any) -> str:
    """将任意值转为可展示的字符串。"""
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def is_empty(value: Any) -> bool:
    """判断值是否为空（None 或纯空白字符串）。"""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False
