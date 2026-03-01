"""校验公共工具 — 提供 issue 构造、数据集查找、行数据读取等共用函数。

被 validate_local.py、validate_relations.py、validate_global.py、local_rule_engine.py 引用。
核心功能: make_issue、find_dataset_sheet、iter_rows_from_entry、canonical_key
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from common import (
    atomic_write_json,
    category_label_zh,
    dataset_configs,
    file_matches,
    is_empty,
    normalize_path_text,
    severity_label_zh,
    severity_rank,
    stable_issue_id,
    utc_now_iso,
    value_text,
)

# Re-export everything for backward compatibility
__all__ = [
    "atomic_write_json",
    "canonical_key",
    "category_label_zh",
    "dataset_configs",
    "file_matches",
    "find_dataset_sheet",
    "has_min_digits",
    "is_empty",
    "make_issue",
    "make_exception_issue",
    "normalize_path_text",
    "parse_int_like",
    "parse_number",
    "rows_from_entry",
    "iter_rows_from_entry",
    "severity_label_zh",
    "severity_rank",
    "stable_issue_id",
    "utc_now_iso",
    "value_text",
]


def parse_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
            try:
                return float(text)
            except ValueError:
                return None
    return None


def parse_int_like(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"[+-]?\d+", text):
            try:
                return int(text)
            except ValueError:
                return None
        if re.fullmatch(r"[+-]?\d+\.0+", text):
            try:
                return int(float(text))
            except ValueError:
                return None
    return None


def has_min_digits(value: Any, min_digits: int) -> bool:
    if min_digits <= 0:
        return True
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"[+-]?\d+", text):
            return len(text.lstrip("+-")) >= min_digits
        if re.fullmatch(r"[+-]?\d+\.0+", text):
            integer_part = text.lstrip("+-").split(".", 1)[0]
            return len(integer_part) >= min_digits
        return False
    int_value = parse_int_like(value)
    if int_value is None:
        return False
    return len(str(abs(int_value))) >= min_digits


def canonical_key(value: Any) -> str:
    if is_empty(value):
        return ""
    if isinstance(value, bool):
        return ""
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else str(value)

    text = value_text(value).strip()
    if not text:
        return ""
    if re.fullmatch(r"[+-]?\d+", text):
        try:
            return str(int(text))
        except ValueError:
            return text
    if re.fullmatch(r"[+-]?\d+\.0+", text):
        try:
            return str(int(float(text)))
        except ValueError:
            return text
    return text


def find_dataset_sheet(manifest: dict[str, Any], ds_cfg: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
    expected_file = str(ds_cfg.get("file", "")).strip()
    file_pattern = str(ds_cfg.get("file_pattern", "")).strip()
    expected_sheet = str(ds_cfg.get("sheet", "")).strip()
    files = [x for x in manifest.get("files", []) if isinstance(x, dict)]
    candidates = [x for x in files if file_matches(x, expected_file, file_pattern)]

    if (expected_file or file_pattern) and not candidates:
        return None, "file_missing"
    if not candidates:
        candidates = files

    if expected_sheet:
        for file_item in candidates:
            for sheet in file_item.get("sheets", []):
                sheet_name = str(sheet.get("sheet", ""))
                if sheet_name == expected_sheet:
                    sheet_rows = sheet.get("rows", [])
                    sheet_rows_file = str(sheet.get("rows_file", "")).strip()
                    return (
                        {
                            "file": str(file_item.get("name", "")),
                            "path": str(file_item.get("path", "")),
                            "sheet": sheet_name,
                            "headers": sheet.get("headers", []),
                            "rows": sheet_rows if isinstance(sheet_rows, list) else [],
                            "rows_file": sheet_rows_file,
                        },
                        "ok",
                    )
        return None, "sheet_missing"

    for file_item in candidates:
        sheets = file_item.get("sheets", [])
        if sheets:
            first_sheet = sheets[0]
            first_rows = first_sheet.get("rows", [])
            first_rows_file = str(first_sheet.get("rows_file", "")).strip()
            return (
                {
                    "file": str(file_item.get("name", "")),
                    "path": str(file_item.get("path", "")),
                    "sheet": str(first_sheet.get("sheet", "")),
                    "headers": first_sheet.get("headers", []),
                    "rows": first_rows if isinstance(first_rows, list) else [],
                    "rows_file": first_rows_file,
                },
                "ok",
            )
    return None, "sheet_missing"


def rows_from_entry(entry: dict[str, Any]) -> list[dict[str, Any]]:
    rows = entry.get("rows", [])
    if isinstance(rows, list) and rows:
        return rows

    rows_file = str(entry.get("rows_file", "")).strip()
    if not rows_file:
        return rows if isinstance(rows, list) else []

    path = Path(rows_file)
    if not path.exists():
        return []

    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def make_issue(
    *,
    category: str,
    rule_id: str,
    severity: str,
    message: str,
    file_name: str,
    sheet: str,
    row: int,
    column: str,
    expected: str,
    actual: str,
) -> dict[str, Any]:
    cat = category.lower().strip() or "local"
    return {
        "issue_id": stable_issue_id(rule_id, file_name, sheet, row, column, actual),
        "severity": severity,
        "severity_zh": severity_label_zh(severity),
        "category": cat,
        "category_zh": category_label_zh(cat),
        "rule_id": rule_id,
        "message": message,
        "file": file_name,
        "sheet": sheet,
        "row": row,
        "column": column,
        "cell": "",
        "expected": expected,
        "actual": actual,
    }


def make_exception_issue(
    *,
    category: str,
    rule_id: str,
    exc: Exception,
    file_name: str = "",
    sheet: str = "",
    context: str = "",
) -> dict[str, Any]:
    """将 Python 异常转为标准 issue，用于异常不中断流程。"""
    import traceback

    exc_type = type(exc).__name__
    exc_msg = str(exc)
    tb_short = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__, limit=3))
    message = f"规则 '{rule_id}' 执行异常（{exc_type}）：{exc_msg}"
    if context:
        message = f"[{context}] {message}"
    return make_issue(
        category=category,
        rule_id=f"EXCEPTION_{rule_id}",
        severity="error",
        message=message,
        file_name=file_name,
        sheet=sheet,
        row=0,
        column="",
        expected="规则正常执行",
        actual=f"{exc_type}: {exc_msg}\n{tb_short[:500]}",
    )


def iter_rows_from_entry(
    entry: dict[str, Any],
    chunk_size: int = 2000,
) -> Any:
    """按 chunk 逐块从 JSONL 文件读取行数据的生成器。

    每次 yield 一个 list[dict]，长度最多 chunk_size。
    如果 entry 中内嵌了 rows 列表，则直接按 chunk_size 分块 yield。
    """
    rows = entry.get("rows", [])
    if isinstance(rows, list) and rows:
        for i in range(0, len(rows), chunk_size):
            yield rows[i : i + chunk_size]
        return

    rows_file = str(entry.get("rows_file", "")).strip()
    if not rows_file:
        if isinstance(rows, list):
            yield rows
        return

    path = Path(rows_file)
    if not path.exists():
        return

    buffer: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                buffer.append(obj)
                if len(buffer) >= chunk_size:
                    yield buffer
                    buffer = []
    if buffer:
        yield buffer
