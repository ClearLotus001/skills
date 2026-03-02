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


def _build_entry(file_item: dict[str, Any], sheet: dict[str, Any]) -> dict[str, Any]:
    """从 manifest 文件条目和工作表条目构建统一的 entry 字典。"""
    sheet_rows = sheet.get("rows", [])
    sheet_rows_file = str(sheet.get("rows_file", "")).strip()
    return {
        "file": str(file_item.get("name", "")),
        "path": str(file_item.get("path", "")),
        "sha256": str(file_item.get("sha256", "")),
        "sheet": str(sheet.get("sheet", "")),
        "headers": sheet.get("headers", []),
        "rows": sheet_rows if isinstance(sheet_rows, list) else [],
        "rows_file": sheet_rows_file,
        "row_count_estimate": int(sheet.get("row_count_estimate", 0) or 0),
    }


def _disambiguate_candidates(
    candidates: list[dict[str, Any]],
    ds_cfg: dict[str, Any],
    expected_sheet: str,
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    """从候选文件中收集所有匹配的 (file_item, sheet_item) 对。

    返回列表按行数降序排列，方便取最完整的文件。
    """
    expected_sha256 = str(ds_cfg.get("sha256", "")).strip().lower()
    expected_path = str(ds_cfg.get("file_path", "")).strip()
    expected_path_norm = normalize_path_text(expected_path) if expected_path else ""

    hits: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    for file_item in candidates:
        sheets = file_item.get("sheets", [])
        if not isinstance(sheets, list):
            continue
        target_sheets = sheets
        if expected_sheet:
            target_sheets = [s for s in sheets if str(s.get("sheet", "")) == expected_sheet]
        elif sheets:
            target_sheets = [sheets[0]]

        for sheet_item in target_sheets:
            row_est = int(sheet_item.get("row_count_estimate", 0) or 0)
            priority = 0
            if expected_sha256 and str(file_item.get("sha256", "")).lower() == expected_sha256:
                priority = 2
            elif expected_path_norm:
                file_path_norm = normalize_path_text(str(file_item.get("path", "")))
                if file_path_norm == expected_path_norm or file_path_norm.endswith(f"/{expected_path_norm}"):
                    priority = 1
            hits.append((file_item, sheet_item, priority * 10_000_000 + row_est))

    hits.sort(key=lambda x: -x[2])
    return [(fi, si) for fi, si, __ in hits]


def find_dataset_sheet(
    manifest: dict[str, Any],
    ds_cfg: dict[str, Any],
) -> tuple[dict[str, Any] | None, str]:
    """在 manifest 中查找与 dataset 配置匹配的文件/工作表。

    消歧策略（按优先级）:
      1. ds_cfg 中指定 sha256 → 精确匹配文件哈希
      2. ds_cfg 中指定 file_path → 精确匹配完整路径
      3. 同名多文件 → 选择行数最多的（最完整的版本）

    返回的 entry 中包含 sha256 和完整路径，供下游 issue / 报告引用。
    """
    expected_file = str(ds_cfg.get("file", "")).strip()
    file_pattern = str(ds_cfg.get("file_pattern", "")).strip()
    expected_sheet = str(ds_cfg.get("sheet", "")).strip()
    files = [x for x in manifest.get("files", []) if isinstance(x, dict)]
    candidates = [x for x in files if file_matches(x, expected_file, file_pattern)]

    if (expected_file or file_pattern) and not candidates:
        return None, "file_missing"
    if not candidates:
        candidates = files

    pairs = _disambiguate_candidates(candidates, ds_cfg, expected_sheet)
    if not pairs:
        return None, "sheet_missing"

    best_file, best_sheet = pairs[0]

    # 同名多文件时打印警告
    if len(pairs) > 1:
        chosen_path = str(best_file.get("path", ""))
        chosen_sha = str(best_file.get("sha256", ""))[:12]
        chosen_rows = int(best_sheet.get("row_count_estimate", 0) or 0)
        print(
            f"[同名文件消歧] 数据集匹配到 {len(pairs)} 个同名文件，"
            f"已选择行数最多的版本：{chosen_path} "
            f"(sha256={chosen_sha}…, {chosen_rows} 行)"
        )
        for fi, si in pairs[1:]:
            alt_path = str(fi.get("path", ""))
            alt_sha = str(fi.get("sha256", ""))[:12]
            alt_rows = int(si.get("row_count_estimate", 0) or 0)
            print(f"  跳过：{alt_path} (sha256={alt_sha}…, {alt_rows} 行)")

    return _build_entry(best_file, best_sheet), "ok"


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
    file_path: str = "",
    file_sha256: str = "",
) -> dict[str, Any]:
    cat = category.lower().strip() or "local"
    result: dict[str, Any] = {
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
    if file_path:
        result["file_path"] = file_path
    if file_sha256:
        result["file_sha256"] = file_sha256
    return result


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
