"""值级校验引擎 — 逐行执行 schema/range/row 规则检查。

被 validate_local.py 调用。
支持检查类型: required、string、numeric、min_digits、date、datetime_format、
max_length、regex、increasing、enum、unique、min_length、positive、
non_negative、conditional_required
"""
from __future__ import annotations

import re
from datetime import date, datetime, timezone
from typing import Any

from common import is_empty, value_text
from validation_common import (
    has_min_digits,
    iter_rows_from_entry,
    make_exception_issue,
    make_issue,
    parse_int_like,
    parse_number,
)


def to_python_datetime_format(user_format: str) -> str | None:
    normalized = user_format.strip()
    alias = {
        "YYYY-MM-DD HH:MM:SS": "%Y-%m-%d %H:%M:%S",
        "YYYY-MM-DD HH:mm:ss": "%Y-%m-%d %H:%M:%S",
        "yyyy-MM-dd HH:mm:ss": "%Y-%m-%d %H:%M:%S",
        "yyyy-MM-dd HH:MM:SS": "%Y-%m-%d %H:%M:%S",
        "YYYY-MM-DD": "%Y-%m-%d",
    }
    if normalized in alias:
        return alias[normalized]
    if "%" in normalized:
        return normalized
    return None


def matches_datetime_format(value: Any, user_format: str) -> bool:
    py_format = to_python_datetime_format(user_format)
    if py_format is None:
        return False
    text = value_text(value).strip()
    if not text:
        return False
    try:
        parsed = datetime.strptime(text, py_format)
    except ValueError:
        return False
    return parsed.strftime(py_format) == text


def is_valid_date_value(value: Any) -> bool:
    if isinstance(value, date) and not isinstance(value, datetime):
        return True
    if isinstance(value, datetime):
        return True
    if isinstance(value, (int, float, bool)):
        return False

    text = value_text(value).strip()
    if not text:
        return False

    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            datetime.strptime(text, fmt)
            return True
        except ValueError:
            continue

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.hour == 0 and parsed.minute == 0 and parsed.second == 0:
            return True

    try:
        parsed_iso = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed_iso.hour == 0 and parsed_iso.minute == 0 and parsed_iso.second == 0


def parse_datetime_value(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    if isinstance(value, (int, float, bool)):
        return None

    text = value_text(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_range_value(value: Any, value_type: str) -> Any | None:
    if value_type == "number":
        return parse_number(value)
    if value_type == "date":
        dt = parse_datetime_value(value)
        if dt is None:
            return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    return parse_number(value)


def range_type_from_rule(rule: dict[str, Any], sample_value: Any) -> str:
    configured = str(rule.get("value_type") or rule.get("type") or "").strip().lower()
    if configured in {"number", "numeric"}:
        return "number"
    if configured in {"date", "datetime"}:
        return "date"

    if parse_number(sample_value) is not None:
        return "number"
    if parse_datetime_value(sample_value) is not None:
        return "date"
    return "number"


MAX_EXPRESSION_LENGTH = 500


def _safe_str(v: Any) -> str:
    """安全的 str() 包装：限制输出长度以防 DoS。"""
    s = str(v)
    return s[:10000] if len(s) > 10000 else s


def _safe_int(v: Any) -> int:
    """安全的 int() 包装：限制输入范围。"""
    if isinstance(v, str) and len(v) > 50:
        raise ValueError("整数字符串过长")
    return int(v)


def _safe_float(v: Any) -> float:
    """安全的 float() 包装：限制输入范围。"""
    if isinstance(v, str) and len(v) > 50:
        raise ValueError("浮点数字符串过长")
    return float(v)


def safe_eval_row_expression(expr: str, row_values: dict[str, Any]) -> tuple[Any | None, str | None]:
    if len(expr) > MAX_EXPRESSION_LENGTH:
        return None, f"表达式长度 {len(expr)} 超过限制 {MAX_EXPRESSION_LENGTH}"

    def value(column: str, default: Any = None) -> Any:
        return row_values.get(column, default)

    def text(column: str, default: str = "") -> str:
        v = row_values.get(column, default)
        return value_text(v).strip()

    def num(column: str) -> float | None:
        return parse_number(row_values.get(column))

    def intv(column: str) -> int | None:
        return parse_int_like(row_values.get(column))

    def empty(column: str) -> bool:
        return is_empty(row_values.get(column))

    def exists(column: str) -> bool:
        return not empty(column)

    def match(pattern: str, data: Any) -> bool:
        """正则全匹配。data 直接作为待匹配文本（不再隐式按列名查找）。"""
        try:
            return re.fullmatch(pattern, value_text(data)) is not None
        except re.error:
            return False

    env = {
        "row": row_values,
        "value": value,
        "text": text,
        "num": num,
        "intv": intv,
        "empty": empty,
        "exists": exists,
        "match": match,
        "len": len,
        "min": min,
        "max": max,
        "abs": abs,
        "round": round,
        "str": _safe_str,
        "int": _safe_int,
        "float": _safe_float,
        "bool": bool,
        "True": True,
        "False": False,
        "None": None,
    }
    try:
        result = eval(expr, {"__builtins__": {}}, env)  # noqa: S307
    except Exception as exc:  # noqa: BLE001
        return None, str(exc)
    return result, None


def normalize_checks(rule: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    raw_checks = rule.get("checks")
    if isinstance(raw_checks, dict):
        raw_checks = [raw_checks]
    if isinstance(raw_checks, list):
        for item in raw_checks:
            if isinstance(item, str):
                checks.append({"type": item})
            elif isinstance(item, dict):
                check_type = str(item.get("type") or item.get("check") or "").strip()
                if check_type:
                    x = dict(item)
                    x["type"] = check_type
                    checks.append(x)

    single_check = str(rule.get("check", "")).strip()
    if single_check:
        meta_keys = {"rule_id", "dataset", "column", "severity", "check", "checks", "message"}
        params = {k: v for k, v in rule.items() if k not in meta_keys}
        checks.append({"type": single_check, **params})
    return checks


def append_value_check_issue(
    *,
    issues: list[dict[str, Any]],
    rule_id: str,
    severity: str,
    message: str,
    file_name: str,
    sheet: str,
    row_num: int,
    column: str,
    expected: str,
    actual: str,
) -> None:
    issues.append(
        make_issue(
            category="local",
            rule_id=rule_id,
            severity=severity,
            message=message,
            file_name=file_name,
            sheet=sheet,
            row=row_num,
            column=column,
            expected=expected,
            actual=actual,
        )
    )


def validate_rule_on_rows(
    *,
    issues: list[dict[str, Any]],
    file_name: str,
    sheet: str,
    column: str,
    rows: list[dict[str, Any]],
    check: dict[str, Any],
    default_rule_id: str,
    default_severity: str,
    chunk_state: dict[str, Any] | None = None,
) -> None:
    check_type = str(check.get("type", "")).strip().lower()
    if not check_type:
        return

    severity = str(check.get("severity", default_severity))
    current_rule_id = str(check.get("rule_id", default_rule_id))

    if check_type in {"increasing", "ascending", "strict_increasing"}:
        previous_value: int | None = chunk_state.get("previous_value") if chunk_state else None
        previous_row: int = chunk_state.get("previous_row", 0) if chunk_state else 0
        for row_item in rows:
            if not isinstance(row_item, dict):
                continue
            row_num = int(row_item.get("row", 0) or 0)
            values = row_item.get("values", {})
            if not isinstance(values, dict):
                continue
            value = values.get(column)
            if is_empty(value):
                continue
            int_value = parse_int_like(value)
            if int_value is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为可比较的整数，才能执行递增校验",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="整数",
                    actual=value_text(value) or "空值",
                )
                continue
            if previous_value is not None and int_value <= previous_value:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 需要按行严格递增（上一行值={previous_value}，当前值={int_value}）",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"> 上一有效行({previous_row}) 的值 {previous_value}",
                    actual=str(int_value),
                )
            previous_value = int_value
            previous_row = row_num
        if chunk_state is not None:
            chunk_state["previous_value"] = previous_value
            chunk_state["previous_row"] = previous_row
        return

    if check_type == "unique":
        seen: dict[str, int] = chunk_state.get("seen", {}) if chunk_state else {}
        for row_item in rows:
            if not isinstance(row_item, dict):
                continue
            row_num = int(row_item.get("row", 0) or 0)
            values = row_item.get("values", {})
            if not isinstance(values, dict):
                continue
            value = values.get(column)
            if is_empty(value):
                continue
            key = value_text(value).strip()
            if key in seen:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 值重复（首次出现在行 {seen[key]}）",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="唯一值",
                    actual=f"'{key}' 与行 {seen[key]} 重复",
                )
            else:
                seen[key] = row_num
        if chunk_state is not None:
            chunk_state["seen"] = seen
        return

    for row_item in rows:
        if not isinstance(row_item, dict):
            continue
        row_num = int(row_item.get("row", 0) or 0)
        values = row_item.get("values", {})
        if not isinstance(values, dict):
            continue
        value = values.get(column)

        if check_type == "required":
            if is_empty(value):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 不能为空",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="非空值",
                    actual="空值",
                )
            continue

        if is_empty(value):
            continue

        if check_type == "string":
            if not isinstance(value, str):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为字符串",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="字符串",
                    actual=value_text(value),
                )
            continue

        if check_type == "numeric":
            if parse_number(value) is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为数字",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="数字",
                    actual=value_text(value),
                )
            continue

        if check_type == "min_digits":
            min_digits = int(check.get("min_digits", 0) or 0)
            if not has_min_digits(value, min_digits):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 需要至少 {min_digits} 位数字",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f">= {min_digits} 位数字",
                    actual=value_text(value),
                )
            continue

        if check_type == "date":
            if not is_valid_date_value(value):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为日期格式",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="日期（如 YYYY-MM-DD）",
                    actual=value_text(value),
                )
            continue

        if check_type == "datetime_format":
            fmt = str(check.get("format", "YYYY-MM-DD HH:MM:SS"))
            if not matches_datetime_format(value, fmt):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须符合时间格式 '{fmt}'",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=fmt,
                    actual=value_text(value),
                )
            continue

        if check_type == "max_length":
            max_length = int(check.get("max_length", 0) or 0)
            text_value = value_text(value)
            if max_length > 0 and len(text_value) > max_length:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 长度不能超过 {max_length}",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"长度 <= {max_length}",
                    actual=f"长度 {len(text_value)}",
                )
            continue

        if check_type == "regex":
            pattern = str(check.get("pattern", ""))
            if not pattern:
                continue
            try:
                ok = re.fullmatch(pattern, value_text(value)) is not None
            except re.error:
                ok = False
            if not ok:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 不匹配正则 '{pattern}'",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"匹配正则 {pattern}",
                    actual=value_text(value),
                )
            continue

        if check_type in {"enum", "whitelist"}:
            allowed = check.get("values", [])
            if not isinstance(allowed, list):
                continue
            case_insensitive = bool(check.get("case_insensitive", False))
            text_val = value_text(value).strip()
            if case_insensitive:
                ok = text_val.lower() in {str(v).strip().lower() for v in allowed}
            else:
                ok = text_val in {str(v).strip() for v in allowed}
            if not ok:
                preview = ", ".join(str(v) for v in allowed[:10])
                if len(allowed) > 10:
                    preview += f" ... 共 {len(allowed)} 个"
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 的值不在允许列表中",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"允许值: [{preview}]",
                    actual=text_val,
                )
            continue

        if check_type == "min_length":
            min_len = int(check.get("min_length", 0) or 0)
            text_val = value_text(value)
            if min_len > 0 and len(text_val) < min_len:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 长度不能低于 {min_len}",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"长度 >= {min_len}",
                    actual=f"长度 {len(text_val)}",
                )
            continue

        if check_type == "positive":
            num_val = parse_number(value)
            if num_val is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为正数，但值不是数字",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="> 0",
                    actual=value_text(value),
                )
            elif num_val <= 0:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为正数",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected="> 0",
                    actual=value_text(value),
                )
            continue

        if check_type == "non_negative":
            num_val = parse_number(value)
            if num_val is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为非负数，但值不是数字",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=">= 0",
                    actual=value_text(value),
                )
            elif num_val < 0:
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 必须为非负数",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=">= 0",
                    actual=value_text(value),
                )
            continue

        if check_type == "conditional_required":
            when_expr = str(check.get("when", "")).strip()
            if not when_expr:
                continue
            when_result, when_error = safe_eval_row_expression(when_expr, values)
            if when_error:
                continue
            if bool(when_result) and is_empty(value):
                append_value_check_issue(
                    issues=issues,
                    rule_id=current_rule_id,
                    severity=severity,
                    message=f"字段 '{column}' 在满足条件 '{when_expr}' 时不能为空",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=row_num,
                    column=column,
                    expected=f"当 {when_expr} 时非空",
                    actual="空值",
                )
            continue


def validate_range_rules(
    *,
    rules: dict[str, Any],
    dataset_sheet_lookup: dict[str, dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    range_rules = rules.get("range_rules", [])
    if not isinstance(range_rules, list):
        return

    for idx, rule in enumerate(range_rules):
        if not isinstance(rule, dict):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        column = str(rule.get("column", "")).strip()
        rule_id = str(rule.get("rule_id", f"RANGE_RULE_{idx}"))
        severity = str(rule.get("severity", "error"))
        allow_empty = bool(rule.get("allow_empty", True))
        include_min = bool(rule.get("include_min", rule.get("min_inclusive", True)))
        include_max = bool(rule.get("include_max", rule.get("max_inclusive", True)))
        min_raw = rule.get("min")
        max_raw = rule.get("max")
        if not dataset or not column:
            continue

        try:
            entry = dataset_sheet_lookup.get(dataset)
            if not isinstance(entry, dict):
                continue

            headers = [str(h) for h in entry.get("headers", [])]
            file_name = str(entry.get("file", ""))
            sheet = str(entry.get("sheet", ""))
            if column not in headers:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"范围校验依赖列 '{column}'，但工作表 '{sheet}' 中不存在该列",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=1,
                    column=column,
                    expected=f"列 '{column}' 存在",
                    actual="列缺失",
                )
                continue

            # 从首个 chunk 获取 sample_value 用于类型推断
            sample_value = None
            first_chunk = None
            for chunk in iter_rows_from_entry(entry):
                first_chunk = chunk
                for row_item in chunk:
                    if not isinstance(row_item, dict):
                        continue
                    values = row_item.get("values", {})
                    if not isinstance(values, dict):
                        continue
                    x = values.get(column)
                    if not is_empty(x):
                        sample_value = x
                        break
                break
            value_type = range_type_from_rule(rule, sample_value)

            min_parsed = parse_range_value(min_raw, value_type) if min_raw is not None else None
            max_parsed = parse_range_value(max_raw, value_type) if max_raw is not None else None
            if min_raw is not None and min_parsed is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"范围规则最小值 min 无法按类型 '{value_type}' 解析",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=0,
                    column=column,
                    expected=f"可解析的 {value_type} 最小值",
                    actual=value_text(min_raw),
                )
                continue
            if max_raw is not None and max_parsed is None:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"范围规则最大值 max 无法按类型 '{value_type}' 解析",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=0,
                    column=column,
                    expected=f"可解析的 {value_type} 最大值",
                    actual=value_text(max_raw),
                )
                continue

            # chunk 流式遍历行数据
            def _check_range_rows(rows: list[dict[str, Any]]) -> None:
                for row_item in rows:
                    if not isinstance(row_item, dict):
                        continue
                    row_num = int(row_item.get("row", 0) or 0)
                    values = row_item.get("values", {})
                    if not isinstance(values, dict):
                        continue
                    raw_value = values.get(column)

                    if is_empty(raw_value):
                        if not allow_empty:
                            append_value_check_issue(
                                issues=issues,
                                rule_id=rule_id,
                                severity=severity,
                                message=f"字段 '{column}' 不能为空（范围规则）",
                                file_name=file_name,
                                sheet=sheet,
                                row_num=row_num,
                                column=column,
                                expected="非空值",
                                actual="空值",
                            )
                        continue

                    parsed = parse_range_value(raw_value, value_type)
                    if parsed is None:
                        expected_type = "数字" if value_type == "number" else "日期/时间"
                        append_value_check_issue(
                            issues=issues,
                            rule_id=rule_id,
                            severity=severity,
                            message=f"字段 '{column}' 无法按范围规则类型 '{value_type}' 解析",
                            file_name=file_name,
                            sheet=sheet,
                            row_num=row_num,
                            column=column,
                            expected=expected_type,
                            actual=value_text(raw_value),
                        )
                        continue

                    if min_parsed is not None:
                        too_low = parsed < min_parsed if include_min else parsed <= min_parsed
                        if too_low:
                            comp = ">=" if include_min else ">"
                            append_value_check_issue(
                                issues=issues,
                                rule_id=rule_id,
                                severity=severity,
                                message=f"字段 '{column}' 超出最小范围约束",
                                file_name=file_name,
                                sheet=sheet,
                                row_num=row_num,
                                column=column,
                                expected=f"{comp} {value_text(min_raw)}",
                                actual=value_text(raw_value),
                            )
                            continue

                    if max_parsed is not None:
                        too_high = parsed > max_parsed if include_max else parsed >= max_parsed
                        if too_high:
                            comp = "<=" if include_max else "<"
                            append_value_check_issue(
                                issues=issues,
                                rule_id=rule_id,
                                severity=severity,
                                message=f"字段 '{column}' 超出最大范围约束",
                                file_name=file_name,
                                sheet=sheet,
                                row_num=row_num,
                                column=column,
                                expected=f"{comp} {value_text(max_raw)}",
                                actual=value_text(raw_value),
                            )

            # 处理首个 chunk（已读取用于 sample）然后继续后续 chunks
            if first_chunk:
                _check_range_rows(first_chunk)
            for chunk in iter_rows_from_entry(entry):
                if chunk is first_chunk:
                    continue
                _check_range_rows(chunk)
        except Exception as exc:  # noqa: BLE001
            issues.append(
                make_exception_issue(
                    category="local",
                    rule_id=rule_id,
                    exc=exc,
                    file_name=str(dataset_sheet_lookup.get(dataset, {}).get("file", "")),
                    sheet=str(dataset_sheet_lookup.get(dataset, {}).get("sheet", "")),
                    context="range_rules 执行",
                )
            )


def validate_row_rules(
    *,
    rules: dict[str, Any],
    dataset_sheet_lookup: dict[str, dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    row_rules = rules.get("row_rules", [])
    if not isinstance(row_rules, list):
        return

    for idx, rule in enumerate(row_rules):
        if not isinstance(rule, dict):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        rule_id = str(rule.get("rule_id", f"ROW_RULE_{idx}"))
        severity = str(rule.get("severity", "error"))
        when_expr = str(rule.get("when", "")).strip()
        assert_expr = str(rule.get("expression") or rule.get("assert") or "").strip()
        message = str(rule.get("message", "")).strip() or f"行规则表达式未满足: {assert_expr}"
        if not dataset or not assert_expr:
            continue

        try:
            entry = dataset_sheet_lookup.get(dataset)
            if not isinstance(entry, dict):
                continue

            file_name = str(entry.get("file", ""))
            sheet = str(entry.get("sheet", ""))

            expression_error_reported = False
            rule_aborted = False
            for chunk in iter_rows_from_entry(entry):
                if rule_aborted:
                    break
                for row_item in chunk:
                    if not isinstance(row_item, dict):
                        continue
                    row_num = int(row_item.get("row", 0) or 0)
                    values = row_item.get("values", {})
                    if not isinstance(values, dict):
                        continue

                    if when_expr:
                        when_result, when_error = safe_eval_row_expression(when_expr, values)
                        if when_error:
                            if not expression_error_reported:
                                append_value_check_issue(
                                    issues=issues,
                                    rule_id=rule_id,
                                    severity=severity,
                                    message="row_rules 的 when 表达式执行失败",
                                    file_name=file_name,
                                    sheet=sheet,
                                    row_num=0,
                                    column="",
                                    expected="合法表达式",
                                    actual=when_error,
                                )
                                expression_error_reported = True
                            rule_aborted = True
                            break
                        if not bool(when_result):
                            continue

                    result, err = safe_eval_row_expression(assert_expr, values)
                    if err:
                        if not expression_error_reported:
                            append_value_check_issue(
                                issues=issues,
                                rule_id=rule_id,
                                severity=severity,
                                message="row_rules 的 expression/assert 表达式执行失败",
                                file_name=file_name,
                                sheet=sheet,
                                row_num=0,
                                column="",
                                expected="合法表达式",
                                actual=err,
                            )
                            expression_error_reported = True
                        rule_aborted = True
                        break

                    if not bool(result):
                        append_value_check_issue(
                            issues=issues,
                            rule_id=rule_id,
                            severity=severity,
                            message=message,
                            file_name=file_name,
                            sheet=sheet,
                            row_num=row_num,
                            column=str(rule.get("column", "")),
                            expected="表达式结果为 True",
                            actual="False",
                        )
        except Exception as exc:  # noqa: BLE001
            issues.append(
                make_exception_issue(
                    category="local",
                    rule_id=rule_id,
                    exc=exc,
                    file_name=str(dataset_sheet_lookup.get(dataset, {}).get("file", "")),
                    sheet=str(dataset_sheet_lookup.get(dataset, {}).get("sheet", "")),
                    context="row_rules 执行",
                )
            )
