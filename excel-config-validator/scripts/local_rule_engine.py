# -*- coding: utf-8 -*-
"""值级校验引擎。

按行执行 schema/range/row/aggregate 规则，并提供 row_rules 表达式能力。
包含日期解析、范围比较、正则匹配、枚举白名单、递增/唯一性检查，
以及基于安全 eval 的行级表达式引擎和聚合规则执行器。
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
    configured = str(rule.get("value_type") or "").strip().lower()
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
    """安全版 str()：限制输出长度，避免异常放大。"""
    s = str(v)
    return s[:10000] if len(s) > 10000 else s


def _safe_int(v: Any) -> int:
    """安全版 int()：限制超长输入。"""
    if isinstance(v, str) and len(v) > 50:
        raise ValueError("整数字符串过长")
    return int(v)


def _safe_float(v: Any) -> float:
    """安全版 float()：限制超长输入。"""
    if isinstance(v, str) and len(v) > 50:
        raise ValueError("浮点数字符串过长")
    return float(v)


def compile_row_expression(expr: str) -> tuple[Any | None, str | None]:
    """预编译表达式字符串，返回 (code, error)。"""
    if len(expr) > MAX_EXPRESSION_LENGTH:
        return None, f"表达式长度 {len(expr)} 超过限制 {MAX_EXPRESSION_LENGTH}"
    try:
        code = compile(expr, "<rule_expr>", "eval")
    except SyntaxError as exc:
        return None, f"表达式语法错误: {exc}"
    return code, None


def _build_eval_env(
    row_values: dict[str, Any],
    prev_row_values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """构建表达式执行环境，注入内置函数。"""

    # 基础访问函数
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
        """正则全匹配，data 作为待匹配文本。"""
        try:
            return re.fullmatch(pattern, value_text(data)) is not None
        except re.error:
            return False

    # 日期函数
    def date_val(column: str) -> datetime | None:
        """将列值解析为 datetime。"""
        return parse_datetime_value(row_values.get(column))

    def today() -> datetime:
        """返回今天零点的 datetime。"""
        return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    def days_between(col1: str, col2: str) -> int | None:
        """返回 col2 - col1 的天数差。"""
        d1 = parse_datetime_value(row_values.get(col1))
        d2 = parse_datetime_value(row_values.get(col2))
        if d1 is None or d2 is None:
            return None
        return (d2 - d1).days

    def days_since(column: str) -> int | None:
        """返回该列日期到今天的天数。"""
        d = parse_datetime_value(row_values.get(column))
        if d is None:
            return None
        return (today() - d).days

    def year(column: str) -> int | None:
        """提取列日期的年份。"""
        d = parse_datetime_value(row_values.get(column))
        return d.year if d is not None else None

    def month(column: str) -> int | None:
        """提取列日期的月份。"""
        d = parse_datetime_value(row_values.get(column))
        return d.month if d is not None else None

    def day(column: str) -> int | None:
        """提取列日期的日。"""
        d = parse_datetime_value(row_values.get(column))
        return d.day if d is not None else None

    # 字符串增强函数
    def strip(column: str) -> str:
        """读取列值并去除首尾空白。"""
        return value_text(row_values.get(column)).strip()

    def lower(column: str) -> str:
        """读取列值并转为小写。"""
        return text(column).lower()

    def upper(column: str) -> str:
        """读取列值并转为大写。"""
        return text(column).upper()

    def contains(column: str, substr: str) -> bool:
        """判断列文本是否包含子串。"""
        return substr in text(column)

    def starts_with(column: str, prefix: str) -> bool:
        """判断列文本是否以前缀开头。"""
        return text(column).startswith(prefix)

    def ends_with(column: str, suffix: str) -> bool:
        """判断列文本是否以后缀结尾。"""
        return text(column).endswith(suffix)

    # 跨行引用函数
    def prev_value(column: str, default: Any = None) -> Any:
        """读取上一行列原始值；首行返回 default。"""
        if prev_row_values is None:
            return default
        return prev_row_values.get(column, default)

    def prev_text(column: str, default: str = "") -> str:
        """读取上一行列字符串值。"""
        if prev_row_values is None:
            return default
        v = prev_row_values.get(column, default)
        return value_text(v).strip()

    def prev_num(column: str) -> float | None:
        """读取上一行列数值。"""
        if prev_row_values is None:
            return None
        return parse_number(prev_row_values.get(column))

    # 多列辅助函数
    def sum_cols(*cols: str) -> float | None:
        """多列求和；任一列非数字则返回 None。"""
        total = 0.0
        for col in cols:
            n = parse_number(row_values.get(col))
            if n is None:
                return None
            total += n
        return total

    def coalesce(*cols: str) -> Any:
        """返回多个列中第一个非空值。"""
        for col in cols:
            v = row_values.get(col)
            if not is_empty(v):
                return v
        return None

    def in_list(val: Any, items: Any) -> bool:
        """判断值是否在给定列表/元组中。"""
        if isinstance(items, (list, tuple, set, frozenset)):
            return val in items
        return False

    return {
        "row": row_values,
        # 基础函数
        "value": value,
        "text": text,
        "num": num,
        "intv": intv,
        "empty": empty,
        "exists": exists,
        "match": match,
        # 日期函数
        "date_val": date_val,
        "today": today,
        "days_between": days_between,
        "days_since": days_since,
        "year": year,
        "month": month,
        "day": day,
        # 字符串函数
        "strip": strip,
        "lower": lower,
        "upper": upper,
        "contains": contains,
        "starts_with": starts_with,
        "ends_with": ends_with,
        # 跨行函数
        "prev_value": prev_value,
        "prev_text": prev_text,
        "prev_num": prev_num,
        # 多列函数
        "sum_cols": sum_cols,
        "coalesce": coalesce,
        "in_list": in_list,
        # 标量函数
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


def safe_eval_row_expression(
    expr: str,
    row_values: dict[str, Any],
    *,
    prev_row_values: dict[str, Any] | None = None,
    compiled_code: Any | None = None,
    prebuilt_env: dict[str, Any] | None = None,
) -> tuple[Any | None, str | None]:
    """安全执行行表达式。"""
    if compiled_code is None and len(expr) > MAX_EXPRESSION_LENGTH:
        return None, f"表达式长度 {len(expr)} 超过限制 {MAX_EXPRESSION_LENGTH}"

    env = prebuilt_env if prebuilt_env is not None else _build_eval_env(row_values, prev_row_values)
    try:
        code = compiled_code if compiled_code is not None else expr
        result = eval(code, {"__builtins__": {}}, env)  # noqa: S307
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
                check_type = str(item.get("type") or "").strip()
                if check_type:
                    x = dict(item)
                    x["type"] = check_type
                    checks.append(x)

    single_check = str(rule.get("check", "")).strip()
    if single_check and not checks:
        meta_keys = {"rule_id", "dataset", "column", "severity", "check", "checks", "message", "enabled"}
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
    file_path: str = "",
    file_sha256: str = "",
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
            file_path=file_path,
            file_sha256=file_sha256,
        )
    )

    # 供 validate_local/validate_row_rules 等内部流程复用。
    # file_path / file_sha256 由调用方从 entry 中提取后传入。


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
    """对每个数据集执行范围规则校验（min/max 边界检查）。"""
    range_rules = rules.get("range_rules", [])
    if not isinstance(range_rules, list):
        return

    for idx, rule in enumerate(range_rules):
        if not isinstance(rule, dict):
            continue
        if not rule.get("enabled", True):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        column = str(rule.get("column", "")).strip()
        rule_id = str(rule.get("rule_id", f"RANGE_RULE_{idx}"))
        severity = str(rule.get("severity", "error"))
        allow_empty = bool(rule.get("allow_empty", True))
        include_min = bool(rule.get("include_min", True))
        include_max = bool(rule.get("include_max", True))
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

            # 第一遍：仅提取样本值用于类型推断
            sample_value = None
            for chunk in iter_rows_from_entry(entry):
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
                if sample_value is not None:
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

            # 第二遍：按分块流式执行范围校验（不整表加载）
            for chunk in iter_rows_from_entry(entry):
                for row_item in chunk:
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


def _compile_branches(
    branches: list[dict[str, Any]],
    rule_id: str,
    severity: str,
    file_name: str,
    sheet: str,
    issues: list[dict[str, Any]],
) -> list[dict[str, Any]] | None:
    """预编译 branches 中的 when/assert 表达式。"""
    compiled_branches: list[dict[str, Any]] = []
    for bi, branch in enumerate(branches):
        if not isinstance(branch, dict):
            continue
        b_when = str(branch.get("when", "")).strip()
        b_assert = str(branch.get("assert", "")).strip()
        b_message = str(branch.get("message", "")).strip()
        if not b_assert:
            continue

        c_when = None
        if b_when:
            c_when, err = compile_row_expression(b_when)
            if err:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"branches[{bi}].when 表达式编译失败: {err}",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=0,
                    column="",
                    expected="合法表达式",
                    actual=err,
                )
                return None

        c_assert, err = compile_row_expression(b_assert)
        if err:
            append_value_check_issue(
                issues=issues,
                rule_id=rule_id,
                severity=severity,
                message=f"branches[{bi}].assert 表达式编译失败: {err}",
                file_name=file_name,
                sheet=sheet,
                row_num=0,
                column="",
                expected="合法表达式",
                actual=err,
            )
            return None

        compiled_branches.append({
            "compiled_when": c_when,
            "when_expr": b_when,
            "compiled_assert": c_assert,
            "assert_expr": b_assert,
            "message": b_message or f"分支[{bi}]校验未通过: {b_assert}",
        })
    return compiled_branches


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
        if not rule.get("enabled", True):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        rule_id = str(rule.get("rule_id", f"ROW_RULE_{idx}"))
        severity = str(rule.get("severity", "error"))
        if not dataset:
            continue

        # 判断是否为 branches 模式
        branches_raw = rule.get("branches")
        is_branches_mode = isinstance(branches_raw, list) and len(branches_raw) > 0

        # 传统单表达式字段
        when_expr = str(rule.get("when", "")).strip()
        assert_expr = str(rule.get("assert", "")).strip()

        if not is_branches_mode and not assert_expr:
            continue

        try:
            entry = dataset_sheet_lookup.get(dataset)
            if not isinstance(entry, dict):
                continue

            file_name = str(entry.get("file", ""))
            sheet = str(entry.get("sheet", ""))

            if is_branches_mode:
                # branches 条件分支模式
                compiled_branches = _compile_branches(
                    branches_raw, rule_id, severity, file_name, sheet, issues,
                )
                if compiled_branches is None:
                    continue

                # 编译 else_assert
                else_expr = str(rule.get("else_assert", "")).strip()
                compiled_else = None
                if else_expr:
                    compiled_else, else_err = compile_row_expression(else_expr)
                    if else_err:
                        append_value_check_issue(
                            issues=issues,
                            rule_id=rule_id,
                            severity=severity,
                            message=f"else_assert 表达式编译失败: {else_err}",
                            file_name=file_name,
                            sheet=sheet,
                            row_num=0,
                            column="",
                            expected="合法表达式",
                            actual=else_err,
                        )
                        continue
                else_message = str(rule.get("else_message", "")).strip() or f"else 分支校验未通过: {else_expr}"

                expression_error_reported = False
                rule_aborted = False
                prev_row_values: dict[str, Any] | None = None

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

                        # 每行构建一次执行环境，供所有分支复用
                        row_env = _build_eval_env(values, prev_row_values)

                        matched_branch = False
                        for branch in compiled_branches:
                            # 计算 when 条件
                            if branch["compiled_when"] is not None:
                                w_result, w_err = safe_eval_row_expression(
                                    branch["when_expr"],
                                    values,
                                    prev_row_values=prev_row_values,
                                    compiled_code=branch["compiled_when"],
                                    prebuilt_env=row_env,
                                )
                                if w_err:
                                    if not expression_error_reported:
                                        append_value_check_issue(
                                            issues=issues,
                                            rule_id=rule_id,
                                            severity=severity,
                                            message="branches when 表达式执行失败",
                                            file_name=file_name,
                                            sheet=sheet,
                                            row_num=row_num,
                                            column="",
                                            expected="合法表达式",
                                            actual=w_err,
                                        )
                                        expression_error_reported = True
                                    rule_aborted = True
                                    break
                                if not bool(w_result):
                                    continue

                            # 条件命中或未设置 when 时执行 assert
                            matched_branch = True
                            a_result, a_err = safe_eval_row_expression(
                                branch["assert_expr"],
                                values,
                                prev_row_values=prev_row_values,
                                compiled_code=branch["compiled_assert"],
                                prebuilt_env=row_env,
                            )
                            if a_err:
                                if not expression_error_reported:
                                    append_value_check_issue(
                                        issues=issues,
                                        rule_id=rule_id,
                                        severity=severity,
                                        message="branches assert 表达式执行失败",
                                        file_name=file_name,
                                        sheet=sheet,
                                        row_num=row_num,
                                        column="",
                                        expected="合法表达式",
                                        actual=a_err,
                                    )
                                    expression_error_reported = True
                                rule_aborted = True
                                break

                            if not bool(a_result):
                                rule_column = str(rule.get("column", "")).strip()
                                actual_val = ""
                                if rule_column:
                                    raw_val = values.get(rule_column)
                                    actual_val = value_text(raw_val).strip() if not is_empty(raw_val) else "空"
                                else:
                                    actual_val = "不满足"
                                append_value_check_issue(
                                    issues=issues,
                                    rule_id=rule_id,
                                    severity=severity,
                                    message=branch["message"],
                                    file_name=file_name,
                                    sheet=sheet,
                                    row_num=row_num,
                                    column=rule_column,
                                    expected=branch["message"],
                                    actual=actual_val,
                                )
                            break

                        if rule_aborted:
                            break

                        # 无分支命中时执行 else_assert
                        if not matched_branch and compiled_else is not None:
                            e_result, e_err = safe_eval_row_expression(
                                else_expr,
                                values,
                                prev_row_values=prev_row_values,
                                compiled_code=compiled_else,
                                prebuilt_env=row_env,
                            )
                            if e_err:
                                if not expression_error_reported:
                                    append_value_check_issue(
                                        issues=issues,
                                        rule_id=rule_id,
                                        severity=severity,
                                        message="else_assert 表达式执行失败",
                                        file_name=file_name,
                                        sheet=sheet,
                                        row_num=row_num,
                                        column="",
                                        expected="合法表达式",
                                        actual=e_err,
                                    )
                                    expression_error_reported = True
                                rule_aborted = True
                                break

                            if not bool(e_result):
                                rule_column = str(rule.get("column", "")).strip()
                                actual_val = ""
                                if rule_column:
                                    raw_val = values.get(rule_column)
                                    actual_val = value_text(raw_val).strip() if not is_empty(raw_val) else "空"
                                else:
                                    actual_val = "不满足"
                                append_value_check_issue(
                                    issues=issues,
                                    rule_id=rule_id,
                                    severity=severity,
                                    message=else_message,
                                    file_name=file_name,
                                    sheet=sheet,
                                    row_num=row_num,
                                    column=rule_column,
                                    expected=else_message,
                                    actual=actual_val,
                                )

                        prev_row_values = values

            else:
                # 传统 when/assert 模式
                message = str(rule.get("message", "")).strip() or f"行规则表达式未满足: {assert_expr}"

                compiled_when = None
                if when_expr:
                    compiled_when, when_compile_err = compile_row_expression(when_expr)
                    if when_compile_err:
                        append_value_check_issue(
                            issues=issues,
                            rule_id=rule_id,
                            severity=severity,
                            message=f"row_rules 的 when 表达式编译失败: {when_compile_err}",
                            file_name=file_name,
                            sheet=sheet,
                            row_num=0,
                            column="",
                            expected="合法表达式",
                            actual=when_compile_err,
                        )
                        continue

                compiled_assert, assert_compile_err = compile_row_expression(assert_expr)
                if assert_compile_err:
                    append_value_check_issue(
                        issues=issues,
                        rule_id=rule_id,
                        severity=severity,
                        message=f"row_rules 的 expression/assert 表达式编译失败: {assert_compile_err}",
                        file_name=file_name,
                        sheet=sheet,
                        row_num=0,
                        column="",
                        expected="合法表达式",
                        actual=assert_compile_err,
                    )
                    continue

                expression_error_reported = False
                rule_aborted = False
                prev_row_values: dict[str, Any] | None = None

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

                        row_env = _build_eval_env(values, prev_row_values)

                        if compiled_when is not None:
                            when_result, when_error = safe_eval_row_expression(
                                when_expr,
                                values,
                                prev_row_values=prev_row_values,
                                compiled_code=compiled_when,
                                prebuilt_env=row_env,
                            )
                            if when_error:
                                if not expression_error_reported:
                                    append_value_check_issue(
                                        issues=issues,
                                        rule_id=rule_id,
                                        severity=severity,
                                        message="row_rules 的 when 表达式执行失败",
                                        file_name=file_name,
                                        sheet=sheet,
                                        row_num=row_num,
                                        column="",
                                        expected="合法表达式",
                                        actual=when_error,
                                    )
                                    expression_error_reported = True
                                rule_aborted = True
                                break
                            if not bool(when_result):
                                prev_row_values = values
                                continue

                        result, err = safe_eval_row_expression(
                            assert_expr,
                            values,
                            prev_row_values=prev_row_values,
                            compiled_code=compiled_assert,
                            prebuilt_env=row_env,
                        )
                        if err:
                            if not expression_error_reported:
                                append_value_check_issue(
                                    issues=issues,
                                    rule_id=rule_id,
                                    severity=severity,
                                    message="row_rules 的 expression/assert 表达式执行失败",
                                    file_name=file_name,
                                    sheet=sheet,
                                    row_num=row_num,
                                    column="",
                                    expected="合法表达式",
                                    actual=err,
                                )
                                expression_error_reported = True
                            rule_aborted = True
                            break

                        if not bool(result):
                            rule_column = str(rule.get("column", "")).strip()
                            actual_val = ""
                            if rule_column:
                                raw_val = values.get(rule_column)
                                actual_val = value_text(raw_val).strip() if not is_empty(raw_val) else "空"
                            else:
                                actual_val = "不满足"
                            append_value_check_issue(
                                issues=issues,
                                rule_id=rule_id,
                                severity=severity,
                                message=message,
                                file_name=file_name,
                                sheet=sheet,
                                row_num=row_num,
                                column=rule_column,
                                expected=message,
                                actual=actual_val,
                            )

                        prev_row_values = values
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


AGGREGATE_FUNCTIONS = {"sum", "count", "avg", "min", "max", "distinct_count"}


def _aggregate_column(
    entry: dict[str, Any],
    column: str,
    func: str,
    group_by: str,
) -> dict[str, Any]:
    """对数据集指定列执行聚合并返回结果映射。"""
    accumulators: dict[str, list[float]] = {}
    distinct_sets: dict[str, set[str]] = {}
    count_map: dict[str, int] = {}

    for chunk in iter_rows_from_entry(entry):
        for row_item in chunk:
            if not isinstance(row_item, dict):
                continue
            values = row_item.get("values", {})
            if not isinstance(values, dict):
                continue

            group_key = "__ALL__"
            if group_by:
                gv = values.get(group_by)
                group_key = value_text(gv).strip() if not is_empty(gv) else "__EMPTY__"

            raw_value = values.get(column)

            if func == "count":
                count_map[group_key] = count_map.get(group_key, 0) + (0 if is_empty(raw_value) else 1)
                continue

            if func == "distinct_count":
                if group_key not in distinct_sets:
                    distinct_sets[group_key] = set()
                if not is_empty(raw_value):
                    distinct_sets[group_key].add(value_text(raw_value).strip())
                continue

            # sum / avg / min / max 仅对可解析数值生效
            num_val = parse_number(raw_value)
            if num_val is None:
                continue
            if group_key not in accumulators:
                accumulators[group_key] = []
            accumulators[group_key].append(num_val)

    results: dict[str, Any] = {}
    if func == "count":
        for k, v in count_map.items():
            results[k] = v
    elif func == "distinct_count":
        for k, v in distinct_sets.items():
            results[k] = len(v)
    elif func == "sum":
        for k, vals in accumulators.items():
            results[k] = sum(vals)
    elif func == "avg":
        for k, vals in accumulators.items():
            results[k] = sum(vals) / len(vals) if vals else None
    elif func == "min":
        for k, vals in accumulators.items():
            results[k] = min(vals) if vals else None
    elif func == "max":
        for k, vals in accumulators.items():
            results[k] = max(vals) if vals else None

    return results


def validate_aggregate_rules(
    *,
    rules: dict[str, Any],
    dataset_sheet_lookup: dict[str, dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    """执行 aggregate_rules：聚合后对结果断言。"""
    aggregate_rules = rules.get("aggregate_rules", [])
    if not isinstance(aggregate_rules, list):
        return

    for idx, rule in enumerate(aggregate_rules):
        if not isinstance(rule, dict):
            continue
        if not rule.get("enabled", True):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        column = str(rule.get("column", "")).strip()
        func = str(rule.get("function", "")).strip().lower()
        group_by = str(rule.get("group_by", "")).strip()
        assert_expr = str(rule.get("assert", "")).strip()
        rule_id = str(rule.get("rule_id", f"AGG_RULE_{idx}"))
        severity = str(rule.get("severity", "error"))
        message = str(rule.get("message", "")).strip()

        if not dataset or not column or not func or not assert_expr:
            continue

        if func not in AGGREGATE_FUNCTIONS:
            issues.append(
                make_issue(
                    category="local",
                    rule_id=rule_id,
                    severity="error",
                    message=f"不支持的聚合函数 '{func}'",
                    file_name="",
                    sheet="",
                    row=0,
                    column=column,
                    expected=f"支持的函数: {', '.join(sorted(AGGREGATE_FUNCTIONS))}",
                    actual=func,
                )
            )
            continue

        try:
            entry = dataset_sheet_lookup.get(dataset)
            if not isinstance(entry, dict):
                continue

            file_name = str(entry.get("file", ""))
            sheet = str(entry.get("sheet", ""))
            headers = [str(h) for h in entry.get("headers", [])]

            if column not in headers:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"聚合校验依赖列 '{column}'，但工作表 '{sheet}' 中不存在该列",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=1,
                    column=column,
                    expected=f"列 '{column}' 存在",
                    actual="列缺失",
                )
                continue

            if group_by and group_by not in headers:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"聚合校验分组列 '{group_by}'，但工作表 '{sheet}' 中不存在该列",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=1,
                    column=group_by,
                    expected=f"列 '{group_by}' 存在",
                    actual="列缺失",
                )
                continue

            # 预编译 assert 表达式
            compiled_assert, compile_err = compile_row_expression(assert_expr)
            if compile_err:
                append_value_check_issue(
                    issues=issues,
                    rule_id=rule_id,
                    severity=severity,
                    message=f"聚合规则 assert 表达式编译失败: {compile_err}",
                    file_name=file_name,
                    sheet=sheet,
                    row_num=0,
                    column=column,
                    expected="合法表达式",
                    actual=compile_err,
                )
                continue

            # 执行聚合
            group_results = _aggregate_column(entry, column, func, group_by)

            # 对每个分组执行断言
            for group_key, agg_result in group_results.items():
                if agg_result is None:
                    continue

                env = {
                    "result": agg_result,
                    "group": group_key if group_key != "__ALL__" else "",
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
                    assert_result = eval(compiled_assert, {"__builtins__": {}}, env)  # noqa: S307
                except Exception as exc:  # noqa: BLE001
                    append_value_check_issue(
                        issues=issues,
                        rule_id=rule_id,
                        severity=severity,
                        message=f"聚合规则 assert 表达式执行失败: {exc}",
                        file_name=file_name,
                        sheet=sheet,
                        row_num=0,
                        column=column,
                        expected="合法表达式",
                        actual=str(exc),
                    )
                    break

                if not bool(assert_result):
                    group_display = f" (分组: {group_key})" if group_key != "__ALL__" else ""
                    default_msg = f"聚合校验未通过{group_display}: {func}({column}) = {agg_result}"
                    append_value_check_issue(
                        issues=issues,
                        rule_id=rule_id,
                        severity=severity,
                        message=message or default_msg,
                        file_name=file_name,
                        sheet=sheet,
                        row_num=0,
                        column=column,
                        expected=f"assert: {assert_expr}",
                        actual=f"{func}={agg_result}{group_display}",
                    )
        except Exception as exc:  # noqa: BLE001
            issues.append(
                make_exception_issue(
                    category="local",
                    rule_id=rule_id,
                    exc=exc,
                    file_name=str(dataset_sheet_lookup.get(dataset, {}).get("file", "")),
                    sheet=str(dataset_sheet_lookup.get(dataset, {}).get("sheet", "")),
                    context="aggregate_rules 执行",
                )
            )
