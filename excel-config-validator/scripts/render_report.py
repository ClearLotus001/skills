"""报告渲染 — 合并各阶段 issues 并生成 JSON/CSV/Markdown/HTML 报告。

由 run_validator.py 内部调用，也可独立执行。
输入: ingest_manifest.json、compiled_rules.json、*_issues.json
输出: result.json、issues.csv、report.md、report.html
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

from common import (
    atomic_write_json,
    atomic_write_text,
    category_key,
    category_label_zh,
    dataset_configs,
    severity_key,
    severity_label_zh,
    severity_rank,
    utc_now_iso,
)


RULE_GROUP_KEYS = ("schema_rules", "range_rules", "row_rules", "relation_rules", "global_rules")


def format_timestamp_display(iso_text: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError:
        return iso_text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    local_dt = dt.astimezone()
    offset = local_dt.strftime("%z")
    if offset and len(offset) == 5:
        offset = f"{offset[:3]}:{offset[3:]}"
    return f"{local_dt.strftime('%Y-%m-%d %H:%M:%S')} (UTC{offset})"


def load_issues(issue_file: Path) -> list[dict[str, Any]]:
    if not issue_file.exists():
        return []
    data = json.loads(issue_file.read_text(encoding="utf-8"))
    issues = data.get("issues", [])
    return issues if isinstance(issues, list) else []


def rule_group_label_zh(group_key: str) -> str:
    table = {
        "schema_rules": "结构规则",
        "range_rules": "范围规则",
        "row_rules": "行规则",
        "relation_rules": "关联规则",
        "global_rules": "全局规则",
    }
    return table.get(group_key, group_key)


def check_label_zh(check: str) -> str:
    table = {
        "required": "必填",
        "string": "字符串",
        "numeric": "数字",
        "min_digits": "最少位数",
        "increasing": "递增",
        "unique": "唯一",
        "date": "日期",
        "datetime_format": "时间格式",
        "max_length": "最大长度",
        "min_length": "最小长度",
        "regex": "正则匹配",
        "enum": "枚举",
        "whitelist": "白名单",
        "positive": "正数",
        "non_negative": "非负数",
        "conditional_required": "条件必填",
    }
    return table.get(str(check).strip().lower(), str(check))


def schema_expectation_text(rule: dict[str, Any]) -> str:
    check = str(rule.get("check", "")).strip().lower()
    if check == "required":
        return "不能为空"
    if check == "string":
        return "必须为字符串"
    if check == "numeric":
        return "必须为数字"
    if check == "min_digits":
        n = int(rule.get("min_digits", 0) or 0)
        return f"至少 {n} 位数字" if n > 0 else "必须满足最小位数要求"
    if check == "increasing":
        return "按行严格递增"
    if check == "date":
        return "必须为日期格式"
    if check == "datetime_format":
        fmt = str(rule.get("format") or "YYYY-MM-DD HH:MM:SS").strip()
        return f"必须符合时间格式 {fmt}"
    if check == "max_length":
        n = rule.get("max_length")
        return f"长度不超过 {n}"
    if check == "regex":
        pattern = str(rule.get("pattern") or "").strip()
        return f"必须匹配正则 {pattern}" if pattern else "必须匹配指定正则"
    if check in {"enum", "whitelist"}:
        values = rule.get("values", [])
        preview = ", ".join(str(v) for v in (values[:5] if isinstance(values, list) else []))
        return f"允许值: [{preview}]" if preview else "必须在允许列表内"
    if check == "unique":
        return "列值唯一"
    if check == "min_length":
        n = int(rule.get("min_length", 0) or 0)
        return f"长度不低于 {n}" if n > 0 else "必须满足最小长度要求"
    if check == "positive":
        return "必须为正数"
    if check == "non_negative":
        return "必须为非负数"
    if check == "conditional_required":
        when = str(rule.get("when", "")).strip()
        return f"当 {when} 时不能为空" if when else "条件必填"
    checks = rule_checks_text(rule)
    return f"执行 {checks} 检查" if checks and checks != "-" else "字段校验"


def dataset_location_text(dataset_id: str, ds_cfg: dict[str, dict[str, Any]]) -> str:
    if not dataset_id:
        return "-"
    cfg = ds_cfg.get(dataset_id, {})
    file_name = str(cfg.get("file") or cfg.get("file_pattern") or "").strip()
    sheet = str(cfg.get("sheet", "")).strip()
    if file_name and sheet:
        return f"{file_name}/{sheet}"
    if file_name:
        return file_name
    if sheet:
        return sheet
    return dataset_id


def rule_checks_text(rule: dict[str, Any]) -> str:
    checks = rule.get("checks")
    if isinstance(checks, list):
        labels = []
        for item in checks:
            if isinstance(item, str):
                labels.append(check_label_zh(item))
            elif isinstance(item, dict):
                labels.append(check_label_zh(str(item.get("type") or item.get("check") or "")))
        labels = [x for x in labels if x]
        if labels:
            return "、".join(labels)
    check = str(rule.get("check", "")).strip()
    if check:
        return check_label_zh(check)
    return "-"


def infer_rule_title_and_desc(rule: dict[str, Any], group_key: str, ds_cfg: dict[str, dict[str, Any]]) -> tuple[str, str]:
    explicit_title = str(rule.get("title") or rule.get("name") or rule.get("label") or "").strip()
    explicit_desc = str(rule.get("description") or rule.get("message") or "").strip()
    if explicit_title:
        return explicit_title, explicit_desc or explicit_title

    if group_key == "schema_rules":
        ds = str(rule.get("dataset", ""))
        col = str(rule.get("column", ""))
        check = str(rule.get("check", "")).strip()
        check_label = check_label_zh(check) if check else "字段"
        ds_loc = dataset_location_text(ds, ds_cfg)
        title = f"{ds_loc}.{col} {check_label}校验"
        desc = f"{ds_loc} 的列 '{col}'：{schema_expectation_text(rule)}"
        return title, desc

    if group_key == "range_rules":
        ds = str(rule.get("dataset", ""))
        col = str(rule.get("column", ""))
        min_v = rule.get("min")
        max_v = rule.get("max")
        include_min = bool(rule.get("include_min", True))
        include_max = bool(rule.get("include_max", True))
        left_bracket = "[" if include_min else "("
        right_bracket = "]" if include_max else ")"
        ds_loc = dataset_location_text(ds, ds_cfg)
        title = f"{ds_loc}.{col} 范围校验"
        desc = f"{ds_loc} 的列 '{col}' 范围：{left_bracket}{min_v}, {max_v}{right_bracket}"
        return title, desc

    if group_key == "row_rules":
        ds = str(rule.get("dataset", ""))
        when_expr = str(rule.get("when", "")).strip()
        expr = str(rule.get("expression") or rule.get("assert") or "").strip()
        ds_loc = dataset_location_text(ds, ds_cfg)
        title = f"{ds_loc} 行表达式校验"
        desc = str(rule.get("message", "")).strip() or f"when={when_expr or 'True'}; assert={expr}"
        return title, desc

    if group_key == "relation_rules":
        source = str(rule.get("source_dataset") or rule.get("from_dataset") or "")
        target = str(rule.get("target_dataset") or rule.get("to_dataset") or "")
        source_key = str(rule.get("source_key") or rule.get("from_key") or "")
        target_key = str(rule.get("target_key") or rule.get("to_key") or "")
        mode = str(rule.get("mode") or rule.get("relation_type") or "fk_exists")
        source_loc = dataset_location_text(source, ds_cfg)
        target_loc = dataset_location_text(target, ds_cfg)
        title = f"{source_loc}.{source_key} -> {target_loc}.{target_key}"
        desc = (
            f"{source_loc}.{source_key} 与 "
            f"{target_loc}.{target_key} 关联检查（{mode}）"
        )
        return title, desc

    title = str(rule.get("rule_id", "全局规则"))
    desc = explicit_desc or title
    return title, desc


def build_rule_catalog(compiled_rules: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    raw_rules = compiled_rules.get("rules", {})
    ds_cfg = dataset_configs(raw_rules) if isinstance(raw_rules, dict) else {}
    catalog: list[dict[str, Any]] = []
    by_id: dict[str, dict[str, Any]] = {}

    if not isinstance(raw_rules, dict):
        return catalog, by_id

    for group_key in RULE_GROUP_KEYS:
        group_rules = raw_rules.get(group_key, [])
        if not isinstance(group_rules, list):
            continue
        for idx, item in enumerate(group_rules):
            if not isinstance(item, dict):
                continue
            rule_id = str(item.get("rule_id", f"{group_key}_{idx}"))
            title, desc = infer_rule_title_and_desc(item, group_key, ds_cfg)
            entry = {
                "rule_id": rule_id,
                "rule_group": group_key,
                "rule_group_zh": rule_group_label_zh(group_key),
                "rule_title": title,
                "rule_desc": desc,
            }
            catalog.append(entry)
            if rule_id not in by_id:
                by_id[rule_id] = entry
    return catalog, by_id


def summarize_input_files(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    files = manifest.get("files", [])
    if not isinstance(files, list):
        return out
    for file_item in files:
        if not isinstance(file_item, dict):
            continue
        sheets_out: list[dict[str, Any]] = []
        sheets = file_item.get("sheets", [])
        if isinstance(sheets, list):
            for sheet in sheets:
                if not isinstance(sheet, dict):
                    continue
                sheets_out.append(
                    {
                        "sheet": str(sheet.get("sheet", "")),
                        "header_count": len(sheet.get("headers", []) or []),
                        "row_count_estimate": int(sheet.get("row_count_estimate", 0) or 0),
                    }
                )
        out.append(
            {
                "name": str(file_item.get("name", "")),
                "path": str(file_item.get("path", "")),
                "extension": str(file_item.get("extension", "")),
                "size_bytes": int(file_item.get("size_bytes", 0) or 0),
                "sheet_count": len(sheets_out),
                "sheets": sheets_out,
                "parse_warnings": file_item.get("parse_warnings", []) if isinstance(file_item.get("parse_warnings", []), list) else [],
                "parse_notes": file_item.get("parse_notes", []) if isinstance(file_item.get("parse_notes", []), list) else [],
            }
        )
    return out


def sheet_rows_text(sheets: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for s in sheets:
        if not isinstance(s, dict):
            continue
        parts.append(f"{s.get('sheet', '-')}:约{s.get('row_count_estimate', 0)}行")
    return ", ".join(parts)


def enrich_issues_with_rule_info(
    issues: list[dict[str, Any]],
    catalog_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for issue in issues:
        x = dict(issue)
        rid = str(x.get("rule_id", ""))
        info = catalog_by_id.get(rid)
        if info:
            x["rule_title"] = info.get("rule_title", rid)
            x["rule_desc"] = info.get("rule_desc", "")
            x["rule_group"] = info.get("rule_group", "")
            x["rule_group_zh"] = info.get("rule_group_zh", "")
        else:
            x["rule_title"] = rid or "未知规则"
            x["rule_desc"] = ""
            x["rule_group"] = ""
            x["rule_group_zh"] = ""
        out.append(x)
    return out


def load_i18n_config(i18n_path: Path | None) -> dict[str, Any]:
    if i18n_path is None or not i18n_path.exists():
        return {"exact": {}, "regex": []}
    try:
        data = json.loads(i18n_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"exact": {}, "regex": []}
    if not isinstance(data, dict):
        return {"exact": {}, "regex": []}
    exact = data.get("exact", {})
    regex_rules = data.get("regex", [])
    if not isinstance(exact, dict):
        exact = {}
    if not isinstance(regex_rules, list):
        regex_rules = []
    return {"exact": exact, "regex": regex_rules}


def translate_message_zh(message: str, i18n_config: dict[str, Any] | None = None) -> str:
    msg = message.strip()
    if not msg:
        return msg

    config = i18n_config or {}
    exact = config.get("exact", {})
    if isinstance(exact, dict) and msg in exact:
        return str(exact[msg])

    regex_rules = config.get("regex", [])
    if isinstance(regex_rules, list):
        for item in regex_rules:
            if not isinstance(item, dict):
                continue
            pattern = str(item.get("pattern", ""))
            zh_template = str(item.get("zh", ""))
            if not pattern or not zh_template:
                continue
            try:
                matched = re.match(pattern, msg)
            except re.error:
                continue
            if not matched:
                continue
            translated = zh_template
            for i, value in enumerate(matched.groups()):
                translated = translated.replace(f"{{{i}}}", value)
            return translated

    patterns: list[tuple[str, str]] = [
        (
            r"^missing required column '([^']+)' in sheet '([^']+)'$",
            "工作表 '{1}' 缺少必需列 '{0}'",
        ),
        (
            r"^dataset '([^']+)' expects missing sheet '([^']+)'$",
            "数据集 '{0}' 期望的工作表 '{1}' 不存在",
        ),
        (
            r"^relation source dataset '([^']+)' is undefined$",
            "关联规则中的源数据集 '{0}' 未定义",
        ),
        (
            r"^relation target dataset '([^']+)' is undefined$",
            "关联规则中的目标数据集 '{0}' 未定义",
        ),
        (
            r"^source sheet '([^']+)' for dataset '([^']+)' not found$",
            "数据集 '{1}' 的源工作表 '{0}' 未找到",
        ),
        (
            r"^target sheet '([^']+)' for dataset '([^']+)' not found$",
            "数据集 '{1}' 的目标工作表 '{0}' 未找到",
        ),
        (
            r"^rule_id '([^']+)' appears in multiple rule groups$",
            "规则 ID '{0}' 在多个规则组中重复出现",
        ),
        (
            r"^quality gate failed: errors exceed ([0-9]+)$",
            "质量门禁失败：错误数量超过阈值 {0}",
        ),
    ]
    for pattern, zh_template in patterns:
        matched = re.match(pattern, msg)
        if not matched:
            continue
        translated = zh_template
        for i, value in enumerate(matched.groups()):
            translated = translated.replace(f"{{{i}}}", value)
        return translated

    return msg


def localize_issues(issues: list[dict[str, Any]], i18n_config: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    localized: list[dict[str, Any]] = []
    for issue in issues:
        x = dict(issue)
        x["severity"] = severity_key(x.get("severity") or x.get("severity_zh"))
        x["severity_zh"] = severity_label_zh(x.get("severity"))
        x["category"] = category_key(x.get("category") or x.get("category_zh"))
        x["category_zh"] = category_label_zh(x.get("category"))
        x["message"] = str(x.get("message", ""))
        if not str(x.get("message_zh", "")).strip():
            x["message_zh"] = translate_message_zh(x["message"], i18n_config=i18n_config)
        else:
            x["message_zh"] = str(x["message_zh"])
        localized.append(x)
    return localized


def csv_escape(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def write_issues_csv(path: Path, issues: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = [
        ("issue_id", "问题ID"),
        ("severity_zh", "严重级别"),
        ("severity", "严重级别代码"),
        ("category_zh", "问题类别"),
        ("category", "问题类别代码"),
        ("rule_group_zh", "规则组"),
        ("rule_group", "规则组代码"),
        ("rule_id", "规则ID"),
        ("rule_title", "规则名称"),
        ("rule_desc", "规则说明"),
        ("message_zh", "问题描述"),
        ("message", "问题描述原文"),
        ("file", "文件"),
        ("sheet", "工作表"),
        ("row", "行"),
        ("column", "列"),
        ("cell", "单元格"),
        ("expected", "期望值"),
        ("actual", "实际值"),
    ]
    with NamedTemporaryFile("w", encoding="utf-8", newline="", delete=False, dir=path.parent) as tmp:
        writer = csv.writer(tmp)
        writer.writerow([label for _, label in columns])
        for issue in issues:
            writer.writerow([csv_escape(issue.get(key, "")) for key, _ in columns])
        temp_path = Path(tmp.name)
    temp_path.replace(path)


def build_summary(issues: list[dict[str, Any]]) -> dict[str, Any]:
    severity_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    rule_counts: dict[str, int] = {}
    file_sheet_map: dict[tuple[str, str], dict[str, Any]] = {}
    category_file_sheet_rule_counts: dict[tuple[str, str, str, str], int] = {}

    for issue in issues:
        sev = severity_key(issue.get("severity"))
        cat = category_key(issue.get("category"))
        rid = str(issue.get("rule_id", "UNKNOWN"))
        file_name = str(issue.get("file", "") or "-")
        sheet_name = str(issue.get("sheet", "") or "-")

        severity_counts[sev] = severity_counts.get(sev, 0) + 1
        category_counts[cat] = category_counts.get(cat, 0) + 1
        rule_counts[rid] = rule_counts.get(rid, 0) + 1
        category_file_sheet_rule_counts[(cat, file_name, sheet_name, rid)] = (
            category_file_sheet_rule_counts.get((cat, file_name, sheet_name, rid), 0) + 1
        )

        fs_key = (file_name, sheet_name)
        if fs_key not in file_sheet_map:
            file_sheet_map[fs_key] = {
                "file": file_name,
                "sheet": sheet_name,
                "count": 0,
                "category_counts": {},
                "rule_counts": {},
            }
        file_sheet_map[fs_key]["count"] += 1
        file_sheet_map[fs_key]["category_counts"][cat] = file_sheet_map[fs_key]["category_counts"].get(cat, 0) + 1
        file_sheet_map[fs_key]["rule_counts"][rid] = file_sheet_map[fs_key]["rule_counts"].get(rid, 0) + 1

    top_rules = sorted(rule_counts.items(), key=lambda x: (-x[1], x[0]))[:20]
    by_category = [
        {"category": k, "category_zh": category_label_zh(k), "count": v}
        for k, v in sorted(category_counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    by_file_sheet = []
    for _, item in sorted(file_sheet_map.items(), key=lambda x: (-x[1]["count"], x[0][0], x[0][1])):
        top_rules_in_group = sorted(item["rule_counts"].items(), key=lambda x: (-x[1], x[0]))[:10]
        by_file_sheet.append(
            {
                "file": item["file"],
                "sheet": item["sheet"],
                "count": item["count"],
                "category_counts": item["category_counts"],
                "category_counts_zh": {category_label_zh(k): v for k, v in item["category_counts"].items()},
                "top_rules": [{"rule_id": rid, "count": c} for rid, c in top_rules_in_group],
            }
        )
    by_category_file_sheet_rule = [
        {
            "category": cat,
            "category_zh": category_label_zh(cat),
            "file": file_name,
            "sheet": sheet_name,
            "rule_id": rid,
            "count": c,
        }
        for (cat, file_name, sheet_name, rid), c in sorted(
            category_file_sheet_rule_counts.items(),
            key=lambda x: (-x[1], x[0][0], x[0][1], x[0][2], x[0][3]),
        )[:200]
    ]
    return {
        "total_issues": len(issues),
        "severity_counts": severity_counts,
        "severity_counts_zh": {severity_label_zh(k): v for k, v in severity_counts.items()},
        "category_counts": category_counts,
        "category_counts_zh": {category_label_zh(k): v for k, v in category_counts.items()},
        "by_category": by_category,
        "by_file_sheet": by_file_sheet,
        "by_category_file_sheet_rule": by_category_file_sheet_rule,
        "top_rules": [{"rule_id": rid, "count": count} for rid, count in top_rules],
    }


def render_template(template: str, values: dict[str, str]) -> str:
    output = template
    for key, value in values.items():
        output = output.replace(f"{{{{{key}}}}}", value)
    return output


def default_md_template() -> str:
    return (
        "# {{title}}\n\n"
        "## 报告概览\n\n"
        "- 生成时间：{{generated_at}}\n"
        "- 问题总数：{{total_issues}}\n\n"
        "## 输入文件\n\n"
        "{{input_files}}\n\n"
        "## 检查规则目录\n\n"
        "{{rule_catalog}}\n\n"
        "## 严重级别统计\n\n"
        "{{severity_counts}}\n\n"
        "## 问题类别统计\n\n"
        "{{category_counts}}\n\n"
        "## 高频规则\n\n"
        "{{top_rules}}\n\n"
        "## 文件/页签分组（Top 30）\n\n"
        "{{file_sheet_groups}}\n\n"
        "## 问题明细（最多展示 500 条）\n\n"
        "{{issues_table}}\n"
    )


def default_html_template() -> str:
    return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{{title}}</title>
</head>
<body>
  <h1>{{title}}</h1>
  <p>生成时间：{{generated_at}}</p>
  <p>问题总数：{{total_issues}}</p>
  <pre>{{severity_json}}</pre>
  <script>
    const issues = {{issues_json}};
    document.body.insertAdjacentHTML(
      "beforeend",
      `<p>已载入问题 ${issues.length} 条，可替换为自定义模板查看完整交互。</p>`
    );
  </script>
</body>
</html>
"""


def render_reports(
    out_dir: Path,
    manifest_path: Path,
    compiled_rules_path: Path,
    issue_files: list[Path],
    md_template_path: Path | None = None,
    html_template_path: Path | None = None,
    i18n_path: Path | None = None,
) -> tuple[Path, Path, Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    compiled_rules = json.loads(compiled_rules_path.read_text(encoding="utf-8"))
    i18n_config = load_i18n_config(i18n_path)
    input_files = summarize_input_files(manifest)
    rule_catalog, rule_catalog_by_id = build_rule_catalog(compiled_rules)

    issues: list[dict[str, Any]] = []
    for issue_file in issue_files:
        issues.extend(load_issues(issue_file))
    issues = localize_issues(issues, i18n_config=i18n_config)
    issues = enrich_issues_with_rule_info(issues, rule_catalog_by_id)
    issues.sort(key=lambda x: (severity_rank(str(x.get("severity", "info"))), str(x.get("rule_id", ""))))

    summary = build_summary(issues)
    generated_at = utc_now_iso()
    generated_at_display = format_timestamp_display(generated_at)
    result_payload = {
        "generated_at": generated_at,
        "generated_at_display": generated_at_display,
        "run_id": manifest.get("run_id", ""),
        "input_hash": manifest.get("input_hash", ""),
        "rules_hash": compiled_rules.get("rules_hash", ""),
        "inputs": {
            "input_root": manifest.get("input_root", ""),
            "totals": manifest.get("totals", {}),
            "files": input_files,
        },
        "rules": {
            "selected_rule_set": compiled_rules.get("selected_rule_set"),
            "catalog": rule_catalog,
        },
        "summary": summary,
        "issues": issues,
    }

    result_json_path = out_dir / "result.json"
    issues_csv_path = out_dir / "issues.csv"
    report_md_path = out_dir / "report.md"
    report_html_path = out_dir / "report.html"

    atomic_write_json(result_json_path, result_payload)
    write_issues_csv(issues_csv_path, issues)

    ordered_severity = sorted(summary["severity_counts"].items(), key=lambda x: severity_rank(x[0]))
    severity_lines = "\n".join(f"- {severity_label_zh(k)}: {v}" for k, v in ordered_severity) or "- 无"
    top_rule_lines = "\n".join(
        f"- {x['rule_id']}（{rule_catalog_by_id.get(x['rule_id'], {}).get('rule_title', x['rule_id'])}）：{x['count']}"
        for x in summary.get("top_rules", [])
    ) or "- 无"
    ordered_category = sorted(summary["category_counts"].items(), key=lambda x: (-x[1], x[0]))
    category_lines = "\n".join(f"- {category_label_zh(k)}: {v}" for k, v in ordered_category) or "- 无"
    file_sheet_group_lines = "\n".join(
        (
            f"- {x.get('file', '-')}/{x.get('sheet', '-')}: {x.get('count', 0)}"
            + (
                f"（{', '.join(f'{k}:{v}' for k, v in (x.get('category_counts_zh') or {}).items())}）"
                if isinstance(x.get("category_counts_zh"), dict) and x.get("category_counts_zh")
                else ""
            )
        )
        for x in summary.get("by_file_sheet", [])[:30]
    ) or "- 无"
    input_file_lines = "\n".join(
        (
            f"- {x.get('name', '-')}: {x.get('sheet_count', 0)} 个工作表"
            + (
                f"（{sheet_rows_text(x.get('sheets') or [])}）"
                if isinstance(x.get("sheets"), list) and x.get("sheets")
                else ""
            )
            + (
                f"；解析告警 {len(x.get('parse_warnings') or [])} 条"
                if isinstance(x.get("parse_warnings"), list) and x.get("parse_warnings")
                else ""
            )
            + (
                f"；解析说明 {len(x.get('parse_notes') or [])} 条"
                if isinstance(x.get("parse_notes"), list) and x.get("parse_notes")
                else ""
            )
        )
        for x in input_files
    ) or "- 无"
    rule_catalog_lines = "\n".join(
        (
            f"- [{x.get('rule_group_zh', '规则')}] {x.get('rule_id', '-')}: {x.get('rule_title', '-')}"
            + (f"；{x.get('rule_desc', '')}" if x.get("rule_desc") else "")
        )
        for x in rule_catalog[:80]
    ) or "- 无"

    md_issue_table_lines = [
        "| 严重级别 | 问题类别 | 规则ID | 规则名称 | 问题描述 | 文件 | 工作表 | 行 | 列 |",
        "|---|---|---|---|---|---|---|---:|---|",
    ]
    for issue in issues[:500]:
        md_issue_table_lines.append(
            "| {severity} | {category} | {rule_id} | {rule_title} | {message} | {file} | {sheet} | {row} | {column} |".format(
                severity=severity_label_zh(issue.get("severity")),
                category=category_label_zh(issue.get("category")),
                rule_id=str(issue.get("rule_id", "")).replace("|", "\\|"),
                rule_title=str(issue.get("rule_title", "")).replace("|", "\\|"),
                message=str(issue.get("message_zh", "")).replace("|", "\\|"),
                file=str(issue.get("file", "")).replace("|", "\\|"),
                sheet=str(issue.get("sheet", "")).replace("|", "\\|"),
                row=issue.get("row", ""),
                column=str(issue.get("column", "")).replace("|", "\\|"),
            )
        )

    md_values = {
        "title": "Excel 配置校验报告",
        "generated_at": generated_at_display,
        "total_issues": str(summary["total_issues"]),
        "input_files": input_file_lines,
        "rule_catalog": rule_catalog_lines,
        "severity_counts": severity_lines,
        "category_counts": category_lines,
        "top_rules": top_rule_lines,
        "file_sheet_groups": file_sheet_group_lines,
        "issues_table": "\n".join(md_issue_table_lines),
    }
    md_template = (
        md_template_path.read_text(encoding="utf-8")
        if md_template_path and md_template_path.exists()
        else default_md_template()
    )
    atomic_write_text(report_md_path, render_template(md_template, md_values))

    html_values = {
        "title": "Excel 配置校验报告",
        "generated_at": generated_at_display,
        "total_issues": str(summary["total_issues"]),
        "run_id": str(result_payload["run_id"]),
        "input_hash_short": str(result_payload["input_hash"])[:12],
        "rules_hash_short": str(result_payload["rules_hash"])[:12],
        "severity_json": json.dumps(summary["severity_counts_zh"], ensure_ascii=False, indent=2),
        "issues_json": json.dumps(issues, ensure_ascii=False),
        "inputs_json": json.dumps(input_files, ensure_ascii=False),
        "rule_catalog_json": json.dumps(rule_catalog, ensure_ascii=False),
    }
    html_template = (
        html_template_path.read_text(encoding="utf-8")
        if html_template_path and html_template_path.exists()
        else default_html_template()
    )
    atomic_write_text(report_html_path, render_template(html_template, html_values))

    return result_json_path, issues_csv_path, report_md_path, report_html_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="渲染 JSON/CSV/Markdown/HTML 报告。")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument("--manifest", required=True, help="ingest_manifest.json 路径")
    parser.add_argument("--compiled-rules", required=True, help="compiled_rules.json 路径")
    parser.add_argument("--issue-files", nargs="+", required=True, help="issue JSON 文件列表")
    parser.add_argument("--md-template", default=None, help="可选：Markdown 模板路径")
    parser.add_argument("--html-template", default=None, help="可选：HTML 模板路径")
    parser.add_argument("--message-i18n", default=None, help="可选：message 翻译配置路径")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    out_dir = Path(args.out).resolve()
    manifest_path = Path(args.manifest).resolve()
    compiled_rules_path = Path(args.compiled_rules).resolve()
    issue_files = [Path(x).resolve() for x in args.issue_files]
    md_template_path = Path(args.md_template).resolve() if args.md_template else None
    html_template_path = Path(args.html_template).resolve() if args.html_template else None
    i18n_path = Path(args.message_i18n).resolve() if args.message_i18n else None

    try:
        result_json_path, issues_csv_path, report_md_path, report_html_path = render_reports(
            out_dir=out_dir,
            manifest_path=manifest_path,
            compiled_rules_path=compiled_rules_path,
            issue_files=issue_files,
            md_template_path=md_template_path,
            html_template_path=html_template_path,
            i18n_path=i18n_path,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] render_report 执行失败：{exc}")
        return 1

    print(f"[成功] result.json：{result_json_path}")
    print(f"[成功] issues.csv：{issues_csv_path}")
    print(f"[成功] report.md：{report_md_path}")
    print(f"[成功] report.html：{report_html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
