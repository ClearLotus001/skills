# -*- coding: utf-8 -*-
"""报告渲染与 issue 聚合。

汇总各阶段的 issue 文件，生成 result.json、issues.csv 和 report.html。
包含规则目录构建、issue 本地化、摘要统计、HTML 模板渲染等能力。
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

# 确保脚本在任意工作目录下都能导入同级模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

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


RULE_GROUP_KEYS = ("schema_rules", "range_rules", "row_rules", "relation_rules", "aggregate_rules", "global_rules")


def format_timestamp_display(iso_text: str) -> str:
    """将 ISO 时间戳转为本地显示格式（含时区偏移）。"""
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
    """从 issue JSON 文件中加载 issues 列表。"""
    if not issue_file.exists():
        return []
    data = json.loads(issue_file.read_text(encoding="utf-8"))
    issues = data.get("issues", [])
    return issues if isinstance(issues, list) else []


def rule_group_label_zh(group_key: str) -> str:
    """将规则组键转为中文标签。"""
    table = {
        "schema_rules": "结构规则",
        "range_rules": "范围规则",
        "row_rules": "行规则",
        "relation_rules": "关联规则",
        "aggregate_rules": "聚合规则",
        "global_rules": "全局规则",
    }
    return table.get(group_key, group_key)


def check_label_zh(check: str) -> str:
    """将 check 类型转为中文标签。"""
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
    """根据 schema 规则生成人类可读的期望值描述文本。"""
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
    """返回数据集的可读位置标识（文件名/工作表）。"""
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
    """提取规则中 checks 字段的中文标签文本。"""
    checks = rule.get("checks")
    if isinstance(checks, list):
        labels = []
        for item in checks:
            if isinstance(item, str):
                labels.append(check_label_zh(item))
            elif isinstance(item, dict):
                labels.append(check_label_zh(str(item.get("type") or "")))
        labels = [x for x in labels if x]
        if labels:
            return "、".join(labels)
    check = str(rule.get("check", "")).strip()
    if check:
        return check_label_zh(check)
    return "-"


def infer_rule_title_and_desc(rule: dict[str, Any], group_key: str, ds_cfg: dict[str, dict[str, Any]]) -> tuple[str, str]:
    """推断规则标题和描述，优先使用显式配置，否则按规则组自动生成。"""
    explicit_title = str(rule.get("title") or "").strip()
    explicit_desc = str(rule.get("description") or "").strip()
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
        col = str(rule.get("column", "")).strip()
        ds_loc = dataset_location_text(ds, ds_cfg)
        branches = rule.get("branches")
        if isinstance(branches, list) and branches:
            branch_count = len(branches)
            has_else = bool(str(rule.get("else_assert") or "").strip())
            title = f"{ds_loc} 条件分支校验（{branch_count} 分支{'+ else' if has_else else ''}）"
            desc = str(rule.get("message", "")).strip() or f"{branch_count} 个条件分支"
        else:
            msg = str(rule.get("message", "")).strip()
            if col:
                title = f"{ds_loc}.{col} 行表达式校验"
            else:
                title = f"{ds_loc} 行表达式校验"
            desc = msg or str(rule.get("description", "")).strip()
            if not desc:
                when_expr = str(rule.get("when", "")).strip()
                assert_expr = str(rule.get("assert", "")).strip()
                desc = f"when={when_expr or 'True'}; assert={assert_expr}"
        return title, desc

    if group_key == "relation_rules":
        source = str(rule.get("source_dataset") or "")
        target = str(rule.get("target_dataset") or "")
        source_key = str(rule.get("source_key") or "")
        target_key = str(rule.get("target_key") or "")
        mode = str(rule.get("mode") or "fk_exists")
        source_loc = dataset_location_text(source, ds_cfg)
        target_loc = dataset_location_text(target, ds_cfg)
        title = f"{source_loc}.{source_key} -> {target_loc}.{target_key}"
        desc = (
            f"{source_loc}.{source_key} 与 "
            f"{target_loc}.{target_key} 关联检查（{mode}）"
        )
        return title, desc

    if group_key == "aggregate_rules":
        ds = str(rule.get("dataset", ""))
        col = str(rule.get("column", ""))
        func = str(rule.get("function", "")).strip()
        group_by_col = str(rule.get("group_by", "")).strip()
        ds_loc = dataset_location_text(ds, ds_cfg)
        title = f"{ds_loc}.{col} {func}() 聚合校验"
        group_info = f"（按 '{group_by_col}' 分组）" if group_by_col else ""
        desc = f"{ds_loc} 的列 '{col}' 执行 {func} 聚合校验{group_info}"
        return title, desc

    title = str(rule.get("rule_id", "全局规则"))
    desc = explicit_desc or title
    return title, desc


def build_rule_catalog(compiled_rules: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    """构建规则目录列表和按 rule_id 索引的字典。"""
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
    """从 manifest 中提取输入文件摘要信息。"""
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
                "sha256": str(file_item.get("sha256", "")),
                "extension": str(file_item.get("extension", "")),
                "size_bytes": int(file_item.get("size_bytes", 0) or 0),
                "sheet_count": len(sheets_out),
                "sheets": sheets_out,
                "parse_warnings": file_item.get("parse_warnings", []) if isinstance(file_item.get("parse_warnings", []), list) else [],
                "parse_notes": file_item.get("parse_notes", []) if isinstance(file_item.get("parse_notes", []), list) else [],
            }
        )
    return out


def enrich_issues_with_rule_info(
    issues: list[dict[str, Any]],
    catalog_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """为 issue 补充规则标题、描述和分组信息。"""
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


def localize_issues(issues: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """补齐本地化字段（severity_zh、category_zh、message_zh 等）。"""
    localized: list[dict[str, Any]] = []
    for issue in issues:
        x = dict(issue)
        x["severity"] = severity_key(x.get("severity"))
        x["severity_zh"] = severity_label_zh(x.get("severity"))
        x["category"] = category_key(x.get("category"))
        x["category_zh"] = category_label_zh(x.get("category"))
        x["message"] = str(x.get("message", ""))
        x["message_zh"] = str(x.get("message_zh", "")).strip() or x["message"]
        localized.append(x)
    return localized


def csv_escape(value: Any) -> str:
    """将值转为 CSV 安全的字符串。"""
    if value is None:
        return ""
    return str(value)


def write_issues_csv(path: Path, issues: list[dict[str, Any]]) -> None:
    """将 issue 列表写入中文表头的 CSV 文件。"""
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
        ("file_path", "文件完整路径"),
        ("file_sha256", "文件SHA256"),
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
    """根据 issue 列表构建多维度摘要统计。"""
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
    """使用 {{key}} 占位符替换渲染模板。"""
    output = template
    for key, value in values.items():
        output = output.replace(f"{{{{{key}}}}}", value)
    return output


def default_html_template() -> str:
    """返回内置的默认 HTML 报告模板。"""
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
  <p id="parseWarnSummary">解析告警：统计中...</p>
  <pre>{{severity_json}}</pre>
  <script>
    const issues = {{issues_json}};
    const inputFiles = {{inputs_json}};
    const parseWarnCount = (Array.isArray(inputFiles) ? inputFiles : []).reduce(
      (acc, f) => acc + (Array.isArray(f.parse_warnings) ? f.parse_warnings.length : 0),
      0
    );
    const summaryNode = document.getElementById("parseWarnSummary");
    if (summaryNode) {
      summaryNode.textContent = parseWarnCount > 0
        ? `解析告警：${parseWarnCount} 条（详见完整 report 模板）`
        : "解析告警：无";
    }
    document.body.insertAdjacentHTML(
      "beforeend",
      `<p>已载入问题 ${issues.length} 条，可替换为自定义模板查看完整交互。</p>`
    );
  </script>
</body>
</html>
"""


def _enrich_issues_with_manifest_identity(
    issues: list[dict[str, Any]],
    manifest: dict[str, Any],
) -> None:
    """为缺失文件标识的 issue 回填 file_path / file_sha256。"""
    files = manifest.get("files", [])
    if not isinstance(files, list):
        return

    # 构建 file_name -> (path, sha256) 映射（同名时保留最后一条）
    name_to_identity: dict[str, tuple[str, str]] = {}
    for fi in files:
        if not isinstance(fi, dict):
            continue
        name = str(fi.get("name", ""))
        if name:
            name_to_identity[name] = (
                str(fi.get("path", "")),
                str(fi.get("sha256", "")),
            )

    for issue in issues:
        file_name = str(issue.get("file", ""))
        if not file_name:
            continue
        if not issue.get("file_path") and file_name in name_to_identity:
            issue["file_path"] = name_to_identity[file_name][0]
        if not issue.get("file_sha256") and file_name in name_to_identity:
            issue["file_sha256"] = name_to_identity[file_name][1]


def render_reports(
    out_dir: Path,
    manifest_path: Path,
    compiled_rules_path: Path,
    issue_files: list[Path],
    html_template_path: Path | None = None,
) -> tuple[Path, Path, Path]:
    """汇总 issue 并生成 result.json、issues.csv 和 report.html。"""
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    compiled_rules = json.loads(compiled_rules_path.read_text(encoding="utf-8"))
    input_files = summarize_input_files(manifest)
    rule_catalog, rule_catalog_by_id = build_rule_catalog(compiled_rules)

    issues: list[dict[str, Any]] = []
    for issue_file in issue_files:
        issues.extend(load_issues(issue_file))
    issues = localize_issues(issues)
    issues = enrich_issues_with_rule_info(issues, rule_catalog_by_id)
    _enrich_issues_with_manifest_identity(issues, manifest)
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
    report_html_path = out_dir / "report.html"

    atomic_write_json(result_json_path, result_payload)
    write_issues_csv(issues_csv_path, issues)

    raw_rules = compiled_rules.get("rules", {})
    ds_cfg = dataset_configs(raw_rules) if isinstance(raw_rules, dict) else {}
    rule_sheets = []
    for ds_key, ds_val in ds_cfg.items():
        if isinstance(ds_val, dict) and ds_val.get("file") and ds_val.get("sheet"):
            rule_sheets.append({"file": str(ds_val["file"]), "sheet": str(ds_val["sheet"])})
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
        "rule_sheets_json": json.dumps(rule_sheets, ensure_ascii=False),
    }
    html_template = (
        html_template_path.read_text(encoding="utf-8")
        if html_template_path and html_template_path.exists()
        else default_html_template()
    )
    atomic_write_text(report_html_path, render_template(html_template, html_values))

    return result_json_path, issues_csv_path, report_html_path


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="渲染 JSON/CSV/HTML 报告。")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument("--manifest", required=True, help="ingest_manifest.json 路径")
    parser.add_argument("--compiled-rules", required=True, help="compiled_rules.json 路径")
    parser.add_argument("--issue-files", nargs="+", required=True, help="issue JSON 文件列表")
    parser.add_argument("--html-template", default=None, help="可选：HTML 模板路径")
    return parser


def main() -> int:
    """报告渲染命令行入口，返回退出码。"""
    args = build_parser().parse_args()
    out_dir = Path(args.out).resolve()
    manifest_path = Path(args.manifest).resolve()
    compiled_rules_path = Path(args.compiled_rules).resolve()
    issue_files = [Path(x).resolve() for x in args.issue_files]
    html_template_path = Path(args.html_template).resolve() if args.html_template else None

    try:
        result_json_path, issues_csv_path, report_html_path = render_reports(
            out_dir=out_dir,
            manifest_path=manifest_path,
            compiled_rules_path=compiled_rules_path,
            issue_files=issue_files,
            html_template_path=html_template_path,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] render_report 执行失败：{exc}")
        return 1

    print(f"[成功] result.json：{result_json_path}")
    print(f"[成功] issues.csv：{issues_csv_path}")
    print(f"[成功] report.html：{report_html_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
