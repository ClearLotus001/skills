# -*- coding: utf-8 -*-
"""单表局部校验（schema/range/row/aggregate）。

对每个数据集执行结构校验、范围校验、行表达式校验和聚合校验，
输出 local_issues.json。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# 确保脚本在任意工作目录下都能导入同级模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import atomic_write_json, severity_rank, utc_now_iso
from dataset_resolver import build_dataset_lookup
from local_rule_engine import (
    normalize_checks,
    validate_aggregate_rules,
    validate_range_rules,
    validate_row_rules,
    validate_rule_on_rows,
)
from validation_common import (
    dataset_configs,
    iter_rows_from_entry,
    make_exception_issue,
    make_issue,
)


def validate_schema_rules(
    *,
    rules: dict[str, Any],
    dataset_sheet_lookup: dict[str, dict[str, Any]],
    issues: list[dict[str, Any]],
) -> None:
    """对每个数据集执行 schema 结构校验（必填、类型、唯一性等）。"""
    schema_rules = rules.get("schema_rules", [])
    if not isinstance(schema_rules, list):
        return

    for idx, rule in enumerate(schema_rules):
        if not isinstance(rule, dict):
            continue
        if not rule.get("enabled", True):
            continue

        dataset = str(rule.get("dataset", "")).strip()
        column = str(rule.get("column", "")).strip()
        rule_id = str(rule.get("rule_id", f"SCHEMA_RULE_{idx}"))
        default_severity = str(rule.get("severity", "error"))
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
                issues.append(
                    make_issue(
                        category="local",
                        rule_id=rule_id,
                        severity=default_severity,
                        message=f"工作表 '{sheet}' 缺少必需列 '{column}'",
                        file_name=file_name,
                        sheet=sheet,
                        row=1,
                        column=column,
                        expected=f"列 '{column}' 存在",
                        actual="列缺失",
                    )
                )
                continue

            checks = normalize_checks(rule)
            if not checks:
                continue

            for check in checks:
                chunk_state: dict[str, Any] = {}
                for chunk in iter_rows_from_entry(entry):
                    validate_rule_on_rows(
                        issues=issues,
                        file_name=file_name,
                        sheet=sheet,
                        column=column,
                        rows=chunk,
                        check=check,
                        default_rule_id=rule_id,
                        default_severity=default_severity,
                        chunk_state=chunk_state,
                    )
        except Exception as exc:  # noqa: BLE001
            issues.append(
                make_exception_issue(
                    category="local",
                    rule_id=rule_id,
                    exc=exc,
                    file_name=str(dataset_sheet_lookup.get(dataset, {}).get("file", "")),
                    sheet=str(dataset_sheet_lookup.get(dataset, {}).get("sheet", "")),
                    context="schema_rules 执行",
                )
            )


def _enrich_issues_with_file_identity(
    issues: list[dict[str, Any]],
    dataset_sheet_lookup: dict[str, dict[str, Any]],
) -> None:
    """通过反查数据集映射补齐 file_path 与 file_sha256。"""
    # 构建 (file_name, sheet_name) -> (path, sha256) 映射
    identity_map: dict[tuple[str, str], tuple[str, str]] = {}
    for entry in dataset_sheet_lookup.values():
        if not isinstance(entry, dict):
            continue
        fname = str(entry.get("file", ""))
        sname = str(entry.get("sheet", ""))
        fpath = str(entry.get("path", ""))
        fsha = str(entry.get("sha256", ""))
        if fname:
            identity_map[(fname, sname)] = (fpath, fsha)

    for issue in issues:
        if issue.get("file_path") or issue.get("file_sha256"):
            continue
        key = (str(issue.get("file", "")), str(issue.get("sheet", "")))
        if key in identity_map:
            fpath, fsha = identity_map[key]
            if fpath:
                issue["file_path"] = fpath
            if fsha:
                issue["file_sha256"] = fsha


def validate_local(compiled_path: Path, manifest_path: Path, out_dir: Path) -> Path:
    """执行局部校验并输出 local_issues.json。"""
    compiled = json.loads(compiled_path.read_text(encoding="utf-8"))
    rules = compiled.get("rules", {})
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    issues: list[dict[str, Any]] = []
    datasets = dataset_configs(rules)
    dataset_sheet_lookup = build_dataset_lookup(manifest, datasets, issues)

    validate_schema_rules(rules=rules, dataset_sheet_lookup=dataset_sheet_lookup, issues=issues)
    validate_range_rules(rules=rules, dataset_sheet_lookup=dataset_sheet_lookup, issues=issues)
    validate_row_rules(rules=rules, dataset_sheet_lookup=dataset_sheet_lookup, issues=issues)
    validate_aggregate_rules(rules=rules, dataset_sheet_lookup=dataset_sheet_lookup, issues=issues)

    # 回填 issue 的完整路径与文件指纹
    _enrich_issues_with_file_identity(issues, dataset_sheet_lookup)

    issues.sort(
        key=lambda x: (
            severity_rank(str(x.get("severity", "info"))),
            str(x.get("rule_id", "")),
            str(x.get("file", "")),
            str(x.get("sheet", "")),
            int(x.get("row", 0) or 0),
        )
    )

    result = {
        "stage": "local",
        "stage_zh": "局部校验",
        "generated_at": utc_now_iso(),
        "issue_count": len(issues),
        "issues": issues,
    }
    out_path = out_dir / "local_issues.json"
    atomic_write_json(out_path, result)
    return out_path


def build_parser() -> argparse.ArgumentParser:
    """构建命令行参数解析器。"""
    parser = argparse.ArgumentParser(description="执行局部结构、行级、范围与表达式校验。")
    parser.add_argument("--compiled-rules", required=True, help="compiled_rules.json 路径")
    parser.add_argument("--manifest", required=True, help="ingest_manifest.json 路径")
    parser.add_argument("--out", required=True, help="输出目录")
    return parser


def main() -> int:
    """局部校验命令行入口，返回退出码。"""
    args = build_parser().parse_args()
    compiled_path = Path(args.compiled_rules).resolve()
    manifest_path = Path(args.manifest).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        out_path = validate_local(compiled_path, manifest_path, out_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] validate_local 执行失败：{exc}")
        return 1
    print(f"[成功] 局部校验结果：{out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
