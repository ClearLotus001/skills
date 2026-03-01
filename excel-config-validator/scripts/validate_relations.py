"""跨表关联规则校验 — 执行 relation_rules（fk_exists、set_equal）。

由 run_validator.py 内部调用，也可独立执行。
输入: compiled_rules.json、ingest_manifest.json
输出: _stages/relation_issues.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from common import atomic_write_json, severity_rank, utc_now_iso
from validation_common import (
    canonical_key,
    dataset_configs,
    find_dataset_sheet,
    iter_rows_from_entry,
    make_exception_issue,
    make_issue,
    rows_from_entry,
    value_text,
)


def relation_source_target(relation: dict[str, Any]) -> tuple[str, str]:
    source = str(
        relation.get("source_dataset")
        or relation.get("from_dataset")
        or relation.get("left_dataset")
        or ""
    )
    target = str(
        relation.get("target_dataset")
        or relation.get("to_dataset")
        or relation.get("right_dataset")
        or ""
    )
    return source, target


def relation_keys(relation: dict[str, Any]) -> tuple[str, str]:
    source_key = str(
        relation.get("source_key")
        or relation.get("from_key")
        or relation.get("left_key")
        or ""
    )
    target_key = str(
        relation.get("target_key")
        or relation.get("to_key")
        or relation.get("right_key")
        or ""
    )
    return source_key, target_key


def resolve_dataset_entry(
    *,
    manifest: dict[str, Any],
    dataset_name: str,
    dataset_cfg: dict[str, Any],
    rule_id: str,
    severity: str,
    role_name: str,
    issues: list[dict[str, Any]],
) -> dict[str, Any] | None:
    entry, reason = find_dataset_sheet(manifest, dataset_cfg)
    if entry is not None:
        return entry

    file_text = str(dataset_cfg.get("file") or dataset_cfg.get("file_pattern") or "")
    sheet_text = str(dataset_cfg.get("sheet", ""))
    if reason == "file_missing":
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity=severity,
                message=f"数据集 '{dataset_name}' 的{role_name}文件 '{file_text}' 未找到",
                file_name=file_text,
                sheet=sheet_text,
                row=0,
                column="",
                expected="文件存在",
                actual="文件缺失",
            )
        )
    else:
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity=severity,
                message=f"数据集 '{dataset_name}' 的{role_name}工作表 '{sheet_text}' 未找到",
                file_name=file_text,
                sheet=sheet_text,
                row=0,
                column="",
                expected="工作表存在",
                actual="工作表缺失",
            )
        )
    return None


def table_key_ref(file_name: str, sheet: str, column: str) -> str:
    return f"{file_name}/{sheet}.{column}"


def append_relation_key_issues(
    *,
    relation: dict[str, Any],
    source_entry: dict[str, Any],
    target_entry: dict[str, Any],
    source_key: str,
    target_key: str,
    rule_id: str,
    severity: str,
    issues: list[dict[str, Any]],
) -> None:
    mode = str(relation.get("mode") or relation.get("relation_type") or "fk_exists").strip().lower()
    allow_source_empty = bool(relation.get("allow_source_empty", False))
    supported_modes = {"fk_exists", "set_equal", "equal_set", "same_set"}

    source_headers = [str(x) for x in source_entry.get("headers", [])]
    target_headers = [str(x) for x in target_entry.get("headers", [])]
    source_rows = rows_from_entry(source_entry)
    target_rows = rows_from_entry(target_entry)
    source_file_name = str(source_entry.get("file", ""))
    source_sheet_name = str(source_entry.get("sheet", ""))
    target_file_name = str(target_entry.get("file", ""))
    target_sheet_name = str(target_entry.get("sheet", ""))

    if mode not in supported_modes:
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity="error",
                message=f"不支持的关联模式 '{mode}'",
                file_name=source_file_name,
                sheet=source_sheet_name,
                row=0,
                column="",
                expected="fk_exists 或 set_equal",
                actual=mode,
            )
        )
        return

    if not source_key or not target_key:
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity=severity,
                message="关联规则缺少 source_key 或 target_key",
                file_name=source_file_name,
                sheet=source_sheet_name,
                row=0,
                column="",
                expected="source_key/target_key 均存在",
                actual="键配置缺失",
            )
        )
        return

    if source_key not in source_headers:
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity=severity,
                message=f"源数据集列 '{source_key}' 不存在",
                file_name=source_file_name,
                sheet=source_sheet_name,
                row=1,
                column=source_key,
                expected="源键列存在",
                actual="列缺失",
            )
        )
        return

    if target_key not in target_headers:
        issues.append(
            make_issue(
                category="relation",
                rule_id=rule_id,
                severity=severity,
                message=f"目标数据集列 '{target_key}' 不存在",
                file_name=target_file_name,
                sheet=target_sheet_name,
                row=1,
                column=target_key,
                expected="目标键列存在",
                actual="列缺失",
            )
        )
        return

    source_ref = table_key_ref(source_file_name, source_sheet_name, source_key)
    target_ref = table_key_ref(target_file_name, target_sheet_name, target_key)

    target_key_set: set[str] = set()
    for row_item in target_rows:
        if not isinstance(row_item, dict):
            continue
        values = row_item.get("values", {})
        if not isinstance(values, dict):
            continue
        k = canonical_key(values.get(target_key))
        if k:
            target_key_set.add(k)

    if mode in {"set_equal", "equal_set", "same_set"}:
        source_key_set: set[str] = set()
        for row_item in source_rows:
            if not isinstance(row_item, dict):
                continue
            values = row_item.get("values", {})
            if not isinstance(values, dict):
                continue
            k = canonical_key(values.get(source_key))
            if k:
                source_key_set.add(k)

        missing_in_target = sorted(source_key_set - target_key_set)
        missing_in_source = sorted(target_key_set - source_key_set)

        if missing_in_target:
            issues.append(
                make_issue(
                    category="relation",
                    rule_id=rule_id,
                    severity=severity,
                    message=f"源键集合中有 {len(missing_in_target)} 个值未在目标键集合中出现",
                    file_name=source_file_name,
                    sheet=source_sheet_name,
                    row=0,
                    column=source_key,
                    expected=f"集合与目标一致（{target_ref}）",
                    actual=f"缺失示例: {', '.join(missing_in_target[:5])}",
                )
            )
        if missing_in_source:
            issues.append(
                make_issue(
                    category="relation",
                    rule_id=rule_id,
                    severity=severity,
                    message=f"目标键集合中有 {len(missing_in_source)} 个值未在源键集合中出现",
                    file_name=target_file_name,
                    sheet=target_sheet_name,
                    row=0,
                    column=target_key,
                    expected=f"集合与源一致（{source_ref}）",
                    actual=f"缺失示例: {', '.join(missing_in_source[:5])}",
                )
            )
        return

    for row_item in source_rows:
        if not isinstance(row_item, dict):
            continue
        row_num = int(row_item.get("row", 0) or 0)
        values = row_item.get("values", {})
        if not isinstance(values, dict):
            continue

        raw_source_value = values.get(source_key)
        source_value = canonical_key(raw_source_value)
        if not source_value:
            if not allow_source_empty:
                issues.append(
                    make_issue(
                        category="relation",
                        rule_id=rule_id,
                        severity=severity,
                        message=f"关联键 '{source_key}' 不能为空",
                        file_name=source_file_name,
                        sheet=source_sheet_name,
                        row=row_num,
                        column=source_key,
                        expected=f"非空且可在 {target_ref} 中找到",
                        actual="空值",
                    )
                )
            continue

        if source_value not in target_key_set:
            show_value = value_text(raw_source_value)
            issues.append(
                make_issue(
                    category="relation",
                    rule_id=rule_id,
                    severity=severity,
                    message=f"关联键值 '{show_value}' 未在目标键 '{target_ref}' 中找到",
                    file_name=source_file_name,
                    sheet=source_sheet_name,
                    row=row_num,
                    column=source_key,
                    expected=f"存在于 {target_ref}",
                    actual=show_value,
                )
            )


def validate_relations(compiled_path: Path, manifest_path: Path, out_dir: Path) -> Path:
    compiled = json.loads(compiled_path.read_text(encoding="utf-8"))
    rules = compiled.get("rules", {})
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    relation_rules = rules.get("relation_rules", [])
    datasets_cfg = dataset_configs(rules)
    known_datasets = set(datasets_cfg.keys())

    issues: list[dict[str, Any]] = []
    if isinstance(relation_rules, list):
        for idx, relation in enumerate(relation_rules):
            if not isinstance(relation, dict):
                continue

            rule_id = str(relation.get("rule_id", f"RELATION_RULE_{idx}"))
            severity = str(relation.get("severity", "error"))

            try:
                source, target = relation_source_target(relation)
                source_key, target_key = relation_keys(relation)

                if source and source not in known_datasets:
                    issues.append(
                        make_issue(
                            category="relation",
                            rule_id=rule_id,
                            severity="error",
                            message=f"关联规则中的源数据集 '{source}' 未定义",
                            file_name="",
                            sheet="",
                            row=0,
                            column="",
                            expected="已定义源数据集",
                            actual="源数据集未定义",
                        )
                    )
                    continue

                if target and target not in known_datasets:
                    issues.append(
                        make_issue(
                            category="relation",
                            rule_id=rule_id,
                            severity="error",
                            message=f"关联规则中的目标数据集 '{target}' 未定义",
                            file_name="",
                            sheet="",
                            row=0,
                            column="",
                            expected="已定义目标数据集",
                            actual="目标数据集未定义",
                        )
                    )
                    continue

                source_cfg = datasets_cfg.get(source, {}) if source else {}
                target_cfg = datasets_cfg.get(target, {}) if target else {}

                source_entry = resolve_dataset_entry(
                    manifest=manifest,
                    dataset_name=source,
                    dataset_cfg=source_cfg,
                    rule_id=rule_id,
                    severity=severity,
                    role_name="源",
                    issues=issues,
                )
                if source_entry is None:
                    continue

                target_entry = resolve_dataset_entry(
                    manifest=manifest,
                    dataset_name=target,
                    dataset_cfg=target_cfg,
                    rule_id=rule_id,
                    severity=severity,
                    role_name="目标",
                    issues=issues,
                )
                if target_entry is None:
                    continue

                append_relation_key_issues(
                    relation=relation,
                    source_entry=source_entry,
                    target_entry=target_entry,
                    source_key=source_key,
                    target_key=target_key,
                    rule_id=rule_id,
                    severity=severity,
                    issues=issues,
                )
            except Exception as exc:  # noqa: BLE001
                issues.append(
                    make_exception_issue(
                        category="relation",
                        rule_id=rule_id,
                        exc=exc,
                        context="关联校验",
                    )
                )

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
        "stage": "relation",
        "stage_zh": "关联校验",
        "generated_at": utc_now_iso(),
        "issue_count": len(issues),
        "issues": issues,
    }
    out_path = out_dir / "relation_issues.json"
    atomic_write_json(out_path, result)
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="执行关联结构与键值校验。")
    parser.add_argument("--compiled-rules", required=True, help="compiled_rules.json 路径")
    parser.add_argument("--manifest", required=True, help="ingest_manifest.json 路径")
    parser.add_argument("--out", required=True, help="输出目录")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    compiled_path = Path(args.compiled_rules).resolve()
    manifest_path = Path(args.manifest).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        out_path = validate_relations(compiled_path, manifest_path, out_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] validate_relations 执行失败：{exc}")
        return 1
    print(f"[成功] 关联校验结果：{out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
