# -*- coding: utf-8 -*-
"""规则编译与校验。

读取原始 rules.json，执行结构校验、check 类型验证和 rule_set 过滤，
输出 compiled_rules.json 供后续校验阶段使用。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

# 确保脚本在任意工作目录下都能导入同级模块
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import atomic_write_json, file_sha256, utc_now_iso


RULE_KEYS = (
    "schema_rules",
    "range_rules",
    "row_rules",
    "relation_rules",
    "aggregate_rules",
    "global_rules",
)

SUPPORTED_CHECK_TYPES = {
    "required", "string", "numeric", "min_digits", "increasing", "unique",
    "date", "datetime_format", "max_length", "min_length", "regex",
    "enum", "whitelist", "positive", "non_negative", "conditional_required",
}


def dataset_ids_from_rules(rules: dict[str, Any]) -> set[str]:
    datasets = rules.get("datasets", {})
    result: set[str] = set()
    if isinstance(datasets, dict):
        result.update(str(k) for k in datasets.keys())
    return result


def extract_relation_dataset_refs(relation_rule: dict[str, Any]) -> tuple[str | None, str | None]:
    left = relation_rule.get("source_dataset")
    right = relation_rule.get("target_dataset")
    return (str(left) if left is not None else None, str(right) if right is not None else None)


def extract_dataset_ref(rule: dict[str, Any]) -> str | None:
    ds = rule.get("dataset")
    if ds is None:
        return None
    return str(ds)


def _validate_schema_check_types(rule: dict[str, Any], idx: int, errors: list[str]) -> None:
    """校验 schema_rules 的 check 类型是否合法。"""
    # 单 check 形式
    check = str(rule.get("check", "")).strip().lower()
    if check and check not in SUPPORTED_CHECK_TYPES:
        rid = rule.get("rule_id", f"schema_rules[{idx}]")
        errors.append(f"schema_rules '{rid}' 的 check 类型 '{check}' 不受支持，"
                       f"可用类型：{', '.join(sorted(SUPPORTED_CHECK_TYPES))}")
    # checks 列表形式
    checks = rule.get("checks")
    if isinstance(checks, list):
        for ci, c in enumerate(checks):
            ct = ""
            if isinstance(c, str):
                ct = c.strip().lower()
            elif isinstance(c, dict):
                ct = str(c.get("type", "")).strip().lower()
            if ct and ct not in SUPPORTED_CHECK_TYPES:
                rid = rule.get("rule_id", f"schema_rules[{idx}]")
                errors.append(f"schema_rules '{rid}' 的 checks[{ci}] 类型 '{ct}' 不受支持")


def validate_rules(rules: dict[str, Any], rule_set: str | None) -> list[str]:
    errors: list[str] = []
    if not isinstance(rules, dict):
        return ["rules.json 顶层必须是 JSON 对象"]

    if "datasets" not in rules:
        errors.append("缺少必需顶层字段：datasets")

    if not any(rules.get(key) for key in RULE_KEYS):
        errors.append("至少需要提供一种规则集合（schema/range/row/relation/aggregate/global）")

    datasets = dataset_ids_from_rules(rules)
    if not datasets:
        errors.append("datasets 至少需要定义一个数据集 id")

    for group_key in ("schema_rules", "range_rules", "row_rules", "aggregate_rules"):
        group_rules = rules.get(group_key, [])
        if not isinstance(group_rules, list):
            continue
        for idx, item in enumerate(group_rules):
            if not isinstance(item, dict):
                errors.append(f"{group_key}[{idx}] 必须是对象")
                continue
            ds = extract_dataset_ref(item)
            if ds and ds not in datasets:
                errors.append(f"{group_key}[{idx}] 引用了未知数据集 '{ds}'")

            # schema_rules：校验 check 类型
            if group_key == "schema_rules":
                _validate_schema_check_types(item, idx, errors)

    relation_rules = rules.get("relation_rules", [])
    if isinstance(relation_rules, list):
        for idx, relation in enumerate(relation_rules):
            if not isinstance(relation, dict):
                errors.append(f"relation_rules[{idx}] 必须是对象")
                continue
            left, right = extract_relation_dataset_refs(relation)
            if left and left not in datasets:
                errors.append(f"relation_rules[{idx}] 引用了未知源数据集 '{left}'")
            if right and right not in datasets:
                errors.append(f"relation_rules[{idx}] 引用了未知目标数据集 '{right}'")

    if rule_set:
        rule_sets = rules.get("rule_sets")
        if not isinstance(rule_sets, dict):
            errors.append("已指定 rule_set，但 rule_sets 缺失或不是对象")
        elif rule_set not in rule_sets:
            errors.append(f"rule_sets 中不存在 rule_set '{rule_set}'")
        else:
            selected = rule_sets.get(rule_set)
            if not isinstance(selected, list):
                errors.append(f"rule_sets['{rule_set}'] 必须是 rule_id 列表")
            else:
                declared_ids = {str(x) for x in selected}
                all_rule_ids: set[str] = set()
                for key in RULE_KEYS:
                    items = rules.get(key, [])
                    if not isinstance(items, list):
                        continue
                    for item in items:
                        if isinstance(item, dict) and item.get("rule_id"):
                            all_rule_ids.add(str(item.get("rule_id")))
                missing_ids = sorted(x for x in declared_ids if x not in all_rule_ids)
                if missing_ids:
                    errors.append(
                        f"rule_set '{rule_set}' 中存在未定义 rule_id：{', '.join(missing_ids[:20])}"
                    )

    return errors


def select_rules(raw_rules: dict[str, Any], rule_set: str | None) -> dict[str, Any]:
    out = dict(raw_rules)

    # 过滤 enabled=false 的规则
    for key in RULE_KEYS:
        items = out.get(key, [])
        if not isinstance(items, list):
            continue
        out[key] = [
            item for item in items
            if isinstance(item, dict) and item.get("enabled", True)
        ]

    if not rule_set:
        return out

    rule_sets = raw_rules.get("rule_sets")
    if not isinstance(rule_sets, dict):
        return out
    selected_raw = rule_sets.get(rule_set)
    if not isinstance(selected_raw, list):
        return out
    selected_ids = {str(x) for x in selected_raw}

    referenced_datasets: set[str] = set()
    for key in RULE_KEYS:
        items = out.get(key, [])
        if not isinstance(items, list):
            out[key] = []
            continue
        selected_items = [
            item
            for item in items
            if isinstance(item, dict) and str(item.get("rule_id", "")) in selected_ids
        ]
        out[key] = selected_items

        if key in {"schema_rules", "range_rules", "row_rules", "aggregate_rules"}:
            for item in selected_items:
                ds = item.get("dataset")
                if ds:
                    referenced_datasets.add(str(ds))
        if key == "relation_rules":
            for item in selected_items:
                source = item.get("source_dataset")
                target = item.get("target_dataset")
                if source:
                    referenced_datasets.add(str(source))
                if target:
                    referenced_datasets.add(str(target))

    datasets = raw_rules.get("datasets", {})
    if isinstance(datasets, dict):
        out["datasets"] = {k: v for k, v in datasets.items() if str(k) in referenced_datasets}
    return out


def summarize_rules(rules: dict[str, Any]) -> dict[str, Any]:
    datasets = dataset_ids_from_rules(rules)
    counts = {key: len(rules.get(key, []) or []) for key in RULE_KEYS}
    return {
        "generated_at": utc_now_iso(),
        "dataset_count": len(datasets),
        "datasets": sorted(datasets),
        "rule_counts": counts,
        "rule_sets": sorted((rules.get("rule_sets") or {}).keys()),
    }


def compile_rules(rules_path: Path, out_dir: Path, rule_set: str | None = None) -> tuple[Path, str]:
    raw = json.loads(rules_path.read_text(encoding="utf-8"))
    errors = validate_rules(raw, rule_set)
    if errors:
        raise ValueError("\n".join(errors))

    rules_hash = file_sha256(rules_path)
    selected_rules = select_rules(raw, rule_set)
    summary = summarize_rules(selected_rules)
    summary["selected_rule_set"] = rule_set
    compiled = {
        "compiled_at": utc_now_iso(),
        "rules_hash": rules_hash,
        "selected_rule_set": rule_set,
        "summary": summary,
        "rules": selected_rules,
    }

    compiled_path = out_dir / "compiled_rules.json"
    atomic_write_json(compiled_path, compiled)
    return compiled_path, rules_hash


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="编译并校验 excel-config-validator 的 rules.json。")
    parser.add_argument("--rules", required=True, help="rules.json 文件路径")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument("--rule-set", default=None, help="可选：规则分组 key")
    return parser


def main() -> int:
    """规则编译命令行入口，返回退出码。"""
    args = build_parser().parse_args()
    rules_path = Path(args.rules).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        compiled_path, _ = compile_rules(rules_path, out_dir, args.rule_set)
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] compile_rules 执行失败：{exc}")
        return 1

    print(f"[成功] 规则编译输出：{compiled_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
