"""规则编译与校验 — 将 rules.json 编译为 compiled_rules.json。

由 run_validator.py 内部调用，也可独立执行。
输入: rules.json（原始规则定义）
输出: compiled_rules.json（编译后规则 + 摘要统计）
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from common import atomic_write_json, file_sha256, utc_now_iso


RULE_KEYS = (
    "schema_rules",
    "range_rules",
    "row_rules",
    "relation_rules",
    "global_rules",
)


def dataset_ids_from_rules(rules: dict[str, Any]) -> set[str]:
    datasets = rules.get("datasets", {})
    result: set[str] = set()
    if isinstance(datasets, dict):
        result.update(str(k) for k in datasets.keys())
    elif isinstance(datasets, list):
        for item in datasets:
            if isinstance(item, dict):
                ds = item.get("id") or item.get("name")
                if ds:
                    result.add(str(ds))
    return result


def extract_relation_dataset_refs(relation_rule: dict[str, Any]) -> tuple[str | None, str | None]:
    left = (
        relation_rule.get("source_dataset")
        or relation_rule.get("from_dataset")
        or relation_rule.get("left_dataset")
    )
    right = (
        relation_rule.get("target_dataset")
        or relation_rule.get("to_dataset")
        or relation_rule.get("right_dataset")
    )
    return (str(left) if left is not None else None, str(right) if right is not None else None)


def extract_dataset_ref(rule: dict[str, Any]) -> str | None:
    ds = rule.get("dataset") or rule.get("dataset_id") or rule.get("source_dataset")
    if ds is None:
        return None
    return str(ds)


def validate_rules(rules: dict[str, Any], rule_set: str | None) -> list[str]:
    errors: list[str] = []
    if not isinstance(rules, dict):
        return ["rules.json 顶层必须是 JSON 对象"]

    if "datasets" not in rules:
        errors.append("缺少必需顶层字段：datasets")

    if not any(rules.get(key) for key in RULE_KEYS):
        errors.append("至少需要提供一种规则集合（schema/range/row/relation/global）")

    datasets = dataset_ids_from_rules(rules)
    if not datasets:
        errors.append("datasets 至少需要定义一个数据集 id")

    for group_key in ("schema_rules", "range_rules", "row_rules"):
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
    if not rule_set:
        return dict(raw_rules)
    rule_sets = raw_rules.get("rule_sets")
    if not isinstance(rule_sets, dict):
        return dict(raw_rules)
    selected_raw = rule_sets.get(rule_set)
    if not isinstance(selected_raw, list):
        return dict(raw_rules)
    selected_ids = {str(x) for x in selected_raw}

    out = dict(raw_rules)
    referenced_datasets: set[str] = set()
    for key in RULE_KEYS:
        items = raw_rules.get(key, [])
        if not isinstance(items, list):
            out[key] = []
            continue
        selected_items = [
            item
            for item in items
            if isinstance(item, dict) and str(item.get("rule_id", "")) in selected_ids
        ]
        out[key] = selected_items

        if key in {"schema_rules", "range_rules", "row_rules"}:
            for item in selected_items:
                ds = item.get("dataset") or item.get("dataset_id")
                if ds:
                    referenced_datasets.add(str(ds))
        if key == "relation_rules":
            for item in selected_items:
                source = item.get("source_dataset") or item.get("from_dataset") or item.get("left_dataset")
                target = item.get("target_dataset") or item.get("to_dataset") or item.get("right_dataset")
                if source:
                    referenced_datasets.add(str(source))
                if target:
                    referenced_datasets.add(str(target))

    datasets = raw_rules.get("datasets", {})
    if isinstance(datasets, dict):
        out["datasets"] = {k: v for k, v in datasets.items() if str(k) in referenced_datasets}
    elif isinstance(datasets, list):
        filtered: list[dict[str, Any]] = []
        for item in datasets:
            if not isinstance(item, dict):
                continue
            ds_id = item.get("id") or item.get("name")
            if ds_id and str(ds_id) in referenced_datasets:
                filtered.append(item)
        out["datasets"] = filtered
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
