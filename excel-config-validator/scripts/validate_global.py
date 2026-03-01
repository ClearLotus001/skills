"""全局一致性校验 — 检查规则定义的全局约束（如 rule_id 唯一性）。

由 run_validator.py 内部调用，也可独立执行。
输入: compiled_rules.json
输出: _stages/global_issues.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from common import atomic_write_json, severity_rank, utc_now_iso
from validation_common import make_exception_issue, make_issue


def iter_rule_items(rules: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    out: list[tuple[str, dict[str, Any]]] = []
    for key in ("schema_rules", "range_rules", "row_rules", "relation_rules", "global_rules"):
        values = rules.get(key, [])
        if not isinstance(values, list):
            continue
        for item in values:
            if isinstance(item, dict):
                out.append((key, item))
    return out


def validate_global(compiled_path: Path, out_dir: Path) -> Path:
    compiled = json.loads(compiled_path.read_text(encoding="utf-8"))
    rules = compiled["rules"]
    issues: list[dict[str, Any]] = []

    try:
        seen_rule_ids: dict[str, str] = {}
        for group, item in iter_rule_items(rules):
            rule_id = str(item.get("rule_id", "")).strip()
            if not rule_id:
                continue
            if rule_id in seen_rule_ids and seen_rule_ids[rule_id] != group:
                issues.append(
                    make_issue(
                        category="global",
                        rule_id="GLOBAL_DUPLICATE_RULE_ID",
                        severity="error",
                        message=f"规则 ID '{rule_id}' 在多个规则组中重复出现",
                        file_name="",
                        sheet="",
                        row=0,
                        column="",
                        expected="rule_id 在规则组间保持唯一",
                        actual=f"在 {seen_rule_ids[rule_id]} 与 {group} 中重复",
                    )
                )
            else:
                seen_rule_ids[rule_id] = group
    except Exception as exc:  # noqa: BLE001
        issues.append(
            make_exception_issue(
                category="global",
                rule_id="GLOBAL_CHECK",
                exc=exc,
                context="全局校验",
            )
        )

    issues.sort(
        key=lambda x: (
            severity_rank(str(x.get("severity", "info"))),
            str(x.get("rule_id", "")),
        )
    )

    result = {
        "stage": "global",
        "stage_zh": "全局校验",
        "generated_at": utc_now_iso(),
        "issue_count": len(issues),
        "issues": issues,
    }
    out_path = out_dir / "global_issues.json"
    atomic_write_json(out_path, result)
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="执行全局一致性校验。")
    parser.add_argument("--compiled-rules", required=True, help="compiled_rules.json 路径")
    parser.add_argument("--out", required=True, help="输出目录")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    compiled_path = Path(args.compiled_rules).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        out_path = validate_global(compiled_path, out_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] validate_global 执行失败：{exc}")
        return 1
    print(f"[成功] 全局校验结果：{out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
