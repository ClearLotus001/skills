# -*- coding: utf-8 -*-
"""数据集解析器。

根据 rules.json 中的 datasets 配置，在 ingest_manifest 中定位对应的
文件和工作表入口，解析失败时自动写入 issue。
"""
from __future__ import annotations

from typing import Any

from validation_common import find_dataset_sheet, make_dataset_missing_issue


def resolve_dataset(
    manifest: dict[str, Any],
    dataset_name: str,
    datasets_cfg: dict[str, Any],
    rule_id: str,
    severity: str,
    issues: list[dict[str, Any]],
    role_name: str = "",
) -> dict[str, Any] | None:
    """解析并返回数据集，失败时写入 issue。"""
    cfg = datasets_cfg.get(dataset_name)
    if not cfg:
        return None

    entry, _ = find_dataset_sheet(manifest, cfg)
    if entry:
        return entry

    file_text = str(cfg.get("file") or cfg.get("file_pattern") or "")
    sheet_text = str(cfg.get("sheet", ""))
    issues.append(
        make_dataset_missing_issue(
            dataset_name=dataset_name,
            file_text=file_text,
            sheet_text=sheet_text,
            rule_id=rule_id,
            severity=severity,
            role_name=role_name,
        )
    )
    return None


def build_dataset_lookup(
    manifest: dict[str, Any],
    datasets_cfg: dict[str, Any],
    issues: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """批量解析全部数据集。"""
    lookup = {}
    for ds_name in datasets_cfg.keys():
        entry = resolve_dataset(manifest, ds_name, datasets_cfg, "DATASET_MAPPING", "error", issues)
        if entry:
            lookup[ds_name] = entry
    return lookup
