# -*- coding: utf-8 -*-
"""XLSX/XLSM 包结构预检。

用于在解析前快速发现压缩包损坏、关键部件缺失和关系目标断链等问题。
检查内容包括 ZIP 完整性、必需 XML 部件、关系文件目标指向等。
"""
from __future__ import annotations

import posixpath
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath
from typing import Any
from zipfile import BadZipFile, ZipFile


REQUIRED_PARTS = (
    "[Content_Types].xml",
    "_rels/.rels",
    "xl/workbook.xml",
)


def _normalize_relationship_target(rel_file: str, target: str) -> str:
    raw = (target or "").split("#", 1)[0].strip()
    if not raw:
        return ""

    if raw.startswith("/"):
        normalized = posixpath.normpath(raw.lstrip("/"))
    else:
        rel_parent = PurePosixPath(rel_file).parent
        base = rel_parent.parent if rel_parent.name == "_rels" else rel_parent
        normalized = posixpath.normpath((base / raw).as_posix())

    normalized = normalized.lstrip("./")
    return normalized


def check_xlsx_package(path: Path) -> dict[str, Any]:
    """检查 xlsx/xlsm 压缩包结构并返回告警、备注与摘要。"""
    warnings: list[str] = []
    notes: list[str] = []
    summary: dict[str, Any] = {
        "zip_entry_count": 0,
        "rels_file_count": 0,
        "rels_relationship_count": 0,
        "rels_missing_target_count": 0,
        "xml_parse_error_count": 0,
        "missing_required_parts_count": 0,
    }

    if not path.exists():
        warnings.append(f"文件不存在：{path}")
        return {"warnings": warnings, "notes": notes, "summary": summary}

    try:
        with ZipFile(path) as zf:
            names = zf.namelist()
            name_set = set(names)
            summary["zip_entry_count"] = len(names)

            broken_member = zf.testzip()
            if broken_member:
                warnings.append(f"ZIP 完整性检查失败，损坏条目：{broken_member}")

            missing_parts = [part for part in REQUIRED_PARTS if part not in name_set]
            summary["missing_required_parts_count"] = len(missing_parts)
            for part in missing_parts:
                warnings.append(f"缺少必需部件：{part}")

            xml_candidates = ["xl/workbook.xml"] + [
                n for n in names
                if n.startswith("xl/worksheets/") and n.endswith(".xml")
            ]
            for member in xml_candidates:
                if member not in name_set:
                    continue
                try:
                    ET.fromstring(zf.read(member))
                except Exception as exc:  # noqa: BLE001
                    summary["xml_parse_error_count"] = int(summary["xml_parse_error_count"]) + 1
                    warnings.append(f"XML 解析失败：{member}（{exc}）")

            rel_files = [n for n in names if n.endswith(".rels")]
            summary["rels_file_count"] = len(rel_files)
            for rel_file in rel_files:
                try:
                    root = ET.fromstring(zf.read(rel_file))
                except Exception as exc:  # noqa: BLE001
                    summary["xml_parse_error_count"] = int(summary["xml_parse_error_count"]) + 1
                    warnings.append(f"关系文件 XML 解析失败：{rel_file}（{exc}）")
                    continue

                for node in root.iter():
                    if not str(node.tag).endswith("Relationship"):
                        continue
                    summary["rels_relationship_count"] = int(summary["rels_relationship_count"]) + 1

                    target_mode = str(node.attrib.get("TargetMode") or "").strip().lower()
                    if target_mode == "external":
                        continue

                    target = str(node.attrib.get("Target") or "").strip()
                    if not target:
                        summary["rels_missing_target_count"] = int(summary["rels_missing_target_count"]) + 1
                        warnings.append(f"关系目标为空：{rel_file}")
                        continue

                    normalized = _normalize_relationship_target(rel_file, target)
                    if not normalized:
                        summary["rels_missing_target_count"] = int(summary["rels_missing_target_count"]) + 1
                        warnings.append(f"关系目标无效：{rel_file} -> {target}")
                        continue

                    if normalized.startswith("../"):
                        summary["rels_missing_target_count"] = int(summary["rels_missing_target_count"]) + 1
                        warnings.append(f"关系目标越界：{rel_file} -> {target}")
                        continue

                    if normalized not in name_set:
                        summary["rels_missing_target_count"] = int(summary["rels_missing_target_count"]) + 1
                        warnings.append(f"关系目标不存在：{rel_file} -> {target}（解析后：{normalized}）")

    except BadZipFile:
        warnings.append("不是有效的 ZIP 压缩包或文件已损坏")
        return {"warnings": warnings, "notes": notes, "summary": summary}
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"包结构检查异常：{exc}")
        return {"warnings": warnings, "notes": notes, "summary": summary}

    notes.append(
        "摘要："
        f"ZIP条目={summary['zip_entry_count']}，"
        f"关系文件={summary['rels_file_count']}，"
        f"关系数={summary['rels_relationship_count']}，"
        f"缺失目标={summary['rels_missing_target_count']}，"
        f"XML解析失败={summary['xml_parse_error_count']}，"
        f"缺失必需部件={summary['missing_required_parts_count']}"
    )
    if not warnings:
        notes.append("包结构预检通过")

    return {"warnings": warnings, "notes": notes, "summary": summary}

