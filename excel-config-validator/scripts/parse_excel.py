"""Excel/CSV 解析引擎 — 解析输入文件并生成 JSONL 行存储。

由 run_validator.py 内部调用，也可独立执行。
输入: Excel(.xlsx/.xls/.xlsm/.xlsb) 或 CSV 文件/目录
输出: ingest_manifest.json（解析清单）、_row_store/*.jsonl（行数据分块）
"""
from __future__ import annotations

import argparse
import csv
import fnmatch
import hashlib
import json
import sys
import warnings
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any
from zipfile import ZipFile

# 确保 scripts/ 目录在导入路径中
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import atomic_write_json, dataset_configs, file_sha256, normalize_path_text, utc_now_iso


SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".xlsm", ".xlsb", ".csv"}
IGNORED_ARTIFACT_NAMES = {
    "issues.csv",
    "issues_raw.csv",
}


def discover_input_files(inputs: Path) -> list[Path]:
    if inputs.is_file():
        ext = inputs.suffix.lower()
        if inputs.name.startswith("~$"):
            return []
        if inputs.name.lower() in IGNORED_ARTIFACT_NAMES:
            return []
        return [inputs] if ext in SUPPORTED_EXTENSIONS else []
    files: list[Path] = []
    for path in inputs.rglob("*"):
        if (
            path.is_file()
            and path.suffix.lower() in SUPPORTED_EXTENSIONS
            and not path.name.startswith("~$")
            and path.name.lower() not in IGNORED_ARTIFACT_NAMES
        ):
            files.append(path)
    return sorted(files)


def json_friendly(value: Any) -> Any:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat(sep=" ")
    return value


def row_to_map(headers: list[str], row_values: list[Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    width = max(len(headers), len(row_values))
    for idx in range(width):
        key = headers[idx] if idx < len(headers) else ""
        key = str(key).strip()
        if not key:
            key = f"_col_{idx + 1}"
        out[key] = json_friendly(row_values[idx] if idx < len(row_values) else None)
    return out


# normalize_path_text and dataset_configs are imported from common


def build_projection_plan(compiled_rules_path: Path | None) -> list[dict[str, Any]]:
    if compiled_rules_path is None or not compiled_rules_path.exists():
        return []

    data = json.loads(compiled_rules_path.read_text(encoding="utf-8"))
    rules = data.get("rules", {})
    if not isinstance(rules, dict):
        return []

    datasets = dataset_configs(rules)
    ds_columns: dict[str, set[str]] = {k: set() for k in datasets.keys()}
    ds_project_all: set[str] = set()

    for key in ("schema_rules", "range_rules"):
        items = rules.get(key, [])
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            ds = str(item.get("dataset", "")).strip()
            col = str(item.get("column", "")).strip()
            if ds and ds in ds_columns and col:
                ds_columns[ds].add(col)

    relation_rules = rules.get("relation_rules", [])
    if isinstance(relation_rules, list):
        for item in relation_rules:
            if not isinstance(item, dict):
                continue
            source_ds = str(item.get("source_dataset") or "").strip()
            target_ds = str(item.get("target_dataset") or "").strip()
            source_key = str(item.get("source_key") or "").strip()
            target_key = str(item.get("target_key") or "").strip()
            if source_ds and source_ds in ds_columns and source_key:
                ds_columns[source_ds].add(source_key)
            if target_ds and target_ds in ds_columns and target_key:
                ds_columns[target_ds].add(target_key)

    row_rules = rules.get("row_rules", [])
    if isinstance(row_rules, list):
        for item in row_rules:
            if not isinstance(item, dict):
                continue
            ds = str(item.get("dataset", "")).strip()
            if ds and ds in ds_columns:
                ds_project_all.add(ds)
                col = str(item.get("column", "")).strip()
                if col:
                    ds_columns[ds].add(col)

    plan: list[dict[str, Any]] = []
    for ds, cfg in datasets.items():
        file_name = str(cfg.get("file", "")).strip()
        file_pattern = str(cfg.get("file_pattern", "")).strip()
        sheet = str(cfg.get("sheet", "")).strip()
        plan.append(
            {
                "dataset": ds,
                "file": file_name,
                "file_pattern": file_pattern,
                "sheet": sheet,
                "project_all": ds in ds_project_all,
                "columns": sorted(ds_columns.get(ds, set())),
            }
        )
    return plan


def projection_item_matches(
    item: dict[str, Any],
    *,
    file_name: str,
    file_path: str,
    sheet: str,
) -> bool:
    expected_file = normalize_path_text(str(item.get("file", "")))
    file_pattern = normalize_path_text(str(item.get("file_pattern", "")))
    expected_sheet = str(item.get("sheet", "")).strip()
    normalized_file_name = normalize_path_text(file_name)
    normalized_file_path = normalize_path_text(file_path)

    if expected_sheet and expected_sheet != sheet:
        return False
    if expected_file:
        return (
            normalized_file_name == expected_file
            or normalized_file_path == expected_file
            or normalized_file_path.endswith(f"/{expected_file}")
        )
    if file_pattern:
        return fnmatch.fnmatch(normalized_file_name, file_pattern) or fnmatch.fnmatch(normalized_file_path, file_pattern)
    return True


def projected_headers_for_sheet(
    plan: list[dict[str, Any]],
    *,
    file_name: str,
    file_path: str,
    sheet: str,
    headers: list[str],
) -> list[str]:
    if not headers:
        return []
    if not plan:
        return list(headers)

    matched = [
        item
        for item in plan
        if projection_item_matches(item, file_name=file_name, file_path=file_path, sheet=sheet)
    ]
    if not matched:
        return []
    if any(bool(item.get("project_all")) for item in matched):
        return list(headers)

    col_set: set[str] = set()
    for item in matched:
        for col in item.get("columns", []) or []:
            x = str(col).strip()
            if x:
                col_set.add(x)
    if not col_set:
        return list(headers)

    filtered = [h for h in headers if h in col_set]
    return filtered or list(headers)


def rows_store_path(out_dir: Path, file_path: Path, sheet_name: str) -> Path:
    key = hashlib.sha1(f"{file_path.as_posix()}::{sheet_name}".encode("utf-8")).hexdigest()[:16]
    safe_sheet = "".join(ch if ch.isalnum() else "_" for ch in sheet_name)[:48] or "sheet"
    store_dir = out_dir / "_row_store"
    store_dir.mkdir(parents=True, exist_ok=True)
    return store_dir / f"{key}_{safe_sheet}.jsonl"


def write_rows_store(
    rows: list[dict[str, Any]],
    projected_headers: list[str] | None,
    output_path: Path,
    chunk_size: int,
) -> int:
    keep_all = projected_headers is None
    selected_headers = projected_headers or []
    selected = set(selected_headers)
    size = max(1, int(chunk_size or 1))
    count = 0
    buffer: list[str] = []
    with output_path.open("w", encoding="utf-8", newline="\n") as f:
        for row in rows:
            if not isinstance(row, dict):
                continue
            row_no = int(row.get("row", 0) or 0)
            values = row.get("values", {})
            if not isinstance(values, dict):
                values = {}
            if keep_all:
                new_values = values
            else:
                new_values = {k: values.get(k) for k in selected_headers if k in values}
            payload = {"row": row_no, "values": new_values}
            buffer.append(json.dumps(payload, ensure_ascii=False))
            count += 1
            if len(buffer) >= size:
                f.write("\n".join(buffer))
                f.write("\n")
                buffer.clear()
        if buffer:
            f.write("\n".join(buffer))
            f.write("\n")
    return count


def scan_xlsx_extlst_sheet_xml(path: Path) -> list[str]:
    out: list[str] = []
    try:
        with ZipFile(path) as zf:
            for name in zf.namelist():
                if not (name.startswith("xl/worksheets/sheet") and name.endswith(".xml")):
                    continue
                data = zf.read(name)
                if b"<extLst" in data:
                    out.append(name.rsplit("/", 1)[-1])
    except Exception:  # noqa: BLE001
        return []
    return out


def read_csv_rows(path: Path, encoding: str) -> tuple[list[str], list[dict[str, Any]]]:
    headers: list[str] = []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding=encoding, newline="") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader, start=1):
            if i == 1:
                headers = [str(v).strip() for v in row]
            else:
                rows.append({"row": i, "values": row_to_map(headers, list(row))})
    return headers, rows


def parse_csv_headers(path: Path) -> tuple[list[str], list[dict[str, Any]], list[str], list[str]]:
    parse_warnings: list[str] = []
    notes: list[str] = []
    encodings = ("utf-8-sig", "gb18030", "utf-16")
    headers: list[str] = []
    rows: list[dict[str, Any]] = []
    used_encoding = ""
    last_error = ""

    for encoding in encodings:
        try:
            headers, rows = read_csv_rows(path, encoding)
            used_encoding = encoding
            break
        except UnicodeDecodeError as exc:
            last_error = str(exc)
            continue
        except Exception as exc:  # noqa: BLE001
            parse_warnings.append(f"CSV 解析失败：{exc}")
            return [], [], parse_warnings, notes

    if not used_encoding:
        parse_warnings.append(
            "CSV 编码无法识别（已尝试 UTF-8/GB18030/UTF-16）"
            + (f"：{last_error}" if last_error else "")
        )
        return [], [], parse_warnings, notes
    if used_encoding != "utf-8-sig":
        notes.append(f"CSV 非 UTF-8 编码，已自动按 {used_encoding} 读取")
    if not headers:
        parse_warnings.append("CSV 为空或缺少表头行")
    return headers, rows, parse_warnings, notes


def parse_xlsx_like(path: Path, data_only: bool) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    sheets: list[dict[str, Any]] = []
    parse_warnings: list[str] = []
    parse_notes: list[str] = []
    try:
        from openpyxl import load_workbook  # type: ignore
    except Exception:  # noqa: BLE001
        return sheets, ["缺少 openpyxl，无法解析 .xlsx/.xlsm 表头"], parse_notes

    unknown_extension_hits = 0
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        wb = load_workbook(path, read_only=True, data_only=data_only)
        try:
            for sheet_name in wb.sheetnames:
                ws = wb[sheet_name]
                headers: list[str] = []
                rows: list[dict[str, Any]] = []
                for row_idx, row in enumerate(ws.iter_rows(values_only=True), start=1):
                    row_values = [json_friendly(v) for v in row]
                    if row_idx == 1:
                        headers = ["" if v is None else str(v).strip() for v in row_values]
                        continue
                    rows.append({"row": row_idx, "values": row_to_map(headers, list(row_values))})
                sheets.append(
                    {
                        "sheet": sheet_name,
                        "headers": headers,
                        "row_count_estimate": len(rows),
                        "rows": rows,
                    }
                )
        finally:
            wb.close()
    for w in caught:
        msg = str(w.message)
        if "Unknown extension is not supported and will be removed" in msg:
            unknown_extension_hits += 1
            continue
        parse_warnings.append(f"openpyxl 告警：{msg}")

    if unknown_extension_hits > 0:
        xml_hits = scan_xlsx_extlst_sheet_xml(path)
        xml_text = f"；涉及: {', '.join(xml_hits[:6])}" if xml_hits else ""
        parse_notes.append(
            "检测到 openpyxl 不支持的工作表扩展（extLst）"
            f"，命中 {unknown_extension_hits} 次。根因通常是 Excel 高级特性扩展节点（如部分条件格式/数据验证/切片器）"
            f"{xml_text}。该扩展会被 openpyxl 忽略，但常规单元格值读取仍可继续。"
        )
    return sheets, parse_warnings, parse_notes


def parse_xls(path: Path) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    sheets: list[dict[str, Any]] = []
    parse_warnings: list[str] = []
    notes: list[str] = []
    try:
        import xlrd  # type: ignore
    except Exception:  # noqa: BLE001
        return sheets, ["缺少 xlrd，无法解析 .xls 表头"], notes

    book = xlrd.open_workbook(path.as_posix(), on_demand=True)
    try:
        for sheet_name in book.sheet_names():
            sh = book.sheet_by_name(sheet_name)
            headers: list[str] = []
            rows: list[dict[str, Any]] = []
            if sh.nrows > 0:
                headers = [str(sh.cell_value(0, c)).strip() for c in range(sh.ncols)]
                for row_idx in range(1, sh.nrows):
                    row_values = [json_friendly(sh.cell_value(row_idx, c)) for c in range(sh.ncols)]
                    rows.append({"row": row_idx + 1, "values": row_to_map(headers, row_values)})
            sheets.append(
                {
                    "sheet": sheet_name,
                    "headers": headers,
                    "row_count_estimate": len(rows),
                    "rows": rows,
                }
            )
    finally:
        book.release_resources()
    return sheets, parse_warnings, notes


def parse_xlsb(path: Path) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    sheets: list[dict[str, Any]] = []
    parse_warnings: list[str] = []
    notes: list[str] = []
    try:
        from pyxlsb import open_workbook  # type: ignore
    except Exception:  # noqa: BLE001
        return sheets, ["缺少 pyxlsb，无法解析 .xlsb 表头"], notes

    with open_workbook(path.as_posix()) as wb:
        for sheet_name in wb.sheets:
            headers: list[str] = []
            rows: list[dict[str, Any]] = []
            with wb.get_sheet(sheet_name) as sh:
                for row_idx, row in enumerate(sh.rows(), start=1):
                    row_values = [json_friendly(c.v) for c in row]
                    if row_idx == 1:
                        headers = ["" if v is None else str(v).strip() for v in row_values]
                        continue
                    rows.append({"row": row_idx, "values": row_to_map(headers, row_values)})
            sheets.append(
                {
                    "sheet": sheet_name,
                    "headers": headers,
                    "row_count_estimate": len(rows),
                    "rows": rows,
                }
            )
    return sheets, parse_warnings, notes


def parse_file(path: Path, data_only: bool) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    ext = path.suffix.lower()
    if ext == ".csv":
        headers, rows, warnings, notes = parse_csv_headers(path)
        return ([{"sheet": "_csv_", "headers": headers, "row_count_estimate": len(rows), "rows": rows}], warnings, notes)
    if ext in {".xlsx", ".xlsm"}:
        return parse_xlsx_like(path, data_only=data_only)
    if ext == ".xls":
        return parse_xls(path)
    if ext == ".xlsb":
        return parse_xlsb(path)
    return ([], [f"不支持的文件后缀：{ext}"], [])


def build_manifest(
    inputs: Path,
    out_dir: Path,
    run_id: str,
    fail_on_parser_warning: bool = True,
    compiled_rules_path: Path | None = None,
    row_chunk_size: int = 2000,
    openpyxl_data_only: bool = True,
) -> tuple[Path, str]:
    files = discover_input_files(inputs)
    if not files:
        raise ValueError(f"在 {inputs} 未发现可支持的输入文件")

    all_files: list[dict[str, Any]] = []
    input_hasher = hashlib.sha256()
    parser_warning_count = 0
    parser_note_count = 0
    projection_plan = build_projection_plan(compiled_rules_path)
    row_store_files = 0

    for file_path in files:
        digest = file_sha256(file_path)
        input_hasher.update(digest.encode("utf-8"))
        sheets, warnings, notes = parse_file(file_path, data_only=openpyxl_data_only)
        parser_warning_count += len(warnings)
        parser_note_count += len(notes)

        sheet_entries: list[dict[str, Any]] = []
        for sheet in sheets:
            sheet_name = str(sheet.get("sheet", ""))
            headers = [str(x) for x in (sheet.get("headers", []) or [])]
            rows = sheet.get("rows", [])
            if not isinstance(rows, list):
                rows = []
            projected_headers = projected_headers_for_sheet(
                projection_plan,
                file_name=file_path.name,
                file_path=file_path.as_posix(),
                sheet=sheet_name,
                headers=headers,
            )
            store_path_text = ""
            row_count = len(rows)
            if not (projection_plan and not projected_headers):
                store_path = rows_store_path(out_dir, file_path, sheet_name)
                row_count = write_rows_store(rows, projected_headers, store_path, chunk_size=row_chunk_size)
                store_path_text = store_path.as_posix()
                row_store_files += 1
            sheet_entries.append(
                {
                    "sheet": sheet_name,
                    "headers": headers,
                    "projected_headers": projected_headers,
                    "row_count_estimate": row_count,
                    "rows_file": store_path_text,
                }
            )

        all_files.append(
            {
                "path": file_path.as_posix(),
                "name": file_path.name,
                "extension": file_path.suffix.lower(),
                "size_bytes": file_path.stat().st_size,
                "sha256": digest,
                "sheets": sheet_entries,
                "parse_warnings": warnings,
                "parse_notes": notes,
            }
        )

    manifest = {
        "run_id": run_id,
        "generated_at": utc_now_iso(),
        "input_root": inputs.as_posix(),
        "files": all_files,
        "totals": {
            "file_count": len(all_files),
            "sheet_count": sum(len(x["sheets"]) for x in all_files),
            "parser_warning_count": parser_warning_count,
            "parser_note_count": parser_note_count,
            "row_store_file_count": row_store_files,
        },
        "input_hash": input_hasher.hexdigest(),
        "parse_options": {
            "openpyxl_data_only": openpyxl_data_only,
            "row_chunk_size": int(row_chunk_size),
            "projection_enabled": bool(projection_plan),
            "projection_dataset_count": len(projection_plan),
        },
    }

    manifest_path = out_dir / "ingest_manifest.json"
    atomic_write_json(manifest_path, manifest)

    if fail_on_parser_warning and parser_warning_count > 0:
        raise ValueError("解析阶段产生告警，且 fail_on_parser_warning=true，已停止")

    return manifest_path, manifest["input_hash"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="扫描并解析 Excel/CSV 输入文件。")
    parser.add_argument("--inputs", required=True, help="输入文件或目录")
    parser.add_argument("--out", required=True, help="输出目录")
    parser.add_argument("--run-id", required=True, help="本次运行 ID")
    parser.add_argument(
        "--fail-on-parser-warning",
        dest="fail_on_parser_warning",
        action="store_true",
        help="若存在解析告警则直接失败（默认开启）",
    )
    parser.add_argument(
        "--allow-parser-warning",
        dest="fail_on_parser_warning",
        action="store_false",
        help="允许解析告警，不中断流程",
    )
    parser.add_argument(
        "--compiled-rules",
        default=None,
        help="可选：compiled_rules.json 路径，用于按规则列投影",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=2000,
        help="行数据分块写入大小（默认 2000）",
    )
    parser.add_argument(
        "--keep-formula",
        action="store_true",
        help="读取公式文本而非计算值（默认 data_only=True）",
    )
    parser.set_defaults(fail_on_parser_warning=True)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    inputs = Path(args.inputs).resolve()
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        manifest_path, _ = build_manifest(
            inputs=inputs,
            out_dir=out_dir,
            run_id=args.run_id,
            fail_on_parser_warning=args.fail_on_parser_warning,
            compiled_rules_path=Path(args.compiled_rules).resolve() if args.compiled_rules else None,
            row_chunk_size=max(1, int(args.chunk_size or 1)),
            openpyxl_data_only=not bool(args.keep_formula),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[错误] parse_excel 执行失败：{exc}")
        return 1

    print(f"[成功] 输入清单输出：{manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
