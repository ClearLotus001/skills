"""轻量 Excel/CSV 元数据探查 — 仅输出 sheet 名与列头，不读取行数据。

用法：python scripts/inspect_metadata.py <file1> [file2 ...]
输出：每个文件的 sheet 列表与各 sheet 的列名（JSON 格式，stdout）
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def inspect_xlsx(path: Path) -> dict:
    from openpyxl import load_workbook

    wb = load_workbook(str(path), read_only=True, data_only=True)
    sheets = {}
    for name in wb.sheetnames:
        ws = wb[name]
        headers = []
        for row in ws.iter_rows(min_row=1, max_row=1, values_only=True):
            headers = [str(h) if h is not None else "" for h in row]
            break
        sheets[name] = {"headers": headers}
    wb.close()
    return {"file": path.name, "type": "xlsx", "sheets": sheets}


def inspect_csv(path: Path) -> dict:
    import csv

    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
    sheet_name = path.stem
    return {"file": path.name, "type": "csv", "sheets": {sheet_name: {"headers": headers}}}


def inspect_file(path: Path) -> dict:
    ext = path.suffix.lower()
    if ext in {".xlsx", ".xls", ".xlsm", ".xlsb"}:
        return inspect_xlsx(path)
    elif ext == ".csv":
        return inspect_csv(path)
    else:
        return {"file": path.name, "type": "unknown", "error": f"unsupported extension: {ext}"}


def main() -> int:
    parser = argparse.ArgumentParser(description="探查 Excel/CSV 文件的 sheet 与列头元数据")
    parser.add_argument("files", nargs="+", help="要探查的文件路径")
    args = parser.parse_args()

    results = []
    for f in args.files:
        p = Path(f).resolve()
        if not p.is_file():
            results.append({"file": str(f), "error": "file not found"})
            continue
        try:
            results.append(inspect_file(p))
        except Exception as exc:
            results.append({"file": p.name, "error": str(exc)})

    json.dump(results, sys.stdout, ensure_ascii=False, indent=2)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
