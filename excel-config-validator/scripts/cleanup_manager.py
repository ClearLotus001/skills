# -*- coding: utf-8 -*-
"""中间文件清理工具。

在校验流程成功完成后，清理中间产物（如 compiled_rules.json、
ingest_manifest.json、_row_store、_stages 等），仅保留最终输出文件。
"""
from __future__ import annotations

import shutil
from pathlib import Path


def cleanup_intermediate_files(out_dir: Path, keep_logs: bool = True) -> None:
    """清理中间文件，仅保留最终产物。"""
    to_remove = [
        out_dir / "_scan.json",
        out_dir / "_row_store",
        out_dir / "_stages",
        out_dir / "run_state.json",
        out_dir / "compiled_rules.json",
        out_dir / "ingest_manifest.json",
    ]

    if not keep_logs:
        to_remove.append(out_dir / "_run.log")

    for path in to_remove:
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)
