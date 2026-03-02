"""端到端执行入口 — 串联规则编译、文件解析、校验、报告全流程。

这是校验引擎的唯一执行入口。支持断点恢复（--resume）和质量门禁（--max-errors）。
输入: Excel/CSV 文件 + rules.json
输出: 完整校验输出（result.json、issues.csv、report.html 等）
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# 确保 scripts/ 目录在导入路径中，无论从哪个工作目录调用本脚本
sys.path.insert(0, str(Path(__file__).resolve().parent))

from common import atomic_write_json, setup_file_logging, severity_key

import compile_rules
import inspect_metadata
import parse_excel
import render_report
import validate_global
import validate_local
import validate_relations
from state_manager import RunState, load_state, mark_failure, mark_stage, mark_success, save_state


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def gate_failed(issues: list[dict[str, Any]], max_errors: int | None) -> bool:
    error_count = 0
    for issue in issues:
        severity = issue.get("severity")
        if severity_key(severity) == "error":
            error_count += 1
    if max_errors is None:
        return False
    return error_count > max_errors


def load_issues_from_result(result_path: Path) -> list[dict[str, Any]]:
    data = json.loads(result_path.read_text(encoding="utf-8"))
    issues = data.get("issues", [])
    return issues if isinstance(issues, list) else []


def load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def resolve_templates(base_dir: Path) -> Path:
    templates_dir = base_dir.parent / "assets" / "templates"
    return templates_dir / "report.html"


def scan_inputs(inputs: Path, out_dir: Path) -> int:
    """扫描输入文件/目录的元数据（sheet 名、列头），写入 _scan.json。

    替代单独调用 inspect_metadata.py，减少命令执行次数。
    同时检测同名文件冲突并输出警告。
    """
    files = parse_excel.discover_input_files(inputs)
    if not files:
        print(f"[扫描] 在 {inputs} 未发现可支持的输入文件")
        return 1

    results = []
    for file_path in files:
        try:
            result = inspect_metadata.inspect_file(file_path)
            results.append(result)
        except Exception as exc:  # noqa: BLE001
            results.append({"file": file_path.name, "error": str(exc)})

    # 检测同名文件冲突
    name_groups: dict[str, list[Path]] = {}
    for file_path in files:
        name_groups.setdefault(file_path.name, []).append(file_path)

    duplicate_names = {name: paths for name, paths in name_groups.items() if len(paths) > 1}
    if duplicate_names:
        print(f"\n[警告] 检测到 {len(duplicate_names)} 组同名文件冲突：")
        for name, paths in sorted(duplicate_names.items()):
            print(f"  {name} ({len(paths)} 个副本):")
            for p in paths:
                size_kb = p.stat().st_size / 1024
                print(f"    {p}  ({size_kb:.1f} KB)")
        print(
            "  提示：校验引擎将自动选择行数最多的版本。"
            "如需指定特定文件，请在 rules.json 的 dataset 中添加 file_path 或 sha256 字段。\n"
        )

    scan_path = out_dir / "_scan.json"
    scan_payload: dict[str, Any] = {
        "files": results,
        "file_count": len(results),
    }
    if duplicate_names:
        scan_payload["duplicate_file_names"] = {
            name: [str(p) for p in paths]
            for name, paths in sorted(duplicate_names.items())
        }
    atomic_write_json(scan_path, scan_payload)

    print(f"[扫描] 发现 {len(files)} 个文件：")
    for result in results:
        name = result.get("file", "?")
        sheets = result.get("sheets", {})
        if isinstance(sheets, dict):
            sheet_names = list(sheets.keys())
        else:
            sheet_names = []
        print(f"  {name} ({len(sheet_names)} 个工作表)")
        for sn in sheet_names:
            headers = sheets.get(sn, {}).get("headers", []) if isinstance(sheets, dict) else []
            print(f"    [{sn}] {len(headers)} 列")

    print(f"\n[扫描] 元数据已写入：{scan_path}")
    return 0


def _write_empty_issues(path: Path, stage: str, stage_zh: str) -> None:
    """当校验阶段异常时，写入空 issues 文件以保证后续报告生成正常。"""
    from common import atomic_write_json, utc_now_iso
    atomic_write_json(path, {
        "stage": stage,
        "stage_zh": stage_zh,
        "generated_at": utc_now_iso(),
        "issue_count": 0,
        "issues": [],
        "_note": "此阶段发生异常，已记录在 run_state.json 的 stage_exceptions 中",
    })


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="执行端到端 Excel 配置校验流程。")
    parser.add_argument("--inputs", required=True, help="输入文件或目录")
    parser.add_argument("--rules", default=None, help="rules.json 路径（--scan 模式下可省略）")
    parser.add_argument("--out", required=True, help="运行输出目录")
    parser.add_argument("--rule-set", default=None, help="可选：规则分组 key")
    parser.add_argument("--run-id", default=None, help="可选：运行 ID")
    parser.add_argument("--resume", action="store_true", help="从已有 run_state.json 继续执行")
    parser.add_argument(
        "--log",
        default=None,
        help="日志文件路径（所有输出同时写入此文件，解决 IDE 终端捕获问题）",
    )
    parser.add_argument(
        "--scan",
        action="store_true",
        help="仅扫描输入文件元数据（sheet 名、列头），写入 _scan.json 后退出",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=None,
        help="质量门禁：当错误数量超过该值时判定失败",
    )
    parser.add_argument(
        "--fail-on-parser-warning",
        dest="fail_on_parser_warning",
        action="store_true",
        help="解析阶段若出现告警则直接失败（默认开启）",
    )
    parser.add_argument(
        "--allow-parser-warning",
        dest="fail_on_parser_warning",
        action="store_false",
        help="允许解析告警，不中断流程",
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
    script_dir = Path(__file__).resolve().parent
    out_dir = Path(args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    # 设置日志重定向（解决 IDE 终端输出捕获问题和 CMD 窗口一闪而过问题）
    log_path = args.log
    if log_path is None:
        log_path = out_dir / "_run.log"
    setup_file_logging(log_path)

    inputs = Path(args.inputs).resolve()

    # --scan 模式：仅扫描文件元数据后退出
    if args.scan:
        return scan_inputs(inputs, out_dir)

    # 正常校验模式需要 --rules 参数
    if not args.rules:
        print("[错误] 校验模式需要 --rules 参数")
        return 1

    run_id = args.run_id or utc_now_compact()
    rules = Path(args.rules).resolve()

    state_path = out_dir / "run_state.json"
    state = load_state(state_path) if args.resume else None
    if state is None:
        state = RunState(run_id=run_id)
        save_state(state_path, state)
    run_id = state.run_id

    try:
        compiled_path = out_dir / "compiled_rules.json"
        manifest_path = out_dir / "ingest_manifest.json"
        stages_dir = out_dir / "_stages"
        stages_dir.mkdir(parents=True, exist_ok=True)
        local_path = stages_dir / "local_issues.json"
        relation_path = stages_dir / "relation_issues.json"
        global_path = stages_dir / "global_issues.json"
        result_path = out_dir / "result.json"
        csv_path = out_dir / "issues.csv"
        html_path = out_dir / "report.html"

        stage_exceptions: list[dict[str, str]] = []

        if args.resume and compiled_path.exists():
            compiled_data = load_json(compiled_path)
            state.metadata["rules_hash"] = str(compiled_data.get("rules_hash", ""))
        else:
            mark_stage(state, "preflight")
            compiled_path, rules_hash = compile_rules.compile_rules(rules, out_dir, args.rule_set)
            state.metadata["rules_hash"] = rules_hash
            save_state(state_path, state)

        if args.resume and manifest_path.exists():
            manifest_data = load_json(manifest_path)
            state.metadata["input_hash"] = str(manifest_data.get("input_hash", ""))
            state.metadata["manifest"] = manifest_path.as_posix()
        else:
            mark_stage(state, "ingest")
            manifest_path, input_hash = parse_excel.build_manifest(
                inputs=inputs,
                out_dir=out_dir,
                run_id=run_id,
                fail_on_parser_warning=args.fail_on_parser_warning,
                compiled_rules_path=compiled_path,
                row_chunk_size=max(1, int(args.chunk_size or 1)),
                openpyxl_data_only=not bool(args.keep_formula),
            )
            state.metadata["input_hash"] = input_hash
            state.metadata["manifest"] = manifest_path.as_posix()
            save_state(state_path, state)

        # --- 校验阶段：异常不中断后续阶段 ---
        if not (args.resume and local_path.exists()):
            mark_stage(state, "local")
            try:
                local_path = validate_local.validate_local(compiled_path, manifest_path, stages_dir)
            except Exception as exc:  # noqa: BLE001
                stage_exceptions.append({"stage": "local", "error": str(exc)})
                print(f"[警告] 局部校验阶段异常（已记录，继续执行）：{exc}")
                _write_empty_issues(local_path, "local", "局部校验")
            save_state(state_path, state)

        if not (args.resume and relation_path.exists()):
            mark_stage(state, "relation")
            try:
                relation_path = validate_relations.validate_relations(compiled_path, manifest_path, stages_dir)
            except Exception as exc:  # noqa: BLE001
                stage_exceptions.append({"stage": "relation", "error": str(exc)})
                print(f"[警告] 关联校验阶段异常（已记录，继续执行）：{exc}")
                _write_empty_issues(relation_path, "relation", "关联校验")
            save_state(state_path, state)

        if not (args.resume and global_path.exists()):
            mark_stage(state, "global")
            try:
                global_path = validate_global.validate_global(compiled_path, stages_dir)
            except Exception as exc:  # noqa: BLE001
                stage_exceptions.append({"stage": "global", "error": str(exc)})
                print(f"[警告] 全局校验阶段异常（已记录，继续执行）：{exc}")
                _write_empty_issues(global_path, "global", "全局校验")
            save_state(state_path, state)

        if not (args.resume and result_path.exists() and csv_path.exists() and html_path.exists()):
            mark_stage(state, "report")
            html_template = resolve_templates(script_dir)
            result_path, csv_path, html_path = render_report.render_reports(
                out_dir=out_dir,
                manifest_path=manifest_path,
                compiled_rules_path=compiled_path,
                issue_files=[local_path, relation_path, global_path],
                html_template_path=html_template,
            )
            save_state(state_path, state)

        state.metadata["result_json"] = result_path.as_posix()
        state.metadata["issues_csv"] = csv_path.as_posix()
        state.metadata["report_html"] = html_path.as_posix()
        if stage_exceptions:
            state.metadata["stage_exceptions"] = stage_exceptions
        save_state(state_path, state)

        issues = load_issues_from_result(result_path)
        if gate_failed(issues, args.max_errors):
            mark_failure(state, "gate", f"质量门禁失败：错误数量超过 {args.max_errors}")
            save_state(state_path, state)
            print("[错误] 质量门禁失败。")
            return 2

        if stage_exceptions:
            mark_success(state)
            save_state(state_path, state)
            print(f"[警告] 校验完成（有 {len(stage_exceptions)} 个阶段异常已记录）。输出目录：{out_dir}")
            return 0

        mark_success(state)
        save_state(state_path, state)
        print(f"[成功] 校验完成。输出目录：{out_dir}")
        return 0

    except Exception as exc:  # noqa: BLE001
        mark_failure(state, state.stage, str(exc))
        save_state(state_path, state)
        print(f"[错误] run_validator 执行失败：{exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
