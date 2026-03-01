"""运行状态管理 — 维护 run_state.json 用于断点恢复和阶段跟踪。

由 run_validator.py 内部调用。
管理: run_state.json（运行 ID、当前阶段、耗时、元数据）
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any

from common import atomic_write_json, utc_now_iso

STAGE_ZH = {
    "initialized": "已初始化",
    "preflight": "预检查",
    "ingest": "数据解析",
    "local": "局部校验",
    "relation": "关联校验",
    "global": "全局校验",
    "report": "报告生成",
    "gate": "质量门禁",
}

STATUS_ZH = {
    "running": "执行中",
    "failed": "失败",
    "succeeded": "成功",
}


@dataclass
class RunState:
    run_id: str
    stage: str = "initialized"
    status: str = "running"
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    completed_stages: list[str] = field(default_factory=list)
    checkpoints: dict[str, Any] = field(default_factory=dict)
    retries: dict[str, int] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["stage_zh"] = STAGE_ZH.get(self.stage, self.stage)
        payload["status_zh"] = STATUS_ZH.get(self.status, self.status)
        payload["completed_stages_zh"] = [STAGE_ZH.get(x, x) for x in self.completed_stages]
        return payload


def load_state(path: Path) -> RunState | None:
    if not path.exists():
        return None
    import json
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        return None
    known = {f.name for f in fields(RunState)}
    filtered = {k: v for k, v in data.items() if k in known}
    return RunState(**filtered)


def save_state(path: Path, state: RunState) -> None:
    state.updated_at = utc_now_iso()
    atomic_write_json(path, state.to_dict())


def mark_stage(state: RunState, stage: str, checkpoint: dict[str, Any] | None = None) -> None:
    state.stage = stage
    if stage not in state.completed_stages:
        state.completed_stages.append(stage)
    if checkpoint is not None:
        state.checkpoints[stage] = checkpoint
    state.updated_at = utc_now_iso()


def mark_failure(state: RunState, stage: str, message: str) -> None:
    state.stage = stage
    state.status = "failed"
    state.metadata["error_message"] = message
    state.updated_at = utc_now_iso()


def mark_success(state: RunState) -> None:
    state.status = "succeeded"
    state.updated_at = utc_now_iso()
