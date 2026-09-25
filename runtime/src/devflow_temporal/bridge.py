"""agent-runtime-kit boundary for real local role execution."""

from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import Any

from agent_runtime_kit import (
    AgentResult,
    AgentTask,
    FilesystemAccess,
    PermissionMode,
    PermissionProfile,
    ReadinessStatus,
    check_readiness,
    validate_task,
)
from agent_runtime_kit.adapters import CodexAgentRuntime

ASSESSMENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["pass", "findings", "blocked"]},
        "summary": {"type": "string", "minLength": 1},
        "findings": {"type": "array", "items": {"type": "string"}},
    },
    "required": ["status", "summary", "findings"],
    "additionalProperties": False,
}

ROLE_INSTRUCTIONS = {
    "implement": (
        "Implement the requested change in this disposable local working copy. "
        "Make the smallest coherent edit and inspect its result. Return status=pass only "
        "after a concrete file change; otherwise return blocked with the reason."
    ),
    "review": (
        "Independently review this immutable candidate for correctness, security, and "
        "the requested behavior. Inspect files and report actionable findings. Do not edit "
        "files. Return pass only after a substantive review."
    ),
    "verify": (
        "Independently exercise this candidate through its local entry points and report "
        "observed behavior. This is a disposable verification copy. Return pass only "
        "after checks relevant to the requested change; report failures or gaps as findings."
    ),
}


def role_task(spec: dict[str, Any], role: str, workspace: Path, candidate_id: str) -> AgentTask:
    filesystem = (
        FilesystemAccess.READ_ONLY if role == "review" else FilesystemAccess.WORKSPACE_WRITE
    )
    return AgentTask(
        goal=(
            f"{ROLE_INSTRUCTIONS[role]}\n\n"
            f"Requested task: {spec['goal']}\n"
            f"Candidate ID: {candidate_id}\n"
            "Return the required structured assessment. Do not perform external writes."
        ),
        task_id=f"devflow:{spec['run_id']}:{role}:0:{candidate_id[:12]}",
        system="You are one independent role in a local development workflow.",
        model=spec["model"],
        reasoning_effort=spec["effort"],
        working_directory=workspace,
        permissions=PermissionProfile(mode=PermissionMode.STRICT, filesystem=filesystem),
        output_schema=ASSESSMENT_SCHEMA,
        metadata={"devflow_role": role, "run_id": spec["run_id"]},
    )


async def preflight(spec: dict[str, Any]) -> dict[str, Any]:
    runtime = CodexAgentRuntime()
    unsupported: list[str] = []
    for role in ROLE_INSTRUCTIONS:
        task = role_task(spec, role, Path(spec["repo"]), spec["initial_candidate"]["id"])
        report = validate_task(runtime, task)
        unsupported.extend(f"{role}.{issue.field}: {issue.message}" for issue in report.issues)
    if unsupported:
        raise ValueError("required provider capabilities unavailable: " + "; ".join(unsupported))
    readiness = await check_readiness(runtime)
    if readiness.status is ReadinessStatus.NOT_READY:
        raise ValueError(f"provider is not ready: {readiness.message}")
    return {"status": readiness.status.value, "message": readiness.message}


async def run_codex(
    spec: dict[str, Any], role: str, workspace: Path, candidate_id: str
) -> dict[str, Any]:
    runtime = CodexAgentRuntime()
    task = role_task(spec, role, workspace, candidate_id)
    support = validate_task(runtime, task)
    if not support.supported:
        return {
            "status": "blocked",
            "summary": "provider cannot honor required task controls",
            "findings": [f"{issue.field}: {issue.message}" for issue in support.issues],
            "reported_model": None,
            "reported_effort": None,
            "session_id": None,
            "usage": None,
            "finish_reason": "unsupported",
        }
    result: AgentResult = await runtime.run(task)
    metadata = result.metadata
    assessment = result.parsed_output if result.parsed_output_available else None
    if not result.is_success or not isinstance(assessment, dict):
        status = "blocked"
        summary = "provider did not return a successful structured assessment"
        findings = [f"finish_reason={result.finish_reason}"]
    else:
        status = assessment["status"]
        summary = assessment["summary"]
        findings = assessment["findings"]
    return {
        "status": status,
        "summary": summary,
        "findings": findings,
        "reported_model": metadata.get("model") if isinstance(metadata.get("model"), str) else None,
        "reported_effort": (
            metadata.get("reasoning_effort")
            if isinstance(metadata.get("reasoning_effort"), str)
            else None
        ),
        "session_id": result.session_id,
        "usage": asdict(result.usage),
        "finish_reason": result.finish_reason,
    }
