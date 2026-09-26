"""One isolated kit call in a supervisor-owned child process."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import traceback
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from agent_runtime_kit import (
    AgentResult,
    AgentTask,
    FilesystemAccess,
    PermissionMode,
    PermissionProfile,
    SessionResumeState,
    validate_task,
)
from agent_runtime_kit.adapters import CodexAgentRuntime
from openai_codex import CodexConfig

from .bridge import ASSESSMENT_SCHEMA


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)


def _task(request: dict[str, Any]) -> AgentTask:
    spec = request["spec"]
    role = request["role"]
    policy = spec["policy"]["roles"][role]
    candidate = request["candidate"]
    workspace = Path(request["workspace"])
    findings = request.get("findings") or []
    instructions = {
        "implement": (
            "Implement the accepted plan in this owned checkout. Preserve unrelated work. "
            "Run relevant checks and report concrete evidence. Do not push, open a PR, "
            "merge, or change GitHub tracking; the controller owns those effects. "
            "Status pass requires a substantive candidate change."
        ),
        "review": (
            "Independently review the exact candidate against the accepted plan. "
            "Inspect source and meaningful tests. Report actionable defects and missing "
            "evidence as findings. Do not edit files."
        ),
        "verify": (
            "Independently exercise the exact candidate through the real local entry "
            "points and report observed commands, outcomes, and gaps. This checkout "
            "is disposable. Do not push, open a PR, or change GitHub tracking."
        ),
    }[role]
    recovery = spec["policy"].get("recovery")
    recovery_note = (
        f"Recovered stopped-work provenance is at {spec['state_dir']}/recovery/provenance.json. "
        "If import did not apply cleanly, inspect its feature.patch; resolve only feature "
        "paths in this owned checkout. The old checkout is read-only evidence."
        if recovery and role == "implement"
        else ""
    )
    prompt = (
        f"{instructions}\n\n"
        f"Goal: {spec['goal']}\n\nAccepted plan:\n{spec['accepted_plan']}\n\n"
        f"Candidate: {candidate['id']} at {candidate['head']}\n"
        f"Allowed feature paths: {json.dumps(spec['policy']['allowed_paths'])}\n"
        f"Previous findings to repair: {json.dumps(findings)}\n"
        f"{recovery_note}\n"
        "Return a structured assessment with status, summary, and findings. "
        "A completed turn alone is not a pass."
    )
    if spec["provider"] == "codex" and spec["policy"].get("host_sandbox") != "seatbelt":
        raise ValueError("Codex role requires an outer Seatbelt boundary")
    # Codex's nested macOS sandbox cannot initialize under Seatbelt. The
    # supervisor's OS profile is authoritative; CLI escalation remains denied.
    mode = FilesystemAccess.FULL_ACCESS
    prior = request.get("resume_session")
    return AgentTask(
        goal=prompt,
        task_id=f"delivery:{spec['run_id']}:{role}:{request['iteration']}:{candidate['id'][:12]}",
        system="You are one bounded role in a locally supervised delivery run.",
        model=policy["model"],
        reasoning_effort=policy["effort"],
        working_directory=workspace,
        permissions=PermissionProfile(mode=PermissionMode.STRICT, filesystem=mode),
        resume_from=SessionResumeState(session_id=prior) if prior else None,
        deadline=datetime.now(UTC) + timedelta(seconds=int(policy.get("timeout_seconds", 7200))),
        output_schema=ASSESSMENT_SCHEMA,
        metadata={"run_id": spec["run_id"], "role": role, "iteration": request["iteration"]},
    )


async def _run_codex(request: dict[str, Any]) -> dict[str, Any]:
    binary = request["spec"]["policy"]["codex_bin"]
    if not Path(binary).is_file():
        raise ValueError("configured Codex executable is missing")

    class PinnedConfig(CodexConfig):
        def __init__(self, *, cwd=None, config_overrides=(), env=None):
            super().__init__(codex_bin=binary, cwd=cwd, config_overrides=config_overrides, env=env)

    runtime = CodexAgentRuntime(
        config_cls=PinnedConfig,
        config_overrides=tuple(request["spec"]["policy"]["config_overrides"]),
    )
    task = _task(request)
    support = validate_task(runtime, task)
    if not support.supported:
        return {
            "status": "blocked",
            "summary": "configured provider cannot honor required task controls",
            "findings": [f"{issue.field}: {issue.message}" for issue in support.issues],
            "session_id": None,
            "usage": None,
            "finish_reason": "unsupported",
            "requested_model": task.model,
            "requested_effort": task.reasoning_effort,
            "reported_model": None,
            "reported_effort": None,
        }
    result: AgentResult = await runtime.run(task)
    parsed = result.parsed_output if result.parsed_output_available else None
    if not result.is_success or not isinstance(parsed, dict):
        status = "blocked"
        summary = "provider did not return a successful structured assessment"
        findings = [f"finish_reason={result.finish_reason}"]
    else:
        status = parsed.get("status", "blocked")
        summary = parsed.get("summary", "")
        findings = parsed.get("findings", [])
    if status == "pass" and (not isinstance(summary, str) or not summary.strip() or findings):
        status = "blocked"
        findings = ["pass assessment was missing a summary or contained findings"]
    if not isinstance(findings, list) or any(not isinstance(item, str) for item in findings):
        status = "blocked"
        findings = ["assessment findings were invalid"]
    return {
        "status": status,
        "summary": summary,
        "findings": findings,
        "session_id": result.session_id,
        "usage": asdict(result.usage),
        "finish_reason": result.finish_reason,
        "requested_model": task.model,
        "requested_effort": task.reasoning_effort,
        # This adapter echoes task selection in metadata.model; provider fields
        # are not exposed and must remain unknown.
        "reported_model": None,
        "reported_effort": None,
        "host_sandbox": "seatbelt",
        "tool_calls": [asdict(item) for item in result.tool_calls],
    }


async def _run_fake(request: dict[str, Any]) -> dict[str, Any]:
    role = request["role"]
    has_finding = request["iteration"] in request["spec"]["policy"].get("fake_findings", {}).get(
        role, []
    )
    if role == "implement":
        marker = Path(request["workspace"]) / "devflow-fake-change.txt"
        marker.write_text(f"Deterministic fake change {request['iteration']}\n", encoding="utf-8")
    return {
        "status": "findings" if has_finding else "pass",
        "summary": "deterministic fake role result",
        "findings": [f"fake {role} finding at iteration {request['iteration']}"]
        if has_finding
        else [],
        "session_id": f"fake:{request['spec']['run_id']}:{role}",
        "usage": None,
        "finish_reason": "fake",
        "requested_model": None,
        "requested_effort": None,
        "reported_model": None,
        "reported_effort": None,
        "tool_calls": [],
    }


def main() -> int:
    request_path = Path(sys.argv[1])
    request = json.loads(request_path.read_text(encoding="utf-8"))
    start = Path(request["start_path"])
    output = Path(request["result_path"])
    _write_json(start, {"pid": os.getpid(), "started_at": datetime.now(UTC).isoformat()})
    if sys.stdin.readline().strip() != "GO":
        return 2
    try:
        result = asyncio.run(
            _run_fake(request) if request["spec"].get("provider") == "fake" else _run_codex(request)
        )
        _write_json(output, result)
        return 0
    except BaseException as exc:
        _write_json(
            output,
            {
                "status": "blocked",
                "summary": f"role process failed: {type(exc).__name__}",
                "findings": [str(exc)[:500]],
                "session_id": None,
                "usage": None,
                "finish_reason": "exception",
                "traceback": traceback.format_exc(limit=3)[-1500:],
            },
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
