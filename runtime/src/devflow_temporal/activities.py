"""External work performed by Temporal activities."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any
from uuid import uuid4

from temporalio import activity

from .bridge import run_codex
from .candidate import assert_candidate, candidate_for, snapshot, validate_paths
from .contracts import ROLE_NAMES, RUN_ID_RE
from .receipts import ReceiptStore, RunBindingError


def _blocked(request: dict[str, Any], finding: str) -> dict[str, Any]:
    spec = request["spec"]
    candidate = request["candidate"]
    role = request["role"]
    return {
        "role": role,
        "identity": f"{spec['provider']}:{spec['run_id']}:{role}:0:{candidate['id'][:12]}",
        "provider": spec["provider"],
        "requested_model": spec.get("model"),
        "requested_effort": spec.get("effort"),
        "reported_model": None,
        "reported_effort": None,
        "session_id": None,
        "status": "blocked",
        "summary": finding,
        "findings": [finding],
        "usage": None,
        "input_candidate_id": candidate["id"],
        "candidate": candidate,
        "finish_reason": "blocked",
    }


def _write_evidence(state_dir: Path, run_id: str, role: str, result: dict[str, Any]) -> str:
    evidence_dir = state_dir / "runs" / run_id / "evidence"
    evidence_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    target = evidence_dir / f"{role}.json"
    temporary = evidence_dir / f".{role}.{uuid4().hex}.tmp"
    temporary.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, target)
    return str(target)


async def _fake_role(spec: dict[str, Any], role: str, repo: Path) -> dict[str, Any]:
    state_dir = Path(spec["state_dir"])
    with (state_dir / "fake-invocations.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"run_id": spec["run_id"], "role": role}) + "\n")
    if role == "implement":
        marker = repo / "devflow-temporal-demo.txt"
        with marker.open("x", encoding="utf-8") as stream:
            stream.write(spec["goal"] + "\n")
    if spec.get("fake_change") == role:
        (repo / "unexpected-gate-edit.txt").write_text("changed during gate\n", encoding="utf-8")
    finding = spec.get("fake_finding") == role
    return {
        "status": "findings" if finding else "pass",
        "summary": "deterministic fake finding" if finding else "deterministic fake pass",
        "findings": ["deterministic fake finding"] if finding else [],
        "reported_model": None,
        "reported_effort": None,
        "session_id": None,
        "usage": None,
        "finish_reason": "fake",
    }


@activity.defn(name="run_role")
async def run_role(request: dict[str, Any]) -> dict[str, Any]:
    spec = request["spec"]
    role = request["role"]
    candidate = request["candidate"]
    if role not in ROLE_NAMES or not RUN_ID_RE.fullmatch(spec["run_id"]):
        raise ValueError("invalid role request")
    state_dir = Path(spec["state_dir"])
    receipts = ReceiptStore(state_dir)
    try:
        claim = receipts.claim(spec, role, 0, candidate["id"])
    except RunBindingError as exc:
        return _blocked(request, str(exc))
    if claim.state == "finished":
        assert claim.result is not None
        return claim.result
    if claim.state == "ambiguous":
        result = _blocked(request, "recovery unknown: prior role invocation may still have run")
        result["status"] = "recovery_unknown"
        return result
    assert claim.generation is not None
    try:
        result = await _execute_new(request)
    except Exception as exc:
        # A returned exception is terminal. An interrupted process instead leaves
        # the running receipt behind; redelivery cannot launch another model.
        result = _blocked(request, f"role execution failed: {type(exc).__name__}")
    result["evidence"] = _write_evidence(state_dir, spec["run_id"], role, result)
    receipts.finish(spec["run_id"], role, 0, candidate["id"], claim.generation, result)
    return result


async def _execute_new(request: dict[str, Any]) -> dict[str, Any]:
    spec = request["spec"]
    role = request["role"]
    candidate = request["candidate"]
    repo = Path(spec["repo"])
    state_dir = Path(spec["state_dir"])
    validate_paths(repo, state_dir, require_clean=False)
    candidate_snapshot = state_dir / "runs" / spec["run_id"] / "candidate"
    if role == "implement":
        workspace = repo
        assert_candidate(workspace, candidate)
    else:
        assert_candidate(candidate_snapshot, candidate, git_head=False)
        if role == "review":
            workspace = candidate_snapshot
        else:
            workspace = state_dir / "runs" / spec["run_id"] / "verify-work"
            if workspace.exists():
                raise ValueError("verification working copy already exists")
            shutil.copytree(candidate_snapshot, workspace, symlinks=True)
            assert_candidate(workspace, candidate, git_head=False)

    if spec["provider"] == "fake":
        assessment = await _fake_role(spec, role, workspace)
    elif spec["provider"] == "codex":
        assessment = await run_codex(spec, role, workspace, candidate["id"])
    else:
        raise ValueError("unknown provider")

    produced = candidate
    if role == "implement":
        produced = candidate_for(repo)
        if produced["id"] != candidate["id"]:
            snapshot(repo, state_dir, spec["run_id"], produced)
        elif assessment["status"] == "pass":
            assessment["status"] = "blocked"
            assessment["findings"].append("implementer produced no candidate content change")
    else:
        try:
            assert_candidate(candidate_snapshot, candidate, git_head=False)
            assert_candidate(workspace, candidate, git_head=False)
        except ValueError:
            assessment["status"] = "blocked"
            assessment["findings"].append("candidate changed during independent gate")

    if assessment["status"] == "pass" and assessment["findings"]:
        assessment["status"] = "blocked"
        assessment["findings"].append("pass assessment contained findings")
    if assessment["status"] != "pass" and not assessment["findings"]:
        assessment["findings"].append("role did not establish a pass")

    return {
        "role": role,
        "identity": f"{spec['provider']}:{spec['run_id']}:{role}:0:{candidate['id'][:12]}",
        "provider": spec["provider"],
        "requested_model": spec.get("model"),
        "requested_effort": spec.get("effort"),
        "reported_model": assessment["reported_model"],
        "reported_effort": assessment["reported_effort"],
        "session_id": assessment["session_id"],
        "status": assessment["status"],
        "summary": assessment["summary"],
        "findings": assessment["findings"],
        "usage": assessment["usage"],
        "input_candidate_id": candidate["id"],
        "candidate": produced,
        "finish_reason": assessment["finish_reason"],
    }
