"""External work performed by Temporal activities."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from temporalio import activity

from .candidate import assert_candidate, candidate_for, snapshot, validate_paths


@activity.defn(name="run_role")
async def run_role(request: dict[str, Any]) -> dict[str, Any]:
    spec = request["spec"]
    role = request["role"]
    candidate = request["candidate"]
    repo = Path(spec["repo"])
    state_dir = Path(spec["state_dir"])
    validate_paths(repo, state_dir, require_clean=False)
    if role == "implement":
        workspace = repo
    else:
        workspace = state_dir / "runs" / spec["run_id"] / "candidate"
    assert_candidate(workspace, candidate, git_head=role == "implement")
    if spec["provider"] != "fake":
        raise RuntimeError("real provider bridge is not configured")
    state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    with (state_dir / "fake-invocations.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps({"run_id": spec["run_id"], "role": role}) + "\n")
    if role == "implement":
        (repo / "devflow-temporal-demo.txt").write_text(spec["goal"] + "\n", encoding="utf-8")
        produced = candidate_for(repo)
        snapshot(repo, state_dir, spec["run_id"], produced)
    else:
        produced = candidate
    finding = spec.get("fake_finding") == role
    return {
        "role": role,
        "identity": f"fake:{spec['run_id']}:{role}",
        "provider": "fake",
        "model": None,
        "effort": None,
        "session_id": None,
        "status": "findings" if finding else "pass",
        "summary": "deterministic fake finding" if finding else "deterministic fake pass",
        "findings": ["deterministic fake finding"] if finding else [],
        "usage": None,
        "input_candidate_id": candidate["id"],
        "candidate": produced,
        "evidence": "fake provider; no model invocation",
    }
