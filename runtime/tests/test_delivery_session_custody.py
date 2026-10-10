from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_stopped_resume import saved
from test_delivery_stopped_resume import stopped as stopped
from test_delivery_store import service as service

from devflow_temporal import delivery_session_custody as custody
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_resources import private_directory, write_private
from devflow_temporal.delivery_sandbox import _native_role_home
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def missing_session(stopped, monkeypatch):
    store, broker, state, command = stopped
    base = copy.deepcopy(broker.spec)
    base.update(
        provider="codex",
        policy={
            **base["policy"],
            "host_sandbox": "trusted-local",
            "execution_backend": "native-macos",
        },
    )
    spec = {**base, "role_home_generation": "gate-retry-1"}
    previous = {
        "kind": "prepublication_gate_retry",
        "execution_spec": spec,
        "seal": {"original_spec": base},
        "original_recovery": None,
    }
    session = state["roles"][0]["session_id"]
    home = _native_role_home({"spec": base, "role": "implement", "iteration": 0})
    private_directory(home / "codex" / "sessions")
    transcript = home / "codex" / "sessions" / (session + ".jsonl")
    transcript.write_text("retained session\n")
    attempts = []
    for iteration, chosen in ((0, base), (1, spec)):
        request = {
            "spec": chosen,
            "role": "implement",
            "iteration": iteration,
            "candidate": state["candidate"],
            "workspace": spec["checkout"],
            "resume_session": session if iteration else None,
        }
        key = DeliverySupervisor._job_key(request)
        folder = Path(spec["state_dir"]) / "attempts" / key
        private_directory(folder)
        request["result_path"] = str(folder / "result.json")
        outcome = {
            "state": "finished",
            "exit_code": 1 if iteration else 0,
            "cleanup": "observed-native-confirmed",
            "monitoring_complete": True,
            "cancelled": False,
            "timed_out": False,
            "stdio_drained": True,
            "journal": str(folder / "native-process.json"),
        }
        raw = {
            "status": "blocked" if iteration else "pass",
            "session_id": None if iteration else session,
            "finish_reason": "exception" if iteration else "done",
            "usage": None,
            "summary": "role process failed: InvalidRequestError" if iteration else "Done",
            "findings": [f"JSON-RPC error -32600: no rollout found for thread id {session}"]
            if iteration
            else [],
        }
        result = {**raw, "cleanup": "confirmed", "native_process": outcome}
        journal = {
            "intent": {
                "run_id": spec["run_id"],
                "policy_digest": chosen["policy_digest"],
                "cwd": spec["checkout"],
                "environment_sha256": digest(
                    {"CODEX_HOME": str(_native_role_home(request) / "codex")}
                ),
            },
            "owned": {"100": {"identity": "owner"}, "101": {"identity": "child"}},
            "phase": "finished",
            "monitoring_complete": True,
            "result": outcome,
            "provider_session": {
                "iteration": iteration,
                "role": "implement",
                "session_id": raw["session_id"],
                "resumed_from": request["resume_session"],
                "result_digest": digest(result),
                "output_candidate": {
                    k: state["candidate"][k] for k in ("id", "head", "content_sha256")
                },
            },
        }
        write_private(folder / "request.json", request)
        write_private(folder / "result.json", raw)
        write_private(folder / "native-process.json", journal)
        write_private(
            folder / "launch.json",
            {"environment": {"CODEX_HOME": str(_native_role_home(request) / "codex")}},
        )
        if iteration:
            write_private(
                folder / "native-thread-observation.json",
                {
                    "schema": "devflow-native-kit-threads-v1",
                    "run_id": spec["run_id"],
                    "role": "implement",
                    "iteration": 1,
                    "resumed_from": session,
                    "pid": 101,
                    "start_identity": "child",
                    "state": "unknown",
                    "parent_thread_id": None,
                    "turns": [],
                    "raw_turn_items": None,
                    "collaboration_items": [],
                    "thread_inventory_before": [],
                    "thread_inventory_after": None,
                    "new_child_thread_ids": None,
                },
            )
        attempt = {
            "job_key": key,
            "run_id": spec["run_id"],
            "role": "implement",
            "iteration": iteration,
            "candidate_id": state["candidate"]["id"],
            "session_id": raw["session_id"],
            "state": "finished",
            "cleanup": "confirmed",
            "result_path": request["result_path"],
            "result_json": canonical_json(result),
            "pid": 100,
            "process_identity": "owner",
        }
        attempts.append(attempt)
        role = {
            **result,
            "role": "implement",
            "iteration": iteration,
            "candidate": state["candidate"],
        }
        if iteration:
            state["roles"].append(role)
        else:
            state["roles"][0] = role
    state["iteration"] = command["expected_iteration"] = 1
    # Keep public admission's closed projection and receipts aligned with this stopped history.
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts")
        for attempt in attempts:
            columns = ",".join(attempt)
            marks = ",".join("?" for _ in attempt)
            db.execute(
                f"INSERT INTO delivery_attempts ({columns}) VALUES ({marks})",
                tuple(attempt.values()),
            )
        db.execute("UPDATE delivery_runs SET iteration=1 WHERE run_id='run-1'")
    monkeypatch.setattr(store, "effective_spec", lambda _: spec)
    monkeypatch.setattr(
        custody,
        "implementation_generation",
        lambda current, prior=None: (
            "" if current == spec else current.get("role_home_generation", "")
        ),
    )
    monkeypatch.setattr(resume, "observed_native_cleanup", lambda _: "closed-native-fixture")
    monkeypatch.setattr(resume, "prepare_runtime", lambda selected, *_: copy.deepcopy(selected))
    return store, state, command, spec, attempts, previous, transcript


def test_public_resume_reuses_retained_session_and_rechecks_its_bytes(missing_session):
    store, state, command, _, _, _, transcript = missing_session
    original = copy.deepcopy(state)
    assert store.repair_admission_preflight("run-1", command)["preflight"] is True
    store.continue_repair("run-1", command)
    recovery = saved(store)
    assert recovery["state"] == original
    assert recovery["session_id"] == "original-implementation"
    assert recovery["execution_spec"]["implementation_role_home_generation"] == ""
    assert resume.readback(store, recovery["execution_spec"], recovery) == {"state": "confirmed"}
    transcript.write_text("changed after admission\n")
    with pytest.raises(ValueError, match="session custody changed"):
        resume.readback(store, recovery["execution_spec"], recovery)


def test_missing_rollout_cannot_hide_provider_work_or_an_unrelated_failure(missing_session):
    _, state, _, spec, attempts, previous, _ = missing_session
    folder = Path(attempts[-1]["result_path"]).parent
    path = folder / "native-thread-observation.json"
    original = json.loads(path.read_text())
    custody.implementation_custody(spec, state, attempts, previous)
    for changed in (
        {"parent_thread_id": "accepted-thread"},
        {"turns": [{"turn_id": "started"}]},
        {"start_identity": "foreign-process"},
        {"thread_inventory_before": ["foreign"]},
    ):
        write_private(path, {**original, **changed})
        with pytest.raises(ValueError, match="authenticated failure"):
            custody.implementation_custody(spec, state, attempts, previous)
    write_private(path, original)
    changed = copy.deepcopy(attempts)
    result = json.loads(changed[-1]["result_json"])
    result["findings"] = ["unrelated provider failure"]
    changed[-1]["result_json"] = canonical_json(result)
    with pytest.raises(ValueError, match="authenticated failure"):
        custody.implementation_custody(spec, state, changed, previous)


def test_legacy_gate_chain_retains_the_real_policy_session_home(tmp_path):
    base = {"run_id": "run-1", "state_dir": str(tmp_path), "role_home_generation": "policy-1"}
    first = {**base, "role_home_generation": "gate-retry-1"}
    second = {**base, "role_home_generation": "report-retry-2"}
    recovery = {
        "kind": "published_gate_retry",
        "execution_spec": second,
        "seal": {"original_spec": first},
        "original_recovery": {
            "kind": "prepublication_gate_retry",
            "execution_spec": first,
            "seal": {"original_spec": base},
            "original_recovery": None,
        },
    }
    generation = custody.implementation_generation(second, recovery)
    assert generation == "policy-1"
    assert (
        _native_role_home(
            {
                "spec": {**second, "implementation_role_home_generation": generation},
                "role": "implement",
                "iteration": 2,
            }
        )
        == tmp_path / "role-homes/implement-policy-1"
    )
    assert (
        custody.implementation_generation(
            {**second, "implementation_role_home_generation": ""}, recovery
        )
        == ""
    )
