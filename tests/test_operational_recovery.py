"""Synthetic stopped workers cross the real CLI/store; no native host is called."""

import json
import os
import subprocess
import sys
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path

import pytest
from test_subagent_lifecycle import PARENT, Subagents

from devflow import __version__
from devflow.admission import derived_authority, user_request_admission
from devflow.domain.rules import implementation_completed, transition
from devflow.errors import WorkflowError
from devflow.validation import digest, validate_record


def stopped(tmp_path):
    s = Subagents(tmp_path, package_version=__version__)
    worker = s.start_role()
    s.complete(worker, status="blocked", output_candidate_id=None)
    s.cli("host observe", assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "agent_status": {"completed": "Synthetic partial output"},
        "source_reference": "synthetic:stopped-native-observation"})
    s.cli("work block", blocker={"code": "stopped_on_unexpected_invocation",
                                "reason": "Synthetic prerequisite failure before edits",
                                "next_action": "Validate the remedy and resume the same worker"})
    return s, s.state["assignments"][worker["assignment_id"]]


def recovery(s, worker):
    state = s.state
    snapshot = state["records"]["workflow_snapshot:" + state["attempt"]["workflow_snapshot_id"]]
    return {"schema_version": 1, "record_type": "operational_recovery", "recovery_id": "synthetic-remedy",
            "work_id": s.work_id, "attempt_id": state["attempt"]["attempt_id"],
            "blocked_revision": state["revision"], "blocker_hash": digest(state["blocker"]),
            "scope_hash": state["scope_hash"], "workflow_snapshot_id": snapshot["snapshot_id"],
            "workflow_snapshot_hash": digest(snapshot), "assignment_id": worker["assignment_id"],
            "assignment_action_id": worker["action_id"], "producer_task_id": worker["task_id"],
            "result_hash": digest(worker["implementation_result"]),
            "availability_hash": digest(worker["control_observation"]),
            "artifact_hash": s.service.put_artifact(b"Synthetic actual prerequisite correction and successful probe"),
            "source_reference": "synthetic:remediation-probe", "summary": "The failed prerequisite now succeeds",
            "observed_at": datetime.now(UTC).isoformat()}


def console(s, payload, *, expect=None):
    request = s.private / "recovery-console-request.json"
    request.write_text(json.dumps(payload))
    argv = [sys.executable, "-B", "-m", "devflow.cli", "work", "reconcile", "--request-file", str(request),
            "--repository", str(s.root), "--state-dir", str(s.private), "--json"]
    result = subprocess.run(argv, capture_output=True, text=True, env=os.environ | {
        "CODEX_THREAD_ID": PARENT, "PYTHONPATH": str(Path(__file__).parents[1] / "src")})
    with (s.private / "recovery-console-evidence.jsonl").open("a") as stream:
        stream.write(json.dumps({"argv": argv, "request": payload, "exit_code": result.returncode,
                                 "stdout": result.stdout, "stderr": result.stderr}) + "\n")
    output = json.loads(result.stdout)
    assert result.returncode == (2 if expect else 0), output
    if expect:
        assert output["error"]["code"] == expect, output
    return output


def test_partial_blocked_recovery_preserves_proof_and_reuses_worker(tmp_path):
    s, worker = stopped(tmp_path)
    proof = recovery(s, worker)
    before = s.state
    payload = {"operation_id": "recover-once", "work_id": s.work_id,
               "expected_revision": before["revision"], "clear_blocker": True, "operational_recovery": proof}
    console(s, payload | {"expected_revision": 0}, expect="stale_revision")
    assert s.state == before
    result = console(s, payload)
    after = s.state
    assert after["revision"] == before["revision"] + 1
    assert after["blocker"] is None and after["candidate_id"] is None
    for key in before:
        if key not in {"revision", "blocker", "attempt", "records", "history"}:
            assert after[key] == before[key], key
    assert after["attempt"] == before["attempt"] | {"revision": after["revision"], "blocker": None}
    assert after["history"][:-1] == before["history"]
    assert after["records"] == before["records"] | {"operational_recovery:synthetic-remedy": proof}
    assert not implementation_completed(after)
    assert console(s, payload) == result
    assert s.state == after
    console(s, payload | {"operational_recovery": proof | {"summary": "Changed acknowledgment"}},
            expect="operation_conflict")
    assert s.state == after
    next_action = s.service.next(s.work_id)["actions"][0]
    assert next_action["assignment_id"] == worker["assignment_id"]
    reused = s.assign(identity=worker["assignment_id"])
    assert reused["task_id"] == worker["task_id"] and reused["action_id"] != worker["action_id"]
    s.cli("host prepare", assignment_id=worker["assignment_id"])
    s.cli("host record", assignment_id=worker["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running"}])
    final = s.state
    assert final["candidate_id"] is None
    assert all(final["records"][key] == value for key, value in before["records"].items())
    for name, state in [("before", before), ("recovered", after), ("reactivated", final)]:
        (s.private / f"recovery-{name}-state.json").write_text(json.dumps(state, indent=2) + "\n")


@pytest.mark.parametrize("artifact_state", ["missing", "corrupt"])
def test_artifact_failure_rolls_back_entire_recovery(tmp_path, artifact_state):
    s, worker = stopped(tmp_path)
    proof = recovery(s, worker)
    artifact = s.private / "artifacts" / proof["artifact_hash"]
    if artifact_state == "missing":
        artifact.unlink()
    else:
        artifact.write_bytes(b"Synthetic changed bytes")
    before = s.state
    console(s, {"operation_id": "bad-artifact", "work_id": s.work_id, "expected_revision": before["revision"],
                "clear_blocker": True, "operational_recovery": proof}, expect=f"{artifact_state}_artifact")
    assert s.state == before


@pytest.mark.parametrize("field,value,code", [
    ("work_id", "other-work", "stale_recovery"), ("attempt_id", "other-attempt", "stale_recovery"),
    ("blocked_revision", 0, "stale_recovery"), ("blocker_hash", "0" * 64, "stale_recovery"),
    ("scope_hash", "0" * 64, "stale_recovery"), ("workflow_snapshot_id", "other-policy", "stale_recovery"),
    ("workflow_snapshot_hash", "0" * 64, "stale_recovery"), ("result_hash", "0" * 64, "stale_recovery"),
    ("availability_hash", "0" * 64, "stale_recovery"),
    ("assignment_id", "other-worker", "recovery_worker_mismatch"),
    ("producer_task_id", "other-task", "recovery_worker_mismatch"),
    ("assignment_action_id", "other-action", "recovery_result_mismatch"),
])
def test_cli_rejects_wrong_or_stale_recovery_binding(tmp_path, field, value, code):
    s, worker = stopped(tmp_path)
    proof = recovery(s, worker) | {field: value}
    before = s.state
    s.cli("work reconcile", clear_blocker=True, operational_recovery=proof, expect=code)
    assert s.state == before


@pytest.mark.parametrize("defect,code", [
    ("prepared", "reconcile_required"), ("dispatched", "reconcile_required"),
    ("ambiguous", "reconcile_required"), ("pending_setup", "reconcile_required"),
    ("missing_result", "implementation_result_required"), ("completed_result", "recovery_result_mismatch"),
    ("stale_result", "recovery_result_mismatch"), ("unavailable", "recovery_worker_unavailable"),
    ("running", "recovery_worker_unavailable"), ("stale_availability", "recovery_worker_unavailable"),
    ("missing_startup", "recovery_worker_mismatch"), ("pending_gate", "gate_result_required"),
    ("altered_authority", "admission_binding"), ("candidate", "invalid_recovery_state"),
    ("no_blocker", "invalid_recovery_state"), ("wrong_phase", "invalid_recovery_state"),
])
def test_pure_adversarial_state_does_not_bypass_recovery_guards(tmp_path, defect, code):
    s, worker = stopped(tmp_path)
    proof = recovery(s, worker)
    # Explicit adversarial exported-state copies; the persisted fixture stays untouched.
    state = s.state
    assignment = state["assignments"][worker["assignment_id"]]
    if defect in {"prepared", "dispatched", "ambiguous", "pending_setup"}:
        state["actions"][worker["action_id"]]["status"] = defect
    elif defect == "missing_result":
        assignment.pop("implementation_result")
    elif defect == "completed_result":
        assignment["implementation_result"]["status"] = "completed"
    elif defect == "stale_result":
        assignment["implementation_result"]["assignment_action_id"] = "previous-action"
    elif defect in {"unavailable", "running"}:
        assignment["status"] = defect
    elif defect == "stale_availability":
        assignment["control_observation"]["assignment_action_id"] = "previous-action"
    elif defect == "missing_startup":
        assignment.pop("startup_observation")
    elif defect == "pending_gate":
        state["assignments"]["pending-gate"] = assignment | {
            "assignment_id": "pending-gate", "role": "review", "gate_action_id": "unimported-action"}
    elif defect == "altered_authority":
        state["authority"]["allowed_operations"].remove("edit")
    elif defect == "candidate":
        state["candidate_id"] = "some-candidate"
    elif defect == "no_blocker":
        state["blocker"] = None
    elif defect == "wrong_phase":
        state["phase"] = "verify"
    before = deepcopy(state)
    with pytest.raises(WorkflowError) as error:
        transition(state, "work.reconcile", {"operation_id": "negative-state", "clear_blocker": True,
                   "operational_recovery": proof}, datetime.now(UTC), repository=s.repository)
    assert error.value.code == code
    assert state == before


@pytest.mark.parametrize("delta", ["revision", "title", "snapshot_id", "real_scope", "real_policy"])
def test_blocked_amendment_requires_an_operative_delta_to_clear(tmp_path, delta):
    s, worker = stopped(tmp_path)
    before = s.state
    contract = deepcopy(s.contract) | {"scope_revision": 2}
    fields = {}
    if delta == "title":
        contract["title"] += " editorial rename"
    if delta == "real_scope":
        contract["scope"]["paths"].append("AGENTS.md")
    if delta in {"snapshot_id", "real_policy"}:
        snapshot = deepcopy(before["records"]["workflow_snapshot:" + before["attempt"]["workflow_snapshot_id"]])
        fields["workflow_snapshot"] = snapshot | {"snapshot_id": "new-id", "captured_at": datetime.now(UTC).isoformat()}
        if delta == "real_policy":
            fields["workflow_snapshot"]["workflow_hash"] = "1" * 64
    s.call("work.amend", record=contract, user_request={
        "reference": "synthetic:accepted-delta", "summary": "Synthetic authorized amendment",
        "allowed_operations": ["edit", "check", "create_tasks"]}, **fields)
    after = s.state
    assert after["blocker"] == (None if delta in {"real_scope", "real_policy"} else before["blocker"])
    assert all(after["records"][key] == value for key, value in before["records"].items())
    assert after["assignments"] == before["assignments"]


def test_missing_proof_and_candidate_observation_rules_remain_strict(tmp_path):
    s, worker = stopped(tmp_path)
    before = s.state
    s.cli("work reconcile", clear_blocker=True, expect="missing_evidence")
    s.cli("work reconcile", clear_blocker=True, evidence_ids=["invented"], expect="unknown_evidence")
    proof = recovery(s, worker)
    s.cli("work reconcile", operational_recovery=proof, expect="invalid_request")
    s.cli("work reconcile", clear_blocker=True, operational_recovery=proof | {"candidate_id": None}, expect="invalid_record")
    with pytest.raises(WorkflowError) as error:
        validate_record({"schema_version": 1, "record_type": "observation_evidence", "candidate_id": None},
                        "observation_evidence")
    assert error.value.code == "invalid_record"
    assert s.state == before


def test_recovery_permission_guard_with_consistent_narrow_admission(tmp_path):
    s, worker = stopped(tmp_path)
    proof = recovery(s, worker)
    # Pure boundary fixture with internally consistent admission and derived authority.
    # This does not claim a normal local-endpoint admission can start without edit permission.
    state = s.state
    original = state["records"]["intake_admission:" + state["admission_id"]]
    narrowed_request = original["user_request"] | {"allowed_operations": ["check", "create_tasks"]}
    admission = user_request_admission(state, state["contract"], narrowed_request, s.repository)
    state["admission_id"] = admission["admission_id"]
    state["records"]["intake_admission:" + admission["admission_id"]] = admission
    state["authority"] = derived_authority(admission)
    state["records"]["authority:" + state["authority"]["authority_id"]] = deepcopy(state["authority"])
    state["attempt"]["authority_id"] = state["authority"]["authority_id"]
    before = deepcopy(state)
    with pytest.raises(WorkflowError) as error:
        transition(state, "work.reconcile", {"operation_id": "negative-permission", "clear_blocker": True,
                   "operational_recovery": proof}, datetime.now(UTC), repository=s.repository)
    assert error.value.code == "missing_authority"
    assert state == before
