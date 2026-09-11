"""Synthetic host receipts cross the real CLI/store; no native agents are launched."""

from __future__ import annotations

import io
import itertools
import json
import os
import shutil
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch
from uuid import NAMESPACE_URL, uuid5

import pytest
from domain.helpers import NOW, SHA, H, Scenario, contract, record, workflow_snapshot

from devflow.application.commands import WorkflowService
from devflow.cli import main
from devflow.errors import WorkflowError

PARENT = "11111111-1111-4111-8111-111111111111"


class Subagents(Scenario):
    def __init__(self, path, *, tier=0, phase="implement", package_version=None):
        self.root = path / "repository"
        shutil.copytree(Path(__file__).parents[1] / "fixtures/repositories/prose", self.root)
        self.private = path / "private"
        self.service = WorkflowService(self.private, repository="synthetic:prose-fixture")
        self.repository = "synthetic:prose-fixture"
        self.sequence = itertools.count()
        self.work_id = "synthetic-subagents"
        self.contract = contract(self.work_id, tier)
        self.call("work.ready", record=self.contract, user_request={
            "reference": "synthetic:direct-user-request", "summary": "Synthetic delegated outcome",
            "allowed_operations": ["edit", "check", "create_tasks"],
        })
        attempt = record(
            "attempt", attempt_id="synthetic-attempt", work_id=self.work_id,
            scope_hash=self.state["scope_hash"], authority_id=self.state["authority"]["authority_id"],
            host_id="synthetic-host", owner_task_id=PARENT, phase=phase, blocker=None,
            workflow_snapshot_id="snapshot-1", model_policy_snapshot_id="snapshot-1",
            revision=self.state["revision"], started_at=NOW, status="active",
        )
        snapshot = workflow_snapshot()
        if package_version:
            snapshot["package_version"] = package_version
        started = self.call("work.start", record=attempt, workflow_snapshot=snapshot)
        self.confirm(started["action"], external_id="synthetic-workspace")
        self.config = path / "config.toml"
        self.config.write_text('model="synthetic-coordinator"\nmodel_reasoning_effort="max"\n')
        roles = path / "agents"
        roles.mkdir()
        for role in ("implementer", "reviewer", "qa"):
            (roles / f"{role}.toml").write_text(
                f'model="synthetic-{role}"\nmodel_reasoning_effort="high"\n'
            )
        self.sessions = path / "sessions"
        self.sessions.mkdir()

    def cli(self, command, *, expect=None, **fields):
        payload = {"work_id": self.work_id, "operation_id": f"cli-{next(self.sequence)}",
                   "expected_revision": self.state["revision"], **fields}
        request = self.private / "synthetic-cli-request.json"
        request.write_text(json.dumps(payload))
        output = io.StringIO()
        with redirect_stdout(output), patch.dict(os.environ, {"CODEX_THREAD_ID": PARENT}):
            code = main([*command.split(), "--request-file", str(request),
                         "--repository", str(self.root), "--state-dir", str(self.private), "--json"])
        envelope = json.loads(output.getvalue())
        if expect:
            assert code == 2, envelope
            assert envelope["error"]["code"] == expect, envelope
            return envelope["error"]
        assert code == 0, envelope
        return envelope["result"]

    def assign(self, role="implementation_worker", identity=None, **fields):
        return self.cli(
            "host assign", assignment_id=identity or f"assignment-{role}", role=role,
            owned_paths=["src"], workspace_reference="synthetic:owned-worktree",
            brief="Perform only the bounded synthetic assignment; preserve shared edits.",
            config_path=str(self.config), **fields,
        )["assignment"]

    def startup_source(self, assignment, *, task_id=None, meta_delta=None, turn_delta=None):
        task_id = task_id or str(uuid5(NAMESPACE_URL, assignment["assignment_id"]))
        path = self.sessions / f"rollout-synthetic-{task_id}.jsonl"
        meta = {"id": task_id, "parent_thread_id": PARENT,
                "agent_path": assignment["agent_name"], "thread_source": "subagent"}
        meta.update(meta_delta or {})
        turn = {"turn_id": "synthetic-turn", "model": assignment["role_policy"]["model"],
                "effort": assignment["role_policy"]["reasoning_effort"]}
        turn.update(turn_delta or {})
        path.write_text("\n".join(json.dumps(x) for x in [
            {"type": "session_meta", "payload": meta},
            {"type": "response_item", "payload": "SYNTHETIC PRIVATE BODY MUST NOT BE STORED"},
            {"type": "turn_context", "timestamp": NOW, "payload": turn},
        ]) + "\n")
        return {"task_id": task_id, "session_path": str(path), "session_meta_line": 1,
                "turn_context_line": 3, "sessions_root": str(self.sessions)}

    def launch(self, assignment):
        intent = self.cli("host prepare", assignment_id=assignment["assignment_id"])["intent"]
        assert intent["native_tool"] == "spawn_agent"
        self.cli("host record", assignment_id=assignment["assignment_id"],
                 response={"task_name": assignment["agent_name"], "nickname": "Synthetic"})
        return intent

    def start_role(self, role="implementation_worker", identity=None, *, task_id=None):
        assignment = self.assign(role, identity)
        self.launch(assignment)
        self.cli("host startup", assignment_id=assignment["assignment_id"],
                 **self.startup_source(assignment, task_id=task_id))
        return self.activate(assignment["assignment_id"])

    def activate(self, identity):
        assignment = self.state["assignments"][identity]
        self.cli("host activate", assignment_id=identity)
        intent = self.cli("host prepare", assignment_id=identity)["intent"]
        assert intent["native_tool"] == "followup_task"
        assert intent["arguments"]["target"] == assignment["agent_name"]
        return self.cli("host record", assignment_id=identity, inventory=[{
            "agent_name": assignment["agent_name"], "agent_status": "running",
        }])["assignment"]

    def capture(self, worker=None, identity="candidate-1", tree=SHA, *, expect=None):
        candidate = record(
            "candidate", candidate_id=identity, attempt_id=self.state["attempt"]["attempt_id"],
            scope_hash=self.state["scope_hash"], repository=self.repository, base_sha=SHA,
            head_sha=tree, tree_sha=tree, clean=True, dependency_hash=H, environment_hash=H,
            created_at=NOW,
        )
        return self.cli("candidate record", record=candidate, expect=expect,
                        **({"assignment_id": worker["assignment_id"],
                            "producer_task_id": worker["task_id"]} if worker else {}))

    def complete(self, worker, **changes):
        result = {"assignment_id": worker["assignment_id"], "candidate_id": worker["candidate_id"],
                  "producer_role": "implementation_worker", "status": "completed",
                  "output_candidate_id": self.state["candidate_id"],
                  "evidence_reference": "synthetic:implementation-evidence", **changes}
        return self.cli("host result", assignment_id=worker["assignment_id"], result=result,
                        observed_task_id=worker["task_id"])


def test_new_work_defaults_to_coordinator_and_explicit_role_launch(tmp_path):
    s = Subagents(tmp_path)
    assert s.state["attempt"]["execution_mode"] == "subagent"
    assert s.state["attempt"]["owner_task_id"] == PARENT
    assert s.service.next(s.work_id)["actions"][0]["role"] == "implementation_worker"
    s.capture(expect="implementation_required")
    assignment = s.assign()
    intent = s.launch(assignment)
    assert intent["arguments"]["model"] == "synthetic-implementer"
    assert intent["arguments"]["reasoning_effort"] == "high"
    assert intent["arguments"]["agent_type"] == "default"
    assert intent["arguments"]["fork_turns"] == "none"
    assert "Do not edit product" in intent["arguments"]["message"]
    pending = s.state["assignments"][assignment["assignment_id"]]
    assert pending["task_id"] is None and pending["status"] == "pending_startup"
    s.cli("host activate", assignment_id=assignment["assignment_id"], expect="startup_unverified")
    ready = s.cli("host startup", assignment_id=assignment["assignment_id"],
                  **s.startup_source(assignment))["assignment"]
    assert ready["status"] == "ready" and ready["startup_observation"]["service_tier"] is None
    segment = next(r for r in s.state["records"].values() if r.get("record_type") == "execution_segment")
    assert segment["task_id"] == ready["task_id"]
    assert segment["model_id"] == ready["role_policy"]["model"]
    assert segment["model_policy_hash"] == ready["role_policy"]["policy_hash"]
    assert segment["service_tier"] is None
    artifact = s.service.store.root / "artifacts" / ready["startup_observation"]["artifact_hash"]
    assert b"SYNTHETIC PRIVATE BODY" not in artifact.read_bytes()


@pytest.mark.parametrize("meta_delta,turn_delta,code", [
    ({"parent_thread_id": "wrong-parent"}, {}, "startup_identity_mismatch"),
    ({"agent_path": "/root/wrong"}, {}, "startup_identity_mismatch"),
    ({"thread_source": "user"}, {}, "startup_identity_mismatch"),
    ({}, {"model": "synthetic-coordinator"}, "startup_policy_mismatch"),
    ({}, {"effort": "max"}, "startup_policy_mismatch"),
    ({}, {"effort": None}, "startup_policy_mismatch"),
])
def test_startup_rejects_wrong_native_identity_or_actual_settings(tmp_path, meta_delta, turn_delta, code):
    s = Subagents(tmp_path)
    assignment = s.assign()
    s.launch(assignment)
    s.cli("host startup", assignment_id=assignment["assignment_id"], expect=code,
          **s.startup_source(assignment, meta_delta=meta_delta, turn_delta=turn_delta))
    assert s.state["assignments"][assignment["assignment_id"]]["task_id"] is None


def test_dispatched_launch_recovery_across_new_cli_process_never_spawns_twice(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    s.cli("host prepare", assignment_id=assignment["assignment_id"])
    request = s.private / "replay.json"
    request.write_text(json.dumps({"work_id": s.work_id, "operation_id": "different-process",
                                  "expected_revision": s.state["revision"],
                                  "assignment_id": assignment["assignment_id"]}))
    run = subprocess.run([sys.executable, "-m", "devflow.cli", "host", "prepare", "--json",
                          "--request-file", str(request), "--repository", str(s.root),
                          "--state-dir", str(s.private)], capture_output=True, text=True, check=False,
                         env={**os.environ, "CODEX_THREAD_ID": PARENT})
    assert run.returncode == 0, run.stdout
    assert json.loads(run.stdout)["result"]["reconcile_only"] is True
    s.cli("host reconcile", assignment_id=assignment["assignment_id"], inventory=[],
          expect="ambiguous_host_action")
    result = s.cli("host reconcile", assignment_id=assignment["assignment_id"], inventory=[{
        "agent_name": assignment["agent_name"], "agent_status": "running",
    }])["assignment"]
    assert result["status"] == "pending_startup" and result["task_id"] is None


@pytest.mark.parametrize("roles", [("implementation_worker", "review"), ("review", "implementation_worker"),
                                  ("review", "qa"), ("qa", "review")])
def test_delegated_identities_are_distinct_in_both_registration_orders(tmp_path, roles):
    s = Subagents(tmp_path, tier=2, phase="verify")
    s.capture()
    first = s.start_role(roles[0])
    if roles[0] == "implementation_worker":
        s.capture(first, identity="implemented-candidate")
        s.complete(first)
    second = s.assign(roles[1])
    s.launch(second)
    s.cli("host startup", assignment_id=second["assignment_id"], expect="not_independent",
          **s.startup_source(second, task_id=first["task_id"]))


def test_coordinator_uuid_cannot_be_a_delegated_product_identity(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    s.launch(assignment)
    s.cli("host startup", assignment_id=assignment["assignment_id"], expect="not_independent",
          **s.startup_source(assignment, task_id=PARENT))


def test_control_path_cannot_be_dispatched_from_another_coordinator_tree(tmp_path):
    from devflow.execution import dispatch_action

    s = Subagents(tmp_path)
    assignment = s.assign()
    with patch.dict(os.environ, {"CODEX_THREAD_ID": "another-native-coordinator"}):
        with pytest.raises(WorkflowError, match="recorded coordinator UUID"):
            dispatch_action(s.service, {
                "work_id": s.work_id, "operation_id": "wrong-tree",
                "expected_revision": s.state["revision"], "action_id": assignment["action_id"],
            }, repository=s.root)
    assert s.state["actions"][assignment["action_id"]]["status"] == "prepared"


def test_legacy_request_shape_cannot_bypass_subagent_coordinator_guard(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    request = s.private / "legacy-shaped-subagent.json"
    request.write_text(json.dumps({"work_id": s.work_id, "operation_id": "legacy-bypass",
                                  "expected_revision": s.state["revision"],
                                  "assignment": assignment, "brief": "Unadmitted startup"}))
    process = subprocess.run([sys.executable, "-m", "devflow.cli", "host", "prepare", "--json",
                              "--request-file", str(request), "--repository", str(s.root),
                              "--state-dir", str(s.private)], capture_output=True, text=True, check=False,
                             env={**os.environ, "CODEX_THREAD_ID": "wrong-coordinator"})
    assert process.returncode == 2
    assert json.loads(process.stdout)["error"]["code"] == "managed_host_required"
    assert s.state["actions"][assignment["action_id"]]["status"] == "prepared"


def test_concurrent_native_preparation_returns_only_one_executable_intent(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    request = s.private / "concurrent-prepare.json"
    request.write_text(json.dumps({"work_id": s.work_id, "operation_id": "concurrent-host-prepare",
                                  "expected_revision": s.state["revision"],
                                  "assignment_id": assignment["assignment_id"]}))

    def invoke(_):
        process = subprocess.run([sys.executable, "-m", "devflow.cli", "host", "prepare", "--json",
                                  "--request-file", str(request), "--repository", str(s.root),
                                  "--state-dir", str(s.private)], capture_output=True, text=True,
                                 check=False, env={**os.environ, "CODEX_THREAD_ID": PARENT})
        assert process.returncode == 0, process.stdout
        return json.loads(process.stdout)["result"]

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(invoke, range(2)))
    assert sum("intent" in result for result in results) == 1
    assert sum(result.get("reconcile_only") is True for result in results) == 1


@pytest.mark.parametrize("phase", ["verify", "deliver"])
def test_entry_exemption_ends_when_implementation_is_assigned(tmp_path, phase):
    s = Subagents(tmp_path, phase=phase)
    s.capture()
    s.check()
    worker = s.start_role()
    s.capture(identity="candidate-2", tree="c" * 40, expect="implementation_required")
    with pytest.raises(WorkflowError, match="delegated implementation result"):
        s.check("check-with-active-worker")
    s.capture(worker, "candidate-2", "c" * 40)
    s.complete(worker)
    assert s.check("check-2")["execution_status"] == "PASS"


def test_gate_cannot_consume_proof_while_a_new_implementation_turn_is_active(tmp_path):
    s = Subagents(tmp_path, tier=2)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    evidence = s.check()
    reviewer = s.start_role("review")
    s.assign()
    gate = record(
        "gate_result", gate_id="stale-implementation-gate", assignment_id=reviewer["assignment_id"],
        producer_task_id=reviewer["task_id"], role="review", candidate_id=s.state["candidate_id"],
        scope_hash=s.state["scope_hash"], workflow_hash=H, status="PASS",
        evidence_ids=[evidence["evidence_id"]], finding_ids=[], blocking_finding_ids=[],
        limitations=[], completed_at=NOW, fix_verification_ids=[],
    )
    s.cli("gate record", record=gate)
    assert s.state["records"]["gate_ingestion:stale-implementation-gate"]["historical"]
    assert "review" not in s.state["gate_ids"]


@pytest.mark.parametrize("boundary", ["begin", "dispatch", "readback"])
def test_terminal_boundaries_recheck_current_implementation_completion(tmp_path, boundary):
    s = Subagents(tmp_path)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    s.check()
    action = s.cli("deliver", candidate_id=s.state["candidate_id"])["action"]
    s.assign()
    if boundary == "begin":
        s.cli("action begin", action_id=action["action_id"], expect="implementation_incomplete")
    elif boundary == "dispatch":
        s.cli("action dispatch", action_id=action["action_id"], expect="implementation_incomplete")
    else:
        candidate = s.state["records"]["candidate:" + s.state["candidate_id"]]
        observation = {"action_id": action["action_id"], "payload_hash": action["payload_hash"],
                       "candidate_id": candidate["candidate_id"], "head_sha": candidate["head_sha"],
                       "tree_sha": candidate["tree_sha"], "endpoint": s.contract["endpoint"],
                       "path": s.contract["endpoint"]["target"], "verified": True,
                       "independent_readback": True}
        receipt = s.confirm(action, observation=observation)
        delivery = record(
            "delivery", delivery_id="stale-delivery", work_id=s.work_id,
            attempt_id=s.state["attempt"]["attempt_id"], candidate_id=candidate["candidate_id"],
            authority_id=s.state["authority"]["authority_id"], endpoint=s.contract["endpoint"],
            gate_ids=[], action_id=action["action_id"], receipt_id=receipt["receipt_id"],
            observed_result="Synthetic previous endpoint", delivered_at=NOW,
            merge_binding=None, resulting_merge=None, status="verified",
        )
        s.cli("deliver", record=delivery, observation=observation, expect="implementation_incomplete")
    assert s.state["lifecycle"] == "active"


def test_same_agent_repair_invalidates_completion_and_binds_new_output(tmp_path):
    s = Subagents(tmp_path)
    worker = s.start_role()
    s.capture(worker)
    with pytest.raises(WorkflowError, match="delegated implementation result"):
        s.check()
    s.complete(worker)
    s.check()
    reused = s.assign()
    assert reused["task_id"] == worker["task_id"]
    assert reused["agent_name"] == worker["agent_name"]
    intent = s.cli("host prepare", assignment_id=reused["assignment_id"])["intent"]
    assert intent["native_tool"] == "followup_task"
    s.cli("host record", assignment_id=reused["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running",
    }])
    s.capture(reused, "candidate-2", "c" * 40)
    with pytest.raises(WorkflowError, match="delegated implementation result"):
        s.check("check-2")
    bad = {"assignment_id": reused["assignment_id"], "candidate_id": reused["candidate_id"],
           "producer_role": "implementation_worker", "status": "completed",
           "output_candidate_id": "candidate-1", "evidence_reference": "synthetic:old-proof"}
    s.cli("host result", assignment_id=reused["assignment_id"], result=bad,
          observed_task_id=reused["task_id"], expect="result_mismatch")
    s.cli("host result", assignment_id=reused["assignment_id"], result=bad,
          observed_task_id=PARENT, expect="producer_mismatch")
    s.complete(reused)
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "run_check"


def test_policy_change_needs_observed_replacement_and_preserves_old_identity(tmp_path):
    s = Subagents(tmp_path)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    (tmp_path / "agents/implementer.toml").write_text(
        'model="synthetic-new-model"\nmodel_reasoning_effort="low"\n'
    )
    s.cli("host assign", assignment_id=worker["assignment_id"], role="implementation_worker",
          owned_paths=["src"], brief="Bounded repair", config_path=str(s.config),
          expect="role_policy_changed")
    s.cli("host assign", assignment_id="replacement", role="implementation_worker",
          owned_paths=["src"], brief="Bounded repair", config_path=str(s.config),
          replaces_assignment_id=worker["assignment_id"], expect="replacement_unobserved")
    artifact = s.service.put_artifact(b"Synthetic native target-error evidence; no actual host error shape asserted")
    s.cli("host unavailable", assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "observation_kind": "native_target_error",
        "reason": "Synthetic target cannot be resolved", "artifact_hash": artifact,
        "source_reference": "synthetic:native-target-error",
    })
    replacement = s.assign(identity="replacement", replaces_assignment_id=worker["assignment_id"])
    old = s.state["assignments"][worker["assignment_id"]]
    assert old["status"] == "replaced" and old["task_id"] == worker["task_id"]
    assert replacement["agent_name"] != old["agent_name"]
    assert replacement["role_policy"]["model"] == "synthetic-new-model"


def test_fast_completed_native_status_can_confirm_a_followup(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    s.launch(assignment)
    s.cli("host startup", assignment_id=assignment["assignment_id"], **s.startup_source(assignment))
    s.cli("host activate", assignment_id=assignment["assignment_id"])
    s.cli("host prepare", assignment_id=assignment["assignment_id"])
    running = s.cli("host record", assignment_id=assignment["assignment_id"], inventory=[{
        "agent_name": assignment["agent_name"],
        "agent_status": {"completed": "Synthetic product result arrives before the inventory read"},
    }])["assignment"]
    assert running["status"] == "running"
    action = s.state["actions"][running["action_id"]]
    assert action["observation"]["agent_status"] == "completed"
    assert action["observation"]["status_evidence_kind"] == "completed_object"


def test_explicit_policy_change_can_replace_a_completed_available_agent(tmp_path):
    s = Subagents(tmp_path)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    (tmp_path / "agents/implementer.toml").write_text(
        'model="synthetic-new-model"\nmodel_reasoning_effort="low"\n'
    )
    replacement = s.assign(identity="new-policy", replaces_assignment_id=worker["assignment_id"],
                           replacement_observation={
                               "reason": "policy_change", "agent_name": worker["agent_name"],
                               "agent_status": {"completed": "Synthetic previous result"},
                               "source_reference": "synthetic:list_agents:completed",
                               "requested_change_reference": "synthetic:user:new-role-policy",
                           })
    assert replacement["replacement_observation"]["reason"] == "policy_change"
    old = s.state["assignments"][worker["assignment_id"]]
    assert old["status"] == "replaced" and "unavailable_observation" not in old


def test_bootstrap_response_joins_the_verified_native_segment(tmp_path):
    from devflow.adapters.responses import import_responses, response_key

    s = Subagents(tmp_path)
    assignment = s.assign()
    s.launch(assignment)
    ready = s.cli("host startup", assignment_id=assignment["assignment_id"],
                  **s.startup_source(assignment))["assignment"]
    segment = next(r for r in s.state["records"].values() if r.get("record_type") == "execution_segment")
    assert segment["started_at"] == NOW
    native = {"type": "token_usage_record", "timestamp": NOW, "payload": {
        "thread_id": ready["task_id"], "response_id": "synthetic-bootstrap-response",
        "model": ready["role_policy"]["model"], "usage": {"input_tokens": 5, "output_tokens": 3},
    }}
    key = response_key(ready["task_id"], "synthetic-bootstrap-response")
    rows = import_responses([native], ccusage_version="synthetic-version", segments={segment["segment_id"]: segment},
                            assignments={key: {"segment_id": segment["segment_id"],
                                               "attempt_id": segment["attempt_id"],
                                               "allocations": [{"work_id": s.work_id, "weight": "1"}]}})
    assert rows[0]["task_id"] == ready["task_id"]
    assert rows[0]["attribution_status"] == "complete"
    s.call("usage.record", record=rows[0])


def test_startup_readback_replay_retains_the_first_verification_and_segment(tmp_path):
    s = Subagents(tmp_path)
    assignment = s.assign()
    s.launch(assignment)
    source = s.startup_source(assignment)
    request = {"assignment_id": assignment["assignment_id"], "operation_id": "stable-startup",
               "expected_revision": s.state["revision"], **source}
    first = s.cli("host startup", **request)
    assert s.cli("host startup", **request) == first
    segments = [r for r in s.state["records"].values() if r.get("record_type") == "execution_segment"]
    assert len(segments) == 1


def test_cli_subagent_lifecycle_reaches_verified_local_delivery(tmp_path):
    s = Subagents(tmp_path, tier=2)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    evidence = s.check()
    for role in ("review", "qa"):
        peer = s.start_role(role)
        gate = record(
            "gate_result", gate_id=f"gate-{role}", assignment_id=peer["assignment_id"],
            producer_task_id=peer["task_id"], role=role, candidate_id=s.state["candidate_id"],
            scope_hash=s.state["scope_hash"], workflow_hash=H, status="PASS",
            evidence_ids=[evidence["evidence_id"]], finding_ids=[], blocking_finding_ids=[],
            limitations=[], completed_at=NOW, fix_verification_ids=[],
        )
        s.cli("gate record", record=gate)
    action = s.cli("deliver", candidate_id=s.state["candidate_id"])["action"]
    candidate = s.state["records"]["candidate:" + s.state["candidate_id"]]
    observation = {"action_id": action["action_id"], "payload_hash": action["payload_hash"],
                   "candidate_id": candidate["candidate_id"], "head_sha": candidate["head_sha"],
                   "tree_sha": candidate["tree_sha"], "endpoint": s.contract["endpoint"],
                   "path": s.contract["endpoint"]["target"], "verified": True,
                   "independent_readback": True}
    receipt = record("action_receipt", **{k: action[k] for k in (
        "action_id", "attempt_id", "operation", "payload_hash", "expected_revision")},
        status="confirmed", external_id="synthetic-local-endpoint",
        observations=["Synthetic endpoint independently observed"], recorded_at=NOW)
    receipt_id = s.cli("action record", record=receipt, observation=observation)["receipt_id"]
    delivery = record(
        "delivery", delivery_id="synthetic-delivery", work_id=s.work_id,
        attempt_id=s.state["attempt"]["attempt_id"], candidate_id=candidate["candidate_id"],
        authority_id=s.state["authority"]["authority_id"], endpoint=s.contract["endpoint"],
        gate_ids=action["payload"]["gate_ids"], action_id=action["action_id"], receipt_id=receipt_id,
        observed_result="Synthetic local verified delivery", delivered_at=NOW,
        merge_binding=None, resulting_merge=None, status="verified",
    )
    assert s.cli("deliver", record=delivery, observation=observation)["lifecycle"] == "done"


@pytest.mark.parametrize("command", ["host observe", "host unavailable"])
def test_interrupted_agent_resumes_with_same_verified_identity(tmp_path, command):
    s = Subagents(tmp_path)
    worker = s.start_role()
    interrupted = s.cli(command, assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "agent_status": "interrupted",
        "source_reference": "synthetic:list_agents:after_interrupt",
    })["assignment"]
    assert interrupted["status"] == "interrupted"
    assert "unavailable_observation" not in interrupted
    next_action = s.service.next(s.work_id)["actions"][0]
    assert next_action["operation"] == "send_role"
    assert next_action["reuse_task_id"] == worker["task_id"]
    reused = s.assign(identity=worker["assignment_id"])
    assert reused["task_id"] == worker["task_id"]
    assert reused["startup_observation"] == worker["startup_observation"]
    intent = s.cli("host prepare", assignment_id=reused["assignment_id"])["intent"]
    assert intent["native_tool"] == "followup_task"
    assert intent["arguments"]["target"] == worker["agent_name"]
    running = s.cli("host record", assignment_id=reused["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running",
    }])["assignment"]
    s.capture(running)
    s.complete(running)
    segments = [r for r in s.state["records"].values() if r.get("record_type") == "execution_segment"]
    assert len(segments) == 1


def test_control_observation_does_not_reconcile_an_unresolved_launch(tmp_path):
    s = Subagents(tmp_path)
    worker = s.assign()
    s.launch(worker)
    observed = s.cli("host observe", assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "agent_status": "interrupted",
        "source_reference": "synthetic:list_agents:interrupted_bootstrap",
    })["assignment"]
    assert observed["status"] == "pending_startup" and observed["task_id"] is None
    assert s.state["actions"][worker["action_id"]]["status"] == "pending_setup"
    s.cli("host assign", assignment_id=worker["assignment_id"], role="implementation_worker",
          owned_paths=["src"], brief="Bounded continuation", config_path=str(s.config),
          expect="assignment_state")


def test_completed_control_observation_does_not_promote_blocked_implementation(tmp_path):
    from devflow.domain.rules import implementation_completed

    s = Subagents(tmp_path)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker, status="blocked")
    s.cli("host observe", assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "agent_status": {"completed": "Private synthetic final body"},
        "source_reference": "synthetic:list_agents:completed_blocked_turn",
    })
    assert not implementation_completed(s.state)
    assert "Private synthetic final body" not in json.dumps(s.state)


def test_stale_running_reviewer_can_be_observed_then_reused(tmp_path):
    from devflow.domain.rules import role_next

    s = Subagents(tmp_path, tier=2)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    reviewer = s.start_role("review")
    s.assign(identity=worker["assignment_id"])
    s.cli("host prepare", assignment_id=worker["assignment_id"])
    worker = s.cli("host record", assignment_id=worker["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running",
    }])["assignment"]
    s.capture(worker, identity="candidate-repair", tree="b" * 40)
    s.complete(worker)
    next_action = role_next(s.state, "review")
    assert next_action["kind"] == "observe_role"
    s.cli("host observe", assignment_id=reviewer["assignment_id"], observation={
        "agent_name": reviewer["agent_name"], "agent_status": {"completed": "Synthetic previous review"},
        "source_reference": "synthetic:list_agents:previous_review_completed",
    })
    reused = s.assign("review")
    assert reused["task_id"] == reviewer["task_id"]
    assert reused["candidate_id"] == "candidate-repair"
    intent = s.cli("host prepare", assignment_id=reused["assignment_id"])["intent"]
    assert intent["native_tool"] == "followup_task"


@pytest.mark.parametrize("command", ["host observe", "host unavailable"])
@pytest.mark.parametrize("native_status", ["running", "interrupted", {"completed": "Synthetic bootstrap"}])
def test_bootstrap_control_observation_cannot_bypass_product_activation(tmp_path, command, native_status):
    s = Subagents(tmp_path)
    worker = s.assign()
    s.launch(worker)
    ready = s.cli("host startup", assignment_id=worker["assignment_id"],
                  **s.startup_source(worker))["assignment"]
    observed = s.cli(command, assignment_id=worker["assignment_id"], observation={
        "agent_name": worker["agent_name"], "agent_status": native_status,
        "source_reference": "synthetic:list_agents:bootstrap_only",
    })["assignment"]
    assert observed["status"] == "ready"
    assert s.state["actions"][observed["action_id"]]["operation"] == "launch_role"
    s.capture(observed, expect="implementation_required")
    s.cli("host result", assignment_id=observed["assignment_id"],
          observed_task_id=ready["task_id"], expect="startup_unverified", result={
              "assignment_id": observed["assignment_id"], "candidate_id": None,
              "producer_role": "implementation_worker", "status": "completed",
              "output_candidate_id": "unadmitted-candidate",
              "evidence_reference": "synthetic:bootstrap_is_not_product_evidence",
          })
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "activate_role"
    active = s.activate(worker["assignment_id"])
    s.capture(active)
    s.complete(active)
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "run_check"


def staged_gate(s, peer, status="PASS", *, identity=None, findings=None, evidence_ids=None):
    gate = record(
        "gate_result", gate_id=identity or f"gate-{peer['role']}-{status}",
        assignment_id=peer["assignment_id"], producer_task_id=peer["task_id"], role=peer["role"],
        candidate_id=peer["candidate_id"], scope_hash=peer["scope_hash"],
        workflow_hash=s.state["records"]["workflow_snapshot:" + peer.get(
            "workflow_snapshot_id", s.state["attempt"]["workflow_snapshot_id"])]["workflow_hash"],
        status=status, evidence_ids=evidence_ids or ["check-1"], finding_ids=findings or [],
        blocking_finding_ids=[], limitations=["Synthetic producer limitation"] if status != "PASS" else [],
        completed_at=NOW, fix_verification_ids=[],
        **({"assignment_action_id": peer["gate_action_id"]} if peer.get("gate_action_id") else {}),
    )
    if peer.get("gate_action_id"):
        gate["producer_result_artifact_hash"] = s.service.put_artifact(json.dumps(gate).encode())
    return gate


def staged_candidate(tmp_path, *, tier=2, version="0.5.0", phase="implement"):
    s = Subagents(tmp_path, tier=tier, phase=phase, package_version=version)
    worker = s.start_role() if phase == "implement" else None
    s.capture(worker)
    if worker:
        s.complete(worker)
    s.check()
    return s, worker


@pytest.mark.parametrize("first_status,second_status", [("FAIL", "BLOCKED"), ("BLOCKED", "FAIL")])
def test_initial_nonpassing_results_are_each_imported_before_repair(tmp_path, first_status, second_status):
    s, worker = staged_candidate(tmp_path)
    reviewer = s.start_role("review")
    qa = s.start_role("qa")
    for peer in (reviewer, qa):
        s.cli("host observe", assignment_id=peer["assignment_id"], observation={
            "agent_name": peer["agent_name"], "agent_status": {"completed": "Synthetic result"},
            "source_reference": "synthetic:completed-independent-result",
        })
    s.cli("host assign", assignment_id=worker["assignment_id"], role="implementation_worker",
          owned_paths=["src"], brief="Repair synthetic defects", config_path=str(s.config),
          expect="gate_result_required")
    s.cli("host activate", assignment_id=reviewer["assignment_id"], expect="startup_unverified")
    first = staged_gate(s, reviewer, first_status)
    s.cli("gate record", record=first)
    # One import must work while the second result is still missing: no all-gates deadlock.
    assert s.state["records"][f"gate_result:{first['gate_id']}"] == first
    assert [a["role"] for a in s.service.next(s.work_id)["actions"]] == ["qa"]
    s.cli("host assign", assignment_id=worker["assignment_id"], role="implementation_worker",
          owned_paths=["src"], brief="Repair synthetic defects", config_path=str(s.config),
          expect="gate_result_required")
    second = staged_gate(s, qa, second_status)
    s.cli("gate record", record=second)
    reused = s.assign(identity=worker["assignment_id"])
    assert reused["task_id"] == worker["task_id"]
    assert s.state["records"][f"gate_result:{second['gate_id']}"] == second


def test_completed_or_unavailable_independent_agent_cannot_be_replaced_before_import(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=1)
    peer = s.start_role("review")
    artifact = s.service.put_artifact(b"Synthetic native unavailable evidence")
    s.cli("host unavailable", assignment_id=peer["assignment_id"], observation={
        "agent_name": peer["agent_name"], "source_reference": "synthetic:target-error",
        "observation_kind": "native_target_error", "reason": "Synthetic unavailable target",
        "artifact_hash": artifact,
    })
    s.cli("host assign", assignment_id="replacement-review", role="review", owned_paths=["src"],
          brief="Synthetic replacement", config_path=str(s.config), replaces_assignment_id=peer["assignment_id"],
          expect="gate_result_required")
    s.cli("gate record", record=staged_gate(s, peer, "BLOCKED"))
    # Result ingestion preserves unavailable control state so replacement can still be justified.
    replacement = s.assign("review", "replacement-review", replaces_assignment_id=peer["assignment_id"])
    assert replacement["replaces_assignment_id"] == peer["assignment_id"]


@pytest.mark.parametrize("status", ["PASS", "FAIL", "BLOCKED"])
def test_late_old_release_gate_preserves_producer_result_during_new_implementation(tmp_path, status):
    s, worker = staged_candidate(tmp_path, version="0.4.0")
    peer = s.start_role("review")
    result = staged_gate(s, peer, status)
    # Reproduce the 0.4.0 sequence: implementation could start before the producer gate was imported.
    s.assign(identity=worker["assignment_id"])
    s.cli("host prepare", assignment_id=worker["assignment_id"])
    repair = s.cli("host record", assignment_id=worker["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running",
    }])["assignment"]
    s.capture(repair, identity="candidate-2", tree="c" * 40)
    phase = s.state["phase"]
    s.cli("gate record", record=result)
    assert s.state["records"][f"gate_result:{result['gate_id']}"] == result
    assert not s.state["gate_ids"] and s.state["phase"] == phase
    history = s.state["records"][f"gate_ingestion:{result['gate_id']}"]
    assert history["historical"] and set(history["mismatches"]) == {"candidate", "implementation"}
    assert history["producer_assignment"]["task_id"] == peer["task_id"]
    assert s.state["assignments"][repair["assignment_id"]]["status"] == "running"


def test_activation_result_cannot_be_omitted_or_overwritten(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=1)
    peer = s.start_role("review")
    result = staged_gate(s, peer, "FAIL")
    missing = {k: v for k, v in result.items() if k != "assignment_action_id"}
    s.cli("gate record", record=missing, expect="gate_activation_mismatch")
    s.cli("gate record", record=result)
    s.cli("gate record", record=result | {"gate_id": "rewritten", "status": "PASS"},
          expect="gate_result_conflict")


def test_stage_accounting_and_medium_disposition_are_explicit_delivery_requirements(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=0)
    finding = s.finding(severity="medium")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "resolve_findings"
    s.cli("deliver", expect="not_ready_to_deliver")
    with pytest.raises(WorkflowError, match="Deferral needs"):
        s.call("finding.defer", finding_id=finding["finding_id"], rationale="Follow up later")
    from test_deferrals import issue

    with patch("devflow.adapters.github.GitHubRepository.issue", return_value=issue()):
        s.cli("finding defer", finding_id=finding["finding_id"],
              followup_reference="https://github.com/synthetic/fixture/issues/23", rationale="Bounded follow-up")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "record_accounting"
    with pytest.raises(WorkflowError, match="every registered task segment"):
        s.call("usage.account", status="complete", source_reference="synthetic:collector", limitations=[])
    with pytest.raises(WorkflowError, match="explicit limitations"):
        s.call("usage.account", status="unknown", source_reference="synthetic:collector")
    accounting = s.call("usage.account", status="unavailable", source_reference="synthetic:collector",
                        limitations=["Synthetic collector unavailable; token and cost totals are unknown"])["accounting"]
    assert accounting["usage_response_ids"] == []
    assert not any("cost" in k or "tokens" in k for k in accounting)
    delivered = s.deliver()
    assert delivered["lifecycle"] == "done"
    delivery = s.state["records"]["delivery:delivery-1"]
    prepared = s.state["actions"][delivery["action_id"]]
    assert prepared["payload"]["accounting_id"] == accounting["accounting_id"]


@pytest.mark.parametrize("phase", ["verify", "deliver"])
def test_stage_direct_review_and_delivery_skip_invented_implementation(tmp_path, phase):
    s, worker = staged_candidate(tmp_path, tier=0, phase=phase)
    assert worker is None
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "record_accounting"
    s.call("usage.account", status="unknown", source_reference="synthetic:legacy-proof",
           limitations=["No usage source was provided for the prior work"])
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "deliver"


def test_last_passing_gate_does_not_hide_first_failure_or_a_late_finding(tmp_path):
    s, worker = staged_candidate(tmp_path)
    review = s.start_role("review")
    qa = s.start_role("qa")
    s.cli("gate record", record=staged_gate(s, review, "FAIL"))
    finding = s.finding(severity="medium")
    s.cli("gate record", record=staged_gate(s, qa))
    actions = s.service.next(s.work_id)["actions"]
    assert actions[0]["role"] == "implementation_worker"
    assert actions[0]["assignment_id"] == worker["assignment_id"]
    assert finding["finding_id"] in s.state["findings"]


def test_late_gate_keeps_linked_findings_and_historical_fix_proof(tmp_path):
    s, worker = staged_candidate(tmp_path, tier=1, version="0.4.0")
    reviewer = s.start_role("review")
    finding = s.finding(severity="medium")
    fix = record("fix_verification", verification_id="synthetic-late-fix",
                 finding_id=finding["finding_id"], candidate_id=reviewer["candidate_id"],
                 assignment_id=reviewer["assignment_id"], producer_task_id=reviewer["task_id"],
                 result="verified", evidence_ids=["check-1"],
                 verified_at=NOW)
    s.assign(identity=worker["assignment_id"])
    gate = staged_gate(s, reviewer, "FAIL", findings=[finding["finding_id"]])
    gate["fix_verification_ids"] = [fix["verification_id"]]
    s.cli("gate record", record=gate, fix_verifications=[fix])
    assert s.state["records"][f"fix_verification:{fix['verification_id']}"] == fix
    assert s.state["findings"][finding["finding_id"]]["disposition"] == "open"
    assert s.state["records"][f"gate_result:{gate['gate_id']}"]["finding_ids"] == [finding["finding_id"]]


def test_gate_requires_durable_original_producer_artifact(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=1)
    peer = s.start_role("review")
    gate = staged_gate(s, peer, "BLOCKED")
    missing = {k: v for k, v in gate.items() if k != "producer_result_artifact_hash"}
    s.cli("gate record", record=missing, expect="producer_result_required")
    s.cli("gate record", record=gate | {"producer_result_artifact_hash": "e" * 64},
          expect="missing_artifact")
    assert f"gate_result:{gate['gate_id']}" not in s.state["records"]
    s.cli("gate record", record=gate)


def test_complete_accounting_requires_all_task_segments_but_accepts_unknown_prices(tmp_path):
    s, _ = staged_candidate(tmp_path, tier=0)
    worker_segment = next(r for r in s.state["records"].values()
                          if r.get("record_type") == "execution_segment")
    owner_segment = worker_segment | {"segment_id": "synthetic-owner-segment", "task_id": PARENT,
                                      "role": "owner", "source_reference": "synthetic:owner-session"}
    s.call("segment.record", record=owner_segment)
    for index, segment in enumerate((worker_segment, owner_segment)):
        usage = record("usage", response_id=f"synthetic-usage-{index}", task_id=segment["task_id"],
                       segment_id=segment["segment_id"], recorded_at=NOW, uncached_input_tokens=12,
                       cache_read_tokens=0, cache_write_tokens=0, output_tokens=3, reasoning_output_tokens=1,
                       ccusage_version="synthetic-1", price_snapshot_id=None, api_equivalent_usd=None,
                       estimated_codex_credits=None, allocations=[{"work_id": s.work_id, "weight": 1}],
                       attribution_status="complete", pricing_status="missing_rate")
        s.call("usage.record", record=usage)
        if index == 0:
            with pytest.raises(WorkflowError, match="every registered task segment"):
                s.call("usage.account", status="complete", source_reference="synthetic:collector")
    result = s.call("usage.account", status="complete", source_reference="synthetic:collector",
                    limitations=["Price rates unavailable; costs remain unknown"])["accounting"]
    assert result["status"] == "complete" and len(result["usage_response_ids"]) == 2
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "deliver"


@pytest.mark.parametrize("field,value", [
    ("limitations", ["Coordinator rewrote the producer's limitation"]),
    ("completed_at", "2026-01-01T00:00:00Z"),
    ("status", "FAIL"),
])
def test_gate_import_rejects_rewritten_producer_fields(tmp_path, field, value):
    s, _ = staged_candidate(tmp_path, tier=1)
    peer = s.start_role("review")
    gate = staged_gate(s, peer, "BLOCKED")
    s.cli("gate record", record=gate | {field: value}, expect="producer_result_mismatch")
    assert f"gate_result:{gate['gate_id']}" not in s.state["records"]
    unrelated = s.service.put_artifact(b"Synthetic unrelated bytes")
    s.cli("gate record", record=gate | {"producer_result_artifact_hash": unrelated},
          expect="invalid_producer_result")
    s.cli("gate record", record=gate)


def resume_interrupted(s, peer):
    s.cli("host observe", assignment_id=peer["assignment_id"], observation={
        "agent_name": peer["agent_name"], "agent_status": "interrupted",
        "source_reference": "synthetic:observed-stop-for-workflow-diagnosis",
    })
    prepared = s.cli("host resume", assignment_id=peer["assignment_id"],
                     reason="Synthetic workflow cause repaired; finish the original observations")
    original_round = peer["gate_action_id"]
    assert prepared["action"]["payload"]["continuation_of"] == original_round
    assert prepared["assignment"]["gate_action_id"] == original_round
    intent = s.cli("host prepare", assignment_id=peer["assignment_id"])["intent"]
    assert intent["native_tool"] == "followup_task"
    assert original_round in intent["arguments"]["message"]
    assert "Resume this interrupted activation" in intent["arguments"]["message"]
    assert s.state["actions"][prepared["action"]["action_id"]]["status"] == "dispatched"
    assert s.cli("host prepare", assignment_id=peer["assignment_id"])["reconcile_only"]
    resumed = s.cli("host record", assignment_id=peer["assignment_id"], inventory=[{
        "agent_name": peer["agent_name"], "agent_status": "running",
    }])["assignment"]
    assert resumed["gate_action_id"] == original_round and resumed["action_id"] != original_round
    return resumed


def test_interrupted_independent_round_resumes_without_erasing_pending_gate(tmp_path):
    s, worker = staged_candidate(tmp_path)
    reviewer = s.start_role("review")
    qa = s.start_role("qa")
    resumed = resume_interrupted(s, reviewer)
    # A second interruption still continues the same original producer activation.
    resumed = resume_interrupted(s, resumed)
    s.cli("host assign", assignment_id=worker["assignment_id"], role="implementation_worker",
          owned_paths=["src"], brief="Cannot repair before original results", config_path=str(s.config),
          expect="gate_result_required")
    s.capture(worker, identity="forbidden-candidate", tree="c" * 40, expect="gate_result_required")
    original = staged_gate(s, reviewer, "BLOCKED")
    s.cli("gate record", record=original)
    s.cli("gate record", record=staged_gate(s, qa))
    assert s.state["records"][f"gate_result:{original['gate_id']}"] == original
    assert not s.state["records"][f"gate_ingestion:{original['gate_id']}"]["historical"]
    s.cli("host resume", assignment_id=reviewer["assignment_id"], reason="Not an interrupted pending round",
          expect="invalid_continuation")
    assert s.state["assignments"][reviewer["assignment_id"]]["status"] == "completed"


def test_same_attempt_upgrade_binds_new_gates_to_new_snapshot_and_retains_initial_record(tmp_path):
    s, worker = staged_candidate(tmp_path, tier=1)
    reviewer = s.start_role("review")
    old_result = staged_gate(s, reviewer, "FAIL")
    s.cli("gate record", record=old_result)
    original_attempt = s.state["records"]["attempt:synthetic-attempt"]
    amended = s.contract | {"scope_revision": 2}
    upgraded = workflow_snapshot() | {"snapshot_id": "snapshot-upgrade", "package_version": "0.5.1",
                                      "workflow_hash": "d" * 64, "package_revision": "d" * 40}
    s.call("work.amend", record=amended, workflow_snapshot=upgraded, user_request={
        "reference": "synthetic:resume-after-reviewed-workflow-fix", "summary": "Resume the same outcome",
        "allowed_operations": ["edit", "check", "create_tasks"],
    })
    s.contract = amended
    assert s.state["attempt"]["workflow_snapshot_id"] == "snapshot-upgrade"
    assert s.state["records"]["attempt:synthetic-attempt"] == original_attempt
    updated_worker = s.assign(identity=worker["assignment_id"])
    s.cli("host prepare", assignment_id=updated_worker["assignment_id"])
    updated_worker = s.cli("host record", assignment_id=updated_worker["assignment_id"], inventory=[{
        "agent_name": updated_worker["agent_name"], "agent_status": "running",
    }])["assignment"]
    s.capture(updated_worker, identity="candidate-upgrade", tree="c" * 40)
    s.complete(updated_worker)
    s.check("check-upgrade")
    new_review = s.assign("review", reviewer["assignment_id"])
    s.cli("host prepare", assignment_id=new_review["assignment_id"])
    new_review = s.cli("host record", assignment_id=new_review["assignment_id"], inventory=[{
        "agent_name": new_review["agent_name"], "agent_status": "running",
    }])["assignment"]
    assert new_review["workflow_snapshot_id"] == "snapshot-upgrade"
    gate = staged_gate(s, new_review, identity="gate-after-upgrade", evidence_ids=["check-upgrade"])
    assert gate["workflow_hash"] == upgraded["workflow_hash"]
    s.cli("gate record", record=gate)
    assert s.state["gate_ids"]["review"] == gate["gate_id"]
    assert s.state["records"]["candidate:candidate-upgrade"]["workflow_snapshot_id"] == "snapshot-upgrade"
    assert s.state["records"][f"gate_result:{old_result['gate_id']}"] == old_result
    # A delayed result from this intermediate policy must match neither the initial
    # attempt snapshot nor a later upgrade: its own activation owns the binding.
    later_review = s.assign("review", reviewer["assignment_id"])
    s.cli("host prepare", assignment_id=later_review["assignment_id"])
    later_review = s.cli("host record", assignment_id=later_review["assignment_id"], inventory=[{
        "agent_name": later_review["agent_name"], "agent_status": "running",
    }])["assignment"]
    delayed = staged_gate(s, later_review, "BLOCKED", identity="delayed-middle-policy",
                          evidence_ids=["check-upgrade"])
    s.call("work.amend", record=amended | {"scope_revision": 3},
           workflow_snapshot=upgraded | {"snapshot_id": "snapshot-next", "package_version": "0.5.2",
                                         "workflow_hash": "e" * 64}, user_request={
               "reference": "synthetic:second-workflow-upgrade", "summary": "Preserve late prior observations",
               "allowed_operations": ["edit", "check", "create_tasks"],
           })
    s.cli("gate record", record=delayed)
    assert s.state["records"]["gate_ingestion:delayed-middle-policy"]["historical"]
    assert not s.state["gate_ids"]
    assert s.state["records"]["gate_result:delayed-middle-policy"] == delayed


def test_low_findings_remain_optional_but_unobserved_deferrals_do_not(tmp_path):
    from devflow.domain.rules import next_actions

    s, _ = staged_candidate(tmp_path, tier=0)
    finding = s.finding(severity="low")
    assert s.service.next(s.work_id)["actions"][0]["kind"] == "record_accounting"
    state = s.state
    state["findings"][finding["finding_id"]].update(
        disposition="deferred", followup_reference="https://github.com/synthetic/fixture/issues/23",
        deferral_rationale="Unverified historical deferral",
    )
    assert next_actions(state)[0]["kind"] == "resolve_findings"
