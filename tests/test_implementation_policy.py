"""Current implementation proof must retain its actual activation policy."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from domain.helpers import NOW, SHA, H, record, workflow_snapshot
from test_subagent_lifecycle import Subagents

from devflow.domain.rules import implementation_completed, transition
from devflow.errors import WorkflowError


def upgraded_snapshot():
    return workflow_snapshot() | {"snapshot_id":"new-policy", "package_version":"0.5.2",
                                  "workflow_hash":"c" * 64, "package_revision":"d" * 40}


def amendment(s, contract=None):
    return dict(record=contract or s.contract | {"scope_revision":2},workflow_snapshot=upgraded_snapshot(),
                user_request={"reference":"synthetic:existing-goal", "summary":"Integrate the admitted policy",
                              "allowed_operations":["edit","check","create_tasks"]})


@pytest.mark.parametrize("boundary", ["candidate", "result", "readiness", "delivery"])
def test_stale_activation_cannot_supply_current_policy_proof(tmp_path, boundary):
    s = Subagents(tmp_path, package_version="0.5.2")
    worker = s.start_role()
    state = s.state
    # Pure in-memory model of the independently reproduced historical bad state.
    # No store records are changed by this adversarial fixture.
    snapshot = upgraded_snapshot()
    state["records"]["workflow_snapshot:new-policy"] = snapshot
    state["attempt"]["workflow_snapshot_id"] = snapshot["snapshot_id"]
    candidate = record("candidate", candidate_id="new-candidate",attempt_id=state["attempt"]["attempt_id"],
                       scope_hash=state["scope_hash"],repository=s.repository,base_sha=SHA,head_sha=SHA,tree_sha=SHA,
                       clean=True,dependency_hash=H,environment_hash=H,created_at=NOW,
                       workflow_snapshot_id=snapshot["snapshot_id"])
    result = {"assignment_id":worker["assignment_id"],"candidate_id":worker["candidate_id"],
              "producer_role":"implementation_worker","status":"completed",
              "output_candidate_id":"new-candidate","evidence_reference":"synthetic:actual-old-policy-output"}
    request = {"operation_id":"negative-policy-probe","work_id":s.work_id,"expected_revision":state["revision"]}
    if boundary == "candidate":
        command = "candidate.record"
        request.update(record=candidate,assignment_id=worker["assignment_id"],producer_task_id=worker["task_id"])
    else:
        state["candidate_id"] = candidate["candidate_id"]
        state["records"]["candidate:new-candidate"] = candidate
        state["assignments"][worker["assignment_id"]]["captured_candidate_id"] = candidate["candidate_id"]
        if boundary == "result":
            command = "host.result"
            request.update(assignment_id=worker["assignment_id"],observed_task_id=worker["task_id"],result=result)
        else:
            state["assignments"][worker["assignment_id"]].update(status="completed",implementation_result=result)
            assert not implementation_completed(state)
            if boundary == "readiness":
                return
            command = "deliver"
    before = deepcopy(state)
    with pytest.raises(WorkflowError) as exc:
        transition(state,command,request,datetime.now(UTC),repository=s.repository)
    assert exc.value.code == ("not_ready_to_deliver" if boundary == "delivery" else "stale_policy")
    assert state == before


def test_observed_stop_does_not_replace_original_output_handoff(tmp_path):
    s = Subagents(tmp_path,package_version="0.5.2")
    worker = s.start_role()
    s.cli("host observe",assignment_id=worker["assignment_id"],observation={
        "agent_name":worker["agent_name"],"agent_status":{"completed":"Synthetic actual stopped output"},"source_reference":"synthetic:native-completion"})
    before = s.state
    with pytest.raises(WorkflowError) as exc:
        s.call("work.amend",**amendment(s))
    assert exc.value.code == "implementation_result_required"
    assert s.state == before
    s.cli("host assign",assignment_id=worker["assignment_id"],role="implementation_worker",owned_paths=["src"],
          brief="Must not overwrite the unimported output",config_path=str(s.config),expect="implementation_result_required")
    assert s.state == before


def test_original_blocked_output_survives_scope_policy_change_and_reactivation(tmp_path):
    s = Subagents(tmp_path,package_version="0.5.2")
    worker = s.start_role()
    original = {"assignment_id":worker["assignment_id"],"candidate_id":None,"producer_role":"implementation_worker",
                "assignment_action_id":worker["action_id"],
                "status":"blocked","output_candidate_id":None,"evidence_reference":"synthetic:actual-partial-output"}
    s.cli("host result",assignment_id=worker["assignment_id"],observed_task_id=worker["task_id"],result=original)
    s.cli("host observe",assignment_id=worker["assignment_id"],observation={
        "agent_name":worker["agent_name"],"agent_status":{"completed":"Synthetic actual stopped output"},"source_reference":"synthetic:stopped-after-partial"})
    history = deepcopy(s.state["records"])
    contract = deepcopy(s.contract)
    contract["scope_revision"] = 2
    contract["scope"]["paths"].append("AGENTS.md")
    s.call("work.amend",**amendment(s,contract))
    # A late repeat/import of original partial output remains historical, even across changed scope.
    s.cli("host result",assignment_id=worker["assignment_id"],observed_task_id=worker["task_id"],result=original)
    assert not implementation_completed(s.state)
    reactivated = s.assign(identity=worker["assignment_id"])
    assert reactivated["workflow_snapshot_id"] == "new-policy"
    assert reactivated["task_id"] == worker["task_id"]
    assert all(s.state["records"][k] == v for k,v in history.items())
    assert any(r.get("implementation_result") == original for r in s.state["records"].values())
    s.cli("host prepare",assignment_id=worker["assignment_id"])
    s.cli("host record",assignment_id=worker["assignment_id"],inventory=[{
        "agent_name":worker["agent_name"],"agent_status":"running"}])
    current = s.state
    s.cli("host result",assignment_id=worker["assignment_id"],observed_task_id=worker["task_id"],
          result=original,expect="implementation_activation_mismatch")
    without_binding = {k:v for k,v in original.items() if k != "assignment_action_id"}
    s.cli("host result",assignment_id=worker["assignment_id"],observed_task_id=worker["task_id"],
          result=without_binding,expect="implementation_activation_mismatch")
    assert s.state == current


@pytest.mark.parametrize("resumed_status", ["blocked", "completed"])
def test_interrupted_without_output_requires_resumed_activation_binding(tmp_path, resumed_status):
    s = Subagents(tmp_path, package_version="0.5.2")
    original = s.start_role()
    late = {"assignment_id": original["assignment_id"], "candidate_id": original["candidate_id"],
            "producer_role": "implementation_worker", "status": "blocked", "output_candidate_id": None,
            "evidence_reference": "synthetic:late-first-activation"}
    s.cli("host observe", assignment_id=original["assignment_id"], observation={
        "agent_name": original["agent_name"], "agent_status": "interrupted",
        "source_reference": "synthetic:actual-interruption-before-output"})
    reused = s.assign(identity=original["assignment_id"])
    assert reused["action_id"] != original["action_id"]
    assert reused["task_id"] == original["task_id"]
    assert reused["workflow_snapshot_id"] == original["workflow_snapshot_id"]
    assert reused["candidate_id"] == original["candidate_id"] is None
    s.cli("host prepare", assignment_id=reused["assignment_id"])
    s.cli("host record", assignment_id=reused["assignment_id"], inventory=[{
        "agent_name": reused["agent_name"], "agent_status": "running"}])
    before = s.state
    assert not any(r.get("implementation_result") for r in before["records"].values())
    for result in (late, late | {"assignment_action_id": original["action_id"]}):
        s.cli("host result", assignment_id=original["assignment_id"], observed_task_id=original["task_id"],
              result=result, expect="implementation_activation_mismatch")
        # The full persisted state includes revision, assignments and immutable records.
        assert s.state == before
    correct = late | {"assignment_action_id": reused["action_id"], "status": resumed_status,
                      "evidence_reference": "synthetic:actual-resumed-output"}
    if resumed_status == "completed":
        s.capture(reused)
        correct["output_candidate_id"] = s.state["candidate_id"]
    s.cli("host result", assignment_id=reused["assignment_id"], observed_task_id=reused["task_id"], result=correct)
    assert s.state["assignments"][reused["assignment_id"]]["implementation_result"] == correct
    assert s.state["assignments"][reused["assignment_id"]]["status"] == resumed_status
    assert implementation_completed(s.state) == (resumed_status == "completed")
    assert all(s.state["records"][key] == value for key, value in before["records"].items())


@pytest.mark.parametrize("status", ["blocked", "completed"])
def test_initial_legacy_activation_result_may_omit_action_binding(tmp_path, status):
    s = Subagents(tmp_path, package_version="0.4.0")
    worker = s.start_role()
    if status == "completed":
        s.capture(worker)
    result = {"assignment_id": worker["assignment_id"], "candidate_id": worker["candidate_id"],
              "producer_role": "implementation_worker", "status": status,
              "output_candidate_id": s.state["candidate_id"], "evidence_reference": "synthetic:legacy-first-result"}
    s.cli("host result", assignment_id=worker["assignment_id"], observed_task_id=worker["task_id"], result=result)
    assert s.state["assignments"][worker["assignment_id"]]["implementation_result"] == result
    assert implementation_completed(s.state) == (status == "completed")


def test_historical_native_thread_delivery_keeps_its_existing_proof(tmp_path):
    from domain.helpers import Scenario
    from test_work_reopen import REPOSITORY, complete_pr, reopen_request, reopen_service

    s = Scenario(tmp_path,endpoint="pr",repository=REPOSITORY)
    s.candidate()
    s.check(scenario_ids=["A01"])
    complete_pr(s)
    before = s.state
    request, observation = reopen_request(before,"deliver")
    request["user_request"]["allowed_operations"] = ["publish_pr"]
    reopened = reopen_service(s)
    reopened.execute("work.reopen",request,continuation_observation=observation)
    after = reopened.snapshot(s.work_id)
    assert implementation_completed(after)
    assert after["attempt"]["execution_mode"] == "native_thread"
    assert after["candidate_id"] == before["candidate_id"]
    assert after["check_ids"] == before["check_ids"]
    assert after["assignments"] == before["assignments"] == {}


def test_historical_reused_subagent_results_remain_valid_without_activation_binding(tmp_path):
    from test_work_reopen import REPOSITORY, complete_pr, reopen_request

    s = Subagents(tmp_path, package_version="0.4.0", endpoint="pr", repository=REPOSITORY)
    worker = s.start_role()
    s.capture(worker)
    s.complete(worker)
    reused = s.assign()
    s.cli("host prepare", assignment_id=reused["assignment_id"])
    s.cli("host record", assignment_id=reused["assignment_id"], inventory=[{
        "agent_name": worker["agent_name"], "agent_status": "running",
    }])
    s.capture(reused, "candidate-2", "c" * 40)
    s.complete(reused)
    s.check(scenario_ids=["A01"])
    complete_pr(s)

    # An exported historical state models already-imported 0.4 results. We do
    # not replay an unbound result into a new activation or rewrite the store.
    before = s.state
    completed_actions = set()
    for item in [*before["assignments"].values(), *before["records"].values()]:
        result = item.get("implementation_result")
        if result:
            completed_actions.add(item["action_id"])
            result.pop("assignment_action_id", None)
        if item.get("role") == "implementation_worker":
            item.pop("workflow_snapshot_id", None)
    assert len(completed_actions) == 2
    assert implementation_completed(before)
    request, observation = reopen_request(before, "deliver")
    request["user_request"]["allowed_operations"] = ["publish_pr"]
    after, _ = transition(before, "work.reopen", request, datetime.now(UTC),
                          repository=REPOSITORY, continuation_observation=observation)
    assert implementation_completed(after)
    assert after["candidate_id"] == before["candidate_id"]
    assert after["check_ids"] == before["check_ids"]
    assert after["assignments"] == before["assignments"]
    assert all(after["records"][key] == value for key, value in before["records"].items())
