"""Native handoff state guards with synthetic admissions and no launched tasks."""

import json
from copy import deepcopy

import pytest
from domain.helpers import Scenario, authority, record

from devflow import cli
from devflow.application.commands import WorkflowService
from devflow.errors import WorkflowError
from devflow.execution import dispatch_action


def prepared_peer(tmp_path):
    scenario = Scenario(tmp_path / "state")
    action = scenario.call(
        "action.prepare", operation="launch_role", payload={"role": "implementation_worker"}
    )["action"]
    assignment = record(
        "assignment", assignment_id="synthetic-peer", action_id=action["action_id"],
        attempt_id=scenario.state["attempt"]["attempt_id"], role="implementation_worker",
        owner_task_id="synthetic-owner", task_id=None, client_id=None,
        candidate_id=scenario.state["candidate_id"], owned_paths=["src"],
        workspace_reference="synthetic:peer-workspace", status="prepared",
    )
    scenario.call("assignment.record", record=assignment)
    return scenario, action, assignment


def amend(scenario):
    contract = deepcopy(scenario.contract)
    contract["scope_revision"] = 2
    contract["scope"]["paths"] = ["different-scope"]
    scenario.call("work.amend", record=contract, authority=authority(contract, "auth-2"),
                  approved_delta="synthetic:independent-human-decision")


def dispatch(scenario, action, revision):
    return dispatch_action(scenario.service, {
        "work_id": scenario.work_id, "operation_id": "handoff",
        "action_id": action["action_id"], "expected_revision": revision,
    }, repository="/synthetic/worktree")


def host_prepare(scenario, assignment, revision, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(cli, "_service", lambda args: scenario.service)
    path = tmp_path / "request.json"
    path.write_text(json.dumps({
        "work_id": scenario.work_id, "expected_revision": revision,
        "assignment": assignment, "brief": "Implement the synthetic accepted scope",
    }))
    status = cli.main(["host", "prepare", "--request-file", str(path), "--repository", str(tmp_path)])
    return status, json.loads(capsys.readouterr().out)


@pytest.mark.parametrize("entry", ["dispatch", "host_prepare"])
@pytest.mark.parametrize("change,code", [
    ("cancel", "invalid_state"), ("amend", "stale_action"),
    ("fail", "retry_required"), ("revision", "stale_revision"),
    ("candidate", "stale_action"), ("block", "blocked_work"),
])
def test_native_handoff_requires_current_executable_action(
    tmp_path, monkeypatch, capsys, entry, change, code
):
    scenario, action, assignment = prepared_peer(tmp_path)
    if change == "cancel":
        scenario.call("work.cancel", authority_reference="synthetic:stop")
    elif change == "amend":
        amend(scenario)
    elif change == "fail":
        scenario.confirm(action, status="failed", external_id=None, observation={"no_mutation": True})
    elif change == "candidate":
        scenario.candidate()
    elif change == "block":
        scenario.call("work.block", blocker={
            "code": "missing_input", "reason": "Synthetic required input",
            "next_action": "Request synthetic decision",
        })
    revision = scenario.state["revision"] - (change == "revision")
    if entry == "dispatch":
        with pytest.raises(WorkflowError) as caught:
            dispatch(scenario, action, revision)
        assert caught.value.code == code
    else:
        status, response = host_prepare(scenario, assignment, revision, tmp_path, monkeypatch, capsys)
        assert status == 2
        assert response["error"]["code"] == code
    assert scenario.state["lifecycle"] == ("canceled" if change == "cancel" else "active")


@pytest.mark.parametrize("entry", ["dispatch", "host_prepare"])
def test_current_native_handoff_remains_available_with_synthetic_admission(
    tmp_path, monkeypatch, capsys, entry
):
    scenario, action, assignment = prepared_peer(tmp_path)
    revision = scenario.state["revision"]
    if entry == "dispatch":
        assert dispatch(scenario, action, revision)["requires_native_owner"] is True
    else:
        status, response = host_prepare(scenario, assignment, revision, tmp_path, monkeypatch, capsys)
        assert status == 0
        assert response["result"]["native_tool"] == "create_thread"
    # The native call has not happened, but its intent must be durable before
    # the executable handoff is returned. A second request must not relaunch.
    assert scenario.state["revision"] == revision + 1
    assert scenario.state["actions"][action["action_id"]]["status"] == "dispatched"
    if entry == "dispatch":
        replay = dispatch(scenario, action, revision)
    else:
        status, response = host_prepare(scenario, assignment, revision, tmp_path, monkeypatch, capsys)
        assert status == 0
        replay = response["result"]
    assert replay["reconcile_only"] is True
    assert "native_tool" not in replay
    assert scenario.state["revision"] == revision + 1


@pytest.mark.parametrize("status", ["dispatched", "ambiguous", "pending_setup"])
@pytest.mark.parametrize("change", ["cancel", "amend", "block"])
def test_uncertain_native_operations_remain_recovery_only_after_stop_or_amendment(
    tmp_path, status, change
):
    scenario, action, _ = prepared_peer(tmp_path)
    if status == "dispatched":
        scenario.call("action.begin", action_id=action["action_id"])
    else:
        scenario.confirm(action, status=status, external_id=None)
    if change == "cancel":
        scenario.call("work.cancel", authority_reference="synthetic:stop")
    elif change == "amend":
        amend(scenario)
    else:
        scenario.call("work.block", blocker={
            "code": "missing_input", "reason": "Synthetic required input",
            "next_action": "Request synthetic decision",
        })
    scenario.service = WorkflowService(scenario.service.store.root)
    result = dispatch(scenario, action, revision=0)
    assert result["reconcile_only"] is True
    assert result["status"] == status
    assert not {"action", "native_tool", "requires_native_owner"} & result.keys()


def test_blocked_work_suggests_only_recovery_and_the_outstanding_blocker(tmp_path):
    scenario, prepared, _ = prepared_peer(tmp_path)
    uncertain = scenario.call("action.prepare", operation="launch_role", payload={
        "role": "implementation_worker", "purpose": "synthetic uncertain peer",
    })["action"]
    scenario.call("action.begin", action_id=uncertain["action_id"])
    blocker = {"code": "missing_input", "reason": "Synthetic required input",
               "next_action": "Request synthetic decision"}
    scenario.call("work.block", blocker=blocker)
    actions = scenario.service.next(scenario.work_id)["actions"]
    assert actions == [
        {"kind": "reconcile_action", "action": scenario.state["actions"][uncertain["action_id"]]},
        {"kind": "request_user_action", "blocker": blocker},
    ]
    assert scenario.state["actions"][prepared["action_id"]]["status"] == "prepared"
    with pytest.raises(WorkflowError) as caught:
        scenario.call("action.begin", action_id=prepared["action_id"])
    assert caught.value.code == "blocked_work"
    assert scenario.state["actions"][prepared["action_id"]]["status"] == "prepared"


def test_multiple_failed_actions_can_be_readmitted_without_waiving_the_blocker(tmp_path):
    scenario, first, _ = prepared_peer(tmp_path)
    second = scenario.call("action.prepare", operation="launch_role", payload={
        "role": "implementation_worker", "purpose": "synthetic second failure",
    })["action"]
    for action in (first, second):
        scenario.confirm(action, status="failed", external_id=None, observation={"no_mutation": True})
    scenario.call("action.retry", action_id=first["action_id"])
    assert scenario.state["blocker"]["code"] == "action_failed"
    with pytest.raises(WorkflowError) as caught:
        dispatch(scenario, first, scenario.state["revision"])
    assert caught.value.code == "blocked_work"
    scenario.call("action.retry", action_id=second["action_id"])
    assert scenario.state["blocker"] is None
    assert dispatch(scenario, first, scenario.state["revision"])["requires_native_owner"] is True


def test_failed_action_retry_does_not_clear_unrelated_user_blocker(tmp_path):
    scenario, action, _ = prepared_peer(tmp_path)
    scenario.confirm(action, status="failed", external_id=None, observation={"no_mutation": True})
    blocker = {"code": "missing_input", "reason": "Synthetic required input",
               "next_action": "Request synthetic decision"}
    scenario.call("work.block", blocker=blocker)
    scenario.call("action.retry", action_id=action["action_id"])
    assert scenario.state["blocker"] == blocker
    with pytest.raises(WorkflowError) as caught:
        dispatch(scenario, action, scenario.state["revision"])
    assert caught.value.code == "blocked_work"
