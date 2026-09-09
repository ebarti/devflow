import runpy
from pathlib import Path

import pytest

from devflow.adapters.codex_host import NativeHostBridge
from devflow.errors import WorkflowError


@pytest.fixture
def assignment():
    return {
        "schema_version": 1,
        "record_type": "assignment",
        "assignment_id": "assignment-1",
        "action_id": "action-1",
        "attempt_id": "attempt-1",
        "role": "review",
        "owner_task_id": "owner",
        "task_id": None,
        "client_id": None,
        "candidate_id": "candidate-1",
        "owned_paths": ["src"],
        "workspace_reference": None,
        "status": "prepared",
    }


def test_pending_client_never_becomes_wait_target(assignment):
    bridge = NativeHostBridge()
    pending = bridge.record_launch(assignment, {"clientThreadId": "client-1"})
    assert pending["status"] == "pending_setup" and pending["task_id"] is None
    with pytest.raises(WorkflowError):
        bridge.wait_target(pending)
    with pytest.raises(WorkflowError):
        bridge.record_launch(pending, {"threadId": "client-1"})
    with pytest.raises(WorkflowError):
        bridge.prepare_assignment(pending, "Review")
    running = bridge.record_launch(pending, {"threadId": "actual-1"})
    assert bridge.wait_target(running, cursor="cursor") == {
        "threadId": "actual-1",
        "afterCursor": "cursor",
    }


def test_lost_launch_reconciles_marker_and_context_not_title(assignment):
    bridge = NativeHostBridge()
    intent = bridge.prepare_assignment(assignment, "Review the candidate.")
    assert intent["marker"] in intent["prompt"]
    assert "model" not in intent
    entry = {
        "threadId": "actual-1",
        "title": "Review",
        "prompt": intent["prompt"],
        "assignment_id": assignment["assignment_id"],
        "attempt_id": assignment["attempt_id"],
        "candidate_id": assignment["candidate_id"],
        "owner_task_id": assignment["owner_task_id"],
    }
    assert bridge.reconcile_launch(assignment, [entry])["task_id"] == "actual-1"
    for inventory in [
        [],
        [entry, entry | {"threadId": "duplicate"}],
        [entry | {"prompt": "title match only"}],
        [entry | {"candidate_id": "stale"}],
    ]:
        with pytest.raises(WorkflowError) as error:
            bridge.reconcile_launch(assignment, inventory)
        assert error.value.code == "ambiguous_host_action"


def test_results_bind_registered_task_candidate_and_role(assignment):
    bridge = NativeHostBridge()
    running = bridge.record_launch(assignment, {"threadId": "actual"})
    result = {"assignment_id": "assignment-1", "candidate_id": "candidate-1", "role": "review"}
    assert bridge.validate_result(running, result, observed_task_id="actual") == result
    for altered, producer in [
        (result | {"candidate_id": "stale"}, "actual"),
        (result | {"role": "qa"}, "actual"),
        (result, "owner"),
    ]:
        with pytest.raises(WorkflowError):
            bridge.validate_result(running, altered, observed_task_id=producer)


@pytest.mark.parametrize("receipt_kind", ["direct", "inventory", "both_ids"])
def test_queued_launch_becomes_admitted_running_assignment(tmp_path, receipt_kind):
    """Native pending setup and final task binding cross the actual service boundary."""
    helpers = runpy.run_path(str(Path(__file__).parents[1] / "domain" / "helpers.py"))
    scenario = helpers["Scenario"](tmp_path, tier=1)
    candidate = scenario.candidate()
    action = scenario.call(
        "action.prepare",
        operation="launch_role",
        payload={"role": "review", "candidate_id": candidate["candidate_id"]},
    )["action"]
    prepared = {
        "schema_version": 1,
        "record_type": "assignment",
        "assignment_id": "synthetic-queued-review",
        "action_id": action["action_id"],
        "attempt_id": scenario.state["attempt"]["attempt_id"],
        "role": "review",
        "owner_task_id": scenario.state["attempt"]["owner_task_id"],
        "task_id": None,
        "client_id": None,
        "candidate_id": candidate["candidate_id"],
        "owned_paths": ["src"],
        "workspace_reference": "synthetic:review:workspace",
        "status": "prepared",
    }
    bridge = NativeHostBridge()
    intent = bridge.prepare_assignment(prepared, "Review this synthetic candidate.")
    pending = bridge.record_launch(prepared, {"clientThreadId": "synthetic-client"})
    pending_receipt = scenario.confirm(
        action, status="pending_setup", external_id="synthetic-client"
    )
    scenario.call("assignment.record", record=pending)
    assert scenario.state["assignments"][prepared["assignment_id"]]["status"] == "pending_setup"

    response = {"threadId": "synthetic-review"}
    if receipt_kind == "inventory":
        running = bridge.reconcile_launch(
            pending,
            [response | intent["context"] | {"prompt": intent["prompt"]}],
        )
    else:
        if receipt_kind == "both_ids":
            response["clientThreadId"] = "synthetic-client"
        running = bridge.record_launch(pending, response)
    final_receipt = scenario.confirm(action, external_id="synthetic-review")
    scenario.call("assignment.record", record=running)

    admitted = scenario.state["assignments"][prepared["assignment_id"]]
    assert admitted["status"] == "running"
    assert bridge.wait_target(admitted) == {"threadId": "synthetic-review"}
    assert admitted["client_id"] is None
    receipts = scenario.state["receipts"]
    assert receipts[pending_receipt["receipt_id"]]["external_id"] == "synthetic-client"
    assert receipts[final_receipt["receipt_id"]]["external_id"] == "synthetic-review"
    assert any(
        key.startswith("assignment_event:") and event.get("client_id") == "synthetic-client"
        for key, event in scenario.state["records"].items()
    )
