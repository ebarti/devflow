"""A gate whose native monitor was lost twice yields no verdict: stop, never repair (N-14)."""
from __future__ import annotations

import shutil

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker

from devflow_temporal import delivery_workflow
from devflow_temporal.delivery_workflow import DeliveryWorkflow

GATES = ("delivery_baseline_checks", "delivery_browser_qa", "delivery_checks", "delivery_precheck")
CANDIDATE = {"id": "fixture-candidate", "head": "fixture-head"}
# Exact shape delivery_broker._run_check_list / run_browser_qa return when the
# monitor is lost in both the original and the fresh invocation.
NO_VERDICT = {
    "state": "unknown", "cleanup": "confirmed", "process_cleanup": "observed-native-confirmed",
    "candidate_id": CANDIDATE["id"],
    "reason": "native gate monitor was lost twice; the gate produced no verdict",
    "native_process": {"interrupted_monitor": True}, "interrupted_native_processes": [{}, {}],
}
STOP_REASONS = {
    "delivery_browser_qa": "browser QA gate produced no verdict",
    "delivery_checks": "local check gate produced no verdict",
    "delivery_precheck": "prepublication gate produced no verdict",
}


def _activities(calls, lost):
    def stub(name):
        @activity.defn(name=name)
        async def execute(payload):
            calls.append((name, payload.get("role"), payload.get("iteration")))
            if name == "delivery_prepare":
                return {"candidate": CANDIDATE}
            if name == "delivery_role":
                return {"status": "pass", "candidate": CANDIDATE}
            if name == "delivery_publish":
                return {"candidate": CANDIDATE, "head": CANDIDATE["head"]}
            if name in {"delivery_tracker_start", "delivery_tracker"}:
                return {"state": "consistent"}
            if name == lost:
                return dict(NO_VERDICT)
            return {"state": "passed", "cleanup": "confirmed"}
        return execute

    return [stub(name) for name in (
        *GATES, "delivery_project", "delivery_prepare", "delivery_tracker_start",
        "delivery_role", "delivery_publish", "delivery_ci", "delivery_tracker",
    )]


async def _deliver(tmp_path, lost, run_id):
    spec = {"run_id": run_id, "provider": "fake", "baseline_checks_version": 1,
            "policy": {"max_repairs": 1, "browser_qa": {"id": "browser"},
                       "prepublish_checks": [{"id": "fixture"}]}}
    calls = []
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / f"{run_id}.sqlite3"),
    ) as environment:
        async with Worker(environment.client, task_queue=run_id, workflows=[DeliveryWorkflow],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          activities=_activities(calls, lost)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=run_id, task_queue=run_id)
            result = await handle.result()
            history = await handle.fetch_history()
    roles = [(role, iteration) for name, role, iteration in calls if name == "delivery_role"]
    return roles, result, history


@pytest.mark.asyncio
@pytest.mark.parametrize("lost", sorted(STOP_REASONS))
async def test_no_verdict_gate_stops_without_spending_a_repair(tmp_path, lost):
    roles, result, _ = await _deliver(tmp_path, lost, f"no-verdict-{lost}")
    assert ("implement", 1) not in roles, roles
    assert result["outcome"] == "blocked" and result["error"] == STOP_REASONS[lost]
    assert result["cleanup"] != "unknown"


@pytest.mark.asyncio
async def test_history_recorded_without_marker_replays_with_flag_false(tmp_path, monkeypatch):
    original = delivery_workflow.workflow.patched
    monkeypatch.setattr(
        delivery_workflow.workflow, "patched",
        lambda name: False if name == "delivery-native-launch-window-v1" else original(name),
    )
    roles, result, history = await _deliver(tmp_path, "delivery_browser_qa", "no-verdict-legacy")
    monkeypatch.undo()
    # Without the marker the recorded history took the old path: a repair turn.
    assert ("implement", 1) in roles and result["outcome"] == "blocked"
    assert result["error"] != STOP_REASONS["delivery_browser_qa"]
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
