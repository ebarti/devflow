from __future__ import annotations

import json
import shutil
from pathlib import Path

from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from devflow_temporal.delivery_workflow import DeliveryWorkflow

CHECK_ACTIVITIES = {
    "delivery_checks", "delivery_browser_qa", "delivery_precheck", "delivery_baseline_checks",
}
LEGACY_HISTORY = Path(__file__).parent / "fixtures/check-slots-legacy-history.json"
# Actual prior-code history generated before editing c04f00eb43eb225728b63c82026ffe97a41cafc2.


def fixture_activities():
    candidate = {"id": "fixture-candidate", "head": "fixture-head"}

    def stub(name):
        @activity.defn(name=name)
        async def execute(payload):
            if name == "delivery_prepare":
                return {"candidate": candidate}
            if name == "delivery_role":
                return {"status": "pass", "candidate": candidate}
            if name == "delivery_publish":
                return {"candidate": candidate, "head": candidate["head"]}
            if name in {"delivery_tracker_start", "delivery_tracker"}:
                return {"state": "consistent"}
            if name == "delivery_browser_qa":
                return {"state": "passed", "cleanup": "confirmed", "receipt": "/fixture/qa",
                        "receipt_sha256": "a" * 64, "log": "/fixture/log",
                        "log_sha256": "b" * 64}
            return {"state": "passed", "cleanup": "confirmed"}
        return execute

    return [stub(name) for name in (
        *sorted(CHECK_ACTIVITIES), "delivery_project", "delivery_prepare",
        "delivery_tracker_start", "delivery_role", "delivery_publish", "delivery_ci",
        "delivery_tracker",
    )]


async def test_real_workflow_schedules_heartbeat_checks_and_replays_prior_code(tmp_path):
    spec = {
        "run_id": "check-slots-history", "provider": "fake", "baseline_checks_version": 1,
        "policy": {"max_repairs": 0, "browser_qa": {"id": "browser"},
                   "prepublish_checks": [{"id": "fixture"}]},
    }
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "owned-temporal.sqlite3"),
    ) as environment:
        async with Worker(
            environment.client, task_queue="check-slots-history", workflows=[DeliveryWorkflow],
            activities=fixture_activities(),
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue="check-slots-history",
            )
            result = await handle.result()
            assert result["outcome"] == "delivered", result
            history = await handle.fetch_history()
    scheduled = {
        event.activity_task_scheduled_event_attributes.activity_type.name:
        event.activity_task_scheduled_event_attributes
        for event in history.events if event.HasField("activity_task_scheduled_event_attributes")
    }
    assert CHECK_ACTIVITIES <= scheduled.keys()
    assert all(scheduled[name].heartbeat_timeout.seconds == 30 for name in CHECK_ACTIVITIES)
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json("check-slots-history", json.loads(LEGACY_HISTORY.read_text()))
    )
