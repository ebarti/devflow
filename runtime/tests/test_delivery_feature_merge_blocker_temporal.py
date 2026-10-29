"""Owned Temporal transports the real pre-effect refusal to the public run API."""

from __future__ import annotations

import asyncio
import json
from copy import deepcopy

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_feature_activities
from devflow_temporal import delivery_feature_merge as merger
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@workflow.defn
class MergeBlockerProjectionWorkflow(DeliveryWorkflow):
    @workflow.run
    async def run(self, spec, resilient):
        # Select the supported old/new coordinator paths explicitly. All merge,
        # stop and projection behavior below remains the production protocol.
        self.feature_liveness_v1 = resilient
        return await self._run_protocol(spec, None)


@pytest.mark.parametrize("resilient", [False, True], ids=["legacy", "current"])
async def test_native_stack_blocker_survives_temporal_and_public_projection(
    service, monkeypatch, resilient,
):
    store, spec, record, _, _, gh = setup_feature(service, monkeypatch)
    # This fixture starts at an already verified publication. Unrelated native
    # preparation/terminal-resource contracts are exercised by their own suites.
    spec = {key: value for key, value in spec.items() if key not in {
        "baseline_checks_version", "resource_cleanup_version", "terminal_tracker_version"}}
    ready = asyncio.Event()
    calls = []
    merge_boundary = merger.merge

    def no_submission(*args, **kwargs):
        return merge_boundary(*args, **kwargs, gh=gh,
                              execute=lambda *_a, **_kw: pytest.fail("unsafe stack submitted"))

    monkeypatch.setattr(merger, "merge", no_submission)
    monkeypatch.setattr(delivery_feature_activities, "_context", lambda *_a, **_kw: (store, None))
    monkeypatch.setattr(delivery_activities, "_context", lambda *_a, **_kw: (store, None))

    @activity.defn(name="delivery_prepare")
    async def prepare(_payload):
        return {"candidate": {"id": "retained-parent", "head": spec["base_sha"]}}

    @activity.defn(name="delivery_feature_revision_request")
    async def revision(_payload):
        return None

    @activity.defn(name="delivery_feature_open")
    async def opened(_payload):
        return {"record": deepcopy(record), "budget": registry(spec).budget("I_feature"),
                "checkpoints": {"verified:" + member["chunk_id"]: {}
                                for member in record["manifest"]["publication"]["members"]}}

    @activity.defn(name="delivery_project")
    async def project(payload):
        calls.append(deepcopy(payload))
        result = await delivery_activities.delivery_project(payload)
        if payload["event_type"] == "feature_ready":
            ready.set()
        return result

    @activity.defn(name="delivery_feature_settle_workers")
    async def settle_workers(_payload):
        return {"state": "confirmed"}  # This retained publication has no active workers.

    activities = [prepare, revision, opened, project, settle_workers,
                  delivery_feature_activities.delivery_feature_merge,
                  delivery_feature_activities.delivery_feature_settle_effects,
                  delivery_feature_activities.delivery_feature_stop]
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue="stack-condition-" + str(resilient),
                          workflows=[MergeBlockerProjectionWorkflow], activities=activities,
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                MergeBlockerProjectionWorkflow.run, args=[spec, resilient],
                id=spec["run_id"], task_queue="stack-condition-" + str(resilient))
            await asyncio.wait_for(ready.wait(), 30)
            state = await handle.query(DeliveryWorkflow.status)
            assert state["phase"] == "awaiting_merge"
            decision = state["decision"]
            command = {"command_id": "public-typed-merge", "answer": "merge",
                       "expected_revision": state["revision"], "decision_id": decision["id"],
                       "decision_revision": decision["revision"],
                       "candidate_revision": decision["candidate_revision"]}
            with store._connect() as db:
                db.execute("INSERT INTO delivery_mutations"
                           "(command_id,run_id,kind,request_digest,state) "
                           "VALUES (?,?,'decision',?,'pending')",
                           (command["command_id"], spec["run_id"], digest(command)))
            await handle.execute_update(DeliveryWorkflow.decision, command)
            result = await asyncio.wait_for(handle.result(), 30)
            history = await handle.fetch_history()
        await Replayer(workflows=[MergeBlockerProjectionWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)

    scheduled = [event.activity_task_scheduled_event_attributes.activity_type.name
                 for event in history.events
                 if event.HasField("activity_task_scheduled_event_attributes")]
    assert scheduled.count("delivery_feature_merge") == 1
    blocker = {"code": "stack_member_head_condition_unavailable", "stack_id": 42,
               "pull_requests": [20, 21, 22],
               "required_capability": "atomic_stack_membership_and_member_heads",
               "mutation_started": False}
    merge_result = result["checks"]["feature_merge"]
    assert result["outcome"] == "blocked" and result["phase"] == "blocked"
    assert merge_result == {"state": "blocked", "reason": blocker["code"], "blocker": blocker,
                            "instruction": str(merger.StackMergeConditionUnavailable(
                                42, record["manifest"]["publication"]["members"]))}
    assert blocker["code"] in result["error"] and "no merge was submitted" in result["error"]
    assert "ActivityError" not in result["error"]
    terminal = next(payload for payload in calls if payload["event_type"] == "blocked")
    assert terminal["checks"]["feature_merge"] == merge_result
    assert not any(payload["event_type"] == "feature_readback_pending" for payload in calls)
    assert not gh.closed and not any(method != "GET" for method, _, _ in gh.calls)
    with registry(spec).connect() as db:
        assert db.execute("SELECT 1 FROM execution_effects").fetchone() is None
    with store._connect() as db:
        persisted = json.loads(db.execute("SELECT checks_json FROM delivery_runs WHERE run_id=?",
                                         (spec["run_id"],)).fetchone()[0])
    assert persisted["feature_merge"] == merge_result

    app = create_app(store.config.path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                base_url=store.config.dashboard_url) as client:
        response = await client.get("/api/runs/" + spec["run_id"])
    assert response.status_code == 200, response.text
    public = response.json()["run"]
    assert public["phase"] == "blocked" and public["checks"]["feature_merge"] == merge_result
    assert blocker["code"] in public["error"] and "no merge was submitted" in public["error"]
