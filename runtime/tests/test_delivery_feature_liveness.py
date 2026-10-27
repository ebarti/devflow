"""Feature coordination stays live across infrastructure failures without weakening gates.

Lost worker observations are re-issued within a bound, one chunk's failure
leaves healthy siblings running, infrastructure closures resume only through the
real admission and within a durable bound, and the merge and readback loops back
off instead of stopping. Real local Temporal servers verify the command sequence
and its replay; the real store drives the admission; the asyncio protocol stubs
cover the fast paths; a history recorded before the marker replays unchanged.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_recovered_closure import recovered_worker as recovered_worker
from test_delivery_feature_revision_preparation import (
    pending_preparation as pending_preparation,
)
from test_delivery_feature_revision_subject import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revision_subject import intake_fixture as intake_fixture
from test_delivery_feature_revision_subject import native_configuration as native_configuration
from test_delivery_feature_workflow_temporal import FeatureActivities
from test_delivery_store import service as service

import devflow_temporal.delivery_feature_workflow as protocol
from devflow_temporal.delivery_workflow import DeliveryWorkflow

FIXTURES = Path(__file__).parent / "fixtures"
MARKER = "delivery-feature-liveness-v1"
TRANSIENT = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
             "planning_defect": False, "error": "workstream build failed: ActivityError",
             "failure": {"classification": "transient", "cause_type": "ActivityTimeoutError"}}
TERMINAL = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
            "planning_defect": False, "error": "workstream product repair limit exhausted",
            "failure": {"classification": "terminal"}}
REFUSAL = "blocked implementation has no authenticated launch outcome"


def _feature_spec(run_id):
    return {"run_id": run_id, "provider": "fake", "config_path": "/frozen/config",
            "authorized_endpoint": "merged", "feature_delivery": {"version": 1},
            "policy": {"max_repairs": 10}}


def _handlers(fixtures, *overrides):
    replaced = {activity._Definition.must_from_callable(f).name for f in overrides}
    return [*(f for f in fixtures.handlers()
              if activity._Definition.must_from_callable(f).name not in replaced), *overrides]


def _base_handler(fixtures, name):
    return next(f for f in fixtures.handlers()
                if activity._Definition.must_from_callable(f).name == name)


def _timers(history):
    return [e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
            for e in history.events if e.HasField("timer_started_event_attributes")]


# --- H-6a: the coordinator's worker wait survives lost attempts -------------------------------


@pytest.mark.asyncio
async def test_real_temporal_permanent_github_refusal_stops_without_an_observation_loop():
    fixtures = FeatureActivities()

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        child = payload["kind"] + ":" + payload["chunk_id"]
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk"}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        # GitHubDelivery.api raises GitHubContractError for every gh failure.
        raise ApplicationError("gh: HTTP 401: Bad credentials", type="GitHubContractError")

    spec = _feature_spec("github-refusal")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert result["outcome"] == "blocked"
    assert ("feature build worker for chunk model delivery_feature_finish_worker failed: "
            "GitHubContractError: gh: HTTP 401: Bad credentials") in result["error"]
    assert "feature_observation_retries" not in result["checks"]
    assert "feature_worker_recoveries" not in result["checks"]
    events = [p["event_type"] for n, p in fixtures.calls if n == "delivery_project"]
    assert "feature_worker_observation_lost" not in events
    assert len(history.events) < 120


# --- H-6b: per-chunk failure isolation and bounded infrastructure resume ----------------------


@pytest.mark.asyncio
async def test_real_temporal_infrastructure_closure_resumes_while_the_sibling_keeps_building():
    """One build closes blocked for infrastructure while its sibling is still
    running: the coordinator re-reserves it (the supported resume path), the
    sibling is never cancelled, and the feature completes."""
    fixtures = FeatureActivities()
    base_finish = _base_handler(fixtures, "delivery_feature_finish_worker")
    reserves, waits = {}, {}
    client_resumed, release_model = asyncio.Event(), asyncio.Event()

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        child = payload["kind"] + ":" + payload["chunk_id"]
        reserves[child] = reserves.get(child, 0) + 1
        if child == "build:client" and reserves[child] == 2:
            client_resumed.set()
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk",
                **({"resumed": True} if reserves[child] > 1 else {})}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        child = payload["child_id"]
        waits[child] = waits.get(child, 0) + 1
        if child == "build:client" and waits[child] == 1:
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TRANSIENT)}
        if child == "build:model":
            await asyncio.wait_for(release_model.wait(), 30)
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        if payload["child_id"] == "build:client":
            assert client_resumed.is_set(), "a resumable closure is not retired before its resume"
        return await base_finish(payload)

    spec = _feature_spec("resumed-worker")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(client_resumed.wait(), 30)
            state = await handle.query(DeliveryWorkflow.status)
            release_model.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert state["phase"] == "feature_implementation" and state["outcome"] is None
    assert result["phase"] == "merged"
    assert reserves["build:client"] == 2 and waits["build:client"] == 2
    assert result["checks"]["feature_worker_recoveries"] == {"client:build": 1}
    recovering = [p["message"] for n, p in fixtures.calls
                  if n == "delivery_project" and p["event_type"] == "feature_worker_recovering"]
    assert recovering == ["Resumed the build worker for client after an infrastructure "
                          "failure (1 of 2)"]
    settled = [p["workers"] for n, p in fixtures.calls if n == "delivery_feature_settle_workers"]
    assert settled == [{}], "every worker closed through finish_worker; none was cancelled"


@pytest.mark.asyncio
async def test_real_temporal_chunk_verdict_drains_the_running_sibling_before_stopping():
    """The first chunk's build closes with a gate verdict while its sibling is
    still running: the coordinator waits for the sibling's own closure, then
    stops naming the chunk; the sibling is finished, not cancelled."""
    fixtures = FeatureActivities()
    draining, release_client = asyncio.Event(), asyncio.Event()
    base_project = _base_handler(fixtures, "delivery_project")

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        fixtures.calls.append(("delivery_feature_reserve", deepcopy(payload)))
        child = payload["kind"] + ":" + payload["chunk_id"]
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk"}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        fixtures.calls.append(("delivery_feature_wait_worker", deepcopy(payload)))
        if payload["child_id"] == "build:model":
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TERMINAL)}
        await asyncio.wait_for(release_client.wait(), 30)
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

    @activity.defn(name="delivery_feature_finish_worker")
    async def finish_worker(payload):
        fixtures.calls.append(("delivery_feature_finish_worker", deepcopy(payload)))
        outcome = "blocked" if payload["child_id"] == "build:model" else "delivered"
        return {"record": deepcopy(fixtures.record), "outcome": outcome, "budget": {}}

    @activity.defn(name="delivery_project")
    async def project(payload):
        if payload["event_type"] == "feature_worker_stopped":
            draining.set()
        return await base_project(payload)

    spec = _feature_spec("drained-sibling")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker, finish_worker,
                                               project)):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(draining.wait(), 30)
            state = await handle.query(DeliveryWorkflow.status)
            release_client.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    assert state["outcome"] is None and state["phase"] == "feature_implementation"
    assert result["outcome"] == "blocked"
    assert "model" in result["error"] and "FeatureWorkerStopped" in result["error"]
    finished = [p["child_id"] for n, p in fixtures.calls if n == "delivery_feature_finish_worker"]
    assert finished == ["build:model", "build:client"]
    stopped = [p["message"] for n, p in fixtures.calls
               if n == "delivery_project" and p["event_type"] == "feature_worker_stopped"]
    assert len(stopped) == 1 and "waiting for 1 running build(s)" in stopped[0]
    assert "feature_worker_recoveries" not in result["checks"], "a verdict is never recovered"
    settled = [p["workers"] for n, p in fixtures.calls if n == "delivery_feature_settle_workers"]
    assert settled == [{}]
    assert not any(n == "delivery_feature_reserve" and p["kind"] == "chunk"
                   for n, p in fixtures.calls)


# --- Real admission: what the coordinator offers, reserve() -> resume_worker() admits ----------

IMPLEMENTED = {"role": "implement", "iteration": 0, "status": "pass",
               "session_id": "modeled-implementation", "cleanup": "confirmed"}
ADMITTED = {
    # Latest implementation passed; a controller deadline at a gate stage.
    "implement_pass_then_ci_deadline": ([IMPLEMENTED], "ci_deadline", "waiting_ci"),
    # Latest implementation passed; the review role process died with teardown.
    "review_role_exception": ([IMPLEMENTED, {
        "role": "review", "iteration": 0, "status": "blocked", "session_id": None,
        "cleanup": "confirmed", "finish_reason": "exception", "usage": None,
        "summary": "role process failed: ConnectionError", "findings": ["reset"]}],
        "role_interrupted", "review"),
    # The implementation never launched: nothing unrecorded can exist.
    "implement_prelaunch": ([{
        "role": "implement", "iteration": 0, "status": "blocked", "session_id": None,
        "cleanup": "confirmed", "finish_reason": "prelaunch", "usage": None,
        "summary": "native launch refused", "findings": ["sandbox profile unavailable"]}],
        "role_prelaunch", "implement"),
}
# The implementation process itself died: its launch outcome is unauthenticated.
IMPLEMENT_DEATH = [{"role": "implement", "iteration": 0, "status": "blocked",
                    "session_id": None, "cleanup": "confirmed", "finish_reason": "exception",
                    "usage": None, "summary": "role process failed: ConnectionError",
                    "findings": ["Connection reset by peer"]}]


# --- M-24: merge and readback loops back off instead of stopping ------------------------------


# --- Replay: executions recorded before the marker keep their commands -----------------------


# --- Child side: a worker's own controller classifies its closure -----------------------------


# --- Re-issue paths keep the naming and `_resumable`; allowlist coverage ----------------------


# --- An unsettled readback has a cancel exit and a bound; the effect stays pending -----------


@pytest.mark.asyncio
async def test_real_temporal_operator_cancel_leaves_an_unsettled_readback_pending(monkeypatch):
    fixtures = FeatureActivities()
    waiting, cancel_sent = asyncio.Event(), asyncio.Event()
    settlements = []

    @activity.defn(name="delivery_feature_settle_effects")
    async def settle_effects(payload):
        settlements.append(payload["spec"]["run_id"])
        if len(settlements) == 2:
            waiting.set()
            await asyncio.wait_for(cancel_sent.wait(), 30)
        return {"state": "pending", "effect_key": "merge:stack-42",
                "reason": "RPCError: unavailable"}

    monkeypatch.setattr(protocol, "READBACK_INTERVAL", timedelta(seconds=1))
    spec = _feature_spec("unsettled-readback-cancel")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, settle_effects),
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            await asyncio.wait_for(waiting.wait(), 60)
            state = await handle.query(DeliveryWorkflow.status)
            assert state["phase"] == "waiting_feature_readback" and state["outcome"] is None
            await handle.execute_update(DeliveryWorkflow.cancel, {
                "command_id": "stop-unsettled-readback", "expected_revision": state["revision"],
                "reason": "Operator stops the wait",
            })
            cancel_sent.set()
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    assert result["outcome"] == "cancelled" and result["cleanup"] == "unknown"
    assert "merge:stack-42" in result["error"] and "RPCError: unavailable" in result["error"]
    assert result["checks"]["feature_readback"]["state"] == "pending"
    assert result["checks"]["feature_stop"]["state"] == "withheld"
    assert len(settlements) == 2
    assert not any(n == "delivery_feature_stop" for n, _ in fixtures.calls)
    assert fixtures.calls[-1][0] == "delivery_project"
    assert _timers(history)[:1] == [timedelta(seconds=1)]
