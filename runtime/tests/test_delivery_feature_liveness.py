"""Feature coordination stays live across infrastructure failures without weakening gates.

Lost worker observations are re-issued, one chunk's failure leaves healthy
siblings running, infrastructure closures resume within a durable bound, and
the merge and readback loops are bounded. Real local Temporal servers verify the
new command sequence and its replay; the asyncio protocol stubs cover the fast
paths, and a history recorded before the marker replays on the old commands.
"""
from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutType
from temporalio.exceptions import TimeoutError as ActivityTimeoutError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_execution import Controller, feature_service
from test_delivery_feature_workflow_temporal import FeatureActivities
from test_delivery_github_contract import plan
from test_delivery_store import service as service

import devflow_temporal.delivery_feature_workflow as protocol
from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_feature_execution import register_worker, worker_spec
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow

FIXTURES = Path(__file__).parent / "fixtures"
MARKER = "delivery-feature-liveness-v1"
TRANSIENT = {"phase": "blocked", "cleanup": "confirmed", "planning_defect": False,
             "error": "workstream build failed: ActivityError",
             "failure": {"classification": "transient", "cause_type": "ActivityTimeoutError"}}
TERMINAL = {"phase": "blocked", "cleanup": "confirmed", "planning_defect": False,
            "error": "workstream product repair limit exhausted",
            "failure": {"classification": "terminal"}}


def _activity_error(cause, name="delivery_feature_reserve"):
    error = ActivityError("activity failed", scheduled_event_id=1, started_event_id=2,
                          identity="", activity_type=name, activity_id="1", retry_state=None)
    error.__cause__ = cause
    return error


def _lost_heartbeat():
    return ActivityTimeoutError("heartbeat", type=TimeoutType.HEARTBEAT, last_heartbeat_details=[])


def _delivered(controller, chunk_id, kind):
    if kind == "chunk":
        publication = controller.record["manifest"]["publication"]
        publication["members"].append({
            "chunk_id": chunk_id, "number": len(publication["members"]) + 1,
        })
        if len(publication["members"]) > 1:
            publication["stack_id"] = 42
        controller.record["manifest"]["revision"] += 1
    return {"outcome": "delivered", "record": deepcopy(controller.record), "budget": {"used": 0}}


class LivenessController(Controller):
    def __init__(self, value, *, liveness=True):
        super().__init__(value)
        self.feature_liveness_v1 = liveness
        self.messages = []
        self.sleeps = []

    async def _project(self, spec, event, message):
        self.messages.append((event, message))
        await super()._project(spec, event, message)

    async def _stop(self, spec, message, **kwargs):
        self.messages.append(("stopped", message))
        return await super()._stop(spec, message, **kwargs)


def _protocol_stubs(monkeypatch, controller, *, bounded_wait=False):
    """Protocol stubs run outside Temporal: timers resolve at once and are recorded."""
    monkeypatch.setattr(protocol.workflow, "patched",
                        lambda marker: bounded_wait and marker == "feature-worker-bounded-wait-v1")

    async def sleep(delay):
        controller.sleeps.append(delay)

    async def wait_condition(fn, *, timeout=None):
        if timeout is not None:
            controller.sleeps.append(timeout)
            return
        while not fn():
            await asyncio.sleep(0)

    monkeypatch.setattr(protocol.workflow, "sleep", sleep)
    monkeypatch.setattr(protocol.workflow, "wait_condition", wait_condition)


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


async def _scheduled(history, name):
    scheduled = []
    for event in history.events:
        if not event.HasField("activity_task_scheduled_event_attributes"):
            continue
        attributes = event.activity_task_scheduled_event_attributes
        if attributes.activity_type.name == name:
            request = (await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads))[0]
            scheduled.append((request, attributes))
    return scheduled


# --- H-6a: the coordinator's worker wait survives lost attempts -------------------------------


@pytest.mark.asyncio
async def test_worker_wait_options_let_a_lost_attempt_retry_with_a_full_window(monkeypatch):
    from devflow_temporal import delivery_workflow

    observed = {}

    async def execute(_name, _request, **options):
        observed.update(options)

    monkeypatch.setattr(delivery_workflow.workflow, "execute_activity", execute)
    monkeypatch.setattr(delivery_workflow.workflow, "patched", lambda _flag: False)
    await DeliveryWorkflow()._activity("delivery_feature_wait_worker", {
        "config_path": "/frozen/config", "run_id": "r", "spec_digest": "d", "child_id": "c",
    })
    # The activity polls for 300 s. A retry after an attempt lost late in the
    # window must still get a full window of its own.
    assert "schedule_to_close_timeout" not in observed
    assert observed["start_to_close_timeout"] >= timedelta(seconds=300 + 60)
    assert observed["heartbeat_timeout"] >= timedelta(seconds=60)
    assert observed["retry_policy"].maximum_attempts == 3
    assert observed["retry_policy"].non_retryable_error_types == [
        "ValueError", "TypeError", "OwnershipConflict"]
    assert observed["cancellation_type"] == (
        workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)


@pytest.mark.asyncio
async def test_lost_wait_observations_are_reissued_and_ownership_conflicts_stop(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)
    observations = iter([
        _activity_error(_lost_heartbeat(), "delivery_feature_wait_worker"),
        _activity_error(ApplicationError("database is locked", type="OperationalError"),
                        "delivery_feature_wait_worker"),
        {"closed": False},
        {"closed": True, "outcome": "delivered", "closure": {"cleanup": "confirmed"}},
    ])
    calls = []

    async def execute(name, request):
        calls.append(name)
        if name == "delivery_feature_reserve":
            return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1", "completed": False}
        if name == "delivery_feature_wait_worker":
            item = next(observations)
            if isinstance(item, Exception):
                raise item
            return item
        assert name == "delivery_feature_finish_worker"
        return _delivered(controller, "model", "build")

    controller._activity = execute
    spec = {"run_id": "feature", "config_path": "/frozen/config"}
    finished = await protocol._worker(controller, spec, "model", "build", {})
    assert finished["outcome"] == "delivered"
    assert calls.count("delivery_feature_wait_worker") == 4
    assert controller.state["checks"]["feature_observation_retries"] == {"build:model": 2}
    assert controller.sleeps == [protocol.OBSERVATION_PAUSE] * 2

    conflict = LivenessController(plan())
    _protocol_stubs(monkeypatch, conflict, bounded_wait=True)

    async def conflicting(name, request):
        if name == "delivery_feature_reserve":
            return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1", "completed": False}
        raise _activity_error(ApplicationError(
            "feature wait does not own this worker", type="OwnershipConflict", non_retryable=True,
        ), name)

    conflict._activity = conflicting
    with pytest.raises(ActivityError, match="activity failed"):
        await protocol._worker(conflict, spec, "model", "build", {})
    assert "feature_observation_retries" not in conflict.state["checks"]
    assert conflict.sleeps == []


@pytest.mark.asyncio
async def test_real_temporal_coordinator_reissues_a_wait_lost_after_exhausted_retries(monkeypatch):
    """Three retryable failures end a scheduled wait; the coordinator waits again
    instead of stopping the feature, and the recorded options have no
    schedule_to_close so a lost attempt can retry with a full window."""
    fixtures = FeatureActivities()
    calls = {}

    @activity.defn(name="delivery_feature_reserve")
    async def reserve(payload):
        child = payload["kind"] + ":" + payload["chunk_id"]
        return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                "completed": payload["kind"] == "chunk"}

    @activity.defn(name="delivery_feature_wait_worker")
    async def wait_worker(payload):
        child = payload["child_id"]
        calls[child] = calls.get(child, 0) + 1
        if child == "build:model" and calls[child] <= 3:
            raise ApplicationError("database is locked", type="OperationalError")
        return {"closed": True, "outcome": "delivered",
                "closure": {"phase": "delivered", "cleanup": "confirmed"}}

    monkeypatch.setattr(protocol, "OBSERVATION_PAUSE", timedelta(seconds=1))
    spec = _feature_spec("reissued-wait")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, reserve, wait_worker),
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    assert result["phase"] == "merged"
    assert calls["build:model"] == 4
    assert result["checks"]["feature_observation_retries"] == {"build:model": 1}
    waits = [attributes for request, attributes in await _scheduled(
        history, "delivery_feature_wait_worker") if request["child_id"] == "build:model"]
    assert len(waits) == 2, "the exhausted wait was re-issued as one new command"
    for attributes in waits:
        # The server records an absent schedule_to_close as zero: no overall
        # deadline cuts a retry short any more (it was 6 minutes).
        assert attributes.schedule_to_close_timeout.ToTimedelta() == timedelta(0)
        assert attributes.heartbeat_timeout.ToTimedelta() == timedelta(seconds=60)
        assert attributes.start_to_close_timeout.ToTimedelta() >= timedelta(seconds=360)
        assert attributes.retry_policy.maximum_attempts == 3


# --- H-6b: per-chunk failure isolation and bounded infrastructure resume ----------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("closure, resume_allowed, liveness, finishes, raised", [
    (TRANSIENT, True, True, False, protocol.FeatureWorkerStopped),
    (TRANSIENT, False, True, True, protocol.FeatureWorkerStopped),
    ({**TRANSIENT, "cleanup": "unknown"}, True, True, True, protocol.FeatureWorkerStopped),
    ({**TRANSIENT, "planning_defect": True}, True, True, True, protocol.FeatureWorkerStopped),
    (TERMINAL, True, True, True, protocol.FeatureWorkerStopped),
    (TRANSIENT, True, False, True, RuntimeError),
])
async def test_worker_resumes_an_infrastructure_closure_before_retiring_its_assignment(
    monkeypatch, closure, resume_allowed, liveness, finishes, raised,
):
    controller = LivenessController(plan(), liveness=liveness)
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)
    calls = []

    async def execute(name, request):
        calls.append(name)
        if name == "delivery_feature_reserve":
            return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1", "completed": False}
        if name == "delivery_feature_wait_worker":
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(closure)}
        assert name == "delivery_feature_finish_worker"
        return {"outcome": "blocked", "record": deepcopy(controller.record), "budget": {}}

    controller._activity = execute
    active = {}
    with pytest.raises(raised) as info:
        await protocol._worker(controller, {"run_id": "feature", "config_path": "/frozen"},
                               "model", "build", active, resume_allowed=resume_allowed)
    # finish_worker retires the registry assignment, which a continuation cannot
    # reuse: a resumable closure is handed back before it, every other after it.
    assert ("delivery_feature_finish_worker" in calls) is finishes
    if raised is protocol.FeatureWorkerStopped:
        assert info.value.chunk_id == "model" and info.value.closure == closure
        assert "model" in str(info.value) and "build" in str(info.value)
    else:
        assert str(info.value) == "feature worker stopped with preserved recovery evidence"


@pytest.mark.asyncio
@pytest.mark.parametrize("recovers", [True, False])
async def test_infrastructure_closures_resume_within_a_durable_bound(monkeypatch, recovers):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False):
        attempts.append((chunk_id, kind, resume_allowed))
        failures = sum(1 for item in attempts if item[:2] == (chunk_id, kind))
        if (chunk_id, kind) == ("client", "build") and (failures <= 2 or not recovers):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TRANSIENT))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert [allowed for c, k, allowed in attempts if (c, k) == ("client", "build")] == [
        True, True, False]
    assert controller.state["checks"]["feature_worker_recoveries"] == {"client:build": 2}
    recovering = [m for e, m in controller.messages if e == "feature_worker_recovering"]
    assert len(recovering) == 2 and "client" in recovering[0] and "(2 of 2)" in recovering[1]
    if recovers:
        assert result["phase"] == "merged"
    else:
        assert result["outcome"] == "blocked"
        assert "client" in result["error"] and "exhausted 2 automatic recoveries" in result["error"]
        assert "product repair" not in result["error"]
    assert any(name == "delivery_feature_stop" for name, _ in controller.events)


@pytest.mark.asyncio
async def test_transport_failures_before_admission_are_reissued_but_conflicts_stop(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    failures = iter([
        _activity_error(ApplicationError(
            "command failed (128): git fetch: could not resolve host", type="RuntimeError")),
        _activity_error(_lost_heartbeat(), "delivery_feature_finish_worker"),
    ])
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False):
        attempts.append((chunk_id, kind))
        if (chunk_id, kind) == ("model", "build") and attempts.count(("model", "build")) <= 2:
            raise next(failures)
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["phase"] == "merged"
    assert attempts.count(("model", "build")) == 3
    assert controller.state["checks"]["feature_worker_recoveries"] == {"model:build": 2}
    assert controller.sleeps == [protocol.OBSERVATION_PAUSE] * 2

    conflict = LivenessController(plan())
    _protocol_stubs(monkeypatch, conflict)
    verdicts = []

    async def refused(_controller, spec, chunk_id, kind, active, *, resume_allowed=False):
        verdicts.append((chunk_id, kind))
        if (chunk_id, kind) == ("model", "build"):
            raise _activity_error(ApplicationError(
                "worker's prerequisites have not passed their integrated gates",
                type="OwnershipConflict", non_retryable=True))
        return _delivered(conflict, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", refused)
    result = await protocol.coordinate(
        conflict, {"run_id": "run-two", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked" and "OwnershipConflict" in result["error"]
    assert verdicts.count(("model", "build")) == 1
    assert "feature_worker_recoveries" not in conflict.state["checks"]


@pytest.mark.asyncio
@pytest.mark.parametrize("liveness", [True, False])
async def test_one_chunk_verdict_lets_the_running_sibling_build_close_first(monkeypatch, liveness):
    controller = LivenessController(plan(), liveness=liveness)
    _protocol_stubs(monkeypatch, controller)
    release = asyncio.Event()

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False):
        if (chunk_id, kind) == ("model", "build"):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TERMINAL))
        if (chunk_id, kind) == ("client", "build"):
            await release.wait()
            controller.messages.append(("built", chunk_id))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    run = asyncio.create_task(protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"}))
    await asyncio.sleep(0.05)
    release.set()
    result = await asyncio.wait_for(run, 5)
    events = [event for event, _ in controller.messages]
    assert result["outcome"] == "blocked" and "model" in result["error"]
    assert "chunk_integrating" not in events
    if liveness:
        # The verdict about model is final; client's build is independent work
        # whose sealed closure a continuation reuses. Stop only once it closed.
        stopped = next(m for e, m in controller.messages if e == "feature_worker_stopped")
        assert "waiting for 1 running build(s)" in stopped and "model" in stopped
        assert events.index("built") < events.index("stopped")
        assert "exhausted" not in result["error"]
    else:
        assert "feature_worker_stopped" not in events
        assert events.index("stopped") < events.index("built")


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
                "closure": {"phase": "delivered", "cleanup": "confirmed"}}

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
    assert recovering == ["Recovering the build worker for client after an infrastructure "
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
                "closure": {"phase": "delivered", "cleanup": "confirmed"}}

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


# --- M-24: merge and readback loops are bounded ----------------------------------------------


@pytest.mark.asyncio
async def test_pending_merge_readback_stops_with_a_reason_and_settlement_slows_down(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    monkeypatch.setattr(protocol, "MERGE_READBACK_POLLS", 3)
    monkeypatch.setattr(protocol, "READBACK_FAST_POLLS", 2)
    settlements = iter([
        {"state": "pending", "effect_key": "merge:stack-42", "reason": "RPCError: unavailable"},
        {"state": "pending", "effect_key": "merge:stack-42", "reason": "RPCError: unavailable"},
        {"state": "pending", "effect_key": "merge:stack-42", "reason": "RPCError: unavailable"},
        {"state": "confirmed"},
    ])
    original = controller._activity

    async def execute(name, request):
        if name == "delivery_feature_merge":
            controller.events.append((name, deepcopy(request)))
            return {"state": "pending"}
        if name == "delivery_feature_settle_effects":
            controller.events.append((name, deepcopy(request)))
            return next(settlements)
        return await original(name, request)

    controller._activity = execute

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False):
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked"
    assert "merge readback stayed pending across 3 polls" in result["error"]
    assert sum(1 for name, _ in controller.events if name == "delivery_feature_merge") == 3
    assert controller.sleeps == [
        protocol.READBACK_INTERVAL, protocol.READBACK_INTERVAL,
        protocol.READBACK_INTERVAL, protocol.READBACK_INTERVAL, protocol.READBACK_SLOW_INTERVAL,
    ]
    readback = [m for e, m in controller.messages if e == "feature_readback_pending"]
    assert readback[:2] == ["Reconciling the original GitHub operation"] * 2
    assert ("merge:stack-42" in readback[2] and "after 3 polls" in readback[2]
            and "RPCError: unavailable" in readback[2] and "every 10 minutes" in readback[2])
    assert result["checks"]["feature_readback"] == {"state": "confirmed"}
    assert controller.events[-1][0] == "delivery_feature_stop"


@pytest.mark.asyncio
async def test_real_temporal_bounded_merge_readback_records_timers_and_replays(monkeypatch):
    fixtures = FeatureActivities()
    settlements = []

    @activity.defn(name="delivery_feature_merge")
    async def merge(payload):
        return {"state": "pending"}

    @activity.defn(name="delivery_feature_settle_effects")
    async def settle_effects(payload):
        settlements.append(payload["spec"]["run_id"])
        if len(settlements) < 4:
            return {"state": "pending", "effect_key": "merge:stack-42", "reason": "pending"}
        return {"state": "confirmed"}

    monkeypatch.setattr(protocol, "MERGE_READBACK_POLLS", 3)
    monkeypatch.setattr(protocol, "READBACK_FAST_POLLS", 2)
    monkeypatch.setattr(protocol, "READBACK_INTERVAL", timedelta(seconds=1))
    monkeypatch.setattr(protocol, "READBACK_SLOW_INTERVAL", timedelta(seconds=2))
    spec = _feature_spec("bounded-merge-readback")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, merge, settle_effects),
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    assert result["outcome"] == "blocked"
    assert "merge readback stayed pending across 3 polls" in result["error"]
    assert len(settlements) == 4
    timers = [e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
              for e in history.events if e.HasField("timer_started_event_attributes")]
    assert timers == [timedelta(seconds=1)] * 4 + [timedelta(seconds=2)]
    assert fixtures.calls[-1][0] == "delivery_feature_stop"


# --- Replay: executions recorded before the marker keep their commands -----------------------


@pytest.mark.asyncio
async def test_history_recorded_before_feature_liveness_marker_replays_with_flag_false(monkeypatch):
    content = (FIXTURES / "intake-required-history.json").read_text()
    history = WorkflowHistory.from_json("prior-intake", content)
    markers = []
    for event in history.events:
        if not event.HasField("marker_recorded_event_attributes"):
            continue
        details = event.marker_recorded_event_attributes.details
        for key in ("patch-data", "patch_id"):
            if key in details:
                value = (await DELIVERY_DATA_CONVERTER.decode(details[key].payloads))[0]
                markers.append(value["id"] if isinstance(value, dict) else value)
    assert MARKER not in markers
    observed = []
    original = workflow.patched

    def patched(name):
        result = original(name)
        if name == MARKER:
            observed.append((workflow.instance(), result))
        return result

    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", patched)
    await Replayer(workflows=[DeliveryWorkflow],
                   workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    (instance, result), = observed
    assert result is False and instance.feature_liveness_v1 is False


# --- Child side: a worker's own controller classifies its closure -----------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize("liveness, last_role, cause, expected", [
    (True, {"status": "blocked", "finish_reason": "exception", "cleanup": "confirmed"}, None,
     ("transient", "role_interrupted")),
    (True, {"status": "recovery_unknown", "finish_reason": "interrupted_monitor",
            "cleanup": "confirmed"}, None, ("transient", "role_interrupted")),
    (True, {"status": "blocked", "finish_reason": "exception", "cleanup": "unknown"}, None,
     ("terminal", None)),
    (True, {"status": "findings", "finish_reason": "done", "cleanup": "confirmed"}, None,
     ("terminal", None)),
    (True, {"status": "findings", "finish_reason": "done", "cleanup": "confirmed"},
     _activity_error(_lost_heartbeat(), "delivery_role"), ("transient", "ActivityTimeoutError")),
    (False, {"status": "blocked", "finish_reason": "exception", "cleanup": "confirmed"}, None,
     None),
])
async def test_feature_worker_stop_records_only_infrastructure_as_transient(
    liveness, last_role, cause, expected,
):
    from devflow_temporal.delivery_workflow import _initial_controller_state

    spec = {"run_id": "build:model", "provider": "fake", "policy": {},
            "feature_worker": {"parent_run_id": "feature", "chunk_id": "model", "kind": "build"}}
    controller = DeliveryWorkflow()
    controller.feature_liveness_v1 = liveness
    controller.state = _initial_controller_state(spec)
    controller.state["roles"].append(last_role)

    async def project(_spec, _event, _message):
        return None

    controller._project = project
    state = await controller._stop(spec, "workstream execution needs recovery", cause=cause)
    assert state["outcome"] == "blocked"
    if expected is None:
        assert "failure" not in state["checks"]
    else:
        classification, cause_type = expected
        failure = state["checks"]["failure"]
        assert failure["classification"] == classification
        assert failure.get("cause_type") == cause_type
        assert failure["reason"] == "workstream execution needs recovery"
    assert state["cleanup"] == ("unknown" if last_role["cleanup"] == "unknown" else "none")


@pytest.mark.asyncio
async def test_worker_result_carries_the_closure_its_own_controller_recorded(service, monkeypatch):
    """The observation the coordinator decides a resume from comes from the real
    store row, before finish_worker retires the registry assignment."""
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    reference = {"spec": parent, "child_id": child["run_id"]}
    assert await activities.delivery_feature_worker_result(reference) == {"closed": False}
    failure = {"classification": "transient", "cause_type": "role_interrupted",
               "stage": "implement", "reason": "workstream execution needs recovery"}
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
                   "execution_state='blocked',cleanup='confirmed',error=?,checks_json=? "
                   "WHERE run_id=?", ("workstream execution needs recovery",
                                      json.dumps({"failure": failure}), child["run_id"]))
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda self, run_id, **kwargs: {"result": {"outcome": "blocked"}})
    observation = await activities.delivery_feature_worker_result(reference)
    assert observation == {"closed": True, "outcome": "blocked", "closure": {
        "phase": "blocked", "cleanup": "confirmed", "error": "workstream execution needs recovery",
        "failure": failure, "planning_defect": False,
    }}
    assert protocol._resumable(observation["closure"])
    assert not protocol._resumable({**observation["closure"], "cleanup": "unknown"})
    with store._connect() as db:
        assigned = db.execute("SELECT 1 FROM delivery_runs WHERE run_id=? AND outcome='blocked'",
                              (child["run_id"],)).fetchone()
    assert assigned, "observing a closure never changes the worker's row"
