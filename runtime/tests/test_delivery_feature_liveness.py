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
import errno
import json
import re
import subprocess
import sys
from copy import deepcopy
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutType
from temporalio.exceptions import TimeoutError as ActivityTimeoutError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_execution import Controller, feature_service
from test_delivery_feature_integration import integration
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
from test_delivery_github_contract import plan
from test_delivery_store import service as service

import devflow_temporal.delivery_feature_workflow as protocol
from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_publication as publications
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_execution_registry import OwnershipConflict, UnresolvedEffect
from devflow_temporal.delivery_feature_execution import (
    continue_feature,
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_feature_pass import checkpoint_key
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _interrupted_role

FIXTURES = Path(__file__).parent / "fixtures"
MARKER = "delivery-feature-liveness-v1"
TRANSIENT = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
             "planning_defect": False, "error": "workstream build failed: ActivityError",
             "failure": {"classification": "transient", "cause_type": "ActivityTimeoutError"}}
TERMINAL = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
            "planning_defect": False, "error": "workstream product repair limit exhausted",
            "failure": {"classification": "terminal"}}
REFUSAL = "blocked implementation has no authenticated launch outcome"


def _activity_error(cause, name="delivery_feature_reserve"):
    error = ActivityError("activity failed", scheduled_event_id=1, started_event_id=2,
                          identity="", activity_type=name, activity_id="1", retry_state=None)
    error.__cause__ = cause
    return error


def _lost_heartbeat():
    return ActivityTimeoutError("heartbeat", type=TimeoutType.HEARTBEAT, last_heartbeat_details=[])


def _transient_error(name, type="ConnectionError", message="connection reset"):
    return _activity_error(ApplicationError(message, type=type), name)


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
        self.admissions = []

    async def _project(self, spec, event, message):
        self.messages.append((event, message))
        await super()._project(spec, event, message)

    async def _stop(self, spec, message, **kwargs):
        self.messages.append(("stopped", message))
        return await super()._stop(spec, message, **kwargs)

    async def _activity(self, name, request):
        if name == "delivery_feature_reserve":
            # The real admission is modeled as accepted; refusal tests override it.
            self.admissions.append((request["chunk_id"], request["kind"]))
            child = request["kind"] + ":" + request["chunk_id"]
            return {"spec": {"run_id": child}, "workflow_id": "wf-" + child,
                    "completed": False, "resumed": True}
        return await super()._activity(name, request)


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


def _timers(history):
    return [e.timer_started_event_attributes.start_to_fire_timeout.ToTimedelta()
            for e in history.events if e.HasField("timer_started_event_attributes")]


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
async def test_lost_wait_observations_are_reissued_with_backoff_and_verdicts_stop(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)
    observations = iter([
        _activity_error(_lost_heartbeat(), "delivery_feature_wait_worker"),
        _transient_error("delivery_feature_wait_worker", type="GitHubTransientError",
                         message="gh: HTTP 502"),
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
    assert controller.sleeps == [protocol.OBSERVATION_PAUSE, protocol.OBSERVATION_PAUSE * 2]
    lost = [m for e, m in controller.messages if e == "feature_worker_observation_lost"]
    assert len(lost) == 2 and "delivery_feature_wait_worker for worker build:model" in lost[0]
    assert "TimeoutError" in lost[0] and "GitHubTransientError: gh: HTTP 502" in lost[1]
    assert "retry 2 of" in lost[1]

    for verdict in (
        ApplicationError("feature wait does not own this worker", type="OwnershipConflict",
                         non_retryable=True),
        ApplicationError("gh: HTTP 401: Bad credentials", type="GitHubContractError"),
        ApplicationError("no such table: delivery_runs", type="OperationalError"),
        ApplicationError("[Errno 2] No such file or directory: 'gh'", type="OSError"),
    ):
        refused = LivenessController(plan())
        _protocol_stubs(monkeypatch, refused, bounded_wait=True)

        async def refusing(name, request, _verdict=verdict):
            if name == "delivery_feature_reserve":
                return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1",
                        "completed": False}
            raise _activity_error(_verdict, name)

        refused._activity = refusing
        with pytest.raises(ActivityError, match="activity failed"):
            await protocol._worker(refused, spec, "model", "build", {})
        assert "feature_observation_retries" not in refused.state["checks"]
        assert refused.sleeps == [] and refused.messages == []


@pytest.mark.asyncio
async def test_observations_stay_bounded_and_the_stop_names_the_stuck_observation(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    attempts = []

    async def failing(name, request):
        attempts.append(name)
        raise _transient_error(name, type="GitHubTransientError", message="gh: HTTP 502")

    controller._activity = failing
    spec = {"run_id": "feature", "config_path": "/frozen/config"}
    with pytest.raises(protocol.ObservationLost) as info:
        await protocol._observe(controller, True, spec, "build:model",
                                "delivery_feature_finish_worker", {"child_id": "build:model"})
    assert len(attempts) == protocol.OBSERVATION_RETRIES + 1
    assert controller.state["checks"]["feature_observation_retries"] == {
        "build:model": protocol.OBSERVATION_RETRIES}
    assert controller.sleeps == [
        timedelta(seconds=30), timedelta(seconds=60), timedelta(minutes=2), timedelta(minutes=4),
        timedelta(minutes=8), protocol.OBSERVATION_PAUSE_CAP]
    assert [e for e, _ in controller.messages] == (
        ["feature_worker_observation_lost"] * protocol.OBSERVATION_RETRIES)
    assert "delivery_feature_finish_worker for worker build:model" in str(info.value)
    assert "GitHubTransientError: gh: HTTP 502" in str(info.value)

    # Past the bound the supervisor re-issues the worker (bounded) and then stops
    # with a reason that names the observation, not a generic wrapper.
    coordinator = LivenessController(plan())
    _protocol_stubs(monkeypatch, coordinator)

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        if (chunk_id, kind) == ("model", "build"):
            raise protocol.ObservationLost("delivery_feature_finish_worker", "build:model",
                                           protocol.OBSERVATION_RETRIES,
                                           "GitHubTransientError: gh: HTTP 502")
        return _delivered(coordinator, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        coordinator, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked"
    assert coordinator.state["checks"]["feature_worker_reissues"] == {
        "model:build": protocol.REISSUES}
    assert "feature_worker_recoveries" not in coordinator.state["checks"]
    assert f"exhausted {protocol.REISSUES} re-issues" in result["error"]
    assert "delivery_feature_finish_worker for worker build:model stayed lost" in result["error"]
    assert "GitHubTransientError" in result["error"] and "model" in result["error"]
    assert coordinator.admissions == [], "a lost observation is re-issued, never re-admitted"


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
            raise ApplicationError("connection reset by peer", type="ConnectionError")
        return {"closed": True, "outcome": "delivered",
                "closure": {"outcome": "delivered", "phase": "delivered", "cleanup": "confirmed"}}

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
    lost = [p["message"] for n, p in fixtures.calls
            if n == "delivery_project" and p["event_type"] == "feature_worker_observation_lost"]
    assert len(lost) == 1 and "ConnectionError: connection reset by peer" in lost[0]
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
@pytest.mark.parametrize("closure, resume_allowed, liveness, finishes, raised", [
    (TRANSIENT, True, True, False, protocol.FeatureWorkerStopped),
    (TRANSIENT, False, True, True, protocol.FeatureWorkerStopped),
    ({**TRANSIENT, "cleanup": "unknown"}, True, True, True, protocol.FeatureWorkerStopped),
    ({**TRANSIENT, "planning_defect": True}, True, True, True, protocol.FeatureWorkerStopped),
    ({**TRANSIENT, "outcome": "cancelled", "phase": "cancelled"}, True, True, True,
     protocol.FeatureWorkerStopped),
    (TERMINAL, True, True, True, protocol.FeatureWorkerStopped),
    (TRANSIENT, True, False, True, RuntimeError),
])
async def test_worker_hands_back_only_blocked_infrastructure_closures_before_retiring_them(
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
            return {"closed": True, "outcome": closure["outcome"], "closure": deepcopy(closure)}
        assert name == "delivery_feature_finish_worker"
        return {"outcome": closure["outcome"], "record": deepcopy(controller.record), "budget": {}}

    controller._activity = execute
    with pytest.raises(raised) as info:
        await protocol._worker(controller, {"run_id": "feature", "config_path": "/frozen"},
                               "model", "build", {}, resume_allowed=resume_allowed)
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
async def test_infrastructure_closures_resume_through_admission_within_a_durable_bound(
    monkeypatch, recovers,
):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind, resume_allowed, assignment))
        failures = sum(1 for item in attempts if item[:2] == (chunk_id, kind))
        if (chunk_id, kind) == ("client", "build") and (failures <= 2 or not recovers):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TRANSIENT))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    client = [(allowed, assignment) for c, k, allowed, assignment in attempts
              if (c, k) == ("client", "build")]
    assert [allowed for allowed, _ in client] == [True, True, False]
    assert client[0][1] is None, "the first attempt reserves on its own"
    assert all(assignment["resumed"] for _, assignment in client[1:]), (
        "every re-issue carries the admitted continuation instead of reserving again")
    assert controller.admissions == [("client", "build")] * 2
    assert controller.state["checks"]["feature_worker_recoveries"] == {"client:build": 2}
    recovering = [m for e, m in controller.messages if e == "feature_worker_recovering"]
    assert recovering == [
        "Resumed the build worker for client after an infrastructure failure (1 of 2)",
        "Resumed the build worker for client after an infrastructure failure (2 of 2)",
    ]
    if recovers:
        assert result["phase"] == "merged"
    else:
        assert result["outcome"] == "blocked"
        assert "client" in result["error"] and "exhausted 2 automatic resumes" in result["error"]
        assert "product repair" not in result["error"]
    assert any(name == "delivery_feature_stop" for name, _ in controller.events)


@pytest.mark.asyncio
async def test_refused_admission_stops_with_the_chunk_the_worker_failure_and_the_refusal(
    monkeypatch,
):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    closure = {**TRANSIENT, "error": "workstream execution needs recovery",
               "failure": {"classification": "transient", "cause_type": "role_interrupted"}}

    async def refusing(name, request):
        if name == "delivery_feature_reserve":
            controller.admissions.append((request["chunk_id"], request["kind"]))
            raise _activity_error(ApplicationError(REFUSAL, type="ValueError", non_retryable=True))
        return await Controller._activity(controller, name, request)

    controller._activity = refusing
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind))
        if (chunk_id, kind) == ("model", "build"):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(closure))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked"
    assert attempts.count(("model", "build")) == 1 and controller.admissions == [("model", "build")]
    assert "feature build worker for chunk model" in result["error"]
    assert "stopped (workstream execution needs recovery)" in result["error"]
    assert "automatic resume refused: ValueError: " + REFUSAL in result["error"]
    assert "feature_worker_recoveries" not in controller.state["checks"]
    assert not any(e == "feature_worker_recovering" for e, _ in controller.messages), (
        "resumption is projected only after the admission accepted it")


@pytest.mark.asyncio
async def test_transport_failures_before_admission_are_reissued_but_verdicts_stop(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    failures = iter([
        _transient_error("delivery_feature_reserve", type="BrokerReadbackUnavailable",
                         message="stack prerequisite fetch failed: could not resolve host"),
        _activity_error(_lost_heartbeat(), "delivery_feature_finish_worker"),
    ])
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind))
        if (chunk_id, kind) == ("model", "build") and attempts.count(("model", "build")) <= 2:
            raise next(failures)
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["phase"] == "merged"
    assert attempts.count(("model", "build")) == 3 and controller.admissions == []
    assert controller.state["checks"]["feature_worker_reissues"] == {"model:build": 2}
    assert "feature_worker_recoveries" not in controller.state["checks"]
    assert controller.sleeps == [protocol.OBSERVATION_PAUSE] * 2
    reissued = [m for e, m in controller.messages if e == "feature_worker_recovering"]
    assert len(reissued) == 2 and "BrokerReadbackUnavailable" in reissued[0]
    assert "TimeoutError" in reissued[1] and f"(2 of {protocol.REISSUES})" in reissued[1]

    for verdict in (
        ApplicationError("worker's prerequisites have not passed their integrated gates",
                         type="OwnershipConflict", non_retryable=True),
        ApplicationError("command failed (128): git fetch: not a git repository",
                         type="RuntimeError"),
    ):
        refused = LivenessController(plan())
        _protocol_stubs(monkeypatch, refused)
        verdicts = []

        async def stopping(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                           assignment=None, _verdict=verdict, _refused=refused,
                           _verdicts=verdicts):
            _verdicts.append((chunk_id, kind))
            if (chunk_id, kind) == ("model", "build"):
                raise _activity_error(_verdict)
            return _delivered(_refused, chunk_id, kind)

        monkeypatch.setattr(protocol, "_worker", stopping)
        result = await protocol.coordinate(
            refused, {"run_id": "run-two", "authorized_endpoint": "merged"})
        assert result["outcome"] == "blocked" and verdict.type in result["error"]
        assert verdicts.count(("model", "build")) == 1
        assert "feature_worker_recoveries" not in refused.state["checks"]


@pytest.mark.asyncio
@pytest.mark.parametrize("liveness", [True, False])
async def test_one_chunk_verdict_lets_the_running_sibling_build_close_first(monkeypatch, liveness):
    controller = LivenessController(plan(), liveness=liveness)
    _protocol_stubs(monkeypatch, controller)
    release = asyncio.Event()

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
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


def _closed_chunk_worker(service, monkeypatch, *, roles, failure, error, revision=9,
                         execution="modeled-closed"):
    """A real store, registry and git checkout; only the provider receipts and the
    Temporal closure proof are modeled. Returns what the coordinator observes."""
    store, parent, child, broker, candidate, member, _ = integration(service, monkeypatch)
    value = json.loads(parent["accepted_plan"])
    record = {"manifest": {"revision": 1, "plan": value,
                           "publication": {"stack_id": None, "members": [member]},
                           "workstream_issues": {
                               stream["id"]: {"url": "https://github.com/example/fixture/issues/10"}
                               for stream in value["workstreams"]}}}
    monkeypatch.setattr(activities, "current_record", lambda *_, **__: deepcopy(record))
    monkeypatch.setattr(publications, "current_record", lambda *_, **__: deepcopy(record))
    monkeypatch.setattr(publications, "live_members", lambda *_, **__: [
        {"number": member["number"], "state": "open", "merged": False}])
    roles = [{"candidate": candidate, **role} for role in roles]
    checks = {"failure": failure}
    state = {"run_id": child["run_id"], "phase": "blocked", "outcome": "blocked",
             "execution_state": "blocked", "cleanup": "confirmed", "revision": revision,
             "iteration": 0, "candidate": candidate, "pull_request": None,
             "roles": roles, "checks": checks, "error": error}
    store.project(child["run_id"], phase="blocked", execution_state="blocked", outcome="blocked",
                  cleanup="confirmed", event_type="blocked", message=error,
                  candidate=candidate, pull_request=None, checks=checks, iteration=0,
                  protocol_revision=revision, error=error)
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        for index, role in enumerate(roles):
            receipt = {k: v for k, v in role.items() if k not in {"role", "iteration", "candidate"}}
            db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                       "state,session_id,result_json,cleanup) "
                       "VALUES (?,?,?,0,?,'finished',?,?,'confirmed')",
                       (f"modeled-{index}", child["run_id"], role["role"], candidate["id"],
                        role.get("session_id"), json.dumps(receipt)))
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": execution,
              "request_digest": child["request_digest"], "recovery_digest": None,
              "result": state}
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda self, run_id, **kwargs: deepcopy(closed))
    chunk_id = child["feature_worker"]["chunk_id"]
    registry(parent).checkpoint(
        parent["feature_delivery"]["owner"],
        checkpoint_key(child, f"assignment:{chunk_id}:chunk"),
        {"run_id": child["run_id"], "store_path": str(store.config.tracking_db),
         "chunk_id": chunk_id, "kind": "chunk"})
    return store, parent, child, candidate, state


def _commands(store, child):
    with store._connect() as db:
        recovery = db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone()[0]
        commands = [r[0] for r in db.execute(
            "SELECT command_id FROM delivery_commands WHERE run_id=? ORDER BY rowid",
            (child["run_id"],))]
    return (json.loads(recovery) if recovery else None), commands


@pytest.mark.parametrize("variant", sorted(ADMITTED))
def test_every_closure_the_coordinator_offers_is_admitted_by_the_real_reserve_path(
    service, monkeypatch, variant,
):
    roles, cause, stage = ADMITTED[variant]
    error = "modeled infrastructure stop"
    failure = {"classification": "transient", "cause_type": cause, "stage": stage,
               "reason": error}
    store, parent, child, candidate, _ = _closed_chunk_worker(
        service, monkeypatch, roles=roles, failure=failure, error=error)
    # The child side classifies these exactly as the closure records them.
    assert _interrupted_role([{"candidate": candidate, **r} for r in roles]) == (
        cause if cause != "ci_deadline" else None)
    closure = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed", "error": error,
               "failure": failure, "planning_defect": False}
    assert protocol._resumable(closure)
    admitted = activities.reserve(parent, child["feature_worker"]["chunk_id"], "chunk")
    assert admitted["resumed"] is True and admitted["completed"] is False
    assert admitted["spec"]["run_id"] == child["run_id"]
    recovery, commands = _commands(store, child)
    assert recovery["kind"] == "stopped_delivery_resume"
    assert recovery["state"]["checks"]["failure"] == failure, "the continuation inherits it"
    assert len(commands) == 1 and commands[0].startswith("feature-resume-")


def test_an_implementation_death_is_neither_offered_nor_admitted(service, monkeypatch):
    from devflow_temporal.delivery_workflow import _failure_classification

    error = "workstream execution needs recovery"
    roles = [{"candidate": None, **r} for r in IMPLEMENT_DEATH]
    # What the worker's own _stop now records for this closure: a verdict.
    assert _interrupted_role(roles) is None
    failure = {**_failure_classification(None, None), "stage": "implement", "reason": error}
    assert failure["classification"] == "terminal"
    store, parent, child, candidate, _ = _closed_chunk_worker(
        service, monkeypatch, roles=IMPLEMENT_DEATH, failure=failure, error=error)
    closure = {"outcome": "blocked", "phase": "blocked", "cleanup": "confirmed", "error": error,
               "failure": failure, "planning_defect": False}
    assert not protocol._resumable(closure)
    # And were it ever offered, the admission refuses it with the reason the
    # coordinator surfaces in its stop.
    with pytest.raises(ValueError, match=REFUSAL):
        activities.reserve(parent, child["feature_worker"]["chunk_id"], "chunk")
    recovery, commands = _commands(store, child)
    assert recovery is None and commands == []


def test_a_second_in_generation_resume_admits_its_own_command(service, monkeypatch):
    from devflow_temporal.contracts import digest

    roles, cause, stage = ADMITTED["implement_pass_then_ci_deadline"]
    error = "modeled infrastructure stop"
    failure = {"classification": "transient", "cause_type": cause, "stage": stage,
               "reason": error}
    store, parent, child, candidate, state = _closed_chunk_worker(
        service, monkeypatch, roles=roles, failure=failure, error=error)
    chunk_id = child["feature_worker"]["chunk_id"]
    first = activities.reserve(parent, chunk_id, "chunk")
    recovery, commands = _commands(store, child)
    assert first["resumed"] and len(commands) == 1
    # The admitted continuation closes blocked again for infrastructure.
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
    later = {**state, "revision": 20, "checks": {"failure": failure}}
    store.project(child["run_id"], phase="blocked", execution_state="blocked", outcome="blocked",
                  cleanup="confirmed", event_type="blocked", message=error,
                  candidate=candidate, pull_request=None, checks={"failure": failure},
                  iteration=0, protocol_revision=20, error=error)
    with store._connect() as db:
        # The continuation's own finalization releases the claim it was admitted with.
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": "modeled-closed-again",
              "request_digest": child["request_digest"], "recovery_digest": digest(recovery),
              "result": later}
    monkeypatch.setattr(DeliveryStore, "_completed_temporal_result",
                        lambda self, run_id, **kwargs: deepcopy(closed))
    second = activities.reserve(parent, chunk_id, "chunk")
    assert second["resumed"] and second["spec"]["run_id"] == child["run_id"]
    _, commands = _commands(store, child)
    assert len(commands) == 2 and commands[0] != commands[1]


# --- M-24: merge and readback loops back off instead of stopping ------------------------------


@pytest.mark.asyncio
async def test_slow_merge_readback_backs_off_and_still_ends_merged(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    merges = iter([{"state": "pending"}] * 4)
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
            pending = next(merges, None)
            if pending is not None:
                return pending
            return {"state": "confirmed", "pull_requests": [
                {"number": item["number"]} for item in request["publication"]["pull_requests"]]}
        if name == "delivery_feature_settle_effects":
            controller.events.append((name, deepcopy(request)))
            return next(settlements)
        return await original(name, request)

    controller._activity = execute

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    # A merge confirmed after any number of pending polls ends the feature merged.
    assert result["phase"] == "merged" and result["outcome"] == "delivered"
    assert sum(1 for name, _ in controller.events if name == "delivery_feature_merge") == 5
    assert controller.sleeps == [
        timedelta(seconds=30), timedelta(minutes=1), timedelta(minutes=2), timedelta(minutes=4),
        timedelta(seconds=30), timedelta(minutes=1), timedelta(minutes=2),
    ]
    merging = [m for e, m in controller.messages if e == "feature_merging"]
    assert len(merging) == 2, "one projection names the pending merge"
    assert "#1, #2, #3" in merging[1] and "readback still pending" in merging[1]
    assert "up to 15 minutes" in merging[1]
    readback = [m for e, m in controller.messages if e == "feature_readback_pending"]
    assert len(readback) == 3
    assert all("merge:stack-42" in m and "RPCError: unavailable" in m for m in readback)
    assert [m.rsplit(" in ", 1)[1] for m in readback] == ["30 s", "60 s", "120 s"]
    assert "after 3 readbacks" in readback[2]
    assert result["checks"]["feature_readback"] == {"state": "confirmed"}
    assert controller.events[-1][0] == "delivery_feature_stop"


@pytest.mark.asyncio
async def test_real_temporal_slow_merge_records_backoff_timers_and_replays(monkeypatch):
    fixtures = FeatureActivities()
    base_merge = _base_handler(fixtures, "delivery_feature_merge")
    merges = []

    @activity.defn(name="delivery_feature_merge")
    async def merge(payload):
        merges.append(payload["publication"]["stack_id"])
        if len(merges) <= 3:
            return {"state": "pending"}
        return await base_merge(payload)

    monkeypatch.setattr(protocol, "READBACK_INTERVAL", timedelta(seconds=1))
    monkeypatch.setattr(protocol, "MERGE_READBACK_CAP", timedelta(seconds=2))
    spec = _feature_spec("slow-merge-readback")
    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue=spec["run_id"],
                          workflows=[DeliveryWorkflow],
                          activities=_handlers(fixtures, merge),
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec["run_id"], task_queue=spec["run_id"])
            result = await asyncio.wait_for(handle.result(), 60)
            history = await handle.fetch_history()
        await Replayer(workflows=[DeliveryWorkflow],
                       workflow_runner=UnsandboxedWorkflowRunner()).replay_workflow(history)
    assert result["phase"] == "merged" and len(merges) == 4
    assert _timers(history) == [timedelta(seconds=1), timedelta(seconds=2), timedelta(seconds=2)]
    merging = [p["message"] for n, p in fixtures.calls
               if n == "delivery_project" and p["event_type"] == "feature_merging"]
    assert len(merging) == 2 and "readback still pending" in merging[1]
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
@pytest.mark.parametrize("liveness, roles, cause, expected", [
    (True, IMPLEMENT_DEATH, None, ("terminal", None)),
    (True, [{"role": "implement", "status": "recovery_unknown",
             "finish_reason": "interrupted_monitor", "cleanup": "confirmed"}], None,
     ("terminal", None)),
    (True, ADMITTED["review_role_exception"][0], None, ("transient", "role_interrupted")),
    (True, [{"role": "implement", "status": "findings", "finish_reason": "done",
             "cleanup": "confirmed"},
            {"role": "review", "status": "blocked", "finish_reason": "exception",
             "cleanup": "confirmed"}], None, ("terminal", None)),
    (True, ADMITTED["implement_prelaunch"][0], None, ("transient", "role_prelaunch")),
    (True, [{**IMPLEMENT_DEATH[0], "cleanup": "unknown"}], None, ("terminal", None)),
    (True, [{"role": "implement", "status": "findings", "finish_reason": "done",
             "cleanup": "confirmed"}],
     _activity_error(_lost_heartbeat(), "delivery_role"), ("transient", "ActivityTimeoutError")),
    (False, ADMITTED["review_role_exception"][0], None, None),
])
async def test_feature_worker_stop_records_only_admissible_infrastructure_as_transient(
    liveness, roles, cause, expected,
):
    from devflow_temporal.delivery_workflow import _initial_controller_state

    spec = {"run_id": "build:model", "provider": "fake", "policy": {},
            "feature_worker": {"parent_run_id": "feature", "chunk_id": "model", "kind": "build"}}
    controller = DeliveryWorkflow()
    controller.feature_liveness_v1 = liveness
    controller.state = _initial_controller_state(spec)
    controller.state["roles"].extend(deepcopy(roles))

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
    assert state["cleanup"] == ("unknown" if roles[-1]["cleanup"] == "unknown" else "none")


@pytest.mark.asyncio
@pytest.mark.parametrize("liveness", [True, False])
async def test_feature_worker_continuation_drops_its_predecessors_failure_record(liveness):
    spec = {"run_id": "build:model", "provider": "fake", "policy": {},
            "feature_worker": {"parent_run_id": "feature", "chunk_id": "model", "kind": "build"}}
    controller = DeliveryWorkflow()
    controller.feature_liveness_v1 = liveness
    inherited = {"classification": "transient", "cause_type": "role_interrupted",
                 "stage": "review", "reason": "modeled infrastructure stop"}
    recovery = {"execution_spec": spec, "maximum_iteration": 0,
                "command": {"additional_iterations": 0},
                "execution_candidate": {"id": "c" * 64, "head": "h" * 40},
                "state": {"iteration": 0, "phase": "blocked", "revision": 3, "roles": [],
                          "checks": {"failure": inherited, "repair_budget": {"used": 1}}}}

    async def preflight(_spec, _recovery):
        return False

    controller._confirm_repair_preflight = preflight
    state = await controller._resume_stopped(spec, recovery)
    # A closure's classification describes only its own execution: a cancelled
    # or otherwise closed continuation must never look transient by inheritance.
    assert ("failure" in state["checks"]) is not liveness
    assert state["checks"]["repair_budget"] == {"used": 1}
    assert state["phase"] == "repair_preflight" and state["outcome"] is None


def _registered_build_worker(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    return store, parent, child


@pytest.mark.asyncio
async def test_worker_result_carries_the_closure_its_own_controller_recorded(service, monkeypatch):
    """The observation the coordinator decides a resume from comes from the real
    store row, before finish_worker retires the registry assignment."""
    store, parent, child = _registered_build_worker(service, monkeypatch)
    reference = {"spec": parent, "child_id": child["run_id"]}
    assert await activities.delivery_feature_worker_result(reference) == {"closed": False}
    failure = {"classification": "transient", "cause_type": "role_prelaunch",
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
        "outcome": "blocked", "phase": "blocked", "cleanup": "confirmed",
        "error": "workstream execution needs recovery", "failure": failure,
        "planning_defect": False,
    }}
    assert protocol._resumable(observation["closure"])
    assert not protocol._resumable({**observation["closure"], "cleanup": "unknown"})
    assert not protocol._resumable({**observation["closure"], "outcome": "cancelled"})
    with store._connect() as db:
        assigned = db.execute("SELECT 1 FROM delivery_runs WHERE run_id=? AND outcome='blocked'",
                              (child["run_id"],)).fetchone()
    assert assigned, "observing a closure never changes the worker's row"


# --- Re-issue paths keep the naming and `_resumable`; allowlist coverage ----------------------


@pytest.mark.asyncio
async def test_transient_admission_then_refusal_names_chunk_worker_error_and_refusal(monkeypatch):
    """The first post-closure admission fails transiently; the retried admission is
    refused. The stop names the chunk, the worker's own error and the refusal."""
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)
    reserves = []

    async def execute(name, request):
        if name == "delivery_feature_reserve":
            reserves.append(request["chunk_id"])
            if len(reserves) == 1:
                return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1",
                        "completed": False}
            if len(reserves) == 2:
                raise _transient_error(name, type="TimeoutExpired", message="gh timed out")
            raise _activity_error(ApplicationError(REFUSAL, type="ValueError", non_retryable=True))
        if name == "delivery_feature_wait_worker":
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TRANSIENT)}
        raise AssertionError(name)

    controller._activity = execute
    spec = {"run_id": "feature", "config_path": "/frozen/config"}
    with pytest.raises(protocol.FeatureWorkerStopped) as info:
        await protocol._recovering_worker(controller, spec, "model", "build", {})
    assert info.value.final and reserves == ["model"] * 3
    message = str(info.value)
    assert message.startswith("feature build worker for chunk model stopped (")
    assert TRANSIENT["error"] in message
    assert "automatic resume refused: ValueError: " + REFUSAL in message
    assert not protocol._infrastructure_failure(info.value)
    assert controller.state["checks"]["feature_worker_reissues"] == {"model:build": 1}
    assert "feature_worker_recoveries" not in controller.state["checks"]
    assert [m for e, m in controller.messages if e == "feature_worker_recovering"] == [
        "Re-issuing the build worker admission for model after TimeoutExpired: gh timed out "
        f"(1 of {protocol.REISSUES})"]
    assert controller.sleeps == [protocol.OBSERVATION_PAUSE]


@pytest.mark.asyncio
async def test_lost_finish_of_a_terminal_closure_reobserves_instead_of_reserving(monkeypatch):
    """A lost finish observation of a worker that closed with a verdict is
    re-observed with the assignment already held; the closed worker never
    re-enters reserve(), so the admission cannot bypass `_resumable`."""
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)
    calls = {"reserve": 0, "wait": 0, "finish": 0}

    async def execute(name, request):
        if name == "delivery_feature_reserve":
            calls["reserve"] += 1
            return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1", "completed": False}
        if name == "delivery_feature_wait_worker":
            calls["wait"] += 1
            return {"closed": True, "outcome": "blocked", "closure": deepcopy(TERMINAL)}
        assert name == "delivery_feature_finish_worker"
        calls["finish"] += 1
        if calls["finish"] <= protocol.OBSERVATION_RETRIES + 1:
            raise _transient_error(name, type="TimeoutExpired", message="observer timed out")
        return {"outcome": "blocked", "record": deepcopy(controller.record), "budget": {}}

    controller._activity = execute
    spec = {"run_id": "feature", "config_path": "/frozen/config"}
    with pytest.raises(protocol.FeatureWorkerStopped) as info:
        await protocol._recovering_worker(controller, spec, "model", "build", {})
    assert calls == {"reserve": 1, "wait": 2, "finish": protocol.OBSERVATION_RETRIES + 2}
    assert str(info.value) == (
        "feature build worker for chunk model stopped with preserved recovery evidence")
    assert info.value.closure == TERMINAL and not protocol._resumable(TERMINAL)
    assert controller.state["checks"]["feature_worker_reissues"] == {"model:build": 1}
    assert "feature_worker_recoveries" not in controller.state["checks"]
    recovering = [m for e, m in controller.messages if e == "feature_worker_recovering"]
    assert len(recovering) == 1
    assert recovering[0].startswith(
        "Re-issuing the build worker observation for model after ObservationLost: "
        "delivery_feature_finish_worker for worker build:model stayed lost")


@pytest.mark.asyncio
async def test_finish_verdict_names_the_chunk_and_the_failing_activity(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller, bounded_wait=True)

    async def execute(name, request):
        if name == "delivery_feature_reserve":
            return {"spec": {"run_id": "build:model"}, "workflow_id": "wf-1", "completed": False}
        if name == "delivery_feature_wait_worker":
            return {"closed": True, "outcome": "delivered",
                    "closure": {"outcome": "delivered", "cleanup": "confirmed"}}
        raise _activity_error(ApplicationError("gh: HTTP 401: Bad credentials",
                                               type="GitHubContractError"), name)

    controller._activity = execute
    spec = {"run_id": "feature", "config_path": "/frozen/config"}
    with pytest.raises(protocol.FeatureWorkerStopped) as info:
        await protocol._recovering_worker(controller, spec, "model", "build", {})
    assert str(info.value) == ("feature build worker for chunk model delivery_feature_finish_worker"
                               " failed: GitHubContractError: gh: HTTP 401: Bad credentials")
    assert info.value.final and controller.messages == [] and controller.sleeps == []


@pytest.mark.parametrize("kind, message, transient", [
    ("ConnectionResetError", "[Errno 54] Connection reset by peer", True),
    ("ConnectionRefusedError", "[Errno 61] Connection refused", True),
    ("ConnectionAbortedError", "[Errno 53] Software caused connection abort", True),
    ("BrokenPipeError", "[Errno 32] Broken pipe", True),
    ("OSError", f"[Errno {errno.ETIMEDOUT}] Operation timed out", True),
    ("OSError", f"[Errno {errno.EHOSTUNREACH}] No route to host", True),
    ("OSError", "[Errno 2] No such file or directory: 'gh'", False),
    ("OSError", "unexpected", False),
    ("DatabaseBusy", "database busy: database is locked", True),
    ("OperationalError", "database is locked", True),
    ("OperationalError", "database table is locked", True),
    ("OperationalError", "no such table: delivery_runs", False),
    ("TemporalUnavailable", "UNAVAILABLE: error trying to connect", True),
    ("RPCError", "permission denied", False),
    ("GitHubTransientError", "gh: HTTP 502: Bad Gateway", True),
    ("GitHubContractError", "gh: HTTP 401: Bad credentials", False),
    ("GitHubContractError", "gh: HTTP 404: Not Found", False),
    ("KeyError", "'head'", False),
    ("ValueError", "stopped checkpoint changed before admission", False),
    ("ClosureUnavailable", "closure unavailable", False),
    ("PermissionError", "[Errno 13] Permission denied", False),
])
def test_infrastructure_allowlist_matches_on_the_child_and_coordinator_sides(
    kind, message, transient,
):
    from devflow_temporal.delivery_workflow import _failure_classification

    error = _activity_error(ApplicationError(message, type=kind), "delivery_feature_finish_worker")
    assert (_failure_classification(error, None)["classification"] == "transient") is transient
    assert protocol._transient_activity_failure(error) is transient
    # A non-retryable failure of a transient type is a verdict on both sides.
    final = _activity_error(ApplicationError(message, type=kind, non_retryable=True))
    assert _failure_classification(final, None)["classification"] == "terminal"
    assert not protocol._transient_activity_failure(final)


@pytest.mark.parametrize("stderr, transient", [
    ("gh: HTTP 502: Bad Gateway (https://api.github.com/repos/example/fixture)", True),
    ("gh: HTTP 503: Service Unavailable", True),
    ("gh: HTTP 429: Too Many Requests", True),
    ("gh: HTTP 403: API rate limit exceeded for user ID 1", True),
    ("gh: HTTP 403: You have exceeded a secondary rate limit", True),
    ("gh: HTTP 403: Resource not accessible by integration", False),
    ("gh: HTTP 401: Bad credentials", False),
    ("gh: HTTP 404: Not Found", False),
    ("gh: HTTP 422: Validation Failed", False),
    # Transport failures carry no HTTP status: offline, DNS, TLS, reset connections.
    ("error connecting to api.github.com\ncheck your internet connection or https://githubstatus.com",
     True),
    ("Post \"https://api.github.com/graphql\": dial tcp: lookup api.github.com: no such host",
     True),
    ("net/http: TLS handshake timeout", True),
    ("read tcp 10.0.0.2:51234->140.82.112.6:443: read: connection reset by peer", True),
    # A certificate that fails verification is a deterministic misconfiguration.
    ("Get \"https://api.github.com/\": tls: failed to verify certificate: x509: certificate "
     "signed by unknown authority", False),
    ("could not determine base repo: no git remotes found", False),
    ("unknown command \"foo\" for \"gh api\"", False),
])
def test_github_server_faults_and_rate_limits_are_typed_transient(monkeypatch, stderr, transient):
    from devflow_temporal import delivery_github_contract as contract

    def run(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, stdout="", stderr=stderr)

    monkeypatch.setattr(contract.subprocess, "run", run)
    with pytest.raises(contract.GitHubContractError) as info:
        contract.GitHubDelivery().api("repos/example/fixture")
    assert isinstance(info.value, contract.GitHubTransientError) is transient
    assert protocol.transient_failure(type(info.value).__name__, str(info.value)) is transient
    match = re.search(r"HTTP (\d{3})", stderr)
    assert info.value.status == (int(match[1]) if match else None)


@pytest.mark.asyncio
async def test_worker_result_types_unavailable_temporal_as_infrastructure(service, monkeypatch):
    from temporalio.service import RPCError, RPCStatusCode

    store, parent, child = _registered_build_worker(service, monkeypatch)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
                   "execution_state='blocked',cleanup='confirmed' WHERE run_id=?",
                   (child["run_id"],))
    reference = {"spec": parent, "child_id": child["run_id"]}
    for status, expected in (
        (RPCStatusCode.UNAVAILABLE, activities.TemporalUnavailable),
        (RPCStatusCode.DEADLINE_EXCEEDED, activities.TemporalUnavailable),
        (RPCStatusCode.RESOURCE_EXHAUSTED, activities.TemporalUnavailable),
        (RPCStatusCode.PERMISSION_DENIED, RPCError),
        (RPCStatusCode.NOT_FOUND, RPCError),
    ):
        def failing(self, run_id, _status=status, **kwargs):
            raise RPCError("error trying to connect: tcp connect error", _status, b"")

        monkeypatch.setattr(DeliveryStore, "_completed_temporal_result", failing)
        with pytest.raises(expected) as info:
            await activities.delivery_feature_worker_result(reference)
        name = type(info.value).__name__
        assert protocol.transient_failure(name, str(info.value)) is (
            expected is activities.TemporalUnavailable)
        if expected is activities.TemporalUnavailable:
            assert str(info.value).startswith(status.name + ": ")


# --- An unsettled readback has a cancel exit and a bound; the effect stays pending -----------


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_by", ["cancel", "bound"])
async def test_unsettled_readback_exits_on_cancel_or_bound_with_the_effect_still_pending(
    monkeypatch, exit_by,
):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    monkeypatch.setattr(protocol, "READBACK_POLLS", 4)
    settlements = []
    original = controller._activity

    async def execute(name, request):
        if name == "delivery_feature_settle_effects":
            settlements.append(name)
            if exit_by == "cancel" and len(settlements) == 3:
                controller.cancel_requested = True
            return {"state": "pending", "effect_key": "merge:stack-42",
                    "reason": "RPCError: unavailable"}
        return await original(name, request)

    controller._activity = execute

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    polls = 3 if exit_by == "cancel" else 4
    assert len(settlements) == polls
    assert result["outcome"] == ("cancelled" if exit_by == "cancel" else "blocked")
    assert result["cleanup"] == "unknown"
    assert result["error"] == (
        f"original GitHub operation merge:stack-42 still unconfirmed after {polls} readbacks "
        "(RPCError: unavailable); the feature owner keeps it pending")
    # The effect is neither confirmed nor dropped, and the owner is not stopped.
    assert result["checks"]["feature_readback"]["state"] == "pending"
    assert result["checks"]["feature_stop"] == {
        "state": "withheld", "effect_key": "merge:stack-42", "reason": "RPCError: unavailable"}
    assert not any(name == "delivery_feature_stop" for name, _ in controller.events)
    assert controller.sleeps == [
        timedelta(seconds=30), timedelta(minutes=1), timedelta(minutes=2)][:polls - 1]
    assert controller.messages[-1] == (result["outcome"], result["error"])


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


def test_continue_settles_a_pending_effect_before_stopping_or_refuses_without_writing(
    recovered_worker, monkeypatch,
):
    """The coordinator left (an operator cancel during the readback wait) with an
    external operation still awaiting readback. Continuing reads it back through the
    same settlement first: refused and unchanged while GitHub has not answered,
    admitted once the exact readback confirms the original operation."""
    from devflow_temporal import delivery_github_contract as contract

    store, parent, child, shared, token, command, _ = recovered_worker
    issue = parent["feature_delivery"]["snapshot"]["issue"]
    key = "github_plan:v1"
    shared.intent(token, key, "github_plan", {"body": "Delivery plan v1"})
    comments = []
    monkeypatch.setattr(contract.GitHubDelivery, "pages", lambda self, endpoint: list(comments))

    def snapshot():
        with store._connect() as db:
            runs = db.execute("SELECT count(*) FROM delivery_runs").fetchone()[0]
        with shared.connect() as db:
            workers = [tuple(row) for row in db.execute(
                "SELECT worker_key,state FROM execution_workers WHERE issue_id=? "
                "ORDER BY worker_key", (issue["id"],))]
        return (runs, workers, shared.current(issue["id"])["state"],
                shared.effect(issue["id"], key)["state"], shared.budget(issue["id"]))

    before = snapshot()
    assert before[2] == "draining" and before[3] == "pending"
    with pytest.raises(UnresolvedEffect, match="github_plan:v1") as info:
        continue_feature(store, parent["run_id"], command)
    assert "original plan creation still needs exact readback" in str(info.value)
    assert snapshot() == before, "a refused continuation writes nothing"
    # GitHub answers: exactly one comment with the original body exists.
    comments.append({"id": 501, "node_id": "IC_501", "body": "Delivery plan v1"})
    result = continue_feature(store, parent["run_id"], command)
    assert shared.effect(issue["id"], key)["state"] != "pending"
    successor = store.effective_spec(result["run_id"])
    assert successor["feature_delivery"]["owner"]["generation"] == token["generation"] + 1
    assert shared.current(issue["id"])["state"] == "active"
    assert continue_feature(store, parent["run_id"], command) == result


def test_continue_reports_a_contradicted_effect_as_a_verdict_with_its_remedy(
    recovered_worker, monkeypatch,
):
    """A plan label renamed on GitHub can never read back: continuing refuses
    non-retryably, names what changed and the remedy, writes nothing and never
    confirms or drops the effect; once the label is restored the authentic
    readback confirms it and the continuation proceeds."""
    from devflow_temporal import delivery_feature_readback as readback_module
    from devflow_temporal import delivery_github_contract as contract

    store, parent, child, shared, token, command, _ = recovered_worker
    issue = parent["feature_delivery"]["snapshot"]["issue"]
    key = "github_plan_label:devflow:plan"
    shared.intent(token, key, "github_plan_label", {"name": "devflow:plan"})
    label = {"name": "devflow:renamed"}

    def api(self, endpoint, **kwargs):
        assert "/labels/" in endpoint, endpoint
        return dict(label)

    monkeypatch.setattr(contract.GitHubDelivery, "api", api)
    verdict = readback_module.settle(store, parent)
    assert verdict["state"] == "contradicted" and verdict["effect_key"] == key
    assert verdict["reason"] == "OwnershipConflict: original plan label identity changed"
    assert verdict["remedy"] == "restore the plan label 'devflow:plan' on example/fixture"
    owner = shared.current(issue["id"])["state"]
    for _ in range(2):
        with pytest.raises(OwnershipConflict) as info:
            continue_feature(store, parent["run_id"], command)
        assert not isinstance(info.value, UnresolvedEffect), "a verdict is not a wait"
        message = str(info.value)
        assert key in message and "original plan label identity changed" in message
        assert "will not settle by waiting" in message
        assert "restore the plan label 'devflow:plan' on example/fixture" in message
        assert shared.effect(issue["id"], key)["state"] == "pending"
        assert shared.current(issue["id"])["state"] == owner == "draining"
    label["name"] = "devflow:plan"
    result = continue_feature(store, parent["run_id"], command)
    assert shared.effect(issue["id"], key)["state"] != "pending"
    successor = store.effective_spec(result["run_id"])
    assert successor["feature_delivery"]["owner"]["generation"] == token["generation"] + 1


def test_settle_keeps_transport_and_unresolved_readbacks_pending(recovered_worker, monkeypatch):
    from devflow_temporal import delivery_feature_readback as readback_module
    from devflow_temporal.delivery_github_contract import GitHubTransientError

    store, parent, child, shared, token, command, _ = recovered_worker
    shared.intent(token, "github_plan:v1", "github_plan", {"body": "Delivery plan v1"})
    for failure in (GitHubTransientError("error connecting to api.github.com"),
                    UnresolvedEffect("original plan creation still needs exact readback"),
                    RuntimeError("command failed (1): gh api: unexpected EOF")):
        def failing(store_, spec, entry, gh, _failure=failure):
            raise _failure

        monkeypatch.setattr(readback_module, "settle_one", failing)
        readback = readback_module.settle(store, parent)
        assert readback["state"] == "pending" and readback["effect_key"] == "github_plan:v1"
        assert readback["reason"].startswith(type(failure).__name__ + ": ")
        assert "remedy" not in readback
        with pytest.raises(UnresolvedEffect, match="continue the feature again once it settles"):
            continue_feature(store, parent["run_id"], command)


@pytest.mark.asyncio
async def test_contradicted_readback_stops_the_coordinator_at_once_naming_the_remedy(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    verdict = {"state": "contradicted", "effect_key": "github_plan_label:devflow:plan",
               "kind": "github_plan_label",
               "reason": "OwnershipConflict: original plan label identity changed",
               "remedy": "restore the plan label 'devflow:plan' on example/fixture"}
    settlements = []
    original = controller._activity

    async def execute(name, request):
        if name == "delivery_feature_settle_effects":
            settlements.append(name)
            return dict(verdict)
        return await original(name, request)

    controller._activity = execute

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert len(settlements) == 1 and controller.sleeps == [], "a verdict is not polled"
    assert result["outcome"] == "blocked" and result["cleanup"] == "unknown"
    assert result["error"] == (
        "original GitHub operation github_plan_label:devflow:plan no longer matches its "
        "recorded identity (OwnershipConflict: original plan label identity changed; restore "
        "the plan label 'devflow:plan' on example/fixture); it will not settle by waiting; "
        "the feature owner keeps it pending")
    assert result["checks"]["feature_readback"] == verdict
    assert result["checks"]["feature_stop"]["state"] == "withheld"
    assert not any(name == "delivery_feature_stop" for name, _ in controller.events)


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_continue_settles_a_draining_owner_before_closing_its_open_revision(
    pending_preparation, monkeypatch,
):
    """A draining owner with a pending effect and an open `begin:`: continuing reads
    the effect back first (refused and unchanged while GitHub has not answered),
    then stops the owner, and only then enters the revision closure path, which
    closes the begun attempt (debit kept) and refuses once with the replay; the
    replay continues into the next generation."""
    from devflow_temporal import delivery_feature_readback as readback_module
    from devflow_temporal import delivery_feature_revisions as revisions
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_preparation import prepare_authority

    p = pending_preparation
    store, shared, spec = (p[k] for k in ("store", "shared", "spec"))
    issue_id = spec["feature_delivery"]["owner"]["issue_id"]
    response = continue_feature(store, spec["run_id"], p["request"])
    successor = prepare_authority(store, store.effective_spec(response["run_id"]))
    candidate = DeliveryBroker(store, successor).prepare()["candidate"]
    context = revisions.begin_revision(store, successor)
    revision_id = context["revision_id"]
    token = successor["feature_delivery"]["owner"]
    used = shared.budget(issue_id)["used"]
    # The successor's coordinator leaves its readback wait on an operator cancel
    # with a plan-comment write still pending: the owner drains, the begin stays.
    shared.drain(token)
    key = "github_plan:v2"
    shared.intent(token, key, "github_plan", {"body": "Delivery plan v2"})
    error = (f"original GitHub operation {key} still unconfirmed after 3 readbacks "
             "(RPCError: unavailable); the feature owner keeps it pending")
    # Owned native resources were finalized for real before the coordinator closed;
    # only the external readback stayed pending.
    receipt = RunResources(successor).finalize("cancelled")
    assert receipt["state"] == "confirmed", receipt
    checks = {"feature_readback": {"state": "pending", "effect_key": key},
              "feature_stop": {"state": "withheld", "effect_key": key,
                               "reason": "RPCError: unavailable"},
              "resource_cleanup": receipt}
    with store._connect() as db:
        before = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                 (successor["run_id"],)).fetchone())
    store.project(successor["run_id"], phase="cancelled", execution_state="terminal",
                  outcome="cancelled", cleanup="unknown", event_type="cancelled", message=error,
                  candidate=candidate, pull_request=None, checks=checks, iteration=0,
                  protocol_revision=(before["protocol_revision"] or 0) + 1, error=error)
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (successor["run_id"],)).fetchone())
    request = {"command_id": "continue-drained-revision", "expected_revision": row["revision"]}
    comments = []
    monkeypatch.setattr(readback_module, "GitHubDelivery",
                        lambda: SimpleNamespace(pages=lambda endpoint: list(comments)))
    # GitHub has not answered: refused before any write; the begin stays open.
    with pytest.raises(UnresolvedEffect, match=key):
        continue_feature(store, successor["run_id"], request)
    assert shared.current(issue_id)["state"] == "draining"
    assert shared.effect(issue_id, key)["state"] == "pending"
    assert revisions.revision_request(store.effective_spec(successor["run_id"]),
                                      shared)["revision_id"] == revision_id
    assert shared.budget(issue_id)["used"] == used
    # GitHub answers: the effect settles first, the owner stops, and only then the
    # revision closure path closes the begun attempt and refuses once with the replay.
    comments.append({"id": 601, "node_id": "IC_601", "body": "Delivery plan v2"})
    with pytest.raises(OwnershipConflict,
                       match="attempt stopped before adoption and is now closed") as info:
        continue_feature(store, successor["run_id"], request)
    assert "replay continue-feature continue-drained-revision" in str(info.value)
    assert shared.effect(issue_id, key)["state"] != "pending"
    assert shared.current(issue_id)["state"] == "stopped"
    assert shared.budget(issue_id)["used"] == used, "the begun attempt's debit is kept"
    assert revisions.revision_request(store.effective_spec(successor["run_id"]), shared) is None
    replayed = continue_feature(store, successor["run_id"], request)
    assert replayed == continue_feature(store, successor["run_id"], request)
    following = store.effective_spec(replayed["run_id"])
    assert following["feature_delivery"]["owner"]["generation"] == token["generation"] + 1


@pytest.mark.asyncio
async def test_transient_admission_retries_do_not_spend_a_resume(monkeypatch):
    controller = LivenessController(plan())
    _protocol_stubs(monkeypatch, controller)
    admissions = []
    admitted = controller._activity

    async def execute(name, request):
        if name == "delivery_feature_reserve":
            admissions.append(request["chunk_id"])
            if len(admissions) == 1:
                raise _transient_error(name, type="TimeoutExpired", message="gh timed out")
        return await admitted(name, request)

    controller._activity = execute
    attempts = []

    async def worker(_controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                     assignment=None):
        attempts.append((chunk_id, kind, resume_allowed))
        if (chunk_id, kind) == ("client", "build"):
            raise protocol.FeatureWorkerStopped(chunk_id, kind, closure=deepcopy(TRANSIENT))
        return _delivered(controller, chunk_id, kind)

    monkeypatch.setattr(protocol, "_worker", worker)
    result = await protocol.coordinate(
        controller, {"run_id": "run-one", "authorized_endpoint": "merged"})
    assert result["outcome"] == "blocked" and "exhausted 2 automatic resumes" in result["error"]
    assert [allowed for c, k, allowed in attempts if (c, k) == ("client", "build")] == [
        True, True, False]
    assert admissions == ["client"] * 3, "one transient admission, then two admitted resumes"
    assert controller.state["checks"]["feature_worker_recoveries"] == {"client:build": 2}
    assert controller.state["checks"]["feature_worker_reissues"] == {"client:build": 1}
    assert [m for e, m in controller.messages if e == "feature_worker_recovering"] == [
        "Re-issuing the build worker admission for client after TimeoutExpired: gh timed out "
        f"(1 of {protocol.REISSUES})",
        "Resumed the build worker for client after an infrastructure failure (1 of 2)",
        "Resumed the build worker for client after an infrastructure failure (2 of 2)",
    ]
