"""Controller boundary gaps: a bounded stop never ends unproven, and every closed
terminal record (a closed tracker deadline or a failed feature stop) is reconciled
from the authentic completed execution.

Private Store fixtures and synthetic authoritative-shaped histories exercise the
reconciler without providers. The real local Temporal server records the
reproduced admission shapes, drives the real dispatcher, projection activity and
reconciler against a private Store, and proves the histories replay.
"""
from __future__ import annotations

import asyncio
import copy
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.api.common.v1 import ActivityType, Payloads, WorkflowType
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import (
    Client,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHistory,
)
from temporalio.exceptions import ActivityError, ApplicationError, TimeoutType
from temporalio.exceptions import TimeoutError as ActivityTimeoutError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_feature_execution import feature_service
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal import (
    delivery_activities,
    delivery_feature_activities,
    delivery_terminal_recovery,
)
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_closed_projection import (
    EVENT,
    FEATURE_STOP_EVENT,
    _terminal_request,
    reconcile_completed_projections,
    reconcile_projection,
)
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import registry, require_execution
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _initial_controller_state

LIVENESS = "delivery-liveness-v2"
SDK_FAILURE_TYPES = [ValueError, KeyError, TypeError]


@pytest.fixture(autouse=True)
def forbid_external_github_and_provider_processes(monkeypatch):
    original = subprocess.Popen

    def guarded(argv, *args, **kwargs):
        if isinstance(argv, (list, tuple)) and (
            Path(str(argv[0])).name in {"gh", "codex", "claude"}
            or any("role_runner.py" in str(value) for value in argv)
        ):
            raise AssertionError("Controller fixtures cannot contact GitHub or launch providers")
        return original(argv, *args, **kwargs)

    monkeypatch.setattr(subprocess, "Popen", guarded)


@pytest.fixture
def clock(monkeypatch):
    now = [datetime(2026, 10, 11, tzinfo=UTC)]

    async def wait(predicate, *, timeout):
        if predicate():
            return
        now[0] += timeout
        await asyncio.sleep(0)
        raise TimeoutError

    async def sleep(duration):
        now[0] += duration
        await asyncio.sleep(0)

    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.now", lambda: now[0])
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.wait_condition", wait)
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.sleep", sleep)
    return now


def _activity_failure(name, scheduled_event_id, *, timeout=False):
    error = ActivityError(
        f"{name} response unavailable", scheduled_event_id=scheduled_event_id,
        started_event_id=scheduled_event_id + 1, identity="isolated", activity_type=name,
        activity_id=f"isolated-{scheduled_event_id}", retry_state=None,
    )
    error.__cause__ = (
        ActivityTimeoutError("Private readback timed out", type=TimeoutType.SCHEDULE_TO_CLOSE,
                             last_heartbeat_details=[])
        if timeout else ApplicationError("Private store refusal", non_retryable=True))
    return error


def _configuration_changed():
    raise ApplicationError("service configuration changed during an active run",
                           type="ValueError", non_retryable=True)


async def _replayed_liveness(history):
    """Replay with the program marker observed; the instance attribute must agree."""
    observed = []
    original = workflow.patched

    def patched(name):
        result = original(name)
        if name == LIVENESS:
            observed.append((workflow.instance(), result))
        return result

    with pytest.MonkeyPatch.context() as patch:
        patch.setattr("devflow_temporal.delivery_workflow.workflow.patched", patched)
        await Replayer(
            workflows=[DeliveryWorkflow], workflow_runner=UnsandboxedWorkflowRunner(),
            data_converter=DELIVERY_DATA_CONVERTER,
            workflow_failure_exception_types=SDK_FAILURE_TYPES,
        ).replay_workflow(history)
    (instance, result), = observed
    assert instance.liveness_v2 is result
    return result


async def _markers(history):
    """Recorded patch markers in order, and the first command that follows any marker."""
    markers, first_command = [], None
    for event in history.events:
        if event.HasField("marker_recorded_event_attributes"):
            details = event.marker_recorded_event_attributes.details
            for key in ("patch-data", "patch_id"):
                if key in details:
                    value = (await DELIVERY_DATA_CONVERTER.decode(details[key].payloads))[0]
                    markers.append((event.event_id,
                                    value["id"] if isinstance(value, dict) else value))
        elif first_command is None and (
                event.HasField("activity_task_scheduled_event_attributes")
                or event.HasField("timer_started_event_attributes")):
            first_command = event.event_id
    return markers, first_command


async def _failed_projection(history, failure):
    scheduled = next(event.activity_task_scheduled_event_attributes for event in history.events
                     if event.event_id == failure["scheduled_event_id"])
    assert scheduled.activity_type.name == "delivery_project"
    projection = (await DELIVERY_DATA_CONVERTER.decode(scheduled.input.payloads))[0]
    assert digest(projection) == failure["projection_sha256"]
    return projection


@pytest.mark.asyncio
async def test_real_tracker_conflict_at_stop_is_reconciled_from_the_completed_result(
    native_configuration, monkeypatch,
):
    """rr-workflow C-new: the real admission shape, tracker and projection both failing."""
    config, request = native_configuration
    service = create_app(config.path).state.delivery
    store = service.store
    store.submit(request)
    run_id = request["run_id"]
    spec = store.spec(run_id)
    assert all(spec.get(flag) == 1 for flag in (
        "workflow_failure_boundary_version", "terminal_tracker_version", "tracker_retry_version",
        "resource_cleanup_version"))
    projected = []

    @activity.defn(name="delivery_project")
    async def project(projection):
        projected.append(projection["event_type"])
        if projection["event_type"] == "preparing":
            return await delivery_activities.delivery_project(projection)
        raise ApplicationError("projection failed: ValueError: tracking store refused the row",
                               type="ProjectionRejected", non_retryable=True)

    @activity.defn(name="delivery_prepare")
    async def prepare(_request):
        _configuration_changed()

    @activity.defn(name="delivery_finalize_resources")
    async def finalize(_request):
        _configuration_changed()

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(_request):
        _configuration_changed()

    async with local_temporal() as environment:
        client = await Client.connect(environment.client.service_client.config.target_host,
                                      data_converter=DELIVERY_DATA_CONVERTER)

        async def healthy_client():
            return client

        monkeypatch.setattr(service, "healthy_client", healthy_client)
        async with Worker(client, task_queue=service.config.queue, workflows=[DeliveryWorkflow],
                          activities=[project, prepare, finalize, tracker],
                          workflow_failure_exception_types=SDK_FAILURE_TYPES):
            await service.dispatch_once()
            handle = client.get_workflow_handle("delivery-" + run_id)
            state = await asyncio.wait_for(handle.result(), 60)
        assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
        assert projected == ["preparing", "tracker_deadline"]
        assert state["outcome"] == "blocked" and state["cleanup"] == "unknown"
        assert state["error"] == "terminal tracker synchronization conflicted"
        checkpoint = state["checks"]["terminal_tracker_checkpoint"]
        assert checkpoint["state"] == "conflicted" and checkpoint["closed"]
        failure = state["checks"]["terminal_projection_failure"]
        assert failure["event_type"] == "tracker_deadline"
        assert "controller_failure" not in state["checks"]
        assert "pending_projection_failures" not in state["checks"]
        history = await handle.fetch_history()
        projection = await _failed_projection(history, failure)
        assert projection["outcome"] == "blocked" and _terminal_request(projection)
        markers, first_command = await _markers(history)
        assert markers[0][1] == LIVENESS and markers[0][0] < first_command
        assert await _replayed_liveness(history) is True
        # The private row is still at its last successful projection.
        before = store.detail(run_id)
        assert before["phase"] == "preparing" and before["outcome"] is None
        observed = await reconcile_completed_projections(store, client)
        assert [(item["run_id"], item["state"]) for item in observed] == [(run_id, "reconciled")]
        final = store.detail(run_id)
        assert final["phase"] == "blocked" and final["outcome"] == "blocked"
        assert final["error"] == state["error"] and final["cleanup"] == "unknown"
        assert final["checks"] == state["checks"]
        events = store.events(run_id)
        assert events[-1]["type"] == EVENT
        # Settled: no further Temporal reads, including after a service restart.
        monkeypatch.setattr(client, "get_workflow_handle",
                            lambda *_a, **_k: pytest.fail("settled execution was re-read"))
        assert await reconcile_completed_projections(store, client) == []
        assert await reconcile_completed_projections(DeliveryStore(config), client) == []
        assert store.events(run_id) == events


@pytest.mark.asyncio
async def test_real_tracker_timeout_inside_bounded_stop_closes_completed_with_bound_records():
    """A pending readback projection failing inside the terminal path no longer escapes."""
    spec = {"run_id": "isolated-readback-timeout", "provider": "fake",
            "policy": {"tracker_retry_seconds": 2}, "projection_retry_version": 1,
            "workflow_failure_boundary_version": 1, "resource_cleanup_version": 1,
            "terminal_tracker_version": 1, "tracker_retry_version": 1}
    projected = []

    @activity.defn(name="delivery_project")
    async def project(projection):
        projected.append(projection["event_type"])
        if projection["event_type"] == "preparing":
            return {"phase": projection["phase"]}
        raise ApplicationError("projection failed: ValueError: tracking store refused the row",
                               type="ProjectionRejected", non_retryable=True)

    @activity.defn(name="delivery_prepare")
    async def prepare(_request):
        _configuration_changed()

    @activity.defn(name="delivery_finalize_resources")
    async def finalize(_request):
        _configuration_changed()

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(_request):
        await asyncio.sleep(30)

    async with local_temporal() as environment:
        client = await Client.connect(environment.client.service_client.config.target_host,
                                      data_converter=DELIVERY_DATA_CONVERTER)
        async with Worker(client, task_queue="isolated-boundary-gaps", workflows=[DeliveryWorkflow],
                          activities=[project, prepare, finalize, tracker],
                          workflow_failure_exception_types=SDK_FAILURE_TYPES):
            handle = await client.start_workflow(
                DeliveryWorkflow.run, args=[spec, None], id="delivery-" + spec["run_id"],
                task_queue="isolated-boundary-gaps")
            state = await asyncio.wait_for(handle.result(), 60)
        assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
        assert projected[0] == "preparing" and projected[-1] == "tracker_deadline"
        assert set(projected[1:-1]) == {"tracker_pending"}
        assert state["phase"] == "waiting_tracker" and state["outcome"] is None
        assert state["tracker"]["retryable"] is True
        checkpoint = state["checks"]["terminal_tracker_checkpoint"]
        assert checkpoint["state"] == "pending" and checkpoint["closed"]
        assert checkpoint["waiting"] is False and checkpoint["outcome"] == "blocked"
        pending = state["checks"]["pending_projection_failures"]
        assert pending["count"] == len(projected) - 2 >= 1
        assert {item["event_type"] for item in pending["failures"]} == {"tracker_pending"}
        assert all(type(item["scheduled_event_id"]) is int for item in pending["failures"])
        failure = state["checks"]["terminal_projection_failure"]
        assert failure["event_type"] == "tracker_deadline"
        history = await handle.fetch_history()
        projection = await _failed_projection(history, failure)
        assert projection["outcome"] is None and _terminal_request(projection)
        assert projection["checks"]["pending_projection_failures"] == pending
        assert await _replayed_liveness(history) is True


@pytest.mark.asyncio
@pytest.mark.parametrize("liveness", [False, True])
async def test_readback_failure_inside_bounded_stop_only_closes_with_liveness(
    clock, monkeypatch, liveness,
):
    """rr-workflow D-new: without tracker_retry_version the tracker loop projects
    tracker_pending; the recorded behaviour (ControllerStopUnproven) is kept without
    the liveness marker."""
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched",
                        lambda name: liveness or name != LIVENESS)
    # Direct protocol calls have no workflow context; present one so the marker counts.
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.in_workflow", lambda: True)
    spec = {"run_id": "isolated-readback", "provider": "fake",
            "policy": {"tracker_retry_seconds": 60}, "projection_retry_version": 1,
            "workflow_failure_boundary_version": 1, "resource_cleanup_version": 1,
            "terminal_tracker_version": 1}
    controller = DeliveryWorkflow()
    requests = []

    async def execute(name, request, **_kwargs):
        if name == "delivery_project":
            requests.append(copy.deepcopy(request))
            if request["event_type"] == "preparing":
                return {}
            raise _activity_failure(name, 10 + 3 * len(requests))
        _configuration_changed()

    controller._activity = execute
    if not liveness:
        with pytest.raises(ApplicationError) as raised:
            await controller.run(spec)
        assert raised.value.type == "ControllerStopUnproven"
        assert [r["event_type"] for r in requests] == ["preparing", "tracker_pending",
                                                       "tracker_pending"]
        assert controller.state["checks"]["controller_failure"]["stage"] == "waiting_tracker"
        return
    state = await controller.run(spec)
    assert controller.liveness_v2 is True and controller.terminal_closing is False
    assert [r["event_type"] for r in requests] == [
        "preparing", "tracker_pending", "tracker_pending", "tracker_pending",
        "tracker_retry_required", "tracker_deadline"]
    assert state["phase"] == "waiting_tracker" and state["outcome"] is None
    assert state["cleanup"] == "unknown" and "controller_failure" not in state["checks"]
    checkpoint = state["checks"]["terminal_tracker_checkpoint"]
    assert checkpoint["closed"] and checkpoint["state"] == "pending"
    pending = state["checks"]["pending_projection_failures"]
    assert pending["count"] == 4
    assert [item["event_type"] for item in pending["failures"]] == [
        "tracker_pending", "tracker_pending", "tracker_pending", "tracker_retry_required"]
    assert [item["scheduled_event_id"] for item in pending["failures"]] == [16, 19, 22, 25]
    failure = state["checks"]["terminal_projection_failure"]
    assert failure["event_type"] == "tracker_deadline" and failure["scheduled_event_id"] == 28
    assert failure["projection_sha256"] == digest(requests[-1])
    assert failure["original_error"] == state["error"]
    assert _terminal_request(requests[-1])
    assert requests[-1]["checks"]["pending_projection_failures"] == pending


def _row(store, run_id="run-1"):
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone())


def _reset_preterminal(store, original):
    # The reused fixture seeds full original-session/source/attempt custody.
    # Restore only its private row to just before the failed terminal activity.
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='implementing',execution_state='running',"
                   "outcome=NULL,protocol_revision=8,checks_json='{}',cleanup='none',"
                   "error=NULL WHERE run_id='run-1'")
    store.project("run-1", phase="implementing", execution_state="running",
                  event_type="implementing", message="Private preterminal state",
                  candidate=original["candidate"], checks={}, iteration=0,
                  protocol_revision=8, cleanup="none")


def _bind_store(store, monkeypatch, spec):
    for name in ("effective_spec", "submitted_spec", "spec"):
        monkeypatch.setattr(store, name, lambda _run, _spec=spec: copy.deepcopy(_spec))
    monkeypatch.setattr(store, "owns_execution", lambda _spec: True)


class _History:
    """A mutable stand-in for WorkflowHistory (tests forge events) that still serialises."""

    def __init__(self, workflow_id, events):
        self.workflow_id, self.events = workflow_id, events

    def to_json(self):
        return WorkflowHistory(self.workflow_id, list(self.events)).to_json()


async def _history(spec, calls, *, workflow_id="delivery-run-1"):
    """calls: (name, request, result-or-None) in schedule order; None means failed."""
    events = [HistoryEvent(
        event_id=1, workflow_execution_started_event_attributes=
        WorkflowExecutionStartedEventAttributes(
            workflow_type=WorkflowType(name="DevflowDeliveryWorkflow"),
            input=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([spec, None]))))]
    for index, (name, request, result) in enumerate(calls):
        event_id = 10 + 3 * index
        events.append(HistoryEvent(
            event_id=event_id, activity_task_scheduled_event_attributes=
            ActivityTaskScheduledEventAttributes(
                activity_type=ActivityType(name=name),
                input=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([request])))))
        events.append(HistoryEvent(event_id=event_id + 2, **(
            {"activity_task_failed_event_attributes": ActivityTaskFailedEventAttributes(
                scheduled_event_id=event_id)} if result is None else
            {"activity_task_completed_event_attributes": ActivityTaskCompletedEventAttributes(
                scheduled_event_id=event_id,
                result=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([result])))})))
    return _History(workflow_id, events)


def _closed_client(spec, state, history, *, workflow_id="delivery-run-1"):
    description = SimpleNamespace(
        status=WorkflowExecutionStatus.COMPLETED, close_time=datetime.now(UTC),
        id=workflow_id, run_id="isolated-closed-execution",
        workflow_type="DevflowDeliveryWorkflow")

    async def memo(name, default=None):
        return spec["request_digest"] if name == "request_digest" else default

    description.memo_value = memo

    class Client:
        def get_workflow_handle(self, workflow_id, *, run_id=None):
            assert workflow_id == description.id and run_id in {None, description.run_id}
            return self

        async def describe(self):
            return description

        async def result(self):
            return copy.deepcopy(state)

        async def fetch_history(self):
            return history

    return Client(), description


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["conflicted", "pending-swallowed", "pending-committed"])
async def test_closed_tracker_deadline_records_are_reconciled_and_forgeries_refused(
    unpublished, monkeypatch, clock, form,
):
    """Already recorded flagged runs: the reconciler, not the workflow, accepts the
    tracker_deadline record that replaced the terminal event at stop time."""
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, _, original, _ = unpublished
    spec = {**store.effective_spec("run-1"), "terminal_tracker_version": 1}
    spec["policy"] = {**spec["policy"], "tracker_retry_seconds": 60}
    assert spec["workflow_failure_boundary_version"] == 1 and spec["tracker_retry_version"] == 1
    _bind_store(store, monkeypatch, spec)
    _reset_preterminal(store, original)
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = controller.liveness_v2 = True
    controller.state = copy.deepcopy(original)
    calls = []

    async def execute(name, request, **_kwargs):
        if name == "delivery_terminal_tracker":
            raise _activity_failure(name, 99, timeout=form != "conflicted")
        assert name == "delivery_project"
        event_id = 10 + 3 * len(calls)
        if request["event_type"] == "tracker_pending" and form == "pending-committed":
            store.project("run-1", **{k: v for k, v in request.items() if k != "spec"})
            calls.append((name, copy.deepcopy(request), {}))
            return {}
        calls.append((name, copy.deepcopy(request), None))
        raise _activity_failure(name, event_id)

    controller._activity = execute
    await controller._project(spec, "blocked", original["error"])
    state = controller.state
    failure = state["checks"]["terminal_projection_failure"]
    assert failure["event_type"] == "tracker_deadline"
    projection = calls[-1][1]
    assert projection["event_type"] == "tracker_deadline" and _terminal_request(projection)
    checkpoint = state["checks"]["terminal_tracker_checkpoint"]
    if form == "conflicted":
        assert len(calls) == 1 and checkpoint["state"] == "conflicted"
        assert state["outcome"] == "blocked" and state["phase"] == "blocked"
    else:
        assert checkpoint["state"] == "pending" and checkpoint["closed"]
        assert state["outcome"] is None and state["phase"] == "waiting_tracker"
        readbacks = [request["event_type"] for _, request, _ in calls[:-1]]
        assert readbacks and set(readbacks) == {"tracker_pending"}
        pending = state["checks"].get("pending_projection_failures", {"count": 0})
        assert pending["count"] == (0 if form == "pending-committed" else len(readbacks))
    history = await _history(spec, calls)
    client, _ = _closed_client(spec, state, history)
    before = _row(store)
    if form == "pending-committed":
        assert before["protocol_revision"] == projection["protocol_revision"]
        assert before["phase"] == "waiting_tracker"
    else:
        assert before["protocol_revision"] == 8
    exact = copy.deepcopy(history.events)
    for forgery in ("open-checkpoint", "foreign-outcome", "missing-checkpoint"):
        forged = copy.deepcopy(projection)
        if forgery == "open-checkpoint":
            forged["checks"]["terminal_tracker_checkpoint"]["closed"] = False
        elif forgery == "foreign-outcome":
            forged["outcome"] = "delivered" if form == "conflicted" else "blocked"
        else:
            forged["checks"].pop("terminal_tracker_checkpoint")
        forged_state = copy.deepcopy(state)
        forged_state["checks"]["terminal_projection_failure"]["projection_sha256"] = digest(forged)
        history.events = copy.deepcopy(exact)
        history.events[-2].activity_task_scheduled_event_attributes.input.CopyFrom(
            Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([forged])))
        forged_client, _ = _closed_client(spec, forged_state, history)
        with pytest.raises(ValueError, match="not bound to its failed terminal request"):
            await reconcile_projection(store, forged_client, _row(store))
        assert _row(store) == before
    history.events = exact
    assert (await reconcile_projection(store, client, _row(store)))["state"] == "reconciled"
    final = _row(store)
    assert final["phase"] == state["phase"] and final["outcome"] == state["outcome"]
    assert final["error"] == state["error"] and final["cleanup"] == state["cleanup"]
    assert final["protocol_revision"] == projection["protocol_revision"]
    assert json.loads(final["checks_json"]) == state["checks"]
    assert json.loads(final["tracker_json"]) == state["tracker"]
    events = store.events("run-1")
    assert events[-1]["type"] == EVENT and events[-1]["run_revision"] == final["revision"]
    assert (await reconcile_projection(store, client, _row(store)))["state"] == "observed"
    assert store.events("run-1") == events


async def _failed_feature_stop(unpublished, monkeypatch, *, terminal_failed):
    """A bounded stop whose feature stop failed, with a recorded `stop_feature` double."""
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, _, original, _ = unpublished
    owner = {"issue_id": "private-feature", "generation": 1}
    spec = {**store.effective_spec("run-1"), "feature_delivery": {"owner": owner}}
    _bind_store(store, monkeypatch, spec)
    _reset_preterminal(store, original)
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = controller.liveness_v2 = True
    controller.state = copy.deepcopy(original)
    calls = []

    async def execute(name, request, **_kwargs):
        event_id = 10 + 3 * len(calls)
        if name == "delivery_project":
            if terminal_failed:
                calls.append((name, copy.deepcopy(request), None))
                raise _activity_failure(name, event_id)
            store.project("run-1", **{k: v for k, v in request.items() if k != "spec"})
            calls.append((name, copy.deepcopy(request), {}))
            return {}
        assert name == "delivery_feature_stop"
        calls.append((name, copy.deepcopy(request), None))
        raise _activity_failure(name, event_id)

    controller._activity = execute
    await controller._project(spec, "blocked", original["error"])
    state = controller.state
    record = state["checks"]["feature_stop_failure"]
    stop_request = calls[1][1]
    assert record["scheduled_event_id"] == 13 and record["request_sha256"] == digest(stop_request)
    assert stop_request["checkpoint"]["completed_chunks"] == []
    assert stop_request["checkpoint"]["state"]["checks"] == {
        key: value for key, value in state["checks"].items() if key != "feature_stop_failure"}
    assert ("terminal_projection_failure" in state["checks"]) is terminal_failed
    history = await _history(spec, calls)
    client, _ = _closed_client(spec, state, history)
    stops = []
    results = [{"owner": owner, "state": "stopped"}]

    def stop_feature(actual_spec, checkpoint):
        stops.append((actual_spec, checkpoint))
        return results[-1]

    monkeypatch.setattr(delivery_feature_activities, "stop_feature", stop_feature)
    return store, spec, state, stop_request, history, client, stops, results


@pytest.mark.asyncio
@pytest.mark.parametrize("terminal_failed", [False, True])
async def test_failed_feature_stop_is_replayed_from_its_scheduled_request(
    unpublished, monkeypatch, terminal_failed,
):
    store, spec, state, stop_request, history, client, stops, results = (
        await _failed_feature_stop(unpublished, monkeypatch, terminal_failed=terminal_failed))
    before, before_events = _row(store), store.events("run-1")
    exact = copy.deepcopy(history.events)
    forged = copy.deepcopy(stop_request)
    forged["checkpoint"]["state"]["candidate"] = {"id": "foreign"}
    history.events[-2].activity_task_scheduled_event_attributes.input.CopyFrom(
        Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([forged])))
    with pytest.raises(ValueError, match="failed feature stop request"):
        await reconcile_projection(store, client, _row(store))
    history.events = exact
    results.append({"owner": "foreign", "state": "stopped"})
    with pytest.raises(ValueError, match="did not settle the recorded owner"):
        await reconcile_projection(store, client, _row(store))
    assert stops == [(spec, stop_request["checkpoint"])]
    assert _row(store) == before or terminal_failed
    assert not any(event["type"] == FEATURE_STOP_EVENT for event in store.events("run-1"))
    results.append({"owner": spec["feature_delivery"]["owner"], "state": "stopped"})
    observed = await reconcile_projection(store, client, _row(store))
    assert observed["state"] == "reconciled"
    assert observed["feature_stop_key"].startswith("closed-feature-stop:isolated-closed-execution:")
    assert len(stops) == 2 and stops[-1] == (spec, stop_request["checkpoint"])
    events = store.events("run-1")
    assert events[-1]["type"] == FEATURE_STOP_EVENT
    assert events[-1]["payload"]["scheduled_event_id"] == 13
    final = _row(store)
    if terminal_failed:
        assert [event["type"] for event in events[len(before_events):]] == [
            EVENT, FEATURE_STOP_EVENT]
        assert final["phase"] == "blocked" and json.loads(final["checks_json"]) == state["checks"]
    else:
        assert len(events) == len(before_events) + 1
        assert {k: v for k, v in final.items() if k != "updated_at"} == {
            k: v for k, v in before.items() if k != "updated_at"}
    assert events[-1]["run_revision"] == final["revision"]
    assert (await reconcile_projection(store, client, _row(store)))["state"] == "observed"
    assert len(stops) == 2 and store.events("run-1") == events


@pytest.mark.asyncio
async def test_feature_stop_replay_does_not_wait_for_a_refused_row(unpublished, monkeypatch):
    """The stop request is bound to the result on its own: a row the reconciler
    refuses to overwrite does not keep the feature owner active."""
    store, spec, state, stop_request, _history_, client, stops, _results = (
        await _failed_feature_stop(unpublished, monkeypatch, terminal_failed=True))
    store.project("run-1", phase="newer", execution_state="running", event_type="newer",
                  message="Newer private row", protocol_revision=state["revision"] + 1)
    before = _row(store)
    for _pass in range(2):
        with pytest.raises(ValueError, match="would overwrite a newer row"):
            await reconcile_projection(store, client, _row(store))
    assert stops == [(spec, stop_request["checkpoint"])]
    events = store.events("run-1")
    assert events[-1]["type"] == FEATURE_STOP_EVENT
    assert not any(event["type"] == EVENT for event in events)
    assert {k: v for k, v in _row(store).items() if k != "updated_at"} == {
        k: v for k, v in before.items() if k != "updated_at"}


@pytest.mark.asyncio
async def test_controller_failure_stage_names_the_recovery_without_loaded_state(monkeypatch):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    spec = {"run_id": "isolated-stage", "provider": "fake", "policy": {},
            "workflow_failure_boundary_version": 1}
    for recovery, expected in (
        ({"kind": "unknown", "state": {"run_id": "x"}}, "recovery:unknown"),
        ({"kind": "stopped_delivery_resume", "state": {"phase": "blocked"}}, "blocked"),
        (None, "preparing"),
    ):
        controller = DeliveryWorkflow()
        calls = []

        async def execute(name, request, *, _calls=calls, **_kwargs):
            _calls.append((name, request["event_type"]))
            return {}

        async def protocol(*_args):
            raise ValueError("Private controller defect before any state was loaded")

        controller._activity, controller._run_protocol = execute, protocol
        state = await controller.run(spec, recovery)
        assert state["checks"]["controller_failure"]["stage"] == expected
        assert state["outcome"] == "blocked" and calls == [("delivery_project", "blocked")]


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["readbacks-committed", "all-failed", "deadline-committed"])
async def test_reconciled_expired_deadline_is_admissible_by_terminal_tracker_recovery(
    unpublished, monkeypatch, clock, form,
):
    """The closed tail of a reconciled expired deadline is the failed last projection
    bound by the completed result's record; `recover()` queues the reconciliation-only
    successor without that record."""
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, _, original, _ = unpublished
    spec = {**store.effective_spec("run-1"), "terminal_tracker_version": 1}
    _bind_store(store, monkeypatch, spec)
    _reset_preterminal(store, original)
    # The frozen transition releases only after a confirmed finalization; the
    # native receipt on disk is the one private seam replaced here.
    receipt = {"state": "confirmed", "process_cleanup": "observed-native-confirmed",
               "resource_cleanup": "confirmed", "receipt_sha256": "isolated-finalization"}
    monkeypatch.setattr(delivery_terminal_recovery, "_stopped_cleanup",
                        lambda _spec, **_kwargs: {"receipt_sha256": receipt["receipt_sha256"]})
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = controller.liveness_v2 = True
    controller.state = copy.deepcopy(original)
    controller.state["checks"]["resource_cleanup"] = receipt
    calls = []
    if form == "all-failed":
        # The preterminal projection that committed before the stop (revision 8).
        calls.append(("delivery_project", {
            "spec": spec, "phase": "implementing", "event_type": "implementing",
            "protocol_revision": 8, "iteration": 0, "key": "implementing:0:8"}, {}))

    async def execute(name, request, **_kwargs):
        if name == "delivery_terminal_tracker":
            raise _activity_failure(name, 99, timeout=True)
        assert name == "delivery_project"
        event_id = 10 + 3 * len(calls)
        committed = form == "deadline-committed" or (
            form == "readbacks-committed" and request["event_type"] == "tracker_pending")
        if committed:
            store.project("run-1", **{k: v for k, v in request.items() if k != "spec"})
            calls.append((name, copy.deepcopy(request), {}))
            return {}
        calls.append((name, copy.deepcopy(request), None))
        raise _activity_failure(name, event_id)

    controller._activity = execute
    await controller._project(spec, "blocked", original["error"])
    state = controller.state
    projection = calls[-1][1]
    assert projection["event_type"] == "tracker_deadline" and projection["outcome"] is None
    assert projection["checks"]["terminal_tracker_checkpoint"]["release"] is True
    assert ("terminal_projection_failure" in state["checks"]) is (form != "deadline-committed")
    history = await _history(spec, calls)
    client, _ = _closed_client(spec, state, history)
    if form != "deadline-committed":
        assert (await reconcile_projection(store, client, _row(store)))["state"] == "reconciled"
    row = _row(store)
    assert row["phase"] == "waiting_tracker" and row["outcome"] is None
    assert json.loads(row["checks_json"]) == state["checks"]
    closed = await delivery_terminal_recovery.closed_tail(store, "run-1", client)
    assert closed["projection"]["event_type"] == "tracker_deadline"
    assert closed["projection"]["checks"] == state["checks"]
    assert closed["projection"]["protocol_revision"] == row["protocol_revision"]
    payload = {"command_id": "terminal-" + form, "expected_revision": row["protocol_revision"]}
    response = await delivery_terminal_recovery.recover(store, "run-1", payload, client)
    assert response["phase"] == "terminal_tracker_recovery_queued"
    assert response["reconciliation_only"] is True
    after = _row(store)
    assert after["phase"] == "terminal_tracker_recovery_queued"
    assert after["workflow_id"] == response["workflow_id"] != "delivery-run-1"
    recovery = json.loads(after["recovery_json"])
    assert recovery["kind"] == "terminal_tracker_recovery"
    assert recovery["closed"]["execution_run_id"] == "isolated-closed-execution"
    assert recovery["closed"]["projection"]["event_type"] == "tracker_deadline"
    assert "terminal_projection_failure" not in recovery["state"]["checks"]
    assert recovery["state"]["checks"]["terminal_tracker_checkpoint"]["closed"] is True
    assert recovery["state"]["outcome"] is None and recovery["state"]["run_id"] == "run-1"
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_outbox WHERE run_id='run-1'"
                          ).fetchone()[0] == "pending"
    assert store.events("run-1")[-1]["type"] == "terminal_tracker_recovery_queued"


@pytest.mark.asyncio
async def test_failed_feature_stop_replay_stops_the_real_execution_registry(service, monkeypatch):
    """The replayed stop is the real `stop_feature` against the real ownership
    registry; afterwards the coordinator is stopped for every activity."""
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    run_id = request["run_id"]
    spec = store.effective_spec(run_id)
    owner = spec["feature_delivery"]["owner"]
    assert spec["workflow_failure_boundary_version"] == 1 and not spec.get("feature_worker")
    assert registry(spec).current(owner["issue_id"])["state"] == "active"
    workflow_id = "delivery-" + run_id
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id=? WHERE run_id=?", (workflow_id, run_id))
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = controller.liveness_v2 = True
    controller.state = {**_initial_controller_state(spec), "revision": 2, "phase": "blocked",
                        "execution_state": "blocked", "outcome": "blocked",
                        "error": "preparation failed: ValueError"}
    calls = []

    async def execute(name, projection, **_kwargs):
        event_id = 10 + 3 * len(calls)
        if name == "delivery_project":
            store.project(run_id, **{k: v for k, v in projection.items() if k != "spec"})
            calls.append((name, copy.deepcopy(projection), {}))
            return {}
        assert name == "delivery_feature_stop"
        calls.append((name, copy.deepcopy(projection), None))
        raise _activity_failure(name, event_id)

    controller._activity = execute
    await controller._project(spec, "blocked", controller.state["error"])
    state = controller.state
    assert state["checks"]["feature_stop_failure"]["scheduled_event_id"] == 13
    assert registry(spec).current(owner["issue_id"])["state"] == "active"
    history = await _history(spec, calls, workflow_id=workflow_id)
    client, _ = _closed_client(spec, state, history, workflow_id=workflow_id)
    observed = await reconcile_projection(store, client, _row(store, run_id))
    assert observed["state"] == "reconciled" and "feature_stop_key" in observed
    assert registry(spec).current(owner["issue_id"])["state"] == "stopped"
    with pytest.raises(OwnershipConflict):
        require_execution(store, spec)
    events = store.events(run_id)
    assert events[-1]["type"] == FEATURE_STOP_EVENT
    assert (await reconcile_projection(store, client, _row(store, run_id)))["state"] == "observed"
    assert store.events(run_id) == events


@pytest.mark.asyncio
async def test_boundary_history_recorded_without_the_marker_replays_its_recorded_path():
    """A flagged execution recorded by the previous deployment (no liveness marker,
    stop unproven) replays under this code on its recorded command path."""
    spec = {"run_id": "isolated-premarker", "provider": "fake",
            "policy": {"tracker_retry_seconds": 2}, "projection_retry_version": 1,
            "workflow_failure_boundary_version": 1, "resource_cleanup_version": 1,
            "terminal_tracker_version": 1}

    @activity.defn(name="delivery_project")
    async def project(projection):
        if projection["event_type"] == "preparing":
            return {}
        raise ApplicationError("projection failed: ValueError: tracking store refused the row",
                               type="ProjectionRejected", non_retryable=True)

    @activity.defn(name="delivery_prepare")
    async def prepare(_request):
        _configuration_changed()

    @activity.defn(name="delivery_finalize_resources")
    async def finalize(_request):
        _configuration_changed()

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(_request):
        _configuration_changed()

    original = workflow.patched
    async with local_temporal() as environment:
        client = await Client.connect(environment.client.service_client.config.target_host,
                                      data_converter=DELIVERY_DATA_CONVERTER)
        with pytest.MonkeyPatch.context() as patch:
            # Record exactly as the previous deployment did: no liveness marker command.
            patch.setattr("devflow_temporal.delivery_workflow.workflow.patched",
                          lambda name: False if name == LIVENESS else original(name))
            async with Worker(client, task_queue="isolated-premarker", workflows=[DeliveryWorkflow],
                              activities=[project, prepare, finalize, tracker],
                              workflow_failure_exception_types=SDK_FAILURE_TYPES):
                handle = await client.start_workflow(
                    DeliveryWorkflow.run, args=[spec, None], id="delivery-" + spec["run_id"],
                    task_queue="isolated-premarker")
                with pytest.raises(WorkflowFailureError) as failed:
                    await asyncio.wait_for(handle.result(), 60)
        assert failed.value.cause.type == "ControllerStopUnproven"
        assert (await handle.describe()).status == WorkflowExecutionStatus.FAILED
        history = await handle.fetch_history()
        markers, _ = await _markers(history)
        assert LIVENESS not in {name for _, name in markers}
        assert await _replayed_liveness(history) is False
