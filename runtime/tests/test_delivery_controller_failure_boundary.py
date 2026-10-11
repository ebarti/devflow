"""Controller failure prevention and authenticated public projection recovery.

Private Store/Git fixtures and synthetic authoritative-shaped histories below
exercise admission without providers. The final case uses a new local Temporal
server to verify actual exception classification, completion and replay.
"""
from __future__ import annotations

import asyncio
import copy
import json
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
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
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from temporalio.worker._workflow_instance import _WorkflowInstanceImpl
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

import devflow_temporal.delivery_feature_workflow as feature_workflow
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import DeliveryService, create_app
from devflow_temporal.delivery_closed_projection import EVENT, reconcile_projection
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_stopped_resume import KIND
from devflow_temporal.delivery_workflow import DeliveryWorkflow


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


def _projection_error():
    error = ActivityError(
        "Terminal projection response unavailable", scheduled_event_id=10,
        started_event_id=11, identity="isolated", activity_type="delivery_project",
        activity_id="isolated-project", retry_state=None,
    )
    error.__cause__ = ApplicationError("Private projection refusal", non_retryable=True)
    return error


@pytest.mark.asyncio
async def test_marker_boundary_and_legacy_sdk_failure_classification(monkeypatch):
    spec = {"run_id": "isolated-controller", "provider": "fake", "policy": {}}
    invalid = {"kind": "stopped_delivery_resume", "execution_spec": {"run_id": "foreign"}}
    for marked, patched in ((False, True), (True, False), (True, True)):
        controller = DeliveryWorkflow()
        calls = []

        async def execute(name, request, *, _calls=calls, **_kwargs):
            _calls.append((name, copy.deepcopy(request)))
            assert name == "delivery_project"
            return {}

        controller._activity = execute
        monkeypatch.setattr(workflow, "patched", lambda _name, _patched=patched: _patched)
        supplied = {**spec, **({"workflow_failure_boundary_version": 1} if marked else {})}
        if not (marked and patched):
            with pytest.raises(ValueError, match="finite command authority"):
                await controller.run(supplied, invalid)
            assert calls == [] and controller.state == {}
        else:
            state = await controller.run(supplied, invalid)
            assert state["outcome"] == "blocked" and state["cleanup"] == "unknown"
            assert state["checks"]["controller_failure"]["error_type"] == "ValueError"
            assert [name for name, _ in calls] == ["delivery_project"]

    definition = workflow._Definition.must_from_class(DeliveryWorkflow)
    sdk = SimpleNamespace(_defn=definition, _dynamic_failure_exception_types=None,
                          _worker_level_failure_exception_types=[])
    for error in (ValueError("v"), KeyError("k"), TypeError("t")):
        assert not _WorkflowInstanceImpl.workflow_is_failure_exception(sdk, error)
        sdk._worker_level_failure_exception_types = [ValueError, KeyError, TypeError]
        assert _WorkflowInstanceImpl.workflow_is_failure_exception(sdk, error)
        sdk._worker_level_failure_exception_types = []
    sdk._worker_level_failure_exception_types = [ValueError, KeyError, TypeError]
    assert not _WorkflowInstanceImpl.workflow_is_failure_exception(sdk, RuntimeError("r"))
    assert not _WorkflowInstanceImpl.workflow_is_failure_exception(
        sdk, workflow.NondeterminismError("unmatched command"))
    for error in (workflow.NondeterminismError("unmatched command"), asyncio.CancelledError()):
        controller = DeliveryWorkflow()

        async def protocol(*_args, _error=error):
            raise _error

        controller._run_protocol = protocol
        with pytest.raises(type(error)):
            await controller.run({**spec, "workflow_failure_boundary_version": 1})
        assert controller.state == {}


@pytest.mark.asyncio
async def test_terminal_failure_retains_original_error_and_truthful_cleanup(monkeypatch):
    monkeypatch.setattr(workflow, "patched", lambda _name: True)
    for cleanup in ("confirmed", "unknown"):
        controller = DeliveryWorkflow()
        projected = []

        async def execute(name, request, *, _cleanup=cleanup, _projected=projected, **_kwargs):
            if name == "delivery_prepare":
                raise ValueError("Original preparation failure")
            if name == "delivery_finalize_resources":
                return {"state": _cleanup, "process_cleanup": "observed-native-confirmed",
                        "resource_cleanup": _cleanup}
            assert name == "delivery_project"
            if request["event_type"] == "blocked":
                _projected.append(copy.deepcopy(request))
                raise _projection_error()
            return {}

        controller._activity = execute
        state = await controller.run({"run_id": "isolated-controller", "provider": "fake",
                                      "policy": {}, "resource_cleanup_version": 1,
                                      "workflow_failure_boundary_version": 1})
        assert state["outcome"] == "blocked" and state["cleanup"] == cleanup
        assert "ValueError" in state["error"]
        failure = state["checks"]["terminal_projection_failure"]
        assert failure["original_error"] == state["error"]
        assert failure["projection_sha256"] == digest(projected[0])
        assert failure["scheduled_event_id"] == 10
        assert "terminal_projection_failure" not in projected[0]["checks"]

    controller = DeliveryWorkflow()

    async def failed_protocol(*_args):
        raise ValueError("Original error" + "x" * 2000)

    async def failed_stop(*_args, **_kwargs):
        raise RuntimeError("Feature settlement unknown" + "y" * 2000)

    controller._run_protocol, controller._stop = failed_protocol, failed_stop
    with pytest.raises(ApplicationError) as raised:
        await controller.run({"run_id": "isolated-controller", "policy": {},
                              "workflow_failure_boundary_version": 1})
    assert raised.value.type == "ControllerStopUnproven"
    assert controller.state["cleanup"] == "unknown"
    assert all(len(value["message"]) <= 600 for value in raised.value.details[0].values())


@pytest.mark.asyncio
async def test_dispatch_preserves_legacy_timeout_and_bounds_terminal_tracker(monkeypatch):
    calls = []

    class Client:
        def get_workflow_handle(self, *_args):
            return self

        async def describe(self):
            raise RPCError("Private absent workflow", RPCStatusCode.NOT_FOUND, b"")

        async def start_workflow(self, *_args, **options):
            calls.append(options)

    spec = {"run_id": "isolated-controller", "terminal_tracker_version": 1}
    item = {"run_id": spec["run_id"], "workflow_id": None,
            "request_digest": "fixture", "recovery_json": None}
    store = SimpleNamespace(pending_starts=lambda: [item], effective_spec=lambda _: spec,
                            owns_execution=lambda _: True, mark_start=lambda *_a, **_k: None)

    async def client():
        return Client()

    service = SimpleNamespace(store=store, config=SimpleNamespace(queue="private"),
                              healthy_client=client)
    monkeypatch.setattr("devflow_temporal.delivery_api.execution_retired", lambda _: False)
    await DeliveryService.dispatch_once(service)
    assert calls[-1]["execution_timeout"] == calls[-1]["run_timeout"] == timedelta(hours=72)
    spec["workflow_failure_boundary_version"] = 1
    await DeliveryService.dispatch_once(service)
    assert "execution_timeout" not in calls[-1] and "run_timeout" not in calls[-1]
    item["recovery_json"] = canonical_json({"kind": "terminal_tracker_recovery"})
    await DeliveryService.dispatch_once(service)
    assert calls[-1]["execution_timeout"] == timedelta(hours=72)
    assert calls[-1]["run_timeout"] == timedelta(minutes=14)
    spec["feature_delivery"] = {"owner": "isolated"}
    await DeliveryService.dispatch_once(service)
    assert "execution_timeout" not in calls[-1] and "run_timeout" not in calls[-1]


async def _closed_fixture(unpublished, monkeypatch, *, committed=False):
    store, broker, original, _ = unpublished
    spec = store.effective_spec("run-1")
    assert spec["workflow_failure_boundary_version"] == 1
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
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = True
    controller.state = copy.deepcopy(original)
    projection = None

    async def execute(name, request, **_kwargs):
        nonlocal projection
        assert name == "delivery_project"
        projection = copy.deepcopy(request)
        if committed:
            store.project("run-1", **{k: v for k, v in request.items() if k != "spec"})
        raise _projection_error()

    controller._activity = execute
    await controller._project(spec, "blocked", original["error"])
    state = controller.state
    inputs = await DELIVERY_DATA_CONVERTER.encode([spec, None])
    payloads = await DELIVERY_DATA_CONVERTER.encode([projection])
    history = SimpleNamespace(events=[
        HistoryEvent(event_id=1, workflow_execution_started_event_attributes=
                     WorkflowExecutionStartedEventAttributes(
                         workflow_type=WorkflowType(name="DevflowDeliveryWorkflow"),
                         input=Payloads(payloads=inputs))),
        HistoryEvent(event_id=10, activity_task_scheduled_event_attributes=
                     ActivityTaskScheduledEventAttributes(
                         activity_type=ActivityType(name="delivery_project"),
                         input=Payloads(payloads=payloads))),
        HistoryEvent(event_id=12, activity_task_failed_event_attributes=
                     ActivityTaskFailedEventAttributes(scheduled_event_id=10)),
    ])
    description = SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED,
                                  close_time=datetime.now(UTC), id="delivery-run-1",
                                  run_id="isolated-closed-execution",
                                  workflow_type="DevflowDeliveryWorkflow")

    async def memo(name, default=None):
        return spec["request_digest"] if name == "request_digest" else default

    description.memo_value = memo
    closed = {"workflow_id": description.id, "execution_run_id": description.run_id,
              "closed_at": description.close_time.isoformat(),
              "request_digest": spec["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)

    class Client:
        def get_workflow_handle(self, workflow_id, *, run_id=None):
            assert workflow_id == "delivery-run-1"
            assert run_id in {None, description.run_id}
            return self

        async def describe(self):
            return description

        async def result(self):
            return copy.deepcopy(state)

        async def fetch_history(self):
            return history

    return store, broker, state, projection, Client(), history, description


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_completed_projection_restores_public_status_and_finite_continuation(
    unpublished, monkeypatch, committed,
):
    store, broker, state, projection, client, _, _ = await _closed_fixture(
        unpublished, monkeypatch, committed=committed)
    # Current GitHub absence is the same explicit metadata seam used by the
    # existing stopped-resume fixture; never contact a real repository.
    monkeypatch.setattr(store, "_ensure_no_remote_pr", lambda *_args: None)
    app = create_app(store.config.path)
    service = app.state.delivery
    service.store = store

    async def healthy_client():
        return client

    monkeypatch.setattr(service, "healthy_client", healthy_client)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport,
                                base_url=store.config.dashboard_url) as browser:
        endpoint = "/api/runs/run-1"
        before = (await browser.get(endpoint)).json()["run"]
        assert before["phase"] == ("blocked" if committed else "implementing")
        events_before = store.events("run-1")
        await service.reconcile_completed_projections_once()
        final = (await browser.get(endpoint)).json()["run"]
        assert final["phase"] == "blocked" and final["cleanup"] == "confirmed"
        assert final["checks"] == state["checks"]
        candidate = broker.candidate()
        assert {k: final["candidate"][k] for k in candidate} == candidate
        assert final["error"] == state["error"]
        events_after = store.events("run-1")
        assert events_after[:len(events_before)] == events_before
        assert len(events_after) == len(events_before) + 1
        assert events_after[-1]["type"] == EVENT
        assert len([e for e in events_after if e["payload"].get("key")
                    == projection["key"]]) == int(committed)
        await service.reconcile_completed_projections_once()
        assert store.events("run-1") == events_after
        command = {"continuation_kind": KIND, "command_id": "after-projection-failure",
                   "expected_revision": state["revision"], "expected_iteration": 0,
                   "expected_candidate_id": broker.candidate()["id"],
                   "expected_candidate_head": broker.candidate()["head"],
                   "additional_iterations": 2}
        preflight = await browser.post(endpoint + "/repair-admission-preflight", json=command)
        assert preflight.status_code == 200, preflight.text
        session = await browser.get("/api/session")
        headers = {"Origin": store.config.dashboard_url,
                   "X-Devflow-CSRF": session.json()["csrf_token"]}
        admitted = await browser.post(endpoint + "/continue-repair", json=command, headers=headers)
        assert admitted.status_code == 200, admitted.text
        assert (await browser.post(endpoint + "/continue-repair", json=command,
                                   headers=headers)).json() == admitted.json()
        with store._connect() as db:
            recovery = json.loads(db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
        assert recovery["state"] == state
        assert recovery["session_id"] == "original-implementation"


@pytest.mark.asyncio
async def test_feature_settlement_after_projection_failure_is_history_bound(
    unpublished, monkeypatch,
):
    store, _, state, prior_projection, _, _, description = await _closed_fixture(
        unpublished, monkeypatch)
    # Only the feature ownership/effect/closure metadata is synthetic here.
    # Real controller/coordinator calls and the private Store projection run.
    spec = {**prior_projection["spec"], "feature_delivery": {"owner": "private-owner"}}
    controller = DeliveryWorkflow()
    controller.controller_failure_boundary = controller.feature_coordinating = True
    controller.state = copy.deepcopy(state)
    controller.state["checks"].pop("terminal_projection_failure")
    calls = []

    async def execute(name, request, **_kwargs):
        value = ({"owner": spec["feature_delivery"]["owner"], "state": "stopped"}
                 if name == "delivery_feature_stop" else {"state": "confirmed"})
        calls.append((name, copy.deepcopy(request), value))
        if name == "delivery_project":
            raise _projection_error()
        assert name in {"delivery_feature_settle_workers", "delivery_feature_settle_effects",
                        "delivery_feature_stop"}
        return value

    async def coordinate_pass(actual_controller, actual_spec, _active, _builds):
        assert actual_controller is controller and actual_spec == spec
        await controller._project(spec, "blocked", state["error"])
        return controller.state

    controller._activity = execute
    monkeypatch.setattr(workflow, "patched", lambda _name: False)
    monkeypatch.setattr(feature_workflow, "_coordinate_pass", coordinate_pass)
    completed = await feature_workflow.coordinate(controller, spec)
    assert [name for name, *_ in calls] == ["delivery_project", "delivery_feature_settle_workers",
                                         "delivery_feature_settle_effects", "delivery_feature_stop"]
    assert completed["checks"]["feature_readback"] == {"state": "confirmed"}
    history = SimpleNamespace(events=[HistoryEvent(
        event_id=1, workflow_execution_started_event_attributes=
        WorkflowExecutionStartedEventAttributes(input=Payloads(
            payloads=await DELIVERY_DATA_CONVERTER.encode([spec, None]))))])
    for event_id, (name, request, result) in zip((10, 13, 16, 19), calls, strict=True):
        history.events.append(HistoryEvent(
            event_id=event_id, activity_task_scheduled_event_attributes=
            ActivityTaskScheduledEventAttributes(activity_type=ActivityType(name=name),
                input=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([request])))))
        history.events.append(HistoryEvent(event_id=event_id + 2, **(
            {"activity_task_failed_event_attributes": ActivityTaskFailedEventAttributes(
                scheduled_event_id=event_id)} if name == "delivery_project" else
            {"activity_task_completed_event_attributes": ActivityTaskCompletedEventAttributes(
                scheduled_event_id=event_id,
                result=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([result])))})))
    for name in ("effective_spec", "submitted_spec", "spec"):
        monkeypatch.setattr(store, name, lambda _run: spec)
    monkeypatch.setattr(store, "owns_execution", lambda _spec: True)

    class Client:
        def get_workflow_handle(self, identity, *, run_id=None):
            assert identity == description.id and run_id in {None, description.run_id}
            return self

        async def describe(self):
            return description

        async def result(self):
            return copy.deepcopy(completed)

        async def fetch_history(self):
            return history

    def row():
        with store._connect() as db:
            return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())

    before, exact = row(), copy.deepcopy(history.events)
    for changed in ("readback_result", "readback_spec", "stop_completion", "stop_checkpoint",
                    "stop_owner", "arbitrary_check"):
        history.events = copy.deepcopy(exact)
        if changed == "readback_result":
            history.events[6].activity_task_completed_event_attributes.result.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([{"state": "pending"}])))
        elif changed == "readback_spec":
            history.events[5].activity_task_scheduled_event_attributes.input.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([{"spec": {**spec, "policy": {}}}])))
        elif changed == "stop_completion":
            history.events.pop()
        elif changed == "stop_checkpoint":
            forged = copy.deepcopy(calls[3][1])
            forged["checkpoint"]["state"]["candidate"] = {"id": "foreign"}
            history.events[7].activity_task_scheduled_event_attributes.input.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([forged])))
        elif changed == "stop_owner":
            history.events[8].activity_task_completed_event_attributes.result.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([
                    {"owner": "foreign", "state": "stopped"}])))
        else:
            completed["checks"]["unrelated"] = True
        with pytest.raises(ValueError, match="settlement"):
            await reconcile_projection(store, Client(), row())
        assert row() == before
        completed["checks"].pop("unrelated", None)
    history.events = exact
    events = store.events("run-1")
    assert (await reconcile_projection(store, Client(), row()))["state"] == "reconciled"
    assert json.loads(row()["checks_json"]) == completed["checks"]
    assert store.events("run-1")[:-1] == events
    assert (await reconcile_projection(store, Client(), row()))["state"] == "observed"
    assert len(store.events("run-1")) == len(events) + 1


@pytest.mark.asyncio
async def test_closed_projection_refuses_tampering_newer_row_and_unknown_cleanup(
    unpublished, monkeypatch,
):
    store, _, state, projection, client, history, description = await _closed_fixture(
        unpublished, monkeypatch)

    def row():
        with store._connect() as db:
            return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())

    original_row = row()
    original_state, original_history = copy.deepcopy(state), copy.deepcopy(history)
    for status in (WorkflowExecutionStatus.FAILED, WorkflowExecutionStatus.TIMED_OUT,
                   WorkflowExecutionStatus.RUNNING):
        description.status = status
        assert (await reconcile_projection(store, client, row()))["state"] == "not_completed"
        assert row() == original_row
    description.status = WorkflowExecutionStatus.COMPLETED
    for change in ("memo", "input", "result", "request", "failed", "execution", "newer"):
        state.clear()
        state.update(copy.deepcopy(original_state))
        history.events = copy.deepcopy(original_history.events)
        original_memo = description.memo_value
        description.id = "delivery-run-1"
        if change == "memo":
            async def forged_memo(_name, _default=None):
                return "foreign"
            description.memo_value = forged_memo
        elif change == "input":
            start = history.events[0].workflow_execution_started_event_attributes
            start.input.CopyFrom(Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([
                {**projection["spec"], "policy_digest": "foreign"}, None])))
        elif change == "result":
            state["candidate"] = {"id": "foreign"}
        elif change == "request":
            attributes = history.events[1].activity_task_scheduled_event_attributes
            attributes.input.CopyFrom(Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([
                {**projection, "cleanup": "confirmed_after_role_boundary"}])))
        elif change == "failed":
            history.events.pop()
        elif change == "execution":
            description.id = "delivery-foreign"
        else:
            store.project("run-1", phase="newer", execution_state="running",
                          event_type="newer", message="Newer immutable row",
                          protocol_revision=state["revision"] + 1)
        before = row()
        with pytest.raises(ValueError):
            await reconcile_projection(store, client, before)
        assert row() == before
        description.memo_value = original_memo
        if change == "newer":
            with pytest.raises(ValueError, match="row revision changed"):
                store.project("run-1", phase="blocked", execution_state="blocked",
                              event_type=EVENT, message="Stale CAS",
                              expected_revision=original_row["revision"])
    # Unknown cleanup may be projected truthfully, but cannot authorize repair.
    with store._connect() as db:
        db.execute("DELETE FROM delivery_events WHERE run_id='run-1' AND type='newer'")
        fields = ("phase", "execution_state", "protocol_revision", "revision", "updated_at")
        db.execute("UPDATE delivery_runs SET " + ",".join(f"{k}=?" for k in fields)
                   + " WHERE run_id='run-1'", tuple(original_row[k] for k in fields))
    state.clear()
    state.update(copy.deepcopy(original_state))
    state["cleanup"] = "unknown"
    projection = copy.deepcopy(projection)
    projection["cleanup"] = "unknown"
    state["checks"]["terminal_projection_failure"]["projection_sha256"] = digest(projection)
    history.events = copy.deepcopy(original_history.events)
    history.events[1].activity_task_scheduled_event_attributes.input.CopyFrom(
        Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([projection])))
    description.id = "delivery-run-1"
    assert (await reconcile_projection(store, client, row()))["state"] == "reconciled"
    assert store.detail("run-1")["cleanup"] == "unknown"
    with pytest.raises(ValueError, match="closed finalized"):
        store.repair_admission_preflight("run-1", {
            "continuation_kind": KIND, "command_id": "unknown-cleanup",
            "expected_revision": state["revision"], "expected_iteration": 0,
            "expected_candidate_id": state["candidate"]["id"],
            "expected_candidate_head": state["candidate"]["head"], "additional_iterations": 2,
        })


@pytest.mark.asyncio
async def test_actual_temporal_guard_completion_legacy_backstop_and_replay():
    @activity.defn(name="delivery_project")
    async def project(request):
        if request["spec"]["run_id"] == "isolated-projection":
            raise ApplicationError("Isolated terminal projection failed", non_retryable=True)
        return {"phase": request["phase"]}

    @activity.defn(name="delivery_prepare")
    async def prepare(_request):
        raise ApplicationError("Isolated preparation failed", non_retryable=True)

    async with local_temporal() as environment:
        async with Worker(environment.client, task_queue="isolated-controller-boundary",
                          workflows=[DeliveryWorkflow], activities=[project, prepare],
                          workflow_runner=UnsandboxedWorkflowRunner(),
                          workflow_failure_exception_types=[ValueError, KeyError, TypeError]):
            original = {"run_id": "isolated-controller", "provider": "fake", "policy": {}}
            invalid = {"kind": "stopped_delivery_resume", "execution_spec": {"run_id": "foreign"}}
            for name, spec, recovery in (
                ("marked", {**original, "workflow_failure_boundary_version": 1}, invalid),
                ("marked-projection", {**original, "run_id": "isolated-projection",
                                       "workflow_failure_boundary_version": 1}, invalid),
                ("legacy-backstop", original, invalid), ("legacy-commands", original, None),
            ):
                handle = await environment.client.start_workflow(
                    DeliveryWorkflow.run, args=[spec, recovery], id=name,
                    task_queue="isolated-controller-boundary")
                if name == "legacy-backstop":
                    with pytest.raises(WorkflowFailureError):
                        await asyncio.wait_for(handle.result(), 15)
                    assert (await handle.describe()).status == WorkflowExecutionStatus.FAILED
                    continue
                result = await asyncio.wait_for(handle.result(), 15)
                assert result["outcome"] == "blocked"
                assert (await handle.describe()).status == WorkflowExecutionStatus.COMPLETED
                if name.startswith("marked"):
                    assert result["cleanup"] == "unknown"
                    assert result["checks"]["controller_failure"]["error_type"] == "ValueError"
                    if name == "marked-projection":
                        history = await handle.fetch_history()
                        failure = result["checks"]["terminal_projection_failure"]
                        scheduled = next(event.activity_task_scheduled_event_attributes
                                         for event in history.events
                                         if event.event_id == failure["scheduled_event_id"])
                        request = (await DELIVERY_DATA_CONVERTER.decode(
                            scheduled.input.payloads))[0]
                        assert digest(request) == failure["projection_sha256"]
                        assert request["outcome"] == "blocked"
                else:
                    assert "controller_failure" not in result["checks"]
                await Replayer(
                    workflows=[DeliveryWorkflow], workflow_runner=UnsandboxedWorkflowRunner(),
                    workflow_failure_exception_types=[ValueError, KeyError, TypeError],
                ).replay_workflow(await handle.fetch_history())
