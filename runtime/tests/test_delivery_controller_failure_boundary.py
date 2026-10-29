"""Controller failure prevention and authenticated public projection recovery.

Private Store/Git fixtures and synthetic authoritative-shaped histories below
exercise admission without providers. The final case uses a new local Temporal
server to verify actual exception classification, completion and replay.
"""
from __future__ import annotations

import asyncio
import copy
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.api.common.v1 import ActivityType, Payloads, WorkflowType
from temporalio.api.history.v1 import (
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
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
    client.namespace = store.config.raw.get("temporal_namespace", "default")
    client.service_client = SimpleNamespace(config=SimpleNamespace(
        target_host=store.config.temporal_address))
    for status in (WorkflowExecutionStatus.FAILED, WorkflowExecutionStatus.TIMED_OUT,
                   WorkflowExecutionStatus.RUNNING):
        description.status = status
        if status == WorkflowExecutionStatus.RUNNING:
            assert (await reconcile_projection(store, client, row()))["state"] == "not_completed"
        else:
            # A completed-history fixture cannot authorize an interrupted reset.
            with pytest.raises(ValueError):
                await reconcile_projection(store, client, row())
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
