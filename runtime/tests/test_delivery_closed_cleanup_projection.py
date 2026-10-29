"""Completed results compose only authenticated, later cleanup observations."""
from __future__ import annotations

import asyncio
import copy
import json

import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_controller_boundary_gaps import (
    _closed_client,
    _history,
    _row,
)
from test_delivery_controller_boundary_gaps import (
    forbid_external_github_and_provider_processes as forbid_external_github_and_provider_processes,
)
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_closed_projection import reconcile_projection
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


async def _closed(unpublished, *, committed=False):
    store, _, original, _ = unpublished
    spec = store.effective_spec("run-1")
    # Restore only this disposable fixture's preterminal row. All writes under
    # test below use the ordinary projection transaction.
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET outcome=NULL,protocol_revision=4,error=NULL "
                   "WHERE run_id='run-1'")
    prior = {
        "spec": spec, "phase": "feature_integration", "execution_state": "running",
        "event_type": "chunk_integrating", "message": "Private integration",
        "candidate": original["candidate"], "pull_request": None, "checks": {},
        "tracker": {}, "usage": {}, "decision": None, "intake": None, "iteration": 0,
        "protocol_revision": 5, "outcome": None, "cleanup": "none", "error": None,
        "key": "chunk_integrating:0:5",
    }
    store.project("run-1", **{k: v for k, v in prior.items() if k != "spec"})
    terminal = {
        **prior, "event_type": "tracker_deadline", "key": "tracker_deadline:0:7",
        "protocol_revision": 7, "phase": "blocked", "execution_state": "blocked",
        "outcome": "blocked", "error": "terminal tracker synchronization conflicted",
        "checks": {"terminal_tracker_checkpoint": {
            "closed": True, "waiting": False, "event": "blocked",
            "outcome": "blocked", "state": "conflicted"}},
    }
    failure = {"version": 1, "scheduled_event_id": 13,
               "projection_sha256": digest(terminal), "event_type": "tracker_deadline",
               "protocol_revision": 7}
    state = {"run_id": "run-1", "revision": 7, **{
        k: copy.deepcopy(terminal[k]) for k in (
            "phase", "execution_state", "iteration", "outcome", "cleanup", "error",
            "candidate", "pull_request", "checks", "tracker", "usage", "decision", "intake")}}
    state["cleanup"] = "unknown"
    state["checks"]["terminal_projection_failure"] = failure
    if committed:
        store.project("run-1", **{k: v for k, v in terminal.items() if k != "spec"})
    history = await _history(spec, [("delivery_project", prior, {}),
                                   ("delivery_project", terminal, None)])
    client, description = _closed_client(spec, state, history)
    return store, spec, state, client, description


def _maintenance(store, spec, description, *, uncertain=True, status="COMPLETED"):
    before = _row(store)
    receipt = RunResources(spec).finalize("blocked", uncertain=uncertain)
    closed = {"workflow_id": description.id, "execution_run_id": description.run_id,
              "closed_at": description.close_time.isoformat(), "status": status}
    store.project(
        "run-1", phase=before["phase"], execution_state=before["execution_state"],
        outcome=before["outcome"], error=before["error"],
        checks={**json.loads(before["checks_json"] or "{}"), "resource_cleanup": receipt},
        cleanup=receipt["resource_cleanup"], event_type="resource_cleanup_reconciled",
        message="Owner maintenance observed native cleanup after workflow closure",
        key=digest({"closed_workflow": closed, "native_observations": receipt["processes"],
                    "resource_cleanup": receipt}),
    )
    return receipt


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["mixed-writer", "missing-revision", "unordered-time", "key"])
async def test_cleanup_chain_refuses_unaccounted_or_foreign_writes(unpublished, drift):
    store, spec, _, client, description = await _closed(unpublished)
    _maintenance(store, spec, description)
    first = store.events("run-1")[-1]
    _maintenance(store, spec, description, uncertain=False)
    second = store.events("run-1")[-1]
    with store._connect() as db:
        if drift == "mixed-writer":
            db.execute("UPDATE delivery_events SET type='dispatch_blocked' WHERE sequence=?",
                       (first["sequence"],))
        elif drift == "missing-revision":
            db.execute("DELETE FROM delivery_events WHERE sequence=?", (first["sequence"],))
        elif drift == "unordered-time":
            db.execute("UPDATE delivery_events SET timestamp=? WHERE sequence=?",
                       (second["timestamp"], first["sequence"]))
        else:
            db.execute("UPDATE delivery_events SET payload_json='{}' WHERE sequence=?",
                       (first["sequence"],))
    before, events = _row(store), store.events("run-1")
    with pytest.raises(ValueError, match="would overwrite a newer row"):
        await reconcile_projection(store, client, before)
    assert _row(store) == before and store.events("run-1") == events


@pytest.mark.asyncio
async def test_real_completed_history_reconciles_after_private_cleanup(service):
    """Real scheduling, failed terminal activity, completion and workflow replay."""
    store, request = service
    store.submit(request)
    spec = store.effective_spec("run-1")

    @activity.defn(name="delivery_project")
    async def project(projection):
        if projection["event_type"] != "preparing":
            raise ApplicationError("Private projection refusal", non_retryable=True)
        return store.project("run-1", **{k: v for k, v in projection.items() if k != "spec"})

    @activity.defn(name="delivery_prepare")
    async def prepare(_request):
        raise ApplicationError("Private preparation stop", non_retryable=True)

    @activity.defn(name="delivery_finalize_resources")
    async def finalize(_request):
        return {"state": "unknown", "process_cleanup": "unknown", "resource_cleanup": "unknown"}

    @activity.defn(name="delivery_terminal_tracker")
    async def tracker(_request):
        return {"state": "consistent", "pending": False}

    async with local_temporal() as environment:
        client = await Client.connect(environment.client.service_client.config.target_host,
                                      data_converter=DELIVERY_DATA_CONVERTER)
        async with Worker(client, task_queue="private-cleanup-projection",
                          workflows=[DeliveryWorkflow],
                          activities=[project, prepare, finalize, tracker],
                          workflow_failure_exception_types=[ValueError, KeyError, TypeError]):
            handle = await client.start_workflow(
                DeliveryWorkflow.run, args=[spec, None], id="delivery-run-1",
                task_queue="private-cleanup-projection",
                memo={"request_digest": spec["request_digest"]})
            state = await asyncio.wait_for(handle.result(), 60)
        description = await handle.describe()
        assert description.status == WorkflowExecutionStatus.COMPLETED
        assert state["checks"]["terminal_projection_failure"]["event_type"] == "blocked"
        history = await handle.fetch_history()
        # Successive native maintenance passes run before closed projection in
        # the normal pump: retain both commits and compose the latest receipt.
        _maintenance(store, spec, description)
        receipt = _maintenance(store, spec, description, uncertain=False)
        assert receipt["state"] == "confirmed"
        assert (await reconcile_projection(store, client, _row(store)))["state"] == "reconciled"
        assert store.detail("run-1")["outcome"] == "blocked"
        assert store.detail("run-1")["checks"]["resource_cleanup"] == receipt
        assert (await reconcile_projection(store, client, _row(store)))["state"] == "observed"
        await Replayer(workflows=[DeliveryWorkflow], data_converter=DELIVERY_DATA_CONVERTER,
                       workflow_runner=UnsandboxedWorkflowRunner(),
                       workflow_failure_exception_types=[ValueError, KeyError, TypeError]
                       ).replay_workflow(history)
