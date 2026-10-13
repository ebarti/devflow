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
from devflow_temporal.delivery_closed_projection import EVENT, reconcile_projection
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


def _maintenance(store, spec, description, *, uncertain=True):
    before = _row(store)
    receipt = RunResources(spec).finalize("blocked", uncertain=uncertain)
    closed = {"workflow_id": description.id, "execution_run_id": description.run_id,
              "closed_at": description.close_time.isoformat(), "status": "COMPLETED"}
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


def _observation(store):
    with store._connect() as db:
        store._event(db, "run-1", _row(store)["revision"], "automatic_retry_skipped",
                     "Private observation after maintenance", {"reason": "previous refusal"})


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("uncertain", [False, True])
async def test_completed_result_preserves_authenticated_cleanup_and_replay(
    unpublished, committed, uncertain,
):
    store, spec, state, client, description = await _closed(unpublished, committed=committed)
    original = _row(store)
    receipt = _maintenance(store, spec, description, uncertain=uncertain)
    before = _row(store)
    assert {k for k in before if before[k] != original[k]} == {
        "revision", "checks_json", "cleanup", "updated_at"}
    assert before["updated_at"] > description.close_time.isoformat()
    _observation(store)
    events = store.events("run-1")
    assert (await reconcile_projection(store, client, before))["state"] == "reconciled"
    final = _row(store)
    assert final["phase"] == final["execution_state"] == final["outcome"] == "blocked"
    assert final["protocol_revision"] == 7 and final["error"] == state["error"]
    assert final["cleanup"] == receipt["resource_cleanup"]
    assert json.loads(final["checks_json"]) == {**state["checks"], "resource_cleanup": receipt}
    assert store.events("run-1")[:-1] == events
    assert store.events("run-1")[-1]["type"] == EVENT
    after = store.events("run-1")
    assert (await reconcile_projection(store, client, final))["state"] == "observed"
    assert _row(store) == final and store.events("run-1") == after


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [False, True])
async def test_two_cleanup_passes_before_terminal_reconciliation_compose_latest_receipt(
    unpublished, committed,
):
    store, spec, state, client, description = await _closed(unpublished, committed=committed)
    unknown = _maintenance(store, spec, description)
    _observation(store)
    confirmed = _maintenance(store, spec, description, uncertain=False)
    _observation(store)
    before, events = _row(store), store.events("run-1")
    assert unknown["state"] == "unknown" and confirmed["state"] == "confirmed"
    assert (await reconcile_projection(store, client, before))["state"] == "reconciled"
    final = _row(store)
    assert final["cleanup"] == "confirmed" and final["phase"] == "blocked"
    assert json.loads(final["checks_json"]) == {**state["checks"], "resource_cleanup": confirmed}
    assert store.events("run-1")[:-1] == events
    after = store.events("run-1")
    assert (await reconcile_projection(store, client, final))["state"] == "observed"
    assert _row(store) == final and store.events("run-1") == after


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
async def test_committed_terminal_response_loss_allows_a_trailing_observation(unpublished):
    store, _, state, client, _ = await _closed(unpublished, committed=True)
    _observation(store)
    assert (await reconcile_projection(store, client, _row(store)))["state"] == "reconciled"
    assert json.loads(_row(store)["checks_json"]) == state["checks"]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["protocol", "outcome", "candidate", "decision", "error",
                                   "metadata", "checks", "cleanup", "key", "revision", "closure"])
async def test_cleanup_marker_cannot_hide_newer_delivery_state(unpublished, drift):
    store, spec, _, client, description = await _closed(unpublished)
    _maintenance(store, spec, description)
    columns = {
        "protocol": ("protocol_revision", 8), "outcome": ("outcome", "delivered"),
        "candidate": ("candidate_json", json.dumps({"id": "foreign", "head": "foreign"})),
        "decision": ("decision_json", json.dumps({"id": "new-human-question"})),
        "error": ("error", "newer refusal"), "metadata": ("accepted_plan_text", "new plan"),
        "checks": ("checks_json", json.dumps({**json.loads(_row(store)["checks_json"]),
                                               "newer_gate": {"state": "failed"}})),
        "cleanup": ("cleanup", "confirmed"), "revision": ("revision", 50),
    }
    with store._connect() as db:
        if drift in columns:
            column, value = columns[drift]
            db.execute(f"UPDATE delivery_runs SET {column}=? WHERE run_id='run-1'", (value,))
        elif drift == "key":
            db.execute("UPDATE delivery_events SET payload_json='{}' "
                       "WHERE run_id='run-1' AND type='resource_cleanup_reconciled'")
        else:
            description.run_id = "foreign-completed-execution"
    before, events = _row(store), store.events("run-1")
    with pytest.raises(ValueError):
        await reconcile_projection(store, client, before)
    assert _row(store) == before and store.events("run-1") == events


@pytest.mark.asyncio
async def test_composition_checks_concurrent_metadata_inside_projection_transaction(
    unpublished, monkeypatch,
):
    store, spec, _, client, description = await _closed(unpublished)
    _maintenance(store, spec, description)
    project = store.project
    events = store.events("run-1")

    def raced(*args, **kwargs):
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET accepted_plan_text='concurrent decision' "
                       "WHERE run_id='run-1'")
        return project(*args, **kwargs)

    monkeypatch.setattr(store, "project", raced)
    with pytest.raises(ValueError, match="projection row changed"):
        await reconcile_projection(store, client, _row(store))
    assert _row(store)["accepted_plan_text"] == "concurrent decision"
    assert _row(store)["phase"] == "feature_integration"
    assert store.events("run-1") == events


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
