"""Closed-projection maintenance: persisted due candidates, exponential backoff and
no SQLite work on the API event loop (rr-sweep-2)."""
from __future__ import annotations

import threading
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_controller_failure_boundary import _closed_fixture
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal.delivery_closed_projection import (
    BACKOFF_BASE,
    BACKOFF_CAP,
    OPEN_EXECUTION_BACKOFF_CAP,
    reconcile_completed_projections,
)
from devflow_temporal.delivery_store import DeliveryStore


class _Client:
    def __init__(self):
        self.describes = []
        self.status = WorkflowExecutionStatus.RUNNING
        self.close_time = None
        self.error = None

    def get_workflow_handle(self, workflow_id, *, run_id=None):
        self.workflow_id = workflow_id
        return self

    async def describe(self):
        self.describes.append(self.workflow_id)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(status=self.status, close_time=self.close_time,
                               id=self.workflow_id, run_id="isolated-execution",
                               workflow_type="DevflowDeliveryWorkflow")


def _maintenance_rows(store):
    with store._connect() as db:
        return {(row["run_id"], row["workflow_id"]): dict(row) for row in db.execute(
            "SELECT * FROM delivery_closed_projections")}


def _due_at(store, key):
    return datetime.fromisoformat(_maintenance_rows(store)[key]["next_check_at"])


@pytest.mark.asyncio
async def test_maintenance_persists_backoff_and_settles_closed_executions(service):
    store, request = service
    store.submit(request)
    client = _Client()
    key = ("run-1", "delivery-run-1")
    start = datetime(2026, 10, 11, 12, tzinfo=UTC)
    observed = await reconcile_completed_projections(store, client, now=start)
    assert [(item["run_id"], item["state"]) for item in observed] == [("run-1", "not_completed")]
    assert client.describes == ["delivery-run-1"]
    rows = _maintenance_rows(store)
    assert set(rows) == {key}
    assert rows[key]["state"] == "not_completed" and rows[key]["attempts"] == 1
    assert _due_at(store, key) == start + BACKOFF_BASE
    # Not due yet, including for a restarted service with a fresh store instance.
    for instance in (store, DeliveryStore(store.config)):
        assert await reconcile_completed_projections(
            instance, client, now=start + BACKOFF_BASE - timedelta(seconds=1)) == []
    assert client.describes == ["delivery-run-1"]
    # Exponential backoff for an open execution is capped at a few minutes.
    delays = []
    for _attempt in range(7):
        moment = _due_at(store, key)
        assert len(await reconcile_completed_projections(store, client, now=moment)) == 1
        delays.append(_due_at(store, key) - moment)
    assert delays == [min(OPEN_EXECUTION_BACKOFF_CAP, BACKOFF_BASE * 2 ** n) for n in range(1, 8)]
    assert _maintenance_rows(store)[key]["attempts"] == 8
    # A newer private projection makes the row due at once.
    store.project("run-1", phase="preparing", execution_state="running",
                  event_type="preparing", message="Private progress")
    reads = len(client.describes)
    assert len(await reconcile_completed_projections(
        store, client, now=moment + timedelta(seconds=1))) == 1
    assert len(client.describes) == reads + 1
    # An absent execution is not an error; a changed verdict restarts the backoff.
    client.error = RPCError("Private absent workflow", RPCStatusCode.NOT_FOUND, b"")
    moment = _due_at(store, key)
    assert [item["state"] for item in await reconcile_completed_projections(
        store, client, now=moment)] == ["not_started"]
    assert _due_at(store, key) - moment == BACKOFF_BASE
    assert _maintenance_rows(store)[key]["attempts"] == 1
    # Consecutive unproven verdicts back off from the base again, up to an hour.
    client.error = RuntimeError("Private transport failure")
    delays = []
    for _attempt in range(8):
        moment = _due_at(store, key)
        observed = await reconcile_completed_projections(store, client, now=moment)
        assert [(item["state"], item["error_type"]) for item in observed] == [
            ("unproven", "RuntimeError")]
        delays.append(_due_at(store, key) - moment)
    assert delays == [min(BACKOFF_CAP, BACKOFF_BASE * 2 ** n) for n in range(8)]
    assert delays[-1] == BACKOFF_CAP
    assert _maintenance_rows(store)[key]["error"] == "RuntimeError"
    # A closed, not completed execution settles: never described again.
    client.error = None
    client.status, client.close_time = WorkflowExecutionStatus.CANCELED, datetime.now(UTC)
    moment = _due_at(store, key)
    observed = await reconcile_completed_projections(store, client, now=moment)
    assert [(item["state"], item["closed"]) for item in observed] == [("not_completed", True)]
    assert _maintenance_rows(store)[key]["state"] == "closed_observed"
    assert _maintenance_rows(store)[key]["next_check_at"] is None
    reads = len(client.describes)
    assert await reconcile_completed_projections(
        store, client, now=moment + timedelta(days=1)) == []
    assert len(client.describes) == reads
    # A new execution identity of the same run is a new candidate.
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id='delivery-run-1-recovery' "
                   "WHERE run_id='run-1'")
    client.status, client.close_time = WorkflowExecutionStatus.RUNNING, None
    observed = await reconcile_completed_projections(
        store, client, now=moment + timedelta(days=1))
    assert [item["state"] for item in observed] == ["not_completed"]
    assert client.describes[-1] == "delivery-run-1-recovery"
    assert set(_maintenance_rows(store)) == {key, ("run-1", "delivery-run-1-recovery")}


@pytest.mark.asyncio
async def test_maintenance_pass_keeps_sqlite_work_off_the_event_loop(unpublished, monkeypatch):
    store, _, state, _, client, _, _ = await _closed_fixture(unpublished, monkeypatch)
    original_connect = store._connect
    threads = []

    @contextmanager
    def connect():
        threads.append(threading.current_thread())
        with original_connect() as db:
            yield db

    handles = []
    original_handle = client.get_workflow_handle

    def get_workflow_handle(*args, **kwargs):
        handles.append(args)
        return original_handle(*args, **kwargs)

    client.get_workflow_handle = get_workflow_handle
    now = datetime(2026, 10, 11, 12, tzinfo=UTC)
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(store, "_connect", connect)
        observed = await reconcile_completed_projections(store, client, now=now)
    assert [(item["run_id"], item["state"]) for item in observed] == [("run-1", "reconciled")]
    assert threads and all(thread is not threading.main_thread() for thread in threads)
    assert store.detail("run-1")["phase"] == "blocked"
    assert store.detail("run-1")["checks"] == state["checks"]
    rows = _maintenance_rows(store)
    assert rows[("run-1", "delivery-run-1")]["state"] == "reconciled"
    assert rows[("run-1", "delivery-run-1")]["key"] == observed[0]["key"]
    reads = len(handles)
    assert await reconcile_completed_projections(store, client, now=now + timedelta(days=1)) == []
    assert len(handles) == reads
