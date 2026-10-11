"""A failed feature coordinator continues from its sealed history after retention."""
from __future__ import annotations

import hashlib

import pytest
from temporalio.client import WorkflowHistory
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_feature_failed_closure import failed_parent as failed_parent
from test_delivery_feature_recovered_closure import recovered_worker as recovered_worker
from test_delivery_store import service as service

from devflow_temporal import delivery_closures as closures
from devflow_temporal import delivery_feature_closure as closure
from devflow_temporal.delivery_feature_execution import continue_feature


async def test_failed_coordinator_continues_from_its_sealed_history(failed_parent, monkeypatch):
    store, spec, _child, shared, token, command, state = failed_parent
    workflow_id = "delivery-" + spec["run_id"]
    live = await closure.Client.connect()
    described = await live.get_workflow_handle(workflow_id).describe()
    events = state["events"]
    # Make the fixture's synthetic history consistent with its description, as Temporal's is.
    events[-1].event_time.FromDatetime(described.close_time)
    events[0].workflow_execution_started_event_attributes.original_execution_run_id = (
        "original-execution")
    with pytest.raises(closure.FailedFeatureCoordinator):
        store._completed_temporal_result(spec["run_id"])
    record = closures.sealed_record(store, workflow_id)
    assert record is not None and record["status"] == "FAILED"

    class Expired:
        def __init__(self, workflow):
            self.id = workflow

        async def describe(self):
            raise RPCError("workflow not found", RPCStatusCode.NOT_FOUND, b"")

        async def fetch_history(self):
            raise AssertionError("expired history must not be fetched")

    class ExpiredClient:
        def get_workflow_handle(self, workflow, *, run_id=None):
            return Expired(workflow)

    async def connect(*_args, **_kwargs):
        return ExpiredClient()

    monkeypatch.setattr(closure.Client, "connect", connect)
    continue_feature(store, spec["run_id"], command)
    proof = shared.checkpoints(token["issue_id"])["stopped:1"]["failed_coordinator_closure"]
    retained = hashlib.sha256(WorkflowHistory(workflow_id, events).to_json().encode()).hexdigest()
    assert proof["workflow_status"] == "FAILED" and proof["history_sha256"] == retained
