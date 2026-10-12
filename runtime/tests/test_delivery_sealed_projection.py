"""Independent cross-PR review, synthetic completed history and private store only."""

import pytest
from temporalio.api.common.v1 import Payloads
from temporalio.api.history.v1 import HistoryEvent, WorkflowExecutionCompletedEventAttributes
from temporalio.client import WorkflowHistory
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_controller_failure_boundary import _closed_fixture
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal import delivery_closures as closures
from devflow_temporal.delivery_closed_projection import reconcile_completed_projections
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER


@pytest.mark.asyncio
@pytest.mark.parametrize("temporal_expired", [False, True])
async def test_sealed_terminal_result_can_reconcile_projection(
    unpublished, monkeypatch, temporal_expired
):
    store, broker, state, projection, client, sparse_history, description = await _closed_fixture(
        unpublished, monkeypatch
    )
    spec = store.effective_spec("run-1")
    # Expand the existing unit fixture to a contiguous synthetic closed history.
    by_id = {event.event_id: event for event in sparse_history.events}
    events = [by_id.get(i, HistoryEvent(event_id=i)) for i in range(1, 13)]
    start = events[0].workflow_execution_started_event_attributes
    start.workflow_id = description.id
    start.original_execution_run_id = description.run_id
    start.memo.fields["request_digest"].CopyFrom(
        (await DELIVERY_DATA_CONVERTER.encode([spec["request_digest"]]))[0]
    )
    terminal = HistoryEvent(
        event_id=13,
        workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
            result=Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([state]))
        ),
    )
    terminal.event_time.FromDatetime(description.close_time)
    events.append(terminal)
    history = WorkflowHistory(description.id, events)
    facts = await closures.closure_facts(history)
    closures.seal(store, history, facts, run_id="run-1", source="temporal")
    saved = await closures.sealed_closure(store, description.id)
    assert saved["result"] == state and saved["status"] == "COMPLETED"

    async def retained_history():
        return history

    monkeypatch.setattr(client, "fetch_history", retained_history)
    if temporal_expired:

        async def missing():
            raise RPCError("retention elapsed in synthetic fixture", RPCStatusCode.NOT_FOUND, b"")

        monkeypatch.setattr(client, "describe", missing)
    result = await reconcile_completed_projections(store, client)
    assert result[0]["state"] == "reconciled"
    assert store.detail("run-1")["phase"] == "blocked"
