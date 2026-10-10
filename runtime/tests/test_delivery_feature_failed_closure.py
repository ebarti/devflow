"""Failed coordinator handoff must retain authenticated history and live custody."""
from __future__ import annotations

import asyncio
import json
import shutil
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.api.common.v1 import ActivityType, Memo, Payloads, WorkflowType
from temporalio.api.failure.v1 import ActivityFailureInfo, Failure
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionFailedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import WorkflowExecutionStatus, WorkflowFailureError, WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_feature_recovered_closure import recovered_worker as recovered_worker
from test_delivery_feature_workflow_temporal import FeatureActivities
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_closure as closure
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import continue_feature
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
async def failed_parent(recovered_worker, monkeypatch):
    store, spec, child, shared, token, command, _ = recovered_worker
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET protocol_revision=1 WHERE run_id=?',
                   (spec['run_id'],))
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (spec['run_id'],)).fetchone())
    projected = {key: row[key] for key in ('phase', 'execution_state', 'outcome', 'cleanup',
                                         'error', 'iteration', 'protocol_revision')}
    projected.update(spec=spec, event_type='blocked')
    projected.update({key: json.loads(row[column] or 'null') for key, column in (
        ('candidate', 'candidate_json'), ('pull_request', 'pr_json'), ('checks', 'checks_json'),
        ('tracker', 'tracker_json'), ('usage', 'usage_json'), ('decision', 'decision_json'),
        ('intake', 'intake_json'))})

    async def encoded(value):
        return Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode(value))

    start = WorkflowExecutionStartedEventAttributes(
        workflow_type=WorkflowType(name='DevflowDeliveryWorkflow'),
        input=await encoded([store.submitted_spec(spec['run_id']), None]),
        memo=Memo(fields={'request_digest':
                          (await DELIVERY_DATA_CONVERTER.encode([spec['request_digest']]))[0]}))
    scheduled = ActivityTaskScheduledEventAttributes(
        activity_type=ActivityType(name='delivery_project'), input=await encoded([projected]))
    settlement = ActivityTaskScheduledEventAttributes(
        activity_type=ActivityType(name='delivery_feature_settle_workers'),
        input=await encoded([{'spec': spec}]))
    failure = Failure(message='Activity failed', activity_failure_info=ActivityFailureInfo(
        scheduled_event_id=4, activity_type=ActivityType(name='delivery_feature_settle_workers')))
    events = [
        HistoryEvent(event_id=1, workflow_execution_started_event_attributes=start),
        HistoryEvent(event_id=2, activity_task_scheduled_event_attributes=scheduled),
        HistoryEvent(event_id=3, activity_task_completed_event_attributes=
                     ActivityTaskCompletedEventAttributes(scheduled_event_id=2)),
        HistoryEvent(event_id=4, activity_task_scheduled_event_attributes=settlement),
        HistoryEvent(event_id=5, activity_task_failed_event_attributes=
                     ActivityTaskFailedEventAttributes(scheduled_event_id=4)),
        HistoryEvent(event_id=6, workflow_execution_failed_event_attributes=
                     WorkflowExecutionFailedEventAttributes(failure=failure)),
    ]
    state = {'events': events, 'status': WorkflowExecutionStatus.FAILED}
    closed_at = datetime.now(UTC)

    class Handle:
        async def describe(self):
            async def memo_value(key, default=None):
                return spec['request_digest'] if key == 'request_digest' else default
            return SimpleNamespace(id='delivery-' + spec['run_id'], run_id='original-execution',
                status=state['status'], close_time=closed_at, memo_value=memo_value)

        async def fetch_history(self):
            return WorkflowHistory('delivery-' + spec['run_id'], state['events'])

    class Client:
        def get_workflow_handle(self, workflow_id, *, run_id=None):
            assert workflow_id == 'delivery-' + spec['run_id']
            assert run_id in {None, 'original-execution'}
            return Handle()

    async def connect(*_args, **_kwargs):
        return Client()

    monkeypatch.setattr(closure.Client, 'connect', connect)
    child_closed = store._completed_temporal_result
    monkeypatch.setattr(store, '_completed_temporal_result', lambda run_id, **kwargs:
        DeliveryStore._completed_temporal_result(store, run_id, **kwargs)
        if run_id == spec['run_id'] else child_closed(run_id, **kwargs))
    return store, spec, child, shared, token, command, state


async def test_failed_parent_handoff_authenticates_history_and_preserves_budget(failed_parent):
    store, spec, child, shared, token, command, _ = failed_parent
    budget = shared.budget(token['issue_id'])
    with pytest.raises(closure.FailedFeatureCoordinator):
        store._completed_temporal_result(spec['run_id'])
    result = continue_feature(store, spec['run_id'], command)
    successor = store.effective_spec(result['run_id'])
    assert successor['feature_delivery']['owner']['generation'] == token['generation'] + 1
    assert shared.budget(token['issue_id']) == budget
    proof = shared.checkpoints(token['issue_id'])['stopped:1']['failed_coordinator_closure']
    assert proof['workflow_status'] == 'FAILED' and proof['terminal_projection_event_id'] == 3
    assert proof['execution_run_id'] == 'original-execution'
    assert store.submitted_spec(child['run_id']) == child
    assert continue_feature(store, spec['run_id'], command) == result


@pytest.mark.parametrize('changed', ['start', 'memo', 'owner', 'revision', 'ack', 'failure',
                                   'later-projection', 'live'])
async def test_failed_handoff_rejects_unauthenticated_terminal_history(failed_parent, changed):
    store, spec, _, shared, token, command, state = failed_parent
    events = state['events']
    if changed == 'start':
        initial = deepcopy(store.submitted_spec(spec['run_id']))
        initial['goal'] = 'different request'
        events[0].workflow_execution_started_event_attributes.input.CopyFrom(Payloads(
            payloads=await DELIVERY_DATA_CONVERTER.encode([initial, None])))
    elif changed == 'memo':
        events[0].workflow_execution_started_event_attributes.memo.Clear()
    elif changed == 'owner':
        payload = (await DELIVERY_DATA_CONVERTER.decode(
            events[1].activity_task_scheduled_event_attributes.input.payloads))[0]
        payload['spec']['feature_delivery']['owner']['generation'] += 1
        events[1].activity_task_scheduled_event_attributes.input.CopyFrom(Payloads(
            payloads=await DELIVERY_DATA_CONVERTER.encode([payload])))
    elif changed == 'revision':
        with store._connect() as db:
            db.execute('UPDATE delivery_runs SET protocol_revision=protocol_revision+1 '
                       'WHERE run_id=?', (spec['run_id'],))
    elif changed == 'ack':
        events.pop(2)
    elif changed == 'failure':
        events[-1].workflow_execution_failed_event_attributes.failure.activity_failure_info\
            .scheduled_event_id = 2
    elif changed == 'later-projection':
        later = deepcopy(events[1])
        later.event_id = 7
        events.insert(-1, later)
    else:
        state['status'] = WorkflowExecutionStatus.RUNNING
    with pytest.raises(ValueError):
        continue_feature(store, spec['run_id'], command)
    current = shared.current(token['issue_id'])
    assert shared.token(current) == token and current['state'] == 'draining'


@pytest.mark.parametrize('owned_failure', [True, False])
async def test_settlement_failure_keeps_a_closed_checkpoint_only_with_owned_custody(
    tmp_path, owned_failure,
):
    fixtures = FeatureActivities()
    token = {'issue_id': 'I_feature', 'run_id': 'coordinator', 'generation': 1,
             'store_path': '/fixture/store'}
    spec = {'run_id': 'failed-settlement', 'provider': 'fake',
            'authorized_endpoint': 'published_unmerged',
            'feature_delivery': {'version': 1, 'owner': token}, 'policy': {'max_repairs': 10}}

    @activity.defn(name='delivery_feature_finish_worker')
    async def worker_failure(_):
        raise ApplicationError('Worker custody unresolved', non_retryable=True)

    @activity.defn(name='delivery_feature_settle_workers')
    async def settle_failure(_):
        owner = token if owned_failure else {**token, 'generation': 2}
        raise ApplicationError('Custody remains held', {'owner': owner},
                               type='FeatureWorkerSettlementPending', non_retryable=True)

    replaced = {'delivery_feature_finish_worker', 'delivery_feature_settle_workers'}
    handlers = [f for f in fixtures.handlers()
                if activity._Definition.must_from_callable(f).name not in replaced]
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
        dev_server_database_filename=str(tmp_path / 'temporal.sqlite3'),
    ) as environment:
        async with Worker(environment.client, task_queue=spec['run_id'],
                          workflows=[DeliveryWorkflow],
                          activities=[*handlers, worker_failure, settle_failure]):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec['run_id'], task_queue=spec['run_id'])
            if owned_failure:
                result = await asyncio.wait_for(handle.result(), 30)
                assert result['outcome'] == 'blocked'
            else:
                with pytest.raises(WorkflowFailureError):
                    await asyncio.wait_for(handle.result(), 30)
            history = await handle.fetch_history()
    assert not any(name == 'delivery_feature_stop' for name, _ in fixtures.calls)
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


async def test_pending_settlement_receipt_requires_current_draining_owner(
    recovered_worker, monkeypatch,
):
    store, spec, child, shared, token, _, _ = recovered_worker
    monkeypatch.setattr(activities, '_feature_context', lambda _: (store, None, shared, token))

    async def describe():
        return SimpleNamespace(status=WorkflowExecutionStatus.COMPLETED)

    async def connect(*_args, **_kwargs):
        return SimpleNamespace(get_workflow_handle=lambda _: SimpleNamespace(describe=describe))

    def unknown(*_):
        raise OwnershipConflict('worker custody is unconfirmed')

    monkeypatch.setattr(closure.Client, 'connect', connect)
    monkeypatch.setattr(activities, 'finish_worker', unknown)
    with pytest.raises(ApplicationError) as caught:
        await activities.delivery_feature_settle_workers({'spec': spec, 'workers': {}})
    assert caught.value.type == 'FeatureWorkerSettlementPending'
    assert caught.value.details == ({'owner': token, 'worker': child['run_id']},)
    assert shared.current(token['issue_id'])['state'] == 'draining'
    monkeypatch.setattr(activities, '_feature_context', unknown)
    with pytest.raises(OwnershipConflict, match='custody is unconfirmed'):
        await activities.delivery_feature_settle_workers({'spec': spec, 'workers': {}})
