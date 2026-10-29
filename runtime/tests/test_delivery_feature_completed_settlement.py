"""Continue actual failed settlement after completed worker recovery, without new repairs."""
from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from temporalio.api.common.v1 import ActivityType, Memo, Payloads, WorkflowType
from temporalio.api.history.v1 import (
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import WorkflowHistory
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_closed_cleanup_projection import _closed, _maintenance
from test_delivery_controller_boundary_gaps import _row
from test_delivery_feature_execution import feature_service
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_feature_closure as closure
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_closures import closure_facts, seal
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks


@pytest.fixture
def tmp_path():
    with TemporaryDirectory(prefix='devflow-completed-settlement-') as directory:
        yield Path(directory).resolve()


async def _history(store, spec, state, *, workers=None):
    async def encoded(values):
        return Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode(values))

    workflow_id = store.active_workflow_id(spec['run_id'])
    start = WorkflowExecutionStartedEventAttributes(
        workflow_id=workflow_id, original_execution_run_id='execution-' + spec['run_id'],
        workflow_type=WorkflowType(name='DevflowDeliveryWorkflow'),
        input=await encoded([store.submitted_spec(spec['run_id']), None]),
        memo=Memo(fields={'request_digest':
                         (await DELIVERY_DATA_CONVERTER.encode([spec['request_digest']]))[0]}))
    events = [HistoryEvent(event_id=1, workflow_execution_started_event_attributes=start)]
    if workers is not None:
        events.extend([
            HistoryEvent(event_id=2, activity_task_scheduled_event_attributes=
                ActivityTaskScheduledEventAttributes(
                    activity_type=ActivityType(name='delivery_feature_settle_workers'),
                    input=await encoded([{'spec': spec, 'workers': workers}]))),
            HistoryEvent(event_id=3, activity_task_failed_event_attributes=
                ActivityTaskFailedEventAttributes(scheduled_event_id=2)),
        ])
    terminal = HistoryEvent(event_id=len(events) + 1,
        workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
            result=await encoded([state])))
    terminal.event_time.FromDatetime(datetime.now(UTC) - timedelta(seconds=1))
    events.append(terminal)
    return WorkflowHistory(workflow_id, events)


@pytest.fixture
async def completed_parent(service, monkeypatch, request):
    drift = getattr(request, 'param', None)
    store, submitted, _ = feature_service(service, monkeypatch)
    store.submit(submitted)
    parent = store.effective_spec(submitted['run_id'])
    shared, token = registry(parent), parent['feature_delivery']['owner']
    for number in range(10):
        shared.repair(token, 'original-cycle-' + str(number), 'Original product repair')
    chunk = ordered_chunks(json.loads(parent['accepted_plan']))[0]
    child_kind = 'chunk' if drift == 'worker-cleanup' else 'build'
    child = worker_spec(parent, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind=child_kind, base_sha=parent['base_sha'], base_branch='main')
    register_worker(store, parent, child)
    shared.checkpoint(token, 'assignment:' + chunk['id'] + ':' + child_kind,
        {'run_id': child['run_id'], 'store_path': token['store_path'],
         'chunk_id': chunk['id'], 'kind': child_kind})
    state = {'run_id': parent['run_id'], 'revision': 3, 'iteration': 0, 'candidate_revision': 0,
        'phase': 'blocked', 'execution_state': 'blocked', 'outcome': 'blocked',
        'cleanup': 'unknown', 'error': 'Original settlement disk I/O failure',
        'checks': {'controller_failure': {'stage': 'feature_integration'}},
        'candidate': None, 'pull_request': None, 'tracker': None, 'usage': {},
        'decision': None, 'intake': None}
    workflow_id = store.active_workflow_id(child['run_id'])
    history = await _history(store, parent, state, workers={child['run_id']: workflow_id})
    facts = await closure_facts(history)
    seal(store, history, facts, run_id=parent['run_id'], source='temporal')
    store.project(parent['run_id'], **{key: state[key] for key in (
        'phase', 'execution_state', 'outcome', 'cleanup', 'error', 'iteration', 'checks',
        'candidate', 'pull_request', 'tracker', 'usage', 'decision', 'intake')},
        protocol_revision=state['revision'], event_type='closed_projection_reconciled',
        message='Observed authentic completed terminal result',
        key='closed-project:' + facts['execution_run_id'] + ':' + digest(state))
    receipt = {'state': 'confirmed', 'resource_cleanup': 'confirmed', 'processes': []}
    closed = {key: facts[key] for key in ('workflow_id', 'execution_run_id', 'closed_at',
                                        'request_digest', 'recovery_digest', 'result')}
    maintenance = {key: facts[key] for key in ('workflow_id', 'execution_run_id', 'closed_at')}
    maintenance['status'] = 'COMPLETED'
    store.project(parent['run_id'], phase=state['phase'],
        execution_state=state['execution_state'], outcome=state['outcome'], cleanup='confirmed',
        checks={**state['checks'], 'resource_cleanup': receipt},
        event_type='resource_cleanup_reconciled', message='Confirmed later native cleanup',
        key=digest({'closed_workflow': maintenance, 'native_observations': [],
                    'resource_cleanup': receipt}))
    child_state = {**state, 'run_id': child['run_id'], 'revision': 1, 'cleanup': 'confirmed',
                   'checks': {}, 'error': None}
    if drift == 'worker-cleanup':
        child_state.update(cleanup='unknown', error='terminal tracker synchronization conflicted',
            checks={'terminal_tracker_checkpoint': {
                'state': 'conflicted', 'event': 'blocked', 'outcome': 'blocked',
                'closed': True, 'waiting': False, 'release': False}})
    if drift == 'divergent-counters':
        # A retained recovery carries its execution's counter. Ordinary row
        # projections keep their own count across earlier candidate snapshots.
        candidate = {'id': 'retained-candidate', 'base_sha': child['base_sha'],
                     'head': child['base_sha'], 'content_sha256': 'a' * 64,
                     'environment_digest': 'b' * 64, 'policy_digest': child['policy_digest']}
        for number in range(13):
            store.project(child['run_id'], phase='preparing', execution_state='running',
                candidate={**candidate, 'id': str(number) if number < 12 else candidate['id']},
                event_type='candidate', message='Retained candidate projection')
        child_state.update(candidate=candidate, candidate_revision=9)
    child_history = await _history(store, child, child_state)
    if drift == 'child-memo':
        child_history.events[0].workflow_execution_started_event_attributes.memo.fields[
            'request_digest'].CopyFrom((await DELIVERY_DATA_CONVERTER.encode(['other-request']))[0])
    elif drift == 'child-start':
        initial = deepcopy(store.submitted_spec(child['run_id']))
        initial['goal'] = 'Different admitted work'
        child_history.events[0].workflow_execution_started_event_attributes.input.CopyFrom(Payloads(
            payloads=await DELIVERY_DATA_CONVERTER.encode([initial, None])))
    elif drift == 'child-result':
        child_history.events[-1].workflow_execution_completed_event_attributes.result.CopyFrom(
            Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([
                {**child_state, 'run_id': 'other-child', 'outcome': 'delivered'}])))
    child_facts = await closure_facts(child_history)
    seal(store, child_history, child_facts,
         run_id=child['run_id'], source='temporal')
    store.project(child['run_id'], **{key: child_state[key] for key in (
        'phase', 'execution_state', 'outcome', 'cleanup', 'error', 'iteration', 'checks',
        'candidate', 'pull_request', 'tracker', 'usage', 'decision', 'intake')},
        protocol_revision=child_state['revision'],
        event_type='closed_projection_reconciled' if drift == 'worker-cleanup' else 'blocked',
        message='Interrupted recovery really closed', key=(
            'closed-project:' + child_facts['execution_run_id'] + ':' + digest(child_state)
            if drift == 'worker-cleanup' else 'child-closed'))
    if drift == 'worker-cleanup':
        child_closure = {key: child_facts[key] for key in (
            'workflow_id', 'execution_run_id', 'closed_at')}
        child_closure['status'] = 'COMPLETED'
        store.project(child['run_id'], phase=child_state['phase'],
            execution_state=child_state['execution_state'], outcome=child_state['outcome'],
            error=child_state['error'], cleanup='confirmed',
            checks={**child_state['checks'], 'resource_cleanup': receipt},
            event_type='resource_cleanup_reconciled', message='Observed later worker cleanup',
            key=digest({'closed_workflow': child_closure, 'native_observations': [],
                        'resource_cleanup': receipt}))
        with store._connect() as db:
            store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])

    class Handle:
        def __init__(self, workflow_id):
            self.id = workflow_id

        async def describe(self):
            raise RPCError('retention expired', RPCStatusCode.NOT_FOUND, b'')

    async def connect(*_args, **_kwargs):
        return SimpleNamespace(get_workflow_handle=lambda workflow_id: Handle(workflow_id))

    monkeypatch.setattr(closure.Client, 'connect', connect)
    monkeypatch.setattr(activities, 'current_record', lambda _: {'original': True})
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (parent['run_id'],)).fetchone())
    command = {'command_id': 'settle-original-closed-parent', 'expected_revision': row['revision']}
    return store, parent, child, shared, token, command, history, closed


@pytest.mark.parametrize('committed', [False, True])
async def test_completed_row_accepts_cleanup_composed_by_ordinary_terminal_reconciliation(
    unpublished, committed,
):
    from devflow_temporal.delivery_closed_projection import (
        failed_terminal_tail,
        reconcile_projection,
    )

    store, spec, state, client, description = await _closed(unpublished, committed=committed)
    # The older shared fixture omits the controller's ordinary candidate counter.
    state['candidate_revision'] = _row(store)['candidate_revision']
    _maintenance(store, spec, description)
    _maintenance(store, spec, description, uncertain=False)
    assert (await reconcile_projection(store, client, _row(store)))['state'] == 'reconciled'
    row = _row(store)
    assert store.events('run-1')[-1]['type'] == 'closed_projection_reconciled'
    history = await client.get_workflow_handle(description.id,
                                               run_id=description.run_id).fetch_history()
    projections = {}
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == 'delivery_project':
                request = (await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads))[0]
                projections[event.event_id] = (failed_terminal_tail(
                    state, request, event.event_id) or request)
    closed = {'workflow_id': description.id, 'execution_run_id': description.run_id,
              'closed_at': description.close_time.isoformat(), 'result': state}
    closure._completed_row(store, spec, row, closed, projections)
    assert _row(store) == row
    assert state['cleanup'] == 'unknown' and row['cleanup'] == 'confirmed'


@pytest.mark.parametrize('drift', ['key', 'mixed-writer', 'missing-revision', 'unordered-time',
                                 'after-baseline'])
async def test_composed_terminal_baseline_refuses_changed_cleanup_chain(unpublished, drift):
    from devflow_temporal.delivery_closed_projection import (
        failed_terminal_tail,
        reconcile_projection,
    )

    store, spec, state, client, description = await _closed(unpublished)
    state['candidate_revision'] = _row(store)['candidate_revision']
    _maintenance(store, spec, description)
    first = store.events('run-1')[-1]
    _maintenance(store, spec, description, uncertain=False)
    second = store.events('run-1')[-1]
    await reconcile_projection(store, client, _row(store))
    row = _row(store)
    with store._connect() as db:
        if drift == 'key':
            db.execute("UPDATE delivery_events SET payload_json='{}' WHERE sequence=?",
                       (second['sequence'],))
        elif drift == 'mixed-writer':
            db.execute("UPDATE delivery_events SET type='dispatch_blocked' WHERE sequence=?",
                       (first['sequence'],))
        elif drift == 'missing-revision':
            db.execute('DELETE FROM delivery_events WHERE sequence=?', (first['sequence'],))
        elif drift == 'after-baseline':
            baseline = store.events('run-1')[-1]
            later = datetime.fromisoformat(baseline['timestamp']) + timedelta(seconds=1)
            db.execute('UPDATE delivery_events SET timestamp=? WHERE sequence=?',
                       (later.isoformat(), second['sequence']))
        else:
            db.execute('UPDATE delivery_events SET timestamp=? WHERE sequence=?',
                       (second['timestamp'], first['sequence']))
    history = await client.get_workflow_handle(description.id,
                                               run_id=description.run_id).fetch_history()
    projections = {}
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == 'delivery_project':
                request = (await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads))[0]
                projections[event.event_id] = (failed_terminal_tail(
                    state, request, event.event_id) or request)
    closed = {'workflow_id': description.id, 'execution_run_id': description.run_id,
              'closed_at': description.close_time.isoformat(), 'result': state}
    with pytest.raises(ValueError, match='composed cleanup'):
        closure._completed_row(store, spec, row, closed, projections)
    assert _row(store) == row
