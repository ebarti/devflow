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
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    continue_feature,
    detail,
    register_worker,
    registry,
    worker_key,
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
    child = worker_spec(parent, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind='build', base_sha=parent['base_sha'], base_branch='main')
    register_worker(store, parent, child)
    shared.checkpoint(token, 'assignment:' + chunk['id'] + ':build',
        {'run_id': child['run_id'], 'store_path': token['store_path'],
         'chunk_id': chunk['id'], 'kind': 'build'})
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
    seal(store, child_history, await closure_facts(child_history),
         run_id=child['run_id'], source='temporal')
    store.project(child['run_id'], **{key: child_state[key] for key in (
        'phase', 'execution_state', 'outcome', 'cleanup', 'error', 'iteration', 'checks',
        'candidate', 'pull_request', 'tracker', 'usage', 'decision', 'intake')},
        protocol_revision=child_state['revision'], event_type='blocked',
        message='Interrupted recovery really closed', key='child-closed')

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


async def test_completed_parent_continuation_retains_history_and_exhausted_budget(completed_parent):
    store, parent, child, shared, token, command, history, closed = completed_parent
    budget = shared.budget(token['issue_id'])
    assert budget['used'] == budget['maximum'] == 10
    assert closure.closed_settlement(store, parent['run_id'])['result'] == closed['result']
    assert detail(store, parent)['can_continue']
    before = store.submitted_spec(child['run_id'])
    result = continue_feature(store, parent['run_id'], command)
    successor = store.effective_spec(result['run_id'])
    assert successor['feature_delivery']['owner']['generation'] == token['generation'] + 1
    assert successor['accepted_plan'] == parent['accepted_plan']
    assert successor['plan_approval'] == parent['plan_approval']
    assert shared.budget(token['issue_id']) == budget
    checkpoint = shared.checkpoints(token['issue_id'])['stopped:' + str(token['generation'])]
    proof = checkpoint['completed_coordinator_settlement']
    assert proof['request_sha256'] == digest({'spec': parent,
        'workers': {child['run_id']: store.active_workflow_id(child['run_id'])}})
    assert proof['result_sha256'] == digest(closed['result'])
    assert proof['scheduled_event_id'] == 2 and proof['failed_event_id'] == 3
    assert checkpoint['state'] == closed['result'] and checkpoint['state']['cleanup'] == 'unknown'
    assert 'feature_stop_failure' not in checkpoint['state']['checks']
    assert store.submitted_spec(child['run_id']) == before
    assert (await closure_facts(history))['result'] == closed['result']
    assert continue_feature(store, parent['run_id'], command) == result
    with shared.connect() as db:
        worker = db.execute('SELECT state FROM execution_workers WHERE worker_key=?',
                            (worker_key(child['run_id'], token),)).fetchone()
    assert worker['state'] == 'finished'
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
        assert db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                          (child['run_id'],)).fetchone()[0] is None


@pytest.mark.parametrize('changed', ['start', 'memo', 'result', 'failure', 'settlement-spec',
                                   'stop-scheduled'])
async def test_completed_settlement_refuses_unbound_history(completed_parent, changed):
    store, parent, _, shared, token, _, history, closed = completed_parent
    history = WorkflowHistory(history.workflow_id, deepcopy(history.events))
    events = history.events
    if changed in {'start', 'memo'}:
        start = events[0].workflow_execution_started_event_attributes
        if changed == 'start':
            initial = deepcopy(store.submitted_spec(parent['run_id']))
            initial['goal'] = 'Changed authority'
            start.input.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([initial, None])))
        else:
            start.memo.Clear()
    elif changed == 'result':
        events[-1].workflow_execution_completed_event_attributes.result.CopyFrom(Payloads(
            payloads=await DELIVERY_DATA_CONVERTER.encode([{**closed['result'], 'revision': 4}])))
    elif changed == 'failure':
        events[2].activity_task_failed_event_attributes.scheduled_event_id = 1
    else:
        scheduled = events[1].activity_task_scheduled_event_attributes
        if changed == 'stop-scheduled':
            scheduled.activity_type.name = 'delivery_feature_stop'
        else:
            request = (await DELIVERY_DATA_CONVERTER.decode(scheduled.input.payloads))[0]
            request['spec']['policy']['max_repairs'] = 11
            scheduled.input.CopyFrom(Payloads(
                payloads=await DELIVERY_DATA_CONVERTER.encode([request])))
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (parent['run_id'],)).fetchone())
    with pytest.raises(ValueError):
        await closure.completed_settlement(history, store, parent, row, closed)
    assert shared.token(shared.current(token['issue_id'])) == token
    assert shared.current(token['issue_id'])['state'] == 'active'
    assert shared.budget(token['issue_id'])['used'] == 10


@pytest.mark.parametrize('changed', ['child-live', 'cleanup', 'attempt', 'effect', 'assignment',
                                   'generation', 'workflow', 'plan', 'candidate',
                                   'candidate-revision', 'maintenance'])
async def test_completed_parent_continuation_refuses_changed_custody(
    completed_parent, monkeypatch, changed,
):
    store, parent, child, shared, token, command, _, _ = completed_parent
    if changed == 'child-live':
        original = store._completed_temporal_result

        def completed(run_id, **kwargs):
            if run_id == child['run_id']:
                raise ValueError('worker Temporal execution remains live')
            return original(run_id, **kwargs)

        monkeypatch.setattr(store, '_completed_temporal_result', completed)
    elif changed in {'assignment', 'generation', 'plan'}:
        with shared.connect() as db:
            if changed == 'assignment':
                db.execute("UPDATE execution_workers SET workstream_id='other' WHERE worker_key=?",
                           (worker_key(child['run_id'], token),))
            elif changed == 'generation':
                db.execute('UPDATE execution_workers SET generation=generation+1 '
                           'WHERE worker_key=?',
                           (worker_key(child['run_id'], token),))
            else:
                db.execute('INSERT INTO execution_checkpoints VALUES (?,?,?,?,?,?)',
                    (token['issue_id'], 'plan-revision:adopted:2', token['generation'],
                     'changed', json.dumps({'identity': {'plan_revision': 2,
                                                       'plan_digest': 'f' * 64}}), 'original'))
    else:
        with store._connect() as db:
            if changed == 'cleanup':
                db.execute("UPDATE delivery_runs SET cleanup='unknown' WHERE run_id=?",
                           (child['run_id'],))
            elif changed == 'attempt':
                db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,"
                           "candidate_id,"
                           "state,cleanup) VALUES (? ,?,'verify',0,'original','running','unknown')",
                           ('live-attempt', child['run_id']))
            elif changed == 'effect':
                db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                           ('unresolved-effect', child['run_id'], 'browser_qa', '{}',
                            'pending', None, 'original'))
            elif changed == 'workflow':
                db.execute('UPDATE delivery_runs SET workflow_id=? WHERE run_id=?',
                           ('different-worker-workflow', child['run_id']))
            elif changed == 'candidate-revision':
                db.execute('UPDATE delivery_runs SET candidate_revision=candidate_revision+1 '
                           'WHERE run_id=?', (parent['run_id'],))
            elif changed == 'candidate':
                db.execute('UPDATE delivery_runs SET candidate_json=? WHERE run_id=?',
                           (json.dumps({'id': 'changed'}), parent['run_id']))
            else:
                db.execute('UPDATE delivery_events SET payload_json=? WHERE run_id=? AND type=?',
                           (json.dumps({'key': 'f' * 64, 'iteration': 0, 'candidate_id': None}),
                            parent['run_id'], 'resource_cleanup_reconciled'))
    with pytest.raises((ValueError, OwnershipConflict)):
        continue_feature(store, parent['run_id'], command)
    assert shared.token(shared.current(token['issue_id'])) == token
    assert shared.current(token['issue_id'])['state'] == 'active'
    assert shared.budget(token['issue_id'])['used'] == 10
    assert 'stopped:' + str(token['generation']) not in shared.checkpoints(token['issue_id'])


async def test_completed_settlement_replays_after_worker_receipt_before_stop(
    completed_parent, monkeypatch,
):
    store, parent, _, shared, token, command, _, _ = completed_parent
    original = activities.stop_feature
    calls = []

    def stop(spec, checkpoint):
        calls.append(deepcopy(checkpoint))
        if len(calls) == 1:
            raise OSError('Lost before recording new postclosure stop')
        return original(spec, checkpoint)

    monkeypatch.setattr(activities, 'stop_feature', stop)
    with pytest.raises(OSError):
        continue_feature(store, parent['run_id'], command)
    assert shared.current(token['issue_id'])['state'] == 'active'
    result = continue_feature(store, parent['run_id'], command)
    assert calls[0] == calls[1]
    assert continue_feature(store, parent['run_id'], command) == result
    assert shared.budget(token['issue_id'])['used'] == 10


@pytest.mark.parametrize('completed_parent', ['child-memo', 'child-start', 'child-result'],
                         indirect=True)
async def test_completed_parent_rejects_sealed_child_with_different_admission(completed_parent):
    store, parent, child, shared, token, command, _, _ = completed_parent
    assert detail(store, parent)['can_continue'] is False
    with pytest.raises(ValueError, match='closed worker history'):
        continue_feature(store, parent['run_id'], command)
    assert shared.current(token['issue_id'])['state'] == 'active'
    assert shared.budget(token['issue_id'])['used'] == 10
    with shared.connect() as db:
        assert db.execute('SELECT state FROM execution_workers WHERE worker_key=?',
                          (worker_key(child['run_id'], token),)).fetchone()[0] == 'reserved'
    assert 'stopped:' + str(token['generation']) not in shared.checkpoints(token['issue_id'])


@pytest.mark.parametrize('confirmed', [True, False])
async def test_completed_parent_uses_original_remote_effect_readback(
    completed_parent, monkeypatch, confirmed,
):
    from devflow_temporal import delivery_feature_readback as readback

    store, parent, _, shared, token, command, _, _ = completed_parent
    shared.intent(token, 'original-plan-binding', 'github_plan_bind', {'name': 'original-plan'})
    observed = []

    def issue(*identity):
        observed.append(identity)
        return {'id': token['issue_id'], 'labels': ['original-plan'] if confirmed else []}

    monkeypatch.setattr(readback, 'GitHubDelivery', lambda: SimpleNamespace(issue=issue))
    if confirmed:
        continue_feature(store, parent['run_id'], command)
        assert shared.effect(token['issue_id'], 'original-plan-binding')['state'] == 'complete'
    else:
        with pytest.raises(OwnershipConflict, match='still unconfirmed'):
            continue_feature(store, parent['run_id'], command)
        assert shared.current(token['issue_id'])['state'] == 'active'
        assert shared.effect(token['issue_id'], 'original-plan-binding')['state'] == 'pending'
    assert len(observed) == 1
    assert shared.budget(token['issue_id'])['used'] == 10


async def test_finished_worker_cannot_bypass_later_unknown_native_cleanup(
    completed_parent, monkeypatch,
):
    store, parent, child, shared, token, command, _, _ = completed_parent

    def stop(*_args):
        raise OSError('Interrupted before postclosure stop')

    monkeypatch.setattr(activities, 'stop_feature', stop)
    with pytest.raises(OSError):
        continue_feature(store, parent['run_id'], command)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='unknown' WHERE run_id=?", (child['run_id'],))
    with pytest.raises(OwnershipConflict, match='cleanup remain unsettled'):
        continue_feature(store, parent['run_id'], command)
    assert shared.current(token['issue_id'])['state'] == 'active'
    assert shared.budget(token['issue_id'])['used'] == 10


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


@pytest.mark.parametrize('drift', ['key', 'mixed-writer', 'missing-revision', 'unordered-time'])
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
