"""Complete ordinary reserve/admit; original native attribution and actor are named seams."""
from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from temporalio.api.common.v1 import ActivityType, Memo, Payloads, WorkflowType
from temporalio.api.failure.v1 import ApplicationFailureInfo, Failure
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskFailedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import WorkflowHistory
from test_delivery_feature_execution import feature_service
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_gate_attribution_recovery as attribution
from devflow_temporal import delivery_gate_preflight_continuation as preflight
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_stopped_resume as stopped
from devflow_temporal import delivery_store as stores
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_closures import closure_facts, seal
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_native_dependencies import REGISTRY
from devflow_temporal.delivery_policy_recovery import _rows, work_binding
from devflow_temporal.delivery_resources import (
    RunResources,
    _gate_evidence_root,
    observe_completed_resources,
    read_private,
    write_private,
)
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def tmp_path():
    with TemporaryDirectory(prefix='devflow-interrupted-gate-') as directory:
        yield Path(directory).resolve()


async def history_for(store, spec, recovery, state, born, *, preparation=False):
    async def payload(values):
        return Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode(values))

    workflow_id = store.active_workflow_id(spec['run_id'])
    start = WorkflowExecutionStartedEventAttributes(
        workflow_id=workflow_id, original_execution_run_id='closed-interrupted',
        workflow_type=WorkflowType(name='DevflowDeliveryWorkflow'),
        input=await payload([spec, recovery]),
        memo=Memo(fields={key: (await DELIVERY_DATA_CONVERTER.encode([value]))[0]
                          for key, value in {'request_digest': spec['request_digest'],
                                            'recovery_digest': digest(recovery)}.items()}))
    events = [HistoryEvent(event_id=1, workflow_execution_started_event_attributes=start)]
    common = {'spec': spec, 'candidate': state['candidate'], 'iteration': state['iteration']}
    projected = {**common, **{key: state.get(key) for key in (
        'phase', 'execution_state', 'outcome', 'cleanup', 'error', 'checks',
        'pull_request', 'tracker', 'usage', 'decision', 'intake')},
        'protocol_revision': state['revision'],
        'event_type': 'blocked' if preparation else 'tracker_deadline',
        'message': state['error'], 'key': f"tracker_deadline:7:{state['revision']}"}
    calls = [
        ('delivery_gates_readback', {'spec': spec, 'recovery': recovery}, {'state': 'OPEN'}),
        ('delivery_tracker_start', {'spec': spec, 'repair_continuation': True},
         {'state': 'consistent'}),
        ('delivery_project', {**common, 'event_type': 'gates_retry_started',
                             'protocol_revision': (recovery['state']['revision'] + 1 if preparation
                                                   else state['revision'] - 1)}, {}),
        ('delivery_precheck', common, state['checks']['prepublish']),
        ('delivery_finalize_resources', {'spec': spec, 'outcome': 'blocked'},
         state['checks']['resource_cleanup'] if preparation else None),
        ('delivery_terminal_tracker', {'spec': spec}, state['tracker']),
        ('delivery_project', projected, {}),
    ]
    for number, (name, request, result) in enumerate(calls):
        event_id = len(events) + 1
        scheduled = HistoryEvent(event_id=event_id,
            activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(
                activity_type=ActivityType(name=name), input=await payload([request])))
        scheduled.event_time.FromDatetime(born + timedelta(seconds=number * 2 - 6))
        events.append(scheduled)
        terminal = HistoryEvent(event_id=event_id + 1)
        terminal.event_time.FromDatetime(born + timedelta(seconds=number * 2 - 5))
        if result is None:
            terminal.activity_task_failed_event_attributes.CopyFrom(
                ActivityTaskFailedEventAttributes(scheduled_event_id=event_id,
                    failure=Failure(message='disk I/O error',
                        application_failure_info=ApplicationFailureInfo(type='OperationalError'))))
        else:
            terminal.activity_task_completed_event_attributes.CopyFrom(
                ActivityTaskCompletedEventAttributes(scheduled_event_id=event_id,
                                                      result=await payload([result])))
        events.append(terminal)
    terminal = HistoryEvent(event_id=len(events) + 1,
        workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
            result=await payload([state])))
    terminal.event_time.FromDatetime(born + timedelta(seconds=9))
    events.append(terminal)
    return WorkflowHistory(workflow_id, events), projected


@pytest.fixture
def interrupted_assessment(service, monkeypatch, request):
    store, submitted, _ = feature_service(service, monkeypatch)
    store.submit(submitted)
    parent = store.effective_spec(submitted['run_id'])
    shared, token = registry(parent), parent['feature_delivery']['owner']
    for number in range(10):
        shared.repair(token, f'paid-{number}', 'Original paid product work')
    chunk = ordered_chunks(json.loads(parent['accepted_plan']))[0]
    child = worker_spec(parent, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind='chunk', base_sha=parent['base_sha'], base_branch='main')
    register_worker(store, parent, child)
    shared.checkpoint(token, f"assignment:{chunk['id']}:chunk",
        {'run_id': child['run_id'], 'store_path': token['store_path'],
         'chunk_id': chunk['id'], 'kind': 'chunk'})
    monkeypatch.setattr(activities, 'current_record', lambda _:
        {'manifest': {'plan': json.loads(parent['accepted_plan']), 'publication': {'members': []}}})
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Preserved published candidate\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-qm', 'Preserved candidate')
    _git(broker.checkout, 'push', 'origin', f"HEAD:refs/heads/{child['branch']}")
    candidate = broker.candidate()
    publication = {'number': 7, 'url': 'https://github.com/example/fixture/pull/7',
                   'head': candidate['head'], 'candidate': candidate, 'base': child['base_sha']}
    pr = {'number': 7, 'state': 'OPEN', 'isDraft': False, 'headRefOid': candidate['head']}
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_: deepcopy(pr))
    old = {'run_id': child['run_id'], 'phase': 'blocked', 'outcome': 'blocked',
           'execution_state': 'blocked', 'cleanup': 'confirmed', 'revision': 70, 'iteration': 7,
           'candidate_revision': 9, 'candidate': candidate, 'pull_request': publication,
           'roles': [{'role': 'implement', 'iteration': 7, 'status': 'blocked',
                      'session_id': 'original-session', 'cleanup': 'confirmed',
                      'candidate': candidate}],
           'checks': {}, 'error': 'implementer did not establish a pass', 'usage': {},
           'tracker': {}, 'decision': None, 'intake': None}
    def project(state, event_type='blocked', key=None):
        return store.project(child['run_id'], **{k: state.get(k) for k in (
            'phase', 'outcome', 'execution_state', 'cleanup', 'iteration', 'candidate',
            'pull_request', 'checks', 'error', 'usage', 'tracker', 'decision', 'intake')},
            protocol_revision=state['revision'], event_type=event_type,
            message=state['error'], key=key)
    project(old)
    resources = RunResources(child)
    resources.finalize('blocked')
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
        binding = work_binding(store, child, db)
    row, attempts, effects, _ = _rows(store, child['run_id'])
    closures = {}
    original_closed = {'workflow_id': store.active_workflow_id(child['run_id']),
        'execution_run_id': 'original-assessment', 'closed_at': datetime.now(UTC).isoformat(),
        'request_digest': child['request_digest'], 'recovery_digest': None, 'result': old}
    closures[original_closed['workflow_id']] = original_closed
    def completed(given, _run, *, workflow_id=None):
        assert given.config.tracking_db == store.config.tracking_db
        if workflow_id not in closures:
            raise ValueError('workflow has no completed retained result')
        return deepcopy(closures[workflow_id])
    monkeypatch.setattr(stores.DeliveryStore, '_completed_temporal_result', completed)
    proof = {'cause': 'wrapped_junit_source_attribution', 'unresolved_findings': ['Unpassed work']}
    original_snapshot = gates.snapshot
    seed = {'row': gates._sealed_row(row), 'closed': original_closed, 'original_spec': child,
            'attempts': attempts, 'effects': effects, 'candidate': candidate,
            'publication': publication, 'cleanup': {'journal_sha256': {}},
            'work_binding': binding, 'previous_digest': None, 'prior_gate_digest': None,
            'stage': 'published', 'generation': 2, 'attribution_observation': proof}
    # Original native attribution is the sole assessment-authority seam. Both
    # subsequent continuations use the actual private producers/readback/admission.
    monkeypatch.setattr(gates, 'snapshot', lambda *_a, **_kw: deepcopy(seed))
    command = {'continuation_kind': gates.KIND, 'command_id': 'original-gate-assessment',
        'expected_revision': 70, 'expected_iteration': 7, 'expected_candidate_id': candidate['id'],
        'expected_pr_head': candidate['head'], 'expected_pr_number': 7, 'additional_iterations': 0}
    first = gates.admit(store, child['run_id'], command)
    monkeypatch.setattr(gates, 'snapshot', original_snapshot)
    monkeypatch.setattr(attribution, 'observe', lambda *_a, **_kw: deepcopy(proof))
    spec = store.effective_spec(child['run_id'])
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                        (child['run_id'],)).fetchone()[0])
    state = {**deepcopy(old), 'candidate': recovery['candidate'],
             'pull_request': recovery['publication'], 'revision': 71,
             'error': 'gate retry preflight failed: ActivityError'}
    state['checks'] = {'failure': {'classification': 'terminal', 'stage': 'gates_retry',
                                  'reason': state['error']}}
    project(state)
    resources.finalize('blocked')
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    closed = {**original_closed, 'workflow_id': first['workflow_id'], 'result': state,
              'recovery_digest': digest(recovery)}
    closures[first['workflow_id']] = closed
    monkeypatch.setattr(preflight, 'observe_completed_resources',
                        lambda given, _attempts: observe_completed_resources(given, []))
    assert activities.reserve(parent, chunk['id'], 'chunk')['resumed']
    spec = store.effective_spec(child['run_id'])
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                        (child['run_id'],)).fetchone()[0])
    born = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=30)
    state = {**deepcopy(state), 'revision': 77, 'cleanup': 'unknown',
             'error': 'terminal tracker synchronization conflicted',
             'candidate': recovery['candidate'], 'pull_request': recovery['publication']}
    state['tracker'] = {'state': 'pending', 'pending': True, 'reason': 'OperationalError'}
    state['checks'] = {
        'failure': {'classification': 'terminal', 'stage': 'gates_retry',
                    'reason': 'technical prepublication checks failed'},
        'prepublish': {'candidate_id': state['candidate']['id'], 'state': 'unknown',
                      'cleanup': 'unknown', 'reason': 'OperationalError'},
        'resource_cleanup': {'state': 'unknown', 'resource_cleanup': 'unknown'},
        'terminal_tracker_checkpoint': {'closed': True, 'waiting': False, 'state': 'conflicted',
            'reason': 'technical prepublication checks failed'}}
    history, projected = asyncio.run(history_for(store, spec, recovery, state, born))
    drift = getattr(request, 'param', None)
    if drift == 'failure':
        failure = history.events[10].activity_task_failed_event_attributes.failure
        failure.application_failure_info.type = 'ValueError'
    facts = asyncio.run(closure_facts(history))
    seal(store, history, facts, run_id=child['run_id'], source='temporal')
    closed = {k: facts[k] for k in ('workflow_id', 'execution_run_id', 'closed_at',
                                  'request_digest', 'recovery_digest', 'result')}
    closures[closed['workflow_id']] = closed
    normal_clock = stores._now
    monkeypatch.setattr(stores, '_now', lambda:
                        (born + timedelta(seconds=8)).isoformat())
    project(state, 'tracker_deadline', projected['key'])
    monkeypatch.setattr(stores, '_now', normal_clock)
    receipt = RunResources(spec).finalize('blocked')
    folder = _gate_evidence_root(spec) / 'dependency-preparation/native-8/process'
    staging = Path(spec['state_dir']) / 'transient/dependencies' / spec['policy_digest'] / 'staging'
    environment = {'COREPACK_ENABLE_NETWORK': '0',
                   'npm_config_registry': 'https://' + REGISTRY + '/'}
    argv = ['corepack', 'pnpm@10.24.0', 'fetch', '--frozen-lockfile', '--ignore-scripts',
            '--ignore-pnpmfile', '--store-dir', str(staging / 'store')]
    identity = born.astimezone().strftime('%a %b %d %H:%M:%S %Y')
    journal = {'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
        'argv': argv, 'cwd': str(staging), 'timeout': 1800, 'ports': [],
        'environment_sha256': digest(environment)},
        'monitor': {'pid': 300001, 'identity': identity},
        'owned': {'300002': {'identity': identity, 'ppid': 300001, 'pgid': 300002}},
        'phase': 'finished', 'monitoring_complete': False, 'runtime_identity': {},
        'result': {'provider_outcome': 'unknown', 'exit_code': None, 'interrupted_monitor': True}}
    write_private(folder / 'native-process.json', journal)
    write_private(folder / 'launch.json', {'argv': argv, 'environment': environment})
    write_private(folder / 'ready.json', {'pid': 300002, 'identity': identity})
    # Fake provider/native transport seam: no actor is launched or declared passed.
    # The real archived interrupted actor is separately qualified from saved bytes.
    final = {k: v for k, v in receipt.items() if k not in {'receipt', 'receipt_sha256'}}
    final['processes'] = [{'journal': str(folder / 'native-process.json'),
                           'cleanup': 'observed-native-confirmed', 'monitoring_complete': False}]
    write_private(Path(receipt['receipt']), final)
    manifest = read_private(RunResources(spec).manifest)
    manifest['processes'].append(str(folder / 'native-process.json'))
    manifest['finalization'] = final
    write_private(RunResources(spec).manifest, manifest)
    receipt = {**final, 'receipt': receipt['receipt'],
               'receipt_sha256': hashlib.sha256(Path(receipt['receipt']).read_bytes()).hexdigest()}
    store.project(child['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
        cleanup='confirmed', checks={**state['checks'], 'resource_cleanup': receipt},
        event_type='resource_cleanup_reconciled', message='Observed independent later cleanup',
        key=digest({'closed_workflow': {k: v for k, v in {**closed, 'status': 'COMPLETED'}.items()
                                      if k in {'workflow_id', 'execution_run_id',
                                               'closed_at', 'status'}},
                    'native_observations': receipt['processes'], 'resource_cleanup': receipt}))
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    def native_observation(_spec, _attempts):
        manifest_hash = hashlib.sha256(RunResources(spec).manifest.read_bytes()).hexdigest()
        journal_hash = hashlib.sha256((folder / 'native-process.json').read_bytes()).hexdigest()
        return {'manifest_sha256': manifest_hash,
                'finalization_sha256': receipt['receipt_sha256'],
                'journal_sha256': {str(folder / 'native-process.json'): journal_hash},
                'roots': {}}
    monkeypatch.setattr(preflight, 'observe_completed_resources', native_observation)
    monkeypatch.setattr('devflow_temporal.delivery_native_dependencies.frozen_pnpm_inputs',
                        lambda *_a, **_kw: ('pnpm@10.24.0', {}))
    return SimpleNamespace(store=store, parent=parent, child=child, spec=spec, shared=shared,
        token=token, state=state, closed=closed, recovery=recovery, history=history, folder=folder,
        chunk=chunk, drift=drift, proof=proof, closures=closures)


@pytest.mark.parametrize('passed', [True, False])
def test_reserve_rearms_only_fresh_gates_and_keeps_exhausted_budget(
    interrupted_assessment, monkeypatch, passed,
):
    f = interrupted_assessment
    before, budget = deepcopy(f.closed), f.shared.budget(f.token['issue_id'])
    assert budget['used'] == budget['maximum'] == 10
    with pytest.raises(ValueError, match='closed finalized delivery'):
        stopped.snapshot(f.store, f.child['run_id'])
    with pytest.raises(ValueError, match='already terminal'):
        f.store.begin_mutation(f.child['run_id'], 'reconcile-old-terminal', 'reconcile_tracker',
                               {'command_id': 'reconcile-old-terminal', 'expected_revision': 77})
    result = activities.reserve(f.parent, f.chunk['id'], 'chunk')
    assert result['resumed'] and result['workflow_id'].endswith('-preflight-continuation-2')
    assert activities.reserve(f.parent, f.chunk['id'], 'chunk') == {
        'spec': result['spec'], 'completed': False, 'workflow_id': result['workflow_id']}
    spec = result['spec']
    with f.store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                        (f.child['run_id'],)).fetchone()[0])
    assert recovery['command']['additional_iterations'] == 0
    replay = gates.admit(f.store, f.child['run_id'], recovery['command'])
    assert replay['workflow_id'] == result['workflow_id']
    assert replay['implementation_authority'] is False and replay['additional_iterations'] == 0
    assert recovery['state'] == f.state and f.closed == before
    assert recovery['candidate']['id'] == f.state['candidate']['id']
    assert recovery['publication']['head'] == f.state['pull_request']['head']
    assert recovery['seal']['attribution_observation'] == f.proof
    assert not preflight.interrupted({'recovery_json': json.dumps(recovery),
                                     'checks_json': json.dumps(f.state['checks']),
                                     'error': f.state['error']})
    assert gates.readback(f.store, spec, recovery)['number'] == 7
    assert f.shared.budget(f.token['issue_id']) == budget
    flow, calls = DeliveryWorkflow(), []
    async def activity(name, body, **_kw):
        calls.append(name)
        if name == 'delivery_gates_readback':
            return gates.readback(f.store, body['spec'], body['recovery'])
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            return {'role': body['role'], 'status': 'pass', 'iteration': 7,
                    'session_id': 'fresh-' + body['role'], 'cleanup': 'confirmed',
                    'candidate': recovery['candidate']}
        return {'state': 'passed' if passed else 'failed', 'cleanup': 'confirmed'}
    async def project(*_a, **_kw):
        pass
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    finished = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert finished['outcome'] == ('delivered' if passed else 'blocked')
    assert finished['iteration'] == 7
    assert finished['roles'][:len(f.state['roles'])] == f.state['roles']
    assert f.state['roles'][-1]['status'] == 'blocked'
    assert 'delivery_precheck' in calls and 'delivery_publish' not in calls
    assert f.shared.budget(f.token['issue_id']) == budget
    with f.store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()


@pytest.mark.parametrize('changed', ['effect', 'candidate', 'cleanup-key', 'ready', 'argv'])
def test_interrupted_assessment_refuses_changed_provenance(interrupted_assessment, changed):
    f = interrupted_assessment
    if changed == 'ready':
        write_private(f.folder / 'ready.json', {'pid': 300001,
            'identity': read_private(f.folder / 'native-process.json')['monitor']['identity']})
    elif changed == 'argv':
        journal = read_private(f.folder / 'native-process.json')
        journal['intent']['argv'] = ['corepack', 'pnpm@10.24.0', 'run', 'arbitrary']
        write_private(f.folder / 'native-process.json', journal)
        launch = read_private(f.folder / 'launch.json')
        launch['argv'] = journal['intent']['argv']
        write_private(f.folder / 'launch.json', launch)
    else:
        with f.store._connect() as db:
            if changed == 'effect':
                db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                    ('unrelated', f.child['run_id'], 'check', '{}', 'complete', '{}', 'original'))
            elif changed == 'candidate':
                db.execute('UPDATE delivery_runs SET candidate_json=? WHERE run_id=?',
                           (json.dumps({'id': 'changed'}), f.child['run_id']))
            elif changed == 'cleanup-key':
                db.execute("UPDATE delivery_events SET payload_json='{}' WHERE run_id=? "
                           "AND type='resource_cleanup_reconciled'", (f.child['run_id'],))
    with pytest.raises(ValueError):
        activities.reserve(f.parent, f.chunk['id'], 'chunk')
    assert f.shared.budget(f.token['issue_id'])['used'] == 10
    with f.store._connect() as db:
        assert db.execute('SELECT outcome FROM delivery_runs WHERE run_id=?',
                          (f.child['run_id'],)).fetchone()[0] == 'blocked'


@pytest.mark.parametrize('interrupted_assessment', ['failure'], indirect=True)
def test_interrupted_assessment_requires_actual_operational_failure(interrupted_assessment):
    f = interrupted_assessment
    with pytest.raises(ValueError, match='incomplete infrastructure result'):
        activities.reserve(f.parent, f.chunk['id'], 'chunk')


def test_interrupted_assessment_refuses_row_drift_during_native_readback(
    interrupted_assessment, monkeypatch,
):
    f = interrupted_assessment
    original = preflight.observe_completed_resources
    def drift(*args):
        proof = original(*args)
        with f.store._connect() as db:
            db.execute('UPDATE delivery_runs SET candidate_revision=candidate_revision+1 '
                       'WHERE run_id=?', (f.child['run_id'],))
        return proof
    monkeypatch.setattr(preflight, 'observe_completed_resources', drift)
    with pytest.raises(ValueError, match='ownership or records changed'):
        activities.reserve(f.parent, f.chunk['id'], 'chunk')
    assert f.shared.budget(f.token['issue_id'])['used'] == 10


@pytest.fixture
def preparation_assessment(interrupted_assessment, monkeypatch, request):
    f = interrupted_assessment
    result = activities.reserve(f.parent, f.chunk['id'], 'chunk')
    spec = result['spec']
    with f.store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (spec['run_id'],)).fetchone())
    recovery = json.loads(row['recovery_json'])
    receipt = json.loads(row['checks_json'])['resource_cleanup']
    state = {**deepcopy(f.state), 'revision': recovery['state']['revision'] + 3,
             'cleanup': 'confirmed', 'error': 'An unused prerequisite was not prepared',
             'candidate': recovery['candidate'], 'pull_request': recovery['publication'],
             'tracker': {'state': 'recorded', 'scope': 'local', 'pending': False,
                         'claim_released': True}}
    state['checks'] = {'failure': {'classification': 'terminal', 'stage': 'gates_retry',
                                   'reason': state['error']},
        'prepublish': {'state': 'failed', 'cleanup': 'confirmed', 'source_unchanged': True,
                      'candidate_id': state['candidate']['id'],
                      'results': [{'id': 'unused-preparation', 'passed': False,
                                   'launched': False, 'failure_kind': 'preparation',
                                   'cleanup': 'confirmed'}]},
        'resource_cleanup': receipt,
        'terminal_tracker_checkpoint': {'state': 'confirmed', 'waiting': False, 'release': True}}
    changed = getattr(request, 'param', None)
    item = state['checks']['prepublish']['results'][0]
    if changed == 'launched':
        item['launched'] = True
    elif changed == 'product':
        item['failure_kind'] = 'test'
    elif changed == 'mixed':
        state['checks']['prepublish']['results'].append({**item, 'id': 'assessed', 'passed': True})
    elif changed == 'empty':
        state['checks']['prepublish']['results'] = []
    born = datetime.now(UTC).replace(microsecond=0) - timedelta(seconds=20)
    history, projected = asyncio.run(history_for(
        f.store, spec, recovery, state, born, preparation=True))
    facts = asyncio.run(closure_facts(history))
    seal(f.store, history, facts, run_id=spec['run_id'], source='temporal')
    closed = {k: facts[k] for k in ('workflow_id', 'execution_run_id', 'closed_at',
                                  'request_digest', 'recovery_digest', 'result')}
    f.closures[closed['workflow_id']] = closed
    normal_clock = stores._now
    monkeypatch.setattr(stores, '_now', lambda: (born + timedelta(seconds=8)).isoformat())
    f.store.project(spec['run_id'], **{k: state.get(k) for k in (
        'phase', 'outcome', 'execution_state', 'cleanup', 'iteration', 'candidate',
        'pull_request', 'checks', 'error', 'usage', 'tracker', 'decision', 'intake')},
        protocol_revision=state['revision'], event_type='blocked', message=state['error'],
        key=projected['key'])
    monkeypatch.setattr(stores, '_now', normal_clock)
    with f.store._connect() as db:
        f.store.state.release_work(db, spec['work_id'], 'external:devflow:' + spec['run_id'])
    monkeypatch.setattr('devflow_temporal.delivery_resources.observe_completed_resources',
                        preflight.observe_completed_resources)
    return SimpleNamespace(**{**vars(f), 'spec': spec, 'state': state, 'closed': closed,
                              'recovery': recovery})


@pytest.mark.parametrize('passed', [True, False])
def test_unlaunched_preparation_reserve_replays_without_a_third_interruption(
    preparation_assessment, monkeypatch, passed,
):
    from devflow_temporal.delivery_activities import _context, delivery_gates_readback
    from devflow_temporal.delivery_check_prelaunch import preparation_possible

    f = preparation_assessment
    original, budget = deepcopy(f.closed), f.shared.budget(f.token['issue_id'])
    result = activities.reserve(f.parent, f.chunk['id'], 'chunk')
    assert result['workflow_id'].endswith('-preparation-retry')
    assert result['workflow_id'] not in f.closures
    spec = result['spec']
    assert 'gate_retry_preflight_generation' not in spec
    store, broker = _context(spec)
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                        (spec['run_id'],)).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['kind'] == gates.PRELAUNCH_KIND
    assert recovery['command']['additional_iterations'] == 0
    assert recovery['state'] == f.state and f.closed == original
    assert recovery['original_recovery'] == f.recovery
    assert recovery['candidate']['id'] == f.state['candidate']['id']
    assert recovery['publication']['head'] == f.state['pull_request']['head']
    assert str(broker.evidence_dir).endswith('/published-preparation-admission-2/evidence')
    assert gates.admit(store, spec['run_id'], recovery['command'])['workflow_id'] == result[
        'workflow_id']
    assert asyncio.run(delivery_gates_readback({'spec': spec, 'recovery': recovery}))['number'] == 7
    renewed = {'recovery_json': json.dumps(recovery), 'checks_json': json.dumps(f.state['checks']),
               'error': f.state['error']}
    assert not preparation_possible(renewed) and not preflight.interrupted(renewed)
    with pytest.raises(ValueError):
        gates.admit(store, spec['run_id'],
                    {**recovery['command'], 'command_id': 'second-preparation'})
    assert f.shared.budget(f.token['issue_id']) == budget and budget['used'] == 10
    flow, calls = DeliveryWorkflow(), []
    async def activity(name, body, **_kw):
        calls.append(name)
        if name == 'delivery_gates_readback':
            return gates.readback(store, body['spec'], body['recovery'])
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            return {'role': body['role'], 'status': 'pass', 'iteration': 7,
                    'session_id': 'fresh-' + body['role'], 'cleanup': 'confirmed',
                    'candidate': recovery['candidate']}
        return {'state': 'passed' if passed else 'failed', 'cleanup': 'confirmed'}
    async def project(*_a, **_kw):
        pass
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    finished = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert finished['outcome'] == ('delivered' if passed else 'blocked')
    assert finished['iteration'] == 7
    assert finished['roles'][:len(f.state['roles'])] == f.state['roles']
    assert 'delivery_precheck' in calls and 'delivery_publish' not in calls
    assert f.state['roles'][-1]['status'] == 'blocked'
    assert f.shared.budget(f.token['issue_id']) == budget


@pytest.mark.parametrize('preparation_assessment', ['launched', 'product', 'mixed', 'empty'],
                         indirect=True)
def test_unlaunched_preparation_rejects_assessed_or_ambiguous_results(preparation_assessment):
    from devflow_temporal.delivery_check_prelaunch import preparation_possible

    f = preparation_assessment
    row, *_ = _rows(f.store, f.spec['run_id'])
    assert not preparation_possible(row)
    with pytest.raises(ValueError):
        gates.snapshot(f.store, f.spec['run_id'], gates.PRELAUNCH_KIND)
    assert f.shared.budget(f.token['issue_id'])['used'] == 10
