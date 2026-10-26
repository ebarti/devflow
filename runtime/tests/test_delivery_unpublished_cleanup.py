"""Public closed cleanup through finite same-run admission; external actors are opaque."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from contextlib import contextmanager
from pathlib import Path

import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal import delivery_resources as resources
from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_unpublished_recovery import quarantined_implementation
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture(params=['passed', 'unknown'])
def closed_unpublished(unpublished, monkeypatch, request):
    store, _broker, state, _command = unpublished
    spec = copy.deepcopy(store.effective_spec('run-1'))
    spec['provider'] = 'codex'
    spec['automatic_retry_version'] = spec['retry_budget_version'] = 1
    spec['policy'].update(execution_backend='native-macos', host_sandbox='trusted-local')
    spec['policy_digest'] = digest(spec['policy'])
    broker = DeliveryBroker(store, spec)
    partial = broker.candidate()
    state.update(cleanup='unknown', candidate=partial, tracker=None)
    state['checks'] = {
        'resource_cleanup': {'state': 'unknown', 'resource_cleanup': 'unknown',
                             'process_cleanup': 'unknown', 'reason': 'ActivityError'},
        'failure': {'classification': 'terminal', 'stage': 'prepublish_checks',
                    'reason': 'prepublication process cleanup is unknown'},
        'prepublish': {'state': 'unknown', 'cleanup': 'unknown',
                       'candidate_id': partial['id'], 'reason': 'CalledProcessError'}}
    state['error'] = 'prepublication process cleanup is unknown'
    role = state['roles'][0]
    role['candidate'] = partial
    role['finish_reason'] = 'done'
    role['findings'] = []
    raw = {k: v for k, v in role.items() if k not in {
        'role', 'iteration', 'candidate', 'input_candidate_id', 'attempt_id', 'provider'}}
    job, pid, process_identity = 'original', None, None
    if request.param == 'unknown':
        # The original closed result returned unknown before later teardown.
        state['candidate'] = {**partial, 'id': digest('sealed-input'),
                              'content_sha256': digest('sealed-input-content')}
        state['error'] = 'implementer did not establish a pass'
        state['checks'].pop('prepublish')
        state['checks']['failure'].update(stage='implement', reason=state['error'])
        role.update(status='recovery_unknown', finish_reason='recovery_unknown',
                    session_id=None, cleanup='unknown', input_candidate_id=state['candidate']['id'])
        q = {'spec': spec, 'role': 'implement', 'iteration': 0, 'resume_session': None,
             'workspace': spec['checkout'], 'candidate': state['candidate']}
        job = DeliverySupervisor._job_key(q)
        folder = Path(spec['state_dir']) / 'attempts' / job
        path = folder / 'native-process.json'
        q.update(result_path=str(folder / 'result.json'), start_path=str(folder / 'start.json'))
        intent, owned = {'run_id': spec['run_id']}, {'17017': {'identity': 'opaque-provider'}}
        proof = {'kind': 'interrupted_monitor', 'version': 1,
                 'intent_sha256': digest(intent), 'owned_sha256': digest(owned)}
        native = {'journal': str(path), 'cleanup': 'observed-native-confirmed',
                  'interrupted_monitor': True, 'monitoring_complete': False,
                  'provider_outcome': 'unknown', 'cleanup_reconciliation': proof}
        raw = {'status': 'recovery_unknown', 'finish_reason': 'interrupted_monitor',
               'session_id': None, 'cleanup': 'confirmed',
               'process_cleanup': 'observed-native-confirmed', 'native_process': native}
        journal = {'phase': 'finished', 'monitoring_complete': False, 'owned': owned,
                   'intent': intent, 'result': native, 'cleanup_reconciliation': proof,
                   'provider_session': {'role': 'implement', 'iteration': 0,
                       'session_id': None, 'resumed_from': None,
                       'output_candidate': {k: partial[k]
                                            for k in ('id', 'head', 'content_sha256')},
                       'result_digest': digest(raw)}}
        write_private(folder / 'request.json', q)
        write_private(path, journal)
        pid, process_identity = 17017, 'opaque-provider'
    root = Path(spec['state_dir']) / 'resources'
    write_private(root / 'manifest.json', {'run_id': 'run-1', 'fixture': 'retained'})
    write_private(root / 'finalization.json', {'state': 'unknown', 'outcome': 'blocked'})
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=?,cleanup=?,checks_json=?,error=?, '
                   'candidate_json=?,tracker_json=?,accepted_plan_text=?',
                   (canonical_json(spec), 'unknown', canonical_json(state['checks']),
                    state['error'],
                    canonical_json(state['candidate']), 'null', spec['accepted_plan']))
        db.execute("UPDATE delivery_outbox SET state='sent'")
        db.execute('DELETE FROM delivery_attempts')
        db.execute('INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,'
                   'state,session_id,result_json,cleanup,pid,process_identity,result_path) '
                   "VALUES (?,'run-1','implement',0,?,'finished',?,?,'confirmed',?,?,?)",
                   (job, state['candidate']['id'], role.get('session_id'), canonical_json(raw),
                    pid, process_identity, str(Path(spec['state_dir']) / 'attempts' / job
                                               / 'result.json')))
        store.state.claim_work(db, spec['work_id'], 'external:devflow:run-1', 'fixture')
    closed = store._completed_temporal_result('run-1')
    closed['closed_at'] = '2026-10-07T00:00:00+00:00'
    calls, external = [], {'on_tracker': None}

    def observe(_spec, *_args, **_kwargs):
        return {'manifest_sha256': hashlib.sha256(
                    (root / 'manifest.json').read_bytes()).hexdigest(),
                'finalization_sha256': hashlib.sha256(
                    (root / 'finalization.json').read_bytes()).hexdigest(), 'journal_sha256': {
                    str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                    for p in (Path(spec['state_dir']) / 'attempts').glob('*/native-process.json')}}

    class NativeResources:
        def __init__(self, *_args, **_kwargs):
            pass

        @contextmanager
        def locked(self):
            yield {}

        def finalize(self, outcome):
            calls.append(('finalize', outcome))
            write_private(root / 'finalization.json', {'state': 'confirmed', 'outcome': outcome})
            return {'state': 'confirmed', 'resource_cleanup': 'confirmed',
                    'process_cleanup': 'observed-native-confirmed',
                    'receipt_sha256': observe(spec)['finalization_sha256']}

    def tracker(_spec, status, *, release, terminal, reason):
        calls.append(('tracker', status, release, terminal))
        if external['on_tracker']:
            external['on_tracker']()
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        return {'state': 'recorded', 'pending': False, 'scope': 'local',
                'project_sync': 'asynchronous', 'status': status, 'claim_released': release}

    monkeypatch.setattr(retry, 'RunResources', NativeResources)
    monkeypatch.setattr(retry, 'observe_completed_resources', observe)
    monkeypatch.setattr(retry, 'observe_finalized_resources', observe)
    monkeypatch.setattr(retry, '_tracker_sync', tracker)
    monkeypatch.setattr(DeliveryBroker, '_read_owned_pr', lambda *_: None)
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    monkeypatch.setattr(resources, 'observe_completed_resources', observe)
    monkeypatch.setattr(resources, 'observe_finalized_resources', observe)
    monkeypatch.setattr(resume, 'observed_native_cleanup', lambda *_a, **_k: digest(observe(spec)))
    monkeypatch.setattr('devflow_temporal.delivery_session_custody.implementation_custody',
                        lambda *_: None)  # Provider-session inventory is an opaque boundary.
    monkeypatch.setattr(resume, 'prepare_runtime', lambda value, *_: copy.deepcopy(value))
    return store, broker, state, spec, calls, external, request.param


def command_for(state, candidate, *, unknown=False):
    return {'continuation_kind': resume.KIND, 'command_id': 'after-cleanup',
            'expected_revision': state['revision'], 'expected_iteration': state['iteration'],
            'expected_candidate_id': candidate['id'], 'expected_candidate_head': candidate['head'],
            'additional_iterations': 1 if unknown else 0}


def test_public_unpublished_cleanup_then_existing_finite_continuation(closed_unpublished):
    store, broker, state, spec, calls, _external, shape = closed_unpublished
    original = copy.deepcopy(state)
    before = broker.candidate()
    assert reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)['changed'] is True
    assert state == original and broker.candidate() == before
    assert len(calls) == 2
    assert reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)['changed'] is False
    assert len(calls) == 2  # No duplicate tracker effect and no successor admission.
    assert retry.retry_once(store) == []  # The sealed terminal classification remains terminal.
    command = command_for(state, before, unknown=shape == 'unknown')
    preflight = store.repair_admission_preflight('run-1', command)
    assert preflight['authorized_through_iteration'] == command['additional_iterations']
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id']) is None
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    admitted = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == admitted
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
    execution = store.effective_spec('run-1')
    assert recovery['state'] == original
    assert resume.readback(store, execution, recovery) == {'state': 'confirmed'}
    if shape == 'unknown':
        assert recovery['session_id'] is None and 'resume_iteration' not in recovery
        assert execution['role_home_generation'] == 'stopped-resume-' + recovery['command_digest']
        assert 'implementation_role_home_generation' not in execution
        assert recovery['quarantined_implementation']['provider_outcome'] == 'unknown'
        assert state['roles'][0]['cleanup'] == 'unknown'
    else:
        assert recovery['resume_iteration'] == 0 and recovery['resume_stage'] == 'checks'
        assert recovery['gate_evidence']['implementation_job_key'] == 'original'


@pytest.mark.parametrize('defect', ['plan', 'pre_intake', 'claim_owner', 'issue_binding',
                                   'published_effect', 'pending_effect', 'unobserved_effect',
                                   'active_workflow'])
def test_public_cleanup_never_releases_pre_intake_foreign_or_uncertain_custody(
    closed_unpublished, defect,
):
    store, _broker, state, _spec, calls, _external, _shape = closed_unpublished
    with store._connect() as db:
        if defect == 'plan':
            db.execute("UPDATE delivery_runs SET accepted_plan_text=''")
        elif defect == 'pre_intake':
            state['roles'] = []
        elif defect == 'claim_owner':
            db.execute("UPDATE claims SET owner='foreign'")
        elif defect == 'issue_binding':
            db.execute("UPDATE works SET issue='https://github.com/example/fixture/issues/99'")
        elif defect in {'published_effect', 'pending_effect', 'unobserved_effect'}:
            db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                       ('effect', 'run-1', 'publish' if defect == 'published_effect' else 'other',
                        '{}', 'pending' if defect == 'pending_effect' else 'complete',
                        None if defect == 'unobserved_effect' else '{}', 'now'))
        else:
            db.execute("UPDATE delivery_runs SET execution_state='running'")
    with pytest.raises(ValueError):
        reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    assert calls == []


def test_public_cleanup_cannot_commit_after_a_new_pending_effect(closed_unpublished):
    store, _broker, _state, _spec, _calls, external, _shape = closed_unpublished
    def racing_effect():
        with store._connect() as db:
            db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                       ('effect', 'run-1', 'publish', '{}', 'pending', None, 'now'))
    external['on_tracker'] = racing_effect
    with pytest.raises(ValueError, match='predecessor changed|post-intake custody'):
        reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    with store._connect() as db:
        assert not any(json.loads(row[0]).get('closure') == 'unpublished' for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'"))


@pytest.mark.parametrize('closed_unpublished', ['unknown'], indirect=True)
@pytest.mark.parametrize('defect', ['output', 'result_digest', 'request', 'session', 'raw_result',
                                   'owned', 'closed_role', 'result_file', 'source'])
def test_quarantined_partial_checkpoint_rejects_changed_evidence(closed_unpublished, defect):
    store, broker, state, spec, calls, _external, shape = closed_unpublished
    with store._connect() as db:
        attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
    folder = Path(spec['state_dir']) / 'attempts' / attempt['job_key']
    path = folder / 'native-process.json'
    journal = read_private(path)
    if defect == 'output':
        journal['provider_session']['output_candidate']['id'] = 'f' * 64
    elif defect == 'result_digest':
        journal['provider_session']['result_digest'] = 'f' * 64
    elif defect == 'request':
        request = read_private(folder / 'request.json')
        request['workspace'] = str(folder)
        write_private(folder / 'request.json', request)
    elif defect == 'session':
        journal['provider_session']['session_id'] = 'invented'
    elif defect == 'raw_result':
        attempt['result_json'] = canonical_json({'status': 'pass'})
    elif defect == 'owned':
        journal['owned']['17017']['identity'] = 'replacement'
    elif defect == 'closed_role':
        state['roles'][0]['input_candidate_id'] = 'f' * 64
    elif defect == 'result_file':
        write_private(folder / 'result.json', {'status': 'pass'})
    else:
        (broker.checkout / 'README.md').write_text('Unbound post-stop source\n')
    write_private(path, journal)
    with pytest.raises(ValueError):
        quarantined_implementation(spec, state, [attempt], broker.candidate())
    assert calls == []


@pytest.mark.parametrize('closed_unpublished', ['unknown'], indirect=True)
def test_zero_iterations_cannot_repeat_unknown_provider(closed_unpublished):
    store, broker, state, _spec, _calls, _external, shape = closed_unpublished
    reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    with pytest.raises(ValueError, match='zero additional iterations|new iteration'):
        store.repair_admission_preflight('run-1', command_for(state, broker.candidate()))


@pytest.mark.parametrize('defect', ['anchor_key', 'closed_execution', 'candidate',
                                   'attempt', 'claim'])
def test_post_cleanup_preflight_retains_exact_anchor_and_source_authority(
    closed_unpublished, defect,
):
    store, broker, state, _spec, _calls, _external, shape = closed_unpublished
    reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    command = command_for(state, broker.candidate(), unknown=shape == 'unknown')
    if defect == 'candidate':
        (broker.checkout / 'README.md').write_text('Changed after acknowledged cleanup\n')
    else:
        with store._connect() as db:
            if defect == 'claim':
                store.state.claim_work(db, store.spec('run-1')['work_id'],
                                       'external:devflow:run-1', 'unexpected reacquisition')
            elif defect == 'attempt':
                db.execute("UPDATE delivery_attempts SET result_json='{}'")
            else:
                records = [dict(row) for row in db.execute(
                    "SELECT sequence,payload_json FROM delivery_events "
                    "WHERE type='resource_cleanup_reconciled'")]
                anchor = next(row for row in records
                              if json.loads(row['payload_json']).get('closure') == 'unpublished')
                payload = json.loads(anchor['payload_json'])
                if defect == 'anchor_key':
                    payload['key'] = 'f' * 64
                else:
                    payload['closed_workflow']['execution_run_id'] = 'foreign-execution'
                    payload['key'] = digest({k: v for k, v in payload.items() if k != 'key'})
                db.execute('UPDATE delivery_events SET payload_json=? WHERE sequence=?',
                           (canonical_json(payload), anchor['sequence']))
    with pytest.raises(ValueError):
        store.repair_admission_preflight('run-1', command)
    with store._connect() as db:
        assert db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0] is None
        assert (store.state.claim_for(db, store.spec('run-1')['work_id']) is None) is (
            defect != 'claim')


def test_worker_dispatch_retains_history_and_runs_only_the_authorized_stage(
    closed_unpublished, monkeypatch,
):
    store, broker, state, _spec, _calls, _external, shape = closed_unpublished
    reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    store.continue_repair('run-1', command_for(
        state, broker.candidate(), unknown=shape == 'unknown'))
    execution = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
    flow = DeliveryWorkflow()
    observed = []
    async def confirmed(*_):
        return True
    async def activity(name, body, **_):
        assert name == 'delivery_tracker_start'
        return {'state': 'consistent'}
    async def nothing(*_):
        pass
    async def iterations(_spec, **kwargs):
        observed.append(kwargs)
        assert flow.state['roles'] == state['roles']
        return {'outcome': 'normal-gates'}
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', nothing)
    monkeypatch.setattr(flow, '_run_iterations', iterations)
    assert asyncio.run(flow._resume_stopped(execution, recovery)) == {'outcome': 'normal-gates'}
    assert observed[0]['start_iteration'] == int(shape == 'unknown')
    assert observed[0]['resume_prechecks'] is (shape == 'passed')
    assert observed[0]['prior_implementer_session'] == (
        None if shape == 'unknown' else 'original-implementation')
    assert observed[0]['allow_first_session'] is (shape == 'unknown')
    if shape == 'unknown':
        assert observed[0]['repair_findings'] == []


@pytest.mark.parametrize('closed_unpublished', ['unknown'], indirect=True)
def test_later_normal_resume_retains_only_authenticated_historical_teardown(closed_unpublished):
    store, broker, state, _spec, _calls, _external, _shape = closed_unpublished
    reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)
    first = store.continue_repair('run-1', command_for(state, broker.candidate(), unknown=True))
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
    original_unknown = copy.deepcopy(state['roles'][0])
    execution = store.effective_spec('run-1')
    (broker.checkout / 'README.md').write_text('A separately completed finite implementation\n')
    candidate = DeliveryBroker(store, execution).candidate()
    role = {'role': 'implement', 'iteration': 1, 'status': 'pass', 'finish_reason': 'done',
            'cleanup': 'confirmed', 'session_id': 'fresh-session', 'candidate': candidate}
    raw = {k: v for k, v in role.items() if k not in {'role', 'iteration', 'candidate'}}
    later = {**copy.deepcopy(state), 'revision': state['revision'] + 5, 'iteration': 1,
             'candidate': candidate, 'cleanup': 'confirmed', 'roles': [original_unknown, role]}
    later['checks'] = {'prepublish': {'state': 'failed'}, 'resource_cleanup': {
        'state': 'confirmed', 'resource_cleanup': 'confirmed',
        'process_cleanup': 'observed-native-confirmed'}}
    later['error'] = 'prepublication repair limit exhausted'
    closed = store._completed_temporal_result('run-1')
    closed.update(workflow_id=first['workflow_id'], execution_run_id='closed-new-turn',
                  result=later, recovery_digest=digest(recovery))
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET phase=?,outcome=?,execution_state=?,cleanup=?, '
                   'protocol_revision=?,iteration=?,candidate_json=?,checks_json=?,error=?',
                   ('blocked', 'blocked', 'blocked', 'confirmed', later['revision'], 1,
                    canonical_json(candidate), canonical_json(later['checks']), later['error']))
        db.execute('INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,'
                   "state,session_id,result_json,cleanup) VALUES ('fresh','run-1','implement',"
                   "1,?,'finished','fresh-session',?,'confirmed')",
                   (candidate['id'], canonical_json(raw)))
        store.state.release_work(db, execution['work_id'], 'external:devflow:run-1')
    command = {**command_for(later, candidate, unknown=True), 'command_id': 'second-resume'}
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 2
    store.continue_repair('run-1', command)
    with store._connect() as db:
        second = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
    assert second['session_id'] == 'fresh-session'
    assert 'quarantined_implementation' not in second
    assert len(second['historical_quarantines']) == 1
    assert second['state']['roles'][0] == original_unknown
    assert resume.readback(store, store.effective_spec('run-1'), second) == {'state': 'confirmed'}


def test_historical_teardown_does_not_suppress_another_unknown_role():
    from devflow_temporal.delivery_workflow import _cleanup_evidence_unknown

    old = {'role': 'implement', 'iteration': 0, 'cleanup': 'unknown',
           'finish_reason': 'recovery_unknown'}
    known = {digest(old)}
    assert _cleanup_evidence_unknown({'roles': [old], 'checks': {}}, True)
    assert not _cleanup_evidence_unknown({'roles': [old], 'checks': {}}, True, known)
    new = {**old, 'iteration': 1}
    assert _cleanup_evidence_unknown({'roles': [old, new], 'checks': {}}, True, known)
    assert old['cleanup'] == 'unknown' and old['finish_reason'] == 'recovery_unknown'
