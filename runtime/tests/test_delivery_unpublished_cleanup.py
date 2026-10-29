"""Public closed cleanup through finite same-run admission; external actors are opaque."""
from __future__ import annotations

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
from devflow_temporal.delivery_resources import write_private
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
        proof = {'kind': 'interrupted_monitor', 'version': 1, 'previous_phase': 'authorized',
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


@pytest.mark.asyncio
async def test_public_unpublished_cleanup_then_existing_finite_continuation(closed_unpublished):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, broker, state, spec, calls, _external, shape = closed_unpublished
    original = copy.deepcopy(state)
    before = broker.candidate()
    command = command_for(state, before, unknown=shape == 'unknown')
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        preview = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': command})
        refused = await client.post('/api/runs/run-1/continue-repair',
                                    json=command, headers=headers)
        assert preview.status_code == 200 and not preview.json()['eligible']
        assert {'stopped_cleanup_unconfirmed', 'stopped_owner_conflict'} <= {
            blocker['code'] for blocker in preview.json()['blockers']}
        assert refused.status_code == 409 and refused.json()['detail'] == preview.json()
    assert state == original and broker.candidate() == before
    assert calls == []
    assert reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)['changed'] is True
    assert state == original and broker.candidate() == before
    assert len(calls) == 2
    assert reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=0)['changed'] is False
    assert len(calls) == 2  # No duplicate tracker effect and no successor admission.
    assert retry.retry_once(store) == []  # The sealed terminal classification remains terminal.
    preflight = store.repair_admission_preflight('run-1', command)
    assert preflight['authorized_through_iteration'] == command['additional_iterations']
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id']) is None
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        preview = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': command})
        assert preview.status_code == 200 and preview.json()['eligible'], preview.text
        response = await client.post('/api/runs/run-1/continue-repair',
                                     json=command, headers=headers)
        assert response.status_code == 200, response.text
        replay = await client.post('/api/runs/run-1/continue-repair', json=command, headers=headers)
        assert replay.json() == response.json()
        replay_preview = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': command})
        assert replay_preview.json()['disposition'] == 'already_admitted'
        assert replay_preview.json()['blockers'][0]['code'] == 'already_admitted'
        replay_preflight = await client.post('/api/runs/run-1/repair-admission-preflight',
                                             json=command, headers=headers)
        assert replay_preflight.json()['workflow_id'] == response.json()['workflow_id']
        assert replay_preflight.json()['authorized_through_iteration'] == (
            response.json()['authorized_through_iteration'])
        conflicting = {**command, 'expected_revision': command['expected_revision'] + 1}
        preview_conflict = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': conflicting})
        conflict = await client.post('/api/runs/run-1/continue-repair', json=conflicting,
                                     headers=headers)
        assert preview_conflict.json()['blockers'][0]['code'] == 'command_conflict'
        assert conflict.status_code == 409 and conflict.json()['detail'] == preview_conflict.json()
        with store._connect() as db:
            saved = db.execute('SELECT response_json FROM delivery_commands WHERE command_id=?',
                               (command['command_id'],)).fetchone()[0]
            db.execute('UPDATE delivery_commands SET response_json=? WHERE command_id=?',
                       ('{}', command['command_id']))
        corrupt = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': command})
        rejected = await client.post('/api/runs/run-1/continue-repair', json=command,
                                     headers=headers)
        assert 'command_receipt_unknown' in {b['code'] for b in corrupt.json()['blockers']}
        assert rejected.status_code == 409 and rejected.json()['detail'] == corrupt.json()
        with store._connect() as db:
            db.execute('UPDATE delivery_commands SET response_json=? WHERE command_id=?',
                       (saved, command['command_id']))
        admitted = response.json()
    # Idempotent replay returns the first admission without another owner or debit.
    assert admitted['authorized_through_iteration'] == preflight['authorized_through_iteration']
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id'])['owner'] == 'external:devflow:run-1'
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
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
