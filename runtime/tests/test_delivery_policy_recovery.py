"""Public same-run recovery seals failures instead of forging implementation acceptance."""
from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_policy_recovery import amended_config, resume_preflight
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def preserved(native_configuration, monkeypatch):
    if sys.platform != 'darwin':
        pytest.skip('actual native macOS preparation and stopped-process evidence required')
    config, request = native_configuration
    config.raw['max_repairs'] = 2
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit({**request, 'accepted_plan':
                  'Change the owned README and verify the exact candidate'})
    spec = prepare_authority(store, store.spec(request['run_id']))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Substantive preserved candidate\n')
    candidate = broker.candidate()
    session = '01a1011c-47ae-73b3-b493-d89fb58be635'
    home = Path(spec['state_dir']) / 'role-homes' / 'implement' / 'codex' / 'sessions'
    home.mkdir(parents=True)
    (home / f'rollout-{session}.jsonl').write_text('controlled session transcript\n')
    roles = []
    with store._connect() as db:
        for iteration in range(3):
            role = {'role': 'implement', 'iteration': iteration, 'status': 'findings',
                    'summary': 'Frozen host check unavailable', 'findings': ['Historical failure'],
                    'cleanup': 'confirmed', 'session_id': session, 'candidate': candidate,
                    'finish_reason': 'done'}
            roles.append(role)
            db.execute(
                'INSERT INTO delivery_attempts '
                '(job_key,run_id,role,iteration,candidate_id,state,session_id,result_json,cleanup) '
                "VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                (f'role-{iteration}', spec['run_id'], 'implement', iteration,
                 candidate['id'], session, canonical_json(role)),
            )
        store.state.release_work(db, spec['work_id'], f"external:devflow:{spec['run_id']}")
    cleanup = RunResources(spec).finalize('blocked')
    state = {
        'run_id': spec['run_id'], 'phase': 'blocked', 'execution_state': 'blocked',
        'outcome': 'blocked', 'cleanup': 'none', 'revision': 13, 'iteration': 2,
        'candidate': candidate, 'candidate_revision': 3, 'pull_request': None,
        'roles': roles, 'checks': {'resource_cleanup': cleanup}, 'tracker': {},
        'usage': {}, 'findings': ['Historical host failure'], 'decision': None,
        'error': 'implementer did not establish a pass',
    }
    store.project(spec['run_id'], phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=candidate, checks=state['checks'],
                  protocol_revision=13, iteration=2, outcome='blocked', cleanup='none',
                  error=state['error'])
    closed = {'workflow_id': f"delivery-{spec['run_id']}", 'execution_run_id': 'closed-execution',
              'closed_at': '2026-10-03T10:00:00+00:00', 'result': state,
              'request_digest': spec['request_digest'], 'recovery_digest': None}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_a: None)
    monkeypatch.setattr('devflow_temporal.delivery_policy_recovery._issue', lambda original: {
        'issue': {'url': original['issue_url'], 'body': 'Original required observable outcome'},
        'sha256': digest('Original required observable outcome'),
    })
    path = config.state_root / 'trusted-local.json'
    raw = {**config.raw, 'execution_mode': 'trusted-local'}
    write_private(path, raw)
    payload = {'command_id': 'policy-recovery-1',
               'expected_precheck_sha256': store.policy_recovery_precheck(spec['run_id'])[
                   'precheck_sha256'], 'config_path': str(path),
               'config_sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
               'additional_iterations': 2}
    return store, spec, payload, state


def test_policy_recovery_preserves_candidate_session_failures_and_repeat_effects(preserved):
    store, original, payload, state = preserved
    run_id = original['run_id']
    before = store.submitted_spec(run_id)
    original_candidate = DeliveryBroker(store, original).candidate()
    original_receipt = Path(original['state_dir']) / 'resources' / 'finalization.json'
    original_bytes = original_receipt.read_bytes()
    response = store.recover_execution(run_id, payload)
    assert store.recover_execution(run_id, payload) == response
    assert response['authorized_through_iteration'] == 4
    assert store.submitted_spec(run_id) == before
    effective = store.effective_spec(run_id)
    assert effective['policy']['host_sandbox'] == 'trusted-local'
    assert effective['preparation']['fingerprint'] != original['preparation']['fingerprint']
    candidate = DeliveryBroker(store, effective).candidate()
    assert candidate['id'] == original_candidate['id']
    assert candidate['content_sha256'] == original_candidate['content_sha256']
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        recovery = json.loads(row['recovery_json'])
        assert recovery['state'] == state
        claim = store.state.claim_for(db, original['work_id'])
        assert claim['owner'] == f'external:devflow:{run_id}'
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 3
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 1
        assert db.execute('SELECT state FROM delivery_outbox').fetchone()[0] == 'pending'
    assert original_receipt.read_bytes() == original_bytes
    archived = Path(original['state_dir']) / 'policy-recovery' / 'predecessor' / 'finalization.json'
    assert archived.read_bytes() == original_bytes
    resume_preflight(store, effective, recovery)
    with pytest.raises(ValueError, match='already belongs'):
        store.recover_execution(run_id, {**payload, 'additional_iterations': 1})
    with pytest.raises(ValueError, match='one policy recovery'):
        store.recover_execution(run_id, {**payload, 'command_id': 'second-grant'})
    detail = store.detail(run_id)
    assert detail['execution_policy_recovery']['session_id'] == recovery['session_id']
    assert detail['execution_policy_recovery']['preserved_checks'] == state['checks']


def test_public_policy_provenance_survives_terminal_only_wrapper(preserved):
    store, original, payload, _state = preserved
    run_id = original['run_id']
    store.recover_execution(run_id, payload)
    before = store.detail(run_id)['execution_policy_recovery']
    effective = store.effective_spec(run_id)
    with store._connect() as db:
        row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                         (run_id,)).fetchone()
        original_recovery = json.loads(row[0])
        # Projection-only fixture. Real authenticated terminal admission and
        # dispatch are exercised through HTTP/Temporal in the terminal tests.
        wrapper = {'kind': 'terminal_tracker_recovery',
                   'original_recovery': original_recovery, 'command_id': 'terminal-only',
                   'spec_sha256': digest(effective), 'seal': {'cleanup': 'retained-seal'},
                   'closed': {'workflow_id': 'original-policy-workflow',
                              'execution_run_id': 'closed-policy-execution',
                              'status': 'TIMED_OUT', 'history_sha256': 'sealed-history'}}
        db.execute('UPDATE delivery_runs SET recovery_json=? WHERE run_id=?',
                   (canonical_json(wrapper), run_id))
    detail = store.detail(run_id)
    assert store.effective_spec(run_id) == effective
    assert detail['execution_policy_recovery'] == before
    assert detail['terminal_tracker_recovery']['closed_history_sha256'] == 'sealed-history'
    assert detail['terminal_tracker_recovery']['seal'] == wrapper['seal']
    assert store.list_runs()[0]['execution_retired'] is False
    amendment = {'kind': 'scope_amendment', 'added_paths': ['owning.py']}
    assert store._scope_recovery({'kind': 'terminal_tracker_recovery',
                                  'original_recovery': amendment}) == amendment


@pytest.mark.parametrize('field', ['issue', 'repository'])
def test_policy_recovery_rejects_supported_reassignment_before_claim_or_probe(preserved, field):
    store, original, payload, _state = preserved
    receipt = Path(original['state_dir']) / 'resources/finalization.json'
    before = receipt.read_bytes()
    replacement = ('https://github.com/example/fixture/issues/999' if field == 'issue'
                   else 'github.com/example/replacement')
    with store._connect() as db:
        store.state.update(db, 'work', {'id': original['work_id'], field: replacement}, None)
    with pytest.raises(ValueError, match='frozen authority'):
        store.policy_recovery_precheck(original['run_id'])
    with pytest.raises(ValueError, match='frozen authority'):
        store.recover_execution(original['run_id'], payload)
    with store._connect() as db:
        assert store.state.claim_for(db, original['work_id']) is None
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 0
    assert receipt.read_bytes() == before
    assert not (Path(original['state_dir']) / 'policy-recovery/intent.json').exists()


def test_policy_atomic_grant_rechecks_reassignment_after_local_preparation(preserved, monkeypatch):
    import devflow_temporal.delivery_policy_recovery as module

    store, original, payload, _state = preserved
    prepare = module._prepare

    def reassigned(*args):
        result = prepare(*args)
        with store._connect() as db:
            store.state.update(db, 'work', {'id': original['work_id'],
                'issue': 'https://github.com/example/fixture/issues/999'}, None)
        return result

    monkeypatch.setattr(module, '_prepare', reassigned)
    with pytest.raises(ValueError, match='frozen authority'):
        store.recover_execution(original['run_id'], payload)
    with store._connect() as db:
        assert store.state.claim_for(db, original['work_id']) is None
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 0


def test_policy_resume_and_tracker_start_refuse_a_reclaimed_reassigned_issue(
    preserved, monkeypatch,
):
    from devflow_temporal.delivery_activities import _tracker_sync

    store, original, payload, _state = preserved
    store.recover_execution(original['run_id'], payload)
    effective = store.effective_spec(original['run_id'])
    with store._connect() as db:
        row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                         (original['run_id'],)).fetchone()
        recovery = json.loads(row[0])
        owner = f"external:devflow:{original['run_id']}"
        store.state.release_work(db, original['work_id'], owner)
        store.state.update(db, 'work', {'id': original['work_id'],
            'issue': 'https://github.com/example/fixture/issues/999'}, None)
        store.state.claim_work(db, original['work_id'], owner)
    with pytest.raises(ValueError, match='frozen authority'):
        resume_preflight(store, effective, recovery)
    monkeypatch.setattr('devflow_temporal.delivery_activities._context',
                        lambda _spec: (store, None))
    calls = []
    monkeypatch.setattr('devflow_temporal.delivery_activities.subprocess.run',
                        lambda *args, **_kwargs: calls.append(args))
    with pytest.raises(ValueError, match='frozen authority'):
        _tracker_sync(effective, 'in-progress', release=False)
    assert calls == []


@pytest.mark.asyncio
async def test_uncertain_policy_workflow_start_reads_memo_without_duplicate_start(preserved,
                                                                                monkeypatch):
    store, original, payload, _ = preserved
    queued = store.recover_execution(original['run_id'], payload)
    service = object.__new__(DeliveryService)
    service.store, service.config = store, store.config

    class Temporal:
        starts = 0
        memo = None

        def get_workflow_handle(self, workflow_id):
            assert workflow_id == queued['workflow_id']
            return self

        async def describe(self):
            if self.memo is None:
                raise RPCError('not found', RPCStatusCode.NOT_FOUND, b'')
            return self

        async def memo_value(self, key, default):
            return self.memo.get(key, default)

        async def start_workflow(self, _workflow, **kwargs):
            self.starts += 1
            assert kwargs['args'][1]['kind'] == 'execution_policy_recovery'
            self.memo = kwargs['memo']
            raise TimeoutError('remote accepted, acknowledgement unavailable')

    temporal = Temporal()

    async def healthy():
        return temporal

    monkeypatch.setattr(service, 'healthy_client', healthy)
    with pytest.raises(TimeoutError):
        await service.dispatch_once()
    assert store.recover_execution(original['run_id'], payload) == queued
    await service.dispatch_once()
    assert temporal.starts == 1
    with store._connect() as db:
        assert db.execute('SELECT state FROM delivery_outbox').fetchone()[0] == 'sent'
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 3


@pytest.mark.parametrize('change', [
    'candidate', 'claim', 'unknown-effect', 'live-process', 'config',
])
def test_policy_recovery_refuses_changed_or_uncertain_authority_before_grant(preserved, change,
                                                                          monkeypatch):
    store, original, payload, _ = preserved
    run_id = original['run_id']
    if change == 'candidate':
        Path(original['checkout'], 'README.md').write_text('Other candidate')
    elif change == 'claim':
        with store._connect() as db:
            store.state.claim_work(db, original['work_id'], 'other-owner')
    elif change == 'unknown-effect':
        DeliveryBroker(store, original)._effect('uncertain', 'publish', {'unknown': True})
    elif change == 'live-process':
        manifest = read_private(RunResources(original).manifest)
        journal = read_private(Path(manifest['processes'][0]))
        pid, item = next(iter(journal['owned'].items()))
        monkeypatch.setattr('devflow_temporal.delivery_policy_recovery.process_table', lambda: {
            int(pid): {**item, 'stat': 'S'},
        })
    else:
        config = json.loads(Path(payload['config_path']).read_bytes())
        config['roles']['implement']['effort'] = 'low'
        write_private(Path(payload['config_path']), config)
        payload['config_sha256'] = hashlib.sha256(
            Path(payload['config_path']).read_bytes(),
        ).hexdigest()
    with pytest.raises(ValueError):
        store.recover_execution(run_id, payload)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 0
        assert db.execute('SELECT phase FROM delivery_runs').fetchone()[0] == 'blocked'


@pytest.mark.asyncio
async def test_policy_recovery_runs_gates_before_any_implementation():
    controller = DeliveryWorkflow()
    previous = {'run_id': 'same-run', 'iteration': 2, 'revision': 13, 'outcome': 'blocked',
                'checks': {}, 'candidate': {'id': 'preserved'}}
    spec = {'run_id': 'same-run'}
    recovery = {'effective_spec': spec, 'state': previous, 'start_iteration': 3,
                'maximum_iteration': 4, 'candidate': previous['candidate'],
                'session_id': 'same-session', 'issue_evidence': {'original': True}}
    calls = []

    async def project(*_args):
        pass

    async def preflight(*_args):
        calls.append('preflight')
        return True

    async def activity(name, *_args):
        assert name == 'delivery_tracker_start'
        return {'state': 'consistent'}

    async def iterations(_spec, **kwargs):
        assert kwargs['resume_prechecks'] is True
        assert kwargs['prior_implementer_session'] == 'same-session'
        assert kwargs['repair_findings'] == []
        assert kwargs['authorized_max_iteration'] == 4
        calls.append('gates-first')
        return {'observed': True}

    controller._project = project
    controller._confirm_repair_preflight = preflight
    controller._activity = activity
    controller._run_iterations = iterations
    assert await controller._resume_policy(spec, recovery) == {'observed': True}
    assert calls == ['preflight', 'preflight', 'gates-first']


def test_policy_config_change_is_only_explicit_trust_mode(preserved):
    store, original, payload, _ = preserved
    assert amended_config(original, Path(payload['config_path']), payload['config_sha256']).raw[
        'execution_mode'] == 'trusted-local'
    config = json.loads(Path(payload['config_path']).read_bytes())
    config['max_repairs'] = 3
    write_private(Path(payload['config_path']), config)
    with pytest.raises(ValueError, match='only execution_mode'):
        amended_config(original, Path(payload['config_path']),
                       hashlib.sha256(Path(payload['config_path']).read_bytes()).hexdigest())


@pytest.mark.parametrize('failure', ['prepare', 'remote'])
def test_policy_preparation_failure_preserves_predecessor_and_same_command_retry(
    preserved, monkeypatch, failure,
):
    from devflow_temporal import delivery_native_preparation, delivery_policy_recovery

    store, original, payload, _ = preserved
    resources = RunResources(original)
    before = {name: (resources.root / name).read_bytes()
              for name in ('manifest.json', 'finalization.json')}
    measure, remote = delivery_native_preparation._measure, delivery_policy_recovery._remote
    calls = {'prepare': 0, 'remote': 0}

    def measured(*args, **kwargs):
        calls['prepare'] += 1
        proof = measure(*args, **kwargs)
        if failure == 'prepare' and calls['prepare'] == 1:
            raise RuntimeError('interrupted before trusted proof persistence')
        return proof

    def readback(broker):
        calls['remote'] += 1
        if failure == 'remote' and calls['remote'] == 2:
            raise RuntimeError('post-preparation GitHub readback temporarily unavailable')
        return remote(broker)

    monkeypatch.setattr(delivery_native_preparation, '_measure', measured)
    monkeypatch.setattr(delivery_policy_recovery, '_remote', readback)
    with pytest.raises(RuntimeError):
        store.recover_execution(original['run_id'], payload)
    intent_path = Path(original['state_dir']) / 'policy-recovery' / 'intent.json'
    intent = read_private(intent_path)
    assert intent['command_id'] == payload['command_id']
    assert intent['last_error']
    assert intent['preparation_attempts'][0]['cleanup']['state'] == 'confirmed'
    assert all((resources.root / name).read_bytes() == value for name, value in before.items())
    assert not (Path(original['state_dir']) / 'transient').exists()
    assert 'execution-policy-recovery-intent' in {
        item['id'] for item in store.evidence_index(original['run_id'])}
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 0
        assert store.state.claim_for(db, original['work_id']) is None
    queued = store.recover_execution(original['run_id'], payload)
    assert store.recover_execution(original['run_id'], payload) == queued
    assert calls['prepare'] == (2 if failure == 'prepare' else 1)
    assert all((resources.root / name).read_bytes() == value for name, value in before.items())


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_at_tracker", [False, True])
async def test_new_failed_gate_is_required_before_one_same_session_repair(cancel_at_tracker):
    controller = DeliveryWorkflow()
    candidate = {'id': 'preserved', 'head': 'a' * 40}
    controller.state = {'run_id': 'same-run', 'candidate': candidate, 'candidate_revision': 3,
                        'roles': [], 'checks': {}, 'revision': 13, 'iteration': 2,
                        'usage': {}, 'findings': [], 'pull_request': None, 'cleanup': 'none'}
    spec = {'run_id': 'same-run', 'provider': 'codex', 'terminal_tracker_version': 1,
            'policy': {'max_repairs': 2, 'host_sandbox': 'trusted-local'}}
    observed = []

    async def project(_spec, event, *_args):
        if cancel_at_tracker and event == "tracker_started":
            controller.cancel_requested = True

    async def activity(name, request, **_kwargs):
        iteration = request.get('iteration')
        observed.append((name, iteration))
        if name == 'delivery_precheck':
            return {'state': 'failed' if iteration == 3 else 'passed',
                    'candidate_id': candidate['id'], 'source_unchanged': True, 'results': []}
        if name == 'delivery_role':
            if request['role'] == 'implement':
                assert request['resume_session'] == 'original-session'
                assert request['iteration'] == 4
                assert request['findings']
            return {'status': 'pass', 'candidate': candidate,
                    'session_id': 'original-session' if request['role'] == 'implement'
                    else request['role'] + '-independent'}
        if name == 'delivery_publish':
            return {'state': 'OPEN', 'candidate': candidate, 'head': candidate['head'], 'number': 7}
        if name in {'delivery_checks', 'delivery_ci'}:
            return {'state': 'passed', 'candidate_id': candidate['id'], 'results': []}
        raise AssertionError(name)

    async def published(_spec, _iteration, _candidate, initial, **_kwargs):
        return initial

    controller._project = project
    controller._activity = activity
    controller._published_result = published
    result = await controller._run_iterations(
        spec, start_iteration=3, prior_implementer_session='original-session', repair_findings=[],
        continuation=None, recovery=None, authorized_max_iteration=4, resume_prechecks=True,
    )
    assert result['outcome'] == ('cancelled' if cancel_at_tracker else 'delivered')
    assert observed[:3] == [
        ('delivery_precheck', 3), ('delivery_role', 4), ('delivery_precheck', 4),
    ]
    assert sum(name == 'delivery_role' and iteration == 4 for name, iteration in observed) == 3
