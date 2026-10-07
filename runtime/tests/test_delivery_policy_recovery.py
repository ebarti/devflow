"""Previously admitted policy rows and queued execution retain their recorded contract."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import submit_historical_admission

from devflow_temporal import delivery_policy_recovery
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_continuation import copy_session_state, session_state_digest
from devflow_temporal.delivery_policy_recovery import (
    _prepare,
    _remote,
    _rows,
    amended_config,
    resume_preflight,
    work_binding,
)
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.parametrize('observation,refused', [
    ('live-pid', True), ('listening-port', True), ('unfinished', True),
    ('unmonitored', True), ('stopped', False), ('reused-pid', False), ('zombie', False),
])
def test_retained_stopped_cleanup_observes_without_state_effects(
    intake_fixture, monkeypatch, observation, refused,
):
    path, request = intake_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    spec = store.spec(request['run_id'])
    resources = RunResources(spec)
    journal_path = Path(spec['state_dir']) / 'attempts' / 'recorded' / 'native-process.json'
    recorded_identity, pid, port = 'recorded-start-identity', 424242, 18777
    journal = {
        'phase': 'starting' if observation == 'unfinished' else 'finished',
        'monitoring_complete': observation != 'unmonitored',
        'owned': {str(pid): {'identity': recorded_identity}}, 'ports': [port],
    }
    write_private(journal_path, journal)
    with resources.locked() as manifest:
        manifest['processes'].append(str(journal_path))
        write_private(resources.manifest, manifest)
    write_private(resources.root / 'finalization.json', {
        'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
        'resource_cleanup': 'confirmed',
        'roots': [{'path': str(Path(spec['state_dir']) / 'transient'), 'state': 'removed'}],
    })
    table = {}
    if observation in {'live-pid', 'reused-pid', 'zombie'}:
        table[pid] = {
            'identity': 'different-start-identity' if observation == 'reused-pid'
            else recorded_identity,
            'stat': 'Z' if observation == 'zombie' else 'S',
        }
    observed = []

    def processes():
        observed.append('process-table')
        return table

    def listening(recorded_port):
        assert recorded_port == port
        observed.append('recorded-port')
        return {pid} if observation == 'listening-port' else set()

    monkeypatch.setattr(delivery_policy_recovery, 'process_table', processes)
    monkeypatch.setattr(delivery_policy_recovery, 'listeners', listening)
    with store._connect() as db:
        before_db = list(db.iterdump())
    before_files = {item.relative_to(store.config.state_root):
                    (item.read_bytes(), item.stat().st_mode)
                    for item in store.config.state_root.rglob('*') if item.is_file()}
    if refused:
        with pytest.raises(ValueError, match='process identity or port is still live or unknown'):
            delivery_policy_recovery._stopped_cleanup(spec)
    else:
        result = delivery_policy_recovery._stopped_cleanup(spec)
        assert result['journal_sha256'] == {
            str(journal_path): hashlib.sha256(journal_path.read_bytes()).hexdigest(),
        }
    assert observed[0] == 'process-table'
    if observation == 'listening-port':
        assert 'recorded-port' in observed
    with store._connect() as db:
        assert list(db.iterdump()) == before_db
    assert before_files == {item.relative_to(store.config.state_root):
                            (item.read_bytes(), item.stat().st_mode)
                            for item in store.config.state_root.rglob('*') if item.is_file()}


@pytest.fixture
def preserved(native_configuration, monkeypatch):
    if sys.platform != 'darwin':
        pytest.skip('actual native macOS preparation and stopped-process evidence required')
    config, request = native_configuration
    config.raw['max_repairs'] = 2
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    submit_historical_admission(
        store, {**request, 'accepted_plan':
                'Change the owned README and verify the exact candidate'}, monkeypatch)
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
                    'cleanup': 'confirmed', 'session_id': session, 'finish_reason': 'done'}
            roles.append({**role, 'role': 'implement', 'iteration': iteration,
                          'input_candidate_id': candidate['id'], 'candidate': candidate})
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
    # Historical row fixture: seed the existing schema rather than invoke a
    # retired admission. Native preparation and resume validators stay real.
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_a: None)
    path = config.state_root / 'trusted-local.json'
    write_private(path, {**config.raw, 'execution_mode': 'trusted-local'})
    config_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    payload = {'config_path': str(path), 'config_sha256': config_hash}
    intent_path = Path(spec['state_dir']) / 'policy-recovery' / 'intent.json'
    intent = {'preparation_attempts': [], 'effective_spec': None}
    predecessor = Path(spec['state_dir']) / 'policy-recovery' / 'predecessor'
    for name in ('manifest.json', 'finalization.json'):
        write_private(predecessor / name, read_private(RunResources(spec).root / name))
    effective = _prepare(spec, amended_config(spec, path, config_hash), intent, intent_path)
    copy_session_state(home.parent.parent, home.parent.parent.with_name('implement-policy-1'),
                       session, session_state_digest(home.parent.parent, session))
    row, attempts, effects, _claim = _rows(store, spec['run_id'])
    effective_candidate = DeliveryBroker(store, effective).candidate()
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    seal = {'precheck_sha256': 'a' * 64, 'work_binding': binding,
            'attempts_digest': digest(attempts), 'effects_digest': digest(effects),
            'remote_digest': digest(_remote(DeliveryBroker(store, effective)))}
    recovery = {'kind': 'execution_policy_recovery', 'state': state, 'seal': seal,
                'effective_spec': effective, 'candidate': effective_candidate,
                'session_id': session,
                'config_sha256': config_hash, 'original_spec_digest': digest(spec),
                'maximum_iteration': 4, 'start_iteration': 3,
                'issue_evidence': {'issue': {'url': spec['issue_url']}},
                'predecessor_workflow_id': row['workflow_id'],
                'predecessor_execution_run_id': 'closed-execution'}
    workflow_id = f"delivery-{spec['run_id']}-execution-policy-1"
    with store._connect() as db:
        db.execute('INSERT INTO delivery_policy_recoveries VALUES (?,?,?,?,?)',
                   (spec['run_id'], 'historical-policy-grant', digest(spec), digest(recovery), 4))
        store.state.claim_work(db, spec['work_id'], f"external:devflow:{spec['run_id']}")
        db.execute("UPDATE delivery_runs SET phase='execution_policy_recovery_queued',"
                   "execution_state='queued',outcome=NULL,error=NULL,workflow_id=?,recovery_json=? "
                   'WHERE run_id=?', (workflow_id, canonical_json(recovery), spec['run_id']))
    return store, spec, payload, state



def test_native_raw_results_are_preserved_without_a_candidate_field(preserved):
    store, original, _payload, state = preserved
    run_id = original['run_id']
    with store._connect() as db:
        before = [dict(row) for row in db.execute(
            'SELECT * FROM delivery_attempts ORDER BY job_key')]
    assert all('candidate' not in json.loads(a['result_json']) for a in before)
    observed = store.detail(run_id)['candidate']
    assert {key: observed[key] for key in state['candidate']} == state['candidate']
    with store._connect() as db:
        assert before == [dict(row) for row in db.execute(
            'SELECT * FROM delivery_attempts ORDER BY job_key')]
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 1


def test_historical_policy_row_preserves_candidate_session_failures_and_cleanup(preserved):
    store, original, _payload, state = preserved
    run_id = original['run_id']
    before = store.submitted_spec(run_id)
    original_candidate = DeliveryBroker(store, original).candidate()
    original_receipt = Path(original['state_dir']) / 'resources' / 'finalization.json'
    original_bytes = original_receipt.read_bytes()
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
    detail = store.detail(run_id)
    assert detail['execution_policy_recovery']['session_id'] == recovery['session_id']
    assert detail['execution_policy_recovery']['preserved_checks'] == state['checks']
    assert store.submitted_spec(run_id) == before


def test_public_policy_provenance_survives_terminal_only_wrapper(preserved):
    store, original, _payload, _state = preserved
    run_id = original['run_id']
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


def test_policy_resume_and_tracker_start_refuse_a_reclaimed_reassigned_issue(
    preserved, monkeypatch,
):
    from devflow_temporal.delivery_activities import _tracker_sync

    store, original, _payload, _state = preserved
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
    store, original, _payload, _ = preserved
    queued = store.pending_starts()[0]
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
    await service.dispatch_once()
    assert temporal.starts == 1
    with store._connect() as db:
        assert db.execute('SELECT state FROM delivery_outbox').fetchone()[0] == 'sent'
        assert db.execute('SELECT COUNT(*) FROM delivery_policy_recoveries').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 3


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
    _store, original, payload, _ = preserved
    assert amended_config(original, Path(payload['config_path']), payload['config_sha256']).raw[
        'execution_mode'] == 'trusted-local'
    config = json.loads(Path(payload['config_path']).read_bytes())
    config['max_repairs'] = 3
    write_private(Path(payload['config_path']), config)
    with pytest.raises(ValueError, match='only execution_mode'):
        amended_config(original, Path(payload['config_path']),
                       hashlib.sha256(Path(payload['config_path']).read_bytes()).hexdigest())


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_at_tracker", [False, True])
async def test_new_failed_gate_is_required_before_one_same_session_repair(
    cancel_at_tracker, monkeypatch,
):
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _: True)
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
