"""Private controller proof boundaries; actual archived native proof is qualified separately."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
from contextlib import closing
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_feature_budget_resume import _worker
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_gate_attribution_recovery as attribution
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_check_evidence import retain_artifacts
from devflow_temporal.delivery_resources import private_directory, write_private
from devflow_temporal.delivery_stopped_resume import namespace


@pytest.fixture
def subject(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    subprocess.run(['git', 'init', '-q', str(source)], check=True)
    for key, value in [('user.name', 'Fixture'), ('user.email', 'fixture@example.test')]:
        subprocess.run(['git', '-C', str(source), 'config', key, value], check=True)
    package = source / 'apps/api'
    (package / 'test').mkdir(parents=True)
    (package / 'package.json').write_text('{"name":"@fixture/api"}')
    (package / 'test/smoke.test.ts').write_text('export const fixture = true\n')
    subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(source), '-c', 'commit.gpgsign=false',
                    'commit', '-qm', 'Fixture base'], check=True)
    head = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'],
                                   text=True).strip()
    recipe = {'id': 'api', 'argv': ['node', '--input-type=module', '-e',
        "spawnSync('corepack',['pnpm','--filter','@fixture/api','exec','vitest','run']);"],
        'cwd': '.', 'required_selectors': ['apps/api/test/smoke.test.ts'],
        'selector_evidence_version': 1, 'selector_project': '.', 'junit_required': True}
    state_dir = tmp_path / 'runs/worker'
    private_directory(state_dir)
    policy = {'checks': [recipe], 'max_repairs': 10}
    spec = {'run_id': 'worker', 'state_dir': str(state_dir), 'source_path': str(source),
            'checkout': str(source), 'base_sha': head, 'provider': 'codex', 'policy': policy,
            'policy_digest': digest(policy), 'feature_worker': {'kind': 'chunk'}}
    candidate = {'id': 'a' * 64, 'head': head, 'base_sha': head,
                 'content_sha256': 'b' * 64, 'environment_digest': 'c' * 64,
                 'policy_digest': spec['policy_digest']}
    session, job = 'original-session', '7' * 64
    native = {'state': 'finished', 'exit_code': 0, 'monitoring_complete': True,
              'cleanup': 'observed-native-confirmed', 'stdio_drained': True,
              'timed_out': False, 'cancelled': False,
              'runtime_identity': {'revision': 'old', 'runtime_payload_sha256': 'd' * 64}}
    folder = state_dir / 'checks/6/api'
    private_directory(folder / 'pytest-artifacts')
    (folder / 'pytest-artifacts/junit.xml').write_text(
        '<testsuites><testsuite><testcase classname="test/smoke.test.ts" name="works"/>'
        '</testsuite></testsuites>')
    native['journal'] = str(folder / 'native-process.json')
    journal = {'phase': 'finished', 'monitoring_complete': True, 'result': native,
               'runtime_identity': native['runtime_identity'], 'intent': {
                   'run_id': 'worker', 'policy_digest': spec['policy_digest'],
                   'cwd': str(source), 'argv': recipe['argv']}}
    write_private(Path(native['journal']), journal)
    failed = {'id': 'api', 'passed': False, 'exit_code': 0, 'rejected_output': False,
              'cleanup': 'confirmed', 'argv': recipe['argv'], 'cwd': str(source),
              'native_process': native, 'artifacts': retain_artifacts(folder, candidate),
              'junit': {'tests': 1, 'passed': 1, 'failures': 0, 'errors': 0, 'skipped': 0},
              'evidence_failure': 'required selector contributed no complete passing evidence: '
                                  'apps/api/test/smoke.test.ts'}
    checks = {'prepublish': {'state': 'passed', 'candidate_id': candidate['id'],
                             'source_unchanged': True,
                             'results': [{'passed': True, 'cleanup': 'confirmed'}]},
              'local': {'state': 'failed', 'candidate_id': candidate['id'],
                        'source_unchanged': True, 'results': [failed]}}
    request = {'spec': spec, 'role': 'implement', 'iteration': 7, 'candidate': candidate,
               'evidence_context': {'candidate': candidate, 'iteration': 6, 'checks': checks}}
    write_private(state_dir / 'attempts' / job / 'request.json', request)
    roles = [{'role': 'implement', 'iteration': i, 'status': status, 'candidate': candidate,
              'cleanup': 'confirmed', 'session_id': session, 'finish_reason': 'done',
              'findings': [] if status == 'pass' else ['Original assessment stays unpassed.']}
             for i, status in [(6, 'pass'), (7, 'blocked')]]
    attempts = [{'job_key': str(role['iteration']) * 64, 'role': 'implement',
                 'iteration': role['iteration'], 'state': 'finished', 'cleanup': 'confirmed',
                 'session_id': session, 'result_json': json.dumps({k: v for k, v in role.items()
                     if k not in {'role', 'iteration', 'candidate'}})} for role in roles]
    state = {'error': 'implementer did not establish a pass', 'iteration': 7, 'roles': roles}
    gate = {'kind': 'published_check_prelaunch_retry', 'execution_spec': {
        **spec, 'gate_retry_generation': 1, 'gate_retry_stage': 'published'}}
    write_private(state_dir / 'published-gates-admission/admission.json', gate)
    command = {'command_id': 'prior-worker', 'additional_iterations': 1}
    predecessor = {'run_id': 'worker', 'iteration': 6, 'roles': []}
    transport = {'kind': 'stopped_delivery_resume', 'execution_spec': spec,
                 'execution_candidate': candidate, 'state': predecessor, 'session_id': session,
                 'command': command, 'command_digest': digest({'run_id': 'worker', **command}),
                 'maximum_iteration': 7, 'predecessor_result_digest': digest(predecessor),
                 'closed': {'closed_at': '2026-10-10T10:00:00+00:00'},
                 'original_recovery': gate}
    write_private(state_dir / namespace(transport) / 'admission.json', transport)
    database = tmp_path / 'workflow.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE delivery_gate_admissions(run_id,recovery_json)')
        db.execute('CREATE TABLE delivery_commands(command_id,run_id,request_digest,response_json)')
        db.execute('INSERT INTO delivery_gate_admissions VALUES(?,?)', ('worker', json.dumps(gate)))
        db.execute('INSERT INTO delivery_commands VALUES(?,?,?,?)',
                   (command['command_id'], 'worker', transport['command_digest'],
                    json.dumps({'admission_sha256': digest(transport)})))
    store = SimpleNamespace(_connect=lambda: closing(sqlite3.connect(database)))
    # Cryptographic native progress is a declared unit seam. The archive-only
    # qualification uses the real primitive and actual original native bytes.
    monkeypatch.setattr(attribution, '_progress', lambda *_: {'native_progress_sha256': 'e' * 64})
    monkeypatch.setattr(attribution, '_native_role', lambda *_: {'request_sha256': digest(request)})
    monkeypatch.setattr(attribution, '_runtime', lambda x: deepcopy(x))
    return SimpleNamespace(store=store, spec=spec, row={}, state=state, attempts=attempts,
        closed={'closed_at': '2026-10-10T12:00:00+00:00'}, transport=transport,
        broker=SimpleNamespace(candidate=lambda: candidate, checkout=source),
        request=request, request_path=state_dir / 'attempts' / job / 'request.json',
        failed=failed, roles=roles, database=database, gate=gate)


def observed(subject):
    return attribution.observe(subject.store, subject.spec, subject.row, subject.state,
                               subject.attempts, subject.closed, subject.transport, subject.broker)


def test_same_tree_attribution_proof_keeps_native_rejection_and_bounded_gate_custody(subject):
    before = deepcopy(subject.state)
    proof = observed(subject)
    assert proof['unpassed_status'] == 'blocked'
    assert proof['unresolved_findings'] == subject.roles[-1]['findings']
    assert proof['implementation_evidence']['implementation_job_key'] == '6' * 64
    assert proof['corrected_selector_evidence'][0]['passed'] == 1
    assert subject.state == before and observed(subject) == proof
    with sqlite3.connect(subject.database) as db:
        db.execute('DELETE FROM delivery_commands')
    with pytest.raises(ValueError, match='immutable command authority'):
        observed(subject)


def test_real_defect_changed_report_source_or_native_proof_never_grants_attribution(subject,
                                                                                 monkeypatch):
    source = subject.broker.checkout / 'apps/api/test/smoke.test.ts'
    original = subject.failed['evidence_failure']
    for change in ['exit', 'counts', 'reason', 'source']:
        subject.failed['exit_code'] = 1 if change == 'exit' else 0
        subject.failed['junit']['failures'] = 1 if change == 'counts' else 0
        subject.failed['evidence_failure'] = (
            'real product failure' if change == 'reason' else original)
        if change == 'source':
            source.unlink()
        write_private(subject.request_path, subject.request)
        with pytest.raises(ValueError):
            observed(subject)
        if change == 'source':
            source.write_text('export const fixture = true\n')
    monkeypatch.setattr(attribution, '_progress', lambda *_: (_ for _ in ()).throw(
        ValueError('actual native proof is missing')))
    with pytest.raises(ValueError, match='actual native proof is missing'):
        observed(subject)


def test_public_parent_continuation_at_ten_keeps_one_zero_writer_published_admission(
    service, monkeypatch,
):
    from devflow_temporal import delivery_feature_activities as activities
    from devflow_temporal import delivery_gate_retry as gates
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_feature_execution import continue_feature, worker_key
    from devflow_temporal.delivery_policy_recovery import _rows, work_binding
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    store, parent, child, shared, token = _worker(
        service, monkeypatch, used_by_others=10, kind='chunk')
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Preserved published feature\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fix: fixture')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + child['branch'])
    candidate = broker.candidate()
    publication = {'number': 7, 'head': candidate['head'], 'candidate': candidate,
                   'url': 'https://github.com/example/fixture/pull/7', 'state': 'OPEN'}
    roles = [{'role': 'implement', 'iteration': i, 'status': status,
              'candidate': candidate, 'cleanup': 'confirmed', 'session_id': 'original-session',
              'findings': [] if status == 'pass' else ['Controller verdict needs reconciliation.']}
             for i, status in [(6, 'pass'), (7, 'blocked')]]
    state = {'run_id': child['run_id'], 'revision': 73, 'iteration': 7, 'phase': 'blocked',
             'outcome': 'blocked', 'execution_state': 'blocked', 'cleanup': 'confirmed',
             'error': 'implementer did not establish a pass', 'candidate': candidate,
             'candidate_revision': 11, 'roles': roles, 'checks': {}, 'findings': [],
             'usage': {}, 'pull_request': publication}
    store.project(child['run_id'], phase='blocked', outcome='blocked', cleanup='confirmed',
                  execution_state='blocked', event_type='blocked', message=state['error'],
                  error=state['error'], iteration=7, protocol_revision=73,
                  candidate=candidate, pull_request=publication)
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    shared.finish_worker(token, worker_key(child['run_id'], token), {'cleanup': 'confirmed'})
    shared.stop(token, 'stopped-fixture', {})
    store.project(parent['run_id'], phase='blocked', outcome='blocked', cleanup='confirmed',
                  execution_state='blocked', event_type='blocked', message='Child stop retained')
    with store._connect() as db:
        store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
        revision = db.execute('SELECT revision FROM delivery_runs WHERE run_id=?',
                              (parent['run_id'],)).fetchone()[0]
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: {'result': {}})
    command = {'command_id': 'continue-at-ceiling', 'expected_revision': revision}
    successor = continue_feature(store, parent['run_id'], command)
    assert continue_feature(store, parent['run_id'], command) == successor
    current_parent = store.effective_spec(successor['run_id'])
    assert current_parent['accepted_plan'] == parent['accepted_plan']
    assert current_parent['plan_approval'] == parent['plan_approval']
    assert current_parent['feature_delivery']['owner']['generation'] == token['generation'] + 1
    row, attempts, effects, _ = _rows(store, child['run_id'])
    old_gate = {'kind': gates.PRELAUNCH_KIND, 'fixture': 'sealed original published generation1'}
    with store._connect() as db:
        db.execute('INSERT INTO delivery_gate_admissions VALUES(?,?,?)',
                   (child['run_id'], 'old-published', json.dumps(old_gate)))
        binding = work_binding(store, child, db)
    # Only observation/transport metadata is supplied here. The actual original
    # native/report proof runs separately against the hash-authenticated archive.
    seal = {'row': gates._sealed_row(row), 'closed': {'result': state,
            'workflow_id': 'closed-worker', 'execution_run_id': 'original-closed'},
            'original_spec': child, 'candidate': candidate, 'publication': publication,
            'attempts': attempts, 'effects': effects, 'work_binding': binding,
            'previous_digest': None, 'prior_gate_digest': digest(old_gate),
            'stage': 'published', 'generation': 2, 'attribution_observation': {
                'unresolved_findings': roles[-1]['findings']}}
    monkeypatch.setattr(attribution, 'possible', lambda *_a: True)
    monkeypatch.setattr(gates, 'snapshot', lambda *_a, **_kw: deepcopy(seal))
    resumed = activities.resume_worker(store, current_parent, child, row)
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_gate_admissions '
                                        'WHERE run_id=?', (child['run_id'],)).fetchone()[0])
        saved_command = recovery['command']
        assert store.continue_repair(child['run_id'], saved_command)['workflow_id'] \
            == resumed['workflow_id']
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert saved_command['additional_iterations'] == 0
    assert saved_command['continuation_kind'] == gates.KIND
    assert recovery['state'] == state
    assert shared.budget(token['issue_id'])['used'] == 10
    flow = DeliveryWorkflow()
    calls = []

    async def activity(name, body, **_kw):
        calls.append((name, body))
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            assert 'Controller verdict needs reconciliation.' in str(body)
            return {'role': body['role'], 'status': 'pass', 'iteration': 7,
                    'session_id': 'fresh-' + body['role'], 'cleanup': 'confirmed',
                    'candidate': recovery['candidate']}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*_a, **_kw):
        pass

    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    result = asyncio.run(flow._resume_published_gates(resumed['spec'], recovery))
    assert result['outcome'] == 'delivered' and result['iteration'] == 7
    assert result['roles'][:2] == roles
    assert not any(name == 'delivery_publish' for name, _ in calls)
    assert [body['role'] for name, body in calls if name == 'delivery_role'] == ['review', 'verify']
    assert shared.budget(token['issue_id'])['used'] == 10


def test_terminal_cleanup_requires_independent_attribution_before_existing_reconciliation(
    subject, monkeypatch,
):
    from devflow_temporal import delivery_automatic_retry as retry

    failure = {'classification': 'terminal', 'stage': 'repair'}
    checks = {'failure': failure, 'resource_cleanup': {'state': 'unknown'}}
    row = {'checks_json': json.dumps(checks), 'outcome': 'blocked', 'phase': 'blocked',
           'execution_state': 'blocked', 'pr_json': '{"number":7}',
           'recovery_json': json.dumps(subject.transport), 'workflow_id': 'original',
           'request_digest': 'request'}
    closed = {'workflow_id': 'original', 'execution_run_id': 'closed',
              'closed_at': '2026-10-10T12:00:00+00:00', 'request_digest': 'request',
              'recovery_digest': digest(subject.transport), 'result': {
                  'run_id': 'worker', 'outcome': 'blocked', 'phase': 'blocked',
                  'execution_state': 'blocked', 'cleanup': 'unknown', 'checks': checks}}
    store = SimpleNamespace(_completed_temporal_result=lambda *_a, **_kw: closed)
    monkeypatch.setattr(attribution, 'possible', lambda *_a: True)
    order = []

    def observe(*_args):
        order.append('proof')
        return observed(subject)

    def close(*_args, **_kw):
        order.append('cleanup')
        return {'cleanup': 'confirmed'}

    monkeypatch.setattr(attribution, 'cleanup_observation', observe)
    monkeypatch.setattr(retry, '_close_cleanup', close)
    original = deepcopy(closed)
    assert retry._close_published_cleanup(store, row, subject.spec, lambda: False) \
        == {'cleanup': 'confirmed'}
    assert order == ['proof', 'cleanup'] and closed == original
    subject.failed['exit_code'] = 1
    write_private(subject.request_path, subject.request)
    order.clear()
    with pytest.raises(ValueError):
        retry._close_published_cleanup(store, row, subject.spec, lambda: False)
    assert order == ['proof'] and closed == original


def test_real_modern_local_tracking_closes_cleanup_without_promoting_remote_consistency(
    service, monkeypatch,
):
    from devflow_temporal import delivery_automatic_retry as retry
    from devflow_temporal import delivery_native_process
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_resources import RunResources

    store, request = service
    admitted = DeliveryConfig.admit

    def modern(self, supplied, **kwargs):
        return {**admitted(self, supplied, **kwargs), 'terminal_tracker_version': 1}

    monkeypatch.setattr(DeliveryConfig, 'admit', modern)
    # Empty private resource inventory: no OS actor observation or teardown.
    monkeypatch.setattr(delivery_native_process, 'process_table', lambda: {})
    store.submit(request)
    store.mark_start(request['run_id'], accepted=True)
    spec = store.intake_execution_spec(request['run_id'])
    assert spec['project_sync_version'] == spec['terminal_tracker_version'] == 1
    original_receipt = RunResources(spec).finalize('blocked')
    checks = {'failure': {'classification': 'terminal', 'stage': 'repair'},
              'resource_cleanup': original_receipt}
    store.project(spec['run_id'], phase='blocked', outcome='blocked', execution_state='blocked',
                  cleanup='unknown', event_type='blocked', message='Retained original stop',
                  error='implementer did not establish a pass', checks=checks)
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                              (spec['run_id'],)).fetchone())
    closed = {'workflow_id': 'original-private-workflow', 'execution_run_id': 'closed',
              'result': {'run_id': spec['run_id'], 'error': row['error'], 'checks': checks,
                         'cleanup': 'unknown'}}
    before = deepcopy(closed)
    # The enclosing native/publication proof is tested separately. This invokes
    # the real cleanup transaction, resource receipts and _tracker_sync producer
    # on a private Store; no consistent-return or cleanup-result seam is used.
    updated = retry._close_cleanup(store, row, spec, closed, lambda: False, published=True)
    tracker = json.loads(updated['tracker_json'])
    assert {k: tracker[k] for k in ('state', 'pending', 'scope', 'project_sync',
                                   'status', 'claim_released')} == {
        'state': 'recorded', 'pending': False, 'scope': 'local',
        'project_sync': 'asynchronous', 'status': 'blocked', 'claim_released': True}
    assert updated['cleanup'] == 'confirmed'
    assert updated['error'] == row['error'] and closed == before
    assert updated['recovery_json'] == row['recovery_json']
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id']) is None
        receipts = [json.loads(r[0]) for r in db.execute(
            "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'")]
    durable = next(r for r in receipts if r.get('closure') == 'published')
    assert durable['original_result_digest'] == digest(before['result'])
    assert durable['original_checks'] == checks and durable['tracker'] == tracker
    assert retry._reuse_cleanup(store, updated, spec, closed, lambda: False) is True
    assert retry._tracker_closed({}, tracker) is False
    assert retry._tracker_closed({}, {'state': 'consistent', 'pending': False}) is True
    assert retry._tracker_closed({}, {'state': 'consistent', 'pending': True}) is False
    for change in ({'pending': True}, {'scope': 'remote'}, {'project_sync': 'synchronous'},
                   {'status': 'done'}, {'claim_released': False}, {'state': 'consistent'}):
        monkeypatch.setattr(retry, '_tracker_sync', lambda *_a, change=change, **_kw: {
            **tracker, **change})
        with pytest.raises(ValueError, match='later tracker closure is unobservable'):
            retry._reuse_cleanup(store, updated, spec, closed, lambda: False)
