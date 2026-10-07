from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import private_directory, write_private
from devflow_temporal.delivery_role_evidence import allocate, seal
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.supervisor import DeliverySupervisor

KIND = 'published_controller_retry'


@pytest.fixture
def evidence_failure(published, monkeypatch):
    store, broker, state, closed, _, _ = published
    spec = store.spec('run-1')
    candidate = broker.candidate()
    # Both snapshots describe the same committed source, on the original session.
    prior = allocate({'spec': spec, 'role': 'implement', 'iteration': 0,
                      'workspace': str(broker.checkout), 'candidate': candidate}, '1' * 64)
    (Path(prior['artifact_directory']) / 'original.log').write_text('Original probe\n')
    prior_ref = seal(prior)['role_artifacts']
    request = {'spec': spec, 'role': 'implement', 'iteration': 2,
               'workspace': str(broker.checkout), 'candidate': candidate,
               'resume_session': 'original-session', 'findings': ['Restore original evidence']}
    key = DeliverySupervisor._job_key(request)
    request = allocate(request, key)
    target = Path(request['artifact_directory'])
    (target / 'original.log').write_text('Original probe\n')
    (target / 'restored-inputs.json').write_text('{"original_input":42}\n')
    reference = seal(request)['role_artifacts']
    raw = {'status': 'pass', 'session_id': 'original-session', 'findings': [],
           'candidate': candidate}
    saved = {**raw, 'cleanup': 'confirmed', 'role_artifacts': reference,
             'native_process': {'cleanup': 'confirmed', 'phase': 'finished'}}
    journal = {'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
                          'cwd': str(broker.checkout)},
               'phase': 'finished', 'result': saved['native_process'], 'provider_session': {
        'role': 'implement', 'iteration': 2, 'session_id': 'original-session',
        'resumed_from': 'original-session', 'result_digest': digest(saved),
        'output_candidate': {k: candidate[k] for k in ('id', 'head', 'content_sha256')}}}
    folder = Path(spec['state_dir']) / 'attempts' / key
    private_directory(folder)
    for name, value in [('request.json', request), ('result.json', raw),
                        ('native-process.json', journal)]:
        write_private(folder / name, value)
    implementation = {'role': 'implement', 'iteration': 2, **saved,
                      'status': 'blocked', 'findings': ['implementer produced no candidate change']}
    state.update(iteration=2, cleanup='confirmed', error='implementer did not establish a pass',
                 checks={})
    state['roles'] = [state['roles'][0], implementation]
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts WHERE run_id='run-1'")
        for job, iteration, result, result_path in [
            ('1' * 64, 0, {'role_artifacts': prior_ref}, None),
            (key, 2, saved, str(folder / 'result.json'))]:
            db.execute('INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,'
                       "state,session_id,result_json,cleanup,result_path) VALUES (?,?,'implement',"
                       "?,?,'finished','original-session',?,'confirmed',?)",
                       (job, 'run-1', iteration, candidate['id'], canonical_json(result),
                        result_path))
        db.execute("UPDATE delivery_runs SET cleanup='confirmed',error=?,checks_json='{}',"
                   "workflow_id='delivery-run-1',iteration=2 WHERE run_id='run-1'",
                   (state['error'],))
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup', lambda _: {})
    command = {'continuation_kind': KIND, 'command_id': 'controller-retry-1',
               'expected_revision': state['revision'], 'expected_iteration': state['iteration'],
               'expected_candidate_id': candidate['id'], 'expected_pr_number': 7,
               'expected_pr_head': candidate['head'], 'additional_iterations': 0}
    return store, broker, state, closed, command, folder


def test_authentic_evidence_only_failure_has_one_zero_iteration_continuation(evidence_failure):
    store, broker, state, closed, command, _ = evidence_failure
    original = store.effective_spec('run-1')
    old_state = deepcopy(state)
    old_closed = deepcopy(closed)
    before = broker.candidate()
    old_gate = broker._gate_path('verify', state['iteration'])
    assert store.repair_admission_preflight('run-1', command)['additional_iterations'] == 0
    admitted = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == admitted
    effective = store.effective_spec('run-1')
    assert effective['policy']['max_repairs'] == original['policy']['max_repairs']
    assert state['iteration'] == original['policy']['max_repairs']
    assert broker.candidate() == before
    assert DeliveryBroker(store, effective)._gate_path('verify', state['iteration']) != old_gate
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_gate_admissions '
                                        "WHERE run_id='run-1'").fetchone()[0])
    assert recovery['state'] == old_state and closed == old_closed
    from devflow_temporal.delivery_gate_retry import readback

    assert readback(store, effective, recovery)['number'] == 7
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**command, 'command_id': 'second-controller-retry'})


@pytest.mark.parametrize('corruption', [
    'assessment', 'receipt', 'session', 'job', 'journal', 'artifact', 'no-progress',
    'findings', 'positive-budget', 'stale-head', 'attempt-cleanup', 'closed-cleanup',
])
def test_controller_retry_rejects_unproven_or_drifted_checkpoint(evidence_failure, corruption):
    store, broker, state, _, command, folder = evidence_failure
    key = folder.name
    if corruption in {'assessment', 'job', 'attempt-cleanup'}:
        with store._connect() as db:
            if corruption == 'assessment':
                saved = json.loads(db.execute('SELECT result_json FROM delivery_attempts '
                                              'WHERE job_key=?', (key,)).fetchone()[0])
                saved['status'] = 'blocked'
                db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                           (canonical_json(saved), key))
            elif corruption == 'job':
                db.execute('UPDATE delivery_attempts SET job_key=? WHERE job_key=?',
                           ('f' * 64, key))
            else:
                db.execute("UPDATE delivery_attempts SET cleanup='unknown' WHERE job_key=?", (key,))
    elif corruption in {'receipt', 'session', 'journal'}:
        path = folder / {'receipt': 'result.json', 'session': 'request.json',
                         'journal': 'native-process.json'}[corruption]
        value = json.loads(path.read_text())
        if corruption == 'receipt':
            value['status'] = 'blocked'
        elif corruption == 'session':
            value['resume_session'] = 'other-session'
        else:
            value['provider_session']['result_digest'] = 'f' * 64
        write_private(path, value)
    elif corruption in {'artifact', 'no-progress'}:
        manifest = json.loads(Path(state['roles'][-1]['role_artifacts']['path']).read_text())
        path = Path(next(a['path'] for a in manifest['artifacts']
                         if a['path'].endswith('restored-inputs.json')))
        path.write_text('Changed after seal\n' if corruption == 'artifact' else 'Original probe\n')
        if corruption == 'no-progress':
            # Authenticate a real seal whose new filename carries only an old payload.
            for item in manifest['artifacts']:
                if item['path'] == str(path):
                    item.update(sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                                size=path.stat().st_size)
            reference = state['roles'][-1]['role_artifacts']
            manifest_path = Path(reference['path'])
            manifest_path.write_text(canonical_json(manifest) + '\n')
            reference['sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
            with store._connect() as db:
                saved = json.loads(db.execute('SELECT result_json FROM delivery_attempts '
                                              'WHERE job_key=?', (key,)).fetchone()[0])
                saved['role_artifacts'] = reference
                db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                           (canonical_json(saved), key))
            journal = json.loads((folder / 'native-process.json').read_text())
            journal['provider_session']['result_digest'] = digest(saved)
            write_private(folder / 'native-process.json', journal)
    elif corruption == 'findings':
        state['roles'][-1]['findings'].append('Real product defect')
    elif corruption == 'positive-budget':
        command['additional_iterations'] = 1
    elif corruption == 'stale-head':
        command['expected_pr_head'] = 'f' * 40
    else:
        state['cleanup'] = 'unknown'
    with pytest.raises((ValueError, FileNotFoundError)):
        store.repair_admission_preflight('run-1', command)
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()


@pytest.mark.parametrize('qa_status', ['pass', 'findings'])
def test_continuation_preserves_failure_and_runs_fresh_independent_gates(
    evidence_failure, monkeypatch, qa_status,
):
    store, _, state, _, command, _ = evidence_failure
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    store.continue_repair('run-1', command)
    spec = store.effective_spec('run-1')
    spec['policy']['host_sandbox'] = 'trusted-local'
    with store._connect() as db:
        recovery = json.loads(db.execute(
            'SELECT recovery_json FROM delivery_gate_admissions').fetchone()[0])
    recovery['execution_spec'] = spec
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append((name, body.get('role')))
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            assert ('delivery_checks', None) in calls
            return {'role': body['role'], 'iteration': state['iteration'],
                    'status': qa_status if body['role'] == 'verify' else 'pass',
                    'findings': ['Fresh QA failure'] if qa_status != 'pass' else [],
                    'session_id': 'fresh-' + body['role'], 'candidate': recovery['candidate']}
        return {'state': 'passed', 'cleanup': 'confirmed',
                'candidate_id': recovery['candidate']['id']}

    async def project(*_args):
        pass

    async def stop(_spec, reason):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_stop', stop)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert [r for name, r in calls if name == 'delivery_role'] == ['review', 'verify']
    assert not any('publish' in name for name, _ in calls)
    assert result['iteration'] == state['iteration']
    assert result['roles'][-3]['status'] == 'blocked'
    assert result['roles'][-3]['findings'] == ['implementer produced no candidate change']
    assert result['outcome'] == ('delivered' if qa_status == 'pass' else 'blocked')
    if qa_status != 'pass':
        assert not any(name == 'delivery_ci' for name, _ in calls)


def test_repaired_runtime_guard_uses_actual_failed_implementation_journal(
    evidence_failure, monkeypatch,
):
    from devflow_temporal.delivery_gate_retry import snapshot

    store, broker, state, _, _, folder = evidence_failure
    native_spec = deepcopy(broker.spec)
    native_spec['provider'] = 'codex'
    native_spec['policy'].update(execution_backend='native-macos', host_sandbox='trusted-local',
                                 native_identity={'runtime_payload_sha256': '1' * 64})
    consumed = {'runtime_payload_sha256': '2' * 64}
    process = {'journal': str(folder / 'consumed-process.json'), 'runtime_identity': consumed}
    write_private(folder / 'consumed-process.json', {
        'runtime_identity': consumed, 'result': process,
        'intent': {'run_id': 'run-1', 'policy_digest': native_spec['policy_digest']},
    })
    state['roles'][-1]['native_process'] = process
    monkeypatch.setattr(store, 'effective_spec', lambda _: native_spec)
    monkeypatch.setattr('devflow_temporal.delivery_native_preparation.native_identity',
                        lambda _: consumed)
    with pytest.raises(ValueError, match='requires a repaired measured runtime'):
        snapshot(store, 'run-1', KIND)


@pytest.mark.parametrize('field', ['run_id', 'policy_digest', 'cwd'])
def test_original_journal_intent_must_match_frozen_request(evidence_failure, field):
    store, _, _, _, command, folder = evidence_failure
    path = folder / 'native-process.json'
    journal = json.loads(path.read_text())
    journal['intent'][field] = 'unrelated'
    write_private(path, journal)
    with pytest.raises(ValueError, match='authentic same-session'):
        store.repair_admission_preflight('run-1', command)


def test_readback_rejects_changed_durable_original_assessment(evidence_failure):
    from devflow_temporal.delivery_gate_retry import readback

    store, _, _, _, command, folder = evidence_failure
    store.continue_repair('run-1', command)
    effective = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_gate_admissions')
                              .fetchone()[0])
        saved = json.loads(db.execute('SELECT result_json FROM delivery_attempts WHERE job_key=?',
                                      (folder.name,)).fetchone()[0])
        saved['status'] = 'blocked'
        db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                   (canonical_json(saved), folder.name))
    with pytest.raises(ValueError, match='original attempt'):
        readback(store, effective, recovery)
