"""Previously admitted policy rows and queued execution retain their recorded contract."""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import submit_historical_admission

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
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
    intent = {'command_digest': digest(payload), 'seal_digest': digest(state),
              'preparation_attempts': [], 'effective_spec': None}
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
    def context(_spec, *, terminal_settlement=False):
        assert terminal_settlement is False
        return store, None

    monkeypatch.setattr('devflow_temporal.delivery_activities._context', context)
    calls = []
    monkeypatch.setattr('devflow_temporal.delivery_activities.subprocess.run',
                        lambda *args, **_kwargs: calls.append(args))
    with pytest.raises(ValueError, match='frozen authority'):
        _tracker_sync(effective, 'in-progress', release=False)
    assert calls == []
