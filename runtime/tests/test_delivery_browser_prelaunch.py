"""Read-only no-launch proof and the bounded public recovery admission."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest
from test_delivery_gate_retry import published as published
from test_delivery_gate_retry import service as service
from test_delivery_gate_retry import stopped as stopped
from test_delivery_resources import spec

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_browser_prelaunch import observe, resolved_effect
from devflow_temporal.delivery_gate_retry import PRELAUNCH_KIND, readback
from devflow_temporal.delivery_resources import RunResources, private_directory


@pytest.fixture
def browser_stop(tmp_path):
    owned = spec(tmp_path)
    owned['policy'].update(host_sandbox='trusted-local', browser_qa={
        'argv': ['fixture'], 'ports': {'QA_API_PORT': 14101, 'QA_WEB_PORT': 14102},
        'artifact_paths': ['dist/playwright-report']})
    root = Path(owned['state_dir'])
    gate = root / 'gates/0/verify'
    private_directory(gate.parent)
    resources = RunResources(owned)
    resources.register(gate, 'gate')
    gate.mkdir()
    resources.created(gate)
    output = gate / 'dist/playwright-report'
    output.mkdir(parents=True)
    (output / '.last-run.json').write_text('{"status":"failed","failedTests":[]}')
    folder = root / 'browser-qa/0'
    private_directory(folder)
    profile = folder / 'browser-qa.sb'
    profile.write_text('(version 1)\n(allow default)\n')
    profile.chmod(0o600)
    candidate = {'id': 'c' * 64}
    state = {'candidate': candidate, 'iteration': 0, 'cleanup': 'unknown',
             'error': 'browser QA child cleanup is unknown',
             'roles': [{'role': 'review', 'iteration': 0, 'candidate': candidate,
                        'status': 'pass', 'cleanup': 'confirmed', 'findings': []}],
             'checks': {'browser_qa': {'state': 'unknown', 'cleanup': 'unknown',
                                      'reason': 'ValueError', 'candidate_id': candidate['id']},
                        'local': {'state': 'passed', 'source_unchanged': True,
                                  'candidate_id': candidate['id'], 'results': [{'passed': True}]},
                        'review': {'state': 'passed', 'candidate_id': candidate['id']}}}
    qa = owned['policy']['browser_qa']
    effect = {'effect_key': 'browser_qa:run:0', 'run_id': 'run', 'kind': 'browser_qa',
              'state': 'pending', 'observed_json': None, 'updated_at': 'original',
              'request_json': canonical_json({'iteration': 0, 'candidate_id': candidate['id'],
                                              'policy_digest': owned['policy_digest'],
                                              'qa_config_sha256': digest(qa),
                                              'ports': qa['ports'], 'argv': qa['argv']})}
    assert resources.finalize('blocked', uncertain=True)['state'] == 'unknown'
    return owned, state, [effect], resources, profile, output


def test_observer_preserves_unknown_finalization_and_unregistered_output(browser_stop):
    owned, state, effects, resources, profile, output = browser_stop
    paths = [resources.manifest, resources.root / 'finalization.json', profile,
             output / '.last-run.json']
    before = [p.read_bytes() for p in paths]
    proof = observe(owned, state, effects)
    assert proof['historical_cleanup'] == 'unknown'
    assert proof['pending_effect'] == effects[0]
    assert len(proof['artifacts']) == 1
    assert [p.read_bytes() for p in paths] == before
    assert resolved_effect(proof, state['candidate'])['state'] == 'failed'


@pytest.mark.parametrize('drift', ['launch', 'receipt', 'profile', 'profile-link', 'output-link',
                                 'output-missing', 'registered', 'local', 'review', 'assessment',
                                 'candidate', 'error', 'effect', 'second-effect', 'ports', 'root'])
def test_browser_observer_rejects_ambiguous_or_live_history(browser_stop, monkeypatch, drift):
    owned, state, effects, resources, profile, output = browser_stop
    if drift == 'launch':
        (profile.parent / 'native').mkdir()
    elif drift == 'receipt':
        (profile.parent / 'receipt.json').write_text('{}')
    elif drift == 'profile':
        profile.write_text('changed authority')
    elif drift == 'profile-link':
        other = profile.with_name('other')
        profile.rename(other)
        profile.symlink_to(other)
    elif drift == 'output-link':
        (output / 'link').symlink_to(profile)
    elif drift == 'output-missing':
        (output / '.last-run.json').unlink()
        output.rmdir()
    elif drift == 'registered':
        with resources.locked() as manifest:
            manifest['roots'][str(output)] = {'kind': 'generated', 'identity': None}
            from devflow_temporal.delivery_resources import write_private
            write_private(resources.manifest, manifest)
    elif drift in {'local', 'review'}:
        state['checks'][drift]['state'] = 'failed'
    elif drift == 'assessment':
        state['roles'][0]['findings'] = ['real product issue']
    elif drift == 'candidate':
        state['checks']['local']['candidate_id'] = 'd' * 64
    elif drift == 'error':
        state['error'] = 'different unknown failure'
    elif drift == 'effect':
        effects[0]['request_json'] = '{}'
    elif drift == 'second-effect':
        effects.append({**effects[0], 'effect_key': 'unrelated'})
    elif drift == 'ports':
        monkeypatch.setattr('devflow_temporal.delivery_browser_prelaunch.listeners', lambda _: {99})
    else:
        profile.parents[2].joinpath('gates/0/verify').rename(
            profile.parents[2] / 'replaced-gate')
    with pytest.raises((ValueError, FileNotFoundError)):
        observe(owned, state, effects)


def gate_stop(stopped, monkeypatch, *, previous=False):
    store, broker, state, closed, request = stopped
    if previous:
        store.continue_repair('run-1', request)
        with store._connect() as db:
            row = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                   ('run-1',)).fetchone())
        closed.update(workflow_id=row['workflow_id'],
                      recovery_digest=digest(json.loads(row['recovery_json'])))
    state.update(cleanup='unknown', error='browser QA child cleanup is unknown')
    state['roles'][-1].update(role='review', status='pass', findings=[],
                              candidate=state['candidate'])
    state['checks'] = {'browser_qa': {'state': 'unknown', 'cleanup': 'unknown',
                                    'reason': 'ValueError',
                                    'candidate_id': state['candidate']['id']}}
    broker._effect('browser-pending', 'browser_qa', {'candidate_id': state['candidate']['id']})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='blocked',execution_state='blocked',"
                   "outcome='blocked',cleanup='unknown',error=?,checks_json=? WHERE run_id='run-1'",
                   (state['error'], canonical_json(state['checks'])))
        effect = dict(db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                  ('browser-pending',)).fetchone())
    proof = {'resources': {'observed': True}, 'pending_effect': effect,
             'historical_cleanup': 'unknown', 'evidence_root': str(broker.state_dir)}
    monkeypatch.setattr('devflow_temporal.delivery_browser_prelaunch.observe',
                        lambda *_a, **_kw: deepcopy(proof))
    request.update(continuation_kind=PRELAUNCH_KIND, command_id='browser-prelaunch-recovery')
    return store, broker, state, closed, request, proof


@pytest.mark.parametrize('previous', [False, True])
def test_public_browser_recovery_preserves_history_and_grants_no_source_turn(
    stopped, monkeypatch, previous,
):
    store, broker, state, _, request, proof = gate_stop(stopped, monkeypatch, previous=previous)
    before = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['additional_iterations'] == 0
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects WHERE effect_key='browser-pending'") \
            .fetchone()[0] == 'pending'
    result = store.continue_repair('run-1', request)
    assert result['implementation_authority'] is False
    assert store.continue_repair('run-1', request) == result
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                         ('run-1',)).fetchone()[0])
        effect = dict(db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                  ('browser-pending',)).fetchone())
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['state'] == state and state['cleanup'] == 'unknown'
    assert recovery['seal']['browser_prelaunch_observation'] == proof
    assert effect['state'] == 'complete'
    assert json.loads(effect['observed_json']) == resolved_effect(proof, before)
    assert recovery['execution_spec']['gate_retry_generation'] == 1
    assert broker.candidate() == before
    assert readback(store, store.effective_spec('run-1'), recovery)['number'] == 7
    with store._connect() as db:
        db.execute("UPDATE delivery_effects SET observed_json='{}' WHERE effect_key=?",
                   ('browser-pending',))
    with pytest.raises(ValueError, match='resolution changed'):
        readback(store, store.effective_spec('run-1'), recovery)


def test_second_browser_prelaunch_recovery_is_refused_across_history(stopped, monkeypatch):
    store, _, state, closed, request, _ = gate_stop(stopped, monkeypatch, previous=True)
    store.continue_repair('run-1', request)
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
        db.execute("UPDATE delivery_runs SET phase='blocked',execution_state='blocked',"
                   "outcome='blocked',cleanup='unknown',error=? WHERE run_id='run-1'",
                   (state['error'],))
    closed.update(workflow_id=row['workflow_id'],
                  recovery_digest=digest(json.loads(row['recovery_json'])))
    with pytest.raises(ValueError, match='already received its bounded prelaunch retry'):
        store.continue_repair('run-1', {**request, 'command_id': 'another-prelaunch'})


def test_readback_rejects_changed_original_browser_evidence(stopped, monkeypatch):
    store, _, _, _, request, proof = gate_stop(stopped, monkeypatch)
    store.continue_repair('run-1', request)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs "
                                         "WHERE run_id='run-1'").fetchone()[0])
    monkeypatch.setattr('devflow_temporal.delivery_browser_prelaunch.observe',
                        lambda *_a, **_kw: {**proof, 'profile_sha256': 'changed'})
    with pytest.raises(ValueError, match='preserved evidence changed'):
        readback(store, store.effective_spec('run-1'), recovery)
