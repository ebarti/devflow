"""Real browser observation remains authenticated through admission and readback."""
from __future__ import annotations

import json

import pytest
from test_delivery_gate_retry import published as published
from test_delivery_gate_retry import stopped as stopped
from test_delivery_store import service as original_service

from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_browser_prelaunch import observe, resolved_effect
from devflow_temporal.delivery_gate_retry import PRELAUNCH_KIND, readback
from devflow_temporal.delivery_resources import RunResources, private_directory


@pytest.fixture
def service(tmp_path, monkeypatch):
    store, request = original_service.__wrapped__(tmp_path)
    store.config.raw['repositories']['fixture']['browser_qa'] = {
        'id': 'browser-fixture', 'argv': ['fixture'], 'cwd': '.',
        'ports': {'QA_API_PORT': 14101, 'QA_WEB_PORT': 14102},
        'env': {'JOBCTRL_E2E_ISOLATED': '1'},
        'artifact_paths': ['dist/playwright-report'],
        'test_count_regex': r'(\d+) passed', 'min_tests': 1, 'timeout_seconds': 30,
    }
    store.config.path.write_text(json.dumps(store.config.raw))
    original_admit = type(store.config).admit

    def admit(configuration, body):
        value = original_admit(configuration, body)
        # Freeze custody before submission. Provider and remote transports are
        # modeled; filesystem and host resource observation remain real.
        value['policy']['host_sandbox'] = 'trusted-local'
        value['policy_digest'] = digest(value['policy'])
        value['resource_cleanup_version'] = 1
        return value

    monkeypatch.setattr(type(store.config), 'admit', admit)
    return store, request


@pytest.mark.parametrize('prior_namespace', [False, True])
def test_real_observer_composes_across_admission_and_readback(stopped, monkeypatch,
                                                            prior_namespace):
    store, broker, state, closed, request = stopped
    resources = RunResources(store.effective_spec('run-1'))
    assert resources.finalize('blocked')['state'] == 'confirmed'
    if prior_namespace:
        store.continue_repair('run-1', request)
        with store._connect() as db:
            row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'")
                       .fetchone())
        closed.update(workflow_id=row['workflow_id'],
                      recovery_digest=digest(json.loads(row['recovery_json'])))
    spec = store.effective_spec('run-1')
    broker = DeliveryBroker(store, spec)
    old_root = broker.evidence_dir
    if old_root != broker.state_dir:
        broker.effect_namespace = ':' + str(old_root.relative_to(broker.state_dir).parent)
    gate = broker.gate_checkout('verify', state['iteration'], state['candidate'])
    output = gate / 'dist/playwright-report'
    output.mkdir(parents=True)
    artifact = output / '.last-run.json'
    artifact.write_text('{"status":"failed","failedTests":[]}')
    folder = old_root / 'browser-qa' / str(state['iteration'])
    private_directory(folder)
    profile = folder / 'browser-qa.sb'
    profile.write_bytes(b'(version 1)\n(allow default)\n')
    profile.chmod(0o600)
    qa = spec['policy']['browser_qa']
    key = f"browser_qa:run-1:{state['iteration']}" + broker.effect_namespace
    broker._effect(key, 'browser_qa', {
        'iteration': state['iteration'], 'candidate_id': state['candidate']['id'],
        'policy_digest': spec['policy_digest'], 'qa_config_sha256': digest(qa),
        'ports': qa['ports'], 'argv': qa['argv'],
    })
    state.update(cleanup='unknown', error='browser QA child cleanup is unknown')
    state['roles'][-1].update(role='review', status='pass', findings=[],
                             candidate=state['candidate'])
    final = RunResources(spec).finalize('blocked', uncertain=True)
    assert final['state'] == 'unknown'
    state['checks'] = {
        'browser_qa': {'state': 'unknown', 'cleanup': 'unknown', 'reason': 'ValueError',
                       'candidate_id': state['candidate']['id']},
        'local': {'state': 'passed', 'source_unchanged': True,
                  'candidate_id': state['candidate']['id'],
                  'results': [{'id': 'local-fixture', 'passed': True, 'cleanup': 'confirmed'}]},
        'review': {'state': 'passed', 'candidate_id': state['candidate']['id']},
        'resource_cleanup': final,
    }
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'], checks=state['checks'],
                  iteration=state['iteration'], protocol_revision=state['revision'],
                  outcome='blocked', cleanup='unknown', error=state['error'])
    with store._connect() as db:
        effects = [dict(row) for row in db.execute(
            "SELECT * FROM delivery_effects WHERE run_id='run-1' ORDER BY effect_key")]
    original_proof = observe(spec, state, effects)
    originals = [resources.manifest, resources.root / 'finalization.json', profile, artifact]
    original_bytes = [path.read_bytes() for path in originals]
    original_state = canonical_json(state)
    request.update(continuation_kind=PRELAUNCH_KIND,
                   command_id='composed-browser-prelaunch-' + str(prior_namespace))
    assert store.repair_admission_preflight('run-1', request)['additional_iterations'] == 0
    result = store.continue_repair('run-1', request)
    current = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs "
                                         "WHERE run_id='run-1'").fetchone()[0])
        effect = dict(db.execute('SELECT * FROM delivery_effects WHERE effect_key=?',
                                 (key,)).fetchone())
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['seal']['browser_prelaunch_observation'] == original_proof
    assert canonical_json(recovery['state']) == original_state
    assert [path.read_bytes() for path in originals] == original_bytes
    assert json.loads(effect['observed_json']) == resolved_effect(original_proof,
                                                               recovery['seal']['candidate'])
    assert effect['state'] == 'complete'
    assert result['implementation_authority'] is False
    fresh_broker = DeliveryBroker(store, current)
    assert fresh_broker.evidence_dir != old_root
    assert readback(store, current, recovery)['number'] == 7
    assert store.continue_repair('run-1', request) == result
    artifact.write_text('changed after admission')
    with pytest.raises(ValueError, match='preserved evidence changed'):
        readback(store, current, recovery)
    artifact.write_bytes(original_bytes[-1])
    assert readback(store, current, recovery)['number'] == 7
