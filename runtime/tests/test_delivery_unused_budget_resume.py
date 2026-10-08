from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import test_delivery_gate_retry as gate_tests
from test_delivery_stopped_resume import project, saved
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def fixed_stop(service, monkeypatch):
    store, request = service
    config = copy.deepcopy(store.config.raw)
    config.update(max_attempts=3, max_repairs=2)
    store.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    # Reuse the logical stopped-checkpoint fixture, admitting a current fixed
    # budget instead of the fixture's deliberately retained legacy admission.
    with monkeypatch.context() as current:
        current.setattr(gate_tests, 'submit_historical_admission',
                        lambda owner, body, _: owner.submit(body))
        store, broker, state, _ = gate_tests.unpublished.__wrapped__((store, request), monkeypatch)
    spec = store.spec('run-1')
    state['iteration'] = 1
    state['roles'][0]['iteration'] = 1
    state['checks']['baseline'] = {
        'state': 'passed', 'base_sha': spec['base_sha'],
        'baseline_candidate': {'head': spec['base_sha']},
        'feature_unchanged': True, 'source_unchanged': True, 'results': [],
    }
    project(store, state)
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET iteration=1,result_json=?',
                   (canonical_json(state['roles'][0]),))
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    command = {
        'continuation_kind': resume.KIND, 'command_id': 'resume-unused',
        'expected_revision': state['revision'], 'expected_iteration': 1,
        'expected_candidate_id': broker.candidate()['id'],
        'expected_candidate_head': broker.candidate()['head'], 'additional_iterations': 1,
    }
    return store, broker, state, command


def test_fixed_resume_spends_only_the_unused_original_iteration(fixed_stop):
    store, broker, state, command = fixed_stop
    original = copy.deepcopy(store.submitted_spec('run-1'))
    before = copy.deepcopy(state)
    assert original['retry_budget_version'] == 1
    assert original['policy']['max_repairs'] == 2
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 2
    admitted = store.continue_repair('run-1', command)
    assert admitted['authorized_through_iteration'] == 2
    assert store.continue_repair('run-1', command) == admitted
    assert store.submitted_spec('run-1') == original
    assert store.effective_spec('run-1')['policy']['max_repairs'] == 2
    assert saved(store)['state'] == before
    assert saved(store)['maximum_iteration'] == 2
    assert resume.readback(store, store.effective_spec('run-1'), saved(store)) == {
        'state': 'confirmed'}
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0] == 2
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
        assert store.state.claim_for(db, broker.spec['work_id']) is not None


def test_unused_resume_keeps_the_original_failed_session_and_partial_source(fixed_stop):
    store, broker, state, command = fixed_stop
    (broker.checkout / 'README.md').write_text('Authentic partial original-session edit\n')
    actual = broker.candidate()
    receipt = {'status': 'blocked', 'session_id': 'original-implementation',
               'cleanup': 'confirmed', 'findings': ['Owning verification contract needs repair']}
    state['roles'] = [{**receipt, 'role': 'implement', 'iteration': 1, 'candidate': actual}]
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET result_json=?,session_id=?',
                   (canonical_json(receipt), receipt['session_id']))
    command['expected_candidate_id'] = actual['id']
    store.continue_repair('run-1', command)
    recovery = saved(store)
    assert recovery['execution_candidate'] == actual
    assert recovery['state']['candidate'] != actual
    assert recovery['session_id'] == receipt['session_id']
    assert recovery['maximum_iteration'] == 2


@pytest.mark.parametrize('iterations', [2, 3])
def test_fixed_resume_cannot_extend_the_ceiling_or_prepare_runtime(fixed_stop, monkeypatch,
                                                                 iterations):
    store, _, _, command = fixed_stop
    def unexpected(*_args, **_kwargs):
        pytest.fail('a budget extension reached runtime preparation')
    monkeypatch.setattr(resume, 'prepare_runtime', unexpected)
    with pytest.raises(ValueError, match='fixed repair budget'):
        store.continue_repair('run-1', {**command, 'additional_iterations': iterations})
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0] == 1
        assert store.state.claim_for(db, 'work-1') is None


def test_direct_resume_admission_cannot_bypass_the_fixed_ceiling(fixed_stop, monkeypatch):
    store, _, _, command = fixed_stop
    monkeypatch.setattr(resume, 'prepare_runtime',
                        lambda *_args: pytest.fail('oversized direct admission prepared runtime'))
    with pytest.raises(ValueError, match='fixed repair budget'):
        resume.admit(store, 'run-1', {**command, 'additional_iterations': 2})


def test_resume_native_cleanup_only_observes_owned_closure(monkeypatch):
    from devflow_temporal import delivery_native_process

    observed = {'manifest_sha256': 'a' * 64, 'receipt_sha256': 'b' * 64,
                'journal_sha256': {'owned-journal': 'c' * 64}}
    calls = []
    monkeypatch.setattr(resume, '_stopped_cleanup',
                        lambda spec: calls.append(spec) or observed)
    def unexpected(*_args, **_kwargs):
        pytest.fail('read-only stopped-resume cleanup attempted process control')
    monkeypatch.setattr(delivery_native_process, 'reconcile_process', unexpected)
    monkeypatch.setattr(delivery_native_process, 'stop_observed', unexpected)
    spec = {'provider': 'codex'}
    assert resume.observed_native_cleanup(spec) == digest(observed)
    assert calls == [spec]


@pytest.mark.parametrize(('complete', 'live_monitor', 'accepted'), [
    (True, True, False), ('unknown', False, False), (1, False, False),
    (True, False, True),
])
def test_observed_cleanup_requires_strict_completion_and_separate_monitor_exit(
        monkeypatch, complete, live_monitor, accepted):
    from devflow_temporal import delivery_native_process
    from devflow_temporal import delivery_policy_recovery as policy

    root = Path('/synthetic-not-created')
    resources = SimpleNamespace(root=root, manifest=root / 'manifest.json')
    journal = {'phase': 'finished', 'monitoring_complete': complete, 'owned': {},
               'monitor': {'pid': 12345, 'identity': 'synthetic monitor'}, 'ports': []}
    receipts = {
        resources.manifest: {'processes': [str(root / 'journal.json')]},
        root / 'finalization.json': {'state': 'confirmed',
                                    'process_cleanup': 'observed-native-confirmed',
                                    'resource_cleanup': 'confirmed', 'roots': []},
        root / 'journal.json': journal,
    }
    monkeypatch.setattr(policy, 'RunResources', lambda _spec: resources)
    monkeypatch.setattr(policy, 'read_private', lambda path: receipts[path])
    monkeypatch.setattr(Path, 'read_bytes', lambda _path: b'synthetic bytes')
    monkeypatch.setattr(policy, 'process_table', lambda: {
        12345: {'identity': 'synthetic monitor', 'stat': 'S'}} if live_monitor else {})
    def unexpected(*_args, **_kwargs):
        pytest.fail('observation-only cleanup attempted process control')
    monkeypatch.setattr(delivery_native_process, 'reconcile_process', unexpected)
    monkeypatch.setattr(delivery_native_process, 'stop_observed', unexpected)
    if accepted:
        assert resume.observed_native_cleanup({'provider': 'codex'})
    else:
        with pytest.raises(ValueError, match='still live or unknown'):
            resume.observed_native_cleanup({'provider': 'codex'})
