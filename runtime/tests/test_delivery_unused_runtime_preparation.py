"""Unused preparation survives owner handoff without granting another assessment."""
from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest
from devflow_temporal import delivery_activities as worker_activities
from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_listener_verification as listener
from devflow_temporal import delivery_native_preparation as native
from devflow_temporal import delivery_policy_recovery as policy
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_feature_execution import continue_feature, worker_key
from devflow_temporal.delivery_listener_verification import CAUSE
from devflow_temporal.delivery_preparation import _write
from devflow_temporal.delivery_resources import (
    RunResources,
    _gate_roots,
    observe_finalized_resources,
    read_private,
    write_private,
)
from devflow_temporal.delivery_store import DeliveryStore
from temporalio.testing import ActivityEnvironment
from test_delivery_feature_recovered_closure import recovered_worker as recovered_worker
from test_delivery_store import service as service


@pytest.fixture
def tmp_path():
    with TemporaryDirectory(prefix='devflow-unused-preparation-') as directory:
        yield Path(directory).resolve()


@pytest.mark.parametrize('current_cached', [False, True])
def test_unused_preparation_survives_released_owners_and_admits_the_same_gates(
    recovered_worker, monkeypatch, tmp_path, current_cached,
):
    store, parent, child, shared, old_owner, continue_command, _ = recovered_worker
    successor = continue_feature(store, parent['run_id'], continue_command)
    current_parent = store.effective_spec(successor['run_id'])
    completed = store._completed_temporal_result
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda _store, *args, **kwargs: completed(*args, **kwargs))
    from temporalio.client import Client
    async def no_network(*_args, **_kwargs):
        raise AssertionError('unexpected Temporal transport outside private closure seam')
    monkeypatch.setattr(Client, 'connect', no_network)
    owner = current_parent['feature_delivery']['owner']
    for number in range(9):
        shared.repair(owner, f'other-paid-{number}', 'Previously paid product work')
    budget = shared.budget(owner['issue_id'])
    assert budget['used'] == budget['maximum'] == 10
    shared.reserve_worker(owner, worker_key(child['run_id'], owner),
                          child['feature_worker']['workstream_id'])
    shared.checkpoint(owner, f"assignment:{child['feature_worker']['chunk_id']}:build", {
        'run_id': child['run_id'], 'store_path': owner['store_path'],
        'chunk_id': child['feature_worker']['chunk_id'], 'kind': 'build'})
    # continue_feature really releases the old canonical claim and transfers it.
    with store._connect() as db:
        assert store.state.claim_for(db, parent['work_id'])['owner'] == (
            'external:devflow:' + current_parent['run_id'])
        db.execute("UPDATE delivery_runs SET cleanup='unknown' WHERE run_id=?",
                   (parent['run_id'],))  # stop_feature never normalizes this old projection.
    monkeypatch.setattr(activities, 'current_record', lambda _: {
        'manifest': {'plan': json.loads(current_parent['accepted_plan']),
                     'publication': {'members': []}}})
    broker = DeliveryBroker(store, child)
    broker.prepare()
    binary = tmp_path / 'locked-cli'
    binary.write_text("#!/bin/sh\nprintf 'codex-cli fixture\\n'\n")
    binary.chmod(0o700)
    cli_digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    spec = deepcopy(child)
    spec['provider'] = 'codex'
    spec['policy'].update(host_sandbox='trusted-local', execution_backend='native-macos',
                          codex_bin=str(binary),
                          codex_bin_sha256=cli_digest)
    spec['policy_digest'] = digest(spec['policy'])
    original = deepcopy(spec)
    spec.update(gate_retry_generation=1, gate_retry_stage='report',
                role_home_generation='report-retry-1',
                implementation_role_home_generation='original')
    candidate = DeliveryBroker(store, spec).candidate()
    publication = {'number': 7, 'head': candidate['head'], 'candidate': candidate}
    previous = {'kind': gates.KIND, 'original_spec': original, 'execution_spec': spec,
                'original_recovery': None, 'command': {'additional_iterations': 0},
                'candidate': candidate, 'publication': publication,
                'seal': {'report_observation': {'cause': 'fixture_junit_report_layout'}}}
    # The original completed listener assessment is a named historical transport
    # seam. The changed preparation/admission/resource/owner code runs normally.
    cleanup = RunResources(spec).finalize('blocked')
    checks = {'resource_cleanup': cleanup, 'browser_qa': {'state': 'failed',
                            'native_process': {'listener_conflict': True}}}
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET outcome=NULL WHERE run_id=?', (child['run_id'],))
    store.project(child['run_id'], phase='blocked', execution_state='blocked',
        event_type='blocked', message='Retained listener conflict',
        candidate=candidate, iteration=7, checks=checks, outcome='blocked', cleanup='confirmed',
        protocol_revision=9,
        error='Retained listener conflict')
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=? WHERE run_id=?',
                   (json.dumps(original), child['run_id']))
        db.execute('UPDATE delivery_runs SET recovery_json=? WHERE run_id=?',
                   (json.dumps(previous), child['run_id']))
        db.execute('INSERT INTO delivery_gate_admissions VALUES (?,?,?)',
                   (child['run_id'], 'historical-report', json.dumps(previous)))
        db.execute("UPDATE delivery_outbox SET state='sent' WHERE run_id=?", (child['run_id'],))
    report_root = Path(spec['state_dir']) / gates.gate_namespace(1, 'report')
    write_private(report_root / 'admission.json', previous)
    row, attempts, effects, _ = policy._rows(store, child['run_id'])
    seal = {'row': gates._sealed_row(row), 'original_spec': spec, 'attempts': attempts,
        'effects': effects, 'candidate': candidate, 'publication': {'number': 7},
        'closed': {'workflow_id': row['workflow_id'], 'execution_run_id': 'closed-report-fork',
                   'result': {'run_id': child['run_id'], 'revision': 9, 'iteration': 7,
                              'roles': [{'role': 'implement', 'status': 'blocked',
                                         'session_id': 'original-session'}]}},
        'prior_gate_digest': digest(previous), 'previous_digest': digest(previous),
        'work_binding': None,
        'stage': 'transient', 'generation': 1,
        'verification_observation': {'cause': CAUSE, 'observer': {'runtime': 'fresh'}}}
    with store._connect() as db:
        seal['work_binding'] = policy.work_binding(store, spec, db)
    command = {'continuation_kind': gates.KIND, 'expected_revision': 9,
        'expected_iteration': 7, 'expected_candidate_id': candidate['id'],
        'expected_pr_head': candidate['head'], 'expected_pr_number': 7,
        'additional_iterations': 0}
    def command_for(token):
        return {**command, 'command_id': 'feature-resume-' + digest({
            'owner': token, 'run': child['run_id'],
            'closed': {key: seal['closed'][key]
                       for key in ('workflow_id', 'execution_run_id')}})[:24]}
    old_command, command = command_for(old_owner), command_for(owner)
    old_digest = digest({'run_id': child['run_id'], **old_command})
    command_digest = digest({'run_id': child['run_id'], **command})
    old_identity = {'execution_mode': 'trusted-local', 'runtime_payload_sha256': 'a' * 64,
                    'codex_bin_sha256': cli_digest, 'codex_bin': str(binary),
                    'runtime_dependencies': {'codex_cli_version': 'fixture'}}
    identity = {'execution_mode': 'trusted-local', 'runtime_payload_sha256': 'b' * 64,
                'codex_bin_sha256': cli_digest, 'codex_bin': str(binary),
                    'runtime_dependencies': {'codex_cli_version': 'fixture'}}
    selected = old_identity
    probes = []

    # Only the native host measurement/proof transport and installed-source
    # cleanliness are modeled. Private config/cache hashes, security binding,
    # coordinator/worker custody, DB rows and real resource finalization remain.
    monkeypatch.setattr(gates, '_git', lambda *_args: '')
    monkeypatch.setattr(gates, 'distribution', lambda _: SimpleNamespace(
        locate_file=lambda _: binary))
    # Host identity is the transport seam; cache bytes, proof validation and
    # per-run security binding are real, including the historical cached proof.
    verify = native.verify_native_spec
    def verify_with_historical_host(value):
        with monkeypatch.context() as host:
            host.setattr(native, 'native_identity', lambda _: deepcopy(
                value['policy'].get('native_identity') or selected))
            verify(value)
    monkeypatch.setattr(native, 'verify_native_spec', verify_with_historical_host)
    monkeypatch.setattr(native, 'native_identity', lambda _: deepcopy(selected))
    measured = native._measure_trusted
    def measure(probe, identity, *, state_root):
        probes.append(deepcopy(probe))
        return measured(probe, identity, state_root=state_root)
    monkeypatch.setattr(native, '_measure', measure)
    root = Path(spec['state_dir']) / gates.gate_namespace(1, 'transient')
    old_path = root / 'runtime-preparation/intent.json'
    cache = store.config.state_root / 'preparation-native'
    # Real trusted-local CLI/ancestry probes, native journals and finalization.
    seed = deepcopy(spec)
    seed['state_dir'] = str(root / 'runtime-preparation/seed' / spec['run_id'])
    try:
        proof = measured(seed, old_identity, state_root=store.config.state_root)
    except RuntimeError:
        for log in Path(seed['state_dir']).rglob('monitor.log'):
            print(log.read_text())
        raise
    assert RunResources(seed).finalize('blocked')['state'] == 'confirmed'
    observe_finalized_resources(seed)
    _write(cache / digest(old_identity) / 'proof.json', proof)
    legacy = {'command_digest': old_digest, 'seal_digest': 'c' * 64,
              'preparation_attempts': []}
    policy._prepare(spec, gates.load_execution_config(spec), legacy, old_path)
    assert legacy['state'] == 'prepared' and legacy['preparation_attempts'] == []
    assert legacy['effective_spec']['preparation']['cache_reused'] is True
    old_raw = old_path.read_bytes()
    selected = identity
    if current_cached:
        _write(cache / digest(identity) / 'proof.json', {
            **proof, 'identity': identity, 'fingerprint': digest(identity)})
    environment = ActivityEnvironment()
    environment.info = replace(environment.info, activity_type='delivery_feature_reserve',
                               workflow_id=store.active_workflow_id(current_parent['run_id']))
    def prepare(snapshot=seal):
        return environment.run(lambda: gates.prepare_runtime(
            spec, root, command_digest, digest(snapshot), handoff=(store, snapshot, command)))
    with pytest.raises(ValueError, match='changed authority'):
        gates.prepare_runtime(spec, root, command_digest, digest(seal))
    assert old_path.read_bytes() == old_raw and not probes
    slot = root / 'runtime-preparation/successor/intent.json'
    for state, attempts in [('preparing', []), ('failed', []),
                            ('prepared', [{'state': 'started', 'cleanup': None}]),
                            ('prepared', [{'state': 'stopped', 'cleanup': {'state': 'unknown'}}])]:
        write_private(old_path, {**legacy, 'state': state, 'preparation_attempts': attempts})
        with pytest.raises(ValueError, match='cache-only listener handoff'):
            prepare()
        assert not slot.exists() and not probes
    write_private(old_path, legacy)
    assert old_path.read_bytes() == old_raw
    drifted = deepcopy(legacy)
    drifted['effective_spec']['goal'] = 'Different candidate authority'
    write_private(old_path, drifted)
    with pytest.raises(ValueError, match='frozen source or policy'):
        prepare()
    write_private(old_path, legacy)
    with store._connect() as db:
        db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                   ('different-id-same-old-request', child['run_id'], old_digest, '{}'))
    with pytest.raises(ValueError, match='command was already consumed'):
        prepare()
    with store._connect() as db:
        db.execute('DELETE FROM delivery_commands WHERE command_id=?',
                   ('different-id-same-old-request',))
    assert not slot.exists() and not probes
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?',
                   (child['run_id'],))
    with pytest.raises(ValueError, match='admitted, queued or changed'):
        prepare()
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET revision=revision-1 WHERE run_id=?',
                   (child['run_id'],))
    with store._connect() as db:
        store.state.release_work(db, current_parent['work_id'],
                                 'external:devflow:' + current_parent['run_id'])
    with pytest.raises(ValueError, match='current parent claim'):
        prepare()
    with store._connect() as db:
        store.state.claim_work(db, current_parent['work_id'],
                               'external:devflow:' + current_parent['run_id'],
                               store.config.dashboard_url)
    with shared.connect() as db:
        db.execute("INSERT INTO execution_workers VALUES (?,?,?,?,'reserved',NULL)",
                   (old_owner['issue_id'], 'other-unfinished-worker',
                    'other-private-workstream', old_owner['generation']))
    with pytest.raises(ValueError, match='predecessor custody'):
        prepare()
    with shared.connect() as db:
        db.execute('DELETE FROM execution_workers WHERE worker_key=?',
                   ('other-unfinished-worker',))
    # An exception before durable admission must replay the same prepared intent.
    # Only the historical listener proof/Temporal and installed-host transports
    # are modeled; reserve command selection and all current custody/CAS are real.
    monkeypatch.setattr(gates, 'snapshot', lambda *_a, **_kw: deepcopy(seal))
    validation = []
    def historical_listener(_store, execution, recovery, *, resource_spec):
        assert recovery['seal'] == seal and resource_spec == execution
        assert recovery['original_recovery'] == previous
        bindings = {}
        _gate_roots(execution, execution_specs=bindings)
        assert bindings[report_root / 'evidence'] == spec
        validation.append(True)
    monkeypatch.setattr(listener, 'validate', historical_listener)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda _: {
        'number': 7, 'state': 'OPEN', 'headRefOid': candidate['head']})
    actual_prepare = gates._prepare
    def lose_parent_claim(*args, **kwargs):
        value = actual_prepare(*args, **kwargs)
        with store._connect() as db:
            store.state.release_work(db, current_parent['work_id'],
                                     'external:devflow:' + current_parent['run_id'])
        return value
    monkeypatch.setattr(gates, '_prepare', lose_parent_claim)
    with pytest.raises(ValueError, match='current parent claim'):
        environment.run(activities.reserve, current_parent,
                        child['feature_worker']['chunk_id'], 'build')
    monkeypatch.setattr(gates, '_prepare', actual_prepare)
    with store._connect() as db:
        store.state.claim_work(db, current_parent['work_id'],
                               'external:devflow:' + current_parent['run_id'],
                               store.config.dashboard_url)
    # This seam yields before any admission artifact: a read-only candidate
    # observation can fail immediately after preparation finishes.
    original_candidate = DeliveryBroker.candidate
    def failed_candidate(broker):
        if read_private(slot).get('state') == 'prepared':
            raise OSError('private handoff interruption before durable admission')
        return original_candidate(broker)
    monkeypatch.setattr(DeliveryBroker, 'candidate', failed_candidate)
    with store._connect() as db:
        actual = dict(db.execute('SELECT outcome,phase,workflow_id FROM delivery_runs '
                                 'WHERE run_id=?', (child['run_id'],)).fetchone())
    assert actual['outcome'] == 'blocked', actual
    with pytest.raises(OSError, match='before durable admission'):
        environment.run(activities.reserve, current_parent,
                        child['feature_worker']['chunk_id'], 'build')
    monkeypatch.setattr(DeliveryBroker, 'candidate', original_candidate)
    prepared = read_private(slot)['effective_spec']
    assert prepared != legacy['effective_spec']
    assert prepared['policy']['native_identity'] == identity
    assert prepared['preparation']['cache_reused'] is current_cached
    native.verify_native_spec(prepared)
    first = read_private(slot)
    assert first['predecessor']['intent_sha256'] == hashlib.sha256(old_raw).hexdigest()
    assert first['predecessor']['seal_digest'] == legacy['seal_digest']  # Opaque provenance only.
    assert first['predecessor']['coordinator']['owner'] == old_owner
    assert first['command_digest'] == command_digest and first['seal_digest'] == digest(seal)
    assert prepare() == prepared and read_private(slot) == first
    assert len(probes) == (0 if current_cached else 1)
    if probes:
        probe = probes[0]
        assert Path(probe['state_dir']) == slot.parent / 'probes/0' / child['run_id']
        assert not (Path(probe['state_dir']) / 'transient').exists()
        assert read_private(Path(probe['state_dir']) / 'resources/finalization.json')['state'] \
            == 'confirmed'
        observed = observe_finalized_resources(probe)
        assert len(observed['journal_sha256']) == 2
        assert len(policy._stopped_cleanup(probe, read_only=True)['journal_sha256']) == 2
    assert read_private(slot) == first and old_path.read_bytes() == old_raw
    assert not (root / 'admission.json').exists()
    # Another real coordinator handoff before admission rebinds this SAME slot;
    # it preserves the full finite native-probe ledger and consumes no gate.
    shared.drain(owner)
    store.project(current_parent['run_id'], phase='blocked', execution_state='blocked',
                  outcome='blocked', cleanup='unknown', event_type='blocked',
                  message='Private coordinator closed before gate admission')
    monkeypatch.setattr(store, '_completed_temporal_result',
        lambda run_id, **_kw: {'result': {'outcome': 'blocked'}})
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
        lambda _store, run_id, **_kw: {'result': {'outcome': 'blocked'}})
    with store._connect() as db:
        revision = db.execute('SELECT revision FROM delivery_runs WHERE run_id=?',
                              (current_parent['run_id'],)).fetchone()[0]
    following = continue_feature(store, current_parent['run_id'], {
        'command_id': 'handoff-unused-prepared-gate', 'expected_revision': revision})
    current_parent = store.effective_spec(following['run_id'])
    owner = current_parent['feature_delivery']['owner']
    command = command_for(owner)
    command_digest = digest({'run_id': child['run_id'], **command})
    environment.info = replace(environment.info,
        workflow_id=store.active_workflow_id(current_parent['run_id']))
    snapshots = []
    def lose_claim_after_snapshot(*_args, **_kwargs):
        snapshots.append(True)
        if len(snapshots) == 3:  # resume_worker, admit, final post-preparation snapshot
            with store._connect() as db:
                store.state.release_work(db, current_parent['work_id'],
                                         'external:devflow:' + current_parent['run_id'])
        return deepcopy(seal)
    monkeypatch.setattr(gates, 'snapshot', lose_claim_after_snapshot)
    with pytest.raises(ValueError, match='current parent claim'):
        environment.run(activities.reserve, current_parent,
                        child['feature_worker']['chunk_id'], 'build')
    assert len(snapshots) == 3
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE request_digest=?',
                              (command_digest,)).fetchone()
        assert db.execute('SELECT state FROM delivery_outbox WHERE run_id=?',
                          (child['run_id'],)).fetchone()[0] == 'sent'
        store.state.claim_work(db, current_parent['work_id'],
                               'external:devflow:' + current_parent['run_id'],
                               store.config.dashboard_url)
    monkeypatch.setattr(gates, 'snapshot', lambda *_a, **_kw: deepcopy(seal))
    result = environment.run(activities.reserve, current_parent,
                             child['feature_worker']['chunk_id'], 'build')
    execution = result['spec']
    assert result['resumed'] and result['workflow_id'].endswith('transient-gates-retry-1')
    recovery = read_private(root / 'admission.json')
    assert recovery['command'] == command
    assert recovery['command']['additional_iterations'] == 0
    assert recovery['state'] == seal['closed']['result']
    assert recovery['state']['roles'][0]['status'] == 'blocked'
    assert recovery['state']['roles'][0]['session_id'] == 'original-session'
    assert recovery['execution_spec'] == execution == store.effective_spec(child['run_id'])
    rebound = read_private(slot)
    assert rebound['preparation_attempts'] == first['preparation_attempts']
    assert rebound['effective_spec'] == first['effective_spec']
    assert read_private(slot.parent / ('previous-' + digest(first) + '.json')) == first
    assert rebound['command_digest'] == command_digest
    assert old_path.read_bytes() == old_raw
    assert len(probes) == (0 if current_cached else 1)
    worker_environment = ActivityEnvironment()
    worker_environment.info = replace(worker_environment.info,
        activity_type='delivery_gates_readback', workflow_id=result['workflow_id'])
    assert asyncio.run(worker_environment.run(worker_activities.delivery_gates_readback,
        {'spec': execution, 'recovery': recovery}))['number'] == 7
    assert validation == [True]
    assert gates.admit(store, child['run_id'], command)['workflow_id'] == result['workflow_id']
    with store._connect() as db:
        assert db.execute('SELECT state FROM delivery_outbox WHERE run_id=?',
                          (child['run_id'],)).fetchone()[0] == 'pending'
        assert store.state.claim_for(db, parent['work_id'])['owner'] == (
            'external:devflow:' + current_parent['run_id'])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert shared.budget(owner['issue_id']) == budget
