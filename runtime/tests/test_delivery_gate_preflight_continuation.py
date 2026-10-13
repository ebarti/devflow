"""Public zero-writer rearm; native attribution is a named transport/proof seam."""
from __future__ import annotations

import asyncio
import hashlib
import json
import subprocess
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_gate_retry import published as published
from test_delivery_gate_retry import service as service
from test_delivery_gate_retry import stopped as stopped
from test_delivery_store import _git

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal import delivery_gate_attribution_recovery as attribution
from devflow_temporal import delivery_gate_preflight_continuation as preflight
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_native_process
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_policy_recovery import _rows
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    read_private,
    write_private,
)
from devflow_temporal.delivery_workflow import DeliveryWorkflow


def failed_assessment(seed, monkeypatch):
    store, broker, old_state, old_closed, command = seed
    run_id = old_state["run_id"]
    original_snapshot = gates.snapshot
    seal = original_snapshot(store, run_id)
    proof = {'cause': 'wrapped_junit_source_attribution', 'unresolved_findings': ['Unpassed work']}
    seal.update(generation=2, stage='published', attribution_observation=proof)
    seal['cleanup']['journal_sha256'] = {}
    # This fake-provider fixture models already authenticated original native
    # proof. Actual compact/full native history is exercised by the archive replay.
    monkeypatch.setattr(gates, 'snapshot', lambda *_a, **_kw: deepcopy(seal))
    command = {**command, 'command_id': 'original-admitted-assessment'}
    response = store.continue_repair(run_id, command)
    store.mark_start(run_id, accepted=True)
    monkeypatch.setattr(gates, 'snapshot', original_snapshot)
    monkeypatch.setattr(attribution, 'observe', lambda *_a, **_kw: deepcopy(proof))
    spec = store.effective_spec(run_id)
    root = Path(spec['state_dir'])
    admission = root / 'published-gates-admission-2/admission.json'
    original_bytes = admission.read_bytes()
    recovery = json.loads(original_bytes)
    monkeypatch.setattr(delivery_native_process, 'process_table', lambda: {})
    # Fake-provider finished attempts have no native journals. Exercise the real
    # empty actor/resource observer; all actual native attempts run in the archive proof.
    def native_observation(given, _attempts):
        assert given['provider'] == 'fake'
        return observe_completed_resources(given, [])
    monkeypatch.setattr(preflight, 'observe_completed_resources', native_observation)
    monkeypatch.setattr(retry, 'observe_completed_resources', native_observation)
    # Represent the original buggy finalizer's unconfirmed roots. The real
    # corrected observer/finalizer below must observe and close this receipt.
    receipt = RunResources(spec).finalize('blocked')
    receipt.update(state='unknown', resource_cleanup='unknown')
    raw_receipt = {k: v for k, v in receipt.items() if k not in {'receipt', 'receipt_sha256'}}
    write_private(root / 'resources/finalization.json', raw_receipt)
    manifest = read_private(root / 'resources/manifest.json')
    manifest['finalization'] = raw_receipt
    write_private(root / 'resources/manifest.json', manifest)
    receipt['receipt_sha256'] = hashlib.sha256(
        (root / 'resources/finalization.json').read_bytes()).hexdigest()
    state = deepcopy(recovery['state'])
    state.update(run_id=run_id, phase='blocked', execution_state='blocked', outcome='blocked',
                 cleanup='unknown', candidate=recovery['candidate'],
                 pull_request=recovery['publication'], revision=old_state['revision'] + 1,
                 error='gate retry preflight failed: ActivityError')
    state['checks'] = {'failure': {'classification': 'terminal', 'stage': 'gates_retry',
                                  'reason': state['error']}, 'resource_cleanup': receipt}
    closed = {'workflow_id': response['workflow_id'], 'execution_run_id': 'closed-unstarted',
              'closed_at': '2026-10-13T00:00:00+00:00',
              'request_digest': spec['request_digest'], 'recovery_digest': digest(recovery),
              'result': state}
    store.project(run_id, phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='unknown', error=state['error'], checks=state['checks'],
                  candidate=state['candidate'], pull_request=state['pull_request'],
                  iteration=state['iteration'], protocol_revision=state['revision'],
                  event_type='blocked', message=state['error'])
    monkeypatch.setattr(store, '_completed_temporal_result', lambda _run, *, workflow_id=None:
                        deepcopy(closed if workflow_id == closed['workflow_id'] else old_closed))
    # Modern local tracking uses its real producer. Only the legacy remote
    # transport is doubled; both paths release the actual owned claim.
    def track(_spec, _status, **_kw):
        if _spec.get('project_sync_version') == 1:
            from devflow_temporal.delivery_features import record_tracking

            return record_tracking(store, _spec, _status, _kw['release'])
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:' + run_id)
        return {'state': 'consistent', 'pending': False}
    monkeypatch.setattr(retry, '_tracker_sync', track)
    row, *_ = _rows(store, run_id)
    return SimpleNamespace(store=store, spec=spec, row=row, closed=closed, recovery=recovery,
                           original_bytes=original_bytes, admission=admission, old_command=command,
                           old_response=response, broker=broker)


@pytest.fixture
def failed(stopped, monkeypatch):
    return failed_assessment(stopped, monkeypatch)


def close_and_request(fixture):
    updated = retry._close_published_cleanup(
        fixture.store, fixture.row, fixture.spec, lambda: False)
    assert updated['cleanup'] == 'confirmed'
    assert fixture.admission.read_bytes() == fixture.original_bytes
    assert fixture.closed['result']['cleanup'] == 'unknown'
    return {**fixture.old_command, 'command_id': 'continue-unused-assessment',
            'expected_revision': fixture.closed['result']['revision']}


def test_public_unstarted_assessment_continues_once_without_a_writer(failed, monkeypatch):
    request = close_and_request(failed)
    store = failed.store
    before = deepcopy(failed.closed)
    assert store.repair_admission_preflight('run-1', request)['implementation_authority'] is False
    response = store.continue_repair('run-1', request)
    assert store.continue_repair('run-1', request) == response
    assert store.continue_repair('run-1', failed.old_command) == failed.old_response
    spec = store.effective_spec('run-1')
    assert spec['gate_retry_generation'] == 2 and spec['gate_retry_preflight_generation'] == 1
    assert response['workflow_id'].endswith('-preflight-continuation')
    assert response['workflow_id'] != failed.old_response['workflow_id']
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs '
                                        "WHERE run_id='run-1'").fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['original_recovery'] == failed.recovery
    assert failed.admission.read_bytes() == failed.original_bytes
    assert failed.closed == before
    assert gates.readback(store, spec, recovery)['number'] == 7
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, 'command_id': 'second-distinct-continuation'})
    with pytest.raises(ValueError, match='different inputs'):
        store.continue_repair('run-1', {**request,
                                      'expected_iteration': request['expected_iteration'] + 1})
    original_session = recovery['state']['roles'][0]['session_id']
    flow, calls = DeliveryWorkflow(), []
    async def activity(name, body, **_kw):
        calls.append(name)
        if name == 'delivery_gates_readback':
            return gates.readback(store, body['spec'], body['recovery'])
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            assert 'Unpassed work' in body['findings'][0]
            return {'role': body['role'], 'status': 'pass',
                    'iteration': request['expected_iteration'],
                    'session_id': 'fresh-' + body['role'], 'cleanup': 'confirmed',
                    'candidate': recovery['candidate']}
        return {'state': 'passed', 'cleanup': 'confirmed'}
    async def project(*_a, **_kw):
        pass
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert result['outcome'] == 'delivered' and result['iteration'] == request['expected_iteration']
    assert result['roles'][0]['session_id'] == original_session
    assert 'delivery_publish' not in calls
    old_roles = failed.closed['result']['roles']
    assert result['roles'][:len(old_roles)] == old_roles


def test_changed_source_owner_or_progress_refuses_before_cleanup_and_queue(failed):
    store, spec = failed.store, failed.spec
    raw = failed.admission.read_bytes()
    with pytest.raises(ValueError, match='closed, finalized|unstarted|checkpoint'):
        gates.snapshot(store, 'run-1')  # Retained own claim and unknown cleanup cannot admit.
    original_branch = _git(Path(spec['checkout']), 'branch', '--show-current')
    _git(Path(spec['checkout']), 'checkout', '-qb', 'foreign-branch')
    with pytest.raises((ValueError, subprocess.SubprocessError)):
        retry._close_published_cleanup(store, failed.row, spec, lambda: False)
    _git(Path(spec['checkout']), 'checkout', original_branch)
    origin = _git(Path(spec['checkout']), 'remote', 'get-url', 'origin')
    _git(Path(spec['checkout']), 'remote', 'set-url', 'origin', str(Path(origin).parent))
    with pytest.raises((ValueError, subprocess.SubprocessError)):
        retry._close_published_cleanup(store, failed.row, spec, lambda: False)
    _git(Path(spec['checkout']), 'remote', 'set-url', 'origin', origin)
    assert failed.admission.read_bytes() == raw
    with store._connect() as db:
        assert store.state.claim_for(db, spec['work_id'])
        assert not db.execute("SELECT 1 FROM delivery_commands WHERE "
                              "command_id='continue-unused-assessment'").fetchone()
    state = failed.closed['result']
    state['roles'].append({'role': 'review', 'status': 'findings'})
    with pytest.raises(ValueError, match='unstarted checkpoint'):
        retry._close_published_cleanup(store, failed.row, spec, lambda: False)


def test_resume_rechecks_generation_runtime_and_original_closure(failed, monkeypatch):
    request = close_and_request(failed)
    failed.store.continue_repair('run-1', request)
    spec = failed.store.effective_spec('run-1')
    with failed.store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs '
                                        "WHERE run_id='run-1'").fetchone()[0])
    for change in ({'gate_retry_generation': 3}, {'gate_retry_stage': 'report'},
                   {'gate_retry_preflight_generation': 2}, {'plan_approval': 'changed'}):
        with pytest.raises(ValueError, match='assessment authority'):
            preflight.validate(failed.store, {**spec, **change}, recovery)
    monkeypatch.setattr(failed.store, '_completed_temporal_result',
                        lambda *_a, **_kw: (_ for _ in ()).throw(ValueError('closure unavailable')))
    with pytest.raises(ValueError, match='closure unavailable'):
        gates.readback(failed.store, spec, recovery)


def test_public_parent_handoff_keeps_exhausted_budget_and_original_worker(service, monkeypatch):
    from test_delivery_feature_budget_resume import _attempt
    from test_delivery_feature_integration import integration, role_result

    from devflow_temporal import delivery_feature_activities as activities
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_feature_execution import continue_feature, registry, worker_key
    from devflow_temporal.delivery_store import DeliveryStore

    # Admit the fixture's advancing integration trunk without a pinned base.
    original, supplied = service
    raw = deepcopy(original.config.raw)
    for repository in raw['repositories'].values():
        repository.pop('expected_base_sha', None)
    original.config.path.write_text(json.dumps(raw))
    service = DeliveryStore(DeliveryConfig.load(original.config.path)), supplied
    store, parent, child, broker, candidate, member, _ = integration(service, monkeypatch)
    implemented = role_result(child, broker, candidate)
    def owned(*_a, **_kw):
        head = _git(broker.source, 'ls-remote', 'origin',
                    'refs/heads/' + child['branch']).split()[0]
        return {'number': 20, 'url': member['url'], 'state': 'OPEN', 'headRefOid': head,
                'isDraft': False, 'baseRefName': 'main', 'title': 'feat: original chunk'}
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', owned)
    publication = broker.publish(0, implemented['candidate'])
    _git(broker.checkout, 'checkout', '-B', child['branch'])
    roles = [{'role': 'implement', 'iteration': 0, 'status': 'pass', 'cleanup': 'confirmed',
              'session_id': 'original-implementation', 'candidate': publication['candidate']},
             {'role': 'review', 'iteration': 0, 'status': 'findings', 'cleanup': 'confirmed',
              'session_id': 'historical-review', 'findings': ['Unpassed work']}]
    for role in roles:
        _attempt(store, child, role, publication['candidate']['id'])
    state = {'run_id': child['run_id'], 'revision': 13, 'iteration': 0, 'phase': 'blocked',
             'execution_state': 'blocked', 'outcome': 'blocked', 'cleanup': 'confirmed',
             'candidate': publication['candidate'], 'pull_request': publication, 'roles': roles,
             'checks': {}, 'error': 'repair limit exhausted', 'usage': {}, 'findings': []}
    store.project(child['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='confirmed', error=state['error'], candidate=state['candidate'],
                  pull_request=publication, iteration=0, protocol_revision=13,
                  event_type='blocked', message=state['error'])
    workflow_id = store.active_workflow_id(child['run_id'])
    old_closed = {'workflow_id': workflow_id, 'execution_run_id': 'historical-worker',
                  'request_digest': child['request_digest'], 'recovery_digest': None,
                  'result': state}
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: old_closed)
    monkeypatch.setattr(gates, '_stopped_cleanup', lambda _: {})
    command = {'continuation_kind': gates.KIND, 'command_id': 'seed-original-assessment',
               'expected_revision': 13, 'expected_iteration': 0,
               'expected_candidate_id': state['candidate']['id'], 'expected_pr_number': 20,
               'expected_pr_head': state['candidate']['head'], 'additional_iterations': 0}
    fixture = failed_assessment((store, broker, state, old_closed, command), monkeypatch)
    close_and_request(fixture)
    shared, token = registry(parent), parent['feature_delivery']['owner']
    for cycle in range(10):
        shared.repair(token, 'prior-product-cycle:' + str(cycle), 'Original product repair')
    shared.finish_worker(token, worker_key(child['run_id'], token), {'cleanup': 'confirmed'})
    shared.stop(token, 'stopped', {})
    with store._connect() as db:
        store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
    store.project(parent['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='confirmed', event_type='blocked', message='Worker retained')
    parent_revision = _rows(store, parent['run_id'])[0]['revision']
    parent_command = {'command_id': 'continue-existing-parent',
                      'expected_revision': parent_revision}
    # Parent and child Temporal transport is modeled, never cancelled or reset.
    child_closed = deepcopy(fixture.closed)
    monkeypatch.setattr(store, '_completed_temporal_result', lambda run_id, **kw:
                        child_closed if run_id == child['run_id'] and kw.get('workflow_id') ==
                        child_closed['workflow_id'] else old_closed if run_id == child['run_id']
                        else {'result': {}})
    accepted = continue_feature(store, parent['run_id'], parent_command)
    assert continue_feature(store, parent['run_id'], parent_command) == accepted
    successor = store.effective_spec(accepted['run_id'])
    row, *_ = _rows(store, child['run_id'])
    resumed = activities.resume_worker(store, successor, store.effective_spec(child['run_id']), row)
    assert resumed['resumed'] and resumed['spec']['run_id'] == child['run_id']
    assert resumed['spec']['gate_retry_generation'] == 2
    assert resumed['spec']['gate_retry_preflight_generation'] == 1
    assert gates.readback(store, resumed['spec'], json.loads(
        _rows(store, child['run_id'])[0]['recovery_json']))['number'] == 20
    assert shared.budget(token['issue_id'])['used'] == 10
    assert fixture.admission.read_bytes() == fixture.original_bytes
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
