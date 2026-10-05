from __future__ import annotations

import asyncio
import json

import pytest
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_gate_retry import KIND, readback
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def stopped(published, monkeypatch):
    store, broker, state, closed, _, _ = published
    spec = store.spec('run-1')
    state['cleanup'] = 'confirmed'
    state['roles'].append({'role': 'verify', 'iteration': 1, 'status': 'findings',
                           'findings': ['Receipt omits source hash semantics'],
                           'session_id': 'old-qa', 'cleanup': 'confirmed'})
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES "
                   "('old-qa','run-1','verify',1,?,'finished','old-qa',?,'confirmed')",
                   (state['candidate']['id'], canonical_json(state['roles'][-1])))
        db.execute("UPDATE delivery_runs SET cleanup='confirmed',workflow_id='delivery-run-1' "
                   "WHERE run_id='run-1'")
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    monkeypatch.setattr('devflow_temporal.delivery_gate_retry._stopped_cleanup', lambda _: {})
    request = {'continuation_kind': KIND, 'command_id': 'retry-gates-1',
               'expected_revision': 13, 'expected_iteration': 1,
               'expected_candidate_id': state['candidate']['id'],
               'expected_pr_number': 7, 'expected_pr_head': state['candidate']['head'],
               'additional_iterations': 0}
    return store, broker, state, closed, request


def test_same_run_admission_replay_and_unchanged_source(stopped):
    store, broker, state, _, request = stopped
    before = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['implementation_authority'] is False
    result = store.continue_repair('run-1', request)
    assert result['additional_iterations'] == 0
    assert store.continue_repair('run-1', request) == result
    assert broker.candidate() == before
    spec = store.effective_spec('run-1')
    assert spec['policy'] == store.spec('run-1')['policy']
    assert spec['policy']['max_repairs'] == store.spec('run-1')['policy']['max_repairs']
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert db.execute('SELECT count(*) FROM delivery_repair_grants').fetchone()[0] == 0
    assert readback(store, spec, recovery)['number'] == 7
    assert recovery['state']['roles'][-1]['status'] == 'findings'
    assert RunResources(spec).root.name == 'resources'
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, 'command_id': 'retry-gates-2'})
    with pytest.raises(ValueError, match='different inputs'):
        store.continue_repair('run-1', {**request, 'expected_pr_head': 'a' * 40})


@pytest.mark.parametrize('field,value', [('additional_iterations', 1),
                                        ('expected_revision', 12),
                                        ('expected_pr_head', 'a' * 40),
                                        ('expected_candidate_id', 'a' * 64)])
def test_changed_authority_is_rejected_before_admission(stopped, field, value):
    store, _, _, _, request = stopped
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, field: value})
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_gate_admissions').fetchone()


@pytest.mark.parametrize('qa_status', ['pass', 'findings'])
def test_workflow_runs_fresh_gates_without_implementation_or_publication(
    stopped, monkeypatch, qa_status,
):
    store, _, state, _, request = stopped
    store.continue_repair('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append((name, body.get('role')))
        if name == 'delivery_tracker_start':
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert body['role'] in {'review', 'verify'}
            return {'role': body['role'], 'iteration': 1,
                    'status': qa_status if body['role'] == 'verify' else 'pass',
                    'findings': ['Fresh QA failure'] if qa_status != 'pass' else [],
                    'session_id': 'fresh-' + body['role'], 'candidate': recovery['candidate']}
        if name == 'delivery_tracker':
            return {'state': 'consistent'}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*args):
        pass

    async def stop(_spec, reason):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_stop', stop)
    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert result['outcome'] == ('delivered' if qa_status == 'pass' else 'blocked')
    assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']
    assert not any('publish' in name for name, _ in calls)
    assert result['iteration'] == state['iteration']
    assert result['roles'][-3]['status'] == 'findings'
    assert result['checks']['qa']['state'] == ('passed' if qa_status == 'pass' else 'failed')
    if qa_status != 'pass':
        assert not any(name == 'delivery_ci' for name, _ in calls)
        assert result['error'] == 'repair limit exhausted'


def test_fresh_gate_attempt_cannot_reuse_the_failed_qa_job(stopped):
    from devflow_temporal.supervisor import DeliverySupervisor

    store, broker, _, _, request = stopped
    original = store.spec('run-1')
    supervisor = DeliverySupervisor(store, capacity=1)
    body = {'spec': original, 'role': 'verify', 'iteration': 1,
            'candidate': broker.candidate()}
    old_key, _ = supervisor._claim(body)
    # This seam exercises durable attempt identity only; no provider launches.
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts WHERE job_key=?", (old_key,))
    store.continue_repair('run-1', request)
    current = store.effective_spec('run-1')
    new_key, cached = supervisor._claim({**body, 'spec': current})
    assert new_key != old_key
    assert cached is None
