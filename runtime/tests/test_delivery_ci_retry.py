from __future__ import annotations

import asyncio
import json

import pytest
from test_delivery_gate_retry import stopped as stopped
from test_delivery_metadata_recovery import published as published
from test_delivery_store import service as service

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_gate_retry import CI_KIND
from devflow_temporal.delivery_resources import RunResources
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def waiting_ci(stopped):
    store, broker, state, closed, request = stopped
    candidate = state['candidate']
    state['error'] = 'required CI did not confirm this PR head'
    state['roles'].extend({'role': role, 'iteration': state['iteration'], 'status': 'pass',
                           'session_id': 'passed-' + role, 'candidate': candidate,
                           'cleanup': 'confirmed'} for role in ('review', 'verify'))
    state['checks'] = {key: {'state': 'passed', 'candidate_id': candidate['id']}
                       for key in ('review', 'qa', 'local')}
    state['checks']['ci'] = {'state': 'failed', 'failed': ['Build'], 'head': candidate['head']}
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET error=?,checks_json=? WHERE run_id='run-1'",
                   (state['error'], canonical_json(state['checks'])))
    request.update(continuation_kind=CI_KIND, command_id='retry-ci-1')
    return store, broker, state, closed, request


def test_ci_admission_retains_exact_passes_and_failed_history(waiting_ci):
    store, broker, state, _, request = waiting_ci
    before = broker.candidate()
    assert store.repair_admission_preflight('run-1', request)['additional_iterations'] == 0
    result = store.continue_repair('run-1', request)
    assert result['workflow_id'] == 'delivery-run-1-ci-retry-1'
    assert store.continue_repair('run-1', request) == result
    spec = store.effective_spec('run-1')
    assert spec['policy'] == store.spec('run-1')['policy']
    assert RunResources(spec).root.is_relative_to(store.config.state_root)
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id='run-1'"
        ).fetchone()[0])
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert recovery['state']['roles'] == state['roles']
    assert recovery['state']['checks']['ci']['state'] == 'failed'
    assert broker.candidate() == before
    with pytest.raises(ValueError):
        store.continue_repair('run-1', {**request, 'command_id': 'second-ci-retry'})


@pytest.mark.parametrize('drift', ['qa', 'candidate', 'session', 'local', 'ci', 'unknown'])
def test_ci_admission_rejects_missing_candidate_or_independent_pass(waiting_ci, drift):
    store, _, state, _, request = waiting_ci
    if drift == 'qa':
        state['roles'][-1]['status'] = 'findings'
    elif drift == 'candidate':
        state['roles'][-1]['candidate'] = {'id': 'b' * 64}
    elif drift == 'session':
        state['roles'][-1]['session_id'] = state['roles'][-2]['session_id']
    elif drift == 'unknown':
        state['cleanup'] = 'unknown'
    else:
        state['checks'][drift]['state'] = 'unknown'
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                       (canonical_json(state['checks']),))
    with pytest.raises(ValueError):
        store.continue_repair('run-1', request)


@pytest.mark.parametrize('ci_passes', [True, False])
def test_ci_workflow_observes_only_ci_then_normal_finalization(waiting_ci, monkeypatch, ci_passes):
    store, _, old, _, request = waiting_ci
    store.continue_repair('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    flow = DeliveryWorkflow()
    calls = []

    async def execute(name, body, **kw):
        calls.append(name)
        if name == 'delivery_ci':
            assert body['pull_request']['head'] == old['candidate']['head']
            return {'state': 'passed' if ci_passes else 'failed'}
        return {'state': 'consistent'}

    async def project(spec, event, message):
        calls.append('project:' + event)

    async def stop(spec, reason):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_stop', stop)
    result = asyncio.run(flow._resume_published_gates(spec, recovery))
    assert result['outcome'] == ('delivered' if ci_passes else 'blocked')
    assert result['roles'] == old['roles']
    assert result['checks']['local'] == old['checks']['local']
    assert not any(name in calls for name in (
        'delivery_role', 'delivery_checks', 'delivery_precheck', 'delivery_publish'))
    assert ('project:delivered' in calls) == ci_passes
