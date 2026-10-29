from __future__ import annotations

import copy
import json

import pytest
import test_delivery_gate_retry as gate_tests
from test_delivery_stopped_resume import project, saved
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json
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
@pytest.mark.asyncio
async def test_fixed_resume_cannot_extend_the_ceiling_or_prepare_runtime(fixed_stop, monkeypatch,
                                                                       iterations):
    store, _, _, command = fixed_stop
    def unexpected(*_args, **_kwargs):
        pytest.fail('a budget extension reached runtime preparation')
    monkeypatch.setattr(resume, 'prepare_runtime', unexpected)
    import httpx

    from devflow_temporal.delivery_api import create_app

    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    exhausted = {**command, 'additional_iterations': iterations}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        preview = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': resume.KIND, 'request': exhausted})
        response = await client.post('/api/runs/run-1/continue-repair', json=exhausted,
                                     headers={'Origin': host, 'X-Devflow-CSRF': token})
        assert preview.status_code == 200 and not preview.json()['eligible']
        assert preview.json()['blockers'][0]['code'] == 'product_budget_exhausted'
        assert response.status_code == 409 and response.json()['detail'] == preview.json()
    with pytest.raises(ValueError, match='fixed repair budget'):
        store.continue_repair('run-1', {**command, 'additional_iterations': iterations})
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_commands').fetchone()[0] == 1
        assert store.state.claim_for(db, 'work-1') is None
