"""The public recovery owner remains authoritative over every assessment."""
from __future__ import annotations

import pytest
from test_delivery_gate_retry import service, unpublished  # noqa: F401
from test_delivery_stopped_resume import stopped  # noqa: F401

from devflow_temporal.delivery_lifecycle import RecoveryRefused, assess_transition


@pytest.mark.parametrize('fixture_name', ['unpublished', 'stopped'])
def test_assessment_and_execution_agree_on_exact_checkpoint(request, fixture_name):
    store, broker, _, command = request.getfixturevalue(fixture_name)
    kind = command['continuation_kind']
    observed = assess_transition(store, 'run-1', kind, command)
    assert observed['eligible'] is True
    assert observed['proof_digest'] and observed['candidate']['id'] == broker.candidate()['id']
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                              (command['command_id'],)).fetchone()
    response = store.continue_repair('run-1', command)
    assert response['run_id'] == 'run-1'
    assert store.continue_repair('run-1', command) == response
    assert not assess_transition(store, 'run-1', kind, command)['eligible']


@pytest.mark.parametrize('fixture_name', ['unpublished', 'stopped'])
@pytest.mark.parametrize('field,code,bad', [
    ('expected_revision', 'revision_changed', 10),
    ('expected_candidate_id', 'candidate_changed', 'a' * 64),
    ('expected_candidate_head', 'candidate_head_changed', 'a' * 40),
])
def test_stale_assessment_and_execution_report_same_predicate(
    request, fixture_name, field, code, bad,
):
    store, _, _, command = request.getfixturevalue(fixture_name)
    changed = {**command, field: bad}
    assessment = assess_transition(store, 'run-1', command['continuation_kind'], changed)
    assert not assessment['eligible']
    assert code in [blocker['code'] for blocker in assessment['blockers']]
    with pytest.raises(RecoveryRefused) as failure:
        store.continue_repair('run-1', changed)
    assert failure.value.assessment == assessment
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                              (command['command_id'],)).fetchone()


def test_owner_custody_failure_stays_unknown(stopped):  # noqa: F811
    store, broker, _, command = stopped
    with store._connect() as db:
        store.state.claim_work(db, broker.spec['work_id'], 'foreign-owner', 'foreign')
    assessment = assess_transition(store, 'run-1', command['continuation_kind'], command)
    assert not assessment['eligible'] and assessment['proof_digest'] is None
    assert assessment['blockers'][0]['code'] == 'owner_proof_unknown'
    with pytest.raises(ValueError):
        store.continue_repair('run-1', command)


@pytest.mark.asyncio
async def test_http_preflight_and_execution_expose_same_refusal(unpublished):  # noqa: F811
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, _, _, command = unpublished
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        stale = {**command, 'expected_revision': command['expected_revision'] + 1}
        base = '/api/runs/run-1/'
        assessed = await client.post(base + 'assess-transition', json={
            'kind': command['continuation_kind'], 'request': stale})
        preflight = await client.post(base + 'repair-admission-preflight', json=stale)
        execution = await client.post(base + 'continue-repair', json=stale, headers=headers)
        assert assessed.status_code == 200
        assert preflight.status_code == execution.status_code == 409
        assert assessed.json() == preflight.json()['detail'] == execution.json()['detail']
        assert assessed.json()['blockers'][0]['code'] == 'revision_changed'
