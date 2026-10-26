"""Lifecycle assessment agrees with owner admission at the public API."""
import httpx
import pytest
from test_delivery_gates_admission import stopped as stopped
from test_delivery_merge_followup import delivered as delivered
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app


async def _client(store):
    app = create_app(store.config.path)
    app.state.delivery.store = store
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url=store.config.dashboard_url)


@pytest.mark.asyncio
async def test_merge_assessment_and_execution_agree_and_keep_legacy_replay(delivered, monkeypatch):
    d = delivered
    calls = []
    original = d.store._completed_temporal_result
    def observed(*args, **kwargs):
        calls.append(kwargs.get('preserve', True))
        return original(*args, **kwargs)
    monkeypatch.setattr(d.store, '_completed_temporal_result', observed)
    async with await _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        async def assess(request):
            response = await client.post('/api/runs/run-1/assess-transition', json={
                'kind': 'merge_run', 'request': request})
            assert response.status_code == 200
            return response.json()
        good = await assess(d.command)
        assert good['eligible'] and good['proof_digest']
        assert calls == [False]
        for field, code, value in [('head', 'candidate_head_changed', 'stale'),
                                   ('candidate_id', 'candidate_changed', 'stale'),
                                   ('expected_revision', 'revision_changed', 6)]:
            stale = {**d.command, field: value}
            assessed = await assess(stale)
            assert not assessed['eligible']
            assert code in {b['code'] for b in assessed['blockers']}
            refused = await client.post('/api/runs/run-1/merge', json=stale, headers=headers)
            assert refused.status_code == 409
            assert code in {b['code'] for b in refused.json()['detail']['blockers']}
        with d.store._connect() as db:
            assert db.execute('SELECT count(*) FROM delivery_merge_requests').fetchone()[0] == 0
        admitted = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
        assert admitted.status_code == 200, admitted.text
        assert calls[-1] is True
        replay = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
        assert replay.json() == admitted.json()


@pytest.mark.asyncio
async def test_gates_assessment_requires_owner_authority_and_matches_execution(
    stopped, monkeypatch,
):
    store, _, _, command = stopped
    calls = []
    original = store._completed_temporal_result
    def observed(*args, **kwargs):
        calls.append(kwargs.get('preserve', True))
        return original(*args, **kwargs)
    monkeypatch.setattr(store, '_completed_temporal_result', observed)
    async with await _client(store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': store.config.dashboard_url}
        async def assess(request):
            return (await client.post('/api/runs/run-1/assess-transition', json={
                'kind': 'investigation_gates_only', 'request': request})).json()
        good = await assess(command)
        assert good['eligible'] and good['proof_digest']
        assert calls == [False]
        missing = await assess({'precheck_sha256': command['precheck_sha256']})
        assert not missing['eligible']
        assert 'gates_authority_unknown' in {b['code'] for b in missing['blockers']}
        stale = {**command, 'precheck_sha256': '0' * 64}
        assessment = await assess(stale)
        assert 'custody_changed' in {b['code'] for b in assessment['blockers']}
        refused = await client.post('/api/runs/run-1/admit-gates-only', json=stale, headers=headers)
        assert refused.status_code == 409
        assert 'custody_changed' in {b['code'] for b in refused.json()['detail']['blockers']}
        with store._connect() as db:
            assert db.execute('SELECT count(*) FROM delivery_gate_admissions').fetchone()[0] == 0
        admitted = await client.post('/api/runs/run-1/admit-gates-only',
                                     json=command, headers=headers)
        assert admitted.status_code == 200, admitted.text
        assert calls[-1] is True
        assert (await client.post('/api/runs/run-1/admit-gates-only', json=command,
                                  headers=headers)).json() == admitted.json()


@pytest.mark.asyncio
async def test_merge_current_owner_conflict_cannot_acquire_a_grant(delivered):
    d = delivered
    with d.store._connect() as db:
        d.store.state.claim_work(db, d.spec['work_id'], 'external:other', 'http://other')
    async with await _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        assessed = (await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'merge_run', 'request': d.command})).json()
        assert not assessed['eligible']
        assert 'owner_proof_unknown' in {b['code'] for b in assessed['blockers']}
        refused = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
        assert refused.status_code == 409
    with d.store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_merge_requests').fetchone()[0] == 0
        assert d.store.state.claim_for(db, d.spec['work_id'])['owner'] == 'external:other'


@pytest.mark.asyncio
async def test_gates_native_readiness_unknown_never_grants(stopped, monkeypatch):
    from devflow_temporal import delivery_native_renewal

    store, _, _, command = stopped
    def unavailable(*_args):
        raise ValueError('unavailable test proof')

    monkeypatch.setattr(delivery_native_renewal, 'readiness', unavailable)
    async with await _client(store) as client:
        assessed = (await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'investigation_gates_only', 'request': command})).json()
        assert not assessed['eligible']
        assert 'native_readiness_unknown' in {b['code'] for b in assessed['blockers']}
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_gate_admissions').fetchone()[0] == 0
