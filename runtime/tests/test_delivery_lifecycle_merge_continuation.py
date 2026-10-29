"""Merge assessment must authenticate the existing continuation owner."""
from contextlib import contextmanager

import httpx
import pytest
from merge_activity_test_helper import activity_call
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_merge_followup import delivered as delivered
from test_delivery_merge_native_preparation import relocated_conflict as relocated_conflict
from test_delivery_merge_resolution import conflicted as conflicted
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_execution as execution
from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal.delivery_api import create_app


def _rows(store):
    with store._connect() as db:
        return {
            table: [tuple(row) for row in db.execute(f'SELECT * FROM {table} ORDER BY rowid')]
            for table in ('delivery_commands', 'delivery_effects', 'delivery_attempts',
                          'delivery_merge_requests', 'delivery_mutations')
        }


def _client(store):
    app = create_app(store.config.path)
    app.state.delivery.store = store
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                             base_url=store.config.dashboard_url)


@pytest.mark.asyncio
@pytest.mark.parametrize('blocked_by', ['active_lease', 'unclosed_predecessor', 'transport'])
async def test_public_merge_continuation_requires_closed_unleased_predecessor(
    delivered, monkeypatch, blocked_by,
):
    d = delivered
    followup.admit(d.store, 'run-1', d.command)
    activity_call(followup.record, d.store, d.spec, 'decision_required', {
        'state': 'decision_required', 'reason': 'conflicting_upstream_changes'})
    command = {**d.command, 'command_id': 'continue-after-owner'}
    before = _rows(d.store)
    if blocked_by == 'unclosed_predecessor':
        def unclosed(*_args):
            raise ValueError('controlled predecessor is still running')
        monkeypatch.setattr(execution, 'predecessor_closure', unclosed)
    elif blocked_by == 'transport':
        def unavailable(*_args):
            raise RPCError('controlled outage', RPCStatusCode.UNAVAILABLE, b'')
        monkeypatch.setattr(execution, 'predecessor_closure', unavailable)

    @contextmanager
    def owner():
        if blocked_by == 'active_lease':
            with execution.existing_lease(d.store.config):
                yield
        else:
            yield

    async with _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        with owner():
            result = await client.post('/api/runs/run-1/assess-transition', json={
                'kind': 'merge_run', 'request': command})
            assert result.status_code == 200
            assert result.json()['eligible'] is False
            assert 'owner_proof_unknown' in {b['code'] for b in result.json()['blockers']}
            preflight = await client.get('/api/runs/run-1/merge-preflight')
            assert preflight.status_code == 409
            refused = await client.post('/api/runs/run-1/merge', json=command, headers=headers)
            assert refused.status_code == 409
        assert _rows(d.store) == before


@pytest.mark.asyncio
async def test_delivered_predecessor_transport_failure_is_an_unknown_assessment(
    delivered, monkeypatch,
):
    from devflow_temporal.delivery_interrupted_recovery import installation_recovery_lease

    d = delivered
    with installation_recovery_lease(d.store.config):
        pass
    def unavailable(*_args, **_kwargs):
        raise RPCError('controlled outage', RPCStatusCode.UNAVAILABLE, b'')
    monkeypatch.setattr(d.store, '_completed_temporal_result', unavailable)
    before = _rows(d.store)
    async with _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        response = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'merge_run', 'request': d.command})
        assert response.status_code == 200 and not response.json()['eligible']
        assert 'owner_proof_unknown' in {b['code'] for b in response.json()['blockers']}
        refused = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
        assert refused.status_code == 409
        assert _rows(d.store) == before
