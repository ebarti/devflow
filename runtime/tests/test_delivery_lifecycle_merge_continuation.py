"""Merge assessment must authenticate the existing continuation owner."""
from contextlib import contextmanager

import httpx
import pytest
from merge_activity_test_helper import activity_call
from temporalio.service import RPCError, RPCStatusCode
from test_delivery_merge_followup import delivered as delivered
from test_delivery_merge_native_preparation import relocated_conflict as relocated_conflict
from test_delivery_merge_prelaunch_continuation import blocked, names
from test_delivery_merge_resolution import conflicted as conflicted
from test_delivery_store import service as service

from devflow_temporal import delivery_merge_execution as execution
from devflow_temporal import delivery_merge_followup as followup
from devflow_temporal import delivery_merge_native_preparation as native
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
async def test_public_merge_assessment_authenticates_leased_prelaunch_without_mutation(
    relocated_conflict, monkeypatch,
):
    b = await blocked(relocated_conflict, monkeypatch)
    d = b.c.d
    command = {**d.command, 'command_id': 'continue-assessed-prelaunch'}
    before, files = _rows(d.store), names(d.spec)
    observed_checks = []
    original_recheck = native._PrelaunchProofs.recheck
    def counted(proofs):
        observed_checks.append(True)
        return original_recheck(proofs)
    monkeypatch.setattr(native._PrelaunchProofs, 'recheck', counted)

    async with _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        preflight = await client.get('/api/runs/run-1/merge-preflight')
        assert preflight.status_code == 200, preflight.text
        observed_checks.clear()
        response = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'merge_run', 'request': command})
        assert response.status_code == 200, response.text
        assert response.json()['eligible'] is True, response.text
        assert len(observed_checks) >= 2
        assert _rows(d.store) == before and names(d.spec) == files

        original = native.prelaunch_receipts
        @contextmanager
        def changed(*args, **kwargs):
            with original(*args, **kwargs) as proof:
                intruder = b.folder / 'unexpected'
                intruder.write_text('changed during observation')
                try:
                    yield proof
                finally:
                    intruder.unlink()
        monkeypatch.setattr(native, 'prelaunch_receipts', changed)
        stale = await client.post('/api/runs/run-1/assess-transition', json={
            'kind': 'merge_run', 'request': command})
        assert stale.status_code == 200 and not stale.json()['eligible']
        assert _rows(d.store) == before and names(d.spec) == files
        monkeypatch.setattr(native, 'prelaunch_receipts', original)

        admitted = await client.post('/api/runs/run-1/merge', json=command, headers=headers)
        assert admitted.status_code == 200, admitted.text
        assert admitted.json()['grant_sha256'] == b.c.receipt['grant_sha256']
        assert (await client.post('/api/runs/run-1/merge', json=command,
                                  headers=headers)).json() == admitted.json()
        with d.store._connect() as db:
            effect = db.execute('SELECT state FROM delivery_effects WHERE effect_key=?',
                                (b.effect['effect_key'],)).fetchone()
            assert effect['state'] == 'complete'


@pytest.mark.asyncio
async def test_first_merge_assessment_respects_existing_installation_fence(delivered):
    from devflow_temporal.delivery_feature_execution import registry_path
    from devflow_temporal.delivery_interrupted_recovery import installation_recovery_lease

    d = delivered
    lease_path = registry_path(d.store.config).parent / 'installation-recovery.lock'
    async with _client(d.store) as client:
        session = (await client.get('/api/session')).json()
        headers = {'X-Devflow-CSRF': session['csrf_token'], 'Origin': d.store.config.dashboard_url}
        # This is a disposable fixture; simulate a genuinely absent witness.
        if lease_path.exists():
            lease_path.unlink()
        before = _rows(d.store)
        request = {'kind': 'merge_run', 'request': d.command}
        missing = await client.post('/api/runs/run-1/assess-transition', json=request)
        assert missing.status_code == 200 and not missing.json()['eligible']
        assert not lease_path.exists()
        assert _rows(d.store) == before

        # A closed-run maintenance owner holds the shared lease while the
        # first merge needs an exclusive admission fence.
        with installation_recovery_lease(d.store.config):
            held = await client.post('/api/runs/run-1/assess-transition', json=request)
            assert held.status_code == 200 and not held.json()['eligible']
            assert 'owner_proof_unknown' in {b['code'] for b in held.json()['blockers']}
            refused = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
            assert refused.status_code in {409, 503}
            assert _rows(d.store) == before

        allowed = await client.post('/api/runs/run-1/assess-transition', json=request)
        assert allowed.status_code == 200 and allowed.json()['eligible'], allowed.text
        assert _rows(d.store) == before
        admitted = await client.post('/api/runs/run-1/merge', json=d.command, headers=headers)
        assert admitted.status_code == 200, admitted.text
