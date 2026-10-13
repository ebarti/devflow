"""Fresh metadata admission is absent; already-admitted commands keep replaying."""
from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app


@pytest.mark.asyncio
@pytest.mark.parametrize('endpoint', ['metadata-preflight', 'reconcile-published-metadata'])
async def test_removed_metadata_routes_cannot_dispatch_or_write(api_fixture, monkeypatch, endpoint):
    path, _ = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    with store._connect() as db:
        before = '\n'.join(db.iterdump())
    calls = []
    monkeypatch.setattr(store, endpoint.replace('-', '_'),
                        lambda *_args: calls.append(_args) or {'accepted': True}, raising=False)
    transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
    async with httpx.AsyncClient(transport=transport, base_url='http://127.0.0.1:18770') as browser:
        session = await browser.get('/api/session')
        response = await browser.post('/api/runs/run-1/' + endpoint, json={}, headers={
            'Origin': 'http://127.0.0.1:18770', 'X-Devflow-CSRF': session.json()['csrf_token'],
        })
    assert response.status_code == 404
    assert calls == []
    with store._connect() as db:
        assert '\n'.join(db.iterdump()) == before


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['c04-metadata-history.json', 'parent96-metadata-history.json'])
async def test_genuine_previous_metadata_history_replays_without_writers(name):
    from temporalio.client import WorkflowHistory
    from temporalio.worker import Replayer

    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    path = Path(__file__).parent / 'fixtures' / 'metadata' / name
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('delivery-run-1-metadata-1', path.read_text()))
