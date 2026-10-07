"""Fresh metadata admission is absent; already-admitted commands keep replaying."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal import delivery_control, delivery_metadata_recovery
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_client import DeliveryClient
from devflow_temporal.delivery_mcp import build_server
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.parametrize('name', ['metadata_preflight', 'reconcile_published_metadata'])
def test_new_metadata_store_and_client_methods_are_absent(service, name):
    store, _ = service
    with store._connect() as db:
        before = '\n'.join(db.iterdump())
    assert not hasattr(DeliveryStore, name)
    assert not hasattr(DeliveryClient, name)
    with pytest.raises(AttributeError):
        getattr(store, name)('run-1', {})
    with store._connect() as db:
        assert '\n'.join(db.iterdump()) == before


@pytest.mark.parametrize('name', ['reconcile', '_range', '_snapshot', '_guard'])
def test_new_metadata_writer_helpers_are_absent(name):
    assert not hasattr(delivery_metadata_recovery, name)


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
async def test_removed_metadata_tools_are_not_advertised_or_forwarded(monkeypatch):
    calls = []
    monkeypatch.setattr('devflow_temporal.delivery_mcp.client', lambda *_args: calls.append(_args))
    server = build_server(Path('/fixture/config'))
    async with create_connected_server_and_client_session(server) as s:
        tools = {tool.name for tool in (await s.list_tools()).tools}
        for name in ('metadata_preflight', 'reconcile_published_metadata'):
            assert name not in tools
            response = await s.call_tool(name, {'run_id': 'run-1', 'request_json': '{}'})
            assert response.isError
    assert calls == []


@pytest.mark.parametrize('command', ['metadata-preflight', 'reconcile-published-metadata'])
def test_removed_metadata_cli_commands_are_rejected_before_client(
    api_fixture, monkeypatch, command,
):
    path, _ = api_fixture
    payload = path.parent / 'request.json'
    payload.write_text(json.dumps({}))
    calls = []
    monkeypatch.setattr(delivery_control, 'api_client', lambda *_args: calls.append(_args))
    monkeypatch.setattr(sys, 'argv', ['devflow-delivery', '--config', str(path), command,
                                    '--id', 'run-1', '--request', str(payload)])
    with pytest.raises(SystemExit) as error:
        delivery_control.main()
    assert error.value.code == 2
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['c04-metadata-history.json', 'parent96-metadata-history.json'])
async def test_genuine_previous_metadata_history_replays_without_writers(name):
    from temporalio.client import WorkflowHistory
    from temporalio.worker import Replayer

    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    path = Path(__file__).parent / 'fixtures' / 'metadata' / name
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('delivery-run-1-metadata-1', path.read_text()))
