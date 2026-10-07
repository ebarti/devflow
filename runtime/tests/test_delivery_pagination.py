"""Bounded historical reads through the real disposable API/store boundary."""
import json

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal.delivery_api import create_app


def seeded_app(api_fixture):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    with store._connect() as db:
        original = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        db.execute("UPDATE delivery_runs SET updated_at='1970-01-01T00:00:00+00:00'")
        for n in range(115):
            spec = json.loads(original['request_json'])
            spec.update(run_id=f'run-{n:03}', work_id=f'work-{n:03}')
            row = {**original, 'run_id': spec['run_id'], 'work_id': spec['work_id'],
                   'request_json': json.dumps(spec), 'updated_at': '2026-01-01T00:00:00+00:00'}
            db.execute('INSERT INTO delivery_runs (' + ','.join(row) + ') VALUES (' +
                       ','.join('?' for _ in row) + ')', tuple(row.values()))
    return app


@pytest.mark.asyncio
async def test_default_api_page_bounds_compaction_and_exposes_all_history(api_fixture, monkeypatch):
    app = seeded_app(api_fixture)
    store = app.state.delivery.store
    compact = store._compact
    seen = []
    monkeypatch.setattr(store, '_compact', lambda row: seen.append(row['run_id']) or compact(row))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,
                                client=('127.0.0.1', 1)),
                                base_url='http://127.0.0.1:18770') as browser:
        first = (await browser.get('/api/runs')).json()
        assert len(first['runs']) == 50
        assert len(seen) == 50
        ids = [row['id'] for row in first['runs']]
        cursor = first['next_cursor']
        while cursor:
            page = (await browser.get('/api/runs', params={'cursor': cursor})).json()
            ids += [row['id'] for row in page['runs']]
            cursor = page['next_cursor']
        assert ids == [f'run-{n:03}' for n in reversed(range(115))] + ['run-1']
        assert len(seen) == 116
        assert (await browser.get('/api/runs/run-000')).status_code == 200


@pytest.mark.asyncio
@pytest.mark.parametrize('query', ['limit=0', 'limit=101', 'cursor=not-a-cursor'])
async def test_api_rejects_invalid_page_controls(api_fixture, query):
    app = seeded_app(api_fixture)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,
                                client=('127.0.0.1', 1)),
                                base_url='http://127.0.0.1:18770') as browser:
        assert (await browser.get('/api/runs?' + query)).status_code in {400, 422}


@pytest.mark.asyncio
async def test_cursor_survives_missing_boundary_and_binds_archive_filter(api_fixture):
    app = seeded_app(api_fixture)
    store = app.state.delivery.store
    first = store.list_runs_page(limit=3)
    boundary = first['runs'][-1]['id']
    with store._connect() as db:
        db.execute('DELETE FROM delivery_runs WHERE run_id=?', (boundary,))
    next_page = store.list_runs_page(limit=3, cursor=first['next_cursor'])
    assert [row['id'] for row in next_page['runs']] == ['run-111', 'run-110', 'run-109']
    with pytest.raises(ValueError, match='archive filter'):
        store.list_runs_page(archived=True, cursor=first['next_cursor'])
    from devflow_temporal.delivery_dashboard import statistics_for

    assert statistics_for(store)['total_runs'] == 115


def test_client_forwards_bounded_page_controls(monkeypatch):
    from urllib.parse import parse_qs, urlsplit

    from devflow_temporal.delivery_client import DeliveryClient

    calls = []
    monkeypatch.setattr(DeliveryClient, '_request', lambda _self, *args: calls.append(args) or {})
    caller = object.__new__(DeliveryClient)
    caller.runs()
    assert calls.pop() == ('GET', '/api/runs')
    caller.runs(limit=7, cursor='cursor+/=', archived=True)
    method, url = calls[0]
    assert method == 'GET'
    assert parse_qs(urlsplit(url).query) == {
        'limit': ['7'], 'cursor': ['cursor+/='], 'archived': ['true'],
    }


@pytest.mark.asyncio
async def test_mcp_exposes_and_forwards_page_controls(api_fixture, monkeypatch):
    from mcp.shared.memory import create_connected_server_and_client_session

    from devflow_temporal import delivery_control
    from devflow_temporal.delivery_client import DeliveryClient
    from devflow_temporal.delivery_mcp import build_server

    calls = []
    monkeypatch.setattr(delivery_control, 'ensure_service_running', lambda _config: None)
    monkeypatch.setattr(DeliveryClient, 'login', lambda _caller: None)
    monkeypatch.setattr(DeliveryClient, 'runs', lambda _caller, **kwargs:
                        calls.append(kwargs) or {'runs': [], 'next_cursor': None})
    server = build_server(api_fixture[0])
    async with create_connected_server_and_client_session(server) as session:
        tool = next(tool for tool in (await session.list_tools()).tools if tool.name == 'list_runs')
        assert tool.inputSchema.get('required', []) == []
        assert set(tool.inputSchema['properties']) == {'limit', 'cursor', 'archived'}
        assert not (await session.call_tool('list_runs', {
            'limit': 7, 'cursor': 'next', 'archived': True})).isError
    assert calls == [{'limit': 7, 'cursor': 'next', 'archived': True}]
