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
@pytest.mark.parametrize('query', ['limit=0', 'cursor=not-a-cursor'])
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
