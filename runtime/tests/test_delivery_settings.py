from __future__ import annotations

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_settings import read_repository_access, save_repository_access
from devflow_temporal.delivery_store import DeliveryStore


def test_repository_access_rejects_stale_unknown_or_malformed_edits(service):
    store, _ = service
    save_repository_access(store, {'expected_revision': 0, 'allowed_repositories': []})
    with pytest.raises(ValueError, match='changed'):
        save_repository_access(store, {'expected_revision': 0,
                                      'allowed_repositories': ['example/fixture']})
    with pytest.raises(ValueError, match='not registered'):
        save_repository_access(store, {'expected_revision': 1,
                                      'allowed_repositories': ['elsewhere/repo']})
    for malformed in (None, [], {}, {'expected_revision': True, 'allowed_repositories': []},
                      {'expected_revision': 1, 'allowed_repositories': [None]}):
        with pytest.raises(ValueError, match='invalid'):
            save_repository_access(store, malformed)
    assert read_repository_access(store)['revision'] == 1


@pytest.mark.asyncio
async def test_settings_save_requires_same_origin_csrf_and_readback_persists(api_fixture):
    path, _ = api_fixture
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
    async with httpx.AsyncClient(transport=transport, base_url='http://127.0.0.1:18770') as client:
        body = {'expected_revision': 0, 'allowed_repositories': []}
        assert (await client.post('/api/settings/repositories', json=body)).status_code == 403
        session = await client.get('/api/session')
        headers = {'Origin': 'http://127.0.0.1:18770',
                   'X-Devflow-CSRF': session.json()['csrf_token']}
        foreign = await client.post('/api/settings/repositories', json=body,
                                    headers={**headers, 'Origin': 'https://evil.invalid'})
        assert foreign.status_code == 403
        response = await client.post('/api/settings/repositories', json=body, headers=headers)
        assert response.status_code == 200
        assert response.json()['repositories'][0]['allowed'] is False
        assert read_repository_access(DeliveryStore(app.state.delivery.config)) == response.json()
        stale = await client.post('/api/settings/repositories', json=body, headers=headers)
        assert stale.status_code == 409
