from __future__ import annotations

import copy

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_settings import read_repository_access, save_repository_access
from devflow_temporal.delivery_store import DeliveryStore


def test_repository_permissions_are_deduplicated_durable_and_do_not_change_runs(service):
    store, request = service
    store.config.raw['repositories']['another-profile'] = copy.deepcopy(
        store.config.raw['repositories']['fixture'])
    store.config.raw['repositories']['another-profile']['github_repo'] = 'EXAMPLE/fixture'
    before = copy.deepcopy(store.config.raw)
    receipt = store.submit(request)
    original = store.submitted_spec('run-1')
    access = read_repository_access(store)
    assert len(access['repositories']) == 1
    assert access['repositories'][0]['allowed'] is True
    disabled = save_repository_access(store, {'expected_revision': 0, 'allowed_repositories': []})
    assert disabled['revision'] == 1
    assert disabled['repositories'][0]['allowed'] is False
    reloaded = DeliveryStore(store.config)
    assert read_repository_access(reloaded) == disabled
    assert store.submit(request) == receipt
    assert store.submitted_spec('run-1') == original
    assert store.config.raw == before
    new = {**request, 'run_id': 'run-2', 'work_id': 'work-2', 'command_id': 'command-2',
           'issue_url': 'https://github.com/example/fixture/issues/4'}
    with pytest.raises(ValueError, match='disabled in Settings'):
        reloaded.submit(new)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_commands').fetchone()[0] == 1
    save_repository_access(reloaded, {'expected_revision': 1,
                                     'allowed_repositories': ['example/fixture']})
    assert reloaded.submit(new)['existing'] is False
    assert store.submitted_spec('run-1') == original


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


def test_same_service_config_overlays_share_permissions_but_other_owners_do_not(service):
    store, _ = service
    save_repository_access(store, {'expected_revision': 0, 'allowed_repositories': []})
    overlay = DeliveryStore(DeliveryConfig(store.config.path, copy.deepcopy(store.config.raw)))
    assert read_repository_access(overlay)['repositories'][0]['allowed'] is False
    other_raw = copy.deepcopy(store.config.raw)
    other_raw['state_root'] = str(store.config.state_root.parent / 'other-owner')
    other = DeliveryStore(DeliveryConfig(store.config.path, other_raw))
    assert read_repository_access(other)['repositories'][0]['allowed'] is True


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


def test_permission_is_rechecked_after_slow_admission_validation(service, monkeypatch):
    store, request = service
    admit = DeliveryConfig.admit

    def revoke_during_validation(config, supplied, **kwargs):
        spec = admit(config, supplied, **kwargs)
        save_repository_access(store, {'expected_revision': 0, 'allowed_repositories': []})
        return spec

    monkeypatch.setattr(DeliveryConfig, 'admit', revoke_during_validation)
    with pytest.raises(ValueError, match='disabled in Settings'):
        store.submit(request)
    assert store.list_runs() == []
