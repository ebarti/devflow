"""Feature scope preview uses the same retained authority as execution."""

import copy
from pathlib import Path

import httpx
import pytest
from test_delivery_stopped_scope_resume import stopped_feature_scope as stopped_feature_scope
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_github_contract import GitHubDelivery


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["valid", "stale_candidate", "stale_owner", "changed_scope"])
async def test_public_scope_preview_and_execution_keep_authority(
    stopped_feature_scope, monkeypatch, case
):
    store, parent, _child, broker, _state, command = stopped_feature_scope
    shared = registry(parent)
    issue = parent['feature_delivery']['owner']['issue_id']
    if case == 'stale_candidate':
        (broker.checkout / 'README.md').write_text('Changed after stopped candidate')
    elif case == 'stale_owner':
        with shared.connect() as db:
            db.execute('UPDATE execution_claims SET generation=generation+1 WHERE issue_id=?',
                       (issue,))
    elif case == 'changed_scope':
        command['amended_config_sha256'] = '0' * 64
    budget = shared.budget(issue)
    owner = shared.current(issue)
    with store._connect() as db:
        before = [db.execute('SELECT count(*) FROM ' + name).fetchone()[0]
                  for name in ('delivery_commands', 'delivery_outbox', 'delivery_runs')]
    index = Path(_git(broker.checkout, 'rev-parse', '--git-path', 'index'))
    if not index.is_absolute():
        index = broker.checkout / index
    index_before = index.read_bytes()
    registry_before = shared.path.read_bytes()
    config_dir = store.config.state_root / 'configuration-snapshots'
    config_before = {p.name: p.read_bytes() for p in config_dir.iterdir()}
    snapshot = copy.deepcopy(parent['feature_delivery']['snapshot'])
    monkeypatch.setattr(GitHubDelivery, 'snapshot', lambda *_: copy.deepcopy(snapshot))
    app = create_app(store.config.path)
    app.state.delivery.store = store
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post('/api/runs/' + parent['run_id'] + '/assess-transition',
                                    json={'kind': 'continue_feature', 'request': command})
        assert preview.status_code == 200
        assert preview.json()['eligible'] is (case == 'valid'), preview.json()
        assert shared.current(issue) == owner
        assert shared.budget(issue) == budget
        assert index.read_bytes() == index_before
        assert shared.path.read_bytes() == registry_before
        assert {p.name: p.read_bytes() for p in config_dir.iterdir()} == config_before
        with store._connect() as db:
            assert [db.execute('SELECT count(*) FROM ' + name).fetchone()[0]
                    for name in ('delivery_commands', 'delivery_outbox', 'delivery_runs')] == before
        headers = {'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']}
        path = '/api/runs/' + parent['run_id'] + '/continue-feature'
        response = await client.post(path, json=command, headers=headers)
        if case == 'valid':
            assert response.status_code == 200, response.text
            replay = await client.post(path, json=command, headers=headers)
            assert replay.json() == response.json()
        else:
            assert response.status_code == 409, response.text
            assert shared.current(issue) == owner
            with store._connect() as db:
                after = [db.execute('SELECT count(*) FROM ' + name).fetchone()[0]
                         for name in ('delivery_commands', 'delivery_outbox', 'delivery_runs')]
                assert after == before
    assert shared.budget(issue) == budget
    if case != 'valid':
        return
    spec = store.effective_spec(response.json()['run_id'])
    assert spec['policy']['allowed_paths'] == (
        parent['policy']['allowed_paths'] + command['added_paths'])
    assert spec['feature_scope_amendment']['predecessor'] == parent['feature_delivery']['owner']


@pytest.mark.asyncio
async def test_retained_scope_preview_and_execution_preserve_existing_authority(
    stopped_feature_scope, monkeypatch
):
    from devflow_temporal.delivery_feature_execution import continue_feature

    store, parent, _child, _broker, _state, scope_command = stopped_feature_scope
    snapshot = copy.deepcopy(parent['feature_delivery']['snapshot'])
    monkeypatch.setattr(GitHubDelivery, 'snapshot', lambda *_: copy.deepcopy(snapshot))
    first = continue_feature(store, parent['run_id'], scope_command)
    retained = store.effective_spec(first['run_id'])
    token = retained['feature_delivery']['owner']
    shared = registry(retained)
    shared.stop(token, 'retained-scope-stopped', {})
    store.project(first['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='confirmed', event_type='blocked', message='Retained scope stopped')
    with store._connect() as db:
        store.state.release_work(db, retained['work_id'], 'external:devflow:' + first['run_id'])
        revision = db.execute('SELECT revision FROM delivery_runs WHERE run_id=?',
                              (first['run_id'],)).fetchone()[0]
    request = {'command_id': 'retained-scope-continue', 'expected_revision': revision}
    budget = shared.budget(token['issue_id'])
    before = shared.path.read_bytes()
    app = create_app(store.config.path)
    app.state.delivery.store = store
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post(
            f"/api/runs/{first['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': request},
        )
        assert preview.json()['eligible'], preview.json()
        assert shared.path.read_bytes() == before
        assert shared.budget(token['issue_id']) == budget
        response = await client.post(
            f"/api/runs/{first['run_id']}/continue-feature", json=request,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        assert response.status_code == 200, response.text
    successor = store.effective_spec(response.json()['run_id'])
    assert successor['feature_scope_amendment'] == retained['feature_scope_amendment']
    assert successor['policy'] == retained['policy']
    assert shared.budget(token['issue_id']) == budget
