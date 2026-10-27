"""Read-only proof of a pending, unspent revision preparation continuation."""

import sys
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from test_delivery_store import _git

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_feature_revisions import revision_request
from devflow_temporal.delivery_github_contract import GitHubDelivery

pytest_plugins = ['test_delivery_feature_revision_preparation']


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["valid", "stale_revision", "stale_candidate", "unknown_cleanup", "unknown_effect"]
)
@pytest.mark.skipif(sys.platform != 'darwin', reason='native configuration preparation required')
async def test_public_preparation_retry_preview_and_execute(
    pending_preparation, monkeypatch, case
):
    fixture = pending_preparation
    store, spec, shared, command = (fixture[k] for k in ('store', 'spec', 'shared', 'request'))
    closed = deepcopy(fixture['closed'])
    monkeypatch.setattr('devflow_temporal.delivery_feature_closure.closed_coordinator',
                        lambda _store, _run, **_kwargs: deepcopy(closed))
    record = revision_request(spec, shared)['record']
    monkeypatch.setattr(GitHubDelivery, 'load_record', lambda *_args: deepcopy(record))
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.live_members',
                        lambda *_args: [])
    token = spec['feature_delivery']['owner']
    if case == 'stale_revision':
        command = {**command, 'expected_revision': command['expected_revision'] - 1}
    elif case == 'stale_candidate':
        checkout = Path(spec['checkout'])
        (checkout / 'README.md').write_text('Candidate changed after its stop')
    elif case == 'unknown_cleanup':
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET cleanup='unknown' WHERE run_id=?",
                       (spec['run_id'],))
    elif case == 'unknown_effect':
        with shared.connect() as db:
            db.execute(
                "INSERT INTO execution_effects VALUES (?,?,?,?,?,?,'unknown',NULL,?)",
                (token['issue_id'], 'unconfirmed', token['generation'], 'publish',
                 'unknown', '{}', 'observation'))
    index = Path(_git(Path(spec['checkout']), 'rev-parse', '--git-path', 'index'))
    if not index.is_absolute():
        index = Path(spec['checkout']) / index
    index_before = index.read_bytes()
    budget = shared.budget(token['issue_id'])
    registry = shared.path.read_bytes()
    app = create_app(store.config.path)
    app.state.delivery.store = store
    with store._connect() as db:
        tracking = list(db.iterdump())
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': command},
        )
        assert preview.json()['eligible'] is (case == 'valid'), preview.json()
        assert shared.path.read_bytes() == registry
        assert shared.budget(token['issue_id']) == budget
        assert index.read_bytes() == index_before
        with store._connect() as db:
            assert list(db.iterdump()) == tracking
        response = await client.post(
            f"/api/runs/{spec['run_id']}/continue-feature", json=command,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        if case == 'valid':
            assert response.status_code == 200, response.text
            with store._connect() as db:
                admitted_tracking = list(db.iterdump())
            admitted_registry = shared.path.read_bytes()
            prior = await client.post(
                f"/api/runs/{spec['run_id']}/assess-transition",
                json={'kind': 'continue_feature', 'request': command},
            )
            assert prior.json()['disposition'] == 'already_admitted'
            assert not prior.json()['eligible']
            assert prior.json()['blockers'][0]['code'] == 'already_admitted'
            conflicting = {**command, 'expected_revision': command['expected_revision'] - 1}
            conflict = await client.post(
                f"/api/runs/{spec['run_id']}/assess-transition",
                json={'kind': 'continue_feature', 'request': conflicting},
            )
            assert conflict.json()['blockers'][0]['code'] == 'feature_command_conflict'
            refused = await client.post(
                f"/api/runs/{spec['run_id']}/continue-feature", json=conflicting,
                headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
            )
            assert refused.status_code == 409
            replay = await client.post(
                f"/api/runs/{spec['run_id']}/continue-feature", json=command,
                headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
            )
            assert replay.json() == response.json()
            assert shared.path.read_bytes() == admitted_registry
            with store._connect() as db:
                assert list(db.iterdump()) == admitted_tracking
        else:
            assert response.status_code == 409, response.text
            assert shared.path.read_bytes() == registry
            with store._connect() as db:
                assert list(db.iterdump()) == tracking
    assert shared.budget(token['issue_id']) == budget
    if case == 'valid':
        successor = store.effective_spec(response.json()['run_id'])
        assert successor['feature_plan_revision_request'] == spec['feature_plan_revision_request']


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform != 'darwin', reason='native configuration preparation required')
async def test_public_preparation_retry_replays_partial_claim(pending_preparation, monkeypatch):
    from devflow_temporal import delivery_feature_execution as execution

    fixture = pending_preparation
    store, spec, shared, command = (fixture[k] for k in ('store', 'spec', 'shared', 'request'))
    closed = deepcopy(fixture['closed'])
    monkeypatch.setattr('devflow_temporal.delivery_feature_closure.closed_coordinator',
                        lambda _store, _run, **_kwargs: deepcopy(closed))
    record = revision_request(spec, shared)['record']
    monkeypatch.setattr(GitHubDelivery, 'load_record', lambda *_args: deepcopy(record))
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.live_members',
                        lambda *_args: [])
    real_admit = execution.admit

    def interrupted(*args, **kwargs):
        real_admit(*args, **kwargs)
        raise ConnectionError('lost local successor insert')

    monkeypatch.setattr(execution, 'admit', interrupted)
    with pytest.raises(ConnectionError):
        execution.continue_feature(store, spec['run_id'], command)
    monkeypatch.setattr(execution, 'admit', real_admit)
    budget = shared.budget(spec['feature_delivery']['owner']['issue_id'])
    before = shared.path.read_bytes()
    app = create_app(store.config.path)
    app.state.delivery.store = store
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': command},
        )
        assert preview.json()['eligible'], preview.json()
        assert preview.json()['normalized_lifecycle'] == 'recovering'
        other = {**command, 'command_id': 'different-partial-claim'}
        stale = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': other},
        )
        assert not stale.json()['eligible']
        assert shared.path.read_bytes() == before
        assert shared.budget(spec['feature_delivery']['owner']['issue_id']) == budget
        response = await client.post(
            f"/api/runs/{spec['run_id']}/continue-feature", json=command,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        assert response.status_code == 200, response.text
        from test_delivery_feature_revision_preparation import _failed_preparation
        from test_delivery_feature_revisions import _closed_revision_fixture

        from devflow_temporal.delivery_preparation import prepare_authority

        second = prepare_authority(store, store.effective_spec(response.json()['run_id']))
        second_row, second_closed, _ = _failed_preparation(store, second, shared)
        closures = fixture['closures']
        closures[second['run_id']] = second_closed
        monkeypatch.setattr(
            'devflow_temporal.delivery_feature_closure.closed_coordinator',
            lambda source, run, **_kwargs: (
                deepcopy(closures[run]) if run in closures
                else _closed_revision_fixture(source, run)),
        )
        second_command = {'command_id': 'second-preparation-preview',
                          'expected_revision': second_row['revision']}
        second_registry = shared.path.read_bytes()
        with store._connect() as db:
            second_tracking = list(db.iterdump())
        second_preview = await client.post(
            f"/api/runs/{second['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': second_command},
        )
        assert second_preview.json()['eligible'], second_preview.json()
        assert shared.path.read_bytes() == second_registry
        with store._connect() as db:
            assert list(db.iterdump()) == second_tracking
        assert shared.budget(spec['feature_delivery']['owner']['issue_id']) == budget
        second_response = await client.post(
            f"/api/runs/{second['run_id']}/continue-feature", json=second_command,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        assert second_response.status_code == 200, second_response.text
    assert shared.budget(spec['feature_delivery']['owner']['issue_id']) == budget
