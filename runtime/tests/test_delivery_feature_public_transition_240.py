"""Public feature preflight and execution share the stopped-owner checks."""

import json

import httpx
import pytest
from test_delivery_feature_revision_settlement import _stopped_feature

from devflow_temporal.delivery_api import create_app

pytest_plugins = ["test_delivery_feature_revision_settlement"]


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["valid", "stale_revision", "stale_owner", "stale_config"])
async def test_public_feature_preflight_and_execution_agree(feature, monkeypatch, case):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda *_args, **_kwargs: {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"},
    )
    app = create_app(store.config.path)
    app.state.delivery.store = store
    request = {"command_id": "public-feature-" + case, "expected_revision": row["revision"]}
    if case == "stale_revision":
        request["expected_revision"] -= 1
    elif case == "stale_owner":
        with shared.connect() as db:
            db.execute(
                "UPDATE execution_claims SET generation=generation+1 WHERE issue_id=?",
                (spec["feature_delivery"]["owner"]["issue_id"],),
            )
    elif case == "stale_config":
        config = json.loads(store.config.path.read_text())
        config["capacity"] = config.get("capacity", 2) + 1
        store.config.path.write_text(json.dumps(config))
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    with store._connect() as db:
        before = (db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0],
                  db.execute("SELECT count(*) FROM delivery_outbox").fetchone()[0],
                  db.execute("SELECT candidate_json FROM delivery_runs WHERE run_id=?",
                             (spec["run_id"],)).fetchone()[0])
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    origin = store.config.dashboard_url.rstrip("/")
    async with httpx.AsyncClient(transport=transport, base_url=origin) as client:
        session = await client.get("/api/session")
        assert session.status_code == 200
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={"kind": "continue_feature", "request": request},
        )
        assert preview.status_code == 200
        assessed = preview.json()
        result = await client.post(
            f"/api/runs/{spec['run_id']}/continue-feature",
            json=request,
            headers={"Origin": origin, "X-Devflow-CSRF": session.json()["csrf_token"]},
        )
    if case == "valid":
        assert assessed["eligible"] is True
        assert result.status_code == 200
        assert (
            store.effective_spec(result.json()["run_id"])["feature_delivery"]["owner"]["generation"]
            == 2
        )
    else:
        expected = {
            "stale_revision": "revision_changed",
            "stale_owner": "feature_owner_changed",
            "stale_config": "feature_submission_config_unknown",
        }[case]
        assert assessed["eligible"] is False
        assert expected in {item["code"] for item in assessed["blockers"]}
        assert result.status_code == 409
        assert expected in {item["code"] for item in result.json()["detail"]["blockers"]}
        with store._connect() as db:
            after = (db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0],
                     db.execute("SELECT count(*) FROM delivery_outbox").fetchone()[0],
                     db.execute("SELECT candidate_json FROM delivery_runs WHERE run_id=?",
                                (spec["run_id"],)).fetchone()[0])
        assert after == before
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget


@pytest.mark.asyncio
async def test_public_continuation_unreadable_closure_is_unknown(feature, monkeypatch):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)

    def unavailable(*_args, **_kwargs):
        raise ValueError('closure unavailable')

    monkeypatch.setattr('devflow_temporal.delivery_feature_closure.closed_coordinator', unavailable)
    request = {'command_id': 'unreadable-closure', 'expected_revision': row['revision']}
    budget = shared.budget(spec['feature_delivery']['owner']['issue_id'])
    app = create_app(store.config.path)
    app.state.delivery.store = store
    with store._connect() as db:
        before = list(db.iterdump())
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': request},
        )
        blocker = next(b for b in preview.json()['blockers']
                       if b['code'] == 'feature_coordinator_closure_unknown')
        assert blocker['satisfied'] is None
        result = await client.post(
            f"/api/runs/{spec['run_id']}/continue-feature", json=request,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        assert result.status_code == 409
    with store._connect() as db:
        assert list(db.iterdump()) == before
    assert shared.budget(spec['feature_delivery']['owner']['issue_id']) == budget


@pytest.mark.asyncio
async def test_public_preview_refuses_owner_takeover_during_closure(feature, monkeypatch):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    token = spec['feature_delivery']['owner']
    calls = []

    def takeover(*_args, **_kwargs):
        if not calls:
            calls.append(True)
            shared.claim(spec['feature_delivery']['snapshot'], 'run-concurrent-owner',
                         str(store.config.tracking_db), predecessor=token)
        return {'result': {'cleanup': 'confirmed'}, 'workflow_id': 'closed'}

    monkeypatch.setattr('devflow_temporal.delivery_feature_closure.closed_coordinator', takeover)
    request = {'command_id': 'takeover-race', 'expected_revision': row['revision']}
    app = create_app(store.config.path)
    app.state.delivery.store = store
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': request},
        )
    assert calls
    assert not preview.json()['eligible']
    assert 'feature_registry_proof_unknown' in {b['code'] for b in preview.json()['blockers']}
