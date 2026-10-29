import hashlib
import json
from copy import deepcopy

import pytest
from test_delivery_feature_revision_subject import (
    findings_reconsideration as findings_reconsideration,
)
from test_delivery_feature_revisions import (
    _closed_revision_fixture,
    _stop_revision_fixture,
)
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal.delivery_feature_revisions import revise_feature_plan
from devflow_temporal.delivery_github_contract import GitHubDelivery
from devflow_temporal.delivery_lifecycle import assess_transition


def _contents(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def stopped(feature, monkeypatch):
    store, spec, shared, record, diagnostic = feature
    row = _stop_revision_fixture(store, spec, shared)
    closed = _closed_revision_fixture(store, spec["run_id"])
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    command = {"command_id": "revise-observe-1", "expected_revision": row["revision"],
               "reason": "A dependency needs to be corrected."}
    return store, spec, shared, record, command


def test_public_revision_observes_owned_child_evidence(feature, monkeypatch):
    from pathlib import Path

    from devflow_temporal.delivery_feature_execution import register_worker, worker_key, worker_spec
    from devflow_temporal.delivery_plan_model import ordered_chunks

    store, spec, shared, record, _ = feature
    chunk = ordered_chunks(json.loads(spec["accepted_plan"]))[0]
    child = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    token = spec["feature_delivery"]["owner"]
    shared.checkpoint(token, "assignment:" + chunk["id"] + ":build", {
        "run_id": child["run_id"], "chunk_id": chunk["id"], "kind": "build",
        "store_path": str(store.config.tracking_db)})
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    evidence = Path(child["state_dir"]) / "evidence.json"
    evidence.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence.write_text('{"observation":"retained"}')
    row = _stop_revision_fixture(store, spec, shared)
    closed = _closed_revision_fixture(store, spec["run_id"])
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    request = {"command_id": "child-evidence", "expected_revision": row["revision"],
               "reason": "A retained child result requires revising the plan.",
               "evidence": [{"path": str(evidence), "sha256": _contents(evidence)}]}
    before = _contents(shared.path)
    result = assess_transition(store, spec["run_id"], "revise_feature_plan", request)
    assert result["eligible"], result
    assert _contents(shared.path) == before
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET recovery_json=? WHERE run_id=?",
                   (json.dumps({"kind": "unsupported"}), child["run_id"]))
    unknown = assess_transition(store, spec["run_id"], "revise_feature_plan", request)
    assert not unknown["eligible"]
    assert "feature_revision_evidence_unknown" in {b["code"] for b in unknown["blockers"]}
    assert _contents(shared.path) == before
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET recovery_json=NULL WHERE run_id=?", (child["run_id"],))
    assert revise_feature_plan(store, spec["run_id"], request)["revision_id"]


@pytest.mark.asyncio
async def test_public_revision_unreadable_closure_is_unknown(stopped, monkeypatch):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, spec, shared, _, command = stopped

    def unavailable(*_args, **_kwargs):
        raise ValueError('closure unavailable')

    monkeypatch.setattr('devflow_temporal.delivery_feature_closure.closed_coordinator', unavailable)
    budget = shared.budget(spec['feature_delivery']['owner']['issue_id'])
    before = _contents(shared.path)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    with store._connect() as db:
        tracking_before = list(db.iterdump())
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        session = (await client.get('/api/session')).json()
        preview = await client.post(
            f"/api/runs/{spec['run_id']}/assess-transition",
            json={'kind': 'revise_feature_plan', 'request': command},
        )
        blocker = next(b for b in preview.json()['blockers']
                       if b['code'] == 'feature_revision_closure_unknown')
        assert blocker['satisfied'] is None
        result = await client.post(
            f"/api/runs/{spec['run_id']}/revise-feature-plan", json=command,
            headers={'Origin': origin, 'X-Devflow-CSRF': session['csrf_token']},
        )
        assert result.status_code == 409
    assert _contents(shared.path) == before
    assert shared.budget(spec['feature_delivery']['owner']['issue_id']) == budget
    with store._connect() as db:
        assert list(db.iterdump()) == tracking_before
