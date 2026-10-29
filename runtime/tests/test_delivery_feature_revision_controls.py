"""Behavioral public controls over the real journal/registry and sealed remote bindings."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx
import pytest
from test_delivery_feature_merge import setup_feature
from test_delivery_store import service as legacy_service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_feature_execution import registry
from devflow_temporal.delivery_github_contract import GitHubDelivery

service = legacy_service


def stopped_feature(service, monkeypatch):
    store, spec, record, _requested, _merge_command, gh = setup_feature(service, monkeypatch)
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    shared.repair(token, "historical-1", "first product repair")
    shared.repair(token, "historical-2", "second product repair")
    shared.repair(token, "historical-3", "third product repair")
    shared.repair(token, "historical-4", "fourth product repair")
    store.mark_start(spec["run_id"], accepted=True)
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
            "execution_state='blocked',cleanup='confirmed' WHERE run_id=?",
            (spec["run_id"],),
        )
        store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
        original = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
    shared.stop(token, "closed-test-feature", {"cleanup": "confirmed"})
    monkeypatch.setattr(
        GitHubDelivery, "api", lambda _self, *args, **kwargs: gh.api(*args, **kwargs)
    )
    monkeypatch.setattr(
        type(store),
        "_completed_temporal_result",
        lambda *_args, **_kwargs: {
            "workflow_id": "delivery-" + spec["run_id"],
            "result": {"cleanup": "confirmed"},
        },
    )
    evidence = Path(spec["state_dir"]) / "planning-defect.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text(
        json.dumps({"chunk": "model", "diagnostic": "Future selector is not in this chunk."})
    )
    command = {
        "command_id": "plan-control-1",
        "expected_revision": original["revision"],
        "reason": "The API chunk inherited a later chunk browser selector.",
        "evidence": [
            {"path": str(evidence), "sha256": hashlib.sha256(evidence.read_bytes()).hexdigest()}
        ],
    }
    return store, spec, record, gh, original, command


def assert_revision_receipt(store, spec, original, result):
    assert result["predecessor_run_id"] == spec["run_id"]
    assert result["revision_phase"] == "requested"
    assert result["expected_plan_revision"] == 1
    assert result["repair_budget"]["used"] == 4
    assert result["affected_chunks"] == []
    successor = store.effective_spec(result["run_id"])
    assert successor["issue_url"] == spec["issue_url"]
    assert successor["authorized_endpoint"] == "published_unmerged"
    assert successor["accepted_plan"] == spec["accepted_plan"]
    assert successor["policy"] == spec["policy"]
    assert successor["feature_delivery"]["owner"]["generation"] == 2
    assert successor["feature_plan_revision_request"]["old_identity"]["comment_id"] == 11
    with store._connect() as db:
        old = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        assert old["request_json"] == original["request_json"]
        assert old["pr_json"] == original["pr_json"]
        assert old["iteration"] == original["iteration"]
        runs = [row[0] for row in db.execute("SELECT run_id FROM delivery_runs")]
    assert set(runs) == {spec["run_id"], result["run_id"]}
    assert registry(successor).checkpoints("I_feature")["github-record"]["comment_id"] == 11
    return successor


@pytest.mark.asyncio
async def test_authenticated_http_revision_returns_one_recorded_successor_and_current_readback(
    service,
    monkeypatch,
):
    store, spec, _record, gh, original, command = stopped_feature(service, monkeypatch)
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    url = store.config.dashboard_url
    route = "/api/runs/" + spec["run_id"] + "/revise-feature-plan"
    async with httpx.AsyncClient(transport=transport, base_url=url) as browser:
        blocked = await browser.post(route, json=command, headers={"Origin": url})
        assert blocked.status_code == 403
        with store._connect() as db:
            assert (
                db.execute(
                    "SELECT run_id FROM delivery_runs WHERE run_id!=?", (spec["run_id"],)
                ).fetchone()
                is None
            )
        readback = (await browser.get("/api/runs/" + spec["run_id"])).json()["run"]["feature_plan"]
        assert readback["can_revise"] is True
        assert readback["expected_revision"] == command["expected_revision"]
        assert readback["plan_identity"]["comment_id"] == 11
        assert readback["authority"]["allowed_files"] == ["README.md"]
        assert readback["expected_paths"]["model"] == ["README.md"]
        session = await browser.get("/api/session")
        headers = {"Origin": url, "X-Devflow-CSRF": session.json()["csrf_token"]}
        first = await browser.post(route, json=command, headers=headers)
        assert first.status_code == 200, first.text
        result = first.json()
        assert_revision_receipt(store, spec, original, result)
        replay = await browser.post(route, json=command, headers=headers)
        assert replay.status_code == 200 and replay.json() == result
        conflict = await browser.post(
            route, json={**command, "reason": "A different proposal"}, headers=headers
        )
        assert conflict.status_code == 409 and "different inputs" in conflict.json()["detail"]
        successor = (await browser.get("/api/runs/" + result["run_id"])).json()["run"]
        assert successor["feature_plan"]["phase"] == "requested"
        assert successor["feature_plan"]["reason"] == command["reason"]
        assert successor["feature_plan"]["repair_budget"]["used"] == 4
        assert successor["feature_plan"]["can_revise"] is False
    assert not any(method != "GET" for method, _path, _body in gh.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["stale", "changed_evidence"]
)
async def test_http_invalid_revision_cannot_create_successor_or_change_remote_bindings(
    service,
    monkeypatch,
    change,
):
    store, spec, _record, gh, _original, command = stopped_feature(service, monkeypatch)
    if change == "stale":
        command["expected_revision"] += 1
    elif change == "unchecked_plan":
        command["plan"] = {"scope": "caller replacement"}
    elif change == "unchecked_policy":
        command["policy"] = {"allowed_paths": ["*"]}
    elif change == "long_reason":
        command["reason"] = "x" * 4001
    else:
        Path(command["evidence"][0]["path"]).write_text("changed after diagnostic")
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(
        transport=transport, base_url=store.config.dashboard_url
    ) as browser:
        session = await browser.get("/api/session")
        headers = {
            "Origin": store.config.dashboard_url,
            "X-Devflow-CSRF": session.json()["csrf_token"],
        }
        refused = await browser.post(
            "/api/runs/" + spec["run_id"] + "/revise-feature-plan", json=command, headers=headers
        )
        assert refused.status_code == 409
    with store._connect() as db:
        assert (
            db.execute(
                "SELECT run_id FROM delivery_runs WHERE run_id!=?", (spec["run_id"],)
            ).fetchone()
            is None
        )
    assert registry(spec).current("I_feature")["state"] == "stopped"
    assert registry(spec).budget("I_feature")["used"] == 4
    assert not any(method != "GET" for method, _path, _body in gh.calls)
