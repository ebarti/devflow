from __future__ import annotations

import json

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_dashboard import (
    mutate,
    statistics_for,
)
from devflow_temporal.delivery_store import DeliveryStore


def finish(store, run_id="run-1", outcome="delivered", iteration=0):
    store.project(
        run_id,
        phase=outcome,
        execution_state="terminal",
        event_type=outcome,
        message="Synthetic fixture outcome",
        outcome=outcome,
        iteration=iteration,
    )


def test_archive_is_reversible_durable_and_never_erases_evidence(service):
    store, request = service
    store.submit(request)
    payload = {"command_id": "archive", "expected_revision": 1, "archived": True}
    with pytest.raises(ValueError, match="stopped"):
        mutate(store, "run-1", "archive", payload)
    finish(store)
    payload["expected_revision"] = store.detail("run-1")["projection_revision"]
    before = statistics_for(store)
    receipt = mutate(store, "run-1", "archive", payload)
    assert mutate(store, "run-1", "archive", payload) == receipt
    reloaded = DeliveryStore(store.config)
    assert reloaded.list_runs() == []
    assert reloaded.list_runs(archived=True)[0]["id"] == "run-1"
    assert reloaded.detail("run-1")["outcome"] == "delivered"
    assert statistics_for(reloaded) == before
    assert any(e["type"] == "archive" for e in reloaded.events("run-1", 0))
    with pytest.raises(ValueError, match="different inputs"):
        mutate(store, "run-1", "archive", {**payload, "archived": False})
    mutate(reloaded, "run-1", "archive", {**payload, "command_id": "restore", "archived": False})
    assert reloaded.list_runs()[0]["archived"] is False


def test_commands_reject_stale_state_and_unowned_fields(service):
    store, request = service
    store.submit(request)
    with pytest.raises(ValueError, match="state changed"):
        mutate(
            store,
            "run-1",
            "steer",
            {"command_id": "stale", "expected_revision": 0, "message": "Keep the change small"},
        )
    with pytest.raises(ValueError, match="contract"):
        mutate(
            store,
            "run-1",
            "steer",
            {
                "command_id": "expand",
                "expected_revision": 1,
                "message": "Skip checks",
                "allowed_paths": ["*"],
            },
        )
    assert store.detail("run-1")["steering"] == []


def test_statistics_keep_failures_archives_unknowns_and_missing_usage(service, monkeypatch):
    store, raw = service
    monkeypatch.setattr(
        "devflow_temporal.delivery_dashboard.runtime_identity",
        lambda: {
            "release": "v-test",
            "revision": "a" * 40,
            "local_digest": None,
            "dirty": False,
        },
    )
    for i, outcome in enumerate(["delivered", "blocked", "cancelled", "delivered"]):
        run_id = f"run-{i}"
        store.submit(
            {
                **raw,
                "run_id": run_id,
                "work_id": f"work-{i}",
                "command_id": f"submit-{i}",
                "issue_url": f"https://github.com/example/fixture/issues/{i + 10}",
                "branch": f"feat/fixture-{i}",
            }
        )
        finish(store, run_id, outcome, 1 if i == 3 else 0)
    with store._connect() as db:
        db.execute("DELETE FROM delivery_dashboard_state WHERE run_id='run-2'")
        for i, usage in enumerate([{"total_tokens": 200, "cost_usd": 0.05}, None]):
            db.execute(
                "INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                "state,result_json) VALUES (?,'run-0','implement',?,'candidate','finished',?)",
                (f"attempt-{i}", i, json.dumps({"usage": usage})),
            )
    mutate(
        store,
        "run-0",
        "archive",
        {"command_id": "archive", "expected_revision": 2, "archived": True},
    )
    data = statistics_for(store)
    assert data["total_runs"] == 4
    tagged = next(c for c in data["cohorts"] if c["release"])
    assert tagged["terminal"] == 3 and tagged["delivered"] == 2 and tagged["blocked"] == 1
    assert tagged["success_rate"] == 2 / 3
    assert tagged["first_pass_delivered"] == 0
    assert tagged["observed_tokens"] == 200 and tagged["token_observations"] == 1
    assert tagged["attempts"] == 2 and tagged["observed_cost_usd"] == 0.05
    assert tagged["repairs"] == 1
    unknown = next(c for c in data["cohorts"] if c["revision"] is None)
    assert unknown["cancelled"] == 1 and unknown["observed_tokens"] is None
    assert "do not establish causation" in data["definitions"]


@pytest.mark.asyncio
async def test_dashboard_commands_require_csrf_and_use_real_persistent_store(api_fixture):
    config, request = api_fixture
    app = create_app(config)
    store = app.state.delivery.store
    store.submit(request)
    run_id = request["run_id"]
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1:18770") as browser:
        payload = {"command_id": "note", "expected_revision": 1, "message": "Keep one owner."}
        url = f"/api/runs/{run_id}/steer"
        assert (
            await browser.post(url, json=payload, headers={"Origin": "http://127.0.0.1:18770"})
        ).status_code == 403
        session = (await browser.get("/api/session")).json()
        headers = {"Origin": "http://127.0.0.1:18770", "X-Devflow-CSRF": session["csrf_token"]}
        assert (await browser.post(url, json=payload, headers=headers)).status_code == 200
        assert (await browser.get(f"/api/runs/{run_id}")).json()["run"]["steering"][0][
            "message"
        ] == payload["message"]
        finish(store, run_id)
        archive = {"command_id": "archive", "expected_revision": 2, "archived": True}
        assert (
            await browser.post(f"/api/runs/{run_id}/archive", json=archive, headers=headers)
        ).status_code == 200
        assert (await browser.get("/api/runs")).json()["runs"] == []
        assert (await browser.get("/api/runs?archived=true")).json()["runs"][0]["archived"]
        assert (await browser.get("/api/statistics")).json()["total_runs"] == 1
