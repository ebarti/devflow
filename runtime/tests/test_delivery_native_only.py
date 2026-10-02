"""Retired execution cannot enqueue work; historical records remain readable."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import PreparationError
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


def test_retired_docker_configuration_rejected_before_claim(api_fixture):
    path, request = api_fixture
    raw = json.loads(path.read_text())
    raw["execution_backend"] = "docker"
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    with pytest.raises(ValueError, match="Docker execution is retired"):
        store.submit(request)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_outbox").fetchone()[0] == 0
        assert store.state.row(db, "works", request["work_id"]) is None


@pytest.mark.asyncio
async def test_historical_execution_and_stale_recovery_api_cannot_mutate(api_fixture, monkeypatch):
    path, request = api_fixture
    app = create_app(path)
    store = app.state.delivery.store
    store.submit(request)
    historical = store.spec(request["run_id"])
    historical["provider"] = "codex"
    historical["policy"]["execution_backend"] = "docker"
    # An old amended authority is data, even when its original validator no longer exists.
    recovery = {
        "kind": "scope_amendment", "effective_spec": historical,
        "added_paths": ["test_fixture.py"], "maximum_iteration": 1,
        "predecessor_execution_run_id": "old-execution",
    }
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET request_json=?,recovery_json=?,phase='blocked',"
            "execution_state='blocked',outcome='blocked' WHERE run_id=?",
            (canonical_json(historical), canonical_json(recovery), request["run_id"]),
        )
        db.execute("UPDATE delivery_outbox SET state='sent'")
        before = [tuple(row) for row in db.execute("SELECT * FROM delivery_runs")]
        outbox = [tuple(row) for row in db.execute("SELECT * FROM delivery_outbox")]
        commands = db.execute("SELECT COUNT(*) FROM delivery_commands").fetchone()[0]
    monkeypatch.setattr(
        store, "_completed_temporal_result",
        lambda *_args, **_kwargs: pytest.fail("historical mutation queried Temporal"),
    )
    assert store.detail(request["run_id"])["run"]["phase"] == "blocked"
    for method in (store.continue_repair, store.retry_prelaunch, store.amend_scope,
                   store.recover_publication):
        with pytest.raises(PreparationError, match="read-only"):
            method(request["run_id"], {})
    with pytest.raises(PreparationError, match="read-only"):
        store.begin_mutation(request["run_id"], "old-answer", "decision", {})
    with pytest.raises(PreparationError, match="read-only"):
        store.submit({
            **request, "run_id": "successor", "command_id": "old-successor",
            "branch": "fix/successor", "supersedes_run_id": request["run_id"],
        })
    with pytest.raises(PreparationError, match="read-only"):
        DeliveryBroker(store, historical)
    with pytest.raises(PreparationError, match="read-only"):
        await DeliverySupervisor(store, capacity=1).run({"spec": historical})
    with store._connect() as db:
        assert [tuple(row) for row in db.execute("SELECT * FROM delivery_runs")] == before
        assert [tuple(row) for row in db.execute("SELECT * FROM delivery_outbox")] == outbox
        assert db.execute("SELECT COUNT(*) FROM delivery_commands").fetchone()[0] == commands
        assert db.execute("SELECT COUNT(*) FROM delivery_mutations").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 12345))
    async with httpx.AsyncClient(
        transport=transport, base_url=store.config.dashboard_url
    ) as caller:
        origin = {"Origin": store.config.dashboard_url}
        token = (store.config.state_root / "service-token").read_text().strip()
        login = await caller.post("/api/session", json={"token": token}, headers=origin)
        headers = {**origin, "X-Devflow-CSRF": login.json()["csrf_token"]}
        assert (await caller.get("/api/runs/run-1")).status_code == 200
        retired = await caller.post(
            "/api/runs/run-1/recover-precheck-prelaunch", json={}, headers=headers,
        )
        assert retired.status_code == 404
        assert (await caller.post(
            "/api/runs/run-1/continue-repair", json={"grant_number": 2}, headers=headers,
        )).status_code == 409
    with store._connect() as db:
        assert [tuple(row) for row in db.execute("SELECT * FROM delivery_outbox")] == outbox


def test_retired_action_is_absent_from_public_cli_and_dashboard():
    runtime = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [sys.executable, "-I", "-m", "devflow_temporal.delivery_control", "--help"],
        capture_output=True, text=True, check=True,
    )
    assert "recover-precheck-prelaunch" not in result.stdout
    assert "continue-repair" in result.stdout
    assert "retry-prelaunch" in result.stdout
    assert "amend-scope" in result.stdout
    for source in (runtime / "ui/src").glob("*.ts*"):
        assert "recover-precheck-prelaunch" not in source.read_text()
        assert "grant_number" not in source.read_text()
