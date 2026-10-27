"""Manual supersession authenticates its predecessor before selecting retained Git."""

from __future__ import annotations

import hashlib
import json
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workspace_custody import ensure_owner


@pytest.fixture
def stopped_owned_admission(api_fixture):
    path, supplied = api_fixture
    raw = json.loads(path.read_text())
    raw.update(workspace_custody_version=1, delivery_timeout_seconds=3600, max_attempts=3)
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(supplied)
    original = store.spec(supplied["run_id"])
    owner = ensure_owner(original)
    # This is the existing public store transition for an observed foreign
    # workflow-ID conflict. It proves this admission never owned an execution;
    # no fabricated successful Temporal completion substitutes for that fact.
    store.mark_start_conflict(original["run_id"], "delivery-" + original["run_id"],
                              "workflow ID belongs to an independently owned request")
    store.project(original["run_id"], phase="blocked", execution_state="blocked",
                  event_type="fixture_usage", message="Owned admission retained its clock",
                  outcome="blocked", usage={"tokens": 731, "wall_seconds": 19})
    source = Path(original["source_path"])
    shutil.rmtree(source)
    successor = {
        **supplied, "command_id": "manual-successor", "run_id": "run-successor",
        "branch": "fix/retained-successor", "supersedes_run_id": original["run_id"],
    }
    return store, original, owner, successor


def snapshot(store):
    with store._connect() as db:
        return {
            table: [dict(row) for row in db.execute(f"SELECT * FROM {table} ORDER BY rowid")]
            for table in ("delivery_runs", "delivery_commands", "delivery_outbox",
                          "delivery_events", "delivery_effects", "delivery_attempts",
                          "delivery_execution_authority", "claims", "runtime_sessions")
        }


def owner_bytes(owner):
    return {
        path.relative_to(owner).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in owner.rglob("*") if path.is_file()
    }


async def public_submit(store, supplied):
    app = create_app(store.config.path)
    url = store.config.dashboard_url
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=url) as client:
        session = (await client.get("/api/session")).json()
        return await client.post("/api/runs", json=supplied, headers={
            "origin": url, "x-devflow-csrf": session["csrf_token"],
        })


async def test_public_manual_supersede_reuses_authenticated_owner_after_setup_cleanup(
    stopped_owned_admission,
):
    store, original, owner, successor = stopped_owned_admission
    before = snapshot(store)
    original_owner = owner_bytes(owner)
    assert not Path(original["source_path"]).exists()
    response = await public_submit(store, successor)
    assert response.status_code == 200, response.text
    assert response.json()["run_id"] == successor["run_id"]
    admitted = store.spec(successor["run_id"])
    assert admitted["source_path"] == original["source_path"]
    assert admitted["base_sha"] == original["base_sha"]
    assert admitted["config_digest"] == original["config_digest"]
    assert admitted["policy_digest"] == original["policy_digest"]
    assert admitted["delivery_admitted_at"] == original["delivery_admitted_at"]
    assert admitted["policy"]["max_attempts"] == original["policy"]["max_attempts"]
    assert owner_bytes(owner) == original_owner
    with store._connect() as db:
        assert dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (original["run_id"],)).fetchone()) == before["delivery_runs"][0]
        assert store.state.claim_for(db, original["work_id"])["owner"] == (
            "external:devflow:" + successor["run_id"]
        )
    broker = DeliveryBroker(store, admitted)
    prepared = broker.prepare()
    assert prepared["candidate"]["head"] == original["base_sha"]
    assert broker.source != owner
    assert broker.checkout.is_dir()
    assert (broker.checkout / "README.md").read_text() == "Fixture\n"


@pytest.mark.parametrize("damage", [
    "missing", "foreign-work", "foreign-issue", "same-branch", "foreign-thread",
    "foreign-claim", "running", "unknown-effect", "missing-conflict-event", "missing-witness",
])
async def test_manual_retained_source_refuses_forged_or_unfinished_predecessor_without_effects(
    stopped_owned_admission, damage,
):
    store, original, owner, successor = stopped_owned_admission
    if damage == "missing":
        successor["supersedes_run_id"] = "missing-predecessor"
    elif damage == "foreign-work":
        successor["work_id"] = "foreign-work"
    elif damage == "foreign-issue":
        successor["issue_url"] = "https://github.com/example/fixture/issues/4"
    elif damage == "same-branch":
        successor["branch"] = original["branch"]
    elif damage == "foreign-thread":
        successor["origin_thread_id"] = "01a100ac-efd3-7dd2-9f25-504381f0dcd9"
    elif damage == "missing-witness":
        (Path(original["state_dir"]) / "workspace-writers.lock").unlink()
    else:
        with store._connect() as db:
            if damage == "foreign-claim":
                db.execute("UPDATE claims SET owner='foreign-owner' WHERE work_id=?",
                           (original["work_id"],))
            elif damage == "running":
                db.execute("UPDATE delivery_runs SET execution_state='running' WHERE run_id=?",
                           (original["run_id"],))
            elif damage == "missing-conflict-event":
                db.execute("DELETE FROM delivery_events WHERE run_id=? AND type='blocked'",
                           (original["run_id"],))
            else:
                db.execute("INSERT INTO delivery_effects VALUES "
                           "('unknown-effect',?,'prepare','{}','pending',NULL,'now')",
                           (original["run_id"],))
    before, original_owner = snapshot(store), owner_bytes(owner)
    response = await public_submit(store, successor)
    assert response.status_code == 409, response.text
    assert snapshot(store) == before
    assert owner_bytes(owner) == original_owner
    assert not (store.config.state_root / "runs" / successor["run_id"]).exists()
    assert not (store.config.state_root / "checkouts" / successor["run_id"]).exists()


async def test_manual_source_readmission_waits_for_actual_writer_body_settlement(
    stopped_owned_admission,
):
    from devflow_temporal.delivery_writer_custody import shared

    store, original, owner, successor = stopped_owned_admission
    entered, release = threading.Event(), threading.Event()

    def writer():
        with shared(original):
            entered.set()
            assert release.wait(5)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(writer)
        assert entered.wait(2)
        before, original_owner = snapshot(store), owner_bytes(owner)
        try:
            refused = await public_submit(store, successor)
            assert refused.status_code == 409, refused.text
            assert "lease" in refused.json()["detail"]
            assert snapshot(store) == before
            assert owner_bytes(owner) == original_owner
        finally:
            release.set()
            future.result(timeout=2)
    admitted = await public_submit(store, successor)
    assert admitted.status_code == 200, admitted.text
