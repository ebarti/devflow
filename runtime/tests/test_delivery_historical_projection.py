"""Historical display survives cleanup without granting execution authority."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import httpx
import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_technical_continuation as technical
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import write_private


def retained_history(service, monkeypatch, *, archived=False, cleanup="confirmed"):
    store, request = service
    store.submit(request)
    spec = store.effective_spec(request["run_id"])
    DeliveryBroker(store, spec).prepare()
    checkout = Path(spec["checkout"])
    assert checkout.is_dir() and checkout.is_relative_to(store.config.state_root)
    candidate = {"id": "a" * 64, "head": "b" * 40, "base_sha": "c" * 40, "policy_digest": "d" * 64}
    publication = {
        "number": 1044,
        "head": candidate["head"],
        "url": "https://github.com/example/fixture/pull/1044",
    }
    integration = {
        "tree": "e" * 40,
        "old_head": "f" * 40,
        "main": candidate["base_sha"],
        "subject": "Signed historical integration",
        "signer": "Historical Owner",
    }
    recovery = {
        "kind": "accepted_technical_successor",
        "original_recovery": None,
        "intent_sha256": "0" * 64,
        "resume_stage": "checks",
        "maximum_iteration": 4,
        "native_preparation_renewal": {"path": "/retained/native-proof.json"},
        "integration": integration,
        "execution_spec": {**spec, "base_sha": candidate["base_sha"]},
        "spec": spec,
    }
    write_private(
        Path(spec["state_dir"]) / "technical-successor/integration.json",
        {**integration, "head": candidate["head"], "ref": "refs/devflow/retained"},
    )
    # Isolate the retained integration reader; full authenticated historical custody
    # is additionally exercised on the captured owner's read-only database snapshot.
    monkeypatch.setattr(technical, "_retained", lambda _store, _recovery: {"spec": spec})
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET phase='delivered',outcome='delivered',"
            "execution_state='terminal',cleanup=?,candidate_json=?,pr_json=?,recovery_json=? "
            "WHERE run_id=?",
            (
                cleanup,
                json.dumps(candidate),
                json.dumps(publication),
                json.dumps(recovery),
                spec["run_id"],
            ),
        )
        if archived:
            db.execute(
                "INSERT INTO delivery_dashboard_state(run_id,archived) VALUES (?,1) "
                "ON CONFLICT(run_id) DO UPDATE SET archived=1",
                (spec["run_id"],),
            )
    shutil.rmtree(checkout)
    return store, spec, candidate, publication


def snapshot(store):
    with store._connect() as db:
        rows = [tuple(row) for row in db.execute("SELECT * FROM delivery_runs")]
        attempts = [tuple(row) for row in db.execute("SELECT * FROM delivery_attempts")]
        effects = [tuple(row) for row in db.execute("SELECT * FROM delivery_effects")]
        commands = [tuple(row) for row in db.execute("SELECT * FROM delivery_commands")]
    files = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in store.config.state_root.rglob("*")
        if path.is_file()
    }
    return rows, attempts, effects, commands, files


@pytest.mark.asyncio
@pytest.mark.parametrize("archived", [False, True])
async def test_public_list_and_detail_preserve_cleaned_historical_receipts_without_execution(
    service,
    monkeypatch,
    archived,
):
    store, spec, candidate, publication = retained_history(service, monkeypatch, archived=archived)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    before = snapshot(store)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url=store.config.dashboard_url
    ) as browser:
        response = await browser.get("/api/runs", params={"archived": str(archived).lower()})
        assert response.status_code == 200
        listed = next(item for item in response.json()["runs"] if item["id"] == spec["run_id"])
        response = await browser.get("/api/runs/" + spec["run_id"])
        assert response.status_code == 200
        detailed = response.json()["run"]
    for value in (listed, detailed):
        assert value["phase"] == "delivered" and value["outcome"] == "delivered"
        assert value["cleanup"] == "confirmed" and value["archived"] is archived
        assert value["execution_readback"] == {
            "state": "unavailable",
            "reason": "historical_checkout_absent",
            "checkout": spec["checkout"],
        }
    assert detailed["candidate"]["id"] == candidate["id"]
    assert detailed["candidate"]["head"] == candidate["head"]
    assert detailed["candidate"]["base_sha"] == candidate["base_sha"]
    assert detailed["candidate"]["policy_digest"] == candidate["policy_digest"]
    assert detailed["pull_request"] == publication
    assert detailed["preparation"] is None and detailed["can_steer"] is False
    with pytest.raises(RuntimeError, match="cannot change to"):
        store.effective_spec(spec["run_id"])
    with pytest.raises(ValueError, match="unsupported repair continuation kind"):
        store.continue_repair(spec["run_id"], {"continuation_kind": "accepted_technical_successor"})
    assert snapshot(store) == before
    assert not Path(spec["checkout"]).exists()


@pytest.mark.parametrize("state", ["queued", "unfinished", "replaced_checkout"])
def test_active_or_replaced_execution_keeps_strict_readback(service, monkeypatch, state):
    store, spec, _candidate, _publication = retained_history(service, monkeypatch, archived=True)
    with store._connect() as db:
        if state == "queued":
            db.execute(
                "UPDATE delivery_runs SET outcome=NULL,execution_state='queued' WHERE run_id=?",
                (spec["run_id"],),
            )
        elif state == "unfinished":
            db.execute(
                "INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,state) "
                "VALUES ('unfinished',?,'implement',0,?,'running')",
                (spec["run_id"], "a" * 64),
            )
        else:
            Path(spec["checkout"]).symlink_to(store.config.state_root / "absent-replacement")
    before = snapshot(store)
    for operation in (
        lambda: store.list_runs_page(archived=True),
        lambda: store.detail(spec["run_id"]),
    ):
        with pytest.raises(RuntimeError, match="cannot change to"):
            operation()
    assert snapshot(store) == before


def test_missing_historical_checkout_does_not_normalize_unknown_cleanup_or_invent_candidate_fields(
    service,
    monkeypatch,
):
    store, spec, _candidate, _publication = retained_history(
        service, monkeypatch, cleanup="unknown"
    )
    with store._connect() as db:
        db.execute(
            "UPDATE delivery_runs SET candidate_json=? WHERE run_id=?",
            (json.dumps({"id": "a" * 64, "head": "b" * 40}), spec["run_id"]),
        )
    before = snapshot(store)
    detail = store.detail(spec["run_id"])
    assert detail["cleanup"] == "unknown" and detail["cleanup_recorded"] == "unknown"
    assert detail["candidate"]["base_sha"] is None and detail["candidate"]["policy_digest"] is None
    assert detail["execution_readback"]["state"] == "unavailable"
    assert snapshot(store) == before
