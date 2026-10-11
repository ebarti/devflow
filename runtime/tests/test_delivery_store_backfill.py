"""Store construction must neither replay nor depend on every historical run."""
from __future__ import annotations

import json
import logging
import os

import httpx
import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_features
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


def _second(request):
    return {**request, "run_id": "run-2", "command_id": "command-2", "work_id": "work-2",
            "issue_url": "https://github.com/example/fixture/issues/4", "branch": "feat/fixture-2"}


def _counting_transition(monkeypatch):
    calls = []
    original = delivery_features.transition

    def transition(db, config, run_id):
        calls.append(run_id)
        return original(db, config, run_id)

    monkeypatch.setattr(delivery_features, "transition", transition)
    return calls


def _bump_revision(store, run_id):
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?", (run_id,))


def test_construction_projects_each_run_once_per_revision_and_generation(service, monkeypatch):
    store, request = service
    store.submit(request)
    store.submit(_second(request))
    calls = _counting_transition(monkeypatch)
    DeliveryStore(store.config)
    assert calls == [], "an unchanged history is not replayed per construction"
    _bump_revision(store, "run-1")
    DeliveryStore(store.config)
    assert calls == ["run-1"]
    calls.clear()
    DeliveryStore(store.config)
    assert calls == []
    # Projection code changed: history is re-projected once.
    monkeypatch.setattr(delivery_features, "projection_generation", lambda: "upgraded")
    DeliveryStore(store.config)
    assert calls == ["run-1", "run-2"]
    calls.clear()
    DeliveryStore(store.config)
    assert calls == []


def test_construction_skips_unchanged_rows_whichever_admitted_config_builds_it(
    service, tmp_path, monkeypatch
):
    """Activities build stores from sealed per-run snapshots; the API and CLI
    use the live file. Alternating between them must not replay history."""
    store, request = service
    store.submit(request)
    store.submit(_second(request))
    edited = json.loads(store.config.path.read_text())
    edited["repositories"]["fixture"]["assignee"] = "octocat"
    edited["repositories"]["fixture"]["project_url"] = "https://github.com/orgs/example/projects/9"
    sealed_path = tmp_path / "sealed-snapshot.json"
    sealed_path.write_text(json.dumps(edited))
    sealed = DeliveryConfig.load(sealed_path)
    assert sealed.raw != store.config.raw
    calls = _counting_transition(monkeypatch)
    for config in (sealed, store.config, sealed, store.config):
        DeliveryStore(config)
    assert calls == [], "a binding edit or a config alternation must not replay history"
    assert delivery_features.projection_generation() == delivery_features.projection_generation()


@pytest.mark.asyncio
async def test_skipped_projections_are_visible_in_service_info_and_run_detail(service):
    store, request = service
    store.submit(request)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        delivery_features.record_projection(
            db, store.config, "run-1", revision=1, error="ValueError: registry unreadable")
    assert store.detail("run-1")["feature_projection_error"] == "ValueError: registry unreadable"
    configuration = json.loads(store.config.path.read_text())
    configuration["dashboard_url"] = "http://127.0.0.1:18770"
    store.config.path.write_text(json.dumps(configuration))
    app = create_app(store.config.path)
    transport = httpx.ASGITransport(app=app, client=("127.0.0.1", 10001))
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://127.0.0.1:18770") as browser:
        info = (await browser.get("/api/service")).json()
    assert [(item["run_id"], item["error"]) for item in info["feature_projection_failures"]] == [
        ("run-1", "ValueError: registry unreadable")]


def test_construction_survives_an_unreadable_historical_feature_row(
    service, tmp_path, caplog, monkeypatch
):
    store, request = service
    store.submit(request)
    store.submit(_second(request))
    exposed = tmp_path / "registry-not-private"
    exposed.mkdir(mode=0o755)
    os.chmod(exposed, 0o755)
    with store._connect() as db:
        spec = json.loads(db.execute(
            "SELECT request_json FROM delivery_runs WHERE run_id='run-2'").fetchone()[0])
        spec["feature_delivery"] = {
            "version": 1, "registry": str(exposed / "registry.sqlite3"),
            "owner": {"issue_id": "I_x", "run_id": "run-2",
                      "store_path": str(store.config.tracking_db), "generation": 1},
            "snapshot": {}, "admitted_at": "2026-10-11T00:00:00+00:00",
        }
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE delivery_runs SET request_json=?,revision=revision+1 "
                   "WHERE run_id='run-2'",
                   (json.dumps(spec, sort_keys=True, separators=(",", ":")),))
    with pytest.raises(ValueError, match="private"), store._connect() as db:
        delivery_features.transition(db, store.config, "run-2")
    calls = _counting_transition(monkeypatch)
    with caplog.at_level(logging.WARNING, logger="devflow_temporal.delivery_features"):
        rebuilt = DeliveryStore(store.config)
    assert calls == ["run-2"]
    assert any("run-2" in record.getMessage() and "private" in record.getMessage()
               for record in caplog.records)
    assert {item["run_id"] for item in rebuilt.pending_starts()} == {"run-1", "run-2"}
    assert rebuilt.detail("run-1")["feature_projection_error"] is None
    with rebuilt._connect() as db:
        marker = db.execute("SELECT revision,error FROM delivery_feature_projections "
                            "WHERE run_id='run-2'").fetchone()
        assert marker["revision"] == 2 and "private" in marker["error"]
        # Operators see the skipped row (service info lists these; see below).
        assert [item["run_id"] for item in delivery_features.projection_failures(db)] == ["run-2"]
        assert delivery_features.current(db, request["issue_url"])["status"] == "Queued"
    calls.clear()
    DeliveryStore(store.config)
    assert calls == [], "a recorded skip is not retried on every construction"
    _bump_revision(store, "run-2")
    DeliveryStore(store.config)
    assert calls == ["run-2"], "a changed row is tried again"
