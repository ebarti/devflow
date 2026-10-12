"""Store construction does schema setup only; the service pass maintains projections."""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading

import httpx
import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_features
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_features import NEWEST_RUN_SQL, maintain_projections
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


def _markers(store):
    with store._connect() as db:
        return [tuple(row) for row in db.execute(
            "SELECT run_id,revision,generation,error FROM delivery_feature_projections "
            "ORDER BY run_id")]


def test_construction_never_projects_and_the_service_pass_reprojects_changed_rows(
    service, monkeypatch
):
    store, request = service
    store.submit(request)
    store.submit(_second(request))
    calls = _counting_transition(monkeypatch)
    _bump_revision(store, "run-1")
    monkeypatch.setattr(delivery_features, "projection_generation", lambda: "upgraded")
    DeliveryStore(store.config)
    assert calls == [], "construction does no projection work, however stale the history"
    result = maintain_projections(store)
    assert calls == ["run-1", "run-2"]
    assert (result["pending"], result["projected"], result["skipped"]) == (2, 2, {})
    calls.clear()
    assert maintain_projections(store)["pending"] == 0 and calls == []
    _bump_revision(store, "run-2")
    assert maintain_projections(store)["projected"] == 1 and calls == ["run-2"]
    assert [marker[1:3] for marker in _markers(store)] == [(2, "upgraded"), (2, "upgraded")]


def test_construction_from_another_install_or_config_does_nothing(service, tmp_path, monkeypatch):
    """The CLI, MCP and activities (sealed snapshots) may run other package
    bytes or configurations; only the service re-projects, so they never replay
    history or touch its markers."""
    store, request = service
    store.submit(request)
    store.submit(_second(request))
    before = _markers(store)
    edited = json.loads(store.config.path.read_text())
    edited["repositories"]["fixture"]["assignee"] = "octocat"
    sealed_path = tmp_path / "sealed-snapshot.json"
    sealed_path.write_text(json.dumps(edited))
    sealed = DeliveryConfig.load(sealed_path)
    calls = _counting_transition(monkeypatch)
    monkeypatch.setattr(delivery_features, "projection_generation", lambda: "another-install")
    for config in (store.config, sealed, store.config, sealed):
        DeliveryStore(config)
    assert calls == []
    assert _markers(store) == before


def test_service_pass_commits_per_row_and_holds_no_lock_between_rows(service, monkeypatch):
    store, request = service
    for number in range(1, 6):
        store.submit({**request, "run_id": f"run-{number}", "command_id": f"command-{number}",
                      "work_id": f"work-{number}", "branch": f"feat/fixture-{number}",
                      "issue_url": f"https://github.com/example/fixture/issues/{number}"})
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("DELETE FROM delivery_feature_projections")  # the whole history is stale
    reached, release = threading.Event(), threading.Event()
    original = delivery_features._project_row
    rows = []

    def paused(store_, run_id, generation):
        rows.append(run_id)
        if len(rows) == 3:
            reached.set()
            assert release.wait(5), "maintenance fixture was not released"
        return original(store_, run_id, generation)

    monkeypatch.setattr(delivery_features, "_project_row", paused)
    results = []
    maintainer = threading.Thread(target=lambda: results.append(maintain_projections(store)))
    maintainer.start()
    try:
        assert reached.wait(5), "the pass never reached its third row"
        # Two rows are committed and visible; a writer gets the lock at once.
        probe = sqlite3.connect(store.config.tracking_db, timeout=1)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.execute("UPDATE delivery_runs SET revision=revision+1 WHERE run_id='run-5'")
            committed = {row[0] for row in probe.execute(
                "SELECT run_id FROM delivery_feature_projections")}
            probe.commit()
        finally:
            probe.close()
        assert committed == {"run-1", "run-2"}
    finally:
        release.set()
        maintainer.join(10)
    assert results and results[0]["projected"] == 5 and results[0]["skipped"] == {}
    assert rows == [f"run-{number}" for number in range(1, 6)]
    # run-5 changed while the pass ran; its row was read under its own lock.
    assert [marker[:2] for marker in _markers(store)] == [
        ("run-1", 1), ("run-2", 1), ("run-3", 1), ("run-4", 1), ("run-5", 2)]
    assert maintain_projections(store)["pending"] == 0


def test_newest_run_lookup_uses_the_issue_index(service):
    store, _request = service
    with store._connect() as db:
        plan = db.execute("EXPLAIN QUERY PLAN " + NEWEST_RUN_SQL, ("issue",)).fetchall()
    assert any("delivery_runs_issue_newest" in row[3] for row in plan), plan


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


def test_service_pass_skips_an_unreadable_historical_feature_row(
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
    rebuilt = DeliveryStore(store.config)
    assert calls == [], "construction is independent of the unreadable row"
    with caplog.at_level(logging.WARNING, logger="devflow_temporal.delivery_features"):
        result = maintain_projections(rebuilt)
    assert calls == ["run-2"] and list(result["skipped"]) == ["run-2"]
    assert any("run-2" in record.getMessage() and "private" in record.getMessage()
               for record in caplog.records)
    assert {item["run_id"] for item in rebuilt.pending_starts()} == {"run-1", "run-2"}
    assert rebuilt.detail("run-1")["feature_projection_error"] is None
    with rebuilt._connect() as db:
        marker = db.execute("SELECT revision,error FROM delivery_feature_projections "
                            "WHERE run_id='run-2'").fetchone()
        assert marker["revision"] == 2 and "private" in marker["error"]
        assert [item["run_id"] for item in delivery_features.projection_failures(db)] == ["run-2"]
        assert delivery_features.current(db, request["issue_url"])["status"] == "Queued"
    calls.clear()
    assert maintain_projections(rebuilt)["pending"] == 0 and calls == [], \
        "a recorded skip is retried once per service process, not every pass"
    _bump_revision(store, "run-2")
    maintain_projections(rebuilt)
    assert calls == ["run-2"], "a changed row is tried again"
