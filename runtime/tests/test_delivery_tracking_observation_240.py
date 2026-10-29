"""Observation never creates or changes the original tracking database and sidecars."""

import sqlite3
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_store
from devflow_temporal.delivery_execution_config import observation_mode


def _store(path):
    store = delivery_store.DeliveryStore.__new__(delivery_store.DeliveryStore)
    store.config = SimpleNamespace(tracking_db=path)
    store.read_only = False
    return store


def _live_wal(path):
    writer = sqlite3.connect(path)
    assert writer.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    writer.execute("CREATE TABLE data (value TEXT)")
    writer.commit()
    writer.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    writer.execute("INSERT INTO data VALUES ('only-in-wal')")
    writer.commit()
    return writer


def test_tracking_change_after_last_read_is_detected_at_scope_exit(tmp_path):
    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    try:
        with pytest.raises(sqlite3.OperationalError, match="changed"):
            with observation_mode():
                with _store(path)._connect() as db:
                    assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 1
                writer.execute("INSERT INTO data VALUES ('new-commit')")
                writer.commit()
    finally:
        writer.close()


def test_public_assessment_reports_unknown_when_source_changes_after_last_read(
    tmp_path, monkeypatch,
):
    from devflow_temporal import delivery_lifecycle

    path = tmp_path / "tracking.sqlite3"
    writer = _live_wal(path)
    store = _store(path)

    def assessment(*_args, **_kwargs):
        with store._connect() as db:
            assert db.execute("SELECT count(*) FROM data").fetchone()[0] == 1
        writer.execute("INSERT INTO data VALUES ('concurrent')")
        writer.commit()
        return {"eligible": True}

    monkeypatch.setattr(delivery_lifecycle, "_assess_transition", assessment)
    try:
        result = delivery_lifecycle.assess_transition(store, "run-1", "continue_feature", {})
        assert result["eligible"] is False
        assert "tracking_store_unavailable" in {item["code"] for item in result["blockers"]}
    finally:
        writer.close()
