"""The ordinary reporting entry point measures the real execution path read-only."""

from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

from test_delivery_store import service as service

from devflow_temporal import delivery_store

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "skills/devflow/scripts"))
import state  # noqa: E402


def stamp(seconds):
    return datetime.fromtimestamp(1760000000 + seconds, tz=UTC).isoformat()


def readonly(store):
    db = sqlite3.connect(Path(store.config.tracking_db).as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    return db


def test_real_projection_and_cli_report_open_and_terminal_time_without_writes(service, monkeypatch):
    store, request = service
    clock = [stamp(0)]
    monkeypatch.setattr(delivery_store, "_now", lambda: clock[0])
    store.submit(request)
    with readonly(store) as db:
        assert (
            json.loads(db.execute("SELECT payload_json FROM delivery_events").fetchone()[0]) == {}
        )
        assert db.execute("SELECT count(*) FROM delivery_phase_events").fetchone()[0] == 1
    clock[0] = stamp(10)
    store.project(
        "run-1",
        phase="implement",
        execution_state="running",
        event_type="role_started",
        message="Implement",
        key="implement:0",
        iteration=0,
    )
    with readonly(store) as db:
        statements = []
        db.set_trace_callback(statements.append)
        report = state.metrics(db, "work-1", stamp(20))["delivery_execution"]
        assert report["coverage"]["complete"]
        assert report["works"]["work-1"]["coordinator"] == {"accepted": 10, "implement": 10}
        assert not any(
            s.startswith(("INSERT", "UPDATE", "CREATE", "BEGIN IMMEDIATE")) for s in statements
        )
    # Terminal phase/cleanup observations must not keep adding elapsed time forever.
    clock[0] = stamp(30)
    store.project(
        "run-1",
        phase="delivered",
        execution_state="terminal",
        event_type="delivered",
        message="Delivered",
        outcome="delivered",
    )
    clock[0] = stamp(90)
    store.project(
        "run-1",
        phase="delivered",
        execution_state="terminal",
        event_type="resource_cleanup_reconciled",
        message="Cleanup",
        outcome="delivered",
    )
    command = [
        sys.executable,
        str(ROOT / "skills/devflow/scripts/state.py"),
        "--db",
        str(store.config.tracking_db),
        "metrics",
        "--work-id",
        "work-1",
        "--as-of",
        stamp(100),
    ]
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    report = json.loads(result.stdout)
    assert report["timing"]["delivery_phase_seconds"]["work-1"] == {"accepted": 10, "implement": 20}
    assert report["delivery_execution"]["works"]["work-1"]["elapsed_seconds"] == 30
    assert report["delivery_execution"]["coverage"]["complete"]


def test_missing_old_history_is_unknown_not_current_phase(service, monkeypatch):
    store, request = service
    monkeypatch.setattr(delivery_store, "_now", lambda: stamp(0))
    store.submit(request)
    with store._connect() as db:
        db.execute("UPDATE delivery_events SET payload_json='{}'")
        db.execute("DELETE FROM delivery_phase_events")
    with readonly(store) as db:
        result = state.metrics(db, "work-1", stamp(100))["delivery_execution"]
    assert not result["coverage"]["complete"]
    assert result["works"]["work-1"]["coordinator"] == {"unknown": 100}
