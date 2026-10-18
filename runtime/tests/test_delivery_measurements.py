"""The ordinary reporting entry point measures the real execution path read-only."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.common.v1 import ActivityType, Memo, Payloads, WorkflowType
from temporalio.api.history.v1 import (
    ActivityTaskCompletedEventAttributes,
    ActivityTaskScheduledEventAttributes,
    HistoryEvent,
    WorkflowExecutionCompletedEventAttributes,
    WorkflowExecutionStartedEventAttributes,
)
from temporalio.client import WorkflowHistory
from test_delivery_store import service as service

from devflow_temporal import delivery_closures, delivery_store
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "skills/devflow/scripts"))
import delivery_measurements  # noqa: E402
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


def test_metrics_does_not_create_missing_database(tmp_path):
    missing = tmp_path / "absent.sqlite3"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "skills/devflow/scripts/state.py"),
            "--db",
            str(missing),
            "metrics",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 and not missing.exists()


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


def test_historical_compressed_payload_uses_public_clock_and_closed_result(service, monkeypatch):
    store, request = service
    monkeypatch.setattr(delivery_store, "_now", lambda: stamp(0))
    store.submit(request)
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
        db.execute("UPDATE delivery_events SET payload_json='{}'")
        db.execute("DELETE FROM delivery_phase_events")
        db.execute(
            "INSERT INTO delivery_events(run_id,timestamp,type,message,"
            "run_revision,payload_json) "
            "VALUES(?,?,?,?,?,?)",
            (
                "run-1",
                stamp(12),
                "role_started",
                "Implement",
                2,
                json.dumps({"key": "implement:0"}),
            ),
        )

    async def build():
        async def payload(value):
            return Payloads(payloads=await DELIVERY_DATA_CONVERTER.encode([value]))

        def t(seconds):
            value = Timestamp()
            value.FromDatetime(datetime.fromisoformat(stamp(seconds)))
            return value

        memo = Memo(fields={"request_digest": (await payload(row["request_digest"])).payloads[0]})
        # The normal codec compresses this projection; decoder must not assume plain JSON.
        projection = await payload(
            {
                "phase": "implement",
                "iteration": 0,
                "execution_state": "running",
                "key": "implement:0",
                "event_type": "role_started",
                "checks": {"large": "x" * 300000},
            }
        )
        events = [
            HistoryEvent(
                event_id=1,
                event_time=t(1),
                workflow_execution_started_event_attributes=WorkflowExecutionStartedEventAttributes(
                    workflow_type=WorkflowType(name="DevflowDeliveryWorkflow"),
                    workflow_id="delivery-run-1",
                    original_execution_run_id="execution-1",
                    memo=memo,
                    input=await payload({"run_id": "run-1"}),
                ),
            ),
            HistoryEvent(
                event_id=2,
                event_time=t(10),
                activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(
                    activity_type=ActivityType(name="delivery_project"), input=projection
                ),
            ),
            HistoryEvent(
                event_id=3,
                event_time=t(15),
                activity_task_completed_event_attributes=ActivityTaskCompletedEventAttributes(
                    scheduled_event_id=2, result=await payload({})
                ),
            ),
            # An uncompleted terminal projection cannot be counted as a successful transition.
            HistoryEvent(
                event_id=4,
                event_time=t(25),
                activity_task_scheduled_event_attributes=ActivityTaskScheduledEventAttributes(
                    activity_type=ActivityType(name="delivery_project"),
                    input=await payload(
                        {"phase": "blocked", "event_type": "stopped", "key": "stop"}
                    ),
                ),
            ),
            HistoryEvent(
                event_id=5,
                event_time=t(30),
                workflow_execution_completed_event_attributes=WorkflowExecutionCompletedEventAttributes(
                    result=await payload({"phase": "blocked", "outcome": "blocked"})
                ),
            ),
        ]
        return WorkflowHistory("delivery-run-1", events)

    history = asyncio.run(build())
    root = Path(store.config.state_root) / "closures"
    root.mkdir(mode=0o700)
    digest = delivery_closures._write_sealed(root, delivery_closures.history_json(history))
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_closures VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "delivery-run-1",
                "execution-1",
                "run-1",
                "COMPLETED",
                stamp(30),
                row["request_digest"],
                None,
                digest,
                5,
                "temporal",
                None,
                stamp(31),
            ),
        )
    with readonly(store) as db:
        result = state.metrics(db, "work-1", stamp(100))["delivery_execution"]
    assert result["coverage"]["complete"]
    assert result["coverage"]["failed_projections_excluded"] == 1
    assert result["coverage"]["matched_public_events"] == 1
    assert result["works"]["work-1"]["coordinator"] == {
        "accepted": 12,
        "implement": 18,
        "blocked": 70,
    }
    assert result["works"]["work-1"]["coordinator_reconciliation_seconds"] == 0
    # Corrupt/missing evidence must remain a visible coverage failure.
    (root / (digest + ".json.xz")).unlink()
    with readonly(store) as db:
        result = state.metrics(db, "work-1", stamp(100))["delivery_execution"]
    assert not result["coverage"]["complete"]
    assert result["works"]["work-1"]["coordinator"] == {"unknown": 100}


def test_successor_gap_and_parallel_children_are_separate(service, monkeypatch):
    store, request = service
    monkeypatch.setattr(delivery_store, "_now", lambda: stamp(0))
    store.submit(request)
    with store._connect() as db:
        template = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
        db.execute("DELETE FROM delivery_events")
        for rid, created, parent in [
            ("run-2", 50, None),
            ("worker-1", 2, "run-1"),
            ("worker-2", 4, "run-1"),
        ]:
            row = {**template, "run_id": rid, "created_at": stamp(created)}
            spec = json.loads(row["request_json"])
            if parent:
                spec["feature_worker"] = {"parent_run_id": parent}
            row["request_json"] = json.dumps(spec)
            db.execute(
                f"INSERT INTO delivery_runs({','.join(row)}) VALUES({','.join('?' for _ in row)})",
                tuple(row.values()),
            )
        for rid, phase, start, terminal in [
            ("run-1", "implement", 0, False),
            ("run-1", "blocked", 20, True),
            ("run-2", "feature_integration", 50, False),
            ("worker-1", "implement", 2, False),
            ("worker-1", "delivered", 12, True),
            ("worker-2", "review", 4, False),
            ("worker-2", "delivered", 14, True),
        ]:
            obs = {
                "workflow_id": "delivery-" + rid,
                "phase": phase,
                "iteration": 0,
                "execution_state": "terminal" if terminal else "running",
            }
            db.execute(
                "INSERT INTO delivery_events(run_id,timestamp,type,message,"
                "run_revision,payload_json) "
                "VALUES(?,?,?,?,?,?)",
                (rid, stamp(start), "test", "test", 1, "{}"),
            )
            db.execute(
                "INSERT INTO delivery_phase_events VALUES(last_insert_rowid(),?,?,?,?,?,?)",
                (rid, obs["workflow_id"], phase, obs["execution_state"], 0, stamp(start)),
            )
    with readonly(store) as db:
        result = state.metrics(db, "work-1", stamp(100))["delivery_execution"]
    summary = result["works"]["work-1"]
    assert summary["coordinator"] == {"implement": 20, "blocked": 30, "feature_integration": 50}
    assert summary["worker"] == {"implement": 10, "review": 10}
    assert summary["elapsed_seconds"] == 100 and summary["coordinator_reconciliation_seconds"] == 0
    assert result["coverage"]["complete"]


def test_naive_cutoff_rejected(service):
    store, _ = service
    with readonly(store) as db, pytest.raises(ValueError, match="timezone"):
        delivery_measurements.summarize(db, as_of="2026-10-18T01:05:35")
