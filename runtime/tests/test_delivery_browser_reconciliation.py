"""Owner recovery guards; Temporal/native/tracker boundaries are explicit doubles.

Real completed-process replay is exercised in test_delivery_browser_evidence_limits.
"""
from __future__ import annotations

import copy
import hashlib
import json
import sqlite3
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_browser_qa import reconcile_browser_attempt


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    from devflow_temporal import delivery_automatic_retry, delivery_broker, delivery_browser_qa
    from devflow_temporal import delivery_interrupted_recovery, delivery_policy_recovery

    candidate = {"id": "exact-candidate", "head": "a" * 40}
    publication = {"number": 1, "head": candidate["head"], "state": "OPEN"}
    checks = {"browser_qa": {"state": "unknown", "cleanup": "unknown", "reason": "ValueError"}}
    row = {
        "run_id": "run-owned", "phase": "blocked", "outcome": "blocked",
        "execution_state": "blocked", "iteration": 2, "protocol_revision": 7,
        "error": "browser QA child cleanup is unknown", "cleanup": "unknown",
        "workflow_id": "delivery-run-owned", "request_digest": "request-digest",
        "recovery_json": None, "checks_json": json.dumps(checks),
        "candidate_json": json.dumps(candidate), "pr_json": json.dumps(publication),
        "tracker_json": "null",
    }
    state = {"run_id": row["run_id"], "revision": row["protocol_revision"],
             **{k: row[k] for k in ("phase", "outcome", "execution_state", "iteration", "error")},
             "checks": checks, "candidate": candidate, "pull_request": publication,
             "tracker": None}
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": "closed-execution",
              "closed_at": "2026-10-18T22:04:00+00:00", "request_digest": row["request_digest"],
              "recovery_digest": None, "result": state}
    database = tmp_path / "owner.sqlite3"

    def connect():
        db = sqlite3.connect(database)
        db.row_factory = sqlite3.Row
        return db

    with connect() as db:
        db.execute("CREATE TABLE delivery_runs (" + ",".join(
            key + (" INTEGER" if isinstance(value, int) else " TEXT") for key, value in row.items()
        ) + ")")
        db.execute("INSERT INTO delivery_runs VALUES (" + ",".join("?" for _ in row) + ")",
                   tuple(row.values()))
        db.execute("CREATE TABLE delivery_effects (effect_key TEXT,run_id TEXT,kind TEXT,"
                   "state TEXT,observed_json TEXT)")
        db.execute("INSERT INTO delivery_effects VALUES (?,?,?,?,NULL)",
                   ("browser_qa:run-owned:2", "run-owned", "browser_qa", "pending"))
        db.execute("CREATE TABLE delivery_attempts (run_id TEXT,state TEXT)")
        db.execute("CREATE TABLE delivery_mutations (run_id TEXT,state TEXT,kind TEXT)")
    spec = {"provider": "codex", "run_id": row["run_id"]}
    store = SimpleNamespace(config=object(), _connect=connect, effective_spec=lambda _: spec,
                            owns_execution=lambda _: True,
                            _completed_temporal_result=lambda *a, **k: copy.deepcopy(closed))
    gate = tmp_path / "gate"
    gate.mkdir()
    receipt = tmp_path / "receipt.json"
    receipt.write_text("retained failed gate")
    result = {"state": "failed", "cleanup": "confirmed", "test_count": 10,
              "receipt": str(receipt), "receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest()}
    calls = []

    def reconcile(iteration, current, *, reconcile_only):
        assert iteration == 2 and current == candidate and reconcile_only is True
        calls.append("reconcile")
        return result

    broker = SimpleNamespace(evidence_dir=tmp_path, state_dir=tmp_path,
                             effect_namespace="", candidate=lambda: candidate,
                             _gate_path=lambda *a: gate, run_browser_qa=reconcile)

    def cleanup(*args, published):
        assert args[0] is store and args[2] is spec and args[3] == closed
        assert published is True and args[4]() is False
        assert closed["result"] == state
        calls.append("cleanup")
        return {"cleanup": "confirmed"}

    monkeypatch.setattr(delivery_interrupted_recovery, "installation_recovery_lease",
                        lambda _: nullcontext())
    monkeypatch.setattr(delivery_broker, "DeliveryBroker", lambda *_: broker)
    monkeypatch.setattr(delivery_browser_qa, "candidate_for", lambda _: candidate)
    monkeypatch.setattr(delivery_automatic_retry, "_close_cleanup", cleanup)
    monkeypatch.setattr(delivery_policy_recovery, "reconciled_cleanup", lambda *a, **k: {"bound": True})
    return SimpleNamespace(store=store, closed=closed, state=state, spec=spec, calls=calls,
                           broker=broker, gate=gate, result=result)


def test_owner_reconciliation_keeps_failed_verdict_and_closed_result(recovery):
    before = copy.deepcopy(recovery.closed)
    result = reconcile_browser_attempt(recovery.store, "run-owned", 2)
    assert result["state"] == "finished" and result["cleanup"] == "confirmed"
    assert result["gate_state"] == "failed" and result["test_count"] == 10
    assert recovery.calls == ["reconcile", "cleanup"]
    assert recovery.closed == before


@pytest.mark.parametrize("drift", ["active", "foreign_owner", "closed_identity", "candidate",
                                    "other_effect", "mutation", "unfinished_role", "missing_gate"])
def test_owner_reconciliation_refuses_changed_authority_before_effects(recovery, drift):
    if drift == "foreign_owner":
        recovery.store.owns_execution = lambda _: False
    elif drift == "closed_identity":
        recovery.closed["request_digest"] = "different"
    elif drift == "candidate":
        recovery.broker.candidate = lambda: {"id": "different"}
    elif drift == "missing_gate":
        recovery.gate.rmdir()
    else:
        with recovery.store._connect() as db:
            if drift == "active":
                db.execute("UPDATE delivery_runs SET phase='checks'")
            elif drift == "other_effect":
                db.execute("INSERT INTO delivery_effects VALUES ('other','run-owned','publish','pending',NULL)")
            elif drift == "mutation":
                db.execute("INSERT INTO delivery_mutations VALUES ('run-owned','pending','cancel')")
            elif drift == "unfinished_role":
                db.execute("INSERT INTO delivery_attempts VALUES ('run-owned','running')")
    with recovery.store._connect() as db:
        before = db.serialize()
    with pytest.raises(ValueError):
        reconcile_browser_attempt(recovery.store, "run-owned", 2)
    assert recovery.calls == []
    with recovery.store._connect() as db:
        assert db.serialize() == before


def test_finished_reconciliation_is_observed_without_replay(recovery):
    with recovery.store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='confirmed'")
        db.execute("UPDATE delivery_effects SET state='complete',observed_json=?",
                   (json.dumps(recovery.result),))
    result = reconcile_browser_attempt(recovery.store, "run-owned", 2)
    assert result["state"] == "finished" and result["changed"] is False
    assert recovery.calls == []
