"""Later ordinary maintenance composes over a bound published cleanup anchor."""
from __future__ import annotations

import copy
import json
import sqlite3
from types import SimpleNamespace

import pytest

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_policy_recovery import reconciled_cleanup


@pytest.fixture
def cleanup_chain(tmp_path):
    state = {"run_id": "run-owned", "cleanup": "unknown", "iteration": 2,
             "checks": {"browser_qa": {"state": "unknown"}}}
    closed = {"workflow_id": "delivery-owned", "execution_run_id": "execution-owned",
              "closed_at": "2026-10-18T22:04:00+00:00", "request_digest": "request",
              "recovery_digest": None, "result": state}
    tracker = {"state": "recorded", "claim_released": True}
    old = {"state": "confirmed", "resource_cleanup": "confirmed",
           "process_cleanup": "observed-native-confirmed", "processes": [{"journal": "owned"}],
           "receipt_sha256": "old"}
    latest = {**old, "receipt_sha256": "latest"}
    anchor = {"closure": "published", "original_result_digest": digest(state),
              "closed_workflow": {k: v for k, v in closed.items() if k != "result"},
              "resource_cleanup": old, "tracker": tracker}
    key = digest({"closed_workflow": {**{k: closed[k] for k in (
        "workflow_id", "execution_run_id", "closed_at")}, "status": "COMPLETED"},
        "native_observations": latest["processes"], "resource_cleanup": latest})
    events = [
        {"sequence": 10, "run_revision": 33, "type": "resource_cleanup_reconciled",
         "timestamp": "2026-10-18T22:24:00+00:00", "payload_json": json.dumps(anchor)},
        {"sequence": 11, "run_revision": 34, "type": "resource_cleanup_reconciled",
         "timestamp": "2026-10-18T22:25:00+00:00",
         "payload_json": json.dumps({"key": key, "iteration": 2, "candidate_id": None})},
    ]
    database = tmp_path / "owner.sqlite3"

    def connect():
        db = sqlite3.connect(database)
        db.row_factory = sqlite3.Row
        return db

    with connect() as db:
        db.execute(
            "CREATE TABLE delivery_events (run_id TEXT,sequence INTEGER,run_revision INTEGER,"
            "type TEXT,timestamp TEXT,payload_json TEXT)"
        )
        for event in events:
            db.execute("INSERT INTO delivery_events VALUES (?,?,?,?,?,?)",
                       (state["run_id"], *event.values()))
    row = {"run_id": state["run_id"], "cleanup": "confirmed", "iteration": 2, "revision": 34,
           "updated_at": events[-1]["timestamp"], "request_json": "{}",
           "checks_json": json.dumps({**state["checks"], "resource_cleanup": latest}),
           "tracker_json": json.dumps(tracker)}
    store = SimpleNamespace(_connect=connect)
    return SimpleNamespace(store=store, row=row, state=state, closed=closed, latest=latest,
                           anchor=anchor, events=events)


def test_published_cleanup_accepts_later_contiguous_owner_observation(cleanup_chain):
    c = cleanup_chain
    original = copy.deepcopy(c.state)
    with c.store._connect() as db:
        before = db.serialize()
    proof = reconciled_cleanup(c.store, c.row, c.state, closed=c.closed)
    assert proof is not None
    assert proof["resource_cleanup"] == c.latest
    assert proof["tracker"] == c.anchor["tracker"]
    assert c.state == original
    with c.store._connect() as db:
        assert db.serialize() == before


@pytest.mark.parametrize("drift", ["foreign_write", "revision_gap", "execution", "receipt",
                                    "tracker", "candidate", "iteration", "timestamp"])
def test_published_cleanup_refuses_an_unbound_chain(cleanup_chain, drift):
    c = cleanup_chain
    if drift == "execution":
        c.closed["execution_run_id"] = "different-execution"
    elif drift == "receipt":
        checks = json.loads(c.row["checks_json"])
        checks["resource_cleanup"]["receipt_sha256"] = "changed"
        c.row["checks_json"] = json.dumps(checks)
    elif drift == "tracker":
        c.row["tracker_json"] = json.dumps({"state": "changed"})
    else:
        event = copy.deepcopy(c.events[-1])
        payload = json.loads(event["payload_json"])
        if drift == "foreign_write":
            event["type"] = "role_started"
        elif drift == "revision_gap":
            event["run_revision"] = 35
        elif drift == "candidate":
            payload["candidate_id"] = "different"
        elif drift == "iteration":
            payload["iteration"] = 3
        elif drift == "timestamp":
            event["timestamp"] = c.closed["closed_at"]
        event["payload_json"] = json.dumps(payload)
        with c.store._connect() as db:
            db.execute(
                "UPDATE delivery_events SET run_revision=?,type=?,timestamp=?,payload_json=? "
                "WHERE sequence=11",
                (event["run_revision"], event["type"], event["timestamp"], event["payload_json"]),
            )
    assert reconciled_cleanup(c.store, c.row, c.state, closed=c.closed) is None


@pytest.mark.parametrize('foreign', [False, True])
def test_multiple_observations_require_every_advancing_write_to_be_cleanup(cleanup_chain, foreign):
    c = cleanup_chain
    final = copy.deepcopy(c.events[-1])
    with c.store._connect() as db:
        db.execute("UPDATE delivery_events SET type=? WHERE sequence=11",
                   ('role_started' if foreign else 'resource_cleanup_reconciled',))
        db.execute("INSERT INTO delivery_events VALUES (?,?,?,?,?,?)",
                   (c.row['run_id'], 12, 35, final['type'], '2026-10-18T22:26:00+00:00',
                    final['payload_json']))
    c.row.update(revision=35, updated_at='2026-10-18T22:26:00+00:00')
    proof = reconciled_cleanup(c.store, c.row, c.state, closed=c.closed)
    assert (proof is None) is foreign
