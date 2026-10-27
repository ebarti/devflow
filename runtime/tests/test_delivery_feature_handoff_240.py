"""The feature handoff observes registry custody without executing settlement."""

import hashlib
import json
import sqlite3
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from devflow_temporal.delivery_lifecycle import RecoveryRefused, assess_transition


class ObservationStore:
    def __init__(self, path):
        self.config = SimpleNamespace(tracking_db=path)

    def _connect(self):
        db = sqlite3.connect(self.config.tracking_db)
        db.row_factory = sqlite3.Row
        return db

    @staticmethod
    def _prepared_original(_db, spec):
        return spec


@pytest.fixture
def feature(tmp_path):
    tracking = tmp_path / "tracking.sqlite3"
    shared = ExecutionRegistry(tmp_path / "registry" / "state.sqlite3")
    snapshot = {
        "issue": {
            "id": "feature",
            "url": "https://github.com/o/r/issues/1",
            "repository_id": "repo",
        },
        "body": "Feature",
        "plan": None,
    }
    token = shared.claim(snapshot, "run-1", str(tracking), maximum_repairs=3)
    spec = {"run_id": "run-1", "feature_delivery": {"owner": token, "registry": str(shared.path)}}
    store = ObservationStore(tracking)
    with store._connect() as db:
        db.execute("CREATE TABLE delivery_commands (command_id TEXT, run_id TEXT, "
                   "request_digest TEXT, response_json TEXT)")
        db.execute(
            "CREATE TABLE delivery_runs (run_id TEXT, revision INTEGER, outcome TEXT, "
            "cleanup TEXT, request_json TEXT, recovery_json TEXT, accepted_plan_text TEXT)"
        )
        db.execute(
            "INSERT INTO delivery_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("run-1", 7, "blocked", "confirmed", json.dumps(spec), None, None),
        )
    return store, shared, snapshot, token


def _codes(assessment):
    return {item["code"] for item in assessment["blockers"]}


def _observe(store, kind="continue_feature", revision=7):
    if kind == "revise_feature_plan":
        from devflow_temporal.delivery_feature_handoff import observe_handoff

        return observe_handoff(store, "run-1", {"expected_revision": revision}, operation=kind)
    return assess_transition(store, "run-1", kind, {"expected_revision": revision})


def test_stopped_observation_and_real_takeover_share_owner_proof(feature):
    store, shared, snapshot, token = feature
    shared.stop(token, "stop-1", {})
    budget = shared.budget("feature")
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assessment = _observe(store)
    assert not assessment["eligible"]  # successor admission still needs its own proof
    assert assessment["proof_digest"]
    assert "feature_owner_changed" not in _codes(assessment)
    assert "feature_owner_not_stopped" not in _codes(assessment)
    assert "feature_successor_admission_proof_unknown" in _codes(assessment)
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    assert shared.budget("feature") == budget
    next_token = shared.claim(snapshot, "run-2", str(store.config.tracking_db), predecessor=token)
    assert next_token["generation"] == token["generation"] + 1
    assert shared.budget("feature") == budget
    stale = _observe(store)
    assert "feature_owner_changed" in _codes(stale)
    with pytest.raises(RecoveryRefused) as failure:
        shared.claim(snapshot, "run-3", str(store.config.tracking_db), predecessor=token)
    assert isinstance(failure.value, OwnershipConflict)
    assert "feature_owner_changed" in _codes(failure.value.assessment)


def test_changed_adopted_plan_is_unknown_without_mutation(feature):
    store, shared, _snapshot, token = feature
    shared.stop(token, "stop-1", {})
    with store._connect() as db:
        spec = json.loads(db.execute("SELECT request_json FROM delivery_runs").fetchone()[0])
        spec["accepted_plan"] = "{}"
        db.execute("UPDATE delivery_runs SET request_json=?", (json.dumps(spec),))
    with shared.connect() as db:
        shared._checkpoint(db, token, "plan-revision:adopted:1", {
            "identity": {"plan_revision": 1, "plan_digest": "different"},
        })
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assessment = assess_transition(
        store, "run-1", "continue_feature", {"command_id": "continue-1", "expected_revision": 7}
    )
    assert "feature_plan_authority_unknown" in _codes(assessment)
    assert not assessment["eligible"]
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
