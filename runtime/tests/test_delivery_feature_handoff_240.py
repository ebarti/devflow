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
    return assess_transition(store, "run-1", kind, {"expected_revision": revision})


def test_active_owner_is_observed_without_mutation(feature):
    store, shared, _snapshot, _token = feature
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assessment = _observe(store)
    assert not assessment["eligible"]
    assert "feature_owner_not_stopped" in _codes(assessment)
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    assert shared.current("feature")["state"] == "active"


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


def test_unknown_effect_and_stale_revision_are_reported_without_settlement(feature):
    store, shared, _snapshot, token = feature
    shared.intent(token, "publish:one", "publish", {"head": "abc"})
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assessment = _observe(store, revision=6)
    assert {"feature_effects_unsettled", "revision_changed"} <= _codes(assessment)
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    assert shared.effect("feature", "publish:one")["state"] == "pending"


def test_revision_observes_existing_budget_without_changing_it(feature):
    store, shared, _snapshot, token = feature
    shared.stop(token, "stop-1", {})
    before = shared.budget("feature")
    assessment = _observe(store, "revise_feature_plan")
    assert "feature_product_budget_exhausted" not in _codes(assessment)
    assert "feature_successor_admission_proof_unknown" in _codes(assessment)
    assert shared.budget("feature") == before


@pytest.mark.parametrize("state", ["pending", "unknown"])
def test_unsettled_effect_blocks_preview_and_takeover_with_same_predicate(feature, state):
    store, shared, snapshot, token = feature
    shared.intent(token, "publish:one", "publish", {"head": "abc"})
    with shared.connect() as db:
        db.execute("UPDATE execution_claims SET state='stopped' WHERE issue_id='feature'")
        db.execute("UPDATE execution_effects SET state=? WHERE effect_key='publish:one'", (state,))
    before = shared.budget("feature")
    preview = _observe(store)
    assert "feature_effects_unsettled" in _codes(preview)
    with pytest.raises(RecoveryRefused) as failure:
        shared.claim(snapshot, "run-2", str(store.config.tracking_db), predecessor=token)
    assert "feature_effects_unsettled" in _codes(failure.value.assessment)
    assert shared.effect("feature", "publish:one")["state"] == state
    assert shared.budget("feature") == before


def test_historical_absent_effect_and_identical_successor_replay(feature):
    store, shared, snapshot, token = feature
    shared.intent(token, "publish:absent", "publish", {"head": "abc"})
    shared.finish_effect(token, "publish:absent", {"reason": "no write"}, no_effect=True)
    shared.stop(token, "stop-1", {})
    before = shared.budget("feature")
    assessment = _observe(store)
    assert "feature_effects_unsettled" not in _codes(assessment)
    successor = shared.claim(snapshot, "run-2", str(store.config.tracking_db), predecessor=token)
    replay = shared.claim(snapshot, "run-2", str(store.config.tracking_db), predecessor=token)
    assert replay == successor
    assert shared.effect("feature", "publish:absent")["state"] == "absent"
    assert shared.budget("feature") == before


def test_revision_reports_exhausted_product_budget_without_new_debit(feature):
    store, shared, _snapshot, token = feature
    for index in range(3):
        shared.repair(token, f"repair-{index}", "existing product finding")
    shared.stop(token, "stop-1", {})
    before = shared.budget("feature")
    assessment = _observe(store, "revise_feature_plan")
    assert "feature_product_budget_exhausted" in _codes(assessment)
    assert shared.budget("feature") == before


def test_closed_coordinator_proof_is_observed_without_sealing(feature, monkeypatch):
    from devflow_temporal.delivery_feature_handoff import readonly_feature_spec

    store, shared, _snapshot, token = feature
    shared.stop(token, "stop-1", {})
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assert readonly_feature_spec(store, "run-1")["feature_delivery"]["owner"] == token
    calls = []

    def closure(_store, run_id, *, preserve):
        calls.append((run_id, preserve))
        return {"workflow_id": "delivery-run-1", "result": {"run_id": run_id}}

    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator", closure)
    assessment = _observe(store)
    assert calls == [("run-1", False)]
    assert "feature_coordinator_closure_unknown" not in _codes(assessment)
    assert "feature_successor_admission_proof_unknown" in _codes(assessment)
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before


def test_unreadable_coordinator_closure_stays_unknown(feature, monkeypatch):
    store, shared, _snapshot, token = feature
    shared.stop(token, "stop-1", {})

    def unavailable(*_args, **_kwargs):
        raise OSError("closure unavailable")

    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator", unavailable)
    assessment = _observe(store)
    assert "feature_coordinator_closure_unknown" in _codes(assessment)
    assert not assessment["eligible"]


def test_stopped_continuation_observes_shared_plan_validator(feature):
    store, shared, _snapshot, token = feature
    shared.stop(token, "stop-1", {})
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    assessment = assess_transition(
        store, "run-1", "continue_feature", {"command_id": "continue-1", "expected_revision": 7}
    )
    assert "feature_plan_authority_unknown" not in _codes(assessment)
    assert "feature_successor_admission_proof_unknown" in _codes(assessment)
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before


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
