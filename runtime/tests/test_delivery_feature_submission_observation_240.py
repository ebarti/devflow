"""Feature configuration proof reuses the actual unchanged successor admission."""

import hashlib
import json

import pytest
from test_delivery_feature_revision_settlement import _stopped_feature

from devflow_temporal.delivery_feature_execution import continue_feature
from devflow_temporal.delivery_lifecycle import assess_transition

pytest_plugins = ["test_delivery_feature_revision_settlement"]


def test_stopped_successor_config_observation_and_execution(feature, monkeypatch):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda *_args, **_kwargs: {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"},
    )
    request = {"command_id": "continue-observation", "expected_revision": row["revision"]}
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    with store._connect() as db:
        commands = db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0]
    assessment = assess_transition(store, spec["run_id"], "continue_feature", request)
    codes = {item["code"] for item in assessment["blockers"]}
    assert "feature_plan_authority_unknown" not in codes
    assert "feature_submission_config_unknown" not in codes
    assert "feature_repository_access_unknown" not in codes
    assert "feature_work_claim_unknown" not in codes
    assert "feature_registry_claim_unknown" not in codes
    assert assessment["eligible"] is True
    assert assessment["proof_digest"]
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0] == commands
    result = continue_feature(store, spec["run_id"], request)
    assert store.effective_spec(result["run_id"])["feature_delivery"]["owner"]["generation"] == 2
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget


def test_changed_service_configuration_refuses_preflight_and_execution(feature, monkeypatch):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    request = {"command_id": "continue-stale", "expected_revision": row["revision"]}
    config = json.loads(store.config.path.read_text())
    config["capacity"] = config.get("capacity", 2) + 1
    store.config.path.write_text(json.dumps(config))
    config_bytes = store.config.path.read_bytes()
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    assessment = assess_transition(store, spec["run_id"], "continue_feature", request)
    assert "feature_submission_config_unknown" in {item["code"] for item in assessment["blockers"]}
    assert store.config.path.read_bytes() == config_bytes
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    with pytest.raises(ValueError, match="reload the service"):
        continue_feature(store, spec["run_id"], request)
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget


def test_disabled_repository_blocks_observation_and_execution(feature, monkeypatch):
    from devflow_temporal.delivery_settings import save_repository_access

    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    request = {"command_id": "continue-disabled", "expected_revision": row["revision"]}
    save_repository_access(store, {"expected_revision": 0, "allowed_repositories": []})
    before = hashlib.sha256(shared.path.read_bytes()).hexdigest()
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    assessment = assess_transition(store, spec["run_id"], "continue_feature", request)
    assert "feature_repository_access_unknown" in {item["code"] for item in assessment["blockers"]}
    assert hashlib.sha256(shared.path.read_bytes()).hexdigest() == before
    with pytest.raises(ValueError, match="disabled"):
        continue_feature(store, spec["run_id"], request)
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget


def test_conflicting_work_owner_blocks_preflight_and_execution(feature, monkeypatch):
    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    request = {"command_id": "continue-claimed", "expected_revision": row["revision"]}
    with store._connect() as db:
        store.state.claim_work(db, spec["work_id"], "external:foreign")
        original = dict(store.state.claim_for(db, spec["work_id"]))
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    assessment = assess_transition(store, spec["run_id"], "continue_feature", request)
    assert "feature_work_claim_unknown" in {item["code"] for item in assessment["blockers"]}
    with pytest.raises(ValueError, match="already claimed"):
        continue_feature(store, spec["run_id"], request)
    with store._connect() as db:
        assert dict(store.state.claim_for(db, spec["work_id"])) == original
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget


def test_tracking_change_during_remote_observation_cannot_become_eligible(feature, monkeypatch):
    from devflow_temporal.delivery_feature_execution import GitHubDelivery

    store, spec, shared, _record, _diagnostic = feature
    row = _stopped_feature(store, spec, shared, monkeypatch)
    monkeypatch.setattr(
        "devflow_temporal.delivery_feature_closure.closed_coordinator",
        lambda *_args, **_kwargs: {"result": {"cleanup": "confirmed"}, "workflow_id": "closed"},
    )
    original_snapshot = GitHubDelivery.snapshot

    def changing_snapshot(*args):
        snapshot = original_snapshot(*args)
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET revision=revision+1 WHERE run_id=?",
                       (spec["run_id"],))
        return snapshot

    monkeypatch.setattr(GitHubDelivery, "snapshot", changing_snapshot)
    request = {"command_id": "continue-raced", "expected_revision": row["revision"]}
    budget = shared.budget(spec["feature_delivery"]["owner"]["issue_id"])
    with store._connect() as db:
        count = db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0]
    assessment = assess_transition(store, spec["run_id"], "continue_feature", request)
    assert not assessment["eligible"]
    assert "feature_tracking_changed" in {item["code"] for item in assessment["blockers"]}
    with pytest.raises(ValueError):
        continue_feature(store, spec["run_id"], request)
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_commands").fetchone()[0] == count
    assert shared.budget(spec["feature_delivery"]["owner"]["issue_id"]) == budget
