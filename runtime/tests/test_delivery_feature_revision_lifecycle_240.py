import hashlib
import json
import sys
from copy import deepcopy

import pytest
from test_delivery_feature_revisions import (
    _closed_revision_fixture,
    _stop_revision_fixture,
)
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal.delivery_feature_revisions import revise_feature_plan
from devflow_temporal.delivery_github_contract import GitHubDelivery
from devflow_temporal.delivery_lifecycle import assess_transition


def _contents(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def stopped(feature, monkeypatch):
    store, spec, shared, record, diagnostic = feature
    row = _stop_revision_fixture(store, spec, shared)
    closed = _closed_revision_fixture(store, spec["run_id"])
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    command = {"command_id": "revise-observe-1", "expected_revision": row["revision"],
               "reason": "A dependency needs to be corrected."}
    return store, spec, shared, record, command


def test_public_valid_revision_is_eligible_without_mutation(stopped):
    store, spec, shared, _, command = stopped
    before_registry = _contents(shared.path)
    with store._connect() as db:
        before_db = list(db.iterdump())
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", command)
    assert assessment["eligible"], json.dumps(assessment, indent=2)
    assert assessment["proof_digest"]
    assert _contents(shared.path) == before_registry
    with store._connect() as db:
        assert list(db.iterdump()) == before_db
    result = revise_feature_plan(store, spec["run_id"], command)
    assert result["revision_id"]
    assert revise_feature_plan(store, spec["run_id"], command) == result


def test_revision_preview_refuses_stale_and_unknown_effect_without_effects(stopped):
    store, spec, shared, _, command = stopped
    token = spec["feature_delivery"]["owner"]
    with shared.connect() as db:
        db.execute("INSERT INTO execution_effects "
                   "(issue_id,effect_key,generation,kind,request_digest,request_json,state,"
                   "result_json,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                   (token["issue_id"], "unsettled", token["generation"], "publish", "digest",
                    "{}", "unknown", None, "time"))
    before = _contents(shared.path)
    stale = {**command, "expected_revision": command["expected_revision"] + 1}
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", stale)
    codes = {item["code"] for item in assessment["blockers"]}
    assert {"revision_changed", "feature_effects_unsettled"} <= codes
    from devflow_temporal.delivery_lifecycle import RecoveryRefused

    with pytest.raises(RecoveryRefused) as refused:
        revise_feature_plan(store, spec["run_id"], command)
    assert "feature_effects_unsettled" in {b["code"] for b in refused.value.assessment["blockers"]}
    assert _contents(shared.path) == before


def test_revision_unknown_github_proof_is_not_eligible(stopped, monkeypatch):
    store, spec, shared, _, command = stopped
    def unavailable(*_args, **_kwargs):
        raise OSError("remote unavailable")
    monkeypatch.setattr(GitHubDelivery, "load_record", unavailable)
    before = _contents(shared.path)
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", command)
    assert not assessment["eligible"]
    assert "feature_revision_plan_unknown" in {b["code"] for b in assessment["blockers"]}
    assert _contents(shared.path) == before


def test_pending_exact_revision_can_be_observed_and_replayed(stopped, monkeypatch):
    store, spec, shared, _, command = stopped
    from devflow_temporal import delivery_feature_execution

    real_continue = delivery_feature_execution.continue_feature
    def interrupted(*_args, **_kwargs):
        raise RuntimeError("lost before successor admission")
    monkeypatch.setattr(delivery_feature_execution, "continue_feature", interrupted)
    with pytest.raises(RuntimeError, match="lost before successor"):
        revise_feature_plan(store, spec["run_id"], command)
    monkeypatch.setattr(delivery_feature_execution, "continue_feature", real_continue)
    token = spec["feature_delivery"]["owner"]
    budget = shared.budget(token["issue_id"])
    with shared.connect() as db:
        for index in range(budget["maximum"] - budget["used"]):
            db.execute("INSERT INTO execution_repairs VALUES (?,?,?,?,?)",
                       (token["issue_id"], f"later-{index}", token["generation"],
                        "already spent", "2026-01-01T00:00:00+00:00"))
    assert shared.budget(token["issue_id"])["used"] == budget["maximum"]
    before = _contents(shared.path)
    changed = {**command, "command_id": "different-command"}
    refused = assess_transition(store, spec["run_id"], "revise_feature_plan", changed)
    assert not refused["eligible"]
    assert "feature_product_budget_exhausted" in {b["code"] for b in refused["blockers"]}
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", command)
    assert assessment["eligible"], json.dumps(assessment, indent=2)
    assert _contents(shared.path) == before
    result = revise_feature_plan(store, spec["run_id"], command)
    assert revise_feature_plan(store, spec["run_id"], command) == result


def test_stale_plan_is_unknown_and_does_not_reconcile(stopped, monkeypatch):
    store, spec, shared, record, command = stopped
    record["manifest"]["plan"]["scope"] = "Changed by the GitHub owner"
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    monkeypatch.setattr(GitHubDelivery, "reconcile_record",
                        lambda *_a: pytest.fail("preview must not reconcile"))
    before = _contents(shared.path)
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", command)
    assert not assessment["eligible"]
    assert "feature_revision_plan_unknown" in {b["code"] for b in assessment["blockers"]}
    assert _contents(shared.path) == before


def test_revision_rechecks_plan_under_issue_lock_before_reservation(stopped, monkeypatch):
    store, spec, shared, record, command = stopped
    changed = deepcopy(record)
    changed["manifest"]["plan"]["scope"] = "changed after first observation"
    reads = []
    def current(*_args):
        reads.append(True)
        return deepcopy(record if len(reads) == 1 else changed)
    monkeypatch.setattr("devflow_temporal.delivery_feature_publication.current_record", current)
    before = _contents(shared.path)
    with pytest.raises(ValueError, match="GitHub plan changed before revision admission"):
        revise_feature_plan(store, spec["run_id"], command)
    assert len(reads) == 2
    assert _contents(shared.path) == before



def test_public_revision_observes_owned_child_evidence(feature, monkeypatch):
    from pathlib import Path

    from devflow_temporal.delivery_feature_execution import register_worker, worker_key, worker_spec
    from devflow_temporal.delivery_plan_model import ordered_chunks

    store, spec, shared, record, _ = feature
    chunk = ordered_chunks(json.loads(spec["accepted_plan"]))[0]
    child = worker_spec(spec, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=spec["base_sha"], base_branch="main")
    register_worker(store, spec, child)
    token = spec["feature_delivery"]["owner"]
    shared.checkpoint(token, "assignment:" + chunk["id"] + ":build", {
        "run_id": child["run_id"], "chunk_id": chunk["id"], "kind": "build",
        "store_path": str(store.config.tracking_db)})
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    evidence = Path(child["state_dir"]) / "evidence.json"
    evidence.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence.write_text('{"observation":"retained"}')
    row = _stop_revision_fixture(store, spec, shared)
    closed = _closed_revision_fixture(store, spec["run_id"])
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    request = {"command_id": "child-evidence", "expected_revision": row["revision"],
               "reason": "A retained child result requires revising the plan.",
               "evidence": [{"path": str(evidence), "sha256": _contents(evidence)}]}
    before = _contents(shared.path)
    result = assess_transition(store, spec["run_id"], "revise_feature_plan", request)
    assert result["eligible"], result
    assert _contents(shared.path) == before
    assert revise_feature_plan(store, spec["run_id"], request)["revision_id"]


@pytest.mark.skipif(sys.platform != "darwin", reason="native configuration preparation required")
def test_public_reconsideration_without_workers_is_read_only(native_configuration, monkeypatch,
                                                          tmp_path):
    from test_delivery_feature_revisions import _genuine_fixture

    fixture, command, *_ = _genuine_fixture(native_configuration, monkeypatch, tmp_path,
                                            retain_kind=None)
    store, spec, shared, record, *_ = fixture
    closed = _closed_revision_fixture(store, spec["run_id"])
    monkeypatch.setattr("devflow_temporal.delivery_feature_closure.closed_coordinator",
                        lambda *_a, **_kw: deepcopy(closed))
    monkeypatch.setattr(GitHubDelivery, "load_record", lambda *_a: deepcopy(record))
    from devflow_temporal.delivery_execution_config import observation_mode
    from devflow_temporal.delivery_feature_handoff import readonly_feature_spec, readonly_registry
    from devflow_temporal.delivery_feature_revisions import _reconsideration_proof

    with observation_mode():
        original = readonly_feature_spec(store, spec["run_id"])
        with readonly_registry(original) as view:
            values = view.checkpoints(original["feature_delivery"]["owner"]["issue_id"])
            _reconsideration_proof(store, original, command["reconsideration"], record,
                                   {k: v for k, v in closed.items() if k != "result"}, values,
                                   shared=view)
    before = _contents(shared.path)
    assessment = assess_transition(store, spec["run_id"], "revise_feature_plan", command)
    assert assessment["eligible"], json.dumps(assessment, indent=2)
    assert _contents(shared.path) == before
    assert revise_feature_plan(store, spec["run_id"], command)["revision_id"]


def test_worker_review_snapshot_uses_read_only_closure(monkeypatch):
    from devflow_temporal import delivery_stopped_resume
    from devflow_temporal.delivery_execution_config import observation_mode

    calls = []
    def snapshot(_store, run_id, **kwargs):
        calls.append((run_id, kwargs))
        return {}
    monkeypatch.setattr(delivery_stopped_resume, "_snapshot", snapshot)
    with observation_mode():
        delivery_stopped_resume.review_snapshot(object(), "worker")
    assert calls == [("worker", {"review_subject": True, "_observation": True})]


@pytest.mark.asyncio
async def test_worker_history_observation_never_preserves_closure(monkeypatch):
    from devflow_temporal import delivery_feature_closure
    from devflow_temporal.delivery_execution_config import observation_mode

    saved = {"workflow_id": "worker", "execution_run_id": "execution", "closed_at": "time",
             "request_digest": "request", "recovery_digest": None, "result": {}}
    calls = []
    async def sealed(_store, workflow, **kwargs):
        calls.append((workflow, kwargs))
        return {**saved, "history": "bound history"}
    monkeypatch.setattr(delivery_feature_closure, "sealed_closure", sealed)
    with observation_mode():
        history, bound = await delivery_feature_closure._closed_history(object(), "worker", saved)
    assert history == "bound history" and bound == saved
    assert calls == [("worker", {"read_only": True})]
