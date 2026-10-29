"""Observe the feature-worker readiness handoff boundary without a provider."""

import json
from copy import deepcopy

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_activities import _check_readiness
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_resources import RunResources


def test_feature_readiness_requires_reserved_successor_and_preserves_budget(service, monkeypatch):
    store, request, _ = feature_service(service, monkeypatch)
    original_admit = DeliveryConfig.admit

    def admit(config, *args, **kwargs):
        spec = original_admit(config, *args, **kwargs)
        spec["readiness_version"] = 1
        spec["policy"]["readiness"] = {
            "version": 1,
            "generation": 0,
            "browser_engines": [],
            "temporal_checks": [],
        }
        spec["policy_digest"] = digest(spec["policy"])
        return spec

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(
        parent,
        chunk,
        {"url": "https://github.com/example/fixture/issues/10"},
        kind="chunk",
        base_sha=parent["base_sha"],
        base_branch="main",
    )
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    resources = RunResources(child)
    stage = broker.checkout.with_name("staged-feature-candidate")
    broker.checkout.rename(stage)
    resources.register(broker.checkout, "checkout")
    stage.rename(broker.checkout)
    resources.created(broker.checkout)
    (broker.checkout / "README.md").write_text("Retained feature candidate\n")
    candidate = broker.candidate()
    monkeypatch.setattr(
        "devflow_temporal.delivery_preparation.verify_prepared_spec", lambda *_: None
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_configured_resources.implementation_prerequisites", lambda *_: []
    )
    monkeypatch.setattr(
        "devflow_temporal.delivery_plan_checks.planned_checks", lambda *_a, **_k: []
    )
    monkeypatch.setattr(
        DeliveryBroker,
        "run_implementation_preparation",
        lambda *_a, **_k: {
            "state": "failed",
            "results": [{"id": "missing", "passed": False, "cleanup": "confirmed"}],
            "candidate_id": candidate["id"],
            "source_unchanged": True,
        },
    )
    result = _check_readiness(broker, {"spec": child, "candidate": candidate, "iteration": 0}, 100)
    implementation = {
        "role": "implement",
        "iteration": 0,
        "status": "pass",
        "session_id": "original-implementation",
        "cleanup": "confirmed",
        "candidate": candidate,
    }
    checks = {"prepublish": result, "readiness": result["readiness"]}
    state = {
        "run_id": child["run_id"],
        "revision": 9,
        "iteration": 0,
        "phase": "blocked",
        "execution_state": "blocked",
        "outcome": "blocked",
        "cleanup": "confirmed",
        "candidate": candidate,
        "pull_request": None,
        "roles": [implementation],
        "checks": checks,
        "error": "execution readiness prevented prepublication",
    }
    store.project(
        child["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message=state["error"],
        outcome="blocked",
        cleanup="confirmed",
        candidate=candidate,
        checks=checks,
        pull_request=None,
        error=state["error"],
        iteration=0,
        protocol_revision=9,
    )
    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    before = deepcopy(shared.budget(token["issue_id"]))
    shared.stop(token, "stopped-for-preparation", {"cleanup": "confirmed"})
    with store._connect() as db:
        store.state.release_work(db, parent["work_id"], "external:devflow:" + parent["run_id"])
    store.submit(
        {
            **request,
            "run_id": "run-successor",
            "command_id": "continue-preparation",
            "feature_predecessor": token,
        }
    )
    successor = store.effective_spec("run-successor")
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
            "state,session_id,result_json,cleanup) VALUES "
            "('implementation',?,'implement',0,?,'finished',?,?,'confirmed')",
            (
                child["run_id"],
                candidate["id"],
                implementation["session_id"],
                canonical_json(
                    {
                        k: v
                        for k, v in implementation.items()
                        if k not in {"role", "iteration", "candidate"}
                    }
                ),
            ),
        )
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        row = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (child["run_id"],)).fetchone()
        )
    closed = {
        "workflow_id": row["workflow_id"],
        "execution_run_id": "closed-worker",
        "closed_at": "2020-01-01T00:00:00+00:00",
        "request_digest": child["request_digest"],
        "recovery_digest": None,
        "result": state,
    }
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(
        "devflow_temporal.delivery_gate_retry._stopped_cleanup", lambda *_a, **_k: {}
    )
    with pytest.raises(OwnershipConflict, match="active execution assignment"):
        activities.resume_worker(store, successor, child, row)
    assert shared.budget(token["issue_id"]) == before
    with store._connect() as db:
        assert not db.execute("SELECT 1 FROM delivery_gate_admissions").fetchone()
    next_token = successor["feature_delivery"]["owner"]
    shared.reserve_worker(
        next_token,
        worker_key(child["run_id"], next_token),
        child["feature_worker"]["workstream_id"],
    )
    continued = activities.resume_worker(store, successor, child, row)
    assert continued["resumed"] and continued["spec"]["run_id"] == child["run_id"]
    assert shared.budget(token["issue_id"]) == before
    with store._connect() as db:
        recovery = json.loads(
            db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (child["run_id"],)
            ).fetchone()[0]
        )
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    assert recovery["command"]["additional_iterations"] == 0
    assert recovery["state"]["checks"]["prepublish"]["readiness"] == result["readiness"]
