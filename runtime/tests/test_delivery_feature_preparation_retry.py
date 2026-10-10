"""Feature continuation uses the existing zero-repair gate path for preparation."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_gate_retry import preparation_stop
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_activities import delivery_gates_readback
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    require_execution,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_gate_retry import PREPUBLICATION_KIND
from devflow_temporal.delivery_github_contract import ordered_chunks


@pytest.mark.parametrize("exhausted", [False, True])
def test_feature_retries_preparation_without_charging_or_extending_product_budget(
    service, monkeypatch, exhausted,
):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="chunk", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    broker = DeliveryBroker(store, child)
    broker.prepare()
    (broker.checkout / "README.md").write_text("Retained complete contribution\n")
    candidate = broker.candidate()
    implementation = {"role": "implement", "iteration": 0, "status": "pass",
                      "session_id": "original-implementation", "cleanup": "confirmed",
                      "candidate": candidate}
    checks = {"prepublish": {"state": "failed", "source_unchanged": True,
                             "candidate_id": candidate["id"], "results": [
                                 {"passed": False, "cleanup": "confirmed"}]}}
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 0, "phase": "blocked",
             "execution_state": "blocked", "outcome": "blocked", "cleanup": "confirmed",
             "candidate": candidate, "pull_request": None, "roles": [implementation],
             "checks": checks}
    preparation_stop(store, state)
    store.project(child["run_id"], phase="blocked", execution_state="blocked",
                  event_type="blocked", message=state["error"], outcome="blocked",
                  cleanup="confirmed", candidate=candidate, pull_request=None,
                  iteration=0, protocol_revision=9)
    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    if exhausted:
        for i in range(child["policy"]["max_repairs"]):
            shared.repair(token, f"historical-cycle-{i}", "retained product repair")
    before = shared.budget(token["issue_id"])
    with shared.connect() as db:
        historical = dict(db.execute("SELECT * FROM execution_workers WHERE worker_key=?",
                                      (worker_key(child["run_id"], token),)).fetchone())
    # Exercise the real successor admission and current-generation assignment.
    shared.stop(token, "stopped-for-preparation", {"cleanup": "confirmed"})
    with store._connect() as db:
        store.state.release_work(db, parent["work_id"], "external:devflow:" + parent["run_id"])
    store.submit({**request, "run_id": "run-successor", "command_id": "continue-preparation",
                  "feature_predecessor": token})
    successor = store.effective_spec("run-successor")
    next_token = successor["feature_delivery"]["owner"]
    assert next_token["generation"] == token["generation"] + 1
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) VALUES "
                   "('implementation',?,'implement',0,?,'finished',?,?,'confirmed')",
                   (child["run_id"], candidate["id"], implementation["session_id"],
                    canonical_json(implementation)))
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                              (child["run_id"],)).fetchone())
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": "closed-worker",
              "request_digest": child["request_digest"], "recovery_digest": None, "result": state}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr("devflow_temporal.delivery_gate_retry._stopped_cleanup", lambda _: {})
    result = activities.resume_worker(store, successor, child, row)
    assert result["resumed"] and result["spec"]["run_id"] == child["run_id"]
    assert result["spec"]["policy"] == child["policy"]
    assert store.submitted_spec(child["run_id"]) == child
    assert broker.candidate() == candidate
    assert shared.budget(token["issue_id"]) == before
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                         (child["run_id"],)).fetchone()[0])
        assert not db.execute("SELECT 1 FROM delivery_repair_grants").fetchone()
    assert recovery["kind"] == PREPUBLICATION_KIND
    assert recovery["command"]["additional_iterations"] == 0
    assert recovery["state"] == state and state["iteration"] == 0
    with shared.connect() as db:
        assert dict(db.execute("SELECT * FROM execution_workers WHERE worker_key=?",
                               (worker_key(child["run_id"], token),)).fetchone()) == historical
        current = dict(db.execute("SELECT * FROM execution_workers WHERE worker_key=?",
                                   (worker_key(child["run_id"], next_token),)).fetchone())
    assert current["state"] == "reserved" and current["generation"] == next_token["generation"]
    require_execution(store, result["spec"])
    assert asyncio.run(delivery_gates_readback({"spec": result["spec"],
                                               "recovery": recovery})) is None
    monkeypatch.setattr("temporalio.activity.in_activity", lambda: True)
    monkeypatch.setattr("temporalio.activity.info", lambda: SimpleNamespace(
        workflow_id=result["workflow_id"]))
    require_execution(store, result["spec"])
    monkeypatch.setattr("temporalio.activity.info", lambda: SimpleNamespace(
        workflow_id=row["workflow_id"]))
    with pytest.raises(OwnershipConflict, match="superseded workflow"):
        require_execution(store, result["spec"])
