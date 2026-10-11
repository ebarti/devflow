"""An exhausted feature budget still resumes interrupted work that needs no repair cycle."""

from __future__ import annotations

import asyncio
import json

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_stopped_resume as resume_mod
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_key,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks
from devflow_temporal.delivery_workflow import DeliveryWorkflow

EXHAUSTED = (r"feature product repair limit exhausted: 10 of 10 feature repair cycles used, "
             r"worker iteration \d+ of \d+; continuing this worker needs a new repair cycle")


def _worker(service, monkeypatch, *, own_debits=(), used_by_others):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind="build", base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    shared, token = registry(parent), parent["feature_delivery"]["owner"]
    for iteration in own_debits:
        shared.repair(token, f"{child['run_id']}:{iteration}",
                      f"Product repair at worker iteration {iteration}")
    for number in range(1, used_by_others + 1):
        shared.repair(token, f"worker-other:{number}",
                      f"Product repair at worker iteration {number}")
    return store, parent, child, shared, token


def _row(store, child):
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (child["run_id"],)).fetchone())


def _sealed(monkeypatch, child, *, iteration=0, phase="cancelled", roles=(), checks=None):
    # Only Temporal closure and the final admission are modeled here; the
    # registry, feature admission and worker derivation are real.
    sealed = {"state": {"iteration": iteration, "revision": 3, "phase": phase,
                        "roles": list(roles), "checks": checks or {}},
              "candidate": {"id": "c" * 64, "head": "h" * 40}, "predecessor_spec": child}
    monkeypatch.setattr(resume_mod, "snapshot", lambda store, run_id: sealed)
    monkeypatch.setattr(gates, "prepublication_preparation_failed", lambda closed: False)
    captured = {}

    def admitted(store, run_id, command):
        captured["command"] = command
        return {"workflow_id": "delivery-" + run_id}

    monkeypatch.setattr(resume_mod, "admit", admitted)
    return captured


def _implementation(iteration, status="pass", source="b"):
    return {"role": "implement", "iteration": iteration, "status": status,
            "input_candidate_id": source * 64}


@pytest.mark.parametrize(("used_by_others", "additional"), [(9, 1), (10, 0)])
@pytest.mark.parametrize("roles", [(), (_implementation(0),)], ids=["unstarted", "built"])
def test_interrupted_worker_resumes_whether_or_not_feature_budget_remains(
    service, monkeypatch, used_by_others, additional, roles,
):
    store, parent, child, shared, token = _worker(service, monkeypatch,
                                                  used_by_others=used_by_others)
    captured = _sealed(monkeypatch, child, roles=roles)
    assert activities.resume_worker(store, parent, child, _row(store, child))["resumed"] is True
    assert captured["command"]["additional_iterations"] == additional
    assert captured["command"]["expected_iteration"] == 0
    assert shared.budget(token["issue_id"])["used"] == used_by_others


def test_interrupted_paid_iteration_resumes_after_others_exhaust_budget(service, monkeypatch):
    store, parent, child, shared, token = _worker(service, monkeypatch, own_debits=(1, 2),
                                                  used_by_others=8)
    captured = _sealed(monkeypatch, child, iteration=2, roles=[_implementation(2)])
    assert activities.resume_worker(store, parent, child, _row(store, child))["resumed"] is True
    assert captured["command"]["additional_iterations"] == 0
    assert captured["command"]["expected_iteration"] == 2
    assert shared.budget(token["issue_id"])["used"] == 10


@pytest.mark.parametrize("case", ["findings", "completed_gate", "unchanged_input", "failed_check",
                                  "blocked", "unpaid_iteration"])
def test_resume_that_needs_a_new_repair_cycle_is_refused_with_budget(service, monkeypatch, case):
    store, parent, child, shared, token = _worker(service, monkeypatch, used_by_others=10)
    roles, checks, phase, iteration = [_implementation(0)], {}, "cancelled", 0
    if case == "findings":
        roles = [_implementation(0, status="findings")]
    elif case == "completed_gate":
        roles.append({"role": "review", "iteration": 0, "status": "pass",
                      "input_candidate_id": "c" * 64})
    elif case == "unchanged_input":
        roles = [_implementation(0, source="c")]
    elif case == "failed_check":
        checks = {"prepublish": {"state": "failed"}}
    elif case == "blocked":
        phase = "blocked"
    else:
        # The coordinator advanced the iteration but its repair was never charged.
        roles, iteration = [], 2
    captured = _sealed(monkeypatch, child, iteration=iteration, phase=phase, roles=roles,
                       checks=checks)
    with pytest.raises(OwnershipConflict, match=EXHAUSTED):
        activities.resume_worker(store, parent, child, _row(store, child))
    assert "command" not in captured
    assert shared.budget(token["issue_id"])["used"] == 10


def test_exhausted_budget_admits_interrupted_build_and_reruns_its_iteration(service, monkeypatch):
    store, parent, child, shared, token = _worker(service, monkeypatch, used_by_others=10)
    broker = DeliveryBroker(store, child)
    candidate = broker.prepare()["candidate"]
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 0, "phase": "cancelled",
             "execution_state": "terminal", "outcome": "cancelled", "cleanup": "confirmed",
             "candidate": candidate, "pull_request": None, "roles": [], "checks": {},
             "error": None}
    store.project(child["run_id"], phase="cancelled", execution_state="terminal",
                  outcome="cancelled", cleanup="confirmed", event_type="cancelled",
                  message="Feature coordinator stopped", candidate=candidate,
                  pull_request=None, checks={}, iteration=0, protocol_revision=9, error=None)
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    row = _row(store, child)
    closed = {"workflow_id": row["workflow_id"], "execution_run_id": "closed-worker",
              "request_digest": child["request_digest"], "recovery_digest": None,
              "result": state}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(store, "_ensure_no_remote_pr", lambda *_: None)

    resumed = activities.resume_worker(store, parent, child, row)
    with store._connect() as db:
        recovery = json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                         (child["run_id"],)).fetchone()[0])
        assert resume_mod.custody(db, recovery) == resumed["spec"]
        with pytest.raises(ValueError, match="authority"):
            resume_mod.custody(db, {**recovery, "resume_iteration": 1})
    assert recovery["command"]["additional_iterations"] == 0
    assert recovery["resume_iteration"] == 0 and recovery["maximum_iteration"] == 0

    flow, calls = DeliveryWorkflow(), []

    async def activity(name, body, **_):
        calls.append((name, body.get("iteration")))
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        if name == "delivery_role":
            return {"role": "implement", "iteration": body["iteration"], "status": "pass",
                    "session_id": "original-build", "candidate": candidate,
                    "cleanup": "confirmed"}
        assert name == "delivery_feature_seal_build"
        return {"chunk_id": child["feature_worker"]["chunk_id"]}

    async def confirmed(*_):
        return True

    async def nothing(*_):
        pass

    monkeypatch.setattr(flow, "_activity", activity)
    monkeypatch.setattr(flow, "_confirm_repair_preflight", confirmed)
    monkeypatch.setattr(flow, "_project", nothing)
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _id: True)
    result = asyncio.run(flow._resume_stopped(resumed["spec"], recovery))
    assert result["outcome"] == "delivered", (result.get("error"), calls)
    # Iteration zero is never charged; the feature budget stays exactly exhausted.
    assert calls == [("delivery_tracker_start", None), ("delivery_role", 0),
                     ("delivery_feature_seal_build", None)]
    assert shared.budget(token["issue_id"])["used"] == 10
