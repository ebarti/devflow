"""Interrupted feature workers resume without a repair cycle, whatever the budget holds."""

from __future__ import annotations

import asyncio
import json

import pytest
from test_delivery_feature_execution import feature_service
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_stopped_resume as resume_mod
from devflow_temporal.contracts import digest
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
from devflow_temporal.supervisor import DeliverySupervisor

EXHAUSTED = (r"feature product repair limit exhausted: 10 of 10 feature repair cycles used, "
             r"worker iteration \d+ of \d+; continuing this worker needs a new repair cycle")
# Fields the supervisor adds around a role result; its durable attempt omits them.
RECEIPT_ONLY = {"role", "iteration", "candidate", "attempt_id", "input_candidate_id", "provider"}
CANDIDATE = {"id": "c" * 64, "head": "a" * 40, "content_sha256": "d" * 64}


def implementation(iteration, status="pass"):
    return {"role": "implement", "iteration": iteration, "status": status,
            "session_id": "original-implementation", "cleanup": "confirmed",
            "candidate": {"content_sha256": CANDIDATE["content_sha256"]}}


def gate(role, iteration, status="pass"):
    return {"role": role, "iteration": iteration, "status": status,
            "session_id": "independent-" + role, "cleanup": "confirmed"}


def plan_seal(spec, roles=(), *, iteration=0, phase="cancelled", pr_head=None, checks=None,
              authenticated=True, execution="closed-worker"):
    # Each implementation the supervisor ran keeps its durable finished attempt.
    attempts = [{"job_key": f"implement-{role['iteration']}", "role": "implement",
                 "iteration": role["iteration"], "state": "finished", "cleanup": "confirmed",
                 "session_id": role["session_id"],
                 "result_json": json.dumps({k: v for k, v in role.items()
                                            if k not in RECEIPT_ONLY})}
                for role in roles if role["role"] == "implement" and authenticated]
    return {"predecessor_spec": spec, "candidate": CANDIDATE,
            "session_id": "original-implementation", "attempts": attempts,
            "effects": [], "original_recovery": None,
            "closed": {"workflow_id": "delivery-" + spec["run_id"], "execution_run_id": execution},
            "state": {"phase": phase, "iteration": iteration, "revision": 3, "roles": list(roles),
                      "checks": checks or {},
                      "pull_request": {"number": 7, "head": pr_head} if pr_head else None}}


def worker(kind):
    return {"run_id": "worker", "feature_worker": {"kind": kind}}


def plan(seal):
    value = resume_mod.interrupted_resume(seal)
    return value and (value["resume_iteration"], value["additional_iterations"],
                      value.get("resume_stage"))


def test_gate_attempts_of_an_interrupted_admission_take_its_own_identity():
    request = {"role": "review", "iteration": 2, "candidate": {"id": CANDIDATE["id"]}}
    spec = {"run_id": "worker", "policy_digest": "p" * 64}
    keys = {name: DeliverySupervisor._job_key({**request, "spec": {**spec, **extra}})
            for name, extra in (("original", {}), ("first", {"gate_admission": "a" * 64}),
                                ("second", {"gate_admission": "b" * 64}))}
    assert len(set(keys.values())) == 3
    # The same admission replays its own attempt; an implementer keeps its identity.
    assert keys["first"] == DeliverySupervisor._job_key(
        {**request, "spec": {**spec, "gate_admission": "a" * 64}})
    implement = {**request, "role": "implement"}
    assert DeliverySupervisor._job_key({**implement, "spec": spec}) == (
        DeliverySupervisor._job_key({**implement, "spec": {**spec, "gate_admission": "a" * 64}}))


@pytest.mark.parametrize("case", ["gate_findings", "failed_check", "implementation_findings",
                                  "repair_requested", "unauthenticated", "blocked",
                                  "single_delivery"])
def test_states_needing_a_repair_or_lacking_custody_have_no_interrupted_plan(case):
    spec, roles = worker("chunk"), [implementation(2)]
    options = {"iteration": 2, "pr_head": CANDIDATE["head"]}
    if case == "gate_findings":
        roles.append(gate("review", 2, "findings"))
    elif case == "failed_check":
        options["checks"] = {"local": {"state": "failed"}}
    elif case == "implementation_findings":
        roles = [implementation(2, "findings")]
    elif case == "repair_requested":
        # Passed gates moved the iteration on only because a later check failed.
        roles.append(gate("review", 2))
        options["iteration"] = 3
    elif case == "unauthenticated":
        options["authenticated"] = False
    elif case == "blocked":
        options["phase"] = "blocked"
    else:
        spec = {"run_id": "worker"}
    assert plan(plan_seal(spec, roles, **options)) is None


def test_pending_repair_ignores_a_predecessor_that_reran_its_own_iteration():
    spec = worker("chunk")
    seal = {"session_custody": {"missing_rollout": True}, "predecessor_spec": spec,
            "state": {"iteration": 3},
            "original_recovery": {"kind": resume_mod.KIND, "execution_spec": spec,
                                  "resume_iteration": 3,
                                  "state": {"iteration": 3, "error": None, "roles": []}}}
    assert resume_mod.pending_repair(seal) is None


@pytest.mark.parametrize(("details", "consistent"), [
    ({"execution_spec": {"gate_admission": "a" * 64}, "command_digest": "a" * 64,
      "resume_iteration": 2}, True),
    ({"execution_spec": {"gate_admission": "b" * 64}, "command_digest": "a" * 64,
      "resume_iteration": 2}, False),
    # An interrupted rerun always carries its admission; older revision gate-only
    # recoveries and resumes that never rerun gates at their iteration may lack it.
    ({"execution_spec": {}, "command_digest": "a" * 64, "resume_iteration": 2}, False),
    ({"execution_spec": {}, "command_digest": "a" * 64, "resume_iteration": 2,
      "feature_plan_revision": {"revision_id": "r"}}, True),
    ({"execution_spec": {}, "command_digest": "a" * 64, "pending_repair": {"iteration": 3}}, True),
    ({"execution_spec": {"implement_admission": "a" * 64}, "command_digest": "a" * 64,
      "pending_repair": {"iteration": 3}}, True),
    ({"execution_spec": {"implement_admission": "b" * 64}, "command_digest": "a" * 64,
      "pending_repair": {"iteration": 3}}, False),
], ids=["own", "foreign", "interrupted-missing", "legacy-revision", "legacy-pending-repair",
        "own-implement", "foreign-implement"])
def test_admission_identity_rule(details, consistent):
    assert resume_mod.admission_identity_consistent(details) is consistent


def _worker(service, monkeypatch, *, used_by_others, kind="build"):
    store, request, _ = feature_service(service, monkeypatch)
    store.submit(request)
    parent = store.effective_spec(request["run_id"])
    chunk = ordered_chunks(json.loads(request["accepted_plan"]))[0]
    child = worker_spec(parent, chunk, {"url": "https://github.com/example/fixture/issues/10"},
                        kind=kind, base_sha=parent["base_sha"], base_branch="main")
    register_worker(store, parent, child)
    shared, token = registry(parent), parent["feature_delivery"]["owner"]
    for number in range(1, used_by_others + 1):
        shared.repair(token, f"worker-other:{number}",
                      f"Product repair at worker iteration {number}")
    return store, parent, child, shared, token


def _row(store, child):
    with store._connect() as db:
        return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                               (child["run_id"],)).fetchone())


def _admission(monkeypatch, sealed):
    # Temporal closure and the final admission are modeled; the registry, feature
    # admission, worker derivation and budget accounting are real.
    monkeypatch.setattr(resume_mod, "snapshot", lambda store, run_id: sealed)
    monkeypatch.setattr(gates, "prepublication_preparation_failed", lambda closed: False)
    captured = {}

    def admitted(store, run_id, command):
        captured["command"] = command
        return {"workflow_id": "delivery-" + run_id}

    monkeypatch.setattr(resume_mod, "admit", admitted)
    return captured


@pytest.mark.parametrize("used_by_others", [0, 10])
@pytest.mark.parametrize("built", [False, True], ids=["unstarted", "built"])
def test_interrupted_worker_resumes_at_its_iteration_without_spending_a_cycle(
    service, monkeypatch, used_by_others, built,
):
    store, parent, child, shared, token = _worker(service, monkeypatch,
                                                  used_by_others=used_by_others)
    captured = _admission(monkeypatch, plan_seal(child, [implementation(0)] if built else []))
    assert activities.resume_worker(store, parent, child, _row(store, child))["resumed"] is True
    assert (captured["command"]["expected_iteration"],
            captured["command"]["additional_iterations"]) == (0, 0)
    assert shared.budget(token["issue_id"])["used"] == used_by_others


@pytest.mark.parametrize("used_by_others", [9, 10])
def test_gate_rerun_stays_at_its_iteration_without_a_cycle(service, monkeypatch, used_by_others):
    store, parent, child, shared, token = _worker(service, monkeypatch,
                                                  used_by_others=used_by_others, kind="chunk")
    captured = _admission(monkeypatch, plan_seal(
        child, [implementation(0), gate("review", 0)], pr_head=CANDIDATE["head"]))
    activities.resume_worker(store, parent, child, _row(store, child))
    assert (captured["command"]["expected_iteration"],
            captured["command"]["additional_iterations"]) == (0, 0)
    assert shared.budget(token["issue_id"])["used"] == used_by_others


def test_gate_rerun_at_the_last_worker_iteration_keeps_its_repair_ceiling(service, monkeypatch):
    store, parent, child, *_ = _worker(service, monkeypatch, used_by_others=0, kind="chunk")
    last = child["policy"]["max_repairs"]
    captured = _admission(monkeypatch, plan_seal(
        child, [implementation(last), gate("review", last)], iteration=last,
        pr_head=CANDIDATE["head"]))
    activities.resume_worker(store, parent, child, _row(store, child))
    assert (captured["command"]["expected_iteration"],
            captured["command"]["additional_iterations"]) == (last, 0)


@pytest.mark.parametrize("case", ["findings", "failed_check", "blocked", "repair_requested"])
def test_resume_that_needs_a_new_repair_cycle_is_refused_with_budget(service, monkeypatch, case):
    store, parent, child, shared, token = _worker(service, monkeypatch, used_by_others=10)
    roles, options = [implementation(0)], {}
    if case == "findings":
        roles = [implementation(0, "findings")]
    elif case == "failed_check":
        options["checks"] = {"prepublish": {"state": "failed"}}
    elif case == "blocked":
        options["phase"] = "blocked"
    else:
        options["iteration"] = 1
    captured = _admission(monkeypatch, plan_seal(child, roles, **options))
    with pytest.raises(OwnershipConflict, match=EXHAUSTED):
        activities.resume_worker(store, parent, child, _row(store, child))
    assert "command" not in captured
    assert shared.budget(token["issue_id"])["used"] == 10


def test_repair_with_budget_left_still_draws_on_it(service, monkeypatch):
    store, parent, child, *_ = _worker(service, monkeypatch, used_by_others=9)
    captured = _admission(monkeypatch, plan_seal(child, [implementation(0, "findings")]))
    activities.resume_worker(store, parent, child, _row(store, child))
    assert captured["command"]["additional_iterations"] == 1


def test_pending_repair_debit_may_be_a_bound_revision_alias(service, monkeypatch):
    store, parent, child, shared, token = _worker(service, monkeypatch, used_by_others=0)
    run = child["run_id"]
    assert not activities._paid_repair(shared, token, run, 2)
    shared.repair(token, f"{run}:3", "Product repair at worker iteration 3")
    assert activities._paid_repair(shared, token, run, 3)
    shared.repair(token, "revision-debit", "Planning defect correction")
    alias = {"revision_id": "revision-2", "repair_key": "revision-debit",
             "generation": token["generation"], "reason": "Product repair at worker iteration 2"}
    shared.checkpoint(token, f"plan-revision:repair-alias:{run}:2", alias)
    assert activities._paid_repair(shared, token, run, 2)
    shared.checkpoint(token, f"plan-revision:repair-alias:{run}:4",
                      {**alias, "generation": token["generation"] + 1,
                       "reason": "Product repair at worker iteration 4"})
    assert not activities._paid_repair(shared, token, run, 4)


def test_each_closed_execution_of_a_worker_admits_its_own_resume_command(service, monkeypatch):
    store, parent, child, *_ = _worker(service, monkeypatch, used_by_others=9)
    ids = []
    for execution in ("closed-worker", "closed-worker", "closed-resume"):
        captured = _admission(monkeypatch, plan_seal(child, execution=execution))
        activities.resume_worker(store, parent, child, _row(store, child))
        ids.append(captured["command"]["command_id"])
    # An exact replay of one closure reuses its command; a later closure gets its own.
    assert ids[0] == ids[1] != ids[2]
    assert all(command.startswith("feature-resume-") for command in ids)


def _close(store, child, state, box, *, workflow_id, execution, recovery_digest):
    store.project(child["run_id"], phase="cancelled", execution_state="terminal",
                  outcome="cancelled", cleanup="confirmed", event_type="cancelled",
                  message="Feature coordinator stopped", candidate=state["candidate"],
                  pull_request=state["pull_request"], checks=state["checks"],
                  iteration=state["iteration"], protocol_revision=state["revision"], error=None)
    with store._connect() as db:
        store.state.release_work(db, child["work_id"], "external:devflow:" + child["run_id"])
    box["closed"] = {"workflow_id": workflow_id, "execution_run_id": execution,
                     "request_digest": child["request_digest"],
                     "recovery_digest": recovery_digest, "result": state}


def _recovery(store, child):
    with store._connect() as db:
        return json.loads(db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                                     (child["run_id"],)).fetchone()[0])


def _attempt(store, child, role, candidate_id):
    # The supervisor's durable receipt for a finished role.
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) "
                   "VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                   (f"{role['role']}-{role['iteration']}-{candidate_id[:8]}", child["run_id"],
                    role["role"], role["iteration"], candidate_id, role["session_id"],
                    json.dumps({k: v for k, v in role.items() if k not in RECEIPT_ONLY})))


def _drive(monkeypatch, spec, recovery, respond):
    flow, calls = DeliveryWorkflow(), []

    async def activity(name, body, **_):
        calls.append((name, body.get("role"), body.get("iteration")))
        if name == "delivery_tracker_start":
            return {"state": "consistent"}
        return respond(flow, name, body)

    async def confirmed(*_):
        return True

    async def nothing(*_):
        pass

    monkeypatch.setattr(flow, "_activity", activity)
    monkeypatch.setattr(flow, "_confirm_repair_preflight", confirmed)
    monkeypatch.setattr(flow, "_project", nothing)
    monkeypatch.setattr("devflow_temporal.delivery_workflow.workflow.patched", lambda _id: True)
    return asyncio.run(flow._resume_stopped(spec, recovery)), calls


def test_interrupted_build_resumes_twice_without_a_cycle_or_a_second_implementation(
    service, monkeypatch,
):
    store, parent, child, shared, token = _worker(service, monkeypatch, used_by_others=10)
    broker = DeliveryBroker(store, child)
    candidate = broker.prepare()["candidate"]
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 0, "phase": "cancelled",
             "execution_state": "terminal", "outcome": "cancelled", "cleanup": "confirmed",
             "candidate": candidate, "pull_request": None, "roles": [], "checks": {},
             "error": None}
    box = {}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: box["closed"])
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(store, "_ensure_no_remote_pr", lambda *_: None)
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    _close(store, child, state, box, workflow_id=_row(store, child)["workflow_id"],
           execution="closed-worker", recovery_digest=None)

    first = activities.resume_worker(store, parent, child, _row(store, child))
    recovery = _recovery(store, child)
    assert (recovery["resume_iteration"], recovery["command"]["additional_iterations"],
            recovery.get("resume_stage")) == (0, 0, None)
    with store._connect() as db:
        assert resume_mod.custody(db, recovery) == first["spec"]
        with pytest.raises(ValueError, match="authority"):
            resume_mod.custody(db, {**recovery, "resume_iteration": 1})

    def implement_then_cancel(flow, name, body):
        assert (name, body["role"], body["iteration"]) == ("delivery_role", "implement", 0)
        flow.cancel_requested = True
        result = {"role": "implement", "iteration": 0, "status": "pass",
                  "session_id": "original-build", "candidate": candidate,
                  "cleanup": "confirmed", "input_candidate_id": candidate["id"],
                  "findings": []}
        _attempt(store, child, result, candidate["id"])
        return result

    result, calls = _drive(monkeypatch, first["spec"], recovery, implement_then_cancel)
    assert result["phase"] == "cancelled" and len(calls) == 2

    # Interrupted again after its implementation passed: it is sealed, never run again.
    again = {**result, "cleanup": "confirmed", "revision": result["revision"] + 1}
    _close(store, child, again, box, workflow_id=first["workflow_id"],
           execution="closed-resume", recovery_digest=digest(recovery))
    second = activities.resume_worker(store, parent, child, _row(store, child))
    sealed = _recovery(store, child)
    assert (sealed["resume_iteration"], sealed["command"]["additional_iterations"],
            sealed["resume_stage"]) == (0, 0, "checks")
    with store._connect() as db:
        assert resume_mod.custody(db, sealed) == second["spec"]

    def seal_only(flow, name, body):
        assert name == "delivery_feature_seal_build" and body["candidate"]["id"] == candidate["id"]
        return {"chunk_id": child["feature_worker"]["chunk_id"]}

    result, calls = _drive(monkeypatch, second["spec"], sealed, seal_only)
    assert result["outcome"] == "delivered", result.get("error")
    assert [name for name, *_ in calls] == ["delivery_tracker_start", "delivery_feature_seal_build"]
    assert shared.budget(token["issue_id"])["used"] == 10
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_commands WHERE run_id=?",
                          (child["run_id"],)).fetchone()[0] == 2


@pytest.mark.parametrize("ran", [False, True], ids=["after-publication", "after-review"])
def test_published_chunk_resume_never_publishes_or_implements_again(service, monkeypatch, ran):
    from test_delivery_feature_integration import integration, role_result

    store, parent, child, broker, candidate, member, _ = integration(service, monkeypatch)
    implemented = role_result(child, broker, candidate)
    monkeypatch.setattr(broker, "_publication_base_ref", lambda **_: "main")

    def owned(*_args, **_kwargs):
        tip = _git(broker.source, "ls-remote", "origin", "refs/heads/" + child["branch"]).split()[0]
        return {"number": 20, "url": member["url"], "state": "OPEN", "headRefOid": tip,
                "baseRefName": "main", "isDraft": False, "title": "feat: original chunk"}

    monkeypatch.setattr(DeliveryBroker, "_existing_pr", owned)
    published = broker.publish(0, implemented["candidate"])
    implementation_role = {"role": "implement", "iteration": 0, "status": "pass",
                           "session_id": "modeled-implementation", "cleanup": "confirmed",
                           "candidate": implemented["candidate"]}
    _attempt(store, child, implementation_role, implemented["candidate"]["id"])
    roles, checks = [implementation_role], {"prepublish": {
        "state": "passed", "candidate_id": implemented["candidate"]["id"]}}
    if ran:
        review = gate("review", 0)
        _attempt(store, child, review, published["candidate"]["id"])
        roles.append(review)
        checks["review"] = {"state": "passed", "candidate_id": published["candidate"]["id"]}
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 0, "phase": "cancelled",
             "execution_state": "terminal", "outcome": "cancelled", "cleanup": "confirmed",
             "candidate": published["candidate"], "pull_request": published, "roles": roles,
             "checks": checks, "error": None, "usage": {}, "findings": [],
             "candidate_revision": 2}
    box = {}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: box["closed"])
    token = parent["feature_delivery"]["owner"]
    registry(parent).finish_worker(token, worker_key(child["run_id"], token),
                                   {"cleanup": "confirmed"})
    _close(store, child, state, box, workflow_id=_row(store, child)["workflow_id"],
           execution="closed-worker", recovery_digest=None)

    resumed = activities.resume_worker(store, parent, child, _row(store, child))
    recovery = _recovery(store, child)
    assert (recovery["resume_stage"], recovery["resume_iteration"],
            recovery["command"]["additional_iterations"]) == ("published", 0, 0)
    # Gates rerun at the sealed iteration under this admission's own attempt identity.
    gate_request = {"role": "review", "iteration": 0, "candidate": published["candidate"]}
    assert resumed["spec"]["gate_admission"] == recovery["command_digest"]
    assert DeliverySupervisor._job_key({**gate_request, "spec": resumed["spec"]}) != (
        DeliverySupervisor._job_key({**gate_request, "spec": child}))
    with store._connect() as db:
        assert resume_mod.custody(db, recovery) == resumed["spec"]
        effects = db.execute("SELECT effect_key,request_json,observed_json FROM delivery_effects "
                             "WHERE kind='publish'").fetchall()

    def gates_only(flow, name, body):
        if name == "delivery_role":
            assert body["role"] in {"review", "verify"}
            return {"role": body["role"], "iteration": body["iteration"], "status": "pass",
                    "session_id": "fresh-" + body["role"], "candidate": body["candidate"],
                    "cleanup": "confirmed", "findings": []}
        if name == "delivery_ci":
            return {"state": "passed", "head": body["pull_request"]["head"]}
        assert name in {"delivery_checks", "delivery_browser_qa", "delivery_tracker"}, name
        return {"state": "passed" if name != "delivery_tracker" else "consistent",
                "candidate_id": body.get("candidate", {}).get("id"), "cleanup": "confirmed"}

    result, calls = _drive(monkeypatch, resumed["spec"], recovery, gates_only)
    assert result["outcome"] == "delivered", result.get("error")
    assert result["pull_request"] == published and result["candidate"]["id"] == (
        published["candidate"]["id"])
    assert ("delivery_role", "review", 0) in calls and ("delivery_role", "verify", 0) in calls
    assert not any(name == "delivery_publish" or role == "implement" for name, role, _ in calls)
    # The original publication stays the stack's only publication evidence.
    assert result["checks"]["prepublish"]["candidate_id"] == implemented["candidate"]["id"]
    with store._connect() as db:
        assert db.execute("SELECT effect_key,request_json,observed_json FROM delivery_effects "
                          "WHERE kind='publish'").fetchall() == effects
