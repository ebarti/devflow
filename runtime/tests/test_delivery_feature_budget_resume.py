"""Interrupted feature workers resume without a repair cycle, whatever the budget holds."""

from __future__ import annotations

import asyncio
import copy
import json
import sys
from pathlib import Path

import pytest
from test_delivery_execution_registry import installed_runtime as installed_runtime
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
    continue_feature,
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


@pytest.mark.parametrize("case", ["gate_findings", "single_delivery"])
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


def preparation_stop(state, candidate, stage):
    prerequisite = "native-addon-build" if stage == "prepublish" else "native-execution-authority"
    reason = (f"environment preparation failed: {prerequisite}; "
              "candidate retained without requesting code repair")
    state.update(phase="blocked", outcome="blocked", execution_state="blocked",
                 error="terminal tracker synchronization conflicted")
    state["checks"].update({
        stage: {"state": "failed", "candidate_id": candidate["id"],
                "source_unchanged": True, "cleanup": "confirmed", "results": [
                    {"id": "earlier-prerequisite", "passed": True},
                    {"id": prerequisite, "passed": False, "failure_kind": "preparation",
                     "launched": False, "cleanup": "confirmed"}]},
        "failure": {"classification": "terminal", "stage": (
            "prepublish_checks" if stage == "prepublish" else "checks"), "reason": reason},
        "terminal_tracker_checkpoint": {"state": "conflicted", "event": "blocked",
            "outcome": "blocked", "closed": True, "waiting": False, "release": False,
            "error": reason, "reason": reason}})


def test_preparation_stop_requires_exact_prerequisite_and_implementation_receipts():
    sealed = plan_seal(worker("chunk"), [implementation(2)], iteration=2,
                       pr_head=CANDIDATE["head"])
    sealed["state"]["pull_request"]["candidate"] = CANDIDATE
    preparation_stop(sealed["state"], CANDIDATE, "local")
    assert plan(sealed) == (2, 0, "published")
    for mutation in ("launched", "product-failure", "earlier-failure", "other-check",
                     "reason", "tracker", "session", "attempt", "iteration", "role"):
        changed = copy.deepcopy(sealed)
        check = changed["state"]["checks"]["local"]
        if mutation == "launched":
            check["results"][-1]["launched"] = True
        elif mutation == "product-failure":
            check["results"][-1]["failure_kind"] = "assessment"
        elif mutation == "earlier-failure":
            check["results"][0]["passed"] = False
        elif mutation == "other-check":
            changed["state"]["checks"]["browser_qa"] = {"state": "failed"}
        elif mutation == "reason":
            changed["state"]["checks"]["failure"]["reason"] = "product check failed"
        elif mutation == "tracker":
            changed["state"]["checks"]["terminal_tracker_checkpoint"]["reason"] = "other stop"
        elif mutation == "session":
            changed["session_id"] = "another-session"
        elif mutation == "attempt":
            changed["attempts"][0]["result_json"] = "{}"
        elif mutation == "iteration":
            changed["state"]["iteration"] += 1
        else:
            changed["state"]["roles"].append(gate("review", 2, "findings"))
        assert plan(changed) is None, mutation


def test_pending_repair_ignores_a_predecessor_that_reran_its_own_iteration():
    spec = worker("chunk")
    seal = {"session_custody": {"missing_rollout": True}, "predecessor_spec": spec,
            "state": {"iteration": 3},
            "original_recovery": {"kind": resume_mod.KIND, "execution_spec": spec,
                                  "resume_iteration": 3,
                                  "state": {"iteration": 3, "error": None, "roles": []}}}
    assert resume_mod.pending_repair(seal) is None


@pytest.mark.parametrize(
    ("details", "consistent"),
    [
        (
            {
                "execution_spec": {"gate_admission": "a" * 64},
                "command_digest": "a" * 64,
                "resume_iteration": 2,
            },
            True,
        ),
        (
            {
                "execution_spec": {"implement_admission": "b" * 64},
                "command_digest": "a" * 64,
                "pending_repair": {"iteration": 3},
            },
            False,
        ),
    ],
    ids=["own", "foreign-implement"],
)
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


@pytest.mark.parametrize("case", ["findings", "repair_requested"])
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


def test_unstarted_paid_preparation_keeps_iteration_findings_and_original_session(
    service, monkeypatch,
):
    store, parent, child, shared, token = _worker(
        service, monkeypatch, used_by_others=0, kind='chunk')
    before = copy.deepcopy(child)
    old = {**CANDIDATE, 'base_sha': child['base_sha'], 'environment_digest': 'e' * 64,
           'policy_digest': child['policy_digest']}
    child = copy.deepcopy(child)
    child['policy']['native_identity'] = {'runtime_payload_sha256': 'a' * 64}
    child['policy_digest'] = digest(child['policy'])
    child['repair_allowance'] = {'owner': token}
    child['implementation_role_home_generation'] = ''
    candidate = {**old, 'policy_digest': child['policy_digest']}
    role = {**implementation(7, 'blocked'), 'candidate': old,
            'findings': ['Retained actual product finding']}
    previous = {'kind': resume_mod.KIND, 'predecessor_spec': before,
                'execution_spec': child, 'candidate': old, 'execution_candidate': candidate,
                'state': {'iteration': 7, 'error': 'Prior repair limit exhausted', 'roles': [role]},
                'attempts': [{'role': 'implement', 'iteration': 7, 'state': 'finished',
                              'cleanup': 'confirmed'}], 'effects': [],
                'session_id': 'original-implementation',
                'session_custody': {'generation': '', 'session_id': 'original-implementation'}}
    sealed = {**plan_seal(child, [role], iteration=8), 'candidate': candidate,
              'original_recovery': previous, 'attempts': previous['attempts'],
              'session_custody': previous['session_custody']}
    sealed['state'].update(candidate=candidate, cleanup='confirmed')
    # This fixture models immutable prior-admission readback; actual registry
    # generation/debit, source/inventory checks and public worker selection execute.
    authenticated = []
    def custody(_db, recovery):
        authenticated.append(recovery)
        return recovery['execution_spec']
    monkeypatch.setattr(resume_mod, 'custody', custody)
    shared.repair(token, f"{child['run_id']}:8", 'Product repair at worker iteration 8')
    with store._connect() as db:
        pending = resume_mod.pending_repair(sealed, db=db)
    assert pending == {'iteration': 8, 'findings': [
        'Prior repair limit exhausted', 'Retained actual product finding']}
    broker = type('Broker', (), {'spec': child, 'store': store})()
    assert resume_mod._historical_candidate(broker, sealed['state'], candidate, role,
                                             previous, pending)
    captured = _admission(monkeypatch, sealed)
    activities.resume_worker(store, parent, child, _row(store, child))
    assert captured['command']['expected_iteration'] == 8
    assert captured['command']['additional_iterations'] == 0
    assert shared.budget(token['issue_id'])['used'] == 1
    assert authenticated and all(item == previous for item in authenticated)
    # Another authenticated unstarted admission renews runtime allowance ownership,
    # while the immutable worker:8 debit still belongs to the original owner.
    renewed = copy.deepcopy(child)
    renewed['repair_allowance']['owner'] = {**token, 'generation': token['generation'] + 1}
    renewed['policy']['native_identity'] = {'runtime_payload_sha256': 'b' * 64}
    renewed['policy_digest'] = digest(renewed['policy'])
    rebound = {**candidate, 'policy_digest': renewed['policy_digest']}
    link = {**previous, 'predecessor_spec': child, 'execution_spec': renewed,
            'candidate': candidate, 'execution_candidate': rebound, 'state': sealed['state'],
            'pending_repair': pending, 'original_recovery': previous}
    repeated = {**sealed, 'predecessor_spec': renewed, 'candidate': rebound,
                'state': {**sealed['state'], 'candidate': rebound}, 'original_recovery': link}
    with store._connect() as db:
        assert resume_mod.pending_repair(repeated, db=db) == pending
    broker.spec = renewed
    assert resume_mod._historical_candidate(broker, repeated['state'], rebound, role, link, pending)
    assert shared.budget(token['issue_id'])['used'] == 1
    # Once the paid turn finishes, a publish may change its head but not content.
    # It must leave the pending classifier through its exact admitted receipt.
    finished = copy.deepcopy(sealed)
    output = {**candidate, 'id': 'd' * 64, 'content_sha256': 'f' * 64}
    completed = {**implementation(8), 'candidate': output,
                 'input_candidate_id': candidate['id']}
    finished['candidate'] = {**output, 'id': 'e' * 64, 'head': 'c' * 40}
    reviewed = {**gate('review', 8, 'findings'),
                'input_candidate_id': finished['candidate']['id']}
    publication = {'number': 7, 'head': finished['candidate']['head'],
                   'candidate': finished['candidate'], 'state': 'OPEN'}
    finished['state'] = {**finished['state'], 'candidate': finished['candidate'],
                         'roles': [role, completed, reviewed], 'pull_request': publication}
    finished['effects'] = [{'effect_key': f"publish:{child['run_id']}:8", 'kind': 'publish',
                            'state': 'complete', 'observed_json': json.dumps(publication),
                            'request_json': json.dumps({'input_candidate_id': output['id'],
                                                        'iteration': 8})}]
    finished['session_custody'] = {**finished['session_custody'],
                                  'session_state_digest': 'advanced'}
    finished['attempts'] = copy.deepcopy(finished['attempts'])
    for progressed in (completed, reviewed):
        input_id = progressed['input_candidate_id']
        job = DeliverySupervisor._job_key({'spec': child, 'role': progressed['role'],
                                           'iteration': 8, 'candidate': {'id': input_id}})
        finished['attempts'].append({
            'job_key': job, 'role': progressed['role'], 'iteration': 8, 'candidate_id': input_id,
            'state': 'finished', 'cleanup': 'confirmed', 'session_id': progressed['session_id'],
            'result_json': json.dumps({key: value for key, value in progressed.items()
                                       if key not in RECEIPT_ONLY})})
    with store._connect() as db:
        assert resume_mod.pending_repair(finished, db=db) is None
    for status in ('findings', 'blocked'):
        assessed = copy.deepcopy(finished)
        assessed['state']['roles'][1]['status'] = status
        assessed['state']['roles'][1]['attempt_id'] = assessed['attempts'][-2]['job_key']
        receipt = json.loads(assessed['attempts'][-2]['result_json'])
        assessed['attempts'][-2]['result_json'] = json.dumps({**receipt, 'status': status})
        assessed['state']['roles'] = assessed['state']['roles'][:2]
        assessed['attempts'] = assessed['attempts'][:-1]
        # Nonpass stops before _run_iterations replaces its admitted top candidate.
        assessed['candidate'] = assessed['state']['candidate'] = candidate
        assessed['state']['pull_request'] = None
        assessed['effects'] = copy.deepcopy(previous['effects'])
        with store._connect() as db:
            assert resume_mod.pending_repair(assessed, db=db) is None
    for change in ('receipt', 'review_receipt', 'prior_receipt', 'session', 'home', 'source',
                   'attempt_id', 'extra_attempt', 'extra_effect'):
        altered = copy.deepcopy(finished)
        if change == 'receipt':
            altered['attempts'][-2]['result_json'] = '{}'
        elif change == 'review_receipt':
            altered['attempts'][-1]['result_json'] = '{}'
        elif change == 'prior_receipt':
            altered['attempts'][0]['cleanup'] = 'unknown'
        elif change == 'session':
            altered['session_id'] = 'replacement-session'
        elif change == 'home':
            altered['session_custody']['generation'] = 'replacement-home'
        elif change == 'attempt_id':
            altered['state']['roles'][1]['attempt_id'] = 'unrelated-attempt'
        elif change == 'extra_attempt':
            altered['attempts'].append({**altered['attempts'][-1], 'job_key': 'unrecorded'})
        elif change == 'extra_effect':
            altered['effects'].append({**altered['effects'][-1], 'effect_key': 'unrecorded'})
        else:
            altered['candidate']['content_sha256'] = '0' * 64
        with store._connect() as db, pytest.raises(ValueError, match='unchanged source'):
            resume_mod.pending_repair(altered, db=db)
    for change in ('source', 'attempt', 'cleanup'):
        altered = copy.deepcopy(sealed)
        if change == 'source':
            altered['candidate']['head'] = 'b' * 40
        elif change == 'attempt':
            altered['attempts'].append({'iteration': 8})
        else:
            altered['state']['cleanup'] = 'unknown'
        with store._connect() as db, pytest.raises(ValueError, match='unchanged source'):
            resume_mod.pending_repair(altered, db=db)
    with shared.connect() as db:
        db.execute('UPDATE execution_repairs SET reason=? WHERE repair_key=?',
                   ('Unrelated charge', f"{child['run_id']}:8"))
    with store._connect() as db, pytest.raises(ValueError, match='exact existing product debit'):
        resume_mod.pending_repair(sealed, db=db)


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
    store.project(child["run_id"], phase=state["phase"],
                  execution_state=state["execution_state"], outcome=state["outcome"],
                  cleanup="confirmed", event_type=state["phase"],
                  message="Feature coordinator stopped", candidate=state["candidate"],
                  pull_request=state["pull_request"], checks=state["checks"],
                  iteration=state["iteration"], protocol_revision=state["revision"],
                  error=state["error"])
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


def _drive(monkeypatch, spec, recovery, respond, *, project=False):
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
    if not project:
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


@pytest.mark.parametrize("stop", ["after-publication", "after-review",
                                  "local-preparation", "prepublish-preparation"])
def test_published_chunk_resume_reuses_implementation_without_a_cycle(service, monkeypatch, stop):
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
    if stop == "prepublish-preparation":
        (broker.checkout / "README.md").write_text("Passed later implementation at the same head\n")
        implemented["candidate"] = broker.candidate()
        assert implemented["candidate"]["head"] == published["head"]
        assert implemented["candidate"]["id"] != published["candidate"]["id"]
    stopped_candidate = (implemented["candidate"] if stop == "prepublish-preparation"
                         else published["candidate"])
    implementation_role = {"role": "implement", "iteration": 0, "status": "pass",
                           "session_id": "modeled-implementation", "cleanup": "confirmed",
                           "candidate": implemented["candidate"]}
    _attempt(store, child, implementation_role, implemented["candidate"]["id"])
    roles, checks = [implementation_role], {"prepublish": {
        "state": "passed", "candidate_id": implemented["candidate"]["id"]}}
    if stop == "after-review":
        review = gate("review", 0)
        _attempt(store, child, review, published["candidate"]["id"])
        roles.append(review)
        checks["review"] = {"state": "passed", "candidate_id": published["candidate"]["id"]}
    state = {"run_id": child["run_id"], "revision": 9, "iteration": 0, "phase": "cancelled",
             "execution_state": "terminal", "outcome": "cancelled", "cleanup": "confirmed",
             "candidate": stopped_candidate, "pull_request": published, "roles": roles,
             "checks": checks, "error": None, "usage": {}, "findings": [],
             "candidate_revision": 2}
    if stop.endswith("preparation"):
        preparation_stop(state, stopped_candidate, stop.removesuffix("-preparation"))
    box = {}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: box["closed"])
    token = parent["feature_delivery"]["owner"]
    registry(parent).finish_worker(token, worker_key(child["run_id"], token),
                                   {"cleanup": "confirmed"})
    _close(store, child, state, box, workflow_id=_row(store, child)["workflow_id"],
           execution="closed-worker", recovery_digest=None)

    resumed = activities.resume_worker(store, parent, child, _row(store, child))
    recovery = _recovery(store, child)
    expected_stage = "checks" if stop == "prepublish-preparation" else "published"
    assert (recovery["resume_stage"], recovery["resume_iteration"],
            recovery["command"]["additional_iterations"]) == (expected_stage, 0, 0)
    # Gates rerun at the sealed iteration under this admission's own attempt identity.
    gate_request = {"role": "review", "iteration": 0, "candidate": published["candidate"]}
    assert resumed["spec"]["gate_admission"] == recovery["command_digest"]
    assert DeliverySupervisor._job_key({**gate_request, "spec": resumed["spec"]}) != (
        DeliverySupervisor._job_key({**gate_request, "spec": child}))
    with store._connect() as db:
        assert resume_mod.custody(db, recovery) == resumed["spec"]
        effects = db.execute("SELECT effect_key,request_json,observed_json FROM delivery_effects "
                             "WHERE kind='publish'").fetchall()
        if stop.endswith("preparation"):
            with monkeypatch.context() as changed:
                changed.setattr(resume_mod, "interrupted_resume", lambda _seal: None)
                with pytest.raises(ValueError, match="interrupted iteration authority"):
                    resume_mod.custody(db, recovery)

    def gates_only(flow, name, body):
        if name == "delivery_role":
            assert body["role"] in {"review", "verify"}
            return {"role": body["role"], "iteration": body["iteration"], "status": "pass",
                    "session_id": "fresh-" + body["role"], "candidate": body["candidate"],
                    "cleanup": "confirmed", "findings": []}
        if name == "delivery_ci":
            return {"state": "passed", "head": body["pull_request"]["head"]}
        if name == "delivery_publish":
            assert expected_stage == "checks"
            return {**published, "candidate": body["candidate"]}
        assert name in {"delivery_precheck", "delivery_checks", "delivery_browser_qa",
                        "delivery_tracker"}, name
        return {"state": "passed" if name != "delivery_tracker" else "consistent",
                "candidate_id": body.get("candidate", {}).get("id"), "cleanup": "confirmed"}

    result, calls = _drive(monkeypatch, resumed["spec"], recovery, gates_only)
    assert result["outcome"] == "delivered", result.get("error")
    assert result["candidate"]["content_sha256"] == stopped_candidate["content_sha256"]
    assert result["pull_request"]["candidate"]["id"] == stopped_candidate["id"]
    assert ("delivery_role", "review", 0) in calls and ("delivery_role", "verify", 0) in calls
    assert not any(role == "implement" for _, role, _ in calls)
    assert any(name == "delivery_publish" for name, *_ in calls) == (expected_stage == "checks")
    assert any(name == "delivery_precheck" for name, *_ in calls) == (expected_stage == "checks")
    # Workflow activities are modeled; admission cannot originate a publication effect.
    assert result["checks"]["prepublish"]["candidate_id"] == implemented["candidate"]["id"]
    assert recovery["state"] == state
    assert registry(parent).budget(token["issue_id"])["used"] == 0
    if stop.endswith("preparation"):
        assert recovery["state"]["checks"]["terminal_tracker_checkpoint"]["release"] is False
    with store._connect() as db:
        assert db.execute("SELECT effect_key,request_json,observed_json FROM delivery_effects "
                          "WHERE kind='publish'").fetchall() == effects


def test_new_install_resumes_original_worker_beyond_its_frozen_iteration_ceiling(
    service, monkeypatch, installed_runtime,
):
    from copy import deepcopy

    from devflow_temporal.delivery_native_guard import validate_native_turn

    runtime = installed_runtime
    store, parent, child, shared, token = _worker(
        service, monkeypatch, used_by_others=10, kind="chunk")
    original = deepcopy(store.submitted_spec(child["run_id"]))
    broker = DeliveryBroker(store, child)
    candidate = broker.prepare()["candidate"]
    last = child["policy"]["max_repairs"]
    implemented = {**implementation(last), "candidate": candidate, "findings": []}
    reviewed = {**gate("review", last, "findings"), "findings": ["Repair the failing contract"]}
    _attempt(store, child, implemented, candidate["id"])
    state = {"run_id": child["run_id"], "revision": 9, "iteration": last, "phase": "cancelled",
             "execution_state": "terminal", "outcome": "cancelled", "cleanup": "confirmed",
             "candidate": candidate, "pull_request": None, "roles": [implemented, reviewed],
             "checks": {}, "error": None, "usage": {}, "findings": [], "candidate_revision": 1}
    box = {}
    monkeypatch.setattr(store, "_completed_temporal_result", lambda *_a, **_k: box["closed"])
    monkeypatch.setattr(DeliveryBroker, "_existing_pr", lambda *_a, **_k: None)
    monkeypatch.setattr(store, "_ensure_no_remote_pr", lambda *_: None)
    shared.finish_worker(token, worker_key(child["run_id"], token), {"cleanup": "confirmed"})
    _close(store, child, state, box, workflow_id=_row(store, child)["workflow_id"],
           execution="closed-original-worker", recovery_digest=None)
    shared.stop(token, "original-feature-stopped", {})
    runtime.advance(2)
    shared.renew_repair_allowance(token, runtime.baseline)
    next_token = shared.claim(parent["feature_delivery"]["snapshot"], "next-coordinator",
                              token["store_path"], predecessor=token)
    parent = {**parent, "feature_delivery": {**parent["feature_delivery"], "owner": next_token}}
    admitted = activities.resume_worker(store, parent, child, _row(store, child))
    recovery = _recovery(store, child)
    effective = admitted["spec"]
    assert (recovery["state"]["iteration"], recovery["maximum_iteration"]) == (last, last + 10)
    assert recovery["session_id"] == "original-implementation"
    assert recovery["execution_candidate"]["id"] == candidate["id"]
    assert effective["policy"]["max_repairs"] == last
    assert store.submitted_spec(child["run_id"]) == original
    assert store.continue_repair(child["run_id"], recovery["command"]) == {
        "run_id": child["run_id"], "workflow_id": admitted["workflow_id"],
        "dashboard_url": store.config.dashboard_url + "/runs/" + child["run_id"],
        "phase": "repair_continuation_queued", "authorized_through_iteration": last + 10,
        "admission_sha256": digest(recovery),
    }
    validate_native_turn(effective, "implement", last + 10, store)
    with pytest.raises(ValueError, match="finite turn limit"):
        validate_native_turn(effective, "implement", last + 11, store)

    def first_repair(flow, name, body):
        assert name == "delivery_role" and body["role"] == "implement"
        assert body["iteration"] == last + 1 and body["resume_session"] == recovery["session_id"]
        shared.repair(next_token, f"{child['run_id']}:{body['iteration']}",
                      "Product repair at worker iteration " + str(body["iteration"]))
        flow.cancel_requested = True
        return {**implementation(body["iteration"], "findings"), "candidate": candidate,
                "findings": ["Retained original session"], "finish_reason": "done"}

    result, calls = _drive(monkeypatch, effective, recovery, first_repair)
    assert result["outcome"] in {"blocked", "cancelled"}, result.get("error")
    assert ("delivery_role", "implement", last + 1) in calls
    assert (shared.budget(token["issue_id"])["used"],
            shared.budget(token["issue_id"])["lifetime_used"]) == (1, 11)
    shared.finish_worker(next_token, worker_key(child["run_id"], next_token),
                         {"cleanup": "confirmed"})
    shared.stop(next_token, "renewed-feature-stopped", {})
    shared.claim(parent["feature_delivery"]["snapshot"], "another-coordinator",
                 token["store_path"], predecessor=next_token)
    # Historical custody remains readable, but it cannot launch a stale role.
    with store._connect() as db:
        assert resume_mod.custody(db, recovery) == effective
    with pytest.raises(OwnershipConflict, match="ownership changed"):
        validate_native_turn(effective, "implement", last + 1, store)


@pytest.mark.parametrize("maximum", [10, 100])
@pytest.mark.parametrize("credit_kind", ["paid_now", "paid_before_upgrade", "passed_gates"])
def test_adopted_worker_uses_current_allowance_and_only_authenticated_prepaid_credit(
    service, monkeypatch, installed_runtime, maximum, credit_kind,
):
    from copy import deepcopy

    from devflow_temporal import delivery_feature_revision_recovery as revision
    from devflow_temporal import delivery_feature_revisions as revisions
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_store import DeliveryStore

    legacy, request = service
    raw = deepcopy(legacy.config.raw)
    raw['max_repairs'] = maximum
    legacy.config.path.write_text(json.dumps(raw))
    configured = DeliveryStore(DeliveryConfig.load(legacy.config.path))
    store, parent, child, shared, first = _worker(
        (configured, request), monkeypatch, used_by_others=maximum)
    revision_id = 'correction-with-prepaid-turn'
    repair_key = ('worker-other:1' if credit_kind == 'paid_before_upgrade' else
                  'plan-revision:' + revision_id)
    iteration = maximum
    alias_key = 'plan-revision:repair-alias:' + child['run_id'] + ':' + str(iteration + 1)
    if credit_kind == 'paid_before_upgrade':
        shared.revision_checkpoint(first, alias_key, {
            'revision_id': revision_id, 'repair_key': repair_key, 'generation': first['generation'],
            'reason': 'Product repair at worker iteration ' + str(iteration + 1)})
    shared.finish_worker(first, worker_key(child['run_id'], first), {'cleanup': 'confirmed'})
    shared.stop(first, 'before-renewal', {})
    installed_runtime.advance(2)
    shared.renew_repair_allowance(first, installed_runtime.baseline)
    owner = shared.claim(parent['feature_delivery']['snapshot'], 'renewed-coordinator',
                         first['store_path'], predecessor=first)
    parent = {**parent, 'feature_delivery': {**parent['feature_delivery'], 'owner': owner}}
    if credit_kind == 'paid_now':
        shared.repair(owner, repair_key, 'Planning correction ' + revision_id)
    receipt = {'revision_id': revision_id,
               'identity': {'plan_revision': 2, 'plan_digest': 'a' * 64},
               'diagnostic_worker_run_id': (
                   None if credit_kind == 'passed_gates' else child['run_id']),
               'repair_key': repair_key}
    seal = plan_seal(child, [implementation(iteration, 'findings')], iteration=iteration)
    monkeypatch.setattr(revision, '_receipt', lambda _spec: receipt)
    monkeypatch.setattr(revisions, 'worker_revision_required', lambda *_: True)
    monkeypatch.setattr(revision, '_derive', lambda *_: deepcopy(child))
    monkeypatch.setattr(revision, 'validate_revision_recovery',
                        lambda _store, predecessor, _admission: predecessor)
    monkeypatch.setattr(revision, 'gate_only_recovery', lambda *_: (
        {'exact_passed_implementation': True} if credit_kind == 'passed_gates' else None))
    monkeypatch.setattr(resume_mod, 'snapshot', lambda *_a, **_kw: deepcopy(seal))
    # The adopted-plan reservation is an explicit seam; accounting, exact alias
    # receipts, current installed payload and finite stopped admission are real.
    monkeypatch.setattr(ExecutionRegistry, 'reserve_revision_worker', lambda *_: None)
    admit = resume_mod.admit
    observed = {}

    def preflight(store, run_id, command, **kwargs):
        observed.update(command)
        return {**admit(store, run_id, command, preflight=True, **kwargs),
                'workflow_id': 'modeled-next-workflow'}

    monkeypatch.setattr(resume_mod, 'admit', preflight)
    result = revision.resume_revision_worker(store, parent, child, _row(store, child))
    expected = maximum + int(credit_kind != 'paid_now')
    assert result['resumed'] and observed['additional_iterations'] == expected
    budget = shared.budget(owner['issue_id'])
    assert budget['used'] == int(credit_kind == 'paid_now')
    assert budget['lifetime_used'] == maximum + int(credit_kind == 'paid_now')
    with pytest.raises(ValueError, match='repair limit exhausted|finite exact-checkpoint'):
        admit(store, child['run_id'], {**observed, 'additional_iterations': expected + 1},
              preflight=True, _revision={'revision_id': revision_id})


@pytest.mark.skipif(sys.platform != 'darwin', reason='exclusive native output preservation')
@pytest.mark.parametrize('runtime_rebound', [False, True])
def test_cleanup_only_build_reuses_pass_after_bound_maintenance(
    service, monkeypatch, runtime_rebound,
):
    from devflow_temporal import delivery_orphans as orphans
    from devflow_temporal.delivery_native_process import NativeProcess
    from devflow_temporal.delivery_resources import (
        RunResources,
        observe_finalized_resources,
        read_private,
    )

    store, request, _ = feature_service(service, monkeypatch)
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    (source / '.gitignore').write_text('dist/\n')
    _git(source, 'add', '.gitignore')
    _git(source, 'commit', '-qm', 'ignore disposable output')
    store.config.raw['repositories']['fixture']['expected_base_sha'] = _git(
        source, 'rev-parse', 'HEAD')
    store.config.path.write_text(json.dumps(store.config.raw))
    store.submit(request)
    parent = store.effective_spec(request['run_id'])
    chunk = ordered_chunks(json.loads(request['accepted_plan']))[0]
    child = worker_spec(parent, chunk, {'url': 'https://github.com/example/fixture/issues/10'},
                        kind='build', base_sha=parent['base_sha'], base_branch='main')
    # The parent/registry stay disposable. No Codex provider or global registry is used.
    child['provider'] = 'codex'
    child['resource_cleanup_version'] = 1
    child['policy'].update(host_sandbox='trusted-local', execution_backend='native-macos',
                           browser_qa={'artifact_paths': ['dist/playwright-report']},
                           environment_proof_sha256='a' * 64, security_binding_sha256='b' * 64,
                           native_identity={'runtime_payload_sha256': 'c' * 64})
    child['policy_digest'] = digest(child['policy'])
    register_worker(store, parent, child)
    shared, token = registry(parent), parent['feature_delivery']['owner']
    for iteration in range(10):
        shared.repair(token, f'other:{iteration}', 'Other already charged repairs')
    broker = DeliveryBroker(store, child)
    broker.prepare()
    resources = RunResources(child)
    report = broker.checkout / 'dist/playwright-report'
    report.parent.mkdir()
    resources.register(report, 'generated')  # Historical pre-role allocated/null custody.
    result = NativeProcess(child, Path(child['state_dir']) / 'output-probe',
        argv=[sys.executable, '-c',
              "from pathlib import Path; p=Path('dist/playwright-report'); p.mkdir(); "
              "(p/'.last-run.json').write_text('native stand-in'); "
              "Path('README.md').write_text('Completed implementation\\n')"],
        cwd=broker.checkout, environment={'PATH': '/usr/bin:/bin'}, timeout=15).run()
    assert result['cleanup'] == 'observed-native-confirmed'
    candidate = broker.candidate()
    role = {**implementation(2), 'candidate': candidate, 'findings': []}
    _attempt(store, child, role, candidate['id'])
    monkeypatch.setattr(activities, '_context', lambda spec: (store, DeliveryBroker(store, spec)))
    checkpoint = activities.seal_build(child, candidate)
    failed = resources.finalize('blocked')
    assert failed['state'] == 'unknown'
    state = {'run_id': child['run_id'], 'revision': 9, 'iteration': 2, 'phase': 'blocked',
             'execution_state': 'blocked', 'outcome': 'blocked', 'cleanup': 'unknown',
             'candidate': candidate, 'pull_request': None, 'roles': [role], 'tracker': {},
             'checks': {'build_checkpoint': checkpoint, 'resource_cleanup': failed},
             'error': 'resource cleanup is unknown'}
    store.project(child['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
        cleanup='unknown', candidate=candidate, checks=state['checks'], tracker=state['tracker'],
        iteration=2,
        protocol_revision=9, error=state['error'], event_type='blocked', message=state['error'])
    closed = {'workflow_id': _row(store, child)['workflow_id'], 'execution_run_id': 'original',
              'closed_at': '2026-10-16T00:00:00+00:00', 'request_digest': child['request_digest'],
              'recovery_digest': None, 'result': copy.deepcopy(state)}
    # Server/GitHub transport, runtime preparation and session-home observation are seams.
    # The bound result/attempt/session, resources, maintenance, admission and budget are real.
    closures = {child['run_id']: closed}
    monkeypatch.setattr(store, '_completed_temporal_result',
                        lambda run_id, **_k: copy.deepcopy(closures[run_id]))

    def handoff(parent):
        owner = parent['feature_delivery']['owner']
        stopped = {'run_id': parent['run_id'], 'revision': 3, 'iteration': 0,
                   'phase': 'blocked', 'execution_state': 'blocked', 'outcome': 'blocked',
                   'cleanup': 'confirmed', 'error': 'Worker resource cleanup needs recovery'}
        store.project(parent['run_id'], **{k: v for k, v in stopped.items()
                                          if k not in {'run_id', 'revision'}},
                      protocol_revision=3, event_type='blocked', message=stopped['error'])
        shared.stop(owner, 'stopped:' + str(owner['generation']), {'state': stopped})
        with store._connect() as db:
            store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
        closures[parent['run_id']] = {
            'workflow_id': store.active_workflow_id(parent['run_id']),
            'execution_run_id': 'closed-parent-' + str(owner['generation']),
            'request_digest': parent['request_digest'], 'recovery_digest': None, 'result': stopped}
        result = continue_feature(store, parent['run_id'], {
            'command_id': 'continue-parent-' + str(owner['generation']),
            'expected_revision': _row(store, parent)['revision']})
        return store.effective_spec(result['run_id'])

    def prepare_runtime(spec, *_a):
        prepared = copy.deepcopy(spec)
        if runtime_rebound:
            prepared['policy'].update(
                environment_proof_sha256='d' * 64, security_binding_sha256='e' * 64,
                native_identity={'runtime_payload_sha256': 'f' * 64})
            prepared['policy_digest'] = digest(prepared['policy'])
        return prepared

    monkeypatch.setattr(resume_mod, 'prepare_runtime', prepare_runtime)
    monkeypatch.setattr('devflow_temporal.delivery_session_custody.implementation_custody',
                        lambda *_a: None)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_a, **_k: None)
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_a: None)
    described = {key: closed[key] for key in ('workflow_id', 'execution_run_id', 'closed_at')}
    for column in ('pr_json', 'tracker_json'):
        changed = {**_row(store, child), column: json.dumps({'changed': True})}
        with pytest.raises(ValueError, match='exact completed build result'):
            orphans._completed_build_cleanup(
                store, changed, child, {**described, 'status': 'COMPLETED'})
    project = store.project

    def lost_projection(*_a, **_k):
        raise RuntimeError('lost maintenance projection acknowledgement')

    monkeypatch.setattr(store, 'project', lost_projection)
    with pytest.raises(RuntimeError, match='lost maintenance projection'):
        orphans._reconcile(store, _row(store, child), child, {**described, 'status': 'COMPLETED'})
    assert _row(store, child)['cleanup'] == 'unknown'
    assert orphans._disposable_work(read_private(resources.manifest), _row(store, child))
    monkeypatch.setattr(store, 'project', project)
    orphans._reconcile(store, _row(store, child), child, {**described, 'status': 'COMPLETED'})
    after = _row(store, child)
    assert after['cleanup'] == 'confirmed' and after['outcome'] == 'blocked'
    assert not report.exists() and broker.candidate() == candidate
    folder = next((resources.root / 'preserved-generated-output').iterdir())
    assert (folder / 'output/.last-run.json').read_text() == 'native stand-in'
    assert read_private(resources.manifest)['roots'][str(report)]['identity'] is None
    monkeypatch.setattr(activities, 'current_record', lambda *_a: {})  # GitHub readback seam.
    activities.finish_worker(parent, child['run_id'])
    seal = resume_mod.snapshot(store, child['run_id'])
    assert seal['state'] == state and seal['cleanup_reconciliation']['closed_workflow'] == {
        key: value for key, value in closed.items() if key != 'result'}
    for change in ({'error': 'real product failure'},
                   {'checks': {**state['checks'], 'local': {'state': 'failed'}}},
                   {'roles': [{**role, 'status': 'findings', 'findings': ['repair']}]}):
        changed = {**state, **change}
        record = {**seal['cleanup_reconciliation'], 'original_result_digest': digest(changed)}
        record['key'] = digest({k: v for k, v in record.items() if k != 'key'})
        assert resume_mod.interrupted_resume({**seal, 'state': changed,
                                              'cleanup_reconciliation': record}) is None
    assert resume_mod.interrupted_resume({**seal, 'session_id': 'foreign-session'}) is None
    with store._connect() as db:
        store.state.claim_work(db, child['work_id'], 'external:devflow:foreign',
                               store.config.dashboard_url)
    with pytest.raises(ValueError, match='closed finalized delivery'):
        resume_mod.snapshot(store, child['run_id'])
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:foreign')
    original_source = (broker.checkout / 'README.md').read_bytes()
    (broker.checkout / 'README.md').write_text('Unsealed source edit')
    with pytest.raises(ValueError, match='candidate changed'):
        resume_mod.snapshot(store, child['run_id'])
    (broker.checkout / 'README.md').write_bytes(original_source)
    altered = {**seal['cleanup_reconciliation'], 'original_result_digest': 'f' * 64}
    assert resume_mod.interrupted_resume({**seal, 'cleanup_reconciliation': altered}) is None
    with store._connect() as db:
        proof = next(dict(row) for row in db.execute(
            "SELECT rowid AS proof_rowid,* FROM delivery_events WHERE run_id=? "
            "AND type='resource_cleanup_reconciled'",
            (child['run_id'],))
                     if json.loads(row['payload_json']) == seal['cleanup_reconciliation'])
        db.execute('DELETE FROM delivery_events WHERE rowid=?', (proof['proof_rowid'],))
    with pytest.raises(ValueError, match='closed finalized delivery'):
        resume_mod.snapshot(store, child['run_id'])
    with store._connect() as db:
        store._event(db, child['run_id'], proof['run_revision'], proof['type'], proof['message'],
                     seal['cleanup_reconciliation'])
    before_budget = shared.budget(token['issue_id'])
    parent = handoff(parent)
    resumed = activities.resume_worker(store, parent, child, _row(store, child))
    recovery = _recovery(store, child)
    assert (recovery['resume_iteration'], recovery['command']['additional_iterations'],
            recovery['resume_stage'], recovery['maximum_iteration']) == (2, 0, 'checks', 2)
    assert recovery['session_id'] == role['session_id']
    assert recovery['state'] == state and closed['result'] == state
    assert resume_mod.readback(store, resumed['spec'], recovery) == {'state': 'confirmed'}

    effective = resumed['spec']
    assert (effective['policy_digest'] != child['policy_digest']) == runtime_rebound
    saved = {name: (folder / name).read_bytes() for name in ('intent.json', 'receipt.json')}
    observe_finalized_resources(effective)  # Original preservation, current runtime authority.
    for authority in ('native_identity', 'browser_qa'):
        foreign = copy.deepcopy(effective)
        foreign['policy'][authority] = ({'runtime_payload_sha256': '0' * 64}
                                         if authority == 'native_identity'
                                         else {'artifact_paths': ['dist/foreign']})
        foreign['policy_digest'] = digest(foreign['policy'])
        with pytest.raises(ValueError):
            observe_finalized_resources(foreign)
    assert RunResources(effective).finalize('blocked')['state'] == 'confirmed'
    assert RunResources(effective).finalize('blocked')['state'] == 'confirmed'

    def terminal_resources(flow, name, body):
        if name == 'delivery_feature_seal_build':
            return activities.seal_build(body['spec'], body['candidate'])
        if name == 'delivery_finalize_resources':
            return RunResources(body['spec']).finalize(body['outcome'], uncertain=body['uncertain'])
        assert name == 'delivery_project'
        store.project(body['spec']['run_id'], **{k: v for k, v in body.items() if k != 'spec'})

    # Replay a later failed revalidation, when the allocated source is already
    # absent and only its preserved marker can make maintenance discover work.
    destination = folder / 'output'
    retained = folder / 'retained-original'
    destination.rename(retained)
    destination.mkdir()
    (destination / 'foreign').write_text('preserve replacement')
    try:
        second, calls = _drive(monkeypatch, effective, recovery, terminal_resources, project=True)
        assert second['outcome'] == 'blocked' and second['cleanup'] == 'unknown'
        assert second['error'] == 'resource cleanup is unknown'
        assert (destination / 'foreign').read_text() == 'preserve replacement'
        assert all((folder / name).read_bytes() == raw for name, raw in saved.items())
        store._native_cleanup_scan_after = 0
        pending = [row for row in orphans._unfinished_rows(store)
                   if row['run_id'] == child['run_id']]
        assert len(pending) == 1
    finally:
        (destination / 'foreign').unlink()
        destination.rmdir()
        retained.rename(destination)
    closed.update(workflow_id=resumed['workflow_id'], execution_run_id='second-cleanup-failure',
                  closed_at='2026-10-16T01:00:00+00:00', recovery_digest=digest(recovery),
                  result=copy.deepcopy(second))
    described = {key: closed[key] for key in ('workflow_id', 'execution_run_id', 'closed_at')}
    orphans._reconcile(store, pending[0], effective, {**described, 'status': 'COMPLETED'})
    assert _row(store, child)['cleanup'] == 'confirmed'
    assert closed['result'] == second and recovery['state'] == state
    store._native_cleanup_scan_after = 0
    assert not any(row['run_id'] == child['run_id'] for row in orphans._unfinished_rows(store))
    activities.finish_worker(parent, child['run_id'])

    parent = handoff(parent)
    again = activities.resume_worker(store, parent, effective, _row(store, child))
    latest = _recovery(store, child)
    assert latest['command']['additional_iterations'] == 0
    assert latest['session_id'] == role['session_id'] and latest['state'] == second
    assert resume_mod.readback(store, again['spec'], latest) == {'state': 'confirmed'}
    delivered, calls = _drive(monkeypatch, again['spec'], latest, terminal_resources, project=True)
    assert delivered['outcome'] == 'delivered' and delivered['iteration'] == 2
    assert delivered['candidate'] == {**candidate, 'policy_digest': effective['policy_digest']}
    assert delivered['roles'] == [role]
    assert not any(name == 'delivery_role' for name, *_ in calls)
    observe_finalized_resources(again['spec'])
    activities.finish_worker(parent, child['run_id'])
    assert all((folder / name).read_bytes() == raw for name, raw in saved.items())
    integrated = worker_spec(parent, chunk, {'url': child['issue_url']}, kind='chunk',
                             seed=checkpoint, base_sha=parent['base_sha'], base_branch='main')
    register_worker(store, parent, integrated)
    ordinary = DeliveryBroker(store, integrated)
    ordinary.prepare()
    assert (ordinary.checkout / 'README.md').read_text() == 'Completed implementation\n'
    assert shared.budget(token['issue_id']) == before_budget
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_attempts WHERE run_id=?',
                          (child['run_id'],)).fetchone()[0] == 1
