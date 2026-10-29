"""Interrupted feature workers resume without a repair cycle, whatever the budget holds."""

from __future__ import annotations

import copy
import json

import pytest
from test_delivery_execution_registry import installed_runtime as installed_runtime
from test_delivery_feature_execution import feature_service
from test_delivery_store import service as service

from devflow_temporal import delivery_feature_activities as activities
from devflow_temporal import delivery_gate_retry as gates
from devflow_temporal import delivery_stopped_resume as resume_mod
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_execution_registry import OwnershipConflict
from devflow_temporal.delivery_feature_execution import (
    register_worker,
    registry,
    worker_spec,
)
from devflow_temporal.delivery_github_contract import ordered_chunks
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


def _attempt(store, child, role, candidate_id):
    # The supervisor's durable receipt for a finished role.
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,session_id,result_json,cleanup) "
                   "VALUES (?,?,?,?,?,'finished',?,?,'confirmed')",
                   (f"{role['role']}-{role['iteration']}-{candidate_id[:8]}", child["run_id"],
                    role["role"], role["iteration"], candidate_id, role["session_id"],
                    json.dumps({k: v for k, v in role.items() if k not in RECEIPT_ONLY})))
