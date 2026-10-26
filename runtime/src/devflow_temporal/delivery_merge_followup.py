"""Explicit merge-only handoff for an ordinary, already delivered PR.

The original delivery and endpoint remain immutable. A separate durable command
owns the later merge, its Temporal dispatch, current-target checks and readback.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import ExitStack

from temporalio.service import RPCError

from .contracts import canonical_json, digest
from .delivery_lifecycle import RecoveryRefused, evaluate_transition, require_transition
from .delivery_merge import (
    MergeBroker,
    native_implementation_output,
    require_merge_gates,
)
from .delivery_policy_recovery import work_binding
from .delivery_preparation import execution_retired
from .delivery_resources import observe_finalized_resources
from .delivery_store import _now

ACTIVE = {"queued", "merging", "resolving_conflicts", "waiting_ci", "waiting_tracker",
          "waiting_cleanup", "unknown"}


class MergeCleanupPending(ValueError):
    """Fresh custody is required, independently of the immutable source grant."""


_FIELDS = (
    "request_digest",
    "request_json",
    "recovery_json",
    "candidate_json",
    "pr_json",
    "checks_json",
    "iteration",
    "protocol_revision",
    "phase",
    "execution_state",
    "outcome",
    "cleanup",
)


def initialize(db):
    from .delivery_merge_execution import initialize as initialize_executions

    initialize_executions(db)
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_merge_requests (
        run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
        command_id TEXT UNIQUE NOT NULL, grant_json TEXT NOT NULL,
        workflow_id TEXT UNIQUE NOT NULL, dispatched INTEGER NOT NULL DEFAULT 0,
        phase TEXT NOT NULL, result_json TEXT, error TEXT,
        created_at TEXT NOT NULL, updated_at TEXT NOT NULL)""")


def current(db, run_id):
    row = db.execute("SELECT * FROM delivery_merge_requests WHERE run_id=?", (run_id,)).fetchone()
    return dict(row) if row else None


def _continuable(saved):
    if saved is None:
        return False
    result = json.loads(saved["result_json"] or "null")
    if not isinstance(result, dict):
        return False
    reason = result.get("reason")
    return ((saved["phase"] == "decision_required" and reason == "conflicting_upstream_changes")
            or (saved["phase"] == "unknown" and reason in {
                "RuntimeError: merge resolution native cleanup remains unconfirmed",
                "RuntimeError: merge native role was blocked before provider launch"})
            or (saved["phase"] == "blocked" and reason
                == "ValueError: predecessor process/root custody is unconfirmed"))


def _snapshot(row):
    return {key: row[key] for key in _FIELDS}


def _immutable_row(row):
    result = _snapshot(row)
    result.pop("cleanup")
    checks = json.loads(result["checks_json"] or "{}")
    checks.pop("resource_cleanup", None)
    result["checks_json"] = canonical_json(checks)
    return result


def _native_proof(proof):
    return {**{k: v for k, v in proof.items() if k not in {"row", "checks", "resources"}},
            "row": _immutable_row(proof["row"]),
            "checks": {k: v for k, v in proof["checks"].items() if k != "resource_cleanup"}}


def _current_publication_ready(db, row, pr):
    if not pr:
        return False
    observed = db.execute("SELECT observation_json,error FROM delivery_pr_observations "
                          "WHERE lower(url)=lower(?)", (pr["url"],)).fetchone()
    if observed:
        value = json.loads(observed[0] or "null")
        if observed[1] or not value or value.get("state") != "OPEN" or (
            value.get("head") != pr.get("head")
        ):
            return False
    issue = db.execute("SELECT observation_json,error FROM delivery_issue_observations "
                       "WHERE lower(url)=lower(?)", (row["issue_url"],)).fetchone()
    if issue:
        value = json.loads(issue[0] or "{}")
        labels = {item.casefold() for item in value.get("labels", [])}
        if issue[1] or value.get("state") == "CLOSED" or labels & {
            "status: deferred", "do not merge yet", "status: cancelled", "wontfix", "won't fix",
        }:
            return False
    return True


def action(db, row, spec):
    saved = current(db, row["run_id"])
    if saved:
        request = json.loads(saved["grant_json"])["request"]
        return {
            "available": _continuable(saved),
            "resume": _continuable(saved),
            **{key: request[key] for key in ("candidate_id", "head", "pr_number")},
            "phase": saved["phase"],
            "error": saved["error"],
            "command_id": saved["command_id"],
            "updated_at": saved["updated_at"],
            "result": json.loads(saved["result_json"] or "null"),
        }
    pr = json.loads(row["pr_json"] or "null")
    candidate = json.loads(row["candidate_json"] or "null")
    checks = json.loads(row["checks_json"] or "{}")
    eligible = (
        not spec.get("feature_delivery")
        and spec.get("provider") == "codex"
        and spec.get("authorized_endpoint") == "published_unmerged"
        and row["outcome"] == "delivered"
        and row["execution_state"] == "terminal"
        and row["cleanup"] == "confirmed"
        and pr
        and candidate
        and _current_publication_ready(db, row, pr)
        and all(
            checks.get(stage, {}).get("state") == "passed"
            for stage in ("prepublish", "local", "review", "qa", "ci")
        )
    )
    return {
        "available": bool(eligible),
        "phase": "available" if eligible else "unavailable",
        "candidate_id": candidate.get("id") if candidate else None,
        "head": pr.get("head") if pr else None,
        "pr_number": pr.get("number") if pr else None,
    }


def _executed_specs(store, spec):
    """Follow authenticated gate and stopped-resume admissions, never disk discovery."""
    from .delivery_gate_retry import effective_spec as gate_spec
    from .delivery_stopped_resume import custody

    yield spec
    with store._connect() as db:
        saved = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
    recovery = json.loads(saved[0]) if saved and saved[0] else None
    previous = spec
    while recovery:
        if recovery.get("kind") in {
            "published_gate_retry",
            "prepublication_gate_retry",
            "published_check_prelaunch_retry",
            "published_ci_retry",
            "published_controller_retry",
        }:
            if gate_spec(store, recovery["original_spec"], recovery) != previous:
                raise ValueError("merge lost its authenticated gate execution")
            previous = recovery["seal"]["original_spec"]
        elif recovery.get("kind") == "stopped_delivery_resume":
            with store._connect() as db:
                if custody(db, recovery) != previous:
                    raise ValueError("merge lost its authenticated stopped execution")
            from .delivery_feature_revision_recovery import retained_recovery

            previous = retained_recovery(recovery)["predecessor_spec"]
        else:
            break
        yield previous
        recovery = recovery.get("original_recovery")


def _publication_matches(store, spec, current, observed):
    """A sealed gate renewal may rebind runtime policy, never published source."""
    if current == observed:
        return True
    with store._connect() as db:
        row = db.execute("SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                         (spec["run_id"],)).fetchone()
    recovery = json.loads(row[0]) if row and row[0] else None
    if recovery and recovery.get("kind") in {
        "published_gate_retry", "published_check_prelaunch_retry", "published_ci_retry",
        "published_controller_retry",
    }:
        from .delivery_gate_retry import effective_spec

        if (effective_spec(store, recovery["original_spec"], recovery) != spec
                or recovery["publication"] != current
                or recovery["candidate"] != current.get("candidate")):
            return False
        previous = recovery["seal"]["publication"]
        old_candidate = previous.get("candidate", {})
        if (current["candidate"].get("policy_digest") != spec["policy_digest"]
                or old_candidate.get("policy_digest")
                != recovery["seal"]["original_spec"]["policy_digest"]
                or {**current, "candidate": {
                    **current["candidate"], "policy_digest": old_candidate.get("policy_digest")
                }} != previous):
            return False
        return previous == observed
    return False


def evidence(store, spec):
    """Qualify actual native source/gates without granting permission to merge."""
    if (
        execution_retired(spec)
        or spec.get("feature_delivery")
        or spec.get("provider") != "codex"
        or spec.get("authorized_endpoint") != "published_unmerged"
    ):
        raise ValueError("merge follow-up requires an ordinary publication-only delivery")
    if not store.owns_execution(spec):
        raise ValueError("merge belongs to another service transport")
    with store._connect() as db:
        from .delivery_settings import require_repository_access

        require_repository_access(store, db, spec["github_repo"])
        work_binding(store, spec, db)
        row = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        )
        if not current(db, spec["run_id"]) and not _current_publication_ready(
            db, row, json.loads(row["pr_json"] or "null")
        ):
            raise ValueError("merge requires the current validated PR commit without an owner hold")
        attempts = [
            dict(item)
            for item in db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY rowid", (spec["run_id"],)
            )
        ]
        from .delivery_merge_resolution import role_keys

        merge_roles = role_keys(db, spec["run_id"])
        attempts = [a for a in attempts if a["job_key"] not in merge_roles]
        publications = [
            dict(item)
            for item in db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=? AND kind='publish' "
                "AND state='complete' ORDER BY rowid",
                (spec["run_id"],),
            )
        ]
    if row["outcome"] != "delivered" or row["execution_state"] != "terminal":
        raise ValueError("merge requires a delivered run with confirmed terminal cleanup")
    if row["cleanup"] != "confirmed":
        raise MergeCleanupPending("predecessor process/root custody is unconfirmed")
    candidate, pr, checks = [
        json.loads(row[key] or "null") for key in ("candidate_json", "pr_json", "checks_json")
    ]
    cleanup = checks.get("resource_cleanup", {})
    if (cleanup.get("state") != "confirmed"
            or cleanup.get("process_cleanup") != "observed-native-confirmed"
            or cleanup.get("resource_cleanup") != "confirmed"):
        raise MergeCleanupPending("predecessor process/root custody is unconfirmed")
    matching = [item for item in publications if _publication_matches(
        store, spec, pr, json.loads(item["observed_json"] or "null"))]
    if not matching:
        raise ValueError("merge has no authenticated completed publication")
    publication = json.loads(matching[-1]["request_json"])
    implementations = [
        a
        for a in attempts
        if a["role"] == "implement" and json.loads(a["result_json"] or "{}").get("status") == "pass"
    ]
    if not implementations:
        raise ValueError("merge has no accepted native implementation")
    accepted = implementations[-1]
    accepted["controller_output_candidate"] = native_implementation_output(
        spec, accepted, executed=_executed_specs(store, spec)
    )
    # Failed/prelaunch attempts remain historical and must be settled. The
    # accepted producer, rather than a later rejected turn, owns source evidence.
    ordered = [a for a in attempts if a["role"] != "implement"] + [accepted]
    if any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts):
        raise ValueError("merge cannot overlap an unfinished or unconfirmed native role")
    require_merge_gates(
        {**spec, "authorized_endpoint": "merged", "merge_version": 1},
        candidate,
        pr,
        checks,
        ordered,
        publication,
    )
    try:
        resources = observe_finalized_resources(spec)
    except ValueError as exc:
        raise MergeCleanupPending(str(exc)) from exc
    return {
        "row": _snapshot(row),
        "candidate": candidate,
        "pull_request": pr,
        "checks": checks,
        "publication": publication,
        "resources": resources,
        "attempts_sha256": digest(attempts),
    }


def _exclusive(store, db, row, *, retained_owner=None, prelaunch=None):
    spec = json.loads(row["request_json"])
    work_binding(store, spec, db)
    issue = store.state.issue_resource(row["issue_url"])
    latest = next(
        (
            r["run_id"]
            for r in db.execute("SELECT run_id,issue_url FROM delivery_runs ORDER BY rowid DESC")
            if store.state.issue_resource(r["issue_url"]) == issue
        ),
        None,
    )
    unsettled = db.execute(
        "SELECT 1 FROM delivery_effects WHERE run_id=? AND state!='complete'", (row['run_id'],)
    ).fetchone()
    if prelaunch is not None:
        from .delivery_merge_native_preparation import _PrelaunchProofs

        if not isinstance(prelaunch, _PrelaunchProofs):
            raise ValueError('merge exclusivity requires its leased prelaunch observation')
        unsettled = prelaunch.unsettled(db, row['run_id'])
    if (
        latest != row["run_id"]
        or (store.state.claim_for(db, row["work_id"]) is not None
            and store.state.claim_for(db, row["work_id"])["owner"] != retained_owner)
        or unsettled
        or db.execute(
            "SELECT 1 FROM delivery_mutations WHERE run_id=? AND state IN ('pending','unknown')",
            (row["run_id"],),
        ).fetchone()
        or db.execute(
            "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state!='sent'", (row["run_id"],)
        ).fetchone()
    ):
        raise ValueError("merge requires the current issue owner with settled effects")


def _lifecycle_facts(proof):
    return {
        'operation': 'merge_run', 'owner': 'merge_followup', 'owner_proof': True,
        'normalized_lifecycle': 'terminal', 'revision': proof['row']['protocol_revision'],
        'iteration': proof['row']['iteration'], 'candidate_id': proof['candidate']['id'],
        'candidate_head': proof['pull_request']['head'], 'publication_required': True,
        'publication_number': proof['pull_request']['number'], 'proof_digest': digest(proof),
        'comparisons': [('revision', 'expected_revision', 'revision_changed'),
                        ('candidate_id', 'candidate_id', 'candidate_changed'),
                        ('candidate_head', 'head', 'candidate_head_changed'),
                        ('publication_number', 'pr_number', 'publication_changed')],
    }


def _valid_request(request):
    required = {"command_id", "expected_revision", "candidate_id", "head", "pr_number", "answer"}
    return not (
        not isinstance(request, dict)
        or set(request) != required
        or not isinstance(request["command_id"], str)
        or not request["command_id"].strip()
        or len(request["command_id"]) > 200
        or request["answer"] != "merge"
        or type(request["expected_revision"]) is not int
        or type(request["pr_number"]) is not int
        or request["pr_number"] < 1
        or not all(
            isinstance(request[key], str) and 0 < len(request[key]) <= 200
            for key in ("candidate_id", "head")
        )
    )


def _predecessor_closure(store, spec, saved):
    """Treat unavailable SDK observation as an unknown predecessor, never as proof."""
    from .delivery_merge_execution import predecessor_closure

    try:
        return predecessor_closure(store, spec, saved)
    except RPCError as exc:
        raise ValueError('predecessor merge closure is unavailable') from exc


def assess(store, run_id, request):
    """Inspect current merge authority; assessment does not acquire a grant."""
    try:
        if not _valid_request(request):
            return evaluate_transition({
                'operation': 'merge_run', 'owner': 'merge_followup', 'comparisons': [],
                'predicates': [{'code': 'merge_authorization_missing', 'satisfied': False,
                                'missing_or_unknown_fact': 'explicit merge authorization',
                                'next_action': 'provide the explicit merge command',
                                'new_authorization_required': True}],
            }, {})
        with ExitStack() as leases:
            with store._connect() as db:
                previous = current(db, run_id)
                prior = db.execute(
                    'SELECT run_id,request_digest FROM delivery_commands WHERE command_id=?',
                    (request['command_id'],),
                ).fetchone()
            if prior and (prior['run_id'] != run_id or prior['request_digest'] != digest({
                    'kind': 'merge_run', 'run_id': run_id, 'request': request})):
                raise ValueError('merge command ID belongs to different inputs')
            if previous:
                if not _continuable(previous):
                    raise ValueError('merge already owns a request')
                from .delivery_merge_execution import existing_lease

                leases.enter_context(existing_lease(store.config, exclusive=True))
            spec, proof, _ = _observe_merge(store, run_id, request, previous)
            if previous:
                original = json.loads(previous['grant_json'])
                if (original['spec_sha256'] != digest(spec)
                        or _immutable_row(original['row']) != _immutable_row(proof['row'])
                        or (original.get('native_proof_sha256') is not None
                            and original['native_proof_sha256'] != digest(_native_proof(proof)))):
                    raise ValueError('merge continuation changed delivered evidence')
                _predecessor_closure(store, spec, previous)
            prelaunch = None
            if previous and previous['phase'] == 'unknown':
                from .delivery_merge_resolution import prelaunch_receipts

                prelaunch = leases.enter_context(prelaunch_receipts(store, spec, previous))
            with store._connect() as db:
                row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
                if _snapshot(row) != proof['row'] or current(db, run_id) != previous:
                    raise ValueError('merge authority changed during assessment')
                _exclusive(store, db, row, prelaunch=prelaunch,
                           retained_owner='external:devflow:' + run_id if prelaunch else None)
            if prelaunch:
                prelaunch.recheck()
            return evaluate_transition(_lifecycle_facts(proof), request)
    except RecoveryRefused as exc:
        return exc.assessment
    except (ValueError, RuntimeError, OSError, KeyError, TypeError):
        return evaluate_transition({'operation': 'merge_run', 'owner': 'merge_followup',
                                    'comparisons': []}, {})


def preflight(store, run_id):
    spec = store.effective_spec(run_id)
    proof = evidence(store, spec)
    with ExitStack() as leases:
        with store._connect() as db:
            saved = current(db, run_id)
        if saved:
            if not _continuable(saved):
                raise ValueError("merge already owns a request; read its current result")
            from .delivery_merge_execution import existing_lease

            leases.enter_context(existing_lease(store.config, exclusive=True))
            _predecessor_closure(store, spec, saved)
        prelaunch = None
        if saved and _continuable(saved) and saved['phase'] == 'unknown':
            from .delivery_merge_resolution import prelaunch_receipts

            prelaunch = leases.enter_context(prelaunch_receipts(store, spec, saved))
        with store._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            _exclusive(store, db, row, prelaunch=prelaunch,
                       retained_owner='external:devflow:' + run_id if prelaunch else None)
        if prelaunch:
            prelaunch.recheck()
    return {
        "candidate_id": proof["candidate"]["id"],
        "head": proof["pull_request"]["head"],
        "pr_number": proof["pull_request"]["number"],
        "phase": "available",
    }


def _observe_merge(store, run_id, request, previous_merge, *, preserve=False):
    spec = store.effective_spec(run_id)
    proof = evidence(store, spec)
    require_transition(_lifecycle_facts(proof), request)
    closed = store._completed_temporal_result(
        run_id, workflow_id=store.active_workflow_id(run_id), preserve=preserve)
    state = closed.get("result") or {}
    if (
        closed["request_digest"] != proof["row"]["request_digest"]
        or closed["recovery_digest"]
        != (
            digest(json.loads(proof["row"]["recovery_json"]))
            if proof["row"]["recovery_json"]
            else None
        )
        or state.get("outcome") != "delivered"
        or state.get("cleanup") != "confirmed"
        or state.get("run_id") != run_id
        or state.get("revision") != proof["row"]["protocol_revision"]
        or state.get("candidate") != proof["candidate"]
        or state.get("pull_request") != proof["pull_request"]
    ):
        raise ValueError("merge predecessor is not the completed delivered candidate")
    # Read only: a changed remote PR never acquires a merge grant.
    broker = MergeBroker(store, spec)
    if previous_merge:
        from .delivery_merge_reconciliation import reconcile

        remote = broker.api(f"repos/{spec['github_repo']}/pulls/{proof['pull_request']['number']}")
        if reconcile(broker, proof["pull_request"], remote, lineage_only=True)["state"] != "ready":
            raise ValueError("original target reconciliation still needs its readback")
    else:
        remote = broker.publication(proof["pull_request"])
    if remote["merged"] or remote["state"] != "open":
        raise ValueError("merge requires the existing open unmerged PR")
    return spec, proof, closed


def admit(store, run_id, request):
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_merge_execution import existing_lease

    # Closed-run maintenance holds the shared installation fence throughout its
    # finalization. Admission must not overlap its last historical projection.
    with store._connect() as db:
        previous = current(db, run_id)
    lease = existing_lease if previous else installation_recovery_lease
    with lease(store.config, exclusive=True):
        return _admit(store, run_id, request)


def _admit(store, run_id, request):
    if not _valid_request(request):
        raise ValueError("merge requires an explicit exact-candidate merge command")
    bound = digest({"kind": "merge_run", "run_id": run_id, "request": request})
    with store._connect() as db:
        prior = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (request["command_id"],)
        ).fetchone()
        if prior:
            if prior["run_id"] != run_id or prior["request_digest"] != bound:
                raise ValueError("merge command ID belongs to different inputs")
            return json.loads(prior["response_json"])
        previous_merge = current(db, run_id)
    spec, proof, closed = _observe_merge(store, run_id, request, previous_merge, preserve=True)
    workflow_id = "delivery-" + run_id + "-merge-" + bound[:20]
    grant = {
        "request": request,
        "spec_sha256": digest(spec),
        "proof_sha256": digest(proof),
        "native_proof_sha256": digest(_native_proof(proof)),
        "row": proof["row"],
        "closed": {k: v for k, v in closed.items() if k != "result"},
    }
    response = {
        "run_id": run_id,
        "phase": "queued",
        "workflow_id": workflow_id,
        "command_id": request["command_id"],
        "grant_sha256": digest(grant),
        "native_proof_sha256": digest(_native_proof(proof)),
    }
    if previous_merge is not None:
        if not _continuable(previous_merge):
            raise ValueError("merge already owns a request; read its current result")
        original = json.loads(previous_merge["grant_json"])
        if (_immutable_row(original["row"]) != _immutable_row(proof["row"])
                or (original.get("native_proof_sha256") is not None
                    and original["native_proof_sha256"] != digest(_native_proof(proof)))):
            raise ValueError("merge continuation changed its original delivered evidence")
        grant = original
        response["grant_sha256"] = digest(original)
        response["predecessor_merge"] = _predecessor_closure(store, spec, previous_merge)
    with ExitStack() as leases:
        prelaunch = []
        if previous_merge and previous_merge["phase"] == "unknown":
            from .delivery_merge_resolution import prelaunch_receipts

            prelaunch = leases.enter_context(prelaunch_receipts(store, spec, previous_merge))
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT * FROM delivery_commands WHERE command_id=?", (request["command_id"],)
            ).fetchone()
            if prior:
                if prior["run_id"] != run_id or prior["request_digest"] != bound:
                    raise ValueError("merge command ID belongs to different inputs")
                return json.loads(prior["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if (
                _snapshot(row) != proof["row"]
                or current(db, run_id) != previous_merge
                or digest(store.effective_spec(run_id)) != grant["spec_sha256"]
            ):
                raise ValueError("merge authority changed during admission")
            if prelaunch:
                _exclusive(store, db, row, prelaunch=prelaunch,
                           retained_owner="external:devflow:" + run_id)
                prelaunch.recheck()
            for receipt in prelaunch:
                attempt = db.execute('SELECT * FROM delivery_attempts WHERE job_key=? AND run_id=?',
                                     (receipt['job_key'], run_id)).fetchone()
                if not attempt or digest(dict(attempt)) != receipt['attempt_sha256']:
                    raise ValueError('merge prelaunch attempt changed before settlement')
                found = db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                   (receipt["effect_key"],)).fetchone()
                if found["request_json"] != receipt["request_json"] or found["state"] == "complete":
                    raise ValueError("merge prelaunch effect changed during continuation")
                db.execute("UPDATE delivery_effects SET state='complete',"
                           "observed_json=?,updated_at=? "
                           "WHERE effect_key=?",
                           (canonical_json(receipt), _now(), receipt["effect_key"]))
            _exclusive(store, db, row,
                       retained_owner="external:devflow:" + run_id if prelaunch else None)
            if store.state.claim_for(db, spec["work_id"]) is None:
                store.state.claim_work(
                    db, spec["work_id"], "external:devflow:" + run_id,
                    store.config.dashboard_url + "/runs/" + run_id,
                )
            if previous_merge:
                db.execute(
                    "UPDATE delivery_merge_requests SET workflow_id=?,phase='queued',dispatched=0,"
                    "result_json=NULL,error=NULL,updated_at=? WHERE run_id=?",
                    (workflow_id, _now(), run_id),
                )
            else:
                db.execute(
                    "INSERT INTO delivery_merge_requests "
                    "(run_id,command_id,grant_json,workflow_id,phase,created_at,updated_at) "
                    "VALUES (?,?,?,?,'queued',?,?)",
                    (run_id, request["command_id"], canonical_json(grant), workflow_id,
                     _now(), _now()),
                )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (request["command_id"], run_id, bound, canonical_json(response)),
            )
            store._event(
                db,
                run_id,
                row["revision"],
                "merge_continued" if previous_merge else "merge_requested",
                ("Continuing conflict resolution under the existing merge authorization"
                 if previous_merge else "Merge authorized for the existing delivered PR"),
                response,
            )
            if prelaunch:
                prelaunch.recheck()
        return response


def authorize(store, spec, grant_sha256):
    from .delivery_merge_execution import require_current

    with store._connect() as db:
        require_current(db, spec)
        saved = current(db, spec["run_id"])
        if not saved:
            raise ValueError("merge follow-up has no durable authorization")
        grant = json.loads(saved["grant_json"])
        command = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (saved["command_id"],)
        ).fetchone()
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (spec["run_id"],)).fetchone()
        work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec["work_id"])
        witness = grant.get("native_proof_sha256")
        for item in db.execute("SELECT * FROM delivery_commands WHERE run_id=?", (spec["run_id"],)):
            response = json.loads(item["response_json"])
            resumed_request = {**grant["request"], "command_id": item["command_id"]}
            if (response.get("workflow_id") == saved["workflow_id"]
                    and response.get("grant_sha256") == grant_sha256
                    and item["request_digest"] == digest({"kind": "merge_run",
                        "run_id": spec["run_id"], "request": resumed_request})):
                witness = response.get("native_proof_sha256", witness)
        if (
            digest(grant) != grant_sha256
            or grant["spec_sha256"] != digest(spec)
            or _immutable_row(row) != _immutable_row(grant["row"])
            or not command
            or json.loads(command["response_json"])["grant_sha256"] != grant_sha256
            or command["request_digest"]
            != digest({"kind": "merge_run", "run_id": spec["run_id"], "request": grant["request"]})
            or saved["phase"] not in ACTIVE
            or not claim
            or claim["owner"] != "external:devflow:" + spec["run_id"]
        ):
            raise ValueError("merge follow-up authority, candidate or ownership changed")
    proof = evidence(store, spec)
    if (digest(_native_proof(proof)) != witness if witness is not None
            else digest(proof) != grant["proof_sha256"]):
        raise ValueError("merge follow-up native evidence changed")
    return proof


def record(store, spec, phase, result=None, error=None, *, only_if=None):
    """Merge progress is separate from the historical delivery projection."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        from .delivery_merge_execution import require_current

        require_current(db, spec, required=True, phase=phase)
        saved = current(db, spec["run_id"])
        if only_if is not None and saved["phase"] != only_if:
            return
        if (
            saved["phase"] == phase
            and saved["result_json"] == canonical_json(result)
            and saved["error"] == error
        ):
            return
        db.execute(
            "UPDATE delivery_merge_requests SET phase=?,result_json=?,error=?,updated_at=? "
            "WHERE run_id=?",
            (phase, canonical_json(result), error, _now(), spec["run_id"]),
        )
        if phase in {"merged", "blocked", "decision_required"}:
            unresolved = db.execute(
                "SELECT 1 FROM delivery_effects WHERE run_id=? AND state!='complete'",
                (spec["run_id"],),
            ).fetchone()
            if not unresolved:
                store.state.release_work(db, spec["work_id"], "external:devflow:" + spec["run_id"])
                db.execute(
                    "UPDATE runtime_sessions SET closed_at=? WHERE id=?",
                    (store.state.now(), "external:devflow:" + spec["run_id"]),
                )
        seq = store._event(
            db,
            spec["run_id"],
            db.execute(
                "SELECT revision FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
            ).fetchone()[0],
            "merge_" + phase,
            error or "PR merge: " + phase.replace("_", " "),
            {"phase": phase, "result": result, "error": error},
        )
        db.execute(
            "UPDATE delivery_phase_events SET workflow_id=?,phase=?,execution_state=? "
            "WHERE event_sequence=?",
            (
                saved["workflow_id"],
                "merge_" + phase,
                "running" if phase in ACTIVE else "terminal",
                seq,
            ),
        )


def step(store, spec, grant_sha256, cancel_event):
    """One bounded observation/effect; uncertain writes retain their original intent."""
    from .delivery_activities import _tracker_sync
    from .delivery_merge import merge_verified

    try:
        with store._connect() as db:
            from .delivery_merge_execution import require_current

            require_current(db, spec, required=True)
            saved = current(db, spec["run_id"])
            if not saved or digest(json.loads(saved["grant_json"])) != grant_sha256:
                raise ValueError("merge activity has no matching durable authorization")
            phase = saved["phase"]
            if phase in {"merged", "blocked", "decision_required"}:
                return json.loads(saved["result_json"])
        if phase == "queued":
            record(store, spec, "merging")
        result = merge_verified(store, spec, {"merge_authorization": grant_sha256}, cancel_event)
        if result["state"] == "resolve_conflicts":
            record(store, spec, "resolving_conflicts", result)
        elif result["state"] == "pending":
            record(store, spec, "waiting_ci", result)
        elif result["state"] == "confirmed":
            tracker = _tracker_sync(spec, "done", release=False)
            if tracker.get("pending") or tracker.get("state") not in {"consistent", "recorded"}:
                result = {
                    "state": "pending",
                    "reason": "merged_tracker_readback",
                    "merge": result,
                    "tracker": tracker,
                }
                record(store, spec, "waiting_tracker", result)
            else:
                result = {**result, "tracker": tracker}
                record(store, spec, "merged", result)
        else:
            record(store, spec, result["state"], result, result.get("reason"))
        return result
    except MergeCleanupPending as exc:
        result = {"state": "pending", "reason": "Waiting for resource custody: " + str(exc)}
        record(store, spec, "waiting_cleanup", result, str(exc))
        return result
    except Exception as exc:
        # A transport error may follow a submitted write. Keep its claim and
        # expose uncertainty; never replay the write with a fresh identity.
        with store._connect() as db:
            unresolved = db.execute(
                "SELECT 1 FROM delivery_effects WHERE run_id=? AND state!='complete'",
                (spec["run_id"],),
            ).fetchone()
        phase = "unknown" if unresolved else "blocked"
        observing = unresolved and isinstance(exc, (RuntimeError, OSError))
        result = {
            "state": "pending" if observing else phase,
            "reason": f"{type(exc).__name__}: {str(exc)[:500]}",
        }
        record(store, spec, phase, result, result["reason"])
        return result


def failed(store, spec, grant_sha256, reason):
    with store._connect() as db:
        from .delivery_merge_execution import require_current

        require_current(db, spec, required=True)
        saved = current(db, spec["run_id"])
        if not saved or digest(json.loads(saved["grant_json"])) != grant_sha256:
            raise ValueError("failed merge activity has no matching durable authorization")
        if saved["phase"] in {"merged", "blocked", "decision_required"}:
            return json.loads(saved["result_json"])
    result = {"state": "unknown", "reason": reason[:500]}
    record(store, spec, "unknown", result, result["reason"])
    return result


async def dispatch(service, client):
    """Resolve an uncertain start by its stable workflow ID and grant digest."""
    from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
    from temporalio.exceptions import WorkflowAlreadyStartedError
    from temporalio.service import RPCError, RPCStatusCode

    from .delivery_merge_workflow import MergeWorkflow

    def pending_requests():
        with service.store._connect() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT * FROM delivery_merge_requests WHERE dispatched=0 AND phase='queued'"
                )
            ]

    pending = await asyncio.to_thread(pending_requests)

    def acknowledge(selected, spec, execution_run_id):
        from .delivery_merge_execution import bind_execution, existing_lease

        with existing_lease(service.config), service.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            bind_execution(db, spec, digest(json.loads(selected["grant_json"])), {
                "workflow_id": selected["workflow_id"], "execution_run_id": execution_run_id})
            db.execute(
                "UPDATE delivery_merge_requests SET dispatched=1,"
                "error=CASE WHEN phase='queued' THEN NULL ELSE error END "
                "WHERE run_id=? AND workflow_id=? AND grant_json=?",
                (selected["run_id"], selected["workflow_id"], selected["grant_json"]),
            )

    def rejected(item, exc):
        from .delivery_merge_execution import dispatch_scope

        spec = service.store.effective_spec(item["run_id"])
        with dispatch_scope(service.store, spec, item):
            record(
                service.store,
                spec,
                "queued",
                {"state": "pending", "reason": "merge_dispatch"},
                f"Merge dispatch pending: {type(exc).__name__}: {str(exc)[:300]}",
                only_if="queued",
            )

    for item in pending:
        try:
            spec = await asyncio.to_thread(service.store.effective_spec, item["run_id"])
            if not await asyncio.to_thread(service.store.owns_execution, spec):
                continue
            bound = digest(json.loads(item["grant_json"]))
            handle = client.get_workflow_handle(item["workflow_id"])
            try:
                found = await handle.describe()
            except RPCError as exc:
                if exc.status != RPCStatusCode.NOT_FOUND:
                    raise
                found = None
            if found is None:
                started = await client.start_workflow(
                    MergeWorkflow.run,
                    args=[spec, bound],
                    id=item["workflow_id"],
                    task_queue=service.config.queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    memo={"merge_authorization": bound},
                )
                execution_run_id = started.first_execution_run_id
            else:
                if (found.id != item["workflow_id"]
                        or found.workflow_type != "DevflowMergeWorkflow"
                        or await found.memo_value("merge_authorization", None) != bound):
                    raise ValueError("merge workflow ID belongs to another authorization")
                execution_run_id = found.run_id
                from .delivery_merge_execution import _original_start

                # A reset copies workflow type, inputs and memo. Only its
                # canonical original start can reconcile an unknown start ACK.
                await asyncio.wait_for(_original_start(
                    service.store, {"spec": spec, "merge_authorization": bound},
                    {"workflow_id": item["workflow_id"], "execution_run_id": execution_run_id},
                    require_running=False), timeout=10)
            await asyncio.to_thread(acknowledge, item, spec, execution_run_id)
        except WorkflowAlreadyStartedError:
            # Lost start acknowledgement: the next pass reads the memo.
            continue
        except Exception as exc:
            await asyncio.to_thread(rejected, item, exc)
