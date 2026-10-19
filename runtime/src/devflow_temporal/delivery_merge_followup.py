"""Explicit merge-only handoff for an ordinary, already delivered PR.

The original delivery and endpoint remain immutable. A separate durable command
owns the later merge, its Temporal dispatch, current-target checks and readback.
"""

from __future__ import annotations

import asyncio
import json

from .contracts import canonical_json, digest
from .delivery_merge import (
    MergeBroker,
    native_implementation_output,
    require_merge_gates,
)
from .delivery_policy_recovery import work_binding
from .delivery_preparation import execution_retired
from .delivery_resources import observe_finalized_resources
from .delivery_store import _now

ACTIVE = {"queued", "merging", "resolving_conflicts", "waiting_ci", "waiting_tracker", "unknown"}
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
    reason = json.loads(saved["result_json"] or "{}").get("reason")
    return ((saved["phase"] == "decision_required" and reason == "conflicting_upstream_changes")
            or (saved["phase"] == "unknown" and reason
                == "RuntimeError: merge resolution native cleanup remains unconfirmed"))


def _snapshot(row):
    return {key: row[key] for key in _FIELDS}


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
    if (
        row["outcome"] != "delivered"
        or row["execution_state"] != "terminal"
        or row["cleanup"] != "confirmed"
    ):
        raise ValueError("merge requires a delivered run with confirmed terminal cleanup")
    candidate, pr, checks = [
        json.loads(row[key] or "null") for key in ("candidate_json", "pr_json", "checks_json")
    ]
    matching = [item for item in publications if json.loads(item["observed_json"] or "null") == pr]
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
    resources = observe_finalized_resources(spec)
    return {
        "row": _snapshot(row),
        "candidate": candidate,
        "pull_request": pr,
        "checks": checks,
        "publication": publication,
        "resources": resources,
        "attempts_sha256": digest(attempts),
    }


def _exclusive(store, db, row, *, retained_owner=None):
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
    if (
        latest != row["run_id"]
        or (store.state.claim_for(db, row["work_id"]) is not None
            and store.state.claim_for(db, row["work_id"])["owner"] != retained_owner)
        or db.execute(
            "SELECT 1 FROM delivery_effects WHERE run_id=? AND state!='complete'", (row["run_id"],)
        ).fetchone()
        or db.execute(
            "SELECT 1 FROM delivery_mutations WHERE run_id=? AND state IN ('pending','unknown')",
            (row["run_id"],),
        ).fetchone()
        or db.execute(
            "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state!='sent'", (row["run_id"],)
        ).fetchone()
    ):
        raise ValueError("merge requires the current issue owner with settled effects")


def preflight(store, run_id):
    spec = store.effective_spec(run_id)
    proof = evidence(store, spec)
    with store._connect() as db:
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        _exclusive(store, db, row)
    return {
        "candidate_id": proof["candidate"]["id"],
        "head": proof["pull_request"]["head"],
        "pr_number": proof["pull_request"]["number"],
        "phase": "available",
    }


def admit(store, run_id, request):
    required = {"command_id", "expected_revision", "candidate_id", "head", "pr_number", "answer"}
    if (
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
    ):
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
    spec = store.effective_spec(run_id)
    proof = evidence(store, spec)
    if request["expected_revision"] != proof["row"]["protocol_revision"] or any(
        request[key] != value
        for key, value in (
            ("candidate_id", proof["candidate"]["id"]),
            ("head", proof["pull_request"]["head"]),
            ("pr_number", proof["pull_request"]["number"]),
        )
    ):
        raise ValueError("stale run revision or merge candidate; refresh the run")
    closed = store._completed_temporal_result(run_id, workflow_id=store.active_workflow_id(run_id))
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
    remote = MergeBroker(store, spec).publication(proof["pull_request"])
    if remote["merged"] or remote["state"] != "open":
        raise ValueError("merge requires the existing open unmerged PR")
    workflow_id = "delivery-" + run_id + "-merge-" + bound[:20]
    grant = {
        "request": request,
        "spec_sha256": digest(spec),
        "proof_sha256": digest(proof),
        "row": proof["row"],
        "closed": {k: v for k, v in closed.items() if k != "result"},
    }
    response = {
        "run_id": run_id,
        "phase": "queued",
        "workflow_id": workflow_id,
        "command_id": request["command_id"],
        "grant_sha256": digest(grant),
    }
    if previous_merge is not None:
        if not _continuable(previous_merge):
            raise ValueError("merge already owns a request; read its current result")
        original = json.loads(previous_merge["grant_json"])
        if original["row"] != proof["row"] or original["proof_sha256"] != digest(proof):
            raise ValueError("merge continuation changed its original delivered evidence")
        grant = original
        response["grant_sha256"] = digest(original)
    prelaunch = []
    if previous_merge and previous_merge["phase"] == "unknown":
        from .delivery_merge_resolution import prelaunch_receipts

        prelaunch = prelaunch_receipts(store, spec, previous_merge)
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
        for receipt in prelaunch:
            found = db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                               (receipt["effect_key"],)).fetchone()
            if found["request_json"] != receipt["request_json"] or found["state"] == "complete":
                raise ValueError("merge prelaunch effect changed during continuation")
            db.execute("UPDATE delivery_effects SET state='complete',observed_json=?,updated_at=? "
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
                (run_id, request["command_id"], canonical_json(grant), workflow_id, _now(), _now()),
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
    return response


def authorize(store, spec, grant_sha256):
    with store._connect() as db:
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
        if (
            digest(grant) != grant_sha256
            or grant["spec_sha256"] != digest(spec)
            or _snapshot(row) != grant["row"]
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
    if digest(proof) != grant["proof_sha256"]:
        raise ValueError("merge follow-up native evidence changed")
    return proof


def record(store, spec, phase, result=None, error=None, *, only_if=None):
    """Merge progress is separate from the historical delivery projection."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
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

    def acknowledge(run_id):
        with service.store._connect() as db:
            db.execute(
                "UPDATE delivery_merge_requests SET dispatched=1,"
                "error=CASE WHEN phase='queued' THEN NULL ELSE error END WHERE run_id=?",
                (run_id,),
            )

    def rejected(item, exc):
        spec = service.store.effective_spec(item["run_id"])
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
                await client.start_workflow(
                    MergeWorkflow.run,
                    args=[spec, bound],
                    id=item["workflow_id"],
                    task_queue=service.config.queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    memo={"merge_authorization": bound},
                )
            elif await found.memo_value("merge_authorization", None) != bound:
                raise ValueError("merge workflow ID belongs to another authorization")
            await asyncio.to_thread(acknowledge, item["run_id"])
        except WorkflowAlreadyStartedError:
            # Lost start acknowledgement: the next pass reads the memo.
            continue
        except Exception as exc:
            await asyncio.to_thread(rejected, item, exc)
