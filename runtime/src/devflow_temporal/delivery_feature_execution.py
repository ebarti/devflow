"""Runtime execution references for GitHub-owned feature plans.

Public runs coordinate features. Internal executions are worker attempts beneath
those runs; their definitions are immutable snapshots of the GitHub plan. They
reuse the existing native role, check, evidence and publication boundaries.
"""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_execution_registry import ExecutionRegistry, OwnershipConflict
from .delivery_feature_gates import STAGES, derive_chunk_gates, validate_chunk_gates
from .delivery_github_contract import GitHubDelivery, ordered_chunks, validate_plan


def registry_path(config) -> Path:
    default = Path.home() / ".local/state/devflow/execution-ownership/registry.sqlite3"
    path = Path(config.raw.get("execution_registry", str(default)))
    if not path.is_absolute() or (
        config.raw.get("provider", "codex") != "fake" and path != default
    ):
        raise ValueError("native runtimes must share the user's canonical execution registry")
    return path


def registry(spec) -> ExecutionRegistry:
    return ExecutionRegistry(Path(spec["feature_delivery"]["registry"]))


def plan_for(spec):
    value = json.loads(spec["accepted_plan"])
    if not isinstance(value, dict) or not {"scope", "acceptance", "workstreams"} <= value.keys():
        raise ValueError("feature delivery requires a structured GitHub workstream plan")
    keys = ("version", "scope", "acceptance", "workstreams", "final_gates") \
        if value.get("version") == 2 else ("scope", "acceptance", "workstreams")
    plan = validate_plan({key: value[key] for key in keys},
                         allowed_paths=spec["policy"].get("allowed_paths"))
    validate_chunk_gates(plan, spec)
    return plan



def admit(store, db, spec):
    """Claim before enqueueing a coordinator, keyed by the remote issue node ID."""
    from .delivery_store import _now

    shared = ExecutionRegistry(registry_path(store.config))
    source = str(store.config.tracking_db)
    if spec["provider"] != "fake":
        with shared.connect() as claims:
            if not claims.execute(
                "SELECT 1 FROM execution_migrated_stores WHERE store_path=?", (source,)
            ).fetchone():
                raise OwnershipConflict("import legacy execution custody before feature admission")
    snapshot = shared.snapshot(spec["run_id"], source)
    if snapshot is None:
        snapshot = GitHubDelivery().snapshot(spec["issue_url"], spec["github_repo"])
    predecessor = spec.pop("feature_predecessor", None)
    # Existing legacy publication is never silently reclassified as a stack.
    legacy = db.execute(
        "SELECT run_id FROM delivery_runs WHERE issue_url=? "
        "AND json_extract(request_json,'$.feature_delivery') IS NULL "
        "AND (pr_json IS NOT NULL OR outcome IS NULL) LIMIT 1",
        (spec["issue_url"],),
    ).fetchone()
    if legacy:
        raise OwnershipConflict(
            "issue has legacy execution/publication custody; explicit adoption "
            "is required before a feature run: " + legacy[0]
        )
    if snapshot["delivery"]:
        accepted = snapshot["delivery"]["manifest"]["plan"]
        spec["accepted_plan"] = canonical_json(accepted)
        plan_for(spec)
        spec["intake_required"] = False
    elif spec["accepted_plan"]:
        plan_for(spec)
    if (not spec["accepted_plan"] and not snapshot["delivery"]
            or spec["accepted_plan"] and json.loads(spec["accepted_plan"]).get("version") == 2):
        spec["feature_plan_version"] = 2
    token = shared.claim(
        snapshot,
        spec["run_id"],
        source,
        maximum_repairs=spec["policy"]["max_repairs"],
        predecessor=predecessor,
    )
    spec["feature_delivery"] = {
        "version": 1,
        "registry": str(shared.path),
        "owner": token,
        "snapshot": snapshot,
        "admitted_at": _now(),
    }
    spec.pop("merge_version", None)
    spec.pop("automatic_retry_version", None)
    spec.setdefault("publication_base_ref", snapshot["default_branch"])


def require_execution(store, spec, *, allow_stopped=False):
    feature = spec.get("feature_delivery")
    if not feature:
        return
    shared = registry(spec)
    child = spec.get("feature_worker")
    if child:
        # A worker can be explicitly reattached by a successor coordinator. Its
        # old Temporal execution remains fenced by the exact workflow ID.
        from temporalio import activity

        current = shared.current(feature["owner"]["issue_id"])
        if current is None or current["state"] not in {"active", "draining"}:
            raise OwnershipConflict("worker's feature has no active coordinator")
        with shared.connect() as db:
            row = db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                (current["issue_id"], worker_key(spec["run_id"], current)),
            ).fetchone()
        if not row or row["generation"] != current["generation"] or row["state"] == "finished":
            raise OwnershipConflict("worker no longer has an active execution assignment")
        identity = spec.get("feature_plan_revision")
        from .delivery_feature_revisions import adopted_plan_identity

        adopted = adopted_plan_identity(spec, shared=shared)
        bound_adopted = {key: adopted[key] for key in ("plan_revision", "plan_digest")}
        if ((identity is not None and identity != bound_adopted)
                or (identity is None and adopted["plan_revision"] > 1)):
            raise OwnershipConflict("worker belongs to a superseded feature plan revision")
        if activity.in_activity() and (
            activity.info().workflow_id != store.active_workflow_id(spec["run_id"])
        ):
            raise OwnershipConflict("worker activity belongs to a superseded workflow")
    else:
        with shared.connect() as db:
            current = shared.require(db, feature["owner"], active=False)
            allowed = {"active", "draining", "stopped"} if allow_stopped else {"active", "draining"}
            if current["state"] not in allowed:
                raise OwnershipConflict("feature coordinator is stopped")


def worker_key(run_id, token):
    return f"{run_id}:generation:{token['generation']}"


def worker_spec(parent, chunk, issue, *, kind, base_sha, base_branch, seed=None):
    """Derive an immutable worker input with version-specific authority and gates."""
    from .delivery_feature_pass import checkpoints

    integration = checkpoints(parent)["integration-pass"] if kind == "chunk" else None
    suffix = digest({"owner": parent["feature_delivery"]["owner"], "chunk": chunk["id"],
                     "kind": kind, "integration_pass": integration,
                     **({"plan_revision": parent.get("feature_plan_revision"),
                         "plan_digest": digest(plan_for(parent))}
                        if json.loads(parent["accepted_plan"]).get("version") == 2 else {})})[:20]
    run_id = "worker-" + suffix
    spec = deepcopy(parent)
    for key in (
        "prepared_environment",
        "preparation",
        "continuation",
        "supersedes_run_id",
        "merge_version",
        "automatic_retry_version",
        "origin_thread_id",
    ):
        spec.pop(key, None)
    plan = plan_for(parent)
    final = chunk["id"] == ordered_chunks(plan)[-1]["id"]
    selected = derive_chunk_gates(parent, plan, chunk, final=final and kind == "chunk")
    spec.update(selected)
    policy = spec["policy"]
    for key in ("security_binding_sha256", "environment_proof_sha256"):
        policy.pop(key, None)
    if plan.get("version") != 2:
        policy["allowed_paths"] = list(chunk["allowed_paths"])
    else:
        spec["feature_plan_revision"] = parent.get("feature_plan_revision", {
            "plan_revision": 1, "plan_digest": digest(plan)})
    policy["initial_decision_prompt"] = None
    policy["recovery"] = None
    policy["pr_body"] = (
        chunk["scope"]
        + "\n\nAcceptance:\n"
        + "\n".join("- " + text for text in chunk["acceptance"])
        + f"\n\nParent feature: Refs {parent['issue_url']}"
        + f"\nWorkstream {chunk['workstream_id']}: Refs {issue['url']}"
        + f"\nChunk: {chunk['id']}\n"
    )
    accepted = {key: chunk[key] for key in ("scope", "steps", "verification", "acceptance")}
    if kind == "chunk":
        accepted["integration"] = (
            "Finish and verify this complete chunk on the current stack "
            "base. Inspect the imported implementation, resolve any "
            "integration conflicts and preserve other chunks."
        )
        if final:
            accepted["feature_acceptance"] = plan["acceptance"]
    spec.update(
        run_id=run_id,
        work_id=run_id,
        command_id=run_id,
        request_digest=digest(
            {
                "parent": parent["request_digest"],
                "chunk": chunk,
                "kind": kind,
                "base": base_sha,
                "seed": seed,
            }
        ),
        goal=chunk["title"],
        publication_summary="feat: " + chunk["title"],
        issue_url=issue["url"],
        accepted_plan=canonical_json(accepted),
        intake_required=False,
        base_sha=base_sha,
        base_ref=base_sha,
        publication_base_ref=base_branch,
        authorized_endpoint="published_unmerged",
        branch="feat/df-"
        + digest(parent["feature_delivery"]["owner"]["issue_id"])[:10]
        + "-"
        + chunk["id"]
        + ("-build-" + suffix[:6] if kind == "build" else ""),
        state_dir=str(Path(parent["state_dir"]).parent / run_id),
        checkout=str(Path(parent["checkout"]).parent / run_id),
        feature_worker={
            "parent_run_id": parent["run_id"],
            "chunk_id": chunk["id"],
            "workstream_id": chunk["workstream_id"],
            "kind": kind,
            "seed": seed,
            "integration_pass": integration["number"] if integration else 0,
        },
        policy_digest=digest(policy),
    )
    if integration:
        previous = next((member for member in integration["members"]
                         if member["chunk_id"] == chunk["id"]), None)
        if previous:
            spec["branch"] = previous["branch"]
            spec["local_branch"] = "feat/df-integration-" + suffix
            spec["feature_worker"].update(previous_publication=previous, seed=None)
    # Chunk gates qualify their integrated base; the coordinator already checked
    # the feature baseline. Workers must not interpret a stack layer as trunk.
    spec.pop("baseline_checks_version", None)
    policy.pop("baseline_checks", None)
    spec["policy_digest"] = digest(policy)
    return spec


def revised_worker_spec(parent_spec, proposed_plan, chunk_id, *, expected_revision, worker=None):
    """Derive a fresh execution snapshot; never alter original stored authority."""
    plan = validate_plan(proposed_plan, allowed_paths=parent_spec["policy"].get("allowed_paths"))
    if plan.get("version") != 2:
        raise ValueError("worker revision requires explicit v2 plan adoption")
    if (not isinstance(expected_revision, dict)
            or set(expected_revision) != {"plan_revision", "plan_digest"}
            or type(expected_revision["plan_revision"]) is not int
            or expected_revision["plan_revision"] < 1
            or expected_revision["plan_digest"] != digest(plan)):
        raise ValueError("worker revision identity does not bind the proposed plan")
    chunks = ordered_chunks(plan)
    chunk = next((item for item in chunks if item["id"] == chunk_id), None)
    if chunk is None:
        raise ValueError("worker revision names an unknown original chunk")
    if worker is not None and (worker.get("feature_worker", {}).get("chunk_id") != chunk_id
            or worker.get("feature_delivery", {}).get("owner", {}).get("issue_id")
            != parent_spec["feature_delivery"]["owner"]["issue_id"]):
        raise ValueError("worker revision cannot change feature or chunk custody")
    result = deepcopy(worker if worker is not None else parent_spec)
    selected = derive_chunk_gates(parent_spec, plan, chunk,
                                  final=chunks[-1]["id"] == chunk_id and
                                  result.get("feature_worker", {}).get("kind") == "chunk")
    if worker is not None:
        # Whole-feature admission uses the parent; the retained worker keeps its
        # non-gate execution authority, including its original source boundary.
        gate_policy = selected.pop("policy")
        result["policy"].update({stage: gate_policy[stage] for stage in STAGES})
    result.update(selected)
    policy = result["policy"]
    for name in ("security_binding_sha256", "environment_proof_sha256"):
        policy.pop(name, None)
    policy["initial_decision_prompt"] = None
    policy["recovery"] = None
    if worker is not None:
        policy["pr_body"] = (
            chunk["scope"] + "\n\nAcceptance:\n"
            + "\n".join("- " + text for text in chunk["acceptance"])
            + f"\n\nParent feature: Refs {parent_spec['issue_url']}"
            + f"\nWorkstream {chunk['workstream_id']}: Refs {worker['issue_url']}"
            + f"\nChunk: {chunk_id}\n")
    policy.pop("baseline_checks", None)
    accepted = {key: chunk[key] for key in ("scope", "steps", "verification", "acceptance")}
    if worker is not None:
        previous_plan = json.loads(worker["accepted_plan"])
        if previous_plan.get("integration"):
            accepted["integration"] = previous_plan["integration"]
    if chunks[-1]["id"] == chunk_id and result.get("feature_worker", {}).get("kind") == "chunk":
        accepted["feature_acceptance"] = plan["acceptance"]
    result.update(accepted_plan=canonical_json(accepted),
                  feature_plan_revision=deepcopy(expected_revision), policy_digest=digest(policy))
    result.pop("baseline_checks_version", None)
    for name in ("prepared_environment", "preparation"):
        result.pop(name, None)
    return result


def register_worker(store, parent, spec):
    from .delivery_store import _now

    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    shared.reserve_worker(
        token, worker_key(spec["run_id"], token), spec["feature_worker"]["workstream_id"]
    )
    shared.checkpoint(token, "worker-input:" + worker_key(spec["run_id"], token), spec)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        previous = db.execute(
            "SELECT request_json FROM delivery_runs WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if previous:
            if json.loads(previous[0]) != spec:
                raise OwnershipConflict("worker attempt is already bound to another input")
            return spec
        store.state.record(
            db,
            "work",
            {
                "id": spec["work_id"],
                "title": spec["goal"],
                "repository": "github.com/" + spec["github_repo"],
                "issue": spec["issue_url"],
                "status": "starting",
            },
        )
        store.state.claim_work(
            db, spec["work_id"], "external:devflow:" + spec["run_id"], store.config.dashboard_url
        )
        stamp = _now()
        db.execute(
            """INSERT INTO delivery_runs(run_id,request_digest,request_json,work_id,
            issue_url,repository_key,phase,execution_state,revision,created_at,updated_at,workflow_id)
            VALUES (?,?,?,?,?,?,'accepted','queued',1,?,?,?)""",
            (
                spec["run_id"],
                spec["request_digest"],
                canonical_json(spec),
                spec["work_id"],
                spec["issue_url"],
                spec["repository_key"],
                stamp,
                stamp,
                "delivery-" + spec["run_id"],
            ),
        )
        db.execute(
            "INSERT INTO delivery_outbox(run_id,state,updated_at) VALUES (?,'pending',?)",
            (spec["run_id"], stamp),
        )
        store._event(db, spec["run_id"], 1, "worker_reserved", "Feature worker assigned", {})
    return spec


def continue_feature(store, run_id, request, *, _revision=None):
    from .delivery_config import COMMAND_RE
    from .delivery_feature_closure import closed_coordinator

    if (
        not isinstance(request, dict)
        or set(request) != {"command_id", "expected_revision"}
        or not isinstance(request["command_id"], str)
        or not COMMAND_RE.fullmatch(request["command_id"])
        or type(request["expected_revision"]) is not int
    ):
        raise ValueError(
            "feature continuation requires a command ID and current projection revision"
        )
    original = store.submitted_spec(run_id)
    if not original.get("feature_delivery") or original.get("feature_worker"):
        raise ValueError("select a feature coordinator to continue its recorded delivery")
    with store._connect() as db:
        row = db.execute(
            "SELECT revision,outcome FROM delivery_runs WHERE run_id=?", (run_id,)
        ).fetchone()
        prior = db.execute(
            "SELECT response_json FROM delivery_commands WHERE command_id=?",
            (request["command_id"],),
        ).fetchone()
    successor_id = "run-feature-" + digest({"run": run_id, "command": request})[:20]
    if prior:
        response = json.loads(prior[0])
        if response["run_id"] != successor_id:
            raise OwnershipConflict("continuation command already belongs to another request")
        return response
    if row["revision"] != request["expected_revision"] or row["outcome"] not in {
        "blocked",
        "cancelled",
    }:
        raise OwnershipConflict("feature is not at the specified stopped checkpoint")
    shared = registry(original)
    token = original["feature_delivery"]["owner"]
    current = shared.current(token["issue_id"])
    if current and shared.token(current) == token and current["state"] == "draining":
        # A worker can complete a separately admitted infrastructure recovery
        # after its coordinator has stopped. Reconcile that closed work through
        # the same settlement boundary before allowing the next generation.
        from .delivery_feature_activities import finish_worker, stop_feature

        closed = closed_coordinator(store, run_id)
        execution = store.effective_spec(run_id)
        with shared.connect() as db:
            workers = [dict(worker) for worker in db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND generation=? "
                "AND state!='finished'", (token["issue_id"], token["generation"]))]
        for worker in workers:
            child_id = worker['worker_key'].split(':generation:')[0]
            child = store.effective_spec(child_id)
            if child.get('feature_delivery', {}).get('owner', {}).get('issue_id') \
                    != token['issue_id'] or not child.get('feature_worker'):
                raise OwnershipConflict('worker closure belongs to a different feature')
            store._completed_temporal_result(child_id,
                                            workflow_id=store.active_workflow_id(child_id))
            finish_worker(execution, child_id)
        stop_feature(execution, {'state': closed['result'],
                                 'reconciled_workers': [w['worker_key'] for w in workers],
                                 **({'failed_coordinator_closure': {
                                     key: value for key, value in closed.items() if key != 'result'
                                 }} if closed.get('workflow_status') == 'FAILED' else {})})
        current = shared.current(token['issue_id'])
    if current is None or shared.token(current) != token or current["state"] != "stopped":
        raise OwnershipConflict("feature workers or effects have not reached a safe handoff")
    closed_coordinator(store, run_id)
    fields = {
        "work_id",
        "issue_url",
        "repository_key",
        "goal",
        "base_ref",
        "authorized_endpoint",
        "origin_thread_id",
        "publication_summary",
        "plan_approval",
    }
    supplied = {key: value for key, value in original.items() if key in fields}
    supplied.update(
        command_id=request["command_id"],
        run_id=successor_id,
        branch="feat/df-coordinator-" + digest(successor_id)[:16],
        feature_predecessor=token,
    )
    accepted = store.effective_spec(run_id).get("accepted_plan")
    if accepted:
        # The supplied plan is already accepted. Store admission restores the
        # original approval policy from its authenticated stopped predecessor.
        supplied.update(accepted_plan=accepted, plan_approval="automatic")
    return (store.submit(supplied, _feature_revision=_revision) if _revision is not None
            else store.submit(supplied))


def _draining_handoff_ready(store, shared, token):
    """Project eligibility; the command still authenticates closure and custody."""
    with shared.connect() as db:
        if db.execute("SELECT 1 FROM execution_effects WHERE issue_id=? AND state='pending'",
                      (token['issue_id'],)).fetchone():
            return False
        workers = list(db.execute(
            "SELECT worker_key,generation FROM execution_workers "
            "WHERE issue_id=? AND state!='finished'", (token['issue_id'],)))
    if any(worker['generation'] != token['generation'] for worker in workers):
        return False
    ids = [token['run_id'], *(w['worker_key'].split(':generation:')[0] for w in workers)]
    with store._connect() as db:
        for run_id in ids:
            row = db.execute('SELECT outcome,cleanup FROM delivery_runs WHERE run_id=?',
                             (run_id,)).fetchone()
            if (not row or row['outcome'] not in {'delivered', 'blocked', 'cancelled'}
                    or row['cleanup'] != 'confirmed'
                    or db.execute("SELECT 1 FROM delivery_attempts WHERE run_id=? "
                                  "AND (state!='finished' OR cleanup!='confirmed')",
                                  (run_id,)).fetchone()
                    or db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? "
                                  "AND (state!='complete' OR observed_json IS NULL)",
                                  (run_id,)).fetchone()):
                return False
    return True


def detail(store, spec):
    if not spec.get("feature_delivery") or spec.get("feature_worker"):
        return None
    shared = registry(spec)
    token = spec["feature_delivery"]["owner"]
    current = shared.current(token["issue_id"])
    from .delivery_feature_pass import checkpoints as current_checkpoints

    checkpoints = current_checkpoints(spec)
    record = checkpoints.get("github-record")
    workers = []
    assignments = [value for key, value in checkpoints.items() if key.startswith("assignment:")]
    with store._connect() as db:
        for assignment in assignments:
            if assignment["store_path"] != str(store.config.tracking_db):
                continue
            row = db.execute(
                "SELECT run_id,phase,outcome,pr_json,cleanup FROM delivery_runs WHERE run_id=?",
                (assignment["run_id"],),
            ).fetchone()
            if row:
                workers.append(
                    {
                        **assignment,
                        **dict(row),
                        "pull_request": json.loads(row["pr_json"] or "null"),
                        "workstream_id": store.effective_spec(row["run_id"]).get(
                            "feature_worker", {}).get("workstream_id"),
                        "issue_url": store.effective_spec(row["run_id"]).get("issue_url"),
                    }
                )
                workers[-1].pop("pr_json")
    return {
        "issue_id": token["issue_id"],
        "owner": shared.token(current) if current else None,
        "ownership_state": current["state"] if current else "missing",
        "can_continue": bool(
            current and shared.token(current) == token
            and (current["state"] == "stopped" or (current["state"] == "draining"
                 and _draining_handoff_ready(store, shared, token)))
        ),
        "github_plan_url": spec["issue_url"] + "#issuecomment-" + str(record["comment_id"])
        if record
        else None,
        "workers": workers,
        "repair_budget": shared.budget(token["issue_id"]),
    }
