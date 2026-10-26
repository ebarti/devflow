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



def _require_migrated_store(shared, spec, source):
    if spec["provider"] != "fake":
        with shared.connect() as claims:
            if not claims.execute(
                "SELECT 1 FROM execution_migrated_stores WHERE store_path=?", (source,)
            ).fetchone():
                raise OwnershipConflict("import legacy execution custody before feature admission")


def _require_no_legacy_custody(db, spec):
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


def observe_admission(store, spec):
    """Read GitHub before the admission transaction; a frozen registry snapshot wins.

    Custody preconditions come first: a refused admission never reads GitHub,
    and a GitHub failure is never reported in its place.
    """
    shared = ExecutionRegistry(registry_path(store.config))
    source = str(store.config.tracking_db)
    _require_migrated_store(shared, spec, source)
    with store._connect() as db:
        _require_no_legacy_custody(db, spec)
    if shared.snapshot(spec["run_id"], source) is not None:
        return None
    return GitHubDelivery().snapshot(spec["issue_url"], spec["github_repo"])


def admit(store, db, spec, observed=None):
    """Claim before enqueueing a coordinator, keyed by the remote issue node ID."""
    from .delivery_store import _now

    shared = ExecutionRegistry(registry_path(store.config))
    source = str(store.config.tracking_db)
    _require_migrated_store(shared, spec, source)
    snapshot = shared.snapshot(spec["run_id"], source)
    if snapshot is None:
        # The caller holds the tracking write lock: no GitHub call may run here.
        if observed is None:
            raise ValueError("feature admission requires its GitHub snapshot to be "
                             "observed before the write transaction")
        snapshot = observed
    predecessor = spec.pop("feature_predecessor", None)
    retry_values = shared.checkpoints(snapshot["issue"]["id"])
    for key, receipt in retry_values.items():
        if (key.startswith("plan-revision:preparation-retry:")
                and receipt["successor_id"] == spec["run_id"]):
            from .delivery_execution_registry import preparation_lineage

            public = retry_values.get("plan-revision:request:" + receipt["revision_id"])
            with shared.connect() as claims:
                chain = preparation_lineage(claims, public) if public else []
            if (spec.get("feature_plan_revision_preparation_retry")
                    != {"checkpoint_key": key, "sha256": digest(receipt)}
                    or spec.get("feature_plan_revision_request") != public
                    or receipt["predecessor"] != predecessor
                    or not chain or chain[-1] != (key, receipt)):
                raise OwnershipConflict(
                    "preparation successor lacks its server reservation binding")
    _require_no_legacy_custody(db, spec)
    if snapshot["delivery"]:
        manifest = snapshot["delivery"]["manifest"]
        accepted = manifest["plan"]
        scope = spec.get("feature_scope_amendment")
        if scope:
            identity = spec.get("feature_plan_revision", scope["plan_identity"])
            if (canonical_json(accepted) != canonical_json(json.loads(spec["accepted_plan"]))
                    or digest(accepted) != identity["plan_digest"]
                    or manifest.get("plan_revision", 1) != identity["plan_revision"]):
                raise OwnershipConflict(
                    "scope continuation cannot change its accepted feature plan")
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


def _worker_assignment(store, spec):
    feature = spec["feature_delivery"]
    shared = registry(spec)
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
    return shared, current, row


def require_execution(store, spec, *, allow_stopped=False, terminal_settlement=False):
    feature = spec.get("feature_delivery")
    if not feature:
        return
    shared = registry(spec)
    child = spec.get("feature_worker")
    if child:
        # A worker can be explicitly reattached by a successor coordinator. Its
        # old Temporal execution remains fenced by the exact workflow ID.
        from temporalio import activity

        shared, current, _ = _worker_assignment(store, spec)
        if current['store_path'] != str(store.config.tracking_db):
            raise OwnershipConflict('worker assignment belongs to another owner store')
        if not terminal_settlement and current['state'] != 'active':
            raise OwnershipConflict('worker activity requires an active coordinator')
        if spec.get("repair_allowance"):
            from .delivery_stopped_resume import validate_worker_allowance

            validate_worker_allowance(spec, current=not terminal_settlement)
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


def require_worker_handoff(store, spec):
    """Authorize coordinator reservation without granting worker activity authority."""
    from temporalio import activity

    from .delivery_feature_pass import checkpoints

    if not spec.get("feature_worker"):
        raise OwnershipConflict("worker handoff requires its retained assignment")
    if not activity.in_activity() or (
        activity.info().workflow_id == store.active_workflow_id(spec["run_id"])
    ):
        require_execution(store, spec)
        return
    shared, current, assignment = _worker_assignment(store, spec)
    parent = store.effective_spec(current["run_id"])
    child = spec["feature_worker"]
    bound = checkpoints(parent).get(f"assignment:{child['chunk_id']}:{child['kind']}")
    if (activity.info().activity_type != "delivery_feature_reserve"
            or activity.info().workflow_id != store.active_workflow_id(current["run_id"])
            or current["store_path"] != str(store.config.tracking_db)
            or parent.get("feature_worker")
            or parent.get("feature_delivery", {}).get("owner") != shared.token(current)
            or assignment["workstream_id"] != child["workstream_id"]
            or bound != {"run_id": spec["run_id"], "store_path": current["store_path"],
                         "chunk_id": child["chunk_id"], "kind": child["kind"]}):
        raise OwnershipConflict("worker handoff does not belong to its current coordinator")
    require_execution(store, parent)


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
    from .delivery_feature_scope import MARKER

    amendment = parent.get(MARKER)
    if amendment:
        added = amendment["scope"]["added_paths"]
        if amendment["chunk_id"] == chunk["id"]:
            policy["allowed_paths"] = list(dict.fromkeys([*policy["allowed_paths"], *added]))
        else:
            policy["allowed_paths"] = [
                path for path in policy["allowed_paths"] if path not in added]
            spec.pop(MARKER, None)
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


def assess_feature_continuation(store, run_id, request):
    """Observe only: settlement and successor admission have not yet been proved."""
    from .delivery_lifecycle import evaluate_transition

    with store._connect() as db:
        row = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
    return evaluate_transition({
        "operation": "continue_feature", "owner": "feature_continuation",
        "owner_proof": None, "normalized_lifecycle": "unknown",
        "revision": row["revision"] if row else None,
        "comparisons": [("revision", "expected_revision", "revision_changed")],
        "predicates": [{"code": "feature_settlement_proof_unknown", "satisfied": None,
                        "missing_or_unknown_fact": (
                            "settled feature effects, worker custody and successor admission"),
                        "next_action": "inspect feature settlement and successor admission"}],
    }, request)


def continue_feature(store, run_id, request, *, _revision=None):
    from .delivery_config import COMMAND_RE
    from .delivery_feature_closure import closed_coordinator, closed_settlement, closed_worker
    from .delivery_feature_scope import FIELDS as SCOPE_FIELDS
    from .delivery_feature_scope import amendment

    abandon = request.get("abandon_revision") if isinstance(request, dict) else None
    if (
        not isinstance(request, dict)
        or not {"command_id", "expected_revision"} <= set(request)
        or set(request) - {"command_id", "expected_revision", "abandon_revision", *SCOPE_FIELDS}
        or (set(request) & SCOPE_FIELDS and not SCOPE_FIELDS <= set(request))
        or (set(request) & SCOPE_FIELDS and (abandon is not None or _revision is not None))
        or not isinstance(request["command_id"], str)
        or not COMMAND_RE.fullmatch(request["command_id"])
        or type(request["expected_revision"]) is not int
        or ("abandon_revision" in request
            and (not isinstance(abandon, str) or not 1 <= len(abandon) <= 128))
    ):
        raise ValueError(
            "feature continuation requires a command ID and current projection revision"
        )
    original = store.submitted_spec(run_id)
    if not original.get("feature_delivery") or original.get("feature_worker"):
        raise ValueError("select a feature coordinator to continue its recorded delivery")
    with store._connect() as db:
        row = db.execute(
            "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)
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
    scope = (amendment(store, store.effective_spec(run_id), request, successor_id)
             if SCOPE_FIELDS <= set(request) else None)
    closed = None
    recorded_workers = None
    if current and shared.token(current) == token and current['state'] == 'active':
        # A generic infrastructure failure can close the coordinator before its
        # settlement activity drains custody. Only its actual failed request can
        # admit the same local settlement after all assigned work really closes.
        closed = closed_settlement(store, run_id)
        recorded_workers = _settlement_workers(store, store.effective_spec(run_id), closed)
    if current and shared.token(current) == token and current['state'] in {'active', 'draining'}:
        # A worker can complete a separately admitted infrastructure recovery
        # after its coordinator has stopped. Reconcile that closed work through
        # the same settlement boundary before allowing the next generation.
        from .delivery_execution_registry import UnresolvedEffect
        from .delivery_feature_activities import finish_worker, stop_feature
        from .delivery_feature_readback import settle

        execution = store.effective_spec(run_id)
        # The coordinator may have closed while an external operation still awaited
        # readback. Read it back here, through the same idempotent settlement the
        # coordinator used, before anything is written: an effect is never
        # confirmed or dropped without an authentic readback, and an unresolved one
        # refuses this continuation until GitHub answers.
        readback = settle(store, execution)
        if readback["state"] == "contradicted":
            # A definitive verdict, not a wait: retrying cannot settle it.
            raise OwnershipConflict(
                f"original GitHub operation {readback['effect_key']} no longer matches its "
                f"recorded identity ({readback['reason']}); it will not settle by waiting: "
                f"{readback['remedy']}, then continue the feature")
        if readback["state"] != "confirmed":
            raise UnresolvedEffect(
                f"original GitHub operation {readback['effect_key']} is still unconfirmed "
                f"({readback['reason']}); continue the feature again once it settles")
        if recorded_workers is not None and not _draining_handoff_ready(
                store, shared, token, worker_ids=closed['settlement']['workers']):
            raise OwnershipConflict('closed feature workers or cleanup remain unsettled')
        closed = closed or closed_coordinator(store, run_id)
        with shared.connect() as db:
            workers = [dict(worker) for worker in db.execute(
                "SELECT * FROM execution_workers WHERE issue_id=? AND generation=? "
                "AND state!='finished'", (token["issue_id"], token["generation"]))]
        if recorded_workers is not None:
            workers = recorded_workers
        worker_closures = {}
        for worker in workers:
            child_id = worker['worker_key'].split(':generation:')[0]
            child = store.effective_spec(child_id)
            if child.get('feature_delivery', {}).get('owner', {}).get('issue_id') \
                    != token['issue_id'] or not child.get('feature_worker'):
                raise OwnershipConflict('worker closure belongs to a different feature')
            if recorded_workers is not None:
                child_closed = closed_worker(
                    store, child_id, closed['settlement']['workers'][child_id])
                worker_closures[child_id] = {
                    key: child_closed[key] for key in (
                        'workflow_id', 'execution_run_id', 'closed_at', 'history_sha256')}
                worker_closures[child_id]['result_sha256'] = digest(child_closed['result'])
            else:
                store._completed_temporal_result(child_id,
                                                workflow_id=store.active_workflow_id(child_id))
            finish_worker(execution, child_id)
        if recorded_workers is not None:
            with store._connect() as db:
                latest = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                    (run_id,)).fetchone()
            if dict(latest) != dict(row) or store.effective_spec(run_id) != execution:
                raise OwnershipConflict('closed feature checkpoint changed before settlement')
        stop_feature(execution, {'state': closed['result'],
                                 'reconciled_workers': [w['worker_key'] for w in workers],
                                 **({'completed_coordinator_settlement': {**closed['settlement'],
                                                                         'worker_closures':
                                                                         worker_closures}}
                                    if recorded_workers is not None else {}),
                                 **({'failed_coordinator_closure': {
                                     key: value for key, value in closed.items() if key != 'result'
                                 }} if closed.get('workflow_status') == 'FAILED' else {})})
        current = shared.current(token['issue_id'])
    if _revision is None:
        # Only once a draining owner has settled its effects and stopped: a pending
        # plan revision presumes a stopped, closed predecessor, and its closure
        # path needs the stopped checkpoint.
        from .delivery_feature_revisions import (
            abandon_reserved_revision,
            continue_pending_preparation,
            revision_request,
        )

        execution = store.effective_spec(run_id)
        if abandon is not None:
            # Idempotent: a reservation this command already closed is simply absent.
            abandon_reserved_revision(store, execution, abandon, request["command_id"])
        elif revision_request(execution, shared):
            _revision = continue_pending_preparation(store, execution, row, request, successor_id)
            current = shared.current(token["issue_id"])
    replay_claim = bool(_revision and "preparation_retry" in _revision and current
                       and current["run_id"] == successor_id and current["state"] == "active"
                       and shared.snapshot(successor_id, str(store.config.tracking_db)))
    if not replay_claim and (
        current is None or shared.token(current) != token or current["state"] != "stopped"
    ):
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
    admission = {}
    if original["provider"] != "fake" and not replay_claim:
        admission["_feature_allowance"] = token
    if scope is not None:
        admission["_feature_scope"] = scope
    elif _revision is not None:
        admission["_feature_revision"] = _revision
    return store.submit(supplied, **admission)


def _renew_repair_allowance(store, shared, token):
    """A supported continuation may renew only from server-written runtime evidence."""
    budget = shared.budget(token["issue_id"])
    try:
        baseline = {}
        if not budget.get("allowance"):
            with store._connect() as db:
                row = db.execute(
                    "SELECT identity_json FROM delivery_dashboard_state WHERE run_id=?",
                    (token["run_id"],),
                ).fetchone()
            recorded = json.loads(row[0]) if row and row[0] else {}
            if recorded.get("dirty") is not False or recorded.get("local_digest") is not None:
                raise ValueError("predecessor has no clean recorded runtime identity")
            native = store.spec(token["run_id"])["policy"].get("native_identity", {})
            from .delivery_feature_review_context import _protected_root, _sources

            _, payload = _sources(_protected_root(), recorded.get("revision"))
            if native and native.get("runtime_payload_sha256") != payload:
                raise ValueError("predecessor runtime differs from its prepared payload")
            baseline = {"revision": recorded["revision"], "runtime_payload_sha256": payload}
        shared.renew_repair_allowance(token, baseline)
    except (OSError, ValueError) as exc:
        # An old, still-funded feature retains its original continuation contract.
        # Missing or unverified identity never creates a new allowance.
        if isinstance(exc, OwnershipConflict) or budget["used"] >= budget["maximum"]:
            raise OwnershipConflict("cannot authenticate a new installed repair allowance: "
                                    + str(exc)) from exc


def _settlement_workers(store, spec, closed):
    """Retain the failed request's exact current worker assignments and adopted plan."""
    from .delivery_feature_pass import checkpoints
    from .delivery_feature_revisions import adopted_plan_identity

    shared, token = registry(spec), spec['feature_delivery']['owner']
    if token['store_path'] != str(store.config.tracking_db):
        raise OwnershipConflict('closed settlement belongs to another tracking store')
    adopted = adopted_plan_identity(spec, shared=shared)
    identity = {key: adopted[key] for key in ('plan_revision', 'plan_digest')}
    if (adopted['plan_digest'] != digest(plan_for(spec))
            or (spec.get('feature_plan_revision') is not None
                and spec['feature_plan_revision'] != identity)
            or (spec.get('feature_plan_revision') is None and adopted['plan_revision'] > 1)):
        raise OwnershipConflict('closed settlement belongs to a superseded feature plan')
    assignments = checkpoints(spec)
    workers = closed['settlement']['workers']
    with shared.connect() as db:
        shared.require(db, token, active=False)
        rows = {worker['worker_key']: dict(worker) for worker in db.execute(
            'SELECT * FROM execution_workers WHERE issue_id=?', (token['issue_id'],))}
    expected = {worker_key(child_id, token) for child_id in workers}
    if any(worker['state'] != 'finished' and key not in expected for key, worker in rows.items()):
        raise OwnershipConflict('failed settlement omits a current worker assignment')
    bound = []
    for child_id, workflow_id in sorted(workers.items()):
        child = store.effective_spec(child_id)
        worker = rows.get(worker_key(child_id, token))
        role = child.get('feature_worker') or {}
        child_owner = child.get('feature_delivery', {}).get('owner', {})
        assignment = assignments.get(f"assignment:{role.get('chunk_id')}:{role.get('kind')}")
        if (worker is None or worker['generation'] != token['generation']
                or worker['workstream_id'] != role.get('workstream_id')
                or child_owner.get('issue_id') != token['issue_id']
                or child_owner.get('store_path') != token['store_path']
                or workflow_id != store.active_workflow_id(child_id)
                or assignment != {'run_id': child_id, 'store_path': token['store_path'],
                                  'chunk_id': role.get('chunk_id'), 'kind': role.get('kind')}
                or (child.get('feature_plan_revision') is not None
                    and child['feature_plan_revision'] != identity)
                or (child.get('feature_plan_revision') is None and adopted['plan_revision'] > 1)):
            raise OwnershipConflict('failed settlement worker assignment changed')
        bound.append(worker)
    return bound


def _active_handoff_ready(store, shared, token):
    if not _draining_handoff_ready(store, shared, token):
        return False
    from .delivery_feature_closure import closed_settlement, closed_worker

    try:
        closed = closed_settlement(store, token['run_id'], sealed_only=True)
        _settlement_workers(store, store.effective_spec(token['run_id']), closed)
        for child_id, workflow_id in closed['settlement']['workers'].items():
            closed_worker(store, child_id, workflow_id, sealed_only=True)
    except (ValueError, OwnershipConflict, OSError):
        return False
    return _draining_handoff_ready(store, shared, token,
                                   worker_ids=closed['settlement']['workers'])


def _draining_handoff_ready(store, shared, token, *, worker_ids=()):
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
    ids = [token['run_id'], *(w['worker_key'].split(':generation:')[0] for w in workers),
           *worker_ids]
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
                 and _draining_handoff_ready(store, shared, token))
                 or (current["state"] == "active" and _active_handoff_ready(store, shared, token)))
        ),
        "github_plan_url": spec["issue_url"] + "#issuecomment-" + str(record["comment_id"])
        if record
        else None,
        "workers": workers,
        "repair_budget": shared.budget(token["issue_id"]),
    }
