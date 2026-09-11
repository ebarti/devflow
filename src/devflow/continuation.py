"""Explicit continuation of one verified PR outcome; historical proof is immutable."""

from copy import deepcopy

from devflow.admission import derived_authority, execution_admission, user_request_admission
from devflow.validation import digest, validate_record


def reopen_admission(state, request, verifier, now, repository):
    from devflow.domain.rules import require

    require(
        set(request)
        <= {
            "operation_id",
            "work_id",
            "expected_revision",
            "record",
            "user_request",
            "workflow_snapshot",
            "continuation",
            "reuse_candidate",
        },
        "invalid_request",
        "Unknown fields in PR continuation",
    )
    require(
        "user_request" in request,
        "user_request_required",
        "Reopen requires a fresh recorded user request",
    )
    options = request.get("continuation", {})
    require(isinstance(options, dict), "invalid_request", "continuation must be an object")
    admission = user_request_admission(
        state,
        request.get("record", {}),
        request["user_request"],
        repository,
        continuation_of={
            "prior_delivery_id": options.get("prior_delivery_id"),
            "prior_revision": request.get("expected_revision"),
            "entry_phase": options.get("entry_phase"),
            "request_hash": digest(request),
        },
    )
    return execution_admission(
        state,
        request["record"],
        admission["admission_id"],
        verifier,
        now,
        repository=repository,
        requested=admission,
    )


def reopen(state, request, now, admission, observation):
    from devflow.domain.rules import (
        authority,
        blocking_findings,
        current_candidate,
        current_passing_evidence,
        get,
        input_signature,
        missing_checks,
        missing_scenarios,
        require,
        require_gate_handoff,
        required_roles,
        save,
        scope_hash,
        valid_gates,
    )

    require(
        state["lifecycle"] == "done"
        and state["contract"]["endpoint"]["kind"] == "pr"
        and state["attempt"]
        and state["attempt"]["status"] == "done",
        "invalid_state",
        "Only a verified completed PR outcome can reopen",
    )
    options = request["continuation"]
    require(
        isinstance(options, dict)
        and set(options)
        == {
            "entry_phase",
            "prior_delivery_id",
            "owner_task_id",
            "host_id",
            "pr_number",
            "head_ref",
            "base_ref",
            "expected_head",
        },
        "invalid_request",
        "Supply exact continuation identity and entry phase",
    )
    require(
        options["entry_phase"] in {"implement", "deliver"},
        "invalid_request",
        "Reopen enters Implement or Deliver",
    )
    prior = get(state, "delivery", state["delivery_id"])
    action = state["actions"].get(prior["action_id"], {})
    old_pr = action.get("observation", {})
    require(
        options["prior_delivery_id"] == prior["delivery_id"]
        and prior["status"] == "verified"
        and prior["endpoint"]["kind"] == "pr"
        and action.get("status") == "confirmed"
        and old_pr.get("verified") is True
        and old_pr.get("independent_readback") is True
        and old_pr.get("pr_number")
        and old_pr.get("node_id")
        and old_pr.get("action_marker"),
        "unverified_delivery",
        "Reopen needs the original verified PR receipt and identity",
    )
    require(
        options["owner_task_id"] == state["attempt"]["owner_task_id"]
        and options["host_id"] == state["attempt"]["host_id"],
        "continuation_owner",
        "Reopen retains the original coordinator and host",
    )
    require(
        not any(
            a["status"] in {"prepared", "dispatched", "ambiguous", "pending_setup"}
            for a in state["actions"].values()
        ),
        "reconcile_required",
        "Resolve every unfinished action before reopening",
    )
    require_gate_handoff(state)
    require(
        not any(
            a["status"]
            in {"prepared", "pending_startup", "pending_setup", "ready", "running", "interrupted"}
            or (
                a["role"] == "implementation_worker"
                and not a.get("implementation_result")
                and a["status"] != "replaced"
            )
            for a in state["assignments"].values()
        ),
        "producer_result_required",
        "Import or reconcile unfinished producer output before reopening",
    )
    require(
        all(
            any(
                r.get("record_type") == "gate_result"
                and r["assignment_id"] == a["assignment_id"]
                and r["candidate_id"] == a["candidate_id"]
                and (
                    not a.get("gate_action_id")
                    or r.get("assignment_action_id") == a["gate_action_id"]
                )
                for r in state["records"].values()
            )
            for a in state["assignments"].values()
            if a["role"] in {"review", "qa"} and a["status"] != "replaced"
        ),
        "gate_result_required",
        "Every retained independent producer needs its original imported result",
    )
    contract = validate_record(request["record"], "work_contract")
    # A continuation repairs this outcome, rather than admitting an independent defect.
    immutable = set(contract) - {
        "scope_revision",
        "context",
        "endpoint",
        "title",
        "scope",
        "risk",
        "verification",
        "acceptance",
    }
    require(
        all(contract[k] == state["contract"][k] for k in immutable)
        and contract["endpoint"]["kind"] == "pr",
        "continuation_scope",
        "Reopen must retain this outcome and source lineage",
    )
    previous = state["contract"]
    require(
        set(previous["scope"]["paths"]) <= set(contract["scope"]["paths"])
        and previous["risk"]["tier"] <= contract["risk"]["tier"]
        and all(a in contract["acceptance"] for a in previous["acceptance"])
        and all(
            set(previous["verification"][k]) <= set(contract["verification"][k])
            for k in ("recipes", "scenarios", "documentation_owners")
        ),
        "continuation_scope",
        "A continuation cannot remove owned paths or weaken risk, acceptance or verification",
    )
    acceptance_ids = [a["id"] for a in contract["acceptance"]]
    require(
        len(acceptance_ids) == len(set(acceptance_ids)),
        "duplicate_acceptance",
        "Acceptance IDs must be unique",
    )
    require(
        contract == state["contract"]
        or contract["scope_revision"] == state["contract"]["scope_revision"] + 1,
        "scope_revision",
        "Changed continuation contracts require the next scope revision",
    )
    from devflow.domain.endpoints import validate_endpoint

    validate_endpoint(contract["endpoint"])
    require(
        admission["repository"] == state["authority"]["repository"],
        "repository_transfer",
        "Reopen cannot transfer repositories",
    )
    require(
        isinstance(observation, dict)
        and observation.get("repository") == admission["repository"]
        and observation.get("pr_number") == options["pr_number"] == old_pr["pr_number"]
        and observation.get("node_id") == old_pr["node_id"]
        and observation.get("head_ref") == options["head_ref"] == old_pr["head_ref"]
        and observation.get("base_ref") == options["base_ref"] == contract["endpoint"]["target"]
        and observation.get("head_sha") == options["expected_head"]
        and observation.get("state") == "open"
        and observation.get("draft") is False
        and observation.get("action_marker") == old_pr["action_marker"],
        "continuation_pr",
        "Current open PR must match the delivered PR, repository, source, target and expected head",
    )
    old_snapshot = get(state, "workflow_snapshot", state["attempt"]["workflow_snapshot_id"])
    snapshot = validate_record(request.get("workflow_snapshot", old_snapshot), "workflow_snapshot")
    upgrade = snapshot.get("continuation_upgrade")
    if upgrade and snapshot != old_snapshot:
        require(
            upgrade["work_id"] == state["work_id"]
            and upgrade["prior_snapshot_id"] == old_snapshot["snapshot_id"]
            and upgrade["prior_package_revision"] == old_snapshot["package_revision"]
            and upgrade["prior_delivery_id"] == prior["delivery_id"],
            "snapshot_mismatch",
            "Upgrade must identify this completed delivery and original pin",
        )
    reuse = options["entry_phase"] == "deliver"
    if reuse:
        candidate = current_candidate(state, state["candidate_id"])
        expected = {
            k: candidate[k]
            for k in ("candidate_id", "head_sha", "tree_sha", "dependency_hash", "environment_hash")
        }
        require(
            request.get("reuse_candidate") == expected
            and observation["head_sha"] == candidate["head_sha"]
            and scope_hash(contract) == state["scope_hash"]
            and snapshot == old_snapshot,
            "continuation_reuse",
            "Deliver reuse requires the identical candidate, operative scope and workflow inputs",
        )
        gates = valid_gates(state)
        evidence = current_passing_evidence(state)
        require(
            not missing_checks(state)
            and not missing_scenarios(state, evidence)
            and set(required_roles(state)) <= set(gates)
            and not blocking_findings(state, state["candidate_id"])
            and all(
                g["scope_hash"] == state["scope_hash"]
                and g["workflow_hash"] == snapshot["workflow_hash"]
                for g in gates.values()
            )
            and all(
                e["input_signature"] == input_signature(candidate, e)
                for e in evidence
                if e["record_type"] == "check_evidence"
            )
            and {a["id"] for a in contract["acceptance"]}
            <= set().union(*(set(e["acceptance_ids"]) for e in evidence)),
            "continuation_reuse",
            "Deliver reuse requires complete applicable check, acceptance and gate proof",
        )
    else:
        require(
            state["attempt"].get("execution_mode") == "subagent",
            "continuation_execution_mode",
            "Conflict repair requires an existing delegated subagent attempt",
        )
        require(
            "reuse_candidate" not in request,
            "invalid_request",
            "Implement cannot carry delivery reuse assertions",
        )
    auth = derived_authority(admission)
    continuation = {
        "schema_version": 1,
        "record_type": "work_continuation",
        "continuation_id": "continuation-" + digest([request["operation_id"], request])[:24],
        "work_id": state["work_id"],
        "attempt_id": state["attempt"]["attempt_id"],
        "prior_revision": state["revision"],
        "prior_delivery_id": prior["delivery_id"],
        "prior_admission_id": state["admission_id"],
        "prior_authority_id": state["authority"]["authority_id"],
        "prior_scope_hash": state["scope_hash"],
        "prior_workflow_snapshot_id": old_snapshot["snapshot_id"],
        "admission_id": admission["admission_id"],
        "authority_id": auth["authority_id"],
        "scope_hash": scope_hash(contract),
        "workflow_snapshot_id": snapshot["snapshot_id"],
        "owner_task_id": options["owner_task_id"],
        "host_id": options["host_id"],
        "entry_phase": options["entry_phase"],
        "pr_observation": deepcopy(observation),
        "prior_candidate_id": state["candidate_id"],
        "prior_gate_ids": deepcopy(state["gate_ids"]),
        "prior_check_ids": deepcopy(state["check_ids"]),
        "prior_accounting_id": state.get("accounting_id"),
        "reused_candidate": deepcopy(request.get("reuse_candidate")),
        "created_at": now.isoformat(),
    }
    for r in (admission, auth, contract, snapshot, continuation):
        save(state, r)
    state.update(
        authority=auth,
        admission_id=admission["admission_id"],
        contract=deepcopy(contract),
        scope_hash=scope_hash(contract),
        continuation_id=continuation["continuation_id"],
        lifecycle="active",
        phase=options["entry_phase"],
        blocker=None,
        delivery_id=None,
        last_delivery_id=None,
        accounting_id=None,
    )
    for permission in (
        ["edit", "check", "create_tasks", "publish_pr"] if not reuse else ["publish_pr"]
    ):
        authority(state, now, permission)
    state["attempt"].update(
        status="active",
        scope_hash=state["scope_hash"],
        authority_id=auth["authority_id"],
        workflow_snapshot_id=snapshot["snapshot_id"],
        model_policy_snapshot_id=snapshot["snapshot_id"],
    )
    if not reuse:
        state.update(candidate_id=None, gate_ids={}, check_ids={}, fix_observations={})
    return {"continuation": continuation}
