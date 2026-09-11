"""Pure workflow transitions. All time and external observations are explicit inputs."""

from copy import deepcopy
from datetime import datetime

from devflow.admission import (
    BOOKKEEPING,
    derived_authority,
    execution_admission,
    requested_admission,
)
from devflow.domain.endpoints import readback_matches, validate_action_target, validate_endpoint
from devflow.errors import WorkflowError
from devflow.validation import digest, validate_record

ID_FIELDS = {
    "intake_admission": "admission_id",
    "work_continuation": "continuation_id",
    "work_contract": "scope_revision",
    "outcome_event": "event_id",
    "authority": "authority_id",
    "attempt": "attempt_id",
    "candidate": "candidate_id",
    "assignment": "assignment_id",
    "check_evidence": "evidence_id",
    "observation_evidence": "evidence_id",
    "gate_result": "gate_id",
    "gate_ingestion": "gate_id",
    "gate_result_recovery": "recovery_id",
    "finding": "finding_id",
    "fix_verification": "verification_id",
    "delivery": "delivery_id",
    "workflow_snapshot": "snapshot_id",
    "execution_segment": "segment_id",
    "usage": "response_id",
    "usage_accounting": "accounting_id",
}
ACTION_OPERATIONS = {
    "run_check",
    "prepare_workspace",
    "launch_role",
    "send_role",
    "publish_pr",
    "push_branch",
    "publish_finding",
    "reply_finding",
    "resolve_thread",
    "merge",
    "release",
    "sync_projection",
    "publish_status",
    "local_delivery",
}
PERMISSIONS = {
    "run_check": "check",
    "launch_role": "create_tasks",
    "send_role": "create_tasks",
    "publish_pr": "publish_pr",
    "push_branch": "publish_pr",
    "publish_finding": "publish_findings",
    "reply_finding": "publish_findings",
    "resolve_thread": "publish_findings",
    "merge": "merge",
    "release": "release",
    "publish_status": "publish_status",
    "prepare_workspace": "edit",
    "local_delivery": "edit",
    "sync_projection": "publish_pr",
}


def require(condition, code, message):
    if not condition:
        raise WorkflowError(code, message)


def subagent_mode(state):
    # Absence on historical stored attempts means the original visible-thread bridge.
    return (state.get("attempt") or {}).get("execution_mode") == "subagent"


def stage_contract(state):
    """Stronger handoffs apply to the attempt's pin, never rewrite old releases."""
    if not subagent_mode(state):
        return False
    snapshot = state["records"].get(
        "workflow_snapshot:" + state["attempt"]["workflow_snapshot_id"], {}
    )
    version = snapshot.get("package_version", "0.0.0").split("-")[0].split(".")
    return len(version) == 3 and all(v.isdigit() for v in version) and tuple(map(int, version)) >= (0, 5, 0)


def pending_gate_results(state):
    if not stage_contract(state):
        return []
    imported = {(r["assignment_id"], r.get("assignment_action_id"))
                for r in state["records"].values() if r.get("record_type") == "gate_result"}
    return [a for a in state["assignments"].values()
            if a["role"] in {"review", "qa"} and a.get("gate_action_id")
            and (a["assignment_id"], a["gate_action_id"]) not in imported]


def require_gate_handoff(state, role=None):
    pending = pending_gate_results(state)
    if role in {"review", "qa"}:
        pending = [a for a in pending if a["role"] == role]
    require(not pending, "gate_result_required",
            "Import every activated independent producer's PASS/FAIL/BLOCKED result before repair or reuse")


def validate_role_continuation(state, payload):
    assignment = state["assignments"].get(payload.get("assignment_id"))
    recovering = bool(payload.get("result_recovery_id"))
    observed_status = (assignment or {}).get("control_observation", {}).get("agent_status")
    allowed_statuses = {"completed", "interrupted"} if recovering else {"interrupted"}
    require(assignment is not None and assignment in pending_gate_results(state)
            and observed_status in allowed_statuses
            and assignment["status"] in {observed_status, "ready"}
            and assignment.get("control_observation", {}).get("agent_status") == observed_status
            and assignment.get("startup_observation")
            and assignment["candidate_id"] == state["candidate_id"]
            and assignment["scope_hash"] == state["scope_hash"]
            and payload.get("continuation_of") == assignment["gate_action_id"]
            and payload.get("role") == assignment["role"]
            and payload.get("policy_hash") == assignment["role_policy"]["policy_hash"]
            and payload.get("brief_hash") == digest(assignment["brief"]),
            "invalid_continuation", "Resume needs the observed interrupted producer on its original candidate, scope and policy")
    if recovering:
        recovery = get(state, "gate_result_recovery", payload["result_recovery_id"])
        validate_result_recovery(state, assignment, recovery)
    previous = state["actions"].get(payload.get("previous_action_id"), {})
    require(previous.get("status") == "confirmed"
            and previous.get("payload", {}).get("assignment_id") == assignment["assignment_id"],
            "reconcile_required", "Reconcile the previous host action before continuing its round")
    return assignment


def resume_subagent_assignment(state, request, now):
    require(subagent_mode(state), "host_mode", "Resume needs a subagent attempt")
    assignment = state["assignments"].get(request["assignment_id"])
    require(assignment is not None and assignment["status"] == "interrupted"
            and isinstance(request.get("reason"), str) and bool(request["reason"].strip()),
            "invalid_continuation", "Observe interruption and provide a bounded continuation reason")
    payload = {"role": assignment["role"], "assignment_id": assignment["assignment_id"],
               "host_kind": "subagent", "agent_name": assignment["agent_name"],
               "policy_hash": assignment["role_policy"]["policy_hash"],
               "brief_hash": digest(assignment["brief"]), "dispatch_id": request["operation_id"],
               "continuation_of": assignment.get("gate_action_id"),
               "previous_action_id": assignment["action_id"], "reason": request["reason"]}
    if assignment.get("result_recovery_id"):
        payload["result_recovery_id"] = assignment["result_recovery_id"]
    validate_role_continuation(state, payload)
    action = prepare_action(state, "send_role", payload, {}, now)
    resumed = assignment | {"status": "ready", "action_id": action["action_id"],
                            "continuation_of": assignment["gate_action_id"],
                            "continuation_reason": request["reason"]}
    save_assignment(state, resumed)
    return {"assignment": resumed, "action": action}


def implementation_actions(state):
    return sorted(a["action_id"] for a in state["actions"].values()
                  if a["operation"] in {"launch_role", "send_role"}
                  and a["payload"].get("role") == "implementation_worker")


def validate_result_recovery(state, assignment, recovery):
    original = recovery["original_result"]
    snapshot_id = assignment.get("workflow_snapshot_id", state["attempt"]["workflow_snapshot_id"])
    snapshot = get(state, "workflow_snapshot", snapshot_id)
    require(recovery["gate_action_id"] == assignment.get("gate_action_id")
            and recovery["assignment_id"] == assignment["assignment_id"]
            and recovery["producer_task_id"] == assignment["task_id"]
            and recovery["candidate_id"] == assignment["candidate_id"] == state["candidate_id"]
            and recovery["scope_hash"] == assignment["scope_hash"] == state["scope_hash"]
            and recovery["workflow_snapshot_id"] == snapshot_id == state["attempt"]["workflow_snapshot_id"]
            and original.get("workflow_hash") == snapshot["workflow_hash"]
            and recovery["implementation_action_ids"] == implementation_actions(state),
            "invalid_result_recovery", "Recovery must retain the original producer, activation and inputs")


def recover_subagent_result(state, request, recovery_input, now):
    require(stage_contract(state), "host_mode", "Result recovery needs a 0.5 subagent attempt")
    require(set(request) == {"work_id", "operation_id", "expected_revision", "assignment_id",
                             "original_result_artifact_hash"},
            "invalid_request", "Recovery accepts only an assignment and original artifact hash")
    assignment = state["assignments"].get(request["assignment_id"])
    require(assignment is not None and assignment in pending_gate_results(state)
            and assignment["status"] == "completed"
            and assignment.get("control_observation", {}).get("agent_status") == "completed"
            and assignment.get("control_observation", {}).get("source_reference")
            and recovery_input is not None,
            "invalid_result_recovery", "Observe the completed pending producer and preserve its malformed original JSON")
    original = recovery_input["original_result"]
    gate_action = state["actions"][assignment["gate_action_id"]]
    require(not any(state["actions"][identity]["expected_revision"] > gate_action["expected_revision"]
                    for identity in implementation_actions(state)),
            "invalid_result_recovery", "Implementation changed after the original gate activation")
    require(all(original.get(k) == v for k, v in {
        "assignment_id": assignment["assignment_id"], "assignment_action_id": assignment["gate_action_id"],
        "producer_task_id": assignment["task_id"], "candidate_id": assignment["candidate_id"],
        "scope_hash": assignment["scope_hash"], "role": assignment["role"],
    }.items()) and original.get("status") in {"PASS", "FAIL", "BLOCKED"},
            "invalid_result_recovery", "Original JSON must identify this producer, activation and verdict")
    if assignment.get("result_recovery_id"):
        recovery = get(state, "gate_result_recovery", assignment["result_recovery_id"])
        previous = state["actions"][assignment["action_id"]]
        require(recovery["artifact_hash"] == request["original_result_artifact_hash"]
                and recovery["original_result"] == original,
                "invalid_result_recovery", "Repeated recovery must reuse its immutable original artifact")
        require(previous["status"] == "confirmed"
                and previous["payload"].get("result_recovery_id") == recovery["recovery_id"],
                "reconcile_required", "Reconcile the prior recovery follow-up before another correction")
    else:
        recovery = {
            "schema_version": 1, "record_type": "gate_result_recovery",
            "recovery_id": "recovery-" + digest([request["operation_id"], recovery_input])[:24],
            "assignment_id": assignment["assignment_id"], "gate_action_id": assignment["gate_action_id"],
            "producer_task_id": assignment["task_id"], "candidate_id": assignment["candidate_id"],
            "scope_hash": assignment["scope_hash"],
            "workflow_snapshot_id": assignment.get("workflow_snapshot_id", state["attempt"]["workflow_snapshot_id"]),
            "implementation_action_ids": implementation_actions(state),
            "artifact_hash": request["original_result_artifact_hash"], **recovery_input,
            "created_at": now.isoformat(),
            "available_evidence_ids": sorted(r["evidence_id"] for r in state["records"].values()
                                             if r.get("record_type") in {"check_evidence", "observation_evidence"}
                                             and r["candidate_id"] == assignment["candidate_id"]),
        }
    validate_result_recovery(state, assignment, recovery)
    require(recovery["available_evidence_ids"], "invalid_result_recovery",
            "Recovery needs existing candidate evidence to serialize")
    save(state, recovery, "gate_result_recovery")
    payload = {"role": assignment["role"], "assignment_id": assignment["assignment_id"],
               "host_kind": "subagent", "agent_name": assignment["agent_name"],
               "policy_hash": assignment["role_policy"]["policy_hash"],
               "brief_hash": digest(assignment["brief"]), "dispatch_id": request["operation_id"],
               "continuation_of": assignment["gate_action_id"],
               "previous_action_id": assignment["action_id"], "result_recovery_id": recovery["recovery_id"]}
    validate_role_continuation(state, payload)
    action = prepare_action(state, "send_role", payload, {}, now)
    resumed = assignment | {"status": "ready", "action_id": action["action_id"],
                            "continuation_of": assignment["gate_action_id"],
                            "result_recovery_id": recovery["recovery_id"]}
    save_assignment(state, resumed)
    return {"assignment": resumed, "action": action, "recovery": recovery}


def validate_corrected_result(state, producer, record):
    recoveries = [r for r in state["records"].values()
                  if r.get("record_type") == "gate_result_recovery"
                  and r["assignment_id"] == record["assignment_id"]
                  and r["gate_action_id"] == record.get("assignment_action_id")]
    if not recoveries:
        return
    recovery = recoveries[0]
    original = recovery["original_result"]
    mutable = {"evidence_ids", "limitations", "producer_result_artifact_hash"}
    require({k: v for k, v in record.items() if k not in mutable}
            == {k: v for k, v in original.items() if k not in mutable}
            and record.get("limitations", [])[:len(original.get("limitations", []))]
            == original.get("limitations", [])
            and set(original.get("evidence_ids", [])) <= set(record["evidence_ids"])
            and set(record["evidence_ids"]) <= set(recovery["available_evidence_ids"]),
            "result_recovery_mismatch", "Corrected result must preserve the original verdict, findings and limitations")
    action = state["actions"].get(producer["action_id"], {})
    require(producer.get("result_recovery_id") == recovery["recovery_id"]
            and state["assignments"].get(producer["assignment_id"], {}).get("action_id") == producer["action_id"]
            and action.get("status") == "confirmed"
            and action.get("payload", {}).get("result_recovery_id") == recovery["recovery_id"],
            "result_recovery_unconfirmed", "Record the original producer's recovery follow-up before importing its correction")


def unresolved_delivery_findings(state):
    if not stage_contract(state):
        return []
    return [f["finding_id"] for f in state["findings"].values()
            if (f["severity"] == "medium" or f["disposition"] == "deferred")
            and f["disposition"] not in {"duplicate", "not_a_defect"}
            and not technically_fixed(state, f, state["candidate_id"])
            and not (f["disposition"] == "deferred" and f.get("followup_reference")
                     and f.get("deferral_rationale") and f.get("followup_observation"))]


def accounting_sources(state):
    segments = [r for r in state["records"].values() if r.get("record_type") == "execution_segment"
                and r["attempt_id"] == state["attempt"]["attempt_id"]]
    segment_ids = {r["segment_id"] for r in segments}
    usage = [r for r in state["records"].values() if r.get("record_type") == "usage"
             and r.get("segment_id") in segment_ids
             and any(a["work_id"] == state["work_id"] and a["weight"] > 0 for a in r["allocations"])]
    tasks = {state["attempt"]["owner_task_id"]} | {
        a["task_id"] for a in state["assignments"].values() if a.get("task_id")}
    return segments, usage, tasks


def complete_accounting_sources(segments, usage, tasks):
    return bool(usage) and (
        {r["segment_id"] for r in segments} <= {r["segment_id"] for r in usage}
        and tasks <= {r["task_id"] for r in segments}
        and all(r.get("source_reference") for r in segments)
    )


def current_accounting(state):
    identity = state.get("accounting_id")
    entry = state["records"].get(f"usage_accounting:{identity}") if identity else None
    if (entry and entry["attempt_id"] == state["attempt"]["attempt_id"]
            and entry["candidate_id"] == state["candidate_id"]):
        if entry["status"] == "complete":
            segments, usage, tasks = accounting_sources(state)
            if (not complete_accounting_sources(segments, usage, tasks)
                    or set(entry["segment_ids"]) != {r["segment_id"] for r in segments}
                    or set(entry["usage_response_ids"]) != {r["response_id"] for r in usage}):
                return None
        return entry
    return None


def require_delivery_accountability(state):
    if stage_contract(state):
        require_gate_handoff(state)
        require(not unresolved_delivery_findings(state), "unresolved_findings",
                "Medium findings require a verified fix or a linked follow-up with rationale")
        require(current_accounting(state) is not None, "accounting_required",
                "Record usage coverage or explicit unknown/unavailable accounting before delivery")


def implementation_required(state):
    return subagent_mode(state) and (
        (state["records"].get("work_continuation:" + str(state.get("continuation_id")), {}).get(
            "entry_phase", state["attempt"].get("entry_phase", "implement")) == "implement")
        or any(a["role"] == "implementation_worker" for a in state["assignments"].values())
    )


def implementation_policy_matches(state, assignment, candidate=None):
    # Missing historical assignment fields resolve through the immutable attempt,
    # never through the current (possibly amended) attempt pointer.
    original = get(state, "attempt", assignment["attempt_id"])
    snapshot_id = assignment.get("workflow_snapshot_id", original["workflow_snapshot_id"])
    return snapshot_id == state["attempt"]["workflow_snapshot_id"] and (
        candidate is None or candidate.get("workflow_snapshot_id", snapshot_id) == snapshot_id
    )


def require_implementation_handoff(state, assignments=None):
    """An observed stop is not an imported producer result."""
    workers = assignments if assignments is not None else state["assignments"].values()
    require(not any(a["role"] == "implementation_worker"
                    and state["actions"].get(a["action_id"], {}).get("operation") == "send_role"
                    and state["actions"][a["action_id"]]["status"] == "confirmed"
                    and not a.get("implementation_result") for a in workers),
            "implementation_result_required", "Import the original worker output before changing scope/policy or reusing its activation")


def implementation_completed(state):
    if not implementation_required(state):
        return True
    return any(a["role"] == "implementation_worker" and a["status"] == "completed"
               and a.get("scope_hash") == state["scope_hash"]
               and implementation_policy_matches(state, a, state["records"].get("candidate:" + str(state["candidate_id"])))
               and (a.get("implementation_result") or {}).get("output_candidate_id") == state["candidate_id"]
               and (a.get("implementation_result") or {}).get("status") == "completed"
               and state["candidate_id"] is not None for a in state["assignments"].values())


def role_next(state, role):
    peers = [a for a in state["assignments"].values()
             if a["role"] == role and a["status"] != "replaced"]
    peer = peers[-1] if peers else None
    result = {"kind": "launch_role", "role": role, "candidate_id": state["candidate_id"],
              "assignments": peers}
    if peer:
        result["assignment_id"] = peer["assignment_id"]
        if peer["status"] in {"pending_startup", "pending_setup", "running", "prepared"}:
            result["kind"] = "wait_roles"
            if peer["status"] == "running" and peer["candidate_id"] != state["candidate_id"]:
                result.update(kind="observe_role", agent_name=peer["agent_name"])
        elif peer["status"] == "ready":
            result["kind"] = "activate_role"
        elif peer["status"] in {"unavailable", "blocked"}:
            result.update(kind="request_user_action", reason="Role needs availability/replacement evidence")
        elif peer.get("task_id"):
            result.update(operation="send_role", reuse_task_id=peer["task_id"])
    return result


def save_assignment(state, record):
    validate_record(record, "assignment")
    if record.get("task_id"):
        require(record["task_id"] != state["attempt"]["owner_task_id"],
                "not_independent", "Coordinator cannot perform delegated product work")
        require(not any(a.get("task_id") == record["task_id"] and a["role"] != record["role"]
                        for a in state["assignments"].values()),
                "not_independent", "Implementation, review and QA require distinct identities")
    state["assignments"][record["assignment_id"]] = deepcopy(record)
    state["records"][f"assignment_event:{digest(record)}"] = deepcopy(record)


def prepare_subagent_assignment(state, request, now):
    from devflow.model_policy import validate_role_policy

    require(subagent_mode(state), "host_mode", "This command needs a subagent attempt")
    policy = request["role_policy"]
    validate_role_policy(policy)
    role, identity = request["role"], request["assignment_id"]
    require(role == policy["role"] and role in {"implementation_worker", "review", "qa"},
            "role_policy_mismatch", "Assignment role differs from resolved policy")
    require(bool(request["owned_paths"]) and bool(request["brief"].strip()),
            "unbounded_assignment", "A role needs explicit ownership and a bounded brief")
    require_gate_handoff(state, role)
    existing = state["assignments"].get(identity)
    if existing:
        if (existing["status"] != "interrupted" or existing["scope_hash"] != state["scope_hash"]
                or not implementation_policy_matches(state, existing)):
            require_implementation_handoff(state, [existing])
        if existing.get("gate_action_id") or role == "implementation_worker":
            require_gate_handoff(state)
        require(existing["role"] == role and existing.get("host_kind") == "subagent",
                "assignment_conflict", "Cannot change an assignment's role or transport")
        require(existing["role_policy"]["policy_hash"] == policy["policy_hash"],
                "role_policy_changed", "Changed role policy requires an observed replacement")
        require(existing["status"] in {"ready", "completed", "blocked", "interrupted"} and existing["task_id"],
                "assignment_state", "Reuse requires a verified available agent")
        require(state["actions"][existing["action_id"]]["status"] == "confirmed",
                "reconcile_required", "Finish the previous host action before preparing a follow-up")
        assignment = deepcopy(existing)
        operation = "send_role"
    else:
        peers = [a for a in state["assignments"].values()
                 if a["role"] == role and a["status"] != "replaced"]
        replaced = state["assignments"].get(request.get("replaces_assignment_id"))
        require(not peers or (len(peers) == 1 and replaced is peers[0]),
                "replacement_required", "Reuse the existing role or record its replacement")
        if replaced:
            require_gate_handoff(state)
            replacement = request.get("replacement_observation")
            if replacement and replacement.get("reason") == "policy_change":
                from devflow.adapters.codex_host import observed_agent_status

                require(replaced["role"] == role and replaced["status"] in {"completed", "ready", "interrupted"}
                        and replaced["role_policy"]["policy_hash"] != policy["policy_hash"]
                        and replacement.get("agent_name") == replaced["agent_name"]
                        and bool(replacement.get("source_reference"))
                        and bool(replacement.get("requested_change_reference"))
                        and observed_agent_status(replacement.get("agent_status")) in {"completed", "interrupted"},
                        "replacement_unobserved", "Policy replacement needs an explicit change and a stopped agent")
                observed_status = observed_agent_status(replacement["agent_status"])
                replacement = {key: replacement[key] for key in (
                    "reason", "agent_name", "source_reference", "requested_change_reference"
                )} | {"agent_status": observed_status,
                      "status_evidence_kind": "completed_object" if observed_status == "completed" else "string",
                      "previous_policy_hash": replaced["role_policy"]["policy_hash"],
                      "replacement_policy_hash": policy["policy_hash"]}
            else:
                require(replaced["role"] == role and replaced["status"] == "unavailable"
                        and replaced.get("unavailable_observation")
                        and state["actions"][replaced["action_id"]]["status"] == "confirmed",
                        "replacement_unobserved", "Replacement needs observed unavailability")
                replacement = deepcopy(replaced["unavailable_observation"])
        task_name = "df_" + digest([state["attempt"]["owner_task_id"],
                                   state["attempt"]["attempt_id"], identity])[:24]
        coordinator = request.get("coordinator_agent_name", "/root")
        assignment = {
            "schema_version": 1, "record_type": "assignment", "assignment_id": identity,
            "attempt_id": state["attempt"]["attempt_id"], "role": role,
            "owner_task_id": state["attempt"]["owner_task_id"], "task_id": None, "client_id": None,
            "host_kind": "subagent", "coordinator_agent_name": coordinator,
            "task_name": task_name, "agent_name": coordinator + "/" + task_name,
            "role_policy": deepcopy(policy), "startup_observation": None,
        }
        if replaced:
            assignment.update(replaces_assignment_id=replaced["assignment_id"],
                              replacement_observation=replacement)
            save_assignment(state, replaced | {"status": "replaced",
                                               "replaced_by_assignment_id": identity})
        operation = "launch_role"
    assignment.update(candidate_id=state["candidate_id"], scope_hash=state["scope_hash"],
                      workflow_snapshot_id=state["attempt"]["workflow_snapshot_id"],
                      owned_paths=request["owned_paths"],
                      workspace_reference=request.get("workspace_reference"), brief=request["brief"],
                      status="ready" if existing else "prepared")
    assignment.pop("implementation_result", None)
    assignment.pop("captured_candidate_id", None)
    assignment.pop("gate_action_id", None)
    assignment.pop("continuation_of", None)
    assignment.pop("continuation_reason", None)
    assignment.pop("result_recovery_id", None)
    action = prepare_action(state, operation, {
        "role": role, "assignment_id": identity, "host_kind": "subagent",
        "agent_name": assignment["agent_name"], "policy_hash": policy["policy_hash"],
        "brief_hash": digest(assignment["brief"]), "dispatch_id": request["operation_id"],
    }, {}, now)
    assignment["action_id"] = action["action_id"]
    save_assignment(state, assignment)
    return {"assignment": assignment, "action": action}


def host_receipt(state, assignment, status, external_id, observation, now):
    action = state["actions"][assignment["action_id"]]
    record = {"schema_version": 1, "record_type": "action_receipt",
              **{key: action[key] for key in (
                  "action_id", "attempt_id", "operation", "payload_hash", "expected_revision")},
              "status": status, "external_id": external_id,
              "observations": ["Observed supported host result"], "recorded_at": now.isoformat()}
    return record_receipt(state, {"record": record, "observation": observation})


def scope_hash(contract):
    return digest({k: v for k, v in contract.items() if k not in {"title", "scope_revision"}})


def input_signature(candidate, evidence):
    return digest(
        {
            "scope_hash": candidate["scope_hash"],
            "tree_sha": candidate["tree_sha"],
            "dependency_hash": candidate["dependency_hash"],
            "environment_hash": candidate["environment_hash"],
            "scenario_ids": evidence.get("scenario_ids", []),
            **{
                k: evidence[k]
                for k in (
                    "recipe_id",
                    "recipe_version",
                    "acceptance_ids",
                    "argv",
                    "cwd",
                    "environment_profile",
                )
            },
        }
    )


def blank(work_id):
    return {
        "work_id": work_id,
        "revision": 0,
        "lifecycle": "backlog",
        "phase": None,
        "history": [],
        "phase_history": [],
        "scope_hash": None,
        "contract": None,
        "authority": None,
        "attempt": None,
        "candidate_id": None,
        "gate_ids": {},
        "check_ids": {},
        "records": {},
        "assignments": {},
        "findings": {},
        "fix_observations": {},
        "actions": {},
        "receipts": {},
        "receipt_observations": {},
        "blocker": None,
        "delivery_id": None,
    }


def save(state, record, kind=None):
    validate_record(record, kind)
    field = ID_FIELDS[record["record_type"]]
    key = f"{record['record_type']}:{record[field]}"
    old = state["records"].get(key)
    require(
        old is None or old == record, "immutable_record", f"Record identity already exists: {key}"
    )
    state["records"][key] = deepcopy(record)
    return record


def get(state, kind, identity):
    result = state["records"].get(f"{kind}:{identity}")
    require(result is not None, "unknown_record", f"Unknown {kind}: {identity}")
    return result


def authority(state, now, operation=None):
    record = state["authority"]
    require(
        record is not None and not record["revoked"],
        "missing_authority",
        "Active authority required",
    )
    require(
        record["work_id"] == state["work_id"] and record["scope_hash"] == state["scope_hash"],
        "authority_scope",
        "Authority does not cover this work scope",
    )
    expires = record["expires_at"]
    require(
        expires is None or datetime.fromisoformat(expires.replace("Z", "+00:00")) > now,
        "expired_authority",
        "Authority has expired",
    )
    if operation:
        require(
            operation in record["allowed_operations"],
            "missing_authority",
            f"Authority does not allow {operation}",
        )
    return record


def active(state):
    require(state["lifecycle"] == "active", "invalid_state", "An active attempt is required")
    require(state["attempt"] is not None, "invalid_state", "Attempt is missing")
    return state["attempt"]


def unblocked(state):
    require(
        state["blocker"] is None, "blocked_work", "Resolve the work blocker before new execution"
    )


def current_candidate(state, identity=None):
    require(
        state["candidate_id"] is not None, "missing_candidate", "Record a clean candidate first"
    )
    identity = identity or state["candidate_id"]
    require(
        identity == state["candidate_id"],
        "stale_candidate",
        "Record must name the current candidate",
    )
    candidate = get(state, "candidate", identity)
    require(
        candidate["scope_hash"] == state["scope_hash"], "stale_scope", "Candidate scope has changed"
    )
    return candidate


def independent_assignment(state, assignment_id, producer, candidate_id, role=None):
    assignment = state["assignments"].get(assignment_id)
    require(assignment is not None, "unknown_assignment", "Role assignment is not registered")
    if subagent_mode(state):
        require(assignment.get("host_kind") == "subagent" and assignment.get("startup_observation"),
                "startup_unverified", "Independent result needs observed subagent startup")
    require(
        assignment["role"] in {"review", "qa"},
        "not_independent",
        "Independent review or QA required",
    )
    require(
        assignment["status"] in {"running", "completed"} and assignment["task_id"] is not None,
        "pending_assignment",
        "Role task is not ready",
    )
    require(
        assignment["task_id"] == producer and producer != state["attempt"]["owner_task_id"],
        "not_independent",
        "Result must come from the registered independent task",
    )
    require(
        assignment["candidate_id"] == candidate_id,
        "stale_candidate",
        "Assignment candidate mismatch",
    )
    if role:
        require(assignment["role"] == role, "wrong_role", "Gate role does not match assignment")
    return assignment


def evidence_records(state, ids, candidate_id, producer=None, passing=True):
    result = []
    for identity in ids:
        record = state["records"].get(f"check_evidence:{identity}") or state["records"].get(
            f"observation_evidence:{identity}"
        )
        require(record is not None, "unknown_evidence", f"Unknown evidence: {identity}")
        require(
            record["candidate_id"] == candidate_id, "stale_evidence", "Evidence candidate mismatch"
        )
        if record["record_type"] == "check_evidence":
            if passing:
                require(
                    state["check_ids"].get(record["recipe_id"]) == record["evidence_id"],
                    "stale_evidence",
                    "A later check result superseded this evidence",
                )
                require(
                    record["execution_status"] == "PASS",
                    "nonpassing_evidence",
                    "Required evidence did not pass",
                )
        elif producer:
            require(
                record["producer_task_id"] == producer,
                "wrong_producer",
                "Observation belongs to another task",
            )
        result.append(record)
    return result


def technically_fixed(state, finding, candidate_id):
    return finding["disposition"] == "verified_fixed" and any(
        get(state, "fix_verification", identity)["candidate_id"] == candidate_id
        and get(state, "fix_verification", identity)["result"] == "verified"
        for identity in finding["fix_verification_ids"]
    )


def blocking_findings(state, candidate_id):
    return [
        f["finding_id"]
        for f in state["findings"].values()
        if f["severity"] in {"blocker", "high"}
        and f["disposition"] not in {"duplicate", "not_a_defect"}
        and not technically_fixed(state, f, candidate_id)
    ]


def implementation_action_id(state):
    workers = [a for a in state["assignments"].values()
               if a["role"] == "implementation_worker" and a["status"] != "replaced"]
    return workers[-1]["action_id"] if workers else None


def role_gate(state, role):
    identity = state.get("gate_ids", {}).get(role)
    if identity:
        return get(state, "gate_result", identity)
    if stage_contract(state):
        # Finding imports invalidate PASS proof, but cannot erase an unaddressed failure.
        gates = [r for r in state["records"].values() if r.get("record_type") == "gate_result"
                 and r["role"] == role and r["candidate_id"] == state["candidate_id"]
                 and not state["records"].get(f"gate_ingestion:{r['gate_id']}", {}).get("historical")]
        latest = max(gates, key=lambda r: state["records"].get(
            f"gate_ingestion:{r['gate_id']}", {}).get("admitted_revision", -1), default=None)
        if latest and latest["status"] != "PASS":
            return latest
    return None


def invalidated_gate_repairs(state):
    latest = {}
    for item in state["records"].values():
        if item.get("record_type") != "gate_ingestion":
            continue
        gate = get(state, "gate_result", item["gate_id"])
        if gate["candidate_id"] != state["candidate_id"]:
            continue
        previous = latest.get(gate["role"])
        if previous is None or item["admitted_revision"] > previous["admitted_revision"]:
            latest[gate["role"]] = item
    return [item["gate_id"] for item in latest.values()
            if {"evidence", "findings"}.intersection(item["mismatches"])
            and item["implementation_action_id"] == implementation_action_id(state)]


def gate_needs_repair(state, gate):
    if not stage_contract(state):
        return state["phase"] == "implement"
    ingestion = state["records"].get(f"gate_ingestion:{gate['gate_id']}", {})
    return ingestion.get("implementation_action_id") == implementation_action_id(state)


def required_roles(state):
    tier = state["contract"]["risk"]["tier"]
    return [] if tier == 0 else ["review"] if tier == 1 else ["review", "qa"]


def valid_gates(state):
    candidate_id = state["candidate_id"]
    latest = {
        role: get(state, "gate_result", identity)
        for role, identity in state.get("gate_ids", {}).items()
    }
    return {
        role: gate
        for role, gate in latest.items()
        if gate["status"] == "PASS" and gate["candidate_id"] == candidate_id
    }


def missing_checks(state):
    checks = [
        get(state, "check_evidence", identity) for identity in state.get("check_ids", {}).values()
    ]
    recipes = {
        r["recipe_id"]
        for r in checks
        if r["candidate_id"] == state["candidate_id"] and r["execution_status"] == "PASS"
    }
    return [r for r in state["contract"]["verification"]["recipes"] if r not in recipes]


def current_passing_evidence(state):
    return [
        e
        for e in state["records"].values()
        if e["record_type"] in {"check_evidence", "observation_evidence"}
        and e["candidate_id"] == state["candidate_id"]
        and (
            e["record_type"] == "observation_evidence"
            or (
                e["execution_status"] == "PASS"
                and state["check_ids"].get(e["recipe_id"]) == e["evidence_id"]
            )
        )
    ]


def missing_scenarios(state, evidence=None):
    # Acceptance IDs are a compatibility alias only when the required scenario
    # literally names that acceptance ID. Named scenarios need explicit proof.
    evidence = current_passing_evidence(state) if evidence is None else evidence
    covered = set().union(
        *(set(e.get("scenario_ids", [])) | set(e["acceptance_ids"]) for e in evidence)
    )
    return sorted(set(state["contract"]["verification"]["scenarios"]) - covered)


def prepare_action(
    state, operation, payload, expected_remote_state, now, action_id=None, *, terminal=False
):
    require(operation in ACTION_OPERATIONS, "unknown_action", f"Unsupported action {operation}")
    active(state)
    authority(state, now, PERMISSIONS[operation])
    payload = deepcopy(payload)
    payload.setdefault("scope_hash", state["scope_hash"])
    payload.setdefault("candidate_id", state["candidate_id"])
    require(payload["scope_hash"] == state["scope_hash"], "stale_scope", "Action scope mismatch")
    if payload["candidate_id"] is not None:
        current_candidate(state, payload["candidate_id"])
    if operation in {"launch_role", "send_role"}:
        if payload.get("continuation_of"):
            validate_role_continuation(state, payload)
        else:
            require_gate_handoff(state, payload.get("role"))
        require(
            payload.get("role") in {"review", "qa", "implementation_worker"},
            "wrong_role",
            "Role action must name a supported peer role",
        )
        if payload["role"] in {"review", "qa"}:
            current_candidate(state, payload["candidate_id"])
            require(implementation_completed(state), "implementation_incomplete",
                    "Independent roles require the delegated implementation result")
    if operation == "run_check":
        require(implementation_completed(state), "implementation_incomplete",
                "Recorded checks require the delegated implementation result")
    validate_action_target(state, operation, payload, expected_remote_state, terminal=terminal)
    fingerprint = digest(
        {
            "operation": operation,
            "payload": payload,
            "expected_remote_state": expected_remote_state,
            "terminal_delivery": terminal,
        }
    )
    action_id = action_id or f"action-{fingerprint[:24]}"
    for existing_action in state["actions"].values():
        if (
            existing_action["operation"] == operation
            and existing_action.get("terminal_delivery", False) == terminal
            and existing_action["payload_hash"] == digest(payload)
            and existing_action["expected_remote_state"] == expected_remote_state
            and existing_action["status"] != "invalidated"
        ):
            return existing_action
    existing = state["actions"].get(action_id)
    if existing:
        require(
            existing.get("terminal_delivery", False) == terminal,
            "action_conflict",
            "Action delivery purpose changed",
        )
        require(
            existing["payload_hash"] == digest(payload),
            "action_conflict",
            "Action ID reused with different payload",
        )
        require(
            existing["expected_remote_state"] == expected_remote_state,
            "action_conflict",
            "Expected remote state changed",
        )
        return existing
    action = {
        "action_id": action_id,
        "terminal_delivery": terminal,
        "attempt_id": state["attempt"]["attempt_id"],
        "operation": operation,
        "payload": payload,
        "payload_hash": digest(payload),
        "expected_remote_state": deepcopy(expected_remote_state),
        "expected_revision": state["revision"] + 1,
        "status": "prepared",
        "receipts": [],
    }
    state["actions"][action_id] = action
    return action


def next_actions(state):
    if state["lifecycle"] == "done":
        return [
            {
                "kind": "done",
                "delivery_id": state["delivery_id"],
                "pending_publication": [
                    f["finding_id"]
                    for f in state["findings"].values()
                    if f["publication"] == "pending_pr"
                ],
            }
        ]
    if state["lifecycle"] == "backlog":
        return [{"kind": "prepare_scope"}]
    if state["lifecycle"] == "canceled":
        return [{"kind": "request_user_action", "reason": "Work was canceled"}]
    pending = [
        a
        for a in state["actions"].values()
        if a["status"] in {"prepared", "ambiguous", "pending_setup", "dispatched"}
    ]
    if state["blocker"] is not None:
        return [
            {"kind": "reconcile_action", "action": action}
            for action in pending if action["status"] != "prepared"
        ] + [{"kind": "request_user_action", "blocker": state["blocker"]}]
    if pending:
        return [
            {
                "kind": "reconcile_action"
                if a["status"] != "prepared"
                else {
                    "publish_finding": "publish_findings",
                    "resolve_thread": "close_fixed_threads",
                    "local_delivery": "deliver",
                    "merge": "deliver",
                    "release": "deliver",
                    "publish_pr": "publish_candidate",
                    "send_role": "wait_roles",
                    "reply_finding": "close_fixed_threads",
                    "sync_projection": "reconcile_action",
                    "publish_status": "deliver",
                }.get(a["operation"], a["operation"]),
                "action": a,
            }
            for a in pending
        ]
    if state["lifecycle"] == "ready":
        return [{"kind": "prepare_workspace", "reason": "Start the authorized attempt"}]
    waiting = pending_gate_results(state)
    if waiting:
        return [{"kind": "resume_role" if a["status"] == "interrupted" else "import_gate_result", "role": a["role"],
                 "assignment_id": a["assignment_id"], "assignment_action_id": a["gate_action_id"],
                 "candidate_id": a["candidate_id"],
                 **({"recovery_command": "devflow host recover-result",
                     "recovery_reason": "Only if original JSON was rejected for evidence serialization"}
                    if a.get("control_observation", {}).get("agent_status") == "completed" else {})}
                for a in waiting]
    if not state["candidate_id"]:
        if implementation_required(state):
            return [role_next(state, "implementation_worker")]
        return [{"kind": "capture_candidate" if subagent_mode(state) else "implement"}]
    if implementation_required(state):
        workers = [a for a in state["assignments"].values()
                   if a["role"] == "implementation_worker" and a["status"] != "replaced"]
        worker = workers[-1] if workers else None
        if not worker or not implementation_completed(state):
            return [role_next(state, "implementation_worker")]
    invalidated = invalidated_gate_repairs(state) if stage_contract(state) else []
    if invalidated:
        if implementation_required(state):
            return [role_next(state, "implementation_worker") | {"gate_ids": invalidated,
                    "reason": "Later evidence or findings invalidated the imported producer PASS"}]
        return [{"kind": "request_user_action", "gate_ids": invalidated,
                 "reason": "Review-only proof was invalidated after the producer finished"}]
    missing = missing_checks(state)
    if missing:
        return [
            {"kind": "run_check", "recipe_id": r, "candidate_id": state["candidate_id"]}
            for r in missing
        ]
    blockers = blocking_findings(state, state["candidate_id"])
    if blockers:
        if implementation_required(state):
            needs_repair = [identity for identity in blockers if (
                state["findings"][identity]["disposition"] != "fix_pending"
                or state["fix_observations"].get(identity, {}).get("candidate_id") != state["candidate_id"]
            )]
            if needs_repair:
                return [role_next(state, "implementation_worker") | {"finding_ids": needs_repair}]
            if not required_roles(state):
                return [role_next(state, "review") | {"finding_ids": blockers}]
        else:
            return [{"kind": "repair_findings", "finding_ids": blockers}]
    gates = valid_gates(state)
    roles = [role for role in required_roles(state) if role not in gates]
    if roles:
        result = []
        for role in roles:
            if subagent_mode(state):
                gate = role_gate(state, role)
                if gate and gate["status"] == "FAIL" and gate_needs_repair(state, gate):
                    result.append(role_next(state, "implementation_worker") if (
                        implementation_required(state)
                    ) else {"kind": "request_user_action", "reason": "Review-only gate failed"})
                elif gate and gate["status"] == "BLOCKED":
                    result.append({"kind": "request_user_action", "reason": "Role gate is blocked"})
                else:
                    result.append(role_next(state, role))
                continue
            assignments = [
                a
                for a in state["assignments"].values()
                if a["role"] == role and a["candidate_id"] == state["candidate_id"]
            ]
            live = [a for a in assignments if a["status"] in {"running", "pending_setup"}]
            if live:
                result.append(
                    {
                        "kind": "wait_roles",
                        "role": role,
                        "candidate_id": state["candidate_id"],
                        "assignments": live,
                    }
                )
                continue
            gate_id = state.get("gate_ids", {}).get(role)
            gate = get(state, "gate_result", gate_id) if gate_id else None
            peers = assignments or [a for a in state["assignments"].values() if a["role"] == role]
            peer = (
                state["assignments"].get(gate["assignment_id"])
                if gate
                else next((a for a in peers if a["task_id"]), None)
            )
            rerun = {
                "kind": "launch_role",
                "role": role,
                "candidate_id": state["candidate_id"],
                "assignments": peers,
            }
            if peer and peer["task_id"]:
                rerun.update(
                    operation="send_role",
                    reuse_task_id=peer["task_id"],
                    assignment_id=peer["assignment_id"],
                )
            if gate and gate["status"] != "PASS" and state["phase"] == "implement":
                result.append(
                    {
                        "kind": "request_user_action"
                        if gate["status"] == "BLOCKED"
                        else "implement",
                        "role": role,
                        "gate_id": gate_id,
                        "limitations": gate["limitations"],
                        "reason": "Address the completed gate result before requesting its rerun",
                        "rerun": rerun,
                    }
                )
            elif peer and peer["status"] == "blocked":
                result.append(
                    {
                        "kind": "request_user_action",
                        "role": role,
                        "reason": "The assigned role task is blocked",
                        "rerun": rerun,
                    }
                )
            else:
                result.append(rerun)
        return result
    if state["contract"]["endpoint"]["kind"] != "local":
        unpublished = [
            f["finding_id"] for f in state["findings"].values() if f["publication"] != "published"
        ]
        if unpublished:
            return [{"kind": "publish_findings", "finding_ids": unpublished}]
        unclosed = [
            f["finding_id"]
            for f in state["findings"].values()
            if technically_fixed(state, f, state["candidate_id"]) and f["closure"] != "resolved"
        ]
        if unclosed:
            return [{"kind": "close_fixed_threads", "finding_ids": unclosed}]
    missing = missing_scenarios(state)
    if missing:
        return [
            {
                "kind": "run_check",
                "scenario_ids": missing,
                "candidate_id": state["candidate_id"],
                "reason": "Required scenarios lack execution evidence",
            }
        ]
    unresolved = unresolved_delivery_findings(state)
    if unresolved:
        return [{"kind": "resolve_findings", "finding_ids": unresolved,
                 "reason": "Verify a fix or defer to linked follow-up work with rationale"}]
    if stage_contract(state) and not current_accounting(state):
        return [{"kind": "record_accounting", "candidate_id": state["candidate_id"]}]
    return [{"kind": "deliver", "candidate_id": state["candidate_id"]}]


def record_evidence(state, record):
    historical = record.get("candidate_id") != state["candidate_id"]
    require(historical or implementation_completed(state), "implementation_incomplete",
            "Recorded verification needs the delegated implementation result")
    if historical and not subagent_mode(state):
        current_candidate(state, record.get("candidate_id"))
    validate_record(record)
    require(
        record["record_type"] in {"check_evidence", "observation_evidence"},
        "invalid_record",
        "Expected evidence record",
    )
    candidate = (get(state, "candidate", record["candidate_id"]) if historical
                 else current_candidate(state, record["candidate_id"]))
    existing = state["records"].get(f"{record['record_type']}:{record['evidence_id']}")
    if existing:
        require(existing == record, "immutable_record", "Evidence identity already exists")
        return
    contract = state["contract"]
    if historical:
        contracts = [r for r in state["records"].values() if r.get("record_type") == "work_contract"
                     and scope_hash(r) == candidate["scope_hash"]]
        require(contracts, "stale_scope", "Historical evidence needs its admitted scope")
        contract = contracts[-1]
    acceptance_ids = {a["id"] for a in contract["acceptance"]}
    require(
        set(record["acceptance_ids"]) <= acceptance_ids,
        "unknown_acceptance",
        "Evidence names unknown acceptance",
    )
    if record["record_type"] == "check_evidence":
        require(
            record["input_signature"] == input_signature(candidate, record),
            "stale_signature",
            "Check inputs do not match candidate",
        )
        require(
            datetime.fromisoformat(record["ended_at"].replace("Z", "+00:00"))
            >= datetime.fromisoformat(record["started_at"].replace("Z", "+00:00")),
            "invalid_time",
            "Check ended before it started",
        )
        if record["execution_status"] == "PASS":
            require(
                record["process_status"] == "success"
                and (record["executed_assertions"] > 0 or record["observations"]),
                "unexecuted_check",
                "PASS requires successful execution and assertions or observations",
            )
    save(state, record)
    if record["record_type"] == "check_evidence" and not historical:
        previous_id = state["check_ids"].get(record["recipe_id"])
        previous = get(state, "check_evidence", previous_id) if previous_id else None
        state["check_ids"][record["recipe_id"]] = record["evidence_id"]
        if record["execution_status"] != "PASS" or (
            previous and previous["input_signature"] != record["input_signature"]
        ):
            state["gate_ids"] = {}


def record_fix(state, record, *, historical=False):
    if historical:
        validate_record(record, "fix_verification")
        require(record["finding_id"] in state["findings"], "unknown_finding", "Unknown historical finding")
        get(state, "candidate", record["candidate_id"])
        evidence_records(state, record["evidence_ids"], record["candidate_id"],
                         record["producer_task_id"], passing=False)
        save(state, record)
        return
    validate_record(record, "fix_verification")
    existing = state["records"].get(f"fix_verification:{record['verification_id']}")
    if existing:
        require(existing == record, "immutable_record", "Fix verification identity already exists")
        return
    current_candidate(state, record["candidate_id"])
    independent_assignment(
        state, record["assignment_id"], record["producer_task_id"], record["candidate_id"]
    )
    finding = state["findings"].get(record["finding_id"])
    require(finding is not None, "unknown_finding", "Fix references unknown finding")
    require(
        finding["fix_reference"] is not None,
        "missing_fix",
        "Record the fixing commit/reference first",
    )
    require(
        state["fix_observations"].get(record["finding_id"], {}).get("candidate_id")
        == record["candidate_id"],
        "stale_fix",
        "Fix containment observation is for a different candidate",
    )
    evidence_records(
        state,
        record["evidence_ids"],
        record["candidate_id"],
        record["producer_task_id"],
        record["result"] == "verified",
    )
    save(state, record)
    finding["fix_verification_ids"] = list(
        dict.fromkeys(finding["fix_verification_ids"] + [record["verification_id"]])
    )
    if record["result"] == "verified":
        finding["disposition"] = "verified_fixed"
        finding["fix_evidence_ids"] = record["evidence_ids"]
    else:
        finding["disposition"] = "fix_pending"


def validate_gate_readiness(state, record, evidence):
    """Readiness may change after a producer finishes; its original result cannot."""
    evidence_records(state, record["evidence_ids"], record["candidate_id"],
                     record["producer_task_id"], passing=True)
    require(not record["blocking_finding_ids"] and not blocking_findings(state, record["candidate_id"]),
            "blocking_findings", "High/Blocker findings require independent technical fix verification first")
    require(not missing_checks(state), "missing_checks", "Required command evidence is incomplete")
    covered = set().union(*(set(e["acceptance_ids"]) for e in evidence))
    require({a["id"] for a in state["contract"]["acceptance"]} <= covered,
            "missing_acceptance", "Gate evidence must cover every acceptance criterion")
    if record["role"] == "qa":
        require(not missing_scenarios(state, evidence), "missing_scenarios",
                "QA evidence must cover every required scenario")
        require(any(e["record_type"] == "observation_evidence" or e.get("executed_assertions", 0) > 0
                    for e in evidence), "missing_product_proof",
                "QA needs executed assertions or independent product observations")


def record_gate(state, record):
    validate_record(record, "gate_result")
    existing = state["records"].get(f"gate_result:{record['gate_id']}")
    if existing:
        require(existing == record, "immutable_record", "Gate identity already exists")
        return
    if not subagent_mode(state):
        current_candidate(state, record["candidate_id"])
        independent_assignment(state, record["assignment_id"], record["producer_task_id"],
                               record["candidate_id"], record["role"])
    candidate = get(state, "candidate", record["candidate_id"])
    assignment = state["assignments"].get(record["assignment_id"])
    candidates = [r for r in state["records"].values() if r.get("record_type") == "assignment"
                  and r["assignment_id"] == record["assignment_id"]]
    if assignment:
        candidates.append(assignment)
    matches = [a for a in candidates if a["candidate_id"] == record["candidate_id"]
               and a["task_id"] == record["producer_task_id"] and a["role"] == record["role"]
               and (not record.get("assignment_action_id")
                    or a.get("gate_action_id", a["action_id"]) == record["assignment_action_id"])
               and a["status"] in {"running", "completed", "interrupted", "unavailable", "replaced"}]
    require(matches, "not_independent", "Gate needs its original activated producer assignment")
    producer = matches[-1]
    validate_corrected_result(state, producer, record)
    require(producer["task_id"] != state["attempt"]["owner_task_id"],
            "not_independent", "Coordinator cannot supply an independent gate")
    if subagent_mode(state):
        require(producer.get("startup_observation"), "startup_unverified",
                "Independent result needs observed subagent startup")
    if producer.get("gate_action_id"):
        require(bool(record.get("producer_result_artifact_hash")), "producer_result_required",
                "Import the original producer output artifact with its structured gate result")
        require(not any(r.get("record_type") == "gate_result"
                        and r["assignment_id"] == record["assignment_id"]
                        and r.get("assignment_action_id") == producer["gate_action_id"]
                        for r in state["records"].values()),
                "gate_result_conflict", "An activation already has its immutable producer result")
        require(record.get("assignment_action_id") == producer["gate_action_id"],
                "gate_activation_mismatch", "Gate must identify its original activation action")
    attempt = get(state, "attempt", candidate["attempt_id"])
    snapshot_id = producer.get("workflow_snapshot_id", candidate.get(
        "workflow_snapshot_id", attempt["workflow_snapshot_id"]))
    snapshot = get(state, "workflow_snapshot", snapshot_id)
    require(candidate.get("workflow_snapshot_id", snapshot_id) == snapshot_id,
            "stale_policy", "Candidate and producer workflow snapshots differ")
    require(record["scope_hash"] == candidate["scope_hash"] == producer.get("scope_hash", candidate["scope_hash"])
            and record["workflow_hash"] == snapshot["workflow_hash"],
            "stale_policy", "Gate must retain its original scope and workflow policy")
    mismatches = []
    if record["candidate_id"] != state["candidate_id"]:
        mismatches.append("candidate")
    if record["scope_hash"] != state["scope_hash"]:
        mismatches.append("scope")
    if not implementation_completed(state):
        mismatches.append("implementation")
    if (not assignment or producer["action_id"] != assignment["action_id"]
            or assignment["status"] == "replaced"):
        mismatches.append("activation")
    historical = bool(mismatches)
    if historical:
        require(subagent_mode(state), "stale_candidate", "Historical import needs a subagent producer")
    else:
        current_candidate(state, record["candidate_id"])
    evidence = evidence_records(
        state,
        record["evidence_ids"],
        record["candidate_id"],
        record["producer_task_id"],
        record["status"] == "PASS" and not historical and not producer.get("gate_action_id"),
    )
    for identity in record["finding_ids"] + record["blocking_finding_ids"]:
        require(identity in state["findings"], "unknown_finding", "Gate references unknown finding")
    for identity in record["fix_verification_ids"]:
        fix = get(state, "fix_verification", identity)
        require(
            fix["candidate_id"] == record["candidate_id"]
            and fix["assignment_id"] == record["assignment_id"],
            "stale_fix",
            "Gate fix verification does not match assignment/candidate",
        )
    proof_invalidated = False
    if record["status"] == "PASS" and not historical:
        try:
            validate_gate_readiness(state, record, evidence)
        except WorkflowError as exc:
            readiness_errors = {"stale_evidence", "nonpassing_evidence", "blocking_findings",
                                "missing_checks", "missing_acceptance", "missing_scenarios",
                                "missing_product_proof"}
            if not producer.get("gate_action_id") or exc.code not in readiness_errors:
                raise
            # A producer-authenticated result is evidence even after newer facts
            # invalidate its PASS. Persist it before authorizing any repair.
            mismatches.append("findings" if exc.code == "blocking_findings" else "evidence")
            historical = proof_invalidated = True
    save(state, record)
    save(state, {
        "schema_version": 1, "record_type": "gate_ingestion",
        "gate_id": record["gate_id"], "historical": historical, "mismatches": mismatches,
        "current_candidate_id": state["candidate_id"],
        "producer_assignment": deepcopy(producer),
        "implementation_action_id": implementation_action_id(state),
        "admitted_revision": state["revision"] + 1,
    })
    if not historical or proof_invalidated:
        if assignment["status"] in {"running", "completed"}:
            save_assignment(state, assignment | {"status": "completed"})
        if proof_invalidated:
            state["gate_ids"].pop(record["role"], None)
            state["phase"] = "implement"
        else:
            state["gate_ids"][record["role"]] = record["gate_id"]
            state["phase"] = "verify" if record["status"] == "PASS" else "implement"


def verify_delivery(state, record, observation):
    require_delivery_accountability(state)
    require(implementation_completed(state), "implementation_incomplete",
            "Delivery readback requires the current delegated implementation result")
    validate_record(record, "delivery")
    candidate = current_candidate(state, record["candidate_id"])
    require(
        record["work_id"] == state["work_id"]
        and record["attempt_id"] == state["attempt"]["attempt_id"],
        "delivery_identity",
        "Delivery work or attempt mismatch",
    )
    require(
        record["endpoint"] == state["contract"]["endpoint"]
        and record["authority_id"] == state["authority"]["authority_id"],
        "delivery_authority",
        "Delivery endpoint or authority mismatch",
    )
    action = state["actions"].get(record["action_id"])
    require(
        action is not None and action["status"] == "confirmed",
        "missing_receipt",
        "Delivery requires a confirmed prepared action",
    )
    require(
        record["receipt_id"] in action["receipts"],
        "missing_receipt",
        "Delivery receipt does not belong to action",
    )
    require(
        action["payload"].get("candidate_id") == candidate["candidate_id"]
        and action["payload"].get("scope_hash") == state["scope_hash"]
        and action["payload"].get("endpoint") == record["endpoint"],
        "stale_delivery",
        "Prepared delivery binding has changed",
    )
    require(
        action.get("terminal_delivery") is True,
        "unprepared_delivery",
        "Terminal delivery requires its evaluated delivery intent",
    )
    validate_action_target(
        state,
        action["operation"],
        action["payload"],
        action["expected_remote_state"],
        terminal=True,
    )
    expected_operation = {
        "local": "local_delivery",
        "pr": "publish_pr",
        "merge": "merge",
        "release": "release",
    }[record["endpoint"]["kind"]]
    require(
        action["operation"] == expected_operation,
        "wrong_receipt",
        "Receipt is not a delivery action",
    )
    expected_observation = {
        "action_id": action["action_id"],
        "payload_hash": action["payload_hash"],
        "candidate_id": candidate["candidate_id"],
        "head_sha": candidate["head_sha"],
        "tree_sha": candidate["tree_sha"],
        "verified": record["status"] == "verified",
        "independent_readback": True,
    }
    require(
        all(observation.get(k) == v for k, v in expected_observation.items()),
        "unverified_delivery",
        "Independent adapter observation must match the prepared candidate and endpoint",
    )
    require(
        action.get("observation") == observation,
        "unbound_observation",
        "Delivery must use the observation persisted with its receipt",
    )
    if record["status"] != "verified":
        save(state, record)
        state["last_delivery_id"] = record["delivery_id"]
        state["blocker"] = {
            "code": record["status"],
            "reason": "Delivery is queued or its observed result is unverified",
            "next_action": "Reconcile the actual endpoint before reporting completion",
        }
        return
    require(
        readback_matches(record["endpoint"], observation),
        "unverified_endpoint",
        "Actual endpoint target readback does not match the accepted target",
    )
    gates = valid_gates(state)
    expected_gates = {gates[role]["gate_id"] for role in required_roles(state) if role in gates}
    require(
        set(record["gate_ids"]) == expected_gates
        and len(expected_gates) == len(required_roles(state)),
        "missing_gates",
        "Delivery gate set is incomplete or stale",
    )
    require(
        set(action["payload"].get("gate_ids", [])) == expected_gates,
        "stale_delivery",
        "Gate set changed since delivery preparation",
    )
    require(
        not missing_checks(state) and not blocking_findings(state, candidate["candidate_id"]),
        "incomplete_proof",
        "Delivery proof is incomplete",
    )
    evidence = current_passing_evidence(state)
    covered = set().union(*(set(e["acceptance_ids"]) for e in evidence))
    require(
        not missing_scenarios(state, evidence),
        "missing_scenarios",
        "Delivery lacks required scenario execution evidence",
    )
    require(
        {a["id"] for a in state["contract"]["acceptance"]} <= covered,
        "missing_acceptance",
        "Delivery lacks acceptance evidence",
    )
    if record["endpoint"]["kind"] != "local":
        require(
            all(f["publication"] == "published" for f in state["findings"].values()),
            "publication_due",
            "All confirmed findings must be published",
        )
        require(
            all(
                not technically_fixed(state, f, candidate["candidate_id"])
                or (f["closure"] == "resolved" and f["resolution_readback"])
                for f in state["findings"].values()
            ),
            "closure_due",
            "Verified fixed threads require closure readback",
        )
        require(
            observation.get("refs") == action["expected_remote_state"]
            and bool(action["expected_remote_state"]),
            "stale_refs",
            "Remote observations must match expected refs",
        )
    if record["endpoint"]["kind"] == "merge":
        binding = record["merge_binding"]
        actual = record["resulting_merge"]
        require(
            binding == action["payload"].get("merge_binding"), "stale_merge", "Merge plan changed"
        )
        require(
            observation.get("protection_verified") is True
            and observation.get("required_checks_verified") is True,
            "unprotected_delivery",
            "Protected fresh source/target checks are required",
        )
        require(
            any(h["head_sha"] == candidate["head_sha"] for h in binding["source_heads"]),
            "stale_refs",
            "Source head does not match candidate",
        )
        require(
            actual is not None
            and actual["tree_sha"] == binding["expected_integrated_tree"]
            and actual["target_ancestry_verified"] is True,
            "merge_tree_mismatch",
            "Actual integrated tree and target ancestry must match the plan",
        )
        require(
            observation.get("resulting_merge") == actual,
            "unverified_merge",
            "Merge result lacks matching independent readback",
        )
    require(
        record["status"] == "verified",
        "unverified_delivery",
        "Queued or exposed-unverified delivery cannot complete work",
    )
    save(state, record)
    state["lifecycle"] = "done"
    state["delivery_id"] = record["delivery_id"]
    state["attempt"]["status"] = "done"


def record_receipt(state, request):
    record = validate_record(request["record"], "action_receipt")
    action = state["actions"].get(record["action_id"])
    require(action is not None, "unknown_action", "Receipt has no prepared action")
    require(
        action["status"] != "invalidated",
        "stale_action",
        "Action was invalidated by a scope/candidate change",
    )
    require(
        all(
            record[k] == action[k]
            for k in ("attempt_id", "operation", "payload_hash", "expected_revision")
        ),
        "receipt_mismatch",
        "Receipt does not match committed intent",
    )
    require(
        action["status"] != "confirmed",
        "action_confirmed",
        "Confirmed action cannot regress",
    )
    if record["status"] == "confirmed":
        require(
            record["external_id"] is not None and bool(record["observations"]),
            "missing_readback",
            "Confirmation requires external identity and readback",
        )
    receipt_id = request.get("receipt_id", f"receipt-{digest(record)[:24]}")
    require(
        receipt_id not in state["receipts"],
        "receipt_conflict",
        "Receipt identity already used",
    )
    state["receipts"][receipt_id] = deepcopy(record)
    state["records"][f"action_receipt:{receipt_id}"] = deepcopy(record)
    action["receipts"].append(receipt_id)
    action["status"] = record["status"]
    action["observation"] = deepcopy(request.get("observation", {}))
    state.setdefault("receipt_observations", {})[receipt_id] = deepcopy(action["observation"])
    if record["status"] == "failed":
        state["blocker"] = {
            "code": "action_failed",
            "reason": "External action failed",
            "next_action": "Reconcile the failed action with new evidence",
        }
    return receipt_id


def validate_action_admission(state, action, now):
    authority(state, now, PERMISSIONS[action["operation"]])
    validate_action_target(
        state,
        action["operation"],
        action["payload"],
        action["expected_remote_state"],
        terminal=action.get("terminal_delivery", False),
    )
    if action["operation"] in {"local_delivery", "merge", "release"}:
        require(implementation_completed(state), "implementation_incomplete",
                "Terminal dispatch requires the current delegated implementation result")
        require(
            action.get("terminal_delivery") is True,
            "unprepared_delivery",
            "Endpoint action lacks an evaluated delivery intent",
        )
    require(
        action["payload"]["scope_hash"] == state["scope_hash"],
        "stale_scope",
        "Prepared scope changed",
    )
    if action["payload"]["candidate_id"] is not None:
        current_candidate(state, action["payload"]["candidate_id"])
    if action["operation"] in {"launch_role", "send_role"}:
        if action["payload"].get("continuation_of"):
            validate_role_continuation(state, action["payload"])
        else:
            require_gate_handoff(state, action["payload"].get("role"))
        if action["payload"].get("role") in {"review", "qa"}:
            require(implementation_completed(state), "implementation_incomplete",
                    "Independent dispatch needs the completed implementation candidate")
    if action["operation"] == "run_check":
        require(implementation_completed(state), "implementation_incomplete",
                "Check dispatch needs the completed implementation candidate")
    if action.get("terminal_delivery"):
        require_delivery_accountability(state)
        require(
            not missing_scenarios(state),
            "missing_scenarios",
            "Delivery scenarios changed after action preparation",
        )
        require(
            not missing_checks(state)
            and not blocking_findings(state, state["candidate_id"])
            and set(required_roles(state)) <= set(valid_gates(state)),
            "stale_delivery",
            "Delivery proof changed after action preparation",
        )
        require(
            set(action["payload"].get("gate_ids", []))
            == {valid_gates(state)[role]["gate_id"] for role in required_roles(state)},
            "stale_delivery",
            "Prepared gate set changed",
        )
        require(
            state["blocker"] is None,
            "blocked_delivery",
            "Blocked work cannot dispatch delivery",
        )
        if action["operation"] != "local_delivery":
            require(
                all(
                    f["publication"] == "published"
                    and (
                        not technically_fixed(state, f, state["candidate_id"])
                        or (f["closure"] == "resolved" and f["resolution_readback"])
                    )
                    for f in state["findings"].values()
                ),
                "publication_due",
                "Publication and closure must finish before dispatch",
            )


def transition(original, command, request, now, dependency_states=None, *,
               trusted_verifier=None, repository=None, deferral_observation=None, recovery_input=None,
               continuation_observation=None):
    state = deepcopy(original)
    details = {}
    admission = None
    if command == "work.reopen":
        from devflow.continuation import reopen_admission
        admission = reopen_admission(state, request, trusted_verifier, now, repository)
    elif command in {"work.ready", "work.amend"}:
        admission = requested_admission(state, request, trusted_verifier, now, repository=repository)
    elif command not in BOOKKEEPING:
        admission = execution_admission(
            state, state["contract"] or {}, state.get("admission_id"), trusted_verifier, now,
            repository=repository or (state.get("authority") or {}).get("repository"),
        )
    if command == "work.reopen":
        from devflow.continuation import reopen
        details = reopen(state, request, now, admission, continuation_observation)
    elif command in {"work.ready", "work.amend"}:
        record = validate_record(request["record"], "work_contract")
        if "workflow_snapshot" in request:
            require(
                command == "work.amend" and state["lifecycle"] == "active",
                "invalid_request",
                "Only active amendments accept a workflow snapshot",
            )
        require(record["work_id"] == state["work_id"], "wrong_work", "Contract work mismatch")
        validate_endpoint(record["endpoint"])
        if command == "work.ready":
            require(
                state["lifecycle"] == "backlog",
                "invalid_state",
                "Only Backlog work can become Ready",
            )
            require(
                record["scope_revision"] == 1,
                "scope_revision",
                "Initial scope revision must be one",
            )
        else:
            require(
                state["lifecycle"] in {"ready", "active"},
                "invalid_state",
                "Only Ready or active work can be amended",
            )
            require(
                record["scope_revision"] == state["contract"]["scope_revision"] + 1,
                "scope_revision",
                "Scope revisions must increase by one",
            )
            require(
                admission["decision_kind"] == "user_request" or bool(request.get("approved_delta")),
                "missing_authority",
                "Amendment requires recorded user-approved delta",
            )
        ids = [a["id"] for a in record["acceptance"]]
        require(len(ids) == len(set(ids)), "duplicate_acceptance", "Acceptance IDs must be unique")
        dependencies = dependency_states or {}
        require(
            all(
                d != state["work_id"] and dependencies.get(d) == "done"
                for d in record["dependencies"]
            ),
            "unresolved_dependency",
            "Every dependency must be known and Done",
        )
        if command == "work.amend" and state["attempt"] and (
                scope_hash(record) != state["scope_hash"]
                or request.get("workflow_snapshot", {}).get("snapshot_id", state["attempt"]["workflow_snapshot_id"])
                != state["attempt"]["workflow_snapshot_id"]):
            require_implementation_handoff(state)
        state["scope_hash"] = scope_hash(record)
        # Caller-written Authority records are historical claims, never decisions.
        auth = derived_authority(admission)
        save(state, admission)
        state["admission_id"] = admission["admission_id"]
        if original["authority"] is not None:
            require(
                auth["repository"] == original["authority"]["repository"],
                "repository_transfer",
                "Scope amendment cannot transfer repository ownership",
            )
        state["authority"] = auth
        authority(state, now, "edit")
        endpoint_permission = {
            "local": "edit",
            "pr": "publish_pr",
            "merge": "merge",
            "release": "release",
        }[record["endpoint"]["kind"]]
        authority(state, now, endpoint_permission)
        save(state, auth)
        save(state, record)
        state["contract"] = record
        state["candidate_id"] = None
        state["gate_ids"] = {}
        state["check_ids"] = {}
        state["blocker"] = None
        for action in state["actions"].values():
            if action["status"] == "prepared":
                action["status"] = "invalidated"
        if state["attempt"]:
            if "workflow_snapshot" in request:
                snapshot = save(state, request["workflow_snapshot"], "workflow_snapshot")
                state["attempt"]["workflow_snapshot_id"] = snapshot["snapshot_id"]
                state["attempt"]["model_policy_snapshot_id"] = snapshot["snapshot_id"]
            state["attempt"]["scope_hash"] = state["scope_hash"]
            state["attempt"]["authority_id"] = auth["authority_id"]
            state["phase"] = "implement"
        else:
            state["lifecycle"] = "ready"
    elif command == "work.cancel":
        require(
            state["lifecycle"] in {"ready", "active"},
            "invalid_state",
            "Only Ready or active work can be canceled",
        )
        require(
            bool(request.get("authority_reference")),
            "missing_authority",
            "Explicit cancellation instruction reference required",
        )
        state["lifecycle"] = "canceled"
        if state["attempt"]:
            state["attempt"]["status"] = "canceled"
    elif command == "work.start":
        require(
            state["lifecycle"] == "ready", "already_claimed", "Work must be Ready and unclaimed"
        )
        authority(state, now, "edit")
        record = deepcopy(request["record"])
        record.setdefault("execution_mode", "subagent")
        record.setdefault("entry_phase", record["phase"])
        validate_record(record, "attempt")
        require(record["entry_phase"] == record["phase"], "invalid_attempt",
                "Entry phase must match the starting phase")
        require(
            record["work_id"] == state["work_id"]
            and record["scope_hash"] == state["scope_hash"]
            and record["authority_id"] == state["authority"]["authority_id"],
            "attempt_identity",
            "Attempt scope/work/authority mismatch",
        )
        require(
            record["status"] == "active" and record["blocker"] is None,
            "invalid_attempt",
            "New attempt must be active and unblocked",
        )
        require(
            record["revision"] == state["revision"], "stale_revision", "Attempt revision mismatch"
        )
        snapshot = save(state, request["workflow_snapshot"], "workflow_snapshot")
        require(
            snapshot["snapshot_id"] == record["workflow_snapshot_id"],
            "missing_snapshot",
            "Workflow snapshot mismatch",
        )
        require(
            record["model_policy_snapshot_id"] == snapshot["snapshot_id"],
            "missing_snapshot",
            "Model policy must resolve to captured snapshot",
        )
        save(state, record)
        state["attempt"] = deepcopy(record)
        state["phase"] = record["phase"]
        state["lifecycle"] = "active"
        details["action"] = prepare_action(
            state,
            "prepare_workspace",
            {
                "owner_task_id": record["owner_task_id"],
                "owned_paths": state["contract"]["scope"]["paths"],
            },
            {},
            now,
        )
    elif command == "outcome.record":
        record = validate_record(request["record"], "outcome_event")
        require(
            state["attempt"] is not None
            and record["work_id"] == state["work_id"]
            and record["attempt_id"] == state["attempt"]["attempt_id"],
            "outcome_identity",
            "Outcome must identify its historical work and attempt",
        )
        if record["candidate_id"]:
            get(state, "candidate", record["candidate_id"])
        if record["event_kind"] in {"first_ready_handoff", "exposed"}:
            require(
                record["candidate_id"] is not None,
                "missing_candidate",
                "Exposure and handoff require a candidate",
            )
        if record["event_kind"] == "defect_confirmed":
            origin = record["details"]["origin_candidate_id"]
            if origin:
                get(state, "candidate", origin)
            require(
                record["details"]["attribution"] != "confirmed" or origin is not None,
                "unknown_origin",
                "Confirmed defect attribution requires a known candidate",
            )
            require(
                record["details"]["attribution"] != "unknown" or origin is None,
                "unknown_origin",
                "Unknown attribution must not invent an origin candidate",
            )
        save(state, record)
    elif command == "usage.account":
        active(state)
        candidate = current_candidate(state)
        status = request["status"]
        require(status in {"complete", "partial", "unknown", "unavailable"},
                "invalid_accounting", "Unknown accounting coverage status")
        require(bool(request.get("source_reference", "").strip()),
                "invalid_accounting", "Accounting needs a source reference")
        limitations = request.get("limitations", [])
        require(status == "complete" or (limitations and all(str(x).strip() for x in limitations)),
                "invalid_accounting", "Incomplete accounting needs explicit limitations")
        segments, usage, tasks = accounting_sources(state)
        segment_ids = {r["segment_id"] for r in segments}
        require(status != "complete" or complete_accounting_sources(segments, usage, tasks),
                "incomplete_accounting", "Complete coverage needs imported usage for every registered task segment")
        accounting = {"schema_version": 1, "record_type": "usage_accounting",
                      "accounting_id": "accounting-" + digest(request)[:24],
                      "attempt_id": state["attempt"]["attempt_id"],
                      "candidate_id": candidate["candidate_id"], "status": status,
                      "source_reference": request["source_reference"], "limitations": limitations,
                      "segment_ids": sorted(segment_ids),
                      "usage_response_ids": sorted(r["response_id"] for r in usage),
                      "recorded_at": now.isoformat()}
        save(state, accounting)
        state["accounting_id"] = accounting["accounting_id"]
        details["accounting"] = accounting
    elif command in {"usage.record", "segment.record"}:
        require(
            state["attempt"] is not None,
            "missing_attempt",
            "Accounting requires a recorded attempt",
        )
        record = request["record"]
        kind = "usage" if command == "usage.record" else "execution_segment"
        validate_record(record, kind)
        if kind == "usage":
            known_tasks = {state["attempt"]["owner_task_id"]} | {
                a["task_id"] for a in state["assignments"].values() if a["task_id"]
            }
            require(
                record["task_id"] in known_tasks, "unknown_task", "Usage task is not registered"
            )
            if record["segment_id"] is not None:
                segment = get(state, "execution_segment", record["segment_id"])
                require(
                    segment["task_id"] == record["task_id"],
                    "wrong_task",
                    "Usage segment belongs to another task",
                )
            require(
                sum(a["weight"] for a in record["allocations"]) <= 1,
                "overallocated_usage",
                "Response allocation exceeds one",
            )
            require(
                len({a["work_id"] for a in record["allocations"]}) == len(record["allocations"]),
                "duplicate_allocation",
                "Each work may have only one response allocation",
            )
            require(
                record["reasoning_output_tokens"] <= record["output_tokens"],
                "invalid_usage",
                "Reasoning tokens are a subset of output tokens",
            )
        else:
            require(
                record["attempt_id"] == state["attempt"]["attempt_id"],
                "wrong_attempt",
                "Execution segment attempt mismatch",
            )
            known_tasks = {state["attempt"]["owner_task_id"]} | {
                a["task_id"] for a in state["assignments"].values() if a["task_id"]
            }
            require(
                record["task_id"] in known_tasks, "unknown_task", "Segment task is not registered"
            )
        save(state, record, kind)
    else:
        active(state)
        if command not in BOOKKEEPING:
            authority(state, now)
        if command == "host.assign":
            details.update(prepare_subagent_assignment(state, request, now))
        elif command == "host.recover-result":
            details.update(recover_subagent_result(state, request, recovery_input, now))
        elif command == "host.resume":
            details.update(resume_subagent_assignment(state, request, now))
        elif command == "host.activate":
            assignment = state["assignments"].get(request["assignment_id"])
            require(assignment is not None and assignment["status"] == "ready",
                    "startup_unverified", "Activation requires verified startup")
            details.update(prepare_subagent_assignment(state, {
                **request, **{key: assignment[key] for key in (
                    "role", "role_policy", "brief", "owned_paths", "workspace_reference")},
            }, now))
        elif command in {"host.record", "host.startup", "host.unavailable", "host.observe", "host.result"}:
            assignment = state["assignments"].get(request["assignment_id"])
            require(assignment is not None and assignment.get("host_kind") == "subagent",
                    "unknown_assignment", "Expected a recorded subagent assignment")
            action = state["actions"][assignment["action_id"]]
            if command in {"host.unavailable", "host.observe"}:
                from devflow.adapters.codex_host import observed_agent_status

                observation = request["observation"]
                require(assignment["status"] != "replaced",
                        "assignment_state", "A replaced assignment cannot become active again")
                require(observation.get("agent_name") == assignment["agent_name"]
                        and bool(observation.get("source_reference")),
                        "unavailability_unobserved", "Record the exact agent's observed unavailability")
                if "agent_status" in observation:
                    status = observed_agent_status(observation["agent_status"])
                    control = {"agent_name": assignment["agent_name"], "agent_status": status,
                               "source_reference": observation["source_reference"],
                               "status_evidence_kind": "completed_object" if status == "completed" else "string"}
                    assignment = assignment | {"control_observation": control}
                    if (assignment.get("task_id") and action["status"] == "confirmed"
                            and action["operation"] == "send_role"):
                        if status in {"running", "interrupted"}:
                            assignment["status"] = status
                        elif status == "completed" and assignment["status"] != "ready":
                            assignment["status"] = "completed"
                else:
                    require(command == "host.unavailable"
                            and observation.get("observation_kind") == "native_target_error"
                            and bool(observation.get("reason")) and bool(observation.get("artifact_hash"))
                            and action["status"] == "confirmed",
                            "unavailability_unobserved", "Unavailability needs native target-error evidence; omission is insufficient")
                    assignment = assignment | {"status": "unavailable", "unavailable_observation": observation}
            elif command == "host.result":
                from devflow.adapters.codex_host import NativeHostBridge

                result = NativeHostBridge.validate_result(
                    assignment, request["result"], observed_task_id=request["observed_task_id"]
                )
                require(assignment["role"] == "implementation_worker",
                        "wrong_role", "Independent review/QA results use gate record")
                require(result.get("status") in {"completed", "blocked"}
                        and bool(result.get("evidence_reference")),
                        "missing_result_evidence", "Implementation result needs evidence and status")
                if result["status"] == "completed":
                    require(assignment["scope_hash"] == state["scope_hash"],
                            "stale_scope", "Completed result assignment scope changed")
                    require(implementation_policy_matches(state, assignment, state["records"].get(
                        "candidate:" + str(state["candidate_id"]))),
                            "stale_policy", "Completed implementation must use the current candidate workflow snapshot")
                    require(result.get("output_candidate_id") is not None
                            and result["output_candidate_id"] == state["candidate_id"]
                            and assignment.get("captured_candidate_id") == state["candidate_id"],
                            "result_mismatch", "Completed implementation must identify the current candidate")
                # Count actual activations even when interrupted before output; exclude bootstrap.
                prior_round = any(r.get("record_type") == "assignment"
                                  and r["assignment_id"] == assignment["assignment_id"]
                                  and r["action_id"] != assignment["action_id"]
                                  and state["actions"].get(r["action_id"], {}).get("operation") == "send_role"
                                  and state["actions"][r["action_id"]]["status"] == "confirmed"
                                  for r in state["records"].values())
                if prior_round or "assignment_action_id" in result:
                    require(result.get("assignment_action_id") == assignment["action_id"],
                            "implementation_activation_mismatch", "Reused worker output must identify its actual producing activation")
                assignment = assignment | {"status": result["status"], "implementation_result": result}
            else:
                require(action["status"] in {"dispatched", "pending_setup", "ambiguous"},
                        "reconcile_required", "Host observation requires a dispatched action")
                require(assignment["scope_hash"] == state["scope_hash"]
                        and assignment["candidate_id"] == state["candidate_id"],
                        "stale_action", "Host startup/activation candidate or scope changed")
                if command == "host.startup":
                    observation = request["observation"] | {"verified_at": now.isoformat()}
                    policy = assignment["role_policy"]
                    require(assignment["status"] == "pending_startup"
                            and observation.get("parent_thread_id") == assignment["owner_task_id"]
                            and observation.get("agent_path") == assignment["agent_name"]
                            and observation.get("model") == policy["model"]
                            and observation.get("reasoning_effort") == policy["reasoning_effort"]
                            and observation.get("policy_hash") == policy["policy_hash"]
                            and bool(observation.get("artifact_hash")),
                            "startup_mismatch", "Startup identity/settings must match the assignment")
                    assignment = assignment | {"task_id": observation["task_id"], "status": "ready",
                                               "startup_observation": observation}
                    host_receipt(state, assignment, "confirmed", assignment["task_id"], observation, now)
                    snapshot = get(state, "workflow_snapshot", state["attempt"]["workflow_snapshot_id"])
                    save(state, {
                        "schema_version": 1, "record_type": "execution_segment",
                        "segment_id": "startup-" + digest([assignment["assignment_id"],
                                                          observation["task_id"]])[:24],
                        "attempt_id": assignment["attempt_id"], "task_id": observation["task_id"],
                        "role": assignment["role"], "model_id": observation["model"],
                        "reasoning_effort": observation["reasoning_effort"],
                        "service_tier": observation.get("service_tier"),
                        "workflow_hash": snapshot["workflow_hash"], "model_policy_hash": policy["policy_hash"],
                        "started_at": observation["started_at"], "ended_at": None,
                        "source_reference": observation["source_reference"],
                    }, "execution_segment")
                elif action["operation"] == "launch_role":
                    from devflow.adapters.codex_host import NativeHostBridge

                    bridge = NativeHostBridge()
                    assignment = (bridge.reconcile_launch(assignment, request["inventory"])
                                  if "inventory" in request else
                                  bridge.record_launch(assignment, request["response"]))
                    host_receipt(state, assignment, "pending_setup", assignment["agent_name"],
                                 {"agent_name": assignment["agent_name"]}, now)
                else:
                    from devflow.adapters.codex_host import observed_agent_status

                    matches = [item for item in request["inventory"]
                               if item.get("agent_name") == assignment["agent_name"]]
                    require(len(matches) == 1, "activation_unobserved", "Read back the exact agent after follow-up")
                    status = observed_agent_status(matches[0].get("agent_status"))
                    require(status in {"running", "completed"}, "activation_unobserved",
                            "Follow-up agent has not started")
                    require(assignment.get("startup_observation") and assignment.get("task_id"),
                            "startup_unverified", "Follow-up requires verified startup")
                    host_receipt(state, assignment, "confirmed", assignment["task_id"],
                                 {"agent_name": assignment["agent_name"], "agent_status": status,
                                  "status_evidence_kind": "completed_object" if (
                                      status == "completed"
                                  ) else "string"}, now)
                    assignment = assignment | {"status": "running"}
                    if stage_contract(state) and assignment["role"] in {"review", "qa"}:
                        assignment["gate_action_id"] = action["payload"].get("continuation_of", action["action_id"])
            save_assignment(state, assignment)
            details["assignment"] = assignment
        elif command == "candidate.record":
            require_gate_handoff(state)
            record = validate_record(request["record"], "candidate")
            if stage_contract(state):
                require(record.get("workflow_snapshot_id", state["attempt"]["workflow_snapshot_id"])
                        == state["attempt"]["workflow_snapshot_id"],
                        "stale_policy", "Candidate must bind the current workflow snapshot")
                record = record | {"workflow_snapshot_id": state["attempt"]["workflow_snapshot_id"]}
            require(
                record["attempt_id"] == state["attempt"]["attempt_id"]
                and record["scope_hash"] == state["scope_hash"]
                and record["repository"] == state["authority"]["repository"],
                "candidate_identity",
                "Candidate attempt/scope/repository mismatch",
            )
            if implementation_required(state):
                worker = state["assignments"].get(request.get("assignment_id"))
                require(worker is not None and worker["role"] == "implementation_worker"
                        and worker["status"] == "running" and worker.get("startup_observation")
                        and worker["task_id"] == request.get("producer_task_id")
                        and worker["scope_hash"] == state["scope_hash"],
                        "implementation_required", "Candidate needs the verified delegated implementer")
                require(implementation_policy_matches(state, worker, record),
                        "stale_policy", "Candidate and activated implementation worker must use the current workflow snapshot")
            require(
                not any(
                    old["record_type"] == "candidate"
                    and old["repository"] == record["repository"]
                    and old["head_sha"] == record["head_sha"]
                    and old["tree_sha"] != record["tree_sha"]
                    for old in state["records"].values()
                ),
                "inconsistent_git_object",
                "The same Git commit cannot identify different trees",
            )
            require(
                f"candidate:{record['candidate_id']}" not in state["records"]
                or state["candidate_id"] == record["candidate_id"],
                "stale_candidate",
                "Historical candidate IDs cannot replace the current snapshot",
            )
            save(state, record)
            if implementation_required(state):
                save_assignment(state, worker | {"captured_candidate_id": record["candidate_id"]})
            if state["candidate_id"] != record["candidate_id"]:
                for action in state["actions"].values():
                    if (
                        action["status"] == "prepared"
                        and action["payload"].get("candidate_id") is not None
                    ):
                        action["status"] = "invalidated"
                state["candidate_id"] = record["candidate_id"]
                state["gate_ids"] = {}
                state["check_ids"] = {}
                state["phase"] = "implement"
        elif command in {"check.record", "evidence.record"}:
            authority(state, now, "check")
            record_evidence(state, request["record"])
            if not missing_checks(state):
                state["phase"] = "verify"
        elif command == "check.complete":
            record = validate_record(request["record"], "check_evidence")
            candidate = current_candidate(state, record["candidate_id"])
            action = state["actions"].get(request["receipt"]["action_id"])
            require(
                action is not None
                and action["operation"] == "run_check"
                and action["status"] in {"dispatched", "ambiguous"},
                "unknown_check_run",
                "Check evidence requires its dispatched run action",
            )
            binding = action["payload"]
            require(
                all(
                    record[key] == binding[key]
                    for key in (
                        "candidate_id",
                        "recipe_id",
                        "recipe_version",
                        "acceptance_ids",
                        "environment_profile",
                    )
                ),
                "check_binding",
                "Check evidence does not match its admitted recipe and candidate",
            )
            require(
                binding["scope_hash"] == state["scope_hash"],
                "stale_scope",
                "Check run scope changed",
            )
            observation = request.get("observation", {})
            require(
                observation.get("head_sha") == candidate["head_sha"]
                and observation.get("tree_sha") == candidate["tree_sha"]
                and observation.get("repository") == candidate["repository"]
                and observation.get("artifact_hash") == record["artifact_hash"],
                "check_binding",
                "Check readback does not match its candidate and artifact",
            )
            require(
                record["execution_status"] != "PASS"
                or observation.get("checkout_verified") is True,
                "candidate_drift",
                "Passing check evidence requires clean post-execution checkout verification",
            )
            require(
                request["receipt"]["status"] == "confirmed"
                and request["receipt"]["external_id"] == record["evidence_id"],
                "check_binding",
                "Check receipt must identify the recorded evidence",
            )
            record_evidence(state, record)
            details["receipt_id"] = record_receipt(
                state,
                {
                    "record": request["receipt"],
                    "receipt_id": request["receipt_id"],
                    "observation": observation,
                },
            )
            details["evidence"] = record
            state["phase"] = "verify" if not missing_checks(state) else "implement"
        elif command == "finding.record":
            record = validate_record(request["record"], "finding")
            require(record["work_id"] == state["work_id"], "wrong_work", "Finding work mismatch")
            get(state, "candidate", record["candidate_id"])
            evidence_records(state, record["evidence_ids"], record["candidate_id"], passing=False)
            require(
                record["disposition"] == "open"
                and not record["fix_verification_ids"]
                and record["fix_reference"] is None,
                "invalid_finding",
                "New confirmed finding starts open with no invented fix proof",
            )
            require(
                record["publication"] in {"pending_pr", "due"}
                and record["thread_id"] is None
                and record["comment_id"] is None
                and not record["resolution_readback"],
                "invalid_finding",
                "Publication and closure require external readback",
            )
            save(state, record)
            require(
                record["finding_id"] not in state["findings"],
                "duplicate_finding",
                "Finding is already recorded",
            )
            state["findings"][record["finding_id"]] = deepcopy(record)
            state["gate_ids"] = {}
            state["phase"] = "implement"
        elif command == "finding.defer":
            finding = state["findings"].get(request["finding_id"])
            require(finding is not None, "unknown_finding", "Unknown finding")
            from devflow.deferrals import validate_issue_observation

            reference = request.get("followup_reference", "")
            rationale = request.get("rationale", "")
            linked_work = request.get("related_work_id")
            require(isinstance(rationale, str) and bool(rationale.strip()),
                    "followup_required", "Deferral needs a linked issue or unresolved follow-up work and rationale")
            if linked_work:
                lifecycle = (dependency_states or {}).get(linked_work)
                require(linked_work != state["work_id"] and lifecycle in {"backlog", "ready", "active"}
                        and reference in {"", f"work:{linked_work}"},
                        "unverified_followup", "Follow-up work must exist, remain unresolved, and match the reference")
                reference = f"work:{linked_work}"
                observation = {"kind": "work_item", "work_id": linked_work, "reference": reference,
                               "lifecycle": lifecycle, "observed_at": now.isoformat()}
            else:
                observation = validate_issue_observation(reference, deferral_observation)
                prior = finding.get("followup_observation", {})
                require(prior.get("reference") != reference or all(
                    prior.get(key) == observation[key] for key in ("issue_id", "issue_node_id")),
                    "followup_identity_changed", "The same issue URL cannot silently acquire another stable identity")
            require(finding["disposition"] not in {"verified_fixed", "duplicate", "not_a_defect"},
                    "invalid_disposition", "Only unresolved findings can be deferred")
            finding.update(disposition="deferred", followup_reference=reference,
                           related_work_id=linked_work, deferral_rationale=rationale,
                           followup_observation=deepcopy(observation))
            validate_record(finding, "finding")
            state["records"][f"finding_event:{digest(finding)}"] = deepcopy(finding)
        elif command == "finding.fix":
            finding = state["findings"].get(request["finding_id"])
            require(finding is not None, "unknown_finding", "Unknown finding")
            require(bool(request.get("fix_reference")), "missing_fix", "Fix reference required")
            current_candidate(state, request["candidate_id"])
            candidate = current_candidate(state, request["candidate_id"])
            observation = request.get("observation", {})
            expected = {
                "candidate_id": candidate["candidate_id"],
                "head_sha": candidate["head_sha"],
                "fix_reference": request["fix_reference"],
                "fix_contained": True,
                "independent_readback": True,
            }
            require(
                all(observation.get(k) == v for k, v in expected.items()),
                "unverified_fix",
                "Fix reference requires candidate-contained adapter readback",
            )
            state["fix_observations"][finding["finding_id"]] = deepcopy(observation)
            finding["fix_reference"] = request["fix_reference"]
            finding["disposition"] = "fix_pending"
            finding["resolution_readback"] = False
            state["gate_ids"] = {}
        elif command == "fix.record":
            record_fix(state, request["record"])
        elif command == "gate.record":
            gate = request["record"]
            peer = state["assignments"].get(gate["assignment_id"], {})
            historical = subagent_mode(state) and (
                gate["candidate_id"] != state["candidate_id"] or not implementation_completed(state)
                or peer.get("status") == "replaced"
                or (gate.get("assignment_action_id")
                    and gate["assignment_action_id"] != peer.get("gate_action_id", peer.get("action_id")))
            )
            for fix in request.get("fix_verifications", []):
                require(fix["producer_task_id"] == gate["producer_task_id"]
                        and fix["assignment_id"] == gate["assignment_id"]
                        and fix["candidate_id"] == gate["candidate_id"],
                        "stale_fix", "Gate fix must retain its original producer and candidate")
                record_fix(state, fix, historical=historical)
            record_gate(state, gate)
        elif command == "assignment.record":
            record = validate_record(request["record"], "assignment")
            require(not subagent_mode(state) and record.get("host_kind") != "subagent",
                    "managed_host_required", "Subagent assignments use the journaled host commands")
            action = state["actions"].get(record["action_id"])
            require(
                action is not None and action["operation"] in {"launch_role", "send_role"},
                "unknown_action",
                "Assignment needs a prepared role action",
            )
            require(
                record["attempt_id"] == state["attempt"]["attempt_id"]
                and record["owner_task_id"] == state["attempt"]["owner_task_id"],
                "wrong_owner",
                "Assignment attempt/owner mismatch",
            )
            require(
                record["candidate_id"] == action["payload"].get("candidate_id")
                and record["role"] == action["payload"].get("role"),
                "assignment_mismatch",
                "Assignment does not match prepared role/candidate",
            )
            if record["role"] in {"review", "qa"}:
                require(
                    record["task_id"] != record["owner_task_id"],
                    "not_independent",
                    "Owner cannot act as independent reviewer/QA",
                )
                require(
                    not any(
                        a["task_id"] == record["task_id"] and a["role"] != record["role"]
                        for a in state["assignments"].values()
                        if record["task_id"]
                    ),
                    "not_independent",
                    "Review and QA require distinct peer tasks",
                )
            elif record["role"] == "implementation_worker" and record["task_id"]:
                require(not any(a["task_id"] == record["task_id"] and a["role"] in {"review", "qa"}
                                for a in state["assignments"].values()),
                        "not_independent", "Implementation cannot reuse review/QA identity")
            if record["status"] in {"running", "completed"}:
                require(
                    action["status"] == "confirmed"
                    and record["task_id"] is not None
                    and record["client_id"] is None,
                    "pending_assignment",
                    "Only a confirmed ready task may run",
                )
                receipt = state["receipts"][action["receipts"][-1]]
                require(
                    receipt["external_id"] == record["task_id"],
                    "wrong_task",
                    "Assignment task must match host receipt",
                )
            existing = state["assignments"].get(record["assignment_id"])
            if existing:
                require(
                    existing["role"] == record["role"]
                    and existing["task_id"] in {None, record["task_id"]},
                    "assignment_conflict",
                    "Cannot silently replace a role task",
                )
            state["assignments"][record["assignment_id"]] = deepcopy(record)
            # Assignment revisions are projections; their originals remain immutable history.
            state["records"][f"assignment_event:{digest(record)}"] = deepcopy(record)
        elif command == "action.prepare":
            require(not (subagent_mode(state) and request["operation"] in {"launch_role", "send_role"}),
                    "managed_host_required", "Prepare subagent roles with host assign or activate")
            require(
                request["operation"] not in {"merge", "release", "local_delivery"},
                "delivery_required",
                "Prepare endpoint mutations through deliver so required proof is evaluated",
            )
            details["action"] = prepare_action(
                state,
                request["operation"],
                request.get("payload", {}),
                request.get("expected_remote_state", {}),
                now,
                request.get("action_id"),
            )
        elif command == "action.begin":
            action = state["actions"].get(request["action_id"])
            require(action is not None, "unknown_action", "No prepared action exists")
            require(
                action["status"] == "prepared",
                "reconcile_required",
                "Already dispatched or uncertain actions must be reconciled without repeating the mutation",
            )
            unblocked(state)
            validate_action_admission(state, action, now)
            action["status"] = "dispatched"
            details["action"] = action
        elif command == "action.retry":
            action = state["actions"].get(request["action_id"])
            require(
                action is not None and action["status"] == "failed",
                "retry_forbidden",
                "Only a definitely failed action may be explicitly retried",
            )
            observations = state.get("receipt_observations", {})
            require(
                bool(action["receipts"])
                and all(
                    state["receipts"][identity]["status"] == "failed"
                    and observations.get(identity, {}).get("no_mutation") is True
                    for identity in action["receipts"]
                ),
                "reconcile_required",
                "Every earlier result must prove no mutation; uncertainty requires reconciliation",
            )
            # Clear only the failed-action blocker; other failures or user blockers remain.
            if (
                state["blocker"]
                and state["blocker"]["code"] == "action_failed"
                and not any(
                    a["status"] == "failed" and a["action_id"] != action["action_id"]
                    for a in state["actions"].values()
                )
            ):
                state["blocker"] = None
            validate_action_admission(state, action, now)
            action["status"] = "prepared"
            details["action"] = action
        elif command == "action.record":
            details["receipt_id"] = record_receipt(state, request)
        elif command in {"finding.publish", "finding.close"}:
            authority(state, now, "publish_findings")
            finding = state["findings"].get(request["finding_id"])
            require(finding is not None, "unknown_finding", "Unknown finding")
            action = state["actions"].get(request["action_id"])
            expected_operation = (
                "publish_finding" if command == "finding.publish" else "resolve_thread"
            )
            require(
                action is not None
                and action["operation"] == expected_operation
                and action["status"] == "confirmed"
                and action["payload"].get("finding_id") == finding["finding_id"],
                "missing_receipt",
                "Finding transition requires matching confirmed action",
            )
            observation = action.get("observation", {})
            require(
                observation.get("independent_readback") is True
                and observation.get("finding_id") == finding["finding_id"],
                "missing_readback",
                "Finding identity must be independently read back",
            )
            if command == "finding.publish":
                require(
                    all(observation.get(k) for k in ("pr_reference", "thread_id", "comment_id")),
                    "missing_anchor",
                    "Publication needs PR/thread/comment identities",
                )
                finding.update(
                    {k: observation[k] for k in ("pr_reference", "thread_id", "comment_id")}
                )
                finding["publication"] = "published"
                finding["closure"] = (
                    "pending" if finding["disposition"] == "verified_fixed" else "not_due"
                )
            else:
                require(
                    technically_fixed(state, finding, state["candidate_id"]),
                    "unverified_fix",
                    "Technical fix verification required before closure",
                )
                require(
                    set(required_roles(state)) <= set(valid_gates(state)),
                    "missing_gates",
                    "Final gates required before remote closure",
                )
                require(
                    finding["publication"] == "published"
                    and observation.get("thread_id") == finding["thread_id"]
                    and observation.get("resolved") is True
                    and observation.get("fix_contained") is True
                    and observation.get("reply_verified") is True,
                    "unverified_closure",
                    "Closure requires published identity, contained fix, reply and resolution readback",
                )
                candidate = current_candidate(state)
                require(
                    observation.get("candidate_id") == candidate["candidate_id"]
                    and observation.get("head_sha") == candidate["head_sha"]
                    and observation.get("fix_reference") == finding["fix_reference"],
                    "stale_closure",
                    "Closure must prove the current candidate contains this fixing reference",
                )
                finding["closure"] = "resolved"
                finding["resolution_readback"] = True
        elif command == "deliver":
            endpoint = state["contract"]["endpoint"]
            operation = {
                "local": "local_delivery",
                "pr": "publish_pr",
                "merge": "merge",
                "release": "release",
            }[endpoint["kind"]]
            authority(state, now, PERMISSIONS[operation])
            if "record" in request:
                verify_delivery(state, request["record"], request.get("observation", {}))
            else:
                candidate = current_candidate(state, request.get("candidate_id"))
                require(
                    next_actions(state)
                    == [{"kind": "deliver", "candidate_id": candidate["candidate_id"]}],
                    "not_ready_to_deliver",
                    "Required proof or actions are incomplete",
                )
                payload = {
                    "candidate_id": candidate["candidate_id"],
                    "scope_hash": state["scope_hash"],
                    "endpoint": endpoint,
                    "head_sha": candidate["head_sha"],
                    "tree_sha": candidate["tree_sha"],
                    "gate_ids": [
                        valid_gates(state)[role]["gate_id"] for role in required_roles(state)
                    ],
                }
                if stage_contract(state):
                    payload["accounting_id"] = current_accounting(state)["accounting_id"]
                if endpoint["kind"] == "pr":
                    requested_refs = request.get("expected_remote_state", {})
                    earlier = []
                    for previous in state["actions"].values():
                        previous_values = {
                            **previous["expected_remote_state"],
                            **previous["payload"],
                        }
                        if (
                            previous["operation"] == "publish_pr"
                            and previous["status"] == "confirmed"
                            and not previous.get("terminal_delivery")
                            and readback_matches(endpoint, previous.get("observation", {}))
                            and all(
                                previous_values.get(key) == requested_refs.get(key)
                                for key in ("head_ref", "base_ref", "title", "body")
                            )
                        ):
                            earlier.append(previous["action_id"])
                    require(
                        len(earlier) <= 1,
                        "publication_conflict",
                        "Existing PR publication correlation is ambiguous",
                    )
                    if earlier:
                        payload["publication_action_id"] = earlier[0]
                if endpoint["kind"] == "merge":
                    require(
                        bool(request.get("merge_binding")),
                        "missing_merge_plan",
                        "Expected integrated-tree merge binding required",
                    )
                    payload["merge_binding"] = request["merge_binding"]
                details["action"] = prepare_action(
                    state,
                    operation,
                    payload,
                    request.get("expected_remote_state", {}),
                    now,
                    terminal=True,
                )
                state["phase"] = "deliver"
        elif command == "work.reconcile":
            # Reconciliation never invents a remote readback or clears an uncertain action.
            if request.get("clear_blocker"):
                require(
                    bool(request.get("evidence_ids")),
                    "missing_evidence",
                    "Clearing a blocker requires evidence",
                )
                evidence_records(state, request["evidence_ids"], state["candidate_id"])
                state["blocker"] = None
        elif command == "work.block":
            blocker = request["blocker"]
            require(
                all(
                    isinstance(blocker.get(k), str) and blocker[k]
                    for k in ("code", "reason", "next_action")
                ),
                "invalid_blocker",
                "Blocker needs code, reason and next action",
            )
            state["blocker"] = deepcopy(blocker)
        else:
            raise WorkflowError("unknown_command", f"Unsupported command: {command}")
    if state["lifecycle"] == "active" and stage_contract(state):
        accounting = current_accounting(state)
        accounting_id = accounting["accounting_id"] if accounting else None
        for action in state["actions"].values():
            if (action["status"] == "prepared" and action.get("terminal_delivery")
                    and action["payload"].get("accounting_id") != accounting_id):
                # No mutation has happened yet: stale accounting cannot dispatch,
                # and the next command must be able to prepare a fresh intent.
                action["status"] = "invalidated"
    timestamp = now.isoformat()
    old_phase = original["phase"] if original["lifecycle"] == "active" else None
    new_phase = state["phase"] if state["lifecycle"] == "active" else None
    if old_phase != new_phase:
        if state["phase_history"] and state["phase_history"][-1]["ended_at"] is None:
            state["phase_history"][-1]["ended_at"] = timestamp
        if new_phase:
            state["phase_history"].append(
                {"phase": new_phase, "started_at": timestamp, "ended_at": None}
            )
    state["history"].append(
        {
            "operation_id": request["operation_id"],
            "command": command,
            "revision": state["revision"] + 1,
            "recorded_at": timestamp,
            "request_hash": digest(request),
            "from_phase": old_phase,
            "to_phase": new_phase,
            "approved_delta": request.get("approved_delta"),
            "authority_reference": request.get("authority_reference"),
        }
    )
    state["revision"] += 1
    if state["attempt"]:
        state["attempt"]["revision"] = state["revision"]
        state["attempt"]["phase"] = state["phase"]
        state["attempt"]["blocker"] = state["blocker"]
    return state, details
