"""Pure workflow transitions. All time and external observations are explicit inputs."""

from copy import deepcopy
from datetime import datetime

from devflow.errors import WorkflowError
from devflow.validation import digest, validate_record

ID_FIELDS = {
    "work_contract": "scope_revision",
    "outcome_event": "event_id",
    "authority": "authority_id",
    "attempt": "attempt_id",
    "candidate": "candidate_id",
    "assignment": "assignment_id",
    "check_evidence": "evidence_id",
    "observation_evidence": "evidence_id",
    "gate_result": "gate_id",
    "finding": "finding_id",
    "fix_verification": "verification_id",
    "delivery": "delivery_id",
    "workflow_snapshot": "snapshot_id",
    "execution_segment": "segment_id",
    "usage": "response_id",
}
ACTION_OPERATIONS = {
    "run_check",
    "prepare_workspace",
    "launch_role",
    "send_role",
    "publish_pr",
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


def prepare_action(state, operation, payload, expected_remote_state, now, action_id=None):
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
        require(
            payload.get("role") in {"review", "qa", "implementation_worker"},
            "wrong_role",
            "Role action must name a supported peer role",
        )
        if payload["role"] in {"review", "qa"}:
            current_candidate(state, payload["candidate_id"])
    fingerprint = digest(
        {"operation": operation, "payload": payload, "expected_remote_state": expected_remote_state}
    )
    action_id = action_id or f"action-{fingerprint[:24]}"
    for existing_action in state["actions"].values():
        if (
            existing_action["operation"] == operation
            and existing_action["payload_hash"] == digest(payload)
            and existing_action["expected_remote_state"] == expected_remote_state
            and existing_action["status"] != "invalidated"
        ):
            return existing_action
    existing = state["actions"].get(action_id)
    if existing:
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
    if state["blocker"]:
        return [{"kind": "request_user_action", "blocker": state["blocker"]}]
    if state["lifecycle"] == "ready":
        return [{"kind": "prepare_workspace", "reason": "Start the authorized attempt"}]
    if not state["candidate_id"]:
        return [{"kind": "implement"}]
    missing = missing_checks(state)
    if missing:
        return [
            {"kind": "run_check", "recipe_id": r, "candidate_id": state["candidate_id"]}
            for r in missing
        ]
    blockers = blocking_findings(state, state["candidate_id"])
    if blockers:
        return [{"kind": "repair_findings", "finding_ids": blockers}]
    gates = valid_gates(state)
    roles = [role for role in required_roles(state) if role not in gates]
    if roles:
        result = []
        for role in roles:
            assignments = [
                a
                for a in state["assignments"].values()
                if a["role"] == role and a["candidate_id"] == state["candidate_id"]
            ]
            result.append(
                {
                    "kind": "wait_roles" if assignments else "launch_role",
                    "role": role,
                    "candidate_id": state["candidate_id"],
                    "assignments": assignments,
                }
            )
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
    return [{"kind": "deliver", "candidate_id": state["candidate_id"]}]


def record_evidence(state, record):
    validate_record(record)
    require(
        record["record_type"] in {"check_evidence", "observation_evidence"},
        "invalid_record",
        "Expected evidence record",
    )
    candidate = current_candidate(state, record["candidate_id"])
    existing = state["records"].get(f"{record['record_type']}:{record['evidence_id']}")
    if existing:
        require(existing == record, "immutable_record", "Evidence identity already exists")
        return
    acceptance_ids = {a["id"] for a in state["contract"]["acceptance"]}
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
    if record["record_type"] == "check_evidence":
        previous_id = state["check_ids"].get(record["recipe_id"])
        previous = get(state, "check_evidence", previous_id) if previous_id else None
        state["check_ids"][record["recipe_id"]] = record["evidence_id"]
        if record["execution_status"] != "PASS" or (
            previous and previous["input_signature"] != record["input_signature"]
        ):
            state["gate_ids"] = {}


def record_fix(state, record):
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


def record_gate(state, record):
    validate_record(record, "gate_result")
    existing = state["records"].get(f"gate_result:{record['gate_id']}")
    if existing:
        require(existing == record, "immutable_record", "Gate identity already exists")
        return
    current_candidate(state, record["candidate_id"])
    independent_assignment(
        state,
        record["assignment_id"],
        record["producer_task_id"],
        record["candidate_id"],
        record["role"],
    )
    snapshot = get(state, "workflow_snapshot", state["attempt"]["workflow_snapshot_id"])
    require(
        record["scope_hash"] == state["scope_hash"]
        and record["workflow_hash"] == snapshot["workflow_hash"],
        "stale_policy",
        "Gate scope or policy mismatch",
    )
    evidence = evidence_records(
        state,
        record["evidence_ids"],
        record["candidate_id"],
        record["producer_task_id"],
        record["status"] == "PASS",
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
    if record["status"] == "PASS":
        require(
            not record["blocking_finding_ids"]
            and not blocking_findings(state, record["candidate_id"]),
            "blocking_findings",
            "High/Blocker findings require independent technical fix verification first",
        )
        require(
            not missing_checks(state), "missing_checks", "Required command evidence is incomplete"
        )
        covered = set().union(*(set(e["acceptance_ids"]) for e in evidence))
        require(
            {a["id"] for a in state["contract"]["acceptance"]} <= covered,
            "missing_acceptance",
            "Gate evidence must cover every acceptance criterion",
        )
    if record["status"] == "PASS" and record["role"] == "qa":
        require(
            not missing_scenarios(state, evidence),
            "missing_scenarios",
            "QA evidence must cover every required scenario",
        )
        require(
            any(
                e["record_type"] == "observation_evidence" or e.get("executed_assertions", 0) > 0
                for e in evidence
            ),
            "missing_product_proof",
            "QA needs executed assertions or independent product observations",
        )
    save(state, record)
    state["gate_ids"][record["role"]] = record["gate_id"]
    state["assignments"][record["assignment_id"]]["status"] = "completed"
    state["phase"] = "verify" if record["status"] == "PASS" else "implement"


def verify_delivery(state, record, observation):
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
        "endpoint": record["endpoint"],
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
    if record["status"] == "failed":
        state["blocker"] = {
            "code": "action_failed",
            "reason": "External action failed",
            "next_action": "Reconcile the failed action with new evidence",
        }
    return receipt_id


def transition(original, command, request, now, dependency_states=None):
    state = deepcopy(original)
    details = {}
    if command in {"work.ready", "work.amend"}:
        record = validate_record(request["record"], "work_contract")
        require(record["work_id"] == state["work_id"], "wrong_work", "Contract work mismatch")
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
                bool(request.get("approved_delta")),
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
        state["scope_hash"] = scope_hash(record)
        auth = validate_record(request["authority"], "authority")
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
        record = validate_record(request["record"], "attempt")
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
        authority(state, now)
        if command == "candidate.record":
            record = validate_record(request["record"], "candidate")
            require(
                record["attempt_id"] == state["attempt"]["attempt_id"]
                and record["scope_hash"] == state["scope_hash"]
                and record["repository"] == state["authority"]["repository"],
                "candidate_identity",
                "Candidate attempt/scope/repository mismatch",
            )
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
            authority(state, now, "check")
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
            for fix in request.get("fix_verifications", []):
                record_fix(state, fix)
            record_gate(state, request["record"])
        elif command == "assignment.record":
            record = validate_record(request["record"], "assignment")
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
            authority(state, now, PERMISSIONS[action["operation"]])
            require(
                action["payload"]["scope_hash"] == state["scope_hash"],
                "stale_scope",
                "Prepared scope changed",
            )
            if action["payload"]["candidate_id"] is not None:
                current_candidate(state, action["payload"]["candidate_id"])
            if action["operation"] in {"merge", "release", "local_delivery"}:
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
            action["status"] = "dispatched"
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
                if endpoint["kind"] == "merge":
                    require(
                        bool(request.get("merge_binding")),
                        "missing_merge_plan",
                        "Expected integrated-tree merge binding required",
                    )
                    payload["merge_binding"] = request["merge_binding"]
                details["action"] = prepare_action(
                    state, operation, payload, request.get("expected_remote_state", {}), now
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
