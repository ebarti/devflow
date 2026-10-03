"""Cause-specific stopped investigation admission: same iteration, gates only."""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_continuation import session_state_digest
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock, _private_bytes
from .delivery_resources import private_directory


def _reference(path, expected):
    if not isinstance(path, str) or not isinstance(expected, str):
        raise ValueError("gates admission authority reference identity is invalid")
    path = Path(path)
    info = path.lstat()
    if (
        not path.is_absolute()
        or not path.is_file()
        or path.is_symlink()
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or info.st_mode & 0o022
        or info.st_size > 1024 * 1024
    ):
        raise ValueError("gates admission authority reference is unsafe")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected:
        raise ValueError("gates admission authority reference changed")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("gates admission authority reference must be an object")
    return value


def _accepted_plan(semantic, spec):
    readback = _reference(
        semantic["accepted_plan_readback"], semantic["accepted_plan_readback_sha256"]
    )
    accepted = readback.get("accepted_plan", {})
    try:
        content = json.loads(spec["accepted_plan"])
    except (TypeError, ValueError):
        content = spec["accepted_plan"]
    if (
        readback.get("run_id") != spec["run_id"]
        or type(accepted.get("revision")) is not int
        or accepted["revision"] < 1
        or accepted.get("digest") != digest(content)
        or canonical_json(accepted.get("content")) != canonical_json(content)
    ):
        raise ValueError("gates-only accepted investigation plan changed")


def authority_readback(spec, seal, payload):
    """Read-only receipt, custody and accepted-plan binding; no admission effects."""
    authority = _reference(payload["authority_path"], payload["authority_sha256"])
    semantic = _reference(payload["semantic_path"], payload["semantic_sha256"])
    scope = authority.get("scope", {})
    if (
        authority.get("decision_owner") != "main task"
        or authority.get("new_user_approval_required") is not False
        or scope.get("run_id") != spec["run_id"]
        or scope.get("work_id") != spec["work_id"]
        or scope.get("original_session") != seal["session_id"]
        or type(scope.get("iteration")) is not int
        or scope["iteration"] != seal["iteration"]
        or type(scope.get("max_commands")) is not int
        or scope["max_commands"] != 1
        or canonical_json(scope.get("current_after_candidate")) != canonical_json(seal["candidate"])
        or scope.get("frozen_input_candidate_id") != seal["input_candidate_id"]
        or not isinstance(scope.get("allowed_paths"), list)
        or sorted(scope["allowed_paths"]) != sorted(spec["policy"]["allowed_paths"])
        or scope.get("authority_sha256") != payload["semantic_sha256"]
        or semantic.get("decision_owner") != "main task"
        or semantic.get("new_user_approval_required") is not False
        or set(semantic.get("scope_unchanged", [])) != set(spec["policy"]["allowed_paths"])
    ):
        raise ValueError("gates-only investigation authority changed")
    custody = authority.get("custody_proof", {})
    _reference(custody["path"], custody["sha256"])
    _accepted_plan(semantic, spec)
    return authority, semantic


def preflight(store, run_id):
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    closed = store._completed_temporal_result(
        run_id,
        workflow_id=row["workflow_id"] or "delivery-" + run_id,
    )
    state = closed["result"]
    implementation = next(
        (
            role
            for role in reversed(state.get("roles", []))
            if role.get("role") == "implement" and role.get("iteration") == state.get("iteration")
        ),
        None,
    )
    latest = next(
        (
            attempt
            for attempt in attempts
            if implementation
            and attempt["role"] == "implement"
            and attempt["iteration"] == implementation["iteration"]
            and attempt["session_id"] == implementation.get("session_id")
        ),
        None,
    )
    if (
        state.get("run_id") != run_id
        or state.get("outcome") != "blocked"
        or state.get("error") != "implementer did not establish a pass"
        or row["outcome"] != "blocked"
        or row["protocol_revision"] != state.get("revision")
        or closed["request_digest"] != row["request_digest"]
        or closed["recovery_digest"] != (digest(previous) if previous else None)
        or state.get("pull_request") is not None
        or row["pr_json"] not in (None, "null")
        or not implementation
        or implementation.get("status") == "pass"
        or not implementation.get("session_id")
        or implementation.get("cleanup") != "confirmed"
        or not latest
        or latest["candidate_id"] != implementation.get("input_candidate_id")
        or any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts)
        or any(e["state"] != "complete" or not e["observed_json"] for e in effects)
        or claim is not None
    ):
        raise ValueError("gates-only admission requires its stopped failed implementation custody")
    frozen = json.loads(row["candidate_json"] or "null")
    if not isinstance(frozen, dict):
        raise ValueError("gates-only frozen input candidate is absent or invalid")
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    if (
        canonical_json(candidate) != canonical_json(implementation.get("candidate"))
        or candidate["head"] != spec["base_sha"]
        or frozen.get("id") != implementation["input_candidate_id"]
        or not broker._changed_paths()
        or not broker._changed_paths() <= set(spec["policy"]["allowed_paths"])
        or any(
            role.get("session_id") != implementation["session_id"]
            for role in state["roles"]
            if role.get("role") == "implement"
        )
        or _git(broker.checkout, "branch", "--show-current") != spec["branch"]
        or _git(broker.checkout, "remote", "get-url", "--push", "origin") != spec["origin_url"]
        or _git(broker.source, "remote", "get-url", "origin") != spec["origin_url"]
        or _git(broker.source, "ls-remote", "origin", f"refs/heads/{spec['branch']}")
        or broker._existing_pr() is not None
    ):
        raise ValueError(
            "gates-only admission lost authentic after-candidate/base/scope/remote proof"
        )
    result_path = Path(latest["result_path"])
    raw = _private_bytes(result_path, Path(spec["state_dir"]))
    if canonical_json(json.loads(raw)) != canonical_json(json.loads(latest["result_json"])):
        raise ValueError("gates-only native result receipt disagrees with frozen attempt")
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    seal = {
        "spec_digest": digest(spec),
        "row": row,
        "closed": closed,
        "attempts": attempts,
        "effects": effects,
        "candidate": candidate,
        "session_id": implementation["session_id"],
        "work_binding": binding,
        "cleanup": _stopped_cleanup(spec),
        "iteration": state["iteration"],
        "input_candidate_id": implementation["input_candidate_id"],
        "result_sha256": hashlib.sha256(raw).hexdigest(),
        "session_sha256": session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement", implementation["session_id"]
        ),
        "original_recovery": previous,
    }
    return {
        "run_id": run_id,
        "candidate": candidate,
        "iteration": state["iteration"],
        "precheck_sha256": digest(seal),
        "seal": seal,
    }


def admit(store, run_id, payload):
    fields = {
        "command_id",
        "precheck_sha256",
        "authority_path",
        "authority_sha256",
        "semantic_path",
        "semantic_sha256",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != fields
        or not isinstance(payload.get("command_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", payload["command_id"])
        or any(
            not isinstance(payload.get(key), str) or not re.fullmatch(r"[0-9a-f]{64}", payload[key])
            for key in ("precheck_sha256", "authority_sha256", "semantic_sha256")
        )
    ):
        raise ValueError("gates-only request fields do not match the contract")
    command_digest = digest({"run_id": run_id, **payload})
    spec = store.effective_spec(run_id)
    root = Path(spec["state_dir"]) / "gates-admission"
    private_directory(root)
    with _lock(root / "controller.lock"):
        with store._connect() as db:
            prior = db.execute(
                "SELECT * FROM delivery_commands WHERE command_id=?", (payload["command_id"],)
            ).fetchone()
            granted = db.execute(
                "SELECT * FROM delivery_gate_admissions WHERE run_id=?", (run_id,)
            ).fetchone()
        if prior:
            if prior["request_digest"] != command_digest:
                raise ValueError("command ID already belongs to different inputs")
            return json.loads(prior["response_json"])
        if granted:
            raise ValueError("this original run already received its gates-only admission")
        observed = preflight(store, run_id)
        if observed["precheck_sha256"] != payload["precheck_sha256"]:
            raise ValueError("gates-only preflight changed")
        seal = observed["seal"]
        authority, semantic = authority_readback(spec, seal, payload)
        recovery = {
            "kind": "investigation_gates_only",
            "command": payload,
            "seal": seal,
            "state": seal["closed"]["result"],
            "candidate": seal["candidate"],
            "original_recovery": seal["original_recovery"],
            "authority": authority,
            "semantic": semantic,
        }
        _immutable(root / "admission.json", recovery)
        preserve_resources(root, spec)
        workflow_id = "delivery-" + run_id + "-gates-only-1"
        response = {
            "run_id": run_id,
            "phase": "gates_only_queued",
            "workflow_id": workflow_id,
            "candidate_id": seal["candidate"]["id"],
            "iteration": seal["iteration"],
            "implementation_authority": False,
        }
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if canonical_json(dict(row)) != canonical_json(seal["row"]):
                raise ValueError("gates admission lost its exact frozen projection")
            if canonical_json(work_binding(store, spec, db)) != canonical_json(
                seal["work_binding"]
            ):
                raise ValueError("gates admission lost its frozen issue authority")
            if store.state.claim_for(db, spec["work_id"]) is not None:
                raise ValueError("gates-only admission lost released ownership")
            store.state.claim_work(
                db, spec["work_id"], f"external:devflow:{run_id}", store.config.dashboard_url
            )
            work_binding(store, spec, db)
            db.execute(
                "INSERT INTO delivery_gate_admissions VALUES (?,?,?)",
                (run_id, payload["command_id"], canonical_json(recovery)),
            )
            db.execute(
                "UPDATE delivery_runs SET phase='gates_only_queued',"
                "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1,"
                "workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?",
                (workflow_id, canonical_json(recovery), store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                "WHERE run_id=?",
                (store.state.now(), run_id),
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (payload["command_id"], run_id, command_digest, canonical_json(response)),
            )
            store._event(
                db,
                run_id,
                seal["row"]["revision"] + 1,
                "gates_only_queued",
                "Authentic failed assessment preserved; investigation gates only admitted",
                {
                    "candidate_id": seal["candidate"]["id"],
                    "precheck_sha256": digest(seal),
                    "implementation_authority": False,
                },
            )
        return response


def readback(store, spec, recovery):
    broker = DeliveryBroker(store, spec)
    seal = recovery["seal"]
    if (
        digest(spec) != seal["spec_digest"]
        or canonical_json(broker.candidate()) != canonical_json(seal["candidate"])
        or canonical_json(_stopped_cleanup(spec)) != canonical_json(seal["cleanup"])
    ):
        raise ValueError("gates-only stopped candidate or cleanup changed")
    _reference(recovery["command"]["authority_path"], recovery["command"]["authority_sha256"])
    _reference(recovery["command"]["semantic_path"], recovery["command"]["semantic_sha256"])
    _accepted_plan(recovery["semantic"], spec)
    row, attempts, effects, claim = _rows(store, spec["run_id"])
    closed = store._completed_temporal_result(
        spec["run_id"],
        workflow_id=seal["closed"]["workflow_id"],
    )
    latest = next(
        a
        for a in seal["attempts"]
        if a["role"] == "implement" and a["iteration"] == seal["iteration"]
    )
    raw = _private_bytes(Path(latest["result_path"]), Path(spec["state_dir"]))
    if (
        canonical_json(attempts) != canonical_json(seal["attempts"])
        or canonical_json(effects) != canonical_json(seal["effects"])
        or canonical_json(closed) != canonical_json(seal["closed"])
        or hashlib.sha256(raw).hexdigest() != seal["result_sha256"]
        or session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement", seal["session_id"]
        )
        != seal["session_sha256"]
        or claim is None
        or claim["owner"] != f"external:devflow:{spec['run_id']}"
        or _git(broker.checkout, "branch", "--show-current") != spec["branch"]
        or _git(broker.checkout, "remote", "get-url", "--push", "origin") != spec["origin_url"]
        or _git(broker.source, "remote", "get-url", "origin") != spec["origin_url"]
        or _git(broker.source, "ls-remote", "origin", f"refs/heads/{spec['branch']}")
        or broker._existing_pr() is not None
    ):
        raise ValueError("gates-only frozen execution, session or source custody changed")
    _reference(
        recovery["authority"]["custody_proof"]["path"],
        recovery["authority"]["custody_proof"]["sha256"],
    )
    with store._connect() as db:
        if canonical_json(work_binding(store, spec, db)) != canonical_json(seal["work_binding"]):
            raise ValueError("gates-only work binding changed")
        granted = db.execute(
            "SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?", (spec["run_id"],)
        ).fetchone()
        if not granted or granted[0] != canonical_json(recovery):
            raise ValueError("gates-only durable admission changed")
    return {"candidate": seal["candidate"], "implementation_authority": False}
