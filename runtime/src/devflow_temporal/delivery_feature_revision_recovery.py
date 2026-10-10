"""Authenticated original-worker recovery after an adopted plan correction."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_continuation import continuation_authority
from .delivery_execution_registry import OwnershipConflict


def _receipt(spec):
    from .delivery_feature_revisions import _latest, _values

    return _latest(_values(spec))


def validate_revision_recovery(store, predecessor, admission, *, historical=False):
    """No new caller-selected policy: exactly derive from the adopted parent plan."""
    from .delivery_feature_revisions import (
        PREFIX,
        _values,
        adopted_plan_identity,
        authenticate_adopted_plan_custody,
        worker_revision_required,
    )

    parent = store.effective_spec(admission["parent_run_id"])
    receipt = _values(parent).get(
        PREFIX + "adopted:" + str(admission["plan_identity"]["plan_revision"])
    )
    identity = (
        {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
        if receipt
        else None
    )
    if (
        not receipt
        or receipt["revision_id"] != admission.get("revision_id")
        or identity != admission.get("plan_identity")
        or (
            not historical
            and (
                adopted_plan_identity(parent) != receipt["identity"]
                or not worker_revision_required(parent, predecessor)
            )
        )
    ):
        raise OwnershipConflict("worker recovery has no exact adopted plan correction")
    authenticate_adopted_plan_custody(store, parent, receipt)
    # Historic custody remains readable after a later adoption. Only active
    # admission above must match the current global revision.
    parent = deepcopy(parent)
    parent["accepted_plan"] = canonical_json(receipt["plan"])
    parent["feature_plan_revision"] = identity
    derived = _derive(parent, receipt, predecessor)
    for key in (
        "run_id",
        "work_id",
        "request_digest",
        "config_path",
        "config_digest",
        "state_dir",
        "checkout",
        "branch",
        "base_sha",
        "authorized_endpoint",
    ):
        if derived.get(key) != predecessor.get(key):
            raise OwnershipConflict("worker revision changed original execution custody")
    if admission.get("predecessor_spec_digest") != digest(predecessor) or admission.get(
        "derived_spec_digest"
    ) != digest(derived):
        raise OwnershipConflict("worker revision changed its immutable derivation")
    return derived


def _derive(parent, receipt, predecessor):
    from .delivery_feature_execution import revised_worker_spec
    from .delivery_feature_revisions import worker_revision_affected

    identity = {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
    if worker_revision_affected(parent, predecessor, through_revision=identity["plan_revision"]):
        derived = revised_worker_spec(
            parent,
            receipt["plan"],
            predecessor["feature_worker"]["chunk_id"],
            expected_revision=identity,
            worker=predecessor,
        )
    else:
        # An unaffected unfinished session changes only plan/owner identity. Its
        # exact immutable worker policy and accepted chunk requirements survive.
        derived = deepcopy(predecessor)
        derived["feature_plan_revision"] = identity
    derived["feature_delivery"]["owner"] = deepcopy(parent["feature_delivery"]["owner"])
    derived["feature_worker"]["parent_run_id"] = parent["run_id"]
    return derived


def validate_custody(store, recovery):
    admission = recovery.get("feature_plan_revision")
    if not admission:
        return
    derived = validate_revision_recovery(
        store, recovery["predecessor_spec"], admission, historical=True
    )
    execution = recovery["execution_spec"]
    if (
        continuation_authority(execution["policy"]) != continuation_authority(derived["policy"])
        or execution.get("feature_plan_revision") != admission["plan_identity"]
        or execution["feature_delivery"]["owner"] != derived["feature_delivery"]["owner"]
        or any(
            execution.get(key) != derived.get(key)
            for key in (
                "run_id",
                "state_dir",
                "checkout",
                "branch",
                "accepted_plan",
                "config_digest",
                "feature_worker",
            )
        )
    ):
        raise OwnershipConflict("worker revision effective snapshot changed its derived authority")


def resume_revision_worker(store, parent, child, row=None):
    from .delivery_feature_execution import registry, worker_key
    from .delivery_feature_revisions import worker_revision_required
    from .delivery_stopped_resume import KIND, admit, snapshot

    receipt = _receipt(parent)
    if not receipt:
        raise OwnershipConflict("worker recovery has no adopted plan correction")
    command_id = (
        "revision-worker-"
        + digest(
            {
                "revision": receipt["revision_id"],
                "run": child["run_id"],
            }
        )[:32]
    )
    shared = registry(parent)
    token = parent["feature_delivery"]["owner"]
    with shared.connect() as db:
        shared.require(db, token)
    with store._connect() as db:
        prior = db.execute(
            "SELECT response_json FROM delivery_commands WHERE command_id=? AND run_id=?",
            (command_id, child["run_id"]),
        ).fetchone()
        saved = db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?", (child["run_id"],)
        ).fetchone()
    if prior:
        recovery = json.loads(saved[0] or "null") if saved else None
        if (
            not recovery
            or recovery.get("feature_plan_revision", {}).get("revision_id")
            != receipt["revision_id"]
            or recovery["command"]["command_id"] != command_id
        ):
            raise OwnershipConflict("revision worker command has different recovery custody")
        spec = store.effective_spec(child["run_id"])
        shared.checkpoint(
            token,
            "worker-input:"
            + worker_key(child["run_id"], token)
            + ":revision:"
            + str(receipt["identity"]["plan_revision"]),
            spec,
        )
        result = json.loads(prior[0])
        return {
            "spec": spec,
            "completed": False,
            "workflow_id": result["workflow_id"],
            "resumed": True,
        }
    if not worker_revision_required(parent, child):
        raise OwnershipConflict("original worker has no affected plan revision to recover")
    receipt = _receipt(parent)
    identity = {key: receipt["identity"][key] for key in ("plan_revision", "plan_digest")}
    if row is None:
        with store._connect() as db:
            saved_row = db.execute(
                "SELECT * FROM delivery_runs WHERE run_id=?", (child["run_id"],)
            ).fetchone()
        row = dict(saved_row) if saved_row else None
    from .delivery_feature_revisions import worker_revision_affected

    if row and row["outcome"] == "delivered" and not worker_revision_affected(parent, child):
        return {"spec": child, "completed": True, "workflow_id": row["workflow_id"]}
    derived = _derive(parent, receipt, child)
    admission = {
        "revision_id": receipt["revision_id"],
        "plan_identity": identity,
        "parent_run_id": parent["run_id"],
        "predecessor_spec_digest": digest(child),
        "derived_spec_digest": digest(derived),
    }
    validate_revision_recovery(store, child, admission)
    sealed = snapshot(store, child["run_id"], _revision=admission)
    budget = shared.budget(token["issue_id"])
    state, candidate = sealed["state"], sealed["candidate"]
    associated = receipt.get("diagnostic_worker_run_id") == child["run_id"]
    gate_only = gate_only_recovery(store, sealed, admission)
    remaining = min(
        budget["maximum"] - budget["used"] + int(bool(associated or gate_only)),
        child["policy"]["max_repairs"] - state["iteration"],
    )
    if remaining < 1 and not (remaining == 0 and gate_only):
        raise OwnershipConflict("feature product repair limit exhausted")
    command = {
        "continuation_kind": KIND,
        "command_id": command_id,
        "expected_revision": state["revision"],
        "expected_iteration": state["iteration"],
        "expected_candidate_id": candidate["id"],
        "expected_candidate_head": candidate["head"],
        "additional_iterations": remaining,
    }
    shared.reserve_revision_worker(
        token,
        worker_key(child["run_id"], token),
        child["feature_worker"]["workstream_id"],
        admission,
    )
    if associated:
        iteration = state["iteration"] + 1
        alias_key = f"{child['run_id']}:{iteration}"
        shared.revision_checkpoint(
            token,
            "plan-revision:repair-alias:" + alias_key,
            {
                "revision_id": receipt["revision_id"],
                "repair_key": receipt["repair_key"],
                "generation": token["generation"],
                "reason": "Product repair at worker iteration " + str(iteration),
            },
        )
    result = admit(store, child["run_id"], command, _revision=admission)
    spec = store.effective_spec(child["run_id"])
    shared.checkpoint(
        token,
        "worker-input:"
        + worker_key(child["run_id"], token)
        + ":revision:"
        + str(receipt["identity"]["plan_revision"]),
        spec,
    )
    return {"spec": spec, "completed": False, "workflow_id": result["workflow_id"], "resumed": True}


def compact_recovery(recovery, root):
    """Keep new revision transports finite while sealing complete original custody."""
    from .delivery_metadata_recovery import _immutable

    path = root / "revision-worker-custody.json"
    _immutable(path, recovery)
    reference = {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    previous = recovery["state"]
    roles = previous.get("roles", [])
    selected = {}
    for role in reversed(roles):
        if role.get("role") != "implement":
            continue
        key = "implementation_session" if role.get("session_id") else "implement"
        if key not in selected:
            # Keep exact latest implementation receipts for native resume
            # qualification. Prior review/gate history remains in custody_ref.
            selected[key] = deepcopy(role)
    state = {
        key: deepcopy(previous.get(key))
        for key in (
            "run_id",
            "phase",
            "execution_state",
            "outcome",
            "cleanup",
            "error",
            "revision",
            "iteration",
            "candidate_revision",
            "candidate",
            "pull_request",
            "decision",
            "tracker",
        )
    }
    state["error"] = str(state.get("error") or "")[:4000] or None
    state["decision"] = None
    state["intake"] = None
    state["roles"] = list(reversed(list(selected.values())))
    state["findings"] = [str(value)[:4000] for value in previous.get("findings", [])[-16:]]
    state["checks"] = {
        "retained_checkpoint": {
            "state": "preserved",
            "state_sha256": digest(previous),
            "custody_ref": reference,
        }
    }
    state["usage"] = {"retained_checkpoint_sha256": digest(previous.get("usage"))}
    result = {
        key: deepcopy(recovery[key])
        for key in (
            "kind",
            "command",
            "command_digest",
            "execution_spec",
            "execution_candidate",
            "maximum_iteration",
            "session_id",
            "feature_plan_revision",
            "predecessor_result_digest",
        )
    }
    result.update(custody_version=1, custody_ref=reference, state=state)
    for key in (
        "session_custody",
        "pending_repair",
        "resume_stage",
        "resume_iteration",
        "gate_evidence",
    ):
        if key in recovery:
            result[key] = deepcopy(recovery[key])
    if len(canonical_json(result).encode()) > 256 * 1024:
        raise ValueError("revision worker transport exceeds its bounded custody envelope")
    return result


def retained_recovery(recovery):
    """Hydrate only for local custody checks; never send the retained history again."""
    if not recovery or recovery.get("custody_version") != 1:
        return recovery
    from .delivery_resources import read_private
    from .delivery_stopped_resume import namespace

    spec = recovery["execution_spec"]
    reference = recovery.get("custody_ref") or {}
    path = Path(reference.get("path", ""))
    expected = Path(spec["state_dir"]) / namespace(recovery) / "revision-worker-custody.json"
    if path != expected or path.resolve(strict=True) != path:
        raise ValueError("revision worker custody reference escaped its sealed namespace")
    value = read_private(path)
    content = path.read_bytes()
    if (
        hashlib.sha256(content).hexdigest() != reference.get("sha256")
        or json.loads(content) != value
    ):
        raise ValueError("retained revision worker custody bytes changed")
    if (
        any(
            recovery.get(key) != value.get(key)
            for key in (
                "kind",
                "command",
                "command_digest",
                "execution_spec",
                "execution_candidate",
                "maximum_iteration",
                "session_id",
                "feature_plan_revision",
                "predecessor_result_digest",
                "session_custody",
                "pending_repair",
            )
        )
        or compact_recovery_view(value, path) != recovery
    ):
        raise ValueError("revision worker envelope differs from its immutable custody")
    return value


def compact_recovery_view(recovery, path):
    # The immutable file already exists; _immutable performs only exact readback.
    return compact_recovery(recovery, path.parent)


def gate_evidence(seal):
    """A gate correction may reuse only an exact passed implementation tree."""
    candidate = seal["candidate"]
    for role in reversed(seal["state"].get("roles", [])):
        if (
            role.get("role") != "implement"
            or role.get("status") != "pass"
            or role.get("cleanup") != "confirmed"
            or role.get("session_id") != seal.get("session_id")
            or role.get("candidate", {}).get("content_sha256") != candidate["content_sha256"]
        ):
            continue
        raw = {
            key: value
            for key, value in role.items()
            if key
            not in {
                "role",
                "iteration",
                "candidate",
                "attempt_id",
                "input_candidate_id",
                "provider",
            }
        }
        matches = [
            row
            for row in seal["attempts"]
            if row["role"] == "implement"
            and row["iteration"] == role["iteration"]
            and row["state"] == "finished"
            and row["cleanup"] == "confirmed"
            and row["session_id"] == role.get("session_id")
            and json.loads(row["result_json"] or "null") == raw
        ]
        if len(matches) == 1:
            return {
                "candidate_id": candidate["id"],
                "content_sha256": candidate["content_sha256"],
                "implementation_job_key": matches[0]["job_key"],
                "implementation_result_sha256": digest(raw),
                "session_id": seal["session_id"],
            }
    return None


def gate_only_recovery(store, seal, admission):
    from .delivery_feature_revisions import worker_revision_affected

    parent = store.effective_spec(admission["parent_run_id"])
    if seal["predecessor_spec"]["feature_worker"]["kind"] != "chunk" or worker_revision_affected(
        parent,
        seal["predecessor_spec"],
        through_revision=admission["plan_identity"]["plan_revision"],
        implementation=True,
    ):
        return None
    return gate_evidence(seal)
