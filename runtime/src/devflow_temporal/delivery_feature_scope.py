"""One explicit file permission correction for a retained stopped feature build."""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

from .contracts import digest
from .delivery_config import scope_amendment_config
from .delivery_execution_registry import OwnershipConflict

FIELDS = {"added_paths", "amended_config_path", "amended_config_sha256"}
MARKER = "feature_scope_amendment"


def _binding(seal):
    return {
        "spec_sha256": digest(seal["predecessor_spec"]),
        "state_sha256": digest(seal["state"]),
        "attempts_sha256": digest(seal["attempts"]),
        "effects_sha256": digest(seal["effects"]),
        "recovery_sha256": digest(seal["original_recovery"]),
        "session_custody_sha256": digest(seal.get("session_custody")),
        "candidate": seal["candidate"],
        "session_id": seal["session_id"],
        "workflow_id": seal["closed"]["workflow_id"],
        "execution_run_id": seal["closed"]["execution_run_id"],
    }


def amendment(store, parent, command, successor_id, *, shared=None):
    """Observe authentic stopped custody before continuation can produce effects."""
    from .delivery_feature_execution import plan_for, registry, worker_key
    from .delivery_feature_pass import checkpoints
    from .delivery_feature_revisions import adopted_plan_identity, validate_diagnostic
    from .delivery_stopped_resume import review_snapshot, scope_spec

    shared, token = shared or registry(parent), parent["feature_delivery"]["owner"]
    current = shared.current(token["issue_id"])
    if (parent.get(MARKER) or current is None or shared.token(current) != token
            or current["state"] != "stopped"
            or token["store_path"] != str(store.config.tracking_db)):
        raise OwnershipConflict("scope continuation requires its exact settled feature owner")
    scope = {key: command[key] for key in FIELDS}
    _configuration(parent, scope)
    identity = adopted_plan_identity(parent, shared)
    if (identity["plan_digest"] != digest(plan_for(parent))
            or parent.get("feature_plan_revision", {
                "plan_revision": 1, "plan_digest": identity["plan_digest"]})
            != {key: identity[key] for key in ("plan_revision", "plan_digest")}):
        raise OwnershipConflict("scope continuation belongs to a superseded feature plan")
    stopped = []
    values = checkpoints(parent, shared=shared)
    for key, assignment in values.items():
        if not key.startswith("assignment:") or not key.endswith(":build"):
            continue
        if assignment["store_path"] != token["store_path"]:
            raise OwnershipConflict("scope continuation worker belongs to another store")
        with store._connect() as db:
            row = db.execute("SELECT outcome,error FROM delivery_runs WHERE run_id=?",
                             (assignment["run_id"],)).fetchone()
        if row and row["outcome"] == "blocked" and row["error"] == (
                "workstream identified a planning defect"):
            stopped.append(assignment)
    if len(stopped) != 1:
        raise OwnershipConflict("scope continuation requires one retained planning-defect build")
    assignment = stopped[0]
    seal = review_snapshot(store, assignment["run_id"])
    child = seal["predecessor_spec"]
    worker = child.get("feature_worker", {})
    with shared.connect() as db:
        row = db.execute("SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                         (token["issue_id"], worker_key(child["run_id"], token))).fetchone()
    latest = next((role for role in reversed(seal["state"].get("roles", []))
                   if role.get("role") == "implement"), {})
    if (not row or row["state"] != "finished" or row["generation"] != token["generation"]
            or row["workstream_id"] != worker.get("workstream_id")
            or child["feature_delivery"]["owner"]["issue_id"] != token["issue_id"]
            or child["feature_delivery"]["owner"]["store_path"] != token["store_path"]
            or worker.get("kind") != "build" or worker.get("chunk_id") != assignment["chunk_id"]
            or latest.get("status") not in {"blocked", "findings"}
            or latest.get("finish_reason") != "done" or not latest.get("findings")
            or not seal["session_id"] or latest.get("session_id") != seal["session_id"]):
        raise OwnershipConflict("scope continuation lost its closed implementation finding")
    diagnostic = latest.get("planning_defect")
    validate_diagnostic(store, parent, diagnostic, identity=identity,
                        candidate_id=seal["candidate"]["id"], shared=shared)
    if (diagnostic["candidate_id"] != seal["candidate"]["id"]
            or diagnostic != seal["state"].get("checks", {}).get("planning_defect")):
        raise OwnershipConflict("scope continuation diagnostic changed its retained candidate")
    receipt = {"predecessor": token, "successor_id": successor_id,
               "worker_run_id": child["run_id"], "chunk_id": worker["chunk_id"],
               "plan_identity": {key: identity[key] for key in ("plan_revision", "plan_digest")},
               "scope": scope, "binding": _binding(seal)}
    # Use the ordinary file/candidate checks without admitting a native process.
    scope_spec(store, seal, scope, _feature_scope=receipt)
    return receipt


def _configuration(parent, scope):
    path, sha = scope["amended_config_path"], scope["amended_config_sha256"]
    if not isinstance(path, str) or not isinstance(sha, str):
        raise ValueError("scope amendment configuration must be an absolute private file")
    return scope_amendment_config(parent, Path(path), sha, scope["added_paths"])


def _apply(spec, receipt, config):
    result = deepcopy(spec)
    result["policy"]["allowed_paths"] += receipt["scope"]["added_paths"]
    result.update(config_path=str(config.path), config_digest=digest(config.raw),
                  policy_digest=digest(result["policy"]), **{MARKER: deepcopy(receipt)})
    return result


def prepare_submission(store, spec, receipt=None):
    """Carry the frozen feature scope through the existing successor admission."""
    from .delivery_execution_config import preserve_execution_config

    predecessor = spec.get("feature_predecessor")
    if not predecessor:
        if receipt:
            raise OwnershipConflict("scope amendment requires a feature predecessor")
        return
    original = store.effective_spec(predecessor["run_id"])
    retained = original.get(MARKER)
    if receipt is None and retained is None:
        return
    if receipt:
        if amendment(store, original, receipt["scope"], spec["run_id"]) != receipt:
            raise OwnershipConflict("scope continuation changed before admission")
        config = _configuration(original, receipt["scope"])
        effective = _apply(original, receipt, config)
        preserve_execution_config(config)
    else:
        authenticate_receipt(store, retained)
        effective = original
    apply_submission(spec, effective)


def apply_submission(spec, effective):
    """Apply the same frozen scope authority to a prospective or real successor."""
    identities = {key: spec[key] for key in (
        "run_id", "command_id", "request_digest", "state_dir", "checkout", "branch",
        "feature_predecessor")}
    spec.clear()
    spec.update(deepcopy(effective), **identities)
    for key in ("feature_delivery", "preparation", "prepared_environment"):
        spec.pop(key, None)


def authenticate_receipt(store, receipt):
    with store._connect() as db:
        return receipt_config(db, receipt)


def receipt_config(db, receipt):
    rows = {row[0]: json.loads(row[1]) for row in db.execute(
        "SELECT run_id,request_json FROM delivery_runs WHERE run_id IN (?,?)",
        (receipt["successor_id"], receipt["predecessor"]["run_id"]))}
    admitted = rows.get(receipt["successor_id"], {})
    if admitted.get(MARKER) != receipt:
        raise OwnershipConflict("feature scope lost its immutable continuation admission")
    predecessor = rows[receipt["predecessor"]["run_id"]]
    if predecessor["feature_delivery"]["owner"] != receipt["predecessor"]:
        raise OwnershipConflict("feature scope changed its predecessor owner")
    return _configuration(predecessor, receipt["scope"])


def worker_scope(store, seal, command, receipt=None):
    """Amend a stopped worker without re-admitting its derived gates or base."""
    if receipt is None:
        from .delivery_feature_execution import registry

        child = seal["predecessor_spec"]
        shared = registry(child)
        current = shared.current(child["feature_delivery"]["owner"]["issue_id"])
        parent = store.effective_spec(current["run_id"]) if current else {}
        receipt = parent.get(MARKER)
        if (not receipt or parent["feature_delivery"]["owner"] != shared.token(current)
                or current["state"] != "active"):
            raise OwnershipConflict("worker scope requires its admitted feature continuation")
        config = authenticate_receipt(store, receipt)
    else:
        from .delivery_execution_config import is_observation_mode

        if is_observation_mode():
            from .delivery_feature_handoff import readonly_feature_spec

            predecessor = readonly_feature_spec(store, receipt["predecessor"]["run_id"])
        else:
            predecessor = store.effective_spec(receipt["predecessor"]["run_id"])
        config = _configuration(predecessor, receipt["scope"])
    if (receipt["worker_run_id"] != seal["predecessor_spec"]["run_id"]
            or receipt["scope"] != {key: command[key] for key in FIELDS}
            or receipt["binding"] != _binding(seal)):
        raise OwnershipConflict("worker scope differs from the sealed stopped implementation")
    return _apply(seal["predecessor_spec"], receipt, config)
