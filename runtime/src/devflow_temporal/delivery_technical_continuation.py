"""Retained technical-successor custody and effective-spec readers."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker
from .delivery_continuation import session_state_digest
from .delivery_policy_recovery import work_binding
from .delivery_resources import _ancestors, read_private
from .delivery_resources import _identity as root_identity
from .delivery_technical_integration import reference

PACKET_FIELDS = {"prospective_path", "prospective_sha256"}


def _authority(spec, payload):
    authority = reference(payload["authority_path"], payload["authority_sha256"])
    limits = authority.get("technical_limits", {})
    required = {
        "max_successor_commands_per_run": 1,
        "max_additional_native_preparation_generations_per_run": 1,
        "max_total_additional_native_preparation_generations": 2,
        "max_owned_probe_attempts_per_additional_generation": 2,
        "native_renewal_provider_turns": 0,
        "native_renewal_implementation_turns": 0,
        "additional_feature_repair_grants": 0,
        "907_implementation_turns": 0,
        "907_iteration_ceiling": 4,
        "1005_iteration_ceiling": 5,
    }
    if (
        authority.get("decision_owner") != "main task"
        or authority.get("new_user_approval_required") is not False
        or not isinstance(authority.get("authority_source"), str)
        or not authority["authority_source"]
        or any(
            type(limits.get(key)) is not int or limits[key] != value
            for key, value in required.items()
        )
    ):
        raise ValueError("technical successor authority exceeds the original finite bounds")
    scopes = [authority.get("907", {}), authority.get("1005_integration", {})]
    matching = [scope for scope in scopes if scope.get("run_id") == spec["run_id"]]
    if len(matching) != 1 or matching[0].get("work_id") != spec["work_id"]:
        raise ValueError("technical successor does not own this accepted original")
    scope = matching[0]
    integration = scope is scopes[1]
    if (
        integration != (PACKET_FIELDS <= set(payload))
        or spec["base_sha"] != scope.get("frozen_original_base" if integration else "frozen_base")
        or payload["expected_iteration"] != 4
        or payload["expected_pr_head"]
        != scope.get("owned_predecessor_head" if integration else "published_head")
        or (
            integration
            and (
                type(scope.get("max_integration_operations")) is not int
                or scope["max_integration_operations"] != 1
            )
        )
        or (not integration and scope.get("allowed_source_change") is not False)
    ):
        raise ValueError("technical successor changed its exact source/base/iteration authority")
    bindings = authority.get("trigger_bindings", {})
    evidence = {key: reference(value["path"], value["sha256"]) for key, value in bindings.items()}
    if not {
        "sealed_actual_failures",
        "consumed_native_renewal_authority",
        "consumed907_gates_only_authority",
        "consumed1005_metadata_authority",
        "1005_conflict_classification",
        "1005_three_tree_inputs",
    } <= set(evidence):
        raise ValueError("technical successor predecessor authority is incomplete")
    return authority, scope, integration, evidence


def native_predecessor(predecessor, authority):
    """Authenticate consumed54, never a reset from the first native generation."""
    from .delivery_native_renewal import _authority as renewal_authority
    from .delivery_native_renewal import _old_proof, effective_spec

    original = predecessor["original_spec"]
    effective = effective_spec(original, predecessor["recovery"])
    receipt = read_private(Path(predecessor["recovery"]["native_preparation_renewal"]["path"]))
    bound = authority["trigger_bindings"]["consumed_native_renewal_authority"]
    if (
        receipt["authority_path"] != bound["path"]
        or receipt["authority_sha256"] != bound["sha256"]
        or canonical_json(renewal_authority(original, receipt["command"]))
        != canonical_json(receipt["authority"])
        or canonical_json(effective) != canonical_json(predecessor["spec"])
    ):
        raise ValueError("technical native successor immediate consumed generation changed")
    _old_proof(original)
    _old_proof(effective)
    return receipt


def _observe_resources(spec, *, unknown_allowed):
    from .delivery_resources import observe_finalized_resources

    return observe_finalized_resources(spec, unknown_allowed=unknown_allowed)

def _quiescent(store, spec):
    with store._connect() as db:
        if db.execute("SELECT 1 FROM delivery_attempts WHERE state != 'finished'").fetchone():
            raise ValueError("technical continuation requires global native quiescence")
        if db.execute(
            "SELECT 1 FROM delivery_runs WHERE run_id != ? AND "
            "execution_state IN ('running','queued')",
            (spec["run_id"],),
        ).fetchone():
            raise ValueError("technical continuation cannot overlap foreign managed work")
        work_binding(store, spec, db)
        return store.state.claim_for(db, spec["work_id"])


def _retained_db(db, recovery):
    root = Path(recovery["spec"]["state_dir"]) / "technical-successor"
    _ancestors(root / "intent.json")
    root_identity(root)
    if root.lstat().st_mode & 0o777 != 0o700:
        raise ValueError("technical successor namespace is not private and owned")
    seal = read_private(root / "intent.json")
    row = db.execute(
        "SELECT intent_json FROM delivery_technical_successors WHERE run_id=?",
        (recovery["spec"]["run_id"],),
    ).fetchone()
    if (
        not row
        or canonical_json(json.loads(row[0])) != canonical_json(seal)
        or digest(seal) != recovery.get("intent_sha256")
    ):
        raise ValueError("technical successor immutable intent or durable authority changed")
    for key, value in seal.items():
        if key not in {"candidate", "execution_spec"} and canonical_json(
            recovery.get(key)
        ) != canonical_json(value):
            raise ValueError("technical successor retained predecessor changed")
    _authority(seal["spec"], seal["command"])
    if seal["integration"]:
        packet = reference(
            seal["command"]["prospective_path"], seal["command"]["prospective_sha256"]
        )
        if (
            packet.get("authority_sha256") != seal["command"]["authority_sha256"]
            or packet.get("expected_complete_tree_sha1") != seal["integration"]["tree"]
            or packet.get("expected_file_count") != seal["integration"]["file_count"]
        ):
            raise ValueError("technical successor complete integration evidence changed")
    for name in ("manifest.json", "finalization.json"):
        path = root / "predecessor-resources" / name
        raw = path.read_bytes()
        read_private(path)
        expected = seal["resources"][
            "manifest_sha256" if name == "manifest.json" else "finalization_sha256"
        ]
        if hashlib.sha256(raw).hexdigest() != expected:
            raise ValueError("technical successor predecessor resource bytes changed")
    closure = read_private(root / "closure.json")
    closure_reference = recovery.get("closure_reference", {})
    cleanup_path = root / "closure-finalization.json"
    cleanup = read_private(cleanup_path)
    if (
        closure.get("predecessor_resources") != seal["resources"]
        or closure.get("state") != "confirmed"
        or closure_reference.get("path") != str(root / "closure.json")
        or closure_reference.get("sha256")
        != hashlib.sha256((root / "closure.json").read_bytes()).hexdigest()
        or cleanup.get("state") != "confirmed"
        or cleanup.get("process_cleanup") != "observed-native-confirmed"
        or cleanup.get("resource_cleanup") != "confirmed"
        or closure.get("cleanup", {}).get("receipt_sha256")
        != hashlib.sha256(cleanup_path.read_bytes()).hexdigest()
        or canonical_json(cleanup)
        != canonical_json(
            {
                key: value
                for key, value in closure.get("cleanup", {}).items()
                if key not in {"receipt", "receipt_sha256"}
            }
        )
    ):
        raise ValueError("technical successor append-only cleanup closure changed")
    return seal


def namespace_custody(db, spec, recovery):
    seal = _retained_db(db, recovery)
    root = Path(spec["state_dir"]) / "technical-successor"
    if canonical_json(read_private(root / "admission.json")) != canonical_json(
        recovery
    ) or canonical_json(recovery["execution_spec"]) != canonical_json(spec):
        raise ValueError("technical namespace admission or effective spec changed")
    from .delivery_native_renewal import effective_spec as native_spec

    native_spec(seal["proposed_spec"], recovery, technical=True)


def _retained(store, recovery):
    with store._connect() as db:
        return _retained_db(db, recovery)


def effective_spec(store, original, recovery):
    """The typed lineage is shared by native, namespace and public projection readers."""
    seal = _retained(store, recovery)
    if canonical_json(original) != canonical_json(seal["spec"]):
        raise ValueError("technical successor skipped its immediately consumed effective spec")
    from .delivery_native_renewal import effective_spec as native_spec
    from .delivery_technical_integration import readback as integration_readback

    integration_readback(recovery["execution_spec"], recovery)
    value = native_spec(seal["proposed_spec"], recovery, technical=True)
    if canonical_json(value) != canonical_json(recovery["execution_spec"]):
        raise ValueError("technical successor native/base applicability changed")
    return value


def readback(store, spec, recovery, *, require_claim=True):
    _retained(store, recovery)
    if canonical_json(store.effective_spec(spec["run_id"])) != canonical_json(spec):
        raise ValueError("technical successor current execution spec changed")
    from .delivery_native_preparation import verify_native_spec
    from .delivery_repair import published_identity

    verify_native_spec(spec)
    published_identity(DeliveryBroker(store, spec), recovery["candidate"], recovery["publication"])
    with store._connect() as db:
        work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec["work_id"])
    if (require_claim and claim is None) or (
        claim is not None and claim["owner"] != f"external:devflow:{spec['run_id']}"
    ):
        raise ValueError("technical successor lost its owned claim before gates")
    if (
        session_state_digest(
            Path(spec["state_dir"]) / "role-homes/implement", recovery["session_id"]
        )
        != recovery["session_sha256"]
    ):
        raise ValueError("technical successor changed original session evidence")
    return {
        "candidate": recovery["candidate"],
        "publication": recovery["publication"],
        "maximum_iteration": 4,
        "additional_implementation_turns": 0,
    }
