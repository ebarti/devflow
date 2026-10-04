"""One append-only stopped-resource closure and optional current-payload native child."""

from __future__ import annotations

import json
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker
from .delivery_investigation_adjudication import (
    FIELDS,
    _bytes,
    _candidate_checkpoint,
    _controller,
)
from .delivery_metadata_recovery import _immutable
from .delivery_policy_recovery import _rows, work_binding
from .delivery_resources import _ancestors, private_directory, read_private
from .delivery_technical_integration import reference

KIND = "stopped_resource_closure"
ABANDON = "abandon_pending_resource_closure"
LIMITS = {
    "iteration6": False,
    "native_generations_907": 0,
    "new_implementation_turns": 0,
    "new_resource_closure_transactions": 1,
    "probe_attempts_within_generation": 2,
    "supplemental_native_generations_1005": 2,
    "total_original1005_iteration_cap": 4,
}


def _request(payload):
    from .delivery_investigation_adjudication import KIND as ADJUDICATION
    from .delivery_investigation_adjudication import _request as check

    if (
        not isinstance(payload, dict)
        or set(payload) != FIELDS
        or payload["continuation_kind"] != KIND
    ):
        raise ValueError("resource closure requires its explicit bounded typed request")
    check({**payload, "continuation_kind": ADJUDICATION})


def _authority(payload, run_id, *, retained=None):
    value = reference(payload["authority_path"], payload["authority_sha256"])
    if (
        value.get("kind")
        != "bounded_original1005_resource_closure_and_current_payload_native_renewal"
        or value.get("schema") != 1
        or value.get("owner") != "/root"
        or value.get("run_id") != run_id
        or value.get("initial_iteration") != 4
        or value.get("finite_limits") != LIMITS
    ):
        raise ValueError("resource closure authority changes its original or finite limits")
    bindings = {b["name"]: b for b in value["bindings"]}
    if set(bindings) != {
        "actual_failure",
        "physical_quiescence",
        "reliability_goal",
        "immediate_consumed_native_parent",
    } or len(bindings) != len(value["bindings"]):
        raise ValueError("resource closure authority has incomplete immediate ancestry")
    failure = reference(bindings["actual_failure"]["path"], bindings["actual_failure"]["sha256"])
    physical = reference(
        bindings["physical_quiescence"]["path"], bindings["physical_quiescence"]["sha256"]
    )
    reference(
        bindings["reliability_goal"]["path"],
        bindings["reliability_goal"]["sha256"],
    )
    _bytes(
        bindings["immediate_consumed_native_parent"]["path"],
        bindings["immediate_consumed_native_parent"]["sha256"],
    )
    if (
        failure.get("run_id") != run_id
        or failure.get("runtime_candidate") != value["original_runtime_sha"]
        or failure.get("cause_confirmed") is not True
        or failure["run"]["candidate"] != value["candidate"]
        or failure["run"]["cleanup"] != "unknown"
        or failure["run"]["sequence"] != value["initial_terminal_sequence"]
        or failure.get("individual_process_count") != 68
        or failure.get("all_process_receipts_confirmed") is not True
        or physical.get("originals", {}).get(run_id, {}).get("registered_journals") != 68
        or physical["originals"][run_id].get("all_owned_actors_stopped_and_ports_clear") is not True
    ):
        raise ValueError(
            "resource closure no longer binds the confirmed inherited-checkpoint defect"
        )
    for binding in failure["evidence"].values():
        file = Path(binding["original"])
        if retained and file in {
            Path(retained["spec"]["state_dir"]) / "resources" / name
            for name in ("manifest.json", "finalization.json")
        }:
            key = "manifest_sha256" if file.name == "manifest.json" else "finalization_sha256"
            if binding["sha256"] != retained["resources"][key]:
                raise ValueError("resource closure archived predecessor hash changed")
            # Finalization updates the active registry; the immutable admission owns
            # these exact original bytes, rather than adopting a new UNKNOWN result.
            file = (
                Path(retained["spec"]["state_dir"])
                / "resource-closure/predecessor-resources"
                / file.name
            )
        _bytes(file, binding["sha256"])
    _bytes(failure["history"]["path"], failure["history"]["sha256"])
    return value, bindings, failure


def _parent(store, spec, previous, authority, bindings):
    from .delivery_native_renewal import _old_proof
    from .delivery_native_renewal import effective_spec as native_spec
    from .delivery_technical_continuation import _retained

    if previous.get("kind") != "accepted_technical_successor" or not previous.get("integration"):
        raise ValueError("resource closure skipped its consumed integrated technical predecessor")
    seal = _retained(store, previous)
    if native_spec(seal["proposed_spec"], previous, technical=True) != spec:
        raise ValueError("resource closure changed the consumed immediate native spec")
    parent = Path(previous["native_preparation_renewal"]["path"])
    receipt = read_private(parent)
    expected = bindings["immediate_consumed_native_parent"]
    if (
        expected["path"] != str(parent.with_name("proof.json"))
        or receipt["retained_proof"] != {"path": expected["path"], "sha256": expected["sha256"]}
        or receipt["source_revision"] != authority["original_runtime_sha"]
    ):
        raise ValueError("resource closure immediate parent proof/source changed")
    _old_proof(spec)
    return {
        "generation": previous["native_preparation_renewal"],
        "proof": expected,
        "spec_sha256": digest(spec),
    }


def _readiness(store, spec, payload, authority, parent):
    from .delivery_native_preparation import native_identity, verify_native_spec
    from .delivery_native_renewal import _payload_only

    identity = native_identity(spec)
    before = spec["policy"]["native_identity"]
    _payload_only(before, identity)
    controller = _controller(store, spec, payload)
    if identity == before:
        verify_native_spec(spec)
    return {
        "required": identity != before,
        "identity": identity,
        "before": before,
        "authority": authority,
        "source": Path(controller["installed_source_root"]),
        "source_revision": controller["source_revision"],
        "config_sha256": controller["config_sha256"],
        "parent": parent,
    }, controller


def _browser_checkpoint(actual, recorded):
    """Failure reports omit diagnostic text and the separately bound native journal."""
    metadata = {"diagnostic", "native_process"}
    return {k: v for k, v in actual.items() if k not in metadata} == {
        k: v for k, v in recorded.items() if k not in metadata
    }


def _snapshot(store, run_id, payload):
    authority, bindings, failure = _authority(payload, run_id)
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row["recovery_json"])
    parent = _parent(store, spec, previous, authority, bindings)
    readiness, controller = _readiness(store, spec, payload, authority, parent)
    closed = store._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
    state = closed["result"]
    candidate = {k: v for k, v in authority["candidate"].items() if k != "revision"}
    _candidate_checkpoint(state, authority, row)
    if (
        spec["work_id"] != authority["work_id"]
        or spec["provider"] != "codex"
        or row["outcome"] != "blocked"
        or row["cleanup"] != "unknown"
        or row["iteration"] != 4
        or row["protocol_revision"] != payload["expected_revision"]
        or closed["workflow_id"] != row["workflow_id"]
        or closed["request_digest"] != spec["request_digest"]
        or closed["recovery_digest"] != digest(previous)
        or state.get("revision") != payload["expected_revision"]
        or state.get("iteration") != 4
        or state.get("outcome") != "blocked"
        or state.get("cleanup") != "unknown"
        or state.get("error") != "repair limit exhausted"
        or state.get("candidate") != candidate
        or payload["expected_candidate_id"] != candidate["id"]
        or payload["expected_pr_head"] != candidate["head"]
        or state.get("pull_request", {}).get("number") != payload["expected_pr_number"]
        or not _browser_checkpoint(
            state.get("checks", {}).get("browser_qa", {}), failure["browser"]
        )
        or state["checks"].get("resource_cleanup", {}).get("state") != "unknown"
        or claim != failure["run"]["tracker"]["observed"]["claim"]
        or any(a["state"] != "finished" or a["cleanup"] == "unknown" for a in attempts)
        or any(e["state"] not in {"complete", "failed"} for e in effects)
        or any(r.get("cleanup") != "confirmed" for r in state.get("roles", []))
    ):
        raise ValueError(
            "resource closure lost its exact UNKNOWN, claim or fresh title failure checkpoint"
        )
    from .delivery_repair import _historical_browser_rejection, published_identity
    from .delivery_technical_continuation import _observe_resources, _quiescent

    _historical_browser_rejection(state["checks"]["browser_qa"], spec)
    if _quiescent(store, spec) != claim:
        raise ValueError("resource closure original claim changed")
    observed = _observe_resources(spec, unknown_allowed=True)
    if len(observed["journal_sha256"]) != 68:
        raise ValueError("resource closure current actor inventory changed")
    with store._connect() as db:
        work_binding(store, spec, db)
        sequence = db.execute(
            "SELECT MAX(sequence) FROM delivery_events WHERE run_id=?", (run_id,)
        ).fetchone()[0]
    if sequence != authority["initial_terminal_sequence"]:
        raise ValueError("resource closure terminal sequence changed")
    published_identity(DeliveryBroker(store, spec), state["candidate"], state["pull_request"])
    return {
        "kind": KIND,
        "command": payload,
        "authority": authority,
        "controller": controller,
        "spec": spec,
        "original_row": row,
        "original_recovery": previous,
        "closed": closed,
        "state": state,
        "attempts": attempts,
        "effects": effects,
        "claim": claim,
        "parent": parent,
        "resources": observed,
        "readiness": {k: str(v) if isinstance(v, Path) else v for k, v in readiness.items()},
    }, readiness


def custody(db, recovery):
    root = Path(recovery["spec"]["state_dir"]) / "resource-closure"
    _ancestors(root / "admission.json")
    intent = read_private(root / "intent.json")
    row = db.execute(
        "SELECT intent_json,state FROM delivery_resource_closures WHERE run_id=?",
        (recovery["spec"]["run_id"],),
    ).fetchone()
    if (
        not row
        or row[1] != "queued"
        or json.loads(row[0]) != intent
        or recovery.get("intent_sha256") != digest(intent)
        or canonical_json(read_private(root / "admission.json")) != canonical_json(recovery)
        or any(recovery.get(k) != v for k, v in intent.items()
               if k not in {"candidate", "publication", "execution_spec"})
    ):
        raise ValueError("resource closure immutable durable admission changed")
    _authority(recovery["command"], recovery["spec"]["run_id"], retained=intent)
    return intent


def effective_spec(store, original, recovery):
    with store._connect() as db:
        seal = custody(db, recovery)
    if original != seal["spec"]:
        raise ValueError("resource closure skipped its immediate effective native parent")
    from .delivery_native_renewal import effective_spec as native_spec

    return native_spec(original, recovery, supplemental=True)


def readback(store, spec, recovery):
    with store._connect() as db:
        seal = custody(db, recovery)
        work_binding(store, spec, db)
    authority, bindings, _ = _authority(recovery["command"], spec["run_id"], retained=seal)
    _parent(store, seal["spec"], recovery["original_recovery"], authority, bindings)
    _controller(store, spec, recovery["command"])
    from .delivery_native_preparation import verify_native_spec
    from .delivery_repair import published_identity

    verify_native_spec(spec)
    if store.effective_spec(spec["run_id"]) != spec:
        raise ValueError("resource closure current native spec changed")
    from .delivery_technical_continuation import _observe_resources

    row, _, _, claim = _rows(store, spec["run_id"])
    if row["outcome"] is None and row["phase"] not in {
        "resource_closure_queued", "resource_closure_preflight",
    }:
        # Idempotent public readback may overlap the owned check processes.
        # It authenticates the existing admission without claiming final cleanup.
        published_identity(DeliveryBroker(store, spec), recovery["candidate"],
                           recovery["publication"])
        return {"state": "observed", "historical_unknown_retained": True,
                "current_payload_verified": True, "cleanup": "pending"}
    observed = _observe_resources(spec, unknown_allowed=True)
    if any(observed["journal_sha256"].get(path) != sha
           for path, sha in seal["resources"]["journal_sha256"].items()):
        raise ValueError("resource closure current actor inventory or birth evidence changed")
    if row["outcome"] is None:
        if (
            claim != seal["claim"]
            or row["workflow_id"] != f"delivery-{spec['run_id']}-resource-closure-1"
        ):
            raise ValueError("resource closure lost its fresh stopped resource/claim checkpoint")
    elif row["outcome"] is not None:
        final = read_private(Path(spec["state_dir"]) / "resources/finalization.json")
        check = json.loads(row["checks_json"])["resource_cleanup"]
        if (
            row["cleanup"] != "confirmed"
            or claim is not None
            or check.get("state") != "confirmed"
            or final.get("state") != "confirmed"
            or check.get("receipt_sha256") != observed["finalization_sha256"]
        ):
            raise ValueError("resource closure duplicate lacks actual confirmed cleanup readback")
    for name, key in (
        ("manifest.json", "manifest_sha256"),
        ("finalization.json", "finalization_sha256"),
    ):
        _bytes(
            Path(spec["state_dir"]) / "resource-closure/predecessor-resources" / name,
            seal["resources"][key],
        )
    published_identity(DeliveryBroker(store, spec), recovery["candidate"], recovery["publication"])
    return {
        "state": "observed",
        "historical_unknown_retained": True,
        "current_payload_verified": True,
    }


def _renewed_candidate(store, original, renewed):
    # The new namespace is not durable until admission commits. Source identity
    # is read through the existing admitted namespace; renewal changes only policy.
    from .delivery_native_renewal import _same_execution

    if original != renewed:
        _same_execution(original, renewed)
    return {**DeliveryBroker(store, original).candidate(),
            "policy_digest": renewed["policy_digest"]}


def abandon_pending(store, run_id, payload, *, preflight=False):
    """Retire one unqueued admission after a controller install; retain all bytes."""
    _request({**payload, "continuation_kind": KIND})
    if payload.get("continuation_kind") != ABANDON:
        raise ValueError("pending abandonment requires its explicit discriminator")
    from .delivery_native_guard import reject_nested_controller
    from .delivery_preparation import _lock

    reject_nested_controller()
    command_digest = digest({"run_id": run_id, **payload})
    with store._connect() as db:
        command = db.execute("SELECT * FROM delivery_commands WHERE command_id=?",
                             (payload["command_id"],)).fetchone()
        prior = db.execute("SELECT * FROM delivery_resource_closures WHERE run_id=?",
                           (run_id,)).fetchone()
    if command:
        if command["request_digest"] != command_digest:
            raise ValueError("pending abandonment command belongs to different inputs")
        return {**json.loads(command["response_json"]), "existing": True}
    if not prior or prior["state"] != "pending":
        raise ValueError("only an unqueued pending resource closure can be abandoned")
    old = json.loads(prior["intent_json"])
    fresh, _ = _snapshot(store, run_id, {**payload, "continuation_kind": KIND})
    if any(fresh[k] != old[k] for k in (
        "spec", "original_row", "original_recovery", "closed", "state", "attempts",
        "effects", "claim", "parent", "resources",
    )):
        raise ValueError("pending abandonment changed the original stopped checkpoint")
    if fresh["controller"]["source_revision"] == old["controller"]["source_revision"]:
        raise ValueError("pending abandonment requires a different reviewed controller install")
    root = Path(old["spec"]["state_dir"]) / "resource-closure"
    archive = root.with_name("resource-closure-abandoned-" + digest(old))
    if any(p != archive for p in root.parent.glob("resource-closure-abandoned-*")):
        raise ValueError("this run already exhausted its one pending abandonment")
    source = archive if archive.exists() else root
    if read_private(source / "intent.json") != old:
        raise ValueError("pending abandonment lost immutable original admission")
    response = {"run_id": run_id, "phase": "pending_resource_closure_abandoned",
                "archive": str(archive), "original_checkpoint_unchanged": True,
                "additional_iterations": 0, "existing": False}
    if preflight:
        return {**response, "preflight": True}
    with _lock(root.parent / "resource-closure-abandon.lock"):
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM delivery_resource_closures WHERE run_id=?",
                                 (run_id,)).fetchone()
            original = db.execute("SELECT * FROM delivery_runs WHERE run_id=?",
                                  (run_id,)).fetchone()
            if (dict(current) != dict(prior) or dict(original) != old["original_row"]
                    or store.state.claim_for(db, old["spec"]["work_id"]) != old["claim"]):
                raise ValueError("pending abandonment checkpoint changed before effects")
            # A crash after rename can retry using the exact archived intent.
            if not archive.exists():
                root.rename(archive)
            elif root.exists():
                raise ValueError("pending admission and archive both exist")
            _immutable(archive / "abandonment.json", {
                "command": payload, "command_digest": command_digest,
                "old_intent_sha256": digest(old), "new_controller": fresh["controller"],
                "original_root": str(root), "archive": str(archive),
                "historical_native_generation_is_not_current_execution": True,
            })
            db.execute("UPDATE delivery_resource_closures SET state='abandoned',response_json=? "
                       "WHERE run_id=?", (canonical_json(response), run_id))
            db.execute("INSERT INTO delivery_commands VALUES (?,?,?,?)",
                       (payload["command_id"], run_id, command_digest, canonical_json(response)))
    return response


def admit(store, run_id, payload, *, preflight=False):
    _request(payload)
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    command_digest = digest({"run_id": run_id, **payload})
    with store._connect() as db:
        prior = db.execute(
            "SELECT * FROM delivery_resource_closures WHERE run_id=?", (run_id,)
        ).fetchone()
        command = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (payload["command_id"],)
        ).fetchone()
    if command and command["request_digest"] != command_digest:
        raise ValueError("resource closure command belongs to different inputs")
    if prior and prior["state"] != "abandoned":
        seal = json.loads(prior["intent_json"])
        if seal["command"] != payload:
            raise ValueError("this original already received its one resource closure")
        if prior["state"] == "queued":
            recovery = read_private(
                Path(seal["spec"]["state_dir"]) / "resource-closure/admission.json"
            )
            readback(store, recovery["execution_spec"], recovery)
            return {**json.loads(prior["response_json"]), "existing": True, "preflight": preflight}
    else:
        seal, _ = _snapshot(store, run_id, payload)
    fresh, readiness = _snapshot(store, run_id, payload)
    if fresh != seal:
        raise ValueError("resource closure pending whole request changed")
    root = Path(seal["spec"]["state_dir"]) / "resource-closure"
    _ancestors(root / "intent.json", allow_missing=True)
    if (root / "intent.json").exists() and read_private(root / "intent.json") != seal:
        raise ValueError("resource closure orphan seal is not attributable to the same request")
    response = {
        "run_id": run_id,
        "workflow_id": f"delivery-{run_id}-resource-closure-1",
        "phase": "resource_closure_queued",
        "additional_iterations": 0,
        "existing": False,
        "dashboard_url": f"{store.config.dashboard_url}/runs/{run_id}",
    }
    if preflight:
        return {
            **response,
            "preflight": True,
            "precheck_sha256": digest(seal),
            "native_generation_required": readiness["required"],
        }
    from .delivery_preparation import _lock

    with _lock(root / "controller.lock"):
        fresh, readiness = _snapshot(store, run_id, payload)
        if fresh != seal:
            raise ValueError("resource closure whole preflight changed before effects")
        private_directory(root)
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if (
                dict(current) != seal["original_row"]
                or store.state.claim_for(db, seal["spec"]["work_id"]) != seal["claim"]
            ):
                raise ValueError("resource closure stopped state or retained claim changed")
            _immutable(root / "intent.json", seal)
            if not prior:
                db.execute(
                    "INSERT INTO delivery_resource_closures (run_id,command_id,intent_json,state) "
                    "VALUES (?,?,?,'pending')",
                    (run_id, payload["command_id"], canonical_json(seal)),
                )
            else:
                db.execute(
                    "UPDATE delivery_resource_closures SET command_id=?,intent_json=?,"
                    "state='pending',response_json=NULL WHERE run_id=? AND state='abandoned'",
                    (payload["command_id"], canonical_json(seal), run_id),
                )
        private_directory(root / "predecessor-resources")
        for name, key in (
            ("manifest.json", "manifest_sha256"),
            ("finalization.json", "finalization_sha256"),
        ):
            source = Path(seal["spec"]["state_dir"]) / "resources" / name
            raw = _bytes(source, seal["resources"][key])
            _immutable(root / "predecessor-resources" / name, json.loads(raw), raw=raw)
        from .delivery_native_renewal import renew

        new_spec, generation = renew(
            seal["spec"],
            {
                **payload,
                "preparation_authority_path": payload["authority_path"],
                "preparation_authority_sha256": payload["authority_sha256"],
            },
            command_digest,
            supplemental={"readiness": readiness, "predecessor": seal["parent"]},
        )
        candidate = _renewed_candidate(store, seal["spec"], new_spec)
        before = seal["state"]["candidate"]
        if any(
            candidate[k] != before[k]
            for k in ("head", "base_sha", "content_sha256", "environment_digest")
        ):
            raise ValueError("resource closure current proof changed feature source/base/content")
        publication = {**seal["state"]["pull_request"], "candidate": candidate}
        recovery = {
            **seal,
            "intent_sha256": digest(seal),
            "execution_spec": new_spec,
            "candidate": candidate,
            "publication": publication,
            "native_preparation_renewal": generation,
            "source_applicability": {
                "before": before,
                "after": candidate,
                "native_parent": seal["parent"],
                "raw_browser_receipt": seal["state"]["checks"]["browser_qa"],
                "new_payload_not_executed_historical_gates": True,
            },
        }
        _immutable(root / "admission.json", recovery)
        with store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if (
                dict(current) != seal["original_row"]
                or store.state.claim_for(db, seal["spec"]["work_id"]) != seal["claim"]
            ):
                raise ValueError(
                    "resource closure lost original checkpoint/claim after native preparation"
                )
            db.execute(
                "UPDATE delivery_runs SET phase='resource_closure_queued',execution_state='queued',"
                "outcome=NULL,error=NULL,revision=revision+1,workflow_id=?,recovery_json=?,"
                "updated_at=? WHERE run_id=?",
                (response["workflow_id"], canonical_json(recovery), store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                "WHERE run_id=?",
                (store.state.now(), run_id),
            )
            db.execute(
                "UPDATE delivery_resource_closures SET state='queued',response_json=? "
                "WHERE run_id=?",
                (canonical_json(response), run_id),
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (payload["command_id"], run_id, command_digest, canonical_json(response)),
            )
            store._event(
                db,
                run_id,
                current["revision"] + 1,
                response["phase"],
                "Historical UNKNOWN retained; strict current-proof resource closure queued",
                {"intent_sha256": digest(seal), "additional_iterations": 0},
            )
    return response
