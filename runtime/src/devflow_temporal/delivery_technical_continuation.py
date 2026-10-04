"""One append-only technical successor of an already admitted stopped delivery."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import subprocess
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig
from .delivery_continuation import session_state_digest
from .delivery_metadata_recovery import _identity, _immutable
from .delivery_native_process import listeners, process_table
from .delivery_policy_recovery import _rows, work_binding
from .delivery_resources import RunResources, _ancestors, read_private
from .delivery_resources import _identity as root_identity
from .delivery_technical_integration import prospective, reference

KIND = "accepted_technical_successor"
FIELDS = {
    "continuation_kind",
    "command_id",
    "expected_revision",
    "expected_iteration",
    "expected_candidate_id",
    "expected_pr_number",
    "expected_pr_head",
    "additional_iterations",
    "authority_path",
    "authority_sha256",
    "expected_source_revision",
}
PACKET_FIELDS = {"prospective_path", "prospective_sha256"}


def _request(payload):
    if (
        not isinstance(payload, dict)
        or set(payload) not in (FIELDS, FIELDS | PACKET_FIELDS)
        or payload.get("continuation_kind") != KIND
        or type(payload.get("additional_iterations")) is not int
        or payload["additional_iterations"] != 0
        or not isinstance(payload.get("command_id"), str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", payload["command_id"])
        or any(
            type(payload.get(key)) is not int or payload[key] < lower
            for key, lower in (
                ("expected_revision", 1),
                ("expected_iteration", 0),
                ("expected_pr_number", 1),
            )
        )
        or any(
            not isinstance(payload.get(key), str)
            or not re.fullmatch(r"[0-9a-f]{" + str(size) + "}", payload[key])
            for key, size in (
                ("expected_candidate_id", 64),
                ("expected_pr_head", 40),
                ("expected_source_revision", 40),
                ("authority_sha256", 64),
            )
        )
    ):
        raise ValueError("technical successor requires an explicit zero-iteration typed request")


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


def _source_readiness(spec, payload, authority, predecessor):
    from .delivery_native_preparation import native_identity
    from .delivery_native_renewal import _payload_only

    receipt = native_predecessor(predecessor, authority)
    source = Path(__file__).resolve().parents[3]
    before, after = spec["policy"]["native_identity"], native_identity(spec)
    _payload_only(before, after)
    config = DeliveryConfig.load(Path(spec["config_path"]))
    if (
        before.get("runtime_payload_sha256") == after.get("runtime_payload_sha256")
        or spec["policy"].get("host_sandbox") != "trusted-local"
        or spec.get("role_home_generation") != "policy-1"
        or spec.get("terminal_tracker_version") != 1
        or source != Path(receipt["installed_source_root"])
        or str(source / "runtime/src/devflow_temporal/delivery_native_renewal.py")
        != receipt["runtime_import_path"]
        or digest(config.raw) != spec["config_digest"]
        or hashlib.sha256(config.path.read_bytes()).hexdigest()
        != authority["installed_predecessor"]["config_sha256"]
        or receipt["source_revision"] != authority["installed_predecessor"]["source"]
        or _git(source, "rev-parse", "HEAD") != payload["expected_source_revision"]
        or _git(source, "status", "--porcelain", "--untracked-files=all")
    ):
        raise ValueError("technical successor requires the clean same installed source/config root")
    _git(
        source,
        "merge-base",
        "--is-ancestor",
        receipt["source_revision"],
        payload["expected_source_revision"],
    )
    return {
        "required": True,
        "before": before,
        "identity": after,
        "authority": authority,
        "source": source,
        "source_revision": payload["expected_source_revision"],
        "config_sha256": hashlib.sha256(config.path.read_bytes()).hexdigest(),
    }


def _observe_resources(spec, *, unknown_allowed):
    """Fresh read-only actor, port, lease and inode observation, never teardown."""
    state = Path(spec["state_dir"])
    _ancestors(state / "resources/manifest.json")
    manifest_path, final_path = (
        state / "resources" / name for name in ("manifest.json", "finalization.json")
    )
    manifest, finalization = read_private(manifest_path), read_private(final_path)
    if (
        manifest.get("run_id") != spec["run_id"]
        or manifest.get("state_identity") != root_identity(state)
        or finalization.get("state")
        not in ({"confirmed", "unknown"} if unknown_allowed else {"confirmed"})
        or finalization.get("process_cleanup")
        not in (
            {"observed-native-confirmed", "unknown"}
            if unknown_allowed
            else {"observed-native-confirmed"}
        )
        or any(
            item.get("cleanup") != "observed-native-confirmed"
            for item in finalization.get("processes", [])
        )
    ):
        raise ValueError("technical predecessor process/root custody is unconfirmed")
    table = process_table()
    journals, roots = {}, {}
    registry = RunResources(spec, read_only=True)
    receipts = {item["journal"]: item for item in finalization.get("processes", [])}
    if set(receipts) != set(manifest["processes"]):
        raise ValueError("technical predecessor finalized actor inventory changed")
    for raw in manifest["processes"]:
        path = Path(raw)
        if not path.is_relative_to(state) or path.resolve(strict=True) != path:
            raise ValueError("technical predecessor journal escaped its original root")
        _ancestors(path)
        value = read_private(path)
        receipt = receipts[raw]
        if (
            receipt.get("monitoring_complete") is not True
            or receipt.get("owned_ports_clear") is not True
            or sorted(receipt.get("observed_pids", [])) != sorted(map(int, value.get("owned", {})))
        ):
            raise ValueError(
                "technical predecessor actor identities lost their finalized inventory"
            )
        lock_path = path.with_name("native-process.lock")
        descriptor = os.open(lock_path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("technical predecessor still owns a native launch lease") from exc
        finally:
            os.close(descriptor)
        if (
            value.get("phase") != "finished"
            or value.get("monitoring_complete") is not True
            or any(
                table.get(int(pid), {}).get("identity") == actor["identity"]
                and not table[int(pid)]["stat"].startswith("Z")
                for pid, actor in value.get("owned", {}).items()
            )
            or any(listeners(port) for port in value.get("ports", []))
        ):
            raise ValueError("technical predecessor still has a live actor or port")
        journals[raw] = hashlib.sha256(path.read_bytes()).hexdigest()
    for raw, entry in manifest["roots"].items():
        path = Path(raw)
        registry._allowed(path, entry["kind"], finalizing=True)
        _ancestors(path, allow_missing=True)
        if os.path.lexists(path):
            if entry["identity"] != root_identity(path):
                raise ValueError("technical predecessor root identity changed")
            if entry.get("receipt", {}).get("state") in {"removed", "already_absent"}:
                raise ValueError("technical predecessor finalized root was recreated")
            observed = subprocess.run(
                ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if observed.returncode not in (0, 1) or any(
                line.startswith("p") and line[1:].isdecimal()
                for line in observed.stdout.splitlines()
            ):
                raise ValueError("technical predecessor has a live root user or unreadable lease")
            roots[raw] = entry["identity"]
        else:
            roots[raw] = None
    return {
        "manifest_sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        "finalization_sha256": hashlib.sha256(final_path.read_bytes()).hexdigest(),
        "journal_sha256": journals,
        "roots": roots,
    }


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


@contextmanager
def _claim_lease(store, seal):
    """Only the released original is borrowed; failed admission releases that lease."""
    spec = seal["spec"]
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec["work_id"])
        if seal["claim"] is not None:
            if canonical_json(claim) != canonical_json(seal["claim"]):
                raise ValueError("technical original retained claim changed")
        elif claim is None:
            store.state.claim_work(
                db,
                spec["work_id"],
                f"external:devflow:{spec['run_id']}",
                store.config.dashboard_url,
            )
        elif claim["owner"] != f"external:devflow:{spec['run_id']}":
            raise ValueError("technical released claim is now foreign")
    try:
        yield
    except BaseException:
        if seal["claim"] is None:
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                store.state.release_work(db, spec["work_id"], f"external:devflow:{spec['run_id']}")
        raise


def _snapshot(store, run_id, payload):
    spec = store.effective_spec(run_id)
    authority, scope, integration, evidence = _authority(spec, payload)
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row["recovery_json"]) if row["recovery_json"] else None
    if not previous or previous.get("kind") != (
        "published_metadata_recovery" if integration else "investigation_gates_only"
    ):
        raise ValueError("technical successor requires its immediately consumed admission")
    closed = store._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
    state = closed["result"]
    sealed = evidence["sealed_actual_failures"]["runs"].get(run_id)
    if not sealed:
        raise ValueError("technical successor has no sealed authentic failure trigger")
    historical = reference(sealed["row"]["path"], sealed["row"]["sha256"])
    fields = (
        "request_json",
        "request_digest",
        "recovery_json",
        "workflow_id",
        "protocol_revision",
        "iteration",
        "candidate_json",
        "pr_json",
        "checks_json",
        "error",
        "cleanup",
        "phase",
        "outcome",
        "execution_state",
    )
    if (
        any(row[key] != historical[key] for key in fields)
        or closed["request_digest"] != row["request_digest"]
        or closed["recovery_digest"] != digest(previous)
        or closed["workflow_id"] != row["workflow_id"]
        or state.get("run_id") != run_id
        or any(
            state.get(key) != row[key]
            for key in ("phase", "outcome", "execution_state", "error", "cleanup", "iteration")
        )
        or state.get("revision") != row["protocol_revision"]
        or state["revision"] != payload["expected_revision"]
        or state["iteration"] != payload["expected_iteration"]
        or state.get("candidate") != json.loads(row["candidate_json"])
        or state.get("pull_request") != json.loads(row["pr_json"])
        or state.get("checks") != json.loads(row["checks_json"])
        or state["candidate"]["id"] != payload["expected_candidate_id"]
        or state["candidate"]["head"] != payload["expected_pr_head"]
        or state["pull_request"]["number"] != payload["expected_pr_number"]
        or state["pull_request"]["url"] != scope["published_pr"]
        or state["outcome"] != "blocked"
        or any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts)
        or any(e["state"] != "complete" or not e["observed_json"] for e in effects)
        or len(attempts) != len(state.get("roles", []))
        or sorted((a["role"], a["iteration"], a["session_id"]) for a in attempts)
        != sorted((r["role"], r["iteration"], r.get("session_id")) for r in state["roles"])
    ):
        raise ValueError("technical successor lost its exact closed failure checkpoint")
    if integration:
        local = state["checks"].get("local")
        if (
            local != sealed.get("local_check")
            or local.get("state") != "unknown"
            or state.get("cleanup") != "unknown"
        ):
            raise ValueError("technical cleanup closure is not the authenticated prelaunch gate")
    else:
        failed = next((a for a in reversed(state["roles"]) if a["role"] == "review"), {})
        if (
            failed.get("finish_reason") != "prelaunch"
            or failed.get("session_id") is not None
            or failed.get("usage") is not None
            or failed.get("cleanup") != "confirmed"
            or state["checks"].get("prepublish", {}).get("state") != "passed"
        ):
            raise ValueError(
                "technical review continuation is not the zero-provider launch failure"
            )
    command = previous["command"]
    bound = authority["trigger_bindings"][
        "consumed1005_metadata_authority" if integration else "consumed907_gates_only_authority"
    ]
    if command["authority_path"] != bound["path"] or command["authority_sha256"] != bound["sha256"]:
        raise ValueError("technical consumed admission authority changed")
    # The shared resolver authenticates the durable immutable admission and exact execution spec.
    from .delivery_resources import _gate_evidence_root

    _gate_evidence_root(spec)
    original = previous["spec"] if integration else previous["original_spec"]
    predecessor = {"original_spec": original, "spec": spec, "recovery": previous}
    readiness = _source_readiness(spec, payload, authority, predecessor)
    observed = _observe_resources(spec, unknown_allowed=integration)
    retained = sealed.get("resources", {})
    if observed["manifest_sha256"] != retained.get("resources/manifest.json", {}).get(
        "sha256"
    ) or observed["finalization_sha256"] != retained.get("resources/finalization.json", {}).get(
        "sha256"
    ):
        raise ValueError("technical predecessor sealed resource bytes changed before admission")
    if _quiescent(store, spec) != claim or (claim is None if integration else claim is not None):
        raise ValueError("technical continuation original claim custody changed")
    session = scope["original_implementer_session"]
    if any(r.get("session_id") != session for r in state["roles"] if r["role"] == "implement"):
        raise ValueError("technical successor changed original implementer session")
    session_digest = session_state_digest(Path(spec["state_dir"]) / "role-homes/implement", session)
    expected_session = (
        previous["session_sha256"] if integration else previous["seal"]["session_sha256"]
    )
    if session_digest != expected_session:
        raise ValueError("technical successor original session evidence changed")
    broker = DeliveryBroker(store, spec)
    from .delivery_repair import published_identity

    published_identity(broker, state["candidate"], state["pull_request"])
    merge, proposed = None, deepcopy(spec)
    if integration:
        packet = reference(payload["prospective_path"], payload["prospective_sha256"])
        output = prospective(spec, scope, packet, payload["authority_sha256"])
        remote = _git(broker.source, "ls-remote", "origin", "refs/heads/main")
        if not remote or remote.split()[0] != scope["authorized_current_main"]:
            raise ValueError("technical integration current main changed before admission")
        merge = {
            "old_head": scope["owned_predecessor_head"],
            "main": scope["authorized_current_main"],
            "original_base": spec["base_sha"],
            "tree": output["tree"],
            "file_count": len(output["index"]),
            "preparation_inputs": output["preparation_inputs"],
            "subject": "chore: integrate current main into preserved coaching delivery",
            "signer": _identity(broker)[0],
            "accepted_plan_sha256": digest(spec["accepted_plan"]),
            "packet_path": payload["prospective_path"],
            "packet_sha256": payload["prospective_sha256"],
        }
        proposed["base_sha"] = merge["main"]
    else:
        output = None
    controller = process_table().get(os.getpid())
    if not controller:
        raise ValueError("technical controller process identity is unobservable")
    seal = {
        "kind": KIND,
        "command": payload,
        "authority": authority,
        "scope": scope,
        "spec": spec,
        "proposed_spec": proposed,
        "integration": merge,
        "original_row": row,
        "state": state,
        "closed": closed,
        "attempts": attempts,
        "effects": effects,
        "claim": claim,
        "original_recovery": previous,
        "native_predecessor": predecessor,
        "resources": observed,
        "session_id": session,
        "session_sha256": session_digest,
        "resume_stage": "checks" if integration else "review",
        "maximum_iteration": 4,
        "controller": {
            "pid": os.getpid(),
            "identity": controller["identity"],
            "source_revision": payload["expected_source_revision"],
        },
    }
    return seal, readiness, output


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


def _pending(store, seal):
    """Read actual partial effects; an accepted replay never recaptures authority."""
    root = Path(seal["spec"]["state_dir"]) / "technical-successor"
    _ancestors(root / "intent.json")
    if canonical_json(read_private(root / "intent.json")) != canonical_json(seal):
        raise ValueError("technical pending immutable request changed")
    authority, scope, integration, _evidence = _authority(seal["spec"], seal["command"])
    if (
        canonical_json(authority) != canonical_json(seal["authority"])
        or canonical_json(scope) != canonical_json(seal["scope"])
        or integration != bool(seal["integration"])
    ):
        raise ValueError("technical pending whole authority changed")
    row, attempts, effects, _claim = _rows(store, seal["spec"]["run_id"])
    if (
        row != seal["original_row"]
        or attempts != seal["attempts"]
        or effects != seal["effects"]
        or store._completed_temporal_result(
            seal["spec"]["run_id"], workflow_id=seal["closed"]["workflow_id"]
        )
        != seal["closed"]
        or session_state_digest(
            Path(seal["spec"]["state_dir"]) / "role-homes/implement", seal["session_id"]
        )
        != seal["session_sha256"]
    ):
        raise ValueError("technical pending closed history or source session changed")
    claim = _quiescent(store, seal["spec"])
    if seal["claim"] is not None and canonical_json(claim) != canonical_json(seal["claim"]):
        raise ValueError("technical pending retained claim changed")
    observed = _observe_resources(seal["spec"], unknown_allowed=integration)
    current = read_private(Path(seal["spec"]["state_dir"]) / "resources/manifest.json")
    archive = root / "predecessor-resources/manifest.json"
    if archive.exists():
        original = read_private(archive)
        if set(current["roots"]) != set(original["roots"]) or any(
            current["roots"][path]["kind"] != entry["kind"]
            or current["roots"][path]["identity"] != entry["identity"]
            for path, entry in original["roots"].items()
        ):
            raise ValueError("technical pending resource allocation or identity changed")
    elif canonical_json(observed) != canonical_json(seal["resources"]):
        raise ValueError("technical pending unexplained resources before archive")
    if observed["journal_sha256"] != seal["resources"]["journal_sha256"]:
        raise ValueError("technical pending predecessor actor journal changed")
    if (root / "closure.json").exists():
        closure = read_private(root / "closure.json")
        if closure["predecessor_resources"] != seal["resources"] or closure["state"] != "confirmed":
            raise ValueError("technical pending cleanup closure changed")
        cleanup = _closure_cleanup(seal, root, observe_only=True)
        if closure != {
            "state": "confirmed",
            "predecessor_resources": seal["resources"],
            "never_started_gate": seal["resume_stage"],
            "cleanup": cleanup,
        }:
            raise ValueError("technical pending cleanup receipt changed")
    return observed


def _closure_cleanup(seal, root, *, observe_only=False):
    """Read a completed owning cleanup after a lost response without rewriting it."""
    spec = seal["spec"]
    source = Path(spec["state_dir"]) / "resources/finalization.json"
    saved = root / "closure-finalization.json"
    manifest = read_private(source.with_name("manifest.json"))
    current = read_private(source)
    # A crash may occur after finalize has atomically written its receipt but before
    # retaining the copy. Its manifest and actual root/actor observations prove that
    # effect; the old UNKNOWN receipt alone cannot become a completed fresh closure.
    completed = saved.exists() or (
        (root / "closure-intent.json").exists()
        and hashlib.sha256(source.with_name("manifest.json").read_bytes()).hexdigest()
        != seal["resources"]["manifest_sha256"]
        and current.get("state") == "confirmed"
        and manifest.get("finalization") == current
    )
    if not completed:
        if observe_only:
            raise ValueError("technical pending cleanup has no completed retained receipt")
        RunResources(spec).finalize("blocked")
        manifest, current = read_private(source.with_name("manifest.json")), read_private(source)
    raw = source.read_bytes()
    if (
        current.get("state") != "confirmed"
        or current.get("outcome") != "blocked"
        or current.get("process_cleanup") != "observed-native-confirmed"
        or current.get("resource_cleanup") != "confirmed"
        or manifest.get("finalization") != current
        or current.get("roots")
        != [
            entry["receipt"]
            for _path, entry in sorted(
                manifest["roots"].items(), key=lambda item: -len(Path(item[0]).parts)
            )
        ]
        or (saved.exists() and saved.read_bytes() != raw)
    ):
        raise ValueError("technical successor fresh owning cleanup remains unknown or changed")
    _observe_resources(spec, unknown_allowed=False)
    if not observe_only:
        _immutable(saved, current, raw=raw)
    return {**current, "receipt": str(source), "receipt_sha256": hashlib.sha256(raw).hexdigest()}


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


def continue_technical(store, run_id, payload, *, preflight=False):
    _request(payload)
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    command_digest = digest({"run_id": run_id, **payload})
    with store._connect() as db:
        prior = db.execute(
            "SELECT * FROM delivery_technical_successors WHERE run_id=?", (run_id,)
        ).fetchone()
        command = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (payload["command_id"],)
        ).fetchone()
    if command and command["request_digest"] != command_digest:
        raise ValueError("technical command ID already belongs to different inputs")
    if prior:
        seal = json.loads(prior["intent_json"])
        if canonical_json(seal["command"]) != canonical_json(payload):
            raise ValueError("accepted original already has its one technical successor")
        if prior["state"] == "queued":
            recovery = read_private(
                Path(seal["spec"]["state_dir"]) / "technical-successor/admission.json"
            )
            # Stable readback authenticates completed integration/preparation/publication even after
            # the new workflow has closed and released its claim; no effect is repeated.
            effective_spec(store, seal["spec"], recovery)
            from .delivery_repair import published_identity

            published_identity(
                DeliveryBroker(store, recovery["execution_spec"]),
                recovery["candidate"],
                recovery["publication"],
            )
            return {**json.loads(prior["response_json"]), "existing": True, "preflight": preflight}
        _pending(store, seal)
        readiness = _source_readiness(
            seal["spec"], payload, seal["authority"], seal["native_predecessor"]
        )
        # Pending effects are owned by this sealed request, never a new preflight baseline.
        output = (
            prospective(
                seal["spec"],
                seal["scope"],
                reference(payload["prospective_path"], payload["prospective_sha256"]),
                payload["authority_sha256"],
            )
            if seal["integration"]
            else None
        )
    else:
        seal, readiness, output = _snapshot(store, run_id, payload)
    response = {
        "run_id": run_id,
        "workflow_id": f"delivery-{run_id}-technical-1",
        "phase": "technical_successor_queued",
        "authorized_through_iteration": 4,
        "additional_iterations": 0,
        "existing": False,
        "dashboard_url": f"{store.config.dashboard_url}/runs/{run_id}",
    }
    if preflight:
        return {
            **response,
            "preflight": True,
            "precheck_sha256": digest(seal),
            "resume_stage": seal["resume_stage"],
            "prospective_tree": seal["integration"]["tree"] if seal["integration"] else None,
        }
    from .delivery_preparation import _lock
    from .delivery_resources import private_directory

    root = Path(seal["spec"]["state_dir"]) / "technical-successor"
    with _lock(root / "controller.lock"):
        private_directory(root)
        if not prior:
            # Revalidate the whole request before the first durable write and claim mutation.
            fresh, readiness, output = _snapshot(store, run_id, payload)
            if canonical_json(fresh) != canonical_json(seal):
                raise ValueError("technical successor request changed before immutable admission")
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                current = db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)
                ).fetchone()
                claim = store.state.claim_for(db, seal["spec"]["work_id"])
                if (
                    dict(current) != seal["original_row"]
                    or canonical_json(claim) != canonical_json(seal["claim"])
                    or db.execute(
                        "SELECT 1 FROM delivery_technical_successors WHERE run_id=?", (run_id,)
                    ).fetchone()
                ):
                    raise ValueError(
                        "technical successor claim or checkpoint changed before sealing"
                    )
                work_binding(store, seal["spec"], db)
                if claim is None:
                    store.state.claim_work(
                        db,
                        seal["spec"]["work_id"],
                        f"external:devflow:{run_id}",
                        store.config.dashboard_url,
                    )
                _immutable(root / "intent.json", seal)
                db.execute(
                    "INSERT INTO delivery_technical_successors "
                    "(run_id,command_id,intent_json,state) VALUES (?,?,?,'pending')",
                    (run_id, payload["command_id"], canonical_json(seal)),
                )
        with _claim_lease(store, seal):
            for name, key in (
                ("manifest.json", "manifest_sha256"),
                ("finalization.json", "finalization_sha256"),
            ):
                archive = root / "predecessor-resources" / name
                if not archive.exists():
                    source = Path(seal["spec"]["state_dir"]) / "resources" / name
                    if hashlib.sha256(source.read_bytes()).hexdigest() != seal["resources"][key]:
                        raise ValueError(
                            "technical predecessor archive has an unexplained missing byte"
                        )
                    private_directory(archive.parent)
                    _immutable(archive, read_private(source), raw=source.read_bytes())
                if hashlib.sha256(archive.read_bytes()).hexdigest() != seal["resources"][key]:
                    raise ValueError("technical predecessor archive changed")
            closure_path = root / "closure.json"
            if not closure_path.exists():
                # Old UNKNOWN and all process identities remain in predecessor-resources. The
                # fresh closure records actual observation and supported owning cleanup only.
                observed = _observe_resources(
                    seal["spec"], unknown_allowed=bool(seal["integration"])
                )
                progress = root / "closure-intent.json"
                if not progress.exists() and canonical_json(observed) != canonical_json(
                    seal["resources"]
                ):
                    raise ValueError("technical predecessor changed before append-only closure")
                _immutable(
                    progress,
                    {
                        "predecessor_resources": seal["resources"],
                        "closed_sha256": digest(seal["closed"]),
                    },
                )
                cleanup = _closure_cleanup(seal, root)
                _immutable(
                    closure_path,
                    {
                        "state": "confirmed",
                        "predecessor_resources": seal["resources"],
                        "never_started_gate": seal["resume_stage"],
                        "cleanup": cleanup,
                    },
                )
            if seal["integration"]:
                from .delivery_technical_integration import integrate

                head = integrate(DeliveryBroker(store, seal["spec"]), seal, output, root)
            else:
                head = seal["state"]["candidate"]["head"]
            from .delivery_native_renewal import renew

            native_request = {
                **payload,
                "preparation_authority_path": payload["authority_path"],
                "preparation_authority_sha256": payload["authority_sha256"],
            }
            effective, renewal = renew(
                seal["proposed_spec"],
                native_request,
                command_digest,
                technical={"readiness": readiness, "predecessor": seal["native_predecessor"]},
            )
            if renewal is None:
                raise ValueError(
                    "technical successor did not produce its required child native generation"
                )
            # Candidate construction precedes the durable gate namespace admission.
            broker = DeliveryBroker(store, seal["spec"])
            broker.spec = effective
            candidate = broker.candidate()
            if candidate["head"] != head:
                raise ValueError("technical successor source changed after child preparation")
            publication = {
                **seal["state"]["pull_request"],
                "head": head,
                "candidate": candidate,
                "base": effective["base_sha"],
            }
            from .delivery_repair import published_identity

            published_identity(broker, candidate, publication)
            recovery = {
                **seal,
                "candidate": candidate,
                "publication": publication,
                "execution_spec": effective,
                "native_preparation_renewal": renewal,
                "intent_sha256": digest(seal),
                "closure_reference": {
                    "path": str(closure_path),
                    "sha256": hashlib.sha256(closure_path.read_bytes()).hexdigest(),
                },
            }
            _immutable(root / "admission.json", recovery)
            with store._connect() as db:
                db.execute("BEGIN IMMEDIATE")
                current = db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)
                ).fetchone()
                claim = store.state.claim_for(db, seal["spec"]["work_id"])
                if (
                    dict(current) != seal["original_row"]
                    or claim is None
                    or claim["owner"] != f"external:devflow:{run_id}"
                ):
                    raise ValueError("technical successor lost stopped checkpoint or owning claim")
                work_binding(store, seal["spec"], db)
                db.execute(
                    "UPDATE delivery_runs SET phase='technical_successor_queued',"
                    "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1,"
                    "workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?",
                    (response["workflow_id"], canonical_json(recovery), store.state.now(), run_id),
                )
                db.execute(
                    "UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                    "WHERE run_id=?",
                    (store.state.now(), run_id),
                )
                db.execute(
                    "UPDATE delivery_technical_successors SET state='queued',response_json=? "
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
                    "technical_successor_queued",
                    "Preserved original admitted for technical gates without a feature turn",
                    {
                        "intent_sha256": digest(seal),
                        "maximum_iteration": 4,
                        "additional_implementation_turns": 0,
                    },
                )
            return response
