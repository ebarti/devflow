"""One ordinary published gate recovery after a completed locked prerequisite."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts import digest
from .delivery_runtime_location import private_bytes

STAGE = "prerequisite"
FIELD = "ordinary_prerequisite_observation"


def possible(spec, state):
    """A selection hint only; observe authenticates the complete native producer."""
    local = state.get("checks", {}).get("local") or {}
    results = local.get("results")
    return bool(
        spec.get("provider") == "codex"
        and not spec.get("feature_worker")
        and not spec.get("feature_delivery")
        and spec.get("policy", {}).get("host_sandbox") == "trusted-local"
        and local.get("state") == "failed"
        and local.get("source_unchanged") is True
        and isinstance(results, list)
        and 1 <= len(results) <= 64
        and all(isinstance(item, dict) for item in results)
        and all(item.get("passed") is True for item in results[:-1])
        and results[-1].get("passed") is False
        and not any(
            role.get("iteration") == state.get("iteration")
            and role.get("role") in {"review", "verify"}
            for role in state.get("roles", [])
        )
    )


def _native(spec, recipe, item, evidence, checkout):
    from .delivery_sandbox import native_check_argv

    native = item.get("native_process") or {}
    identifier = recipe["id"]
    folder = evidence / identifier / "native"
    paths = [folder / name for name in ("native-process.json", "launch.json", "process.log")]
    raw = {path: private_bytes(path) for path in paths}
    journal, launch = json.loads(raw[paths[0]]), json.loads(raw[paths[1]])
    cwd = str(checkout / recipe.get("cwd", "."))
    command = native_check_argv(spec, "devflow-check", Path(cwd), recipe["argv"])
    intent = journal.get("intent") or {}
    runtime = native.get("runtime_identity") or {}
    log_hash = hashlib.sha256(raw[paths[2]]).hexdigest()
    if (
        not all(isinstance(x, dict) for x in (native, journal, launch, intent, runtime))
        or item.get("id") != identifier
        or item.get("argv") != recipe["argv"]
        or item.get("cwd") != cwd
        or type(item.get("exit_code")) is not int
        or (item["exit_code"] != 0 if item["passed"] else item["exit_code"] <= 0)
        or item.get("launched") is False
        or item.get("failure_kind") is not None
        or item.get("cleanup") != "confirmed"
        or item.get("process_cleanup") != "observed-native-confirmed"
        or item.get("rejected_output") is not False
        or item.get("rejection_causes") != []
        or item.get("evidence_failure")
        or item.get("interrupted_native_processes")
        or item.get("plan_provenance") != recipe.get("plan_provenance")
        or native.get("state") != "finished"
        or native.get("exit_code") != item["exit_code"]
        or native.get("journal") != str(paths[0])
        or native.get("log") != str(paths[2])
        or item.get("log") != str(paths[2])
        or item.get("log_sha256") != log_hash
        or native.get("log_sha256") != log_hash
        or native.get("cleanup") != "observed-native-confirmed"
        or any(native.get(k) is not True for k in ("monitoring_complete", "stdio_drained"))
        or any(native.get(k) is not False for k in ("cancelled", "timed_out", "listener_conflict"))
        or native.get("interrupted_monitor", False) is not False
        or journal.get("phase") != "finished"
        or journal.get("monitoring_complete") is not True
        or journal.get("result") != native
        or journal.get("runtime_identity") != runtime
        or runtime.get("dirty") is not False
        or runtime.get("runtime_payload_sha256")
        != (spec["policy"].get("native_identity") or {}).get("runtime_payload_sha256")
        or intent.get("run_id") != spec["run_id"]
        or intent.get("policy_digest") != spec["policy_digest"]
        or intent.get("cwd") != cwd
        or intent.get("argv") != command
        or intent.get("timeout") != recipe.get("timeout_seconds", 600)
        or intent.get("ports") != []
        or journal.get("ports") != []
        or launch.get("argv") != command
        or not isinstance(launch.get("environment"), dict)
        or intent.get("environment_sha256") != digest(launch["environment"])
    ):
        raise ValueError("published prerequisite lost its exact completed native command")
    return {str(path): hashlib.sha256(value).hexdigest() for path, value in raw.items()}


def evidence(spec, state, attempts, previous, broker, *, resource_spec=None):
    """Read historical bytes without requiring the old gate checkout to survive."""
    from .delivery_plan_checks import planned_checks
    from .delivery_resources import _gate_roots
    from .delivery_session_custody import (
        completed_passing_implementation,
        implementation_custody,
    )
    from .delivery_stopped_resume import _implementation_receipt
    from .delivery_validation_schedule import local_checks

    if not possible(spec, state):
        raise ValueError("ordinary published prerequisite recovery has no exact unused gates")
    candidate = state["candidate"]
    local, checks = state["checks"]["local"], state["checks"]
    roles = [role for role in state.get("roles", []) if role.get("iteration") == state["iteration"]]
    implementation = next((r for r in reversed(roles) if r.get("role") == "implement"), {})
    matching = [
        a for a in attempts if a["role"] == "implement" and a["iteration"] == state["iteration"]
    ]
    output = implementation.get("candidate") or {}
    if (
        len(roles) != 1
        or implementation.get("status") != "pass"
        or implementation.get("findings") != []
        or len(matching) != 1
        or implementation.get("cleanup") != "confirmed"
        or not _implementation_receipt(spec, implementation, matching[0])
        or any(
            output.get(k) != candidate.get(k)
            for k in ("content_sha256", "base_sha", "environment_digest", "policy_digest")
        )
        or local.get("candidate_id") != candidate["id"]
        or checks.get("prepublish", {}).get("state") != "passed"
        or checks.get("prepublish", {}).get("source_unchanged") is not True
        or set(checks)
        - {
            "baseline",
            "prepublish",
            "local",
            "resource_cleanup",
            "failure",
            "terminal_tracker_checkpoint",
        }
    ):
        raise ValueError("published prerequisite cannot replace an implementation or gate verdict")
    custody = implementation_custody(spec, state, attempts, previous)
    passed = completed_passing_implementation(
        {
            "predecessor_spec": spec,
            "state": state,
            "session_id": implementation.get("session_id"),
            "attempts": attempts,
            "candidate": output,
            "session_custody": custody,
        }
    )
    current = resource_spec or spec
    bindings = {}
    roots = _gate_roots(current, execution_specs=bindings)
    namespaces = [root for root, admitted in bindings.items() if admitted == spec]
    if len(namespaces) != 1:
        raise ValueError("published prerequisite lost its exact admitted predecessor namespace")
    namespace = namespaces[0]
    evidence_root = namespace / "checks" / str(state["iteration"])
    # The original gate directory may have legitimately ended custody. Derive
    # the same admitted plan over the current physical retained candidate, then
    # substitute only that already-derived gate subject's path in the recipe.
    historical_checkout = namespace / "gates" / str(state["iteration"]) / "verify"
    planned = planned_checks(spec, broker.checkout, evidence_root)
    recipes = local_checks(spec, spec["policy"].get("checks", []), planned)
    results = local["results"]
    if len(results) > len(recipes):
        raise ValueError("published prerequisite results exceeded the frozen schedule")
    selected = recipes[: len(results)]
    failed_recipe = selected[-1]
    provenance = failed_recipe.get("plan_provenance") or {}
    if (
        failed_recipe.get("kind") == "test"
        or not provenance.get("metadata")
        or not provenance.get("test_paths")
        or "project" not in provenance
        or "uv_sha256" not in provenance
        or failed_recipe.get("generated_directories")
        != [str(Path(provenance["project"]) / ".venv")]
        or failed_recipe["argv"][1:5] != ["sync", "--locked", "--extra", "dev"]
        or any(recipe.get("kind") == "test" for recipe in selected)
    ):
        raise ValueError("published failed command is not its planned locked prerequisite")
    expected_error = (
        "environment preparation failed: "
        + failed_recipe["id"]
        + "; candidate retained without requesting code repair"
    )
    if state.get("error") != expected_error or checks.get("failure") != {
        "classification": "terminal",
        "stage": "checks",
        "reason": expected_error,
    }:
        raise ValueError("published prerequisite lost its exact controller stop")
    files = {}
    for recipe, item in zip(selected, results, strict=True):
        # All relative recipe authority comes from the current same-content
        # candidate. Historical absolute argv paths are checked separately.
        files.update(_native(spec, recipe, item, evidence_root, historical_checkout))
    if any(
        hashlib.sha256(private_bytes(path)).hexdigest() != expected
        for path, expected in files.items()
    ):
        raise ValueError("published prerequisite evidence changed during observation")
    after = {}
    if _gate_roots(current, execution_specs=after) != roots or after != bindings:
        raise ValueError("published prerequisite namespace changed during observation")
    return {
        "cause": "completed_planned_python_prerequisite",
        "implementation": passed,
        "session_custody": custody,
        "local_sha256": digest(local),
        "recipes": selected,
        "files": files,
    }


def observe(store, spec, row, state, attempts, effects, closed, previous, broker):
    from .delivery_amendment_capacity import live_owner
    from .delivery_policy_recovery import _rows, work_binding
    from .delivery_resources import observe_completed_resources
    from .delivery_unpublished_recovery import quarantine_history, teardown_checkpoint

    proof = evidence(spec, state, attempts, previous, broker)
    history = quarantine_history(previous)
    resources = observe_completed_resources(
        spec, attempts, quarantined=teardown_checkpoint(history) if history else None
    )
    current_cleanup = json.loads(row["checks_json"]).get("resource_cleanup") or {}
    if (
        resources["finalization_sha256"] != current_cleanup.get("receipt_sha256")
        or resources["roots"].get(spec["checkout"]) is None
    ):
        raise ValueError("published prerequisite lost its complete published cleanup")
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        if (
            _rows(store, spec["run_id"])[:3] != (row, attempts, effects)
            or store.state.claim_for(db, spec["work_id"]) is not None
        ):
            raise ValueError("published prerequisite current records or issue authority changed")
        if (
            db.execute(
                "SELECT 1 FROM delivery_mutations WHERE run_id=? "
                "AND (state!='complete' OR kind='cancel')",
                (spec["run_id"],),
            ).fetchone()
            or db.execute(
                "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state!='sent'", (spec["run_id"],)
            ).fetchone()
        ):
            raise ValueError("published prerequisite has unfinished delivery authority")
        _, owner = live_owner(spec, db=db)
    return {
        **proof,
        "resources": resources,
        "work_binding": binding,
        "operational_owner": owner,
        "closed_sha256": digest(closed),
    }


def authority(spec, recovery):
    from .delivery_continuation import continuation_authority
    from .delivery_gate_retry import KIND

    previous = recovery.get("original_recovery")
    before = recovery.get("seal", {}).get("original_spec") or {}
    proof = recovery.get("seal", {}).get(FIELD) or {}
    history = previous
    while history:
        if history.get("seal", {}).get(FIELD):
            raise ValueError("ordinary prerequisite already received its one gate recovery")
        history = history.get("original_recovery")
    if (
        recovery.get("kind") != KIND
        or proof.get("cause") != "completed_planned_python_prerequisite"
        or spec.get("gate_retry_stage") != STAGE
        or spec.get("gate_retry_generation") != 1
        or spec.get("gate_retry_preflight_generation")
        or spec.get("gate_retry_prepared_handoff")
        or recovery.get("command", {}).get("additional_iterations") != 0
        or recovery["state"] != recovery["seal"]["closed"]["result"]
        or recovery["seal"].get("previous_digest") != (digest(previous) if previous else None)
        or continuation_authority(spec["policy"]) != continuation_authority(before["policy"])
        or any(
            spec.get(k) != before.get(k)
            for k in (
                "run_id",
                "work_id",
                "state_dir",
                "checkout",
                "source_path",
                "branch",
                "base_sha",
                "accepted_plan",
                "feature_worker",
                "feature_delivery",
                "config_digest",
                "plan_approval",
                "authorized_endpoint",
                "origin_url",
                "base_ref",
                "publication_base_ref",
                "expected_paths",
                "provider",
            )
        )
    ):
        raise ValueError("ordinary prerequisite changed its finite gate-only authority")
    return previous, before, proof


def validate(store, spec, recovery, *, resource_spec=None):
    from .delivery_amendment_capacity import live_owner
    from .delivery_feature_revision_subject import ObservationBroker
    from .delivery_gate_retry import _rows, gate_namespace
    from .delivery_listener_verification import historical_records
    from .delivery_policy_recovery import work_binding
    from .delivery_stopped_resume import observed_preparation_cleanup
    from .delivery_unpublished_recovery import quarantine_history, teardown_checkpoint

    previous, before, proof = authority(spec, recovery)
    closed = store._completed_temporal_result(
        spec["run_id"], workflow_id=recovery["seal"]["closed"]["workflow_id"]
    )
    if closed != recovery["seal"]["closed"] or digest(closed) != proof["closed_sha256"]:
        raise ValueError("published prerequisite closed execution changed")
    historical_records(store, recovery, resource_spec or spec)
    broker = ObservationBroker(store, before, resource_spec=resource_spec or spec)
    observed = evidence(
        before,
        closed["result"],
        recovery["seal"]["attempts"],
        previous,
        broker,
        resource_spec=resource_spec or spec,
    )
    if any(proof.get(key) != value for key, value in observed.items()):
        raise ValueError("published prerequisite original native authority changed")
    root = Path(spec["state_dir"]) / gate_namespace(1, STAGE) / "predecessor-resources"
    for name, key in [
        ("manifest.json", "manifest_sha256"),
        ("finalization.json", "finalization_sha256"),
    ]:
        if hashlib.sha256(private_bytes(root / name)).hexdigest() != proof["resources"][key]:
            raise ValueError("published prerequisite original resource evidence changed")
    history = quarantine_history(previous)
    observed_preparation_cleanup(
        before,
        resource_spec=resource_spec or spec,
        quarantined=teardown_checkpoint(history) if history else None,
    )
    row, _, _, claim = _rows(store, spec["run_id"])
    if (
        row["work_id"] != spec["work_id"]
        or not claim
        or claim["owner"] != "external:devflow:" + spec["run_id"]
    ):
        raise ValueError("published prerequisite lost its current owned claim")
    with store._connect() as db:
        if (
            work_binding(store, spec, db) != proof["work_binding"]
            or live_owner(before, db=db)[1] != proof["operational_owner"]
        ):
            raise ValueError("published prerequisite current issue or operational owner changed")
