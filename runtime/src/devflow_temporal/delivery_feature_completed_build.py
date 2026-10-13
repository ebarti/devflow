"""Observe a completed, unchanged build as source evidence, without resuming it.

The ordinary revision handoff already reuses these builds. A sealed patch and
its actual native pass prove that source; they do not recreate Temporal closure
or authorize another implementation turn or any product gate.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path
from types import SimpleNamespace

from .contracts import digest
from .delivery_feature_revision_subject import (
    ObservationBroker,
    _private_receipt,
    _runtime,
    _time,
    full_candidate,
)
from .delivery_provider_capacity import capacity_error
from .delivery_resources import _ancestors


def _successful_turns(records, session):
    for index, record in enumerate(records):
        turn = record.get("turn") or {}
        if record.get("thread_id") != session:
            return False
        if turn.get("status") == "completed" and turn.get("error") is None:
            continue
        if index == len(records) - 1:
            return False
        if (turn.get("status") == "failed"
                and capacity_error((turn.get("error") or {}).get("codexErrorInfo"))):
            continue
        if not turn and (record.get("start_error") or {}).get("type") == "ServerBusyError":
            continue
        return False
    return bool(records)


def _bytes(path):
    _ancestors(path)
    if path.resolve(strict=True) != path:
        raise ValueError("completed build source left its canonical private path")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1
                or before.st_size > 16 * 1024 * 1024):
            raise ValueError("completed build source is not a bounded owned private file")
        raw = stream.read(16 * 1024 * 1024 + 1)
        after = os.fstat(stream.fileno())
        keys = ("st_dev", "st_ino", "st_size", "st_mode", "st_uid", "st_nlink", "st_mtime_ns")
        if len(raw) != before.st_size or any(getattr(before, k) != getattr(after, k) for k in keys):
            raise ValueError("completed build source changed during observation")
    return raw


def _native_pass(spec, row, attempts, candidate):
    from .delivery_native_threads import NativeThreadObservation
    from .delivery_sandbox import _native_role_home
    from .delivery_session_custody import _same_assessment
    from .supervisor import DeliverySupervisor

    latest = [a for a in attempts
              if a["role"] == "implement" and a["iteration"] == row["iteration"]]
    if len(latest) != 1 or any(a["role"] != "implement" for a in attempts):
        raise ValueError("completed build lacks its unique final supervised implementation")
    attempt = latest[0]
    folder = Path(spec["state_dir"]) / "attempts" / attempt["job_key"]
    request = _private_receipt(folder / "request.json", canonical=True)
    result = json.loads(attempt["result_json"] or "null")
    raw = _private_receipt(folder / "result.json")
    journal = _private_receipt(folder / "native-process.json")
    launch = _private_receipt(folder / "launch.json")
    start = _private_receipt(folder / "start.json")
    native = (result or {}).get("native_process") or {}
    intent, metadata = journal.get("intent") or {}, journal.get("provider_session") or {}
    input_candidate = full_candidate(request.get("candidate"))
    resumed = request.get("resume_session")
    argv = intent.get("argv") or []
    if (not isinstance(result, dict) or not _same_assessment(raw, result)
            or result.get("status") != "pass" or result.get("finish_reason") != "done"
            or result.get("cleanup") != "confirmed" or result.get("findings")
            or not attempt["pid"] or not attempt["process_identity"] or not attempt["session_id"]
            or attempt["session_id"] != result.get("session_id")
            or request.get("spec") != spec or request.get("role") != "implement"
            or request.get("iteration") != row["iteration"]
            or request.get("revision_context") is not None
            or request.get("native_authorized") is not True
            or request.get("role_evidence_key") != attempt["job_key"]
            or DeliverySupervisor._job_key(request) != attempt["job_key"]
            or input_candidate["id"] != attempt["candidate_id"]
            or any(input_candidate[key] != candidate[key] for key in
                   ("base_sha", "environment_digest", "policy_digest"))
            or request.get("workspace") != spec["checkout"]
            or request.get("result_path") != str(folder / "result.json")
            or attempt["result_path"] != request["result_path"]
            or request.get("start_path") != str(folder / "start.json") or "ready_path" in request
            or type(start.get("pid")) is not int or start["pid"] <= 0
            or not (_time(row["created_at"]) <= _time(attempt["started_at"])
                    <= _time(start["started_at"]) <= _time(attempt["finished_at"])
                    <= _time(row["updated_at"]))
            or native.get("state") != "finished" or native.get("exit_code") != 0
            or native.get("cleanup") != "observed-native-confirmed"
            or native.get("monitoring_complete") is not True
            or native.get("stdio_drained") is not True
            or any(native.get(key) is not False for key in ("cancelled", "timed_out"))
            or native.get("journal") != str(folder / "native-process.json")
            or journal.get("phase") != "finished" or journal.get("monitoring_complete") is not True
            or journal.get("result") != native
            or journal.get("runtime_identity") != native.get("runtime_identity")
            or intent.get("run_id") != spec["run_id"]
            or intent.get("policy_digest") != spec["policy_digest"]
            or intent.get("cwd") != spec["checkout"]
            or intent.get("environment_sha256") != digest(launch.get("environment"))
            or argv != launch.get("argv") or len(argv) != 5
            or argv[1:4] != ["-I", "-m", "devflow_temporal.role_runner"]
            or argv[4] != str(folder / "request.json") or not Path(argv[0]).is_absolute()
            or launch.get("environment", {}).get("CODEX_HOME")
            != str(_native_role_home(request) / "codex")
            or journal.get("owned", {}).get(str(attempt["pid"]), {}).get("identity")
            != attempt["process_identity"]
            or metadata.get("session_id") != attempt["session_id"]
            or metadata.get("resumed_from") != resumed
            or (resumed is not None and resumed != attempt["session_id"])
            or metadata.get("role") != "implement" or metadata.get("iteration") != row["iteration"]
            or metadata.get("result_digest") != digest(result)
            or metadata.get("output_candidate") != {
                key: candidate[key] for key in ("id", "head", "content_sha256")}):
        raise ValueError("completed build lost its exact native pass, output, session or monitor")
    reference = result.get("native_thread_observation") or {}
    observation_path = folder / "native-thread-observation.json"
    observation_bytes = _bytes(observation_path)
    observation = json.loads(observation_bytes)

    class ObservedBytes:
        def __str__(self):
            return str(observation_path)

        def read_bytes(self):
            return observation_bytes

    # Reuse the producer's bounded projection over the single authenticated
    # read. The full typed turns and result references have different schemas.
    projected = NativeThreadObservation.reference(SimpleNamespace(
        data=observation, path=ObservedBytes(),
        max_attempts=spec["policy"].get("provider_max_attempts", 1)))
    if (reference.get("path") != str(observation_path)
            or reference.get("sha256") != hashlib.sha256(observation_bytes).hexdigest()
            or reference != projected
            or any(observation.get(key) != value for key, value in {
                "state": "confirmed", "role": "implement", "iteration": row["iteration"],
                "run_id": spec["run_id"], "parent_thread_id": attempt["session_id"],
                "pid": start["pid"], "start_identity": journal.get("owned", {}).get(
                    str(start["pid"]), {}).get("identity"),
                "resumed_from": resumed, "new_child_thread_ids": [], "collaboration_items": [],
            }.items())
            or not observation.get("start_identity") or not observation.get("turns")
            or not _successful_turns(observation["turns"], attempt["session_id"])):
        raise ValueError("completed build lost its actual native thread observation")
    return {"job_key": attempt["job_key"], "request_sha256": digest(request),
            "result_sha256": digest(result), "native_journal_sha256": digest(journal),
            "launch_sha256": digest(launch), "observation_sha256": reference["sha256"],
            "runtime_identity": _runtime(native.get("runtime_identity"))}


def completed_build_subject(store, parent, spec, row, attempts, assignment, values, shared):
    """Authenticate reusable source only; no Temporal result, session admission or debit."""
    from .delivery_execution_config import load_execution_config
    from .delivery_feature_execution import worker_key
    from .delivery_feature_revisions import _checkpoint_proof, revision_checkpoints
    from .delivery_policy_recovery import _rows, work_binding
    from .delivery_source_scope import require_authorized
    from .delivery_stopped_resume import observed_native_cleanup

    worker = spec["feature_worker"]
    chunk = worker["chunk_id"]
    current = revision_checkpoints(parent, values)
    sealed = current.get("build:" + chunk)
    checkpoint_keys = [key for key, value in values.items() if value == sealed
                       and (key == "build:" + chunk or key.endswith(":build:" + chunk))]
    assignment_keys = [key for key, value in values.items() if value == assignment
                       and (key == "assignment:" + chunk + ":build"
                            or key.endswith(":assignment:" + chunk + ":build"))]
    observed_row, observed_attempts, effects, claim = _rows(store, spec["run_id"])
    if (dict(row) != observed_row or list(map(dict, attempts)) != observed_attempts
            or worker["kind"] != "build" or assignment.get("kind") != "build"
            or assignment.get("run_id") != spec["run_id"]
            or not checkpoint_keys or not assignment_keys
            or row["phase"] != "delivered" or row["outcome"] != "delivered"
            or row["execution_state"] != "terminal" or row["cleanup"] != "confirmed"
            or row["recovery_json"] is not None or row["pr_json"] is not None
            or claim is not None or not attempts
            or any(a["state"] != "finished" or a["cleanup"] != "confirmed" for a in attempts)
            or any(e["state"] != "complete" or e["observed_json"] is None
                   or e["kind"] == "publish" for e in effects)
            or spec.get("provider") != "codex"
            or spec["policy"].get("execution_backend") != "native-macos"
            or spec["policy_digest"] != digest(spec["policy"])
            or not store.owns_execution(spec)
            or digest(load_execution_config(spec).raw) != spec["config_digest"]):
        raise ValueError("completed build lacks finalized original source ownership")
    issue_id = parent["feature_delivery"]["owner"]["issue_id"]
    _checkpoint_proof(shared, issue_id, values, checkpoint_keys + assignment_keys)
    with shared.connect() as db:
        finished = db.execute("SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?",
                              (issue_id, worker_key(spec["run_id"],
                                                    spec["feature_delivery"]["owner"]))).fetchone()
    receipt = json.loads(finished["receipt_json"] or "null") if finished else None
    if (not finished or finished["state"] != "finished" or not receipt
            or receipt.get("run_id") != spec["run_id"] or receipt.get("outcome") != "delivered"
            or receipt.get("cleanup") != "confirmed"):
        raise ValueError("completed build lost its released source assignment")
    candidate = full_candidate(json.loads(row["candidate_json"] or "null"))
    checks = json.loads(row["checks_json"] or "{}")
    if (not sealed or sealed != checks.get("build_checkpoint")
            or sealed.get("worker_run_id") != spec["run_id"]
            or sealed.get("base") != spec["base_sha"]
            or sealed.get("candidate") != candidate
            or candidate["base_sha"] != spec["base_sha"]
            or candidate["policy_digest"] != spec["policy_digest"]):
        raise ValueError("completed build lost its exact sealed source candidate")
    require_authorized(spec["policy"], sealed["paths"])
    revision = spec.get("feature_plan_revision", {}).get("plan_revision")
    filename = f"feature-implementation-revision-{revision}.patch" if revision \
        else "feature-implementation.patch"
    if (sealed["path"] != str(Path(spec["state_dir"]) / filename)
            or hashlib.sha256(_bytes(Path(sealed["path"]))).hexdigest() != sealed["sha256"]):
        raise ValueError("completed build sealed patch is missing or changed")
    consumed = []
    for key, value in values.items():
        if key.startswith("worker-input:") and isinstance(value, dict):
            seed = value.get("feature_worker", {}).get("seed")
            if seed and seed.get("worker_run_id") == spec["run_id"]:
                if seed != sealed:
                    raise ValueError("completed build differs from its consumed source seed")
                consumed.append(key)
    _checkpoint_proof(shared, issue_id, values, consumed)
    native = _native_pass(spec, row, attempts, candidate)
    finalization_path = Path(spec["state_dir"]) / "resources/finalization.json"
    finalization_bytes = _bytes(finalization_path)
    finalization = json.loads(finalization_bytes)
    recorded_cleanup = {**finalization, "receipt": str(finalization_path),
                        "receipt_sha256": hashlib.sha256(finalization_bytes).hexdigest()}
    if (recorded_cleanup != checks.get("resource_cleanup")
            or finalization.get("state") != "confirmed"
            or finalization.get("outcome") != "delivered"
            or finalization.get("process_cleanup") != "observed-native-confirmed"
            or finalization.get("resource_cleanup") != "confirmed"):
        raise ValueError("completed build lost its exact source finalization receipt")
    broker = ObservationBroker(store, spec)
    changed = sorted(broker.validate_candidate_scope())
    broker._record()
    broker.observe_unpublished()
    if broker.candidate() != candidate or changed != sealed["paths"]:
        raise ValueError("completed build checkout differs from its sealed source")
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    proof = {"kind": "completed_unchanged_build_source", "checkpoint_sha256": digest(sealed),
             "native_pass": native, "consumed_inputs_sha256": digest(sorted(consumed)),
             "cleanup_digest": observed_native_cleanup(spec, read_only=True),
             "work_binding": binding, "row_sha256": digest(dict(row)),
             "attempts_sha256": digest(list(map(dict, attempts))),
             "effects_sha256": digest(effects)}
    return {"candidate": candidate, "terminal_candidate": candidate,
            "current_review_subject": None, "progress_custody": proof,
            "continuation_prerequisites_sha256": digest(proof)}
