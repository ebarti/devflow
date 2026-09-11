"""Capture actual instruction bytes and a separate nonsecret model-settings snapshot."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from devflow import __version__
from devflow.errors import WorkflowError
from devflow.installation import installed_release
from devflow.profiles import assert_admitted_profile, digest, load_profile
from devflow.runtime import package_root
from devflow.validation import validate_record


def observe_subagent_startup(assignment: dict, request: dict, put_artifact) -> dict:
    """Read two explicitly selected native metadata records, never transcript payloads.

    A spawn receipt establishes only a control path. This independent filesystem
    read binds that path to its native UUID and observed turn settings. Only the
    allowlisted projection is retained; service tier may be genuinely unknown.
    """
    task_id = request["task_id"]
    try:
        if str(UUID(task_id)) != task_id:
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise WorkflowError("invalid_host_identity", "A native child UUID is required") from exc
    path = Path(request["session_path"]).expanduser()
    root = Path(request.get("sessions_root", "~/.codex/sessions")).expanduser().resolve()
    if (path.is_symlink() or not path.resolve().is_relative_to(root)
            or not path.name.endswith(f"-{task_id}.jsonl")):
        raise WorkflowError("unsafe_metadata_source", "Select the child's exact native session file")
    selected = {"session_meta": request.get("session_meta_line", 1),
                "turn_context": request["turn_context_line"]}
    if any(type(n) is not int or n < 1 for n in selected.values()):
        raise WorkflowError("invalid_metadata_source", "Metadata line numbers must be positive")
    if len(set(selected.values())) != 2:
        raise WorkflowError("invalid_metadata_source", "Select distinct metadata records")
    records = {}
    with path.open() as stream:
        for line_number, raw in enumerate(stream, 1):
            if line_number in selected.values():
                if len(raw) > 8 * 1024 * 1024:
                    raise WorkflowError("invalid_metadata_source", "Metadata record is too large")
                item = json.loads(raw)
                kind = next(k for k, n in selected.items() if n == line_number)
                if item.get("type") != kind:
                    raise WorkflowError("invalid_metadata_source", "Selected line is not metadata")
                payload = item["payload"]
                allowed = ("id", "parent_thread_id", "agent_path", "thread_source") if (
                    kind == "session_meta"
                ) else ("turn_id", "model", "effort", "service_tier")
                records[kind] = {key: payload.get(key) for key in allowed}
                if kind == "turn_context":
                    records[kind]["timestamp"] = item.get("timestamp")
                if kind == "session_meta":
                    nested = payload.get("source", {}).get("subagent", {}).get("thread_spawn", {})
                    if nested and any(nested.get(key) != payload.get(key) for key in (
                        "parent_thread_id", "agent_path"
                    )):
                        raise WorkflowError("startup_identity_mismatch", "Native identity fields disagree")
            if line_number >= max(selected.values()):
                break
    if set(records) != set(selected):
        raise WorkflowError("metadata_unavailable", "Selected startup metadata is unavailable")
    meta, turn = records["session_meta"], records["turn_context"]
    if (meta["id"] != task_id or meta["parent_thread_id"] != assignment["owner_task_id"]
            or meta["agent_path"] != assignment["agent_name"] or meta["thread_source"] != "subagent"):
        raise WorkflowError("startup_identity_mismatch", "Native child parent/path/UUID differs")
    policy = assignment["role_policy"]
    if (not turn["turn_id"] or turn["model"] != policy["model"]
            or turn["effort"] != policy["reasoning_effort"]):
        raise WorkflowError("startup_policy_mismatch", "Observed model/effort differs from role policy")
    try:
        started_at = datetime.fromisoformat(turn["timestamp"].replace("Z", "+00:00"))
        if started_at.tzinfo is None or started_at > datetime.now(UTC):
            raise ValueError
    except (ValueError, TypeError, AttributeError) as exc:
        raise WorkflowError("invalid_metadata_timestamp", "Startup turn needs an observed timezone-aware timestamp") from exc
    reference = (f"native-session-jsonl:{path.resolve()}#session_meta={selected['session_meta']}"
                 f"&turn_context={selected['turn_context']}")
    observation = {"task_id": task_id, "parent_thread_id": meta["parent_thread_id"],
                   "agent_path": meta["agent_path"], "turn_id": turn["turn_id"],
                   "model": turn["model"], "reasoning_effort": turn["effort"],
                   "service_tier": turn["service_tier"], "source_reference": reference,
                   "policy_hash": policy["policy_hash"], "started_at": started_at.isoformat()}
    raw = json.dumps({"metadata": records, "observation": observation}, sort_keys=True).encode()
    return observation | {"artifact_hash": put_artifact(raw)}


def validate_start_snapshot(repository: Path, snapshot: dict, require_artifact) -> None:
    """Reject an unresumable or unsubstantiated policy before claiming work."""
    validate_record(snapshot, "workflow_snapshot")
    profile = load_profile(repository)
    release = installed_release(package_root())
    if (
        snapshot["package_revision"] != release["revision"]
        or release["revision"] != profile.lock["revision"]
        or snapshot["package_version"] != __version__
    ):
        raise WorkflowError("release_mismatch", "Attempt must capture the executing pinned release")
    assert_admitted_profile(profile, snapshot["repository_profile_reference"])
    for source in snapshot["instruction_sources"]:
        require_artifact(source["hash"])
    if snapshot["effective_settings_reference"] != f"sha256:{snapshot['model_policy_hash']}":
        raise WorkflowError("settings_mismatch", "Observed settings must bind their stored bytes")
    require_artifact(snapshot["model_policy_hash"])
    expected = digest(
        {"package_version": __version__, "release": profile.lock,
         "instructions": snapshot["instruction_sources"], "profile": profile.fingerprint}
    )
    if snapshot["workflow_hash"] != expected:
        raise WorkflowError("snapshot_mismatch", "Workflow hash must bind captured package and inputs")


def capture_snapshot(repository: Path, request: dict, put_artifact) -> dict:
    profile = load_profile(repository)
    release = installed_release(package_root())
    if release["revision"] != profile.lock["revision"]:
        raise WorkflowError("release_mismatch", "Capture must use the repository's pinned release")
    sources = []
    paths = [(profile.root / entry["reference"]) for entry in profile.sources]
    paths += [Path(p).expanduser() for p in request.get("instruction_paths", [])]
    for path in dict.fromkeys(paths):
        if not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
            raise WorkflowError(
                "instruction_unavailable", "Instruction must be an existing bounded file"
            )
        raw = path.read_bytes()
        key = put_artifact(raw)
        if key != hashlib.sha256(raw).hexdigest():
            raise WorkflowError("artifact_mismatch", "Instruction artifact hash mismatch")
        sources.append({"reference": str(path.resolve()), "hash": key})
    settings = request.get("effective_settings")
    if not isinstance(settings, dict) or not settings:
        raise WorkflowError(
            "settings_required", "Supply observed effective settings; defaults are not inferred"
        )
    allowed = {"model", "reasoning_effort", "service_tier", "role", "source_reference"}
    if not set(settings) <= allowed or not all(
        isinstance(settings.get(k), str) and settings[k]
        for k in ("model", "reasoning_effort", "source_reference")
    ) or (settings.get("service_tier") is not None and not (
        isinstance(settings["service_tier"], str) and settings["service_tier"]
    )):
        raise WorkflowError(
            "settings_invalid", "Only observed nonsecret model settings are accepted"
        )
    raw_settings = json.dumps(settings, sort_keys=True, separators=(",", ":")).encode()
    settings_hash = put_artifact(raw_settings)
    workflow_hash = digest(
        {
            "package_version": __version__,
            "release": profile.lock,
            "instructions": sources,
            "profile": profile.fingerprint,
        }
    )
    return {
        "schema_version": 1,
        "record_type": "workflow_snapshot",
        "snapshot_id": request["snapshot_id"],
        "package_version": __version__,
        "package_revision": release["revision"],
        "workflow_hash": workflow_hash,
        "model_policy_hash": settings_hash,
        "instruction_sources": sources,
        "repository_profile_reference": f"sha256:{profile.fingerprint}",
        "effective_settings_reference": f"sha256:{settings_hash}",
        "captured_at": datetime.now(UTC).isoformat(),
    }
