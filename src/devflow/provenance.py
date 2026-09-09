"""Capture actual instruction bytes and a separate nonsecret model-settings snapshot."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

from devflow import __version__
from devflow.errors import WorkflowError
from devflow.installation import installed_release
from devflow.profiles import assert_admitted_profile, digest, load_profile
from devflow.runtime import package_root
from devflow.validation import validate_record


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
        for k in ("model", "reasoning_effort", "service_tier", "source_reference")
    ):
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
