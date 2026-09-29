"""Capture instruction, routing-policy and observed-settings bytes separately."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from pathlib import Path

from devflow import __version__
from devflow.errors import WorkflowError
from devflow.installation import installed_release
from devflow.profiles import assert_admitted_profile, digest, load_profile
from devflow.runtime import package_root
from devflow.validation import validate_record


def _policy_manifest(sources: list[dict]) -> dict:
    return {"schema_version": 1, "kind": "model_routing_policy", "sources": sources}


def _workflow_hash(profile, sources, policy_reference=None, *, separate_policy=False):
    values = {"package_version": __version__, "release": profile.lock,
              "instructions": sources, "profile": profile.fingerprint}
    if separate_policy:
        values["model_policy_reference"] = policy_reference
    return digest(values)


def _capture_sources(paths, put_artifact):
    sources = []
    for path in dict.fromkeys(Path(p).expanduser().resolve() for p in paths):
        if not path.is_file() or path.stat().st_size > 8 * 1024 * 1024:
            raise WorkflowError(
                "instruction_unavailable", "Instruction must be an existing bounded file"
            )
        raw = path.read_bytes()
        key = put_artifact(raw)
        if key != hashlib.sha256(raw).hexdigest():
            raise WorkflowError("artifact_mismatch", "Instruction artifact hash mismatch")
        sources.append({"reference": str(path), "hash": key})
    return sources


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
    settings_reference = snapshot["effective_settings_reference"]
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", settings_reference):
        raise WorkflowError("settings_mismatch", "Observed settings must bind their stored bytes")
    require_artifact(settings_reference.removeprefix("sha256:"))
    separate = "model_policy_status" in snapshot
    if not separate:
        # Historical 0.3.0 records used this field for actual settings. Preserve
        # their binding, but reports must label this as legacy settings only.
        if settings_reference != f"sha256:{snapshot['model_policy_hash']}":
            raise WorkflowError("settings_mismatch", "Observed settings must bind their stored bytes")
    elif snapshot["model_policy_status"] == "captured":
        policy_sources = snapshot["model_policy_sources"]
        policy_hash = digest(_policy_manifest(policy_sources))
        if (not policy_sources or snapshot["model_policy_hash"] != policy_hash
                or snapshot["model_policy_reference"] != f"sha256:{policy_hash}"):
            raise WorkflowError("model_policy_mismatch", "Routing policy must bind its own sources")
        for source in policy_sources:
            require_artifact(source["hash"])
        require_artifact(policy_hash)
    elif (snapshot["model_policy_hash"] is not None
          or snapshot["model_policy_reference"] is not None
          or snapshot["model_policy_sources"]):
        raise WorkflowError("model_policy_mismatch", "Unavailable routing policy cannot claim a hash")
    expected = _workflow_hash(
        profile, snapshot["instruction_sources"], snapshot.get("model_policy_reference"),
        separate_policy=separate,
    )
    if snapshot["workflow_hash"] != expected:
        raise WorkflowError("snapshot_mismatch", "Workflow hash must bind captured package and inputs")


def capture_snapshot(repository: Path, request: dict, put_artifact) -> dict:
    profile = load_profile(repository)
    release = installed_release(package_root())
    if release["revision"] != profile.lock["revision"]:
        raise WorkflowError("release_mismatch", "Capture must use the repository's pinned release")
    paths = [(profile.root / entry["reference"]) for entry in profile.sources]
    paths += [Path(p).expanduser() for p in request.get("instruction_paths", [])]
    sources = _capture_sources(paths, put_artifact)
    policy_sources = _capture_sources(request.get("model_policy_paths", []), put_artifact)
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
    policy_hash = None
    if policy_sources:
        policy = _policy_manifest(policy_sources)
        raw_policy = json.dumps(policy, sort_keys=True, separators=(",", ":"),
                                ensure_ascii=False).encode()
        policy_hash = put_artifact(raw_policy)
        if policy_hash != digest(policy):
            raise WorkflowError("artifact_mismatch", "Routing-policy artifact hash mismatch")
    policy_reference = f"sha256:{policy_hash}" if policy_hash else None
    workflow_hash = _workflow_hash(profile, sources, policy_reference, separate_policy=True)
    return {
        "schema_version": 1,
        "record_type": "workflow_snapshot",
        "snapshot_id": request["snapshot_id"],
        "package_version": __version__,
        "package_revision": release["revision"],
        "workflow_hash": workflow_hash,
        "model_policy_hash": policy_hash,
        "model_policy_reference": policy_reference,
        "model_policy_sources": policy_sources,
        "model_policy_status": "captured" if policy_sources else "unavailable",
        "instruction_sources": sources,
        "repository_profile_reference": f"sha256:{profile.fingerprint}",
        "effective_settings_reference": f"sha256:{settings_hash}",
        "captured_at": datetime.now(UTC).isoformat(),
    }
