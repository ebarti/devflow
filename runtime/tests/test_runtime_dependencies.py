"""Lock/export agreement and versioned image authority, including legacy readbacks."""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import tomllib
from pathlib import Path

import pytest

from devflow_temporal import delivery_config, delivery_preparation
from devflow_temporal import runtime_dependencies as dependencies
from devflow_temporal.payload import payload_digest


@pytest.mark.parametrize("change", ["cli_version", "git_source"])
def test_mixed_or_unlocked_runtime_dependencies_are_rejected(tmp_path, change):
    lock = (dependencies.RUNTIME / "uv.lock").read_text()
    if change == "cli_version":
        lock = lock.replace('name = "openai-codex-cli-bin"\nversion = "0.160.0"',
                            'name = "openai-codex-cli-bin"\nversion = "0.159.0"')
    else:
        lock = lock.replace('source = { registry = "https://pypi.org/simple" }',
                            'source = { git = "https://example.invalid/kit" }', 1)
    (tmp_path / "uv.lock").write_text(lock)
    with pytest.raises(ValueError, match="versions differ|public-index"):
        dependencies.locked_dependency_identity(tmp_path)


def test_frozen_export_uses_locked_kit_and_sdk_and_refuses_changed_lock(tmp_path):
    identity = dependencies.locked_dependency_identity()
    exported = dependencies.frozen_requirements(identity)
    assert f'agent-runtime-kit=={identity["kit_version"]} ' in exported
    assert f'openai-codex=={identity["codex_sdk_version"]} ' in exported
    assert f'openai-codex-cli-bin=={identity["codex_cli_version"]} ' in exported
    lock = tomllib.loads((dependencies.RUNTIME / "uv.lock").read_text())
    kit = next(package for package in lock["package"] if package["name"] == "agent-runtime-kit")
    assert all(wheel["hash"] in exported for wheel in kit["wheels"])
    assert "git+" not in exported and "devflow-temporal" not in exported
    (tmp_path / "uv.lock").write_bytes((dependencies.RUNTIME / "uv.lock").read_bytes() + b"\n")
    with pytest.raises(ValueError, match="lock changed before"):
        dependencies.frozen_requirements(identity, tmp_path)


@pytest.mark.parametrize("change", [None, "kit", "sdk", "lock", "legacy"])
def test_image_readback_enforces_dependency_identity_and_retains_legacy_contract(
    tmp_path, monkeypatch, change
):
    package = delivery_preparation.PACKAGE
    profile = delivery_preparation.SECCOMP
    docker = Path("/usr/bin/true").resolve()
    (tmp_path / "pnpm-lock.yaml").write_text("lockfileVersion: '9.0'\n")
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    identity = dependencies.locked_dependency_identity()
    container = {
        "docker_bin": str(docker), "docker_bin_sha256": sha(docker),
        "image_id": "sha256:" + "a" * 64, "platform": "linux/arm64",
        "seccomp_profile": str(profile), "seccomp_sha256": sha(profile),
        "role_runner_sha256": sha(package / "role_runner.py"),
        "runtime_payload_sha256": payload_digest(package, delivery_preparation.LAUNCHER),
        "codex_bin": delivery_preparation.CODEX_BINARY,
        "codex_bin_sha256": dependencies.CODEX_BINARY_SHA256,
        "pnpm_lock_sha256": sha(tmp_path / "pnpm-lock.yaml"),
        "runtime_dependencies": identity,
    }
    labels = {
        "devflow.role_runner_sha256": container["role_runner_sha256"],
        "devflow.runtime_payload_sha256": container["runtime_payload_sha256"],
        "devflow.codex_bin_sha256": container["codex_bin_sha256"],
        **dependencies.dependency_labels(identity),
    }
    if change == "kit":
        labels["devflow.kit_version"] = "0.5.2"
    elif change == "sdk":
        labels["devflow.codex_sdk_version"] = "0.157.1"
    elif change == "lock":
        container["runtime_dependencies"] = {**identity, "lock_sha256": "b" * 64}
    elif change == "legacy":
        container.pop("runtime_dependencies")
        labels = {key: value for key, value in labels.items()
                  if key not in dependencies.dependency_labels(identity)}
        labels.update({"devflow.kit_revision": delivery_config.LEGACY_KIT_REVISION,
                       "devflow.codex_cli_version": delivery_config.LEGACY_CODEX_VERSION})
    image = {"Id": container["image_id"], "Os": "linux", "Architecture": "arm64",
             "Config": {"Labels": labels}}
    monkeypatch.setattr(delivery_config.subprocess, "run", lambda *_args, **_kwargs:
                        subprocess.CompletedProcess([], 0, json.dumps([image]), ""))
    if change in {"kit", "sdk", "lock"}:
        with pytest.raises(ValueError, match="launch chain|dependency lock changed"):
            delivery_config._container_identity(container, source=tmp_path)
    else:
        actual = delivery_config._container_identity(container, source=tmp_path)
        assert actual["image_id"] == container["image_id"]
        if change == "legacy":
            assert "runtime_dependencies" not in actual
        else:
            assert actual["runtime_dependencies"] == copy.deepcopy(identity)
