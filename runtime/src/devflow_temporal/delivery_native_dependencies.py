"""Frozen PNPM fetch data, never candidate setup code or package-manager hooks."""

from __future__ import annotations

import hashlib
import json
import re
import stat
import subprocess
from pathlib import Path
from urllib.parse import urlsplit

import yaml

REGISTRY = "registry.npmjs.org"


def _registry_url(value: str) -> None:
    url = urlsplit(value)
    if (
        url.scheme != "https"
        or url.hostname != REGISTRY
        or url.port not in (None, 443)
        or url.username
        or url.password
        or url.query
        or url.fragment
    ):
        raise ValueError("dependency lock target is outside the admitted npm registry")


def _registry_data(value) -> None:
    pending, visited, count = [value], set(), 0
    while pending:
        item = pending.pop()
        count += 1
        if count > 200000:
            raise ValueError("frozen dependency data is unbounded")
        if isinstance(item, (list, dict)):
            if id(item) in visited:
                raise ValueError("frozen dependency data contains recursive or aliased setup")
            visited.add(id(item))
            pending.extend(item if isinstance(item, list) else [*item, *item.values()])
        elif isinstance(item, str) and re.match(
            r"^(?:https?:|git\+|git:|ssh:|github:|file:)", item
        ):
            _registry_url(item)


def frozen_pnpm_inputs(
    spec: dict, checkout: Path, *, provenance: dict | None = None
) -> tuple[str, dict[str, bytes]]:
    source_hashes = {}

    def blob(name: str, *, optional: bool = False) -> bytes | None:
        path = Path(name)
        if path.is_absolute() or any(p in {"..", ".git", ".codex"} for p in path.parts):
            raise ValueError("frozen PNPM input escaped the admitted Git tree")
        prefix = ["git", "-c", "core.fsmonitor=false", "-C", spec["source_path"]]
        entry = subprocess.check_output([*prefix, "ls-tree", spec["base_sha"], "--", name])
        if not entry and optional:
            return None
        if not entry.startswith((b"100644 blob ", b"100755 blob ")):
            raise ValueError("frozen PNPM input is absent or is not regular Git data")
        content = subprocess.check_output([*prefix, "show", spec["base_sha"] + ":" + name])
        if len(content) > 10 * 1024 * 1024:
            raise ValueError("frozen PNPM input exceeds its bounded size")
        source_hashes[name] = hashlib.sha256(content).hexdigest()
        return content

    lock = blob("pnpm-lock.yaml")
    manifest = json.loads(blob("package.json"))
    manager = manifest.get("packageManager")
    if not isinstance(manager, str) or not re.fullmatch(
        r"pnpm@\d+\.\d+\.\d+(?:\+sha(?:224|256|512)\.[0-9a-fA-F]+)?", manager
    ):
        raise ValueError("native dependency preparation requires a frozen exact PNPM version")
    for name, expected in (("pnpm-lock.yaml", lock),):
        path = checkout / name
        if path.is_symlink() or not path.is_file() or path.read_bytes() != expected:
            raise ValueError("candidate dependency lock differs from the admitted Git base")
    candidate_manifest = checkout / "package.json"
    if (
        candidate_manifest.is_symlink()
        or not candidate_manifest.is_file()
        or json.loads(candidate_manifest.read_bytes()).get("packageManager") != manager
    ):
        raise ValueError("candidate package manager differs from the admitted Git base")
    document = yaml.safe_load(lock)
    if not isinstance(document, dict) or str(document.get("lockfileVersion")) != "9.0":
        raise ValueError("native dependency preparation requires the supported frozen PNPM lock")
    _registry_data(document)
    packages = document.get("packages") or {}
    if not isinstance(packages, dict) or len(packages) > 20000:
        raise ValueError("frozen dependency package set is malformed or unbounded")
    for name, package in packages.items():
        if not isinstance(name, str) or ":" in name or not isinstance(package, dict):
            raise ValueError("dependency lock contains an unsupported external package source")
        resolution = package.get("resolution", {})
        if (
            not isinstance(resolution, dict)
            or set(resolution) - {"integrity", "tarball"}
            or not re.fullmatch(
                r"sha(?:256|384|512)-[A-Za-z0-9+/=]+", str(resolution.get("integrity", ""))
            )
        ):
            raise ValueError("dependency lock requires registry package integrity")
        if "tarball" in resolution:
            _registry_url(resolution["tarball"])
    inputs = {
        "pnpm-lock.yaml": lock,
        "package.json": (json.dumps({"packageManager": manager}) + "\n").encode(),
    }
    workspace = blob("pnpm-workspace.yaml", optional=True)
    if workspace is not None:
        value = yaml.safe_load(workspace)
        if not isinstance(value, dict):
            raise ValueError("frozen PNPM workspace is malformed")
        _registry_data(value)
        # Only data needed by fetch is copied. Build/setup/config dependencies,
        # credentials, hooks and candidate project configuration are excluded.
        inputs["pnpm-workspace.yaml"] = yaml.safe_dump(
            {
                key: value[key]
                for key in ("packages", "catalog", "catalogs", "patchedDependencies")
                if key in value
            }
        ).encode()
    patches = document.get("patchedDependencies") or {}
    if not isinstance(patches, dict) or len(patches) > 32:
        raise ValueError("frozen patch inputs are malformed or unbounded")
    for patch in patches.values():
        if not isinstance(patch, dict) or not isinstance(patch.get("path"), str):
            raise ValueError("frozen dependency patch path is malformed")
        if not patch["path"].endswith(".patch") or patch["path"] in inputs:
            raise ValueError("frozen dependency patch cannot supply setup configuration")
        inputs[patch["path"]] = blob(patch["path"])
    if provenance is not None:
        provenance.update(
            schema="devflow-frozen-pnpm-input-provenance-v1",
            source_input_hashes=source_hashes,
            input_hashes_semantics="staged bytes after the declared transformations",
            transformations={
                name: ({"operation": "json-key-allowlist", "keys": ["packageManager"]}
                       if name == "package.json" else
                       {"operation": "yaml-key-allowlist", "keys": [
                           "packages", "catalog", "catalogs", "patchedDependencies"]}
                       if name == "pnpm-workspace.yaml" else {"operation": "identity"})
                for name in inputs
            },
        )
    return manager, inputs


def write_frozen_inputs(staging: Path, inputs: dict[str, bytes]) -> dict[str, str]:
    from .delivery_resources import private_directory

    hashes = {}
    for name, content in inputs.items():
        path = staging / name
        private_directory(path.parent)
        if path.is_symlink():
            raise ValueError("frozen dependency input was replaced")
        if path.exists():
            if not stat.S_ISREG(path.lstat().st_mode) or path.read_bytes() != content:
                raise ValueError("frozen dependency input changed across replay")
        else:
            path.write_bytes(content)
            path.chmod(0o600)
        hashes[name] = hashlib.sha256(content).hexdigest()
    return hashes
