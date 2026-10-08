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


def frozen_native_projects(spec: dict, checkout: Path) -> list[str]:
    """Find native owners from the admitted lock, independent of test prose."""
    _, inputs = frozen_pnpm_inputs(spec, checkout)
    importers = yaml.safe_load(inputs["pnpm-lock.yaml"]).get("importers", {})
    if not isinstance(importers, dict) or len(importers) > 256:
        raise ValueError("native dependency importers are malformed or unbounded")
    projects = []
    for name, importer in importers.items():
        if (not isinstance(name, str) or Path(name).is_absolute()
                or ".." in Path(name).parts or not isinstance(importer, dict)):
            raise ValueError("native dependency importer escaped the frozen workspace")
        dependencies = importer.get("dependencies", {})
        if not isinstance(dependencies, dict):
            raise ValueError("native dependency importer is malformed")
        if "better-sqlite3" in dependencies:
            projects.append(name)
    return sorted(projects)


def native_addon_authority(spec: dict, checkout: Path, projects: list[str]) -> dict | None:
    """A single registry target explicitly allowed by unchanged frozen manifests."""
    manager, _ = frozen_pnpm_inputs(spec, checkout)
    hashes = {}

    def unchanged(name):
        entry = subprocess.check_output(
            ["git", "-C", spec["source_path"], "ls-tree", spec["base_sha"], "--", name]
        )
        if not entry.startswith((b"100644 blob ", b"100755 blob ")):
            raise ValueError("native addon metadata is not regular frozen Git data")
        raw = subprocess.check_output(
            ["git", "-C", spec["source_path"], "show", spec["base_sha"] + ":" + name]
        )
        path = checkout / name
        if (
            len(raw) > 10 * 1024 * 1024
            or path.resolve(strict=True) != path
            or not path.is_file()
            or path.read_bytes() != raw
        ):
            raise ValueError("native addon metadata differs from the frozen Git base")
        hashes[name] = hashlib.sha256(raw).hexdigest()
        return raw

    selected = []
    for project in sorted(set(projects)):
        path = Path(project)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("native addon project escaped its tracked owner")
        manifest = json.loads(unchanged((path / "package.json").as_posix()))
        if "better-sqlite3" in manifest.get("dependencies", {}):
            selected.append(project)
    if not selected:
        return None
    root = json.loads(unchanged("package.json"))
    lock = yaml.safe_load(unchanged("pnpm-lock.yaml"))
    allowed = root.get("pnpm", {}).get("onlyBuiltDependencies", [])
    workspace = checkout / "pnpm-workspace.yaml"
    if workspace.exists():
        workspace_data = yaml.safe_load(unchanged("pnpm-workspace.yaml"))
        allowed = workspace_data.get("onlyBuiltDependencies", allowed)
    if not isinstance(allowed, list) or "better-sqlite3" not in allowed:
        raise ValueError("frozen metadata does not permit the native addon target")
    versions = {lock["importers"][p]["dependencies"]["better-sqlite3"]["version"] for p in selected}
    if len(versions) != 1:
        raise ValueError("native addon requires one exact frozen version")
    version = versions.pop()
    if not isinstance(version, str) or not re.fullmatch(r"\d+\.\d+\.\d+", version):
        raise ValueError("native addon lock target is not an exact supported version")
    target = "better-sqlite3@" + version
    integrity = lock["packages"][target]["resolution"]["integrity"]
    # No candidate configuration can select hooks, registries, scripts or build tools.
    for name in (".npmrc", ".pnpmfile.cjs", "pnpmfile.cjs"):
        if (checkout / name).exists() or (checkout / name).is_symlink():
            raise ValueError("candidate package-manager setup is not native build authority")
    pending, packages, dependency_links = [target], {}, {}
    while pending:
        key = pending.pop()
        if key in packages:
            continue
        if len(packages) >= 128 or not re.fullmatch(r"[a-z0-9_.-]+@\d+\.\d+\.\d+", key):
            raise ValueError("native addon dependency closure is unsupported or unbounded")
        snapshot = lock.get("snapshots", {}).get(key, {})
        packages[key] = lock["packages"][key]["resolution"]["integrity"]
        dependency_links[key] = {
            name: name + "@" + value
            for name, value in snapshot.get("dependencies", {}).items()
        }
        pending.extend(dependency_links[key].values())
    return {
        "target": target,
        "version": version,
        "integrity": integrity,
        "registry_packages": packages,
        "dependency_links": dependency_links,
        "package_manager": manager,
        "projects": selected,
        "metadata": hashes,
        "base_sha": spec["base_sha"],
        "source_path": spec["source_path"],
        "policy_digest": spec.get("policy_digest"),
    }


def validate_native_addon(
    checkout: Path, authority: dict, store: Path, *, dependency: bool = False
) -> Path:
    """Read back registry file integrities before executing the target lifecycle."""
    import base64
    import os
    import sys

    name, version = authority["target"].split("@")
    root = checkout / "node_modules/.pnpm" / authority["target"] / "node_modules" / name
    if (root.resolve(strict=True) != root or not root.is_dir()
            or root.stat().st_uid != os.getuid()):
        raise ValueError("native addon package left its generated checkout")
    if not dependency:
        for project in authority["projects"]:
            link = checkout / project / "node_modules" / name
            if not link.is_symlink() or link.resolve(strict=True) != root:
                raise ValueError("native addon project alias differs from its frozen lock target")
    expected_links = authority.get("dependency_links", {}).get(authority["target"], {})
    for parent in (root.parent, root / "node_modules"):
        if not parent.exists():
            continue
        if parent.resolve(strict=True) != parent:
            raise ValueError("native addon dependency alias has an indirect module root")
        for link in parent.iterdir():
            if link.name in {".bin", root.name}:
                continue
            target = expected_links.get(link.name)
            expected = (checkout / "node_modules/.pnpm" / target / "node_modules" / link.name
                        if target else None)
            if not expected or not link.is_symlink() or link.resolve(strict=True) != expected:
                raise ValueError(
                    "native addon dependency alias differs from its frozen lock target"
                )
    for dependency_name, target in expected_links.items():
        link = root.parent / dependency_name
        expected = checkout / "node_modules/.pnpm" / target / "node_modules" / dependency_name
        if not link.is_symlink() or link.resolve(strict=True) != expected:
            raise ValueError("native addon dependency alias is missing its frozen lock target")
    modules = checkout / "node_modules/.modules.yaml"
    if modules.resolve(strict=True) != modules or not modules.is_file():
        raise ValueError("native addon module ownership is not fixed")
    layout = yaml.safe_load(modules.read_bytes())
    actual_store = Path(layout["storeDir"])
    if actual_store != store / "v10" or actual_store.resolve(strict=True) != actual_store:
        raise ValueError("native addon store differs from controller frozen fetch")
    if Path(layout["virtualStoreDir"]) not in {Path(".pnpm"), checkout / "node_modules/.pnpm"}:
        raise ValueError("native addon virtual store escaped its generated root")
    algorithm, encoded = authority["integrity"].split("-", 1)
    hex_digest = base64.b64decode(encoded, validate=True).hex()[:64]
    index = (
        actual_store
        / "index"
        / hex_digest[:2]
        / (hex_digest[2:] + "-" + authority["target"] + ".json")
    )
    info = index.lstat()
    if (
        index.resolve(strict=True) != index
        or not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_nlink != 1
        or info.st_size > 10 * 1024 * 1024
    ):
        raise ValueError("native addon registry index is not bounded owned data")
    data = json.loads(index.read_bytes())
    files = data.get("files")
    if (
        data.get("name") != name
        or data.get("version") != version
        or not isinstance(files, dict)
        or not 1 <= len(files) <= 10000
    ):
        raise ValueError("native addon registry index has a different target")
    for name, record in files.items():
        path = root / name
        if (
            Path(name).is_absolute()
            or ".." in Path(name).parts
            or path.resolve(strict=True) != path
            or not path.is_file()
            or path.stat().st_uid != os.getuid()
        ):
            raise ValueError("native addon source escaped its registry package")
        algorithm, encoded = record["integrity"].split("-", 1)
        if algorithm not in {"sha256", "sha384", "sha512"}:
            raise ValueError("native addon source integrity algorithm is unsupported")
        if hashlib.new(algorithm, path.read_bytes()).digest() != base64.b64decode(
            encoded, validate=True
        ):
            raise ValueError("native addon source differs from frozen registry integrity")
    for path in root.rglob("*"):
        relative = path.relative_to(root)
        if (
            relative.parts[0] not in {"build", "node_modules"}
            and path.is_file()
            and str(relative) not in files
        ):
            raise ValueError("native addon contains unverified registry source")
    build = root / "build"
    if build.is_symlink() or (build.exists() and any(
        p.is_symlink() and not (
            p.relative_to(build).parts in {
                ('node_gyp_bins', 'python'), ('node_gyp_bins', 'python3')
            }
            and p.resolve(strict=True) == Path(sys.executable).resolve(strict=True)
        ) for p in build.rglob('*')
    )):
        raise ValueError("native addon build output contains an aliased generated path")
    for bin_root in (checkout / 'node_modules/.bin', root / 'node_modules/.bin',
                     root.parent / '.bin'):
        if (bin_root / 'node').exists() or (bin_root / 'node').is_symlink():
            raise ValueError('native addon lifecycle cannot shadow the frozen Node interpreter')
    if dependency:
        return root
    for target, integrity in authority["registry_packages"].items():
        if target != authority["target"]:
            validate_native_addon(
                checkout, {**authority, "target": target, "integrity": integrity},
                store, dependency=True
            )
    manifest = json.loads((root / "package.json").read_bytes())
    if manifest.get("scripts", {}).get(
        "install"
    ) != "prebuild-install || node-gyp rebuild --release" or set(manifest.get("scripts", {})) & {
        "preinstall",
        "postinstall",
    }:
        raise ValueError("native addon lifecycle differs from the bounded target build")
    return root


def frozen_native_builder(spec: dict, manager: str) -> dict:
    """Bind the already-frozen PNPM cache builder, never a candidate lifecycle shim."""
    import os

    cache = Path(spec['policy']['package_manager_cache'])
    version = manager.split('@', 1)[1].split('+', 1)[0]
    root = cache / 'v1/pnpm' / version
    modules = root / 'dist/node_modules'
    entry = modules / 'node-gyp/bin/node-gyp.js'
    if cache.resolve(strict=True) != cache or modules.resolve(strict=True) != modules:
        raise ValueError('native builder escaped the frozen Corepack cache')
    manifests = {}
    for name in ('.corepack', 'package.json', 'dist/node_modules/node-gyp/package.json'):
        path = root / name
        info = path.lstat()
        if (path.resolve(strict=True) != path or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid() or info.st_size > 1024 * 1024):
            raise ValueError('native builder metadata is not owned regular cache data')
        manifests[name] = path.read_bytes()
    locator = json.loads(manifests['.corepack'])['locator']
    pnpm = json.loads(manifests['package.json'])
    builder = json.loads(manifests['dist/node_modules/node-gyp/package.json'])
    if (locator != {'name': 'pnpm', 'reference': version}
            or pnpm.get('name') != 'pnpm' or pnpm.get('version') != version
            or builder.get('name') != 'node-gyp' or builder.get('bin') != './bin/node-gyp.js'):
        raise ValueError('native builder does not belong to the frozen PNPM package')
    hashes, size = {}, 0
    for path in sorted(modules.rglob('*')):
        info = path.lstat()
        if path.is_symlink() or info.st_uid != os.getuid():
            raise ValueError('native builder module closure contains a foreign or aliased source')
        if path.is_dir():
            continue
        if not stat.S_ISREG(info.st_mode):
            raise ValueError('native builder module closure is not regular source')
        size += info.st_size
        if size > 64 * 1024 * 1024 or len(hashes) >= 10000:
            raise ValueError('native builder module closure exceeds its bounded size')
        hashes[str(path.relative_to(modules))] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not entry.is_file() or entry.resolve(strict=True) != entry:
        raise ValueError('native builder entry is not fixed authenticated cache source')
    closure_digest = hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()
    return {'absolute_path': str(entry), 'realpath': str(entry),
            'sha256': hashlib.sha256(entry.read_bytes()).hexdigest(),
            'content_sha256': closure_digest,
            'version': builder['version'], 'package_manager': manager,
            'metadata': {name: hashlib.sha256(content).hexdigest()
                         for name, content in manifests.items()}}
