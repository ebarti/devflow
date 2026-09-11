"""Pinned releases and explicit, conflict-checked managed link/config transactions.

Plans never mutate targets. Apply replaces directory entries, never writes through
links. Rollback restores managed entries only and retains releases, environments,
work, evidence and history. This is not a host-wide migration framework.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import re
import shutil
import stat
import subprocess
import tarfile
import tempfile
import tomllib
import uuid
from pathlib import Path

from devflow.durability import flush_descriptor, flush_directory
from devflow.errors import WorkflowError


def _hash(data):
    return hashlib.sha256(data).hexdigest()


def _json_hash(value):
    return _hash(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def _git(source, *args):
    result = subprocess.run(["git", "-C", str(source), *args], capture_output=True, check=False)
    if result.returncode:
        raise WorkflowError("install_source", "Could not verify the pinned Git source")
    return result.stdout


def _archive(source, revision):
    if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision):
        raise WorkflowError("install_unpinned", "Installation requires a full immutable commit ID")
    if _git(source, "rev-parse", "HEAD").decode().strip() != revision:
        raise WorkflowError("install_source", "Source HEAD does not match the pinned commit")
    if _git(source, "status", "--porcelain", "--untracked-files=all").strip():
        raise WorkflowError("install_dirty", "Source must be clean, including untracked files")
    tree = _git(source, "rev-parse", f"{revision}^{{tree}}").decode().strip()
    archive = _git(source, "archive", "--format=tar", revision)
    files = {}
    with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
        for item in bundle:
            path = Path(item.name)
            if path.is_absolute() or ".." in path.parts or item.issym() or item.islnk():
                raise WorkflowError("install_source", "Release must contain ordinary files only")
            if item.isfile():
                content = bundle.extractfile(item).read()
                files[item.name] = {"hash": _hash(content), "executable": bool(item.mode & 0o111)}
                local = Path(source) / path
                if local.is_symlink() or not local.is_file() or _hash(local.read_bytes()) != _hash(content):
                    raise WorkflowError("install_dirty", "Source bytes differ from the pinned archive")
    if ".devflow-release.json" in files:
        raise WorkflowError("install_source", "Release metadata path is reserved for the installer")
    if not {"pyproject.toml", "uv.lock", "skills/devflow/SKILL.md"}.issubset(files):
        raise WorkflowError("install_source", "Release lacks package lock or skill entry")
    project = tomllib.loads((Path(source) / "pyproject.toml").read_text())["project"]
    version = re.match(r"^(\d+)\.(\d+)\.(\d+)", project.get("version", ""))
    if version and tuple(map(int, version.groups())) >= (0, 5, 0):
        from devflow.skill_routing import STAGE_SKILLS

        if not {f"skills/{name}/SKILL.md" for name in STAGE_SKILLS}.issubset(files):
            raise WorkflowError("install_source", "Release lacks the complete stage skill catalog")
    return tree, archive, files


def _safe_parents(path):
    for parent in path.parents:
        if parent.is_symlink():
            raise WorkflowError("install_symlink", "Refusing a target with a symlink parent")


def _snapshot(path, *, resolved=True):
    path = Path(path)
    _safe_parents(path)
    if path.is_symlink():
        target = os.readlink(path)
        result = {"kind": "symlink", "target": target}
        if resolved:
            try:
                resolved_path = path.resolve(strict=True)
                result["resolved_path"] = str(resolved_path)
                result["resolved_hash"] = _content_hash(resolved_path)
            except (FileNotFoundError, RuntimeError):
                result["resolved_path"] = None
                result["resolved_hash"] = None
        return result
    if not path.exists():
        return {"kind": "absent"}
    if not path.is_file():
        raise WorkflowError("install_collision", "Managed target is not an ordinary file or link")
    return {"kind": "file", "content": base64.b64encode(path.read_bytes()).decode(),
            "mode": stat.S_IMODE(path.stat().st_mode)}


def _content_hash(path):
    if path.is_file():
        return _hash(path.read_bytes())
    if path.is_dir():
        entries = {}
        for entry in sorted(path.rglob("*")):
            relative = str(entry.relative_to(path))
            if entry.is_symlink():
                entries[relative] = {"link": os.readlink(entry)}
            elif entry.is_file():
                entries[relative] = {"hash": _hash(entry.read_bytes())}
        return _json_hash(entries)
    raise WorkflowError("install_collision", "Unsupported shared target")


def plan_install(source, revision, install_root, *, links, owned_paths, consumers=(), files=None):
    """links maps an explicit owned target to a relative release path.

    files optionally maps explicit owned config targets to their complete new text.
    Existing bytes are retained for rollback. No model configuration is generated.
    consumers lists known shared entry paths, including non-adopting host clients.
    """
    source = Path(source).resolve(strict=True)
    root = Path(os.path.abspath(install_root))
    _safe_parents(root / "releases")
    tree, archive, contents = _archive(source, revision)
    release = root / "releases" / revision
    if source == release or source in release.parents or release in source.parents:
        raise WorkflowError("install_source", "Release root must be separate from the source checkout")
    owned = {str(Path(os.path.abspath(path))) for path in owned_paths}
    operations = []
    all_targets = set(links) | set(files or {})
    if set(links) & set(files or {}):
        raise WorkflowError("install_conflict", "Target has more than one operation")
    for target in sorted(all_targets, key=str):
        path = Path(os.path.abspath(target))
        if str(path) not in owned:
            raise WorkflowError("install_unowned", "Target was not explicitly enrolled as owned")
        if root == path or root in path.parents or source == path or source in path.parents:
            raise WorkflowError("install_unowned", "Managed targets cannot overwrite source/release storage")
        before = _snapshot(path)
        if target in links:
            relative = Path(links[target])
            if relative.is_absolute() or ".." in relative.parts:
                raise WorkflowError("install_path", "Release link must be a relative contained path")
            if not any(name == str(relative) or name.startswith(str(relative) + "/")
                       for name in contents):
                raise WorkflowError("install_path", "Link does not identify a release entry")
            after = {"kind": "symlink", "target": str(release / relative)}
        else:
            after = {"kind": "file", "content": base64.b64encode(
                files[target].encode()).decode(), "mode": before.get("mode", 0o600)}
        operations.append({"path": str(path), "before": before, "after": after})
    inventory = [{"path": str(Path(os.path.abspath(path))), "before": _snapshot(path)}
                 for path in consumers]
    for consumer in inventory:
        resolved = consumer["before"].get("resolved_path")
        if resolved and any(resolved == op["path"] or Path(resolved) in Path(op["path"]).parents
                            for op in operations):
            raise WorkflowError("install_shared", "An operation would change a shared consumer target")
    manifest = {"schema_version": 1, "source": str(source), "revision": revision,
                "tree": tree, "archive_hash": _hash(archive), "contents": contents,
                "release_dir": str(release), "install_root": str(root),
                "runtime_argv": ["env", "PYTHONDONTWRITEBYTECODE=1",
                                 f"UV_PROJECT_ENVIRONMENT={root / 'environments' / revision}",
                                 "uv", "run", "--frozen", "--project", str(release), "devflow"],
                "operations": operations, "consumers": inventory,
                "owned_paths": sorted(owned), "status": "planned"}
    manifest["plan_id"] = _json_hash(manifest)
    return manifest


def _validate_plan(manifest, *, approved_paths, approved_root, approved_plan_id):
    if manifest.get("schema_version") != 1:
        raise WorkflowError("install_manifest", "Unsupported installation manifest")
    original = {key: value for key, value in manifest.items()
                if key not in {"plan_id", "status", "applied_states", "journal"}}
    original["status"] = "planned"
    if (_json_hash(original) != manifest.get("plan_id")
            or manifest.get("plan_id") != approved_plan_id):
        raise WorkflowError("install_manifest", "Installation plan content changed")
    root = Path(os.path.abspath(approved_root))
    allowed = {str(Path(os.path.abspath(path))) for path in approved_paths}
    if Path(manifest["install_root"]) != root:
        raise WorkflowError("install_unowned", "Manifest root is outside the approved install root")
    revision = manifest.get("revision", "")
    if (not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", revision)
            or Path(manifest["release_dir"]) != root / "releases" / revision):
        raise WorkflowError("install_manifest", "Release path is not derived from its immutable pin")
    targets = set()
    for operation in manifest["operations"]:
        path = Path(operation["path"])
        if (str(path) not in allowed or str(path) != os.path.abspath(path)
                or path in targets or root == path or root in path.parents):
            raise WorkflowError("install_unowned", "Manifest operation is outside independent approval")
        targets.add(path)
        _safe_parents(path)
        after = operation["after"]
        if after.get("kind") not in {"file", "symlink"}:
            raise WorkflowError("install_manifest", "Unsupported managed operation")
        if after["kind"] == "symlink":
            destination = Path(after["target"])
            if (".." in destination.parts
                    or not destination.is_relative_to(Path(manifest["release_dir"]))):
                raise WorkflowError("install_manifest", "Managed link escapes pinned release")
    if any(left in right.parents for left in targets for right in targets if left != right):
        raise WorkflowError("install_unowned", "Managed targets cannot contain other operations")
    _safe_parents(root / "releases")


def _open_directory(path):
    """Open each ancestor without following links, including during creation."""
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in Path(path).parts[1:]:
            try:
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
            except FileNotFoundError:
                os.mkdir(part, mode=0o700, dir_fd=descriptor)
                child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                dir_fd=descriptor)
            try:
                flush_descriptor(descriptor)
            except BaseException:
                os.close(child)
                raise
            os.close(descriptor)
            descriptor = child
        flush_descriptor(descriptor)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _write_entry(path, state):
    path = Path(path)
    _safe_parents(path)
    directory = _open_directory(path.parent)
    temporary = ".devflow-" + uuid.uuid4().hex
    try:
        if state["kind"] == "absent":
            try:
                metadata = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                return
            if stat.S_ISDIR(metadata.st_mode):
                raise WorkflowError("install_collision", "Refusing to remove user directory")
            os.unlink(path.name, dir_fd=directory)
            flush_descriptor(directory)
            return
        if state["kind"] == "symlink":
            os.symlink(state["target"], temporary, dir_fd=directory)
        else:
            descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                                 0o600, dir_fd=directory)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(base64.b64decode(state["content"]))
                stream.flush()
                os.fchmod(stream.fileno(), state["mode"])
                flush_descriptor(stream.fileno())
        os.replace(temporary, path.name, src_dir_fd=directory, dst_dir_fd=directory)
        flush_descriptor(directory)
    finally:
        try:
            os.unlink(temporary, dir_fd=directory)
        except FileNotFoundError:
            pass
        os.close(directory)


def _persist(manifest):
    path = Path(manifest["install_root"]) / "install-manifests" / (manifest["plan_id"] + ".json")
    _safe_parents(path)
    if path.is_symlink():
        raise WorkflowError("install_symlink", "Manifest target is a symlink")
    content = json.dumps(manifest, sort_keys=True, indent=2).encode()
    _write_entry(path, {"kind": "file", "content": base64.b64encode(content).decode(), "mode": 0o600})


def _verify_release(release, contents):
    if release.is_symlink():
        raise WorkflowError("install_symlink", "Release directory is a symlink")
    for relative, expected in contents.items():
        path = release / relative
        _safe_parents(path)
        if path.is_symlink() or not path.is_file() or _hash(path.read_bytes()) != expected["hash"]:
            raise WorkflowError("install_release", "Installed release content does not match the pin")
        if bool(path.stat().st_mode & 0o111) != expected["executable"]:
            raise WorkflowError("install_release", "Installed release executable mode changed")
    actual = {str(path.relative_to(release)) for path in release.rglob("*") if not path.is_dir()}
    if actual - {".devflow-release.json"} != set(contents):
        raise WorkflowError("install_release", "Installed release contains unexpected entries")


def _receipt(manifest, approval):
    """Only the durable journal supplies mutable recovery state."""
    _validate_plan(manifest, **approval)
    path = Path(approval["approved_root"]) / "install-manifests" / (
        approval["approved_plan_id"] + ".json")
    _safe_parents(path)
    if path.is_symlink():
        raise WorkflowError("install_manifest", "Durable receipt cannot be a symlink")
    if not path.exists():
        if manifest["status"] != "planned":
            raise WorkflowError("install_manifest", "Durable applied receipt is unavailable")
        return None
    try:
        saved = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise WorkflowError("install_manifest", "Durable receipt is unreadable") from exc
    _validate_plan(saved, **approval)
    if ("applied_states" in manifest
            and manifest["applied_states"] != saved.get("applied_states")):
        raise WorkflowError("install_manifest", "Manifest differs from durable applied receipt")
    return saved


def _expected_after(operation):
    after = dict(operation["after"])
    if after["kind"] == "symlink":
        target = Path(after["target"]).resolve(strict=True)
        after.update(resolved_path=str(target), resolved_hash=_content_hash(target))
    return after


def _positions(manifest):
    positions = []
    for operation, after in zip(manifest["operations"], manifest["applied_states"], strict=True):
        actual = _snapshot(operation["path"])
        if actual == after and actual == operation["before"]:
            positions.append("both")
        elif actual == after:
            positions.append("after")
        elif actual == operation["before"]:
            positions.append("before")
        else:
            raise WorkflowError("install_conflict",
                                "Managed target has an unexpected state; recovery preserves user work")
    return positions


def _check_consumers(manifest):
    targets = {operation["path"] for operation in manifest["operations"]}
    for entry in manifest["consumers"]:
        if entry["path"] not in targets and _snapshot(entry["path"]) != entry["before"]:
            raise WorkflowError("install_conflict", "Shared consumer changed after planning")


def _transition(manifest, direction):
    """Persist intent before each swap; observed bytes settle uncertain completion."""
    positions = _positions(manifest)  # Preflight every path before the first mutation.
    _check_consumers(manifest)
    desired = "after" if direction == "apply" else "before"
    order = list(range(len(manifest["operations"])))
    if direction == "rollback":
        order.reverse()
    current = dict(manifest, status="applying" if direction == "apply" else "rolling_back")
    current["journal"] = {"direction": direction, "positions": positions, "pending_index": None}
    _persist(current)
    for index in order:
        positions = _positions(current)
        if positions[index] in {desired, "both"}:
            os.close(_open_directory(Path(current["operations"][index]["path"]).parent))
            continue
        current["journal"] = {"direction": direction, "positions": positions,
                              "pending_index": index}
        _persist(current)
        operation = current["operations"][index]
        # Re-read immediately before the operation, never trust journal position alone.
        _positions(current)
        _write_entry(operation["path"], operation["after" if direction == "apply" else "before"])
        positions = _positions(current)
        current["journal"] = {"direction": direction, "positions": positions, "pending_index": None}
        _persist(current)
    result = dict(current, status="applied" if direction == "apply" else "rolled_back")
    _persist(result)
    return result


def _install_release(manifest):
    tree, archive, contents = _archive(manifest["source"], manifest["revision"])
    if (tree != manifest["tree"] or _hash(archive) != manifest["archive_hash"]
            or contents != manifest["contents"]):
        raise WorkflowError("install_source", "Pinned source changed after planning")
    release = Path(manifest["release_dir"])
    _safe_parents(release)
    if release.exists() or release.is_symlink():
        _verify_release(release, contents)
        metadata = installed_release(release)
        if metadata["tree"] != manifest["tree"]:
            raise WorkflowError("install_release", "Installed tree differs from approved source")
        flush_directory(release.parent)
        return
    os.close(_open_directory(release.parent))
    temporary = Path(tempfile.mkdtemp(prefix=".release-", dir=release.parent))
    try:
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(temporary, filter="data")
        _verify_release(temporary, contents)
        metadata = {"schema_version": 1, "revision": manifest["revision"], "tree": tree,
                    "contents_hash": _json_hash(contents)}
        (temporary / ".devflow-release.json").write_text(json.dumps(metadata, sort_keys=True))
        for path in temporary.rglob("*"):
            if path.is_file():
                executable = contents.get(str(path.relative_to(temporary)), {}).get("executable")
                path.chmod(0o555 if executable else 0o444)
        for path in temporary.rglob("*"):
            if path.is_file():
                with path.open("rb") as stream:
                    flush_descriptor(stream.fileno())
        for path in sorted((p for p in temporary.rglob("*") if p.is_dir()),
                           key=lambda p: len(p.parts), reverse=True):
            flush_directory(path)
        flush_directory(temporary)
        os.rename(temporary, release)
        flush_directory(release.parent)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def installed_release(package_root):
    """Verify the installer marker and every packaged file before returning its SHA."""
    root = Path(package_root)
    _safe_parents(root / ".devflow-release.json")
    marker = root / ".devflow-release.json"
    if root.is_symlink() or marker.is_symlink() or not marker.is_file():
        raise WorkflowError("install_release", "Installed release marker is unavailable")
    try:
        metadata = json.loads(marker.read_text())
    except (ValueError, OSError) as exc:
        raise WorkflowError("install_release", "Installed release marker is unreadable") from exc
    if (metadata.get("schema_version") != 1 or metadata.get("revision") != root.name
            or root.parent.name != "releases"
            or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", metadata.get("revision", ""))):
        raise WorkflowError("install_release", "Installed release location or revision is invalid")
    contents = {}
    for path in root.rglob("*"):
        if path == marker:
            continue
        if path.is_symlink():
            raise WorkflowError("install_release", "Installed package contains a symlink")
        if path.is_file():
            contents[str(path.relative_to(root))] = {
                "hash": _hash(path.read_bytes()), "executable": bool(path.stat().st_mode & 0o111)}
    if _json_hash(contents) != metadata.get("contents_hash"):
        raise WorkflowError("install_release", "Installed release content does not match its marker")
    return metadata


def apply_install(manifest, *, approved_paths, approved_root, approved_plan_id):
    approval = dict(approved_paths=approved_paths, approved_root=approved_root,
                    approved_plan_id=approved_plan_id)
    saved = _receipt(manifest, approval)
    if saved is None:
        for entry in [*manifest["operations"], *manifest["consumers"]]:
            if _snapshot(entry["path"]) != entry["before"]:
                raise WorkflowError("install_conflict", "Target or consumer changed after planning")
        _install_release(manifest)
        saved = dict(manifest, applied_states=[_expected_after(operation)
                                              for operation in manifest["operations"]])
        # Original rollback bytes and deterministic expected states precede the first swap.
        _persist(saved)
    else:
        _verify_release(Path(saved["release_dir"]), saved["contents"])
        installed_release(saved["release_dir"])
    if saved["status"] == "applied":
        if any(position not in {"after", "both"} for position in _positions(saved)):
            raise WorkflowError("install_conflict", "Applied target changed")
        for operation in saved["operations"]:
            os.close(_open_directory(Path(operation["path"]).parent))
        _persist(saved)
        return saved
    try:
        return _transition(saved, "apply")
    except BaseException:
        # Recover ordinary exceptions immediately. A killed process leaves the durable
        # applying journal for a later explicit apply/rollback call to reconcile.
        try:
            _transition(saved, "rollback")
        except BaseException:
            pass  # Original error plus durable journal remain; third states are preserved.
        raise


def rollback_install(manifest, *, approved_paths, approved_root, approved_plan_id):
    approval = dict(approved_paths=approved_paths, approved_root=approved_root,
                    approved_plan_id=approved_plan_id)
    saved = _receipt(manifest, approval)
    if saved is None:
        raise WorkflowError("install_manifest", "Durable apply journal is unavailable")
    if saved["status"] == "rolled_back":
        if any(position not in {"before", "both"} for position in _positions(saved)):
            raise WorkflowError("install_conflict", "Rolled-back target changed")
        for operation in saved["operations"]:
            os.close(_open_directory(Path(operation["path"]).parent))
        _persist(saved)
        return saved
    return _transition(saved, "rollback")


def resolve_workflow(*, active_version=None, repository_lock=None, legacy_version):
    """A missing enrollment selects frozen legacy; an invalid lock is an error."""
    if active_version:
        if not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}", active_version):
            raise WorkflowError("invalid_enrollment", "Active attempt version must be a full pin")
        return {"version": active_version, "source": "active_attempt"}
    if repository_lock is not None:
        if (repository_lock.get("schema_version") != 1
                or not re.fullmatch(r"[a-f0-9]{40}|[a-f0-9]{64}",
                                    repository_lock.get("revision", ""))):
            raise WorkflowError("invalid_enrollment", "Repository workflow lock is missing a full pin")
        return {"version": repository_lock["revision"], "source": "repository"}
    if not legacy_version:
        raise WorkflowError("legacy_unavailable", "Frozen legacy route is required")
    return {"version": legacy_version, "source": "frozen_legacy"}
