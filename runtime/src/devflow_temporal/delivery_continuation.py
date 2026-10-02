"""Immutable inputs for a guarded continuation of a finished implementation turn."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import stat
import tempfile
from pathlib import Path

from .contracts import digest


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def selected_manifest(root: Path, paths: list[str]) -> dict[str, dict[str, object]]:
    """Describe only explicitly recovered files, rejecting linked ancestors/files."""

    root_info = root.lstat()
    if not stat.S_ISDIR(root_info.st_mode) or root_info.st_uid != os.getuid():
        raise ValueError("continuation source is not an owned directory")
    root = root.resolve(strict=True)
    if len(paths) != len(set(paths)):
        raise ValueError("continuation recovery paths are duplicated")
    manifest: dict[str, dict[str, object]] = {}
    for raw in sorted(paths):
        relative = Path(raw)
        if relative.is_absolute() or not relative.parts or any(
            part in {".", ".."} for part in relative.parts
        ) or raw != relative.as_posix():
            raise ValueError("continuation recovery path is not a relative file")
        current = root
        for part in relative.parts[:-1]:
            current /= part
            info = current.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
                raise ValueError("continuation recovery parent is not an owned directory")
        file = root / relative
        try:
            info = file.lstat()
        except FileNotFoundError:
            manifest[raw] = {"type": "absent"}
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
            raise ValueError("continuation recovery file is not an owned regular file")
        manifest[raw] = {
            "type": "file",
            "sha256": _hash(file),
            "size": info.st_size,
            "mode": stat.S_IMODE(info.st_mode),
            "mtime_ns": info.st_mtime_ns,
        }
    return manifest


def selected_digest(root: Path, paths: list[str]) -> str:
    return digest(selected_manifest(root, paths))


SESSION_DB_RE = re.compile(
    r"^(?:state|thread_history|queue|goals|memories|logs)_[0-9]+\.sqlite(?:-(?:shm|wal))?$"
)


def session_state_paths(role_home: Path, session_id: str) -> list[str]:
    """Select only provider session data; auth, profile, caches and tools are regenerated."""

    home_info = role_home.lstat()
    if not stat.S_ISDIR(home_info.st_mode) or home_info.st_uid != os.getuid():
        raise ValueError("continuation role home is not an owned directory")
    codex = role_home / "codex"
    if not codex.is_dir() or codex.is_symlink():
        raise ValueError("continuation Codex home is unavailable")
    paths = [
        "codex/" + file.name
        for file in codex.iterdir()
        if file.name == "installation_id" or SESSION_DB_RE.fullmatch(file.name)
    ]
    sessions = codex / "sessions"
    if sessions.is_dir() and not sessions.is_symlink():
        paths.extend(
            file.relative_to(role_home).as_posix()
            for file in sessions.rglob("*")
            if session_id in file.name and (file.is_file() or file.is_symlink())
        )
    if not any(session_id in Path(path).name for path in paths):
        raise ValueError("continuation session transcript is missing")
    return sorted(paths)


def session_state_digest(role_home: Path, session_id: str) -> str:
    return digest(selected_manifest(role_home, session_state_paths(role_home, session_id)))


def copy_session_state(
    source_home: Path, destination_home: Path, session_id: str, expected_digest: str
) -> None:
    """Atomically carry provider session data without copying auth or permissions."""

    paths = session_state_paths(source_home, session_id)
    if session_state_digest(source_home, session_id) != expected_digest:
        raise ValueError("continuation source session state changed")
    if destination_home.exists() or destination_home.is_symlink():
        if destination_home.is_symlink() or session_state_digest(
            destination_home, session_id
        ) != expected_digest:
            raise ValueError("continuation destination session state changed")
        return
    parent = destination_home.parent
    parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".continuation-", dir=parent))
    try:
        for relative in paths:
            target = staging / relative
            target.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            shutil.copy2(source_home / relative, target, follow_symlinks=False)
        if session_state_digest(staging, session_id) != expected_digest:
            raise ValueError("continuation session copy differs from its source")
        if session_state_digest(source_home, session_id) != expected_digest:
            raise ValueError("continuation source session changed during copy")
        os.replace(staging, destination_home)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def continuation_authority(policy: dict) -> dict:
    """Compare configured native authority before new per-run measurement."""

    authority = {
        key: value
        for key, value in policy.items()
        if key
        not in {
            "recovery",
            "security_binding_sha256",
            "environment_proof_sha256",
            "native_identity",
            "codex_bin_sha256",
        }
    }
    return authority
