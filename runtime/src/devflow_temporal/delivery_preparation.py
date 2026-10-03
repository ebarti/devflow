"""Shared native preparation evidence and immutable per-run authority binding."""
from __future__ import annotations

import fcntl
import hashlib
import os
import stat
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest

PACKAGE = Path(__file__).resolve().parent
RUNTIME = PACKAGE.parents[1]

class PreparationError(RuntimeError):
    """Preparation could not establish its actual execution boundary."""


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _directory(path: Path) -> None:
    if not path.parent.exists():
        _directory(path.parent)
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o777 != 0o700
    ):
        raise PreparationError("preparation directory must be private and owned")


def _private_bytes(path: Path, root: Path) -> bytes:
    _directory(root)
    if not path.is_absolute() or not path.is_relative_to(root):
        raise PreparationError("preparation evidence escaped its owned directory")
    current = root
    for part in path.relative_to(root).parts[:-1]:
        current /= part
        _directory(current)
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink != 1
    ):
        raise PreparationError("preparation evidence must be an owned private regular file")
    return path.read_bytes()


def _write(path: Path, value: Any) -> None:
    _directory(path.parent)
    content = (canonical_json(value) + "\n").encode()
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _lock(root: Path):
    _directory(root)
    descriptor = os.open(root / "preparation.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_nlink != 1
        ):
            raise PreparationError("preparation lock is not private and owned")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _reference(path: Path) -> dict:
    return {"path": str(path), "sha256": _hash(path)}


def run_binding(spec: dict) -> str:
    policy = {
        key: value
        for key, value in spec["policy"].items()
        if key not in {"security_binding_sha256", "environment_proof_sha256"}
    }
    keys = (
        "run_id",
        "work_id",
        "repository_key",
        "source_path",
        "origin_url",
        "github_repo",
        "base_sha",
        "base_ref",
        "branch",
        "state_dir",
        "checkout",
        "authorized_endpoint",
        "config_digest",
    )
    return digest({
        **{key: spec[key] for key in keys}, "policy": policy,
        **({"plan_approval": spec["plan_approval"]} if "plan_approval" in spec else {}),
        **{key: spec[key] for key in ("origin_thread_id", "blocking_questions_version")
           if key in spec},
    })


def execution_retired(spec: dict) -> bool:
    return (
        spec.get("provider") == "codex"
        and spec.get("policy", {}).get("execution_backend") != "native-macos"
    )


def require_native_execution(spec: dict) -> None:
    if execution_retired(spec):
        raise PreparationError("Historical Docker execution is read-only and cannot be resumed")


def verify_prepared_spec(spec: dict) -> None:
    require_native_execution(spec)
    if spec.get("provider") == "fake":
        return
    from .delivery_native_preparation import verify_native_spec
    verify_native_spec(spec)


def prepare_authority(store: Any, spec: dict) -> dict:
    require_native_execution(spec)
    if spec.get("provider") == "fake":
        return spec
    from .delivery_native_preparation import prepare_native_authority
    return prepare_native_authority(store, spec)
