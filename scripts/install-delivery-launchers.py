#!/usr/bin/env python3.12
"""Explicitly install canonical delivery launchers; never install another runtime."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parent.parent
NAMES = ("devflow", "devflow-delivery", "devflow-delivery-mcp")
REVISION = "3c1363bd9ff54c4a2a8da1c52fa2662fa34ca4a0"
LEGACY = {
    "scripts/devflow": "7a04a52394d1200ad409e8189eebc84bddc98d97c4bfa021a8c25cdf33b76ac6",
    ".devflow-release.json": "755cae06b58b80812d1471de88ceccd1da3b98fafd26336a9a5255facc1bbb91",
    "pyproject.toml": "680ddf3c272370678dffd89663b299536b61d92461ed5086c84907afe6181c05",
}
FIELDS = {"schema_version", "command_id", "bin_dir", "runtime_dir", "source_revision",
          "source_tree", "config_path", "config_sha256", "entry_sha256", "expected_before"}


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def path(value):
    if not isinstance(value, str) or not value or "\n" in value or "\0" in value:
        raise ValueError("invalid launcher path")
    result = Path(value)
    if not result.is_absolute() or ".." in result.parts or result.resolve() != result:
        raise ValueError("launcher path is not canonical or has an alias")
    for parent in reversed(result.parents):
        if parent.exists() and not stat.S_ISDIR(parent.lstat().st_mode):
            raise ValueError("launcher ancestor is not a directory")
    return result


def read(file, *, mode=None, limit=1024 * 1024):
    path(str(file))
    fd = os.open(file, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                or before.st_nlink != 1 or before.st_size > limit
                or (mode is not None and stat.S_IMODE(before.st_mode) != mode)):
            raise ValueError("launcher evidence is not an owned bounded regular file")
        with os.fdopen(os.dup(fd), "rb") as stream:
            raw = stream.read(limit + 1)
        after = os.fstat(fd)
        if (len(raw) != before.st_size or any(getattr(before, key) != getattr(after, key)
                for key in ("st_dev", "st_ino", "st_mode", "st_uid", "st_nlink",
                            "st_size", "st_mtime_ns", "st_ctime_ns"))):
            raise ValueError("launcher evidence changed while reading")
        return raw
    finally:
        os.close(fd)


def directory(folder, *, private=False, create=False):
    path(str(folder))
    if create and not folder.exists():
        if not folder.parent.exists():
            directory(folder.parent, create=True)
        folder.mkdir(mode=0o700)
    info = folder.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) & 0o022
            or (private and stat.S_IMODE(info.st_mode) != 0o700)):
        raise ValueError("launcher directory is not owned and protected")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def immutable(file, raw, mode=0o600):
    directory(file.parent, create=True)
    if file.exists() or file.is_symlink():
        if read(file, mode=mode) != raw:
            raise ValueError("immutable launcher evidence changed")
        return
    stage = file.with_name(".stage-" + file.name + "-" + sha(raw))
    if stage.exists() or stage.is_symlink():
        if not raw.startswith(read(stage, mode=mode)):
            raise ValueError("unattributable launcher staging bytes")
        stage.unlink()
    fd = os.open(stage, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    os.link(stage, file)
    stage.unlink()
    fd = os.open(file.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def link_state(file):
    path(str(file.parent))
    if not os.path.lexists(file):
        return {"type": "absent"}
    info = file.lstat()
    if not stat.S_ISLNK(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("foreign launcher destination preserved")
    return {"type": "symlink", "target": os.readlink(file)}


def legacy(request):
    before = request["expected_before"]["devflow"]
    if before == {"type": "absent"}:
        return {}
    release = Path(request["bin_dir"]).parent / "share/devflow/releases" / REVISION
    expected = {"type": "symlink", "target": str(release / "scripts/devflow")}
    if before != expected:
        raise ValueError("only the recognized immutable legacy devflow launcher may migrate")
    directory(release)
    saved = {}
    for name, expected_hash in LEGACY.items():
        raw = read(release / name)
        if sha(raw) != expected_hash:
            raise ValueError("recognized legacy release bytes changed")
        saved[name] = raw
    marker = json.loads(saved[".devflow-release.json"])
    if (marker["revision"] != REVISION or marker["schema_version"] != 1
            or marker["tree"] != "39c64386a78904e6cbd33ac832cc7a92309d415c"
            or marker["contents_hash"] != "43e27a2bd72a2331327c5e95dd46fb5ea2f96da5905da0085eb8dfa012b2a69b"):
        raise ValueError("legacy release marker identity changed")
    if stat.S_IMODE((release / "scripts/devflow").lstat().st_mode) not in (0o555, 0o755):
        raise ValueError("legacy entry is not the recognized executable")
    return saved


def runtime(request):
    folder = path(request["runtime_dir"])
    if folder != ROOT / "runtime":
        raise ValueError("use the installer in the canonical installed runtime checkout")
    directory(folder)
    def git(*args):
        return subprocess.check_output(["git", "-C", str(ROOT), *args], text=True,
                                       env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"}).strip()
    if (git("rev-parse", "HEAD") != request["source_revision"]
            or git("rev-parse", "HEAD^{tree}") != request["source_tree"]
            or git("status", "--porcelain", "--untracked-files=normal")):
        raise ValueError("canonical installed source revision/tree or clean state changed")
    project = tomllib.loads(read(folder / "pyproject.toml").decode())
    expected = {"devflow-delivery": "devflow_temporal.delivery_control:main",
                "devflow-delivery-mcp": "devflow_temporal.delivery_mcp:main"}
    if any(project["project"]["scripts"].get(name) != target for name, target in expected.items()):
        raise ValueError("runtime does not expose the canonical delivery contract")
    for name in expected:
        entry = folder / ".venv/bin" / name
        if sha(read(entry, mode=0o755)) != request["entry_sha256"][name]:
            raise ValueError("canonical execution entry changed")
    if sha(read(path(request["config_path"]), mode=0o600)) != request["config_sha256"]:
        raise ValueError("canonical service configuration changed")


def validate(request):
    if (not isinstance(request, dict) or set(request) != FIELDS
            or type(request["schema_version"]) is not int or request["schema_version"] != 1
            or not isinstance(request["command_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}", request["command_id"])
            or set(request["entry_sha256"]) != set(NAMES[1:])
            or set(request["expected_before"]) != set(NAMES)):
        raise ValueError("invalid scoped launcher request")
    for value, length in [(request["source_revision"], 40), (request["source_tree"], 40),
                          (request["config_sha256"], 64),
                          *((h, 64) for h in request["entry_sha256"].values())]:
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{" + str(length) + "}", value):
            raise ValueError("invalid launcher identity digest")
    for name in NAMES[1:]:
        if request["expected_before"][name] != {"type": "absent"}:
            raise ValueError("canonical commands must be absent before their explicit installation")
    bin_dir = path(request["bin_dir"])
    identity = directory(bin_dir)
    journal = bin_dir.parent / "share/devflow/delivery-launchers" / request["command_id"]
    path(str(journal))
    for parent in (journal, *journal.parents):
        if parent == bin_dir.parent:
            break
        if parent.exists():
            directory(parent, private=parent in (journal, journal.parent))
    runtime(request)
    saved = legacy(request)
    return bin_dir, journal, identity, saved


def wrapper(request, name):
    entry = "devflow-delivery" if name == "devflow" else name
    return ("#!/bin/sh\n# Canonical delivery entry; devflow aliases devflow-delivery.\nexec "
            + shlex.quote(str(Path(request["runtime_dir"]) / ".venv/bin" / entry))
            + " --config " + shlex.quote(request["config_path"]) + ' "$@"\n').encode()


def guarded(request, journal, *, rollback=False):
    bin_dir, actual, identity, saved = validate(request)
    if actual != journal:
        raise ValueError("launcher journal left its owned operation namespace")
    manifest = journal / "manifest.json"
    intent = {"request": request, "bin_identity": identity}
    if manifest.exists() and json.loads(read(manifest, mode=0o600)) != intent:
        raise ValueError("launcher manifest or directory identity changed")
    states = {}
    for name in NAMES:
        current = link_state(bin_dir / name)
        after = {"type": "symlink", "target": str(journal / "entries" / name)}
        if current not in (request["expected_before"][name], after):
            raise ValueError("launcher destination drifted; preserving all paths")
        if current == after and read(journal / "entries" / name, mode=0o755) != wrapper(request, name):
            raise ValueError("installed launcher bytes changed")
        if not manifest.exists() and current != request["expected_before"][name]:
            raise ValueError("unrecorded partial launcher installation")
        states[name] = current
    if rollback and not manifest.exists():
        raise ValueError("rollback requires its immutable original manifest")
    for marker, state in (("applied.json", "applied"), ("rollback-intent.json", "rollback"),
                          ("rolled-back.json", "rolled_back")):
        receipt = journal / marker
        if receipt.exists() or receipt.is_symlink():
            if read(receipt, mode=0o600) != encoded({"state": state, "manifest_sha256": sha(encoded(intent))}):
                raise ValueError("launcher progress receipt changed")
    if (journal / "rolled-back.json").exists():
        if states != request["expected_before"]:
            raise ValueError("rolled-back launcher state drifted")
    elif (journal / "applied.json").exists() and not (journal / "rollback-intent.json").exists():
        if any(states[n] != {"type": "symlink", "target": str(journal / "entries" / n)} for n in NAMES):
            raise ValueError("applied launcher state drifted")
    return intent, saved, states


@contextmanager
def locked(journal):
    directory(journal.parent, private=True, create=True)
    lock = journal.parent / "controller.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise ValueError("launcher lock is not private and owned")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def replace_link(file, expected, wanted, command_id):
    if link_state(file) != expected:
        raise ValueError("launcher changed before its guarded replacement")
    if wanted == {"type": "absent"}:
        file.unlink()
    else:
        stage = file.with_name(".devflow-" + command_id + "-" + file.name)
        if os.path.lexists(stage):
            if link_state(stage) != wanted:
                raise ValueError("foreign launcher staging path preserved")
            stage.unlink()
        stage.symlink_to(wanted["target"])
        if link_state(file) != expected:
            stage.unlink()
            raise ValueError("launcher changed before publication")
        os.replace(stage, file)
    fd = os.open(file.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def apply(request, *, preflight=False):
    _, journal, _, _ = validate(request)
    intent, saved, states = guarded(request, journal)
    if preflight:
        return {"state": "ready", "manifest": str(journal / "manifest.json"), "before": states}
    with locked(journal):
        intent, saved, states = guarded(request, journal)
        if (journal / "rollback-intent.json").exists():
            raise ValueError("this launcher operation is rolling back or rolled back")
        existing = (journal / "applied.json").exists()
        immutable(journal / "manifest.json", encoded(intent))
        for name, raw in saved.items():
            immutable(journal / "legacy" / name.replace("/", "-"), raw)
        for name in NAMES:
            immutable(journal / "entries" / name, wrapper(request, name), 0o755)
        for name in NAMES:
            _, _, states = guarded(request, journal)
            after = {"type": "symlink", "target": str(journal / "entries" / name)}
            if states[name] != after:
                replace_link(Path(request["bin_dir"]) / name, states[name], after, request["command_id"])
        _, _, states = guarded(request, journal)
        immutable(journal / "applied.json", encoded({"state": "applied", "manifest_sha256": sha(encoded(intent))}))
        return {"state": "applied", "existing": existing, "manifest": str(journal / "manifest.json"),
                "manifest_sha256": sha(encoded(intent)), "commands": states,
                "devflow_contract": "alias of canonical devflow-delivery"}


def inspect(manifest, expected_hash, *, rollback=False):
    raw = read(manifest, mode=0o600)
    if sha(raw) != expected_hash:
        raise ValueError("launcher rollback manifest hash changed")
    intent = json.loads(raw)
    request, journal = intent["request"], manifest.parent
    if manifest.name != "manifest.json":
        raise ValueError("not a launcher manifest")
    if not rollback:
        _, _, states = guarded(request, journal, rollback=True)
        state = "rolled_back" if (journal / "rolled-back.json").exists() else "rolling_back" if (journal / "rollback-intent.json").exists() else "applied" if (journal / "applied.json").exists() else "pending"
        target = request["expected_before"] if state == "rolled_back" else {
            name: {"type": "symlink", "target": str(journal / "entries" / name)} for name in NAMES}
        if state not in ("pending", "rolling_back") and states != target:
            raise ValueError("terminal launcher readback drifted")
        return {"state": state, "commands": states, "source_revision": request["source_revision"],
                "devflow_contract": "alias of canonical devflow-delivery"}
    guarded(request, journal, rollback=True)
    with locked(journal):
        guarded(request, journal, rollback=True)
        immutable(journal / "rollback-intent.json", encoded({"state": "rollback", "manifest_sha256": expected_hash}))
        for name in NAMES:
            _, _, states = guarded(request, journal, rollback=True)
            before = request["expected_before"][name]
            if states[name] != before:
                replace_link(Path(request["bin_dir"]) / name, states[name], before, request["command_id"])
        _, _, states = guarded(request, journal, rollback=True)
        immutable(journal / "rolled-back.json", encoded({"state": "rolled_back", "manifest_sha256": expected_hash}))
        return {"state": "rolled_back", "commands": states, "manifest": str(manifest)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("preflight", "apply", "status", "rollback"))
    parser.add_argument("--request", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--sha256", required=True)
    args = parser.parse_args()
    if args.operation in ("preflight", "apply"):
        if args.request is None or args.manifest is not None:
            parser.error("preflight/apply require only --request and --sha256")
        raw = read(args.request, mode=0o600)
        if sha(raw) != args.sha256:
            raise ValueError("launcher request hash changed")
        result = apply(json.loads(raw), preflight=args.operation == "preflight")
    else:
        if args.manifest is None or args.request is not None:
            parser.error("status/rollback require only --manifest and --sha256")
        result = inspect(args.manifest, args.sha256, rollback=args.operation == "rollback")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        raise SystemExit(str(exc)) from exc
