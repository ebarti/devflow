"""Per-run ownership and resumable finalization of explicitly allocated roots."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .contracts import digest


def private_directory(path: Path) -> None:
    if not path.parent.exists():
        private_directory(path.parent)
    _ancestors(path)
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("resource parent is not an owned directory")


def read_private(path: Path) -> dict:
    info = path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o777 != 0o600
        or info.st_nlink != 1
    ):
        raise ValueError("resource evidence is not a private owned file")
    return json.loads(path.read_bytes())


def write_private(path: Path, value: dict) -> None:
    private_directory(path.parent)
    if path.exists() or path.is_symlink():
        read_private(path)
    temporary = path.with_name(path.name + f".{os.getpid()}.new")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, "w") as stream:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary.exists():
            temporary.unlink()


def _identity(path: Path) -> dict[str, int]:
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError("registered resource root was replaced")
    return {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid}


def _ancestors(path: Path, *, allow_missing: bool = False) -> None:
    for parent in reversed(path.parents):
        if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
            raise ValueError("resource ancestor was replaced")
        if not allow_missing and not parent.exists():
            raise ValueError("resource ancestor is absent")


def _remove_contents(fd: int) -> None:
    """Never follow an entry, including a symlink swapped during traversal."""
    for name in os.listdir(fd):
        info = os.stat(name, dir_fd=fd, follow_symlinks=False)
        if stat.S_ISDIR(info.st_mode):
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            try:
                if os.fstat(child).st_ino != info.st_ino:
                    raise ValueError("temporary directory changed during finalization")
                _remove_contents(child)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=fd)
        else:
            os.unlink(name, dir_fd=fd)


def remove_directory(path: Path, identity: dict) -> None:
    _ancestors(path)
    parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        fd = os.open(path.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
        try:
            info = os.fstat(fd)
            if {"device": info.st_dev, "inode": info.st_ino, "uid": info.st_uid} != identity:
                raise ValueError("temporary root identity changed before deletion")
            _remove_contents(fd)
            if _identity(path) != identity:
                raise ValueError("temporary root changed during deletion")
        finally:
            os.close(fd)
        os.rmdir(path.name, dir_fd=parent)
    finally:
        os.close(parent)
    if os.path.lexists(path):
        raise ValueError("temporary root is still present after removal")


class RunResources:
    def __init__(self, spec: dict) -> None:
        self.spec = spec
        self.state = Path(spec["state_dir"])
        self.root = self.state / "resources"
        self.manifest = self.root / "manifest.json"
        if not self.state.is_absolute() or self.state.name != spec["run_id"]:
            raise ValueError("resource registry is outside its run")
        private_directory(self.root)
        _ancestors(self.state)

    @contextmanager
    def locked(self):
        descriptor = os.open(
            self.root / "ownership.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600
        )
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            value = (
                read_private(self.manifest)
                if self.manifest.exists()
                else {
                    "schema": "devflow-run-resources-v1",
                    "run_id": self.spec["run_id"],
                    "state_identity": _identity(self.state),
                    "roots": {},
                    "processes": [],
                }
            )
            if value.get("run_id") != self.spec["run_id"] or value["state_identity"] != _identity(
                self.state
            ):
                raise ValueError("run resource ownership changed")
            yield value
        finally:
            os.close(descriptor)

    def _allowed(self, path: Path, kind: str, *, finalizing: bool = False) -> None:
        short = Path("/private/tmp") / ("dfqa-" + digest(str(self.state))[:20])
        valid = path == self.state / "transient" or path == short
        if kind == "checkout":
            valid = path == Path(self.spec["checkout"])
        elif kind == "gate":
            relative = path.relative_to(self.state / "gates")
            valid = (
                len(relative.parts) == 2
                and relative.parts[0].isdecimal()
                and relative.parts[1] in {"review", "verify"}
            )
        elif kind == "generated":
            allowed_names = {
                "node_modules",
                *(self.spec["policy"].get("browser_qa") or {}).get("artifact_paths", []),
            }
            ownership = read_private(self.manifest)["roots"] if self.manifest.exists() else {}
            valid = False
            # Broker admission bounds each gate against durable operator grants
            # before registering it. Finalization uses that recorded ownership,
            # including a removed parent, rather than the original repair budget.
            for raw_root, entry in ownership.items():
                root = Path(raw_root)
                if entry["kind"] not in {"checkout", "gate"}:
                    continue
                if not any(path == root / name for name in allowed_names):
                    continue
                self._allowed(root, entry["kind"], finalizing=finalizing)
                if os.path.lexists(root) and _identity(root) != entry["identity"]:
                    raise ValueError("generated resource parent was replaced")
                valid = True
        if not valid or not path.is_absolute() or ".." in path.parts:
            raise ValueError("resource root is outside its registered run boundary")
        _ancestors(path, allow_missing=finalizing)

    def register(self, path: Path, kind: str) -> None:
        self._allowed(path, kind)
        with self.locked() as manifest:
            old = manifest["roots"].get(str(path))
            if old is not None:
                if old["kind"] != kind:
                    raise ValueError("resource purpose changed")
                if (
                    old.get("identity")
                    and os.path.lexists(path)
                    and _identity(path) != old["identity"]
                ):
                    raise ValueError("registered resource root was replaced")
                if (
                    kind != "checkout"
                    and not os.path.lexists(path)
                    and old.get("receipt", {}).get("state") in {"removed", "already_absent"}
                ):
                    old.update(
                        identity=None, state="allocated", generation=old.get("generation", 0) + 1
                    )
                    old.pop("receipt", None)
                    write_private(self.manifest, manifest)
                return
            if os.path.lexists(path):
                raise ValueError("cannot adopt an existing unregistered resource")
            manifest["roots"][str(path)] = {"kind": kind, "state": "allocated", "identity": None}
            write_private(self.manifest, manifest)

    def created(self, path: Path) -> None:
        with self.locked() as manifest:
            entry = manifest["roots"][str(path)]
            identity = _identity(path)
            if entry.get("identity") not in (None, identity):
                raise ValueError("resource identity changed after allocation")
            entry.update(identity=identity, state="created")
            entry.pop("receipt", None)
            write_private(self.manifest, manifest)

    def scratch(self, kind: str, key: str) -> Path:
        if not key or Path(key).is_absolute() or ".." in Path(key).parts:
            raise ValueError("scratch key escaped its run")
        path = self.state / "transient"
        self.register(path, "transient")
        with self.locked() as manifest:
            entry = manifest["roots"][str(path)]
            if entry["identity"] is None:
                path.mkdir(mode=0o700)
                entry.update(identity=_identity(path), state="created")
                write_private(self.manifest, manifest)
            elif _identity(path) != entry["identity"]:
                raise ValueError("transient root changed")
        result = path / kind / key
        _ancestors(path)
        private_directory(result)
        return result

    def browser_scratch(self) -> Path:
        path = Path("/private/tmp") / ("dfqa-" + digest(str(self.state))[:20])
        self.register(path, "browser-scratch")
        if not path.exists():
            path.mkdir(mode=0o700)
            self.created(path)
        return path

    def process(self, journal: Path) -> None:
        if not journal.is_relative_to(self.state) or journal.is_symlink():
            raise ValueError("process journal left its run")
        with self.locked() as manifest:
            if str(journal) not in manifest["processes"]:
                manifest["processes"].append(str(journal))
                write_private(self.manifest, manifest)

    def finalize(self, outcome: str, *, uncertain: bool = False) -> dict[str, Any]:
        from .delivery_native_process import reconcile_process

        with self.locked() as manifest:
            process_receipts = [reconcile_process(Path(path)) for path in manifest["processes"]]
            uncertain = uncertain or any(
                item["cleanup"] != "observed-native-confirmed" for item in process_receipts
            )
            manifest["finalization"] = {"outcome": outcome, "state": "running"}
            write_private(self.manifest, manifest)
            receipts = []
            for raw_path, entry in sorted(
                manifest["roots"].items(), key=lambda item: -len(Path(item[0]).parts)
            ):
                path = Path(raw_path)
                receipt = {"path": raw_path, "kind": entry["kind"]}
                try:
                    self._allowed(path, entry["kind"], finalizing=True)
                    if not os.path.lexists(path):
                        receipt.update(state="already_absent")
                        if entry.get("clean_base_removal"):
                            receipt["clean_base_removal"] = entry["clean_base_removal"]
                    elif uncertain:
                        receipt.update(
                            state="retained",
                            reason="native process monitoring or teardown is unknown",
                        )
                    elif entry["identity"] is None or _identity(path) != entry["identity"]:
                        raise ValueError("resource creation or replacement has no proven ownership")
                    elif entry["kind"] in {"checkout", "gate"}:
                        reason = self._retain_source(path, entry["kind"], outcome)
                        if reason:
                            receipt.update(state="retained", reason=reason)
                        else:
                            if entry["kind"] == "checkout" and outcome == "cancelled":
                                from .candidate import candidate_for

                                candidate = candidate_for(path)
                                if candidate["head"] != self.spec["base_sha"]:
                                    raise ValueError("cancelled removable source is not the base")
                                # Persist before removal, so a lost completion can
                                # authenticate the absent clean source on retry.
                                proof = {"candidate": candidate, "base_sha": self.spec["base_sha"],
                                         "outcome": outcome}
                                entry["clean_base_removal"] = proof
                                receipt["clean_base_removal"] = proof
                            entry["state"] = "removing"
                            write_private(self.manifest, manifest)
                            subprocess.run(
                                [
                                    "git",
                                    "-C",
                                    self.spec["source_path"],
                                    "worktree",
                                    "remove",
                                    str(path),
                                ],
                                check=True,
                                capture_output=True,
                                timeout=30,
                            )
                            if os.path.lexists(path):
                                raise ValueError("Git worktree remains after removal")
                            receipt.update(state="removed")
                    else:
                        entry["state"] = "removing"
                        write_private(self.manifest, manifest)
                        remove_directory(path, entry["identity"])
                        receipt.update(state="removed")
                except (OSError, ValueError, subprocess.SubprocessError) as exc:
                    receipt.update(
                        state="failed_unknown",
                        reason=f"{type(exc).__name__}: {str(exc)[:300]}",
                        retryable=isinstance(exc, (OSError, subprocess.TimeoutExpired)),
                    )
                entry["receipt"] = receipt
                write_private(self.manifest, manifest)
                receipts.append(receipt)
            failed = uncertain or any(item["state"] == "failed_unknown" for item in receipts)
            result = {
                "state": "unknown" if failed else "confirmed",
                "outcome": outcome,
                "process_cleanup": "unknown" if uncertain else "observed-native-confirmed",
                "resource_cleanup": "unknown" if failed else "confirmed",
                "roots": receipts,
                "processes": process_receipts,
                "sessions_and_durable_evidence": "retained",
                "retryable": any(item.get("retryable") for item in receipts),
            }
            manifest["finalization"] = result
            write_private(self.manifest, manifest)
            receipt_path = self.root / "finalization.json"
            write_private(receipt_path, result)
            return {
                **result,
                "receipt": str(receipt_path),
                "receipt_sha256": hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
            }

    def _retain_source(self, path: Path, kind: str, outcome: str) -> str | None:
        def git(*args):
            return subprocess.check_output(
                ["git", "--no-optional-locks", "-C", str(path), *args], text=True, timeout=30
            ).strip()

        if git("rev-parse", "--show-toplevel") != str(path):
            raise ValueError("registered worktree root changed")
        if git("status", "--porcelain", "--untracked-files=all"):
            return "dirty or untracked candidate source is preserved"
        if git("ls-files", "--others", "--ignored", "--exclude-standard"):
            return "ignored worktree data requires explicit preservation"
        if Path("/usr/sbin/lsof").is_file():
            observed = subprocess.run(
                ["/usr/sbin/lsof", "-nP", "-F", "p", "+D", str(path)],
                capture_output=True,
                text=True,
                timeout=30,
            )
            if observed.returncode not in (0, 1):
                raise ValueError("candidate process/open-file inspection is unavailable")
            # macOS lsof can return 1 while reporting valid matches. Inspect
            # its machine-readable identities, not just its exit status.
            if any(
                line.startswith("p") and line[1:].isdecimal()
                for line in observed.stdout.splitlines()
            ):
                return "a live process still uses the candidate worktree"
        if kind == "gate":
            return None
        if outcome == "blocked":
            return "blocked-run candidate and recovery source are preserved"
        head = git("rev-parse", "HEAD")
        if head == self.spec["base_sha"]:
            return None
        remote = subprocess.check_output(
            [
                "git",
                "-C",
                self.spec["source_path"],
                "ls-remote",
                "origin",
                "refs/heads/" + self.spec["branch"],
            ],
            text=True,
            timeout=30,
        )
        if not remote.strip() or remote.split()[0] != head:
            return "unpushed or unconfirmed candidate source is preserved"
        if outcome != "delivered":
            return "published source remains available for recovery"
        return None
