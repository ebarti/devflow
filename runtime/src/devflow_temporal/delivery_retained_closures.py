"""Explicit data-only sealing from a stopped owner's retained Temporal snapshot.

This owner cannot dispatch, project, change an execution transport or install a
runtime. The isolated endpoint is a history source, never execution authority.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import lzma
import os
import re
import shlex
import shutil
import signal
import socket
import sqlite3
import stat
import subprocess
import sys
import time
import uuid
import zlib
from contextlib import ExitStack, closing, contextmanager
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_filesystem_custody import _immutable, _publish_exclusive
from .delivery_resources import write_private
from .delivery_runtime_location import bootstrap, private_bytes, receipt_bytes

SCHEMA = "devflow.retained-closure-capture/v1"
EVENT = "retained_closure_captured"
ANSWER = "Capture only the two selected closed histories from an owned copy; change no delivery."
BASELINE = "824f00f9cb664e6e61285589bf3c258e651d6653"
READER_SHA = "42a038da4346cddd0265691bd17d69b8f3884333e4d329165ab43f793c7e9476"
READER_RECEIPT_SHA = "367453a3dcaa67faacbab846588335179529c10d57966fb6fbc47d40b7697b4e"
READER_FORMULA_SHA = "896766197690da2638d33740ebaa4ca0f05b1b675f4bbf245641800ef770cdce"
READER_HEADER = "Temporal CLI 1.9.1 (Server 1.32.0, UI 2.54.1)"
MAX_SNAPSHOT = 4 * 1024**3
MAX_HISTORY = 48 * 1024**2
MAX_DECODED = 64 * 1024**2
MAX_PAYLOAD = 32 * 1024**2
_SDK_LIMIT_PINNED = False
MAX_EVENTS = 65536
MAX_PAGES = 512
TOTAL_SECONDS = 90
STAT_KEYS = (
    "st_dev",
    "st_ino",
    "st_uid",
    "st_mode",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)
FIELDS = {"command_id", "migration_path", "migration_sha256", "captures", "reader", "answer"}
SELECTION = {
    "run_id",
    "workflow_id",
    "execution_run_id",
    "revision",
    "protocol_revision",
    "request_digest",
    "recovery_digest",
}


def _sha(raw):
    return hashlib.sha256(raw).hexdigest()


def _stamp(info):
    return {key: getattr(info, key) for key in STAT_KEYS}


def _folder(path):
    path = Path(path)
    if not path.exists():
        path.mkdir(mode=0o700)
    return _existing_folder(path)


def _existing_folder(path):
    path = Path(path)
    info = path.lstat()
    if (
        path.resolve(strict=True) != path
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("capture directory is not its private owned generation")
    return path


def _new_folder(path):
    """A writable generation never adopts an existing staging namespace."""
    try:
        path.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise ValueError("capture writable generation already exists") from exc
    return _existing_folder(path)


def _object(info):
    return {key: getattr(info, key) for key in ("st_dev", "st_ino", "st_uid", "st_mode")}


def _master(master, expected):
    """Only the original held source family can appoint pristine copy members."""
    _existing_folder(master)
    if {p.name for p in master.iterdir()} != set(expected) | {"manifest.json"}:
        raise ValueError("capture pristine master has an unregistered member")
    if json.loads(receipt_bytes(master / "manifest.json")) != expected:
        raise ValueError("capture immutable master belongs to another source generation")
    for name, value in expected.items():
        if _file_hash(master / name) != value["sha256"]:
            raise ValueError("capture pristine master bytes changed")


def _publish(path, value):
    _folder(path.parent)
    _immutable(path, canonical_json(value).encode())
    return _sha(receipt_bytes(path))


def _file_hash(path):
    path = Path(path)
    if path.resolve(strict=True) != path:
        raise ValueError("capture protected content has a linked path")
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = _stamp(os.fstat(fd))
        if (
            not stat.S_ISREG(before["st_mode"])
            or before["st_uid"] != os.getuid()
            or before["st_nlink"] != 1
            or before["st_mode"] & 0o022
            or before["st_size"] > MAX_SNAPSHOT
        ):
            raise ValueError("capture protected content has unsafe custody or size")
        sha = hashlib.sha256()
        while chunk := os.read(fd, 1024**2):
            sha.update(chunk)
        if before != _stamp(os.fstat(fd)) or before != _stamp(path.lstat()):
            raise ValueError("capture protected content changed while observed")
        return sha.hexdigest()
    finally:
        os.close(fd)


def _inputs(config, supplied):
    from .delivery_config import COMMAND_RE
    from .delivery_feature_review_context import _git
    from .delivery_runtime_installation import bootstrap_inputs
    from .delivery_runtime_location import source_root

    if (
        not isinstance(supplied, dict)
        or set(supplied) != FIELDS
        or supplied["answer"] != ANSWER
        or not isinstance(supplied["command_id"], str)
        or not COMMAND_RE.fullmatch(supplied["command_id"])
        or not isinstance(supplied["captures"], list)
        or len(supplied["captures"]) != 2
        or any(not isinstance(v, dict) or set(v) != SELECTION for v in supplied["captures"])
        or any(not isinstance(v["run_id"], str) for v in supplied["captures"])
        or len({v["run_id"] for v in supplied["captures"]}) != 2
        or not isinstance(supplied["migration_path"], str)
        or not isinstance(supplied["migration_sha256"], str)
    ):
        raise ValueError("invalid explicit two-execution retained-history capture")
    data = config.state_root / "temporal.sqlite3"
    if data.exists() and config.tracking_db.samefile(data):
        raise ValueError("capture tracking database aliases original Temporal data")
    migration_path = Path(supplied["migration_path"])
    if not migration_path.is_relative_to(config.state_root / "operator-inputs"):
        raise ValueError("capture migration input left original operator inputs")
    raw = private_bytes(migration_path)
    if _sha(raw) != supplied["migration_sha256"]:
        raise ValueError("capture migration input bytes changed")
    migration = json.loads(raw)
    if (
        len(migration.get("runs", [])) != 6
        or not {v["run_id"] for v in supplied["captures"]} <= set(migration["runs"])
        or _sha(private_bytes(config.path)) != migration["owner_before_sha256"]
        or os.path.lexists(config.state_root / "runtime-installations/current.json")
    ):
        raise ValueError("capture requires the unchanged original six-run installation owner")
    inputs = bootstrap_inputs(config, migration)
    for name in ("state.py", "schema.sql"):
        path = "skills/devflow/scripts/" + name
        if (source_root() / path).read_bytes() != _git(
            source_root(), "show", BASELINE + ":" + path
        ):
            raise ValueError("capture shared helper/schema differs from original baseline")
    for item in supplied["captures"]:
        if (
            any(
                not isinstance(item[key], str) or not item[key]
                for key in ("run_id", "workflow_id", "execution_run_id", "request_digest")
            )
            or any(
                type(item[key]) is not int or item[key] < 1
                for key in ("revision", "protocol_revision")
            )
            or (
                item["recovery_digest"] is not None
                and (
                    not isinstance(item["recovery_digest"], str)
                    or not re.fullmatch(r"[0-9a-f]{64}", item["recovery_digest"])
                )
            )
        ):
            raise ValueError("capture selection lacks exact current execution authority")
    return migration, inputs


def _reader(config, declared):
    """Explicit current configured reader; no historical executable hash is invented."""
    if (
        declared
        != {
            "configured_path": config.raw.get("temporal_bin"),
            "resolved_path": "/opt/homebrew/Cellar/temporal/1.9.1/bin/temporal",
            "sha256": READER_SHA,
            "version": "1.9.1",
            "server_version": "1.32.0",
            "historical_start_binary_sha256": None,
        }
        or declared["configured_path"] != "/opt/homebrew/bin/temporal"
    ):
        raise ValueError("capture requires the explicit configured copy-only Temporal reader")
    path = Path(declared["configured_path"])
    resolved = Path(declared["resolved_path"])
    if path.resolve(strict=True) != resolved or _file_hash(resolved) != READER_SHA:
        raise ValueError("configured Temporal reader bytes or symlink chain changed")
    prefix = resolved.parent.parent
    installed = prefix / "INSTALL_RECEIPT.json"
    formula = prefix / ".brew/temporal.rb"
    if _file_hash(installed) != READER_RECEIPT_SHA or _file_hash(formula) != READER_FORMULA_SHA:
        raise ValueError("configured Temporal reader Homebrew provenance changed")
    metadata = json.loads(installed.read_bytes())
    if (
        metadata.get("poured_from_bottle") is not True
        or metadata.get("binary_relocation_files") != []
        or metadata.get("source", {}).get("versions", {}).get("stable") != "1.9.1"
    ):
        raise ValueError("configured Temporal reader has no exact nonrelocated bottle proof")
    log = config.state_root / "temporal.log"
    raw = private_bytes(log, maximum=16 * 1024**2)
    if READER_HEADER.encode() not in raw.splitlines():
        raise ValueError("original Temporal startup version evidence is absent")
    if sys.version_info[:3] != (3, 12, 13) or version("temporalio") != "1.33.0":
        raise ValueError("capture requires the original Python3.12.13 and framework SDK1.33")
    return {
        **declared,
        "identity": _stamp(resolved.stat()),
        "configured_link": os.readlink(path),
        "configured_identity": _stamp(path.lstat()),
        "homebrew_receipt_sha256": READER_RECEIPT_SHA,
        "homebrew_formula_sha256": READER_FORMULA_SHA,
        "original_log_sha256": _sha(raw),
        "sdk": "1.33.0",
        "grpc_receive_limit_bytes": MAX_HISTORY,
        "python": "3.12.13",
    }


def _pin_inputs(paths, leases):
    pinned = {}
    for path in set(map(Path, paths)):
        if path.resolve(strict=True) != path:
            raise ValueError("capture input has a linked name")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
        leases.callback(os.close, fd)
        pinned[path] = fd, _stamp(os.fstat(fd)), _file_hash(path)

    def current():
        for path, (fd, expected, sha) in pinned.items():
            if (
                expected != _stamp(os.fstat(fd))
                or expected != _stamp(path.lstat())
                or _file_hash(path) != sha
            ):
                raise ValueError("capture original input file generation changed")

    current()
    return current


def _listeners(port):
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-t", f"-iTCP:{port}", "-sTCP:LISTEN"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode not in {0, 1} or result.stderr.strip():
        raise ValueError("capture listener visibility is indeterminate")
    if any(not v.isdecimal() for v in result.stdout.splitlines()):
        raise ValueError("capture listener inventory is malformed")
    return {int(v) for v in result.stdout.splitlines()}


def _capture_stopped(config):
    from .delivery_control import _ports
    from .delivery_runtime_installation import _stopped

    _stopped(config)
    if any(_listeners(port) for port in _ports(config)[1:]):
        raise ValueError("capture original service port is still occupied")


def _collect(store, migration, supplied, leases):
    from .delivery_amendment_capacity import submitted_owner
    from .delivery_native_renewal import _old_proof
    from .delivery_policy_recovery import work_binding
    from .delivery_runtime_installation import (
        _closure,
        _global_native,
        _historical_specs,
        _native_inputs,
        _native_observation,
        destination,
    )
    from .delivery_runtime_location import KIND, PENDING, REVERSED

    # Existing closure/config readers can repair benign directory modes.
    # Capture, like installation preflight, refuses drift before those readers.
    for folder in (
        store.config.state_root,
        store.config.state_root / "closures",
        store.config.state_root / "configuration-snapshots",
    ):
        if not folder.is_dir() or folder.resolve(strict=True) != folder:
            raise ValueError("capture original evidence directory is absent or linked")
        info = folder.lstat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("capture original evidence directory is not already private")
    for path in (store.config.state_root / "configuration-snapshots").glob("*.json"):
        private_bytes(path)
    _capture_stopped(store.config)
    from .delivery_closures import sealed_record

    selected = {v["run_id"]: v for v in supplied["captures"]}
    items = []
    installed = None
    for run in sorted(migration["runs"]):
        spec = store.effective_spec(run)
        if spec.get("feature_worker") or spec.get("feature_delivery"):
            raise ValueError("capture requires original ordinary native deliveries")
        _original, owner = submitted_owner(spec)
        if owner.path != store.config.path or not store.owns_execution(spec):
            raise ValueError("capture belongs to another original operational owner")
        history = _historical_specs(store, spec)
        for previous in history:
            _old_proof(previous)
        measured = destination(spec)
        if installed is not None and measured != installed:
            raise ValueError("capture cohort has different native runtime authority")
        installed = measured
        with store._connect() as db:
            if (
                not store.state._schema_current(db)
                or db.execute("PRAGMA journal_mode").fetchone()[0] != "wal"
            ):
                raise ValueError("capture requires the original current shared schema and WAL mode")
            row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run,)).fetchone())
            if db.execute(
                "SELECT 1 FROM delivery_events WHERE run_id=? AND type IN (?,?,?)",
                (run, KIND, PENDING, REVERSED),
            ).fetchone():
                raise ValueError("capture must precede installation acknowledgement")
            tables = {
                name: [
                    dict(r)
                    for r in db.execute(
                        f"SELECT * FROM {name} WHERE run_id=? ORDER BY {key}", (run,)
                    )
                ]
                for name, key in (
                    ("delivery_attempts", "job_key"),
                    ("delivery_effects", "effect_key"),
                    ("delivery_mutations", "command_id"),
                )
            }
            claim = store.state.claim_for(db, spec["work_id"])
            claim = (
                {k: claim[k] for k in ("resource", "work_id", "owner", "claimed_at")}
                if claim
                else None
            )
            work = work_binding(store, spec, db)
            if not db.execute(
                "SELECT 1 FROM delivery_outbox WHERE run_id=? AND state='sent'", (run,)
            ).fetchone():
                raise ValueError("capture execution was never dispatched")
            sealed = db.execute(
                "SELECT * FROM delivery_closures WHERE workflow_id=?",
                (row["workflow_id"] or "delivery-" + run,),
            ).fetchall()
        if (
            any(v["state"] != "finished" for v in tables["delivery_attempts"])
            or claim
            and claim["owner"] != "external:devflow:" + run
        ):
            raise ValueError("capture has unfinished native work or a foreign claim")
        if run in selected:
            sealed_record(store, row["workflow_id"] or "delivery-" + run)
            expected = selected[run]
            recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
            current = {
                key: row[key]
                for key in ("run_id", "revision", "protocol_revision", "request_digest")
            }
            current.update(
                workflow_id=row["workflow_id"] or "delivery-" + run,
                execution_run_id=expected["execution_run_id"],
                recovery_digest=digest(recovery) if recovery else None,
            )
            if (
                current != expected
                or sealed
                or row["outcome"] is not None
                or row["execution_state"] != "running"
                or row["phase"] not in {"repair", "checks"}
            ):
                raise ValueError("capture selected current execution or revision changed")
            closure_record = None
        else:
            _closed, closure_record = _closure(store, row)
        items.append(
            {
                "original_spec": spec,
                "historical_specs": history,
                "row": row,
                "tables": tables,
                "claim": claim,
                "work_binding": work,
                "closure_record": closure_record,
                "native": _native_observation(spec, leases),
            }
        )
    authority = {"runs": items, "owner_path": str(store.config.path), "destination": installed}
    with store._connect() as db:
        authority["native_inputs"] = _native_inputs(store, db)
    authority["global_native"] = _global_native(store.config, authority, leases)
    return authority


def _current(store, authority, db):
    from .delivery_native_renewal import _old_proof
    from .delivery_policy_recovery import work_binding
    from .delivery_runtime_installation import (
        _global_current,
        _native_current,
        _native_inputs,
    )

    _capture_stopped(store.config)
    _global_current(authority)
    if _native_inputs(store, db) != authority["native_inputs"]:
        raise ValueError("capture native execution input changed")
    for item in authority["runs"]:
        row, spec = item["row"], item["original_spec"]
        current = db.execute(
            "SELECT * FROM delivery_runs WHERE run_id=?", (row["run_id"],)
        ).fetchone()
        if current is None or dict(current) != row:
            raise ValueError("capture current delivery row changed")
        for name, key in (
            ("delivery_attempts", "job_key"),
            ("delivery_effects", "effect_key"),
            ("delivery_mutations", "command_id"),
        ):
            if [
                dict(r)
                for r in db.execute(
                    f"SELECT * FROM {name} WHERE run_id=? ORDER BY {key}", (row["run_id"],)
                )
            ] != item["tables"][name]:
                raise ValueError("capture attempt/effect/command changed")
        claim = store.state.claim_for(db, spec["work_id"])
        claim = (
            {k: claim[k] for k in ("resource", "work_id", "owner", "claimed_at")} if claim else None
        )
        if claim != item["claim"] or work_binding(store, spec, db) != item["work_binding"]:
            raise ValueError("capture claim/work binding changed")
        for previous in item["historical_specs"]:
            _old_proof(previous)
        if store.effective_spec(row["run_id"]) != spec:
            raise ValueError("capture admitted execution bytes changed")
        _native_current(item)
        if item["closure_record"] is None:
            from .delivery_closures import sealed_record

            sealed_record(store, row["workflow_id"] or "delivery-" + row["run_id"])
        actual_closures = [
            dict(v)
            for v in db.execute(
                "SELECT * FROM delivery_closures WHERE workflow_id=?",
                (row["workflow_id"] or "delivery-" + row["run_id"],),
            )
        ]
        if actual_closures != ([item["closure_record"]] if item["closure_record"] else []):
            raise ValueError("capture selected closure record changed")
        if item["closure_record"]:
            from .delivery_closures import _private_root, _read_sealed

            _read_sealed(_private_root(store), item["closure_record"]["history_sha256"])


def _fd_records(paths):
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-F0pfnDi", "--", *map(str, paths)],
        capture_output=True,
        timeout=10,
    )
    if result.returncode != 0 or result.stderr.strip():
        raise ValueError("capture original data FD visibility is indeterminate")
    pid, current, records = None, None, []
    for raw in result.stdout.split(b"\0"):
        field = raw.strip(b"\n")
        if not field:
            continue
        key, value = chr(field[0]), field[1:].decode("utf-8", errors="strict")
        if key == "p":
            if not value.isdecimal():
                raise ValueError("capture data FD inventory has no process identity")
            pid = int(value)
        elif key == "f":
            current = {"pid": pid, "fd": value}
            records.append(current)
        elif key in {"D", "i"}:
            if current is None:
                raise ValueError("capture data FD inventory is incomplete")
            current[key] = value
    return records


class SourceFamily:
    """Held read-only original bytes. SQLite is permitted only on derived copies."""

    def __init__(self, root):
        self.root = Path(root)
        self.paths = {}
        self.descriptors = {}
        self.initial = None

    def names(self):
        names = sorted(
            p.name
            for p in self.root.iterdir()
            if p.name == "temporal.sqlite3" or p.name.startswith("temporal.sqlite3-")
        )
        if "temporal.sqlite3" not in names or set(names) - {
            "temporal.sqlite3",
            "temporal.sqlite3-wal",
            "temporal.sqlite3-shm",
            "temporal.sqlite3-journal",
        }:
            raise ValueError("capture source has an unexpected SQLite journal family")
        return names

    def __enter__(self):
        _folder(self.root)
        self.directory = _stamp(self.root.stat())
        try:
            total = 0
            for name in self.names():
                path = self.root / name
                if path.resolve(strict=True) != path:
                    raise ValueError("capture original data has a linked name")
                fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
                self.descriptors[name], self.paths[name] = fd, path
                info = os.fstat(fd)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid()
                    or info.st_nlink != 1
                    or info.st_mode & 0o022
                    or info.st_size > MAX_SNAPSHOT
                    or (name.endswith("-journal") and info.st_size)
                ):
                    raise ValueError("capture source is unsafe, oversized or has a hot journal")
                total += info.st_size
            if (
                total > MAX_SNAPSHOT
                or shutil.disk_usage(self.root).free < total * 3 + 128 * 1024**2
            ):
                raise ValueError("capture exceeds its bounded snapshot/disk allowance")
            self.initial = self.observe()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_args):
        for fd in self.descriptors.values():
            os.close(fd)
        self.descriptors.clear()

    def observe(self):
        if self.names() != sorted(self.descriptors) or _stamp(self.root.stat()) != self.directory:
            raise ValueError("capture original SQLite namespace changed")
        values = {}
        for name, fd in self.descriptors.items():
            before = _stamp(os.fstat(fd))
            if before != _stamp(self.paths[name].lstat()):
                raise ValueError("capture original SQLite generation changed")
            os.lseek(fd, 0, os.SEEK_SET)
            sha = hashlib.sha256()
            while chunk := os.read(fd, 1024**2):
                sha.update(chunk)
            if before != _stamp(os.fstat(fd)) or before != _stamp(self.paths[name].lstat()):
                raise ValueError("capture original SQLite bytes changed during observation")
            values[name] = {"identity": before, "sha256": sha.hexdigest()}
        own = {(os.getpid(), fd) for fd in self.descriptors.values()}
        seen = set()
        identities = {(v["identity"]["st_dev"], v["identity"]["st_ino"]) for v in values.values()}
        for item in _fd_records(self.paths.values()):
            try:
                pair = int(item["D"], 16), int(item["i"])
                fd = re.fullmatch(r"([0-9]+)[rwu]?", item["fd"])
                subject = item["pid"], int(fd[1]) if fd else None
            except (KeyError, ValueError) as exc:
                raise ValueError("capture original data FD inventory is incomplete") from exc
            if pair in identities:
                if subject not in own:
                    raise ValueError("capture original Temporal data still has a foreign FD")
                seen.add(subject)
        if seen != own:
            raise ValueError("capture original data FD visibility is incomplete")
        if self.initial is not None and values != self.initial:
            raise ValueError("capture original SQLite source changed")
        return values

    def snapshot(self, folder):
        observed = self.observe()
        master = folder / "master"
        if not master.exists():
            stage = folder / (".snapshot-" + uuid.uuid4().hex)
            _new_folder(stage)
            for name, fd in self.descriptors.items():
                target = stage / name
                output = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_CLOEXEC, 0o600)
                try:
                    os.lseek(fd, 0, os.SEEK_SET)
                    with os.fdopen(os.dup(output), "wb") as stream:
                        while chunk := os.read(fd, 1024**2):
                            stream.write(chunk)
                        stream.flush()
                        os.fsync(stream.fileno())
                finally:
                    os.close(output)
                if _file_hash(target) != observed[name]["sha256"]:
                    raise ValueError("capture snapshot bytes differ from original source")
            self.observe()
            _publish(stage / "manifest.json", observed)
            _publish_exclusive(stage, master)
        _master(master, observed)
        return observed


def _working(folder, namespace, expected):
    """Recover/backup only the owned copy; never reuse the original SHM cache."""
    master = folder / "master"
    _master(master, expected)
    working = _new_folder(folder / "working")
    raw, target = working / "raw.sqlite3", working / "server.sqlite3"
    with ExitStack() as held:
        directory = os.open(working, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY | os.O_CLOEXEC)
        held.callback(os.close, directory)
        directory_identity = _object(os.fstat(directory))
        files = {}

        def current(*paths):
            if directory_identity != _object(working.lstat()) or directory_identity != _object(
                os.fstat(directory)
            ):
                raise ValueError("capture working directory generation changed")
            for path in paths:
                fd, identity = files[path]
                info = os.fstat(fd)
                if (
                    _object(info) != identity
                    or _object(path.lstat()) != identity
                    or info.st_nlink != 1
                    or path.lstat().st_nlink != 1
                ):
                    raise ValueError("capture working file generation changed")

        for old, path in (
            ("temporal.sqlite3", raw),
            ("temporal.sqlite3-wal", Path(str(raw) + "-wal")),
            (None, target),
        ):
            if old and old not in expected:
                continue
            output = os.open(
                path, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600
            )
            held.callback(os.close, output)
            files[path] = output, _object(os.fstat(output))
            if old:
                descriptor = os.open(
                    master / old, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC
                )
                try:
                    before = _stamp(os.fstat(descriptor))
                    sha = hashlib.sha256()
                    # FileIO handles partial writes; never reopen a destination name.
                    with os.fdopen(os.dup(output), "wb", closefd=True) as stream:
                        while chunk := os.read(descriptor, 1024**2):
                            sha.update(chunk)
                            stream.write(chunk)
                        stream.flush()
                    os.fsync(output)
                    if (
                        before != _stamp(os.fstat(descriptor))
                        or before != _stamp((master / old).lstat())
                        or sha.hexdigest() != expected[old]["sha256"]
                    ):
                        raise ValueError("capture copied master generation changed")
                finally:
                    os.close(descriptor)
            current(path)
        current(*files)
        with closing(sqlite3.connect(raw)) as source:
            current(raw, target)
            if source.execute("PRAGMA integrity_check").fetchone() != ("ok",):
                raise ValueError("capture working snapshot fails SQLite integrity")
            rows = source.execute(
                "SELECT id FROM namespaces WHERE name=? AND length(id)=16 LIMIT 2", (namespace,)
            ).fetchall()
            if len(rows) != 1:
                raise ValueError("capture pristine snapshot lacks its original namespace")
            identity = str(uuid.UUID(bytes=bytes(rows[0][0])))
            current(raw, target)
            with closing(sqlite3.connect(target)) as dest:
                current(raw, target)
                source.backup(dest)
                current(raw, target)
        current(raw, target)
        _master(master, expected)
    return target, identity


def _ports():
    with ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(3)]
        for entry in sockets:
            entry.bind(("127.0.0.1", 0))
        return tuple(entry.getsockname()[1] for entry in sockets)


def _argv(binary, database, ports):
    return [
        binary,
        "server",
        "start-dev",
        "--headless",
        "--ip",
        "127.0.0.1",
        "--port",
        str(ports[0]),
        "--http-port",
        str(ports[1]),
        "--metrics-port",
        str(ports[2]),
        "--db-filename",
        str(database),
        "--log-level",
        "warn",
        "--dynamic-config-value",
        "frontend.enableServerVersionCheck=false",
    ]


def _reader_authority(folder):
    capture = folder.parent.parent
    if (
        folder.parent.name != "readers"
        or not folder.name.isdecimal()
        or folder.resolve(strict=True) != folder
    ):
        raise ValueError("capture reader left its owned generation namespace")
    plan = json.loads(private_bytes(folder / "intent.json"))
    intent_raw = receipt_bytes(capture / "intent.json")
    command = json.loads(intent_raw)["command"]
    launch = json.loads(private_bytes(folder / "launch.json"))
    ports = plan.get("ports")
    if (
        not isinstance(ports, list)
        or len(ports) != 3
        or len(set(ports)) != 3
        or any(type(port) is not int or not 1024 < port < 65536 for port in ports)
        or plan["capture_intent_sha256"] != _sha(intent_raw)
        or plan["command_digest"] != digest(command)
        or plan["cwd"] != str(folder)
        or plan["database"] != str(folder / "snapshot/working/server.sqlite3")
        or plan["cwd_identity"] != _object(folder.lstat())
        or plan["working_identity"] != _object((folder / "snapshot/working").lstat())
        or plan["database_identity"] != _object(Path(plan["database"]).lstat())
        or Path(plan["database"]).lstat().st_nlink != 1
        or plan["argv"] != _argv(command["reader"]["resolved_path"], plan["database"], ports)
        or launch["argv"] != plan["argv"]
        or launch["environment"] != _environment(folder)
    ):
        raise ValueError("capture reader has no independent command/path authority")
    return plan


def _environment(folder):
    return {
        "HOME": str(folder / "home"),
        "TMPDIR": str(folder / "tmp"),
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "TZ": "UTC",
        "PYTHONDONTWRITEBYTECODE": "1",
    }


def _reader_command(folder, pid):
    plan = _reader_authority(folder)
    output = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="], capture_output=True, text=True, timeout=5
    )
    if output.returncode != 0:
        raise ValueError("capture live reader command visibility is indeterminate")
    actual = shlex.split(output.stdout.strip())
    gates = [
        [python, "-I", "-B", "-m", "devflow_temporal.native_child", str(folder)]
        for python in {sys.executable, str(Path(sys.executable).resolve())}
    ]
    if actual != plan["argv"] and actual not in gates:
        raise ValueError("capture live actor is not its derived copy-server command")


def _table():
    # The private child records UTC. Reattachment must use the same clock view,
    # independently of a CLI's inherited timezone or the host's local timezone.
    output = subprocess.run(
        ["ps", "-A", "-o", "pid=,ppid=,pgid=,stat=,lstart="],
        env={"PATH": "/usr/bin:/bin", "TZ": "UTC", "LANG": "C"},
        check=True,
        capture_output=True,
        text=True,
        timeout=5,
    ).stdout
    result = {}
    for line in output.splitlines():
        parts = line.strip().split(maxsplit=4)
        if len(parts) != 5 or not all(v.isdecimal() for v in parts[:3]):
            raise ValueError("capture process visibility is incomplete")
        result[int(parts[0])] = {
            "ppid": int(parts[1]),
            "pgid": int(parts[2]),
            "stat": parts[3],
            "identity": parts[4],
        }
    return result


def _owned(folder):
    """An ungated Popen/ready file alone never authorizes signalling an actor."""
    authorized = folder / "authorized.json"
    if not authorized.exists():
        ready = folder / "ready.json"
        if ready.exists():
            value = json.loads(private_bytes(ready))
            if _table().get(value["pid"], {}).get("identity") == value["identity"]:
                raise ValueError(
                    "capture child died before authorization; live identity is unknown"
                )
        return {}
    _reader_authority(folder)
    value = json.loads(private_bytes(authorized))
    if (
        _sha(private_bytes(folder / "ready.json")) != value["ready_sha256"]
        or _sha(private_bytes(folder / "launch.json")) != value["launch_sha256"]
        or _sha(private_bytes(folder / "intent.json")) != value["intent_sha256"]
    ):
        raise ValueError("capture reader launch authority changed")
    owned = {int(pid): entry for pid, entry in value["owned"].items()}
    journal = folder / "actors.json"
    if journal.exists():
        previous = json.loads(private_bytes(journal))
        if previous.get("authorization_sha256") != _sha(private_bytes(authorized)):
            raise ValueError("capture reader actor journal belongs to another launch")
        owned.update({int(pid): entry for pid, entry in previous["owned"].items()})
    table = _table()
    root = value["root_pid"]
    if (
        set(value["owned"]) != {str(root)}
        or value["owned"][str(root)]["ppid"] != value["creator_pid"]
    ):
        raise ValueError("capture root was not its recorded operator child")
    if table.get(root, {}).get("identity") == owned[root]["identity"]:
        _reader_command(folder, root)
    live = {
        pid
        for pid, entry in owned.items()
        if table.get(pid, {}).get("identity") == entry["identity"]
    }
    while True:
        children = {
            pid for pid, entry in table.items() if entry["ppid"] in live and pid not in live
        }
        if not children:
            break
        for pid in children:
            owned[pid] = table[pid]
        live.update(children)
    if any(entry["pgid"] != value["root_pid"] for entry in owned.values()):
        raise ValueError("capture reader has an unproven process generation")
    write_private(
        journal,
        {
            "authorization_sha256": _sha(private_bytes(authorized)),
            "owned": {str(pid): v for pid, v in owned.items()},
        },
    )
    return owned


def _no_reader_cwd(folder):
    """Popen without a durable PID can only wait at this exact trusted cwd/GO gate.

    Absence must be observed, never inferred from an absent ready file. Unknown
    pre-GO actors are not signalable and do not permit a replacement generation.
    """
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-F0pfnDi", "--", str(folder)], capture_output=True, timeout=10
    )
    if (
        result.returncode not in {0, 1}
        or result.stderr.strip()
        or result.returncode == 1
        and result.stdout.strip()
    ):
        raise ValueError("capture unacknowledged child visibility is indeterminate")
    if result.stdout.strip():
        raise ValueError("capture reader still has an unacknowledged cwd/descriptor")


def _cleanup(folder):
    if not (folder / "intent.json").exists():
        if (folder / "ready.json").exists() or (folder / "authorized.json").exists():
            raise ValueError("capture child has no durable launch intent")
        return  # The launch intent always precedes Popen; incomplete copy staging is inert.
    owned = _owned(folder)
    if not owned:
        _no_reader_cwd(folder)
    for sig in (signal.SIGTERM, signal.SIGKILL):
        table = _table()
        for pid, entry in owned.items():
            if (
                table.get(pid, {}).get("identity") == entry["identity"]
                and table[pid]["pgid"] == entry["pgid"]
                and not table[pid]["stat"].startswith("Z")
            ):
                try:
                    os.kill(pid, sig)
                except ProcessLookupError:
                    pass
        for _ in range(25):
            table = _table()
            if not any(
                table.get(pid, {}).get("identity") == entry["identity"]
                and not table[pid]["stat"].startswith("Z")
                for pid, entry in owned.items()
            ):
                break
            time.sleep(0.05)
        else:
            continue
        break
    else:
        raise ValueError("capture owned copy server cleanup is unknown")
    plan = json.loads(private_bytes(folder / "intent.json"))
    if any(_listeners(port) for port in plan["ports"]):
        raise ValueError("capture copy-server port cleanup is unknown")
    table = _table()
    if owned:
        root = json.loads(private_bytes(folder / "authorized.json"))["root_pid"]
        if any(
            entry["pgid"] == root and not entry["stat"].startswith("Z") for entry in table.values()
        ):
            raise ValueError("capture copy-server process group cleanup is unknown")
    if any(
        table.get(pid, {}).get("identity") == v["identity"]
        and not table[pid]["stat"].startswith("Z")
        for pid, v in owned.items()
    ):
        raise ValueError("capture copy-server actor cleanup is unknown")
    _no_reader_cwd(folder)
    _publish(folder / "cleanup.json", {"state": "confirmed", "intent_sha256": digest(plan)})


@contextmanager
def _sdk_receive_limit():
    """A fixed local read-client cap, never a native policy/environment amendment.

    SDK1.33 Core honors this process-only setting before allocating an incoming
    gRPC message. The public command runs standalone; an already used SDK runtime
    outside this owner is refused rather than assuming its cached transport cap.
    """
    global _SDK_LIMIT_PINNED
    from temporalio import runtime

    key = "TEMPORAL_MAX_INCOMING_GRPC_BYTES"
    original = os.environ.get(key)
    if version("temporalio") != "1.33.0" or original not in {None, str(MAX_HISTORY)}:
        raise ValueError("capture SDK receive resource authority changed")
    if runtime._runtime_ref._default_runtime is not None and not _SDK_LIMIT_PINNED:
        raise ValueError("capture requires a fresh standalone SDK owner process")
    os.environ[key] = str(MAX_HISTORY)
    _SDK_LIMIT_PINNED = True
    try:
        yield
    finally:
        if original is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original


def _payloads(message):
    if message.DESCRIPTOR.full_name == "temporal.api.common.v1.Payload":
        yield message
        return
    for field, value in message.ListFields():
        if field.message_type is None:
            continue
        if field.is_repeated:
            values = value.values() if field.message_type.GetOptions().map_entry else value
            for child in values:
                if hasattr(child, "DESCRIPTOR"):
                    yield from _payloads(child)
        else:
            yield from _payloads(value)


def _bounded_payloads(history, consumed=0):
    """Prove expansion bounds BEFORE the unchanged workflow codec allocates it."""
    total = consumed
    for event in history.events:
        for payload in _payloads(event):
            encoding = payload.metadata.get("encoding")
            if encoding not in {b"binary/zlib", b"binary/xz"}:
                size = len(payload.data)
            else:
                size, pending = 0, payload.data
                decoder = (
                    zlib.decompressobj()
                    if encoding == b"binary/zlib"
                    else lzma.LZMADecompressor(memlimit=32 * 1024**2)
                )
                try:
                    while True:
                        chunk = decoder.decompress(
                            pending, max_length=min(1024**2, MAX_PAYLOAD - size) + 1
                        )
                        size += len(chunk)
                        if size > MAX_PAYLOAD or total + size > MAX_DECODED:
                            raise ValueError(
                                "capture decoded history exceeds its payload/total bound"
                            )
                        if decoder.eof:
                            if decoder.unused_data:
                                raise ValueError("capture compressed payload has trailing data")
                            break
                        pending = decoder.unconsumed_tail if encoding == b"binary/zlib" else b""
                        if not pending and (
                            encoding == b"binary/zlib"
                            and not chunk
                            or encoding == b"binary/xz"
                            and decoder.needs_input
                        ):
                            raise ValueError("capture compressed payload is incomplete")
                except (lzma.LZMAError, zlib.error) as exc:
                    raise ValueError(
                        "capture compressed payload is invalid or exceeds memory bound"
                    ) from exc
            total += size
            if size > MAX_PAYLOAD or total > MAX_DECODED:
                raise ValueError("capture decoded history exceeds its payload/total bound")
    return total - consumed


async def _history(handle, deadline, budget):
    from temporalio.client import WorkflowHistory, WorkflowHistoryEventFilterType

    from .delivery_closures import history_json

    iterator = handle.fetch_history_events(
        page_size=128,
        event_filter_type=WorkflowHistoryEventFilterType.ALL_EVENT,
        rpc_timeout=timedelta(seconds=10),
    )
    events = []
    seen_tokens = set()
    while True:
        budget["pages"] += 1
        if budget["pages"] > MAX_PAGES:
            raise ValueError("capture full history exceeds its total page bound")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("capture full history exceeds its total time bound")
        await asyncio.wait_for(iterator.fetch_next_page(), remaining)
        page = iterator.current_page or []
        size = sum(event.ByteSize() for event in page) + len(iterator.next_page_token or b"")
        budget["bytes"] += size
        budget["events"] += len(page)
        if budget["bytes"] > MAX_HISTORY or budget["events"] > MAX_EVENTS:
            raise ValueError("capture full history exceeds its total byte/event bound")
        events.extend(page)
        if iterator.next_page_token is None:
            break
        token_sha = _sha(iterator.next_page_token)
        if token_sha in seen_tokens:
            raise ValueError("capture full history repeats a pagination token")
        seen_tokens.add(token_sha)
    history = WorkflowHistory(handle.id, events)
    # Protobuf map field order varies between fresh SDK processes. Preserve
    # canonical SDK JSON from the FIRST export, as the closure owner already
    # does, so an identical full re-observation never rewrites staged evidence.
    raw = history_json(history)
    budget["json_bytes"] = budget.get("json_bytes", 0) + len(raw)
    if budget["json_bytes"] > MAX_HISTORY:
        raise ValueError("capture full history JSON exceeds its byte bound")
    return history, raw


def _listener_proof(folder, root_pid):
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-a", "-p", str(root_pid), "-iTCP", "-sTCP:LISTEN", "-F0pn"],
        check=False,
        capture_output=True,
        timeout=5,
    )
    if result.returncode != 0 or result.stderr.strip():
        raise ValueError("capture copy listener visibility is indeterminate")
    addresses = []
    for raw in result.stdout.split(b"\0"):
        field = raw.strip(b"\n")
        if field.startswith(b"n"):
            address = field[1:].decode()
            if not re.fullmatch(r"(?:127\.0\.0\.1|\[::1\]):[0-9]+", address):
                raise ValueError("capture copy server exposed a nonloopback listener")
            addresses.append(address)
    if not addresses:
        raise ValueError("capture copy server has no independently observed listeners")
    _publish(
        folder / "listeners.json",
        {
            "authorization_sha256": _sha(private_bytes(folder / "authorized.json")),
            "addresses": sorted(set(addresses)),
        },
    )


async def _exports(config, reader, authority, selections, namespace_id, folder):
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest
    from temporalio.client import Client, WorkflowExecutionStatus

    from .delivery_closed_projection import observe_terminal_projection
    from .delivery_closures import closure_facts, history_json
    from .delivery_codec import DELIVERY_DATA_CONVERTER

    deadline = time.monotonic() + TOTAL_SECONDS
    plan = json.loads(private_bytes(folder / "intent.json"))
    address = "127.0.0.1:" + str(plan["ports"][0])
    root_pid = json.loads(private_bytes(folder / "authorized.json"))["root_pid"]
    startup = min(deadline, time.monotonic() + 20)
    client, last = None, None
    while client is None:
        _owned(folder)
        live = _table().get(root_pid)
        if not live or live["stat"].startswith("Z"):
            raise ValueError("capture copy server exited before readiness; inspect its private log")
        remaining = startup - time.monotonic()
        if remaining <= 0:
            raise ValueError(
                "capture copy server did not become ready within its finite deadline"
            ) from last
        owners = [_listeners(port) for port in plan["ports"]]
        if any(pids and pids != {root_pid} for pids in owners):
            raise ValueError("capture copy endpoint has a foreign listener")
        if all(pids == {root_pid} for pids in owners):
            try:
                client = await asyncio.wait_for(
                    Client.connect(
                        address,
                        namespace=config.raw.get("temporal_namespace", "default"),
                        data_converter=DELIVERY_DATA_CONVERTER,
                    ),
                    min(2, remaining),
                )
            except (RuntimeError, OSError, TimeoutError) as exc:
                last = exc
        if client is None:
            await asyncio.sleep(0.1)
    _listener_proof(folder, root_pid)
    if any(_listeners(port) != {root_pid} for port in plan["ports"]):
        raise ValueError("capture copy endpoint has a foreign listener")
    response = await asyncio.wait_for(
        client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=client.namespace), timeout=timedelta(seconds=10)
        ),
        10,
    )
    if response.namespace_info.id != namespace_id:
        raise ValueError("capture copy namespace differs from pristine snapshot")
    budget = {"events": 0, "bytes": 0, "pages": 0}
    exports = []
    for selected in selections:
        item = next(v for v in authority["runs"] if v["row"]["run_id"] == selected["run_id"])
        spec, row = item["original_spec"], item["row"]
        exact = client.get_workflow_handle(
            selected["workflow_id"], run_id=selected["execution_run_id"]
        )
        description = await asyncio.wait_for(exact.describe(rpc_timeout=timedelta(seconds=10)), 10)
        current = await asyncio.wait_for(
            client.get_workflow_handle(selected["workflow_id"]).describe(
                rpc_timeout=timedelta(seconds=10)
            ),
            10,
        )
        if (
            current.run_id != selected["execution_run_id"]
            or description.run_id != selected["execution_run_id"]
            or description.status != WorkflowExecutionStatus.COMPLETED
            or not description.close_time
            or description.workflow_type != "DevflowDeliveryWorkflow"
        ):
            raise ValueError(
                "capture snapshot selected execution is not its exact completed workflow"
            )
        if (
            description.history_length > MAX_EVENTS
            or not 0 < description.raw_info.history_size_bytes <= MAX_HISTORY
        ):
            raise ValueError(
                "capture described history exceeds or lacks its prefetch resource bound"
            )
        history, raw = await _history(exact, deadline, budget)
        budget["decoded_bytes"] = budget.get("decoded_bytes", 0) + _bounded_payloads(
            history, budget.get("decoded_bytes", 0)
        )
        facts = await closure_facts(history)
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        if (
            facts["execution_run_id"] != selected["execution_run_id"]
            or facts["status"] != "COMPLETED"
            or facts["closed_at"] != description.close_time.isoformat()
            or facts["event_count"] != description.history_length
            or facts["request_digest"] != row["request_digest"]
            or facts["recovery_digest"] != selected["recovery_digest"]
            or facts["inputs"] != [spec, recovery]
            or history.events[0].workflow_execution_started_event_attributes.task_queue.name
            != config.queue
            or await description.memo_value("request_digest", None) != row["request_digest"]
            or await description.memo_value("recovery_digest", None) != selected["recovery_digest"]
        ):
            raise ValueError("capture full history start/memo/recovery/description changed")
        result = facts["result"]
        failure = (
            result.get("checks", {}).get("terminal_projection_failure")
            if isinstance(result, dict)
            else None
        )
        if not failure:
            raise ValueError("capture selected history has no authentic failed terminal projection")
        observation, proposal = await observe_terminal_projection(
            reader, row, spec, description, result, history, failure, {}
        )
        envelope = {
            "schema": SCHEMA,
            "workflow_id": selected["workflow_id"],
            "execution_run_id": selected["execution_run_id"],
            "run_id": selected["run_id"],
            "namespace": client.namespace,
            "task_queue": config.queue,
            "namespace_id": namespace_id,
            "raw_sha256": _sha(raw),
            "history_sha256": _sha(history_json(history)),
            "reader_intent_sha256": digest(plan),
            "source_intent_sha256": plan["capture_intent_sha256"],
            "pages_total": budget["pages"],
            "terminal_observation": observation,
            "terminal_proposal_sha256": digest(proposal),
        }
        target = folder / "exports"
        _folder(target)
        _immutable(target / (selected["run_id"] + ".json"), raw)
        _publish(target / (selected["run_id"] + ".envelope.json"), envelope)
        exports.append({"history": history, "facts": facts, "envelope": envelope})
        _owned(folder)
    return exports


async def _bounded_exports(*args):
    try:
        return await asyncio.wait_for(_exports(*args), TOTAL_SECONDS)
    except TimeoutError as exc:
        raise ValueError("capture SDK export exceeded its total time bound") from exc


def _publish_exports(folder, exports):
    from .delivery_closures import history_json

    target = _folder(folder / "exports")
    for value in exports:
        envelope = value["envelope"]
        run = envelope["run_id"]
        raw = history_json(value["history"])
        _immutable(target / (run + ".json"), raw)
        path = target / (run + ".envelope.json")
        if path.exists():
            previous = json.loads(private_bytes(path))
            # Acknowledgement loss can span reader generations. A fresh SDK
            # export must reproduce every semantic/raw byte binding; retain the
            # original, independently owned reader provenance, never rewrite it.
            mutable = {"reader_intent_sha256", "pages_total"}
            if {k: v for k, v in previous.items() if k not in mutable} != {
                k: v for k, v in envelope.items() if k not in mutable
            }:
                raise ValueError("capture staged export differs from authentic re-observation")
            plans = [
                p
                for p in (folder / "readers").glob("*/intent.json")
                if digest(json.loads(private_bytes(p))) == previous["reader_intent_sha256"]
            ]
            if (
                len(plans) != 1
                or json.loads(private_bytes(plans[0].parent / "cleanup.json"))["state"]
                != "confirmed"
            ):
                raise ValueError("capture prior export has no proven reader cleanup")
            value["envelope"] = previous
        else:
            _publish(path, envelope)


def _serve(config, store, authority, supplied, folder, intent_sha256, launch_guard):
    readers = _folder(folder / "readers")
    entries = list(readers.iterdir())
    if {p.name for p in entries} != {str(i) for i in range(len(entries))}:
        raise ValueError("capture reader generations are not their exact owned sequence")
    for prior in sorted(entries):
        _existing_folder(prior)
        _cleanup(prior)
    if len(entries) >= 8:
        raise ValueError("capture exhausted its bounded explicit reader generations")
    intent_raw = receipt_bytes(folder / "intent.json")
    if _sha(intent_raw) != intent_sha256:
        raise ValueError("capture original source intent changed before working preparation")
    expected = json.loads(intent_raw)["source"]
    reader = _new_folder(readers / str(len(entries)))
    # Every generation has a new writable copy; the pristine master is never served.
    generation = _new_folder(reader / "snapshot")
    _master(folder / "master", expected)
    shutil.copytree(folder / "master", generation / "master")
    database, namespace = _working(
        generation, config.raw.get("temporal_namespace", "default"), expected
    )
    _folder(reader / "home")
    _folder(reader / "tmp")
    ports = _ports()
    from .delivery_control import _ports as service_ports

    if set(ports) & set(service_ports(config)[1:]):
        raise ValueError("capture copy endpoint overlaps original execution transport")
    argv = _argv(supplied["reader"]["resolved_path"], database, ports)
    environment = _environment(reader)
    _publish(reader / "launch.json", {"argv": argv, "environment": environment})
    _publish(
        reader / "intent.json",
        {
            "capture_intent_sha256": intent_sha256,
            "command_digest": digest(supplied),
            "ports": list(ports),
            "namespace_id": namespace,
            "database": str(database),
            "argv": argv,
            "cwd": str(reader),
            "cwd_identity": _object(reader.lstat()),
            "working_identity": _object((generation / "working").lstat()),
            "database_identity": _object(database.lstat()),
        },
    )
    _reader(config, supplied["reader"])
    with (reader / "server.log").open("xb") as log:
        (reader / "server.log").chmod(0o600)
        child = subprocess.Popen(
            [sys.executable, "-I", "-B", "-m", "devflow_temporal.native_child", str(reader)],
            stdin=subprocess.PIPE,
            stdout=log,
            stderr=subprocess.STDOUT,
            cwd=reader,
            env=environment,
            start_new_session=True,
            close_fds=True,
        )
        try:
            deadline = time.monotonic() + 10
            while not (reader / "ready.json").exists():
                if child.poll() is not None or time.monotonic() >= deadline:
                    raise ValueError("capture child never reached its durable GO gate")
                time.sleep(0.02)
            ready = json.loads(private_bytes(reader / "ready.json"))
            entry = _table().get(child.pid)
            if (
                not entry
                or ready != {"pid": child.pid, "identity": entry["identity"]}
                or entry["pgid"] != child.pid
            ):
                raise ValueError("capture child identity changed before GO")
            launch_guard()
            _publish(
                reader / "authorized.json",
                {
                    "root_pid": child.pid,
                    "creator_pid": os.getpid(),
                    "owned": {str(child.pid): entry},
                    "ready_sha256": _sha(private_bytes(reader / "ready.json")),
                    "launch_sha256": _sha(private_bytes(reader / "launch.json")),
                    "intent_sha256": _sha(private_bytes(reader / "intent.json")),
                },
            )
            child.stdin.write(b"GO\n")
            child.stdin.flush()
            child.stdin.close()
            exports = asyncio.run(
                _bounded_exports(config, store, authority, supplied["captures"], namespace, reader)
            )
        finally:
            if child.stdin and not child.stdin.closed:
                child.stdin.close()  # A pre-receipt child gets EOF, never GO or signal authority.
            if not (reader / "authorized.json").exists():
                child.wait(timeout=5)  # EOF is the pre-GO exit protocol; no unknown signal.
            _cleanup(reader)
            child.wait(timeout=5)
    return exports, str(reader)


def _response(config, folder, supplied):
    path = folder / "receipt.json"
    raw = receipt_bytes(path)
    receipt = json.loads(raw)
    if receipt["command"] != supplied:
        raise ValueError("capture command identity already binds different inputs")
    return {
        "kind": SCHEMA,
        "command_id": supplied["command_id"],
        "receipt": str(path),
        "receipt_sha256": _sha(raw),
        "histories": receipt["histories"],
    }


def verify_record(store, record):
    """Historical snapshot proof has no current native or transport launch permission."""
    with store._connect() as db:
        rows = db.execute(
            "SELECT command_id,request_digest,response_json FROM delivery_commands "
            "WHERE command_id LIKE 'retained-history-%'"
        ).fetchall()
    for row in rows:
        value = json.loads(row["response_json"])
        if value.get("receipt_sha256") != record["source_sha256"]:
            continue
        path = Path(value["receipt"])
        if (
            path.parent.parent != store.config.state_root / "closure-captures"
            or path.name != "receipt.json"
            or _sha(receipt_bytes(path)) != record["source_sha256"]
        ):
            raise ValueError("snapshot closure provenance receipt changed")
        receipt = json.loads(receipt_bytes(path))
        for name, expected in (
            ("owner-before.json", receipt["owner_sha256"]),
            ("migration-input.json", receipt["command"]["migration_sha256"]),
            ("owner-proposal.json", receipt["migration"]["owner_after_sha256"]),
        ):
            if _sha(private_bytes(path.parent / name)) != expected:
                raise ValueError("snapshot closure original input archive changed")
        if (
            receipt.get("schema") != SCHEMA
            or receipt["owner_path"] != str(store.config.path)
            or digest(receipt["command"]) != row["request_digest"]
            or row["command_id"] != "retained-history-" + receipt["command"]["command_id"]
        ):
            raise ValueError("snapshot closure has no exact public acknowledgement")
        for item in receipt["histories"]:
            if all(
                item[key] == record[key]
                for key in ("run_id", "workflow_id", "execution_run_id", "history_sha256")
            ):
                raw = private_bytes(
                    path.parent / "exports" / (item["run_id"] + ".json"), maximum=MAX_HISTORY
                )
                envelope = json.loads(
                    private_bytes(path.parent / "exports" / (item["run_id"] + ".envelope.json"))
                )
                if _sha(raw) != item["raw_sha256"] or envelope != item:
                    raise ValueError("snapshot closure immutable export changed")
                return
    raise ValueError("snapshot closure has no public capture acknowledgement")


def _capture(config, supplied):
    from .delivery_closures import insert_seal, prepare_seal
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_runtime_installation import destination, same_destination
    from .delivery_runtime_service_guard import check, describe, installation_lease
    from .delivery_store import DeliveryStore

    migration, inputs = _inputs(config, supplied)
    folder = config.state_root / "closure-captures" / supplied["command_id"]
    with installation_recovery_lease(config, exclusive=True), ExitStack() as leases:
        service = leases.enter_context(installation_lease(config, migration))
        if os.fstat(service).st_nlink != 1:
            raise ValueError("capture cannot create or consume an installation guard")
        with bootstrap(config, inputs):
            store = DeliveryStore(config, read_only=True)
            key = "retained-history-" + supplied["command_id"]
            with store._connect() as db:
                prior = db.execute(
                    "SELECT * FROM delivery_commands WHERE command_id=?", (key,)
                ).fetchone()
            if prior:
                response = _response(config, folder, supplied)
                if (
                    prior["request_digest"] != digest(supplied)
                    or json.loads(prior["response_json"]) != response
                ):
                    raise ValueError("capture replay differs from its public acknowledgement")
                for item in response["histories"]:
                    with store._connect() as db:
                        record = dict(
                            db.execute(
                                "SELECT * FROM delivery_closures WHERE workflow_id=? "
                                "AND execution_run_id=?",
                                (item["workflow_id"], item["execution_run_id"]),
                            ).fetchone()
                        )
                    verify_record(store, record)
                return {**response, "replayed": True}
            reader = _reader(config, supplied["reader"])
            revalidate_inputs = _pin_inputs(
                [
                    config.path,
                    Path(supplied["migration_path"]),
                    Path(migration["owner_after_path"]),
                    Path(supplied["reader"]["resolved_path"]),
                    Path(supplied["reader"]["resolved_path"]).parent.parent
                    / "INSTALL_RECEIPT.json",
                    Path(supplied["reader"]["resolved_path"]).parent.parent / ".brew/temporal.rb",
                    config.state_root / "temporal.log",
                    config.state_root / "service-stopped.json",
                ],
                leases,
            )
            authority = _collect(store, migration, supplied, leases)
            _folder(folder.parent)
            _folder(folder)
            _immutable(folder / "owner-before.json", inputs["before"])
            _immutable(
                folder / "migration-input.json", private_bytes(Path(supplied["migration_path"]))
            )
            _immutable(folder / "owner-proposal.json", inputs["after"])
            guard = describe(config, migration["command_id"], service)
            with SourceFamily(config.state_root) as source:
                intent = {
                    "schema": SCHEMA,
                    "command": supplied,
                    "migration": migration,
                    "owner_path": str(config.path),
                    "owner_sha256": _sha(inputs["before"]),
                    "reader": reader,
                    "authority": authority,
                    "source": source.initial,
                    "service_lock": guard,
                }
                intent_sha = _publish(folder / "intent.json", intent)
                source.snapshot(folder)

                def launch_guard():
                    check(config, guard, service, linked=False)
                    revalidate_inputs()
                    source.observe()  # Also proves the gated child inherited NO original FDs.
                    if _reader(config, supplied["reader"]) != reader:
                        raise ValueError("capture reader changed before GO")
                    with store._connect() as observed:
                        _current(store, authority, observed)

                exports, owned_reader = _serve(
                    config, store, authority, supplied, folder, intent_sha, launch_guard
                )
                source.observe()
                check(config, guard, service, linked=False)
                revalidate_inputs()
                if (
                    _reader(config, supplied["reader"]) != reader
                    or _sha(private_bytes(config.path)) != intent["owner_sha256"]
                ):
                    raise ValueError("capture original owner or configured reader changed")
                if not same_destination(
                    authority["destination"], destination(authority["runs"][0]["original_spec"])
                ):
                    raise ValueError("capture reviewed runtime source changed")
                _publish_exports(folder, exports)
                histories = [v["envelope"] for v in exports]
                receipt = {
                    **intent,
                    "histories": histories,
                    "reader_generation": owned_reader,
                    "cleanup": "confirmed",
                    "original_source_unchanged": True,
                }
                receipt_path = folder / "receipt.json"
                if receipt_path.exists():
                    previous = json.loads(receipt_bytes(receipt_path))
                    if digest(
                        {k: v for k, v in previous.items() if k != "reader_generation"}
                    ) != digest({k: v for k, v in receipt.items() if k != "reader_generation"}):
                        raise ValueError("capture immutable receipt differs from fresh authority")
                    prior_reader = Path(previous["reader_generation"])
                    if prior_reader.parent != folder / "readers":
                        raise ValueError("capture receipt reader left its owned generation")
                    if not (prior_reader / "authorized.json").exists():
                        raise ValueError(
                            "capture receipt reader never received durable authorization"
                        )
                    plan = _reader_authority(prior_reader)
                    if json.loads(private_bytes(prior_reader / "cleanup.json")) != {
                        "state": "confirmed",
                        "intent_sha256": digest(plan),
                    }:
                        raise ValueError("capture receipt has no exact original reader cleanup")
                    for envelope in previous["histories"]:
                        old_export = prior_reader / "exports" / (envelope["run_id"] + ".json")
                        if (
                            _sha(private_bytes(old_export, maximum=MAX_HISTORY))
                            != envelope["raw_sha256"]
                        ):
                            raise ValueError("capture original reader export bytes changed")
                    receipt = previous
                receipt_sha = _publish(receipt_path, receipt)
                records = [
                    prepare_seal(
                        store,
                        v["history"],
                        v["facts"],
                        run_id=v["envelope"]["run_id"],
                        source="temporal_snapshot",
                        source_sha256=receipt_sha,
                    )
                    for v in exports
                ]
                response = _response(config, folder, supplied)
                # The ONLY writable connection is this append-only metadata commit.
                # No normal Store bootstrap/migration or run projection is invoked.
                with closing(store.state.connect(config.tracking_db)) as db, db:
                    db.execute("BEGIN IMMEDIATE")
                    _current(store, authority, db)
                    source.observe()
                    check(config, guard, service, linked=False)
                    revalidate_inputs()
                    if private_bytes(config.path) != inputs["before"]:
                        raise ValueError("capture original owner changed before acknowledgement")
                    for record in records:
                        insert_seal(db, record, clear_observation=False)
                    db.execute(
                        "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                        (
                            key,
                            supplied["captures"][0]["run_id"],
                            digest(supplied),
                            canonical_json(response),
                        ),
                    )
                    for selected in supplied["captures"]:
                        row = next(
                            v["row"]
                            for v in authority["runs"]
                            if v["row"]["run_id"] == selected["run_id"]
                        )
                        db.execute(
                            "INSERT INTO delivery_events "
                            "(run_id,timestamp,type,message,run_revision,payload_json) "
                            "VALUES (?,?,?,?,?,?)",
                            (
                                row["run_id"],
                                store.state.now(),
                                EVENT,
                                "Captured authentic closed history from an owned data snapshot; "
                                "delivery unchanged",
                                row["revision"],
                                canonical_json(response),
                            ),
                        )
                return {**response, "replayed": False}


def capture(config, supplied):
    """Public standalone owner; no shared service/client runtime is modified."""
    with _sdk_receive_limit():
        return _capture(config, supplied)
