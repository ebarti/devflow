"""Exact original service-lock custody across unmodified legacy adapters.

The known second name makes legacy lifecycle code fail before mutation. New
code recognizes that exception only through the cohort's immutable authority;
flock remains exclusive. No lock replacement, daemon holder or generic link
exception is involved.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path

from .delivery_runtime_location import (
    _acknowledged,
    _transport,
    pending_acknowledged,
    private_bytes,
    receipt_bytes,
    reversed_reference,
    source_root,
)


class GuardCustodyError(ValueError):
    """An acknowledged guard lost its exact original two-name custody."""


def _identity(path, descriptor):
    if path.resolve(strict=True) != path:
        raise ValueError("installation service lock has a linked ancestor")
    info, named = os.fstat(descriptor), path.lstat()
    if (
        not stat.S_ISREG(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o600
        or info.st_nlink not in {1, 2}
        or (info.st_dev, info.st_ino) != (named.st_dev, named.st_ino)
        or info.st_size > 65536
    ):
        raise ValueError("installation service lock is not its original private owned file")
    from .delivery_filesystem_custody import _volume

    volume = _volume(descriptor) if sys.platform == "darwin" else None
    os.lseek(descriptor, 0, os.SEEK_SET)
    raw = os.read(descriptor, 65537)
    after = os.fstat(descriptor)
    keys = ("st_dev", "st_ino", "st_uid", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
    if len(raw) != info.st_size or any(getattr(info, key) != getattr(after, key) for key in keys):
        raise ValueError("installation original service lock changed during observation")
    return {
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "mode": stat.S_IMODE(info.st_mode),
        "birthtime": getattr(info, "st_birthtime", None),
        "volume_uuid": volume,
        "sha256": hashlib.sha256(raw).hexdigest(),
    }


def _same(expected, current):
    actual = dict(current)
    if actual["device"] != expected["device"]:
        if (
            not expected.get("volume_uuid")
            or expected["volume_uuid"] != actual.get("volume_uuid")
            or expected.get("birthtime") is None
            or expected["birthtime"] != actual.get("birthtime")
        ):
            return False
        actual["device"] = expected["device"]
    return expected == actual


def describe(config, command, descriptor):
    path = config.state_root / "service.lock"
    link = config.state_root / "runtime-installations" / command / "service-lock.guard"
    return {"path": str(path), "link": str(link), "identity": _identity(path, descriptor)}


def check(config, proof, descriptor, *, linked):
    path, link = Path(proof["path"]), Path(proof["link"])
    if path != config.state_root / "service.lock" or (
        not link.is_relative_to(config.state_root / "runtime-installations")
        or link.name != "service-lock.guard"
    ):
        raise ValueError("installation service guard left its original owner namespace")
    if not _same(proof["identity"], _identity(path, descriptor)):
        raise ValueError("installation original service lock generation changed")
    info = os.fstat(descriptor)
    if linked:
        parent = link.parent.lstat()
        named = link.lstat()
        if (
            link.resolve(strict=True) != link
            or not stat.S_ISDIR(parent.st_mode)
            or parent.st_uid != os.getuid()
            or stat.S_IMODE(parent.st_mode) != 0o700
            or info.st_nlink != 2
            or (named.st_dev, named.st_ino, named.st_uid, named.st_mode)
            != (info.st_dev, info.st_ino, info.st_uid, info.st_mode)
        ):
            raise ValueError("installation service guard is not its exact two owned names")
    elif info.st_nlink != 1 or os.path.lexists(link):
        raise ValueError("installation service lock acquired an unacknowledged additional name")


def observe(config, receipt):
    """Read active custody only; never create a name, take EX or run an FD census."""
    try:
        proof = receipt["service_guard"]
        descriptor = os.open(proof["path"], os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            check(config, proof, descriptor, linked=True)
        finally:
            os.close(descriptor)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        raise GuardCustodyError("acknowledged service guard custody changed") from exc


def census(descriptor, proof):
    """Count every inode-wide FD, including a thread paused before first flock."""
    result = subprocess.run(
        ["/usr/sbin/lsof", "-nP", "-F0pfnDi", "--", proof["path"]], capture_output=True, timeout=10
    )
    if result.returncode != 0 or result.stderr.strip():
        raise ValueError("installation foreign service-lock FD visibility is indeterminate")
    process, current, records = None, None, []
    for raw in result.stdout.split(b"\0"):
        field = raw.strip(b"\n")
        if not field:
            continue
        key, value = chr(field[0]), field[1:].decode("utf-8", errors="strict")
        if key == "p":
            if not value.isdigit():
                raise ValueError("installation service-lock FD inventory has no process identity")
            process = int(value)
        elif key == "f":
            current = {"pid": process, "fd": value}
            records.append(current)
        elif key in {"D", "i"}:
            if current is None:
                raise ValueError("installation service-lock FD inventory is incomplete")
            current[key] = value
    expected = os.fstat(descriptor)
    owned = []
    for item in records:
        try:
            identity = int(item["D"], 16), int(item["i"])
        except (KeyError, ValueError) as exc:
            raise ValueError(
                "installation service-lock inode-wide FD inventory is incomplete"
            ) from exc
        if (
            identity != (expected.st_dev, expected.st_ino)
            or item["pid"] is None
            or not item["fd"].isdigit()
        ):
            raise ValueError("installation service-lock FD inventory is ambiguous")
        if (item["pid"], int(item["fd"])) == (os.getpid(), descriptor):
            owned.append(item)
    if len(owned) != 1:
        raise ValueError("installation service-lock FD census cannot see its exact own descriptor")
    foreign = [item for item in records if item not in owned]
    if foreign:
        raise ValueError(
            "unfinished installation: foreign service-lock FDs remain; no further "
            "relocation effect is authorized and prevalidated legacy lifecycle may still act"
        )


def install(config, receipt, reference, descriptor):
    """ALL cohort PENDING is durable before this first real lifecycle guard effect."""
    if not pending_acknowledged(config, receipt, reference):
        raise ValueError("service guard has no complete original pending cohort")
    proof = receipt["service_guard"]
    link = Path(proof["link"])
    if not os.path.lexists(link):
        check(config, proof, descriptor, linked=False)
        os.link(proof["path"], link, follow_symlinks=False)
        directory = os.open(link.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    check(config, proof, descriptor, linked=True)
    census(descriptor, proof)


def _reference(config):
    for name in ("current.json", "pending.json"):
        path = config.state_root / "runtime-installations" / name
        if os.path.lexists(path):
            return json.loads(private_bytes(path))
    if config.tracking_db.exists():
        from .delivery_runtime_location import PENDING

        with sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            if not db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='delivery_events'"
            ).fetchone():
                return None  # A legacy pre-delivery database cannot contain a guard ACK.
            for row in db.execute(
                "SELECT payload_json FROM delivery_events WHERE type=? ORDER BY sequence DESC",
                (PENDING,),
            ):
                value = json.loads(row[0])
                path = Path(value["receipt"])
                if path.is_relative_to(config.state_root / "runtime-installations"):
                    return {
                        "command_id": path.parent.name,
                        "receipt": str(path),
                        "receipt_sha256": value["receipt_sha256"],
                    }
    return None


def _receipt(config, *, stop=False, reference=None):
    reference = reference or _reference(config)
    if reference is None:
        return None
    raw = receipt_bytes(Path(reference["receipt"]))
    if hashlib.sha256(raw).hexdigest() != reference["receipt_sha256"]:
        raise ValueError("installation service guard receipt changed")
    receipt = json.loads(raw)
    if (
        receipt["owner_path"] != str(config.path)
        or receipt["transport"] != _transport(config)
        or receipt["destination"]["root"] != str(source_root())
    ):
        raise ValueError("installation service guard belongs to another original owner or program")
    try:
        active = _acknowledged(config, reference=reference)
    except ValueError:
        if not stop:
            raise
        active = None  # An exact beforeimage is permitted only for stopped recovery below.
    if active is None:
        if not stop or not pending_acknowledged(config, receipt, reference):
            raise ValueError("pending installation service guard does not authorize startup")
        if hashlib.sha256(private_bytes(config.path)).hexdigest() not in {
            receipt["owner_before_sha256"],
            receipt["owner_after_sha256"],
        }:
            raise ValueError("pending installation original owner changed")
        with sqlite3.connect(config.tracking_db.as_uri() + "?mode=ro", uri=True) as db:
            db.execute("PRAGMA query_only=ON")
            for item in receipt["runs"]:
                row = db.execute(
                    "SELECT request_digest,request_json FROM delivery_runs WHERE run_id=?",
                    (item["run_id"],),
                ).fetchone()
                if (
                    not row
                    or row[0] != item["request_digest"]
                    or hashlib.sha256(row[1].encode()).hexdigest() != item["request_json_sha256"]
                ):
                    raise ValueError(
                        "pending installation service guard lost original cohort authority"
                    )
    from .delivery_runtime_location import validate_destination

    validate_destination(
        receipt["destination"], receipt["runs"][0]["original_spec"], strict_source=False,
        installed_identity=receipt["runs"][0].get("installed_native_identity"),
    )
    return receipt


@contextmanager
def installation_lease(config, supplied):
    """Same original EX flock as legacy start, including attributable pending replay."""
    path = config.state_root / "service.lock"
    receipt_path = (
        config.state_root / "runtime-installations" / supplied["command_id"] / "receipt.json"
    )

    def committed_guard():
        if not receipt_path.exists():
            return None
        reference = {
            "command_id": supplied["command_id"],
            "receipt": str(receipt_path),
            "receipt_sha256": hashlib.sha256(receipt_bytes(receipt_path)).hexdigest(),
        }
        if _acknowledged(
            config, historical=True, reference=reference
        ) is None and not reversed_reference(config, reference):
            return None
        # Historical proof permits data inspection; every effectful recovery
        # still needs the committed original guard, even when nlink became 1.
        return _receipt(config, stop=True, reference=reference)

    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        proof = describe(config, supplied["command_id"], descriptor)
        committed = committed_guard()
        linked = committed is not None or os.fstat(descriptor).st_nlink == 2
        if linked:
            receipt = committed or _receipt(
                config,
                stop=True,
                reference={
                    "command_id": supplied["command_id"],
                    "receipt": str(receipt_path),
                    "receipt_sha256": hashlib.sha256(receipt_bytes(receipt_path)).hexdigest(),
                },
            )
            if receipt is None or receipt["command"] != supplied:
                raise ValueError("service lock has no exact pending installation owner")
            proof = receipt["service_guard"]
            check(config, proof, descriptor, linked=True)
        else:
            check(config, proof, descriptor, linked=False)
        from .delivery_control import _deadline, _remaining

        deadline = _deadline(config)
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.05, _remaining(deadline)))
        check(config, proof, descriptor, linked=linked)
        yield descriptor
        if linked or os.path.lexists(Path(proof["link"])) or committed_guard() is not None:
            check(config, proof, descriptor, linked=True)
            census(descriptor, proof)
    finally:
        os.close(descriptor)


@contextmanager
def recognized_lifecycle(config, deadline, *, stop=False):
    """No generic multi-link allowance: require this exact committed two-name owner."""
    path = config.state_root / "service.lock"
    reference = _reference(config)
    if reference is None:
        if not path.exists() or path.lstat().st_nlink == 1:
            yield False
            return
        raise ValueError("service lock additional name has no acknowledged installation authority")
    receipt = _receipt(config, stop=stop, reference=reference)
    if receipt is None:
        raise ValueError("service lock additional name has no acknowledged installation authority")

    def linked_mode(current):
        # A genuine PENDING crash before the first link may still be stopped,
        # using its recorded original inode. It grants no native/start authority.
        if (
            stop
            and path.exists()
            and path.lstat().st_nlink == 1
            and not os.path.lexists(Path(current["service_guard"]["link"]))
            and _acknowledged(config, historical=True, reference=reference) is None
            and not reversed_reference(config, reference)
        ):
            from .delivery_runtime_registrations import sealed_objects, validate

            sealed_objects(current)
            validate(current["registrations"], before=True, objects=current["installed_objects"])
            return False
        return True

    descriptor = os.open(path, os.O_RDWR | os.O_NONBLOCK | os.O_NOFOLLOW)
    try:
        proof = receipt["service_guard"]
        check(config, proof, descriptor, linked=linked_mode(receipt))
        from .delivery_control import _remaining

        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                time.sleep(min(0.05, _remaining(deadline)))
        # Authority may have changed while this descriptor waited for EX.
        renewed = _receipt(config, stop=stop, reference=reference)
        if renewed is None or renewed["service_guard"] != proof:
            raise ValueError("installation service guard authority changed while waiting")
        linked = linked_mode(renewed)
        check(config, proof, descriptor, linked=linked)
        try:
            active = _acknowledged(config)
        except ValueError:
            if not stop:
                raise
            active = None
        if active is None:
            census(descriptor, proof)  # A pending stop must not overclaim an old waiter settled.
        yield True
        check(config, proof, descriptor, linked=linked)
    finally:
        os.close(descriptor)
