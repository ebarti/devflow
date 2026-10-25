"""Bounded immutable representation for the two stopped recovery owners.

The small root remains the acknowledged file. Parts carry complete canonical
authority, never permissions of their own. Generic private-file/history limits
are unchanged; this explicit representation has a separate finite total bound.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from contextlib import ExitStack
from pathlib import Path

from .contracts import canonical_json
from .delivery_filesystem_custody import _immutable, _publish_exclusive

SCHEMA = "devflow.stopped-runtime-authority-parts/v1"
CAPTURE_INTENT = "retained-closure-intent"
CAPTURE_RECEIPT = "retained-closure-receipt"
INSTALLATION_RECEIPT = "runtime-installation-receipt"
PART_BYTES = 1024 * 1024
MAX_PARTS = 64
MAX_BYTES = PART_BYTES * MAX_PARTS
INLINE_BYTES = 16 * 1024 * 1024
_KINDS = {
    CAPTURE_INTENT: ("closure-captures", "intent.json", "devflow.retained-closure-capture/v1"),
    CAPTURE_RECEIPT: ("closure-captures", "receipt.json", "devflow.retained-closure-capture/v1"),
    INSTALLATION_RECEIPT: (
        "runtime-installations",
        "receipt.json",
        "devflow-installed-runtime-location-v1",
    ),
}
_STAMP = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


def _stamp(info):
    return tuple(getattr(info, key) for key in _STAMP)


def _directory(path):
    info = path.lstat()
    if (
        path.resolve(strict=True) != path
        or not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or stat.S_IMODE(info.st_mode) != 0o700
    ):
        raise ValueError("runtime authority directory is not its private owned namespace")
    return info


def _namespace(path, kind):
    path = Path(path)
    if kind not in _KINDS:
        raise ValueError("unknown runtime authority owner")
    parent, name, _schema = _KINDS[kind]
    if (
        not path.is_absolute()
        or path.name != name
        or path.parent.parent.name != parent
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", path.parent.name)
    ):
        raise ValueError("runtime authority left its exact owner/document namespace")
    _directory(path.parent)
    return path, path.parent / (path.name + ".authority-parts")


def _logical(value, kind):
    if (
        not isinstance(value, dict)
        or value.get("schema") != _KINDS[kind][2]
        or any(not isinstance(key, str) for key in value)
    ):
        raise ValueError("runtime authority has another logical owner or schema")


def _chunks(value):
    """Canonical UTF-8, with bounded output pieces instead of one giant string."""
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    pending = bytearray()
    total = 0
    for token in encoder.iterencode(value):
        raw = token.encode("utf-8")
        total += len(raw)
        if total > MAX_BYTES:
            raise ValueError("runtime authority exceeds its explicit aggregate byte bound")
        offset = 0
        while offset < len(raw):
            count = min(PART_BYTES - len(pending), len(raw) - offset)
            pending.extend(memoryview(raw)[offset : offset + count])
            offset += count
            if len(pending) == PART_BYTES:
                yield bytes(pending)
                pending.clear()
    if pending:
        yield bytes(pending)


def _name(part):
    return f"{part['index']:04d}-{part['sha256']}.part"


def _fingerprint(value):
    whole = hashlib.sha256()
    total = 0
    for raw in _chunks(value):
        whole.update(raw)
        total += len(raw)
    return whole.hexdigest(), total


def _prefix(directory):
    """A crashed publication owns one bounded exact sequential prefix."""
    entries = {}
    with os.scandir(directory) as observed:
        for entry in observed:
            match = re.fullmatch(r"([0-9]{4})-([0-9a-f]{64})\.part", entry.name)
            if not match or len(entries) >= MAX_PARTS:
                raise ValueError("runtime authority has unregistered or excessive published parts")
            index = int(match[1])
            info = entry.stat(follow_symlinks=False)
            if (
                index >= MAX_PARTS
                or index in entries
                or not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or not 1 <= info.st_size <= PART_BYTES
            ):
                raise ValueError("runtime authority published prefix has changed index or custody")
            entries[index] = entry.name
    if set(entries) != set(range(len(entries))):
        raise ValueError("runtime authority published parts are not one exact prefix")
    return entries


def _part(directory, part, raw):
    """Keep crash staging outside the exact published-parts namespace."""
    from .delivery_runtime_location import private_bytes

    path = directory / _name(part)
    prefix = _prefix(directory)
    if part["index"] > len(prefix) or (
        part["index"] in prefix and prefix[part["index"]] != path.name
    ):
        raise ValueError("runtime authority occupied part index binds different immutable bytes")
    if os.path.lexists(path):
        if private_bytes(path, maximum=PART_BYTES) != raw:
            raise ValueError("runtime authority immutable part conflicts")
        return
    staging = directory.parent / ".authority-staging"
    if not staging.exists():
        staging.mkdir(mode=0o700)
    _directory(staging)
    staged = staging / (directory.name + "-" + path.name)
    _immutable(staged, raw)
    before = _stamp(staged.lstat())
    if private_bytes(staged, maximum=PART_BYTES) != raw or before != _stamp(staged.lstat()):
        raise ValueError("runtime authority staged part changed")
    try:
        _publish_exclusive(staged, path)
    except FileExistsError:
        # The exact existing publication must agree. A separate immutable stage
        # can remain inert; no abandoned stage is deleted or appointed authority.
        pass
    if private_bytes(path, maximum=PART_BYTES) != raw:
        raise ValueError("runtime authority published part changed")
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def publish(path, value, kind):
    """Publish an exact legacy small document or complete bounded part manifest.

    A failed old oversized root is never converted or overwritten. Unsupported
    totals can leave inert parts, but cannot publish an unreadable final root.
    """
    from .delivery_runtime_location import receipt_bytes

    path, directory = _namespace(path, kind)
    _logical(value, kind)
    if os.path.lexists(path):
        root_sha = hashlib.sha256(receipt_bytes(path)).hexdigest()
        existing = load(path, kind, expected_sha256=root_sha)
        if _fingerprint(existing) != _fingerprint(value):
            raise ValueError("runtime authority immutable document conflicts")
        if hashlib.sha256(receipt_bytes(path)).hexdigest() != root_sha:
            raise ValueError("runtime authority immutable root changed")
        return root_sha
    parts, buffered, whole = [], [], hashlib.sha256()
    total = 0

    def save(raw):
        if not directory.exists():
            directory.mkdir(mode=0o700)
        _directory(directory)
        part = {"index": len(parts), "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
        _part(directory, part, raw)
        parts.append(part)

    for raw in _chunks(value):
        total += len(raw)
        whole.update(raw)
        if total <= INLINE_BYTES and not parts:
            buffered.append(raw)
        else:
            for prior in buffered:
                save(prior)
            buffered.clear()
            save(raw)
    if not parts:
        if os.path.lexists(directory):
            _directory(directory)
            if _prefix(directory):
                raise ValueError("runtime authority inline document conflicts with published parts")
        root_raw = b"".join(buffered)
    else:
        if set(_prefix(directory).values()) != {_name(part) for part in parts}:
            raise ValueError("runtime authority has unregistered immutable parts")
        manifest = {
            "schema": SCHEMA,
            "kind": kind,
            "owner": str(path.parent),
            "document": path.name,
            "canonical_sha256": whole.hexdigest(),
            "canonical_bytes": total,
            "root_fields": sorted(value),
            "part_bytes": PART_BYTES,
            "part_count": len(parts),
            "parts": parts,
        }
        root_raw = canonical_json(manifest).encode()
    _immutable(path, root_raw)
    root_sha = hashlib.sha256(root_raw).hexdigest()
    # Complete interpretation is checked before a caller may use the root hash.
    load(path, kind, expected_sha256=root_sha)
    return root_sha


def load(path, kind, *, expected_sha256=None):
    """Authenticate every part and held generation before returning any facts."""
    path, directory = _namespace(path, kind)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    try:
        root_before = _stamp(os.fstat(descriptor))
        if root_before != _stamp(path.lstat()):
            raise ValueError("runtime authority root generation changed before observation")
        value = _load(path, directory, kind, expected_sha256, descriptor, root_before)
        if root_before != _stamp(os.fstat(descriptor)) or root_before != _stamp(path.lstat()):
            raise ValueError("runtime authority held root generation changed")
        return value
    finally:
        os.close(descriptor)


def _load(path, directory, kind, expected_sha256, root_descriptor, root_before):
    from .delivery_runtime_location import receipt_bytes

    raw = receipt_bytes(path)
    if expected_sha256 is not None and hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("runtime authority root differs from its acknowledged bytes")
    value = json.loads(raw)
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        _logical(value, kind)  # Legacy bounded full receipts retain their semantics.
        return value
    if (
        set(value)
        != {
            "schema",
            "kind",
            "owner",
            "document",
            "canonical_sha256",
            "canonical_bytes",
            "root_fields",
            "part_bytes",
            "part_count",
            "parts",
        }
        or canonical_json(value).encode() != raw
        or value["kind"] != kind
        or value["owner"] != str(path.parent)
        or value["document"] != path.name
        or value["part_bytes"] != PART_BYTES
        or type(value["canonical_bytes"]) is not int
        or not INLINE_BYTES < value["canonical_bytes"] <= MAX_BYTES
        or type(value["part_count"]) is not int
        or not 1 <= value["part_count"] <= MAX_PARTS
        or not isinstance(value["parts"], list)
        or len(value["parts"]) != value["part_count"]
        or not isinstance(value["canonical_sha256"], str)
        or not re.fullmatch(r"[0-9a-f]{64}", value["canonical_sha256"])
    ):
        raise ValueError("runtime authority manifest has incomplete or changed bounds")
    expected = []
    for index, part in enumerate(value["parts"]):
        if (
            not isinstance(part, dict)
            or set(part) != {"index", "bytes", "sha256"}
            or type(part["index"]) is not int
            or part["index"] != index
            or type(part["bytes"]) is not int
            or not 1 <= part["bytes"] <= PART_BYTES
            or index + 1 < len(value["parts"])
            and part["bytes"] != PART_BYTES
            or not isinstance(part["sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", part["sha256"])
        ):
            raise ValueError("runtime authority part order, hash or size changed")
        expected.append(_name(part))
    if sum(part["bytes"] for part in value["parts"]) != value["canonical_bytes"]:
        raise ValueError("runtime authority aggregate is incomplete")
    directory_before = _stamp(_directory(directory))
    if set(_prefix(directory).values()) != set(expected):
        raise ValueError("runtime authority has missing or unregistered parts")
    whole, content, held = hashlib.sha256(), bytearray(), []
    with ExitStack() as leases:
        directory_fd = os.open(
            directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        leases.callback(os.close, directory_fd)
        for part, name in zip(value["parts"], expected, strict=True):
            member = directory / name
            descriptor = os.open(member, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
            leases.callback(os.close, descriptor)
            info = os.fstat(descriptor)
            before = _stamp(info)
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
                or info.st_nlink != 1
                or info.st_size != part["bytes"]
            ):
                raise ValueError("runtime authority part is not its bounded private owned file")
            chunk = bytearray()
            while len(chunk) <= PART_BYTES:
                block = os.read(descriptor, min(128 * 1024, PART_BYTES + 1 - len(chunk)))
                if not block:
                    break
                chunk.extend(block)
            if len(chunk) != part["bytes"] or hashlib.sha256(chunk).hexdigest() != part["sha256"]:
                raise ValueError("runtime authority part bytes changed")
            whole.update(chunk)
            content.extend(chunk)
            held.append((descriptor, member, before))

        def current():
            if (
                root_before != _stamp(path.lstat())
                or root_before != _stamp(os.fstat(root_descriptor))
                or directory_before != _stamp(os.fstat(directory_fd))
                or directory_before != _stamp(directory.lstat())
                or any(
                    before != _stamp(os.fstat(fd)) or before != _stamp(name.lstat())
                    for fd, name, before in held
                )
            ):
                raise ValueError("runtime authority held generation changed")

        current()
        if (
            whole.hexdigest() != value["canonical_sha256"]
            or len(content) != value["canonical_bytes"]
        ):
            raise ValueError("runtime authority whole document changed")
        logical = json.loads(content)
        _logical(logical, kind)
        if sorted(logical) != value["root_fields"]:
            raise ValueError("runtime authority logical members changed")
        # The complete input has already passed the finite aggregate bound.
        # Reuse the existing exact canonical serializer for fresh equality;
        # the streaming initial publisher still refuses excessive totals.
        canonical = canonical_json(logical).encode()
        if (
            len(canonical) > MAX_BYTES
            or hashlib.sha256(canonical).hexdigest() != value["canonical_sha256"]
        ):
            raise ValueError("runtime authority logical bytes are not canonical")
        current()
        return logical
