"""Complete owner evidence survives bounded representation and exact replay."""

from __future__ import annotations

import hashlib
import json
import os

import pytest

from devflow_temporal import delivery_runtime_authority as owner
from devflow_temporal import delivery_runtime_location as location
from devflow_temporal.contracts import canonical_json, digest


def namespace(root, kind=owner.CAPTURE_INTENT):
    parent, name, schema = owner._KINDS[kind]
    path = root / parent / "owned-command" / name
    path.parent.mkdir(parents=True, mode=0o700)
    return path, {"schema": schema}


def replace(path, value):
    path.write_bytes(canonical_json(value).encode())
    path.chmod(0o600)


@pytest.fixture
def small(tmp_path, monkeypatch):
    # Exercise the exact representation arithmetic without copying32MiB for
    # each malformed manifest. One separate case uses all real production caps.
    monkeypatch.setattr(owner, "INLINE_BYTES", 256)
    monkeypatch.setattr(owner, "PART_BYTES", 128)
    monkeypatch.setattr(owner, "MAX_BYTES", 128 * owner.MAX_PARTS)
    path, value = namespace(tmp_path)
    value["actors"] = [[i, "Sat Oct 24 14:20:35 2026"] for i in range(70)]
    owner.publish(path, value, owner.CAPTURE_INTENT)
    return path, value


def test_real_complete_authority_above_observed_size_and_both_document_owners(tmp_path):
    # The production failure is dominated by full row/recovery/table evidence,
    # with another4.7MiB of actor/journal authority. Retain those actual shapes;
    # no unused padding field makes this document large.
    histories = [
        {
            "command_id": f"finite-continuation-{i}",
            "request_digest": digest(["request", i]),
            "candidate": {"head": digest(["head", i]), "content": digest(["content", i])},
            "source_paths": [f"apps/web/src/feature-{i}/{j}.ts" for j in range(24)],
        }
        for i in range(650)
    ]
    runs = []
    for i in range(6):
        frozen = {
            "run_id": f"run-{i}",
            "policy": {"capacity": 6, "browser_read_roots": [] if i < 2 else ["/owned/browser"]},
            "admissions": histories,
        }
        rows = {
            "request_json": canonical_json(frozen),
            "recovery_json": canonical_json({"prior": frozen, "current": frozen}),
            "checks_json": canonical_json({"closed": frozen, "original": frozen}),
        }
        runs.append({"row": rows, "original_spec": frozen, "historical_specs": [frozen]})
    global_native = {
        "actors": [[700000 + i, f"Sat Oct 24 {i % 24:02}:20:35 2026"] for i in range(113498)],
        "journals": {f"/owned/state/{i}/native-process.json": digest(i) for i in range(2527)},
    }
    path, value = namespace(tmp_path)
    value["authority"] = {"runs": runs, "global_native": global_native}
    raw_size = sum(map(len, owner._chunks(value)))
    assert 30571522 < raw_size < owner.MAX_BYTES
    logical_sha = digest(value)
    first = owner.publish(path, value, owner.CAPTURE_INTENT)
    manifest = json.loads(location.receipt_bytes(path))
    assert manifest["canonical_bytes"] == raw_size
    assert manifest["canonical_sha256"] == logical_sha
    assert path.stat().st_size < 16384
    restored = owner.load(path, owner.CAPTURE_INTENT, expected_sha256=first)
    assert restored["authority"]["global_native"]["actors"][-1] == global_native["actors"][-1]
    assert digest(restored) == logical_sha
    assert owner.publish(path, value, owner.CAPTURE_INTENT) == first
    # Installation has the same large complete shape and a distinct typed root.
    target, installed = namespace(tmp_path, owner.INSTALLATION_RECEIPT)
    installed.update(value["authority"])
    root_sha = owner.publish(target, installed, owner.INSTALLATION_RECEIPT)
    assert digest(
        owner.load(target, owner.INSTALLATION_RECEIPT, expected_sha256=root_sha)
    ) == digest(installed)
    assert location.RECEIPT_LIMIT == 16 * 1024 * 1024
    with pytest.raises(ValueError, match="bounded private"):
        # The original generic private4MiB reader is still strict.
        legacy = tmp_path / "oversized-original.json"
        legacy.write_bytes(b"x" * (4 * 1024 * 1024 + 1))
        legacy.chmod(0o600)
        location.private_bytes(legacy)


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "corrupt",
        "reordered",
        "duplicate",
        "unknown-schema",
        "wrong-owner",
        "wrong-kind",
        "bytes",
        "count",
        "whole-sha",
        "fields",
        "extra",
    ],
)
def test_incomplete_or_changed_representation_never_yields_facts(small, defect):
    path, _value = small
    root = json.loads(path.read_bytes())
    folder = path.parent / (path.name + ".authority-parts")
    part = folder / owner._name(root["parts"][-1])
    if defect == "missing":
        part.unlink()
    elif defect == "corrupt":
        part.write_bytes(b"!" + part.read_bytes()[1:])
    elif defect == "extra":
        (folder / "unregistered.part").write_bytes(b"foreign")
    else:
        if defect == "reordered":
            root["parts"].reverse()
        elif defect == "duplicate":
            root["parts"][-1] = root["parts"][0]
        elif defect == "unknown-schema":
            root["schema"] += "-unknown"
        elif defect == "wrong-owner":
            root["owner"] += "-other"
        elif defect == "wrong-kind":
            root["kind"] = owner.INSTALLATION_RECEIPT
        elif defect == "bytes":
            root["canonical_bytes"] += 1
        elif defect == "count":
            root["part_count"] += 1
        elif defect == "whole-sha":
            root["canonical_sha256"] = "0" * 64
        else:
            root["root_fields"] = ["invented"]
        replace(path, root)
    with pytest.raises((ValueError, FileNotFoundError)):
        owner.load(path, owner.CAPTURE_INTENT)


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "mode"])
def test_parts_require_private_regular_single_name_custody(small, tmp_path, kind):
    path, _value = small
    root = json.loads(path.read_bytes())
    part = path.parent / (path.name + ".authority-parts") / owner._name(root["parts"][-1])
    original = part.read_bytes()
    other = tmp_path / "foreign"
    other.write_bytes(original)
    other.chmod(0o600)
    if kind == "mode":
        part.chmod(0o644)
    else:
        part.unlink()
        if kind == "symlink":
            part.symlink_to(other)
        elif kind == "hardlink":
            os.link(other, part)
        else:
            os.mkfifo(part, 0o600)
    with pytest.raises((ValueError, OSError)):
        owner.load(path, owner.CAPTURE_INTENT)


def test_held_parts_detect_restored_mtime_and_replacement_during_final_decode(small, monkeypatch):
    path, _value = small
    original = owner.json.loads
    manifest = original(path.read_bytes())
    part = path.parent / (path.name + ".authority-parts") / owner._name(manifest["parts"][0])
    stamp = part.stat()

    def changed(raw):
        result = original(raw)
        if isinstance(raw, bytearray):
            content = part.read_bytes()
            part.write_bytes(content)
            os.utime(part, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
        return result

    monkeypatch.setattr(owner.json, "loads", changed)
    with pytest.raises(ValueError, match="held generation"):
        owner.load(path, owner.CAPTURE_INTENT)


def test_exact_upper_boundary_and_overflow_before_final_root(tmp_path, monkeypatch):
    monkeypatch.setattr(owner, "INLINE_BYTES", 256)
    monkeypatch.setattr(owner, "PART_BYTES", 128)
    monkeypatch.setattr(owner, "MAX_BYTES", 1024)
    path, value = namespace(tmp_path)
    overhead = len(canonical_json({**value, "data": ""}).encode())
    value["data"] = "a" * (1024 - overhead)
    owner.publish(path, value, owner.CAPTURE_INTENT)
    assert owner.load(path, owner.CAPTURE_INTENT) == value
    target, other = namespace(tmp_path / "overflow")
    other["data"] = value["data"] + "x"
    with pytest.raises(ValueError, match="aggregate byte bound"):
        owner.publish(target, other, owner.CAPTURE_INTENT)
    assert not target.exists()


def test_part_publication_crash_is_inert_and_exact_replay_completes(tmp_path, monkeypatch):
    monkeypatch.setattr(owner, "INLINE_BYTES", 256)
    monkeypatch.setattr(owner, "PART_BYTES", 128)
    monkeypatch.setattr(owner, "MAX_BYTES", 8192)
    path, value = namespace(tmp_path)
    value["actors"] = [[i, "Sat Oct 24 14:20:35 2026"] for i in range(70)]
    immutable = owner._part
    count = 0

    def crash(directory, part, raw):
        nonlocal count
        immutable(directory, part, raw)
        count += 1
        if count == 3:
            raise RuntimeError("owned crash after immutable part fsync")

    monkeypatch.setattr(owner, "_part", crash)
    with pytest.raises(RuntimeError, match="owned crash"):
        owner.publish(path, value, owner.CAPTURE_INTENT)
    assert not path.exists()
    assert len(list(path.parent.glob("*.authority-parts/*.part"))) == 3
    monkeypatch.setattr(owner, "_part", immutable)
    expected = owner.publish(path, value, owner.CAPTURE_INTENT)
    assert owner.load(path, owner.CAPTURE_INTENT, expected_sha256=expected) == value
    with pytest.raises(ValueError, match="acknowledged bytes"):
        owner.load(path, owner.CAPTURE_INTENT, expected_sha256="0" * 64)


def test_legacy_bounded_and_failed_oversized_immutable_roots_are_never_converted(tmp_path):
    path, value = namespace(tmp_path)
    original = (canonical_json(value) + "\n").encode()
    path.write_bytes(original)
    path.chmod(0o600)
    assert owner.publish(path, value, owner.CAPTURE_INTENT) == hashlib.sha256(original).hexdigest()
    assert path.read_bytes() == original
    target, _other = namespace(tmp_path / "failed")
    oversized = b"x" * (location.RECEIPT_LIMIT + 1)
    target.write_bytes(oversized)
    target.chmod(0o600)
    with pytest.raises(ValueError, match="bounded private"):
        owner.publish(target, value, owner.CAPTURE_INTENT)
    assert target.read_bytes() == oversized
    assert not target.with_name(target.name + ".authority-parts").exists()


def test_crashed_atomic_staging_is_inert_outside_complete_part_namespace(tmp_path, monkeypatch):
    monkeypatch.setattr(owner, "INLINE_BYTES", 256)
    monkeypatch.setattr(owner, "PART_BYTES", 128)
    monkeypatch.setattr(owner, "MAX_BYTES", 8192)
    path, value = namespace(tmp_path)
    value["actors"] = [[i, "Sat Oct 24 14:20:35 2026"] for i in range(70)]
    staging = path.parent / ".authority-staging"
    staging.mkdir(mode=0o700)
    abandoned = staging / ".custody-owned-interrupted-stage"
    abandoned.write_bytes(b"partial unacknowledged staging")
    abandoned.chmod(0o600)
    owner.publish(path, value, owner.CAPTURE_INTENT)
    assert owner.load(path, owner.CAPTURE_INTENT) == value
    assert abandoned.read_bytes() == b"partial unacknowledged staging"
