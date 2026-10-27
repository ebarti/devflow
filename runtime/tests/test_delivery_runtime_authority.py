"""Complete owner evidence survives bounded representation and exact replay."""

from __future__ import annotations

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


def test_name_replacement_during_actual_root_fd_read_refuses(tmp_path, monkeypatch):
    path, value = namespace(tmp_path)
    replace(path, value)
    reader = owner.os.read
    swapped = []

    def changed(descriptor, size):
        raw = reader(descriptor, size)
        if not swapped:
            replacement = path.with_name("different-root.json")
            replace(replacement, {**value, "different": True})
            os.replace(replacement, path)
            swapped.append(True)
        return raw

    monkeypatch.setattr(owner.os, "read", changed)
    with pytest.raises(ValueError, match="held root changed during"):
        owner.load(path, owner.CAPTURE_INTENT)
    assert swapped
