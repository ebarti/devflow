"""Owned protocol/evidence bounds; no remote CI execution or success assertion."""

from __future__ import annotations

import json
import os
import sys
import time

import pytest

from devflow_temporal import delivery_ci_inventory_io as io


def test_exact_hash_publication_and_crash_prefix_replay(tmp_path, monkeypatch):
    root, raw = tmp_path / "observed", b"complete immutable source bytes"
    link = io.os.link
    monkeypatch.setattr(
        io.os,
        "link",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("owned crash after staging fsync")),
    )
    with pytest.raises(RuntimeError, match="owned crash"):
        io.publish(root, raw, limit=1024)
    stage = root / (".stage-" + io.sha256(raw))
    assert stage.read_bytes() == raw
    monkeypatch.setattr(io.os, "link", link)
    result = io.publish(root, raw, limit=1024)
    assert not stage.exists() and len(list(root.iterdir())) == 1
    assert io.publish(root, raw, limit=1024) == result
    with io.owned_bytes(result["path"], 1024, expected=result["sha256"]) as observed:
        assert observed == raw


@pytest.mark.parametrize("change", ["symlink", "hardlink", "fifo", "mode", "oversize"])
def test_private_raw_evidence_never_follows_or_accepts_changed_objects(tmp_path, change):
    path = tmp_path / "observed"
    path.write_bytes(b"source")
    path.chmod(0o600)
    if change == "symlink":
        real = path.with_name("real")
        path.rename(real)
        path.symlink_to(real)
    elif change == "hardlink":
        os.link(path, path.with_name("alias"))
    elif change == "fifo":
        path.unlink()
        os.mkfifo(path, 0o600)
    elif change == "mode":
        path.chmod(0o644)
    began = time.monotonic()
    with pytest.raises((ValueError, OSError)):
        with io.owned_bytes(path, 2 if change == "oversize" else 1024):
            pytest.fail("invalid evidence reached decode")
    assert time.monotonic() - began < 1


@pytest.mark.parametrize("change", [None, "truncated", "count", "duplicate", "overbound"])
def test_complete_bounded_pages_are_joined_without_truncation(change):
    calls = []

    def run(command, _limit, _remaining):
        endpoint = next(x for x in command if x.startswith("repos/"))
        calls.append(endpoint)
        page = 1 if "page=1&" in endpoint else 2
        ids = list(range(1, 101)) if page == 1 else [101]
        total = 101
        if change == "truncated":
            ids = ids[:3]
        elif change == "count" and page == 2:
            total = 102
        elif change == "duplicate" and page == 2:
            ids = [1]
        elif change == "overbound":
            total = 1001
        body = json.dumps({"total_count": total, "jobs": [{"id": i} for i in ids]}).encode()
        return b"HTTP/2.0 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body

    reader = io.GitHubRead(repositories={"example/fixture"}, runner=run)
    if change:
        with pytest.raises(ValueError):
            reader.pages("repos/example/fixture/actions/runs/1/jobs", "jobs", head="a" * 40)
    else:
        assert (
            len(reader.pages("repos/example/fixture/actions/runs/1/jobs", "jobs", head="a" * 40))
            == 101
        )
        assert len(calls) == 2 and all("head_sha=" + "a" * 40 in p for p in calls)


@pytest.mark.parametrize("mode", ["bytes", "deadline"])
def test_real_owned_transport_child_is_bounded_and_reaped(mode):
    code = "import os;os.write(1,b'x'*100000)" if mode == "bytes" else "import time;time.sleep(2)"
    reader = io.GitHubRead(repositories={"example/fixture"}, timeout=1 if mode == "bytes" else 0.05)
    began = time.monotonic()
    with pytest.raises(ValueError, match="bound|deadline"):
        reader._bytes([sys.executable, "-I", "-c", code], 1024)
    assert time.monotonic() - began < 1.5
