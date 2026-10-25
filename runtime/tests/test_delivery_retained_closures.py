"""Bounded snapshot and export custody, using only this test's owned data."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import subprocess
import sys
import time
import uuid
from contextlib import closing
from types import SimpleNamespace

import pytest
from temporalio.api.history.v1 import HistoryEvent

from devflow_temporal import delivery_retained_closures as capture
from devflow_temporal.delivery_resources import write_private


def source(tmp_path, *, wal=False):
    root = tmp_path / "original"
    root.mkdir(mode=0o700)
    database = root / "temporal.sqlite3"
    namespace = uuid.uuid4()
    code = """
import os,sqlite3,sys,uuid
db=sqlite3.connect(sys.argv[1]);db.execute('PRAGMA journal_mode=WAL')
db.execute('CREATE TABLE namespaces(id BLOB,name TEXT)')
db.execute('CREATE TABLE probe(value TEXT)')
db.execute('INSERT INTO namespaces VALUES (?,?)',(uuid.UUID(sys.argv[2]).bytes,'default'))
db.commit();db.execute('PRAGMA wal_checkpoint(TRUNCATE)')
db.execute("INSERT INTO probe VALUES ('committed WAL')");db.commit()
if sys.argv[3]=='wal':os._exit(0)
db.close()
"""
    subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-c",
            code,
            str(database),
            str(namespace),
            "wal" if wal else "closed",
        ],
        check=True,
    )
    return root, database, namespace


def bytes_of(root):
    return {p.name: p.read_bytes() for p in root.iterdir() if p.is_file()}


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
@pytest.mark.parametrize("wal", [False, True])
def test_consistent_raw_snapshot_recovers_only_owned_copy_without_reusing_shm(tmp_path, wal):
    root, database, namespace = source(tmp_path, wal=wal)
    before = bytes_of(root)
    if wal:
        assert (root / "temporal.sqlite3-wal").stat().st_size > 0
        assert "temporal.sqlite3-shm" in before
    folder = tmp_path / "capture"
    folder.mkdir(mode=0o700)
    with capture.SourceFamily(root) as observed:
        observed.snapshot(folder)
        working, actual_namespace = capture._working(folder, "default")
        assert actual_namespace == str(namespace)
        with closing(sqlite3.connect(working)) as copy:
            assert copy.execute("SELECT value FROM probe").fetchall() == [("committed WAL",)]
        assert observed.observe() == observed.initial
        observed.snapshot(folder)  # Exact master reuse; no source SQLite access.
    assert bytes_of(root) == before
    assert all((folder / "master" / name).read_bytes() == raw for name, raw in before.items())
    assert database.is_file()
    if wal:
        assert (folder / "master/temporal.sqlite3-shm").read_bytes() == before[
            "temporal.sqlite3-shm"
        ]
        assert not (folder / "working/raw.sqlite3-shm").exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
@pytest.mark.parametrize("defect", ["journal", "unknown", "hardlink", "symlink", "fifo", "mode"])
def test_snapshot_refuses_hot_or_unowned_original_family(tmp_path, defect):
    root, database, _namespace = source(tmp_path)
    if defect == "journal":
        (root / "temporal.sqlite3-journal").write_bytes(b"hot")
    elif defect == "unknown":
        (root / "temporal.sqlite3-mj-unknown").write_bytes(b"unknown")
    elif defect == "hardlink":
        os.link(database, tmp_path / "second-name")
    elif defect == "symlink":
        original = tmp_path / "elsewhere"
        database.rename(original)
        database.symlink_to(original)
    elif defect == "fifo":
        database.unlink()
        os.mkfifo(database, 0o600)
    else:
        database.chmod(0o666)
    started = time.monotonic()
    with pytest.raises((ValueError, OSError)):
        with capture.SourceFamily(root):
            raise AssertionError("Unsafe original data cannot be observed")
    assert time.monotonic() - started < 3


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
def test_original_fd_census_excludes_only_exact_owned_descriptors(tmp_path):
    root, database, _ = source(tmp_path)
    # A second FD in this SAME process must refuse, even if it is read-only.
    with database.open("rb"), pytest.raises(ValueError, match="foreign FD"):
        with capture.SourceFamily(root):
            pass
    with capture.SourceFamily(root) as observed:
        with database.open("rb"), pytest.raises(ValueError, match="foreign FD"):
            observed.observe()


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
@pytest.mark.parametrize("defect", ["content", "restored-mtime", "replace", "new-wal", "master"])
def test_snapshot_revalidates_original_and_master_generations(tmp_path, defect):
    root, database, _ = source(tmp_path)
    folder = tmp_path / "capture"
    folder.mkdir(mode=0o700)
    with capture.SourceFamily(root) as observed:
        observed.snapshot(folder)
        if defect in {"content", "restored-mtime"}:
            info = database.stat()
            with database.open("r+b") as stream:
                stream.seek(200)
                stream.write(b"changed")
            if defect == "restored-mtime":
                os.utime(database, ns=(info.st_atime_ns, info.st_mtime_ns))
        elif defect == "replace":
            raw = database.read_bytes()
            database.unlink()
            database.write_bytes(raw)
        elif defect == "new-wal":
            (root / "temporal.sqlite3-wal").write_bytes(b"unexpected generation")
        else:
            (folder / "master/temporal.sqlite3").write_bytes(b"changed master")
        with pytest.raises(ValueError, match="changed|generation"):
            observed.snapshot(folder)


class Pages:
    def __init__(self, pages, *, repeated=False, slow=False):
        self.pages, self.index, self.repeated, self.slow = pages, 0, repeated, slow
        self.current_page = None
        self.next_page_token = None

    async def fetch_next_page(self):
        if self.slow:
            await asyncio.sleep(10)
        self.current_page = self.pages[self.index]
        self.index += 1
        self.next_page_token = (
            (b"repeated" if self.repeated else str(self.index).encode())
            if (self.index < len(self.pages))
            else None
        )


def handle(pages):
    def iterator(**kwargs):
        assert kwargs["page_size"] == 128
        assert kwargs["event_filter_type"].name == "ALL_EVENT"
        return pages

    return SimpleNamespace(id="owned-workflow", fetch_history_events=iterator)


@pytest.mark.asyncio
async def test_sdk_export_consumes_all_pages_and_preserves_workflow_envelope_subject():
    pages = Pages([[HistoryEvent(event_id=1)], [HistoryEvent(event_id=2)]])
    budget = {"pages": 0, "events": 0, "bytes": 0}
    history, raw = await capture._history(handle(pages), time.monotonic() + 3, budget)
    assert history.workflow_id == "owned-workflow" and len(history.events) == 2
    assert budget["pages"] == 2 and budget["events"] == 2
    assert "workflowId" not in json.loads(raw)


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", ["pages", "events", "bytes", "json", "time", "repeated"])
async def test_sdk_export_excess_refuses_instead_of_truncating(monkeypatch, limit):
    pages = Pages(
        [
            [HistoryEvent(event_id=1)],
            [HistoryEvent(event_id=2)],
            [HistoryEvent(event_id=3)],
            [HistoryEvent(event_id=4)],
        ],
        repeated=limit == "repeated",
    )
    budget = {"pages": 0, "events": 0, "bytes": 0}
    if limit == "pages":
        monkeypatch.setattr(capture, "MAX_PAGES", 1)
    elif limit == "events":
        monkeypatch.setattr(capture, "MAX_EVENTS", 1)
    elif limit == "bytes":
        monkeypatch.setattr(capture, "MAX_HISTORY", 1)
    elif limit == "json":
        budget["json_bytes"] = capture.MAX_HISTORY
    deadline = time.monotonic() + (-1 if limit == "time" else 3)
    with pytest.raises(ValueError, match="bound|pagination"):
        await capture._history(handle(pages), deadline, budget)


def test_pre_go_ready_identity_cannot_grant_signal_or_blind_replacement(tmp_path, monkeypatch):
    reader = tmp_path / "reader"
    reader.mkdir(mode=0o700)
    write_private(reader / "ready.json", {"pid": 12345, "identity": "owned fixture identity"})
    write_private(reader / "intent.json", {"ports": []})
    monkeypatch.setattr(capture, "_table", lambda: {12345: {"identity": "owned fixture identity"}})
    monkeypatch.setattr(
        capture.os, "kill", lambda *_: pytest.fail("No PID receipt means no signal")
    )
    with pytest.raises(ValueError, match="before authorization"):
        capture._cleanup(reader)
    assert not (reader / "cleanup.json").exists()


def test_public_capture_route_authenticates_before_ordinary_missing_helper_store(
    tmp_path, monkeypatch
):
    from devflow_temporal import delivery_control as control

    request = tmp_path / "request.json"
    write_private(request, {"explicit": "owned"})
    owner = object()
    monkeypatch.setattr(control.DeliveryConfig, "load", lambda *_: owner)
    monkeypatch.setattr(
        control, "DeliveryStore", lambda *_a, **_k: pytest.fail("Ordinary bootstrap")
    )
    calls = []
    monkeypatch.setattr(
        capture, "capture", lambda config, value: calls.append((config, value)) or {}
    )
    args = SimpleNamespace(
        command="capture-retained-closures", request=request, config=str(request)
    )
    control._run(args, SimpleNamespace(error=lambda value: pytest.fail(value)))
    assert calls == [(owner, {"explicit": "owned"})]
