"""Bounded snapshot and export custody, using only this test's owned data."""

from __future__ import annotations

import asyncio
import json
import lzma
import os
import sqlite3
import subprocess
import sys
import time
import uuid
import zlib
from contextlib import ExitStack, closing
from types import SimpleNamespace

import pytest
from temporalio.api.history.v1 import HistoryEvent
from temporalio.client import WorkflowHistory

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
        working, actual_namespace = capture._working(folder, "default", observed.initial)
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


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
@pytest.mark.parametrize("member", ["temporal.sqlite3-wal", "temporal.sqlite3-journal", "extra"])
def test_master_namespace_cannot_add_unregistered_wal_or_other_members(tmp_path, member):
    root, _database, _ = source(tmp_path)
    original = bytes_of(root)
    folder = tmp_path / "capture"
    folder.mkdir(mode=0o700)
    with capture.SourceFamily(root) as observed:
        observed.snapshot(folder)
        unexpected = folder / "master" / member
        unexpected.write_bytes(b"not part of the original held family")
        unexpected.chmod(0o600)
        with pytest.raises(ValueError, match="unregistered member"):
            observed.snapshot(folder)
        with pytest.raises(ValueError, match="unregistered member"):
            capture._working(folder, "default", observed.initial)
    assert not (folder / "working").exists()
    assert bytes_of(root) == original


@pytest.mark.parametrize("alias", ["raw-symlink", "target-symlink", "target-hardlink"])
def test_real_serve_refuses_staging_collision_before_any_alias_write(tmp_path, monkeypatch, alias):
    # Exact causal 2cd shape: readers/{1} was reused because len(entries)==1.
    original = tmp_path / "owned-original.sqlite3"
    master = tmp_path / "capture/master"
    working = tmp_path / "capture/readers/1/snapshot/working"
    for path in (master, working):
        path.mkdir(mode=0o700, parents=True)
        for parent in path.parents:
            if parent == tmp_path:
                break
            parent.chmod(0o700)
    for path, value in ((original, "preserve original"), (master / "temporal.sqlite3", "copy")):
        with closing(sqlite3.connect(path)) as db:
            db.execute("CREATE TABLE namespaces(id BLOB,name TEXT)")
            db.execute("INSERT INTO namespaces VALUES (?,?)", (uuid.uuid4().bytes, "default"))
            db.execute("CREATE TABLE probe(value TEXT)")
            db.execute("INSERT INTO probe VALUES (?)", (value,))
            db.commit()
        path.chmod(0o600)
    child = working / ("raw.sqlite3" if alias == "raw-symlink" else "server.sqlite3")
    if alias == "target-hardlink":
        os.link(original, child)
    else:
        child.symlink_to(original)
    before = original.read_bytes()
    monkeypatch.setattr(capture, "_ports", lambda: pytest.fail("No port or process preparation"))
    with pytest.raises(ValueError, match="exact owned sequence"):
        capture._serve(
            SimpleNamespace(raw={}), None, None, {}, tmp_path / "capture", "unused", None
        )
    assert original.read_bytes() == before
    assert not (working.parent / "master").exists()


@pytest.mark.skipif(sys.platform != "darwin", reason="actual complete native lsof visibility")
def test_working_copy_never_reuses_existing_empty_staging(tmp_path):
    root, _database, _ = source(tmp_path)
    folder = tmp_path / "capture"
    folder.mkdir(mode=0o700)
    with capture.SourceFamily(root) as observed:
        observed.snapshot(folder)
        (folder / "working").mkdir(mode=0o700)
        with pytest.raises(ValueError, match="generation already exists"):
            capture._working(folder, "default", observed.initial)
    assert not list((folder / "working").iterdir())


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
async def test_sdk_export_canonicalizes_unordered_maps_for_exact_crash_replay():
    from devflow_temporal.delivery_closures import history_json

    history = payload_history(b"json/plain", [b"owned"])
    payload = history.events[0].workflow_execution_completed_event_attributes.result.payloads[0]
    payload.metadata.update({"z-last": b"value", "a-first": b"same"})
    budget = {"pages": 0, "events": 0, "bytes": 0}
    _history, raw = await capture._history(
        handle(Pages([history.events])), time.monotonic() + 3, budget
    )
    assert raw == history_json(history)
    assert raw != history.to_json().encode()


@pytest.mark.asyncio
async def test_sdk_pagination_tokens_count_toward_total_byte_limit(monkeypatch):
    pages = Pages([[HistoryEvent(event_id=1)], [HistoryEvent(event_id=2)]])
    original = pages.fetch_next_page

    async def large_token():
        await original()
        pages.next_page_token = b"x" * 256

    pages.fetch_next_page = large_token
    monkeypatch.setattr(capture, "MAX_HISTORY", 128)
    with pytest.raises(ValueError, match="byte/event bound"):
        await capture._history(
            handle(pages), time.monotonic() + 3, {"pages": 0, "events": 0, "bytes": 0}
        )


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


@pytest.mark.parametrize("defect", ["content", "restored-mtime", "replace", "link", "missing"])
def test_capture_input_fd_and_name_generation_remain_bound(tmp_path, defect):
    path = tmp_path / "owner.json"
    write_private(path, {"original": "owned"})
    with ExitStack() as leases:
        current = capture._pin_inputs([path], leases)
        info, raw = path.stat(), path.read_bytes()
        if defect in {"content", "restored-mtime"}:
            path.write_bytes(raw.replace(b"owned", b"other"))
            if defect == "restored-mtime":
                os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns))
        elif defect == "replace":
            path.unlink()
            path.write_bytes(raw)
            path.chmod(0o600)
        elif defect == "link":
            os.link(path, tmp_path / "second-name")
        else:
            path.unlink()
        with pytest.raises((ValueError, FileNotFoundError)):
            current()


def payload_history(encoding, payloads):
    events = []
    for index, raw in enumerate(payloads, 1):
        event = HistoryEvent(event_id=index)
        payload = event.workflow_execution_completed_event_attributes.result.payloads.add()
        payload.metadata["encoding"] = encoding
        payload.data = raw
        events.append(event)
    return WorkflowHistory("owned-workflow", events)


@pytest.mark.parametrize("encoding", [b"binary/zlib", b"binary/xz"])
@pytest.mark.parametrize("defect", [None, "payload", "total", "truncated", "trailing"])
def test_payload_expansion_bound_is_proved_before_existing_codec(monkeypatch, encoding, defect):
    monkeypatch.setattr(capture, "MAX_PAYLOAD", 128)
    monkeypatch.setattr(capture, "MAX_DECODED", 192)
    compressed = (zlib.compress if encoding == b"binary/zlib" else lzma.compress)(b"x" * 100)
    blobs = [compressed]
    if defect == "payload":
        monkeypatch.setattr(capture, "MAX_PAYLOAD", 99)
    elif defect == "total":
        blobs *= 2
    elif defect == "truncated":
        blobs = [compressed[:-1]]
    elif defect == "trailing":
        blobs = [compressed + b"unconsumed"]
    history = payload_history(encoding, blobs)
    if defect:
        with pytest.raises(ValueError, match="bound|incomplete|trailing"):
            capture._bounded_payloads(history)
    else:
        assert capture._bounded_payloads(history) == 100


def test_uncompressed_payloads_obey_same_total_bound(monkeypatch):
    monkeypatch.setattr(capture, "MAX_PAYLOAD", 128)
    monkeypatch.setattr(capture, "MAX_DECODED", 192)
    with pytest.raises(ValueError, match="bound"):
        capture._bounded_payloads(payload_history(b"json/plain", [b"x" * 100] * 2))
    with pytest.raises(ValueError, match="bound"):
        capture._bounded_payloads(payload_history(b"json/plain", [b"x" * 100]), consumed=100)


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
