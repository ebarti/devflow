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
        with capture._working(folder, "default", observed.initial) as prepared:
            working, actual_namespace, pin = prepared
            pin()
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
        assert kwargs["skip_archival"] is True
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
