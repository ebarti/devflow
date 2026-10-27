"""Owned dispatcher custody tests: no Temporal or provider is contacted."""
import asyncio
import json
import sqlite3
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_pause as pause
from devflow_temporal.delivery_api import _dispatch_start
from devflow_temporal.delivery_pause_dispatch_lease import (
    dispatch_lease,
    lock_path,
)
from devflow_temporal.delivery_pause_writers import PauseWriterCustody


class Store:
    def __init__(self, root):
        self.config = SimpleNamespace(state_root=root)
        self.database = root / "tracking.sqlite3"
        with self._connect() as db:
            pause.initialize(db)

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.database, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()


def fixture(tmp_path):
    store = Store(tmp_path)
    with store._connect() as db:
        db.execute("CREATE TABLE delivery_runs (run_id TEXT,workflow_id TEXT,request_digest TEXT,"
                   "request_json TEXT,recovery_json TEXT,execution_state TEXT)")
        db.execute("CREATE TABLE delivery_outbox (run_id TEXT,state TEXT)")
        db.execute("INSERT INTO delivery_runs VALUES "
                   "('root','delivery-root','digest','{}',NULL,'queued')")
        db.execute("INSERT INTO delivery_outbox VALUES ('root','pending')")
    with store._connect() as db:
        db.execute("INSERT INTO delivery_pause_control VALUES "
                   "('root','running',0,0,'delivery-root','',1,NULL,?)", (pause._now(),))
    spec = {"run_id": "root", "pause_intake_version": 1, "request_digest": "digest",
            "state_dir": str(tmp_path / "state")}
    item = {"workflow_id": "delivery-root", "request_digest": "digest", "request_json": "{}",
            "recovery_json": None}
    return store, spec, item


def test_pause_waits_for_dispatcher_body_and_keeps_outbox(tmp_path):
    store, spec, item = fixture(tmp_path)
    entered, release = threading.Event(), threading.Event()

    def dispatch():
        with dispatch_lease(store, spec, item):
            entered.set()
            assert release.wait(5)

    worker = threading.Thread(target=dispatch)
    worker.start()
    try:
        assert entered.wait(5)
        assert (pause.request_pause(store, "root", "pause", 0, intake_only=True)["state"]
                == "pause_requested")
        with pytest.raises(ValueError, match="dispatcher still owns"):
            with PauseWriterCustody(store, spec):
                pytest.fail("active dispatcher custody was accepted")
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    with PauseWriterCustody(store, spec) as custody, store._connect() as db:
        custody.verify(db)
        row = db.execute("SELECT state FROM delivery_outbox WHERE run_id='root'").fetchone()
        assert row[0] == "pending"
    with pytest.raises(pause.PauseDeferred):
        with dispatch_lease(store, spec, item):
            pytest.fail("paused dispatcher observed remote state")


def test_replaced_dispatcher_lock_fails_closed(tmp_path):
    store, spec, item = fixture(tmp_path)
    with dispatch_lease(store, spec, item):
        pass
    path = lock_path(store, "root")
    path.unlink()
    path.touch(mode=0o600)
    with pytest.raises(ValueError, match="identity changed"):
        with PauseWriterCustody(store, spec):
            pytest.fail("replaced dispatcher lock was accepted")


@pytest.mark.asyncio
async def test_actual_dispatcher_retains_lease_during_readback_and_failure_ack(
    tmp_path, monkeypatch
):
    from contextlib import nullcontext

    from devflow_temporal import delivery_pause_namespace

    store, spec, item = fixture(tmp_path)
    from devflow_temporal import delivery_execution_authority

    spec["execution_authority_version"] = 1
    monkeypatch.setattr(delivery_execution_authority, "reserve",
                        lambda *_args: {"start_spec_json": json.dumps(spec)})
    # Controlled installation seam; this test exercises dispatcher custody only.
    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", lambda _: nullcontext())
    entered, release = asyncio.Event(), asyncio.Event()
    acknowledgements = []

    class Handle:
        async def describe(self):
            entered.set()
            await release.wait()
            raise RuntimeError("controlled readback failure")

    class Client:
        def get_workflow_handle(self, workflow_id):
            assert workflow_id == "delivery-root"
            return Handle()

    def failed(run_id, reason, *, uncertain):
        acknowledgements.append((run_id, reason, uncertain))

    store.record_dispatch_failure = failed
    task = asyncio.create_task(_dispatch_start(SimpleNamespace(store=store), Client(), item, spec))
    await asyncio.wait_for(entered.wait(), 5)
    pause.request_pause(store, "root", "pause", 0, intake_only=True)
    with pytest.raises(ValueError, match="dispatcher still owns"):
        with PauseWriterCustody(store, spec):
            pytest.fail("readback body was not retained")
    release.set()
    await asyncio.wait_for(task, 5)
    assert acknowledgements == []  # Pause won before the failure observation.
    with PauseWriterCustody(store, spec) as custody, store._connect() as db:
        custody.verify(db)
