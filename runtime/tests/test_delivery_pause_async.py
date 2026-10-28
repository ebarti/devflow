"""Cancellation never releases a custody scope before its owned worker stops."""
import asyncio
import sqlite3
import threading

import pytest

from devflow_temporal.delivery_pause_dispatch import _held


@pytest.mark.asyncio
async def test_repeated_cancel_during_scope_exit_joins_cleanup():
    exiting, release = threading.Event(), threading.Event()
    events = []

    class Scope:
        def __enter__(self):
            events.append('acquired')
            return self

        def __exit__(self, *_args):
            exiting.set()
            assert release.wait(5)
            events.append('released')

    async def run():
        async with _held(Scope()):
            await asyncio.Event().wait()

    task = asyncio.create_task(run())
    while not events:
        await asyncio.sleep(.001)
    task.cancel()
    assert await asyncio.to_thread(exiting.wait, 2)
    task.cancel()
    await asyncio.sleep(.01)
    assert not task.done() and events == ['acquired']
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert events == ['acquired', 'released']


@pytest.mark.asyncio
async def test_intake_shutdown_joins_actual_pause_publication(tmp_path, monkeypatch):
    import json
    from contextlib import contextmanager
    from types import SimpleNamespace

    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_intake as intake
    from devflow_temporal import delivery_pause_intake_pump as pump
    from devflow_temporal import delivery_pause_namespace as namespace
    from devflow_temporal.delivery_api import DeliveryService

    path = tmp_path / 'publication.sqlite3'
    receipt = {'kind': 'unstarted_intake', 'resumable': True, 'checkpoint_id': 'intake:proof'}

    class Store:
        config = SimpleNamespace(state_root=tmp_path)

        def _connect(self):
            db = sqlite3.connect(path)
            db.row_factory = sqlite3.Row
            return db

    store = Store()
    with store._connect() as db:
        pause.initialize(db)
        db.execute("INSERT INTO delivery_pause_control VALUES "
                   "('root','pausing',1,1,'wf','',1,?,?)",
                   (json.dumps(receipt), pause._now()))
    entered, release = threading.Event(), threading.Event()
    events = []

    @contextmanager
    def controlled_scope(_config):
        events.append('acquired')
        try:
            yield
        finally:
            events.append('released')

    def selected(_store, _root):
        return {'epoch': 1, 'run_revision': 1, 'state': 'pausing',
                'checkpoint_json': json.dumps(receipt)}

    async def sealed(*_args, **_kwargs):
        return receipt

    original_publish = pause.publish_paused

    def blocked_publish(*args):
        entered.set()
        assert release.wait(5)
        result = original_publish(*args)
        events.append('published')
        return result

    monkeypatch.setattr(namespace, 'intake_namespace_scope', controlled_scope)
    monkeypatch.setattr(pump, '_page', lambda *_args: ['root'])
    monkeypatch.setattr(pump, '_selected', selected)
    monkeypatch.setattr(intake, '_spec', lambda *_args: {})
    monkeypatch.setattr(intake, 'seal_with_sdk', sealed)
    monkeypatch.setattr(pause, 'publish_paused', blocked_publish)
    service = SimpleNamespace(store=store, config=object())
    task = asyncio.create_task(DeliveryService.pause_intake_loop(service))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(.01)
    task.cancel()
    await asyncio.sleep(.01)
    assert not task.done() and events == ['acquired']
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert events == ['acquired', 'published', 'released']
    with store._connect() as db:
        assert pause._row(db, 'root')['state'] == 'paused'
