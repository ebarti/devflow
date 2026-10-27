"""Cancellation never releases a custody scope before its owned worker stops."""
import asyncio
import sqlite3
import threading

import pytest

from devflow_temporal.delivery_pause_async import joined_thread
from devflow_temporal.delivery_pause_dispatch import _held


@pytest.mark.asyncio
async def test_repeated_cancel_during_acquisition_joins_and_releases_once():
    entered, release = threading.Event(), threading.Event()
    events = []

    class Scope:
        def __enter__(self):
            entered.set()
            assert release.wait(5)
            events.append('acquired')
            return self

        def __exit__(self, *_args):
            events.append('released')

    async def run():
        async with _held(Scope()):
            events.append('body')

    task = asyncio.create_task(run())
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(.01)
    task.cancel()
    await asyncio.sleep(.01)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert events == ['acquired', 'released']


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
@pytest.mark.parametrize('shutdown', [False, True])
async def test_timeout_or_shutdown_holds_scope_through_worker_commit(tmp_path, shutdown):
    path = tmp_path / 'scope.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE commits (value TEXT)')
    entered, release = threading.Event(), threading.Event()
    events = []

    class Scope:
        def __enter__(self):
            events.append('acquired')
            return self

        def __exit__(self, *_args):
            events.append('released')

    def commit():
        entered.set()
        assert release.wait(5)
        with sqlite3.connect(path) as db:
            db.execute("INSERT INTO commits VALUES ('committed')")
        events.append('committed')

    async def run():
        async with _held(Scope()):
            await asyncio.wait_for(joined_thread(commit), timeout=.01)

    task = asyncio.create_task(run())
    assert await asyncio.to_thread(entered.wait, 2)
    await asyncio.sleep(.04)
    if shutdown:
        task.cancel()
        await asyncio.sleep(.01)
        task.cancel()
    await asyncio.sleep(.01)
    assert not task.done() and events == ['acquired']
    release.set()
    with pytest.raises(asyncio.CancelledError if shutdown else TimeoutError):
        await asyncio.wait_for(task, 2)
    assert events == ['acquired', 'committed', 'released']
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT value FROM commits').fetchone()[0] == 'committed'


@pytest.mark.asyncio
async def test_intake_loop_shutdown_keeps_namespace_until_timed_out_worker_commits(
        tmp_path, monkeypatch):
    from contextlib import contextmanager
    from types import SimpleNamespace

    from devflow_temporal import delivery_pause_intake as intake
    from devflow_temporal import delivery_pause_intake_pump as pump
    from devflow_temporal import delivery_pause_namespace as namespace
    from devflow_temporal.delivery_api import DeliveryService

    path = tmp_path / 'intake.sqlite3'
    with sqlite3.connect(path) as db:
        db.execute('CREATE TABLE commits (value TEXT)')
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
                'checkpoint_json': None}

    async def sdk_observation(*_args, **_kwargs):
        return {}

    def seal(*_args, **_kwargs):
        entered.set()
        assert release.wait(5)
        with sqlite3.connect(path) as db:
            db.execute("INSERT INTO commits VALUES ('committed')")
        events.append('committed')
        return {'kind': 'unstarted_intake', 'resumable': True}

    monkeypatch.setattr(namespace, 'intake_namespace_scope', controlled_scope)
    monkeypatch.setattr(pump, '_page', lambda *_args: ['root'])
    monkeypatch.setattr(pump, '_selected', selected)
    monkeypatch.setattr(pump, '_record_status', lambda *_args: None)
    monkeypatch.setattr(pump, '_SDK_TIMEOUT_SECONDS', .01)
    monkeypatch.setattr(intake, '_spec', lambda *_args: {})
    monkeypatch.setattr(intake, '_observe_no_start', sdk_observation)
    monkeypatch.setattr(intake, 'seal_unstarted', seal)
    service = SimpleNamespace(store=object(), config=object())
    task = asyncio.create_task(DeliveryService.pause_intake_loop(service))
    assert await asyncio.to_thread(entered.wait, 2)
    await asyncio.sleep(.04)
    task.cancel()
    await asyncio.sleep(.01)
    task.cancel()
    await asyncio.sleep(.01)
    assert not task.done() and events == ['acquired']
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 2)
    assert events == ['acquired', 'committed', 'released']
    with sqlite3.connect(path) as db:
        assert db.execute('SELECT value FROM commits').fetchone()[0] == 'committed'


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
