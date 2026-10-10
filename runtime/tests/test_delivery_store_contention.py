"""A feature backfill must coexist with live execution writes."""
from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

from test_delivery_store import service as service

from devflow_temporal import delivery_features
from devflow_temporal.delivery_store import DeliveryStore


def test_feature_backfill_does_not_deadlock_a_concurrent_execution_write(service, monkeypatch):
    store, request = service
    store.submit(request)
    second = {**request, 'run_id': 'run-2', 'command_id': 'command-2',
              'work_id': 'work-2', 'issue_url': 'https://github.com/example/fixture/issues/4'}
    store.submit(second)
    with store._connect() as db:
        # Model an existing execution database before feature projections existed.
        db.execute('DELETE FROM delivery_feature_events')
        db.execute('DELETE FROM delivery_features')
    reading, release, writing = (threading.Event() for _ in range(3))
    original = delivery_features.transition

    def transition(db, config, run_id):
        if threading.current_thread().name.startswith('backfill') and not reading.is_set():
            reading.set()
            assert release.wait(5), 'backfill fixture was not released'
        return original(db, config, run_id)

    monkeypatch.setattr(delivery_features, 'transition', transition)

    def write_execution():
        with sqlite3.connect(store.config.tracking_db, timeout=5) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
                       "revision=revision+1 WHERE run_id=?", (request['run_id'],))
            original(db, store.config, request['run_id'])
            writing.set()

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix='backfill') as readers, \
            ThreadPoolExecutor(max_workers=1) as writers:
        initialized = readers.submit(DeliveryStore, store.config)
        try:
            assert reading.wait(5), 'backfill never reached its first execution'
            written = writers.submit(write_execution)
            # The old deferred reader and the writer now deadlock upgrading to a
            # write lock. A serialized initializer keeps the writer waiting instead.
            writing.wait(0.25)
        finally:
            release.set()
        initialized.result(timeout=5)
        written.result(timeout=5)
    with store._connect() as db:
        first = delivery_features.current(db, request['issue_url'])
        other = delivery_features.current(db, second['issue_url'])
    assert first['status'] == 'Blocked' and first['run_revision'] == 2
    assert other['status'] == 'Queued'
