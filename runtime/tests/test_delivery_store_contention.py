"""The projection maintenance pass must coexist with live execution writes."""
from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor

from test_delivery_store import service as service

from devflow_temporal import delivery_features
from devflow_temporal.delivery_features import maintain_projections


def test_maintenance_pass_neither_blocks_nor_deadlocks_a_concurrent_execution_write(
    service, monkeypatch
):
    store, request = service
    store.submit(request)
    second = {**request, 'run_id': 'run-2', 'command_id': 'command-2',
              'work_id': 'work-2', 'issue_url': 'https://github.com/example/fixture/issues/4'}
    store.submit(second)
    with store._connect() as db:
        # Model an existing execution database before feature projections existed.
        db.execute('BEGIN IMMEDIATE')
        db.execute('DELETE FROM delivery_feature_events')
        db.execute('DELETE FROM delivery_features')
        db.execute('DELETE FROM delivery_feature_projections')
    reached, release, written = (threading.Event() for _ in range(3))
    project_row = delivery_features._project_row

    def paused(store_, run_id, generation):
        if run_id == second['run_id'] and not reached.is_set():
            reached.set()
            assert release.wait(5), 'maintenance fixture was not released'
        return project_row(store_, run_id, generation)

    monkeypatch.setattr(delivery_features, '_project_row', paused)
    transition = delivery_features.transition

    def write_execution():
        with sqlite3.connect(store.config.tracking_db, timeout=1) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE delivery_runs SET phase='blocked',outcome='blocked',"
                       "revision=revision+1 WHERE run_id=?", (request['run_id'],))
            transition(db, store.config, request['run_id'])
            written.set()

    with ThreadPoolExecutor(max_workers=1) as maintainers, \
            ThreadPoolExecutor(max_workers=1) as writers:
        maintained = maintainers.submit(maintain_projections, store)
        try:
            assert reached.wait(5), 'the pass never reached its second row'
            writing = writers.submit(write_execution)
            # No write lock is held between rows: the writer finishes while the
            # pass is still in flight, within its own one-second busy timeout.
            assert written.wait(3), 'the maintenance pass blocked a live execution write'
        finally:
            release.set()
        result = maintained.result(timeout=5)
        writing.result(timeout=5)
    assert result['projected'] == 2 and result['skipped'] == {}
    with store._connect() as db:
        first = delivery_features.current(db, request['issue_url'])
        other = delivery_features.current(db, second['issue_url'])
    assert first['status'] == 'Blocked' and first['run_revision'] == 2
    assert other['status'] == 'Queued'
    # The writer did not record a marker; the next pass refreshes that one row only.
    assert maintain_projections(store)['projected'] == 1
    assert maintain_projections(store)['pending'] == 0
