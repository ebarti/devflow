"""Pure SQLite and transport stubs; no service/Temporal/process fixtures."""
from __future__ import annotations

import sqlite3

import pytest
from test_delivery_store import service as delivery_service

from devflow_temporal import delivery_project_events as events

service = delivery_service


def test_committed_event_notifies_with_committed_state_and_rollback_does_not(service, monkeypatch):
    store, request = service
    seen = []

    def notified(path):
        with sqlite3.connect(path) as reader:
            count = reader.execute('SELECT count(*) FROM delivery_feature_events').fetchone()[0]
            seen.append(count)

    monkeypatch.setattr(events, 'notify', notified)
    store.submit(request)
    assert seen == [1]
    with pytest.raises(RuntimeError), store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        db.execute("UPDATE delivery_runs SET phase='blocked'")
        store._event(db, request['run_id'], 2, 'blocked', 'uncommitted', {})
        raise RuntimeError('rollback')
    assert seen == [1]
    store.detail(request['run_id'])
    assert seen == [1]  # Reads cannot drive a notification loop.
    store.project(request['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='committed', outcome='blocked')
    assert seen == [1, 2]
