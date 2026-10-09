"""Pure SQLite and transport stubs; no service/Temporal/process fixtures."""
from __future__ import annotations

import json
import selectors
import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from test_delivery_project_sync import Remote, published
from test_delivery_store import service as delivery_service

from devflow_temporal import delivery_project_events as events
from devflow_temporal.delivery_project_sync import DAY, ProjectSynchronizer, serve
from devflow_temporal.delivery_store import DeliveryStore

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


class SocketStub:
    def __init__(self, failure=None):
        self.failure = failure
        self.sent = []
        self.blocking = None
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def setblocking(self, value):
        self.blocking = value

    def sendto(self, data, address):
        assert self.blocking is False
        if self.failure:
            raise self.failure
        self.sent.append((data, address))


@pytest.mark.parametrize('failure', [FileNotFoundError('stopped'), BlockingIOError('full'),
                                   PermissionError('inaccessible'), RuntimeError('transport')])
def test_notification_failure_never_fails_committed_delivery(service, monkeypatch, failure):
    store, request = service
    client = SocketStub(failure)
    monkeypatch.setattr(events.socket, 'socket', lambda *_: client)
    store.submit(request)
    assert client.closed and client.blocking is False
    assert store.detail(request['run_id'])['phase'] == 'accepted'
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_feature_events').fetchone()[0] == 1
    remote = Remote()
    # No notification was delivered. Startup still discovers and mirrors the state.
    assert ProjectSynchronizer([DeliveryStore(store.config)], remote).tick()[
        request['issue_url']]['status'] == 'Queued'
    assert remote.writes == ['Queued']


def test_notification_contains_no_payload_or_remote_authority(tmp_path, monkeypatch):
    client = SocketStub()
    monkeypatch.setattr(events.socket, 'socket', lambda *_: client)
    database = tmp_path / 'state.sqlite3'
    events.notify(database)
    assert client.sent == [(b'feature_changed', str(events.endpoint(database)))]
    assert len(str(events.endpoint(database)).encode()) < 104
    assert events.endpoint(database) != events.endpoint(tmp_path / 'other.sqlite3')


def test_wait_coalesces_notifications_without_discarding_later_wakeups():
    received = [b'one', b'two', b'untrusted input']
    timeouts = []

    def receive(_size):
        if received:
            return received.pop(0)
        raise BlockingIOError

    def select(timeout):
        timeouts.append(timeout)
        return [(SimpleNamespace(fileobj=SimpleNamespace(recv=receive)), selectors.EVENT_READ)]

    notifications = events.Notifications()
    notifications.selector = SimpleNamespace(select=select)
    assert notifications.wait(DAY)
    assert not received and timeouts == [DAY]
    received.append(b'later commit')
    assert notifications.wait(DAY)
    assert not received


def test_service_waits_for_events_and_daily_deadlines_without_local_polling(service):
    store, request = service
    store.submit(request)
    store.project(request['run_id'], phase='implementation', execution_state='running',
                  event_type='progress', message='active publication', pull_request={
                      'url': 'https://github.com/example/fixture/pull/7',
                      'state': 'OPEN', 'head': 'a' * 40, 'number': 7})
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    waits = []

    class StopTest(BaseException):
        pass

    class NotificationStub:
        def wait(self, timeout):
            waits.append(timeout)
            if len(waits) == 1:
                assert remote.writes == ['In progress']
                assert timeout == DAY
                # A new local event interrupts the daily wait immediately.
                store.project(request['run_id'], phase='blocked', execution_state='blocked',
                              event_type='blocked', message='committed', outcome='blocked')
                return True
            assert remote.writes == ['In progress', 'Blocked']
            assert len(remote.reads) == 1
            assert timeout == DAY
            raise StopTest

    with pytest.raises(StopTest):
        serve(sync, NotificationStub())
    assert waits == [DAY, DAY]


def test_pending_project_retry_wakes_without_new_event_or_daily_read(service):
    store, _, _ = published(service)
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    remote = Remote()
    remote.failure = True
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    sync.tick()
    assert sync.next_delay() == 15
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote, clock=lambda: stamp)
    restarted.tick()
    assert restarted.next_delay() == 15 and len(remote.writes) == 1
    stamp += timedelta(seconds=15)
    remote.failure = False
    restarted.tick()
    assert len(remote.writes) == 2 and len(remote.reads) == 1
    assert restarted.next_delay() == DAY - 15


@pytest.mark.parametrize('old_interval', [300, 3600])
def test_daily_configuration_rebases_old_deadlines_once_without_remote_reads(service, old_interval):
    store, _, _ = published(service)
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    remote = Remote()
    old = ProjectSynchronizer([store], remote, interval=old_interval, project_interval=old_interval,
                              clock=lambda: stamp)
    old.tick()
    with store._connect() as db:
        db.execute('DELETE FROM delivery_sync_settings')  # Original worker had no settings.
        before = db.execute('SELECT count(*) FROM delivery_feature_events').fetchone()[0]
    daily = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    daily.tick()
    restarted = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    restarted.tick()
    assert len(remote.reads) == len(remote.writes) == 1
    assert restarted.next_delay() == DAY
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_feature_events').fetchone()[0] == before
        assert db.execute('SELECT next_check_at FROM delivery_pr_observations').fetchone()[0] == (
            stamp + timedelta(days=1)).isoformat()
        assert db.execute('SELECT next_attempt_at FROM delivery_project_outbox').fetchone()[0] == (
            stamp + timedelta(days=1)).isoformat()
    stamp += timedelta(seconds=DAY - 1)
    restarted.tick()
    assert len(remote.reads) == len(remote.writes) == 1
    stamp += timedelta(seconds=1)
    restarted.tick()
    assert len(remote.reads) == len(remote.writes) == 2


def test_new_interval_preserves_pending_retry_deadline(service):
    store, _, _ = published(service)
    remote = Remote()
    remote.failure = True
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    ProjectSynchronizer([store], remote, interval=300, project_interval=300,
                        clock=lambda: stamp).tick()
    daily = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    daily.tick()
    assert daily.next_delay() == 15
    assert len(remote.writes) == len(remote.reads) == 1


def test_pr_and_project_schedules_are_independent(service):
    store, _, _ = published(service)
    remote = Remote()
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote, project_interval=300, clock=lambda: stamp)
    sync.tick()
    stamp += timedelta(seconds=300)
    sync.tick()
    assert len(remote.writes) == 2 and len(remote.reads) == 1


def test_head_change_invalidates_review_even_if_local_candidate_has_changed(service):
    store, request, receipt = published(service)
    remote = Remote()
    remote.head = 'c' * 40
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET candidate_json=?',
                   (json.dumps({'head': remote.head}),))
    feature = ProjectSynchronizer([store], remote).tick()[request['issue_url']]
    assert feature['status'] == 'Needs validation'
    assert store.detail(request['run_id'])['pull_request'] == receipt


@pytest.mark.parametrize('interval', [299, 86401, True, '86400', 86400.0])
def test_reconciliation_interval_validation(service, interval):
    store, _ = service
    with pytest.raises(ValueError, match='intervals'):
        ProjectSynchronizer([store], interval=interval)
    with pytest.raises(ValueError, match='intervals'):
        ProjectSynchronizer([store], project_interval=interval)


def test_listener_binds_configured_databases_before_wait_and_cleans_up(tmp_path, monkeypatch):
    from pathlib import Path

    endpoints = [tmp_path / 'private' / 'one.sock', tmp_path / 'private' / 'two.sock']
    databases = [tmp_path / 'one.db', tmp_path / 'two.db']
    instances = []
    registrations = []

    class Listener(SocketStub):
        def __init__(self):
            super().__init__()
            instances.append(self)

        def bind(self, name):
            self.bound = name
            Path(name).touch()  # Stub endpoint, not a listening socket/server.

    class Selector:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            pass

        def register(self, listener, flags):
            registrations.append((listener, flags))

    monkeypatch.setattr(events, 'endpoint', lambda db: endpoints[databases.index(db)])
    monkeypatch.setattr(events.socket, 'socket', lambda *_: Listener())
    monkeypatch.setattr(events.selectors, 'DefaultSelector', Selector)
    with events.Notifications().listen(databases):
        assert [item.bound for item in instances] == list(map(str, endpoints))
        assert [item.blocking for item in instances] == [False, False]
        assert registrations == [(item, selectors.EVENT_READ) for item in instances]
        assert all(path.stat().st_mode & 0o777 == 0o600 for path in endpoints)
        assert endpoints[0].parent.stat().st_mode & 0o777 == 0o700
    assert all(item.closed for item in instances)
    assert all(not path.exists() for path in endpoints)


def test_listener_refuses_symlinked_endpoint_without_touching_target(tmp_path, monkeypatch):
    root = tmp_path / 'private'
    root.mkdir(mode=0o700)
    target = tmp_path / 'keep'
    target.write_text('owned by someone else')
    link = root / 'event.sock'
    link.symlink_to(target)
    monkeypatch.setattr(events, 'endpoint', lambda _: link)
    with (pytest.raises(ValueError, match='unowned'),
          events.Notifications().listen([tmp_path / 'db'])):
        pytest.fail('listener accepted a symlink')
    assert target.read_text() == 'owned by someone else'


def test_same_pr_new_validated_publication_refreshes_immediately_and_replays_once(service):
    store, request = service
    store.submit(request)
    receipt = {'url': 'https://github.com/example/fixture/pull/7', 'state': 'OPEN',
               'head': 'a' * 40, 'number': 7}
    store.project(request['run_id'], phase='review', execution_state='running',
                  event_type='published', message='first publication', pull_request=receipt)
    stamp = datetime(2026, 10, 9, tzinfo=UTC)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    sync.tick()
    assert len(remote.reads) == 1
    stamp += timedelta(seconds=60)
    remote.head = 'c' * 40
    updated = {**receipt, 'head': remote.head}
    store.project(request['run_id'], phase='delivered', execution_state='terminal',
                  event_type='delivered', message='same PR, new publication passed gates',
                  outcome='published_unmerged', pull_request=updated)
    view = sync.tick()[request['issue_url']]
    assert view['status'] == 'Awaiting merge'
    assert view['pull_requests'][0]['observation']['head'] == updated['head']
    assert len(remote.reads) == 2
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote, clock=lambda: stamp)
    restarted.tick()
    restarted.tick()
    assert len(remote.reads) == 2
    assert store.detail(request['run_id'])['pull_request'] == updated
    # An external push is still found by the daily reconciliation, not by polling.
    remote.head = 'd' * 40
    stamp += timedelta(seconds=DAY - 1)
    assert restarted.tick()[request['issue_url']]['status'] == 'Awaiting merge'
    assert len(remote.reads) == 2
    stamp += timedelta(seconds=1)
    assert restarted.tick()[request['issue_url']]['status'] == 'Needs validation'
    assert len(remote.reads) == 3


def test_upgrade_establishes_publication_inputs_once(service):
    store, _, _ = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    sync.tick()
    with store._connect() as db:
        db.execute('DELETE FROM delivery_pr_observation_inputs')
    restarted = ProjectSynchronizer([store], remote)
    restarted.tick()
    restarted.tick()
    assert len(remote.reads) == 2
