"""Pure SQLite/Git/stub checks; no Temporal or native process fixtures."""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest
from test_delivery_store import service as delivery_service

from devflow_temporal.delivery_features import transition
from devflow_temporal.delivery_project_sync import DAY, ProjectSynchronizer
from devflow_temporal.delivery_store import DeliveryStore

service = delivery_service


class Remote:
    def __init__(self):
        self.state = "OPEN"
        self.head = "a" * 40
        self.reads = []
        self.writes = []
        self.failure = False

    def pull_request(self, url):
        self.reads.append(url)
        return {"url": url, "state": self.state, "head": self.head,
                "base": "b" * 40, "merged_at": None}

    def issue(self, url):
        return {"url": url, "state": "OPEN", "state_reason": None, "closed_at": None}

    def mirror(self, feature, fence):
        fence()
        self.writes.append(feature["status"])
        if self.failure:
            raise RuntimeError("GitHub unavailable")
        return {"status": feature["status"], "assignee": "owner"}


def published(service, *, active=False):
    store, request = service
    store.submit(request)
    receipt = {"url": "https://github.com/example/fixture/pull/7", "state": "OPEN",
               "head": "a" * 40, "number": 7}
    store.project(request["run_id"], phase="merging" if active else "delivered",
                  execution_state="running" if active else "terminal",
                  event_type="published", message="published",
                  outcome=None if active else "published_unmerged",
                  pull_request=receipt)
    return store, request, receipt


def test_event_and_feature_roll_back_with_run_transaction(service):
    store, request = service
    store.submit(request)
    with store._connect() as db:
        before = db.execute("SELECT payload_json FROM delivery_features").fetchone()[0]
        count = db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0]
    with pytest.raises(RuntimeError), store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("UPDATE delivery_runs SET phase='blocked'")
        store._event(db, request["run_id"], 2, "blocked", "fail", {})
        raise RuntimeError("crash before commit")
    with store._connect() as db:
        assert db.execute("SELECT payload_json FROM delivery_features").fetchone()[0] == before
        assert db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0] == count


def test_issue_read_failure_never_acknowledges_synced_project(service):
    store, request, _ = published(service)
    remote = Remote()

    def unavailable(_url):
        raise RuntimeError("issue read unavailable")

    remote.issue = unavailable
    result = ProjectSynchronizer([store], remote).tick()[request["issue_url"]]
    assert result["mirror"]["state"] == "pending"
    assert "issue read unavailable" in result["mirror"]["last_error"]
    assert remote.writes == []


def load_tracking_helpers(store, monkeypatch):
    import importlib.util
    import sys

    monkeypatch.setitem(sys.modules, "state", store.state)
    modules = {}
    for name in ("github", "reconcile"):
        loader = importlib.util.spec_from_file_location(
            name, store.config.helpers_dir / f"{name}.py",
        )
        module = importlib.util.module_from_spec(loader)
        monkeypatch.setitem(sys.modules, name, module)
        loader.loader.exec_module(module)
        modules[name] = module
    return modules["github"], modules["reconcile"]


def second_owner(store, tmp_path, *, copied=False):
    import sqlite3

    from devflow_temporal.delivery_config import DeliveryConfig

    target = tmp_path / "z-current.sqlite3"
    if copied:
        with store._connect() as source, sqlite3.connect(target) as destination:
            source.backup(destination)
    raw = {**store.config.raw, "tracking_db": str(target),
           "state_root": str(tmp_path / "second-state")}
    config = tmp_path / "second.json"
    config.write_text(json.dumps(raw))
    return DeliveryStore(DeliveryConfig.load(config))


def test_handoff_fences_actual_legacy_apply_before_remote_effect(service, tmp_path, monkeypatch):
    import fcntl
    import hashlib

    old, request = service
    old.submit(request)
    with old._connect() as db:
        spec = json.loads(db.execute("SELECT request_json FROM delivery_runs").fetchone()[0])
        spec.pop("project_sync_version")
        db.execute("UPDATE delivery_runs SET request_json=?", (json.dumps(spec),))
        transition(db, old.config, request["run_id"])
    new = second_owner(old, tmp_path)
    new.submit({**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"})
    github, reconcile = load_tracking_helpers(old, monkeypatch)
    monkeypatch.setattr(github, "gh", lambda *_a, **_k: pytest.fail("legacy remote effect"))
    with old._connect() as db:
        old.state.update(db, "work", {"id": request["work_id"], "details": {
            "github": {"project": "https://github.com/users/example/projects/1"},
        }}, None)
        saved = reconcile.queue(db, request["work_id"], "sync", {
            "issue": request["issue_url"], "status": "in-review", "release": False,
            "project": "https://github.com/users/example/projects/1",
            "project_status": "In review", "assignee": "owner",
        }, owner="external:devflow:run-1")
        assert reconcile.current(db, saved)
    lock_name = hashlib.sha256(request["work_id"].encode()).hexdigest()[:24]
    lock_path = old.config.tracking_db.with_name(f".reconcile-{lock_name}.lock")
    remote = Remote()
    mirror = remote.mirror

    def overlap(feature, fence):
        with lock_path.open("a") as handle, pytest.raises(BlockingIOError):
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with old._connect() as db:
            assert github.feature_state(db, request["issue_url"])["run_id"] == "run-2"
            assert reconcile.apply_one(db, saved)["state"] == "superseded"
        return mirror(feature, fence)

    remote.mirror = overlap
    sync = ProjectSynchronizer([old, new], remote)
    # A legacy write already in flight owns the lock; the projector must wait.
    with lock_path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert sync.tick() == {}
        assert not remote.writes
    assert sync.tick()[request["issue_url"]]["mirror"]["state"] == "consistent"
    assert remote.writes == ["Queued"]


def test_explicit_refresh_is_durable_and_preserves_execution_and_daily_schedule(
    service, monkeypatch,
):
    store, request, receipt = published(service)
    remote = Remote()
    stamp = datetime(2026, 10, 10, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    sync.tick()
    original = store.detail(request["run_id"])
    notified = []
    monkeypatch.setattr("devflow_temporal.delivery_project_sync.notify", notified.append)
    queued = sync.request_pr_refresh([receipt["url"]])
    assert queued == {"state": "queued", "pull_requests": [receipt["url"]]}
    assert notified == [store.config.tracking_db]
    assert len(remote.reads) == 1 and len(remote.writes) == 1
    assert sync.next_delay() == 15
    remote.state = "MERGED"
    restarted = ProjectSynchronizer([DeliveryStore(store.config)], remote,
                                     clock=lambda: stamp, interval=DAY, project_interval=DAY)
    assert restarted.tick()[request["issue_url"]]["status"] == "Done"
    detail = store.detail(request["run_id"])
    for key in ("outcome", "phase", "pull_request", "iteration"):
        assert detail[key] == original[key]
    restarted.tick()
    assert len(remote.reads) == 2
    with store._connect() as db:
        assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()
        due = db.execute("SELECT next_check_at FROM delivery_pr_observations").fetchone()[0]
        assert datetime.fromisoformat(due) == stamp + timedelta(days=1)


def test_same_status_revision_during_mirror_cannot_acknowledge_old_event(service):
    store, request, _ = published(service, active=True)
    remote = Remote()
    remote.state = "MERGED"
    mirror = remote.mirror

    def superseded(feature, fence):
        result = mirror(feature, fence)
        store.project(request["run_id"], phase="merged", execution_state="terminal",
                      event_type="delivered", message="Issue closure confirmed",
                      outcome="delivered")
        return result

    remote.mirror = superseded
    sync = ProjectSynchronizer([store], remote)
    assert sync.tick() == {}
    remote.mirror = mirror
    result = sync.tick()[request["issue_url"]]
    assert result["mirror"]["state"] == "consistent"
    assert remote.writes == ["Done", "Done"]


def test_request_arriving_during_remote_read_is_not_lost(service):
    store, _, receipt = published(service)
    remote = Remote()
    sync = ProjectSynchronizer([store], remote)
    sync.tick()
    sync.request_pr_refresh([receipt["url"]])
    read = remote.pull_request

    def another_request(url):
        sync.request_pr_refresh([url])
        return read(url)

    remote.pull_request = another_request
    sync.tick()
    with store._connect() as db:
        assert db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchone()
    remote.pull_request = read
    sync.tick()
    assert len(remote.reads) == 3
    with store._connect() as db:
        assert not db.execute("SELECT * FROM delivery_pr_refresh_requests").fetchall()
