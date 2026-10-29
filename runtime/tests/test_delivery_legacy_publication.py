"""Tracking-only bridges retain explicit custody and immutable legacy outcomes."""

from datetime import UTC, datetime, timedelta

import pytest
from test_delivery_project_sync import Remote, published
from test_delivery_store import service as delivery_service

from devflow_temporal.delivery_legacy_publication import bind
from devflow_temporal.delivery_project_sync import ProjectSynchronizer

service = delivery_service


def unpublished_retry(service, *, active=False):
    store, request, receipt = published(service)
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        store.state.release_work(db, request["work_id"], "external:devflow:" + request["run_id"])
    retry = {**request, "run_id": "run-2", "work_id": "work-2", "command_id": "command-2"}
    store.submit(retry)
    if not active:
        store.project(retry["run_id"], phase="blocked", execution_state="blocked",
                      event_type="blocked", message="Baseline failed", outcome="blocked")
    return store, request, receipt, retry


def test_explicit_binding_reconciles_known_pr_without_changing_legacy_runs(service):
    store, request, receipt, retry = unpublished_retry(service)
    remote = Remote()
    remote.state = "MERGED"
    stamp = datetime(2026, 10, 10, tzinfo=UTC)
    sync = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    assert sync.tick()[request["issue_url"]]["status"] == "Blocked"
    with store._connect() as db:
        original = [dict(row) for row in db.execute("SELECT * FROM delivery_runs ORDER BY run_id")]
    recorded = bind(sync, request["run_id"])
    assert recorded["projection_run_id"] == retry["run_id"]
    assert recorded["publication_run_id"] == request["run_id"]
    assert recorded["pull_requests"] == [receipt["url"]]
    stamp += timedelta(minutes=1)
    restarted = ProjectSynchronizer([store], remote, clock=lambda: stamp)
    result = restarted.tick()[request["issue_url"]]
    assert result["run_id"] == retry["run_id"] and result["status"] == "Done"
    assert result["mirror"]["state"] == "consistent"
    assert result["legacy_publication_binding"]["run_id"] == request["run_id"]
    # New explicit custody triggers readback before the daily deadline.
    assert len(remote.reads) == 2
    with store._connect() as db:
        assert original == [dict(row) for row in db.execute("SELECT * FROM delivery_runs "
                                                           "ORDER BY run_id")]
        events = db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0]
    assert store.detail(retry["run_id"])["pull_request"] is None
    assert store.detail(retry["run_id"])["outcome"] == "blocked"
    assert store.detail(request["run_id"])["pull_request"] == receipt
    assert bind(restarted, request["run_id"]) == recorded
    restarted.tick()
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM delivery_feature_events").fetchone()[0] == events
    assert len(remote.reads) == len(remote.writes) == 2


@pytest.mark.parametrize("change", ["receipt", "retry_resumed"])
def test_reference_changed_during_mirror_cannot_acknowledge_stale_binding(service, change):
    store, request, receipt, retry = unpublished_retry(service)
    remote = Remote()
    remote.state = "MERGED"
    sync = ProjectSynchronizer([store], remote)
    bind(sync, request["run_id"])
    mirror = remote.mirror

    def changed_during_mirror(feature, fence):
        if change == "receipt":
            # A supported writer updates the older publisher without changing
            # the selected retry's cached feature/version.
            store.project(request["run_id"], phase="delivered", execution_state="terminal",
                          event_type="published", message="Updated retained publication receipt",
                          outcome="published_unmerged",
                          pull_request={**receipt, "head": "c" * 40})
        else:
            # Historical writers predate the bridge and can resume either side.
            target = request if change == "publisher_resumed" else retry
            with store._connect() as db:
                db.execute("UPDATE delivery_runs SET phase='queued',execution_state='running',"
                           "outcome=NULL WHERE run_id=?", (target["run_id"],))
        return mirror(feature, fence)

    remote.mirror = changed_during_mirror
    assert sync.tick() == {}
    with store._connect() as db:
        pending = dict(db.execute("SELECT * FROM delivery_project_outbox").fetchone())
    assert pending["state"] == "pending"
    assert pending["receipt_json"] is None and pending["checked_at"] is None
    assert store.detail(retry["run_id"])["feature"]["mirror"]["state"] == "pending"
    remote.mirror = mirror
    result = sync.tick()[request["issue_url"]]
    assert result["status"] == ("Queued" if change == "retry_resumed" else "Blocked")
    assert "legacy_publication_binding" not in result
    assert result["mirror"]["state"] == "consistent"
