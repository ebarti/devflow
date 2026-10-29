"""Queued pause transitions need no unused notification journal."""

import pytest
from test_delivery_pause_intake import (
    begin_intake_settlement,
    controlled_no_start,
)
from test_delivery_pause_intake import (
    unstarted as unstarted,
)
from test_delivery_store import service as service

from devflow_temporal import delivery_pause as pause
from devflow_temporal import delivery_pause_intake as intake


@pytest.mark.parametrize("legacy_table", [False, True])
def test_intake_pause_resume_without_notification_journal(unstarted, legacy_table):
    store, spec, run = unstarted
    root = spec["run_id"]
    with store._connect() as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                          ("delivery_pause_notifications",)).fetchone() is None
        if legacy_table:
            db.execute("CREATE TABLE delivery_pause_notifications "
                       "(root_id TEXT NOT NULL, revision INTEGER NOT NULL, state TEXT NOT NULL, "
                       "PRIMARY KEY(root_id, revision))")
            db.execute("INSERT INTO delivery_pause_notifications VALUES (?, 73, 'pending')",
                       (root,))
        pause.initialize(db)

    first = pause.request_pause(store, root, "pause", 0, intake_only=True)
    assert pause.request_pause(store, root, "pause", 0, intake_only=True) == first
    begin_intake_settlement(store, root)
    intake.seal_unstarted(store, root, 1, run["revision"],
                          prove_unstarted=controlled_no_start(store))
    paused = pause.publish_paused(store, root, 1)
    resume = pause.begin_resume(store, root, "resume", paused["revision"], 1)
    assert pause.begin_resume(store, root, "resume", paused["revision"], 1) == resume
    active = intake.activate_unstarted(store, root, 1, run["revision"],
                                       prove_unstarted=controlled_no_start(store))
    assert active["state"] == "running"
    with store._connect() as db:
        table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                           ("delivery_pause_notifications",)).fetchone()
        assert bool(table) is legacy_table
        if legacy_table:
            assert [tuple(row) for row in db.execute(
                "SELECT root_id, revision, state FROM delivery_pause_notifications")] == [
                    (root, 73, "pending")]
        outbox = db.execute("SELECT state FROM delivery_outbox WHERE run_id=?", (root,))
        assert outbox.fetchone()[0] == "pending"
        assert store.state.claim_for(db, run["work_id"])["owner"] == "external:devflow:" + root
