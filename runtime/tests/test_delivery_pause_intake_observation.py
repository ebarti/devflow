"""Owned intake snapshots are observed before root admission and reject SQL ABA."""

from contextlib import contextmanager

import pytest
from test_delivery_pause_intake import begin_intake_settlement, controlled_no_start
from test_delivery_pause_intake import unstarted as source_unstarted
from test_delivery_store import service as service

from devflow_temporal import delivery_pause as pause
from devflow_temporal import delivery_pause_intake as intake


@pytest.fixture
def unstarted(service, monkeypatch):
    return source_unstarted.__wrapped__(service, monkeypatch)


@pytest.mark.parametrize("action", ["seal", "activate"])
def test_intake_snapshot_rejects_aba_between_observation_and_fence(unstarted, monkeypatch, action):
    store, spec, row = unstarted
    root = spec["run_id"]
    pause.request_pause(store, root, "pause", 0, intake_only=True)
    begin_intake_settlement(store, root)
    if action == "activate":
        intake.seal_unstarted(
            store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
        )
        paused = pause.publish_paused(store, root, 1)
        pause.begin_resume(store, root, "resume", paused["revision"], 1)
    original = pause.root_mutex

    @contextmanager
    def changed_before_lock(owner, run_id):
        # Two independent committed writes restore the exact original row.
        for state in ("unknown", "pending"):
            with store._connect() as db:
                db.execute("UPDATE delivery_outbox SET state=? WHERE run_id=?", (state, root))
        with original(owner, run_id):
            yield

    monkeypatch.setattr(pause, "root_mutex", changed_before_lock)
    with pytest.raises(ValueError, match="changed before acknowledgement"):
        getattr(intake, f"{action}_unstarted")(
            store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
        )
    with store._connect() as db:
        control = pause._row(db, root)
        assert control["state"] == ("pausing" if action == "seal" else "resuming")
        if action == "seal":
            assert control["checkpoint_json"] is None
        else:
            interval = db.execute(
                "SELECT resumed_at FROM delivery_pause_intervals WHERE root_id=? AND epoch=1",
                (root,),
            ).fetchone()
            assert interval[0] is None


@pytest.mark.parametrize("action", ["seal", "activate"])
def test_intake_capture_and_hash_do_not_run_under_root(unstarted, monkeypatch, action):
    store, spec, row = unstarted
    root = spec["run_id"]
    pause.request_pause(store, root, "pause", 0, intake_only=True)
    begin_intake_settlement(store, root)
    if action == "activate":
        intake.seal_unstarted(
            store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
        )
        paused = pause.publish_paused(store, root, 1)
        pause.begin_resume(store, root, "resume", paused["revision"], 1)
    original_mutex, original_capture, original_digest = (
        pause.root_mutex,
        intake._capture,
        intake.digest,
    )
    held = []

    @contextmanager
    def observed_mutex(owner, run_id):
        with original_mutex(owner, run_id):
            held.append(True)
            try:
                yield
            finally:
                held.pop()

    def capture(*args, **kwargs):
        assert not held
        return original_capture(*args, **kwargs)

    def digest(value):
        assert not held
        return original_digest(value)

    monkeypatch.setattr(pause, "root_mutex", observed_mutex)
    monkeypatch.setattr(intake, "_capture", capture)
    monkeypatch.setattr(intake, "digest", digest)
    result = getattr(intake, f"{action}_unstarted")(
        store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
    )
    assert result["resumable"] if action == "seal" else result["state"] == "running"


def test_intake_seal_replays_same_receipt_before_and_after_paused(unstarted):
    store, spec, row = unstarted
    root = spec["run_id"]
    pause.request_pause(store, root, "pause", 0, intake_only=True)
    begin_intake_settlement(store, root)
    receipt = intake.seal_unstarted(
        store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
    )
    assert (
        intake.seal_unstarted(
            store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
        )
        == receipt
    )
    pause.publish_paused(store, root, 1)
    assert (
        intake.seal_unstarted(
            store, root, 1, row["revision"], prove_unstarted=controlled_no_start(store)
        )
        == receipt
    )
