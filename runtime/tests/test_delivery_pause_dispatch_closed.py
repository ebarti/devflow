"""Closed reconciliation rechecks durable ownership and never creates an actor."""
import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_execution_authority as authority
from devflow_temporal import delivery_pause as pause
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_pause_dispatch import _acknowledge_selected_start
from devflow_temporal.delivery_pause_execution import original_request


def _controlled_resume(store, run_id):
    # Isolate receipt mechanics after the separate intake-resume protocol.
    with store._connect() as db:
        db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id=?", (run_id,))


@pytest.mark.parametrize("changed", ["missing_start", "owner", "sealed", "stale_epoch"])
def test_closed_start_refuses_changed_owner_or_missing_admission(service, monkeypatch, changed):
    store, request = service
    original_admit = DeliveryConfig.admit

    def admit(config, *args, **kwargs):
        return {**original_admit(config, *args, **kwargs), "pause_intake_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store.submit(request)
    _controlled_resume(store, request["run_id"])
    run_id = request["run_id"]
    workflow_id = "delivery-" + run_id
    authority.reserve(store, store.pending_starts()[0], store.effective_spec(run_id))
    identity = {"workflow_id": workflow_id, "execution_run_id": "owned-sdk-run"}
    original = {**original_request(store, run_id),
                "sdk_start_proof": {"event1_sha256": "a" * 64}}
    spec = original["spec"]
    if changed != "missing_start":
        with pause.start_admission(store, run_id, workflow_id, 1, epoch=1,
                                   validate=lambda *_: None):
            pass
    with store._connect() as db:
        if changed == "owner":
            db.execute("UPDATE delivery_pause_control SET owner_generation=2 WHERE root_id=?",
                       (run_id,))
        elif changed == "sealed":
            db.execute("UPDATE delivery_pause_control SET state='paused' WHERE root_id=?",
                       (run_id,))
        elif changed == "stale_epoch":
            db.execute("UPDATE delivery_pause_control SET epoch=epoch+1 WHERE root_id=?",
                       (run_id,))
    with pytest.raises(ValueError):
        _acknowledge_selected_start(store, spec, identity, original)
    with store._connect() as db:
        assert db.execute("SELECT execution_run_id FROM delivery_pause_control WHERE root_id=?",
                          (run_id,)).fetchone()[0] == ""


def test_start_receipt_is_immutable_and_exact_replay_is_idempotent(service, monkeypatch):
    import sqlite3

    store, request = service
    admit_original = DeliveryConfig.admit

    def admit(config, *args, **kwargs):
        return {**admit_original(config, *args, **kwargs), "pause_intake_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store.submit(request)
    _controlled_resume(store, request["run_id"])
    run_id = request["run_id"]
    workflow_id = "delivery-" + run_id
    authority.reserve(store, store.pending_starts()[0], store.effective_spec(run_id))
    original = {**original_request(store, run_id),
                "sdk_start_proof": {"event1_sha256": "a" * 64}}
    identity = {"workflow_id": workflow_id, "execution_run_id": "sdk-run"}
    with pause.start_admission(store, run_id, workflow_id, 1, epoch=1,
                               validate=lambda *_: None):
        pass
    assert _acknowledge_selected_start(store, original["spec"], identity, original)["state"] == (
        "start_recorded")
    with store._connect() as db:
        receipt = tuple(db.execute("SELECT * FROM delivery_pause_start_receipts").fetchone())
        outbox = tuple(db.execute("SELECT * FROM delivery_outbox WHERE run_id=?", (run_id,))
                       .fetchone())
    assert _acknowledge_selected_start(store, original["spec"], identity, original)["state"] == (
        "start_recorded")
    with store._connect() as db:
        saved = db.execute("SELECT * FROM delivery_pause_start_receipts").fetchone()
        assert tuple(saved) == receipt
        assert tuple(db.execute("SELECT * FROM delivery_outbox WHERE run_id=?", (run_id,))
                     .fetchone()) == outbox
        assert db.execute("PRAGMA recursive_triggers").fetchone()[0] == 0
        for statement in (
            "UPDATE delivery_pause_start_receipts SET start_epoch=1",
            "DELETE FROM delivery_pause_start_receipts",
            "INSERT OR REPLACE INTO delivery_pause_start_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ):
            with pytest.raises(sqlite3.IntegrityError, match="immutable pause start receipt"):
                db.execute(statement, receipt if "INSERT" in statement else ())
