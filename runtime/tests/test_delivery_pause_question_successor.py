"""Intake question ownership after a separately authenticated recovery start."""

import json

import pytest
from test_delivery_pause_question import _controlled_intake_started
from test_delivery_pause_question import intake_fixture as intake_fixture

from devflow_temporal import delivery_execution_authority as authority
from devflow_temporal.delivery_question_sender import pump_blocking_questions


def _successor(store):
    """Controlled SQL selection and start binding; SDK coverage is separate."""
    recovery = json.dumps({"kind": "owned-recovery"}, sort_keys=True, separators=(",", ":"))
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET workflow_id=?,recovery_json=? WHERE run_id=?",
                   ("delivery-run-1-successor", recovery, "run-1"))
        db.execute("UPDATE delivery_outbox SET state='pending' WHERE run_id='run-1'")
    spec = store.spec("run-1")
    reservation = authority.reserve(store, store.pending_starts()[0], spec)
    authority.bind(store, reservation, "successor-sdk")
    store.mark_start("run-1", accepted=True, expected_workflow_id="delivery-run-1-successor")


def test_intake_successor_uses_original_receipt_and_selected_owner(intake_fixture, tmp_path,
                                                                 monkeypatch):
    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    _successor(store)
    pump_blocking_questions(store)
    pump_blocking_questions(store)
    assert len(log.read_text().splitlines()) == 1
    assert store.question_notifications("run-1")[0]["state"] == "queued"


@pytest.mark.parametrize("tamper", ["outbox", "claim", "recovery", "execution", "token",
                                    "control", "original", "actor"])
def test_intake_successor_refuses_changed_authority(intake_fixture, tmp_path, monkeypatch, tamper):
    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    _successor(store)
    changes = {
        "outbox": "UPDATE delivery_outbox SET state='unknown' WHERE run_id='run-1'",
        "claim": "UPDATE claims SET owner='external:devflow:other' WHERE work_id='work-1'",
        "recovery": "UPDATE delivery_runs SET recovery_json='{}' WHERE run_id='run-1'",
        "execution": "UPDATE delivery_execution_authority SET execution_run_id=NULL "
                     "WHERE workflow_id='delivery-run-1-successor'",
        "token": "UPDATE delivery_execution_authority SET start_token='' "
                 "WHERE workflow_id='delivery-run-1-successor'",
        "control": "UPDATE delivery_pause_control SET state='paused' WHERE root_id='run-1'",
        "original": "UPDATE delivery_execution_authority SET execution_run_id='other' "
                    "WHERE workflow_id='delivery-run-1'",
        "actor": "INSERT INTO delivery_pause_actors VALUES ('run-1','other','other',1,'run-1')",
    }
    with store._connect() as db:
        db.execute(changes[tamper])
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "pending"
