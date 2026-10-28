"""The intake-only sender may use a retained exact non-projecting start ACK."""

import hashlib

from test_delivery_pause_question import _controlled_intake_started
from test_delivery_pause_question import intake_fixture as intake_fixture

from devflow_temporal.delivery_question_sender import pump_blocking_questions


def _no_actor_start(store, *, receipt=True):
    """Controlled SQL start identities; public SDK coverage is separate."""
    with store._connect() as db:
        db.execute("DELETE FROM delivery_pause_actor_status WHERE root_id='run-1'")
        db.execute("DELETE FROM delivery_pause_actors WHERE root_id='run-1'")
        db.execute("UPDATE delivery_pause_control SET execution_run_id='' WHERE root_id='run-1'")
        authority = dict(db.execute("SELECT * FROM delivery_execution_authority").fetchone())
        start = dict(db.execute("SELECT * FROM delivery_pause_steps WHERE step='start'").fetchone())
        if receipt:
            def sha(value):
                return hashlib.sha256((value or "").encode()).hexdigest()

            db.execute("INSERT INTO delivery_pause_start_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
                "run-1", authority["workflow_id"], authority["execution_run_id"], 1,
                start["epoch"], authority["start_token"], authority["request_digest"],
                sha(authority["request_json"]), sha(authority["recovery_json"]),
                sha(authority["start_spec_json"]), "a" * 64))


def test_start_receipt_is_checked_before_writer_and_not_reparsed_under_handoff(
    intake_fixture, tmp_path, monkeypatch,
):
    import sqlite3

    from devflow_temporal import delivery_pause_question

    store, log = _controlled_intake_started(
        intake_fixture, tmp_path, monkeypatch, with_receipt=False)
    _no_actor_start(store)
    original = delivery_pause_question._intake_start_receipt
    calls = []

    def verify_outside_writer(db, *args):
        probe = sqlite3.connect(store.config.tracking_db, timeout=0)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.rollback()
        finally:
            probe.close()
        calls.append(True)
        return original(db, *args)

    monkeypatch.setattr(delivery_pause_question, "_intake_start_receipt", verify_outside_writer)
    pump_blocking_questions(store)
    assert calls and len(log.read_text().splitlines()) == 1


def test_receipt_pages_advance_past_backlog_and_oversized_row(
    intake_fixture, tmp_path, monkeypatch,
):
    store, log = _controlled_intake_started(
        intake_fixture, tmp_path, monkeypatch, with_receipt=False)
    _no_actor_start(store)
    with store._connect() as db:
        item = dict(db.execute("SELECT * FROM delivery_question_notifications").fetchone())
        import json

        for number in range(65):
            stale = {**json.loads(item["question_json"]), "id": f"old-{number}"}
            db.execute("INSERT INTO delivery_question_notifications VALUES (?,?,?,?,?,?,?,?,?)", (
                f"fixture-{number:03d}", "run-1", f"old-{number}", 1, item["thread_id"],
                "x" * (300 * 1024) if number == 0 else json.dumps(stale),
                "pending", None, "0"))
    for _ in range(12):
        pump_blocking_questions(store)
        if log.exists():
            break
    assert log.exists() and len(log.read_text().splitlines()) == 1
    with store._connect() as db:
        rows = db.execute("SELECT state FROM delivery_question_notifications "
                          "WHERE notification_id=?", (item["notification_id"],)).fetchone()
        assert rows[0] == "queued"
        assert db.execute("SELECT state FROM delivery_question_notifications "
                          "WHERE notification_id='fixture-000'").fetchone()[0] == "pending"
