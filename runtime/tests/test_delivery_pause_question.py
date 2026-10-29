"""Pause-enabled question sends remain fenced until their provenance is retained."""

import json

import pytest
from test_delivery_questions import THREAD, _question
from test_delivery_questions import intake_fixture as source_intake_fixture

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_question_sender import pump_blocking_questions


@pytest.fixture
def intake_fixture(tmp_path):
    return source_intake_fixture.__wrapped__(tmp_path)


def _controlled_intake_started(intake_fixture, tmp_path, monkeypatch, *, with_receipt=True):
    """Real store and process; SDK start identities are controlled fixture evidence."""
    from contextlib import contextmanager

    from test_delivery_questions import _queue_binary

    from devflow_temporal import delivery_execution_authority as authority
    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_namespace

    @contextmanager
    def controlled_namespace(_config):
        yield

    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", controlled_namespace)
    path, request = intake_fixture
    binary, log = _queue_binary(tmp_path)
    config = json.loads(path.read_text())
    config["codex_bin"] = str(binary)
    path.write_text(json.dumps(config))
    original = DeliveryConfig.admit

    def admit(config, supplied, **kwargs):
        return {**original(config, supplied, **kwargs), "pause_intake_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store = create_app(path).state.delivery.store
    store.submit({**request, "origin_thread_id": THREAD})
    with store._connect() as db:
        db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id='run-1'")
    spec = store.submitted_spec("run-1")
    item = store.pending_starts()[0]
    reservation = authority.reserve(store, item, spec)
    with pause.start_admission(
        store, "run-1", "delivery-run-1", 1, epoch=1, validate=lambda *_: None
    ):
        pass
    authority.bind(store, reservation, "fixture-execution")
    store.mark_start("run-1", accepted=True, expected_workflow_id="delivery-run-1")
    if with_receipt:
        import hashlib

        def sha(value):
            return hashlib.sha256((value or "").encode()).hexdigest()

        with store._connect() as db:
            start = db.execute("SELECT epoch FROM delivery_pause_steps WHERE root_id=? "
                               "AND step='start'", ("run-1",)).fetchone()
            db.execute("INSERT INTO delivery_pause_start_receipts VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
                "run-1", "delivery-run-1", "fixture-execution", 1, start[0],
                reservation["start_token"], reservation["request_digest"],
                sha(reservation["request_json"]), sha(reservation["recovery_json"]),
                sha(reservation["start_spec_json"]), "a" * 64))
    _question(store)
    return store, log


@pytest.mark.parametrize("tamper", ["outbox", "actor", "control", "authority", "start", "claim"])
def test_intake_sender_requires_every_original_start_binding(
    intake_fixture, tmp_path, monkeypatch, tamper
):
    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    edits = {
        "outbox": "UPDATE delivery_outbox SET state='unknown' WHERE run_id='run-1'",
        "actor": "INSERT INTO delivery_pause_actors VALUES "
                 "('run-1','other','other',1,'run-1')",
        "control": "UPDATE delivery_pause_control SET state='paused' WHERE root_id='run-1'",
        "authority": "UPDATE delivery_execution_authority SET execution_run_id='other' "
        "WHERE run_id='run-1'",
        "start": "DELETE FROM delivery_pause_steps WHERE root_id='run-1' AND step='start'",
        "claim": "UPDATE claims SET owner='external:devflow:other' WHERE work_id='work-1'",
    }
    with store._connect() as db:
        db.execute(edits[tamper])
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "pending"


def test_intake_pause_race_waits_for_popen_and_cannot_reopen_admission(
    intake_fixture, tmp_path, monkeypatch
):
    import threading

    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_question_sender as sender

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    popen = sender.subprocess.Popen
    with store._connect() as db:
        revision = db.execute("SELECT revision FROM delivery_pause_control WHERE root_id='run-1'")
        revision = revision.fetchone()[0]
    entered, finished = threading.Event(), threading.Event()
    rejected = []
    threads = []

    def pause_request():
        entered.set()
        try:
            pause.request_pause(store, "run-1", "late-pause", revision, intake_only=True)
        except ValueError as exc:
            rejected.append(str(exc))
        finally:
            finished.set()

    def observed_popen(*args, **kwargs):
        thread = threading.Thread(target=pause_request)
        threads.append(thread)
        thread.start()
        assert entered.wait(3)
        assert not finished.wait(0.1)
        return popen(*args, **kwargs)

    monkeypatch.setattr(sender.subprocess, "Popen", observed_popen)
    pump_blocking_questions(store)
    for thread in threads:
        thread.join(5)
    assert finished.is_set()
    assert rejected and "after start admission" in rejected[0]
    assert len(log.read_text().splitlines()) == 1
