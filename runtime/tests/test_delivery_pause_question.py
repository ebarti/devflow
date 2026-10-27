"""Pause-enabled question sends remain fenced until their provenance is retained."""

import json

import pytest
from test_delivery_questions import BLOCKER, THREAD, _question
from test_delivery_questions import intake_fixture as source_intake_fixture

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_question_sender import pump_blocking_questions


@pytest.fixture
def intake_fixture(tmp_path):
    return source_intake_fixture.__wrapped__(tmp_path)


class Queue:
    def __init__(self):
        self.messages = []

    def send(self, binary, thread, message):
        self.messages.append((binary, thread, message))
        return {"thread_id": thread, "queued_submission_id": "fixture"}


def test_pause_controlled_question_is_retained_without_starving_legacy(intake_fixture, monkeypatch):
    path, request = intake_fixture
    original = DeliveryConfig.admit

    def admit(config, supplied, **kwargs):
        spec = original(config, supplied, **kwargs)
        if supplied["run_id"] == "run-1":
            spec["pause_control_version"] = 1
        return spec

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store = create_app(path).state.delivery.store
    store.submit({**request, "origin_thread_id": THREAD})
    store.submit(
        {
            **request,
            "run_id": "run-2",
            "work_id": "work-2",
            "command_id": "cmd-2",
            "issue_url": "https://github.com/example/fixture/issues/4",
            "branch": "feat/other",
            "origin_thread_id": THREAD,
        }
    )
    _question(store)
    decision = {
        "id": "run-2:question:1",
        "kind": "question",
        "revision": 1,
        "candidate_revision": 1,
        "prompt": "Another issue?",
        "options": ["Yes"],
        "state": "pending",
        "blocker": BLOCKER,
    }
    store.project(
        "run-2",
        phase="waiting_question",
        execution_state="waiting",
        event_type="question_pending",
        message="Blocking clarification needed",
        decision=decision,
        protocol_revision=4,
        key="question:1",
    )
    queue = Queue()
    pump_blocking_questions(store, queue)
    pump_blocking_questions(store, queue)
    assert len(queue.messages) == 1
    assert "Another issue?" in queue.messages[0][2]
    assert store.question_notifications("run-1")[0]["state"] == "pending"
    assert store.question_notifications("run-2")[0]["state"] == "queued"


def test_owner_changed_after_claim_stays_unknown_and_never_sends(intake_fixture, monkeypatch):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    store.submit({**request, "origin_thread_id": THREAD})
    _question(store)
    original_spec = store.spec

    def changed_spec(run_id):
        return {**original_spec(run_id), "pause_control_version": 1}

    monkeypatch.setattr(store, "spec", changed_spec)
    queue = Queue()
    pump_blocking_questions(store, queue)
    pump_blocking_questions(store, queue)
    assert queue.messages == []
    assert store.question_notifications("run-1")[0]["state"] == "unknown"


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


def test_intake_started_question_sends_once_with_owned_process(
    intake_fixture, tmp_path, monkeypatch
):
    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    pump_blocking_questions(store)
    pump_blocking_questions(store)
    assert len(log.read_text().splitlines()) == 1
    assert store.question_notifications("run-1")[0]["state"] == "queued"


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


def test_intake_owner_change_after_claim_never_reaches_popen(intake_fixture, tmp_path, monkeypatch):
    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    original = store.question_notification_current

    def changed_after_check(item):
        current = original(item)
        with store._connect() as db:
            db.execute("UPDATE delivery_outbox SET state='unknown' WHERE run_id='run-1'")
        return current

    monkeypatch.setattr(store, "question_notification_current", changed_after_check)
    pump_blocking_questions(store)
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "unknown"


def test_child_is_reaped_if_handoff_exit_fails(tmp_path, monkeypatch):
    from contextlib import contextmanager

    from test_delivery_questions import _queue_binary

    from devflow_temporal import delivery_question_sender as sender

    binary, _ = _queue_binary(tmp_path, mode="timeout")
    real_popen = sender.subprocess.Popen
    processes = []

    def capture(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        return process

    @contextmanager
    def failing_handoff():
        yield
        raise ValueError("fence exit failed")

    monkeypatch.setattr(sender.subprocess, "Popen", capture)
    with pytest.raises(ValueError, match="fence exit failed"):
        sender.CodexQuestionQueue().send(str(binary), THREAD, "question", handoff=failing_handoff())
    assert len(processes) == 1 and processes[0].poll() is not None


def test_unavailable_namespace_retains_pending_intake_question(
    intake_fixture, tmp_path, monkeypatch
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause_namespace

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)

    @contextmanager
    def unavailable(_config):
        raise ValueError("namespace custody unavailable")
        yield

    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", unavailable)
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "pending"


def test_intake_post_spawn_fence_failure_reaps_child_and_records_unknown(
    intake_fixture, tmp_path, monkeypatch
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause_question as pause_question
    from devflow_temporal import delivery_question_sender as sender

    store, _ = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    original_handoff, real_popen = pause_question.intake_handoff, sender.subprocess.Popen
    processes = []

    @contextmanager
    def failing(store, item, spec):
        with original_handoff(store, item, spec):
            yield
        raise ValueError("handoff exit failed")

    def capture(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(pause_question, "intake_handoff", failing)
    monkeypatch.setattr(sender.subprocess, "Popen", capture)
    pump_blocking_questions(store)
    pump_blocking_questions(store)
    assert len(processes) == 1 and processes[0].poll() is not None
    assert store.question_notifications("run-1")[0]["state"] == "unknown"


def test_intake_data_version_change_before_root_fence_blocks_spawn(
    intake_fixture, tmp_path, monkeypatch
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause as pause

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    original_mutex = pause.root_mutex

    @contextmanager
    def changed_before_lock(target, root):
        with store._connect() as db:
            db.execute("UPDATE delivery_outbox SET state='unknown' WHERE run_id='run-1'")
        with original_mutex(target, root):
            yield

    monkeypatch.setattr(pause, "root_mutex", changed_before_lock)
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "unknown"


def test_unavailable_namespace_does_not_abandon_intake_dispatch(
    intake_fixture, tmp_path, monkeypatch
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause_namespace

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    assert store.claim_question_notification(allow_intake=True) is not None

    @contextmanager
    def unavailable(_config):
        raise ValueError("namespace custody unavailable")
        yield

    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", unavailable)
    pump_blocking_questions(store)
    assert not log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "dispatching"


def test_intake_final_receipt_is_written_inside_namespace(intake_fixture, tmp_path, monkeypatch):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause_namespace

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    active = []

    @contextmanager
    def namespace(_config):
        active.append(True)
        try:
            yield
        finally:
            active.pop()

    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", namespace)
    original = store.finish_question_notification

    def checked_finish(*args):
        assert active == [True]
        return original(*args)

    monkeypatch.setattr(store, "finish_question_notification", checked_finish)
    pump_blocking_questions(store)
    assert log.exists()
    assert store.question_notifications("run-1")[0]["state"] == "queued"


def test_namespace_exit_failure_keeps_exact_committed_receipt_without_unscoped_write(
    intake_fixture, tmp_path, monkeypatch, caplog
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_pause_namespace

    store, log = _controlled_intake_started(intake_fixture, tmp_path, monkeypatch)
    finished = []

    @contextmanager
    def failing_exit(_config):
        yield
        if finished:
            raise ValueError("namespace exit failed")

    monkeypatch.setattr(delivery_pause_namespace, "intake_namespace_scope", failing_exit)
    original = store.finish_question_notification

    def marked_finish(*args):
        original(*args)
        finished.append(True)

    monkeypatch.setattr(store, "finish_question_notification", marked_finish)
    pump_blocking_questions(store)
    row = store.question_notifications("run-1")[0]
    assert len(log.read_text().splitlines()) == 1
    assert row["state"] == "queued" and row["receipt"]["thread_id"] == THREAD
    assert "Question sender custody unavailable" in caplog.text
