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


@pytest.mark.parametrize("replacement", [False, True])
def test_start_receipt_schema_refuses_missing_or_forged_trigger(service, replacement):
    from devflow_temporal.delivery_pause_dispatch import require_start_receipt_schema

    store, _ = service
    with store._connect() as db:
        require_start_receipt_schema(db)
        db.execute("DROP TRIGGER pause_start_receipt_no_replace")
        if replacement:
            db.execute("""CREATE TRIGGER pause_start_receipt_no_replace
                BEFORE INSERT ON delivery_pause_start_receipts BEGIN SELECT 1; END""")
        with pytest.raises(ValueError, match="schema or protection"):
            require_start_receipt_schema(db)


def test_sent_outbox_cannot_acquire_retroactive_start_receipt(service, monkeypatch):
    store, request = service
    admit_original = DeliveryConfig.admit

    def admit(config, *args, **kwargs):
        return {**admit_original(config, *args, **kwargs), "pause_intake_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store.submit(request)
    _controlled_resume(store, request["run_id"])
    run_id, workflow_id = request["run_id"], "delivery-" + request["run_id"]
    authority.reserve(store, store.pending_starts()[0], store.effective_spec(run_id))
    original = {**original_request(store, run_id),
                "sdk_start_proof": {"event1_sha256": "a" * 64}}
    identity = {"workflow_id": workflow_id, "execution_run_id": "sdk-run"}
    with pause.start_admission(store, run_id, workflow_id, 1, epoch=1,
                               validate=lambda *_: None):
        pass
    with store._connect() as db:
        db.execute("UPDATE delivery_outbox SET state='sent' WHERE run_id=?", (run_id,))
    with pytest.raises(ValueError, match="atomic provenance"):
        _acknowledge_selected_start(store, original["spec"], identity, original)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts").fetchone()[0] == 0


def _selected_start(service, monkeypatch):
    store, request = service
    admit_original = DeliveryConfig.admit

    def admit(config, *args, **kwargs):
        return {**admit_original(config, *args, **kwargs), "pause_intake_version": 1}

    monkeypatch.setattr(DeliveryConfig, "admit", admit)
    store.submit(request)
    _controlled_resume(store, request["run_id"])
    run_id, workflow_id = request["run_id"], "delivery-" + request["run_id"]
    authority.reserve(store, store.pending_starts()[0], store.effective_spec(run_id))
    original = {**original_request(store, run_id),
                "sdk_start_proof": {"event1_sha256": "a" * 64}}
    identity = {"workflow_id": workflow_id, "execution_run_id": "sdk-run"}
    with pause.start_admission(store, run_id, workflow_id, 1, epoch=1,
                               validate=lambda *_: None):
        pass
    return store, original, identity


def test_start_ack_writer_never_decodes_or_hashes(service, monkeypatch):
    import hashlib
    import json
    from contextlib import contextmanager

    store, original, identity = _selected_start(service, monkeypatch)
    root_mutex = pause.root_mutex
    old_loads, old_hash = json.loads, hashlib.sha256
    held = False

    @contextmanager
    def track_root(*args, **kwargs):
        nonlocal held
        with root_mutex(*args, **kwargs):
            held = True
            try:
                yield
            finally:
                held = False

    def guarded_loads(*args, **kwargs):
        assert not held, "JSON decoded while root writer held"
        return old_loads(*args, **kwargs)

    def guarded_hash(*args, **kwargs):
        assert not held, "SHA computed while root writer held"
        return old_hash(*args, **kwargs)

    monkeypatch.setattr(pause, "root_mutex", track_root)
    monkeypatch.setattr(json, "loads", guarded_loads)
    monkeypatch.setattr(hashlib, "sha256", guarded_hash)
    assert _acknowledge_selected_start(store, original["spec"], identity, original)["state"] == (
        "start_recorded")


@pytest.mark.parametrize("kind", ["aba", "unrelated"])
def test_start_ack_refuses_any_intervening_database_commit(service, monkeypatch, kind):
    from contextlib import contextmanager

    store, original, identity = _selected_start(service, monkeypatch)
    with store._connect() as db:
        db.execute("CREATE TABLE owned_unrelated (value INTEGER)")
        db.execute("INSERT INTO owned_unrelated VALUES (0)")
    root_mutex = pause.root_mutex

    @contextmanager
    def changed_before_root(*args, **kwargs):
        if kind == "aba":
            for revision in (42, 1):
                with store._connect() as db:
                    db.execute("UPDATE delivery_pause_control SET revision=? WHERE root_id=?",
                               (revision, original["spec"]["run_id"]))
        else:
            with store._connect() as db:
                db.execute("UPDATE owned_unrelated SET value=1")
        with root_mutex(*args, **kwargs):
            yield

    monkeypatch.setattr(pause, "root_mutex", changed_before_root)
    with pytest.raises(ValueError, match="authority changed"):
        _acknowledge_selected_start(store, original["spec"], identity, original)
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts").fetchone()[0] == 0
        assert db.execute("SELECT state FROM delivery_outbox WHERE run_id=?",
                          (original["spec"]["run_id"],)).fetchone()[0] == "pending"


def test_bounded_start_original_refuses_oversize_before_json_decode(service, monkeypatch):
    import json
    import sqlite3

    store, original, _ = _selected_start(service, monkeypatch)
    run_id = original["spec"]["run_id"]
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET request_json=? WHERE run_id=?",
                   ('"' + 'x' * (4 * 1024 * 1024) + '"', run_id))
    called = []
    old = json.loads

    def decode(*args, **kwargs):
        called.append(True)
        return old(*args, **kwargs)

    monkeypatch.setattr(json, "loads", decode)
    with pytest.raises(sqlite3.DataError):
        original_request(store, run_id, bounded=True)
    assert called == []


def test_start_authority_recheck_uses_only_selected_primary_keys(service, monkeypatch):
    from devflow_temporal.delivery_pause_dispatch import _RAW_ORIGINAL_SQL

    store, original, identity = _selected_start(service, monkeypatch)
    run_id, workflow_id = original["spec"]["run_id"], identity["workflow_id"]
    reservation = original["execution_authority"]
    parameters = (
        workflow_id, run_id, "delivery-" + run_id, workflow_id,
        reservation["request_digest"], reservation["request_json"], reservation["recovery_json"],
        run_id, reservation["request_digest"], reservation["request_json"],
        reservation["recovery_json"], reservation["start_spec_json"], reservation["start_token"],
        reservation["generation"], identity["execution_run_id"],
    )
    with store._connect() as db:
        plan = [row[3] for row in db.execute("EXPLAIN QUERY PLAN " + _RAW_ORIGINAL_SQL,
                                             parameters)]
    assert len(plan) == 2
    assert all("SEARCH " in line and "INDEX" in line for line in plan)


@pytest.mark.asyncio
async def test_start_ack_refuses_aba_during_sdk_authentication(service, monkeypatch):
    import asyncio

    from devflow_temporal import delivery_pause_dispatch as dispatch

    store, original, identity = _selected_start(service, monkeypatch)

    def change_and_restore():
        for revision in (42, 1):
            with store._connect() as db:
                db.execute("UPDATE delivery_pause_control SET revision=? WHERE root_id=?",
                           (revision, original["spec"]["run_id"]))

    async def controlled_sdk(*_args, **_kwargs):
        # The SDK seam only supplies the already selected proof in this unit test.
        await asyncio.to_thread(change_and_restore)
        return original

    monkeypatch.setattr(dispatch, "authenticate_original_start", controlled_sdk)
    with pytest.raises(ValueError, match="changed during SDK observation"):
        await dispatch.bind_start(store, original["spec"], identity["workflow_id"],
                                  identity["execution_run_id"])
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_pause_start_receipts").fetchone()[0] == 0
        assert db.execute("SELECT state FROM delivery_outbox WHERE run_id=?",
                          (original["spec"]["run_id"],)).fetchone()[0] == "pending"


def test_start_ack_holds_sqlite_writer_until_receipt_commit(service, monkeypatch):
    import sqlite3

    from devflow_temporal import delivery_pause_dispatch as dispatch

    store, original, identity = _selected_start(service, monkeypatch)
    schema_check = dispatch.require_start_receipt_schema
    blocked = []

    def check_and_contend(db):
        schema_check(db)
        with sqlite3.connect(store.config.tracking_db, timeout=0) as other:
            with pytest.raises(sqlite3.OperationalError, match="locked"):
                other.execute("BEGIN IMMEDIATE")
        blocked.append(True)

    monkeypatch.setattr(dispatch, "require_start_receipt_schema", check_and_contend)
    assert _acknowledge_selected_start(store, original["spec"], identity, original)["state"] == (
        "start_recorded")
    assert blocked == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize("extra", [
    {"pause_intake_version": True},
    {"pause_intake_version": 2},
    {"pause_intake_version": None},
    {"pause_intake_version": 1, "pause_control_version": 1},
    {"pause_intake_version": 1, "feature_delivery": {"owner": "other"}},
    {"pause_intake_version": 1, "feature_worker": {"owner": "other"}},
    {"pause_intake_version": 1, "execution_authority_version": True},
])
async def test_start_receipt_refuses_non_intake_or_malformed_markers(extra):
    from devflow_temporal.delivery_pause_dispatch import bind_start

    spec = {"run_id": "r", "execution_authority_version": 1, **extra}
    with pytest.raises(ValueError, match="exact ordinary intake authority"):
        await bind_start(None, spec, "delivery-r", "sdk-run")
