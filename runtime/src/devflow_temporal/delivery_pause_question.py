"""Fail-closed custody boundary for pause-enabled blocking questions.

The existing notification does not retain the authenticated SDK projection
activity and its subprocess can outlive the sender. Therefore no notification
row, including an apparent acknowledgement, certifies pause settlement yet.
"""


def pause_controlled(spec):
    """Unknown pause protocol versions must not fall through to legacy sending."""
    return "pause_control_version" in spec or "pause_intake_version" in spec


def intake_only(spec):
    return (
        isinstance(spec, dict)
        and "pause_control_version" not in spec
        and type(spec.get("pause_intake_version")) is int
        and spec["pause_intake_version"] == 1
    )


def intake_started(db, item, spec):
    """Check the history-bound start records for the intake-only protocol.

    This read does not admit a send. The handoff rechecks under the root lock.
    """
    if (
        not isinstance(spec, dict)
        or "pause_control_version" in spec
        or type(spec.get("pause_intake_version")) is not int
        or spec["pause_intake_version"] != 1
        or spec.get("feature_delivery")
        or spec.get("feature_worker")
        or type(spec.get("execution_authority_version")) is not int
        or spec["execution_authority_version"] != 1
    ):
        return False
    run_id = item["run_id"]
    row = db.execute(
        """SELECT r.request_json,r.request_digest,r.recovery_json,r.workflow_id,r.work_id,
        o.state AS outbox_state,c.state AS control_state,c.epoch,c.workflow_id AS control_workflow,
        c.execution_run_id,c.owner_generation
        FROM delivery_runs r JOIN delivery_outbox o USING(run_id)
        JOIN delivery_pause_control c ON c.root_id=r.run_id WHERE r.run_id=?""",
        (run_id,),
    ).fetchone()
    if row is None:
        return False
    row = dict(row)
    original_workflow = "delivery-" + run_id
    workflow = row["workflow_id"] or original_workflow
    if (
        row["outbox_state"] != "sent"
        or row["control_state"] != "running"
        or row["control_workflow"] != original_workflow
        or row["execution_run_id"]
        or row["owner_generation"] != 1
    ):
        return False
    claim = db.execute("SELECT owner FROM claims WHERE work_id=?", (row["work_id"],)).fetchone()
    if claim is None or claim[0] != "external:devflow:" + run_id:
        return False
    original = db.execute(
        "SELECT * FROM delivery_execution_authority WHERE workflow_id=?", (original_workflow,)
    ).fetchone()
    start = db.execute(
        """SELECT * FROM delivery_pause_steps WHERE root_id=?
        AND operation_key=? AND step='start'""",
        (run_id, "start:" + original_workflow),
    ).fetchone()
    if original is None or start is None:
        return False
    original, start = dict(original), dict(start)
    if (
        original["run_id"] != run_id
        or original["request_json"] != row["request_json"]
        or original["request_digest"] != row["request_digest"]
        or original["recovery_json"] is not None
        or original["start_spec_json"] != row["request_json"]
        or not original["start_token"]
        or original["generation"] != 1
        or not original["execution_run_id"]
        or start["workflow_id"] != original_workflow
        or start["execution_run_id"] != ""
        or start["owner_generation"] != 1
        or start["epoch"] != row["epoch"]
        or not _intake_start_receipt(db, item, row, original, start, original_workflow)
    ):
        return False
    if workflow == original_workflow:
        return row["recovery_json"] is None

    # A later recovery is outside queued-intake pause scope. Its own exact SDK
    # start must be selected and bound, with the original claim still held.
    import json

    try:
        recovery = json.loads(row["recovery_json"] or "null")
        if not isinstance(recovery, dict) or not recovery:
            return False
        selected = db.execute(
            "SELECT * FROM delivery_execution_authority WHERE workflow_id=?", (workflow,)
        ).fetchone()
        if selected is None:
            return False
        selected = dict(selected)
        if (selected["run_id"] != run_id or not selected["execution_run_id"]
                or not selected["start_token"] or selected["generation"] != 1
                or selected["request_json"] != row["request_json"]
                or selected["request_digest"] != row["request_digest"]
                or selected["recovery_json"] != row["recovery_json"]):
            return False
        from .delivery_execution_authority import require_current

        require_current(db, spec, {
            "workflow_id": workflow, "execution_run_id": selected["execution_run_id"],
            "run_id": run_id, "request_digest": row["request_digest"], "generation": 1,
        }, require_claim=True)
        return True
    except (ValueError, TypeError, KeyError):
        return False



def _intake_start_receipt(db, item, row, authority, start, workflow):
    """Authenticate the original non-projecting start without a pause actor."""
    import hashlib
    import sqlite3

    try:
        from .delivery_pause_dispatch import require_start_receipt_schema

        require_start_receipt_schema(db)
        for table in ("delivery_pause_actors", "delivery_pause_actor_status"):
            if db.execute(f"SELECT 1 FROM {table} WHERE root_id=? LIMIT 1",
                          (item["run_id"],)).fetchone():
                return False
        saved = db.execute("SELECT * FROM delivery_pause_start_receipts WHERE root_id=?",
                           (item["run_id"],)).fetchone()
        if saved is None:
            return False
        receipt = dict(saved)

        def sha(value):
            return hashlib.sha256((value or "").encode()).hexdigest()

        event = receipt["event1_sha256"]
        return bool(
            receipt["workflow_id"] == workflow
            and receipt["execution_run_id"] == authority["execution_run_id"]
            and receipt["owner_generation"] == 1
            and receipt["start_epoch"] == start["epoch"] == row["epoch"]
            and receipt["start_token"] == authority["start_token"]
            and receipt["request_digest"] == authority["request_digest"]
            and receipt["request_json_sha256"] == sha(authority["request_json"])
            and receipt["recovery_json_sha256"] == sha(authority["recovery_json"])
            and receipt["start_spec_sha256"] == sha(authority["start_spec_json"])
            and isinstance(event, str) and len(event) == 64
            and not set(event) - set("0123456789abcdef")
        )
    except (ImportError, KeyError, TypeError, ValueError, sqlite3.Error):
        return False


def intake_handoff(store, item, spec):
    """Hold the root and current ownership through the local process creation."""
    import sqlite3
    from contextlib import contextmanager

    from .delivery_pause import root_mutex

    @contextmanager
    def held():
        with store._connect() as db:
            db.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024 * 1024)
            version = db.execute("PRAGMA data_version").fetchone()[0]
            db.execute("BEGIN")
            saved = db.execute(
                "SELECT * FROM delivery_question_notifications WHERE notification_id=?",
                (item["notification_id"],),
            ).fetchone()
            fields = (
                "notification_id",
                "run_id",
                "decision_id",
                "decision_revision",
                "thread_id",
                "question_json",
            )
            if (
                saved is None
                or saved["state"] != "dispatching"
                or any(saved[key] != item[key] for key in fields)
                or store._spec(db, item["run_id"]) != spec
                or not intake_started(db, item, spec)
                or store._question_command_inflight(db, item)
                or not store._question_is_current(db, item)
            ):
                raise ValueError("intake question start authority changed before handoff")
            db.commit()
            if db.execute("PRAGMA data_version").fetchone()[0] != version:
                raise ValueError("intake question changed during observation")
            with root_mutex(store, item["run_id"]):
                db.execute("BEGIN IMMEDIATE")
                if db.execute("PRAGMA data_version").fetchone()[0] != version:
                    raise ValueError("intake question changed before handoff")
                yield
                db.commit()

    return held()


