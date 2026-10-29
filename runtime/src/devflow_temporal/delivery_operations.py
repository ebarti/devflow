"""Durable transport journal for existing delivery commands.

This journal owns transport identity only. The routed command retains all domain
validation, grants and effect receipts; an interrupted body is never relaunched.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import json
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from .contracts import digest

# Values are existing endpoint names and the domain stage, not new workflows.
KINDS = {
    "submit": ("submit", "admission"),
    "issue-context": ("refresh_issue_context", "execution"),
    "revise-feature-plan": ("revise_feature_plan", "admission"),
    "continue-feature": ("continue_feature", "admission"),
    "recover-publication": ("recover_publication", "admission"),
    "assess-transition": ("assess_transition", "observation"),
    "merge-preflight": ("merge_preflight", "observation"),
    "merge": ("merge_run", "admission"),
    "gates-only-preflight": ("gates_only_preflight", "observation"),
    "admit-gates-only": ("admit_gates_only", "admission"),
    "ci-inventory-preflight": ("ci_inventory_preflight", "observation"),
    "ack-ci-inventory": ("ack_ci_inventory", "acknowledgement"),
    "native-diagnostic-preflight": ("native_diagnostic_preflight", "observation"),
    "ack-native-diagnostic": ("ack_native_diagnostic", "acknowledgement"),
    "repair-admission-preflight": ("repair_admission_preflight", "observation"),
    "continue-repair": ("continue_repair", "admission"),
    "retry-prelaunch": ("retry_prelaunch", "admission"),
    "amend-scope": ("amend_scope", "admission"),
    "decision": ("decision", "execution"),
    "cancel": ("cancel", "execution"),
    "pause": ("request_pause", "execution"),
    "resume": ("request_resume", "execution"),
    "disable-limits": ("disable_delivery_limits", "execution"),
    "reconcile-tracker": ("reconcile_tracker", "execution"),
}
# These existing commands do not start the service and are observations or
# explicit acknowledgements of already-owned work.
NO_START = {
    "assess-transition",
    "merge-preflight",
    "gates-only-preflight",
    "ci-inventory-preflight",
    "native-diagnostic-preflight",
    "repair-admission-preflight",
    "ack-ci-inventory",
    "ack-native-diagnostic",
    "disable-limits",
}


def _domain_namespaces(kind):
    # These are the existing owners' command journals. A preflight is never
    # bound here, so its ID may be reused by its distinct acknowledgement.
    if kind in {"pause", "resume"}:
        return {"pause-commands"}
    if kind == "issue-context":
        return {"issue-context"}
    if kind in {"decision", "cancel", "disable-limits"}:
        return {"temporal-mutations"}
    if kind == "reconcile-tracker":
        return {"temporal-mutations", "delivery-commands"}
    return {"delivery-commands"}


def now() -> str:
    return datetime.now(UTC).isoformat()


def initialize(store) -> None:
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        db.execute("""CREATE TABLE IF NOT EXISTS delivery_operations (
            operation_id TEXT PRIMARY KEY, run_id TEXT, kind TEXT NOT NULL,
            stage TEXT NOT NULL, request_digest TEXT NOT NULL, request_json TEXT NOT NULL,
            state TEXT NOT NULL, owner_generation TEXT, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            progress TEXT NOT NULL, result_json TEXT, error TEXT, effect_refs TEXT)""")
        columns = {row[1] for row in db.execute("PRAGMA table_info(delivery_operations)")}
        if "domain_command_id" not in columns:
            db.execute("ALTER TABLE delivery_operations ADD COLUMN domain_command_id TEXT")
        # Older journals retain their exact requests; backfill the domain key
        # without rewriting or interpreting historical domain receipts.
        rows = db.execute(
            "SELECT operation_id,request_json FROM delivery_operations "
            "WHERE domain_command_id IS NULL AND stage != 'observation'"
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["request_json"])
            except (TypeError, ValueError):
                continue
            command_id = payload.get("command_id") if isinstance(payload, dict) else None
            if isinstance(command_id, str) and command_id:
                db.execute(
                    "UPDATE delivery_operations SET domain_command_id=? WHERE operation_id=?",
                    (command_id, row["operation_id"]),
                )
        db.execute(
            "CREATE INDEX IF NOT EXISTS delivery_operations_run "
            "ON delivery_operations(run_id, created_at)"
        )
        db.execute(
            "CREATE INDEX IF NOT EXISTS delivery_operations_domain_id "
            "ON delivery_operations(domain_command_id)"
        )


def _row(row) -> dict:
    value = dict(row)
    value["request"] = json.loads(value.pop("request_json"))
    value["result"] = json.loads(value.pop("result_json")) if value["result_json"] else None
    value["effect_refs"] = json.loads(value["effect_refs"]) if value["effect_refs"] else []
    value["authority"] = "current_authority_unverified" if value["state"] == "succeeded" else None
    value["next_action"] = {
        "pending": "wait",
        "running": "wait",
        "unknown": "reconcile",
        "succeeded": "inspect_result",
        "refused": "inspect_error",
        "failed": "inspect_error",
    }[value["state"]]
    if (value["kind"] == "resume" and value["state"] == "pending"
            and value["progress"] == "explicit resume required after restart"):
        value["next_action"] = "resubmit_same_operation"
    return value


def read(store, operation_id):
    with store._connect() as db:
        row = db.execute(
            "SELECT * FROM delivery_operations WHERE operation_id=?", (operation_id,)
        ).fetchone()
    if row is None:
        raise HTTPException(404, "operation not found")
    return _row(row)


def _lock(store, operation_id):
    # Database aliases and services with different state roots must fence the
    # same journal row with one OS lock.
    database = Path(store.config.tracking_db).resolve()
    database_key = hashlib.sha256(str(database).encode()).hexdigest()
    root = database.parent / ".delivery-operation-locks" / database_key
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / (hashlib.sha256(operation_id.encode()).hexdigest() + ".lock")
    fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def begin_temporal_mutation(service, run_id, command_id, name, payload):
    """Serialize domain admission against facade and legacy update callers."""
    from .delivery_operation_temporal import current_operation

    fd = _lock(service.store, "\x00domain:" + command_id)
    if fd is None:
        raise ValueError("this command is being admitted; inspect its original operation")
    try:
        current = current_operation(service)
        with service.store._connect() as db:
            rows = db.execute(
                "SELECT operation_id,state,kind FROM delivery_operations WHERE domain_command_id=?",
                (command_id,),
            ).fetchall()
            prior = db.execute(
                "SELECT state FROM delivery_mutations WHERE command_id=?",
                (command_id,),
            ).fetchone()
        operation_kind = {
            "disable_delivery_limits": "disable-limits",
            "reconcile_tracker": "reconcile-tracker",
        }.get(name, name)
        for row in rows:
            if (
                _domain_namespaces(operation_kind) & _domain_namespaces(row["kind"])
                and row["state"] in ("pending", "running", "unknown")
                and (current is None or current["operation_id"] != row["operation_id"])
            ):
                raise ValueError("command belongs to an unresolved durable operation; inspect it")
        if current is not None and prior is not None and prior["state"] in ("pending", "unknown"):
            raise ValueError("prior domain mutation is uncertain; reconcile it before another send")
        try:
            return service.store.begin_mutation(run_id, command_id, name, payload)
        except ValueError as exc:
            # begin_mutation performs no remote send and its database transaction
            # rolls back on rejection. Record that specific pre-send refusal;
            # uncertain prior mutations above deliberately do not take this path.
            if current is not None:
                with service.store._connect() as db:
                    timestamp = now()
                    db.execute(
                        "UPDATE delivery_operations SET state='refused',updated_at=?,finished_at=?,"
                        "progress='refused',error=? WHERE operation_id=? AND state='running' "
                        "AND owner_generation=? AND request_digest=?",
                        (
                            timestamp,
                            timestamp,
                            str(exc),
                            current["operation_id"],
                            current["owner_generation"],
                            current["request_digest"],
                        ),
                    )
            raise
    finally:
        os.close(fd)


def _temporal_refusal(store, operation):
    names = {
        "decision": "decision",
        "cancel": "cancel",
        "disable-limits": "disable_delivery_limits",
        "reconcile-tracker": "reconcile_tracker",
    }
    kind = names.get(operation["kind"])
    payload = operation["request"]
    command_id = payload.get("command_id")
    if kind is None or not isinstance(command_id, str):
        return None
    with store._connect() as db:
        row = db.execute(
            "SELECT request_digest,state,response_json FROM delivery_mutations "
            "WHERE command_id=? AND run_id=? AND kind=?",
            (command_id, operation["run_id"], kind),
        ).fetchone()
    if row is not None and row[0] == digest(payload) and row[1] == "rejected":
        return json.loads(row[2])["error"]
    return None


def _set(store, operation_id, state, *, result=None, error=None):
    with store._connect() as db:
        row = db.execute(
            "SELECT kind,run_id,request_json,effect_refs FROM delivery_operations "
            "WHERE operation_id=?",
            (operation_id,),
        ).fetchone()
        refs = json.loads(row["effect_refs"]) if row["effect_refs"] else []
        run_id = row["run_id"]
        command_id = json.loads(row["request_json"]).get("command_id")
        if state == "succeeded" and command_id:
            refs.append({"kind": row["kind"], "command_id": command_id})
        if row["kind"] == "submit" and isinstance(result, dict):
            run_id = result.get("run_id", run_id)
        db.execute(
            """UPDATE delivery_operations SET state=?, updated_at=?, finished_at=?,
                      progress=?, result_json=?, error=?, effect_refs=?, run_id=?
                      WHERE operation_id=?""",
            (
                state,
                now(),
                now(),
                state,
                json.dumps(result, sort_keys=True) if result is not None else None,
                error,
                json.dumps(refs, sort_keys=True),
                run_id,
                operation_id,
            ),
        )


def _request(service, payload):
    cookie = service.session.session()
    headers = [
        (b"host", urlsplit(service.config.dashboard_url).netloc.encode()),
        (b"origin", service.config.dashboard_url.rstrip("/").encode()),
        (b"cookie", ("devflow_session=" + cookie).encode()),
        (b"x-devflow-csrf", service.session.csrf(cookie).encode()),
        (b"content-type", b"application/json"),
    ]
    raw = json.dumps(payload).encode()

    async def receive():
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request(
        {
            "type": "http",
            "method": "POST",
            "path": "/",
            "headers": headers,
            "client": ("127.0.0.1", 1),
            "server": ("127.0.0.1", 1),
            "scheme": "http",
            "query_string": b"",
        },
        receive,
    )


def _execute(service, endpoints, operation_id):
    # Lock belongs to this OS thread through the entire handler, including its
    # to_thread work. Cancelling the request or event loop cannot release it.
    fd = _lock(service.store, operation_id)
    if fd is None:
        return
    try:
        with service.store._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT * FROM delivery_operations WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None or row["state"] != "pending":
                return
            generation = uuid.uuid4().hex
            db.execute(
                "UPDATE delivery_operations SET state='running', owner_generation=?, "
                "started_at=?, updated_at=?, progress='running' WHERE operation_id=?",
                (generation, now(), now(), operation_id),
            )
            operation = _row(row)
            operation["owner_generation"] = generation
        endpoint = endpoints[KINDS[operation["kind"]][0]]
        request = _request(service, operation["request"])

        async def invoke():
            from .delivery_operation_temporal import enter, leave

            token = enter(service, operation)
            try:
                if operation["kind"] == "submit":
                    return await endpoint(request)
                return await endpoint(request, operation["run_id"])
            finally:
                leave(token)

        try:
            result = asyncio.run(invoke())
        except HTTPException as exc:
            if read(service.store, operation_id)["state"] == "refused":
                return
            rejection = _temporal_refusal(service.store, operation)
            if rejection is not None:
                _set(service.store, operation_id, "refused", error=rejection)
                return
            state = (
                "refused"
                if exc.status_code in (400, 403, 404, 422)
                or (KINDS[operation["kind"]][1] == "observation" and exc.status_code == 409)
                else "unknown"
            )
            _set(service.store, operation_id, state, error=str(exc.detail))
        except Exception as exc:
            _set(
                service.store,
                operation_id,
                "unknown",
                error=f"{type(exc).__name__}: {str(exc)[:500]}",
            )
        else:
            _set(service.store, operation_id, "succeeded", result=result)
    finally:
        os.close(fd)


def _recover_abandoned(store, operation_id):
    fd = _lock(store, operation_id)
    if fd is None:
        return False
    try:
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_operations SET state='unknown', "
                "progress='reconciliation required', updated_at=?, "
                "error='owner stopped before recording a result' "
                "WHERE operation_id=? AND state='running'",
                (now(), operation_id),
            )
    finally:
        os.close(fd)
    return True


def register_operations(app, service, hostguard, mutationguard):
    endpoints = {route.name: route.endpoint for route in app.routes if hasattr(route, "endpoint")}
    initialize(service.store)
    tasks = set()

    def dispatch(operation_id):
        task = asyncio.create_task(asyncio.to_thread(_execute, service, endpoints, operation_id))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def resume_pending():
        with service.store._connect() as db:
            # A resume queued but not admitted before restart must not
            # open a pause without another explicit submission of the same
            # operation. Running/unknown commands still reconcile by receipt.
            db.execute(
                "UPDATE delivery_operations SET progress=?,updated_at=? "
                "WHERE kind='resume' AND state='pending'",
                ("explicit resume required after restart", now()),
            )
            rows = db.execute(
                "SELECT operation_id,state,kind FROM delivery_operations "
                "WHERE state IN ('pending','running')"
            ).fetchall()
        for row in rows:
            if row["state"] == "pending" and row["kind"] != "resume":
                dispatch(row["operation_id"])
            elif row["state"] == "running":
                await asyncio.to_thread(_recover_abandoned, service.store, row["operation_id"])

    service.resume_pending_operations = resume_pending

    @app.post("/api/operations")
    async def create_operation(request: Request):
        hostguard(request)
        supplied = await request.json()
        if not isinstance(supplied, dict) or set(supplied) != {
            "operation_id",
            "run_id",
            "kind",
            "request",
        }:
            raise HTTPException(400, "operation_id, run_id, kind and request are required")
        op, run_id, kind, payload = (
            supplied[k] for k in ("operation_id", "run_id", "kind", "request")
        )
        if (
            not isinstance(op, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", op)
            or not isinstance(kind, str)
            or kind not in KINDS
            or not isinstance(payload, dict)
        ):
            raise HTTPException(400, "invalid operation identity, kind or request")
        if (kind == "submit" and run_id is not None) or (
            kind != "submit" and (not isinstance(run_id, str) or not run_id)
        ):
            raise HTTPException(400, "run_id is required except for submit")
        if KINDS[kind][1] != "observation" or kind == "issue-context":
            mutationguard(request)
        request_digest = digest({"run_id": run_id, "kind": kind, "request": payload})
        command_id = payload.get("command_id")
        if KINDS[kind][1] == "observation" or not isinstance(command_id, str) or not command_id:
            command_id = None
        selected_id = op
        with service.store._connect() as db:
            # This transaction serializes aliases across processes. The domain
            # command is distinct from the transport operation ID and is only
            # bound within the owner's domain; preflight/ACK can legally reuse it.
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT operation_id,request_digest,state FROM delivery_operations "
                "WHERE operation_id=?",
                (op,),
            ).fetchone()
            if prior and prior["request_digest"] != request_digest:
                raise HTTPException(409, "operation ID already belongs to a different request")
            if prior is None and command_id is not None:
                candidates = db.execute(
                    "SELECT operation_id,kind,request_digest,state FROM delivery_operations "
                    "WHERE domain_command_id=? ORDER BY created_at,operation_id",
                    (command_id,),
                ).fetchall()
                aliases = [
                    row
                    for row in candidates
                    if _domain_namespaces(kind) & _domain_namespaces(row["kind"])
                ]
                if len(aliases) > 1:
                    raise HTTPException(409, "multiple prior operations require reconciliation")
                if aliases:
                    prior = aliases[0]
                    if prior["kind"] != kind or prior["request_digest"] != request_digest:
                        raise HTTPException(
                            409, "domain command ID already belongs to a different request"
                        )
                    selected_id = prior["operation_id"]
            if prior is None:
                timestamp = now()
                db.execute(
                    """INSERT INTO delivery_operations
                    (operation_id,run_id,kind,stage,request_digest,request_json,state,
                     created_at,updated_at,progress,domain_command_id)
                    VALUES (?,?,?,?,?,?,'pending',?,?,'queued',?)""",
                    (
                        op,
                        run_id,
                        kind,
                        KINDS[kind][1],
                        request_digest,
                        json.dumps(payload, sort_keys=True),
                        timestamp,
                        timestamp,
                        command_id,
                    ),
                )
                pending = True
            else:
                pending = prior["state"] == "pending"
        if pending:
            dispatch(selected_id)
        operation = await asyncio.to_thread(read, service.store, selected_id)
        return JSONResponse(
            operation,
            status_code=200 if operation["state"] in ("succeeded", "refused", "failed") else 202,
        )

    @app.get("/api/operations/{operation_id}")
    async def operation_status(request: Request, operation_id: str):
        hostguard(request)
        operation = await asyncio.to_thread(read, service.store, operation_id)
        if operation["state"] == "running":
            await asyncio.to_thread(_recover_abandoned, service.store, operation_id)
            operation = await asyncio.to_thread(read, service.store, operation_id)
        return operation

    @app.get("/api/runs/{run_id}/operations")
    async def list_operations(request: Request, run_id: str):
        hostguard(request)

        def read_page():
            def rows_for_run():
                with service.store._connect() as db:
                    return db.execute(
                        "SELECT * FROM delivery_operations WHERE run_id=? "
                        "ORDER BY created_at DESC LIMIT 100",
                        (run_id,),
                    ).fetchall()

            rows = rows_for_run()
            running = [row["operation_id"] for row in rows if row["state"] == "running"]
            for operation_id in running:
                _recover_abandoned(service.store, operation_id)
            if running:
                rows = rows_for_run()
            return {"operations": [_row(row) for row in rows]}

        return await asyncio.to_thread(read_page)

    @app.post("/api/operations/{operation_id}/reconcile")
    async def reconcile_operation(request: Request, operation_id: str):
        mutationguard(request)
        owner_settled = await asyncio.to_thread(_recover_abandoned, service.store, operation_id)
        operation = await asyncio.to_thread(read, service.store, operation_id)
        if not owner_settled:
            return operation
        if operation["state"] != "unknown":
            return operation
        payload, run_id, kind = (
            operation["request"],
            operation["run_id"],
            operation["kind"],
        )
        command_id = payload.get("command_id")
        result = None
        refusal = None
        temporal_pending = False
        with service.store._connect() as db:
            if kind == "ack-native-diagnostic" and isinstance(command_id, str):
                row = db.execute(
                    "SELECT request_digest FROM delivery_native_diagnostics "
                    "WHERE run_id=? AND command_id=?",
                    (run_id, command_id),
                ).fetchone()
                valid = row is not None and row[0] == digest(payload)
            elif kind == "ack-ci-inventory" and isinstance(command_id, str):
                row = db.execute(
                    "SELECT request_digest FROM delivery_ci_inventories "
                    "WHERE run_id=? AND command_id=?",
                    (run_id, command_id),
                ).fetchone()
                valid = row is not None and row[0] == digest({"run_id": run_id, **payload})
            elif kind in ("submit", "merge") and isinstance(command_id, str):
                domain_run = payload.get("run_id") if kind == "submit" else run_id
                expected = (
                    digest({key: value for key, value in payload.items() if key != "command_id"})
                    if kind == "submit"
                    else digest({"kind": "merge_run", "run_id": run_id, "request": payload})
                )
                row = db.execute(
                    "SELECT request_digest,response_json FROM delivery_commands "
                    "WHERE run_id=? AND command_id=?",
                    (domain_run, command_id),
                ).fetchone()
                valid = row is not None and row[0] == expected
                if valid:
                    result = json.loads(row[1])
            elif kind in (
                "decision",
                "cancel",
                "disable-limits",
                "reconcile-tracker",
            ) and isinstance(command_id, str):
                domain_kind = {
                    "disable-limits": "disable_delivery_limits",
                    "reconcile-tracker": "reconcile_tracker",
                }.get(kind, kind)
                row = db.execute(
                    "SELECT request_digest,state,response_json FROM delivery_mutations "
                    "WHERE run_id=? AND command_id=? AND kind=?",
                    (run_id, command_id, domain_kind),
                ).fetchone()
                valid = row is not None and row[0] == digest(payload)
                if valid and row[1] == "complete":
                    result = json.loads(row[2])
                elif valid and row[1] == "rejected":
                    refusal = json.loads(row[2])["error"]
                elif valid and row[1] in ("pending", "unknown"):
                    temporal_pending = True
            else:
                valid = False
        if valid and kind == "ack-native-diagnostic":
            from .delivery_native_diagnostic import info

            result = await asyncio.to_thread(info, service.store, run_id, command_id)
        elif valid and kind == "ack-ci-inventory":
            from .delivery_ci_inventory import info

            result = await asyncio.to_thread(info, service.store, run_id)
        if temporal_pending and kind == "reconcile-tracker":
            from .delivery_terminal_recovery import receipt

            try:
                saved = await asyncio.to_thread(receipt, service.store, run_id, payload)
                if saved is not None:
                    await asyncio.to_thread(service.store.finish_mutation, command_id, saved)
                    result, temporal_pending = saved, False
            except (ValueError, OSError, RuntimeError):
                # A conflicting or unavailable receipt cannot establish success.
                temporal_pending = False
        if temporal_pending:
            from .delivery_operation_temporal import reconcile

            result, refusal = await reconcile(service, operation)
        if result is None and refusal is None:
            from .delivery_operation_receipts import receipt

            try:
                result = await asyncio.to_thread(receipt, service.store, operation)
            except (ValueError, OSError, RuntimeError, KeyError, TypeError):
                # A missing, changed or unavailable owner receipt stays unknown.
                pass
        if result is not None:
            await asyncio.to_thread(_set, service.store, operation_id, "succeeded", result=result)
        elif refusal is not None:
            await asyncio.to_thread(_set, service.store, operation_id, "refused", error=refusal)
        return await asyncio.to_thread(read, service.store, operation_id)
