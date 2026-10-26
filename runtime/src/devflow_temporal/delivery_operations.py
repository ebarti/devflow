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
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

from .contracts import digest

# Values are existing endpoint names and the domain stage, not new workflows.
KINDS = {
    'submit': ('submit', 'admission'),
    'issue-context': ('refresh_issue_context', 'observation'),
    'revise-feature-plan': ('revise_feature_plan', 'admission'),
    'continue-feature': ('continue_feature', 'admission'),
    'recover-publication': ('recover_publication', 'admission'),
    'merge-preflight': ('merge_preflight', 'observation'),
    'merge': ('merge_run', 'admission'),
    'gates-only-preflight': ('gates_only_preflight', 'observation'),
    'admit-gates-only': ('admit_gates_only', 'admission'),
    'ci-inventory-preflight': ('ci_inventory_preflight', 'observation'),
    'ack-ci-inventory': ('ack_ci_inventory', 'acknowledgement'),
    'native-diagnostic-preflight': ('native_diagnostic_preflight', 'observation'),
    'ack-native-diagnostic': ('ack_native_diagnostic', 'acknowledgement'),
    'repair-admission-preflight': ('repair_admission_preflight', 'observation'),
    'continue-repair': ('continue_repair', 'admission'),
    'retry-prelaunch': ('retry_prelaunch', 'admission'),
    'amend-scope': ('amend_scope', 'admission'),
    'decision': ('decision', 'execution'),
    'cancel': ('cancel', 'execution'),
    'disable-limits': ('disable_delivery_limits', 'execution'),
    'reconcile-tracker': ('reconcile_tracker', 'execution'),
}
# These existing commands do not start the service and are observations or
# explicit acknowledgements of already-owned work.
NO_START = {'merge-preflight', 'gates-only-preflight', 'ci-inventory-preflight',
            'native-diagnostic-preflight', 'repair-admission-preflight',
            'ack-ci-inventory', 'ack-native-diagnostic', 'disable-limits'}


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initialize(store) -> None:
    with store._connect() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS delivery_operations (
            operation_id TEXT PRIMARY KEY, run_id TEXT, kind TEXT NOT NULL,
            stage TEXT NOT NULL, request_digest TEXT NOT NULL, request_json TEXT NOT NULL,
            state TEXT NOT NULL, owner_generation TEXT, created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT,
            progress TEXT NOT NULL, result_json TEXT, error TEXT, effect_refs TEXT)''')
        db.execute('CREATE INDEX IF NOT EXISTS delivery_operations_run ON delivery_operations(run_id, created_at)')


def _row(row) -> dict:
    value = dict(row)
    value['request'] = json.loads(value.pop('request_json'))
    value['result'] = json.loads(value.pop('result_json')) if value['result_json'] else None
    value['effect_refs'] = json.loads(value['effect_refs']) if value['effect_refs'] else []
    value['authority'] = 'current_authority_unverified' if value['state'] == 'succeeded' else None
    value['next_action'] = {'pending': 'wait', 'running': 'wait', 'unknown': 'reconcile',
                            'succeeded': 'inspect_result', 'refused': 'inspect_error',
                            'failed': 'inspect_error'}[value['state']]
    return value


def read(store, operation_id):
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_operations WHERE operation_id=?', (operation_id,)).fetchone()
    if row is None:
        raise HTTPException(404, 'operation not found')
    return _row(row)


def _lock(store, operation_id):
    root = Path(store.config.state_root) / 'operation-locks'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / (hashlib.sha256(operation_id.encode()).hexdigest() + '.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    return fd


def _set(store, operation_id, state, *, result=None, error=None):
    with store._connect() as db:
        db.execute('''UPDATE delivery_operations SET state=?, updated_at=?, finished_at=?,
                      progress=?, result_json=?, error=? WHERE operation_id=?''',
                   (state, now(), now(), state, json.dumps(result, sort_keys=True) if result is not None else None,
                    error, operation_id))


def _request(service, payload):
    cookie = service.session.session()
    headers = [(b'host', service.config.dashboard_url.split('://', 1)[-1].rstrip('/').encode()),
               (b'origin', service.config.dashboard_url.rstrip('/').encode()),
               (b'cookie', ('devflow_session=' + cookie).encode()),
               (b'x-devflow-csrf', service.session.csrf(cookie).encode()),
               (b'content-type', b'application/json')]
    raw = json.dumps(payload).encode()
    async def receive():
        return {'type': 'http.request', 'body': raw, 'more_body': False}
    return Request({'type': 'http', 'method': 'POST', 'path': '/', 'headers': headers,
                    'client': ('127.0.0.1', 1), 'server': ('127.0.0.1', 1),
                    'scheme': 'http', 'query_string': b''}, receive)


def _execute(service, endpoints, operation_id):
    # Lock belongs to this OS thread through the entire handler, including its
    # to_thread work. Cancelling the request or event loop cannot release it.
    fd = _lock(service.store, operation_id)
    if fd is None:
        return
    try:
        with service.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM delivery_operations WHERE operation_id=?', (operation_id,)).fetchone()
            if row is None or row['state'] != 'pending':
                return
            generation = uuid.uuid4().hex
            db.execute("UPDATE delivery_operations SET state='running', owner_generation=?, started_at=?, updated_at=?, progress='running' WHERE operation_id=?",
                       (generation, now(), now(), operation_id))
            operation = _row(row)
        endpoint = endpoints[KINDS[operation['kind']][0]]
        request = _request(service, operation['request'])
        async def invoke():
            if operation['kind'] == 'submit':
                return await endpoint(request)
            return await endpoint(request, operation['run_id'])
        try:
            result = asyncio.run(invoke())
        except HTTPException as exc:
            state = 'refused' if exc.status_code in (400, 403, 404, 409, 422) else 'unknown'
            _set(service.store, operation_id, state, error=str(exc.detail))
        except Exception as exc:
            _set(service.store, operation_id, 'unknown', error=f'{type(exc).__name__}: {str(exc)[:500]}')
        else:
            _set(service.store, operation_id, 'succeeded', result=result)
    finally:
        os.close(fd)


def _recover_abandoned(store, operation_id):
    fd = _lock(store, operation_id)
    if fd is None:
        return
    try:
        with store._connect() as db:
            db.execute("UPDATE delivery_operations SET state='unknown', progress='reconciliation required', updated_at=?, error='owner stopped before recording a result' WHERE operation_id=? AND state='running'", (now(), operation_id))
    finally:
        os.close(fd)


def register_operations(app, service, hostguard, mutationguard):
    endpoints = {route.name: route.endpoint for route in app.routes if hasattr(route, 'endpoint')}
    initialize(service.store)
    tasks = set()
    def dispatch(operation_id):
        task = asyncio.create_task(asyncio.to_thread(_execute, service, endpoints, operation_id))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def resume_pending():
        with service.store._connect() as db:
            rows = db.execute("SELECT operation_id,state FROM delivery_operations WHERE state IN ('pending','running')").fetchall()
        for row in rows:
            if row['state'] == 'pending':
                dispatch(row['operation_id'])
            else:
                await asyncio.to_thread(_recover_abandoned, service.store, row['operation_id'])

    service.resume_pending_operations = resume_pending

    @app.post('/api/operations')
    async def create_operation(request: Request):
        hostguard(request)
        supplied = await request.json()
        if not isinstance(supplied, dict) or set(supplied) != {'operation_id', 'run_id', 'kind', 'request'}:
            raise HTTPException(400, 'operation_id, run_id, kind and request are required')
        op, run_id, kind, payload = (supplied[k] for k in ('operation_id', 'run_id', 'kind', 'request'))
        if not isinstance(op, str) or not 1 <= len(op) <= 200 or not isinstance(kind, str) or kind not in KINDS or not isinstance(payload, dict):
            raise HTTPException(400, 'invalid operation identity, kind or request')
        if (kind == 'submit' and run_id is not None) or (kind != 'submit' and (not isinstance(run_id, str) or not run_id)):
            raise HTTPException(400, 'run_id is required except for submit')
        if KINDS[kind][1] != 'observation' or kind == 'issue-context':
            mutationguard(request)
        request_digest = digest({'run_id': run_id, 'kind': kind, 'request': payload})
        with service.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT request_digest FROM delivery_operations WHERE operation_id=?', (op,)).fetchone()
            if prior and prior[0] != request_digest:
                raise HTTPException(409, 'operation ID already belongs to a different request')
            if not prior:
                timestamp = now()
                db.execute('''INSERT INTO delivery_operations
                    (operation_id,run_id,kind,stage,request_digest,request_json,state,created_at,updated_at,progress)
                    VALUES (?,?,?,?,?,?,'pending',?,?,'queued')''',
                    (op, run_id, kind, KINDS[kind][1], request_digest,
                     json.dumps(payload, sort_keys=True), timestamp, timestamp))
        if not prior:
            dispatch(op)
        operation = await asyncio.to_thread(read, service.store, op)
        return JSONResponse(operation, status_code=200 if operation['state'] in ('succeeded','refused','failed') else 202)

    @app.get('/api/operations/{operation_id}')
    async def operation_status(request: Request, operation_id: str):
        hostguard(request)
        return await asyncio.to_thread(read, service.store, operation_id)

    @app.get('/api/runs/{run_id}/operations')
    async def list_operations(request: Request, run_id: str):
        hostguard(request)
        with service.store._connect() as db:
            rows = db.execute('SELECT * FROM delivery_operations WHERE run_id=? ORDER BY created_at DESC LIMIT 100', (run_id,)).fetchall()
        return {'operations': [_row(row) for row in rows]}

    @app.post('/api/operations/{operation_id}/reconcile')
    async def reconcile_operation(request: Request, operation_id: str):
        mutationguard(request)
        await asyncio.to_thread(_recover_abandoned, service.store, operation_id)
        operation = await asyncio.to_thread(read, service.store, operation_id)
        if operation['state'] != 'unknown':
            return operation
        payload, run_id, kind = operation['request'], operation['run_id'], operation['kind']
        command_id = payload.get('command_id')
        result = None
        with service.store._connect() as db:
            if kind == 'ack-native-diagnostic' and isinstance(command_id, str):
                row = db.execute('SELECT request_digest FROM delivery_native_diagnostics WHERE run_id=? AND command_id=?', (run_id, command_id)).fetchone()
                valid = row is not None and row[0] == digest(payload)
            elif kind == 'ack-ci-inventory' and isinstance(command_id, str):
                row = db.execute('SELECT request_digest FROM delivery_ci_inventories WHERE run_id=? AND command_id=?', (run_id, command_id)).fetchone()
                valid = row is not None and row[0] == digest({'run_id': run_id, **payload})
            elif kind in ('decision', 'cancel', 'disable-limits', 'reconcile-tracker') and isinstance(command_id, str):
                domain_kind = {'disable-limits': 'disable_delivery_limits', 'reconcile-tracker': 'reconcile_tracker'}.get(kind, kind)
                row = db.execute('SELECT request_digest,state,response_json FROM delivery_mutations WHERE run_id=? AND command_id=? AND kind=?', (run_id, command_id, domain_kind)).fetchone()
                valid = row is not None and row[0] == digest(payload) and row[1] == 'complete'
                if valid:
                    result = json.loads(row[2])
            else:
                valid = False
        if valid and kind == 'ack-native-diagnostic':
            from .delivery_native_diagnostic import info
            result = await asyncio.to_thread(info, service.store, run_id, command_id)
        elif valid and kind == 'ack-ci-inventory':
            from .delivery_ci_inventory import info
            result = await asyncio.to_thread(info, service.store, run_id)
        if result is not None:
            await asyncio.to_thread(_set, service.store, operation_id, 'succeeded', result=result)
        return await asyncio.to_thread(read, service.store, operation_id)
