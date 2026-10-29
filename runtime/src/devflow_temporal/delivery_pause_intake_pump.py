"""Bounded, restartable lifecycle for explicitly admitted unstarted owners.

A pump pass never starts a workflow or creates a resume intent. It advances only
an existing pause/resume intent using the SDK no-start proof and the original
intake checkpoint; uncertain observations leave the durable fence closed.
"""
from __future__ import annotations

import asyncio
import json

from . import delivery_pause as pause
from . import delivery_pause_intake as intake
from .delivery_pause_async import joined_thread
from .delivery_pause_context import intake_enabled

_MAX_BATCH = 100
_SDK_TIMEOUT_SECONDS = 45


def _selected(store, root_id):
    """Read one exact frozen owner; every mutating helper rechecks its fence."""
    with store._connect() as db:
        db.execute('BEGIN')
        _require_status(db)
        row = db.execute('''SELECT c.state,c.epoch,c.execution_run_id,c.checkpoint_json,
            r.revision AS run_revision,r.request_json,r.execution_state,r.outcome,
            o.state AS outbox_state FROM delivery_pause_control c
            JOIN delivery_runs r ON r.run_id=c.root_id
            JOIN delivery_outbox o ON o.run_id=r.run_id WHERE c.root_id=?''',
            (root_id,)).fetchone()
        if row is None:
            return None
        item = dict(row)
    spec = json.loads(item['request_json'])
    if not isinstance(spec, dict) or not intake_enabled(spec):
        return None
    if item['state'] not in {'pause_requested', 'pausing', 'resuming'}:
        return None
    if (item['execution_run_id'] or item['execution_state'] != 'queued'
            or item['outcome'] is not None or item['outbox_state'] not in {'pending', 'unknown'}):
        item['blocked'] = 'intake owner has already started or changed'
        return item
    if item['checkpoint_json'] is not None:
        checkpoint = json.loads(item['checkpoint_json'])
        if not isinstance(checkpoint, dict) or checkpoint.get('kind') != 'unstarted_intake':
            item['blocked'] = 'intake checkpoint belongs to another owner'
    elif item['state'] == 'resuming':
        item['blocked'] = 'intake resume checkpoint is unavailable'
    return item


def _begin(store, root_id, selected):
    """Atomically select an unstarted owner before entering settlement."""
    with pause.root_mutex(store, root_id), store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        control = pause._row(db, root_id)
        run = db.execute('''SELECT r.revision,r.request_json,r.execution_state,r.outcome,
            o.state AS outbox_state FROM delivery_runs r JOIN delivery_outbox o USING(run_id)
            WHERE r.run_id=?''', (root_id,)).fetchone()
        if (control['state'] != 'pause_requested' or control['epoch'] != selected['epoch']
                or control['execution_run_id'] or run is None
                or run['revision'] != selected['run_revision']
                or run['request_json'] != selected['request_json']
                or run['execution_state'] != 'queued' or run['outcome'] is not None
                or run['outbox_state'] not in {'pending', 'unknown'}):
            raise ValueError('intake owner changed before settlement')
        for table in ('delivery_pause_actors', 'delivery_pause_pending_merges',
                      'delivery_pause_feature_children', 'delivery_pause_steps'):
            if db.execute(f'SELECT 1 FROM {table} WHERE root_id=? LIMIT 1',
                          (root_id,)).fetchone():
                raise ValueError('intake has an admitted actor or handoff')
        pause.require_no_expiry(db, root_id, selected['epoch'])
        db.execute("""UPDATE delivery_pause_control SET state='pausing', revision=revision+1,
            updated_at=? WHERE root_id=?""", (pause._now(), root_id))
        return pause._row(db, root_id)


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_pause_intake_pump_status (
        root_id TEXT PRIMARY KEY, epoch INTEGER NOT NULL, reason TEXT NOT NULL,
        reason_type TEXT NOT NULL, updated_at TEXT NOT NULL)""")


def _require_status(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                      "name='delivery_pause_intake_pump_status'").fetchone():
        raise ValueError('intake pump status schema is unavailable')


def pending_reason(store, root_id):
    """Read a bounded diagnostic only while it matches the active fenced epoch."""
    with store._connect() as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND "
                            "name='delivery_pause_intake_pump_status'").fetchone()
        if not exists:
            return None
        row = db.execute("""SELECT s.epoch,s.reason,s.reason_type FROM
            delivery_pause_intake_pump_status s JOIN delivery_pause_control c
            ON c.root_id=s.root_id AND c.epoch=s.epoch WHERE s.root_id=?
            AND c.state IN ('pause_requested','pausing','resuming')""", (root_id,)).fetchone()
        return dict(row) if row else None


def _record_status(store, root_id, epoch, reason, reason_type):
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        _require_status(db)
        db.execute("""INSERT INTO delivery_pause_intake_pump_status
            (root_id,epoch,reason,reason_type,updated_at)
            SELECT root_id,epoch,?,?,? FROM delivery_pause_control
            WHERE root_id=? AND epoch=? AND state IN ('pause_requested','pausing','resuming')
            ON CONFLICT(root_id) DO UPDATE SET epoch=excluded.epoch,reason=excluded.reason,
              reason_type=excluded.reason_type,updated_at=excluded.updated_at""",
            (reason, reason_type, pause._now(), root_id, epoch))


def _clear_status(store, root_id, epoch):
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        _require_status(db)
        db.execute('DELETE FROM delivery_pause_intake_pump_status WHERE root_id=? AND epoch=?',
                   (root_id, epoch))


def _reason(exc):
    if isinstance(exc, TimeoutError):
        return 'Temporal no-start observation timed out; intake remains fenced'
    if isinstance(exc, (OSError, ConnectionError)):
        return 'Intake custody or Temporal connection is unavailable; intake remains fenced'
    if isinstance(exc, ValueError):
        return 'Intake ownership, checkpoint, or no-start proof is not confirmed'
    return 'Intake settlement requires another authenticated observation'


async def advance(store, root_id, *, client=None):
    """Advance at most one durable transition for one marked owner.

    ``pending`` means no authorization can be inferred from this pass; callers
    may retry. A later pass always re-reads the selected epoch and run revision.
    """
    if not isinstance(root_id, str) or not root_id:
        raise ValueError('intake root ID is required')
    selected = None
    try:
        selected = await asyncio.to_thread(_selected, store, root_id)
        if selected is None:
            return {'root_id': root_id, 'state': 'idle'}
        epoch = selected['epoch']
        if selected.get('blocked'):
            raise ValueError(selected['blocked'])
        # Apply the adapter's ordinary/untimed/custody restrictions before
        # changing the root state, without inventing a no-start observation.
        await asyncio.to_thread(intake._spec, store, root_id, epoch, lambda *_: True)
        if selected['state'] == 'pause_requested':
            row = await joined_thread(_begin, store, root_id, selected)
            await joined_thread(_clear_status, store, root_id, epoch)
            return {'root_id': root_id, 'epoch': epoch, 'state': row['state']}
        if selected['state'] == 'pausing':
            # A replay after a lost acknowledgement reauthenticates the SDK
            # observation and exact saved checkpoint before publishing paused.
            receipt = await asyncio.wait_for(intake.seal_with_sdk(
                store, root_id, epoch, selected['run_revision'], client=client),
                timeout=_SDK_TIMEOUT_SECONDS)
            if (not isinstance(receipt, dict) or receipt.get('kind') != 'unstarted_intake'
                    or receipt.get('resumable') is not True):
                raise ValueError('intake checkpoint receipt changed')
            if selected['checkpoint_json'] is None:
                await joined_thread(_clear_status, store, root_id, epoch)
                return {'root_id': root_id, 'epoch': epoch, 'state': 'pausing'}
            row = await joined_thread(pause.publish_paused, store, root_id, epoch)
            await joined_thread(_clear_status, store, root_id, epoch)
            return {'root_id': root_id, 'epoch': epoch, 'state': row['state']}
        row = await asyncio.wait_for(intake.activate_with_sdk(
            store, root_id, epoch, selected['run_revision'], client=client),
            timeout=_SDK_TIMEOUT_SECONDS)
        await joined_thread(_clear_status, store, root_id, epoch)
        return {'root_id': root_id, 'epoch': epoch, 'state': row['state']}
    except Exception as exc:
        reason, reason_type = _reason(exc), type(exc).__name__[:64]
        result = {'root_id': root_id, 'state': 'pending', 'reason': reason,
                  'reason_type': reason_type}
        if selected is not None:
            result['epoch'] = selected['epoch']
            try:
                await joined_thread(_record_status, store, root_id, selected['epoch'],
                                        reason, reason_type)
            except Exception:
                pass  # Diagnostic failure never opens the intake fence.
        return result


def _page(store, limit, after_root_id):
    with store._connect() as db:
        rows = db.execute('''SELECT c.root_id FROM delivery_pause_control c
            JOIN delivery_runs r ON r.run_id=c.root_id
            WHERE c.root_id>? AND c.state IN ('pause_requested','pausing','resuming')
              AND CASE WHEN json_valid(r.request_json) THEN
                json_type(r.request_json,'$.pause_intake_version')='integer'
                AND json_extract(r.request_json,'$.pause_intake_version')=1
              ELSE 0 END
            ORDER BY c.root_id LIMIT ?''', (after_root_id or '', limit)).fetchall()
        return [row['root_id'] for row in rows]


async def pump(store, *, limit=32, after_root_id=None, client=None):
    """Process a bounded page; use ``next_cursor`` to reach later blocked roots."""
    if type(limit) is not int or not 1 <= limit <= _MAX_BATCH:
        raise ValueError('invalid intake pump batch size')
    if after_root_id is not None and (not isinstance(after_root_id, str) or not after_root_id):
        raise ValueError('invalid intake pump cursor')
    roots = await asyncio.to_thread(_page, store, limit, after_root_id)
    semaphore = asyncio.Semaphore(4)

    async def one(root):
        async with semaphore:
            return await advance(store, root, client=client)

    results = await asyncio.gather(*(one(root) for root in roots))
    return {'results': results, 'next_cursor': roots[-1] if len(roots) == limit else None}
