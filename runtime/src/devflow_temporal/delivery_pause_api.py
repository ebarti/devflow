"""Public controls for the explicitly admitted queued-intake pause."""
import asyncio
import json

from fastapi import HTTPException, Request

from . import delivery_pause as pause
from .delivery_pause_context import intake_enabled


def _admitted(store, root_id):
    if not intake_enabled(store.submitted_spec(root_id)):
        raise ValueError('pause control scope is not enabled by this service')


def status(store, root_id):
    """Report persisted hints; resume is revalidated when requested."""
    _admitted(store, root_id)
    with store._connect() as db:
        db.execute('BEGIN')
        row = pause._row(db, root_id)
        checkpoint = json.loads(row['checkpoint_json']) if row['checkpoint_json'] else None
        run = db.execute('''SELECT r.execution_state,o.state FROM delivery_runs r
            JOIN delivery_outbox o USING(run_id) WHERE r.run_id=?''', (root_id,)).fetchone()
        eligible = bool(row['state'] == 'running' and not row['execution_run_id']
                        and run is not None and run[0] == 'queued'
                        and run[1] in {'pending', 'unknown'}
                        and not db.execute('SELECT 1 FROM delivery_pause_steps WHERE root_id=? '
                                           "AND step='start' LIMIT 1", (root_id,)).fetchone()
                        and not db.execute('SELECT 1 FROM delivery_pause_actors WHERE root_id=? '
                                           'LIMIT 1', (root_id,)).fetchone())
    from .delivery_pause_intake_pump import pending_reason

    diagnostic = pending_reason(store, root_id)
    blockers = []
    if diagnostic and diagnostic['epoch'] == row['epoch']:
        blockers.append(diagnostic['reason'])
    if row['state'] == 'paused' and not checkpoint:
        blockers.append('checkpoint unavailable')
    elif checkpoint and checkpoint.get('resumable') is False:
        blockers.append('checkpoint requires reconciliation')
    desired = {'running': 'running', 'pause_requested': 'paused', 'pausing': 'paused',
               'paused': 'paused', 'resuming': 'running', 'complete': 'complete'}
    return {'root_id': root_id, 'scope': 'before_start', 'pause_eligible': eligible,
            'desired': desired.get(row['state'], 'unknown'), 'observed': row['state'],
            'revision': row['revision'], 'epoch': row['epoch'], 'checkpoint': checkpoint,
            'blockers': blockers, 'notification_pending': False,
            'resume_authorization': 'checked when requested'}


def mount(app, service, mutation, host):
    @app.get('/api/runs/{run_id}/pause-control')
    async def pause_status(request: Request, run_id: str):
        host(request)
        try:
            return await asyncio.to_thread(status, service.store, run_id)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    def apply_command(run_id, action, body):
        from .delivery_pause_namespace import intake_namespace_scope

        _admitted(service.store, run_id)
        with intake_namespace_scope(service.store.config):
            _admitted(service.store, run_id)
            if action == 'pause':
                return pause.request_pause(service.store, run_id, body['command_id'],
                                           body['expected_revision'], intake_only=True)
            return pause.begin_resume(service.store, run_id, body['command_id'],
                                      body['expected_revision'], body['epoch'])

    async def command(request, run_id, action):
        mutation(request)
        try:
            body = await request.json()
            keys = {'command_id', 'expected_revision'} | (
                {'epoch'} if action == 'resume' else set()
            )
            if not isinstance(body, dict) or set(body) != keys:
                raise ValueError('invalid pause command body')
            if action == 'resume' and (type(body['epoch']) is not int or body['epoch'] < 1):
                raise ValueError('invalid pause epoch')
            return await asyncio.to_thread(apply_command, run_id, action, body)
        except (ValueError, TypeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post('/api/runs/{run_id}/pause')
    async def request_pause(request: Request, run_id: str):
        return await command(request, run_id, 'pause')

    @app.post('/api/runs/{run_id}/resume')
    async def request_resume(request: Request, run_id: str):
        return await command(request, run_id, 'resume')
