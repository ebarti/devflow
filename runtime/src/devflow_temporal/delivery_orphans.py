"""Observe closed, owned native executions without launching another role."""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
from pathlib import Path
from types import SimpleNamespace

from temporalio.client import WorkflowExecutionStatus

from .contracts import digest
from .delivery_native_process import (
    launch_binding as _launch_binding,
)
from .delivery_native_process import (
    listeners,
    process_table,
    reconcile_process,
)
from .delivery_native_process import (
    prelaunch_journal as _prelaunch_journal,
)
from .delivery_resources import RunResources, read_private
from .supervisor import DeliverySupervisor, get_supervisor

_LOG = logging.getLogger(__name__)


def _unchanged(store, original):
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                         (original['run_id'],)).fetchone()
    if row is None or any(row[key] != original[key] for key in (
        'request_digest', 'recovery_json', 'workflow_id', 'protocol_revision', 'iteration',
    )):
        raise ValueError('closed execution changed during native cleanup')
    return dict(row)


def _complete_attempt(store, spec, row, on_observed=None):
    supervisor = get_supervisor(store)
    folder = Path(spec['state_dir']) / 'attempts' / row['job_key']
    if not folder.is_absolute() or folder.resolve() != folder:
        raise ValueError('original native attempt path changed')
    if not os.path.lexists(folder / 'native-process.json'):
        # No monitor ever journaled, so no provider child could have started.
        # Only a dead owner's released locks prove that; a live owner keeps its slot.
        result = supervisor._reconcile_launch_window(
            spec, row['job_key'], row, on_observed=on_observed)
        if result is None:
            raise ValueError('original native launch window is not provably absent')
        return result['native_process']
    request = read_private(folder / 'request.json')
    if (request.get('spec') != spec or supervisor._job_key(request) != row['job_key']
            or request['role'] != row['role'] or request['iteration'] != row['iteration']
            or request['candidate']['id'] != row['candidate_id']
            or request.get('result_path') != str(folder / 'result.json')
            or request.get('start_path') != str(folder / 'start.json')):
        raise ValueError('orphaned role original request binding changed')
    journal_path = folder / 'native-process.json'
    journal = read_private(journal_path)
    launch = _launch_binding(folder, journal)
    intent = journal['intent']
    if (intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != spec['policy_digest']
            or intent['cwd'] != request['workspace']
            or (launch is not None and (
                intent['argv'] != launch['argv']
                or intent['environment_sha256'] != digest(launch['environment'])))
            or type(intent['timeout']) is not int or intent['timeout'] <= 0
            or intent['timeout'] > spec['policy']['roles'][request['role']].get(
                'timeout_seconds', 7200)
            or intent['ports'] != [] or journal.get('ports') != []
            or (not journal.get('owned') and not _prelaunch_journal(journal))
            or not journal.get('monitor')):
        raise ValueError('original native monitor is incomplete or changed')
    observed = process_table()
    owned = {int(pid): identity for pid, identity in journal['owned'].items()}
    monitor = journal['monitor']
    owned[monitor['pid']] = monitor
    monitor_active = (observed.get(monitor['pid'], {}).get('identity') == monitor['identity']
                      and not observed[monitor['pid']]['stat'].startswith('Z'))
    if (monitor_active or (journal.get('phase') == 'finished'
            and journal.get('cleanup_reconciliation') is None
            and journal.get('monitoring_complete') is True
            and any(observed.get(pid, {}).get('identity') == identity['identity']
                    and not observed[pid]['stat'].startswith('Z')
                    for pid, identity in owned.items()))
            or any(listeners(port) for port in journal.get('ports', []))):
        raise ValueError('original native processes or ports remain active')
    if row['state'] == 'finished':
        result = json.loads(row['result_json'])
        metadata = journal.get('provider_session', {})
        if (metadata.get('result_digest') != digest(result)
                or metadata.get('session_id') != row['session_id']
                or metadata.get('role') != row['role']
                or metadata.get('iteration') != row['iteration']):
            raise ValueError('original completed native result binding changed')
    receipt = reconcile_process(journal_path)
    if receipt['cleanup'] != 'observed-native-confirmed':
        raise ValueError('original native cleanup remains unknown')
    journal = read_private(journal_path)
    if journal['result'].get('cleanup') != 'observed-native-confirmed':
        raise ValueError('original provider cleanup remains ambiguous')
    if on_observed is not None:
        on_observed(receipt)
    supervisor._complete_native(request, request,
        SimpleNamespace(folder=folder, journal=journal_path), row['job_key'], journal['result'],
        expected=row)
    return receipt



def _observe_registered_process(spec, path):
    if path.resolve() != path or not path.is_relative_to(Path(spec['state_dir'])):
        raise ValueError('registered native process path changed')
    journal = read_private(path)
    intent = journal['intent']
    launch = _launch_binding(path.parent, journal)
    if (intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != spec['policy_digest']
            or (launch is not None and (
                intent['argv'] != launch['argv']
                or intent['environment_sha256'] != digest(launch['environment'])))
            or intent['ports'] != journal.get('ports')
            or (not journal.get('owned') and not _prelaunch_journal(journal))
            or not journal.get('monitor')):
        raise ValueError('registered native monitor is incomplete or changed')
    observed = process_table()
    owned = {int(pid): identity for pid, identity in journal['owned'].items()}
    monitor = journal['monitor']
    owned[monitor['pid']] = monitor
    if (any(observed.get(pid, {}).get('identity') == identity['identity']
            and not observed[pid]['stat'].startswith('Z') for pid, identity in owned.items())
            or any(listeners(port) for port in journal.get('ports', []))):
        raise ValueError('registered native processes or ports remain active')
    receipt = reconcile_process(path)
    if receipt['cleanup'] != 'observed-native-confirmed':
        raise ValueError('registered native cleanup remains unknown')
    return receipt

def _retain_observation(store, original, attempt, closed, observation):
    # Persist the original row before completion can commit, including when its
    # acknowledgement or the later run projection is lost. This is an observation,
    # not a successful role verdict or permission to release the claim.
    payload = {'closed_workflow': closed, 'native_observations': [observation],
               'original_attempts': [attempt]}
    payload['key'] = digest(payload)
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                             (attempt['job_key'],)).fetchone()
        if current is None or dict(current) != attempt:
            raise ValueError('original native attempt changed during observation')
        if not any(json.loads(row[0]).get('key') == payload['key'] for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? "
            "AND type='resource_cleanup_reconciled'", (original['run_id'],))):
            store._event(db, original['run_id'], original['revision'],
                'resource_cleanup_reconciled',
                'Original native cleanup observed before attempt completion', payload)


def _reconcile(store, original, spec, closed):
    current = _unchanged(store, original)
    with store._connect() as db:
        attempts = [dict(row) for row in db.execute(
            'SELECT * FROM delivery_attempts WHERE run_id=?', (spec['run_id'],))]
        uncertain = any(db.execute(
            f"SELECT 1 FROM {table} WHERE run_id=? AND state IN ('pending','unknown') LIMIT 1",
            (spec['run_id'],)).fetchone() is not None for table in (
                'delivery_effects', 'delivery_mutations'))
    observations = []
    resolved = []
    try:
        for attempt in attempts:
            if attempt['state'] == 'finished':
                continue
            observation = _complete_attempt(store, spec, attempt,
                lambda receipt, attempt=attempt: _retain_observation(
                    store, original, attempt, closed, receipt))
            with store._connect() as db:
                completed = db.execute(
                    'SELECT state,cleanup FROM delivery_attempts WHERE job_key=?',
                    (attempt['job_key'],)).fetchone()
            if completed['state'] == 'finished' and completed['cleanup'] == 'confirmed':
                resolved.append(attempt)
                observations.append(observation)
        if not resolved:
            return
        if any(item['state'] == 'finished' and item['cleanup'] != 'confirmed'
               for item in attempts):
            raise ValueError('completed original role cleanup remains unknown')
        outcome = current['outcome']
        if outcome not in {'delivered', 'blocked', 'cancelled'}:
            uncertain = uncertain or closed['status'] == 'COMPLETED'
            outcome = 'cancelled' if closed['status'] == 'CANCELED' else 'blocked'
        resources = RunResources(spec, read_only=True)
        if not resources.manifest.is_file():
            raise ValueError('original native resource ownership manifest is missing')
        with resources.locked() as manifest:
            processes = list(manifest['processes'])
        # Attempts finalized before any launch registered no process on purpose.
        with store._connect() as db:
            launched = [row for row in db.execute(
                'SELECT result_json FROM delivery_attempts WHERE run_id=?', (spec['run_id'],))
                if not (row['result_json'] and json.loads(row['result_json']).get(
                    'finish_reason') == 'prelaunch')]
        if launched and not processes:
            raise ValueError('original native process ownership is missing')
        observations.extend(_observe_registered_process(spec, Path(path)) for path in processes)
        receipt = resources.finalize(outcome, uncertain=uncertain)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        receipt = {'state': 'unknown', 'resource_cleanup': 'unknown',
                   'process_cleanup': 'unknown', 'reason': str(exc)[:300]}
    if not resolved:
        return
    current = _unchanged(store, original)
    checks = {**json.loads(current['checks_json'] or '{}'), 'resource_cleanup': receipt}
    store.project(spec['run_id'], phase=current['phase'],
        execution_state=current['execution_state'], outcome=current['outcome'],
        error=current['error'], checks=checks, cleanup=receipt['resource_cleanup'],
        event_type='resource_cleanup_reconciled',
        message='Owner maintenance observed native cleanup after workflow closure',
        key=digest({'closed_workflow': closed, 'native_observations': observations,
                    'resource_cleanup': receipt}))


def _unfinished_rows(store):
    with store._connect() as db:
        return [dict(row) for row in db.execute(
            "SELECT r.* FROM delivery_runs r WHERE EXISTS (SELECT 1 FROM delivery_attempts a "
            "WHERE a.run_id=r.run_id AND a.state!='finished')")]


def _owned_native_spec(store, run_id):
    spec = store.effective_spec(run_id)
    if (spec.get('provider') != 'codex'
            or spec['policy'].get('execution_backend') != 'native-macos'
            or not store.owns_execution(spec)):
        return None
    return spec


async def reconcile_closed_native(store, client):
    if (client.service_client.config.target_host != store.config.temporal_address
            or client.namespace != store.config.raw.get('temporal_namespace', 'default')):
        raise ValueError('native maintenance transport differs from frozen owner')
    # Store reads stay off the API loop; the repair itself already runs in a thread.
    rows = await asyncio.to_thread(_unfinished_rows, store)
    for row in rows:
        try:
            spec = await asyncio.to_thread(_owned_native_spec, store, row['run_id'])
            if spec is None:
                continue
            handle = client.get_workflow_handle(row['workflow_id'] or 'delivery-'+row['run_id'])
            description = await asyncio.wait_for(handle.describe(), timeout=15)
            recovery = json.loads(row['recovery_json']) if row['recovery_json'] else None
            if (description.status not in {WorkflowExecutionStatus.COMPLETED,
                        WorkflowExecutionStatus.FAILED, WorkflowExecutionStatus.CANCELED,
                        WorkflowExecutionStatus.TERMINATED, WorkflowExecutionStatus.TIMED_OUT}
                    or description.id != handle.id
                    or not description.close_time
                    or await description.memo_value('request_digest', None) != row['request_digest']
                    or await description.memo_value('recovery_digest', None) != (
                        digest(recovery) if recovery else None)):
                continue
            closed = {'workflow_id': description.id, 'execution_run_id': description.run_id,
                      'closed_at': description.close_time.isoformat(),
                      'status': description.status.name}
            await asyncio.to_thread(_reconcile, store, row, spec, closed)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # A transient read or foreign/edited configuration cannot stop another row.
            _LOG.warning('Native cleanup observation unavailable for %s: %s',
                         row['run_id'], type(exc).__name__)


def _held_locks(spec, job_key):
    held = []
    for name, path in (
        ('attempt-owner', DeliverySupervisor._owner_lock_path(spec, job_key)),
        ('native-monitor.lock', Path(spec['state_dir']) / 'attempts' / job_key
         / 'native-monitor.lock'),
    ):
        if not os.path.lexists(path):
            continue
        descriptor = os.open(path, os.O_RDWR | os.O_NOFOLLOW)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            held.append(name)
        finally:
            os.close(descriptor)
    return held


def reconcile_attempt(store, *, job_key=None, run_id=None, role=None, iteration=None):
    """Reconcile one unfinished native attempt after verifying no owned process remains."""
    with store._connect() as db:
        if job_key:
            rows = [dict(row) for row in db.execute(
                'SELECT * FROM delivery_attempts WHERE job_key=?', (job_key,))]
        else:
            rows = [dict(row) for row in db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND role=? AND iteration=? "
                "AND state!='finished'", (run_id, role, iteration))]
    if not rows:
        raise ValueError('no matching unfinished native attempt')
    if len(rows) > 1:
        raise ValueError('several attempts match; name one with --job-key: '
                         + ', '.join(row['job_key'] for row in rows))
    row = rows[0]
    outcome = {key: row[key] for key in ('job_key', 'run_id', 'role', 'iteration')}
    outcome.update(state_before=row['state'], changed=False, reason=None)
    if row['state'] == 'finished':
        outcome.update(state='finished', cleanup=row['cleanup'],
                       result=json.loads(row['result_json']) if row['result_json'] else None)
        return outcome
    spec = store.effective_spec(row['run_id'])
    if (spec.get('provider') != 'codex'
            or spec['policy'].get('execution_backend') != 'native-macos'
            or not store.owns_execution(spec)):
        raise ValueError('attempt is not a native execution owned by this service')
    folder = Path(spec['state_dir']) / 'attempts' / row['job_key']
    journal_path = folder / 'native-process.json'
    live = []
    if journal_path.exists():
        journal = read_private(journal_path)
        tracked = {int(pid): entry for pid, entry in journal.get('owned', {}).items()}
        if journal.get('monitor'):
            tracked[journal['monitor']['pid']] = journal['monitor']
        table = process_table()
        live = sorted(pid for pid, entry in tracked.items()
                      if table.get(pid, {}).get('identity') == entry['identity']
                      and not table[pid]['stat'].startswith('Z'))
    held = _held_locks(spec, row['job_key'])
    if live or held:
        outcome.update(state=row['state'], cleanup=row['cleanup'], live_pids=live,
                       held_locks=held, reason='original native owner, monitor or child is '
                       'still alive; nothing was changed')
        return outcome
    try:
        receipt = _complete_attempt(store, spec, row)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        receipt = None
        outcome['reason'] = f'{type(exc).__name__}: {str(exc)[:300]}'
    with store._connect() as db:
        after = dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                                (row['job_key'],)).fetchone())
    outcome.update(state=after['state'], cleanup=after['cleanup'],
                   changed=after['state'] != row['state'], receipt=receipt,
                   result=json.loads(after['result_json']) if after['result_json'] else None)
    return outcome
