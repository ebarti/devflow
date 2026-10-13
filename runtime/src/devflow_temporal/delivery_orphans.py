"""Observe closed, owned native executions without launching another role."""
from __future__ import annotations

import asyncio
import fcntl
import json
import logging
import os
import time
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
from .delivery_resources import RunResources, _gate_roots, read_private
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
    execution_specs = {}
    _gate_roots(spec, execution_specs=execution_specs)
    execution = execution_specs[max((root for root in execution_specs if path.is_relative_to(root)),
                                    key=lambda root: len(root.parts))]
    expected_digest = ((execution.get('preparation') or {}).get('measured_policy_digest')
                       if path.is_relative_to(Path(spec['state_dir']) / 'native-preparation')
                       else execution['policy_digest']) or execution['policy_digest']
    if (intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != expected_digest
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
    cleanup_attempted = False
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
            if not resolved and not _disposable_work(manifest, current):
                return
            cleanup_attempted = True
        # Attempts finalized before any launch registered no process on purpose.
        with store._connect() as db:
            launched = [row for row in db.execute(
                'SELECT result_json FROM delivery_attempts WHERE run_id=?', (spec['run_id'],))
                if not (row['result_json'] and json.loads(row['result_json']).get(
                    'finish_reason') == 'prelaunch')]
        if launched and not processes:
            raise ValueError('original native process ownership is missing')
        observations.extend(_observe_registered_process(spec, Path(path)) for path in processes)
        _unchanged(store, original)
        receipt = resources.finalize(outcome, uncertain=uncertain)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        receipt = {'state': 'unknown', 'resource_cleanup': 'unknown',
                   'process_cleanup': 'unknown', 'reason': str(exc)[:300]}
    if not resolved and not cleanup_attempted:
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


def _disposable_work(manifest, row):
    finalization = manifest.get('finalization', {})
    if (finalization.get('state') == 'confirmed'
            and (row['cleanup'] == 'unknown' or json.loads(row['checks_json'] or '{}').get(
                'resource_cleanup', {}).get('resource_cleanup') != 'confirmed')):
        return True  # Finalizer committed, but its projection acknowledgement was lost.
    return any(entry['kind'] != 'checkout' and os.path.lexists(raw)
               for raw, entry in manifest.get('roots', {}).items()) or (
                   manifest.get('finalization', {}).get('state') == 'running')


def _unfinished_rows(store):
    # Provider attempts keep their existing maintenance cadence. Supplemental
    # check/preparation/finalizer custody is bounded and inspected once per
    # minute, rather than walking the full historical filesystem every pump.
    with store._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT r.* FROM delivery_runs r WHERE EXISTS (SELECT 1 FROM delivery_attempts a "
            "WHERE a.run_id=r.run_id AND a.state!='finished')")]
        now = time.monotonic()
        if now < getattr(store, '_native_cleanup_scan_after', 0):
            return rows
        store._native_cleanup_scan_after = now + 60
        cursor = getattr(store, '_native_cleanup_scan_cursor', 0)
        supplemental = [dict(row) for row in db.execute(
            "SELECT r.rowid AS cleanup_rowid,r.* FROM delivery_runs r WHERE r.rowid>? "
            "AND NOT EXISTS (SELECT 1 FROM delivery_attempts a "
            "WHERE a.run_id=r.run_id AND a.state!='finished') ORDER BY r.rowid LIMIT 64",
            (cursor,))]
        store._native_cleanup_scan_cursor = (supplemental[-1]['cleanup_rowid']
                                            if len(supplemental) == 64 else 0)
    seen = getattr(store, '_native_cleanup_seen', {})
    store._native_cleanup_seen = seen
    for row in supplemental:
        row.pop('cleanup_rowid')
        try:
            spec = json.loads(row['request_json'])
            state = Path(spec['state_dir'])
            manifest_path = state / 'resources/manifest.json'
            if (not state.is_absolute() or state.name != row['run_id']
                    or manifest_path.resolve() != manifest_path or not manifest_path.exists()):
                continue
            recovery = json.loads(row['recovery_json']) if row['recovery_json'] else None
            if recovery and recovery.get('kind') == 'execution_policy_recovery':
                continue  # Preserve the historical policy recovery receipt contract.
            info = manifest_path.stat()
            signature = (info.st_dev, info.st_ino, info.st_mtime_ns, info.st_size,
                         row['workflow_id'], row['recovery_json'],
                         row['cleanup'], row['checks_json'])
            if seen.get(row['run_id']) == signature:
                continue
            manifest = read_private(manifest_path)
            if manifest.get('run_id') != row['run_id']:
                continue
            if manifest.get('finalization', {}).get('state') == 'confirmed':
                if len(seen) >= 512:
                    seen.pop(next(iter(seen)))
                seen[row['run_id']] = signature
            if _disposable_work(manifest, row):
                rows.append(row)
        except (OSError, ValueError, KeyError, TypeError):
            continue  # A changed or unreadable manifest supplies no cleanup authority.
    return rows


def _owned_native_spec(store, run_id):
    spec = store.effective_spec(run_id)
    if (spec.get('provider') != 'codex'
            or spec['policy'].get('execution_backend') != 'native-macos'
            or not store.owns_execution(spec)):
        return None
    # Ordinary transient-stop cleanup belongs to the existing retry owner.
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        unfinished = db.execute("SELECT 1 FROM delivery_attempts WHERE run_id=? "
                                "AND state!='finished' LIMIT 1", (run_id,)).fetchone()
    if row is not None and unfinished is None:
        from .delivery_automatic_retry import eligible

        if eligible(dict(row), spec):
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
