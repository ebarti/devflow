"""Observe closed, owned native executions without launching another role."""
from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from types import SimpleNamespace

from temporalio.client import WorkflowExecutionStatus

from .contracts import digest
from .delivery_native_process import listeners, process_table, reconcile_process
from .delivery_resources import RunResources, read_private
from .supervisor import get_supervisor

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


def _complete_attempt(store, spec, row):
    supervisor = get_supervisor(store)
    folder = Path(spec['state_dir']) / 'attempts' / row['job_key']
    if not folder.is_absolute() or folder.resolve() != folder:
        raise ValueError('original native attempt path changed')
    request = read_private(folder / 'request.json')
    if (request.get('spec') != spec or supervisor._job_key(request) != row['job_key']
            or request['role'] != row['role'] or request['iteration'] != row['iteration']
            or request['candidate']['id'] != row['candidate_id']
            or request.get('result_path') != str(folder / 'result.json')
            or request.get('start_path') != str(folder / 'start.json')):
        raise ValueError('orphaned role original request binding changed')
    journal_path = folder / 'native-process.json'
    journal = read_private(journal_path)
    launch = read_private(folder / 'launch.json')
    intent = journal['intent']
    if (intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != spec['policy_digest']
            or intent['cwd'] != request['workspace']
            or intent['argv'] != launch['argv']
            or intent['environment_sha256'] != digest(launch['environment'])
            or type(intent['timeout']) is not int or intent['timeout'] <= 0
            or intent['timeout'] > spec['policy']['roles'][request['role']].get(
                'timeout_seconds', 7200)
            or intent['ports'] != [] or journal.get('ports') != []
            or not journal.get('owned') or not journal.get('monitor')
            or journal.get('phase') != 'finished'
            or journal.get('monitoring_complete') is not True):
        raise ValueError('original native monitor is incomplete or changed')
    observed = process_table()
    owned = {int(pid): identity for pid, identity in journal['owned'].items()}
    monitor = journal['monitor']
    owned[monitor['pid']] = monitor
    if (any(observed.get(pid, {}).get('identity') == identity['identity']
            and not observed[pid]['stat'].startswith('Z') for pid, identity in owned.items())
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
    supervisor._complete_native(request, request,
        SimpleNamespace(folder=folder, journal=journal_path), row['job_key'], journal['result'])
    return receipt



def _observe_registered_process(spec, path):
    if path.resolve() != path or not path.is_relative_to(Path(spec['state_dir'])):
        raise ValueError('registered native process path changed')
    journal = read_private(path)
    intent = journal['intent']
    launch = read_private(path.with_name('launch.json'))
    if (intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != spec['policy_digest']
            or intent['argv'] != launch['argv']
            or intent['environment_sha256'] != digest(launch['environment'])
            or intent['ports'] != journal.get('ports')
            or journal.get('phase') != 'finished'
            or journal.get('monitoring_complete') is not True
            or not journal.get('owned') or not journal.get('monitor')):
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
    try:
        for attempt in attempts:
            if attempt['state'] == 'finished':
                if attempt['cleanup'] != 'confirmed':
                    raise ValueError('completed original role cleanup remains unknown')
                # Its immutable result may predate the current accepted plan. Observe its
                # registered processes below without recompleting or rewriting the role.
                continue
            observations.append(_complete_attempt(store, spec, attempt))
        outcome = current['outcome']
        if outcome not in {'delivered', 'blocked', 'cancelled'}:
            uncertain = uncertain or closed['status'] == 'COMPLETED'
            outcome = 'cancelled' if closed['status'] == 'CANCELED' else 'blocked'
        resources = RunResources(spec, read_only=True)
        if not resources.manifest.is_file():
            raise ValueError('original native resource ownership manifest is missing')
        with resources.locked() as manifest:
            processes = list(manifest['processes'])
        if attempts and not processes:
            raise ValueError('original native process ownership is missing')
        observations.extend(_observe_registered_process(spec, Path(path)) for path in processes)
        receipt = resources.finalize(outcome, uncertain=uncertain)
    except (OSError, ValueError, KeyError, RuntimeError) as exc:
        receipt = {'state': 'unknown', 'resource_cleanup': 'unknown',
                   'process_cleanup': 'unknown', 'reason': str(exc)[:300]}
    current = _unchanged(store, original)
    receipt = {**receipt, 'closed_workflow': closed, 'native_observations': observations,
               'original_attempts': [item for item in attempts if item['state'] != 'finished']}
    checks = {**json.loads(current['checks_json'] or '{}'), 'resource_cleanup': receipt}
    store.project(spec['run_id'], phase=current['phase'],
        execution_state=current['execution_state'], outcome=current['outcome'],
        error=current['error'], checks=checks, cleanup=receipt['resource_cleanup'],
        event_type='resource_cleanup_reconciled',
        message='Owner maintenance observed native cleanup after workflow closure',
        key=digest(receipt))


async def reconcile_closed_native(store, client):
    if (client.service_client.config.target_host != store.config.temporal_address
            or client.namespace != store.config.raw.get('temporal_namespace', 'default')):
        raise ValueError('native maintenance transport differs from frozen owner')
    with store._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT r.* FROM delivery_runs r WHERE EXISTS (SELECT 1 FROM delivery_attempts a "
            "WHERE a.run_id=r.run_id AND (a.state!='finished' OR a.cleanup!='confirmed' "
            "OR r.cleanup!='confirmed'))")]
    for row in rows:
        try:
            spec = store.effective_spec(row['run_id'])
            if (spec.get('provider') != 'codex'
                    or spec['policy'].get('execution_backend') != 'native-macos'
                    or not store.owns_execution(spec)):
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
