"""Explicit reconciliation-only successor bound to an authentic closed Temporal tail."""
from __future__ import annotations

import asyncio
import json
import subprocess
from copy import deepcopy
from pathlib import Path

from temporalio.client import WorkflowExecutionStatus

from .contracts import canonical_json, digest
from .delivery_broker import BrokerReadbackUnavailable, DeliveryBroker, _git
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_policy_recovery import _rows, _stopped_cleanup, work_binding
from .delivery_resources import RunResources, read_private


async def closed_tail(store, run_id, client):
    handle = client.get_workflow_handle(store.active_workflow_id(run_id))
    description = await handle.describe()
    if (description.status not in {WorkflowExecutionStatus.COMPLETED,
                                   WorkflowExecutionStatus.TIMED_OUT}
            or not description.close_time
            or description.workflow_type != 'DevflowDeliveryWorkflow'):
        raise ValueError('terminal predecessor is not an authentic completed/timed-out delivery')
    handle = client.get_workflow_handle(description.id, run_id=description.run_id)
    history = await handle.fetch_history()
    started = history.events[0].workflow_execution_started_event_attributes
    inputs = await DELIVERY_DATA_CONVERTER.decode(started.input.payloads)
    input_spec = inputs[0] if inputs else None
    input_recovery = inputs[1] if len(inputs) > 1 else None
    scheduled = {}
    tail = None
    for event in history.events:
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            if attributes.activity_type.name == 'delivery_project':
                values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
                scheduled[event.event_id] = values[0]
        if event.HasField('activity_task_completed_event_attributes'):
            completed = event.activity_task_completed_event_attributes
            request = scheduled.get(completed.scheduled_event_id)
            if request is not None:
                tail = request
    if (tail is None and description.status == WorkflowExecutionStatus.TIMED_OUT
            and isinstance(input_recovery, dict)
            and input_recovery.get('kind') == 'terminal_tracker_recovery'):
        # A queued successor can time out with no worker or before preflight's
        # first projection. Its authenticated start input already carries the
        # previous sealed projection; admit no new authority from a caller.
        tail = input_recovery['closed']['projection']
    if not isinstance(tail, dict) or tail.get('phase') != 'waiting_tracker':
        raise ValueError('closed Temporal tail has no confirmed pending terminal projection')
    return {
        'workflow_id': description.id, 'execution_run_id': description.run_id,
        'status': description.status.name, 'closed_at': description.close_time.isoformat(),
        'request_digest': await description.memo_value('request_digest', None),
        'recovery_digest': await description.memo_value('recovery_digest', None),
        'history_sha256': digest(history.to_json()), 'projection': tail,
        'input_spec': input_spec,
        'input_recovery_sha256': digest(input_recovery) if input_recovery else None,
    }


def receipt(store, run_id, payload):
    command_digest = digest({'kind': 'terminal_tracker_recovery', 'run_id': run_id, **payload})
    with store._connect() as db:
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (payload['command_id'],)).fetchone()
    if prior:
        if prior['run_id'] != run_id or prior['request_digest'] != command_digest:
            raise ValueError('command ID already belongs to different authority')
        return json.loads(prior['response_json'])
    return None


def published_readback(store, spec, candidate, pr):
    if not isinstance(candidate, dict) or not isinstance(pr, dict):
        raise ValueError('terminal published candidate/PR is missing')
    if spec.get('merge_version') == 1 and spec.get('authorized_endpoint') == 'merged':
        from .delivery_merge import merged_readback

        return merged_readback(store, spec, candidate, pr)
    broker = DeliveryBroker(store, spec)
    found = broker._existing_pr()
    try:
        remote = _git(broker.source, 'ls-remote', 'origin', f"refs/heads/{spec['branch']}")
    except (RuntimeError, subprocess.TimeoutExpired, OSError) as exc:
        if spec.get('tracker_retry_version') != 1:
            raise
        raise BrokerReadbackUnavailable('terminal branch readback unavailable') from exc
    if (not found or not remote
            or candidate.get('head') != pr.get('head') or found['headRefOid'] != pr['head']
            or found['number'] != pr['number'] or found['url'] != pr['url']
            or _git(broker.source, 'remote', 'get-url', 'origin') != spec['origin_url']
            or remote.split()[0] != pr['head']):
        raise ValueError('terminal published head changed; gates cannot bless another candidate')
    return {'number': pr['number'], 'head': pr['head'], 'url': pr['url']}


def snapshot(store, spec):
    row, attempts, effects, claim = _rows(store, spec['run_id'])
    with store._connect() as db:
        work = store.state.row(db, 'works', spec['work_id'])
        binding = work_binding(store, spec, db)
    if (store.state.issue_resource(work['issue']) != store.state.issue_resource(spec['issue_url'])
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)
            or any(e['state'] != 'complete' for e in effects)
            or (claim is not None and claim['owner'] != f"external:devflow:{spec['run_id']}")):
        raise ValueError('terminal issue, attempt, effect or ownership changed')
    cleanup = _stopped_cleanup(spec)
    checks = json.loads(row['checks_json'] or '{}')
    if checks.get('resource_cleanup', {}).get('receipt_sha256') != cleanup['receipt_sha256']:
        raise ValueError('terminal cleanup proof differs from the frozen gate')
    published = None
    candidate = json.loads(row['candidate_json'] or 'null')
    if checks.get('terminal_tracker_checkpoint', {}).get('outcome') == 'delivered':
        published = published_readback(store, spec, candidate,
                                      json.loads(row['pr_json'] or 'null'))
    elif candidate is not None:
        if Path(spec['checkout']).exists():
            if DeliveryBroker(store, spec).candidate() != candidate:
                raise ValueError('terminal preserved candidate source changed')
        else:
            resources = RunResources(spec)
            manifest = read_private(resources.manifest)
            receipt = read_private(resources.root / 'finalization.json')
            entry = manifest['roots'].get(spec['checkout'], {})
            removed = next((r for r in receipt['roots'] if r['path'] == spec['checkout']), {})
            expected = {'candidate': {k: candidate.get(k) for k in (
                'head', 'content_sha256', 'id')}, 'base_sha': spec['base_sha'],
                'outcome': 'cancelled'}
            if (checks.get('terminal_tracker_checkpoint', {}).get('outcome') != 'cancelled'
                    or candidate.get('head') != spec['base_sha']
                    or candidate.get('base_sha') != spec['base_sha']
                    or entry.get('kind') != 'checkout' or removed.get('kind') != 'checkout'
                    or removed.get('state') not in {'removed', 'already_absent'}
                    or receipt.get('outcome') != 'cancelled'
                    or entry.get('clean_base_removal') != expected
                    or removed.get('clean_base_removal') != expected):
                raise ValueError('absent terminal source has no owning clean cancelled base proof')
    return row, {'attempts_sha256': digest(attempts), 'effects_sha256': digest(effects),
                 'claim_sha256': digest(claim), 'cleanup': cleanup, 'published': published,
                 'candidate_sha256': digest(candidate), 'work_binding': binding}


def _grant(store, run_id, payload, closed):
    prior = receipt(store, run_id, payload)
    if prior is not None:
        return prior
    spec = store.effective_spec(run_id)
    row, observed = snapshot(store, spec)
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    tail = closed['projection']
    checkpoint = tail.get('checks', {}).get('terminal_tracker_checkpoint', {})
    # Native preparation and automatic plan acceptance transform the immutable
    # initial input. Bind authentic start input to its durable admission stage,
    # while the completed tail remains bound to the final effective authority.
    starts = [spec] if previous else [store.submitted_spec(run_id), store.spec(run_id), spec]
    if row['protocol_revision'] != payload['expected_revision']:
        raise ValueError('stale run revision')
    if (spec.get('terminal_tracker_version') != 1 or tail.get('spec') != spec
            or closed['workflow_id'] != (row['workflow_id'] or f'delivery-{run_id}')
            or closed['input_spec'] not in starts
            or closed['input_recovery_sha256'] != (digest(previous) if previous else None)
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or row['phase'] != 'waiting_tracker' or row['outcome'] is not None
            or tail.get('protocol_revision') != row['protocol_revision']
            or tail.get('execution_state') != row['execution_state']
            or tail.get('outcome') is not None or tail.get('iteration') != row['iteration']
            or tail.get('candidate') != json.loads(row['candidate_json'] or 'null')
            or tail.get('pull_request') != json.loads(row['pr_json'] or 'null')
            or tail.get('checks') != json.loads(row['checks_json'] or '{}')
            or tail.get('cleanup') != row['cleanup'] or tail.get('error') != row['error']
            or checkpoint.get('event') not in {'delivered', 'blocked', 'cancelled'}
            or checkpoint.get('outcome') != checkpoint.get('event')
            or checkpoint.get('status') != (
                ('done' if spec.get('merge_version') == 1 else 'in-review')
                if checkpoint['event'] == 'delivered' else 'blocked')
            or not checkpoint.get('release')):
        raise ValueError('closed terminal projection, cleanup or frozen transition changed')
    while previous and previous.get('kind') == 'terminal_tracker_recovery':
        previous = previous['original_recovery']
    state = {key: deepcopy(value) for key, value in tail.items() if key not in (
        'spec', 'event_type', 'message', 'key', 'protocol_revision',
    )}
    # These are the original durable attempt results, not new role acceptance.
    # Preserve them in the final workflow result as well as in SQLite so a later
    # authorized blocked-candidate repair still has its session provenance.
    with store._connect() as db:
        attempts = db.execute(
            'SELECT result_json FROM delivery_attempts WHERE run_id=? '
            'ORDER BY iteration,job_key', (run_id,),
        ).fetchall()
    state.update(run_id=run_id, revision=tail['protocol_revision'],
                 candidate_revision=row['candidate_revision'],
                 roles=[json.loads(a['result_json']) for a in attempts if a['result_json']])
    recovery = {
        'kind': 'terminal_tracker_recovery', 'original_recovery': previous,
        'state': state, 'spec_sha256': digest(spec), 'closed': closed,
        'seal': observed, 'command_id': payload['command_id'],
    }
    workflow_id = f"delivery-{run_id}-terminal-tracker-{digest(payload['command_id'])}"
    response = {'run_id': run_id, 'phase': 'terminal_tracker_recovery_queued',
                'workflow_id': workflow_id, 'revision': row['protocol_revision'],
                'reconciliation_only': True}
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (payload['command_id'],)).fetchone()
        if prior:
            expected = digest({'kind': 'terminal_tracker_recovery', 'run_id': run_id, **payload})
            if prior['request_digest'] != expected:
                raise ValueError('terminal command was admitted with conflicting inputs')
            return json.loads(prior['response_json'])
        if dict(current) != row:
            raise ValueError('terminal recovery lost its frozen projection')
        db.execute("UPDATE delivery_runs SET phase='terminal_tracker_recovery_queued',"
                   "execution_state='queued',revision=revision+1,workflow_id=?,recovery_json=?,"
                   'updated_at=? WHERE run_id=?',
                   (workflow_id, canonical_json(recovery), store.state.now(), run_id))
        db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL WHERE run_id=?",
                   (run_id,))
        db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                   (payload['command_id'], run_id,
                    digest({'kind': 'terminal_tracker_recovery', 'run_id': run_id, **payload}),
                    canonical_json(response)))
        store._event(db, run_id, row['revision'] + 1, 'terminal_tracker_recovery_queued',
                     'Explicit readback-only successor sealed; no role or candidate authority',
                     {'closed_history_sha256': closed['history_sha256']})
    return response


async def recover(store, run_id, payload, client):
    closed = await closed_tail(store, run_id, client)
    return await asyncio.to_thread(_grant, store, run_id, payload, closed)


def preflight(store, spec, recovery):
    row, observed = snapshot(store, spec)
    if (recovery.get('kind') != 'terminal_tracker_recovery'
            or recovery['spec_sha256'] != digest(spec)
            or row['recovery_json'] != canonical_json(recovery)
            or observed != recovery['seal']):
        raise ValueError('terminal recovery lost its exact sealed authority')
