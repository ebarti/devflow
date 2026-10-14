"""Observe a stopped planned-check registration failure without changing its history."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .contracts import digest
from .delivery_native_process import listeners, process_table
from .delivery_plan_checks import planned_checks
from .delivery_resources import RunResources, _ancestors, _gate_evidence_root, read_private


def observe(spec, state, previous):
    local = state.get('checks', {}).get('local')
    if (local != {'state': 'unknown', 'cleanup': 'unknown', 'reason': 'ValueError',
                  'candidate_id': state['candidate']['id']}
            or state.get('roles') != previous['state'].get('roles')):
        raise ValueError('check prelaunch recovery requires an unassessed registration failure')
    resources = RunResources(spec)
    manifest = read_private(resources.manifest)
    table = process_table()
    journals = {}
    for raw in manifest['processes']:
        path = Path(raw)
        if not path.is_relative_to(Path(spec['state_dir'])):
            raise ValueError('check prelaunch journal escaped its owned run')
        journal = read_private(path)
        if (journal.get('phase') != 'finished' or not journal.get('monitoring_complete')
                or journal.get('result', {}).get('cleanup') != 'observed-native-confirmed'
                or any(table.get(int(pid), {}).get('identity') == item['identity']
                       and not table[int(pid)]['stat'].startswith('Z')
                       for pid, item in journal.get('owned', {}).items())
                or any(listeners(port) for port in journal.get('ports', []))):
            raise ValueError('check prelaunch has a live or unconfirmed native process')
        journals[raw] = hashlib.sha256(path.read_bytes()).hexdigest()
    evidence = _gate_evidence_root(spec) / 'checks' / str(state['iteration'])
    gate = _gate_evidence_root(spec) / 'gates' / str(state['iteration']) / 'verify'
    checks = planned_checks(spec, gate, evidence)
    dependencies = [c for c in checks if c['id'].startswith('planned-python-dependencies-')]
    if len(dependencies) != 1:
        raise ValueError('check prelaunch requires one exact planned Python environment')
    dependency = dependencies[0]
    profile = (Path(spec['state_dir']) / 'transient/checks'
               / evidence.relative_to(Path(spec['state_dir'])) / dependency['id']
               / 'codex/config.toml')
    generated = gate / dependency['generated_directories'][0]
    _ancestors(profile)
    if (os.path.lexists(evidence / dependency['id'] / 'native') or os.path.lexists(generated)
            or any(Path(raw).is_relative_to(evidence / dependency['id']) for raw in journals)
            or str(generated) in manifest['roots'] or profile.is_symlink()
            or not profile.is_file()):
        raise ValueError('planned Python check was launched or acquired generated data')
    for check in spec['policy']['checks']:
        path = evidence / check['id'] / 'native/native-process.json'
        if str(path) not in journals or read_private(path)['result']['exit_code'] != 0:
            raise ValueError('configured checks lack their preceding completed native receipts')
    return {'state': 'observed-quiescent-prelaunch', 'historical_cleanup': state['cleanup'],
            'manifest_sha256': hashlib.sha256(resources.manifest.read_bytes()).hexdigest(),
            'profile_sha256': hashlib.sha256(profile.read_bytes()).hexdigest(),
            'journal_sha256': journals, 'planned_check': dependency}


PREPARATION_STAGE = 'preparation'


def preparation_possible(row):
    """Hint only: failed prerequisites of an already admitted published assessment."""
    previous = json.loads(row.get('recovery_json') or 'null') or {}
    execution = previous.get('execution_spec') or {}
    checks = json.loads(row.get('checks_json') or '{}')
    precheck = checks.get('prepublish') or {}
    results = precheck.get('results')
    history = previous
    while history:
        if history.get('seal', {}).get('preparation_observation'):
            return False
        history = history.get('original_recovery')
    return bool(previous.get('kind') in {'published_gate_retry', 'published_check_prelaunch_retry'}
                and previous.get('command', {}).get('additional_iterations') == 0
                and execution.get('gate_retry_stage') == 'published'
                and precheck.get('state') == 'failed' and precheck.get('cleanup') == 'confirmed'
                and precheck.get('source_unchanged') is True
                and isinstance(results, list) and results
                and all(isinstance(item, dict) and item.get('passed') is False
                        and item.get('launched') is False
                        and item.get('failure_kind') == 'preparation'
                        and item.get('cleanup') == 'confirmed' for item in results))


async def _preparation_history(store, spec, previous, closed):
    from datetime import UTC

    from .delivery_closed_projection import FIELDS, JSON_FIELDS
    from .delivery_closures import history_sha256, sealed_closure
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_workflow import _tracking_ready

    sealed = await sealed_closure(store, closed['workflow_id'])
    if sealed is None:
        raise ValueError('published preparation predecessor history is unsealed')
    scheduled, completed, times = {}, {}, {}
    for event in sealed['history'].events:
        times[event.event_id] = event.event_time.ToDatetime(tzinfo=UTC)
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
            if len(values) != 1 or not isinstance(values[0], dict):
                raise ValueError('published preparation activity request is malformed')
            scheduled[event.event_id] = (attributes.activity_type.name, values[0])
        elif event.HasField('activity_task_completed_event_attributes'):
            attributes = event.activity_task_completed_event_attributes
            values = await DELIVERY_DATA_CONVERTER.decode(attributes.result.payloads)
            completed[attributes.scheduled_event_id] = (
                event.event_id, values[0] if values else None)
    ids, state = list(scheduled), closed['result']
    names = [scheduled[key][0] for key in ids]
    if names != ['delivery_gates_readback', 'delivery_tracker_start', 'delivery_project',
                 'delivery_precheck', 'delivery_finalize_resources',
                 'delivery_terminal_tracker', 'delivery_project']:
        raise ValueError('published preparation history contains different assessed work')
    readback, tracker, started, precheck, finalizer, terminal, projected = ids
    if (set(completed) != set(ids)
            or any(scheduled[key][1].get('spec') != spec for key in ids)
            or any(not key < completed[key][0] < ids[index + 1]
                   for index, key in enumerate(ids[:-1]))
            or scheduled[readback][1].get('recovery') != previous
            or (completed[readback][1] or {}).get('state') != 'OPEN'
            or not _tracking_ready(spec, completed[tracker][1] or {})
            or scheduled[tracker][1].get('repair_continuation') is not True
            or scheduled[started][1].get('event_type') != 'gates_retry_started'
            or scheduled[started][1].get('candidate') != state['candidate']
            or scheduled[started][1].get('iteration') != state['iteration']
            or scheduled[started][1].get('protocol_revision') != previous['state']['revision'] + 1
            or scheduled[precheck][1].get('candidate') != state['candidate']
            or scheduled[precheck][1].get('iteration') != state['iteration']
            or completed[precheck][1] != state['checks']['prepublish']
            or scheduled[finalizer][1].get('outcome') != 'blocked'
            or completed[finalizer][1] != state['checks']['resource_cleanup']
            or completed[terminal][1] != state['tracker']
            or scheduled[projected][1].get('event_type') != 'blocked'
            or any(scheduled[projected][1].get(key) != state.get(key)
                   for key in (*FIELDS, *JSON_FIELDS))
            or scheduled[projected][1].get('protocol_revision') != state['revision']):
        raise ValueError('published preparation history lost its exact unlaunched result')
    return {'history_sha256': history_sha256(sealed['history']), 'scheduled_event_ids': ids,
            'precheck_completed_event_id': completed[precheck][0],
            'precheck_started_at': times[precheck].isoformat(),
            'precheck_completed_at': times[completed[precheck][0]].isoformat()}


def preparation_observe(store, spec, row, attempts, effects, closed):
    from . import delivery_gate_retry as gates
    from .delivery_feature_closure import closed_worker
    from .delivery_gate_preflight_continuation import precheck_dependency, source_readback
    from .delivery_policy_recovery import work_binding
    from .delivery_resources import observe_completed_resources

    previous, state = json.loads(row['recovery_json']), closed['result']
    if (not preparation_possible(row) or row['cleanup'] != 'confirmed'
            or state.get('roles') != previous['state'].get('roles')
            or state.get('iteration') != previous['state'].get('iteration')
            or state.get('candidate') != previous['candidate']
            or state.get('pull_request') != previous['publication']
            or state.get('checks', {}).get('prepublish', {}).get('candidate_id')
            != state['candidate']['id']
            or set(state.get('checks', {})) != {
                'failure', 'prepublish', 'resource_cleanup', 'terminal_tracker_checkpoint'}
            or attempts != previous['seal']['attempts'] or effects != previous['seal']['effects']
            or any(item['state'] != 'complete' or item['observed_json'] is None
                   for item in effects)):
        raise ValueError('published preparation lost its exact unused gate authority')
    authenticated = closed_worker(store, spec['run_id'], closed['workflow_id'], sealed_only=True)
    if any(authenticated[key] != closed.get(key) for key in closed):
        raise ValueError('published preparation closure changed')
    with ThreadPoolExecutor(max_workers=1) as pool:
        proof = pool.submit(lambda: asyncio.run(
            _preparation_history(store, spec, previous, closed))).result(35)
    if proof['history_sha256'] != authenticated['history_sha256']:
        raise ValueError('published preparation history changed during readback')
    gates.readback(store, spec, previous, resource_spec=spec)
    source_readback(store, spec, previous)
    resources = observe_completed_resources(spec, attempts)
    receipt = json.loads(row['checks_json'])['resource_cleanup']
    journals, old = resources['journal_sha256'], previous['seal']['cleanup']['journal_sha256']
    extra = set(journals) - set(old)
    root = _gate_evidence_root(spec) / 'dependency-preparation'
    if (resources['finalization_sha256'] != receipt.get('receipt_sha256')
            or any(journals.get(key) != value for key, value in old.items())
            or len(extra) > 1 or set(journals) != {item['journal'] for item in receipt['processes']}
            or any(Path(key).name != 'native-process.json'
                   or Path(key).parent.name != 'process'
                   or Path(key).parent.parent.parent != root
                   or not re.fullmatch(r'native-[0-9]+', Path(key).parent.parent.name)
                   for key in extra)):
        raise ValueError('published preparation native records changed')
    for path in extra:
        journal = precheck_dependency(spec, path, journals[path], proof)
        result = journal.get('result') or {}
        if (journal.get('phase') != 'finished' or journal.get('monitoring_complete') is not True
                or result.get('monitoring_complete') is not True
                or journal.get('interrupted_monitor') or result.get('interrupted_monitor')
                or type(result.get('exit_code')) is not int or result['exit_code'] != 0
                or result.get('cleanup') != 'observed-native-confirmed'
                or result.get('stdio_drained') is not True):
            raise ValueError('published preparation prerequisite did not complete successfully')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        if (store.state.claim_for(db, spec['work_id']) is not None
                or gates._rows(store, spec['run_id'])[:3] != (row, attempts, effects)):
            raise ValueError('published preparation ownership or records changed')
    return {**proof, 'resources': resources, 'work_binding': binding}


def preparation_authority(spec, recovery):
    from .delivery_continuation import continuation_authority
    from .delivery_gate_retry import PRELAUNCH_KIND

    previous = recovery.get('original_recovery') or {}
    before = previous.get('execution_spec') or {}
    proof = recovery.get('seal', {}).get('preparation_observation') or {}
    row = {'recovery_json': json.dumps(previous),
           'checks_json': json.dumps(recovery.get('state', {}).get('checks', {}))}
    if (not preparation_possible(row) or recovery.get('kind') != PRELAUNCH_KIND
            or spec.get('gate_retry_stage') != PREPARATION_STAGE
            or type(spec.get('gate_retry_generation')) is not int
            or spec['gate_retry_generation'] != before.get('gate_retry_generation')
            or spec.get('gate_retry_generation') not in (1, 2)
            or spec.get('gate_retry_preflight_generation')
            or spec.get('gate_retry_prepared_handoff')
            or recovery.get('command', {}).get('additional_iterations') != 0
            or recovery['state'] != recovery['seal']['closed']['result']
            or recovery['seal'].get('previous_digest') != digest(previous)
            or recovery['seal'].get('attribution_observation') != previous.get('seal', {}).get(
                'attribution_observation')
            or not proof.get('history_sha256')
            or continuation_authority(spec['policy']) != continuation_authority(before['policy'])
            or any(spec.get(key) != before.get(key) for key in (
                'run_id', 'work_id', 'state_dir', 'checkout', 'source_path', 'branch', 'base_sha',
                'accepted_plan', 'feature_worker', 'feature_delivery', 'feature_plan_revision',
                'config_digest', 'plan_approval', 'authorized_endpoint', 'origin_url', 'base_ref',
                'publication_base_ref', 'expected_paths', 'implementation_role_home_generation',
                'provider'))):
        raise ValueError('published preparation changed its unused gate authority')
    return previous, before, proof


def preparation_validate(store, spec, recovery, *, resource_spec=None):
    from . import delivery_gate_retry as gates
    from .delivery_gate_preflight_continuation import source_readback

    previous, before, proof = preparation_authority(spec, recovery)
    with ThreadPoolExecutor(max_workers=1) as pool:
        observed = pool.submit(lambda: asyncio.run(_preparation_history(
            store, before, previous, recovery['seal']['closed']))).result(35)
    if observed != {key: proof[key] for key in observed}:
        raise ValueError('published preparation predecessor history changed')
    from .delivery_listener_verification import historical_records

    historical_records(store, recovery, resource_spec or spec)
    gates.readback(store, before, previous, resource_spec=resource_spec or spec)
    source_readback(store, before, previous, resource_spec=resource_spec or spec)
