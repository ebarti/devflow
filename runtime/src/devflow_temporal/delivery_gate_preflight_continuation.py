"""One fresh controller continuation of an admitted, unstarted attribution assessment."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_continuation import continuation_authority
from .delivery_resources import observe_completed_resources


def source_readback(store, spec, recovery, *, resource_spec=None):
    from .delivery_broker import _git
    from .delivery_execution_config import load_execution_config
    from .delivery_feature_revision_subject import ObservationBroker
    from .delivery_source_scope import outside_scope

    broker = ObservationBroker(store, spec, resource_spec=resource_spec)
    candidate, publication = broker.candidate(), recovery['publication']
    pr = broker._existing_pr()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    if (candidate != recovery['candidate'] or not pr or pr['state'] != 'OPEN' or pr['isDraft']
            or pr['number'] != publication['number'] or pr['headRefOid'] != candidate['head']
            or remote.split() != [candidate['head'], 'refs/heads/' + spec['branch']]
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.source, 'remote', 'get-url', 'origin') != spec['origin_url']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or _git(broker.checkout, 'status', '--porcelain', '--untracked-files=no')
            or outside_scope(spec['policy'], broker._changed_paths(), checkout=broker.checkout)
            or not store.owns_execution(spec)):
        raise ValueError('gate preflight continuation lost exact owned source or publication')
    load_execution_config(spec)



def interrupted(row):
    """Select only unfinished work from an admitted zero-writer gate assessment."""
    recovery = json.loads(row.get('recovery_json') or 'null') or {}
    execution = recovery.get('execution_spec') or {}
    checks = json.loads(row.get('checks_json') or '{}')
    precheck = checks.get('prepublish') or {}
    checkpoint = checks.get('terminal_tracker_checkpoint') or {}
    previous = recovery
    while previous:
        if previous.get('seal', {}).get('preflight_continuation', {}).get('interrupted_assessment'):
            return False  # Completing one admitted assessment is bounded once.
        previous = previous.get('original_recovery')
    return bool(recovery.get('kind') == 'published_gate_retry'
                and recovery.get('command', {}).get('additional_iterations') == 0
                and execution.get('gate_retry_stage') == 'published'
                and execution.get('feature_worker', {}).get('kind') == 'chunk'
                and recovery.get('seal', {}).get('attribution_observation')
                and recovery.get('seal', {}).get('preflight_continuation')
                and precheck.get('state') == precheck.get('cleanup') == 'unknown'
                and precheck.get('reason') == 'OperationalError'
                and checks.get('failure') == {'classification': 'terminal',
                    'stage': 'gates_retry', 'reason': 'technical prepublication checks failed'}
                and checkpoint.get('closed') is True and checkpoint.get('waiting') is False
                and checkpoint.get('state') == 'conflicted'
                and checkpoint.get('reason') == checks['failure']['reason']
                and row.get('error') == 'terminal tracker synchronization conflicted')


async def interrupted_events(history, spec, recovery, state):
    """Prove the actual incomplete assessment, rather than reinterpret a gate verdict."""
    from .delivery_closed_projection import FIELDS, JSON_FIELDS
    from .delivery_closures import history_sha256
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_workflow import _tracking_ready

    scheduled, completed, failed, times = {}, {}, {}, {}
    for event in history.events:
        times[event.event_id] = event.event_time.ToDatetime(tzinfo=UTC)
        if event.HasField('activity_task_scheduled_event_attributes'):
            attributes = event.activity_task_scheduled_event_attributes
            values = await DELIVERY_DATA_CONVERTER.decode(attributes.input.payloads)
            if len(values) != 1 or not isinstance(values[0], dict):
                raise ValueError('interrupted gate activity request is malformed')
            scheduled[event.event_id] = (attributes.activity_type.name, values[0])
        elif event.HasField('activity_task_completed_event_attributes'):
            attributes = event.activity_task_completed_event_attributes
            values = await DELIVERY_DATA_CONVERTER.decode(attributes.result.payloads)
            completed[attributes.scheduled_event_id] = (
                event.event_id, values[0] if values else None)
        elif event.HasField('activity_task_failed_event_attributes'):
            attributes = event.activity_task_failed_event_attributes
            failed[attributes.scheduled_event_id] = (event.event_id, attributes.failure)
    ids = list(scheduled)
    names = [scheduled[key][0] for key in ids]
    expected = ['delivery_gates_readback', 'delivery_tracker_start', 'delivery_project',
                'delivery_precheck', 'delivery_finalize_resources',
                'delivery_terminal_tracker', 'delivery_project']
    if names != expected or any(scheduled[key][1].get('spec') != spec for key in ids):
        raise ValueError('interrupted gate history contains different admitted work')
    readback, tracker, started, precheck, finalizer, terminal, projected = ids
    if (set(completed) != set(ids) - {finalizer} or set(failed) != {finalizer}
            or any(not key < completed[key][0] < ids[index + 1]
                   for index, key in enumerate(ids[:-1]) if key != finalizer)
            or not finalizer < failed[finalizer][0] < terminal
            or scheduled[readback][1].get('recovery') != recovery
            or (completed[readback][1] or {}).get('state') != 'OPEN'
            or not _tracking_ready(spec, completed[tracker][1] or {})
            or scheduled[tracker][1].get('repair_continuation') is not True
            or scheduled[started][1].get('event_type') != 'gates_retry_started'
            or scheduled[started][1].get('candidate') != state['candidate']
            or scheduled[started][1].get('iteration') != state['iteration']
            or scheduled[started][1].get('protocol_revision') != state['revision'] - 1
            or scheduled[precheck][1].get('candidate') != state['candidate']
            or scheduled[precheck][1].get('iteration') != state['iteration']
            or completed[precheck][1] != state['checks']['prepublish']
            or completed[precheck][1] != {'candidate_id': state['candidate']['id'],
                'state': 'unknown', 'cleanup': 'unknown', 'reason': 'OperationalError'}
            or scheduled[finalizer][1].get('outcome') != 'blocked'
            or failed[finalizer][1].application_failure_info.type != 'OperationalError'
            or completed[terminal][1] != state['tracker']
            or scheduled[projected][1].get('event_type') != 'tracker_deadline'
            or scheduled[projected][1].get('protocol_revision') != state['revision']
            or any(scheduled[projected][1].get(key) != state.get(key)
                   for key in (*FIELDS, *JSON_FIELDS))):
        raise ValueError('interrupted gate history lacks its incomplete infrastructure result')
    return {'history_sha256': history_sha256(history), 'scheduled_event_ids': ids,
            'precheck_completed_event_id': completed[precheck][0],
            'precheck_started_at': times[precheck].isoformat(),
            'precheck_completed_at': times[completed[precheck][0]].isoformat(),
            'finalization_failed_event_id': failed[finalizer][0]}


def observe_interrupted(store, spec, row, attempts, effects, closed):
    from . import delivery_gate_retry as gates
    from .delivery_closures import sealed_closure
    from .delivery_feature_closure import closed_worker
    from .delivery_native_dependencies import REGISTRY, frozen_pnpm_inputs
    from .delivery_native_preparation import native_identity
    from .delivery_policy_recovery import work_binding
    from .delivery_resources import _gate_evidence_root, read_private

    recovery, state = json.loads(row['recovery_json']), closed['result']
    if (not interrupted(row) or row['cleanup'] != 'confirmed'
            or state.get('roles') != recovery['state'].get('roles')
            or state.get('iteration') != recovery['state'].get('iteration')
            or state.get('candidate') != recovery['candidate']
            or state.get('pull_request') != recovery['publication']
            or set(state.get('checks', {})) != {
                'failure', 'prepublish', 'resource_cleanup', 'terminal_tracker_checkpoint'}
            or attempts != recovery['seal']['attempts']
            or effects != recovery['seal']['effects']
            or any(item['state'] != 'complete' or item['observed_json'] is None
                   for item in effects)):
        raise ValueError('interrupted gate continuation lost its admitted assessment')
    authenticated = closed_worker(store, spec['run_id'], closed['workflow_id'], sealed_only=True)
    if any(authenticated[key] != closed.get(key) for key in closed):
        raise ValueError('interrupted gate closure changed')

    async def read():
        sealed = await sealed_closure(store, closed['workflow_id'])
        if sealed is None:
            raise ValueError('interrupted gate history is unsealed')
        return await interrupted_events(sealed['history'], spec, recovery, state)

    with ThreadPoolExecutor(max_workers=1) as pool:
        history = pool.submit(lambda: asyncio.run(read())).result(35)
    if history['history_sha256'] != authenticated['history_sha256']:
        raise ValueError('interrupted gate history changed during readback')
    gates.readback(store, spec, recovery, resource_spec=spec)
    source_readback(store, spec, recovery)
    observed = observe_completed_resources(spec, attempts)
    if observed['finalization_sha256'] != json.loads(row['checks_json'])[
            'resource_cleanup'].get('receipt_sha256'):
        raise ValueError('interrupted gate finalization differs from recorded cleanup')
    old = recovery['seal']['cleanup']['journal_sha256']
    journals = observed['journal_sha256']
    extra = set(journals) - set(old)
    root = _gate_evidence_root(spec) / 'dependency-preparation'
    if (any(journals.get(key) != value for key, value in old.items()) or len(extra) != 1
            or any(Path(key).name != 'native-process.json' or Path(key).parent.name != 'process'
                   or Path(key).parent.parent.parent != root
                   or not re.fullmatch(r'native-[0-9]+', Path(key).parent.parent.name)
                   for key in extra)
            or set(journals) != {item['journal'] for item in
                                 json.loads(row['checks_json'])['resource_cleanup']['processes']}):
        raise ValueError('interrupted gate native inventory differs from its completed precheck')
    # The native observer already proved the actual lost monitor's teardown.
    # Bind this sole new dependency launch to the acknowledged precheck interval;
    # its unknown exit remains unknown and grants no gate pass.
    journal_path = Path(next(iter(extra)))
    journal = read_private(journal_path)
    launch = read_private(journal_path.with_name('launch.json'))
    ready = read_private(journal_path.with_name('ready.json'))
    intent, monitor = journal['intent'], journal['monitor']
    child = journal.get('owned', {}).get(str(ready.get('pid')), {})
    identity = child.get('identity')
    staging = Path(spec['state_dir']) / 'transient/dependencies' / spec['policy_digest'] / 'staging'
    manager, _ = frozen_pnpm_inputs(spec, Path(spec['checkout']))
    argv = ['corepack', manager, 'fetch', '--frozen-lockfile', '--ignore-scripts',
            '--ignore-pnpmfile', '--store-dir', str(staging / 'store')]
    try:
        born = datetime.strptime(monitor['identity'], '%a %b %d %H:%M:%S %Y').astimezone(UTC)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError('interrupted precheck native birth identity is unreadable') from exc
    if (hashlib.sha256(journal_path.read_bytes()).hexdigest() != journals[str(journal_path)]
            or intent['run_id'] != spec['run_id']
            or intent['policy_digest'] != spec['policy_digest']
            or intent['argv'] != launch.get('argv') or intent['argv'] != argv
            or intent['timeout'] != 1800
            or launch.get('environment', {}).get('COREPACK_ENABLE_NETWORK') != '0'
            or launch.get('environment', {}).get('npm_config_registry')
            != 'https://' + REGISTRY + '/'
            or intent['environment_sha256'] != digest(launch.get('environment'))
            or intent['cwd'] != str(staging)
            or intent['ports'] != [] or ready.get('identity') != identity
            or ready.get('pid') == monitor['pid'] or child.get('ppid') != monitor['pid']
            or child.get('pgid') != ready.get('pid')
            or journal.get('runtime_identity', {}).get('runtime_payload_sha256')
            != spec['policy'].get('native_identity', {}).get('runtime_payload_sha256')
            or not datetime.fromisoformat(history['precheck_started_at']) - timedelta(seconds=1)
            <= born <= datetime.fromisoformat(history['precheck_completed_at'])):
        raise ValueError('interrupted gate native launch differs from its actual precheck')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        if (store.state.claim_for(db, spec['work_id']) is not None
                or gates._rows(store, spec['run_id'])[:3] != (row, attempts, effects)):
            raise ValueError('interrupted gate ownership or records changed')
    runtime = native_identity(spec) if spec['provider'] != 'fake' else None
    return {'closed': closed, 'runtime': runtime, 'resources': observed,
            'recovery_digest': digest(recovery), 'work_binding': binding,
            'interrupted_assessment': history,
            'preflight_generation': spec.get('gate_retry_preflight_generation', 0) + 1}

def possible(row):
    recovery = json.loads(row.get('recovery_json') or 'null')
    return bool(recovery and recovery.get('kind') == 'published_gate_retry'
                and recovery.get('execution_spec', {}).get('gate_retry_generation') == 2
                and recovery['execution_spec'].get('gate_retry_stage') == 'published'
                and not recovery['execution_spec'].get('gate_retry_preflight_generation')
                and recovery.get('seal', {}).get('attribution_observation')
                and row.get('error') == 'gate retry preflight failed: ActivityError') \
        or interrupted(row)


def observe(store, spec, row, attempts, effects, closed, *, cleanup=False):
    """Authenticate the unused assessment before any cleanup or queue effect."""
    from . import delivery_gate_retry as gates
    from .delivery_native_preparation import native_identity
    from .delivery_policy_recovery import closed_checks, reconciled_cleanup, work_binding

    if not possible(row):
        raise ValueError('gate preflight continuation requires its unused admitted assessment')
    recovery = json.loads(row['recovery_json'])
    state = closed['result']
    reconciled = reconciled_cleanup(store, row, state)
    state_checks, row_checks = closed_checks(row, state, reconciled)
    failure = state_checks.get('failure') or {}
    if (gates.effective_spec(store, recovery['original_spec'], recovery) != spec
            or closed.get('workflow_id') != row['workflow_id']
            or not closed.get('execution_run_id') or not closed.get('closed_at')
            or closed.get('request_digest') != row['request_digest']
            or closed.get('recovery_digest') != digest(recovery)
            or any(row[key] != 'blocked' or state.get(key) != 'blocked'
                   for key in ('phase', 'outcome', 'execution_state'))
            or state.get('run_id') != spec['run_id']
            or state.get('error') != row['error']
            or state.get('revision') != row['protocol_revision']
            or state.get('iteration') != row['iteration']
            or state.get('iteration') != recovery['state']['iteration']
            or state.get('roles') != recovery['state']['roles']
            or state.get('candidate') != recovery['candidate']
            or json.loads(row['candidate_json']) != recovery['candidate']
            or state.get('pull_request') != recovery['publication']
            or json.loads(row['pr_json']) != recovery['publication']
            or state_checks != row_checks
            or failure != {'classification': 'terminal', 'stage': 'gates_retry',
                           'reason': row['error']}
            or set(state_checks) - {'failure', 'resource_cleanup', 'terminal_tracker_checkpoint'}
            or attempts != recovery['seal']['attempts']
            or effects != recovery['seal']['effects']
            or not cleanup and row['cleanup'] != 'confirmed'):
        raise ValueError('gate preflight continuation lost its exact unstarted checkpoint')
    # Reprove the original native finding, repaired attribution, full current
    # candidate, canonical publication and source scope after the new admission.
    gates.readback(store, spec, recovery, resource_spec=spec)
    source_readback(store, spec, recovery)
    observed = observe_completed_resources(spec, attempts)
    if observed['journal_sha256'] != recovery['seal']['cleanup']['journal_sha256']:
        raise ValueError('gate preflight continuation already launched new native work')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        claim = store.state.claim_for(db, spec['work_id'])
        if ((claim is not None and (not cleanup or claim['owner'] !=
                'external:devflow:' + spec['run_id']))
                or gates._rows(store, spec['run_id'])[:3] != (row, attempts, effects)):
            raise ValueError('gate preflight continuation ownership or records changed')
    runtime = native_identity(spec) if spec['provider'] != 'fake' else None
    old_runtime = spec['policy'].get('native_identity') or {}
    if runtime is not None and (gates._git(Path(__file__).resolve().parents[3], 'status',
            '--porcelain', '--untracked-files=all') or
            runtime['runtime_payload_sha256'] == old_runtime.get('runtime_payload_sha256')):
        raise ValueError('gate preflight continuation requires newly measured repaired source')
    return {'closed': closed, 'runtime': runtime, 'resources': observed,
            'recovery_digest': digest(recovery), 'work_binding': binding}


def snapshot(store, spec, row, attempts, effects, closed):
    from . import delivery_gate_retry as gates
    from .delivery_policy_recovery import _stopped_cleanup

    proof = (observe_interrupted(store, spec, row, attempts, effects, closed)
             if interrupted(row) else observe(store, spec, row, attempts, effects, closed))
    original = json.loads(row['recovery_json'])['seal']['attribution_observation']
    return {'row': gates._sealed_row(row), 'closed': closed, 'original_spec': spec,
            'attempts': attempts, 'effects': effects,
            'candidate': json.loads(row['candidate_json']),
            'publication': json.loads(row['pr_json']),
            'cleanup': (proof['resources'] if proof.get('interrupted_assessment')
                        else _stopped_cleanup(spec)),
            'work_binding': proof['work_binding'], 'previous_digest': proof['recovery_digest'],
            'prior_gate_digest': proof['recovery_digest'], 'stage': 'published',
            'generation': spec['gate_retry_generation'],
            'attribution_observation': original,
            'preflight_continuation': proof}


def _authority(spec, recovery):
    previous = recovery.get('original_recovery') or {}
    before = previous.get('execution_spec') or {}
    proof = recovery['seal'].get('preflight_continuation') or {}
    interrupted_assessment = proof.get('interrupted_assessment')
    expected_generation = before.get('gate_retry_preflight_generation', 0) + 1
    counts = (type(spec.get('gate_retry_preflight_generation')) is int
              and spec['gate_retry_preflight_generation'] == expected_generation
              and expected_generation <= 2
              and spec.get('gate_retry_generation') == before.get('gate_retry_generation')
              and not previous.get('seal', {}).get('preflight_continuation', {}).get(
                  'interrupted_assessment')) if interrupted_assessment else (
                  spec.get('gate_retry_preflight_generation') == 1
                  and not before.get('gate_retry_preflight_generation')
                  and (spec.get('gate_retry_generation'),
                       before.get('gate_retry_generation')) == (2, 2))
    if (not counts
            or (spec.get('gate_retry_stage'), before.get('gate_retry_stage')) != (
                'published', 'published')
            or proof.get('recovery_digest') != digest(previous)
            or recovery['seal'].get('attribution_observation') != previous.get('seal', {}).get(
                'attribution_observation')
            or continuation_authority(spec['policy']) != continuation_authority(before['policy'])
            or any(spec.get(key) != before.get(key) for key in (
                'run_id', 'work_id', 'state_dir', 'checkout', 'source_path', 'branch', 'base_sha',
                'accepted_plan', 'feature_worker', 'feature_delivery', 'feature_plan_revision',
                'config_digest', 'plan_approval', 'authorized_endpoint', 'origin_url', 'base_ref',
                'publication_base_ref', 'expected_paths', 'implementation_role_home_generation',
                'provider'))
            or recovery['command'].get('additional_iterations') != 0
            or recovery['state'] != proof.get('closed', {}).get('result')):
        raise ValueError('gate preflight continuation changed its retained assessment authority')
    if interrupted_assessment:
        _authority(before, previous)  # Authenticate the already admitted original bound.
    return previous, before, proof


def _prepared_envelope(root, seal):
    """Read the canonical unadmitted producer bytes without revising their runtime."""
    from .delivery_preparation import _private_bytes

    path = root / 'admission.json'
    raw = _private_bytes(path, root)
    prepared = json.loads(raw)
    if raw != (canonical_json(prepared) + '\n').encode():
        raise ValueError('prepared gate handoff lost canonical producer bytes')
    _authority(prepared['execution_spec'], prepared)
    old, current = deepcopy(prepared['seal']), deepcopy(seal)
    for value in (old, current):
        value.pop('prepared_handoff', None)
        value['preflight_continuation'].pop('runtime', None)
    command = prepared['command']
    candidate = {**seal['candidate'], 'policy_digest': prepared['execution_spec']['policy_digest']}
    expected = {'continuation_kind': 'published_gate_retry', 'additional_iterations': 0,
                'expected_revision': seal['row']['protocol_revision'],
                'expected_iteration': seal['row']['iteration'],
                'expected_candidate_id': seal['candidate']['id'],
                'expected_pr_head': seal['candidate']['head'],
                'expected_pr_number': seal['publication']['number']}
    if (old != current or set(command) != set(expected) | {'command_id'}
            or any(command[k] != v for k, v in expected.items())
            or prepared.get('kind') != 'published_gate_retry'
            or prepared.get('candidate') != candidate
            or prepared.get('publication') != {**seal['publication'], 'candidate': candidate}
            or prepared.get('original_spec') != prepared['original_recovery']['original_spec']
            or prepared['execution_spec'].get('gate_retry_prepared_handoff')):
        raise ValueError('prepared gate handoff changed its original unstarted checkpoint')
    spec = prepared['execution_spec']
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        from .delivery_resources import read_private
        from .delivery_session_custody import implementation_generation

        intent = read_private(root / 'runtime-preparation/intent.json')
        expected_spec = deepcopy(intent.get('effective_spec'))
        if not expected_spec:
            raise ValueError('prepared gate handoff lost its completed native preparation')
        expected_spec.update(
            implementation_role_home_generation=implementation_generation(
                prepared['seal']['original_spec'], prepared['original_recovery']),
            role_home_generation='gate-retry-2-preflight-continuation',
            gate_retry_generation=2, gate_retry_preflight_generation=1,
            gate_retry_stage='published')
        if (intent['command_digest'] != digest({'run_id': spec['run_id'], **command})
                or intent['seal_digest'] != digest(prepared['seal'])
                or expected_spec != spec):
            raise ValueError('prepared gate handoff lost its exact native intent')
        verify_native_spec(spec)
    return prepared, {'sha256': hashlib.sha256(raw).hexdigest(),
                      'command_digest': digest({'run_id': spec['run_id'], **command})}


def prepared_handoff(store, seal, root):
    """One fixed completion namespace for a prepared but never dispatched admission."""
    if not (root / 'admission.json').exists():
        return None
    prepared, proof = _prepared_envelope(root, seal)
    proof['coordinator'] = _prepared_coordinator(store, prepared)
    with store._connect() as db:
        command = db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                             (prepared['command']['command_id'],)).fetchone()
        admitted = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                              (seal['original_spec']['run_id'],)).fetchone()
        if (command or not admitted
                or json.loads(admitted[0]) != prepared['original_recovery']):
            raise ValueError('prepared gate handoff was already admitted or dispatched')
    return proof


def _prepared_coordinator(store, prepared):
    """The orphan command must be the server-derived command of one admitted owner."""
    from .delivery_feature_execution import registry

    spec = prepared['execution_spec']
    if not spec.get('feature_worker'):
        raise ValueError('prepared gate handoff has no feature coordinator provenance')
    shared = registry(spec)
    closed = prepared['seal']['closed']
    issue_id = spec['feature_delivery']['owner']['issue_id']
    matches = []
    with shared.connect() as db:
        for row in db.execute('SELECT * FROM execution_snapshots WHERE issue_id=? AND store_path=?',
                              (issue_id, str(store.config.tracking_db))):
            owner = {key: row[key] for key in ('issue_id', 'run_id', 'store_path', 'generation')}
            command = 'feature-resume-' + digest({
                'owner': owner, 'run': spec['run_id'],
                'closed': {key: closed[key] for key in ('workflow_id', 'execution_run_id')},
            })[:24]
            if command == prepared['command']['command_id']:
                if digest(json.loads(row['input_json'])) != row['input_digest']:
                    raise ValueError('prepared gate coordinator snapshot changed')
                matches.append({'owner': owner, 'snapshot_sha256': row['input_digest']})
    if len(matches) != 1:
        raise ValueError('prepared gate handoff has no exact coordinator command')
    return matches[0]


def validate(store, spec, recovery, *, resource_spec=None):
    """The measured overlay retains the original assessment and implementation authority."""
    from . import delivery_gate_retry as gates
    from .delivery_native_preparation import verify_native_spec
    from .delivery_policy_recovery import _rows

    previous, before, proof = _authority(spec, recovery)
    if spec.get('gate_retry_prepared_handoff'):
        root = Path(spec['state_dir']) / gates.gate_namespace(2, 'published',
                                                           preflight_generation=1)
        prepared, binding = _prepared_envelope(root, recovery['seal'])
        binding['coordinator'] = _prepared_coordinator(store, prepared)
        if (type(spec['gate_retry_prepared_handoff']) is not int
                or spec['gate_retry_prepared_handoff'] != 1
                or binding != recovery['seal'].get('prepared_handoff')):
            raise ValueError('prepared gate handoff lost its original envelope binding')
    elif recovery['seal'].get('prepared_handoff'):
        raise ValueError('prepared gate handoff lost its execution namespace')
    _, attempts, effects, _ = _rows(store, spec['run_id'])
    if (attempts != recovery['seal']['attempts'] or effects != recovery['seal']['effects']
            or store._completed_temporal_result(spec['run_id'], workflow_id=proof['closed'][
                'workflow_id']) != proof['closed']):
        raise ValueError('gate preflight continuation original closed work changed')
    if spec['provider'] != 'fake':
        if spec['policy'].get('native_identity') != proof['runtime']:
            raise ValueError('gate preflight continuation measured runtime changed')
        # Historical observation retains its measured identity. The established
        # native verifier authenticates an installed trusted transport update.
        verify_native_spec(spec)
    if proof.get('interrupted_assessment'):
        from .delivery_closures import sealed_closure

        async def read():
            sealed = await sealed_closure(store, proof['closed']['workflow_id'])
            if sealed is None:
                raise ValueError('interrupted gate predecessor history is unsealed')
            return await interrupted_events(sealed['history'], before, previous, recovery['state'])

        with ThreadPoolExecutor(max_workers=1) as pool:
            history = pool.submit(lambda: asyncio.run(read())).result(35)
        if history != proof['interrupted_assessment']:
            raise ValueError('interrupted gate predecessor history changed')
    gates.readback(store, before, previous, resource_spec=resource_spec or spec)
    source_readback(store, before, previous, resource_spec=resource_spec or spec)
