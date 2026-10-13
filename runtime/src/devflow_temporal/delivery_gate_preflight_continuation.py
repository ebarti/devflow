"""One fresh controller continuation of an admitted, unstarted attribution assessment."""
from __future__ import annotations

import json
from pathlib import Path

from .contracts import digest
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


def possible(row):
    recovery = json.loads(row.get('recovery_json') or 'null')
    return bool(recovery and recovery.get('kind') == 'published_gate_retry'
                and recovery.get('execution_spec', {}).get('gate_retry_generation') == 2
                and recovery['execution_spec'].get('gate_retry_stage') == 'published'
                and not recovery['execution_spec'].get('gate_retry_preflight_generation')
                and recovery.get('seal', {}).get('attribution_observation')
                and row.get('error') == 'gate retry preflight failed: ActivityError')


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

    proof = observe(store, spec, row, attempts, effects, closed)
    original = json.loads(row['recovery_json'])['seal']['attribution_observation']
    return {'row': gates._sealed_row(row), 'closed': closed, 'original_spec': spec,
            'attempts': attempts, 'effects': effects,
            'candidate': json.loads(row['candidate_json']),
            'publication': json.loads(row['pr_json']), 'cleanup': _stopped_cleanup(spec),
            'work_binding': proof['work_binding'], 'previous_digest': proof['recovery_digest'],
            'prior_gate_digest': proof['recovery_digest'], 'stage': 'published', 'generation': 2,
            'attribution_observation': original,
            'preflight_continuation': proof}


def validate(store, spec, recovery):
    """The measured overlay retains the original assessment and implementation authority."""
    from . import delivery_gate_retry as gates
    from .delivery_native_preparation import native_identity
    from .delivery_policy_recovery import _rows

    previous = recovery.get('original_recovery') or {}
    before = previous.get('execution_spec') or {}
    proof = recovery['seal'].get('preflight_continuation') or {}
    if (spec.get('gate_retry_preflight_generation') != 1
            or before.get('gate_retry_preflight_generation')
            or (spec.get('gate_retry_generation'), before.get('gate_retry_generation')) != (2, 2)
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
                'publication_base_ref', 'expected_paths', 'implementation_role_home_generation'))
            or recovery['command'].get('additional_iterations') != 0
            or recovery['state'] != proof.get('closed', {}).get('result')):
        raise ValueError('gate preflight continuation changed its retained assessment authority')
    _, attempts, effects, _ = _rows(store, spec['run_id'])
    if (attempts != recovery['seal']['attempts'] or effects != recovery['seal']['effects']
            or store._completed_temporal_result(spec['run_id'], workflow_id=proof['closed'][
                'workflow_id']) != proof['closed']):
        raise ValueError('gate preflight continuation original closed work changed')
    if spec['provider'] != 'fake' and (
            native_identity(spec) != proof['runtime']
            or spec['policy'].get('native_identity') != proof['runtime']):
        raise ValueError('gate preflight continuation measured runtime changed')
    gates.readback(store, before, previous, resource_spec=spec)
    source_readback(store, before, previous, resource_spec=spec)
