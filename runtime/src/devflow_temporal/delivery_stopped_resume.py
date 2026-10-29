"""Append-only, finite public resume of an authenticated stopped delivery."""
from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_baseline_contract import preparation_failure, repairable_baseline
from .delivery_broker import DeliveryBroker, _git
from .delivery_browser_mode_recovery import FIELDS as BROWSER_FIELDS
from .delivery_budget_contract import continuation_maximum, fixed_budget_allows
from .delivery_config import ordinary_scope_base, scope_amended_spec, scope_amendment_config
from .delivery_execution_config import load_execution_config
from .delivery_gate_retry import prepare_runtime, transient_stop
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import (
    _remote,
    _rows,
    _stopped_cleanup,
    cleanup_only_build,
    closed_checks,
    completed_cleanup_binding,
    reconciled_cleanup,
    work_binding,
)
from .delivery_preparation import _lock
from .delivery_repair import _ci_diagnostics, published_identity
from .delivery_resources import private_directory, read_private
from .delivery_temporal_prerequisites import FIELDS as TEMPORAL_FIELDS

KIND = 'stopped_delivery_resume'
FIELDS = {'continuation_kind', 'command_id', 'expected_revision', 'expected_iteration',
          'expected_candidate_id', 'expected_candidate_head', 'additional_iterations'}
SCOPE_FIELDS = {'added_paths', 'amended_config_path', 'amended_config_sha256'}


def scope_spec(store, seal, command, *, _feature_scope=None):
    """Add only explicitly named, unchanged tracked files to an authentic stopped role."""
    spec = seal['predecessor_spec']
    if not SCOPE_FIELDS <= command.keys():
        return spec
    if spec.get('feature_worker'):
        from .delivery_feature_scope import worker_scope

        effective = worker_scope(store, seal, command, _feature_scope)
    else:
        latest = next((role for role in reversed(seal['state'].get('roles', []))
                       if role.get('role') == 'implement'), None)
        if (not latest or latest.get('status') not in {'findings', 'blocked'}
                or latest.get('finish_reason') != 'done' or not seal['session_id']
                or not latest.get('findings')
                or seal['state'].get('error') != 'implementer did not establish a pass'):
            raise ValueError('scope resume requires an authenticated implementation finding')
        if latest['status'] == 'blocked':
            from .delivery_session_custody import completed_blocked_finding

            completed_blocked_finding(seal)
        path = command['amended_config_path']
        if not isinstance(path, str) or not isinstance(command['amended_config_sha256'], str):
            raise ValueError('scope amendment configuration must be absolute')
        effective = scope_amended_spec(spec, Path(path), command['amended_config_sha256'],
                                       command['added_paths'])
    broker = DeliveryBroker(store, spec)
    changed = broker._changed_paths(spec['base_sha'])
    for name in command['added_paths']:
        parts = Path(name).parts
        if (not parts or Path(name).is_absolute() or name != Path(name).as_posix()
                or any(part in {'.', '..', '.git', '.codex'} for part in parts)):
            raise ValueError('scope amendment must name tracked files')
        try:
            tracked = _git(broker.checkout, 'ls-files', '--error-unmatch', '--', name)
        except RuntimeError as exc:
            raise ValueError('scope amendment file is not tracked') from exc
        target = broker.checkout / name
        if (tracked != name or name in changed or target.is_symlink()
                or not stat.S_ISREG(target.lstat().st_mode)):
            raise ValueError('scope amendment file is already changed or unsafe')
        parent = target.parent
        while parent != broker.checkout:
            if not stat.S_ISDIR(parent.lstat().st_mode) or parent.is_symlink():
                raise ValueError('scope amendment file has an unsafe ancestor')
            parent = parent.parent
    # The delta is still a proposal. Keep source observation under the sealed
    # predecessor custody until admission grants the new execution namespace.
    if not spec.get('feature_worker') and spec['provider'] == 'codex':
        observed_preparation_cleanup(spec)
    amended = broker.candidate()
    if any(amended[key] != seal['candidate'][key] for key in (
            'id', 'head', 'content_sha256', 'base_sha', 'environment_digest')):
        raise ValueError('scope amendment changed stopped implementation bytes')
    if not spec.get('feature_worker') and spec['provider'] == 'codex':
        ordinary_scope_base(load_execution_config(effective), spec)
    return effective



def remaining_worker_repairs(budget, spec, iteration, *, credit=0):
    """Only a renewed receipt replaces the original worker's absolute ceiling."""
    remaining = budget['maximum'] - budget['used'] + credit
    return (remaining if budget.get('allowance', {}).get('sequence') else
            min(remaining, spec['policy']['max_repairs'] - iteration))


def _observation_registry(spec):
    from .delivery_execution_config import is_observation_mode

    if is_observation_mode():
        from .delivery_feature_revision_subject import _ReadOnlyRegistry

        return _ReadOnlyRegistry(spec["feature_delivery"]["registry"])
    from .delivery_feature_execution import registry

    return registry(spec)


def paid_alias_credit(spec, owner, iteration, revision_id):
    """A sealed existing alias spends its bound debit, including across owners."""
    key = 'plan-revision:repair-alias:' + spec['run_id'] + ':' + str(iteration)
    with _observation_registry(spec).connect() as db:
        row = db.execute('SELECT content_digest,content_json FROM execution_checkpoints '
                         'WHERE issue_id=? AND checkpoint_key=?',
                         (owner['issue_id'], key)).fetchone()
        if not row:
            return None
        alias = json.loads(row['content_json'])
        debit = db.execute('SELECT 1 FROM execution_repairs WHERE issue_id=? AND repair_key=?',
                           (owner['issue_id'], alias.get('repair_key'))).fetchone()
    if (row['content_digest'] != digest(alias) or not debit
            or alias.get('revision_id') != revision_id
            or alias.get('reason') != 'Product repair at worker iteration ' + str(iteration)
            or type(alias.get('generation')) is not int
            or not 1 <= alias['generation'] <= owner['generation']):
        raise ValueError('worker paid repair credit lost its exact alias debit')
    return {'kind': 'alias', 'iteration': iteration, 'revision_id': revision_id,
            'checkpoint_key': key, 'sha256': digest(alias)}


def validate_worker_allowance(spec, *, current=False):
    """Authenticate the receipt even when reading an old immutable recovery."""
    authority = spec['repair_allowance']
    if (not isinstance(authority, dict)
            or set(authority) not in ({'run_id', 'owner', 'receipt', 'start_iteration',
                                      'maximum_iteration'},
                                     {'run_id', 'owner', 'receipt', 'start_iteration',
                                      'maximum_iteration', 'credit'})
            or not fixed_budget_allows(spec, authority.get('start_iteration'), 0, pending=True)
            or not isinstance(authority['owner'], dict)
            or authority['owner'].get('issue_id') != spec['feature_delivery']['owner']['issue_id']
            or authority['owner'].get('store_path')
                != spec['feature_delivery']['owner']['store_path']):
        raise ValueError('worker changed its finite runtime repair allowance')
    receipt = _observation_registry(spec).repair_allowance(authority['owner'], authority['receipt'],
                                             current=current)
    credit = authority.get('credit')
    if credit:
        if (not isinstance(credit, dict)
                or credit.get('iteration') != authority['start_iteration'] + 1
                or not isinstance(credit.get('revision_id'), str)):
            raise ValueError('worker changed its prepaid repair credit')
        if credit.get('kind') == 'alias':
            if paid_alias_credit(spec, authority['owner'], credit['iteration'],
                                 credit['revision_id']) != credit:
                raise ValueError('worker changed its prepaid alias credit')
        elif (credit.get('kind') != 'gates'
              or set(credit) != {'kind', 'iteration', 'revision_id', 'evidence_sha256'}
              or not re.fullmatch(r'[0-9a-f]{64}', str(credit.get('evidence_sha256', '')))):
            raise ValueError('worker changed its sealed gate credit')
    if (authority['maximum_iteration'] - authority['start_iteration']
            > receipt['maximum'] + int(bool(credit))):
        raise ValueError('worker exceeded its installed runtime repair allowance')
    if current:
        from .delivery_feature_review_context import current_runtime_identity

        if current_runtime_identity()['runtime_payload_sha256'] != (
                authority['receipt']['runtime_payload_sha256']):
            raise ValueError('installed runtime differs from the sealed repair allowance')
    return authority


def worker_allowance(spec, iteration, iterations, *, credit=None):
    """Derive extension authority from the current owner, never command fields."""
    if not spec.get('feature_worker'):
        return None
    from .delivery_feature_execution import registry
    from .delivery_feature_review_context import current_runtime_identity

    shared = registry(spec)
    issue_id = spec['feature_delivery']['owner']['issue_id']
    budget = shared.budget(issue_id)
    reference = budget.get('allowance')
    if not reference or reference['sequence'] == 0:
        return None
    if current_runtime_identity()['runtime_payload_sha256'] != reference['runtime_payload_sha256']:
        raise ValueError('installed runtime differs from the current repair allowance')
    current = shared.current(issue_id)
    if not current or current['store_path'] != spec['feature_delivery']['owner']['store_path']:
        raise ValueError('worker repair allowance belongs to a different owner store')
    owner = shared.token(current)
    shared.repair_allowance(owner, reference, current=True)
    if iterations > budget['maximum'] - budget['used'] + int(bool(credit)):
        from .delivery_lifecycle import require_observed_predicates

        require_observed_predicates(KIND, 'feature_registry', {'owner': owner, 'budget': budget}, [{
            'code': 'product_budget_exhausted', 'satisfied': False,
            'missing_or_unknown_fact': 'remaining authenticated feature product repair allowance',
            'next_action': 'inspect the original feature repair allowance',
            'new_authorization_required': True, 'user_input_required': None,
        }], lifecycle='unknown', legacy_message='feature product repair limit exhausted')
    return {'run_id': spec['run_id'], 'owner': owner, 'receipt': reference,
            'start_iteration': iteration, 'maximum_iteration': iteration + iterations,
            **({'credit': credit} if credit else {})}


def _recorded_findings(state):
    """Findings the stopped iteration's gates recorded for the next repair turn."""
    iteration = state.get('iteration')
    recorded = state.get('repair_findings') or {}
    if recorded.get('iteration') == iteration and recorded.get('findings'):
        return list(recorded['findings'])
    return [finding for role in state.get('roles', [])
            if role.get('iteration') == iteration and role.get('role') in {'review', 'verify'}
            for finding in role.get('findings') or []]


def sealed_ci_findings(seal):
    """Failed required-CI diagnostics for a stop that recorded no other findings.

    A transport or controller stop that kept a failed CI verdict is repaired
    from the failed jobs' logs, sealed here exactly as the repair grant seals
    them; when they cannot be sealed the admission refuses, so no repair turn
    ever starts without findings.
    """
    state = seal['state']
    ci = (state.get('checks') or {}).get('ci')
    if (not transient_stop(state) or not isinstance(ci, dict) or ci.get('state') != 'failed'
            or _recorded_findings(state)):
        return None
    return _ci_diagnostics(ci, seal['predecessor_spec'], state.get('pull_request'))


def _unstarted_preflight(seal, previous, db):
    """The admitted quarantined continuation closed before entering its new turn."""
    inherited = previous.get('pending_repair') or {}
    if not (previous.get('quarantined_implementation')
            or inherited.get('kind') == 'unstarted_preflight'):
        return None
    spec, state, before = seal['predecessor_spec'], seal['state'], previous['state']
    failure = state.get('checks', {}).get('failure') or {}
    if (spec.get('provider') != 'codex' or spec.get('feature_worker')
            or spec.get('retry_budget_version') != 1
            or state.get('phase') != state.get('outcome') or state.get('phase') != 'blocked'
            or state.get('execution_state') != 'blocked'
            or state.get('cleanup') not in {'unknown', 'confirmed'}
            or state.get('error') != 'repair authority preflight failed: ActivityError'
            or failure != {'classification': 'terminal', 'stage': 'repair_preflight',
                           'reason': state['error']}):
        return None
    ignored = {'failure', 'controller_failure', 'resource_cleanup', 'terminal_tracker_checkpoint'}
    if (custody(db, seal['original_recovery']) != spec
            or previous['execution_spec'] != spec or state['iteration'] != before['iteration']
            or state.get('roles') != before.get('roles')
            or state.get('usage') != before.get('usage')
            or seal['attempts'] != previous['attempts'] or seal['effects'] != previous['effects']
            or seal.get('session_id') is not None or previous.get('session_id') is not None
            or seal['candidate'] != state.get('candidate')
            or seal['candidate'] != previous['execution_candidate']
            or state.get('pull_request') is not None
            or any(a['iteration'] > state['iteration'] for a in seal['attempts'])
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed'
                   for a in seal['attempts'])
            or any(e['state'] != 'complete' or e['observed_json'] is None for e in seal['effects'])
            or {k: v for k, v in state.get('checks', {}).items() if k not in ignored}
            != {k: v for k, v in before.get('checks', {}).items() if k not in ignored}):
        raise ValueError('unstarted preflight lost its exact admission or unchanged inventory')
    return {'kind': 'unstarted_preflight', 'iteration': state['iteration'] + 1,
            'findings': [], 'maximum_iteration': previous['maximum_iteration'],
            'predecessor_admission_sha256': digest(seal['original_recovery'])}


def pending_repair(seal, *, db=None):
    """Recover unexecuted work from the sealed admission that requested it."""
    from .delivery_feature_revision_recovery import retained_recovery

    previous = retained_recovery(seal.get('original_recovery')) or {}
    if (previous.get('kind') != KIND
            or seal['predecessor_spec'].get('feature_worker', {}).get('kind') == 'build'):
        return None
    if db is not None:
        preflight = _unstarted_preflight(seal, previous, db)
        if preflight is not None:
            return preflight
    unstarted = None
    if not seal.get('session_custody', {}).get('missing_rollout'):
        unstarted = _unstarted_repair(seal, previous, db) if db is not None else None
        if not unstarted:
            return None
        if previous.get('pending_repair'):
            return {**previous['pending_repair'], **(
                {'implementation_preparation': unstarted} if isinstance(unstarted, dict) else {})}
    state = previous['state']
    if previous.get('resume_iteration', state['iteration'] + 1) == state['iteration']:
        # The predecessor re-ran its own sealed iteration; it requested no repair.
        return None
    iteration = state['iteration'] + 1
    if (previous['execution_spec'] != seal['predecessor_spec']
            or iteration != seal['state']['iteration'] or iteration <= 0):
        raise ValueError('unstarted repair differs from its sealed predecessor admission')
    findings = [state['error'], *[
        finding for role in state.get('roles', [])
        if role.get('iteration') == state['iteration']
        for finding in role.get('findings', [])]]
    return {'iteration': iteration,
            'findings': [finding for finding in findings if finding is not None],
            **({'implementation_preparation': unstarted} if isinstance(unstarted, dict) else {})}


def _unstarted_repair(seal, previous, db, *, closing=False):
    """An exact admitted and charged repair has not reached a provider turn."""
    spec, state = seal['predecessor_spec'], seal['state']
    prior = previous['state']
    iteration = state['iteration']
    requested = (previous.get('pending_repair') or {}).get(
        'iteration', previous.get('resume_iteration', prior['iteration'] + 1))
    if ((spec.get('feature_worker') and not spec.get('repair_allowance'))
            or (not spec.get('feature_worker') and spec.get('retry_budget_version') != 1)
            or requested != iteration or iteration <= 0
            or (requested == prior['iteration'] and not previous.get('pending_repair'))):
        return False
    rejected_input = (not spec.get('feature_worker')
                      and bool(seal.get('session_custody', {}).get('input_rejection')))
    current_attempts = [a for a in seal['attempts'] if a['iteration'] == iteration]
    if not spec.get('feature_worker') and current_attempts and not rejected_input:
        return False  # A consumed ordinary turn follows the normal repair rules.
    if custody(db, seal['original_recovery']) != spec:
        raise ValueError('unstarted repair lost its exact predecessor admission')
    candidate = seal['candidate']
    roles, before = state.get('roles', []), prior.get('roles', [])
    remainder = roles[len(before):]
    if not closing and _completed_repair(seal, previous, remainder):
        # This paid turn ran. Ordinary stopped-repair proof decides its next turn.
        return False
    preparation = remainder[0].get('implementation_preparation', {}) if len(remainder) == 1 else {}
    unlaunched = (len(remainder) == 1 and remainder[0].get('role') == 'implement'
                  and remainder[0].get('iteration') == iteration
                  and remainder[0].get('status') == 'blocked'
                  and remainder[0].get('session_id') is None
                  and remainder[0].get('cleanup') == 'confirmed'
                  and remainder[0].get('candidate') == candidate
                  and preparation.get('state') == 'failed'
                  and preparation.get('cleanup') == 'confirmed'
                  and preparation.get('source_unchanged') is True
                  and preparation.get('results')
                  and all(item.get('cleanup') == 'confirmed'
                          for item in preparation['results'])
                  and preparation['results'][-1].get('launched') is False
                  and preparation['results'][-1].get('failure_kind') == 'preparation')
    native_preparation = None
    if (not closing and not unlaunched and len(remainder) == 1
            and remainder[0].get('role') == 'implement'
            and remainder[0].get('iteration') == iteration
            and remainder[0].get('status') == 'blocked'
            and remainder[0].get('session_id') is None
            and remainder[0].get('cleanup') == 'confirmed'
            and remainder[0].get('candidate') == candidate
            and preparation.get('state') == 'failed'
            and preparation.get('cleanup') == 'confirmed'
            and preparation.get('source_unchanged') is True):
        from .delivery_preparation_recovery import completed_prerequisite

        native_preparation = completed_prerequisite(seal, previous, remainder[0])
        unlaunched = bool(native_preparation)
    rejected_input = (rejected_input and len(remainder) == len(current_attempts) == 1
                      and remainder[0].get('role') == current_attempts[0]['role'] == 'implement'
                      and remainder[0].get('iteration') == iteration
                      and remainder[0].get('status') == 'blocked'
                      and remainder[0].get('session_id') is None
                      and remainder[0].get('finish_reason') == 'exception'
                      and remainder[0].get('cleanup') == 'confirmed'
                      and remainder[0].get('candidate') == candidate)
    attempts_before = ([a for a in seal['attempts'] if a not in current_attempts]
                       if rejected_input else seal['attempts'])
    current_custody, prior_custody = seal.get('session_custody'), previous.get('session_custody')
    if rejected_input:
        # SDK resume may append settings metadata before rejecting turn/start.
        # Exact typed rejection proof retains the same session/home and original
        # owner; the new admission seals its observed session bytes, not a rewind.
        current_custody, prior_custody = [
            {k: v for k, v in (value or {}).items()
             if k not in {'input_rejection', 'session_origin', 'session_state_digest'}}
            for value in (current_custody, prior_custody)]
    if (previous['execution_spec'] != spec
            or candidate != state.get('candidate')
            or candidate != previous['execution_candidate']
            or candidate != {**previous['candidate'], 'policy_digest': spec['policy_digest']}
            or roles[:len(before)] != before or (remainder and not (unlaunched or rejected_input))
            or attempts_before != previous['attempts']
            or seal['effects'] != previous['effects']
            or (current_attempts and not rejected_input)
            or any(attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
                   for attempt in seal['attempts'])
            or any(effect['state'] != 'complete' or effect['observed_json'] is None
                   for effect in seal['effects'])
            or seal.get('session_id') != previous.get('session_id')
            or current_custody != prior_custody
            or (not closing and (state.get('phase') not in {'blocked', 'cancelled'}
                                  or state.get('cleanup') != 'confirmed'))
            or (closing and (state.get('phase') != 'cancelling'
                             or state.get('cleanup') != 'pending_role_completion'
                             or state.get('outcome') is not None))):
        raise ValueError('unstarted repair differs from its unchanged source or receipt inventory')
    custody_record = previous.get('session_custody')
    if custody_record and (spec.get('implementation_role_home_generation')
                           != custody_record['generation']):
        raise ValueError('unstarted repair changed its original implementation home')
    # A successor runtime renews allowance authority, not the already paid debit.
    from .delivery_feature_revision_recovery import retained_recovery

    origin = previous
    while origin.get('pending_repair'):
        prior_link = retained_recovery(origin.get('original_recovery')) or {}
        if (prior_link.get('kind') != KIND
                or origin['pending_repair'].get('iteration') != iteration):
            raise ValueError('unstarted repair lost its original debit admission')
        origin = prior_link
    if not spec.get('feature_worker'):
        # Ordinary deliveries have no shared feature debit. The immutable
        # admission plus unchanged inventory and absent provider turn above prove
        # that this exact fixed-budget iteration has not executed yet.
        return native_preparation or True
    shared = _observation_registry(spec)
    owner = origin['execution_spec']['repair_allowance']['owner']
    current_owner = spec['repair_allowance']['owner']
    if any(current_owner.get(key) != owner.get(key) for key in ('issue_id', 'store_path')):
        raise ValueError('unstarted repair changed its original debit owner')
    current = shared.current(owner['issue_id'])
    with shared.connect() as registry_db:
        debit = registry_db.execute(
            'SELECT generation,reason FROM execution_repairs WHERE issue_id=? AND repair_key=?',
            (owner['issue_id'], f"{spec['run_id']}:{iteration}")).fetchone()
    if (not current or current['issue_id'] != owner['issue_id']
            or current['store_path'] != owner['store_path']
            or current['generation'] < owner['generation']
            or not debit or debit['generation'] != owner['generation']
            or debit['reason'] != 'Product repair at worker iteration ' + str(iteration)):
        raise ValueError('unstarted repair lacks its exact existing product debit')
    return True


def _completed_repair(seal, previous, remainder):
    """Authenticate progress beyond the unchanged, unstarted admission inventory."""
    from .supervisor import DeliverySupervisor

    spec, state = seal['predecessor_spec'], seal['state']
    completed = [role for role in remainder if role.get('role') == 'implement'
                 and role.get('iteration') == state['iteration']]
    if len(completed) != 1:
        return False
    role = completed[0]
    candidate = previous['execution_candidate']
    recorded = []
    for progressed in remainder:
        if (progressed.get('role') not in {'implement', 'review', 'verify'}
                or progressed.get('iteration') != state['iteration']
                or not progressed.get('input_candidate_id')):
            return False
        expected = DeliverySupervisor._job_key({
            'spec': spec, 'role': progressed['role'], 'iteration': state['iteration'],
            'candidate': {'id': progressed['input_candidate_id']}})
        if progressed.get('attempt_id', expected) != expected:
            return False
        receipt = {key: value for key, value in progressed.items()
                   if key not in {'role', 'iteration', 'candidate', 'attempt_id',
                                  'input_candidate_id', 'provider'}}
        matches = [attempt for attempt in seal['attempts']
                   if attempt.get('job_key') == expected and attempt['role'] == progressed['role']
                   and attempt['iteration'] == state['iteration']
                   and attempt['candidate_id'] == progressed['input_candidate_id']
                   and attempt['session_id'] == progressed.get('session_id')
                   and attempt['state'] == 'finished' and attempt['cleanup'] == 'confirmed'
                   and json.loads(attempt['result_json']) == receipt]
        if len(matches) != 1:
            return False
        recorded.extend(matches)
    before = previous['state'].get('roles', [])
    return (previous['execution_spec'] == spec and state.get('roles', [])[:len(before)] == before
            and role.get('status') in {'pass', 'findings', 'blocked'}
            and role.get('cleanup') == 'confirmed'
            and role.get('input_candidate_id') == candidate['id']
            and bool(previous.get('session_id'))
            and role.get('session_id') == seal.get('session_id') == previous['session_id']
            and (seal.get('session_custody') or {}).get('generation')
                == (previous.get('session_custody') or {}).get('generation')
            and all(role.get('candidate', {}).get(key) == seal['candidate'].get(key)
                    for key in ('base_sha', 'environment_digest', 'policy_digest'))
            # A nonpass role stops before the workflow replaces its top candidate.
            # _candidate must still authenticate that role's exact current source.
            and (role['status'] != 'pass' or role['candidate'].get('content_sha256')
                 == seal['candidate'].get('content_sha256'))
            and all(attempt in seal['attempts'] for attempt in previous['attempts'])
            and all(attempt in previous['attempts'] or attempt in recorded
                    for attempt in seal['attempts'])
            and all(effect in seal['effects'] for effect in previous['effects'])
            and all(effect in previous['effects'] or (
                effect['effect_key'] == f"publish:{spec['run_id']}:{state['iteration']}"
                and effect['kind'] == 'publish' and effect['state'] == 'complete'
                and json.loads(effect['request_json']) == {
                    'input_candidate_id': role['candidate']['id'], 'iteration': state['iteration']}
                and state.get('pull_request') is not None
                and json.loads(effect['observed_json']) == state['pull_request'])
                    for effect in seal['effects']))


def _interrupted_preparation(seal):
    """Only an unlaunched prerequisite, never a failed product assessment."""
    state = seal['state']
    checks = state.get('checks') or {}
    failure = checks.get('failure') or {}
    stage = {'prepublish_checks': 'prepublish', 'checks': 'local'}.get(failure.get('stage'))
    check = checks.get(stage) or {}
    prerequisite = preparation_failure(check)
    reason = (f'environment preparation failed: {prerequisite}; '
              'candidate retained without requesting code repair')
    failed = [item for item in check.get('results', [])
              if not isinstance(item, dict) or item.get('passed') is not True]
    checkpoint = checks.get('terminal_tracker_checkpoint') or {}
    masked = (state.get('error') == 'terminal tracker synchronization conflicted'
              and checkpoint.get('state') == 'conflicted'
              and checkpoint.get('closed') is True and checkpoint.get('waiting') is False
              and checkpoint.get('release') is False
              and checkpoint.get('event') == checkpoint.get('outcome') == 'blocked'
              and checkpoint.get('error') == checkpoint.get('reason') == reason)
    if (seal['predecessor_spec'].get('feature_worker', {}).get('kind') != 'chunk'
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or not state.get('pull_request')
            or failure.get('classification') != 'terminal' or failure.get('reason') != reason
            or (state.get('error') != reason and not masked)
            or check.get('state') != 'failed' or check.get('source_unchanged') is not True
            or check.get('cleanup') != 'confirmed'
            or check.get('candidate_id') != seal['candidate']['id']
            or prerequisite is None or len(failed) != 1
            or failed[0] != check['results'][-1] or failed[0].get('passed') is not False
            or failed[0].get('failure_kind') != 'preparation'
            or failed[0].get('launched') is not False or failed[0].get('cleanup') != 'confirmed'):
        return None
    return stage


def _unlaunched_assessments(seal):
    """Ordinary published work needs gates, not coding, after exact no-process launches."""
    from .supervisor import DeliverySupervisor

    spec, state, candidate = seal['predecessor_spec'], seal['state'], seal['candidate']
    if (spec.get('feature_worker') or state.get('phase') != 'blocked'
            or state.get('error') not in {'repair limit exhausted',
                                         'independent assessment launch failed before provider'}
            or (state.get('pull_request') or {}).get('head') != candidate['head']):
        return False
    roles = state.get('roles', [])
    last = next((i for i in range(len(roles) - 1, -1, -1)
                 if roles[i].get('role') == 'implement'), None)
    later = roles[last + 1:] if last is not None else []
    if not later or [r.get('role') for r in later] not in (
            ['review'], ['review', 'verify']):
        return False
    checks = state.get('checks') or {}
    for stage in ['local', *(['browser_qa'] if spec['policy'].get('browser_qa') else [])]:
        checked = checks.get(stage, {})
        if (checked.get('state') != 'passed' or checked.get('candidate_id') != candidate['id']
                or checked.get('source_unchanged') is not True
                or (stage == 'browser_qa' and checked.get('cleanup') != 'confirmed')):
            return False
    unlaunched = False
    for role in later:
        key = DeliverySupervisor._job_key({'spec': spec, 'role': role['role'],
            'iteration': state['iteration'], 'candidate': candidate})
        matches = [a for a in seal['attempts'] if a['job_key'] == key]
        if len(matches) != 1:
            return False
        attempt = matches[0]
        if (role.get('cleanup') != 'confirmed' or role.get('candidate') != candidate
                or role.get('iteration') != state['iteration']
                or attempt.get('role') != role['role']
                or attempt.get('iteration') != state['iteration']
                or attempt.get('state') != 'finished' or attempt.get('cleanup') != 'confirmed'
                or not _implementation_receipt(spec, role, attempt)):
            return False
        if role.get('status') == 'pass':
            if (not isinstance(role.get('session_id'), str) or not role['session_id']
                    or attempt.get('session_id') != role['session_id'] or role.get('findings')):
                return False
            continue
        folder = Path(spec['state_dir']) / 'attempts' / key
        try:
            info = folder.lstat()
            absent = (folder.resolve(strict=True) == folder and stat.S_ISDIR(info.st_mode)
                      and info.st_uid == os.getuid() and stat.S_IMODE(info.st_mode) == 0o700
                      and not any(folder.iterdir()))
        except OSError:
            return False
        if (
            role.get("status") != "blocked"
            or role.get("finish_reason") != "prelaunch"
            or role.get("summary") != "role launch failed before any provider process started"
            or role.get("session_id") is not None
            or role.get("usage") is not None
            or any(attempt.get(k) is not None for k in ("pid", "process_identity", "session_id"))
            or attempt.get("result_path") != str(folder / "result.json")
            or not absent
        ):
            return False
        unlaunched = True
    return unlaunched


def interrupted_resume(seal):
    """Plan a stopped worker's continuation that needs no product repair, if any.

    It stays at the sealed iteration. A passed implementation is never run again, a
    matching published candidate is never published again, and gates rerun fresh under the
    admission's own identity.
    """
    from .delivery_feature_revision_recovery import gate_evidence

    spec, state = seal['predecessor_spec'], seal['state']
    cleanup = (cleanup_only_build(spec, state) and completed_cleanup_binding(
        seal.get('cleanup_reconciliation'), seal.get('closed'), state))
    preparation = _interrupted_preparation(seal)
    assessments = _unlaunched_assessments(seal)
    from .delivery_unpublished_recovery import interrupted_prepublication

    prepublication = interrupted_prepublication(seal)
    if ((not spec.get('feature_worker') and not assessments and not prepublication)
            or (state.get('phase') != 'cancelled' and not cleanup and not preparation
                and not assessments and not prepublication)
            or any(isinstance(check, dict) and check.get('state') == 'failed'
                   and name != preparation and not (assessments and name in {'review', 'qa'})
                   for name, check in state.get('checks', {}).items())):
        return None
    roles, iteration = state.get('roles', []), state['iteration']
    if not roles:
        # The first implementation never started, and iteration zero is never charged.
        return ({'resume_iteration': 0, 'additional_iterations': 0}
                if iteration == 0 and not preparation else None)
    last = max((index for index, role in enumerate(roles) if role.get('role') == 'implement'),
               default=None)
    evidence = gate_evidence(seal)
    attempt = next((item for item in seal['attempts'] if evidence
                    and item['job_key'] == evidence['implementation_job_key']), None)
    if (last is None or roles[last].get('status') != 'pass' or attempt is None
            or attempt['iteration'] != roles[last]['iteration']
            # The iteration only advances past a passed implementation for a repair.
            or roles[last]['iteration'] != iteration
            or (not assessments and any(role.get('status') != 'pass'
                                        for role in roles[last + 1:]))):
        return None
    published = (spec.get('feature_worker', {}).get('kind') != 'build'
                 and (state.get('pull_request') or {}).get('head') == seal['candidate']['head']
                 and (not preparation
                      or state['pull_request'].get('candidate') == seal['candidate']))
    return {'resume_iteration': iteration, 'additional_iterations': 0,
            'resume_stage': 'published' if published else 'checks', 'gate_evidence': evidence}


def admission_identity_consistent(details):
    """Recorded gate and implement admissions must be this admission's. A gate
    admission may be missing only where gates never rerun at the sealed iteration or
    on revision recoveries recorded before it existed; an implement admission may be
    missing on pending repairs recorded before it existed."""
    spec = details['execution_spec']
    if spec.get('implement_admission') not in (None, details['command_digest']):
        return False
    recorded = spec.get('gate_admission')
    if recorded is not None:
        return recorded == details['command_digest']
    return not ('resume_iteration' in details and not details.get('feature_plan_revision'))


def executed_specs(store, spec):
    """Yield the specs this run executed under, newest first.

    Each resume link is validated only when the walk reaches it, so a stale older
    link cannot block an implementation that ran under a newer spec.
    """
    from .delivery_feature_revision_recovery import retained_recovery

    yield spec
    with store._connect() as db:
        row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
    recovery = json.loads(row[0]) if row and row[0] else None
    previous = spec
    while recovery and recovery.get('kind') == KIND:
        # Each admission sealed the spec its predecessor execution ran under.
        with store._connect() as db:
            if custody(db, recovery) != previous:
                raise ValueError('resume chain lost its sealed execution authority')
        details = retained_recovery(recovery)
        previous = details['predecessor_spec']
        yield previous
        recovery = details.get('original_recovery')


def observed_native_cleanup(spec, *, read_only=False, quarantined=None, resource_spec=None):
    """Preflight and readback observe closure without controlling any process."""
    if spec['provider'] == 'fake':
        return digest({'provider': 'fake'})
    if quarantined:
        from .delivery_resources import _gate_roots, observe_finalized_resources

        current = resource_spec or spec
        bindings = {}
        authority = _gate_roots(current, execution_specs=bindings)
        if spec not in bindings.values():
            raise ValueError('historical cleanup lost its admitted execution')
        observed = observe_finalized_resources(current, quarantined=quarantined)
        after = {}
        if (_gate_roots(current, execution_specs=after) != authority or after != bindings):
            raise ValueError('historical cleanup resource authority changed during observation')
        return digest(observed)
    return digest(_stopped_cleanup(spec, read_only=True) if read_only else _stopped_cleanup(spec))


def observed_preparation_cleanup(spec, *, resource_spec=None, quarantined=None):
    """Current native reentry needs retained source identity, not only Git bytes."""
    from .delivery_resources import _gate_roots, observe_finalized_resources

    current = resource_spec or spec
    bindings = {}
    authority = _gate_roots(current, execution_specs=bindings)
    if spec not in bindings.values():
        raise ValueError('preparation cleanup lost its admitted predecessor')
    observed = observe_finalized_resources(current, quarantined=quarantined)
    if observed['roots'].get(current['checkout']) is None:
        raise ValueError('unstarted implementation lacks its registered current source')
    after = {}
    if _gate_roots(current, execution_specs=after) != authority or after != bindings:
        raise ValueError('preparation resource authority changed during observation')
    return digest(observed)


def namespace(recovery):
    return 'stopped-resumes/' + recovery['command_digest']


def custody(db, recovery, *, retained_memo=None):
    from .delivery_feature_revision_recovery import retained_recovery

    details = retained_recovery(recovery, memo=retained_memo)
    spec = details['execution_spec']
    # Revision admissions record their own gate resume; any other resume
    # position comes from an interrupted worker's re-derivable plan.
    interrupted = 'resume_iteration' in details and not details.get('feature_plan_revision')
    if spec.get('repair_allowance'):
        authority = validate_worker_allowance(spec)
        if (authority['start_iteration'] != recovery['state']['iteration']
                or authority['maximum_iteration'] != recovery['maximum_iteration']):
            raise ValueError('stopped resume changed its sealed runtime allowance')
        credit = authority.get('credit')
        if credit and (credit['revision_id'] != (details.get('feature_plan_revision') or {}).get(
                'revision_id') or (credit['kind'] == 'gates' and (
                    details.get('resume_stage') != 'checks'
                    or credit['evidence_sha256'] != digest(details.get('gate_evidence'))))):
            raise ValueError('stopped resume changed its prepaid revision authority')
    if (spec.get('retry_budget_version') == 1
            and not fixed_budget_allows(spec, recovery['state']['iteration'],
                                        recovery['command']['additional_iterations'],
                                        pending=bool(recovery.get('pending_repair') or interrupted
                                                     or details.get('resume_stage') == 'checks'))):
        raise ValueError('a fixed repair budget cannot receive additional iterations')
    path = Path(spec['state_dir']) / namespace(recovery) / 'admission.json'
    command = db.execute('SELECT request_digest,response_json FROM delivery_commands '
                         'WHERE command_id=? AND run_id=?',
                         (recovery['command']['command_id'], spec['run_id'])).fetchone()
    if (not command or command[0] != recovery['command_digest']
            or json.loads(command[1]).get('admission_sha256') != digest(recovery)
            or canonical_json(read_private(path)) != canonical_json(recovery)
            or recovery['predecessor_result_digest'] != digest(details['state'])
            or recovery['maximum_iteration'] != continuation_maximum(
                recovery['state']['iteration'], recovery['command']['additional_iterations'],
                recovery.get('pending_repair'))):
        raise ValueError('stopped resume lost its immutable command authority')
    if (('pending_repair' in recovery
             and pending_repair(details, db=db) != recovery['pending_repair'])
            or (recovery['command']['additional_iterations'] == 0
                and not (recovery.get('pending_repair') or interrupted
                         or details.get('resume_stage') == 'checks'))):
        raise ValueError('stopped resume changed its unstarted repair authority')
    if not admission_identity_consistent(details):
        raise ValueError('stopped resume changed its admission identity')
    from .delivery_unpublished_recovery import quarantine_history

    if details.get('historical_quarantines', []) != quarantine_history(
            details.get('original_recovery')):
        raise ValueError('stopped resume changed its historical teardown custody')
    quarantined = details.get('quarantined_implementation')
    if quarantined:
        from .delivery_unpublished_recovery import quarantined_implementation

        if (details['command']['additional_iterations'] <= 0
                or details.get('session_id') is not None or interrupted
                or quarantined_implementation(details['predecessor_spec'], details['state'],
                    details['attempts'], details['candidate']) != quarantined
                or spec.get('role_home_generation') != 'stopped-resume-' + details['command_digest']
                or spec.get('implementation_role_home_generation') is not None):
            raise ValueError('quarantined implementation lost its new finite turn authority')
    if interrupted:
        reconciliation = details.get('cleanup_reconciliation')
        if reconciliation is not None and not any(json.loads(item[0]) == reconciliation
                for item in db.execute("SELECT payload_json FROM delivery_events WHERE run_id=? "
                                       "AND type='resource_cleanup_reconciled'",
                                       (spec['run_id'],))):
            raise ValueError('stopped build lost its original cleanup reconciliation')
        plan = interrupted_resume(details) or {}
        if (plan.get('additional_iterations') != details['command']['additional_iterations']
                or any(details.get(key) != plan.get(key)
                       for key in ('resume_iteration', 'resume_stage', 'gate_evidence'))):
            raise ValueError('stopped resume changed its interrupted iteration authority')
    elif details.get('resume_stage') == 'checks':
        from .delivery_feature_revision_recovery import gate_evidence

        if (not details.get('feature_plan_revision')
                or gate_evidence(details) != details.get('gate_evidence')
                or details.get('resume_iteration') != (details['state']['iteration']
                    + int(details['command']['additional_iterations'] > 0))):
            raise ValueError('revision gate resume lost its exact passed implementation evidence')
    if SCOPE_FIELDS <= recovery['command'].keys():
        latest = next((r for r in reversed(details['state'].get('roles', []))
                       if r.get('role') == 'implement'), {})
        if (not details['predecessor_spec'].get('feature_worker')
                and latest.get('status') == 'blocked'):
            from .delivery_session_custody import completed_blocked_finding

            if details.get('blocked_scope_finding') != completed_blocked_finding(details):
                raise ValueError('blocked scope finding lost its authenticated native receipt')
        elif details.get('blocked_scope_finding') is not None:
            raise ValueError('scope finding proof belongs to a different assessment')
        scope = recovery['command']
        if spec.get('feature_worker'):
            from .delivery_feature_scope import MARKER, receipt_config

            receipt = spec[MARKER]
            amended = receipt_config(db, receipt)
            allowed = [*details['predecessor_spec']['policy']['allowed_paths'],
                       *receipt['scope']['added_paths']]
            if any(scope[key] != receipt['scope'][key] for key in SCOPE_FIELDS):
                raise ValueError('worker scope lost its sealed file delta')
        else:
            amended = scope_amendment_config(
                recovery['predecessor_spec'], Path(scope['amended_config_path']),
                scope['amended_config_sha256'], scope['added_paths'])
            repository = amended.raw['repositories'][spec['repository_key']]
            allowed = repository['allowed_paths']
            from .delivery_source_scope import authority

            if authority(spec['policy']) != authority(repository):
                raise ValueError('stopped scope resume changed its versioned source authority')
        if (spec['policy']['allowed_paths'] != allowed
                or spec['config_digest'] != digest(amended.raw)):
            raise ValueError('stopped scope resume changed its sealed file authority')
    from .delivery_browser_mode_recovery import custody as browser_custody

    browser_custody(db, spec, recovery)
    from .delivery_temporal_prerequisites import custody as temporal_custody

    temporal_custody(db, spec, recovery, retained_memo=retained_memo)
    if details.get('operational_capacity') is not None:
        if spec['policy']['capacity'] != details['predecessor_spec']['policy']['capacity']:
            raise ValueError('amended execution changed frozen policy capacity')
        from .delivery_amendment_capacity import acknowledge

        if acknowledge(details['predecessor_spec'], load_execution_config(spec), db=db) != (
                details['operational_capacity']):
            raise ValueError('amended operational owner acknowledgement changed')
    elif ((SCOPE_FIELDS <= details['command'].keys() or BROWSER_FIELDS <= details['command'].keys())
            and not spec.get('feature_worker')
            and spec['policy']['capacity'] != load_execution_config(spec).raw.get('capacity', 2)):
        raise ValueError('amended scheduling capacity lacks its independent owner receipt')
    return spec


def effective_spec(store, recovery):
    with store._connect() as db:
        spec = custody(db, recovery)
    if recovery.get('feature_plan_revision'):
        from .delivery_feature_revision_recovery import retained_recovery, validate_custody

        validate_custody(store, retained_recovery(recovery))
    return spec


def _historical_candidate(broker, state, candidate, latest, recovery, pending=None):
    """Authenticate native policy rebindings without rewriting the stopped role."""
    from . import delivery_gate_retry as gates
    from .delivery_feature_revision_recovery import retained_recovery

    kinds = {gates.KIND, gates.PREPUBLICATION_KIND, gates.PRELAUNCH_KIND,
             gates.CI_KIND, gates.CONTROLLER_KIND}
    expected = broker.spec
    while recovery and recovery.get('kind') == KIND and pending:
        details = retained_recovery(recovery)
        if (details['execution_spec'] != expected
                or candidate != state.get('candidate')
                or candidate != details['execution_candidate']
                or latest not in details['state'].get('roles', [])):
            raise ValueError('unstarted implementation lost its sealed policy predecessor')
        expected, state, candidate = (details['predecessor_spec'], details['state'],
                                      details['candidate'])
        recovery = details.get('original_recovery')
        pending = details.get('pending_repair')
        if candidate == latest.get('candidate'):
            return True
    if (candidate != state.get('candidate') or not recovery
            or recovery.get('kind') not in kinds):
        return False
    spec = gates.effective_spec(broker.store, recovery['original_spec'], recovery)
    if spec != expected:
        raise ValueError('stopped candidate lost its gate execution authority')
    measured = {'native_identity', 'environment_proof_sha256', 'security_binding_sha256'}
    while recovery and recovery.get('kind') in kinds:
        seal, previous = recovery['seal'], recovery.get('original_recovery')
        before, stopped = seal['original_spec'], recovery['state']
        historical = seal['candidate']
        implementation = next((role for role in reversed(stopped.get('roles', []))
                               if role.get('role') == 'implement'), None)
        previous_digest = digest(previous) if previous else None
        root = Path(spec['state_dir']) / gates.gate_namespace(
            spec.get('gate_retry_generation', 1), spec.get('gate_retry_stage'),
            preflight_generation=spec.get('gate_retry_preflight_generation', 0),
            prepared_handoff=spec.get('gate_retry_prepared_handoff', 0))
        if (read_private(root / 'admission.json') != recovery
                or recovery['execution_spec'] != spec
                or recovery['command']['additional_iterations'] != 0
                or recovery['candidate'] != candidate
                or stopped != seal['closed']['result']
                or stopped.get('candidate') != historical or implementation != latest
                or seal['previous_digest'] != previous_digest
                or seal['closed']['recovery_digest'] != previous_digest
                or any(before[key] != spec[key]
                       for key in ('run_id', 'work_id', 'state_dir', 'checkout', 'provider'))
                or digest(before['policy']) != before['policy_digest']
                or digest(spec['policy']) != spec['policy_digest']
                or historical['policy_digest'] != before['policy_digest']
                or candidate != {**historical, 'policy_digest': spec['policy_digest']}
                or {k: v for k, v in before['policy'].items() if k not in measured}
                != {k: v for k, v in spec['policy'].items() if k not in measured}):
            raise ValueError('stopped implementation lost its sealed policy history')
        if historical == latest.get('candidate'):
            return True
        spec, candidate, recovery = before, historical, retained_recovery(previous)
    return False


def _implementation_receipt(spec, role, attempt):
    """Compare native fields exactly after authenticating a controller-only debit binding."""
    result = json.loads(attempt['result_json'])
    expected = {key: value for key, value in role.items() if key not in {
        'role', 'iteration', 'candidate', 'attempt_id', 'input_candidate_id', 'provider'}}
    if (spec.get('feature_worker') and role['iteration'] > 0
            and (expected.get('planning_defect') is not None
                 or expected.get('planning_defect_repair') is not None)):
        from .delivery_feature_execution import worker_key
        from .delivery_feature_revision_roles import validate_diagnostic
        from .delivery_feature_revision_subject import _ReadOnlyRegistry

        diagnostic = expected.get('planning_defect')
        try:
            validate_diagnostic(diagnostic, chunk_id=spec['feature_worker']['chunk_id'],
                                candidate_id=role['candidate']['id'])
        except (KeyError, TypeError, ValueError):
            return False
        key = spec['run_id'] + ':' + str(role['iteration'])
        binding = {
            'revision_id': 'revision-' + digest({'worker_run_id': spec['run_id'],
                                               'repair_key': key, 'diagnostic': diagnostic})[:24],
            'repair_key': key, 'diagnostic_sha256': digest(diagnostic)}
        if expected.get('planning_defect_repair') != binding:
            return False
        shared = _ReadOnlyRegistry(Path(spec['feature_delivery']['registry']))
        owner = spec['feature_delivery']['owner']
        with shared.connect() as db:
            paid = db.execute('SELECT generation,reason FROM execution_repairs '
                              'WHERE issue_id=? AND repair_key=?',
                              (owner['issue_id'], key)).fetchone()
            if (not paid or paid['generation'] < owner['generation']
                    or paid['reason'] != 'Product repair at worker iteration '
                    + str(role['iteration'])):
                return False
            lineage = [dict(row) for row in db.execute(
                'SELECT issue_id,run_id,store_path,generation FROM execution_snapshots '
                'WHERE issue_id=? AND generation>=? AND generation<=? ORDER BY generation',
                (owner['issue_id'], owner['generation'], paid['generation']))]
            assignment = db.execute(
                'SELECT generation,workstream_id FROM execution_workers '
                'WHERE issue_id=? AND worker_key=?', (owner['issue_id'], worker_key(
                    spec['run_id'], {**owner, 'generation': paid['generation']}))).fetchone()
        if (len(lineage) != paid['generation'] - owner['generation'] + 1
                or not lineage or lineage[0] != owner
                or any(item['store_path'] != owner['store_path'] for item in lineage)
                or not assignment or assignment['generation'] != paid['generation']
                or assignment['workstream_id'] != spec['feature_worker']['workstream_id']):
            return False
        if 'planning_defect_repair' not in result:
            expected.pop('planning_defect_repair')
    return result == expected


def _candidate(broker, state, attempts, *, missing_rollout=None, input_rejection=None,
               recovery=None, pending=None, quarantined=None, historical_quarantines=(),
               typed_refusal=False, lifecycle='unknown'):
    candidate = broker.candidate()

    def refuse(code, fact, action, message, *, satisfied=None):
        if typed_refusal:
            from .delivery_lifecycle import require_observed_predicates

            require_observed_predicates(KIND, 'delivery_stopped_resume', {
                'closed_candidate': state.get('candidate'), 'current_candidate': candidate,
            }, [{
                'code': code, 'satisfied': satisfied, 'missing_or_unknown_fact': fact,
                'next_action': action, 'user_input_required': None,
            }], lifecycle=lifecycle, owner_proof=None, legacy_message=message)
        raise ValueError(message)

    implementations = [r for r in state.get('roles', []) if r.get('role') == 'implement']
    latest = implementations[-1] if implementations else None
    retained_unknown = (latest is not None and pending
        and pending.get('kind') == 'unstarted_preflight'
        and digest(latest) in {p['closed_role_sha256'] for p in historical_quarantines})
    if latest and latest.get('status') != 'pass':
        if latest.get('cleanup') != 'confirmed' and not (quarantined or retained_unknown):
            refuse('stopped_implementation_cleanup_unknown',
                   'confirmed blocked implementation cleanup or its exact quarantine',
                   'reconcile the original implementation custody',
                   'blocked implementation source or session is unconfirmed')
        if (latest.get('candidate') != candidate
                and not _historical_candidate(broker, state, candidate, latest, recovery, pending)):
            refuse('stopped_candidate_changed', 'unchanged or authenticated historical candidate',
                   'inspect the retained candidate and obtain fresh owner proof',
                   'blocked implementation source or session is unconfirmed')
        matches = [a for a in attempts if a['role'] == 'implement'
                   and a['iteration'] == latest['iteration']
                   and a['session_id'] == latest['session_id']
                   and _implementation_receipt(broker.spec, latest, a)]
        prerequisites = latest.get('implementation_preparation', {})
        no_provider = (latest.get('session_id') is None
                       and prerequisites.get('state') == 'failed'
                       and prerequisites.get('cleanup') == 'confirmed'
                       and not any(a['role'] == 'implement'
                                   and a['iteration'] == latest['iteration'] for a in attempts))
        # A closed Temporal activity authenticates controller-only preparation failures.
        # Every provider or supervisor prelaunch receipt must also match its durable attempt.
        if len(matches) != 1 and not no_provider and not (quarantined or retained_unknown):
            refuse('stopped_implementation_receipt_unproven',
                   'one exact supervisor receipt for the blocked implementation',
                   'inspect the original implementation attempt and receipt',
                   'blocked implementation lacks its exact supervisor receipt')
        if latest.get('session_id') is None and not no_provider and (
                latest.get('finish_reason') != 'prelaunch'
                and not (missing_rollout or input_rejection or quarantined or retained_unknown)):
            refuse('stopped_launch_outcome_unproven',
                   'authenticated prelaunch or provider outcome for the blocked implementation',
                   'inspect the original launch and provider custody',
                   'blocked implementation has no authenticated launch outcome')
    elif candidate != state.get('candidate'):
        refuse('stopped_candidate_changed', 'unchanged candidate at the closed checkpoint',
               'inspect the retained candidate and obtain fresh owner proof',
               'stopped candidate changed after its sealed checkpoint', satisfied=False)
    if any((r.get('cleanup') != 'confirmed' or r.get('finish_reason') == 'recovery_unknown')
           and not (quarantined and r is latest)
           and digest(r) not in {p['closed_role_sha256'] for p in historical_quarantines}
           for r in state.get('roles', [])):
        refuse('stopped_role_cleanup_unknown', 'confirmed role cleanup or exact quarantine',
               'reconcile the original role custody', 'stopped role ownership is unknown')
    if any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts):
        refuse('stopped_attempt_custody_unknown', 'finished attempts and confirmed cleanup',
               'reconcile the original attempt custody', 'stopped role ownership is unknown')
    sessions = {r.get('session_id') for r in implementations if r.get('session_id')}
    if len(sessions) > 1:
        refuse('stopped_session_conflict', 'one original implementation session',
               'inspect the retained implementation session history',
               'original implementation session changed', satisfied=False)
    if not sessions and any(a['role'] == 'implement' and a['session_id'] for a in attempts):
        refuse('stopped_session_unrecorded', 'matching role and attempt session identity',
               'inspect the retained role and attempt session evidence',
               'unrecorded implementation session exists', satisfied=False)
    changed = broker._changed_paths(broker.spec['base_sha'])
    from .delivery_source_scope import outside_scope

    if outside_scope(broker.spec['policy'], changed, checkout=broker.checkout):
        refuse('stopped_scope_changed', 'source paths within the original authorized scope',
               'inspect changed paths and the original scope authority',
               'stopped source escaped its frozen scope', satisfied=False)
    _git(broker.checkout, 'merge-base', '--is-ancestor', broker.spec['base_sha'], candidate['head'])
    if (_git(broker.checkout, 'branch', '--show-current')
            != broker.spec.get('local_branch', broker.spec['branch'])
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin')
            != broker.spec['origin_url']):
        refuse('stopped_repository_changed', 'original checkout branch and push destination',
               'inspect the original repository and branch authority',
               'stopped checkout branch or origin changed', satisfied=False)
    return candidate, next(iter(sessions), None)


def snapshot(store, run_id, *, _revision=None, _observation=False):
    return _snapshot(store, run_id, _revision=_revision, _observation=_observation)


def review_snapshot(store, run_id):
    """Observe complete stopped-worker custody without admitting or creating resources."""
    from .delivery_execution_config import is_observation_mode

    return _snapshot(store, run_id, review_subject=True, _observation=is_observation_mode())


def _snapshot(store, run_id, *, _revision=None, review_subject=False, _observation=False):
    spec = store.effective_spec(run_id)
    if spec['provider'] != 'fake' and spec['policy'].get('host_sandbox') != 'trusted-local':
        raise ValueError('stopped resume preserves an already trusted execution policy')
    load_execution_config(spec)
    if not store.owns_execution(spec):
        raise ValueError('stopped execution belongs to a different service')
    row, attempts, effects, claim = _rows(store, run_id)
    # Original admissions use the deterministic ID without a successor override.
    predecessor_id = row['workflow_id'] or 'delivery-' + run_id
    closed = store._completed_temporal_result(
        run_id, workflow_id=predecessor_id, **({'preserve': False} if _observation else {}))
    state = closed['result']
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    stopped = ({'blocked', 'cancelled'} if spec.get('feature_worker') else {'blocked'})
    if review_subject and spec.get('feature_worker', {}).get('kind') == 'build':
        # A completed build remains source evidence; this observer grants no resume.
        stopped.add('delivered')
    if _revision is not None:
        from .delivery_feature_revision_recovery import validate_revision_recovery

        validate_revision_recovery(store, spec, _revision)
        stopped.add('delivered')
    expected_execution = 'terminal' if row['outcome'] in {'cancelled', 'delivered'} else 'blocked'
    reconciled = reconciled_cleanup(store, row, state, closed=closed)
    cleanup_authority = reconciled
    if (cleanup_authority is None and spec.get('feature_worker', {}).get('kind') == 'chunk'
            and row['cleanup'] == 'confirmed' and state.get('cleanup') != 'confirmed'):
        from .delivery_feature_closure import closed_worker

        # Ordinary maintenance may confirm cleanup after a worker's immutable
        # terminal result. Reuse settlement's complete history/row/event proof;
        # neither the closed verdict nor its tracker checkpoint is rewritten.
        authenticated = closed_worker(store, run_id, predecessor_id, sealed_only=True)
        if any(authenticated.get(key) != value for key, value in closed.items()):
            raise ValueError('stopped worker cleanup belongs to a different closed execution')
        cleanup_authority = authenticated
    state_checks, row_checks = closed_checks(row, state, cleanup_authority)
    # Keep the closed verdict immutable while reporting later custody facts
    # separately.  These are exactly the facts checked by the original guard.
    from .delivery_lifecycle import (
        RecoveryRefused,
        lifecycle_from_row,
        require_observed_predicates,
    )

    status_matches = (row['phase'] in stopped and row['outcome'] == row['phase']
        and row['execution_state'] == expected_execution and state.get('run_id') == run_id
        and state.get('phase') == row['phase'] and state.get('outcome') == row['outcome']
        and state.get('execution_state') == expected_execution)
    cleanup_matches = (row['cleanup'] == 'confirmed'
                       and (state.get('cleanup') == 'confirmed' or cleanup_authority is not None))
    identity_matches = (closed.get('workflow_id') == predecessor_id
        and closed['request_digest'] == row['request_digest']
        and closed['recovery_digest'] == (digest(previous) if previous else None))
    checkpoint_matches = (state.get('candidate') == json.loads(row['candidate_json'] or 'null')
        and state_checks == row_checks
        and state.get('pull_request') == json.loads(row['pr_json'] or 'null')
        and state.get('revision') == row['protocol_revision']
        and state.get('iteration') == row['iteration'] and state.get('error') == row['error'])
    plan_available = (isinstance(spec.get('accepted_plan'), str)
                      and bool(spec['accepted_plan'].strip()))
    effects_settled = all(e['state'] == 'complete' and e['observed_json'] is not None
                          for e in effects)
    try:
        require_observed_predicates(
            KIND, 'delivery_stopped_resume', {'row': row, 'closed': closed}, [
            {'code': 'stopped_state_unproven', 'satisfied': status_matches,
             'missing_or_unknown_fact': 'matching stopped or supported terminal run state',
             'next_action': 'inspect the retained run and closed predecessor',
             'user_input_required': None},
            {'code': 'stopped_cleanup_unconfirmed', 'satisfied': True if cleanup_matches else None,
             'missing_or_unknown_fact': 'confirmed cleanup or authenticated later reconciliation',
             'next_action': 'reconcile the original cleanup and custody',
             'user_input_required': None},
            {'code': 'stopped_predecessor_unproven', 'satisfied': identity_matches,
             'missing_or_unknown_fact': 'matching workflow, admission and recovery identities',
             'next_action': 'inspect the closed predecessor and its original admission',
             'user_input_required': None},
            {'code': 'stopped_checkpoint_changed', 'satisfied': checkpoint_matches,
             'missing_or_unknown_fact': 'unchanged candidate, checks, publication and verdict',
             'next_action': 'inspect retained checkpoint and immutable history',
             'user_input_required': None},
            {'code': 'stopped_owner_conflict', 'satisfied': claim is None,
             'missing_or_unknown_fact': 'released original work claim',
             'next_action': 'inspect and reconcile the current work owner',
             'user_input_required': None},
            {'code': 'stopped_effects_unsettled', 'satisfied': True if effects_settled else None,
             'missing_or_unknown_fact': 'settled effects with observed outcomes',
             'next_action': 'reconcile the original effects', 'user_input_required': None},
            {'code': 'accepted_plan_unavailable', 'satisfied': plan_available,
             'missing_or_unknown_fact': 'original accepted plan for implementation resume',
             'next_action': 'inspect retained intake and preparation; no implementation resume '
                            'is authorized without an accepted plan',
             'user_input_required': None},
        ], lifecycle=lifecycle_from_row(row), owner_proof=(
            True if (status_matches and cleanup_matches and identity_matches and checkpoint_matches
                     and claim is None and effects_settled) else None),
            legacy_message=('resume requires a closed finalized delivery with released ownership'
                            if not (status_matches and cleanup_matches and identity_matches
                                    and checkpoint_matches and claim is None and effects_settled)
                            else 'implementation resume requires an accepted plan'))
    except RecoveryRefused as exc:
        if review_subject:
            # Other owners must report their own operation, not a resume refusal.
            raise ValueError(str(exc)) from exc
        raise
    if spec.get('baseline_checks_version') in (1, 2):
        baseline = state.get('checks', {}).get('baseline', {})
        required = {check['id'] for check in spec['policy']['baseline_checks']}
        results = baseline.get('results', [])
        passed = (baseline.get('state') == 'passed' and baseline.get('base_sha') == spec['base_sha']
                  and baseline.get('baseline_candidate', {}).get('head') == spec['base_sha']
                  and baseline.get('feature_unchanged') is True
                  and baseline.get('source_unchanged') is True
                  and required <= {r.get('id') for r in results if r.get('passed') is True}
                  and all(r.get('passed') is True for r in results))
        if not passed and not repairable_baseline(spec, baseline):
            raise ValueError('implementation resume requires the passed immutable baseline'
                             if spec.get('baseline_checks_version') == 1 else
                             'implementation resume requires an authenticated immutable baseline')
    observe_publication = bool(spec.get('feature_worker')
                               and (review_subject or _revision is not None))
    if observe_publication:
        from .delivery_feature_revision_subject import ObservationBroker

        # Only the already authenticated revision admission may observe the old
        # worker under a newly adopted definition; execution remains fenced.
        broker = ObservationBroker(store, spec, revision=(
            _revision['plan_identity'] if _revision is not None else None))
    else:
        broker = DeliveryBroker(store, spec)
    if review_subject:
        broker.validate_candidate_scope()
    from .delivery_feature_revision_recovery import retained_recovery
    from .delivery_session_custody import implementation_custody
    from .delivery_unpublished_recovery import (
        quarantine_history,
        quarantined_implementation,
        teardown_checkpoint,
    )

    history = quarantine_history(previous)
    session_custody = implementation_custody(spec, state, attempts, retained_recovery(previous))
    implementations = [role for role in state.get('roles', []) if role.get('role') == 'implement']
    sessions = {role['session_id'] for role in implementations if role.get('session_id')}
    pending_seal = {'predecessor_spec': spec, 'state': state, 'candidate': state['candidate'],
                    'attempts': attempts, 'effects': effects, 'original_recovery': previous,
                    'session_id': next(iter(sessions)) if len(sessions) == 1 else None,
                    **({'session_custody': session_custody} if session_custody else {})}
    with store._connect() as db:
        pending = pending_repair(pending_seal, db=db)
    retained = pending and pending.get('kind') == 'unstarted_preflight'
    quarantined = (None if retained else
                   quarantined_implementation(spec, state, attempts, broker.candidate()))
    teardown = teardown_checkpoint([*history, *([quarantined] if quarantined else [])])
    if quarantined and (previous or not reconciled
            or reconciled.get('quarantined_implementation') != quarantined):
        raise ValueError('interrupted implementation lacks its separate owned cleanup closure')
    candidate, session = _candidate(
        broker, state, attempts, missing_rollout=(session_custody or {}).get('missing_rollout'),
        input_rejection=(session_custody or {}).get('input_rejection'),
        recovery=previous, pending=pending, quarantined=quarantined, historical_quarantines=history,
        typed_refusal=not review_subject, lifecycle=lifecycle_from_row(row))
    publication = state.get('pull_request')
    if publication:
        published_identity(broker, candidate, publication)
        if candidate['head'] != publication['head']:
            raise ValueError('stopped candidate is not on the published head')
    else:
        if any(e['kind'] == 'publish' for e in effects):
            raise ValueError('unpublished resume has an unresolved publication')
        if observe_publication:
            broker.observe_unpublished()
        else:
            _unpublished_remote(broker)
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    return {'predecessor_spec': spec, 'row': row, 'attempts': attempts, 'effects': effects,
            'closed': closed, 'state': state, 'candidate': candidate, 'session_id': session,
            'publication': publication, 'original_recovery': previous,
            **({'cleanup_reconciliation': reconciled} if reconciled else {}),
            **({'quarantined_implementation': quarantined} if quarantined else {}),
            **({'historical_quarantines': history} if history else {}),
            'cleanup_digest': (observed_preparation_cleanup(spec, quarantined=teardown)
                               if (pending or {}).get('implementation_preparation') else
                               observed_native_cleanup(spec, read_only=True, **(
                                   {"quarantined": teardown} if teardown else {}))
                               if review_subject or _observation else
                               observed_native_cleanup(spec, **(
                                   {"quarantined": teardown} if teardown else {}))),
            'work_binding': binding,
            **({'session_custody': session_custody} if session_custody else {})}


def _unpublished_remote(broker):
    if broker.spec.get('feature_worker', {}).get('previous_publication'):
        from .delivery_feature_publication import verify_retained_publication

        verify_retained_publication(broker)
    else:
        _remote(broker)


def admit(store, run_id, command, *, preflight=False, _revision=None):
    original = store.submitted_spec(run_id)
    allowed = range(0, 102) if original.get('feature_worker') else (0, 1, 2)
    if (not isinstance(command, dict) or set(command) not in (
            FIELDS, FIELDS | SCOPE_FIELDS, FIELDS | BROWSER_FIELDS, FIELDS | TEMPORAL_FIELDS,
            FIELDS | SCOPE_FIELDS | TEMPORAL_FIELDS, FIELDS | BROWSER_FIELDS | TEMPORAL_FIELDS)
            or command.get('continuation_kind') != KIND
            or not isinstance(command.get('command_id'), str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', command['command_id'])
            or any(type(command.get(k)) is not int or command[k] < low for k, low in (
                ('expected_revision', 1), ('expected_iteration', 0)))
            or type(command.get('additional_iterations')) is not int
            or command['additional_iterations'] not in allowed
            or any(not isinstance(command.get(k), str)
                   or not re.fullmatch(r'[0-9a-f]{' + str(size) + '}', command[k])
                   for k, size in [('expected_candidate_id', 64),
                                  ('expected_candidate_head', 40)])):
        raise ValueError('stopped resume requires a finite exact-checkpoint command')
    command_digest = digest({'run_id': run_id, **command})
    with store._connect() as db:
        prior = db.execute('SELECT run_id,request_digest,response_json FROM delivery_commands '
                           'WHERE command_id=?', (command['command_id'],)).fetchone()
    if prior:
        from .delivery_lifecycle import command_receipt

        assessment = command_receipt(KIND, prior, run_id, command_digest)
        if preflight:
            return {**json.loads(prior['response_json']), 'preflight': True,
                    'lifecycle_assessment': assessment}
        return json.loads(prior['response_json'])
    seal = snapshot(store, run_id, _revision=_revision, _observation=preflight)
    gate_only = None
    if _revision is not None:
        from .delivery_feature_revision_recovery import gate_only_recovery

        gate_only = gate_only_recovery(store, seal, _revision)
    with store._connect() as db:
        pending = None if gate_only else pending_repair(seal, db=db)
    ci_findings = None if gate_only else sealed_ci_findings(seal)
    from .delivery_lifecycle import lifecycle_from_row, require_same_proof, require_transition

    lifecycle_assessment = require_transition({
        'operation': KIND, 'owner': 'delivery_stopped_resume', 'owner_proof': True,
        'normalized_lifecycle': lifecycle_from_row(seal['row']), 'proof_digest': digest(seal),
        'revision': seal['state']['revision'],
        'iteration': seal['state']['iteration'], 'candidate_id': seal['candidate']['id'],
        'candidate_head': seal['candidate']['head'],
    }, command)
    # Interrupted work needs no repair cycle; only its exact plan is admitted this way.
    interrupted = None if _revision is not None or pending else interrupted_resume(seal)
    if interrupted and interrupted['additional_iterations'] != command['additional_iterations']:
        interrupted = None
    maximum = continuation_maximum(seal['state']['iteration'],
                                   command['additional_iterations'], pending)
    from .delivery_lifecycle import require_observed_predicates

    require_observed_predicates(KIND, 'delivery_stopped_resume', seal, [
        {'code': 'pending_repair_unproven',
         'satisfied': (command['additional_iterations'] != 0
                       or bool(pending or gate_only or interrupted)),
         'missing_or_unknown_fact': 'authenticated pending repair or interrupted iteration',
         'next_action': 'inspect the retained repair admission and original iteration',
         'user_input_required': None},
        {'code': 'unknown_implementation_requires_new_turn',
         'satisfied': (not seal.get('quarantined_implementation')
                       or command['additional_iterations'] != 0),
         'missing_or_unknown_fact': 'separately authorized new implementation iteration',
         'next_action': 'review the unknown implementation and its original allowance',
         'new_authorization_required': True, 'user_input_required': True},
        {'code': 'pending_repair_ceiling_changed', 'satisfied': maximum is not None,
         'missing_or_unknown_fact': 'unchanged finite pending preflight ceiling',
         'next_action': 'inspect the retained preflight admission and its ceiling',
         'user_input_required': None},
    ], lifecycle=lifecycle_from_row(seal['row']))
    credit = None
    if _revision is not None:
        from .delivery_feature_execution import registry

        shared = registry(seal['predecessor_spec'])
        owner = shared.token(shared.current(seal['predecessor_spec']['feature_delivery'][
            'owner']['issue_id']))
        iteration = seal['state']['iteration'] + 1
        credit = ({'kind': 'gates', 'iteration': iteration,
                   'revision_id': _revision['revision_id'], 'evidence_sha256': digest(gate_only)}
                  if gate_only else paid_alias_credit(seal['predecessor_spec'], owner, iteration,
                                                       _revision['revision_id']))
    authority = worker_allowance(seal['predecessor_spec'], seal['state']['iteration'],
                                 command['additional_iterations'], credit=credit)
    budget_spec = {**original, **({'repair_allowance': authority} if authority else {})}
    if (original.get('retry_budget_version') == 1
            and not fixed_budget_allows(budget_spec, seal['state']['iteration'],
                                        command['additional_iterations'],
                                        pending=bool(pending or gate_only or interrupted))):
        require_observed_predicates(KIND, 'delivery_stopped_resume', seal, [{
            'code': 'product_budget_exhausted', 'satisfied': False,
            'missing_or_unknown_fact': 'remaining original finite product repair allowance',
            'next_action': 'inspect the original repair budget and retained admission',
            'new_authorization_required': True, 'user_input_required': None,
        }], lifecycle=lifecycle_from_row(seal['row']),
            legacy_message='a fixed repair budget cannot receive additional iterations')
    amended = scope_spec(store, seal, command)
    browser_amendment = None
    if BROWSER_FIELDS <= command.keys():
        from .delivery_browser_mode_recovery import proposal

        amended, browser_amendment = proposal(seal, command)
    temporal_preparation = None
    if TEMPORAL_FIELDS <= command.keys():
        from .delivery_temporal_prerequisites import proposal as temporal_proposal

        amended, temporal_preparation = temporal_proposal(seal, command, execution=amended)
    capacity_receipt = None
    if (not seal['predecessor_spec'].get('feature_worker')
            and (SCOPE_FIELDS <= command.keys() or BROWSER_FIELDS <= command.keys())):
        from .delivery_amendment_capacity import acknowledge

        capacity_receipt = acknowledge(seal['predecessor_spec'], load_execution_config(amended))
    scope_finding = None
    if SCOPE_FIELDS <= command.keys() and not seal['predecessor_spec'].get('feature_worker'):
        latest = next((r for r in reversed(seal['state'].get('roles', []))
                       if r.get('role') == 'implement'), {})
        if latest.get('status') == 'blocked':
            from .delivery_session_custody import completed_blocked_finding

            scope_finding = completed_blocked_finding(seal)
    if _revision is not None:
        from .delivery_feature_revision_recovery import validate_revision_recovery

        if SCOPE_FIELDS <= command.keys():
            raise ValueError('revision recovery cannot add an unrelated scope amendment')
        amended = validate_revision_recovery(store, seal['predecessor_spec'], _revision)
    if preflight:
        return {'run_id': run_id, 'preflight': True, 'lifecycle_assessment': lifecycle_assessment,
                'candidate': seal['candidate'],
                'authorized_through_iteration': maximum, 'implementation_authority': True,
                **({'added_paths': command['added_paths']}
                   if SCOPE_FIELDS <= command.keys() else {}),
                **({'browser_qa_amendment': {key: browser_amendment[key] for key in (
                    'original_recipe_sha256', 'effective_recipe_sha256')}}
                   if browser_amendment else {}),
                **({'temporal_test_prerequisites': {
                    'check_sha256': temporal_preparation['check_sha256'],
                    'metadata': temporal_preparation['metadata'],
                    'sdk': temporal_preparation['sdk']}}
                   if temporal_preparation else {})}
    spec = seal['predecessor_spec']
    root = Path(spec['state_dir']) / 'stopped-resumes' / command_digest
    with _lock(Path(spec['state_dir']) / 'stopped-resumes' / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute(
                'SELECT run_id,request_digest,response_json FROM delivery_commands '
                'WHERE command_id=?',
                (command['command_id'],)).fetchone()
        if prior:
            from .delivery_lifecycle import command_receipt

            command_receipt(KIND, prior, run_id, command_digest)
            return json.loads(prior['response_json'])
        # Recheck all stopped readbacks after lock acquisition, before any native work.
        require_same_proof(KIND, 'delivery_stopped_resume',
                           digest(snapshot(store, run_id, _revision=_revision)), digest(seal))
        private_directory(root)
        execution = prepare_runtime(amended, root, command_digest, digest(seal))
        require_same_proof(KIND, 'delivery_stopped_resume',
                           digest(snapshot(store, run_id, _revision=_revision)), digest(seal))
        if _revision is not None:
            if validate_revision_recovery(store, seal['predecessor_spec'], _revision) != amended:
                raise ValueError('adopted worker derivation changed during runtime preparation')
        elif browser_amendment is not None:
            from .delivery_browser_mode_recovery import _transition

            checked, proof = proposal(seal, command)
            if temporal_preparation is not None:
                checked, temporal_proof = temporal_proposal(seal, command, execution=checked)
                if temporal_proof != temporal_preparation:
                    raise ValueError('combined Temporal prerequisite changed during preparation')
            if checked != amended or proof != browser_amendment:
                raise ValueError('browser amendment evidence changed during runtime preparation')
            _transition(amended, execution)
        elif temporal_preparation is not None:
            from .delivery_temporal_prerequisites import _transition as temporal_transition

            checked, proof = temporal_proposal(
                seal, command, execution=scope_spec(store, seal, command))
            if checked != amended or proof != temporal_preparation:
                raise ValueError('Temporal prerequisite evidence changed during preparation')
            temporal_transition(amended, execution)
        elif scope_spec(store, seal, command) != amended:
            raise ValueError('stopped scope configuration changed during runtime preparation')
        if scope_finding is not None:
            if completed_blocked_finding(seal) != scope_finding:
                raise ValueError('blocked scope finding changed during runtime preparation')
        if capacity_receipt is not None:
            if execution['policy']['capacity'] != seal['predecessor_spec']['policy']['capacity']:
                raise ValueError('amendment preparation changed frozen policy capacity')
            if acknowledge(seal['predecessor_spec'], load_execution_config(execution)) != (
                    capacity_receipt):
                raise ValueError('operational owner changed during amendment preparation')
        execution.pop('repair_allowance', None)
        if authority:
            if worker_allowance(spec, seal['state']['iteration'],
                                command['additional_iterations'], credit=credit) != authority:
                raise ValueError('runtime repair allowance changed during preparation')
            execution['repair_allowance'] = authority
        execution['role_home_generation'] = spec.get('role_home_generation', '')
        if not execution['role_home_generation']:
            execution.pop('role_home_generation')
        if seal.get('session_custody'):
            execution['implementation_role_home_generation'] = seal['session_custody']['generation']
        if seal.get('quarantined_implementation'):
            # No old provider session is known. A new finite turn gets an isolated
            # home and starts a new session; the original unknown turn stays sealed.
            execution['role_home_generation'] = 'stopped-resume-' + command_digest
            execution.pop('implementation_role_home_generation', None)
        # Admission identities belong to one admission; a predecessor's never carry over.
        for field in ('gate_admission', 'implement_admission'):
            execution.pop(field, None)
        if interrupted or gate_only:
            # Gates may rerun at the sealed iteration, so their attempts take this
            # admission's identity instead of meeting an earlier execution's receipts.
            execution['gate_admission'] = command_digest
        if pending or (interrupted and 'resume_stage' not in interrupted):
            # The implementer reruns at the sealed iteration with the same input, so
            # its attempt must not be answered from the failed one's durable receipt.
            execution['implement_admission'] = command_digest
        candidate = {**seal['candidate'], 'policy_digest': execution['policy_digest']}
        retained = {k: v for k, v in seal.items() if k not in {'row', 'closed'}}
        retained['closed'] = {k: v for k, v in seal['closed'].items() if k != 'result'}
        recovery = {**retained, 'kind': KIND, 'command': command, 'command_digest': command_digest,
                    'execution_spec': execution, 'execution_candidate': candidate,
                    'maximum_iteration': maximum,
                    'predecessor_result_digest': digest(seal['state']),
                    **({'blocked_scope_finding': scope_finding} if scope_finding else {}),
                    **({'browser_qa_amendment': browser_amendment} if browser_amendment else {}),
                    **({'temporal_test_prerequisites': temporal_preparation}
                       if temporal_preparation else {}),
                    **({'operational_capacity': capacity_receipt} if capacity_receipt else {}),
                    **({'feature_plan_revision': _revision} if _revision is not None else {}),
                    **({'pending_repair': pending} if pending else {}),
                    **({'ci_findings': ci_findings} if ci_findings else {}),
                    **({key: value for key, value in interrupted.items()
                        if key != 'additional_iterations'} if interrupted else {}),
                    **({'resume_stage': 'checks', 'gate_evidence': gate_only,
                        'resume_iteration': seal['state']['iteration']
                        + int(command['additional_iterations'] > 0)} if gate_only else {})}
        if _revision is not None:
            from .delivery_feature_revision_recovery import compact_recovery

            recovery = compact_recovery(recovery, root)
        workflow_id = 'delivery-' + run_id + '-resume-' + command_digest[:20]
        response = {'run_id': run_id, 'workflow_id': workflow_id,
                    'dashboard_url': store.config.dashboard_url + '/runs/' + run_id,
                    'phase': 'repair_continuation_queued',
                    'authorized_through_iteration': maximum, 'admission_sha256': digest(recovery)}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        from .delivery_store import _now

        with store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
            if (dict(current) != seal['row']
                    or store.state.claim_for(db, spec['work_id']) is not None):
                raise ValueError('stopped run changed before public admission')
            attempts = [dict(item) for item in db.execute(
                'SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key', (run_id,))]
            effects = [dict(item) for item in db.execute(
                'SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key', (run_id,))]
            if attempts != seal['attempts'] or effects != seal['effects']:
                raise ValueError('stopped attempt or effect changed during admission')
            if work_binding(store, spec, db) != seal['work_binding']:
                raise ValueError('stopped work binding changed during admission')
            store.state.claim_work(db, spec['work_id'], f'external:devflow:{run_id}',
                                   store.config.dashboard_url)
            db.execute("UPDATE delivery_runs SET phase='repair_continuation_queued', "
                       "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1, "
                       "workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?",
                       (workflow_id, canonical_json(recovery), _now(), run_id))

            db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                       'WHERE run_id=?', (_now(), run_id))
            store._event(db, run_id, current['revision'] + 1, 'stopped_resume_queued',
                         'Finite public resume admitted; original failure retained',
                         {'iteration': seal['state']['iteration'],
                          'authorized_through_iteration': maximum,
                          'command_id': command['command_id'],
                          'predecessor_execution_run_id': seal['closed']['execution_run_id'],
                          **({'added_paths': command['added_paths'],
                              'original_policy_digest': spec['policy_digest'],
                              'effective_policy_digest': execution['policy_digest']}
                             if SCOPE_FIELDS <= command.keys() else {})})
            db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                       (command['command_id'], run_id, command_digest, canonical_json(response)))
        return response


def readback(store, spec, recovery):
    from .delivery_feature_revision_recovery import retained_recovery

    details = retained_recovery(recovery)
    with store._connect() as db:
        if custody(db, recovery) != spec:
            raise ValueError('stopped resume execution policy changed')
        row = db.execute('SELECT workflow_id,recovery_json FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
        claim = store.state.claim_for(db, spec['work_id'])
        if (json.loads(row[1]) != recovery or row[0] != ('delivery-' + spec['run_id']
                + '-resume-' + recovery['command_digest'][:20]) or not claim
                or claim['owner'] != f"external:devflow:{spec['run_id']}"):
            raise ValueError('stopped resume command or ownership changed')
        work_binding(store, spec, db)
        attempts = [dict(item) for item in db.execute(
            'SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key', (spec['run_id'],))]
        effects = [dict(item) for item in db.execute(
            'SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key', (spec['run_id'],))]
        if attempts != details['attempts'] or effects != details['effects']:
            raise ValueError('stopped receipt inventory changed before resume')
    predecessor = details['predecessor_spec']
    current_scope_base = (SCOPE_FIELDS <= details['command'].keys()
                          and not predecessor.get('feature_worker')
                          and predecessor['provider'] == 'codex')
    if current_scope_base:
        ordinary_scope_base(load_execution_config(spec), predecessor)
        observed_preparation_cleanup(predecessor, resource_spec=spec)
    if recovery.get('session_custody'):
        from .delivery_session_custody import implementation_custody

        observed = implementation_custody(predecessor, details['state'], details['attempts'],
                                           retained_recovery(details['original_recovery']))
        if (observed != recovery['session_custody']
                or spec.get('implementation_role_home_generation')
                    != recovery['session_custody']['generation']):
            raise ValueError('stopped implementation session custody changed before resume')
    if (not store.owns_execution(spec)
            or digest(load_execution_config(spec).raw) != spec['config_digest']):
        raise ValueError('stopped service configuration or owner changed')
    from .delivery_unpublished_recovery import quarantine_history, teardown_checkpoint

    history = quarantine_history(details.get('original_recovery'))
    if details.get('historical_quarantines', []) != history:
        raise ValueError('historical provider teardown changed before resume')
    quarantined = details.get('quarantined_implementation')
    if quarantined:
        from .delivery_unpublished_recovery import quarantined_implementation

        if (quarantined_implementation(predecessor, details['state'], details['attempts'],
                                      details['candidate']) != quarantined
                or recovery['session_id'] is not None
                or spec.get('role_home_generation')
                    != 'stopped-resume-' + recovery['command_digest']
                or spec.get('implementation_role_home_generation') is not None):
            raise ValueError('quarantined implementation changed before its new finite turn')
    teardown = teardown_checkpoint([*history, *([quarantined] if quarantined else [])])
    cleanup = (observed_preparation_cleanup(predecessor, resource_spec=spec, quarantined=teardown)
               if (details.get('pending_repair') or {}).get('implementation_preparation') else
               observed_native_cleanup(predecessor, **(
                   {'quarantined': teardown, 'resource_spec': spec} if teardown else {})))
    if cleanup != details['cleanup_digest']:
        raise ValueError('stopped predecessor process ownership changed')
    broker = DeliveryBroker(store, spec)
    if broker.candidate() != recovery['execution_candidate']:
        raise ValueError('stopped source changed before resume')
    if details['publication']:
        published_identity(broker, recovery['execution_candidate'], details['publication'])
    else:
        _unpublished_remote(broker)
    if current_scope_base:
        ordinary_scope_base(load_execution_config(spec), predecessor)
        observed_preparation_cleanup(predecessor, resource_spec=spec)
    return {'state': 'confirmed'}
