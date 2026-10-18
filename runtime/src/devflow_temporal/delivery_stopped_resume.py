"""Append-only, finite public resume of an authenticated stopped delivery."""
from __future__ import annotations

import json
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_baseline_contract import preparation_failure, repairable_baseline
from .delivery_broker import DeliveryBroker, _git
from .delivery_budget_contract import fixed_budget_allows
from .delivery_config import scope_amended_spec, scope_amendment_config
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
        if (not latest or latest.get('status') != 'findings'
                or latest.get('finish_reason') != 'done' or not seal['session_id']
                or not latest.get('findings')
                or seal['state'].get('error') != 'implementer did not establish a pass'):
            raise ValueError('scope resume requires an authenticated implementation finding')
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
    amended = DeliveryBroker(store, effective).candidate()
    if any(amended[key] != seal['candidate'][key] for key in (
            'id', 'head', 'content_sha256', 'base_sha', 'environment_digest')):
        raise ValueError('scope amendment changed stopped implementation bytes')
    return effective



def remaining_worker_repairs(budget, spec, iteration, *, credit=0):
    """Only a renewed receipt replaces the original worker's absolute ceiling."""
    remaining = budget['maximum'] - budget['used'] + credit
    return (remaining if budget.get('allowance', {}).get('sequence') else
            min(remaining, spec['policy']['max_repairs'] - iteration))


def paid_alias_credit(spec, owner, iteration, revision_id):
    """A sealed existing alias spends its bound debit, including across owners."""
    from .delivery_feature_execution import registry

    key = 'plan-revision:repair-alias:' + spec['run_id'] + ':' + str(iteration)
    with registry(spec).connect() as db:
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
    from .delivery_feature_execution import registry

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
    receipt = registry(spec).repair_allowance(authority['owner'], authority['receipt'],
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
        raise ValueError('feature product repair limit exhausted')
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


def pending_repair(seal, *, db=None):
    """Recover unexecuted work from the sealed admission that requested it."""
    from .delivery_feature_revision_recovery import retained_recovery

    previous = retained_recovery(seal.get('original_recovery')) or {}
    if (previous.get('kind') != KIND
            or seal['predecessor_spec'].get('feature_worker', {}).get('kind') == 'build'):
        return None
    if not seal.get('session_custody', {}).get('missing_rollout'):
        if db is None or not _unstarted_repair(seal, previous, db):
            return None
        if previous.get('pending_repair'):
            return previous['pending_repair']
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
            'findings': [finding for finding in findings if finding is not None]}


def _unstarted_repair(seal, previous, db, *, closing=False):
    """An exact admitted and charged repair has not reached a provider attempt."""
    from .delivery_feature_execution import registry

    spec, state = seal['predecessor_spec'], seal['state']
    prior = previous['state']
    iteration = state['iteration']
    requested = (previous.get('pending_repair') or {}).get(
        'iteration', previous.get('resume_iteration', prior['iteration'] + 1))
    if (not spec.get('feature_worker') or not spec.get('repair_allowance')
            or requested != iteration or iteration <= 0
            or (requested == prior['iteration'] and not previous.get('pending_repair'))):
        return False
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
    if (previous['execution_spec'] != spec
            or candidate != state.get('candidate')
            or candidate != previous['execution_candidate']
            or candidate != {**previous['candidate'], 'policy_digest': spec['policy_digest']}
            or roles[:len(before)] != before or (remainder and not unlaunched)
            or seal['attempts'] != previous['attempts']
            or seal['effects'] != previous['effects']
            or any(attempt['iteration'] == iteration for attempt in seal['attempts'])
            or any(attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
                   for attempt in seal['attempts'])
            or any(effect['state'] != 'complete' or effect['observed_json'] is None
                   for effect in seal['effects'])
            or seal.get('session_id') != previous.get('session_id')
            or seal.get('session_custody') != previous.get('session_custody')
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
    shared = registry(spec)
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
    if (not spec.get('feature_worker')
            or (state.get('phase') != 'cancelled' and not cleanup and not preparation)
            or any(isinstance(check, dict) and check.get('state') == 'failed'
                   and name != preparation for name, check in state.get('checks', {}).items())):
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
            or any(role.get('status') != 'pass' for role in roles[last + 1:])):
        return None
    published = (spec['feature_worker'].get('kind') != 'build'
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


def observed_native_cleanup(spec, *, read_only=False):
    """Preflight and readback observe closure without controlling any process."""
    if spec['provider'] == 'fake':
        return digest({'provider': 'fake'})
    return digest(_stopped_cleanup(spec, read_only=True) if read_only else _stopped_cleanup(spec))


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
            or recovery['maximum_iteration'] != (recovery['state']['iteration']
                                                 + recovery['command']['additional_iterations'])):
        raise ValueError('stopped resume lost its immutable command authority')
    if (('pending_repair' in recovery
             and pending_repair(details, db=db) != recovery['pending_repair'])
            or (recovery['command']['additional_iterations'] == 0
                and not (recovery.get('pending_repair') or interrupted
                         or details.get('resume_stage') == 'checks'))):
        raise ValueError('stopped resume changed its unstarted repair authority')
    if not admission_identity_consistent(details):
        raise ValueError('stopped resume changed its admission identity')
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


def _candidate(broker, state, attempts, *, missing_rollout=None, recovery=None, pending=None):
    candidate = broker.candidate()
    implementations = [r for r in state.get('roles', []) if r.get('role') == 'implement']
    latest = implementations[-1] if implementations else None
    if latest and latest.get('status') != 'pass':
        if (latest.get('cleanup') != 'confirmed'
                or (latest.get('candidate') != candidate
                    and not _historical_candidate(
                        broker, state, candidate, latest, recovery, pending))):
            raise ValueError('blocked implementation source or session is unconfirmed')
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
        if len(matches) != 1 and not no_provider:
            raise ValueError('blocked implementation lacks its exact supervisor receipt')
        if latest.get('session_id') is None and not no_provider and (
                latest.get('finish_reason') != 'prelaunch' and not missing_rollout):
            raise ValueError('blocked implementation has no authenticated launch outcome')
    elif candidate != state.get('candidate'):
        raise ValueError('stopped candidate changed after its sealed checkpoint')
    if (any(r.get('cleanup') != 'confirmed' or r.get('finish_reason') == 'recovery_unknown'
            for r in state.get('roles', []))
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)):
        raise ValueError('stopped role ownership is unknown')
    sessions = {r.get('session_id') for r in implementations if r.get('session_id')}
    if len(sessions) > 1:
        raise ValueError('original implementation session changed')
    if not sessions and any(a['role'] == 'implement' and a['session_id'] for a in attempts):
        raise ValueError('unrecorded implementation session exists')
    changed = broker._changed_paths(broker.spec['base_sha'])
    from .delivery_source_scope import outside_scope

    if outside_scope(broker.spec['policy'], changed, checkout=broker.checkout):
        raise ValueError('stopped source escaped its frozen scope')
    _git(broker.checkout, 'merge-base', '--is-ancestor', broker.spec['base_sha'], candidate['head'])
    if (_git(broker.checkout, 'branch', '--show-current')
            != broker.spec.get('local_branch', broker.spec['branch'])
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin')
            != broker.spec['origin_url']):
        raise ValueError('stopped checkout branch or origin changed')
    return candidate, next(iter(sessions), None)


def snapshot(store, run_id, *, _revision=None):
    return _snapshot(store, run_id, _revision=_revision)


def review_snapshot(store, run_id):
    """Observe complete stopped-worker custody without admitting or creating resources."""
    return _snapshot(store, run_id, review_subject=True)


def _snapshot(store, run_id, *, _revision=None, review_subject=False):
    spec = store.effective_spec(run_id)
    if spec['provider'] != 'fake' and spec['policy'].get('host_sandbox') != 'trusted-local':
        raise ValueError('stopped resume preserves an already trusted execution policy')
    load_execution_config(spec)
    if not store.owns_execution(spec):
        raise ValueError('stopped execution belongs to a different service')
    row, attempts, effects, claim = _rows(store, run_id)
    # Original admissions use the deterministic ID without a successor override.
    predecessor_id = row['workflow_id'] or 'delivery-' + run_id
    closed = store._completed_temporal_result(run_id, workflow_id=predecessor_id)
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
    if (row['phase'] not in stopped or row['outcome'] != row['phase']
            or row['execution_state'] != expected_execution or row['cleanup'] != 'confirmed'
            or state.get('run_id') != run_id or state.get('phase') != row['phase']
            or state.get('outcome') != row['outcome']
            or (state.get('cleanup') != 'confirmed' and cleanup_authority is None)
            or state.get('execution_state') != expected_execution
            or closed.get('workflow_id') != predecessor_id
            or state.get('candidate') != json.loads(row['candidate_json'] or 'null')
            or state_checks != row_checks
            or state.get('pull_request') != json.loads(row['pr_json'] or 'null')
            or state.get('revision') != row['protocol_revision']
            or state.get('iteration') != row['iteration'] or state.get('error') != row['error']
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or claim is not None
            or any(e['state'] != 'complete' or e['observed_json'] is None for e in effects)):
        raise ValueError('resume requires a closed finalized delivery with released ownership')
    if not isinstance(spec.get('accepted_plan'), str) or not spec['accepted_plan'].strip():
        raise ValueError('implementation resume requires an accepted plan')
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

    session_custody = implementation_custody(spec, state, attempts, retained_recovery(previous))
    implementations = [role for role in state.get('roles', []) if role.get('role') == 'implement']
    sessions = {role['session_id'] for role in implementations if role.get('session_id')}
    pending_seal = {'predecessor_spec': spec, 'state': state, 'candidate': state['candidate'],
                    'attempts': attempts, 'effects': effects, 'original_recovery': previous,
                    'session_id': next(iter(sessions)) if len(sessions) == 1 else None,
                    **({'session_custody': session_custody} if session_custody else {})}
    with store._connect() as db:
        pending = pending_repair(pending_seal, db=db)
    candidate, session = _candidate(
        broker, state, attempts, missing_rollout=(session_custody or {}).get('missing_rollout'),
        recovery=previous, pending=pending)
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
            **({'cleanup_reconciliation': reconciled}
               if cleanup_only_build(spec, state) and reconciled else {}),
            'cleanup_digest': (observed_native_cleanup(spec, read_only=True) if review_subject
                               else observed_native_cleanup(spec)), 'work_binding': binding,
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
    if (not isinstance(command, dict) or set(command) not in (FIELDS, FIELDS | SCOPE_FIELDS)
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
        prior = db.execute('SELECT request_digest,response_json FROM delivery_commands '
                           'WHERE command_id=?', (command['command_id'],)).fetchone()
    if prior:
        if prior[0] != command_digest:
            raise ValueError('command ID already belongs to different inputs')
        return json.loads(prior[1])
    seal = snapshot(store, run_id, _revision=_revision)
    gate_only = None
    if _revision is not None:
        from .delivery_feature_revision_recovery import gate_only_recovery

        gate_only = gate_only_recovery(store, seal, _revision)
    with store._connect() as db:
        pending = None if gate_only else pending_repair(seal, db=db)
    ci_findings = None if gate_only else sealed_ci_findings(seal)
    if (command['expected_revision'] != seal['state']['revision']
            or command['expected_iteration'] != seal['state']['iteration']
            or command['expected_candidate_id'] != seal['candidate']['id']
            or command['expected_candidate_head'] != seal['candidate']['head']):
        raise ValueError('stopped resume checkpoint is stale')
    # Interrupted work needs no repair cycle; only its exact plan is admitted this way.
    interrupted = None if _revision is not None or pending else interrupted_resume(seal)
    if interrupted and interrupted['additional_iterations'] != command['additional_iterations']:
        interrupted = None
    if command['additional_iterations'] == 0 and not (pending or gate_only or interrupted):
        raise ValueError('zero additional iterations require an authenticated pending repair '
                         'or an interrupted iteration')
    maximum = seal['state']['iteration'] + command['additional_iterations']
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
        raise ValueError('a fixed repair budget cannot receive additional iterations')
    amended = scope_spec(store, seal, command)
    if _revision is not None:
        from .delivery_feature_revision_recovery import validate_revision_recovery

        if SCOPE_FIELDS <= command.keys():
            raise ValueError('revision recovery cannot add an unrelated scope amendment')
        amended = validate_revision_recovery(store, seal['predecessor_spec'], _revision)
    if preflight:
        return {'run_id': run_id, 'preflight': True, 'candidate': seal['candidate'],
                'authorized_through_iteration': maximum, 'implementation_authority': True,
                **({'added_paths': command['added_paths']}
                   if amended is not seal['predecessor_spec'] else {})}
    spec = seal['predecessor_spec']
    root = Path(spec['state_dir']) / 'stopped-resumes' / command_digest
    with _lock(Path(spec['state_dir']) / 'stopped-resumes' / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute(
                'SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?',
                (command['command_id'],)).fetchone()
        if prior:
            if prior[0] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior[1])
        # Recheck all stopped readbacks after lock acquisition, before any native work.
        if digest(snapshot(store, run_id, _revision=_revision)) != digest(seal):
            raise ValueError('stopped checkpoint changed before admission')
        private_directory(root)
        execution = prepare_runtime(amended, root, command_digest, digest(seal))
        if digest(snapshot(store, run_id, _revision=_revision)) != digest(seal):
            raise ValueError('stopped checkpoint changed during runtime preparation')
        if _revision is not None:
            if validate_revision_recovery(store, seal['predecessor_spec'], _revision) != amended:
                raise ValueError('adopted worker derivation changed during runtime preparation')
        elif scope_spec(store, seal, command) != amended:
            raise ValueError('stopped scope configuration changed during runtime preparation')
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
    if observed_native_cleanup(predecessor) != details['cleanup_digest']:
        raise ValueError('stopped predecessor process ownership changed')
    broker = DeliveryBroker(store, spec)
    if broker.candidate() != recovery['execution_candidate']:
        raise ValueError('stopped source changed before resume')
    if details['publication']:
        published_identity(broker, recovery['execution_candidate'], details['publication'])
    else:
        _unpublished_remote(broker)
    return {'state': 'confirmed'}
