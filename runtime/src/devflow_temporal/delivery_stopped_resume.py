"""Append-only, finite public resume of an authenticated stopped delivery."""
from __future__ import annotations

import json
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_baseline_contract import repairable_baseline
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import scope_amended_spec, scope_amendment_config
from .delivery_execution_config import load_execution_config
from .delivery_gate_retry import prepare_runtime, transient_stop
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import (
    _remote,
    _rows,
    _stopped_cleanup,
    closed_checks,
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


def scope_spec(store, seal, command):
    """Add only explicitly named, unchanged tracked files to an authentic stopped role."""
    spec = seal['predecessor_spec']
    if not SCOPE_FIELDS <= command.keys():
        return spec
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


def fixed_budget_allows(spec, iteration, iterations, *, pending=False):
    """Spend original turns or the finite extension sealed by a runtime allowance."""
    maximum = spec.get('policy', {}).get('max_repairs')
    authority = spec.get('repair_allowance')
    if authority is not None:
        if (not spec.get('feature_worker') or not isinstance(authority, dict)
                or authority.get('run_id') != spec.get('run_id')
                or type(authority.get('start_iteration')) is not int
                or type(authority.get('maximum_iteration')) is not int
                or not 0 <= authority['start_iteration'] <= authority['maximum_iteration']
                or authority['maximum_iteration'] - authority['start_iteration'] > 100):
            return False
        maximum = authority['maximum_iteration']
    allowed = range(1, 101) if spec.get('feature_worker') else (1, 2)
    return (type(iteration) is int and iteration >= 0
            and type(iterations) is int
            and (iterations in allowed or (iterations == 0 and pending))
            and type(maximum) is int and maximum >= 0
            and iteration + iterations <= maximum)


def validate_worker_allowance(spec, *, current=False):
    """Authenticate the receipt even when reading an old immutable recovery."""
    from .delivery_feature_execution import registry

    authority = spec['repair_allowance']
    if (not isinstance(authority, dict)
            or set(authority) != {'run_id', 'owner', 'receipt', 'start_iteration',
                                  'maximum_iteration'}
            or not fixed_budget_allows(spec, authority.get('start_iteration'), 0, pending=True)
            or not isinstance(authority['owner'], dict)
            or authority['owner'].get('issue_id') != spec['feature_delivery']['owner']['issue_id']
            or authority['owner'].get('store_path')
                != spec['feature_delivery']['owner']['store_path']):
        raise ValueError('worker changed its finite runtime repair allowance')
    receipt = registry(spec).repair_allowance(authority['owner'], authority['receipt'],
                                             current=current)
    if authority['maximum_iteration'] - authority['start_iteration'] > receipt['maximum']:
        raise ValueError('worker exceeded its installed runtime repair allowance')
    if current:
        from .delivery_feature_review_context import current_runtime_identity

        if current_runtime_identity()['runtime_payload_sha256'] != (
                authority['receipt']['runtime_payload_sha256']):
            raise ValueError('installed runtime differs from the sealed repair allowance')
    return authority


def worker_allowance(spec, iteration, iterations):
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
    if iterations > budget['maximum'] - budget['used']:
        raise ValueError('feature product repair limit exhausted')
    return {'run_id': spec['run_id'], 'owner': owner, 'receipt': reference,
            'start_iteration': iteration, 'maximum_iteration': iteration + iterations}


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


def pending_repair(seal):
    """Recover unexecuted work from the sealed admission that requested it."""
    from .delivery_feature_revision_recovery import retained_recovery

    previous = retained_recovery(seal.get('original_recovery')) or {}
    if (not seal.get('session_custody', {}).get('missing_rollout')
            or previous.get('kind') != KIND
            or seal['predecessor_spec'].get('feature_worker', {}).get('kind') == 'build'):
        return None
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


def interrupted_resume(seal):
    """Plan a cancelled worker's continuation that needs no product repair, if any.

    It stays at the sealed iteration. A passed implementation is never run again, a
    published candidate is never published again, and gates rerun fresh under the
    admission's own identity.
    """
    from .delivery_feature_revision_recovery import gate_evidence

    spec, state = seal['predecessor_spec'], seal['state']
    if (not spec.get('feature_worker') or state.get('phase') != 'cancelled'
            or any(isinstance(check, dict) and check.get('state') == 'failed'
                   for check in state.get('checks', {}).values())):
        return None
    roles, iteration = state.get('roles', []), state['iteration']
    if not roles:
        # The first implementation never started, and iteration zero is never charged.
        return {'resume_iteration': 0, 'additional_iterations': 0} if iteration == 0 else None
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
                 and (state.get('pull_request') or {}).get('head') == seal['candidate']['head'])
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
             and pending_repair(details) != recovery['pending_repair'])
            or (recovery['command']['additional_iterations'] == 0
                and not (recovery.get('pending_repair') or interrupted
                         or details.get('resume_stage') == 'checks'))):
        raise ValueError('stopped resume changed its unstarted repair authority')
    if not admission_identity_consistent(details):
        raise ValueError('stopped resume changed its admission identity')
    if interrupted:
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
        amended = scope_amendment_config(
            recovery['predecessor_spec'], Path(scope['amended_config_path']),
            scope['amended_config_sha256'], scope['added_paths'])
        repository = amended.raw['repositories'][spec['repository_key']]
        if spec['policy']['allowed_paths'] != repository['allowed_paths']:
            raise ValueError('stopped scope resume changed its sealed file authority')
    return spec


def effective_spec(store, recovery):
    with store._connect() as db:
        spec = custody(db, recovery)
    if recovery.get('feature_plan_revision'):
        from .delivery_feature_revision_recovery import retained_recovery, validate_custody

        validate_custody(store, retained_recovery(recovery))
    return spec


def _candidate(broker, state, attempts, *, missing_rollout=None):
    candidate = broker.candidate()
    implementations = [r for r in state.get('roles', []) if r.get('role') == 'implement']
    latest = implementations[-1] if implementations else None
    if latest and latest.get('status') != 'pass':
        if (latest.get('candidate') != candidate or latest.get('cleanup') != 'confirmed'
                ):
            raise ValueError('blocked implementation source or session is unconfirmed')
        matches = [a for a in attempts if a['role'] == 'implement'
                   and a['iteration'] == latest['iteration']
                   and a['session_id'] == latest['session_id']
                   and json.loads(a['result_json']) == {
                       k: v for k, v in latest.items()
                       if k not in {'role', 'iteration', 'candidate', 'attempt_id',
                                    'input_candidate_id', 'provider'}}]
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
    reconciled = reconciled_cleanup(store, row, state)
    state_checks, row_checks = closed_checks(row, state, reconciled)
    if (row['phase'] not in stopped or row['outcome'] != row['phase']
            or row['execution_state'] != expected_execution or row['cleanup'] != 'confirmed'
            or state.get('run_id') != run_id or state.get('phase') != row['phase']
            or state.get('outcome') != row['outcome']
            or (state.get('cleanup') != 'confirmed' and reconciled is None)
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
    candidate, session = _candidate(
        broker, state, attempts, missing_rollout=(session_custody or {}).get('missing_rollout'))
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
    allowed = range(0, 101) if original.get('feature_worker') else (0, 1, 2)
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
    pending = None if gate_only else pending_repair(seal)
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
    authority = worker_allowance(seal['predecessor_spec'], seal['state']['iteration'],
                                 command['additional_iterations'])
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
                                command['additional_iterations']) != authority:
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
