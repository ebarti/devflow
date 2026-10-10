"""Bounded fresh gates on unchanged candidates after measured runtime repairs."""
from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from importlib.metadata import distribution
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _prepare, _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock
from .delivery_resources import read_private, write_private

KIND = 'published_gate_retry'
PREPUBLICATION_KIND = 'prepublication_gate_retry'
PRELAUNCH_KIND = 'published_check_prelaunch_retry'
CI_KIND = 'published_ci_retry'
CONTROLLER_KIND = 'published_controller_retry'


def prepublication_preparation_failed(state):
    """Recognize the controller's exact preparation stop, never a product finding."""
    from .delivery_baseline_contract import preparation_failure

    precheck = state.get('checks', {}).get('prepublish', {})
    prerequisite = preparation_failure(precheck)
    return (precheck.get('state') == 'failed' and prerequisite is not None
            and state.get('error') == f'environment preparation failed: {prerequisite}; '
            'candidate retained without requesting code repair')


def _unpublished_remote_matches(broker, pr, remote):
    if broker.spec.get('feature_worker', {}).get('previous_publication'):
        from .delivery_feature_publication import verify_retained_publication

        verify_retained_publication(broker)
        return True
    return pr is None and not remote


def _consumed_payloads(spec, state, previous, *, include_implementation=False):
    """Follow actual gate launches; old journals retain their preparation identity."""
    records = [role for role in state.get('roles', [])
               if (role.get('role') in {'review', 'verify'}
                   or (include_implementation and role.get('role') == 'implement'))
               and role.get('iteration') == state.get('iteration')]
    for stage in ('prepublish', 'local', 'browser_qa'):
        check = state.get('checks', {}).get(stage, {})
        records.extend(check.get('results', []))
        records.append(check)
    policies = {spec['policy_digest']}
    while previous:
        for key in ('original_spec', 'execution_spec'):
            policy = previous.get(key, {}).get('policy_digest')
            if policy:
                policies.add(policy)
        previous = previous.get('original_recovery')
    payloads = set()
    for record in records:
        process = record.get('native_process', {})
        if not process.get('journal'):
            continue
        path = Path(process['journal'])
        if not path.is_relative_to(Path(spec['state_dir'])) or path.resolve(strict=True) != path:
            raise ValueError('gate controller journal left its original run')
        journal = read_private(path)
        identity = journal.get('runtime_identity')
        if identity is None and process.get('runtime_identity') is None:
            continue  # Historical monitors predate execution-version metadata.
        payload = identity.get('runtime_payload_sha256') if isinstance(identity, dict) else None
        if (journal.get('result') != process or identity != process.get('runtime_identity')
                or journal['intent']['run_id'] != spec['run_id']
                or journal['intent']['policy_digest'] not in policies
                or not isinstance(payload, str) or not re.fullmatch(r'[0-9a-f]{64}', payload)):
            raise ValueError('gate consumed controller evidence changed')
        payloads.add(payload)
    return payloads or {spec['policy']['native_identity']['runtime_payload_sha256']}


def missing_planned_report(spec, state, broker):
    """Prove a delegated report recipe was omitted from a passed local assessment."""
    from .delivery_check_evidence import verify_manifest
    from .delivery_plan_checks import planned_junit_recipes

    local = state.get('checks', {}).get('local', {})
    if local.get('state') != 'passed' or local.get('candidate_id') != state['candidate']['id']:
        raise ValueError('report recovery requires unchanged passed local checks')
    recipes = planned_junit_recipes(spec, broker.checkout, broker.evidence_dir / 'checks')
    if not recipes or any(result.get('plan_provenance', {}).get('recipe')
                          in {c['plan_provenance']['recipe'] for c in recipes}
                          for result in local.get('results', [])):
        raise ValueError('report recovery requires an omitted accepted JUnit recipe')
    configured = {c['id'] for c in spec['policy']['checks'] if c.get('kind') == 'test'}
    missing = []
    for result in local.get('results', []):
        if result.get('id') in configured and result.get('passed') is True:
            reference = result.get('artifacts')
            if reference and verify_manifest(reference, state['candidate']['id'],
                                             Path(spec['state_dir']))['count'] == 0:
                missing.append(reference)
    if not missing:
        raise ValueError('report recovery lacks authenticated missing report evidence')
    return {'omitted_recipes': [c['plan_provenance'] for c in recipes],
            'empty_manifests': missing}


def snapshot(store, run_id, kind=KIND):
    unpublished = kind == PREPUBLICATION_KIND
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
    state = closed['result']
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    ci_only = kind == CI_KIND
    controller_only = kind == CONTROLLER_KIND
    if controller_only and previous is not None:
        raise ValueError('controller retry requires its original finalized checkpoint')
    if (controller_only and (row['iteration'] > spec['policy']['max_repairs']
                             or (spec['provider'] != 'fake' and spec['policy'].get(
                                 'execution_backend') != 'native-macos'))):
        raise ValueError('controller retry cannot change its original native repair ceiling')
    browser_prelaunch = bool(kind == PRELAUNCH_KIND
                             and state.get('error') == 'browser QA child cleanup is unknown')
    prelaunch = browser_prelaunch or bool(
        kind == PRELAUNCH_KIND and previous and previous.get('kind') == KIND
        and spec.get('gate_retry_stage') == 'published'
        and spec.get('gate_retry_generation') == 1)
    report_retry = bool(kind == KIND and previous
                        and previous.get('kind') in {KIND, PRELAUNCH_KIND, CI_KIND}
                        and spec.get('gate_retry_stage') in {None, 'published', 'ci'})
    renewed = bool(unpublished and previous
                   and previous.get('kind') == PREPUBLICATION_KIND
                   and spec.get('gate_retry_generation') == 1)
    history = previous
    published_after_recovery = bool(not unpublished and previous and previous.get('kind') in {
        PREPUBLICATION_KIND, 'pending_publication_retry', 'repair_continuation'})
    if previous and previous.get('kind') == 'repair_continuation' and not previous.get(
            'finalized_checkpoint'):
        published_after_recovery = False
    while history:
        if browser_prelaunch and history.get('kind') == PRELAUNCH_KIND:
            raise ValueError('this run already received its bounded prelaunch retry')
        if report_retry and history.get('execution_spec', {}).get('gate_retry_stage') == 'report':
            raise ValueError('this run already received its bounded report assessment retry')
        if history.get('kind') == KIND:
            published_after_recovery = False
        if ci_only and history.get('kind') == CI_KIND:
            raise ValueError('this run already received its bounded CI observation retry')
        history = history.get('original_recovery')
    if ((renewed or published_after_recovery or prelaunch or report_retry or controller_only)
            and spec['provider'] != 'fake'):
        from .delivery_native_preparation import native_identity
        if native_identity(spec)['runtime_payload_sha256'] in _consumed_payloads(
                spec, state, previous, include_implementation=controller_only):
            raise ValueError('a later gate retry requires a repaired measured runtime')
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    pr = broker._existing_pr()
    roles = [r for r in state.get('roles', []) if r.get('iteration') == row['iteration']]
    implementation = next((r for r in roles if r.get('role') == 'implement'), None)
    failed = [r for r in roles if r.get('role') in {'review', 'verify'}
              and r.get('status') != 'pass' and r.get('findings')]
    precheck = state.get('checks', {}).get('prepublish', {})
    failed_gate = (precheck.get('state') == 'failed'
                   and precheck.get('source_unchanged') is True
                   and precheck.get('candidate_id') == candidate['id']
                   and state.get('checks') == json.loads(row['checks_json'] or '{}')
                   and not any(e['kind'] == 'publish' for e in effects)
                   and any(r.get('passed') is False and r.get('cleanup') == 'confirmed'
                           for r in precheck.get('results', []))) if unpublished else bool(failed)
    prelaunch_observation = None
    browser_observation = None
    report_observation = None
    controller_observation = None
    if controller_only:
        from .delivery_controller_retry import observe

        controller_observation = observe(store, spec, state, attempts, broker)
        failed_gate = True
    if report_retry:
        if state.get('checks') != json.loads(row['checks_json'] or '{}'):
            raise ValueError('report recovery lost its passed check projection')
        report_observation = missing_planned_report(spec, state, broker)
        failed_gate = True
    if browser_prelaunch:
        from .delivery_browser_prelaunch import observe

        if state.get('checks') != json.loads(row['checks_json'] or '{}'):
            raise ValueError('browser prelaunch lost its original check projection')
        browser_observation = observe(spec, state, effects)
        prelaunch_observation = browser_observation['resources']
        failed_gate = True
    elif prelaunch:
        from .delivery_check_prelaunch import observe

        prelaunch_observation = observe(spec, state, previous)
        failed_gate = True
    if ci_only:
        checks = state.get('checks', {})
        latest = {role: next((r for r in reversed(roles) if r.get('role') == role), {})
                  for role in ('review', 'verify')}
        sessions = [implementation.get('session_id') if implementation else None,
                    *(r.get('session_id') for r in latest.values())]
        if (checks != json.loads(row['checks_json'] or '{}')
                or checks.get('ci', {}).get('state') not in {'failed', 'pending'}
                or any(checks.get(k, {}).get('state') != 'passed'
                       or checks[k].get('candidate_id') != candidate['id']
                       for k in ('review', 'qa', 'local'))
                or (spec['policy'].get('browser_qa')
                    and checks.get('browser_qa', {}).get('state') != 'passed')
                or any(r.get('status') != 'pass'
                       or r.get('candidate', {}).get('id') != candidate['id']
                       for r in latest.values())
                or None in sessions or len(set(sessions)) != 3):
            raise ValueError('CI retry requires unchanged independent candidate passes')
        failed_gate = True
    if (spec['provider'] != 'fake' and spec['policy'].get('host_sandbox') != 'trusted-local'):
        raise ValueError('gate retry requires the existing trusted native execution policy')
    unpublished_error = (state.get('error') if prepublication_preparation_failed(state)
                         else 'prepublication repair limit exhausted')
    if ((kind == PRELAUNCH_KIND and not prelaunch)
            or (previous is not None and not (
                renewed or published_after_recovery or prelaunch
                or report_retry or ci_only))
            or row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked'
            or row['cleanup'] != ('unknown' if prelaunch else 'confirmed')
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or state.get('cleanup') != ('unknown' if prelaunch else 'confirmed')
            or row['error'] != state.get('error')
            or state.get('error') not in ({'repair limit exhausted',
                                          'required CI did not confirm this PR head'}
                                        if report_retry
                                        else {'required CI did not confirm this PR head'
                                              if ci_only
                                      else 'browser QA child cleanup is unknown'
                                      if browser_prelaunch else
                                      'local check process cleanup is unknown' if prelaunch
                                      else unpublished_error
                                      if unpublished else state.get('error') if controller_only
                                      else 'repair limit exhausted'})
            or row['protocol_revision'] != state.get('revision')
            or row['iteration'] != state.get('iteration')
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or not implementation or (implementation.get('status') != 'pass'
                                      and not controller_only) or not failed_gate
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)
            or any((e['state'] != 'complete' or not e['observed_json'])
                   and e != (browser_observation or {}).get('pending_effect') for e in effects)
            or (claim is not None and not prelaunch)):
        raise ValueError('gate retry requires a closed, finalized failed gate and released claim')
    frozen = json.loads(row['candidate_json'])
    publication = json.loads(row['pr_json'] or 'null')
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    publication_matches = (
        publication is None and state.get('pull_request') is None
        and _unpublished_remote_matches(broker, pr, remote)
        and candidate['head'] == spec['base_sha']
        and all(implementation.get('candidate', {}).get(k) == candidate.get(k)
                for k in ('id', 'head', 'base_sha', 'content_sha256', 'environment_digest'))
    ) if unpublished else (
        isinstance(publication, dict) and publication == state.get('pull_request')
        and pr and pr['state'] == 'OPEN' and not pr['isDraft']
        and pr['number'] == publication['number'] and pr['headRefOid'] == candidate['head']
        and remote and remote.split()[0] == candidate['head']
        and not _git(broker.checkout, 'status', '--porcelain', '--untracked-files=no')
    )
    if (any(candidate.get(k) != frozen.get(k) for k in candidate)
            or frozen != state.get('candidate') or not publication_matches
            or _git(broker.checkout, 'branch', '--show-current')
            != (spec.get('local_branch', spec['branch']) if unpublished else spec['branch'])
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or _git(broker.source, 'remote', 'get-url', 'origin') != spec['origin_url']
            or not broker._changed_paths() <= set(spec['policy']['allowed_paths'])):
        raise ValueError('gate retry lost unchanged owned source or exact published PR head')
    config = DeliveryConfig.load(Path(spec['config_path']))
    if digest(config.raw) != spec['config_digest']:
        raise ValueError('gate retry frozen configuration changed')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        admitted = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                              (run_id,)).fetchone()
        prior_gate = json.loads(admitted[0]) if admitted else None
        ancestor = previous
        while ancestor and ancestor.get('kind') in {
                'pending_publication_retry', 'repair_continuation'}:
            ancestor = ancestor.get('original_recovery')
        if admitted and (not (renewed or published_after_recovery or prelaunch
                             or report_retry or ci_only)
                         or prior_gate != ancestor):
            raise ValueError('this run already received its bounded gate assessment retry')
    return {'row': row, 'closed': closed, 'original_spec': spec, 'attempts': attempts,
            'effects': effects, 'candidate': candidate, 'publication': publication,
            'cleanup': prelaunch_observation or _stopped_cleanup(spec), 'work_binding': binding,
            'previous': previous, 'prior_gate': prior_gate,
            **({'report_observation': report_observation} if report_observation else {}),
            **({'controller_observation': controller_observation} if controller_only else {}),
            **({'browser_prelaunch_observation': browser_observation}
               if browser_observation else {}),
            'stage': 'report' if report_retry else 'ci' if ci_only else 'published'
                     if published_after_recovery or prelaunch or controller_only else None,
            'generation': (2 if renewed or (prelaunch and not browser_prelaunch)
                           or (browser_prelaunch and spec.get('gate_retry_stage') == 'published')
                           else 1)}


def admit(store, run_id, payload, *, preflight=False):
    kind = payload.get('continuation_kind') if isinstance(payload, dict) else None
    unpublished = kind == PREPUBLICATION_KIND
    head_field = 'expected_candidate_head' if unpublished else 'expected_pr_head'
    fields = {'continuation_kind', 'command_id', 'expected_revision', 'expected_iteration',
              'expected_candidate_id', head_field, 'additional_iterations'}
    if not unpublished:
        fields.add('expected_pr_number')
    if (not isinstance(payload, dict) or set(payload) not in (
            fields, fields | {'verification_test_paths'})
            or ('verification_test_paths' in payload and kind != KIND)
            or kind not in {KIND, PREPUBLICATION_KIND, PRELAUNCH_KIND, CI_KIND, CONTROLLER_KIND}
            or not isinstance(payload.get('command_id'), str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', payload['command_id'])
            or any(type(payload.get(k)) is not int or payload[k] < low for k, low in (
                ('expected_revision', 1), ('expected_iteration', 0),
                *([] if unpublished else [('expected_pr_number', 1)])))
            or type(payload.get('additional_iterations')) is not int
            or payload['additional_iterations'] != 0
            or any(not isinstance(payload.get(k), str)
                   or not re.fullmatch(r'[0-9a-f]{' + str(n) + '}', payload[k])
                   for k, n in (('expected_candidate_id', 64), (head_field, 40)))):
        raise ValueError('gate retry requires an exact zero-repair request')
    command_digest = digest({'run_id': run_id, **payload})
    with store._connect() as db:
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (payload['command_id'],)).fetchone()
    if prior:
        if prior['request_digest'] != command_digest:
            raise ValueError('command ID already belongs to different inputs')
        return json.loads(prior['response_json'])
    seal = snapshot(store, run_id, kind)
    if 'verification_test_paths' in payload:
        from .delivery_plan_checks import planned_checks

        selected = {**seal['original_spec'],
                    'verification_test_paths': payload['verification_test_paths']}
        seal['verification_checks'] = planned_checks(
            selected, DeliveryBroker(store, seal['original_spec']).checkout,
            Path(selected['state_dir']) / 'selected-verification')
        if not seal['verification_checks']:
            raise ValueError('explicit verification selection must execute existing tests')
    if (payload['expected_revision'] != seal['row']['protocol_revision']
            or payload['expected_iteration'] != seal['row']['iteration']
            or payload['expected_candidate_id'] != seal['candidate']['id']
            or payload[head_field] != seal['candidate']['head']
            or (not unpublished
                and payload['expected_pr_number'] != seal['publication']['number'])):
        raise ValueError('stale gate retry checkpoint')
    if preflight:
        return {'run_id': run_id, 'preflight': True, 'implementation_authority': False,
                'additional_iterations': 0, 'candidate_id': seal['candidate']['id']}
    spec = seal['original_spec']
    generation = seal['generation']
    root = Path(spec['state_dir']) / gate_namespace(generation, seal['stage'])
    with _lock(root / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                               (payload['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        if spec['provider'] != 'fake' and _git(
                Path(__file__).resolve().parents[3], 'status', '--porcelain',
                '--untracked-files=all'):
            raise ValueError('gate retry requires clean installed runtime source')
        execution = prepare_runtime(spec, root, command_digest, digest(seal))
        if 'verification_test_paths' in payload:
            execution['verification_test_paths'] = payload['verification_test_paths']
        from .delivery_session_custody import implementation_generation

        execution['implementation_role_home_generation'] = implementation_generation(
            spec, seal['previous'])
        execution['role_home_generation'] = (f'report-retry-{generation}'
                                              if seal['stage'] == 'report'
                                              else f'gate-retry-{generation}')
        execution['gate_retry_generation'] = generation
        if seal['stage']:
            execution['gate_retry_stage'] = seal['stage']
        current = snapshot(store, run_id, kind)
        if 'verification_checks' in seal:
            current['verification_checks'] = planned_checks(
                selected, DeliveryBroker(store, seal['original_spec']).checkout,
                Path(selected['state_dir']) / 'selected-verification')
        if digest(current) != digest(seal):
            raise ValueError('stopped gate checkpoint changed during runtime preparation')
        after = {**DeliveryBroker(store, spec).candidate(),
                 'policy_digest': execution['policy_digest']}
        if any(after[k] != seal['candidate'][k]
               for k in ('id', 'head', 'base_sha', 'content_sha256', 'environment_digest')):
            raise ValueError('gate retry changed feature source')
        recovery = {'kind': kind, 'command': payload, 'seal': seal,
                    'original_recovery': seal['previous'],
                    'original_spec': (seal['previous'].get('original_spec')
                                      or store.intake_execution_spec(run_id)
                                      if seal['previous'] else spec),
                    'execution_spec': execution,
                    'state': seal['closed']['result'], 'candidate': after,
                    'publication': (None if unpublished
                                    else {**seal['publication'], 'candidate': after})}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        workflow_id = (f'delivery-{run_id}-ci-retry-1' if kind == CI_KIND else
                       f'delivery-{run_id}-report-gates-retry-{generation}'
                       if seal['stage'] == 'report' else
                       f'delivery-{run_id}-published-gates-retry-{generation}' if seal['stage']
                       else f'delivery-{run_id}-gates-retry-{generation}')
        response = {'run_id': run_id, 'phase': 'gates_retry_queued', 'workflow_id': workflow_id,
                    'candidate_id': after['id'], 'implementation_authority': False,
                    'additional_iterations': 0}
        with store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
            if canonical_json(dict(row)) != canonical_json(seal['row']):
                raise ValueError('gate retry admission lost its stopped projection')
            if work_binding(store, spec, db) != seal['work_binding']:
                raise ValueError('gate retry admission lost issue ownership')
            if observation := seal.get('browser_prelaunch_observation'):
                from .delivery_browser_prelaunch import resolved_effect

                pending = observation['pending_effect']
                current_effect = db.execute('SELECT * FROM delivery_effects WHERE effect_key=?',
                                            (pending['effect_key'],)).fetchone()
                if not current_effect or dict(current_effect) != pending:
                    raise ValueError('browser prelaunch effect changed before resolution')
                # Finish the authenticated no-launch intent as a failure, never
                # an assessment pass. Its original bytes remain in the admission.
                db.execute("UPDATE delivery_effects SET state='complete',observed_json=?,"
                           'updated_at=? WHERE effect_key=?',
                           (canonical_json(resolved_effect(observation, seal['candidate'])),
                            store.state.now(), pending['effect_key']))
            store.state.claim_work(db, spec['work_id'], f'external:devflow:{run_id}',
                                   store.config.dashboard_url)
            if seal['prior_gate'] is None:
                db.execute('INSERT INTO delivery_gate_admissions VALUES (?,?,?)',
                           (run_id, payload['command_id'], canonical_json(recovery)))
            else:
                prior_gate = db.execute(
                    'SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                    (run_id,)).fetchone()
                if not prior_gate or json.loads(prior_gate[0]) != seal['prior_gate']:
                    raise ValueError('gate retry lost historical admission custody')
                db.execute('UPDATE delivery_gate_admissions SET command_id=?,recovery_json=? '
                           'WHERE run_id=?',
                           (payload['command_id'], canonical_json(recovery), run_id))
            db.execute("UPDATE delivery_runs SET phase='gates_retry_queued',"
                       "execution_state='queued',"
                       "outcome=NULL,error=NULL,revision=revision+1,workflow_id=?,recovery_json=?,"
                       "updated_at=? WHERE run_id=?",
                       (workflow_id, canonical_json(recovery), store.state.now(), run_id))
            db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                       'WHERE run_id=?', (store.state.now(), run_id))
            db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                       (payload['command_id'], run_id, command_digest, canonical_json(response)))
            store._event(db, run_id, row['revision'] + 1, 'gates_retry_queued',
                         'Fresh candidate gates; no implementation or repair grant',
                         response)
        return response


def gate_namespace(generation, stage=None):
    if stage is not None:
        if stage == 'report' and generation == 1:
            return 'report-gates-admission'
        if stage == 'ci' and generation == 1:
            return 'ci-admission'
        if stage != 'published' or generation not in (1, 2):
            raise ValueError('published gate retry exceeds its finite assessment bound')
        return 'published-gates-admission' + ('-2' if generation == 2 else '')
    if generation not in (1, 2):
        raise ValueError('gate retry generation exceeds the finite recovery bound')
    return 'gates-admission' if generation == 1 else 'gates-admission-2'


def effective_spec(store, original, recovery):
    root = Path(original['state_dir']) / gate_namespace(
        recovery['execution_spec'].get('gate_retry_generation', 1),
        recovery['execution_spec'].get('gate_retry_stage'))
    with store._connect() as db:
        saved = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                           (original['run_id'],)).fetchone()
    if (not saved or canonical_json(json.loads(saved[0])) != canonical_json(recovery)
            or canonical_json(read_private(root / 'admission.json')) != canonical_json(recovery)
            or recovery['original_spec'] != original
            or recovery['command']['additional_iterations'] != 0):
        raise ValueError('published gate retry lost its durable admission')
    return recovery['execution_spec']


def readback(store, spec, recovery):
    effective = effective_spec(store, recovery['original_spec'], recovery)
    if effective != spec or DeliveryBroker(store, spec).candidate() != recovery['candidate']:
        raise ValueError('gate retry source or execution authority changed')
    broker = DeliveryBroker(store, spec)
    if observation := recovery['seal'].get('browser_prelaunch_observation'):
        from .delivery_browser_prelaunch import observe, resolved_effect

        with store._connect() as db:
            effect = db.execute('SELECT * FROM delivery_effects WHERE effect_key=?',
                               (observation['pending_effect']['effect_key'],)).fetchone()
        pending = observation['pending_effect']
        if (not effect or any(effect[key] != pending[key] for key in pending
                              if key not in {'state', 'observed_json', 'updated_at'})
                or effect['state'] != 'complete' or json.loads(effect['observed_json'])
                != resolved_effect(observation, recovery['seal']['candidate'])):
            raise ValueError('browser prelaunch resolution changed after admission')
        if observe(recovery['seal']['original_spec'], recovery['state'],
                   recovery['seal']['effects'], resource_spec=spec,
                   evidence_root=Path(observation['evidence_root'])) != observation:
            raise ValueError('browser prelaunch preserved evidence changed after admission')
    if recovery['kind'] == CONTROLLER_KIND:
        from .delivery_controller_retry import observe

        original = recovery['seal']['original_spec']
        _, current_attempts, _, _ = _rows(store, spec['run_id'])
        current = {attempt['job_key']: attempt for attempt in current_attempts}
        if any(current.get(attempt['job_key']) != attempt
               for attempt in recovery['seal']['attempts']):
            raise ValueError('controller retry original attempt changed after admission')
        if observe(store, original, recovery['state'], recovery['seal']['attempts'],
                   broker) != recovery['seal']['controller_observation']:
            raise ValueError('controller retry preserved failure custody changed')
    pr = broker._existing_pr()
    unpublished = recovery['kind'] == PREPUBLICATION_KIND
    if unpublished:
        remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
        if (not _unpublished_remote_matches(broker, pr, remote)
                or recovery['publication'] is not None
                or recovery['candidate']['head'] != spec['base_sha']
                or not broker._changed_paths() <= set(spec['policy']['allowed_paths'])):
            raise ValueError('gate retry unpublished candidate acquired publication or lost scope')
    elif (not pr or pr['number'] != recovery['publication']['number'] or pr['state'] != 'OPEN'
            or pr['headRefOid'] != recovery['candidate']['head']):
        raise ValueError('gate retry exact published PR head changed')
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        verify_native_spec(spec)
    return recovery['publication']


def prepare_runtime(spec, root, command_digest, seal_digest):
    """Bind the same locked executable to current source without changing feature authority."""
    if spec['provider'] == 'fake':
        return deepcopy(spec)
    source = Path(__file__).resolve().parents[3]
    if _git(source, 'status', '--porcelain', '--untracked-files=all'):
        raise ValueError('continuation requires clean installed runtime source')
    with _lock(root / 'runtime-preparation'):
        path = root / 'runtime-preparation' / 'intent.json'
        intent = read_private(path) if path.exists() else {
            'command_digest': command_digest, 'seal_digest': seal_digest,
            'preparation_attempts': []}
        if intent['command_digest'] != command_digest or intent['seal_digest'] != seal_digest:
            raise ValueError('pending runtime preparation belongs to changed authority')
        write_private(path, intent)
        execution = deepcopy(spec)
        binary = Path(distribution('openai-codex-cli-bin').locate_file(
            'codex_cli_bin/bin/codex'))
        if hashlib.sha256(binary.read_bytes()).hexdigest() != spec['policy']['codex_bin_sha256']:
            raise ValueError('continuation cannot change the frozen CLI executable')
        execution['policy']['codex_bin'] = str(binary.resolve())
        return _prepare(execution, DeliveryConfig.load(Path(spec['config_path'])), intent, path)


def effective_repair(store, recovery, *, db=None):
    """Read the sealed modern repair authority, including its historical gate predecessor."""
    spec = recovery['execution_spec']
    run_id = spec['run_id']
    if db is None:
        with store._connect() as connection:
            return effective_repair(store, recovery, db=connection)
    grant = db.execute('SELECT predecessor_result_digest,maximum_iteration,granted_iterations '
                       'FROM delivery_repair_grants WHERE run_id=?', (run_id,)).fetchone()
    root = Path(spec['state_dir']) / 'repair-continuation'
    if (not grant or canonical_json(read_private(root / 'admission.json'))
            != canonical_json(recovery)
            or grant[0] != digest(recovery['state'])
            or grant[1] != recovery['maximum_iteration']
            or grant[2] != recovery['additional_iterations']):
        raise ValueError('finalized repair lost its immutable grant')
    return spec
