"""Fresh published gates after an authenticated, repaired JUnit attribution error.

The latest native assessment stays unpassed. Only unchanged previously passed
source and a retained controller verdict that the repaired parser can explain
permit fresh independent assessment, never another implementation turn.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from .contracts import digest
from .delivery_feature_gates import (
    _wrapped_junit_project,
    resolve_required_selectors,
    retained_junit,
    selector_evidence,
)
from .delivery_feature_revision_recovery import gate_evidence, retained_recovery
from .delivery_feature_revision_subject import (
    _native_role,
    _private_receipt,
    _progress,
    _runtime,
    _time,
    bounded_findings,
    full_candidate,
)


def eligible_stop(spec, state):
    latest = next((r for r in reversed(state.get('roles', []))
                   if r.get('role') == 'implement'), {})
    return (spec.get('feature_worker', {}).get('kind') == 'chunk'
            and state.get('error') == 'implementer did not establish a pass'
            and latest.get('status') in {'blocked', 'findings'})


def possible(store, spec, row):
    """A read-only selection hint; observe still authenticates every fact."""
    if (spec.get('provider') != 'codex'
            or spec.get('feature_worker', {}).get('kind') != 'chunk'
            or row.get('error') != 'implementer did not establish a pass'):
        return False
    with store._connect() as db:
        attempts = list(db.execute('SELECT job_key FROM delivery_attempts WHERE run_id=? '
                                  "AND role='implement' AND iteration=? AND state='finished'",
                                  (spec['run_id'], row['iteration'])))
    if len(attempts) != 1:
        return False
    if not isinstance(attempts[0][0], str) or not re.fullmatch(r'[0-9a-f]{64}', attempts[0][0]):
        return False
    request = _private_receipt(Path(spec['state_dir']) / 'attempts' / attempts[0][0]
                               / 'request.json', canonical=True)
    results = (request.get('evidence_context') or {}).get('checks', {}).get('local', {}).get(
        'results', [])
    failed = [r for r in results if r.get('passed') is not True]
    return (request.get('spec') == spec and len(failed) == 1
            and str(failed[0].get('evidence_failure', '')).startswith(
                'required selector contributed no complete passing evidence: '))


def _gate_ancestor(store, transport):
    from .delivery_gate_retry import KIND, PRELAUNCH_KIND, gate_namespace
    from .delivery_resources import read_private
    from .delivery_stopped_resume import KIND as STOPPED
    from .delivery_stopped_resume import custody

    previous = transport
    with store._connect() as db:
        for _ in range(100):
            if not previous or previous.get('kind') != STOPPED:
                break
            custody(db, previous)
            previous = retained_recovery(previous).get('original_recovery')
        else:
            raise ValueError('attribution recovery exceeds bounded retained admission history')
        saved = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                           (transport['execution_spec']['run_id'],)).fetchone()
    prior = json.loads(saved[0]) if saved else None
    from .delivery_gate_preflight_continuation import _authority

    if (prior or {}).get('execution_spec', {}).get('gate_retry_stage') == 'report':
        from .delivery_gate_retry import _assessment_ancestry

        prior = _assessment_ancestry(prior)[-1]
    if (prior or {}).get('seal', {}).get('preparation_observation'):
        from .delivery_check_prelaunch import preparation_authority

        execution = prior['execution_spec']
        path = Path(execution['state_dir']) / gate_namespace(
            execution['gate_retry_generation'], execution['gate_retry_stage'])
        if read_private(path / 'admission.json') != prior:
            raise ValueError('attribution readback lost its preparation predecessor')
        preparation_authority(execution, prior)
        prior = prior['original_recovery']
    for _ in range(2):
        if not prior or not prior.get('execution_spec', {}).get('gate_retry_preflight_generation'):
            break
        execution = prior['execution_spec']
        path = Path(execution['state_dir']) / gate_namespace(
            execution['gate_retry_generation'], execution.get('gate_retry_stage'),
            preflight_generation=execution['gate_retry_preflight_generation'],
            prepared_handoff=execution.get('gate_retry_prepared_handoff', 0))
        if read_private(path / 'admission.json') != prior:
            raise ValueError('attribution readback lost its owned preflight continuation')
        _authority(execution, prior)
        prior = prior['original_recovery']
    if prior and prior.get('execution_spec', {}).get('gate_retry_preflight_generation'):
        raise ValueError('attribution readback exceeds its bounded preflight continuations')
    if (prior and prior.get('kind') == KIND and prior.get('original_recovery') == transport
            and prior.get('execution_spec', {}).get('gate_retry_stage') == 'published'
            and prior['execution_spec'].get('gate_retry_generation') == 2
            and prior.get('seal', {}).get('attribution_observation', {}).get('prior_gate_digest')
            == digest(previous)):
        # Readback of the exact new immutable admission keeps its predecessor
        # receipt authoritative; it cannot admit another assessment generation.
        path = Path(prior['execution_spec']['state_dir']) / gate_namespace(2, 'published')
        if read_private(path / 'admission.json') != prior:
            raise ValueError('attribution readback lost its exact new gate admission')
        prior = previous
    if (not prior or prior != previous or prior.get('kind') not in {KIND, PRELAUNCH_KIND}
            or prior['execution_spec'].get('gate_retry_stage') != 'published'
            or prior['execution_spec'].get('gate_retry_generation') != 1):
        raise ValueError('attribution recovery lacks its unused bounded published assessment')
    spec = prior['execution_spec']
    path = Path(spec['state_dir']) / gate_namespace(1, 'published') / 'admission.json'
    if read_private(path) != prior:
        raise ValueError('attribution recovery lost its original gate admission')
    return digest(prior)


def observe(store, spec, row, state, attempts, closed, transport, broker):
    if (not eligible_stop(spec, state) or spec.get('provider') != 'codex'
            or not transport or transport.get('kind') != 'stopped_delivery_resume'):
        raise ValueError('attribution retry requires its retained original native worker')
    recovery = retained_recovery(transport)
    session = recovery.get('session_id')
    seal = {'state': state, 'closed': closed, 'candidate': broker.candidate(),
            'session_id': session, 'attempts': attempts, 'original_recovery': transport}
    progress = _progress(store, spec, row, attempts, recovery, seal)
    candidate = full_candidate(seal['candidate'])
    latest = next(r for r in reversed(state['roles']) if r.get('role') == 'implement')
    passed = gate_evidence(seal)
    prior = next((r for r in reversed(state['roles']) if r.get('role') == 'implement'
                  and r.get('status') == 'pass' and r.get('iteration') < latest['iteration']
                  and full_candidate(r.get('candidate')) == candidate), None)
    if (not passed or not prior or latest.get('iteration') != state['iteration']
            or latest.get('finish_reason') != 'done' or latest.get('cleanup') != 'confirmed'
            or latest.get('session_id') != session
            or full_candidate(latest.get('candidate')) != candidate):
        raise ValueError('attribution retry cannot approve changed or unproved implementation')
    findings = bounded_findings(latest.get('findings'))
    matches = [a for a in attempts if a['role'] == 'implement'
               and a['iteration'] == latest['iteration'] and a['session_id'] == session]
    if len(matches) != 1:
        raise ValueError('attribution retry lost its unique latest native assessment')
    attempt = matches[0]
    proof = _native_role(spec, latest, attempt, session, _time(recovery['closed']['closed_at']),
                         _time(closed['closed_at']), recovery['execution_candidate'])
    request = _private_receipt(Path(spec['state_dir']) / 'attempts' / attempt['job_key']
                               / 'request.json', canonical=True)
    context = request.get('evidence_context') or {}
    checks = context.get('checks') or {}
    local, prepublish = checks.get('local') or {}, checks.get('prepublish') or {}
    failures = [r for r in local.get('results', []) if r.get('passed') is not True]
    if (digest(request) != proof['request_sha256']
            or full_candidate(context.get('candidate')) != candidate
            or context.get('iteration') != prior['iteration']
            or request.get('candidate', {}).get('id') != candidate['id']
            or prepublish.get('state') != 'passed'
            or prepublish.get('candidate_id') != candidate['id']
            or prepublish.get('source_unchanged') is not True
            or not prepublish.get('results')
            or any(r.get('passed') is not True or r.get('cleanup') != 'confirmed'
                   for r in prepublish['results'])
            or local.get('state') != 'failed' or local.get('candidate_id') != candidate['id']
            or local.get('source_unchanged') is not True
            or any(r.get('cleanup') != 'confirmed' for r in local.get('results', []))
            or len(failures) != 1):
        raise ValueError('attribution retry lost its unchanged prepublication/local evidence')
    failed = failures[0]
    recipes = [r for r in spec['policy'].get('checks', []) if r['id'] == failed.get('id')]
    if len(recipes) != 1:
        raise ValueError('attribution retry cannot identify its frozen recipe')
    recipe = recipes[0]
    counts = failed.get('junit') or {}
    native = failed.get('native_process') or {}
    if (failed.get('exit_code') != 0 or failed.get('rejected_output') is not False
            or failed.get('cleanup') != 'confirmed' or failed.get('argv') != recipe['argv']
            or not counts.get('tests') or counts.get('passed') != counts['tests']
            or any(counts.get(key) != 0 for key in ('failures', 'errors', 'skipped'))
            or native.get('state') != 'finished' or native.get('exit_code') != 0
            or native.get('monitoring_complete') is not True
            or native.get('cleanup') != 'observed-native-confirmed'
            or native.get('stdio_drained') is not True
            or any(native.get(key) is not False for key in ('timed_out', 'cancelled'))
            or not _wrapped_junit_project(recipe, spec)):
        raise ValueError('attribution retry requires only a completed passing wrapped JUnit')
    journal = _private_receipt(Path(native['journal']))
    if (journal.get('phase') != 'finished' or journal.get('result') != native
            or journal.get('monitoring_complete') is not True
            or journal.get('runtime_identity') != native.get('runtime_identity')
            or journal.get('intent', {}).get('run_id') != spec['run_id']
            or journal.get('intent', {}).get('policy_digest') != spec['policy_digest']
            or journal.get('intent', {}).get('cwd') != failed.get('cwd')
            or journal.get('intent', {}).get('argv') != recipe['argv']):
        raise ValueError('attribution retry lost its original check monitor')
    runtime = _runtime(native.get('runtime_identity'))
    from .delivery_check_evidence import junit_counts

    if junit_counts(failed['artifacts'], candidate['id'], Path(spec['state_dir'])) != counts:
        raise ValueError('attribution retry JUnit counts changed from its retained report')
    report = retained_junit(failed['artifacts'], candidate['id'], Path(spec['state_dir']))
    selectors = resolve_required_selectors(recipe, broker.checkout, spec=spec)
    try:
        selector_evidence(recipe, broker.checkout, selectors, junit=report)
    except ValueError as exc:
        failure = str(exc)
    else:
        raise ValueError('attribution retry retained no exact missing selector verdict')
    corrected = selector_evidence(recipe, broker.checkout, selectors, junit=report, spec=spec)
    if (failure != failed.get('evidence_failure')
            or not failure.startswith(
                'required selector contributed no complete passing evidence: ')
            or len(corrected) != len(selectors)):
        raise ValueError('attribution retry does not prove the repaired selector mapping')
    return {'cause': 'wrapped_junit_source_attribution', 'progress': progress,
            'prior_gate_digest': _gate_ancestor(store, transport),
            'implementation_evidence': passed, 'latest_assessment': proof,
            'unpassed_status': latest['status'], 'unresolved_findings': findings,
            'check_runtime': runtime, 'check_id': recipe['id'],
            'check_journal_sha256': digest(journal), 'artifact_reference': failed['artifacts'],
            'corrected_selector_evidence': corrected}


def cleanup_observation(store, row, spec, closed):
    """Prove the cause and owned publication before ordinary cleanup effects."""
    from .delivery_broker import _git
    from .delivery_feature_revision_subject import ObservationBroker
    from .delivery_policy_recovery import _rows, work_binding
    from .delivery_source_scope import outside_scope

    current, attempts, _, _ = _rows(store, spec['run_id'])
    if current != row:
        raise ValueError('attribution cleanup predecessor changed before observation')
    state = closed['result']
    broker = ObservationBroker(store, spec)
    candidate, publication = broker.candidate(), json.loads(row['pr_json'] or 'null')
    pr = broker._existing_pr()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    if (full_candidate(candidate) != full_candidate(state.get('candidate'))
            or full_candidate(candidate) != full_candidate(json.loads(row['candidate_json']))
            or not isinstance(publication, dict) or publication != state.get('pull_request')
            or not pr or pr['state'] != 'OPEN' or pr['isDraft']
            or pr['number'] != publication['number'] or pr['headRefOid'] != candidate['head']
            or remote.split() != [candidate['head'], 'refs/heads/' + spec['branch']]
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or _git(broker.source, 'remote', 'get-url', 'origin') != spec['origin_url']
            or _git(broker.checkout, 'status', '--porcelain', '--untracked-files=no')
            or outside_scope(spec['policy'], broker._changed_paths(), checkout=broker.checkout)):
        raise ValueError('attribution cleanup lost its unchanged owned published source')
    with store._connect() as db:
        work_binding(store, spec, db)
    return observe(store, spec, row, state, attempts, closed,
                   json.loads(row['recovery_json'] or 'null'), broker)
