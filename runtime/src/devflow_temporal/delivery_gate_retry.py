"""Bounded fresh gates on unchanged candidates after measured runtime repairs."""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shlex
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from importlib.metadata import distribution
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_browser_qa import browser_artifact_possible, observe_browser_artifacts
from .delivery_execution_config import load_execution_config
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import (
    _prepare,
    _rows,
    _stopped_cleanup,
    closed_checks,
    gate_failed,
    reconciled_cleanup,
    work_binding,
)
from .delivery_preparation import _lock
from .delivery_resources import read_private, write_private
from .delivery_source_scope import outside_scope

KIND = 'published_gate_retry'
PREPUBLICATION_KIND = 'prepublication_gate_retry'
PRELAUNCH_KIND = 'published_check_prelaunch_retry'
CI_KIND = 'published_ci_retry'
CONTROLLER_KIND = 'published_controller_retry'
# Same-run continuations for classified transient stops: fresh gates on the
# unchanged published head, and read-only CI observation while it is unchanged.
TRANSIENT_GATE_RETRIES = 2
CI_OBSERVATION_RETRIES = 8


def _sealed_row(row):
    """The stopped row with its retained recovery chain referenced by digest.

    Embedding the chain again in every continuation would multiply the
    record with each generation.
    """
    sealed = dict(row)
    chain = sealed.pop('recovery_json')
    sealed['recovery_digest'] = digest(json.loads(chain)) if chain else None
    return sealed


def _retained_recovery(store, run_id, seal):
    with store._connect() as db:
        saved = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                           (run_id,)).fetchone()
    previous = json.loads(saved[0]) if saved and saved[0] else None
    if (digest(previous) if previous else None) != seal['previous_digest']:
        raise ValueError('stopped gate checkpoint changed before admission')
    return previous


def transient_stop(state):
    """A classified transport or controller stop; gate and model outcomes are not."""
    checks = state.get('checks') or {}
    failure = checks.get('failure') or {}
    return (failure.get('classification') == 'transient'
            or isinstance(checks.get('controller_failure'), dict))


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



def report_layout_possible(row):
    """Selection hint for the exact retained basetemp report failure."""
    previous = json.loads(row.get('recovery_json') or 'null') or {}
    local = json.loads(row.get('checks_json') or '{}').get('local') or {}
    failures = [r for r in local.get('results', []) if r.get('passed') is not True]
    return bool(previous.get('kind') == PRELAUNCH_KIND
                and previous.get('execution_spec', {}).get('gate_retry_stage') == 'preparation'
                and previous.get('seal', {}).get('preparation_observation')
                and local.get('state') == 'failed' and len(failures) == 1
                and failures[0].get('evidence_failure')
                == 'required per-selector JUnit report is missing')


def _report_layout_closed(store, spec, row, closed, previous):
    """Authenticate original start/memo, full row and actual native check activity."""
    from .delivery_closures import sealed_closure
    from .delivery_codec import DELIVERY_DATA_CONVERTER
    from .delivery_feature_closure import completed_worker

    async def read():
        sealed = await sealed_closure(store, closed['workflow_id'])
        if sealed is None:
            raise ValueError('report layout recovery requires its sealed worker history')
        authenticated = await completed_worker(sealed['history'], store, spec, row, closed)
        calls, completed = {}, {}
        for event in sealed['history'].events:
            if event.HasField('activity_task_scheduled_event_attributes'):
                item = event.activity_task_scheduled_event_attributes
                if item.activity_type.name in {'delivery_role', 'delivery_publish'}:
                    raise ValueError('report layout recovery cannot replace assessed product work')
                if item.activity_type.name in {'delivery_precheck', 'delivery_checks'}:
                    values = await DELIVERY_DATA_CONVERTER.decode(item.input.payloads)
                    if values != [{'spec': spec, 'iteration': closed['result']['iteration'],
                                   'candidate': closed['result']['candidate']}]:
                        raise ValueError('report layout activity lost its original candidate')
                    calls[event.event_id] = item.activity_type.name
            elif event.HasField('activity_task_completed_event_attributes'):
                item = event.activity_task_completed_event_attributes
                if item.scheduled_event_id in calls:
                    values = await DELIVERY_DATA_CONVERTER.decode(item.result.payloads)
                    completed[item.scheduled_event_id] = (event.event_id, values)
        ids = list(calls)
        if ([calls[key] for key in ids] != ['delivery_precheck', 'delivery_checks']
                or set(completed) != set(ids)
                or not ids[0] < completed[ids[0]][0] < ids[1] < completed[ids[1]][0]
                or any(completed[key][1] != [closed['result']['checks'][stage]]
                       for key, stage in zip(ids, ('prepublish', 'local'), strict=True))):
            raise ValueError('report layout history lost its exact precheck and local verdicts')
        return authenticated['history_sha256']

    with ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(lambda: asyncio.run(read())).result(35)


def _report_layout_evidence(spec, state, broker, *, resource_spec=None):
    from .candidate import assert_candidate
    from .delivery_check_evidence import _owned_bytes, junit_counts, verify_manifest
    from .delivery_feature_gates import (
        _wrapped_junit_project,
        bind_selector_report,
        resolve_required_selectors,
        retained_junit,
        selector_evidence,
    )
    from .delivery_feature_revision_subject import _private_receipt, _runtime
    from .delivery_resources import _gate_roots

    candidate, checks = state['candidate'], state.get('checks') or {}
    precheck, local = checks.get('prepublish') or {}, checks.get('local') or {}
    failures = [r for r in local.get('results', []) if r.get('passed') is not True]
    if (precheck.get('state') != 'passed' or local.get('state') != 'failed'
            or any(c.get('candidate_id') != candidate['id']
                   or c.get('source_unchanged') is not True for c in (precheck, local))
            or not precheck.get('results') or not local.get('results')
            or any(r.get('passed') is not True or r.get('cleanup') != 'confirmed'
                   for r in precheck['results'])
            or any(r.get('cleanup') != 'confirmed' or type(r.get('exit_code')) is not int
                   or r['exit_code'] != 0 or r.get('rejected_output') is not False
                   or r.get('rejection_causes') != [] for r in local['results'])
            or len(failures) != 1
            or len({r.get('id') for r in local['results']}) != len(local['results'])):
        raise ValueError('report layout recovery requires unchanged complete native checks')
    failed = failures[0]
    recipes = [r for r in spec['policy']['checks'] if r['id'] == failed.get('id')]
    if len(recipes) != 1:
        raise ValueError('report layout recovery lost its frozen configured recipe')
    recipe, native = recipes[0], failed.get('native_process') or {}
    bindings = {}
    _gate_roots(resource_spec or spec, execution_specs=bindings)
    report_predecessor = None
    if resource_spec and resource_spec != spec:
        from .delivery_listener_verification import authority

        root = Path(resource_spec['state_dir']) / gate_namespace(
            resource_spec['gate_retry_generation'], resource_spec.get('gate_retry_stage'))
        newest = read_private(root / 'admission.json')
        if newest.get('seal', {}).get('verification_observation'):
            report_predecessor = authority(resource_spec, newest)[1]
    if any(admitted.get('gate_retry_stage') == 'report'
           and admitted not in ((resource_spec or spec), report_predecessor)
           for admitted in bindings.values()):
        raise ValueError('this run already received its bounded report assessment retry')
    root = Path(spec['state_dir']) / gate_namespace(
        spec['gate_retry_generation'], spec['gate_retry_stage']) / 'evidence'
    if bindings.get(root) != spec:
        raise ValueError('report layout recovery lost its admitted execution namespace')
    configured = {r['id']: r for r in spec['policy']['checks']}
    ids = list(configured)
    prefix = [r for r in local['results'] if r['id'] in configured]
    # The native broker stops at the first rejected verdict. Unexecuted tail
    # recipes remain required by the unchanged policy in the fresh assessment.
    if (local['results'][-1] != failed
            or [r['id'] for r in prefix] != ids[:ids.index(recipe['id']) + 1]
            or any(r.get('argv') != bind_selector_report(configured[r['id']],
                root / 'checks' / str(state['iteration']) / r['id'], prepare=False)['argv']
                   for r in prefix)):
        raise ValueError('report layout recovery lost its exact configured check prefix')
    extra = [r for r in local['results'] if r['id'] not in configured]
    if extra:
        addon = local.get('native_addon_preparation') or {}
        path = root / 'checks' / str(state['iteration']) / 'native-addon-preparation.json'
        dynamic = ['native-addon-node-identity', 'native-addon-build', 'native-addon-load']
        start = next(i for i, r in enumerate(local['results']) if r['id'] == extra[0]['id'])
        if (addon.get('state') != 'passed' or addon.get('candidate_id') != candidate['id']
                or addon.get('receipt') != str(path) or addon.get('results') != extra
                or [r['id'] for r in extra] != dynamic or not start
                or local['results'][start:start + len(extra)] != extra
                or '/store' not in local['results'][start - 1].get('argv', [])
                or read_private(path) != {k: v for k, v in addon.items()
                                          if k not in {'receipt', 'receipt_sha256'}}
                or hashlib.sha256(_owned_bytes(path)).hexdigest() != addon.get('receipt_sha256')):
            raise ValueError('report layout recovery lost its admitted native prerequisite prefix')
    folder = root / 'checks' / str(state['iteration']) / recipe['id']
    checkout = root / 'gates' / str(state['iteration']) / 'verify'
    cwd = checkout / recipe['cwd']
    if (failed.get('evidence_failure') != 'required per-selector JUnit report is missing'
            or failed.get('passed') is not False or type(failed.get('exit_code')) is not int
            or failed['exit_code'] != 0
            or failed.get('rejected_output') is not False or failed.get('rejection_causes') != []
            or failed.get('argv') != recipe['argv'] or failed.get('cwd') != str(cwd)
            or not recipe.get('junit_required') or not _wrapped_junit_project(recipe, spec)
            or native.get('journal') != str(folder / 'native/native-process.json')
            or native.get('log') != str(folder / 'native/process.log')
            or native.get('state') != 'finished' or type(native.get('exit_code')) is not int
            or native['exit_code'] != 0
            or native.get('cleanup') != 'observed-native-confirmed'
            or native.get('monitoring_complete') is not True
            or native.get('stdio_drained') is not True
            or any(native.get(key) is not False for key in (
                'timed_out', 'cancelled', 'listener_conflict'))):
        raise ValueError('report layout recovery requires only a completed passing frozen wrapper')
    assert_candidate(broker.checkout, candidate)
    journal = _private_receipt(Path(native['journal']))
    launch = _private_receipt(folder / 'native/launch.json')
    intent = journal.get('intent') or {}
    environment = launch.get('environment') or {}
    if (journal.get('phase') != 'finished' or journal.get('result') != native
            or journal.get('monitoring_complete') is not True
            or journal.get('runtime_identity') != native.get('runtime_identity')
            or journal.get('interrupted_monitor') or native.get('interrupted_monitor')
            or intent.get('run_id') != spec['run_id']
            or intent.get('policy_digest') != spec['policy_digest']
            or intent.get('argv') != recipe['argv'] or launch.get('argv') != recipe['argv']
            or intent.get('cwd') != str(cwd) or intent.get('ports') != []
            or environment.get('PYTEST_ADDOPTS') != '--basetemp=' + shlex.quote(
                str(folder / 'pytest-artifacts/fixtures'))
            or intent.get('environment_sha256') != digest(environment)
            or hashlib.sha256(_owned_bytes(Path(native['log']))).hexdigest()
            != native.get('log_sha256')):
        raise ValueError('report layout recovery lost its original native launch and environment')
    runtime = _runtime(native.get('runtime_identity'))
    reference = failed.get('artifacts') or {}
    if reference.get('path') != str(folder / 'artifacts.json'):
        raise ValueError('report layout recovery lost its candidate-owned report manifest')
    manifest = verify_manifest(reference, candidate['id'], Path(spec['state_dir']))
    if (manifest['count'] != 1
            or manifest['artifacts'][0]['relative_path'] != 'fixtures/junit.xml'):
        raise ValueError('report layout recovery requires its exact displaced fixture report')
    counts = junit_counts(reference, candidate['id'], Path(spec['state_dir']))
    if (not counts['tests'] or counts['passed'] != counts['tests']
            or counts['passed'] < int(recipe.get('min_tests', 1))
            or any(counts[k] != 0 for k in ('failures', 'errors', 'skipped'))
            or failed.get('test_count') != counts['passed']
            or failed.get('junit') not in (None, counts)):
        raise ValueError('report layout recovery cannot override actual test findings')
    selectors = resolve_required_selectors(recipe, broker.checkout, spec=spec)
    if not selectors:
        raise ValueError('report layout recovery requires its frozen selector evidence')
    corrected = selector_evidence(recipe, broker.checkout, selectors, junit=retained_junit(
        reference, candidate['id'], Path(spec['state_dir'])), spec=spec)
    return {'check_id': recipe['id'], 'check_runtime': runtime,
            'check_journal_sha256': digest(journal),
            'launch_sha256': digest(launch), 'artifact_reference': reference, 'junit': counts,
            'corrected_selector_evidence': corrected}


def _report_layout_observe(store, spec, row, attempts, effects, closed, previous, broker):
    from .delivery_feature_closure import closed_worker
    from .delivery_gate_preflight_continuation import source_readback
    from .delivery_resources import observe_completed_resources

    state = closed['result']
    if (not report_layout_possible(row) or spec.get('resource_cleanup_version') != 1
            or spec.get('feature_worker', {}).get('kind') != 'chunk'
            or state.get('roles') != previous['state'].get('roles')
            or state.get('iteration') != previous['state'].get('iteration')
            or state.get('candidate') != previous['candidate']
            or state.get('pull_request') != previous['publication']
            or attempts != previous['seal']['attempts'] or effects != previous['seal']['effects']
            or not previous['seal'].get('attribution_observation')):
        raise ValueError('report layout recovery lost its unchanged preparation authority')
    authenticated = closed_worker(store, spec['run_id'], closed['workflow_id'], sealed_only=True)
    if any(authenticated[key] != closed.get(key) for key in closed):
        raise ValueError('report layout recovery current worker closure changed')
    history = _report_layout_closed(store, spec, row, closed, previous)
    if history != authenticated['history_sha256']:
        raise ValueError('report layout recovery sealed history changed')
    readback(store, spec, previous, resource_spec=spec)
    source_readback(store, spec, previous)
    evidence = _report_layout_evidence(spec, state, broker)
    resources = observe_completed_resources(spec, attempts)
    current_cleanup = json.loads(row['checks_json'])['resource_cleanup']
    if resources['finalization_sha256'] != current_cleanup.get('receipt_sha256'):
        raise ValueError('report layout recovery lost its finalized native cleanup')
    with store._connect() as db:
        if (_rows(store, spec['run_id'])[:3] != (row, attempts, effects)
                or store.state.claim_for(db, spec['work_id']) is not None):
            raise ValueError('report layout recovery current rows or ownership changed')
    return {'cause': 'fixture_junit_report_layout', 'history_sha256': history,
            'resources': resources, **evidence}


def _report_layout_authority(spec, recovery):
    from .delivery_check_prelaunch import preparation_authority
    from .delivery_continuation import continuation_authority

    previous = recovery.get('original_recovery') or {}
    before = previous.get('execution_spec') or {}
    proof = recovery.get('seal', {}).get('report_observation') or {}
    history = previous
    while history:
        if history.get('execution_spec', {}).get('gate_retry_stage') == 'report':
            raise ValueError('this run already received its bounded report assessment retry')
        history = history.get('original_recovery')
    if (proof.get('cause') != 'fixture_junit_report_layout' or recovery.get('kind') != KIND
            or spec.get('gate_retry_stage') != 'report' or spec.get('gate_retry_generation') != 1
            or spec.get('gate_retry_preflight_generation')
            or spec.get('gate_retry_prepared_handoff')
            or before.get('gate_retry_stage') != 'preparation'
            or recovery.get('command', {}).get('additional_iterations') != 0
            or recovery['state'] != recovery['seal']['closed']['result']
            or recovery['seal'].get('previous_digest') != digest(previous)
            or not previous['seal'].get('attribution_observation')
            or recovery['seal'].get('attribution_observation')
            != previous['seal']['attribution_observation']
            or continuation_authority(spec['policy']) != continuation_authority(before['policy'])
            or any(spec.get(key) != before.get(key) for key in (
                'run_id', 'work_id', 'state_dir', 'checkout', 'source_path', 'branch', 'base_sha',
                'accepted_plan', 'feature_worker', 'feature_delivery', 'feature_plan_revision',
                'config_digest', 'plan_approval', 'authorized_endpoint', 'origin_url', 'base_ref',
                'publication_base_ref', 'expected_paths', 'implementation_role_home_generation',
                'provider'))):
        raise ValueError('report layout retry changed its retained assessment authority')
    preparation_authority(before, previous)
    return previous, before, proof


def _assessment_ancestry(recovery):
    """One verification, report, preparation and two admitted preflight links."""
    from .delivery_check_prelaunch import preparation_authority
    from .delivery_gate_preflight_continuation import _authority
    from .delivery_listener_verification import authority as verification_authority
    from .delivery_prerequisite_gate_recovery import authority as prerequisite_authority

    chain, previous = [], recovery
    for kind in ('prerequisite', 'browser-ordering', 'verification', 'report', 'preparation',
                 'preflight', 'preflight'):
        if not previous:
            break
        spec = previous['execution_spec']
        matches = ((previous.get('seal', {}).get('browser_artifact_observation') or {}).get('cause')
                   == 'browser_artifact_assessment_order' if kind == 'browser-ordering'
                   else bool(previous.get('seal', {}).get('verification_observation'))
                   if kind == 'verification' else spec.get('gate_retry_stage') == kind
                   if kind != 'preflight' else bool(spec.get('gate_retry_preflight_generation')))
        if not matches:
            continue
        root = Path(spec['state_dir']) / gate_namespace(spec['gate_retry_generation'],
            spec.get('gate_retry_stage'),
            preflight_generation=spec.get('gate_retry_preflight_generation', 0),
            prepared_handoff=spec.get('gate_retry_prepared_handoff', 0))
        if read_private(root / 'admission.json') != previous:
            raise ValueError('gate retry finite predecessor admission changed')
        from .delivery_browser_qa import browser_ordering_authority

        authority = (prerequisite_authority if kind == 'prerequisite'
                     else browser_ordering_authority if kind == 'browser-ordering'
                     else verification_authority if kind == 'verification'
                     else _report_layout_authority if kind == 'report' else preparation_authority
                     if kind == 'preparation' else _authority)
        chain.append(previous)
        previous = authority(spec, previous)[0]
    if previous and ((previous.get('seal', {}).get('browser_artifact_observation') or {}).get(
                         'cause') == 'browser_artifact_assessment_order'
                     or previous.get('seal', {}).get('verification_observation')
                     or previous['execution_spec'].get('gate_retry_stage')
                     in {'report', 'preparation', 'prerequisite'}
                     or previous['execution_spec'].get('gate_retry_preflight_generation')):
        raise ValueError('gate retry exceeds its finite retained assessment ancestry')
    return [*chain, previous]


def _report_layout_validate(store, spec, recovery, *, resource_spec=None):
    from .delivery_check_evidence import _owned_bytes
    from .delivery_feature_revision_subject import ObservationBroker
    from .delivery_gate_preflight_continuation import source_readback

    previous, before, proof = _report_layout_authority(spec, recovery)
    row = {**recovery['seal']['row'], 'recovery_json': canonical_json(previous)}
    if _report_layout_closed(store, before, row, recovery['seal']['closed'], previous) \
            != proof['history_sha256']:
        raise ValueError('report layout retry original closure changed')
    latest = resource_spec or spec
    readback(store, before, previous, resource_spec=latest)
    source_readback(store, before, previous, resource_spec=latest)
    broker = ObservationBroker(store, before, resource_spec=latest)
    observed = _report_layout_evidence(before, recovery['state'], broker, resource_spec=latest)
    if any(proof.get(key) != value for key, value in observed.items()):
        raise ValueError('report layout retry original report or native cause changed')
    root = Path(spec['state_dir']) / gate_namespace(1, 'report') / 'predecessor-resources'
    for name, key in [('manifest.json', 'manifest_sha256'),
                      ('finalization.json', 'finalization_sha256')]:
        read_private(root / name)
        if hashlib.sha256(_owned_bytes(root / name)).hexdigest() != proof['resources'][key]:
            raise ValueError('report layout retry lost its original resource custody')
    from .delivery_listener_verification import historical_records

    historical_records(store, recovery, latest)

def snapshot(store, run_id, kind=KIND):
    unpublished = kind == PREPUBLICATION_KIND
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
    state = closed['result']
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    from .delivery_listener_verification import possible as verification_possible

    verification_retry = kind == KIND and verification_possible(row)
    from .delivery_gate_preflight_continuation import possible
    from .delivery_gate_preflight_continuation import snapshot as preflight_snapshot

    if kind == KIND and possible(row):
        return preflight_snapshot(store, spec, row, attempts, effects, closed)
    from .delivery_check_prelaunch import (
        PREPARATION_STAGE,
        preparation_observe,
        preparation_possible,
    )

    if kind == PRELAUNCH_KIND and preparation_possible(row):
        proof = preparation_observe(store, spec, row, attempts, effects, closed)
        if spec['provider'] != 'fake':
            from .delivery_native_preparation import native_identity

            if native_identity(spec)['runtime_payload_sha256'] in _consumed_payloads(
                    spec, state, previous):
                raise ValueError('published preparation retry requires a repaired measured runtime')
        return {'row': _sealed_row(row), 'closed': closed, 'original_spec': spec,
                'attempts': attempts, 'effects': effects, 'candidate': state['candidate'],
                'publication': state['pull_request'], 'cleanup': proof['resources'],
                'work_binding': proof['work_binding'], 'previous_digest': digest(previous),
                'prior_gate_digest': digest(previous), 'stage': PREPARATION_STAGE,
                'generation': spec['gate_retry_generation'], 'preparation_observation': proof,
                **({'attribution_observation': previous['seal']['attribution_observation']}
                   if 'attribution_observation' in previous['seal'] else {})}
    ci_only = kind == CI_KIND
    controller_only = kind == CONTROLLER_KIND
    from .delivery_gate_attribution_recovery import eligible_stop

    browser_artifact_retry = kind == KIND and browser_artifact_possible(store, spec, row)
    attribution_retry = kind == KIND and eligible_stop(spec, state) and not browser_artifact_retry
    failure = state.get('checks', {}).get('failure') or {}
    reconciled = reconciled_cleanup(store, row, state, closed=closed)
    state_checks, row_checks = closed_checks(row, state, reconciled)
    from .delivery_prerequisite_gate_recovery import possible as prerequisite_possible

    prerequisite_retry = kind == KIND and prerequisite_possible(spec, state)
    # A recorded gate verdict stands: a transport or controller stop after it is
    # never a fresh assessment of the same head.
    verdict = gate_failed(state.get('checks') or {})
    # A lost CI observation keeps CI observation semantics, never fresh gates.
    transient_retry = bool(kind == KIND and transient_stop(state) and not verdict
                           and failure.get('stage') != 'waiting_ci'
                           and failure.get('cause_type') not in {
                               'tracker_readback', 'publication_deadline', 'ci_deadline'})
    # A lost observation never recorded a CI state; its classified stop at the
    # CI stage is the same unchanged-head condition as a recorded deadline.
    transient_ci = bool(kind == CI_KIND and not verdict
                        and failure.get('classification') == 'transient'
                        and failure.get('stage') == 'waiting_ci')
    # A controller stop that masked the run's last repair is the exhausted repair
    # it would have reported: the same one-time operator reassessment applies.
    masked_exhaustion = bool(kind == KIND and not unpublished and verdict
                             and transient_stop(state)
                             and row['iteration'] >= spec['policy']['max_repairs'])
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
    layout_report = bool(kind == KIND and report_layout_possible(row))
    report_retry = bool(not transient_retry and not browser_artifact_retry
                        and kind == KIND and previous
                        and previous.get('kind') in {KIND, PRELAUNCH_KIND, CI_KIND}
                        and (spec.get('gate_retry_stage') in {None, 'published', 'ci'}
                             or layout_report))
    renewed = bool(unpublished and previous
                   and previous.get('kind') == PREPUBLICATION_KIND
                   and spec.get('gate_retry_generation') == 1)
    history = previous
    ci_observations = transient_generations = 0
    published_after_recovery = bool(not unpublished and previous and previous.get('kind') in {
        PREPUBLICATION_KIND, 'pending_publication_retry', 'repair_continuation',
        'published_head_validation'})
    if previous and previous.get('kind') == 'repair_continuation' and not previous.get(
            'finalized_checkpoint'):
        published_after_recovery = False
    while history:
        if prerequisite_retry and history.get('seal', {}).get('ordinary_prerequisite_observation'):
            raise ValueError('ordinary prerequisite already received its one gate recovery')
        if browser_prelaunch and history.get('kind') == PRELAUNCH_KIND:
            raise ValueError('this run already received its bounded prelaunch retry')
        if report_retry and history.get('execution_spec', {}).get('gate_retry_stage') == 'report':
            raise ValueError('this run already received its bounded report assessment retry')
        if history.get('kind') == KIND:
            published_after_recovery = False
            if history.get('execution_spec', {}).get('gate_retry_stage') == 'transient':
                transient_generations += 1
        if history.get('kind') == CI_KIND:
            ci_observations += 1
        history = history.get('original_recovery')
    if ci_only and ci_observations >= CI_OBSERVATION_RETRIES:
        raise ValueError('this run already received its bounded CI observation retries')
    if transient_retry and transient_generations >= TRANSIENT_GATE_RETRIES:
        raise ValueError('this run already received its bounded transient gate retries')
    if ((renewed or published_after_recovery or prelaunch or report_retry or controller_only)
            and spec['provider'] != 'fake'):
        from .delivery_native_preparation import native_identity
        if native_identity(spec)['runtime_payload_sha256'] in _consumed_payloads(
                spec, state, previous, include_implementation=controller_only):
            raise ValueError('a later gate retry requires a repaired measured runtime')
    if attribution_retry or verification_retry or browser_artifact_retry:
        from .delivery_feature_revision_subject import ObservationBroker

        broker = ObservationBroker(store, spec)
    else:
        broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    pr = broker._existing_pr()
    roles = [r for r in state.get('roles', []) if r.get('iteration') == row['iteration']]
    implementation = next((r for r in roles if r.get('role') == 'implement'), None)
    failed = [r for r in roles if r.get('role') in {'review', 'verify'}
              and r.get('status') != 'pass' and r.get('findings')]
    precheck = state.get('checks', {}).get('prepublish', {})
    browser = state.get('checks', {}).get('browser_qa', {})
    # Browser setup/test failures happen before the verifier role is launched.
    # The existing bounded gates-only retry must not require that missing role.
    failed_browser = (state_checks == row_checks and browser.get('state') == 'failed'
                      and browser.get('candidate_id') == candidate['id']
                      and browser.get('source_unchanged') is True
                      and browser.get('cleanup') == 'confirmed')
    failed_gate = (precheck.get('state') == 'failed'
                   and precheck.get('source_unchanged') is True
                   and precheck.get('candidate_id') == candidate['id']
                   and state_checks == row_checks
                   and not any(e['kind'] == 'publish' for e in effects)
                   and any(r.get('passed') is False and r.get('cleanup') == 'confirmed'
                           for r in precheck.get('results', []))) if unpublished else bool(
                               failed or failed_browser)
    local = state.get('checks', {}).get('local', {})
    download_failures = [r for r in local.get('results', []) if r.get('passed') is False]
    latest_acceptance = [next((r for r in reversed(roles) if r.get('role') == role), {})
                         for role in ('review', 'verify')]
    local_download_retry = bool(
        kind == KIND and previous and previous.get('kind') == 'published_head_validation'
            and state_checks == row_checks and local.get('state') == 'failed'
            and local.get('candidate_id') == candidate['id']
            and local.get('source_unchanged') is True and download_failures
            and all(r.get('status') == 'pass'
                    and r.get('candidate', {}).get('id') == candidate['id']
                    for r in latest_acceptance)
            and all(r.get('cleanup') == 'confirmed' and 'pytest' in r.get('argv', [])
                    and 'Failed starting test server: Temp download file' in r.get('diagnostic', '')
                    and 'not complete after' in r.get('diagnostic', '')
                    for r in download_failures))
    if local_download_retry:
        failed_gate = True
    prelaunch_observation = None
    browser_observation = None
    report_observation = None
    controller_observation = None
    attribution_observation = None
    browser_artifact_observation = None
    verification_observation = None
    prerequisite_observation = None
    if prerequisite_retry:
        from .delivery_prerequisite_gate_recovery import observe

        if state_checks != row_checks:
            raise ValueError('published prerequisite lost its exact original checks')
        prerequisite_observation = observe(
            store, spec, row, state, attempts, effects, closed, previous, broker)
        failed_gate = True
    if attribution_retry:
        from .delivery_gate_attribution_recovery import observe
        from .delivery_native_preparation import native_identity

        attribution_observation = observe(
            store, spec, row, state, attempts, closed, previous, broker)
        if native_identity(spec)['runtime_payload_sha256'] == attribution_observation[
                'check_runtime']['runtime_payload_sha256']:
            raise ValueError('attribution retry requires a repaired measured runtime')
        failed_gate = True
    if browser_artifact_retry:
        from .delivery_native_preparation import native_identity

        browser_artifact_observation = observe_browser_artifacts(
            store, spec, row, state, attempts, closed, previous, broker)
        if native_identity(spec)["runtime_payload_sha256"] == browser_artifact_observation[
                "check_runtime"]["runtime_payload_sha256"]:
            raise ValueError("browser artifact retry requires a repaired measured runtime")
        failed_gate = True
    if controller_only:
        from .delivery_controller_retry import observe

        controller_observation = observe(store, spec, state, attempts, broker)
        failed_gate = True
    if transient_retry:
        failed_gate = True
    if verification_retry:
        from .delivery_listener_verification import observe

        verification_observation = observe(store, spec, row, attempts, effects, closed, previous)
        failed_gate = True
    if report_retry:
        current_checks = json.loads(row['checks_json'] or '{}')
        original_checks = state.get('checks') or {}
        if layout_report:
            # The real closed-worker proof below authenticates ordinary later
            # cleanup writes; every original assessment verdict remains exact.
            current_checks = {k: v for k, v in current_checks.items() if k != 'resource_cleanup'}
            original_checks = {k: v for k, v in original_checks.items()
                               if k != 'resource_cleanup'}
        if original_checks != current_checks:
            raise ValueError('report recovery lost its passed check projection')
        report_observation = (_report_layout_observe(
            store, spec, row, attempts, effects, closed, previous, broker) if layout_report
            else missing_planned_report(spec, state, broker))
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
        # A controller failure while handling the observation may have left a
        # malformed CI record; the continuation observes CI again regardless.
        recorded_ci = checks.get('ci') if isinstance(checks.get('ci'), dict) else {}
        if (state_checks != row_checks
                or (recorded_ci.get('state') not in {'failed', 'pending'}
                    and not transient_ci)
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
                or report_retry or ci_only or transient_retry or attribution_retry
                or verification_retry or browser_artifact_retry or prerequisite_retry))
            or row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked'
            or row['cleanup'] != ('unknown' if prelaunch else 'confirmed')
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or (prelaunch and state.get('cleanup') != 'unknown')
            or (not prelaunch and state.get('cleanup') != 'confirmed' and reconciled is None)
            or row['error'] != state.get('error')
            or (not (transient_retry or transient_ci or masked_exhaustion)
                and state.get('error') not in ({'repair limit exhausted',
                                          'required CI did not confirm this PR head'}
                                        if report_retry
                                        else {'required CI did not confirm this PR head'
                                              if ci_only
                                      else 'browser QA child cleanup is unknown'
                                      if browser_prelaunch else
                                      'local check process cleanup is unknown' if prelaunch
                                      else unpublished_error
                                      if unpublished else state.get('error')
                                      if (controller_only or attribution_retry
                                          or browser_artifact_retry or prerequisite_retry)
                                      else 'repair limit exhausted'}))
            or row['protocol_revision'] != state.get('revision')
            or row['iteration'] != state.get('iteration')
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or not implementation or (implementation.get('status') != 'pass'
                                      and not (controller_only or attribution_retry
                                               or browser_artifact_retry
                                               or layout_report or verification_retry))
            or not failed_gate
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
            or outside_scope(spec['policy'], broker._changed_paths(), checkout=broker.checkout)):
        raise ValueError('gate retry lost unchanged owned source or exact published PR head')
    load_execution_config(spec)
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
                             or report_retry or ci_only or transient_retry or attribution_retry
                             or verification_retry or browser_artifact_retry or prerequisite_retry)
                         or (digest(prior_gate) != attribution_observation['prior_gate_digest']
                             if attribution_retry else prior_gate != ancestor)):
            raise ValueError('this run already received its bounded gate assessment retry')
    result = {'row': _sealed_row(row), 'closed': closed, 'original_spec': spec,
            'attempts': attempts,
            'effects': effects, 'candidate': candidate, 'publication': publication,
            'cleanup': (prerequisite_observation['resources'] if prerequisite_retry
                        else verification_observation['resources'] if verification_retry
                        else report_observation['resources'] if layout_report
                        else prelaunch_observation or _stopped_cleanup(spec)),
            'work_binding': binding,
            'previous_digest': digest(previous) if previous else None,
            'prior_gate_digest': digest(prior_gate) if prior_gate else None,
            **({'local_download_retry': True} if local_download_retry else {}),
            **({'ordinary_prerequisite_observation': prerequisite_observation}
               if prerequisite_observation else {}),
            **({'report_observation': report_observation} if report_observation else {}),
            **({'verification_observation': verification_observation}
               if verification_observation else {}),
            **({'controller_observation': controller_observation} if controller_only else {}),
            **({'browser_artifact_observation': browser_artifact_observation}
               if browser_artifact_retry else {}),
            **({'attribution_observation': (previous['seal']['attribution_observation']
                                           if layout_report or verification_retry
                                           else attribution_observation)}
               if attribution_retry or layout_report or verification_retry else {}),
            **({'browser_prelaunch_observation': browser_observation}
               if browser_observation else {}),
            'stage': 'prerequisite' if prerequisite_retry
                     else 'transient' if transient_retry or verification_retry
                     else 'report' if report_retry
                     else 'ci' if ci_only else 'published'
                     if published_after_recovery or prelaunch or controller_only
                     or attribution_retry or browser_artifact_retry else None,
            'generation': (transient_generations + 1 if transient_retry
                           else ci_observations + 1 if ci_only
                           else (2 if previous else 1)
                           if attribution_retry or browser_artifact_retry
                           else 2 if renewed
                           or (prelaunch and not browser_prelaunch)
                           or (browser_prelaunch and spec.get('gate_retry_stage') == 'published')
                           else 1)}
    if ci_only:
        from .delivery_ci_inventory import FIELD, for_seal

        if observation := for_seal(store, spec, result):
            result[FIELD] = observation
    return result


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
    previous = _retained_recovery(store, run_id, seal)
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
    preflight_generation = (seal['preflight_continuation'].get('preflight_generation', 1)
                            if 'preflight_continuation' in seal else 0)
    layout = (seal.get('report_observation') or {}).get('cause') == 'fixture_junit_report_layout'
    verification = bool(seal.get('verification_observation'))
    ordering = (seal.get('browser_artifact_observation') or {}).get(
        'cause') == 'browser_artifact_assessment_order'
    if (preflight_generation or seal.get('preparation_observation')
            or layout or verification or ordering) \
            and spec.get('feature_worker'):
        from .delivery_feature_execution import require_worker_handoff

        require_worker_handoff(store, spec)
    root = Path(spec['state_dir']) / gate_namespace(
        generation, seal['stage'], preflight_generation=preflight_generation)
    with _lock(root / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                               (payload['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        prepared = None
        if preflight_generation == 1:
            from .delivery_gate_preflight_continuation import prepared_handoff

            prepared = prepared_handoff(store, seal, root)
            if prepared:
                from .delivery_resources import private_directory

                seal['prepared_handoff'] = prepared
                root = root / 'prepared-handoff'
                private_directory(root)
        if spec['provider'] != 'fake' and _git(
                Path(__file__).resolve().parents[3], 'status', '--porcelain',
                '--untracked-files=all'):
            raise ValueError('gate retry requires clean installed runtime source')
        execution = prepare_runtime(
            spec, root, command_digest, digest(seal),
            **({'handoff': (store, seal, payload)} if verification else {}))
        preparation_owner = None
        preparation_path = root / 'runtime-preparation/successor/intent.json'
        if verification and preparation_path.exists():
            prepared_intent = read_private(preparation_path)
            if (prepared_intent['command_digest'] != command_digest
                    or prepared_intent['seal_digest'] != digest(seal)):
                raise ValueError('unused runtime preparation changed before gate admission')
            preparation_owner = prepared_intent['coordinator']['owner']
        if 'verification_test_paths' in payload:
            execution['verification_test_paths'] = payload['verification_test_paths']
        from .delivery_session_custody import implementation_generation

        execution['implementation_role_home_generation'] = implementation_generation(
            spec, previous)
        execution['role_home_generation'] = (
            f'report-retry-{generation}' if seal['stage'] == 'report'
            else f'transient-gate-retry-{generation}' if seal['stage'] == 'transient'
            else f'gate-retry-{generation}')
        execution['gate_retry_generation'] = generation
        if layout or verification:
            execution.pop('gate_retry_preflight_generation', None)
            execution.pop('gate_retry_prepared_handoff', None)
        if seal.get('preparation_observation'):
            execution.pop('gate_retry_preflight_generation', None)
            execution.pop('gate_retry_prepared_handoff', None)
            execution['role_home_generation'] = f'gate-preparation-retry-{generation}'
        if preflight_generation:
            execution['gate_retry_preflight_generation'] = preflight_generation
            execution.pop('gate_retry_prepared_handoff', None)
            execution['role_home_generation'] = f'gate-retry-{generation}-preflight-continuation'
            if preflight_generation > 1:
                execution['role_home_generation'] += f'-{preflight_generation}'
            if prepared:
                execution['gate_retry_prepared_handoff'] = 1
                execution['role_home_generation'] += '-prepared-handoff'
        if seal['stage']:
            execution['gate_retry_stage'] = seal['stage']
        current = snapshot(store, run_id, kind)
        if prepared:
            original_root = root.parent
            if prepared_handoff(store, current, original_root) != prepared:
                raise ValueError('prepared gate handoff changed during runtime preparation')
            current['prepared_handoff'] = prepared
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
                    'original_recovery': previous,
                    'original_spec': (previous.get('original_spec')
                                      or store.intake_execution_spec(run_id)
                                      if previous else spec),
                    'execution_spec': execution,
                    'state': seal['closed']['result'], 'candidate': after,
                    'publication': (None if unpublished
                                    else {**seal['publication'], 'candidate': after})}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        workflow_id = (f'delivery-{run_id}-ci-retry-{generation}' if kind == CI_KIND else
                       f'delivery-{run_id}-report-gates-retry-{generation}'
                       if seal['stage'] == 'report' else
                       f'delivery-{run_id}-transient-gates-retry-{generation}'
                       if seal['stage'] == 'transient' else
                       f'delivery-{run_id}-published-gates-retry-{generation}' if seal['stage']
                       else f'delivery-{run_id}-gates-retry-{generation}')
        if preflight_generation:
            workflow_id += '-preflight-continuation'
            if preflight_generation > 1:
                workflow_id += f'-{preflight_generation}'
        if seal.get('preparation_observation'):
            workflow_id += '-preparation-retry'
        response = {'run_id': run_id, 'phase': 'gates_retry_queued', 'workflow_id': workflow_id,
                    'candidate_id': after['id'], 'implementation_authority': False,
                    'additional_iterations': 0}
        with store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
            if canonical_json(_sealed_row(row)) != canonical_json(seal['row']):
                raise ValueError('gate retry admission lost its stopped projection')
            if (verification or ordering or seal.get('ordinary_prerequisite_observation')
                    or seal.get('ordinary_ci_inventory')):
                if store.state.claim_for(db, spec['work_id']) is not None:
                    raise ValueError('listener verification admission lost its released claim')
                for table, key, expected in [('delivery_attempts', 'job_key', seal['attempts']),
                                             ('delivery_effects', 'effect_key', seal['effects'])]:
                    records = [dict(item) for item in db.execute(
                        f'SELECT * FROM {table} WHERE run_id=? ORDER BY {key}', (run_id,))]
                    if records != expected:
                        raise ValueError('listener verification admission lost its full ledger')
            if work_binding(store, spec, db) != seal['work_binding']:
                raise ValueError('gate retry admission lost issue ownership')
            if ((preflight_generation or seal.get('preparation_observation')
                 or layout or verification or ordering)
                    and spec.get('feature_worker')):
                require_worker_handoff(store, spec)
            if preparation_owner is not None:
                _preparation_parent_claim(store, preparation_owner, db)
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
            acquired = store.state.claim_work(
                db, spec['work_id'], f'external:devflow:{run_id}', store.config.dashboard_url)
            if seal.get('ordinary_prerequisite_observation') or seal.get('ordinary_ci_inventory'):
                from .delivery_prerequisite_gate_recovery import claim_identity

                response['claim_identity'] = claim_identity(acquired['claim'])
            if seal['prior_gate_digest'] is None:
                db.execute('INSERT INTO delivery_gate_admissions VALUES (?,?,?)',
                           (run_id, payload['command_id'], canonical_json(recovery)))
            else:
                prior_gate = db.execute(
                    'SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                    (run_id,)).fetchone()
                if (not prior_gate
                        or digest(json.loads(prior_gate[0])) != seal['prior_gate_digest']):
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


def gate_namespace(generation, stage=None, *, preflight_generation=0, prepared_handoff=0):
    if prepared_handoff:
        if (type(prepared_handoff) is not int or prepared_handoff != 1
                or preflight_generation != 1):
            raise ValueError('prepared gate handoff exceeds its one completion namespace')
        return gate_namespace(generation, stage, preflight_generation=1) + '/prepared-handoff'
    if preflight_generation:
        if type(preflight_generation) is not int or preflight_generation not in (1, 2) \
                or generation not in (1, 2) or stage != 'published':
            raise ValueError('gate preflight continuation exceeds its bounded assessment')
        suffix = f'-{preflight_generation}' if preflight_generation > 1 else ''
        return gate_namespace(generation, stage) + '/preflight-continuation' + suffix
    if stage is not None:
        if stage == 'prerequisite' and type(generation) is int and generation == 1:
            return 'ordinary-prerequisite-gates-admission'
        if stage == 'preparation' and type(generation) is int and generation in (1, 2):
            return f'published-preparation-admission-{generation}'
        if stage == 'report' and generation == 1:
            return 'report-gates-admission'
        if stage == 'ci' and 1 <= generation <= CI_OBSERVATION_RETRIES:
            return 'ci-admission' + (f'-{generation}' if generation > 1 else '')
        if stage == 'transient' and 1 <= generation <= TRANSIENT_GATE_RETRIES:
            return f'transient-gates-admission-{generation}'
        if stage != 'published' or generation not in (1, 2):
            raise ValueError('published gate retry exceeds its finite assessment bound')
        return 'published-gates-admission' + ('-2' if generation == 2 else '')
    if generation not in (1, 2):
        raise ValueError('gate retry generation exceeds the finite recovery bound')
    return 'gates-admission' if generation == 1 else 'gates-admission-2'


def effective_spec(store, original, recovery):
    root = Path(original['state_dir']) / gate_namespace(
        recovery['execution_spec'].get('gate_retry_generation', 1),
        recovery['execution_spec'].get('gate_retry_stage'),
        preflight_generation=recovery['execution_spec'].get('gate_retry_preflight_generation', 0),
        prepared_handoff=recovery['execution_spec'].get('gate_retry_prepared_handoff', 0))
    with store._connect() as db:
        saved = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                           (original['run_id'],)).fetchone()
    current = json.loads(saved[0]) if saved else None
    if current != recovery:
        if recovery not in _assessment_ancestry(current):
            raise ValueError('published gate retry historical admission changed')
    if (not saved
            or canonical_json(read_private(root / 'admission.json')) != canonical_json(recovery)
            or recovery['original_spec'] != original
            or recovery['command']['additional_iterations'] != 0):
        raise ValueError('published gate retry lost its durable admission')
    if recovery.get('seal', {}).get('ordinary_prerequisite_observation'):
        from .delivery_prerequisite_gate_recovery import authority

        authority(recovery['execution_spec'], recovery)
    if recovery.get('seal', {}).get('ordinary_ci_inventory'):
        from .delivery_ci_inventory import authority

        authority(store, recovery)
    return recovery['execution_spec']


def readback(store, spec, recovery, *, resource_spec=None):
    effective = effective_spec(store, recovery['original_spec'], recovery)
    from .delivery_feature_revision_subject import ObservationBroker

    broker = (ObservationBroker(store, spec, resource_spec=resource_spec)
              if resource_spec is not None else DeliveryBroker(store, spec))
    if effective != spec or broker.candidate() != recovery['candidate']:
        raise ValueError('gate retry source or execution authority changed')
    if recovery['seal'].get('ordinary_ci_inventory'):
        from .delivery_ci_inventory import validate

        validate(store, spec, recovery, resource_spec=resource_spec or spec)
    if recovery['seal'].get('ordinary_prerequisite_observation'):
        from .delivery_prerequisite_gate_recovery import validate

        validate(store, spec, recovery, resource_spec=resource_spec or spec)
    elif recovery['seal'].get('verification_observation'):
        from .delivery_listener_verification import validate

        validate(store, spec, recovery, resource_spec=resource_spec or spec)
    elif (recovery['seal'].get('report_observation') or {}).get(
            'cause') == 'fixture_junit_report_layout':
        _report_layout_validate(store, spec, recovery, resource_spec=resource_spec or spec)
    elif recovery['seal'].get('preparation_observation'):
        from .delivery_check_prelaunch import preparation_validate

        preparation_validate(store, spec, recovery, resource_spec=resource_spec or spec)
    elif spec.get('gate_retry_preflight_generation'):
        from .delivery_gate_preflight_continuation import validate

        validate(store, spec, recovery, resource_spec=resource_spec or spec)
    elif expected := recovery["seal"].get("browser_artifact_observation"):
        original = recovery["seal"]["original_spec"]
        _, current_attempts, _, _ = _rows(store, spec["run_id"])
        current = {attempt["job_key"]: attempt for attempt in current_attempts}
        if any(current.get(attempt["job_key"]) != attempt
               for attempt in recovery["seal"]["attempts"]):
            raise ValueError("browser artifact retry original native attempt changed")
        closed = store._completed_temporal_result(
            spec["run_id"], workflow_id=recovery["seal"]["closed"]["workflow_id"])
        if (closed != recovery["seal"]["closed"] or observe_browser_artifacts(
                store, original, recovery["seal"]["row"], closed["result"],
                recovery["seal"]["attempts"], closed, recovery["original_recovery"],
                ObservationBroker(
                    store, original, resource_spec=resource_spec or spec),
                resource_spec=resource_spec or spec) != expected):
            raise ValueError("browser artifact retry original cause or source evidence changed")
    elif expected := recovery['seal'].get('attribution_observation'):
        from .delivery_feature_revision_subject import ObservationBroker
        from .delivery_gate_attribution_recovery import observe

        original = recovery['seal']['original_spec']
        _, current_attempts, _, _ = _rows(store, spec['run_id'])
        current = {attempt['job_key']: attempt for attempt in current_attempts}
        if any(current.get(attempt['job_key']) != attempt
               for attempt in recovery['seal']['attempts']):
            raise ValueError('attribution retry original native attempt changed')
        closed = store._completed_temporal_result(
            spec['run_id'], workflow_id=recovery['seal']['closed']['workflow_id'])
        if closed != recovery['seal']['closed']:
            raise ValueError('attribution retry original Temporal result changed')
        if observe(store, original, recovery['seal']['row'], closed['result'],
                   recovery['seal']['attempts'], closed, recovery['original_recovery'],
                   ObservationBroker(store, original, resource_spec=resource_spec or spec)) \
                != expected:
            raise ValueError('attribution retry original cause or source evidence changed')
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
                or outside_scope(spec['policy'], broker._changed_paths(),
                                 checkout=broker.checkout)):
            raise ValueError('gate retry unpublished candidate acquired publication or lost scope')
    elif (not pr or pr['number'] != recovery['publication']['number'] or pr['state'] != 'OPEN'
            or pr['headRefOid'] != recovery['candidate']['head']):
        raise ValueError('gate retry exact published PR head changed')
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        verify_native_spec(spec)
    return recovery['publication']


def _preparation_parent_claim(store, owner, db):
    from .delivery_feature_execution import require_execution

    parent = store.effective_spec(owner['run_id'])
    if (parent.get('feature_delivery', {}).get('owner') != owner
            or owner['store_path'] != str(store.config.tracking_db)
            or store.state.claim_for(db, parent['work_id']) is None):
        raise ValueError('unused runtime preparation lost its current parent claim')
    require_execution(store, parent)
    work_binding(store, parent, db)
    return parent


def _unused_runtime_preparation(spec, root, raw, handoff, command_digest, seal_digest):
    """Preserve unused preparation; only the fresh stopped snapshot grants gates."""
    from .delivery_continuation import continuation_authority
    from .delivery_feature_closure import closed_coordinator
    from .delivery_feature_execution import registry, require_worker_handoff, worker_key
    from .delivery_gate_preflight_continuation import _prepared_coordinator
    from .delivery_listener_verification import CAUSE
    from .delivery_native_preparation import native_identity, pinned_identity, verify_native_spec

    store, seal, command = handoff
    intent = json.loads(raw)
    prepared = intent.get('effective_spec') or {}
    preparation = prepared.get('preparation') or {}
    if (seal.get('verification_observation', {}).get('cause') != CAUSE
            or seal['original_spec'] != spec or not spec.get('feature_worker')
            or seal['stage'] != 'transient' or seal['generation'] != 1
            or root != Path(spec['state_dir']) / gate_namespace(1, 'transient')
            or command.get('continuation_kind') != KIND
            or command.get('additional_iterations') != 0
            or set(intent) != {'command_digest', 'seal_digest', 'preparation_attempts',
                               'state', 'effective_spec'}
            or intent.get('state') != 'prepared' or intent.get('preparation_attempts') != []
            or preparation.get('cache_reused') is not True
            or 'measured_policy_digest' in preparation
            or any(not isinstance(intent.get(key), str)
                   or not re.fullmatch(r'[0-9a-f]{64}', intent[key])
                   for key in ('command_digest', 'seal_digest'))
            or spec['policy'].get('host_sandbox') != 'trusted-local'):
        raise ValueError('unused runtime preparation has no cache-only listener handoff')
    pending = root / 'admission.json'
    if pending.exists() or pending.is_symlink():
        envelope = read_private(pending)
        if (envelope.get('command') != command or envelope.get('seal') != seal
                or digest(envelope.get('original_recovery')) != seal['previous_digest']):
            raise ValueError('unused runtime preparation has a different pending admission')
    config = load_execution_config(spec)

    def frozen_preparation(value):
        verify_native_spec(value)
        expected = deepcopy(spec)
        expected.update(config_path=str(config.path), config_digest=digest(config.raw),
                        terminal_tracker_version=1)
        expected['policy']['codex_bin'] = str(Path(distribution(
            'openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')).resolve())
        value = deepcopy(value)
        for item in (expected, value):
            item.pop('preparation', None)
            item.pop('policy_digest', None)
            item['policy'] = continuation_authority(item['policy'])
        if value != expected:
            raise ValueError('unused runtime preparation changed its frozen source or policy')

    frozen_preparation(prepared)
    shared = registry(spec)
    current = shared.current(spec['feature_delivery']['owner']['issue_id'])
    if not current or current['state'] not in {'active', 'draining'}:
        raise ValueError('unused runtime preparation has no current coordinator')
    current_token = shared.token(current)
    closed = {key: seal['closed'][key] for key in ('workflow_id', 'execution_run_id')}

    def command_for(owner):
        return {**command, 'command_id': 'feature-resume-' + digest({
            'owner': owner, 'run': spec['run_id'], 'closed': closed})[:24]}

    if (command_for(current_token) != command
            or digest({'run_id': spec['run_id'], **command}) != command_digest):
        raise ValueError('unused runtime preparation requires its current server command')

    def producer(request_digest, *, released):
        matches = []
        with shared.connect() as db:
            for row in db.execute('SELECT * FROM execution_snapshots '
                                  'WHERE issue_id=? AND store_path=?',
                                  (current_token['issue_id'], str(store.config.tracking_db))):
                previous_command = command_for(shared.token(row))
                if digest({'run_id': spec['run_id'], **previous_command}) == request_digest:
                    matches.append(previous_command)
        if len(matches) != 1:
            raise ValueError('unused runtime preparation lost its original server command')
        previous_command = matches[0]
        proof = _prepared_coordinator(store, {
            'execution_spec': spec, 'seal': seal, 'command': previous_command})
        owner = proof['owner']
        with store._connect() as db:
            if db.execute('SELECT 1 FROM delivery_commands WHERE command_id=? OR request_digest=?',
                          (previous_command['command_id'], request_digest)).fetchone():
                raise ValueError('unused runtime preparation command was already consumed')
        if owner == current_token and not released:
            return proof
        with shared.connect() as db:
            worker = db.execute('SELECT * FROM execution_workers WHERE issue_id=? AND worker_key=?',
                                (owner['issue_id'], worker_key(spec['run_id'], owner))).fetchone()
            stopped = db.execute('SELECT * FROM execution_checkpoints WHERE issue_id=? '
                                 'AND checkpoint_key=?',
                                 (owner['issue_id'],
                                  'stopped:' + str(owner['generation']))).fetchone()
            if (owner['generation'] >= current_token['generation']
                    or current_token['store_path'] != owner['store_path']
                    or not worker or worker['state'] != 'finished'
                    or worker['workstream_id'] != spec['feature_worker']['workstream_id']
                    or (json.loads(worker['receipt_json'] or 'null') or {}).get('cleanup')
                    != 'confirmed'
                    or not stopped or stopped['generation'] != owner['generation']
                    or stopped['content_digest'] != digest(json.loads(stopped['content_json']))
                    or db.execute("SELECT 1 FROM execution_workers WHERE issue_id=? "
                                  "AND generation=? AND state!='finished'",
                                  (owner['issue_id'], owner['generation'])).fetchone()
                    or db.execute("SELECT 1 FROM execution_effects WHERE issue_id=? "
                                  "AND generation=? "
                                  "AND state='pending'", (owner['issue_id'], owner['generation']))
                    .fetchone()):
                raise ValueError('unused runtime preparation still has predecessor custody')
        with store._connect() as db:
            previous_parent = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                         (owner['run_id'],)).fetchone()
            current_parent = store.effective_spec(current_token['run_id'])
            if (not previous_parent
                    or previous_parent['outcome'] not in {'delivered', 'blocked', 'cancelled'}
                    or previous_parent['work_id'] != current_parent['work_id']):
                raise ValueError('unused runtime preparation changed its canonical parent work')
            work_binding(store, current_parent, db)
        parent_closed = closed_coordinator(store, owner['run_id'])
        if json.loads(stopped['content_json']).get('state') != parent_closed['result']:
            raise ValueError('unused runtime preparation lost its stopped coordinator closure')
        return proof

    original_producer = producer(intent['command_digest'], released=True)
    require_worker_handoff(store, spec)
    with store._connect() as db:
        _preparation_parent_claim(store, current_token, db)
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
        admission = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                               (spec['run_id'],)).fetchone()
        queue = db.execute('SELECT state FROM delivery_outbox WHERE run_id=?',
                           (spec['run_id'],)).fetchone()
        if (not row or _sealed_row(row) != seal['row']
                or row['workflow_id'] != seal['closed']['workflow_id']
                or not admission or digest(json.loads(admission[0])) != seal['prior_gate_digest']
                or not queue or queue[0] != 'sent'):
            raise ValueError('unused runtime preparation was admitted, queued or changed')
        for table, key, expected in [('delivery_attempts', 'job_key', seal['attempts']),
                                     ('delivery_effects', 'effect_key', seal['effects'])]:
            if [dict(item) for item in db.execute(
                    f'SELECT * FROM {table} WHERE run_id=? ORDER BY {key}', (spec['run_id'],))] \
                    != expected:
                raise ValueError('unused runtime preparation lost its full ledger')
    binding = {'command_digest': command_digest, 'seal_digest': seal_digest,
               'coordinator': _prepared_coordinator(store, {
                   'execution_spec': spec, 'seal': seal, 'command': command}),
               'predecessor': {'intent_sha256': hashlib.sha256(raw).hexdigest(),
                               'command_digest': intent['command_digest'],
                               'seal_digest': intent['seal_digest'],
                               'coordinator': original_producer}}
    path = root / 'runtime-preparation' / 'successor' / 'intent.json'
    successor = read_private(path) if path.exists() or path.is_symlink() else {
        **binding, 'preparation_attempts': []}
    if successor.get('predecessor') != binding['predecessor']:
        raise ValueError('unused runtime preparation predecessor changed')
    if any(successor.get(key) != value for key, value in binding.items()):
        previous = producer(successor['command_digest'], released=False)
        if successor.get('coordinator') != previous:
            raise ValueError('unused runtime preparation successor coordinator changed')
        for index, attempt in enumerate(successor['preparation_attempts']):
            if (attempt.get('state') != 'stopped' or attempt.get('error')
                    or attempt.get('cleanup', {}).get('state') != 'confirmed'
                    or Path(attempt['spec']['state_dir'])
                    != path.parent / 'probes' / str(index) / spec['run_id']):
                raise ValueError('unused runtime preparation still has an unfinished attempt')
            _stopped_cleanup(attempt['spec'], read_only=True)
        _immutable(path.parent / ('previous-' + digest(successor) + '.json'), successor)
        successor = {**successor, **binding}
    if successor.get('effective_spec'):
        frozen_preparation(successor['effective_spec'])
        identity = native_identity(successor['effective_spec'])
        if pinned_identity(identity, spec) != pinned_identity(
                successor['effective_spec']['policy']['native_identity'], spec):
            _immutable(path.parent / ('prepared-' + digest(successor) + '.json'), successor)
            successor = {key: value for key, value in successor.items()
                         if key not in {'state', 'effective_spec'}}
    return path, successor


def prepare_runtime(spec, root, command_digest, seal_digest, *, handoff=None):
    """Bind the same locked executable to current source without changing feature authority."""
    if spec['provider'] == 'fake':
        return deepcopy(spec)
    source = Path(__file__).resolve().parents[3]
    if _git(source, 'status', '--porcelain', '--untracked-files=all'):
        raise ValueError('continuation requires clean installed runtime source')
    binary = Path(distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex'))
    if hashlib.sha256(binary.read_bytes()).hexdigest() != spec['policy']['codex_bin_sha256']:
        raise ValueError('continuation cannot change the frozen CLI executable')
    from .delivery_runtime_location import require_preparation_location

    require_preparation_location(spec, binary.resolve())
    execution = deepcopy(spec)
    execution['policy']['codex_bin'] = str(binary.resolve())
    execution['policy_digest'] = digest(execution['policy'])
    with _lock(root / 'runtime-preparation'):
        original_path = root / 'runtime-preparation' / 'intent.json'
        raw = read_private(original_path, raw=True) if original_path.exists() else None
        intent = json.loads(raw) if raw is not None else {
            'command_digest': command_digest, 'seal_digest': seal_digest,
            'preparation_attempts': []}
        path = original_path
        if intent['command_digest'] != command_digest or intent['seal_digest'] != seal_digest:
            if handoff is None:
                raise ValueError('pending runtime preparation belongs to changed authority')
            path, intent = _unused_runtime_preparation(
                spec, root, raw, handoff, command_digest, seal_digest)
        write_private(path, intent)
        result = _prepare(execution, load_execution_config(spec), intent, path)
        if path != original_path:
            # Native preparation yields to another coordinator. Its current token,
            # transferred claim, stopped row and full ledger must still be exact.
            _unused_runtime_preparation(spec, root, raw, handoff, command_digest, seal_digest)
            if read_private(original_path, raw=True) != raw:
                raise ValueError('unused runtime preparation predecessor changed '
                                 'during preparation')
        return result


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
