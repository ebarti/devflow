"""Complete one failed controller publication on its already-pushed exact commit."""
from __future__ import annotations

import json
import re
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_execution_config import load_execution_config
from .delivery_gate_retry import _retained_recovery, _sealed_row, prepare_runtime
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock
from .delivery_publication import stopped_at_publication
from .delivery_resources import read_private
from .delivery_source_scope import outside_scope

KIND = 'pending_publication_retry'
# Same-run completions of one stopped publication while its candidate is unchanged.
PUBLICATION_RETRIES = 3


def _chain(recovery):
    while recovery:
        yield recovery
        recovery = recovery.get('original_recovery')


def _refuse(code, fact, action, proof, *, message=None):
    from .delivery_lifecycle import RecoveryRefused, require_transition

    try:
        require_transition({
            'operation': 'recover_publication', 'owner': 'pending_publication',
            'owner_proof': True, 'normalized_lifecycle': 'stopped', 'proof_digest': digest(proof),
            'comparisons': [], 'predicates': [{
                'code': code, 'satisfied': False, 'missing_or_unknown_fact': fact,
                'next_action': action,
            }],
        }, {})
    except RecoveryRefused as exc:
        if message is not None:
            exc.args = (message,)
        raise


def snapshot(store, run_id, *, observation=False):
    spec = store.effective_spec(run_id)
    if (spec['provider'] != 'fake' and (
            spec['policy'].get('execution_backend') != 'native-macos'
            or spec['policy'].get('host_sandbox') != 'trusted-local')):
        raise ValueError('pending publication requires its existing trusted native policy')
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    closed = store._completed_temporal_result(
        run_id, workflow_id=row['workflow_id'], **({'preserve': False} if observation else {}))
    state = closed['result']
    candidate = state.get('candidate', {})
    published = state.get('pull_request')
    precheck = state.get('checks', {}).get('prepublish', {})
    implementation = next((r for r in reversed(state.get('roles', []))
                           if r.get('role') == 'implement'), {})
    pending = [e for e in effects if e['state'] != 'complete' or not e['observed_json']]
    # The stopped iteration's publish effect: absent when a confirmed pre-push
    # failure discarded it, pending once a push or PR creation was attempted.
    effect = pending[0] if len(pending) == 1 else None
    bound = json.loads(effect['observed_json'] or 'null') if effect else None
    settling_unknown = row['cleanup'] == state.get('cleanup') == 'unknown'
    from .delivery_lifecycle import require_transition

    allowed_claim = claim is None or (settling_unknown and claim['owner'] ==
                                     'external:devflow:' + run_id)
    valid_effect = (len(pending) <= 1 and (effect is None or (
        effect['effect_key'] == f"publish:{run_id}:{row['iteration']}"
        and effect['kind'] == 'publish' and effect['state'] == 'pending'
        and json.loads(effect['request_json']) == {
            'iteration': row['iteration'], 'input_candidate_id': candidate.get('id')})))
    predicates = [
        {'code': 'publication_retry_budget_exhausted',
         'satisfied': sum(1 for r in _chain(previous) if r.get('kind') == KIND)
                      < PUBLICATION_RETRIES,
         'missing_or_unknown_fact': 'remaining publication continuation allowance',
         'next_action': 'inspect retained publication recovery history'},
        {'code': 'publication_owner_conflict', 'satisfied': allowed_claim,
         'missing_or_unknown_fact': 'released claim or exact original publication owner',
         'next_action': 'reconcile current publication ownership'},
        {'code': 'publication_cleanup_unknown',
         'satisfied': row['cleanup'] in {'confirmed', 'unknown'}
                      and state.get('cleanup') == row['cleanup'],
         'missing_or_unknown_fact': 'matching closed cleanup observation',
         'next_action': 'reconcile original cleanup evidence'},
        {'code': 'publication_attempts_unsettled',
         'satisfied': all(a['state'] == 'finished' and a['cleanup'] == 'confirmed'
                          for a in attempts),
         'missing_or_unknown_fact': 'finished attempts and confirmed native cleanup',
         'next_action': 'reconcile original native attempt custody'},
        {'code': 'publication_effect_unresolved', 'satisfied': valid_effect,
         'missing_or_unknown_fact': 'single bound original publication effect',
         'next_action': 'reconcile the original publication effect'},
        {'code': 'publication_checks_unproven',
         'satisfied': precheck.get('state') == 'passed'
                      and precheck.get('source_unchanged') is True
                      and precheck.get('candidate_id') == candidate.get('id')
                      and bool(precheck.get('results'))
                      and all(r.get('passed') is True and r.get('cleanup') == 'confirmed'
                              for r in precheck.get('results', [])),
         'missing_or_unknown_fact': 'passed checks bound to the original candidate',
         'next_action': 'inspect original prepublication check evidence'},
    ]
    require_transition({
        'operation': 'recover_publication', 'owner': 'pending_publication', 'owner_proof': True,
        'normalized_lifecycle': 'stopped', 'proof_digest': digest({'row': row, 'closed': closed}),
        'comparisons': [], 'predicates': predicates,
    }, {})
    if (row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked'
            or row['cleanup'] not in {'confirmed', 'unknown'}
            or state.get('phase') != 'blocked'
            or row['error'] != state.get('error')
            or state.get('checks') != json.loads(row['checks_json'] or '{}')
            or not stopped_at_publication(state)
            or state.get('cleanup') != row['cleanup'] or state.get('outcome') != 'blocked'
            or state.get('revision') != row['protocol_revision']
            or state.get('iteration') != row['iteration']
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or candidate != json.loads(row['candidate_json'] or 'null')
            or published != json.loads(row['pr_json'] or 'null')
            # A repair iteration starts from the head its PR already shows.
            or (published is not None and (not isinstance(published, dict)
                                           or published.get('head') != candidate.get('head')))
            or precheck.get('state') != 'passed' or precheck.get('source_unchanged') is not True
            or precheck.get('candidate_id') != candidate.get('id')
            or not precheck.get('results')
            or any(r.get('passed') is not True or r.get('cleanup') != 'confirmed'
                   for r in precheck['results'])
            or implementation.get('status') != 'pass'
            or implementation.get('iteration') != row['iteration']
            or any(implementation.get('candidate', {}).get(k) != candidate.get(k)
                   for k in ('id', 'head', 'base_sha', 'content_sha256', 'environment_digest'))
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)
            or (claim is not None and not (settling_unknown and claim['owner'] ==
                                          'external:devflow:' + run_id))
            or len(pending) > 1
            or (effect is not None and (
                effect['effect_key'] != f"publish:{run_id}:{row['iteration']}"
                or effect['kind'] != 'publish' or effect['state'] != 'pending'
                or json.loads(effect['request_json']) != {
                    'iteration': row['iteration'], 'input_candidate_id': candidate.get('id')}))):
        _refuse('publication_checkpoint_unproven',
                'matching closed publication checkpoint and implementation evidence',
                'inspect the original closed publication evidence', {'row': row, 'closed': closed})
    broker = DeliveryBroker(store, spec)
    observed = broker.candidate()
    broker._validate_publication_commits()
    pr = broker._existing_pr()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    remote_head = remote.split()[0] if remote else None
    committed = observed['head'] != candidate['head']
    previous_head = published['head'] if published else None
    pushed = committed and remote_head == observed['head']
    if (any(observed.get(k) != candidate.get(k) for k in (
            'content_sha256', 'base_sha', 'environment_digest', 'policy_digest'))
            # Exactly the checked implementation: still uncommitted, or committed
            # once on top of the head it was checked at.
            or (committed and (_git(broker.checkout, 'rev-parse', 'HEAD^') != candidate['head']
                               or broker._changed_paths()))
            or (not committed and observed['id'] != candidate['id'])
            or outside_scope(spec['policy'], broker._changed_paths(spec['base_sha']),
                             checkout=broker.checkout)
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            # origin holds the pushed head, or still the head this iteration started from.
            or not (pushed or remote_head == previous_head)
            # A bound effect names the pushed head and the PR it saw, and a push it
            # confirmed must still be on origin.
            or (bound is not None and (
                bound.get('head') != observed['head']
                or bound.get('number') not in {None, pr['number'] if pr else None}
                or (bound.get('remote_confirmed') and not pushed)))
            # The owned PR is the one this run published, at the head it had or the
            # pushed one; a first publication may have been created by a lost attempt.
            or (published is None and pr is not None and pr['headRefOid'] != observed['head'])
            or (published is not None and (
                pr is None or pr['number'] != published['number']
                or pr['headRefOid'] not in {previous_head, observed['head']}))):
        _refuse('publication_source_or_remote_changed',
                'unchanged source, authorized scope and exact remote publication',
                'inspect candidate and remote publication evidence',
                {'row': row, 'candidate': observed, 'remote_head': remote_head, 'pr': pr})
    load_execution_config(spec)
    settlement = None
    if settling_unknown:
        # A later exact observation may settle an unknown publication stop. It
        # grants no new mutation and never rewrites its historical provider or
        # cleanup result. The executor lease and all native actors must be idle.
        if (spec.get('resource_cleanup_version') != 1 or effect is None or pr is None
                or not pushed or pr['headRefOid'] != observed['head']):
            _refuse('publication_exact_readback_unknown',
                    'exact original completed pull request and resource custody',
                    'reconcile the original publication and native custody',
                    {'row': row, 'candidate': observed, 'pr': pr},
                    message='unknown publication requires its exact completed PR readback')
        try:
            if observation:
                publication = broker.observe_publish(
                    row['iteration'], candidate, expected_head=observed['head'],
                    expected_pr_number=pr['number'])
            else:
                publication = broker.reconcile_publish(
                    row['iteration'], candidate, expected_head=observed['head'],
                    expected_pr_number=pr['number'], complete=False,
                )
        except (ValueError, OSError) as exc:
            from .delivery_lifecycle import RecoveryRefused

            if isinstance(exc, RecoveryRefused):
                raise
            _refuse('publication_executor_custody_unknown',
                    'authenticated idle publisher and bound remote readback',
                    'inspect the publication executor and remote evidence',
                    {'row': row, 'candidate': observed, 'pr': pr})
        if publication.get('state') == 'pending':
            _refuse('publication_executor_or_effect_unknown',
                    'inactive original publisher and exact remote outcome',
                    'wait for the original publisher and reconcile its remote result',
                    {'row': row, 'candidate': observed, 'publication': publication},
                    message='original publication executor or remote outcome remains unknown')
        from .delivery_resources import observe_completed_resources

        settlement = {'publication': publication,
                      'native_cleanup': observe_completed_resources(spec, attempts)}
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    return {'row': _sealed_row(row), 'closed': closed, 'spec': spec,
            'previous_digest': digest(previous) if previous else None,
            'candidate': observed, 'pr': pr, 'published': published, 'pushed': pushed,
            'remote_head': remote_head, 'work_binding': binding,
            'cleanup': (settlement['native_cleanup'] if settlement else
                        _stopped_cleanup(spec, read_only=True) if observation
                        else _stopped_cleanup(spec)),
            **({'publication_settlement': settlement} if settlement else {})}

def admit(store, run_id, payload, *, preflight=False):
    fields = {'command_id', 'expected_revision', 'expected_candidate_id',
              'expected_head', 'expected_pr_number'}
    if (set(payload) != fields or type(payload['expected_pr_number']) is not int
            or payload['expected_pr_number'] != 0
            or type(payload['expected_revision']) is not int or payload['expected_revision'] < 1
            or not isinstance(payload['command_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', payload['command_id'])
            or any(not isinstance(payload[k], str) or not re.fullmatch(
                r'[0-9a-f]{' + str(n) + '}', payload[k])
                for k, n in (('expected_candidate_id', 64), ('expected_head', 40)))):
        raise ValueError('invalid pending publication retry identity')
    command_digest = digest({'run_id': run_id, **payload})
    with store._connect() as db:
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (payload['command_id'],)).fetchone()
    if prior:
        if prior['request_digest'] != command_digest:
            raise ValueError('command ID already belongs to different inputs')
        return json.loads(prior['response_json'])
    seal = snapshot(store, run_id, observation=preflight)
    previous = _retained_recovery(store, run_id, seal)
    from .delivery_lifecycle import require_same_proof, require_transition

    assessment = require_transition({
        'operation': 'recover_publication', 'owner': 'pending_publication', 'owner_proof': True,
        'normalized_lifecycle': 'stopped', 'proof_digest': digest(seal),
        'revision': seal['row']['protocol_revision'],
        'candidate_id': seal['closed']['result']['candidate']['id'],
        'candidate_head': seal['candidate']['head'],
        'comparisons': [('revision', 'expected_revision', 'revision_changed'),
                        ('candidate_id', 'expected_candidate_id', 'candidate_changed'),
                        ('candidate_head', 'expected_head', 'candidate_head_changed')],
    }, payload)
    if preflight:
        return {'run_id': run_id, 'preflight': True, 'lifecycle_assessment': assessment}
    spec = seal['spec']
    generation = 1 + sum(1 for r in _chain(previous) if r.get('kind') == KIND)
    namespace = 'publication-retry' + (f'-{generation}' if generation > 1 else '')
    root = Path(spec['state_dir']) / namespace
    with _lock(root / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                               (payload['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        execution = prepare_runtime(spec, root, command_digest, digest(seal))
        require_same_proof('recover_publication', 'pending_publication',
                           digest(snapshot(store, run_id)), digest(seal))
        recovery = {'kind': KIND, 'command': payload, 'seal': seal,
                    'original_spec': (previous['original_spec'] if previous else spec),
                    'original_recovery': previous, 'execution_spec': execution,
                    'state': seal['closed']['result'], 'namespace': namespace}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        workflow_id = f'delivery-{run_id}-publication-retry-{generation}'
        response = {'run_id': run_id, 'phase': 'publication_recovery_queued',
                    'workflow_id': workflow_id, 'implementation_authority': False}
        with store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
            if (_sealed_row(dict(current)) != seal['row']
                    or work_binding(store, spec, db) != seal['work_binding']):
                raise ValueError('pending publication lost its stopped ownership')
            store.state.claim_work(db, spec['work_id'], f'external:devflow:{run_id}',
                                   store.config.dashboard_url)
            db.execute("UPDATE delivery_runs SET phase='publication_recovery_queued',"
                       "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1,"
                       "workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?",
                       (workflow_id, canonical_json(recovery), store.state.now(), run_id))
            db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=? "
                       'WHERE run_id=?', (store.state.now(), run_id))
            db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                       (payload['command_id'], run_id, command_digest, canonical_json(response)))
            store._event(db, run_id, current['revision'] + 1, 'publication_recovery_queued',
                         'Completing the pending publication; no implementation turn', response)
        return response


def effective_spec(store, original, recovery):
    with store._connect() as db:
        saved = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                           (original['run_id'],)).fetchone()
        custody(db, recovery)
    if (not saved or json.loads(saved[0]) != recovery or recovery['original_spec'] != original):
        raise ValueError('pending publication lost its admitted runtime authority')
    return recovery['execution_spec']


def custody(db, recovery):
    spec = recovery['execution_spec']
    saved = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                       (spec['run_id'],)).fetchone()
    path = (Path(spec['state_dir']) / recovery.get('namespace', 'publication-retry')
            / 'admission.json')
    if not saved or json.loads(saved[0]) != recovery or read_private(path) != recovery:
        raise ValueError('pending publication admission custody changed')


def readback(store, spec, recovery):
    effective_spec(store, recovery['original_spec'], recovery)
    broker = DeliveryBroker(store, spec)
    observed = broker.candidate()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    remote_head = remote.split()[0] if remote else None
    seal = recovery['seal']
    # origin is unchanged since admission, or carries the sealed head meanwhile.
    expected_remote = {observed['head']} | (
        {seal['remote_head']} if 'remote_head' in seal else set())
    if (spec != recovery['execution_spec'] or remote_head not in expected_remote
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or any(observed[k] != seal['candidate'][k] for k in (
            'id', 'head', 'base_sha', 'content_sha256', 'environment_digest'))):
        raise ValueError('pending publication source changed after admission')
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        verify_native_spec(spec)
    settlement = seal.get('publication_settlement')
    if settlement:
        from .delivery_resources import observe_completed_resources

        _row, attempts, _effects, _claim = _rows(store, spec['run_id'])
        publication = broker.reconcile_publish(
            recovery['state']['iteration'], recovery['state']['candidate'],
            expected_head=seal['candidate']['head'],
            expected_pr_number=settlement['publication']['number'], complete=False,
        )
        if (publication != settlement['publication']
                or observe_completed_resources(spec, attempts) != settlement['native_cleanup']):
            raise ValueError('settled publication or native actor custody changed after admission')
    return seal['candidate']
