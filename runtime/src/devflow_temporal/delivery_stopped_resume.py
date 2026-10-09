"""Append-only, finite public resume of an authenticated stopped delivery."""
from __future__ import annotations

import json
import re
import stat
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_baseline_contract import repairable_baseline
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig, scope_amended_spec, scope_amendment_config
from .delivery_gate_retry import prepare_runtime
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _remote, _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock
from .delivery_repair import published_identity
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


def fixed_budget_allows(spec, iteration, iterations):
    """A resume may spend unused original turns, never enlarge the frozen ceiling."""
    maximum = spec.get('policy', {}).get('max_repairs')
    allowed = range(1, 101) if spec.get('feature_worker') else (1, 2)
    return (type(iteration) is int and iteration >= 0
            and type(iterations) is int and iterations in allowed
            and type(maximum) is int and maximum >= 0
            and iteration + iterations <= maximum)


def observed_native_cleanup(spec):
    """Preflight and readback observe closure without controlling any process."""
    return digest({'provider': 'fake'} if spec['provider'] == 'fake' else _stopped_cleanup(spec))


def namespace(recovery):
    return 'stopped-resumes/' + recovery['command_digest']


def custody(db, recovery):
    spec = recovery['execution_spec']
    if (spec.get('retry_budget_version') == 1
            and not fixed_budget_allows(spec, recovery['state']['iteration'],
                                        recovery['command']['additional_iterations'])):
        raise ValueError('a fixed repair budget cannot receive additional iterations')
    path = Path(spec['state_dir']) / namespace(recovery) / 'admission.json'
    command = db.execute('SELECT request_digest,response_json FROM delivery_commands '
                         'WHERE command_id=? AND run_id=?',
                         (recovery['command']['command_id'], spec['run_id'])).fetchone()
    if (not command or command[0] != recovery['command_digest']
            or json.loads(command[1]).get('admission_sha256') != digest(recovery)
            or canonical_json(read_private(path)) != canonical_json(recovery)
            or recovery['predecessor_result_digest'] != digest(recovery['state'])
            or recovery['maximum_iteration'] != (recovery['state']['iteration']
                                                 + recovery['command']['additional_iterations'])):
        raise ValueError('stopped resume lost its immutable command authority')
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
        return custody(db, recovery)


def _candidate(broker, state, attempts):
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
                latest.get('finish_reason') != 'prelaunch'):
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
    if changed - set(broker.spec['policy']['allowed_paths']):
        raise ValueError('stopped source escaped its frozen scope')
    _git(broker.checkout, 'merge-base', '--is-ancestor', broker.spec['base_sha'], candidate['head'])
    if (_git(broker.checkout, 'branch', '--show-current')
            != broker.spec.get('local_branch', broker.spec['branch'])
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin')
            != broker.spec['origin_url']):
        raise ValueError('stopped checkout branch or origin changed')
    return candidate, next(iter(sessions), None)


def snapshot(store, run_id):
    spec = store.effective_spec(run_id)
    if spec['provider'] != 'fake' and spec['policy'].get('host_sandbox') != 'trusted-local':
        raise ValueError('stopped resume preserves an already trusted execution policy')
    if digest(DeliveryConfig.load(Path(spec['config_path'])).raw) != spec['config_digest']:
        raise ValueError('stopped service configuration changed')
    if not store.owns_execution(spec):
        raise ValueError('stopped execution belongs to a different service')
    row, attempts, effects, claim = _rows(store, run_id)
    # Original admissions use the deterministic ID without a successor override.
    predecessor_id = row['workflow_id'] or 'delivery-' + run_id
    closed = store._completed_temporal_result(run_id, workflow_id=predecessor_id)
    state = closed['result']
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    stopped = ({'blocked', 'cancelled'} if spec.get('feature_worker') else {'blocked'})
    expected_execution = 'terminal' if row['outcome'] == 'cancelled' else 'blocked'
    if (row['phase'] not in stopped or row['outcome'] != row['phase']
            or row['execution_state'] != expected_execution or row['cleanup'] != 'confirmed'
            or state.get('run_id') != run_id or state.get('phase') != row['phase']
            or state.get('outcome') != row['outcome'] or state.get('cleanup') != 'confirmed'
            or state.get('execution_state') != expected_execution
            or closed.get('workflow_id') != predecessor_id
            or state.get('candidate') != json.loads(row['candidate_json'] or 'null')
            or state.get('checks') != json.loads(row['checks_json'] or '{}')
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
    broker = DeliveryBroker(store, spec)
    candidate, session = _candidate(broker, state, attempts)
    publication = state.get('pull_request')
    if publication:
        published_identity(broker, candidate, publication)
    else:
        if any(e['kind'] == 'publish' for e in effects):
            raise ValueError('unpublished resume has an unresolved publication')
        _unpublished_remote(broker)
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    return {'predecessor_spec': spec, 'row': row, 'attempts': attempts, 'effects': effects,
            'closed': closed, 'state': state, 'candidate': candidate, 'session_id': session,
            'publication': publication, 'original_recovery': previous,
            'cleanup_digest': observed_native_cleanup(spec), 'work_binding': binding}


def _unpublished_remote(broker):
    if broker.spec.get('feature_worker', {}).get('previous_publication'):
        from .delivery_feature_publication import verify_retained_publication

        verify_retained_publication(broker)
    else:
        _remote(broker)


def admit(store, run_id, command, *, preflight=False):
    original = store.submitted_spec(run_id)
    allowed = range(1, 101) if original.get('feature_worker') else (1, 2)
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
    seal = snapshot(store, run_id)
    if (command['expected_revision'] != seal['state']['revision']
            or command['expected_iteration'] != seal['state']['iteration']
            or command['expected_candidate_id'] != seal['candidate']['id']
            or command['expected_candidate_head'] != seal['candidate']['head']):
        raise ValueError('stopped resume checkpoint is stale')
    maximum = seal['state']['iteration'] + command['additional_iterations']
    if (original.get('retry_budget_version') == 1
            and not fixed_budget_allows(original, seal['state']['iteration'],
                                        command['additional_iterations'])):
        raise ValueError('a fixed repair budget cannot receive additional iterations')
    amended = scope_spec(store, seal, command)
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
        if digest(snapshot(store, run_id)) != digest(seal):
            raise ValueError('stopped checkpoint changed before admission')
        private_directory(root)
        execution = prepare_runtime(amended, root, command_digest, digest(seal))
        if digest(snapshot(store, run_id)) != digest(seal):
            raise ValueError('stopped checkpoint changed during runtime preparation')
        if scope_spec(store, seal, command) != amended:
            raise ValueError('stopped scope configuration changed during runtime preparation')
        execution['role_home_generation'] = spec.get('role_home_generation', '')
        if not execution['role_home_generation']:
            execution.pop('role_home_generation')
        candidate = {**seal['candidate'], 'policy_digest': execution['policy_digest']}
        retained = {k: v for k, v in seal.items() if k not in {'row', 'closed'}}
        retained['closed'] = {k: v for k, v in seal['closed'].items() if k != 'result'}
        recovery = {**retained, 'kind': KIND, 'command': command, 'command_digest': command_digest,
                    'execution_spec': execution, 'execution_candidate': candidate,
                    'maximum_iteration': maximum,
                    'predecessor_result_digest': digest(seal['state'])}
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
        if attempts != recovery['attempts'] or effects != recovery['effects']:
            raise ValueError('stopped receipt inventory changed before resume')
    predecessor = recovery['predecessor_spec']
    if (not store.owns_execution(spec)
            or digest(DeliveryConfig.load(Path(spec['config_path'])).raw) != spec['config_digest']):
        raise ValueError('stopped service configuration or owner changed')
    if observed_native_cleanup(predecessor) != recovery['cleanup_digest']:
        raise ValueError('stopped predecessor process ownership changed')
    broker = DeliveryBroker(store, spec)
    if broker.candidate() != recovery['execution_candidate']:
        raise ValueError('stopped source changed before resume')
    if recovery['publication']:
        published_identity(broker, recovery['execution_candidate'], recovery['publication'])
    else:
        _unpublished_remote(broker)
    return {'state': 'confirmed'}
