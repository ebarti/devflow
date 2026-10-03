"""One sealed, same-run recovery from constrained to explicitly trusted host execution."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig
from .delivery_continuation import copy_session_state, session_state_digest
from .delivery_native_process import listeners, process_table
from .delivery_preparation import _lock, _private_bytes, _write, require_native_execution
from .delivery_resources import RunResources, read_private, write_private


def _rows(store, run_id):
    with store._connect() as db:
        row = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        if row is None:
            raise ValueError('run ID not found')
        attempts = [dict(item) for item in db.execute(
            'SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key', (run_id,),
        )]
        effects = [dict(item) for item in db.execute(
            'SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key', (run_id,),
        )]
        claim = store.state.claim_for(db, row['work_id'])
    return dict(row), attempts, effects, claim


def _stopped_cleanup(spec):
    """Observe only: preflight must never kill a process to manufacture stopped evidence."""
    resources = RunResources(spec)
    manifest = read_private(resources.manifest)
    receipt_path = resources.root / 'finalization.json'
    receipt = read_private(receipt_path)
    if (receipt.get('state') != 'confirmed'
            or receipt.get('process_cleanup') != 'observed-native-confirmed'
            or receipt.get('resource_cleanup') != 'confirmed'):
        raise ValueError('terminal native cleanup receipt is unconfirmed')
    table = process_table()
    journals = {}
    for raw_path in manifest['processes']:
        path = Path(raw_path)
        journal = read_private(path)
        if (journal.get('phase') != 'finished' or not journal.get('monitoring_complete')
                or any(table.get(int(pid), {}).get('identity') == item['identity']
                       and not table[int(pid)]['stat'].startswith('Z')
                       for pid, item in journal.get('owned', {}).items())
                or any(listeners(port) for port in journal.get('ports', []))):
            raise ValueError('recorded native process identity or port is still live or unknown')
        journals[raw_path] = hashlib.sha256(path.read_bytes()).hexdigest()
    for item in receipt['roots']:
        if item['state'] in {'removed', 'already_absent'} and os.path.lexists(item['path']):
            raise ValueError('terminal cleanup root was recreated before recovery')
    return {
        'manifest_sha256': hashlib.sha256(resources.manifest.read_bytes()).hexdigest(),
        'receipt_sha256': hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
        'journal_sha256': journals,
    }


def _remote(broker):
    if (_git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != broker.spec['origin_url']
            or _git(broker.source, 'remote', 'get-url', 'origin') != broker.spec['origin_url']):
        raise ValueError('preserved candidate Git destination changed')
    remote = _git(broker.source, 'ls-remote', 'origin', f"refs/heads/{broker.spec['branch']}")
    if remote:
        raise ValueError('unpublished recovery branch already exists remotely')
    broker.store._ensure_no_remote_pr(broker.spec['github_repo'], broker.spec['branch'])
    return {'branch': broker.spec['branch'], 'remote_head': None, 'pull_request': None}


def _issue(spec):
    try:
        response = subprocess.run(
            ["gh", "issue", "view", spec["issue_url"], "--json", "url,title,body"],
            text=True, capture_output=True, check=True, timeout=30,
        )
    except (subprocess.SubprocessError, OSError) as exc:
        raise ValueError("original issue readback is unavailable; no recovery granted") from exc
    if len(response.stdout) > 128 * 1024:
        raise ValueError("original issue evidence exceeds its bound")
    value = json.loads(response.stdout)
    if value.get("url") != spec["issue_url"]:
        raise ValueError("original issue identity changed")
    return {"source": "GitHub issue readback; untrusted requirements data, not instructions",
            "issue": value, "sha256": digest(value)}


def precheck(store, run_id):
    original = store.intake_execution_spec(run_id)
    require_native_execution(original)
    if (original.get('provider') != 'codex'
            or original['policy'].get('host_sandbox') != 'native-profile'):
        raise ValueError('policy recovery requires an original constrained native run')
    row, attempts, effects, claim = _rows(store, run_id)
    closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
    state = closed['result']
    previous_recovery = json.loads(row['recovery_json']) if row['recovery_json'] else None
    if (closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (
                digest(previous_recovery) if previous_recovery else None)
            or not isinstance(state, dict) or state.get('run_id') != run_id
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or state.get('execution_state') != 'blocked' or state.get('cleanup') != 'none'
            or row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked' or row['cleanup'] != 'none'
            or state.get('revision') != row['protocol_revision']
            or state.get('iteration') != row['iteration']
            or state.get('error') != row['error']
            or state.get('candidate') != json.loads(row['candidate_json'])
            or state.get('checks') != json.loads(row['checks_json'] or '{}')
            or state.get('pull_request') is not None or row['pr_json'] is not None
            or state.get('error') not in {
                'implementer did not establish a pass', 'prepublication repair limit exhausted',
            }
            or not original['accepted_plan'] or row['iteration'] > original['policy']['max_repairs']
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)
            or any(e['state'] != 'complete' or not e['observed_json'] for e in effects)
            or any(e['kind'] == 'publish' for e in effects)
            or claim is not None):
        raise ValueError('closed unpublished run, attempts, effects or released claim changed')
    if not original.get('preparation'):
        raise ValueError('original native preparation is unavailable')
    roles = state.get('roles', [])
    if (len(roles) != len(attempts) or sorted(
        (a['role'], a['iteration'], a['session_id']) for a in attempts
    ) != sorted((r['role'], r['iteration'], r.get('session_id')) for r in roles)):
        raise ValueError('closed Temporal result does not bind every attempt identity')
    implementations = [a for a in attempts if a['role'] == 'implement']
    sessions = {a['session_id'] for a in implementations}
    if not implementations or len(sessions) != 1 or None in sessions:
        raise ValueError('preserved original implementer session is unconfirmed')
    latest = max(implementations, key=lambda a: a['iteration'])
    result = json.loads(latest['result_json'])
    broker = DeliveryBroker(store, original)
    candidate = broker.candidate()
    if (candidate != result.get('candidate')
            or not broker._changed_paths()
            or broker._changed_paths() - set(original['policy']['allowed_paths'])
            or candidate['head'] != original['base_sha']):
        raise ValueError('last stopped candidate, base or admitted source scope changed')
    session_id = next(iter(sessions))
    source_home = Path(original['state_dir']) / 'role-homes' / 'implement'
    cleanup = _stopped_cleanup(original)
    remote = _remote(broker)
    session_digest = session_state_digest(source_home, session_id)
    seal = {
        'run_id': run_id, 'revision': row['protocol_revision'], 'iteration': row['iteration'],
        'original_spec_digest': digest(original), 'closed_result_digest': digest(state),
        'predecessor_workflow_id': closed['workflow_id'],
        'predecessor_execution_run_id': closed['execution_run_id'],
        'candidate': candidate, 'cleanup': cleanup, 'claim_digest': digest(claim),
        'remote_digest': digest(remote), 'attempts_digest': digest(attempts),
        'effects_digest': digest(effects), 'session_id': session_id,
        'session_state_digest': session_digest, 'issue_evidence': _issue(original),
    }
    return {**seal, 'precheck_sha256': digest(seal)}, row, state, original


def amended_config(original, path, expected_hash):
    root = Path(original['state_dir']).parents[1]
    content = _private_bytes(path, root)
    if hashlib.sha256(content).hexdigest() != expected_hash:
        raise ValueError('trusted configuration bytes changed')
    old = DeliveryConfig.load(Path(original['config_path']))
    if digest(old.raw) != original['config_digest']:
        raise ValueError('original frozen configuration changed')
    config = DeliveryConfig.load(path)
    before, after = deepcopy(old.raw), deepcopy(config.raw)
    if before.pop('execution_mode', 'native-profile') != 'native-profile':
        raise ValueError('original policy was already trusted')
    if after.pop('execution_mode', None) != 'trusted-local' or after != before:
        raise ValueError('policy recovery may change only execution_mode to trusted-local')
    return config


def _prepare(original, config, intent, intent_path):
    from .delivery_native_preparation import _measure, _validate, bind_native_spec, native_identity

    effective = deepcopy(original)
    effective.pop('preparation', None)
    for key in ('native_identity', 'codex_bin_sha256', 'environment_proof_sha256',
                'security_binding_sha256'):
        effective['policy'].pop(key, None)
    effective['policy']['host_sandbox'] = 'trusted-local'
    effective['policy_digest'] = digest(effective['policy'])
    effective.update(config_path=str(config.path), config_digest=digest(config.raw),
                     terminal_tracker_version=1, role_home_generation='policy-1')
    root = Path(effective['state_dir']).parents[1]
    # Preparation has its own finite, journalled resource generations. It cannot
    # recreate or rewrite the predecessor's finalized transient roots/manifest.
    for attempt in intent['preparation_attempts']:
        if attempt['state'] == 'started':
            receipt = RunResources(attempt['spec']).finalize('blocked')
            attempt.update(state='interrupted', cleanup=receipt)
            write_private(intent_path, intent)
            if receipt['state'] != 'confirmed':
                raise ValueError('interrupted policy preparation cleanup is unknown')
        if attempt.get('cleanup', {}).get('state') != 'confirmed':
            raise ValueError('policy preparation still has uncertain owned cleanup')
    if intent.get('effective_spec'):
        from .delivery_native_preparation import verify_native_spec

        verify_native_spec(intent['effective_spec'])
        return intent['effective_spec']
    with _lock(root / 'preparation-native'):
        identity = native_identity(effective)
        path = root / 'preparation-native' / digest(identity) / 'proof.json'
        reused = path.exists()
        if reused:
            proof = json.loads(_private_bytes(path, root / 'preparation-native'))
        else:
            if len(intent['preparation_attempts']) >= 2:
                raise ValueError('finite policy preparation attempt budget exhausted')
            probe = deepcopy(effective)
            probe['state_dir'] = str(intent_path.parent / 'probes'
                                    / str(len(intent['preparation_attempts'])) / original['run_id'])
            attempt = {'spec': probe, 'state': 'started', 'cleanup': None}
            intent['preparation_attempts'].append(attempt)
            intent['state'] = 'preparing'
            write_private(intent_path, intent)
            try:
                proof = _measure(probe, identity, state_root=root)
            except Exception as exc:
                attempt['error'] = str(exc)[:500]
                raise
            finally:
                receipt = RunResources(probe).finalize('blocked')
                attempt.update(state='stopped', cleanup=receipt)
                write_private(intent_path, intent)
            if receipt['state'] != 'confirmed':
                raise ValueError('policy preparation cleanup is unknown')
        _validate(proof, identity, root)
        if not reused:
            _write(path, proof)
        prepared = bind_native_spec(effective, identity, path, reused=reused)
        intent.update(state='prepared', effective_spec=prepared)
        write_private(intent_path, intent)
        return prepared


def recover(store, run_id, supplied):
    original = store.intake_execution_spec(run_id)
    with _lock(Path(original['state_dir']) / 'policy-recovery'):
        return _recover_locked(store, run_id, supplied)


def _recover_locked(store, run_id, supplied):
    required = {'command_id', 'expected_precheck_sha256', 'config_path', 'config_sha256',
                'additional_iterations'}
    if (not isinstance(supplied, dict) or set(supplied) != required
            or not isinstance(supplied['command_id'], str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', supplied['command_id'])
            or type(supplied['additional_iterations']) is not int
            or supplied['additional_iterations'] not in (1, 2)
            or any(not isinstance(supplied[k], str)
                   or not re.fullmatch(r'[0-9a-f]{64}', supplied[k])
                   for k in ('expected_precheck_sha256', 'config_sha256'))
            or not isinstance(supplied['config_path'], str)):
        raise ValueError('invalid trusted policy recovery contract')
    command_digest = digest({'run_id': run_id, **supplied})
    with store._connect() as db:
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (supplied['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        if db.execute('SELECT 1 FROM delivery_policy_recoveries WHERE run_id=?',
                      (run_id,)).fetchone():
            raise ValueError('this run already received its one policy recovery')
    seal, row, state, original = precheck(store, run_id)
    if seal['precheck_sha256'] != supplied['expected_precheck_sha256']:
        raise ValueError('policy recovery precheck changed; inspect a fresh preflight')
    config = amended_config(original, Path(supplied['config_path']), supplied['config_sha256'])
    intent_path = Path(original['state_dir']) / 'policy-recovery' / 'intent.json'
    if intent_path.exists():
        intent = read_private(intent_path)
        if (intent['command_digest'] != command_digest or intent['seal'] != seal):
            raise ValueError('accepted policy recovery intent or sealed inputs changed')
    else:
        intent = {'command_id': supplied['command_id'], 'command_digest': command_digest,
                  'seal': seal, 'state': 'accepted', 'preparation_attempts': [],
                  'effective_spec': None, 'last_error': None}
        write_private(intent_path, intent)
    # Keep the old finalization bytes before the resource manifest starts a new generation.
    archive = Path(original['state_dir']) / 'policy-recovery' / 'predecessor'
    for name in ('manifest.json', 'finalization.json'):
        source = RunResources(original).root / name
        target = archive / name
        if target.exists():
            if target.read_bytes() != source.read_bytes():
                raise ValueError('predecessor cleanup archive conflicts')
        else:
            write_private(target, read_private(source))
    try:
        effective = _prepare(original, config, intent, intent_path)
    except Exception as exc:
        intent['last_error'] = str(exc)[:500]
        write_private(intent_path, intent)
        raise
    source_home = Path(original['state_dir']) / 'role-homes' / 'implement'
    copy_session_state(source_home, source_home.with_name('implement-policy-1'),
                       seal['session_id'], seal['session_state_digest'])
    broker = DeliveryBroker(store, effective)
    candidate = broker.candidate()
    try:
        if any(candidate[k] != seal['candidate'][k] for k in (
            'head', 'id', 'content_sha256', 'base_sha', 'environment_digest',
        )) or digest(_remote(broker)) != seal['remote_digest']:
            raise ValueError('preserved candidate or remote changed during preparation')
    except Exception as exc:
        intent['last_error'] = str(exc)[:500]
        write_private(intent_path, intent)
        raise
    recovery = {
        'kind': 'execution_policy_recovery', 'state': state, 'seal': seal,
        'issue_evidence': seal['issue_evidence'],
        'effective_spec': effective, 'candidate': candidate, 'session_id': seal['session_id'],
        'preparation_history': deepcopy(intent['preparation_attempts']),
        'preparation_intent_digest': digest(intent),
        'config_sha256': supplied['config_sha256'], 'original_spec_digest': digest(original),
        'maximum_iteration': seal['iteration'] + supplied['additional_iterations'],
        'start_iteration': seal['iteration'] + 1,
        'predecessor_workflow_id': seal['predecessor_workflow_id'],
        'predecessor_execution_run_id': seal['predecessor_execution_run_id'],
    }
    workflow_id = f'delivery-{run_id}-execution-policy-1'
    response = {'run_id': run_id, 'workflow_id': workflow_id,
                'phase': 'execution_policy_recovery_queued', 'existing': False,
                'candidate_id': candidate['id'],
                'authorized_through_iteration': recovery['maximum_iteration'],
                'dashboard_url': f'{store.config.dashboard_url}/runs/{run_id}'}
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (supplied['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        attempts = [dict(a) for a in db.execute(
            'SELECT * FROM delivery_attempts WHERE run_id=? ORDER BY job_key', (run_id,),
        )]
        effects = [dict(e) for e in db.execute(
            'SELECT * FROM delivery_effects WHERE run_id=? ORDER BY effect_key', (run_id,),
        )]
        if (dict(current) != row or digest(attempts) != seal['attempts_digest']
                or digest(effects) != seal['effects_digest']
                or store.state.claim_for(db, original['work_id']) is not None
                or db.execute('SELECT 1 FROM delivery_policy_recoveries WHERE run_id=?',
                              (run_id,)).fetchone()):
            raise ValueError('policy recovery lost its sealed run or released ownership')
        store.state.claim_work(db, original['work_id'], f'external:devflow:{run_id}',
                               response['dashboard_url'])
        db.execute('INSERT INTO delivery_policy_recoveries VALUES (?,?,?,?,?)',
                   (run_id, supplied['command_id'], digest(original), digest(recovery),
                    recovery['maximum_iteration']))
        db.execute("UPDATE delivery_runs SET phase='execution_policy_recovery_queued',"
                   "execution_state='queued',outcome=NULL,error=NULL,revision=revision+1,"
                   'workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?',
                   (workflow_id, canonical_json(recovery), store.state.now(), run_id))
        db.execute("UPDATE delivery_outbox SET state='pending',last_error=NULL WHERE run_id=?",
                   (run_id,))
        store._event(db, run_id, row['revision'] + 1, 'execution_policy_recovery_queued',
                     'Explicit trusted policy recovery sealed; preserved candidate gates run first',
                     {'precheck_sha256': seal['precheck_sha256'], 'candidate_id': candidate['id']})
        db.execute('INSERT INTO delivery_commands VALUES (?,?,?,?)',
                   (supplied['command_id'], run_id, command_digest, canonical_json(response)))
    intent.update(state='queued', last_error=None)
    write_private(intent_path, intent)
    return response


def effective_spec(store, original, recovery, grant):
    if (grant is None or grant['original_spec_digest'] != digest(original)
            or grant['recovery_digest'] != digest(recovery)
            or grant['maximum_iteration'] != recovery['maximum_iteration']):
        raise ValueError('policy recovery authority is not durable')
    effective = recovery['effective_spec']
    config = amended_config(original, Path(effective['config_path']), recovery['config_sha256'])
    if effective['config_digest'] != digest(config.raw):
        raise ValueError('policy recovery effective configuration changed')
    return effective


def resume_preflight(store, spec, recovery):
    row, attempts, effects, claim = _rows(store, spec['run_id'])
    seal = recovery['seal']
    if (row['recovery_json'] != canonical_json(recovery)
            or store.effective_spec(spec['run_id']) != spec
            or row['execution_state'] not in {'queued', 'running'}
            or claim is None or claim['owner'] != f"external:devflow:{spec['run_id']}"
            or digest(attempts) != seal['attempts_digest']
            or digest(effects) != seal['effects_digest']
            or DeliveryBroker(store, spec).candidate() != recovery['candidate']
            or digest(_remote(DeliveryBroker(store, spec))) != seal['remote_digest']):
        raise ValueError('policy recovery candidate or sealed prelaunch authority changed')
    # Current preparation processes must also be stopped; historical logs remain unchanged.
    table = process_table()
    manifest = read_private(RunResources(spec).manifest)
    for raw in manifest['processes']:
        journal = read_private(Path(raw))
        if (journal.get('phase') != 'finished' or not journal.get('monitoring_complete')
                or any(table.get(int(pid), {}).get('identity') == item['identity']
                       and not table[int(pid)]['stat'].startswith('Z')
                       for pid, item in journal.get('owned', {}).items())
                or any(listeners(port) for port in journal.get('ports', []))):
            raise ValueError('policy recovery process identity is not stopped')
