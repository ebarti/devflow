"""Validate and resume execution-policy recoveries admitted by previous releases."""
from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig
from .delivery_native_process import listeners, process_table
from .delivery_preparation import _lock, _private_bytes, _write
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


def work_binding(store, spec, db):
    """Canonical source authority; status changes never authorize issue reassignment."""
    work = store.state.row(db, 'works', spec['work_id'])
    resource = store.state.issue_resource(spec['issue_url'])
    repository = 'github.com/' + spec['github_repo'].casefold()
    claim = store.state.claim_for(db, spec['work_id'])
    if (not work or store.state.issue_resource(work['issue']) != resource
            or work['repository'].rstrip('/').casefold() != repository
            or (claim is not None and (
                claim['resource'] != resource or claim['work_id'] != spec['work_id']
                or claim['owner'] != f"external:devflow:{spec['run_id']}"))):
        raise ValueError('work issue/repository or claim resource differs from frozen authority')
    return {'issue_resource': resource, 'repository': repository}


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
        identities = list(journal.get('owned', {}).items())
        monitor = journal.get('monitor')
        if monitor:
            identities.append((monitor['pid'], monitor))
        if (journal.get('phase') != 'finished' or journal.get('monitoring_complete') is not True
                or any(table.get(int(pid), {}).get('identity') == item['identity']
                       and not table[int(pid)]['stat'].startswith('Z')
                       for pid, item in identities)
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
                     terminal_tracker_version=1)
    if original['policy'].get('host_sandbox') != 'trusted-local':
        effective['role_home_generation'] = 'policy-1'
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
    with store._connect() as db:
        if work_binding(store, spec, db) != seal['work_binding']:
            raise ValueError('policy recovery frozen work authority changed')
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
