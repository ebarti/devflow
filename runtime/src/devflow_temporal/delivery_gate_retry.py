"""One explicit fresh assessment of an unchanged, stopped published candidate."""
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


def snapshot(store, run_id):
    spec = store.effective_spec(run_id)
    row, attempts, effects, claim = _rows(store, run_id)
    closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
    state = closed['result']
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    broker = DeliveryBroker(store, spec)
    candidate = broker.candidate()
    pr = broker._existing_pr()
    roles = [r for r in state.get('roles', []) if r.get('iteration') == row['iteration']]
    implementation = next((r for r in roles if r.get('role') == 'implement'), None)
    failed = [r for r in roles if r.get('role') in {'review', 'verify'}
              and r.get('status') != 'pass' and r.get('findings')]
    if (spec['provider'] != 'fake' and spec['policy'].get('host_sandbox') != 'trusted-local'):
        raise ValueError('gate retry requires the existing trusted native execution policy')
    if (previous is not None or row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked' or row['cleanup'] != 'confirmed'
            or state.get('phase') != 'blocked' or state.get('outcome') != 'blocked'
            or state.get('cleanup') != 'confirmed' or state.get('error') != 'repair limit exhausted'
            or row['protocol_revision'] != state.get('revision')
            or row['iteration'] != state.get('iteration')
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] is not None
            or not implementation or implementation.get('status') != 'pass' or not failed
            or any(a['state'] != 'finished' or a['cleanup'] != 'confirmed' for a in attempts)
            or any(e['state'] != 'complete' or not e['observed_json'] for e in effects)
            or claim is not None):
        raise ValueError('gate retry requires a closed, finalized failed gate and released claim')
    frozen = json.loads(row['candidate_json'])
    publication = json.loads(row['pr_json'])
    if (any(candidate.get(k) != frozen.get(k) for k in candidate)
            or frozen != state.get('candidate') or publication != state.get('pull_request')
            or not pr or pr['state'] != 'OPEN' or pr['isDraft']
            or pr['number'] != publication['number'] or pr['headRefOid'] != candidate['head']
            or _git(broker.checkout, 'status', '--porcelain', '--untracked-files=no')
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or _git(broker.source, 'remote', 'get-url', 'origin') != spec['origin_url']
            or _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch']).split()[0]
            != candidate['head']
            or not broker._changed_paths() <= set(spec['policy']['allowed_paths'])):
        raise ValueError('gate retry lost unchanged owned source or exact published PR head')
    config = DeliveryConfig.load(Path(spec['config_path']))
    if digest(config.raw) != spec['config_digest']:
        raise ValueError('gate retry frozen configuration changed')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
        if db.execute('SELECT 1 FROM delivery_gate_admissions WHERE run_id=?',
                      (run_id,)).fetchone():
            raise ValueError('this run already received its one gate assessment retry')
    return {'row': row, 'closed': closed, 'original_spec': spec, 'attempts': attempts,
            'effects': effects, 'candidate': candidate, 'publication': publication,
            'cleanup': _stopped_cleanup(spec), 'work_binding': binding}


def admit(store, run_id, payload, *, preflight=False):
    fields = {'continuation_kind', 'command_id', 'expected_revision', 'expected_iteration',
              'expected_candidate_id', 'expected_pr_number', 'expected_pr_head',
              'additional_iterations'}
    if (not isinstance(payload, dict) or set(payload) != fields
            or payload.get('continuation_kind') != KIND
            or not isinstance(payload.get('command_id'), str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:-]{0,127}', payload['command_id'])
            or any(type(payload.get(k)) is not int or payload[k] < low for k, low in (
                ('expected_revision', 1), ('expected_iteration', 0), ('expected_pr_number', 1)))
            or type(payload.get('additional_iterations')) is not int
            or payload['additional_iterations'] != 0
            or any(not isinstance(payload.get(k), str)
                   or not re.fullmatch(r'[0-9a-f]{' + str(n) + '}', payload[k])
                   for k, n in (('expected_candidate_id', 64), ('expected_pr_head', 40)))):
        raise ValueError('published gate retry requires an exact zero-repair request')
    command_digest = digest({'run_id': run_id, **payload})
    with store._connect() as db:
        prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                           (payload['command_id'],)).fetchone()
    if prior:
        if prior['request_digest'] != command_digest:
            raise ValueError('command ID already belongs to different inputs')
        return json.loads(prior['response_json'])
    seal = snapshot(store, run_id)
    if (payload['expected_revision'] != seal['row']['protocol_revision']
            or payload['expected_iteration'] != seal['row']['iteration']
            or payload['expected_candidate_id'] != seal['candidate']['id']
            or payload['expected_pr_head'] != seal['candidate']['head']
            or payload['expected_pr_number'] != seal['publication']['number']):
        raise ValueError('stale published gate retry checkpoint')
    if preflight:
        return {'run_id': run_id, 'preflight': True, 'implementation_authority': False,
                'additional_iterations': 0, 'candidate_id': seal['candidate']['id']}
    spec = seal['original_spec']
    root = Path(spec['state_dir']) / 'gates-admission'
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
        intent_path = root / 'retry-preparation.json'
        intent = read_private(intent_path) if intent_path.exists() else {
            'command_digest': command_digest, 'seal_digest': digest(seal),
            'preparation_attempts': []}
        if intent['command_digest'] != command_digest or intent['seal_digest'] != digest(seal):
            raise ValueError('pending gate retry preparation belongs to changed authority')
        write_private(intent_path, intent)
        execution = deepcopy(spec)
        if spec['provider'] != 'fake':
            # Relocate the identical locked CLI; keep its bytes and version.
            binary = Path(distribution('openai-codex-cli-bin').locate_file(
                'codex_cli_bin/bin/codex'))
            if (hashlib.sha256(binary.read_bytes()).hexdigest()
                    != spec['policy']['codex_bin_sha256']):
                raise ValueError('gate retry cannot change the frozen CLI executable')
            execution['policy']['codex_bin'] = str(binary.resolve())
            execution = _prepare(execution, DeliveryConfig.load(Path(spec['config_path'])),
                                 intent, intent_path)
        execution['role_home_generation'] = 'gate-retry-1'
        execution['gate_retry_generation'] = 1
        if digest(snapshot(store, run_id)) != digest(seal):
            raise ValueError('stopped gate checkpoint changed during runtime preparation')
        after = DeliveryBroker(store, execution).candidate()
        if any(after[k] != seal['candidate'][k]
               for k in ('id', 'head', 'base_sha', 'content_sha256', 'environment_digest')):
            raise ValueError('gate retry changed feature source')
        recovery = {'kind': KIND, 'command': payload, 'seal': seal,
                    'original_recovery': None, 'original_spec': spec, 'execution_spec': execution,
                    'state': seal['closed']['result'], 'candidate': after,
                    'publication': {**seal['publication'], 'candidate': after}}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        workflow_id = f'delivery-{run_id}-gates-retry-1'
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
            store.state.claim_work(db, spec['work_id'], f'external:devflow:{run_id}',
                                   store.config.dashboard_url)
            db.execute('INSERT INTO delivery_gate_admissions VALUES (?,?,?)',
                       (run_id, payload['command_id'], canonical_json(recovery)))
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
                         'Fresh gates on the existing PR; no implementation or repair grant',
                         response)
        return response


def effective_spec(store, original, recovery):
    root = Path(original['state_dir']) / 'gates-admission'
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
    pr = broker._existing_pr()
    if (not pr or pr['number'] != recovery['publication']['number'] or pr['state'] != 'OPEN'
            or pr['headRefOid'] != recovery['candidate']['head']):
        raise ValueError('gate retry exact published PR head changed')
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        verify_native_spec(spec)
    return recovery['publication']
