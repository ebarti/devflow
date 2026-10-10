"""Complete one failed controller publication on its already-pushed exact commit."""
from __future__ import annotations

import json
import re
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_config import DeliveryConfig
from .delivery_gate_retry import prepare_runtime
from .delivery_metadata_recovery import _immutable, preserve_resources
from .delivery_policy_recovery import _rows, _stopped_cleanup, work_binding
from .delivery_preparation import _lock
from .delivery_resources import read_private
from .delivery_source_scope import outside_scope

KIND = 'pending_publication_retry'


def snapshot(store, run_id):
    spec = store.effective_spec(run_id)
    if (spec['provider'] != 'fake' and (
            spec['policy'].get('execution_backend') != 'native-macos'
            or spec['policy'].get('host_sandbox') != 'trusted-local')):
        raise ValueError('pending publication requires its existing trusted native policy')
    row, attempts, effects, claim = _rows(store, run_id)
    previous = json.loads(row['recovery_json']) if row['recovery_json'] else None
    closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
    state = closed['result']
    candidate = state.get('candidate', {})
    precheck = state.get('checks', {}).get('prepublish', {})
    implementation = next((r for r in reversed(state.get('roles', []))
                           if r.get('role') == 'implement'), {})
    pending = [e for e in effects if e['state'] != 'complete' or not e['observed_json']]
    if (previous and previous.get('kind') == KIND
            or row['phase'] != 'blocked' or row['outcome'] != 'blocked'
            or row['execution_state'] != 'blocked' or row['cleanup'] != 'confirmed'
            or state.get('phase') != 'blocked'
            or row['error'] != state.get('error')
            or state.get('checks') != json.loads(row['checks_json'] or '{}')
            or state.get('error') != 'publication unresolved: ActivityError'
            or state.get('cleanup') != 'confirmed' or state.get('outcome') != 'blocked'
            or state.get('revision') != row['protocol_revision']
            or state.get('iteration') != row['iteration']
            or closed['request_digest'] != row['request_digest']
            or closed['recovery_digest'] != (digest(previous) if previous else None)
            or candidate != json.loads(row['candidate_json'] or 'null')
            or state.get('pull_request') is not None or json.loads(row['pr_json'] or 'null')
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
            or claim is not None or len(pending) != 1
            or pending[0]['effect_key'] != f"publish:{run_id}:{row['iteration']}"
            or pending[0]['kind'] != 'publish' or pending[0]['state'] != 'pending'
            or pending[0]['observed_json'] is not None
            or json.loads(pending[0]['request_json']) != {
                'iteration': row['iteration'], 'input_candidate_id': candidate.get('id')}):
        raise ValueError('pending publication lacks its closed passed-check checkpoint')
    broker = DeliveryBroker(store, spec)
    observed = broker.candidate()
    broker._validate_publication_commits()
    pr = broker._existing_pr()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    if (any(observed.get(k) != candidate.get(k) for k in (
            'content_sha256', 'base_sha', 'environment_digest', 'policy_digest'))
            or observed['head'] == candidate['head']
            or _git(broker.checkout, 'rev-parse', 'HEAD^') != candidate['head']
            or broker._changed_paths()
            or outside_scope(spec['policy'], broker._changed_paths(spec['base_sha']),
                             checkout=broker.checkout)
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or not remote or remote.split()[0] != observed['head']
            or (pr is not None and pr['headRefOid'] != observed['head'])):
        raise ValueError('pending publication source, remote head or scope changed')
    if digest(DeliveryConfig.load(Path(spec['config_path'])).raw) != spec['config_digest']:
        raise ValueError('pending publication frozen configuration changed')
    with store._connect() as db:
        binding = work_binding(store, spec, db)
    return {'row': row, 'closed': closed, 'spec': spec, 'previous': previous,
            'candidate': observed, 'pr': pr, 'work_binding': binding,
            'cleanup': _stopped_cleanup(spec)}


def admit(store, run_id, payload):
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
    seal = snapshot(store, run_id)
    if (payload['expected_revision'] != seal['row']['protocol_revision']
            or payload['expected_candidate_id'] != seal['closed']['result']['candidate']['id']
            or payload['expected_head'] != seal['candidate']['head']):
        raise ValueError('stale pending publication checkpoint')
    spec = seal['spec']
    root = Path(spec['state_dir']) / 'publication-retry'
    with _lock(root / 'controller.lock'):
        with store._connect() as db:
            prior = db.execute('SELECT * FROM delivery_commands WHERE command_id=?',
                               (payload['command_id'],)).fetchone()
        if prior:
            if prior['request_digest'] != command_digest:
                raise ValueError('command ID already belongs to different inputs')
            return json.loads(prior['response_json'])
        execution = prepare_runtime(spec, root, command_digest, digest(seal))
        if digest(snapshot(store, run_id)) != digest(seal):
            raise ValueError('pending publication changed during runtime preparation')
        recovery = {'kind': KIND, 'command': payload, 'seal': seal,
                    'original_spec': (seal['previous']['original_spec']
                                      if seal['previous'] else spec),
                    'original_recovery': seal['previous'], 'execution_spec': execution,
                    'state': seal['closed']['result']}
        _immutable(root / 'admission.json', recovery)
        preserve_resources(root, spec)
        workflow_id = f'delivery-{run_id}-publication-retry-1'
        response = {'run_id': run_id, 'phase': 'publication_recovery_queued',
                    'workflow_id': workflow_id, 'implementation_authority': False}
        with store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
            if (dict(current) != seal['row']
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
    path = Path(spec['state_dir']) / 'publication-retry/admission.json'
    if not saved or json.loads(saved[0]) != recovery or read_private(path) != recovery:
        raise ValueError('pending publication admission custody changed')


def readback(store, spec, recovery):
    effective_spec(store, recovery['original_spec'], recovery)
    broker = DeliveryBroker(store, spec)
    observed = broker.candidate()
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
    if (spec != recovery['execution_spec'] or not remote
            or remote.split()[0] != observed['head']
            or _git(broker.checkout, 'branch', '--show-current') != spec['branch']
            or _git(broker.checkout, 'remote', 'get-url', '--push', 'origin') != spec['origin_url']
            or any(observed[k] != recovery['seal']['candidate'][k] for k in (
            'id', 'head', 'base_sha', 'content_sha256', 'environment_digest'))):
        raise ValueError('pending publication source changed after admission')
    if spec['provider'] != 'fake':
        from .delivery_native_preparation import verify_native_spec
        verify_native_spec(spec)
    return recovery['seal']['candidate']
