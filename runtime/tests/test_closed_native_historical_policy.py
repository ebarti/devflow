"""Historical native custody uses private fixtures and opaque actors, never a provider."""
from __future__ import annotations

import copy
from pathlib import Path

import pytest
from test_delivery_completed_resources import published_scope
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as native
from devflow_temporal import delivery_orphans as orphans
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_gate_retry import gate_namespace
from devflow_temporal.delivery_resources import _gate_roots, read_private, write_private
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def historical_role(service, monkeypatch):
    store, submitted = service
    store.submit(submitted)
    original = store.spec(submitted['run_id'])
    historical, historical_namespace = published_scope(store, original)
    state = Path(original['state_dir'])
    previous = read_private(state / historical_namespace / 'admission.json')
    current = copy.deepcopy(historical)
    current['policy']['roles']['review']['timeout_seconds'] = 20
    current['policy_digest'] = digest(current['policy'])
    current['gate_retry_generation'] = 2
    namespace = gate_namespace(2, 'published')
    admission = {'kind': 'published_gate_retry', 'execution_spec': current,
                 'original_recovery': previous}
    write_private(state / namespace / 'admission.json', admission)
    with store._connect() as db:
        db.execute('UPDATE delivery_gate_admissions SET recovery_json=?',
                   (canonical_json(admission),))
        db.execute('UPDATE delivery_runs SET recovery_json=?', (canonical_json(admission),))
    request = {'spec': historical, 'role': 'review', 'iteration': 0,
               'candidate': {'id': 'retained-candidate'}, 'resume_session': None,
               'workspace': str(state / historical_namespace / 'evidence/gates/0/review')}
    job = DeliverySupervisor._job_key(request)
    folder = state / 'attempts' / job
    path = folder / 'native-process.json'
    request.update(result_path=str(folder / 'result.json'), start_path=str(folder / 'start.json'))
    launch = {'argv': ['opaque-review'], 'environment': {'FIXTURE': 'synthetic'}}
    runtime = {'revision': 'retained-runtime', 'runtime_payload_sha256': digest('fixture')}
    process = {'cleanup': 'observed-native-confirmed', 'journal': str(path),
               'runtime_identity': runtime}
    result = {'status': 'pass', 'session_id': 'retained-session', 'cleanup': 'confirmed',
              'native_process': process}
    row = {'job_key': job, 'run_id': original['run_id'], 'role': 'review', 'iteration': 0,
           'candidate_id': request['candidate']['id'], 'state': 'finished', 'cleanup': 'confirmed',
           'pid': 17017, 'process_identity': 'opaque-provider', 'session_id': 'retained-session',
           'result_json': canonical_json(result), 'result_path': str(folder / 'result.json')}
    with store._connect() as db:
        db.execute('INSERT INTO delivery_attempts (' + ','.join(row) + ') VALUES ('
                   + ','.join('?' for _ in row) + ')', tuple(row.values()))
    journal = {'intent': {'run_id': original['run_id'],
                         'policy_digest': historical['policy_digest'], 'argv': launch['argv'],
                         'environment_sha256': digest(launch['environment']),
                         'cwd': request['workspace'], 'timeout': 10, 'ports': []},
               'phase': 'finished', 'monitoring_complete': True, 'ports': [],
               'owned': {'17017': {'identity': 'opaque-provider'}},
               'monitor': {'pid': 17019, 'identity': 'opaque-monitor'},
               'runtime_identity': runtime, 'result': process,
               'provider_session': {'result_digest': digest(result),
                                    'session_id': row['session_id'],
                                    'role': 'review', 'iteration': 0, 'resumed_from': None}}
    write_private(path, journal)
    write_private(folder / 'request.json', request)
    write_private(folder / 'launch.json', launch)
    journal['lifecycle_locks'] = {}
    for name in ('native-monitor.lock', 'native-process.lock'):
        lock_path = folder / name
        lock_path.touch(mode=0o600)
        info = lock_path.stat()
        journal['lifecycle_locks'][name] = {'device': info.st_dev, 'inode': info.st_ino,
                                          'uid': info.st_uid}
    write_private(path, journal)
    table, ports = {}, set()
    monkeypatch.setattr(orphans, 'process_table', lambda: table)
    monkeypatch.setattr(native, 'process_table', lambda: table)
    monkeypatch.setattr(orphans, 'listeners', lambda _port: ports)
    monkeypatch.setattr(native, 'listeners', lambda _port: ports)
    monkeypatch.setattr(native, 'stop_observed', lambda _owned: True)
    return store, current, path, request, journal, result, table, ports


@pytest.mark.parametrize('legacy', [False, True])
def test_original_policy_role_keeps_existing_observation(historical_role, legacy):
    store, current, path, request, journal, result, _table, _ports = historical_role
    executions = {}
    _gate_roots(current, execution_specs=executions)
    original = executions[Path(current['state_dir'])]
    request['spec'] = original
    old_job = path.parent.name
    job = 'legacy-monitor' if legacy else DeliverySupervisor._job_key(request)
    folder = path.parent.with_name(job)
    path.parent.rename(folder)
    path = folder / 'native-process.json'
    request.update(result_path=str(folder / 'result.json'), start_path=str(folder / 'start.json'))
    result['native_process']['journal'] = str(path)
    journal['intent']['policy_digest'] = original['policy_digest']
    journal['provider_session']['result_digest'] = digest(result)
    write_private(path, journal)
    if legacy:
        path.with_name('request.json').unlink()
        journal.pop('provider_session')
        write_private(path, journal)
        with store._connect() as db:
            db.execute('DELETE FROM delivery_attempts WHERE job_key=?', (old_job,))
    else:
        write_private(path.with_name('request.json'), request)
        with store._connect() as db:
            db.execute('UPDATE delivery_attempts SET job_key=?,result_path=?,result_json=? '
                       'WHERE job_key=?',
                       (job, request['result_path'], canonical_json(result), old_job))
    assert orphans._observe_registered_process(current, path)['cleanup'] \
        == 'observed-native-confirmed'


@pytest.mark.parametrize('changed', ['wrong_policy', 'symlink'])
def test_historical_policy_override_refuses_changed_custody(historical_role, monkeypatch, changed):
    store, current, path, request, journal, result, _table, _ports = historical_role
    updates = {}
    if changed == 'wrong_policy':
        journal['intent']['policy_digest'] = current['policy_digest']
    elif changed in {'original_policy', 'original_request', 'missing_request'}:
        executions = {}
        _gate_roots(current, execution_specs=executions)
        original = executions[Path(current['state_dir'])]
        journal['intent']['policy_digest'] = original['policy_digest']
        if changed == 'original_request':
            request['spec'] = original
    elif changed == 'forged_spec':
        request['spec']['accepted_plan'] += '\nForged scope.'
    elif changed == 'wrong_job':
        request['iteration'] += 1
    elif changed == 'forged_result':
        result['summary'] = 'forged durable result'
        updates['result_json'] = canonical_json(result)
    elif changed in {'session', 'candidate', 'process_identity', 'row_path'}:
        key = {'session': 'session_id', 'candidate': 'candidate_id',
               'row_path': 'result_path'}.get(changed, changed)
        updates[key] = 'foreign'
    elif changed == 'request_path':
        request['start_path'] = str(path.with_name('foreign-start.json'))
    elif changed == 'native_path':
        result['native_process']['journal'] = str(path.with_name('foreign-native.json'))
        updates['result_json'] = canonical_json(result)
        journal['provider_session']['result_digest'] = digest(result)
    elif changed == 'runtime':
        journal['runtime_identity'] = {'revision': 'foreign-runtime'}
    elif changed == 'missing_runtime':
        journal.pop('runtime_identity')
    elif changed == 'nonfinished':
        updates['state'] = 'unknown'
    elif changed == 'unconfirmed':
        updates['cleanup'] = 'unknown'
    elif changed == 'launch':
        journal['intent']['environment_sha256'] = digest('foreign environment')
    if updates:
        with store._connect() as db:
            db.execute('UPDATE delivery_attempts SET ' + ','.join(key + '=?' for key in updates),
                       tuple(updates.values()))
    if changed == 'missing_attempt':
        with store._connect() as db:
            db.execute('DELETE FROM delivery_attempts')
    write_private(path.with_name('request.json'), request)
    write_private(path, journal)
    if changed == 'missing_request':
        path.with_name('request.json').unlink()
    if changed == 'symlink':
        link = path.with_name('linked-native.json')
        link.symlink_to(path)
        path = link
    reconciled = []
    monkeypatch.setattr(orphans, 'reconcile_process', lambda p: reconciled.append(p))
    with pytest.raises((OSError, ValueError)):
        orphans._observe_registered_process(current, path)
    assert not reconciled
