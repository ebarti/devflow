"""Historical native custody uses private fixtures and opaque actors, never a provider."""
from __future__ import annotations

import copy
import fcntl
import os
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


def test_historical_finished_role_uses_exact_admitted_policy_without_changing_custody(
    historical_role,
):
    store, current, path, request, _journal, _result, _table, _ports = historical_role
    executions = {}
    _gate_roots(current, execution_specs=executions)
    assert request['spec'] in executions.values()
    assert request['spec'] != current
    original_policy = executions[Path(current['state_dir'])]['policy_digest']
    assert original_policy != request['spec']['policy_digest']
    before = {p: p.read_bytes() for p in path.parent.iterdir()}
    with store._connect() as db:
        attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
    receipt = orphans._observe_registered_process(current, path)
    assert receipt['cleanup'] == 'observed-native-confirmed'
    assert receipt['observed_pids'] == [17017, 17019]
    assert all(p.read_bytes() == data for p, data in before.items())
    with store._connect() as db:
        assert dict(db.execute('SELECT * FROM delivery_attempts').fetchone()) == attempt


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


@pytest.mark.parametrize('actor', ['monitor', 'owned'])
def test_historical_finished_role_still_refuses_live_actors(historical_role, actor):
    _store, current, path, _request, _journal, _result, table, _ports = historical_role
    pid, identity = (17019, 'opaque-monitor') if actor == 'monitor' else (17017, 'opaque-provider')
    table[pid] = {'identity': identity, 'stat': 'S'}
    with pytest.raises(ValueError, match='processes or ports remain active'):
        orphans._observe_registered_process(current, path)


@pytest.mark.parametrize('name', ['native-monitor.lock', 'native-process.lock'])
def test_original_policy_intent_cannot_skip_historical_original_lease(historical_role, name):
    _store, current, path, _request, journal, _result, _table, _ports = historical_role
    executions = {}
    _gate_roots(current, execution_specs=executions)
    journal['intent']['policy_digest'] = executions[Path(current['state_dir'])]['policy_digest']
    write_private(path, journal)
    descriptor = os.open(path.with_name(name), os.O_RDONLY)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises((OSError, ValueError)):
            orphans._observe_registered_process(current, path)
    finally:
        os.close(descriptor)


@pytest.mark.parametrize(
    "name,changed", [("native-monitor.lock", "held"), ("native-process.lock", "symlink")]
)
def test_historical_policy_override_refuses_unavailable_original_lease(
    historical_role,
    monkeypatch,
    name,
    changed,
):
    _store, current, path, _request, _journal, _result, _table, _ports = historical_role
    lock_path = path.with_name(name)
    descriptor = os.open(lock_path, os.O_RDONLY)
    try:
        if changed == 'held':
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        elif changed in {'replaced', 'symlink'}:
            original = lock_path.with_name(name + '.original')
            lock_path.rename(original)
            if changed == 'replaced':
                lock_path.touch(mode=0o600)
            else:
                lock_path.symlink_to(original)
        else:
            lock_path.chmod(0o644)
        reconciled = []
        monkeypatch.setattr(orphans, 'reconcile_process', lambda p: reconciled.append(p))
        with pytest.raises((OSError, ValueError)):
            orphans._observe_registered_process(current, path)
        assert not reconciled
    finally:
        os.close(descriptor)


def test_historical_original_leases_are_held_until_reconciliation_finishes(
    historical_role, monkeypatch,
):
    _store, current, path, _request, _journal, _result, _table, _ports = historical_role
    reconcile = orphans.reconcile_process

    def observed_under_leases(original):
        for name in ('native-monitor.lock', 'native-process.lock'):
            descriptor = os.open(original.with_name(name), os.O_RDONLY)
            try:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(descriptor)
        return reconcile(original)

    monkeypatch.setattr(orphans, 'reconcile_process', observed_under_leases)
    assert orphans._observe_registered_process(current, path)['cleanup'] \
        == 'observed-native-confirmed'
    for name in ('native-monitor.lock', 'native-process.lock'):
        descriptor = os.open(path.with_name(name), os.O_RDONLY)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)


@pytest.mark.parametrize('changed', ['request', 'journal'])
def test_historical_custody_is_rechecked_under_original_leases(
    historical_role, monkeypatch, changed,
):
    _store, current, path, request, journal, _result, _table, _ports = historical_role
    flock = fcntl.flock
    acquired = 0

    def change_after_acquiring(descriptor, operation):
        nonlocal acquired
        flock(descriptor, operation)
        acquired += 1
        if acquired == 2:
            if changed == 'request':
                request['workspace'] = 'foreign workspace'
                write_private(path.with_name('request.json'), request)
            else:
                journal['intent']['cwd'] = 'foreign workspace'
                write_private(path, journal)

    monkeypatch.setattr(orphans.fcntl, 'flock', change_after_acquiring)
    with pytest.raises(ValueError, match='custody changed under its leases'):
        orphans._observe_registered_process(current, path)
    assert acquired == 2
    for name in ('native-monitor.lock', 'native-process.lock'):
        descriptor = os.open(path.with_name(name), os.O_RDONLY)
        try:
            flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(descriptor)


@pytest.mark.parametrize('obstruction', [None, 'lock'])
def test_current_interrupted_command_keeps_original_reconciliation_and_fences(
    historical_role, obstruction,
):
    _store, current, _role_path, _request, _journal, _result, _table, ports = historical_role
    folder = (Path(current['state_dir']) / gate_namespace(2, 'published')
              / 'evidence/dependency-preparation/native/process')
    path = folder / 'native-process.json'
    launch = {'argv': ['opaque-check'], 'environment': {'FIXTURE': 'synthetic'}}
    journal = {'intent': {'run_id': current['run_id'], 'policy_digest': current['policy_digest'],
                          'argv': launch['argv'],
                          'environment_sha256': digest(launch['environment']),
                          'cwd': str(folder), 'timeout': 10, 'ports': [32400]},
               'phase': 'authorized', 'monitoring_complete': False, 'ports': [32400],
               'owned': {'17017': {'identity': 'opaque-command'}},
               'monitor': {'pid': 17019, 'identity': 'opaque-monitor'}}
    if obstruction == 'wrong_policy':
        journal['intent']['policy_digest'] = 'foreign'
    elif obstruction == 'port':
        ports.add(17017)
    write_private(path, journal)
    write_private(folder / 'launch.json', launch)
    for name in ('native-process.lock', 'native-monitor.lock'):
        (folder / name).touch(mode=0o600)
    descriptor = os.open(folder / 'native-monitor.lock', os.O_RDONLY)
    try:
        if obstruction == 'lock':
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if obstruction:
            with pytest.raises(ValueError):
                orphans._observe_registered_process(current, path)
            assert read_private(path) == journal
        else:
            receipt = orphans._observe_registered_process(current, path)
            assert receipt['cleanup'] == 'observed-native-confirmed'
            result = read_private(path)['result']
            assert result['provider_outcome'] == 'unknown' and result['exit_code'] is None
            assert result['monitoring_complete'] is False
            assert 'provider_session' not in read_private(path)
    finally:
        os.close(descriptor)
