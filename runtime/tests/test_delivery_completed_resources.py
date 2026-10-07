"""Read-only controller contracts with opaque process/port doubles; no native launches."""
from __future__ import annotations

import copy
import json

import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as processes
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    observe_finalized_resources,
    write_private,
)


@pytest.fixture
def completed(service, monkeypatch):
    store, request = service
    store.submit(request)
    spec = store.intake_execution_spec(request['run_id'])
    from pathlib import Path
    state = Path(spec['state_dir'])
    resources = RunResources(spec)
    candidate = {'id': 'opaque-candidate'}
    identity = {'run_id': spec['run_id'], 'role': 'implement', 'iteration': 0,
                'candidate_id': candidate['id'], 'policy_digest': spec['policy_digest']}
    job_key = digest(identity)
    folder = state / 'attempts' / job_key
    folder.mkdir(parents=True, mode=0o700)
    path = folder / 'native-process.json'
    (folder / 'native-process.lock').touch(mode=0o600)
    role_request = {'spec': spec, 'role': 'implement', 'iteration': 0, 'candidate': candidate,
                    'workspace': spec['checkout'], 'result_path': str(folder / 'result.json'),
                    'start_path': str(folder / 'start.json')}
    launch = {'argv': ['opaque', 'role'], 'environment': {'OPAQUE': 'fixture'}}
    result = {'status': 'blocked', 'session_id': 'original-session', 'cleanup': 'confirmed'}
    row = {'job_key': job_key, 'role': 'implement', 'iteration': 0, 'state': 'finished',
           'cleanup': 'confirmed', 'candidate_id': candidate['id'],
           'session_id': 'original-session', 'result_json': json.dumps(result),
           'pid': 17, 'process_identity': 'opaque-start'}
    journal = {'phase': 'finished', 'monitoring_complete': True, 'ports': [],
               'owned': {'17': {'identity': 'opaque-start'}},
               'monitor': {'pid': 19, 'identity': 'opaque-monitor'},
               'result': {'cleanup': 'observed-native-confirmed'},
               'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
                          'argv': launch['argv'], 'cwd': spec['checkout'], 'ports': [],
                          'timeout': 10, 'environment_sha256': digest(launch['environment'])},
               'provider_session': {'result_digest': digest(result), 'role': row['role'],
                                    'iteration': row['iteration'], 'session_id': row['session_id']}}
    write_private(path, journal)
    write_private(folder / 'launch.json', launch)
    write_private(folder / 'request.json', role_request)
    resources.process(path)
    write_private(state / 'resources/finalization.json', {
        'state': 'unknown', 'outcome': 'blocked', 'process_cleanup': 'unknown',
        'resource_cleanup': 'unknown',
        'processes': [{'journal': str(path), 'cleanup': 'unknown',
                       'monitoring_complete': False, 'owned_ports_clear': False,
                       'observed_pids': [17]}]})
    table = {}
    ports = set()
    monkeypatch.setattr(processes, 'process_table', lambda: table)
    monkeypatch.setattr(processes, 'listeners', lambda _port: ports)
    return spec, row, path, journal, role_request, table, ports


def test_fresh_completion_is_separate_from_historical_unknown_receipt(completed):
    spec, row, path, _journal, _request, _table, _ports = completed
    before = path.read_bytes()
    with pytest.raises(ValueError, match='unconfirmed'):
        observe_finalized_resources(spec, unknown_allowed=True)
    observation = observe_completed_resources(spec, [row])
    assert str(path) in observation['journal_sha256']
    assert path.read_bytes() == before  # No retrofill or completion-state write.


@pytest.mark.parametrize('changed', ['missing_digest', 'result', 'session', 'role', 'iteration',
                                     'run', 'environment', 'monitor_live', 'owned_live',
                                     'port_busy', 'inventory', 'request', 'pid', 'missing_monitor',
                                     'incomplete', 'lock_busy'])
def test_later_completed_observation_requires_original_bindings(completed, changed, monkeypatch):
    spec, original, path, journal, request, table, ports = completed
    row = copy.deepcopy(original)
    if changed == 'missing_digest':
        journal['provider_session'].pop('result_digest')
    elif changed == 'result':
        row['result_json'] = json.dumps({'status': 'pass'})
    elif changed in {'session', 'role', 'iteration'}:
        journal['provider_session'][changed if changed != 'session' else 'session_id'] = 'foreign'
    elif changed == 'run':
        journal['intent']['run_id'] = 'foreign'
    elif changed == 'environment':
        journal['intent']['environment_sha256'] = 'foreign'
    elif changed in {'monitor_live', 'owned_live'}:
        pid = 19 if changed == 'monitor_live' else 17
        table[pid] = {'identity': 'opaque-monitor' if pid == 19 else 'opaque-start', 'stat': 'S'}
    elif changed == 'port_busy':
        journal['ports'] = journal['intent']['ports'] = [12345]
        ports.add(999)
    elif changed == 'inventory':
        row['job_key'] = 'foreign'
    elif changed == 'request':
        request['spec'] = {**spec, 'policy_digest': 'foreign'}
        write_private(path.with_name('request.json'), request)
    elif changed == 'pid':
        row['process_identity'] = 'foreign'
    elif changed == 'missing_monitor':
        journal.pop('monitor')
    elif changed == 'incomplete':
        journal['monitoring_complete'] = False
    elif changed == 'lock_busy':
        import fcntl

        from devflow_temporal import delivery_resources
        def busy(_fd, operation):
            assert operation == fcntl.LOCK_EX | fcntl.LOCK_NB
            raise BlockingIOError('opaque original lease is busy')
        monkeypatch.setattr(delivery_resources.fcntl, 'flock', busy)
    write_private(path, journal)
    with pytest.raises((ValueError, KeyError)):
        observe_completed_resources(spec, [row])
