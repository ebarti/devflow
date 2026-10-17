"""Read-only controller contracts with opaque process/port doubles; no native launches."""
from __future__ import annotations

import copy
import hashlib
import json
import threading
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_store import service as service

from devflow_temporal import delivery_native_process as processes
from devflow_temporal import delivery_orphans as orphans
from devflow_temporal import delivery_resources as resources
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    observe_finalized_resources,
    write_private,
)


@pytest.fixture
def completed(service, monkeypatch, *, role="implement"):
    store, request = service
    store.submit(request)
    spec = store.intake_execution_spec(request['run_id'])
    from pathlib import Path
    state = Path(spec['state_dir'])
    resources = RunResources(spec)
    candidate = {'id': 'opaque-candidate'}
    identity = {'run_id': spec['run_id'], 'role': role, 'iteration': 0,
                'candidate_id': candidate['id'], 'policy_digest': spec['policy_digest']}
    job_key = digest(identity)
    folder = state / 'attempts' / job_key
    folder.mkdir(parents=True, mode=0o700)
    path = folder / 'native-process.json'
    (folder / 'native-process.lock').touch(mode=0o600)
    role_request = {'spec': spec, 'role': role, 'iteration': 0, 'candidate': candidate,
                    'workspace': spec['checkout'], 'result_path': str(folder / 'result.json'),
                    'start_path': str(folder / 'start.json')}
    launch = {'argv': ['opaque', 'role'], 'environment': {'OPAQUE': 'fixture'}}
    result = {'status': 'blocked', 'session_id': 'original-session', 'cleanup': 'confirmed'}
    row = {'job_key': job_key, 'role': role, 'iteration': 0, 'state': 'finished',
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


def published_scope(store, original):
    from pathlib import Path

    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_gate_retry import gate_namespace

    current = copy.deepcopy(original)
    role = current['policy']['roles']['implement']
    role['timeout_seconds'] = role.get('timeout_seconds', 7200) + 1
    current['policy_digest'] = digest(current['policy'])
    if 'preparation' in current:
        current['preparation']['measured_policy_digest'] = digest('later-measurement')
    current.update(gate_retry_stage='published', gate_retry_generation=1)
    namespace = gate_namespace(1, 'published')
    admission = {'kind': 'published_gate_retry', 'execution_spec': current,
                 'original_recovery': None}
    write_private(Path(original['state_dir']) / namespace / 'admission.json', admission)
    with store._connect() as db:
        db.execute('INSERT INTO delivery_gate_admissions '
                   '(run_id,command_id,recovery_json) VALUES (?,?,?)',
                   (current['run_id'], 'fixture-admission', canonical_json(admission)))
        db.execute('UPDATE delivery_runs SET recovery_json=?', (canonical_json(admission),))
    return current, namespace


def test_retained_journals_use_exact_preparation_and_namespace_authority(
        completed, service):
    """Real private DB/admission/path guards; actor and port tables remain opaque."""
    from pathlib import Path

    from devflow_temporal.contracts import canonical_json
    from devflow_temporal.delivery_resources import private_directory, read_private

    original, row, path, journal, request, _table, _ports = completed
    store, _submitted = service
    original = copy.deepcopy(original)
    original['preparation'] = {'measured_policy_digest': digest('original-measurement')}
    request['spec'] = original
    write_private(path.with_name('request.json'), request)
    with store._connect() as db:
        submitted = json.loads(db.execute('SELECT request_json FROM delivery_runs').fetchone()[0])
        db.execute('INSERT INTO delivery_preparations VALUES (?,?,?,?,?)',
                   (original['run_id'], digest(submitted), digest(original),
                    canonical_json(original), 'fixture'))
    state = Path(original['state_dir'])
    current, namespace = published_scope(store, original)

    paths = [path]
    commands = [('dependency-preparation/process', original['policy_digest']),
                ('native-preparation/probe', digest('original-measurement')),
                (namespace + '/evidence/native-checks/probe', current['policy_digest'])]
    for relative, policy in commands:
        folder = state / relative
        private_directory(folder)
        cloned = copy.deepcopy(journal)
        cloned['intent']['policy_digest'] = policy
        target = folder / 'native-process.json'
        write_private(target, cloned)
        write_private(target.with_name('launch.json'), read_private(path.with_name('launch.json')))
        target.with_name('native-process.lock').touch(mode=0o600)
        RunResources(original).process(target)
        paths.append(target)
    final = read_private(state / 'resources/finalization.json')
    final['processes'] = [{**final['processes'][0], 'journal': str(p)} for p in paths]
    write_private(state / 'resources/finalization.json', final)
    before = {str(p): p.read_bytes() for p in paths}
    assert set(observe_completed_resources(current, [row])['journal_sha256']) == set(before)
    assert all(p.read_bytes() == before[str(p)] for p in paths)
    # A valid digest from another admitted scope cannot authorize this journal.
    for target, wrong in [(paths[1], current['policy_digest']),
                          (paths[2], digest('later-measurement')),
                          (paths[3], original['policy_digest'])]:
        changed = read_private(target)
        changed['intent']['policy_digest'] = wrong
        write_private(target, changed)
        with pytest.raises(ValueError, match='launch binding changed'):
            observe_completed_resources(current, [row])
        target.write_bytes(before[str(target)])
    with store._connect() as db:
        db.execute("UPDATE delivery_preparations SET effective_spec_digest='foreign'")
    with pytest.raises(ValueError, match='preparation custody changed'):
        observe_completed_resources(current, [row])
    with store._connect() as db:
        db.execute('UPDATE delivery_preparations SET effective_spec_digest=?', (digest(original),))
    # A later role keeps its original attempt path and policy across admissions.
    # Preservation holds both authenticated lifecycle leases through its yield.
    import fcntl

    from devflow_temporal.supervisor import DeliverySupervisor

    later_request = copy.deepcopy(request)
    later_request['spec'] = current
    folder = state / 'attempts' / DeliverySupervisor._job_key(later_request)
    private_directory(folder)
    later_path = folder / 'native-process.json'
    later_request.update(result_path=str(folder / 'result.json'),
                         start_path=str(folder / 'start.json'))
    later_journal = copy.deepcopy(journal)
    later_journal['intent']['policy_digest'] = current['policy_digest']
    later_journal['runtime_identity'] = {'opaque': 'original-runtime'}
    later_journal['result'].update(journal=str(later_path),
                                   runtime_identity=later_journal['runtime_identity'])
    result = {**json.loads(row['result_json']), 'native_process': later_journal['result']}
    later_journal['provider_session']['result_digest'] = digest(result)
    write_private(later_path, later_journal)
    write_private(folder / 'request.json', later_request)
    write_private(folder / 'launch.json', read_private(path.with_name('launch.json')))
    for name in ('native-monitor.lock', 'native-process.lock'):
        (folder / name).touch(mode=0o600)
    later_row = {**row, 'run_id': original['run_id'], 'job_key': folder.name,
                 'result_path': str(folder / 'result.json'), 'result_json': canonical_json(result)}
    with store._connect() as db:
        columns = ','.join(later_row)
        placeholders = ','.join('?' for _key in later_row)
        db.execute(f'INSERT INTO delivery_attempts ({columns}) VALUES ({placeholders})',
                   tuple(later_row.values()))
    owner = RunResources(current)
    owner.process(later_path)
    before_role = later_path.read_bytes()
    with owner.locked() as manifest:
        with owner._closed_journals(manifest, [str(later_path)]) as observed:
            assert observed == [{'journal': str(later_path),
                                 'sha256': hashlib.sha256(before_role).hexdigest()}]
            for name in ('native-monitor.lock', 'native-process.lock'):
                with (folder / name).open('rb') as lease:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(lease.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert later_path.read_bytes() == before_role
        later_journal['provider_session']['result_digest'] = 'foreign'
        write_private(later_path, later_journal)
        with pytest.raises(ValueError, match='completed role binding changed'):
            with owner._closed_journals(manifest, [str(later_path)]):
                pytest.fail('changed historical receipt authorized preservation')
        later_path.write_bytes(before_role)


def test_retained_original_intake_binds_the_durably_accepted_plan(intake_fixture, monkeypatch):
    from test_delivery_intake import _authorization, _record_plan

    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    config, supplied = intake_fixture
    store = DeliveryStore(DeliveryConfig.load(config))
    store.submit(supplied)
    submitted = store.spec(supplied['run_id'])
    assert submitted['intake_required'] and not submitted['accepted_plan']
    plan = json.loads(config.read_text())['fake_intake'][2]['plan']
    authorization = _authorization(submitted)
    _record_plan(store, plan, authorization)
    store.accept_intake_plan(submitted['run_id'], 1, digest(plan), plan,
                             authorization=authorization)
    original, row, _path, _journal, _request, _table, _ports = completed.__wrapped__(
        (store, supplied), monkeypatch)
    current, _namespace = published_scope(store, original)
    assert observe_completed_resources(current, [row])['journal_sha256']
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET accepted_plan_text='foreign accepted plan'")
    with pytest.raises(ValueError, match='request lost its admitted execution policy'):
        observe_completed_resources(current, [row])


@pytest.fixture
def completed_intake(intake_fixture, monkeypatch):
    """Private prepared intake completed before real plan acceptance; no provider."""
    from test_delivery_intake import _authorization, _record_plan

    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    config, supplied = intake_fixture
    store = DeliveryStore(DeliveryConfig.load(config))
    original, row, path, journal, request, table, ports = completed.__wrapped__(
        (store, supplied), monkeypatch, role='intake')
    submitted = copy.deepcopy(original)
    original['preparation'] = {'measured_policy_digest': original['policy_digest']}
    journal['runtime_identity'] = {'opaque': 'original-intake-runtime'}
    journal['result'].update(journal=str(path), runtime_identity=journal['runtime_identity'])
    result = {**json.loads(row['result_json']), 'native_process': journal['result']}
    row.update(run_id=original['run_id'], result_path=str(path.with_name('result.json')),
               result_json=canonical_json(result))
    journal['provider_session']['result_digest'] = digest(result)
    journal['lifecycle_locks'] = {}
    for name in ('native-monitor.lock', 'native-process.lock'):
        lock = path.with_name(name)
        lock.touch(mode=0o600, exist_ok=True)
        info = lock.stat()
        journal['lifecycle_locks'][name] = {
            'device': info.st_dev, 'inode': info.st_ino, 'uid': info.st_uid}
    write_private(path, journal)
    write_private(path.with_name('request.json'), request)
    write_private(path.with_name('result.json'), result)
    with store._connect() as db:
        # Frozen preparation is injected explicitly: the fake provider never measures a host.
        db.execute('INSERT INTO delivery_preparations VALUES (?,?,?,?,?)',
                   (original['run_id'], digest(submitted), digest(original),
                    canonical_json(original), 'private-fixture'))
        columns, placeholders = ','.join(row), ','.join('?' for _key in row)
        db.execute(f'INSERT INTO delivery_attempts ({columns}) VALUES ({placeholders})',
                   tuple(row.values()))
    plan = json.loads(config.read_text())['fake_intake'][2]['plan']
    authorization = _authorization(submitted)
    _record_plan(store, plan, authorization)
    accepted = store.accept_intake_plan(original['run_id'], 1, digest(plan), plan,
                                        authorization=authorization)
    current = copy.deepcopy(original)
    current['accepted_plan'] = accepted['accepted_plan']
    monkeypatch.setattr(orphans, 'process_table', lambda: table)
    monkeypatch.setattr(orphans, 'listeners', lambda _port: ports)
    return store, current, row, path, journal, request, table


def _observe_intake(fixture, surface):
    _store, current, row, path, _journal, _request, _table = fixture
    if surface == 'orphan':
        return orphans._observe_registered_process(current, path)
    if surface == 'completed':
        return observe_completed_resources(current, [row])
    owner = RunResources(current)
    with owner.locked() as manifest, owner._closed_journals(manifest, [str(path)]) as observed:
        return observed


@pytest.mark.parametrize('surface', ['orphan', 'finalizer', 'completed'])
def test_completed_preplan_intake_survives_plan_acceptance(completed_intake, surface):
    _store, current, _row, path, _journal, request, _table = completed_intake
    assert request['spec']['accepted_plan'] == '' and current['accepted_plan']
    before = {name: path.with_name(name).read_bytes()
              for name in ('request.json', 'result.json', 'native-process.json')}
    assert _observe_intake(completed_intake, surface)
    assert all(path.with_name(name).read_bytes() == value for name, value in before.items())
    # The original request remains a valid cleanup caller, without changing the current plan.
    original = (completed_intake[0], request['spec'], *completed_intake[2:])
    assert _observe_intake(original, surface)
    executions = {}
    resources._gate_roots(current, execution_specs=executions)
    assert executions[path.parents[2]] == current


def test_preplan_cleanup_requires_the_current_authenticated_feature_overlay(completed_intake):
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry

    store, current, row, path, journal, request, table = completed_intake
    shared = ExecutionRegistry(store.config.path.parent / 'ownership/registry.sqlite3')
    snapshot = {'issue': {'id': 'private-feature', 'url': current['issue_url'],
                           'repository_id': 'private-repository'}}
    token = shared.claim(snapshot, current['run_id'], str(store.config.tracking_db))
    feature = {'registry': str(shared.path), 'owner': token, 'snapshot': snapshot}
    submitted = store.submitted_spec(current['run_id'])
    # Explicit private admission seam: fake preparation gains the real feature
    # custody fields and preparation marker, with both digests regenerated.
    submitted.update(feature_delivery=feature, preparation_version=1)
    request['spec'].update(feature_delivery=feature, preparation_version=1)
    current.update(feature_delivery=feature, preparation_version=1)
    write_private(path.with_name('request.json'), request)
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=?', (canonical_json(submitted),))
        db.execute('UPDATE delivery_preparations SET submitted_spec_digest=?, '
                   'effective_spec_digest=?,effective_spec_json=?',
                   (digest(submitted), digest(request['spec']), canonical_json(request['spec'])))
    overlay = copy.deepcopy(current)
    plan = json.loads(overlay['accepted_plan'])
    plan['scope'] = 'The adopted plan keeps the original completed intake custody'
    overlay['accepted_plan'] = json.dumps(plan)
    identity = {'plan_revision': 1, 'plan_digest': digest(plan)}
    overlay['feature_plan_revision'] = identity
    key = 'plan-revision:spec:' + current['run_id'] + ':1'
    shared.checkpoint(token, key, {'identity': identity, 'revision_id': 'private-adoption',
                                  'spec': overlay, 'spec_digest': digest(overlay)})
    effective = store.effective_spec(current['run_id'])
    assert effective == overlay and effective != current
    fixture = store, effective, row, path, journal, request, table
    for surface in ('orphan', 'finalizer', 'completed'):
        assert _observe_intake(fixture, surface)
        changed = {**effective, 'accepted_plan': 'an unadmitted caller plan'}
        with pytest.raises(ValueError, match='admitted execution policy'):
            _observe_intake((store, changed, *fixture[2:]), surface)
    # The namespace remains the separately authenticated SQL-accepted plan.
    executions = {}
    resources._gate_roots(effective, execution_specs=executions)
    assert executions[path.parents[2]] == current
    with shared.connect() as db:
        db.execute('UPDATE execution_checkpoints SET content_digest=? WHERE checkpoint_key=?',
                   ('foreign', key))
    with pytest.raises(ValueError, match='checkpoint digest changed'):
        _observe_intake(fixture, 'orphan')


@pytest.mark.parametrize('surface', ['orphan', 'finalizer', 'completed'])
@pytest.mark.parametrize('changed', [
    'accepted_plan', 'preparation', 'request_plan', 'request_policy', 'result', 'session',
    'unfinished', 'live_actor', 'other_role',
])
def test_completed_preplan_intake_preserves_custody(completed_intake, surface, changed):
    from devflow_temporal.supervisor import DeliverySupervisor

    store, _current, row, path, journal, request, table = completed_intake
    if changed == 'accepted_plan':
        with store._connect() as db:
            db.execute("UPDATE delivery_runs SET accepted_plan_text='foreign plan'")
    elif changed == 'preparation':
        with store._connect() as db:
            db.execute("UPDATE delivery_preparations SET effective_spec_digest='foreign'")
    elif changed == 'request_plan':
        request['spec']['accepted_plan'] = 'unadmitted plan'
    elif changed == 'request_policy':
        request['spec']['policy']['roles']['intake']['timeout_seconds'] = 99999
        request['spec']['policy_digest'] = digest(request['spec']['policy'])
    elif changed == 'result':
        row['result_json'] = canonical_json({'status': 'pass'})
    elif changed == 'session':
        journal['provider_session']['session_id'] = 'foreign'
    elif changed == 'unfinished':
        row['state'] = 'running'
    elif changed == 'live_actor':
        table[17] = {'identity': 'opaque-start', 'stat': 'S'}
    elif changed == 'other_role':
        request['role'] = row['role'] = 'implement'
        row['job_key'] = DeliverySupervisor._job_key(request)
    write_private(path.with_name('request.json'), request)
    write_private(path, journal)
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET result_json=?,state=?',
                   (row['result_json'], row['state']))
    with pytest.raises(ValueError):
        _observe_intake(completed_intake, surface)


@pytest.mark.parametrize('changed', ['missing_digest', 'lock_busy'])
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
        original_flock = fcntl.flock

        def busy(descriptor, operation):
            if operation == fcntl.LOCK_SH:
                return original_flock(descriptor, operation)
            assert operation == fcntl.LOCK_EX | fcntl.LOCK_NB
            raise BlockingIOError('opaque original lease is busy')
        monkeypatch.setattr(delivery_resources.fcntl, 'flock', busy)
    write_private(path, journal)
    with pytest.raises((ValueError, KeyError)):
        observe_completed_resources(spec, [row])


@pytest.fixture
def retained(tmp_path, monkeypatch):
    state = tmp_path / 'private-worker'
    spec = {'run_id': state.name, 'state_dir': str(state), 'policy': {},
            'checkout': str(tmp_path / 'candidate')}
    owner = resources.RunResources(spec)
    gate = state / 'gates/7/verify'
    resources.private_directory(gate.parent)
    owner.register(gate, 'gate')
    resources.private_directory(gate)
    owner.created(gate)
    monkeypatch.setattr(processes, 'process_table', lambda: {})
    monkeypatch.setattr(resources.subprocess, 'run',
                        lambda *_args, **_kwargs: SimpleNamespace(returncode=1, stdout=''))
    monkeypatch.setattr(resources.RunResources, '_retain_source',
                        lambda *_args: 'private synthetic retained gate')
    owner.finalize('blocked')
    return spec, owner, gate


def test_observation_and_finalizer_share_one_complete_resource_generation(retained, monkeypatch):
    spec, owner, _gate = retained
    final = owner.root / 'finalization.json'
    before = owner.manifest.read_bytes(), final.read_bytes()
    baseline = resources.observe_completed_resources(spec, [])
    writer_attempted, running, finish = (threading.Event() for _ in range(3))
    reader_lease, errors = [], []
    flock, write = resources.fcntl.flock, resources.write_private

    def authenticated_flock(descriptor, operation):
        if threading.current_thread() is writer and operation == resources.fcntl.LOCK_EX:
            writer_attempted.set()
        flock(descriptor, operation)
        if (threading.current_thread() is threading.main_thread()
                and operation == resources.fcntl.LOCK_SH):
            reader_lease.append(True)

    def paused_write(path, value):
        write(path, value)
        if path == owner.manifest and value.get('finalization', {}).get('state') == 'running':
            running.set()
            assert finish.wait(5)

    def finalize():
        try:
            owner.finalize('blocked')
        except BaseException as exc:
            errors.append(exc)

    writer = threading.Thread(target=finalize, name='private-resource-finalizer')

    def overlap(argv, **_kwargs):
        assert argv[0] == '/usr/sbin/lsof'
        writer.start()
        assert writer_attempted.wait(5)
        if not reader_lease:
            # Force the old unlocked reader to return the real intermediate hash.
            assert running.wait(5)
        return SimpleNamespace(returncode=1, stdout='')

    monkeypatch.setattr(resources.fcntl, 'flock', authenticated_flock)
    monkeypatch.setattr(resources, 'write_private', paused_write)
    monkeypatch.setattr(resources.subprocess, 'run', overlap)
    try:
        observation = resources.observe_completed_resources(spec, [])
        assert observation == baseline
        assert observation['manifest_sha256'] == hashlib.sha256(before[0]).hexdigest()
    finally:
        finish.set()
        if writer.ident is not None:
            writer.join(5)
    assert not writer.is_alive() and not errors
    assert (owner.manifest.read_bytes(), final.read_bytes()) == before

    # Observation must authenticate an existing private lease, never repair it.
    lock = owner.root / 'ownership.lock'
    lock.chmod(0o644)
    with pytest.raises(ValueError, match='ownership lock.*private'):
        resources.observe_completed_resources(spec, [])
    assert lock.stat().st_mode & 0o777 == 0o644
    lock.unlink()
    with pytest.raises(FileNotFoundError):
        resources.observe_completed_resources(spec, [])
    assert not lock.exists()
    assert (owner.manifest.read_bytes(), final.read_bytes()) == before


def test_retained_custody_is_complete_but_new_or_changed_resources_remain_pending(
        retained, monkeypatch):
    spec, owner, gate = retained
    row = {'cleanup_rowid': 1, 'run_id': spec['run_id'], 'request_json': canonical_json(spec),
           'recovery_json': None, 'workflow_id': 'private-closed-worker', 'cleanup': 'confirmed',
           'checks_json': canonical_json({'resource_cleanup': {'resource_cleanup': 'confirmed'}})}

    class PrivateRows:
        def execute(self, sql, _parameters=()):
            if 'WHERE EXISTS' in sql:
                return []
            assert 'cleanup_rowid' in sql
            return [row]

    class PrivateStore:
        @contextmanager
        def _connect(self):
            yield PrivateRows()

    store, clock = PrivateStore(), [100.0]
    monkeypatch.setattr(orphans.time, 'monotonic', lambda: clock[0])

    def scan():
        clock[0] += 61
        return orphans._unfinished_rows(store)

    assert scan() == []
    before = owner.manifest.read_bytes()
    owner.finalize('blocked')
    assert owner.manifest.read_bytes() == before
    assert scan() == []  # Identical-byte publication is not new disposable work.

    row['cleanup'] = 'unknown'
    assert len(scan()) == 1  # A lost cleanup acknowledgement still needs reconciliation.
    row['cleanup'] = 'confirmed'
    assert scan() == []
    row['checks_json'] = '{}'
    assert len(scan()) == 1
    row['checks_json'] = canonical_json({'resource_cleanup': {'resource_cleanup': 'confirmed'}})
    assert scan() == []

    original = gate.with_name('preserved-original')
    gate.rename(original)
    gate.mkdir(mode=0o700)
    assert len(scan()) == 1  # Unchanged manifest metadata cannot hide a replacement.
    gate.rmdir()
    original.rename(gate)
    assert scan() == []

    journal = owner.state / 'private-check/native-process.json'
    resources.write_private(journal, {'phase': 'allocated'})
    owner.process(journal)
    assert len(scan()) == 1  # A new actor is not covered by the old finalization inventory.
    with owner.locked() as manifest:
        manifest['processes'] = []
        resources.write_private(owner.manifest, manifest)
    assert scan() == []

    artifact = owner.state / 'checks/7/new/pytest-artifacts'
    resources.private_directory(artifact.parent)
    owner.register(artifact, 'check-artifacts')
    resources.private_directory(artifact)
    owner.created(artifact)
    assert len(scan()) == 1  # A newly allocated root still requires its original finalizer.
