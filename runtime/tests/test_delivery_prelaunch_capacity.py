from __future__ import annotations

import asyncio
import json
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager, nullcontext
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio import activity
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import (
    delivery_activities,
    delivery_native_process,
    delivery_role_evidence,
    supervisor,
)
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor, get_supervisor

PROVIDER = '''
import json,sys,time
from pathlib import Path
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
state=Path(request['spec']['state_dir'])
with (state/'invocations').open('a') as stream: stream.write('one\\n')
(state/'provider-started').touch()
while (state/'hold-provider').exists() and not (state/'release-provider').exists():
    time.sleep(0.02)
write_private(Path(request['result_path']), {'status':'blocked','summary':'Fixture completed',
    'findings':[],'session_id':'fixture-session','usage':None,'finish_reason':'fixture'})
'''


@pytest.fixture
def native_role(api_fixture, tmp_path, monkeypatch, request):
    prewarm_readiness = getattr(request, 'param', True)
    path, submission = api_fixture
    raw = json.loads(path.read_text())
    raw.update(provider='codex', execution_mode='trusted-local', codex_bin=str(
        distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    raw['execution_registry'] = str(tmp_path / 'execution-registry.sqlite3')
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'} for role in raw['roles']}
    check = {'id': 'fixture-check', 'argv': ['git', 'diff', '--check']}
    raw['repositories']['fixture'].update(prepublish_checks=[check], checks=[check],
        required_ci=['test'], project_url='https://github.com/users/example/projects/1',
        assignee='example', readiness={'version': 1, 'generation': 0,
            'browser_engines': [], 'temporal_checks': []})
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    from devflow_temporal.delivery_native_preparation import prepare_native_authority
    spec = prepare_native_authority(store, store.spec(submission['run_id']))
    broker = DeliveryBroker(store, spec)
    request = {'spec': spec, 'role': 'implement', 'iteration': 0,
               'candidate': broker.prepare()['candidate']}
    from devflow_temporal.delivery_readiness import describe_requirements, prepare_and_probe

    # Complete actual readiness before timing or injecting faults into the
    # product launch. Preparation probes keep their own native process owner.
    requirements = describe_requirements(spec, broker.checkout,
                                         accepted_plan=spec['accepted_plan'])
    custody = nullcontext()
    if spec.get('workspace_custody_version') == 1:
        from devflow_temporal.delivery_writer_custody import shared

        custody = shared(spec)
    if prewarm_readiness:
        with custody:
            readiness = prepare_and_probe(store, spec, request['candidate'], requirements, 'plan')
        assert readiness['state'] == 'passed', readiness
    owner = get_supervisor(store)
    key = owner._job_key(request)
    state = Path(spec['state_dir'])
    folder = state / 'attempts' / key
    provider = tmp_path / 'provider.py'
    provider.write_text(PROVIDER)
    original = delivery_native_process.NativeProcess
    created = []

    def native(spec, folder, **options):
        if folder.parent != state / 'attempts':
            return original(spec, folder, **options)
        options.update(argv=[sys.executable, '-I', str(provider), str(folder / 'request.json')],
                       timeout=10)
        process = original(spec, folder, **options)
        created.append(process)
        return process

    monkeypatch.setattr(delivery_native_process, 'NativeProcess', native)
    monkeypatch.setattr(supervisor, 'prepare_native_role',
                        lambda request, folder: ('fixture', {'PATH': '/usr/bin:/bin'}))
    case = SimpleNamespace(store=store, broker=broker, request=request, owner=owner, key=key,
                           state=state, folder=folder, created=created, process_type=original)
    yield case
    # Keep complete probe artifacts while removing only fixture-owned checkout resources.
    (state / 'release-provider').touch()
    for journal_path in folder.glob('native-process.json'):
        journal = read_private(journal_path)
        owned = {int(pid): value for pid, value in journal.get('owned', {}).items()}
        if journal.get('monitor'):
            owned[journal['monitor']['pid']] = journal['monitor']
        assert delivery_native_process.stop_observed(owned)
    receipt = RunResources(spec).finalize('blocked')
    (tmp_path / 'finalization.json').write_text(json.dumps(receipt, indent=2))
    if broker.checkout.exists():
        subprocess.run(['git', '-C', str(broker.source), 'worktree', 'remove', '--force',
                        str(broker.checkout)], check=True, capture_output=True)


def _attempt(case):
    with case.store._connect() as db:
        rows = [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')]
        occupied = db.execute("SELECT COUNT(*) FROM delivery_attempts WHERE state IN "
                              "('starting','running','unknown')").fetchone()[0]
    observation = {'attempts': rows, 'occupied': occupied,
                   'provider_invocations': (case.state / 'invocations').read_text().splitlines()
                   if (case.state / 'invocations').exists() else [],
                   'journal_exists': (case.folder / 'native-process.json').exists()}
    (case.state / 'probe-observation.json').write_text(json.dumps(observation, indent=2))
    assert len(rows) == 1
    return rows[0], occupied


async def _wait(event):
    deadline = time.monotonic() + 10
    while not event.is_set():
        assert time.monotonic() < deadline, 'fixture did not reach its controlled boundary'
        await asyncio.sleep(0.01)


def _assert_prelaunch(case, result=None):
    row, occupied = _attempt(case)
    assert row['state'] == 'finished', row
    assert row['cleanup'] == 'confirmed'
    assert occupied == 0
    saved = json.loads(row['result_json'])
    assert saved['finish_reason'] == 'prelaunch'
    assert saved['status'] == 'blocked'
    if result is not None:
        assert result['cleanup'] == 'confirmed'
        assert result['finish_reason'] == 'prelaunch'
    assert not (case.folder / 'native-process.json').exists()
    assert not (case.state / 'invocations').exists()
    assert RunResources(case.request['spec']).finalize('blocked')['resource_cleanup'] == 'confirmed'


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['log_privacy', 'journal_prepublication'])
async def test_actual_monitor_prelaunch_failure_releases_owned_capacity(
        native_role, monkeypatch, failure):
    case = native_role
    if failure == 'log_privacy':
        def prepare(request, folder):
            log = folder / 'monitor.log'
            log.write_text('original non-private log\n')
            log.chmod(0o644)
            return 'fixture', {'PATH': '/usr/bin:/bin'}
        monkeypatch.setattr(supervisor, 'prepare_native_role', prepare)
    else:
        original = subprocess.Popen

        def popen(argv, *args, **kwargs):
            owner = sys._getframe(1).f_locals.get('self')
            if (argv[-1] == 'devflow_temporal.delivery_native_process'
                    and getattr(owner, 'folder', None) == case.folder):
                if failure == 'spawn':
                    raise OSError('fixture monitor exec failed before spawn')
                script = 'raise SystemExit(2)'
                if failure == 'journal_prepublication':
                    script = (
                        'import json,sys\n'
                        'from pathlib import Path\n'
                        'from devflow_temporal import delivery_native_process as native\n'
                        'request=json.load(sys.stdin)\n'
                        "folder=Path(request.pop('folder'))\n"
                        "spec=request.pop('spec')\n"
                        "request['cwd']=Path(request['cwd'])\n"
                        "request['ports']=tuple(request['ports'])\n"
                        'native.process_table=lambda: {}\n'
                        'native.NativeProcess(spec,folder,**request)._run(monitor=True)\n'
                    )
                argv = [sys.executable, '-I', '-c', script]
            return original(argv, *args, **kwargs)

        monkeypatch.setattr(subprocess, 'Popen', popen)
    result = await delivery_role(case.request)
    row, _ = _attempt(case)
    created = len(case.created)
    assert created == 1, 'prelaunch failure bypassed the actual monitor boundary'
    assert await delivery_role(case.request) == result
    assert _attempt(case)[0] == row
    assert len(case.created) == created
    _assert_prelaunch(case, result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "repeat_cancel,source", [(False, "explicit"), (False, "heartbeat_timeout")]
)
async def test_cancelled_preparation_joins_before_releasing_capacity(
    native_role, monkeypatch, repeat_cancel, source
):
    case = native_role
    entered, release, ended = threading.Event(), threading.Event(), threading.Event()

    def prepare(request, folder):
        entered.set()
        assert release.wait(10), 'preparation was not released'
        ended.set()
        return 'fixture', {'PATH': '/usr/bin:/bin'}

    monkeypatch.setattr(supervisor, 'prepare_native_role', prepare)
    if source == 'heartbeat_timeout':
        monkeypatch.setattr(activity, 'in_activity', lambda: True)
        monkeypatch.setattr(activity, 'heartbeat', lambda *args: None)
        monkeypatch.setattr(activity, 'cancellation_details', lambda: SimpleNamespace(
            cancel_requested=False, timed_out=True, not_found=False, worker_shutdown=False))
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await _wait(entered)
        task.cancel()
        await asyncio.sleep(0.05)
        if repeat_cancel:
            task.cancel()
            await asyncio.sleep(0.05)
        assert not task.done(), 'cancellation abandoned the active preparation thread'
        row, occupied = _attempt(case)
        assert occupied == 1 and row['state'] == 'starting'
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _wait(ended)
        _assert_prelaunch(case)
        assert case.created == []
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_constructor_cannot_launch_after_its_waiter_exits(native_role, monkeypatch):
    case = native_role
    entered, release = threading.Event(), threading.Event()
    original = delivery_native_process.NativeProcess

    def construct(*args, **kwargs):
        process = original(*args, **kwargs)
        if process.folder != case.folder:
            return process
        entered.set()
        assert release.wait(10), 'constructor was not released'
        return process

    monkeypatch.setattr(delivery_native_process, 'NativeProcess', construct)
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await _wait(entered)
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        assert _attempt(case)[1] == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        _assert_prelaunch(case)
        assert len(case.created) == 1
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)




@pytest.mark.asyncio
async def test_delayed_native_run_cannot_release_capacity_from_cancellation_alone(
        native_role, monkeypatch):
    case = native_role
    entered, release = threading.Event(), threading.Event()
    original = case.process_type.run

    def run(process):
        if process.folder != case.folder:
            return original(process)
        entered.set()
        assert release.wait(10), 'monitor entry was not released'
        return original(process)

    monkeypatch.setattr(case.process_type, 'run', run)
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await _wait(entered)
        task.cancel()
        await asyncio.sleep(0.1)
        assert not task.done()
        row, occupied = _attempt(case)
        assert occupied == 1 and row['state'] == 'starting'
        assert not (case.folder / 'native-process.json').exists()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        row, occupied = _attempt(case)
        assert row['state'] == 'finished' and occupied == 0
        assert json.loads(row['result_json'])['finish_reason'] != 'prelaunch'
        assert (case.folder / 'native-process.json').exists()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_heartbeat_replacement_preserves_an_already_launched_provider(
        native_role, monkeypatch):
    case = native_role
    (case.state / 'hold-provider').touch()
    monkeypatch.setattr(activity, 'in_activity', lambda: True)
    monkeypatch.setattr(activity, 'heartbeat', lambda *args: None)
    monkeypatch.setattr(activity, 'cancellation_details', lambda: SimpleNamespace(
        cancel_requested=False, timed_out=True, not_found=False, worker_shutdown=False))
    task = asyncio.create_task(delivery_role(case.request))
    replacement = None
    try:
        deadline = time.monotonic() + 10
        while not (case.state / 'provider-started').exists():
            assert time.monotonic() < deadline
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert _attempt(case)[1] == 1
        journal = read_private(case.folder / 'native-process.json')
        table = delivery_native_process.process_table()
        assert any(table.get(int(pid), {}).get('identity') == entry['identity']
                   for pid, entry in journal['owned'].items())
        replacement = asyncio.create_task(delivery_role(case.request))
        await asyncio.sleep(0.1)
        assert _attempt(case)[1] == 1
        assert (case.state / 'invocations').read_text().splitlines() == ['one']
        (case.state / 'release-provider').touch()
        result = await replacement
        assert result['cleanup'] == 'confirmed'
        row, occupied = _attempt(case)
        assert row['state'] == 'finished' and occupied == 0
        assert result['session_id'] == 'fixture-session'
        assert (case.state / 'invocations').read_text().splitlines() == ['one']
    finally:
        (case.state / 'release-provider').touch()
        await asyncio.gather(task, *([replacement] if replacement else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_live_detached_monitor_exception_retains_capacity_and_never_replaces_provider(
        native_role, monkeypatch):
    case = native_role
    (case.state / 'hold-provider').touch()
    original = case.process_type._request_cancel

    def interrupt(process):
        if (case.state / 'provider-started').exists():
            raise RuntimeError('fixture observer interrupted after provider launch')
        return original(process)

    monkeypatch.setattr(case.process_type, '_request_cancel', interrupt)
    try:
        for _ in range(2):
            result = await delivery_role(case.request)
            assert result['cleanup'] == 'unknown'
            row, occupied = _attempt(case)
            assert row['state'] == 'unknown' and row['cleanup'] == 'unknown'
            assert occupied == 1
            assert (case.state / 'invocations').read_text().splitlines() == ['one']
            assert json.loads((case.folder / 'native-process.json').read_text())['owned']
    finally:
        (case.state / 'release-provider').touch()




@pytest.mark.asyncio
@pytest.mark.parametrize('ownership', ['candidate_id', 'process_identity'])
async def test_prelaunch_failure_cannot_finalize_another_reservation(
        native_role, monkeypatch, ownership):
    case = native_role

    value = 1 if ownership == 'pid' else 'foreign-owner'

    def prepare(request, folder):
        with case.store._connect() as db:
            db.execute(f'UPDATE delivery_attempts SET {ownership}=? WHERE job_key=?',
                       (value, case.key))
        raise RuntimeError('fixture preparation failed after ownership changed')

    monkeypatch.setattr(supervisor, 'prepare_native_role', prepare)
    result = await delivery_role(case.request)
    row, occupied = _attempt(case)
    assert result['cleanup'] == 'unknown'
    assert row['state'] == 'starting' and row['cleanup'] == 'none'
    assert row[ownership] == value and occupied == 1
    assert row['result_json'] is None
    assert case.created == []










@pytest.mark.asyncio
@pytest.mark.parametrize('replacement', ['same_worker', 'other_worker'])
async def test_retry_while_cancelled_preparation_is_joining_keeps_final_prelaunch_result(
        native_role, monkeypatch, replacement):
    case = native_role
    entered, release = threading.Event(), threading.Event()

    def prepare(request, folder):
        entered.set()
        assert release.wait(10), 'preparation was not released'
        return 'fixture', {'PATH': '/usr/bin:/bin'}

    monkeypatch.setattr(supervisor, 'prepare_native_role', prepare)
    first = asyncio.create_task(delivery_role(case.request))
    retry = None
    try:
        await _wait(entered)
        first.cancel()
        await asyncio.sleep(0.05)
        if replacement == 'same_worker':
            retry = asyncio.create_task(delivery_role(case.request))
            await asyncio.sleep(0.1)
            assert not retry.done()
        else:
            other = DeliverySupervisor(case.store, capacity=case.owner.capacity)
            enriched = {**case.request, 'workspace': str(case.broker.checkout), 'review_diff': None}
            observed = await other.run(enriched)
            assert observed['cleanup'] == 'unknown'
        assert _attempt(case)[1] == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        result = await retry if retry else await other.run(enriched)
        _assert_prelaunch(case, result)
        assert case.created == []
    finally:
        release.set()
        await asyncio.gather(first, *([retry] if retry else []), return_exceptions=True)








@pytest.mark.asyncio
async def test_actual_prelaunch_receipt_preserves_empty_folder_retry_predicate(
        native_role, monkeypatch):
    case = native_role

    def allocate(request, job_key, *, store=None):
        raise ValueError('fixture evidence handoff rejected after admission')

    monkeypatch.setattr(delivery_role_evidence, 'allocate', allocate)
    result = await delivery_activities.delivery_role(case.request)
    row, occupied = _attempt(case)
    names = sorted(path.name for path in case.folder.iterdir())
    (case.state / 'retry-folder-observation.json').write_text(json.dumps(
        {'attempt': row, 'result': result, 'names': names}, indent=2))
    assert result['finish_reason'] == 'prelaunch' and occupied == 0
    # retry_prelaunch and its resume readback both require this exact predicate.
    assert not any(case.folder.iterdir()), names




@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['preparation', 'before_thread_entry'])
async def test_cancelled_executor_waiter_joins_actual_native_worker_without_spin(
        native_role, monkeypatch, phase):
    case = native_role
    preparing, release, finished = threading.Event(), threading.Event(), threading.Event()
    captured = []
    cancelled_awaits = 0
    shield = asyncio.shield
    failure = case.owner._native_failure

    def prepare(request, folder):
        if phase == 'preparation':
            preparing.set()
            assert release.wait(10), 'actual preparation thread was not released'
        return 'fixture', {'PATH': '/usr/bin:/bin'}

    if phase == 'before_thread_entry':
        loop = asyncio.get_running_loop()
        submit = loop.run_in_executor

        def run_in_executor(executor, function, *args):
            values = (function, *args, *getattr(function, 'args', ()))
            if any(getattr(value, '__name__', None) == 'monitor' for value in values):
                def delayed():
                    preparing.set()
                    assert release.wait(10), 'actual queued executor work was not released'
                    return function(*args)
                return submit(executor, delayed)
            return submit(executor, function, *args)

        monkeypatch.setattr(loop, 'run_in_executor', run_in_executor)

    def native_failure(*args, **options):
        try:
            return failure(*args, **options)
        finally:
            finished.set()

    def bounded_shield(future):
        nonlocal cancelled_awaits
        if not captured:
            captured.append(future)
        if future.cancelled():
            cancelled_awaits += 1
            if cancelled_awaits == 30:
                raise RuntimeError('bounded probe: native cancel-join spins on cancelled waiter')
        return shield(future)

    from devflow_temporal import supervisor
    monkeypatch.setattr(supervisor, 'prepare_native_role', prepare)
    monkeypatch.setattr(case.owner, '_native_failure', native_failure)
    monkeypatch.setattr(asyncio, 'shield', bounded_shield)
    task = asyncio.create_task(delivery_activities.delivery_role(case.request))
    try:
        await _wait(preparing)
        assert captured and isinstance(captured[0], asyncio.Task)
        captured[0].cancel()
        await asyncio.sleep(0.05)
        (case.state / 'cancelled-executor-observation.json').write_text(json.dumps(
            {'cancelled_shield_awaits': cancelled_awaits, 'outer_done': task.done(),
             'actual_worker_terminal': finished.is_set()}, indent=2))
        assert not task.done(), 'cancelled proxy abandoned or spun instead of joining actual worker'
        row, occupied = _attempt(case)
        assert occupied == (1 if phase == 'preparation' else 0) and not finished.is_set()
        assert row['state'] == ('starting' if phase == 'preparation' else 'queued')
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and cancelled_awaits <= 1
        row, occupied = _attempt(case)
        assert row['state'] == 'finished' and row['cleanup'] == 'confirmed' and occupied == 0
        assert not (case.state / 'invocations').exists()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await _wait(finished)




@pytest.mark.asyncio
@pytest.mark.parametrize('checkpoint', ['transaction', 'capacity_count'])
async def test_queued_replacement_while_admission_transaction_waits(
        native_role, monkeypatch, checkpoint):
    case = native_role
    entered, release = threading.Event(), threading.Event()
    admitting = threading.local()
    connect, acquire = case.store._connect, case.owner._enter_capacity

    class AdmissionDB:
        def __init__(self, db):
            self.db = db

        def execute(self, sql, *args):
            boundary = (sql == 'BEGIN IMMEDIATE' if checkpoint == 'transaction'
                        else 'SELECT COUNT(*) FROM delivery_attempts' in sql)
            if boundary and getattr(admitting, 'active', False):
                entered.set()
                assert release.wait(10), 'waiting admission transaction was not released'
            return self.db.execute(sql, *args)

    @contextmanager
    def controlled():
        with connect() as db:
            yield AdmissionDB(db)

    def enter(key, **options):
        admitting.active = True
        try:
            return acquire(key, **options)
        finally:
            admitting.active = False

    monkeypatch.setattr(case.store, '_connect', controlled)
    monkeypatch.setattr(case.owner, '_enter_capacity', enter)
    monkeypatch.setattr(activity, 'in_activity', lambda: True)
    monkeypatch.setattr(activity, 'heartbeat', lambda *args: None)
    monkeypatch.setattr(activity, 'cancellation_details', lambda: SimpleNamespace(
        cancel_requested=False, timed_out=True, not_found=False, worker_shutdown=False))
    task = asyncio.create_task(delivery_role(case.request))
    try:
        await _wait(entered)
        with sqlite3.connect(f'file:{case.store.config.tracking_db}?mode=ro', uri=True) as db:
            db.row_factory = sqlite3.Row
            before = dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                                     (case.key,)).fetchone())
        assert before['state'] == 'queued' and before['started_at'] is None
        task.cancel()
        await asyncio.sleep(0.05)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        with connect() as db:
            after = dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                                    (case.key,)).fetchone())
        (case.state / 'queued-transaction-observation.json').write_text(json.dumps(
            {'before': before, 'after': after}, indent=2))
        assert after == before, after
        assert not (case.state / 'invocations').exists()
        retry = await delivery_role(case.request)
        assert retry['finish_reason'] == 'fixture'
        assert (case.state / 'invocations').read_text().splitlines() == ['one']
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def _poll_until(predicate):
    deadline = time.monotonic() + 10
    while not predicate():
        assert time.monotonic() < deadline, 'fixture did not reach its controlled boundary'
        await asyncio.sleep(0.02)
