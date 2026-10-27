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
from temporalio.testing import ActivityEnvironment
from test_delivery_api import api_fixture as api_fixture

from devflow_temporal import (
    delivery_native_process,
    supervisor,
)
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import get_supervisor

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
    monkeypatch.setattr(activity, 'cancellation_details', lambda: SimpleNamespace(
        cancel_requested=False, timed_out=True, not_found=False, worker_shutdown=False))
    environment = ActivityEnvironment()
    task = asyncio.create_task(environment.run(delivery_role, case.request))
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
        retry = await environment.run(delivery_role, case.request)
        assert retry['finish_reason'] == 'fixture'
        assert (case.state / 'invocations').read_text().splitlines() == ['one']
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
