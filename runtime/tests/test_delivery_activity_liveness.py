from __future__ import annotations

import asyncio
import json
import shutil
import socket
import subprocess
import sys
import time
from datetime import timedelta
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow
from temporalio import activity
from temporalio.client import WorkflowFailureError, WorkflowHistory
from temporalio.testing import ActivityEnvironment, WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_api import api_fixture as api_fixture
from test_delivery_role_reattach import FIXED_NATIVE_PROVIDER, NATIVE_DRIVER

from devflow_temporal import delivery_activities, delivery_baseline
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.asyncio
@pytest.mark.parametrize('name', [
    'delivery_checks', 'delivery_precheck', 'delivery_browser_qa', 'delivery_baseline_checks',
])
async def test_long_gate_activity_emits_liveness_heartbeat(monkeypatch, tmp_path, name):
    def slow(*_args):
        time.sleep(0.05)
        return {'state': 'passed'}

    broker = SimpleNamespace(run_checks=slow, run_prechecks=slow, run_browser_qa=slow)
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path))
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (store, broker))
    monkeypatch.setattr(delivery_baseline, 'run_baseline_checks', slow)
    environment = ActivityEnvironment()
    heartbeats = []
    environment.on_heartbeat = lambda *details: heartbeats.append(details)
    result = await environment.run(getattr(delivery_activities, name), {
        'spec': {'run_id': 'liveness', 'provider': 'fake'}, 'iteration': 0,
        'candidate': {'id': 'input'},
    })
    assert result['state'] == 'passed'
    assert heartbeats, 'long-running gate sent no heartbeat'
    assert heartbeats[0][0]['run_id'] == 'liveness'


@pytest.mark.asyncio
@pytest.mark.parametrize('name', [
    'delivery_intake', 'delivery_role', 'delivery_checks', 'delivery_precheck',
    'delivery_browser_qa', 'delivery_baseline_checks',
])
async def test_native_activity_options_bound_worker_loss_retries(monkeypatch, name):
    from devflow_temporal import delivery_workflow

    observed = {}

    async def execute(_name, _request, **options):
        observed.update(options)
        return {'state': 'passed'}

    monkeypatch.setattr(delivery_workflow.workflow, 'execute_activity', execute)
    monkeypatch.setattr(delivery_workflow.workflow, 'patched', lambda _name: True)
    await DeliveryWorkflow()._activity(name, {
        'spec': {'policy': {'execution_backend': 'native-macos'}},
    })
    assert observed.get('heartbeat_timeout') == timedelta(seconds=15)
    assert observed['retry_policy'].maximum_attempts == 3
    assert observed['schedule_to_close_timeout'] == timedelta(hours=2)


@pytest.mark.asyncio
async def test_legacy_history_keeps_original_activity_options(monkeypatch):
    from devflow_temporal import delivery_workflow

    observed = {}

    async def execute(_name, _request, **options):
        observed.update(options)

    monkeypatch.setattr(delivery_workflow.workflow, 'execute_activity', execute)
    monkeypatch.setattr(delivery_workflow.workflow, 'patched', lambda _name: False)
    await DeliveryWorkflow()._activity('delivery_role', {
        'spec': {'policy': {'execution_backend': 'native-macos'}},
    })
    assert 'heartbeat_timeout' not in observed
    assert observed['retry_policy'].maximum_attempts == 1


@pytest.mark.asyncio
async def test_replay_actual_previous_activity_history():
    path = (Path(__file__).parent / 'fixtures/activity_liveness'
            / 'activity-liveness-previous-history.json')
    history = WorkflowHistory.from_json('record-activity-liveness', path.read_text())
    await Replayer(workflows=[ActivityLivenessWorkflow]).replay_workflow(history)


@pytest.mark.asyncio
async def test_blocking_intake_preparation_keeps_heartbeating(monkeypatch):
    candidate = {'id': 'input'}

    def context(_spec):
        time.sleep(6)
        return object(), SimpleNamespace(checkout=Path('.'), candidate=lambda: candidate)

    async def run(_request):
        return {'status': 'pass', 'findings': []}

    monkeypatch.setattr(delivery_activities, '_context', context)
    monkeypatch.setattr(delivery_activities, 'get_supervisor',
                        lambda _store: SimpleNamespace(run=run))
    environment = ActivityEnvironment()
    heartbeats = []
    environment.on_heartbeat = lambda *details: heartbeats.append(details)
    result = await environment.run(delivery_activities.delivery_intake, {
        'spec': {'run_id': 'liveness', 'provider': 'fake'}, 'iteration': 0,
        'candidate': candidate,
    })
    assert result['status'] == 'pass'
    assert len(heartbeats) >= 2


@pytest.mark.asyncio
@pytest.mark.parametrize('outcome,expected_attempts', [
    ('transient', 3), ('permanent', 1), ('eventual', 2), ('findings', 1),
])
async def test_temporal_retries_are_finite_and_preserve_gate_outcomes(outcome, expected_attempts):
    attempts = []

    @activity.defn(name='delivery_role')
    async def probe(_request):
        attempt = activity.info().attempt
        attempts.append(attempt)
        if outcome == 'permanent':
            raise ValueError('invalid role input')
        if outcome == 'transient' or outcome == 'eventual' and attempt == 1:
            raise RuntimeError('temporary transport failure')
        return {'status': 'findings' if outcome == 'findings' else 'pass'}

    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        async with Worker(environment.client, task_queue='activity-retry-bound',
                          workflows=[ActivityLivenessWorkflow], activities=[probe]):
            handle = await environment.client.start_workflow(
                ActivityLivenessWorkflow.run, {
                    'activity_name': 'delivery_role',
                    'request': {'spec': {'policy': {'execution_backend': 'native-macos'}}},
                }, id='retry-bound', task_queue='activity-retry-bound',
            )
            if outcome in {'transient', 'permanent'}:
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
            else:
                result = await handle.result()
                assert result['status'] == ('findings' if outcome == 'findings' else 'pass')
            history = await handle.fetch_history()
            await Replayer(workflows=[ActivityLivenessWorkflow]).replay_workflow(history)
    assert attempts == list(range(1, expected_attempts+1))


CHECK_PROGRAM = '''
import os,sys,time,threading,urllib.request
from pathlib import Path
from http.server import BaseHTTPRequestHandler,HTTPServer
state=Path(sys.argv[1])
servers=[]
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'ready')
    def log_message(self,*args): pass
for key in ['QA_API_PORT','QA_WEB_PORT']:
    if key in os.environ:
        server=HTTPServer(('127.0.0.1',int(os.environ[key])),Handler)
        servers.append(server)
        threading.Thread(target=server.serve_forever,daemon=True).start()
with (state/'invocations').open('a') as stream: stream.write('one\\n')
print('started',flush=True)
(state/'started').touch()
while not (state/'release').exists(): time.sleep(0.02)
if servers:
    for server in servers:
        assert urllib.request.urlopen('http://127.0.0.1:'+str(server.server_port)).read()==b'ready'
        server.shutdown(); server.server_close()
    print('2 passed',flush=True)
else:
    assert 1+1==2
    print('1 passed',flush=True)
'''


WORKER_DRIVER = NATIVE_DRIVER.split('async def execute():')[0].replace(
    "options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]",
    "if request.get('role'): "
    "options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]",
).replace("options['timeout']=20", "options['timeout']=90") + '''
from devflow_temporal import delivery_sandbox
def check_profile(spec,checkout,folder,check,**kwargs):
    return 'fixture',{'PATH':'/usr/bin:/bin'}
def browser_profile(spec,checkout,folder,scratch,qa):
    profile=folder/'browser-qa.sb'
    profile.write_text('offline socket fixture\\n')
    return profile,{'PATH':'/usr/bin:/bin',**{k:str(v) for k,v in qa['ports'].items()}}
delivery_sandbox.prepare_native_check=check_profile
delivery_sandbox.native_check_argv=lambda spec,profile,cwd,argv: argv
import devflow_temporal.delivery_browser_qa as browser
browser.prepare_browser_qa=browser_profile
sys.path.insert(0,sys.argv[4])
from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker
from devflow_temporal.delivery_activities import delivery_checks,delivery_precheck,delivery_intake
@activity.defn(name='blocking_loop_probe')
async def block_loop(_request):
    (state/'stall-started').touch()
    import time
    time.sleep(20)
    (state/'stall-finished').touch()
    return {}
from devflow_temporal.delivery_activities import delivery_baseline_checks,delivery_browser_qa
async def main():
    client=await Client.connect(sys.argv[3])
    async with Worker(client,task_queue='activity-worker-loss',
        workflows=[ActivityLivenessWorkflow],activities=[delivery_role,delivery_checks,
            delivery_precheck,delivery_baseline_checks,delivery_browser_qa,delivery_intake,block_loop]):
        await asyncio.Event().wait()
asyncio.run(main())
'''


@pytest.mark.asyncio
@pytest.mark.parametrize('stage,interruption', [
    (stage, 'worker_loss') for stage in [
        'implement', 'review', 'verify', 'checks', 'precheck', 'baseline_checks'
    ]
] + [
    pytest.param('browser_qa', 'worker_loss', marks=pytest.mark.skipif(
        sys.platform != 'darwin', reason='actual macOS TCP listener inspection required')),
    ('implement', 'loop_stall'), ('review', 'loop_stall'), ('verify', 'loop_stall'),
    ('intake', 'loop_stall'), ('implement', 'cancel'),
    ('implement', 'deadline'), ('implement', 'terminate'),
    ('review', 'dirty_result'), ('verify', 'dirty_result'),
])
async def test_real_temporal_worker_loss_reuses_original_native_command(
    api_fixture, tmp_path, stage, interruption,
):
    from devflow_temporal.delivery_native_process import stop_observed
    from devflow_temporal.delivery_resources import RunResources, read_private

    path, submission = api_fixture
    raw = json.loads(path.read_text())
    raw.update(provider='codex', execution_mode='trusted-local', codex_bin=str(
        distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'} for role in raw['roles']}
    if stage == 'intake':
        raw['roles']['intake'] = {'model': 'gpt-6.1-sol', 'effort': 'high'}
    repository = raw['repositories']['fixture']
    source = Path(repository['source_path'])
    program = source / 'liveness.py'
    program.write_text(CHECK_PROGRAM)
    for args in [('add', 'liveness.py'), ('commit', '-qm', 'test: liveness fixture')]:
        subprocess.run(['git', '-C', str(source), *args], check=True)
    repository['expected_base_sha'] = subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    state = Path(raw['state_root']) / 'runs' / submission['run_id']
    check = {'id': 'fixture-check', 'kind': 'test', 'argv': [sys.executable,
        'liveness.py', str(state)], 'test_count_regex': r'(\d+) passed', 'min_tests': 1}
    repository.update(prepublish_checks=[check], checks=[check], baseline_check_ids=[check['id']],
        required_ci=['test'], project_url='https://github.com/users/example/projects/1',
        assignee='example')
    if stage == 'browser_qa':
        ports = []
        for _ in range(2):
            with socket.socket() as lease:
                lease.bind(('127.0.0.1', 0))
                ports.append(lease.getsockname()[1])
        repository['browser_qa'] = {'id': 'socket-qa', 'argv': check['argv'],
            'ports': dict(zip(['QA_API_PORT', 'QA_WEB_PORT'], ports, strict=True)),
            'test_count_regex': check['test_count_regex'], 'min_tests': 2,
            'timeout_seconds': 30, 'artifact_paths': [], 'read_roots': [],
            'env': {'JOBCTRL_E2E_ISOLATED': '1'}}
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.spec(submission['run_id'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    if stage in {'review', 'verify'}:
        (broker.checkout / 'README.md').write_text('Feature for the read-only gate\n')
        subprocess.run(['git', '-C', str(broker.checkout), 'add', 'README.md'], check=True)
        subprocess.run(['git', '-C', str(broker.checkout), 'commit', '-qm',
                        'test: published candidate fixture'], check=True)
    request = {'spec': spec, 'iteration': 0, 'candidate': broker.candidate()}
    name = 'delivery_role' if stage in {'implement', 'review', 'verify'} else 'delivery_'+stage
    if name in {'delivery_role', 'delivery_intake'}:
        request['role'] = stage
    driver, provider = tmp_path / 'worker.py', tmp_path / 'provider.py'
    driver.write_text(WORKER_DRIVER)
    provider.write_text(FIXED_NATIVE_PROVIDER.replace(
        "(Path(request['workspace'])/'README.md').write_text('Changed by the fixed provider\\n')",
        "if request['role']=='implement': "
        "(Path(request['workspace'])/'README.md').write_text('Changed by the fixed provider\\n')",
    ))
    temporary_scratch = stage in {'review', 'verify'} and interruption == 'loop_stall'
    if temporary_scratch or interruption == 'dirty_result':
        text = provider.read_text().replace(
            "print('started',flush=True)",
            "(Path(request['workspace'])/'scratch-output.txt').write_text('temporary output')\n"
            "print('started',flush=True)",
        )
        if temporary_scratch:
            text = text.replace(
                "write_private(Path(request['result_path'])",
                "(Path(request['workspace'])/'scratch-output.txt').unlink()\n"
                "write_private(Path(request['result_path'])",
            )
        provider.write_text(text)
    input_path = tmp_path / 'input.json'
    input_path.write_text(json.dumps(request))
    workers, logs = [], []
    try:
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which('temporal'),
        ) as environment:
            argv = [sys.executable, '-I', str(driver), str(input_path), str(provider),
                    environment.client.service_client.config.target_host,
                    str(Path(__file__).parent)]
            logs.append((tmp_path / 'first-worker.log').open('wb'))
            workers.append(subprocess.Popen(argv, stdout=logs[0], stderr=subprocess.STDOUT))
            handle = await environment.client.start_workflow(
                ActivityLivenessWorkflow.run, {'activity_name': name, 'request': request,
                    **({'hours': 40/3600} if interruption == 'deadline' else {})},
                id='worker-loss', task_queue='activity-worker-loss',
            )
            deadline = time.monotonic()+20
            while not (state / 'started').exists():
                assert workers[0].poll() is None, (tmp_path / 'first-worker.log').read_text()
                assert time.monotonic()<deadline, (tmp_path / 'first-worker.log').read_text()
                await asyncio.sleep(0.05)
            if interruption == 'worker_loss':
                workers[0].kill()
                await asyncio.to_thread(workers[0].wait, 5)
            elif interruption in {'loop_stall', 'dirty_result'}:
                stall = await environment.client.start_workflow(
                    ActivityLivenessWorkflow.run, {
                        'activity_name': 'blocking_loop_probe', 'request': request,
                    }, id='loop-stall', task_queue='activity-worker-loss',
                )
                deadline = time.monotonic()+10
                while not (state / 'stall-started').exists():
                    assert time.monotonic()<deadline
                    await asyncio.sleep(0.05)
            elif interruption in {'deadline', 'terminate'}:
                if interruption == 'terminate':
                    await handle.terminate(reason='Fixture closes without a replacement activity')
                try:
                    early = await asyncio.wait_for(handle.result(), 55)
                except WorkflowFailureError:
                    pass
                else:
                    pytest.fail(f'Workflow finished before its deadline: {early}; '
                                f'worker log: {(tmp_path / "first-worker.log").read_text()}')
                # Let the live worker receive the deadline cancellation via heartbeat.
                await asyncio.sleep(15)
                # A physical SIGKILL without any replacement worker cannot run this
                # finalizer; that separate orphan-reconciliation case is not covered.
                assert workers[0].poll() is None
                (state / 'release').touch()
                deadline = time.monotonic()+10
                while True:
                    with store._connect() as db:
                        row = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
                    if row['state'] == 'finished':
                        break
                    assert time.monotonic()<deadline, (json.dumps(row),
                        (tmp_path / 'first-worker.log').read_text())
                    await asyncio.sleep(0.05)
                assert row['cleanup'] == 'confirmed' and row['finished_at']
                result = json.loads(row['result_json'])
                assert result['status'] == 'pass'
                assert result['session_id'] == 'fixed-native-session'
                assert result['native_process']['cancelled'] is False
                with store._connect() as db:
                    assert db.execute(
                        "SELECT COUNT(*) FROM delivery_attempts "
                        "WHERE state IN ('starting','running','unknown')").fetchone()[0] == 0
                assert (state / 'invocations').read_text().splitlines() == ['one']
                assert RunResources(spec).finalize('blocked')['resource_cleanup'] == 'confirmed'
                return
            else:
                store.project(spec['run_id'], phase='cancelling', execution_state='cancelling',
                              event_type='cancel_requested', message='Fixture user cancellation')
                await handle.cancel()
                with pytest.raises(WorkflowFailureError):
                    await asyncio.wait_for(handle.result(), 15)
                deadline = time.monotonic()+15
                while True:
                    paths = list(state.rglob('native-process.json'))
                    if paths and read_private(paths[0]).get('phase') == 'finished':
                        break
                    assert time.monotonic()<deadline
                    await asyncio.sleep(0.05)
                assert read_private(paths[0])['result']['cancelled'] is True
                assert not (paths[0].parent / 'result.json').exists()
                assert (state / 'invocations').read_text().splitlines() == ['one']
                assert RunResources(spec).finalize('cancelled')['resource_cleanup'] == 'confirmed'
                return
            logs.append((tmp_path / 'replacement-worker.log').open('wb'))
            workers.append(subprocess.Popen(argv, stdout=logs[1], stderr=subprocess.STDOUT))
            if interruption in {'loop_stall', 'dirty_result'}:
                await asyncio.wait_for(stall.result(), 30)
                # Let rejected heartbeats cancel the live first attempt before releasing its child.
                await asyncio.sleep(2)
                assert workers[0].poll() is None
            (state / 'release').touch()
            try:
                result = await asyncio.wait_for(handle.result(), 40)
            except TimeoutError:
                await handle.cancel()
                raise
            if interruption == 'dirty_result':
                assert result['status'] == 'blocked', result
                assert 'candidate or controller diff changed' in ' '.join(result['findings'])
            else:
                assert result.get('status', result.get('state')) in {'pass', 'passed'}, result
            history = await handle.fetch_history()
            started = [e.activity_task_started_event_attributes for e in history.events
                       if e.HasField('activity_task_started_event_attributes')]
            assert started[-1].attempt == 2
            assert (state / 'invocations').read_text().splitlines() == ['one']
            if stage == 'implement':
                assert (state / 'preparation-calls').read_text().splitlines() == ['one']
            if name in {'delivery_role', 'delivery_intake'}:
                assert result['session_id'] == 'fixed-native-session'
                assert result['native_process']['cancelled'] is False
            elif stage == 'browser_qa':
                assert result['cleanup'] == 'confirmed'
            else:
                assert result['results']
                assert all(row['cleanup'] == 'confirmed' for row in result['results'])
            await Replayer(workflows=[ActivityLivenessWorkflow]).replay_workflow(history)
            outcome = 'blocked' if interruption == 'dirty_result' else 'delivered'
            assert RunResources(spec).finalize(outcome)['resource_cleanup'] == 'confirmed'
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.terminate()
                try:
                    await asyncio.to_thread(worker.wait, 5)
                except subprocess.TimeoutExpired:
                    worker.kill()
                    await asyncio.to_thread(worker.wait, 5)
        for log in logs:
            log.close()
        for path in state.rglob('native-process.json'):
            journal = read_private(path)
            owned = {int(pid): value for pid, value in journal.get('owned', {}).items()}
            if journal.get('monitor'):
                owned[journal['monitor']['pid']] = journal['monitor']
            assert stop_observed(owned)
