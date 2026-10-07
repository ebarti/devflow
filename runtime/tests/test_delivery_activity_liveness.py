from __future__ import annotations

import ast
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
    # Isolate the original liveness policy from the independent check-slot patch.
    monkeypatch.setattr(delivery_workflow.workflow, 'patched',
                        lambda flag: flag == 'delivery-activity-liveness-v1')
    await DeliveryWorkflow()._activity(name, {
        'spec': {'policy': {'execution_backend': 'native-macos'}},
    })
    assert observed.get('heartbeat_timeout') == timedelta(seconds=15)
    assert observed['retry_policy'].maximum_attempts == 3
    assert observed['schedule_to_close_timeout'] == timedelta(hours=2)


@pytest.mark.asyncio
@pytest.mark.parametrize('liveness,slots', [(False, False), (True, False),
                                          (False, True), (True, True)])
@pytest.mark.parametrize('backend', ['native-macos', 'fake'])
@pytest.mark.parametrize('name', ['delivery_intake', 'delivery_role', 'delivery_checks',
                                 'delivery_precheck', 'delivery_browser_qa',
                                 'delivery_baseline_checks'])
async def test_independent_activity_patches_preserve_each_reviewed_policy(
    monkeypatch, name, backend, liveness, slots,
):
    from devflow_temporal import delivery_workflow

    observed = {}
    flags = {'delivery-activity-liveness-v1': liveness, 'delivery-check-slots-v1': slots}
    async def execute(_name, _request, **options):
        observed.update(options)
    monkeypatch.setattr(delivery_workflow.workflow, 'execute_activity', execute)
    monkeypatch.setattr(delivery_workflow.workflow, 'patched', lambda flag: flags.get(flag, False))
    await DeliveryWorkflow()._activity(name, {'spec': {'policy': {'execution_backend': backend}}})
    native_liveness = backend == 'native-macos' and liveness
    slotted = slots and name in {'delivery_checks', 'delivery_precheck',
                                'delivery_browser_qa', 'delivery_baseline_checks'}
    # PR77's independent check policy controls heartbeat/cancellation when active;
    # PR73 still controls native retries and the unchanged overall activity deadline.
    if slotted:
        assert observed['heartbeat_timeout'] == timedelta(seconds=30)
        assert observed['cancellation_type'] == (
            delivery_workflow.workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED)
    else:
        assert 'cancellation_type' not in observed
        if native_liveness:
            assert observed['heartbeat_timeout'] == timedelta(seconds=15)
        else:
            assert 'heartbeat_timeout' not in observed
    assert observed['start_to_close_timeout'] == timedelta(hours=2)
    assert observed['retry_policy'].maximum_attempts == (3 if native_liveness else 1)
    if native_liveness:
        assert observed['schedule_to_close_timeout'] == timedelta(hours=2)
        assert observed['retry_policy'].initial_interval == timedelta(seconds=1)
        assert observed['retry_policy'].maximum_interval == timedelta(seconds=10)
        assert observed['retry_policy'].non_retryable_error_types == [
            'ValueError', 'TypeError', 'PermissionError', 'NativeProcessUnknown']
    else:
        assert 'schedule_to_close_timeout' not in observed


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
from socketserver import TCPServer
state=Path(sys.argv[1])
print('fixture-child-entered',flush=True)
servers=[]
class FixtureHTTPServer(HTTPServer):
    def server_bind(self):
        # Fixed numeric loopback probes need binding/listening, not reverse DNS.
        TCPServer.server_bind(self)
        self.server_name,self.server_port=self.server_address[:2]
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.end_headers(); self.wfile.write(b'ready')
    def log_message(self,*args): pass
for key in ['QA_API_PORT','QA_WEB_PORT']:
    if key in os.environ:
        print('fixture-child-binding:'+key,flush=True)
        server=FixtureHTTPServer(('127.0.0.1',int(os.environ[key])),Handler)
        print('fixture-child-bound:'+key,flush=True)
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


WORKER_DRIVER = 'import os,time\n' + NATIVE_DRIVER.split('async def execute():')[0].replace(
    "options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]",
    "if request.get('role'): "
    "options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]",
).replace("options['timeout']=20", "options['timeout']=90") + '''
from devflow_temporal import delivery_sandbox

def trace_fixture_boundary(name, execute):
    def observed(*args, **kwargs):
        print(json.dumps({'boundary':name,'event':'enter','time':time.monotonic()}),flush=True)
        try:
            result=execute(*args,**kwargs)
        except Exception as exc:
            print(json.dumps({'boundary':name,'event':'error','type':type(exc).__name__,
                'message':str(exc)[:700],'time':time.monotonic()}),flush=True)
            raise
        details=({k:result.get(k) for k in ['state','cleanup','reason']} if isinstance(result,dict)
                 else {})
        print(json.dumps({'boundary':name,'event':'complete',**details,
            'time':time.monotonic()}),flush=True)
        return result
    return observed

def check_profile(spec,checkout,folder,check,**kwargs):
    return 'fixture',{'PATH':'/usr/bin:/bin'}
def browser_profile(spec,checkout,folder,scratch,qa):
    profile=folder/'browser-qa.sb'
    profile.write_text('offline socket fixture\\n')
    return profile,{'PATH':'/usr/bin:/bin',**{k:str(v) for k,v in qa['ports'].items()}}
delivery_sandbox.prepare_native_check=check_profile
delivery_sandbox.native_check_argv=lambda spec,profile,cwd,argv: argv
import devflow_temporal.delivery_browser_qa as browser
browser.prepare_browser_qa=trace_fixture_boundary('browser-profile',browser_profile)
browser._ports_free=trace_fixture_boundary('browser-ports-free',browser._ports_free)
resources=__import__('devflow_temporal.delivery_resources',fromlist=['RunResources'])
resources.RunResources.browser_scratch=trace_fixture_boundary(
    'browser-scratch',resources.RunResources.browser_scratch)
for method in ['run_browser_qa','gate_checkout','_run_native_check']:
    setattr(delivery_broker.DeliveryBroker,method,trace_fixture_boundary(
        method,getattr(delivery_broker.DeliveryBroker,method)))
delivery_native_process.NativeProcess=trace_fixture_boundary(
    'native-construction',delivery_native_process.NativeProcess)
sys.path.insert(0,sys.argv[4])
from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow
from temporalio import activity
original_heartbeat=activity.heartbeat
def observed_heartbeat(*details):
    if details and isinstance(details[0],dict):
        print(json.dumps({'boundary':'heartbeat','stage':details[0].get('stage'),
            'time':time.monotonic()}),flush=True)
    return original_heartbeat(*details)
activity.heartbeat=observed_heartbeat
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
from devflow_temporal import delivery_activities
delivery_activities._context=trace_fixture_boundary('activity-context',delivery_activities._context)
original_execute_check=delivery_activities._execute_check
async def observed_execute_check(*args,**kwargs):
    print(json.dumps({'boundary':'check-executor','event':'enter','time':time.monotonic()}),flush=True)
    try:
        result=await original_execute_check(*args,**kwargs)
    except Exception as exc:
        print(json.dumps({'boundary':'check-executor','event':'error','type':type(exc).__name__,
            'message':str(exc)[:700],'time':time.monotonic()}),flush=True)
        raise
    print(json.dumps({'boundary':'check-executor','event':'complete','time':time.monotonic(),
        **{k:result.get(k) for k in ['state','cleanup','reason']}}),flush=True)
    return result
delivery_activities._execute_check=observed_execute_check
async def main():
    client=await Client.connect(sys.argv[3])
    async with Worker(client,task_queue='activity-worker-loss',
        workflows=[ActivityLivenessWorkflow],activities=[delivery_role,delivery_checks,
            delivery_precheck,delivery_baseline_checks,delivery_browser_qa,delivery_intake,
            block_loop]) as worker:
        while not worker.is_running:
            await asyncio.sleep(0.05)
        print(f'fixture-worker-ready:{os.getpid()}',flush=True)
        await asyncio.Event().wait()
asyncio.run(main())
'''


async def _wait_for_fixture_worker_ready(worker, log_path):
    # Cold subprocess imports/connection get their own bound, outside activity timeouts.
    deadline = time.monotonic()+60
    signal = f'fixture-worker-ready:{worker.pid}'
    while True:
        diagnostic = log_path.read_text()
        exit_code = worker.poll()
        assert exit_code is None, (
            f'fixture worker initialization exited ({exit_code}):\n{diagnostic}')
        if signal in diagnostic.splitlines():
            return
        assert time.monotonic()<deadline, f'fixture worker initialization timed out:\n{diagnostic}'
        await asyncio.sleep(0.05)


def _fixture_native_start_diagnostic(log_path, state):
    # These are retained synthetic fixture outputs, never a claim about missing journals.
    logs = {'worker': log_path.read_text()[-12000:]}
    for name in ('process.log', 'monitor.log'):
        for path in sorted(state.rglob(name))[:4]:
            if path.is_symlink() or not path.is_relative_to(state):
                continue
            logs[str(path.relative_to(state))] = path.read_text()[-4000:]
    return logs


async def _wait_for_fixture_native_start(worker, log_path, state, handle):
    completion = asyncio.create_task(handle.result())
    deadline = time.monotonic()+20
    try:
        while not (state / 'started').exists():
            assert worker.poll() is None, _fixture_native_start_diagnostic(log_path, state)
            if completion.done():
                try:
                    result = completion.result()
                except Exception as exc:
                    raise AssertionError(('workflow failed before native start', str(exc),
                        _fixture_native_start_diagnostic(log_path, state))) from exc
                raise AssertionError(('workflow completed before native start', result,
                    _fixture_native_start_diagnostic(log_path, state)))
            assert time.monotonic()<deadline, (
                'native command did not start within 20 seconds after worker readiness',
                _fixture_native_start_diagnostic(log_path, state))
            await asyncio.sleep(0.05)
    finally:
        # Stop only the local result observer, never the workflow or original command.
        completion.cancel()
        await asyncio.gather(completion, return_exceptions=True)


@pytest.mark.asyncio
async def test_fixture_worker_announces_readiness_after_sdk_validation():
    # Execute only main's control flow with opaque doubles: no imports, server or child.
    output, entered = [], []

    class ClientDouble:
        @staticmethod
        async def connect(_address):
            entered.append('connected')
            return object()

    class WorkerDouble:
        def __init__(self, *_args, **_kwargs):
            self.polls = 0

        @property
        def is_running(self):
            self.polls += 1
            if self.polls == 3:
                entered.append('running')
                return True
            assert not output
            return False

        async def __aenter__(self):
            assert entered == ['connected']
            entered.append('worker')
            return self

        async def __aexit__(self, *_args):
            pass

    class EventDouble:
        async def wait(self):
            assert entered == ['connected', 'worker', 'running']
            assert output == [('fixture-worker-ready:123', True)]

    async def sleep(_seconds):
        assert not output and entered == ['connected', 'worker']

    tree = ast.parse(WORKER_DRIVER)
    main = next(node for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
                and node.name == 'main')
    namespace = {'Client': ClientDouble, 'Worker': WorkerDouble,
                 'sys': SimpleNamespace(argv=['driver', 'input', 'provider', 'address']),
                 'os': SimpleNamespace(getpid=lambda: 123),
                 'asyncio': SimpleNamespace(Event=EventDouble, sleep=sleep),
                 'print': lambda text, *, flush: output.append((text, flush))}
    namespace.update({name: object() for name in [
        'ActivityLivenessWorkflow', 'delivery_role', 'delivery_checks', 'delivery_precheck',
        'delivery_baseline_checks', 'delivery_browser_qa', 'delivery_intake', 'block_loop']})
    exec(compile(ast.Module(body=[main], type_ignores=[]), '<fixture-main>', 'exec'), namespace)
    await namespace['main']()


def test_fixture_worker_readiness_precedes_workflow_submission():
    source = ast.parse(Path(__file__).read_text())
    fixture = next(node for node in source.body if isinstance(node, ast.AsyncFunctionDef)
                   and node.name == 'test_real_temporal_worker_loss_reuses_original_native_command')
    submission = min(node.lineno for node in ast.walk(fixture)
                     if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                     and node.func.attr == 'start_workflow')
    readiness = [node.lineno for node in ast.walk(fixture)
                 if isinstance(node, ast.Await) and isinstance(node.value, ast.Call)
                 and isinstance(node.value.func, ast.Name)
                 and node.value.func.id == '_wait_for_fixture_worker_ready']
    assert len(readiness) == 2 and min(readiness) < submission, (
        'workflow/activity deadline starts before worker initialization is observed')


@pytest.mark.asyncio
async def test_fixture_worker_cold_start_is_separate_from_native_start(monkeypatch, tmp_path):
    clock, sleeps = [0.0], []
    log = tmp_path / 'worker.log'
    log.write_text('fixture-worker-ready:999\n')  # A foreign PID cannot satisfy readiness.

    async def sleep(seconds):
        sleeps.append(seconds)
        clock[0] += 1
        if clock[0] == 25:
            log.write_text('fixture-worker-ready:123\n')

    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(asyncio, 'sleep', sleep)
    await _wait_for_fixture_worker_ready(SimpleNamespace(pid=123, poll=lambda: None), log)
    assert clock[0] == 25 and len(sleeps) == 25
    # Native startup retains its own 20s allowance, not the spent bootstrap clock.
    fixture = next(node for node in ast.parse(Path(__file__).read_text()).body
                   if isinstance(node, ast.AsyncFunctionDef)
                   and node.name == '_wait_for_fixture_native_start')
    deadline = next(node for node in ast.walk(fixture) if isinstance(node, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == 'deadline'
                            for target in node.targets))
    assert ast.unparse(deadline.value) == 'time.monotonic() + 20'


@pytest.mark.asyncio
@pytest.mark.parametrize('exit_code', [None, 7])
async def test_fixture_worker_readiness_reports_bounded_startup_failure(
    monkeypatch, tmp_path, exit_code,
):
    clock = [0.0]
    log = tmp_path / 'worker.log'
    log.write_text('initialization diagnostic\n')

    async def sleep(_seconds):
        clock[0] += 1

    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(asyncio, 'sleep', sleep)
    with pytest.raises(AssertionError, match=(
        'initialization timed out' if exit_code is None else r'initialization exited \(7\)'
    )) as error:
        await _wait_for_fixture_worker_ready(SimpleNamespace(pid=123, poll=lambda: exit_code), log)
    assert 'initialization diagnostic' in str(error.value)
    assert clock[0] == (60 if exit_code is None else 0)


@pytest.mark.asyncio
async def test_fixture_worker_readiness_rejects_exited_worker_with_signal(tmp_path):
    log = tmp_path / 'worker.log'
    log.write_text('fixture-worker-ready:123\n')
    with pytest.raises(AssertionError, match=r'initialization exited \(0\)'):
        await _wait_for_fixture_worker_ready(SimpleNamespace(pid=123, poll=lambda: 0), log)


def test_fixture_browser_profile_exposes_owned_startup_boundary():
    output = []
    profile = SimpleNamespace(write_text=lambda _text: None)

    class Folder:
        def __truediv__(self, _name):
            return profile

    tree = ast.parse(WORKER_DRIVER)
    definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef)
                   and node.name in {'browser_profile', 'trace_fixture_boundary'}]
    assignment = next(node for node in tree.body if isinstance(node, ast.Assign)
                      and any(ast.unparse(target) == 'browser.prepare_browser_qa'
                              for target in node.targets))
    browser = SimpleNamespace()
    namespace = {'browser': browser, 'print': lambda text, *, flush: output.append(text),
                 'time': SimpleNamespace(monotonic=lambda: 1), 'json': json}
    exec(compile(ast.Module(body=[*definitions, assignment], type_ignores=[]),
                 '<browser-profile-boundary>', 'exec'), namespace)
    result = browser.prepare_browser_qa({}, None, Folder(), None, {'ports': {'PORT': 123}})
    assert result == (profile, {'PATH': '/usr/bin:/bin', 'PORT': '123'})
    assert output and 'browser-profile' in output[0] and 'enter' in output[0]
    assert 'complete' in output[-1]


def test_fixture_observes_terminal_workflow_during_native_start_wait():
    fixture = next(node for node in ast.parse(Path(__file__).read_text()).body
                   if isinstance(node, ast.AsyncFunctionDef)
                   and node.name == 'test_real_temporal_worker_loss_reuses_original_native_command')
    calls = [node for node in ast.walk(fixture) if isinstance(node, ast.Await)
             and isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name)
             and node.value.func.id == '_wait_for_fixture_native_start']
    assert len(calls) == 1, 'native-start waiter ignores early returned gate outcomes'


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', [False, True])
async def test_fixture_native_start_reports_early_gate_result(tmp_path, failure):
    log = tmp_path / 'worker.log'
    log.write_text('fixture-worker-ready:123\n')
    state = tmp_path / 'state'
    state.mkdir()

    async def result():
        if failure:
            raise RuntimeError('opaque gate rejection')
        return {'state': 'unknown', 'reason': 'ValueError'}

    handle = SimpleNamespace(result=result)
    worker = SimpleNamespace(poll=lambda: None)
    with pytest.raises(AssertionError, match=(
        'workflow failed before native start' if failure
        else 'workflow completed before native start'
    )) as error:
        await _wait_for_fixture_native_start(worker, log, state, handle)
    assert ('opaque gate rejection' if failure else 'ValueError') in str(error.value)
    assert 'fixture-worker-ready:123' in str(error.value)
    assert list(state.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize('started', [False, True])
async def test_fixture_native_start_keeps_deadline_and_only_cancels_observer(
    monkeypatch, tmp_path, started,
):
    clock, cancelled = [0], []
    log, state = tmp_path / 'worker.log', tmp_path / 'state'
    log.write_text('fixture-worker-ready:123\n')
    state.mkdir()
    native = state / 'browser-qa' / 'native'
    native.mkdir(parents=True)
    (native / 'process.log').write_text('fixture-child-binding:QA_API_PORT\n')
    (native / 'monitor.log').write_text('opaque monitor diagnostic\n')
    yielded = asyncio.sleep

    async def sleep(_seconds):
        clock[0] += 1
        if started and clock[0] == 3:
            (state / 'started').touch()
        await yielded(0)

    async def result():
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.append('observer')

    monkeypatch.setattr(time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(asyncio, 'sleep', sleep)
    # There is deliberately no workflow cancel method available to this helper.
    handle = SimpleNamespace(result=result)
    worker = SimpleNamespace(poll=lambda: None)
    if started:
        await _wait_for_fixture_native_start(worker, log, state, handle)
        assert clock[0] == 3
    else:
        with pytest.raises(AssertionError, match='within 20 seconds') as error:
            await _wait_for_fixture_native_start(worker, log, state, handle)
        assert 'fixture-child-binding:QA_API_PORT' in str(error.value)
        assert 'opaque monitor diagnostic' in str(error.value)
        assert clock[0] == 20
    assert cancelled == ['observer']
    assert (native / 'process.log').read_text() == 'fixture-child-binding:QA_API_PORT\n'


def test_fixture_boundary_diagnostics_preserve_original_result_and_exception():
    tree = ast.parse(WORKER_DRIVER)
    definition = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                      and node.name == 'trace_fixture_boundary')
    output = []
    namespace = {'json': json, 'time': SimpleNamespace(monotonic=lambda: 1),
                 'print': lambda text, *, flush: output.append(json.loads(text))}
    exec(compile(ast.Module(body=[definition], type_ignores=[]), '<boundary>', 'exec'), namespace)
    expected, calls = {'state': 'unknown', 'reason': 'ValueError'}, []

    def execute(*args, **kwargs):
        calls.append((args, kwargs))
        return expected

    observed = namespace['trace_fixture_boundary']('owned-boundary', execute)
    assert observed('original', candidate='unchanged') is expected
    assert calls == [(('original',), {'candidate': 'unchanged'})]
    assert [row['event'] for row in output] == ['enter', 'complete']
    assert output[-1]['state'] == 'unknown' and output[-1]['reason'] == 'ValueError'
    original = ValueError('original rejection')

    def reject():
        raise original

    with pytest.raises(ValueError) as error:
        namespace['trace_fixture_boundary']('owned-boundary', reject)()
    assert error.value is original
    assert output[-1]['event'] == 'error' and output[-1]['type'] == 'ValueError'


@pytest.mark.parametrize('fail_at', [None, 'bind', 'listen'])
def test_fixture_http_constructor_keeps_numeric_loopback_bind_without_dns(monkeypatch, fail_at):
    from http.server import HTTPServer
    from socketserver import TCPServer

    sockets, lookups = [], []
    original_error = OSError('opaque socket rejection')

    class SocketDouble:
        def __init__(self):
            self.operations = []
            sockets.append(self)

        def setsockopt(self, *_args):
            self.operations.append('setsockopt')

        def bind(self, address):
            self.address = address
            self.operations.append(('bind', address))
            if fail_at == 'bind':
                raise original_error

        def getsockname(self):
            self.operations.append('getsockname')
            return self.address

        def listen(self, backlog):
            self.operations.append(('listen', backlog))
            if fail_at == 'listen':
                raise original_error

        def close(self):
            self.operations.append('close')

    def forbidden_lookup(name):
        lookups.append(name)
        raise AssertionError('reverse DNS is outside the numeric loopback fixture contract')

    monkeypatch.setattr(socket, 'socket', lambda *_args: SocketDouble())
    monkeypatch.setattr(socket, 'getfqdn', forbidden_lookup)
    tree = ast.parse(CHECK_PROGRAM)
    definition = [node for node in tree.body if isinstance(node, ast.ClassDef)
                  and node.name == 'FixtureHTTPServer']
    constructor = next(node.value for node in ast.walk(tree) if isinstance(node, ast.Assign)
                       and any(isinstance(target, ast.Name) and target.id == 'server'
                               for target in node.targets))
    namespace = {'HTTPServer': HTTPServer, 'TCPServer': TCPServer, 'Handler': object()}
    definition_code = compile(
        ast.Module(body=definition, type_ignores=[]), '<fixture-server>', 'exec')
    exec(definition_code, namespace)
    constructor_code = compile(ast.Expression(constructor), '<fixture-constructor>', 'eval')
    servers = []
    for key, port in [('QA_API_PORT', 12345), ('QA_WEB_PORT', 23456)]:
        namespace.update(key=key, os=SimpleNamespace(environ={key: str(port)}))
        if fail_at:
            with pytest.raises(OSError) as error:
                eval(constructor_code, namespace)
            assert error.value is original_error and lookups == []
            assert sockets[-1].operations[-1] == 'close'
            return
        server = eval(constructor_code, namespace)
        servers.append(server)
        assert server.server_name == '127.0.0.1' and server.server_port == port
        assert sockets[-1].operations == [
            'setsockopt', ('bind', ('127.0.0.1', port)), 'getsockname',
            ('listen', server.request_queue_size)]
        assert type(server).server_activate is TCPServer.server_activate
        assert type(server).serve_forever is HTTPServer.serve_forever
    assert lookups == []
    for server in servers:
        server.server_close()
    assert all(child.operations[-1] == 'close' for child in sockets)


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
            await _wait_for_fixture_worker_ready(workers[0], tmp_path / 'first-worker.log')
            handle = await environment.client.start_workflow(
                ActivityLivenessWorkflow.run, {'activity_name': name, 'request': request,
                    **({'hours': 40/3600} if interruption == 'deadline' else {})},
                id='worker-loss', task_queue='activity-worker-loss',
            )
            await _wait_for_fixture_native_start(
                workers[0], tmp_path / 'first-worker.log', state, handle)
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
            await _wait_for_fixture_worker_ready(workers[1], tmp_path / 'replacement-worker.log')
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
