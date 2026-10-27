from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import time
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import submit_before_readiness

from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import get_supervisor


@pytest.fixture
def role_fixture(api_fixture):
    path, submission = api_fixture
    raw = json.loads(path.read_text())
    raw['repositories']['fixture']['allowed_paths'].append('devflow-fake-change.txt')
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.spec(submission['run_id'])
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()['candidate']
    return store, broker, {'spec': spec, 'role': 'implement', 'iteration': 0,
                           'candidate': candidate}


@pytest.mark.asyncio
async def test_lost_activity_response_reuses_completed_implementation(role_fixture, monkeypatch):
    spawn = asyncio.create_subprocess_exec
    bootstrap = """
import runpy,sys
sys.path.insert(0,FIXTURE_SOURCE)
class FixtureOnlyImports:
    def find_spec(self, name, *args):
        if name.partition('.')[0] in {'agent_runtime_kit','openai_codex'}:
            raise ImportError('fake role imported provider SDK: '+name)
sys.meta_path.insert(0,FixtureOnlyImports())
runpy.run_module('devflow_temporal.role_runner',run_name='__main__')
""".replace('FIXTURE_SOURCE', repr(str(Path(__file__).resolve().parents[1] / 'src')))

    async def fixture_child(*argv, **kwargs):
        argv = list(argv)
        argv[argv.index(sys.executable)] = str(Path(sys.executable).resolve())
        argv[argv.index('-m'):] = ['-c', bootstrap, argv[-1]]
        return await spawn(*argv, **kwargs)

    monkeypatch.setattr(asyncio, 'create_subprocess_exec', fixture_child)
    store, broker, request = role_fixture
    first = await delivery_role(request)
    assert first['status'] == 'pass'
    assert broker.candidate()['id'] != request['candidate']['id']
    with store._connect() as db:
        before = [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')]
    assert len(before) == 1 and before[0]['state'] == 'finished'
    # The workflow retries the original activity input after losing its response.
    second = await delivery_role(request)
    assert second == first
    with store._connect() as db:
        assert [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')] == before


@pytest.mark.asyncio
async def test_late_fake_start_exits_without_authorizing_work(role_fixture, monkeypatch):
    store, broker, request = role_fixture
    spawn, sleep = asyncio.create_subprocess_exec, asyncio.sleep
    children = []
    code = """
import json,os,sys,time
from pathlib import Path
request=json.loads(Path(sys.argv[1]).read_text())
start=Path(request['start_path'])
while not start.with_suffix('.release').exists(): time.sleep(0.01)
start.write_text(json.dumps({'pid':os.getpid()}))
if sys.stdin.readline().strip() != 'GO': raise SystemExit(2)
(Path(request['workspace'])/'devflow-fake-change.txt').touch()
"""

    async def delayed_child(*argv, **kwargs):
        argv = list(argv)
        argv[argv.index(sys.executable)] = str(Path(sys.executable).resolve())
        argv[argv.index('-m'):] = ['-c', code, argv[-1]]
        child = await spawn(*argv, **kwargs)
        children.append(child)
        wait = child.wait
        start = Path(argv[-1]).with_name('start.release')

        async def release_then_wait():
            # The child starts only after the supervisor denies its startup.
            start.touch()
            return await wait()

        monkeypatch.setattr(child, 'wait', release_then_wait)
        return child

    async def poll_without_delay(_seconds):
        await sleep(0)

    monkeypatch.setattr(asyncio, 'create_subprocess_exec', delayed_child)
    monkeypatch.setattr(asyncio, 'sleep', poll_without_delay)
    try:
        result = await asyncio.wait_for(delivery_role(request), timeout=3)
        assert result['status'] == 'recovery_unknown'
        assert len(children) == 1 and children[0].returncode == 2
        assert not (broker.checkout / 'devflow-fake-change.txt').exists()
        with store._connect() as db:
            row = db.execute('SELECT state,cleanup,result_path FROM delivery_attempts').fetchone()
        assert row['state'] == 'unknown' and row['cleanup'] == 'unknown'
        result_path = Path(row['result_path'])
        assert result_path.with_name('start.json').is_file()
        assert not result_path.exists()
    finally:
        for child in children:
            if child.returncode is None:
                child.kill()
            await child.wait()


@pytest.mark.asyncio
async def test_completed_implementation_cannot_adopt_a_later_checkout_edit(role_fixture):
    _, broker, request = role_fixture
    await delivery_role(request)
    (broker.checkout / 'README.md').write_text('Unrelated later edit\n')
    with pytest.raises(ValueError, match='changed'):
        await delivery_role(request)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', [
    {'resume_session': 'different-session'}, {'feedback': ['different finding']},
    {'continuation': True}, {'result_path': '/not/the/recorded/result'},
    {'evidence_context': {'checks': ['different evidence']}},
])
async def test_completed_role_rejects_changed_activity_request(role_fixture, change):
    store, _, request = role_fixture
    await delivery_role(request)
    with store._connect() as db:
        before = [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')]
    with pytest.raises(RuntimeError, match='request changed'):
        await delivery_role({**request, **change})
    with store._connect() as db:
        assert [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')] == before


@pytest.mark.asyncio
async def test_queued_role_cannot_adopt_a_changed_checkout(role_fixture):
    store, broker, request = role_fixture
    get_supervisor(store)._claim(request)
    (broker.checkout / 'README.md').write_text('Not written by a launched role\n')
    with pytest.raises(ValueError, match='checkout changed'):
        await delivery_role(request)
    with store._connect() as db:
        assert db.execute('SELECT state FROM delivery_attempts').fetchone()[0] == 'queued'


@pytest.mark.asyncio
async def test_legacy_result_without_output_candidate_stays_fail_closed(role_fixture):
    store, _, request = role_fixture
    await delivery_role(request)
    with store._connect() as db:
        row = db.execute('SELECT job_key,result_json FROM delivery_attempts').fetchone()
        legacy = json.loads(row['result_json'])
        legacy.pop('candidate', None)
        db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                   (json.dumps(legacy), row['job_key']))
    start = Path(request['spec']['state_dir']) / 'attempts' / row['job_key'] / 'start.json'
    metadata = json.loads(start.read_text())
    metadata.pop('output_candidate', None)
    metadata.pop('result_digest', None)
    start.write_text(json.dumps(metadata))
    with pytest.raises(ValueError, match='candidate changed'):
        await delivery_role(request)


@pytest.mark.asyncio
@pytest.mark.parametrize('role', ['review', 'verify'])
async def test_unchanged_legacy_finished_gate_replays(role_fixture, role):
    store, broker, request = role_fixture
    request = {**request, 'role': role, 'workspace': str(broker.checkout)}
    supervisor = get_supervisor(store)
    first = await supervisor.run(request)
    with store._connect() as db:
        row = db.execute('SELECT job_key,result_json FROM delivery_attempts').fetchone()
        legacy = json.loads(row['result_json'])
        legacy.pop('candidate', None)
        db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                   (json.dumps(legacy), row['job_key']))
    # No output binding existed before reattachment was introduced.
    start = Path(request['spec']['state_dir']) / 'attempts' / row['job_key'] / 'start.json'
    metadata = json.loads(start.read_text())
    metadata.pop('output_candidate', None)
    start.write_text(json.dumps(metadata))
    assert await supervisor.run(request) == {k: v for k, v in first.items() if k != 'candidate'}
    (broker.checkout / 'README.md').write_text('Later gate edit\n')
    with pytest.raises(ValueError, match='candidate changed'):
        await supervisor.run(request)


@pytest.mark.asyncio
@pytest.mark.parametrize('drift', ['result', 'metadata', 'shape'])
async def test_completed_output_binding_rejects_stale_or_malformed_metadata(role_fixture, drift):
    from devflow_temporal.delivery_resources import read_private, write_private

    store, _, request = role_fixture
    await delivery_role(request)
    with store._connect() as db:
        row = db.execute('SELECT job_key,result_json FROM delivery_attempts').fetchone()
        if drift == 'result':
            result = json.loads(row['result_json'])
            result['summary'] = 'Different completed result'
            db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                       (json.dumps(result), row['job_key']))
    start = Path(request['spec']['state_dir']) / 'attempts' / row['job_key'] / 'start.json'
    if drift != 'result':
        metadata = read_private(start)
        if drift == 'metadata':
            metadata['result_digest'] = 'stale'
        else:
            metadata['output_candidate'] = {'unexpected': 'shape'}
        write_private(start, metadata)
    with pytest.raises(RuntimeError, match='output binding'):
        await delivery_role(request)


NATIVE_DRIVER = '''
import asyncio,json,sys
from pathlib import Path
from devflow_temporal import delivery_broker,delivery_native_process,delivery_preparation,supervisor
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
provider=Path(sys.argv[2])
state=Path(request['spec']['state_dir'])
delivery_preparation.verify_prepared_spec=lambda spec: None
supervisor.prepare_native_role=lambda req,folder: ('fixture', {'PATH':'/usr/bin:/bin'})
original=delivery_native_process.NativeProcess
def native(spec,folder,**options):
    if folder.parent != state/'attempts':
        return original(spec,folder,**options)
    options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]
    options['timeout']=20
    return original(spec,folder,**options)
delivery_native_process.NativeProcess=native
def prerequisites(self,iteration,candidate,**kwargs):
    assert self.candidate()==candidate, 'preparation repeated after implementation'
    if kwargs.get('readiness_attempt') is None:
        with (state/'preparation-calls').open('a') as stream: stream.write('one\\n')
    return {'state':'passed','cleanup':'confirmed','results':[],
            'candidate_id':candidate['id'],'source_unchanged':self.candidate()==candidate}
delivery_broker.DeliveryBroker.run_implementation_preparation=prerequisites
async def execute():
    result=await delivery_role(request)
    if sys.argv[3]=='hold':
        (state/'completed').touch()
        while True: await asyncio.sleep(0.02)
    write_private(state/'activity-result.json',result)
asyncio.run(execute())
'''

FIXED_NATIVE_PROVIDER = '''
import json,sys,time
from pathlib import Path
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
state=Path(request['spec']['state_dir'])
with (state/'invocations').open('a') as stream: stream.write('one\\n')
(Path(request['workspace'])/'README.md').write_text('Changed by the fixed provider\\n')
print('started',flush=True)
(state/'started').touch()
while not (state/'release').exists(): time.sleep(0.02)
write_private(Path(request['result_path']),{'status':'pass','summary':'Fixed provider finished',
    'findings':[],'session_id':'fixed-native-session','usage':None,'finish_reason':'fixture'})
print('completed',flush=True)
'''


@pytest.mark.parametrize('invalid_source', [False, True])
def test_real_supervisor_completion_preserves_receipt_and_releases_capacity(
        api_fixture, tmp_path, invalid_source, monkeypatch):
    from devflow_temporal.delivery_resources import RunResources, read_private
    from devflow_temporal.delivery_stopped_resume import _candidate

    path, submission = api_fixture
    raw = json.loads(path.read_text())
    raw.update(provider='codex', execution_mode='trusted-local', codex_bin=str(
        distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'}
                    for role in raw['roles']}
    check = {'id': 'fixture-check', 'argv': [str(Path(sys.executable).resolve()),
             '-c', "print('1 passed')"], 'test_count_regex': r'(\d+) passed', 'min_tests': 1}
    raw['repositories']['fixture'].update(prepublish_checks=[check], checks=[check],
        required_ci=['test'], project_url='https://github.com/users/example/projects/1',
        assignee='example')
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    submit_before_readiness(store, submission, monkeypatch)
    spec = store.spec(submission['run_id'])
    broker = DeliveryBroker(store, spec)
    request = {'spec': spec, 'role': 'implement', 'iteration': 0,
               'candidate': broker.prepare()['candidate']}
    state = Path(spec['state_dir'])
    driver, provider, input_path = (tmp_path / name for name in (
        'worker.py', 'provider.py', 'input.json'))
    driver.write_text(NATIVE_DRIVER)
    provider_text = FIXED_NATIVE_PROVIDER.replace("'status':'pass'", "'status':'blocked'")
    if invalid_source:
        provider_text = provider_text.replace("print('started',flush=True)",
            "(Path(request['workspace'])/'unsafe-link').symlink_to('/missing-target')\n"
            "print('started',flush=True)")
    provider.write_text(provider_text)
    input_path.write_text(json.dumps(request))
    (state / 'release').touch()
    try:
        completed = subprocess.run([sys.executable, '-I', str(driver), str(input_path),
            str(provider), 'finish'], capture_output=True, text=True, timeout=30)
        with store._connect() as db:
            attempts = [dict(row) for row in db.execute('SELECT * FROM delivery_attempts')]
        assert len(attempts) == 1
        assert attempts[0]['state'] == 'finished'
        assert attempts[0]['cleanup'] == 'confirmed'
        raw_result = json.loads(attempts[0]['result_json'])
        assert raw_result['status'] == 'blocked'
        assert 'candidate' not in raw_result
        if invalid_source:
            assert any('symlink' in finding for finding in raw_result['findings'])
        else:
            assert completed.returncode == 0, completed.stderr
            result = read_private(state / 'activity-result.json')
            actual, session = _candidate(broker, {'roles': [result]}, attempts)
            assert actual == result['candidate']
            assert session == result['session_id']
            from devflow_temporal.delivery_resources import write_private

            journal_path = state / 'attempts' / attempts[0]['job_key'] / 'native-process.json'
            journal = read_private(journal_path)
            journal['intent']['run_id'] = 'foreign-run'
            write_private(journal_path, journal)
            with pytest.raises(RuntimeError, match='ownership changed'):
                get_supervisor(store).retained_request(
                    {**request, 'workspace': str(broker.checkout)})
            journal['intent']['run_id'] = spec['run_id']
            write_private(journal_path, journal)
    finally:
        assert RunResources(spec).finalize('blocked')['resource_cleanup'] == 'confirmed'


@pytest.mark.parametrize('checkpoint', ['running', 'completed'])
def test_original_activity_reattaches_after_real_worker_sigkill(
        api_fixture, tmp_path, checkpoint, monkeypatch):
    from devflow_temporal.delivery_native_process import stop_observed
    from devflow_temporal.delivery_resources import RunResources, read_private

    path, submission = api_fixture
    raw = json.loads(path.read_text())
    raw.update(provider='codex', execution_mode='trusted-local', codex_bin=str(
        distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'}
                    for role in raw['roles']}
    check = {'id': 'fixture-check', 'argv': [str(Path(sys.executable).resolve()),
             '-c', "print('1 passed')"], 'test_count_regex': r'(\d+) passed', 'min_tests': 1}
    raw['repositories']['fixture'].update(prepublish_checks=[check], checks=[check],
        required_ci=['test'], project_url='https://github.com/users/example/projects/1',
        assignee='example')
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    submit_before_readiness(store, submission, monkeypatch)
    spec = store.spec(submission['run_id'])
    broker = DeliveryBroker(store, spec)
    request = {'spec': spec, 'role': 'implement', 'iteration': 0,
               'candidate': broker.prepare()['candidate']}
    state = Path(spec['state_dir'])
    driver, provider = tmp_path / 'worker.py', tmp_path / 'provider.py'
    driver.write_text(NATIVE_DRIVER)
    provider.write_text(FIXED_NATIVE_PROVIDER)
    input_path = tmp_path / 'input.json'
    input_path.write_text(json.dumps(request))
    if checkpoint == 'completed':
        (state / 'release').touch()
    logs = [(tmp_path / f'worker-{i}.log').open('wb') for i in range(2)]
    workers = []
    try:
        workers.append(subprocess.Popen([sys.executable, '-I', str(driver), str(input_path),
            str(provider), 'hold'], stdout=logs[0], stderr=subprocess.STDOUT))
        marker = state / ('completed' if checkpoint == 'completed' else 'started')
        deadline = time.monotonic() + 15
        while not marker.exists():
            assert workers[0].poll() is None, (tmp_path / 'worker-0.log').read_text()
            assert time.monotonic() < deadline, 'first worker did not reach its checkpoint'
            time.sleep(0.02)
        assert broker.candidate()['id'] != request['candidate']['id']
        workers[0].kill()
        workers[0].wait(timeout=5)
        workers.append(subprocess.Popen([sys.executable, '-I', str(driver), str(input_path),
            str(provider), 'finish'], stdout=logs[1], stderr=subprocess.STDOUT))
        (state / 'release').touch()
        workers[1].wait(timeout=15)
        assert workers[1].returncode == 0, (tmp_path / 'worker-1.log').read_text()
        result = read_private(state / 'activity-result.json')
        assert result['status'] == 'pass'
        assert result['session_id'] == 'fixed-native-session'
        assert result['input_candidate_id'] == request['candidate']['id']
        assert result['candidate'] == broker.candidate()
        assert (state / 'invocations').read_text().splitlines() == ['one']
        assert (state / 'preparation-calls').read_text().splitlines() == ['one']
        assert Path(result['native_process']['log']).read_text().splitlines() == [
            'started', 'completed']
        with store._connect() as db:
            assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 1
        assert RunResources(spec).finalize('delivered')['resource_cleanup'] == 'confirmed'
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
                worker.wait(timeout=5)
        for log in logs:
            log.close()
        for journal in (state / 'attempts').glob('*/native-process.json'):
            saved = read_private(journal)
            owned = {int(pid): value for pid, value in saved.get('owned', {}).items()}
            if saved.get('monitor'):
                owned[saved['monitor']['pid']] = saved['monitor']
            assert stop_observed(owned)
