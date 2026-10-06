from __future__ import annotations

import json
import subprocess
import sys
import time
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture

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
async def test_lost_activity_response_reuses_completed_implementation(role_fixture):
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
        legacy.pop('candidate')
        db.execute('UPDATE delivery_attempts SET result_json=? WHERE job_key=?',
                   (json.dumps(legacy), row['job_key']))
    with pytest.raises(ValueError, match='candidate changed'):
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
    options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]
    options['timeout']=20
    return original(spec,folder,**options)
delivery_native_process.NativeProcess=native
def prerequisites(self,iteration,candidate):
    assert self.candidate()==candidate, 'preparation repeated after implementation'
    with (state/'preparation-calls').open('a') as stream: stream.write('one\\n')
    return {'state':'passed','cleanup':'confirmed','results':[]}
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


@pytest.mark.parametrize('checkpoint', ['running', 'completed'])
def test_original_activity_reattaches_after_real_worker_sigkill(api_fixture, tmp_path, checkpoint):
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
    store.submit(submission)
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
