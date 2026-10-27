from __future__ import annotations

import json
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
async def test_queued_role_cannot_adopt_a_changed_checkout(role_fixture):
    store, broker, request = role_fixture
    get_supervisor(store)._claim(request)
    (broker.checkout / 'README.md').write_text('Not written by a launched role\n')
    with pytest.raises(ValueError, match='checkout changed'):
        await delivery_role(request)
    with store._connect() as db:
        assert db.execute('SELECT state FROM delivery_attempts').fetchone()[0] == 'queued'


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
