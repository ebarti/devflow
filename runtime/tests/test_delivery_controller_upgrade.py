"""Owner-controlled source updates retain trusted native proof and process custody."""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_native_process import stop_observed
from devflow_temporal.delivery_preparation import PACKAGE
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import get_supervisor

pytestmark = pytest.mark.skipif(sys.platform != "darwin", reason="actual native Mac preparation")

PREPARATION_DRIVER = '''
import json,subprocess,sys
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import prepare_authority,verify_prepared_spec
from devflow_temporal.delivery_resources import read_private,write_private
from devflow_temporal.delivery_store import DeliveryStore
store=DeliveryStore(DeliveryConfig.load(Path(sys.argv[3])))
if sys.argv[1]=='prepare':
    spec=prepare_authority(store,store.submitted_spec(sys.argv[4]))
    write_private(Path(sys.argv[5]),spec)
else:
    spec=read_private(Path(sys.argv[5]))
    verify_prepared_spec(spec)
    assert store.prepared_spec(spec['run_id'])==spec
    if sys.argv[1]=='gates':
        broker=DeliveryBroker(store,spec)
        broker.prepare()
        subprocess.run(['git','-C',str(broker.checkout),'add','README.md'],check=True)
        subprocess.run(['git','-C',str(broker.checkout),'commit','-qm',
            'test: owned candidate for final gates','--signoff'],check=True)
        candidate=broker.candidate()
        precheck=broker.run_prechecks(0,candidate)
        checks=broker.run_checks(0,candidate)
        assert precheck['state']==checks['state']=='passed', (precheck,checks)
        write_private(Path(spec['state_dir'])/'upgrade-gates.json',
            {'precheck':precheck,'checks':checks})
'''

PROVIDER_TRANSPORT = '''
import asyncio,json,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from devflow_temporal import delivery_native_process
from devflow_temporal.delivery_activities import delivery_role
original=delivery_native_process.NativeProcess
def process(spec,folder,**options):
    if options['argv'][2:4]==['-m','devflow_temporal.role_runner']:
        options['argv']=[sys.executable,'-I',sys.argv[2],str(folder/'request.json'),sys.argv[1]]
        options['timeout']=90
    return original(spec,folder,**options)
delivery_native_process.NativeProcess=process
'''

ROLE_DRIVER = PROVIDER_TRANSPORT + '''
from devflow_temporal.delivery_resources import read_private,write_private
request=read_private(Path(sys.argv[3]))
result=asyncio.run(delivery_role(request))
write_private(Path(request['spec']['state_dir'])/'queued-role-result.json',result)
'''

WORKER_DRIVER = PROVIDER_TRANSPORT + '''
from temporalio.client import Client
from temporalio.worker import Worker
sys.path.insert(0,sys.argv[4])
from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow
async def main():
    client=await Client.connect(sys.argv[3])
    async with Worker(client,task_queue='controller-upgrade',
        workflows=[ActivityLivenessWorkflow],activities=[delivery_role]):
        await asyncio.Event().wait()
asyncio.run(main())
'''

PROVIDER = '''
import json,sys,time
from pathlib import Path
sys.path.insert(0,sys.argv[2])
from devflow_temporal.delivery_native_guard import validate_role_ancestry
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
validate_role_ancestry(request)
if request['iteration']==1:
    assert request.get('resume_session')=='same-upgrade-session'
state=Path(request['spec']['state_dir'])
with (state/'invocations').open('a') as stream:
    stream.write(str(request['iteration'])+'\\n')
(Path(request['workspace'])/'README.md').write_text(
    'Changed by the owned provider '+str(request['iteration'])+'\\n')
print('started',flush=True)
(state/('started-'+str(request['iteration']))).touch()
while not (state/'release').exists(): time.sleep(0.02)
write_private(Path(request['result_path']),{'status':'pass','summary':'Owned provider finished',
    'findings':[],'session_id':'same-upgrade-session','usage':None,'finish_reason':'fixture'})
print('completed',flush=True)
'''

NEGATIVE_DRIVER = '''
import hashlib,json,os,sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from devflow_temporal import delivery_native_preparation
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import _context
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import PreparationError,verify_prepared_spec
from devflow_temporal.delivery_resources import read_private,write_private
from devflow_temporal.delivery_store import DeliveryStore
spec=read_private(Path(sys.argv[2]))
store=DeliveryStore(DeliveryConfig.load(Path(spec['config_path'])))
kind,name=json.loads(sys.argv[3])
if kind=='observed':
    original=delivery_native_preparation.native_identity
    def changed(value):
        current=original(value)
        previous=current[name]
        if isinstance(previous,dict):
            current[name]={**previous,'fixture_drift':'different'}
        elif isinstance(previous,list):
            current[name]=[*previous,str(Path(sys.argv[2]).parent/'different-root')]
        elif name.endswith('_sha256'):
            current[name]=hashlib.sha256((previous+'different').encode()).hexdigest()
        else:
            current[name]=str(previous)+'-different'
        return current
    delivery_native_preparation.native_identity=changed
elif kind=='binding':
    if name in {'source_path','checkout','config_digest','run_id'}:
        spec[name]+='changed'
    elif name=='model': spec['policy']['roles']['implement']['model']='different-model'
    elif name=='scope': spec['policy']['allowed_paths'].append('other.py')
    elif name=='sandbox': spec['policy']['host_sandbox']='native-profile'
    elif name=='dependency': spec['policy']['runtime_dependencies']['lock_sha256']='f'*64
    elif name=='overrides': spec['policy']['config_overrides']=[]
    elif name=='fingerprint': spec['preparation']['fingerprint']='f'*64
    elif name=='security': spec['preparation']['security_binding_sha256']='f'*64
    spec['policy_digest']=digest(spec['policy'])
elif kind=='proof':
    path=Path(spec['preparation']['environment']['path'])
    if name=='mode': path.chmod(0o644)
    elif name=='hardlink': os.link(path,path.with_name('linked.json'))
    elif name=='owner':
        owner=os.getuid(); os.getuid=lambda: owner+1
    elif name=='canonical-path':
        spec['preparation']['environment']['path']=str(path.with_name('other.json'))
    elif name=='hash': spec['preparation']['environment']['sha256']='f'*64
    elif name=='content': path.write_bytes(path.read_bytes()+b' ')
    else:
        proof=read_private(path)
        if name=='identity':
            proof['identity']['runtime_payload_sha256']='f'*64
            proof['fingerprint']=digest(proof['identity'])
        elif name=='observed':
            measured=Path(proof['measurement']['observed']['path'])
            write_private(measured,{'mode':'trusted-local','host_read':False,
                'owned_tmp_write':True,'managed_depth':'1'})
            proof['measurement']['observed']['sha256']=hashlib.sha256(measured.read_bytes()).hexdigest()
        write_private(path,proof)
        hash=hashlib.sha256(path.read_bytes()).hexdigest()
        spec['preparation']['environment']['sha256']=hash
        spec['policy']['environment_proof_sha256']=hash
        spec['policy_digest']=digest(spec['policy'])
elif kind=='context' and name=='configuration':
    path=Path(spec['config_path'])
    raw=json.loads(path.read_text()); raw['max_repairs']=2
    path.write_text(json.dumps(raw))
try:
    if kind=='context' and name=='configuration': _context(spec)
    elif kind=='context' and name=='candidate':
        broker=DeliveryBroker(store,spec)
        candidate=broker.prepare()['candidate']
        (broker.checkout/'README.md').write_text('Unexpected source change')
        broker.run_prechecks(0,candidate)
    else: verify_prepared_spec(spec)
except (PreparationError,ValueError) as error:
    print(type(error).__name__+': '+str(error))
else:
    raise AssertionError('controller upgrade accepted '+kind+'/'+name+' drift')
'''


@pytest.fixture
def installed_controller(native_configuration, tmp_path, request):
    config, submission = native_configuration
    config.raw['execution_mode'] = getattr(request, 'param', 'trusted-local')
    auth = tmp_path / 'fixture-auth.json'
    write_private(auth, {'fixture': 'offline; never a provider credential'})
    config.raw['codex_auth_path'] = str(auth)
    config.path.write_text(json.dumps(config.raw))
    package = tmp_path / 'installation/runtime/src/devflow_temporal'
    shutil.copytree(PACKAGE, package, ignore=shutil.ignore_patterns('__pycache__'))
    previous = gzip.decompress((Path(__file__).parent / 'fixtures/controller-upgrade'
        / 'native-preparation-e50e66f.py.gz').read_bytes())
    assert hashlib.sha256(previous).hexdigest() == (
        '2ce61db4129f91925d841bd38c2a3ffdb5a18b25ae8123865bdd92b06f1c36bf')
    (package / 'delivery_native_preparation.py').write_bytes(previous)
    shutil.copy2(PACKAGE.parents[1] / 'uv.lock', package.parents[1] / 'uv.lock')
    (package.parents[1] / '.venv').symlink_to(Path(sys.prefix), target_is_directory=True)
    store = DeliveryStore(config)
    store.submit({**submission,
                  'accepted_plan': 'Change only the fixture README and run its checks'})
    driver = tmp_path / 'preparation.py'
    driver.write_text(PREPARATION_DRIVER)
    spec_path = tmp_path / 'prepared.json'
    fixture = SimpleNamespace(store=store, config=config, package=package, driver=driver,
                              spec_path=spec_path, run_id=submission['run_id'], root=tmp_path)
    prepared = _command(fixture, 'prepare')
    assert prepared.returncode == 0, prepared.stderr
    fixture.spec = read_private(spec_path)
    try:
        yield fixture
    finally:
        state = Path(fixture.spec['state_dir'])
        for journal_path in (state / 'attempts').glob('*/native-process.json'):
            journal = read_private(journal_path)
            owned = {int(pid): value for pid, value in journal.get('owned', {}).items()}
            if journal.get('monitor'):
                owned[journal['monitor']['pid']] = journal['monitor']
            assert stop_observed(owned)
        assert RunResources(fixture.spec).finalize('blocked')['resource_cleanup'] == 'confirmed'


def _command(fixture, action):
    return subprocess.run([sys.executable, '-I', str(fixture.driver), action,
        str(fixture.package.parent), str(fixture.config.path), fixture.run_id,
        str(fixture.spec_path)], capture_output=True, text=True, timeout=45)


def _upgrade(fixture):
    source = fixture.package / 'delivery_native_preparation.py'
    write_private(fixture.root / 'previous-source.json', {
        str(path.relative_to(fixture.package)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in fixture.package.rglob('*.py')})
    replacement = (PACKAGE / source.name).read_bytes()
    if replacement == source.read_bytes():
        replacement += b'\n# Owner-installed controller source update.\n'
    source.write_bytes(replacement)


def _proof_bytes(spec):
    proof = Path(spec['preparation']['environment']['path'])
    value = read_private(proof)
    paths = [proof, *(Path(value['measurement'][key]['path'])
                     for key in ('observed', 'log', 'path_control'))]
    return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}


def test_queued_trusted_preparation_and_real_broker_gates_survive_source_update(
    installed_controller,
):
    fixture = installed_controller
    original = fixture.spec_path.read_bytes()
    proof = _proof_bytes(fixture.spec)
    broker = DeliveryBroker(fixture.store, fixture.spec)
    request = {'spec': fixture.spec, 'role': 'implement', 'iteration': 0,
               'candidate': broker.prepare()['candidate']}
    get_supervisor(fixture.store)._claim(request)
    _upgrade(fixture)
    role, provider, role_input = (fixture.root / name for name in (
        'queued-role.py', 'provider.py', 'queued-role.json'))
    role.write_text(ROLE_DRIVER)
    provider.write_text(PROVIDER)
    write_private(role_input, request)
    state = Path(fixture.spec['state_dir'])
    (state / 'release').touch()
    launched = subprocess.run([sys.executable, '-I', str(role), str(fixture.package.parent),
        str(provider), str(role_input)], capture_output=True, text=True, timeout=30)
    assert launched.returncode == 0, launched.stderr
    queued = read_private(state / 'queued-role-result.json')
    assert queued['status'] == 'pass', queued
    assert (state / 'invocations').read_text().splitlines() == ['0']
    result = _command(fixture, 'gates')
    assert result.returncode == 0, result.stderr
    assert fixture.spec_path.read_bytes() == original
    assert fixture.store.prepared_spec(fixture.run_id) == fixture.spec
    assert _proof_bytes(fixture.spec) == proof
    gates = read_private(Path(fixture.spec['state_dir']) / 'upgrade-gates.json')
    assert gates['precheck']['results'][0]['test_count'] == 2
    assert gates['checks']['results'][0]['test_count'] == 2


@pytest.mark.parametrize('field', [
    'execution_mode', 'platform', 'os_version', 'architecture', 'python', 'python_sha256',
    'codex_bin', 'codex_bin_sha256', 'packages', 'runtime_dependencies', 'config_overrides',
    'toolchain_roots', 'package_manager_cache', 'browser_read_roots', 'protected_commands',
])
def test_trusted_update_refuses_every_other_observed_identity_field(installed_controller, field):
    _refuses(installed_controller, 'observed', field)


@pytest.mark.parametrize('kind,name', [
    *[('binding', field) for field in ('source_path', 'checkout', 'config_digest', 'run_id',
        'model', 'scope', 'sandbox', 'dependency', 'overrides', 'fingerprint', 'security')],
    *[('proof', field) for field in ('mode', 'hardlink', 'owner', 'canonical-path', 'hash',
        'content', 'identity', 'observed')],
    ('context', 'configuration'), ('context', 'candidate'),
])
def test_trusted_update_preserves_proof_authority_and_candidate_guards(
    installed_controller, kind, name,
):
    _refuses(installed_controller, kind, name)


def _refuses(fixture, kind, name):
    _upgrade(fixture)
    driver = fixture.root / 'negative.py'
    driver.write_text(NEGATIVE_DRIVER)
    result = subprocess.run([sys.executable, '-I', str(driver), str(fixture.package.parent),
        str(fixture.spec_path), json.dumps([kind, name])],
        capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith(('PreparationError:', 'ValueError:'))


@pytest.mark.parametrize('installed_controller', ['native-profile'], indirect=True)
def test_actual_native_profile_source_update_still_requires_its_existing_strict_path(
    installed_controller,
):
    fixture = installed_controller
    _upgrade(fixture)
    result = _command(fixture, 'verify')
    assert result.returncode == 1
    assert 'PreparationError: native transport update has no valid installation receipt' in (
        result.stderr)


@pytest.mark.asyncio
async def test_real_worker_sigkill_reattaches_after_controller_upgrade_and_runs_next_role(
    installed_controller,
):
    from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow

    fixture = installed_controller
    broker = DeliveryBroker(fixture.store, fixture.spec)
    request = {'spec': fixture.spec, 'role': 'implement', 'iteration': 0,
               'candidate': broker.prepare()['candidate']}
    state = Path(fixture.spec['state_dir'])
    driver, provider = fixture.root / 'worker.py', fixture.root / 'provider.py'
    driver.write_text(WORKER_DRIVER)
    provider.write_text(PROVIDER)
    workers, logs = [], []
    original_spec, original_proof = fixture.spec_path.read_bytes(), _proof_bytes(fixture.spec)
    try:
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which('temporal'),
            dev_server_database_filename=str(fixture.root / 'owned-temporal.sqlite3'),
        ) as environment:
            argv = [sys.executable, '-I', str(driver), str(fixture.package.parent), str(provider),
                    environment.client.service_client.config.target_host,
                    str(Path(__file__).parent)]
            logs.append((fixture.root / 'previous-worker.log').open('wb'))
            workers.append(subprocess.Popen(argv, stdout=logs[0], stderr=subprocess.STDOUT))
            handle = await environment.client.start_workflow(ActivityLivenessWorkflow.run,
                {'activity_name': 'delivery_role', 'request': request},
                id='owned-upgrade-role', task_queue='controller-upgrade')
            deadline = time.monotonic() + 25
            while not (state / 'started-0').exists():
                assert workers[0].poll() is None, (fixture.root / 'previous-worker.log').read_text()
                assert time.monotonic() < deadline, 'original provider did not start'
                await asyncio.sleep(0.05)
            journal_path = next((state / 'attempts').glob('*/native-process.json'))
            before = read_private(journal_path)
            retained_request = journal_path.with_name('request.json').read_bytes()
            workers[0].kill()
            await asyncio.to_thread(workers[0].wait, 5)
            assert workers[0].returncode < 0
            _upgrade(fixture)
            logs.append((fixture.root / 'replacement-worker.log').open('wb'))
            workers.append(subprocess.Popen(argv, stdout=logs[1], stderr=subprocess.STDOUT))
            (state / 'release').touch()
            result = await asyncio.wait_for(handle.result(), 45)
            assert result['status'] == 'pass', result
            assert result['session_id'] == 'same-upgrade-session'
            assert (state / 'invocations').read_text().splitlines() == ['0']
            assert journal_path.with_name('request.json').read_bytes() == retained_request
            after = read_private(journal_path)
            assert after['intent'] == before['intent']
            assert all(after['owned'].get(pid) == value for pid, value in before['owned'].items())
            assert after['monitor'] == before['monitor']
            assert after['provider_session']['session_id'] == 'same-upgrade-session'
            history = await handle.fetch_history()
            original_history = history.to_json()
            (fixture.root / 'upgrade-history.json').write_text(original_history)
            started = [event.activity_task_started_event_attributes for event in history.events
                       if event.HasField('activity_task_started_event_attributes')]
            assert started[-1].attempt == 2
            await Replayer(workflows=[ActivityLivenessWorkflow]).replay_workflow(history)
            assert history.to_json() == original_history
            next_role = {**request, 'iteration': 1, 'candidate': result['candidate'],
                         'resume_session': result['session_id']}
            resumed = await environment.client.execute_workflow(ActivityLivenessWorkflow.run,
                {'activity_name': 'delivery_role', 'request': next_role},
                id='owned-upgrade-next-role', task_queue='controller-upgrade')
            assert resumed['status'] == 'pass', resumed
            assert resumed['session_id'] == result['session_id']
            assert (state / 'invocations').read_text().splitlines() == ['0', '1']
            gates = _command(fixture, 'gates')
            assert gates.returncode == 0, gates.stderr
            assert fixture.spec_path.read_bytes() == original_spec
            assert _proof_bytes(fixture.spec) == original_proof
            write_private(fixture.root / 'upgrade-observed.json', {
                'original_invocations': 1, 'total_invocations_after_next_role': 2,
                'provider_session': result['session_id'], 'worker_attempt': started[-1].attempt,
                'request_unchanged': journal_path.with_name('request.json').read_bytes()
                    == retained_request,
                'original_owned_identities': before['owned'], 'monitor_identity': before['monitor'],
                'original_proof_hashes': original_proof, 'proof_unchanged': True,
                'history_sha256': hashlib.sha256(original_history.encode()).hexdigest(),
                'resource_cleanup': 'checked by fixture finalizer',
            })
    finally:
        for worker in workers:
            if worker.poll() is None:
                worker.kill()
                worker.wait(timeout=5)
        for log in logs:
            log.close()
