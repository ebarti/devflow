from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_store import _git

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def scope_broker(api_fixture, monkeypatch):
    path, request = api_fixture
    raw = json.loads(path.read_text())
    repository = raw['repositories']['fixture']
    source = Path(repository['source_path'])
    (source / 'protected.txt').write_text('Protected baseline file\n')
    _git(source, 'add', 'protected.txt')
    _git(source, 'commit', '-qm', 'Fixture protected source')
    repository['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request['run_id']))
    broker.prepare()
    monkeypatch.setattr(broker, '_existing_pr', lambda: {
        'number': 7, 'url': 'https://github.com/example/fixture/pull/7', 'state': 'OPEN',
        'headRefOid': broker.candidate()['head'],
    })
    return broker


def commit_role_change(broker, scenario, *, signed=True):
    checkout = broker.checkout
    if scenario in {'committed', 'mixed'}:
        (checkout / 'protected.txt').write_text('Out-of-scope role change\n')
    elif scenario == 'rename-out':
        _git(checkout, 'mv', 'README.md', 'escape.md')
    elif scenario == 'rename-in':
        _git(checkout, 'mv', '-f', 'protected.txt', 'README.md')
    elif scenario == 'deletion':
        _git(checkout, 'rm', 'protected.txt')
    else:
        (checkout / 'README.md').write_text('In-scope role change\n')
    _git(checkout, 'add', '-A')
    _git(checkout, 'commit', *(['--signoff'] if signed else []), '-qm', 'fix: role change')
    if scenario == 'mixed':
        (checkout / 'README.md').write_text('Uncommitted in-scope change\n')


@pytest.mark.parametrize('scenario', ['committed', 'mixed', 'rename-out', 'rename-in', 'deletion'])
def test_publish_rejects_full_committed_scope_before_remote_push(scope_broker, scenario):
    broker = scope_broker
    commit_role_change(broker, scenario)
    remote_before = _git(broker.source, 'ls-remote', 'origin')
    with pytest.raises(ValueError, match='outside allowed paths'):
        broker.publish(0, broker.candidate())
    assert _git(broker.source, 'ls-remote', 'origin') == remote_before


def test_unsigned_role_commit_already_cannot_be_published(scope_broker):
    broker = scope_broker
    commit_role_change(broker, 'allowed', signed=False)
    remote_before = _git(broker.source, 'ls-remote', 'origin')
    with pytest.raises(ValueError, match='Signed-off-by'):
        broker.publish(0, broker.candidate())
    assert _git(broker.source, 'ls-remote', 'origin') == remote_before


@pytest.mark.parametrize('scenario,expected', [
    ('rename-in', {'README.md', 'protected.txt'}),
    ('rename-out', {'README.md', 'escape.md'}),
])
def test_changed_paths_counts_both_rename_sides(scope_broker, scenario, expected):
    broker = scope_broker
    commit_role_change(broker, scenario)
    assert broker._changed_paths(broker.spec['base_sha']) == expected


def test_publish_rejects_foreign_index_changes_hidden_by_worktree(scope_broker):
    broker = scope_broker
    (broker.checkout / 'protected.txt').write_text('Staged foreign change\n')
    _git(broker.checkout, 'add', 'protected.txt')
    (broker.checkout / 'protected.txt').write_text('Protected baseline file\n')
    (broker.checkout / 'README.md').write_text('Allowed working change\n')
    remote_before = _git(broker.source, 'ls-remote', 'origin')
    with pytest.raises(ValueError, match='outside allowed paths'):
        broker.publish(0, broker.candidate())
    assert _git(broker.source, 'ls-remote', 'origin') == remote_before


def test_role_normalization_preserves_index_and_worktree(scope_broker):
    broker = scope_broker
    initial = broker.candidate()
    commit_role_change(broker, 'allowed', signed=False)
    (broker.checkout / 'README.md').write_text('Staged allowed change\n')
    _git(broker.checkout, 'add', 'README.md')
    (broker.checkout / 'README.md').write_text('Unstaged allowed change\n')
    index = _git(broker.checkout, 'show', ':README.md')
    worktree = (broker.checkout / 'README.md').read_bytes()
    normalized = broker.admit_implementation(initial)
    assert normalized['head'] == initial['head']
    assert _git(broker.checkout, 'show', ':README.md') == index
    assert (broker.checkout / 'README.md').read_bytes() == worktree
    assert _git(broker.checkout, 'diff', '--cached', '--name-only') == 'README.md'
    assert _git(broker.checkout, 'diff', '--name-only') == 'README.md'


@pytest.mark.parametrize('name', [' foreign.txt', 'line\nbreak.txt', 'trailing.txt '])
def test_changed_paths_preserves_exact_untracked_filenames(scope_broker, name):
    (scope_broker.checkout / name).write_text('Foreign file\n')
    assert scope_broker._changed_paths() == {name}
    with pytest.raises(ValueError, match='outside allowed paths'):
        scope_broker.validate_candidate_scope()


NATIVE_ROLE_DRIVER = '''
import asyncio,json,sys
from pathlib import Path
from devflow_temporal import delivery_broker,delivery_native_process,delivery_preparation,supervisor
from devflow_temporal.delivery_activities import delivery_role
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
provider=Path(sys.argv[2])
delivery_preparation.verify_prepared_spec=lambda spec: None
supervisor.prepare_native_role=lambda req,folder: ('fixture', {'PATH':'/usr/bin:/bin'})
original=delivery_native_process.NativeProcess
if request['feedback'][0]=='legacy-signed':
    supervisor.DeliverySupervisor._admit_role_output=lambda *args: None
def native(spec,folder,**options):
    options['argv']=[sys.executable,'-I',str(provider),str(folder/'request.json')]
    options['timeout']=20
    return original(spec,folder,**options)
delivery_native_process.NativeProcess=native
delivery_broker.DeliveryBroker.run_implementation_preparation=lambda *args: {
    'state':'passed','cleanup':'confirmed','results':[]}
result=asyncio.run(delivery_role(request))
write_private(Path(request['spec']['state_dir'])/'activity-result.json',result)
'''

NATIVE_COMMIT_PROVIDER = '''
import json,subprocess,sys
from pathlib import Path
from devflow_temporal.delivery_resources import write_private
request=json.loads(Path(sys.argv[1]).read_text())
workspace=Path(request['workspace'])
scenario=request['feedback'][0]
target='protected.txt' if scenario=='outside' else 'README.md'
(workspace/target).write_text('Changed by the offline role\\n')
git=['git','-C',str(workspace),'-c','core.hooksPath=/dev/null',
     '-c','user.name=Fixture Role','-c','user.email=role@example.invalid']
if scenario=='rewritten':
    subprocess.run(git+['checkout','--orphan','rewritten'],check=True)
subprocess.run(git+['add','-A'],check=True)
subprocess.run(git+['commit','-qm','fix: role-created commit']+
    ([] if scenario=='unsigned' else ['--signoff']),check=True)
head=subprocess.check_output(git+['rev-parse','HEAD'],text=True).strip()
write_private(Path(request['artifact_directory'])/'role-commit.json',{'head':head})
print('provider commit '+head,flush=True)
write_private(Path(request['result_path']),{'status':'pass','summary':'Offline role completed',
    'findings':[],'session_id':'original-offline-session','usage':None,'finish_reason':'fixture'})
'''


@pytest.mark.parametrize('scenario', [
    'outside', 'unsigned', 'signed', 'rewritten', 'legacy-signed',
])
def test_native_role_admission_enforces_scope_and_controller_commit_ownership(
        api_fixture, tmp_path, monkeypatch, scenario):
    from devflow_temporal.delivery_resources import RunResources, read_private

    path, submission = api_fixture
    raw = json.loads(path.read_text())
    repository = raw['repositories']['fixture']
    source = Path(repository['source_path'])
    (source / 'protected.txt').write_text('Protected baseline file\n')
    _git(source, 'add', 'protected.txt')
    _git(source, 'commit', '-qm', 'Fixture protected source')
    repository['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    raw.update(provider='codex', execution_mode='trusted-local', codex_bin=str(
        distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'} for role in raw['roles']}
    check = {'id': 'fixture-check', 'argv': [str(Path(sys.executable).resolve()),
             '-c', "print('1 passed')"], 'test_count_regex': r'(\d+) passed', 'min_tests': 1}
    repository.update(prepublish_checks=[check], checks=[check], required_ci=['test'],
        project_url='https://github.com/users/example/projects/1', assignee='example')
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    broker = DeliveryBroker(store, store.spec(submission['run_id']))
    initial = broker.prepare()['candidate']
    request = {'spec': broker.spec, 'role': 'implement', 'iteration': 0,
               'candidate': initial, 'feedback': [scenario]}
    driver, provider, input_path = (tmp_path / name for name in (
        'worker.py', 'provider.py', 'input.json'))
    driver.write_text(NATIVE_ROLE_DRIVER)
    provider.write_text(NATIVE_COMMIT_PROVIDER)
    input_path.write_text(json.dumps(request))
    try:
        command = subprocess.run([sys.executable, '-I', str(driver), str(input_path),
            str(provider)], capture_output=True, text=True, timeout=30)
        assert command.returncode == 0, command.stderr
        result = read_private(broker.state_dir / 'activity-result.json')
        with store._connect() as db:
            attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
        raw_result = json.loads(attempt['result_json'])
        assert attempt['state'] == 'finished' and attempt['cleanup'] == 'confirmed'
        # The real provider's stdout proves git commit --signoff was executable.
        log = Path(raw_result['native_process']['log']).read_text()
        assert 'provider commit ' in log
        if scenario == 'legacy-signed':
            assert result['status'] == 'blocked' and raw_result['status'] == 'pass'
            manifest = read_private(Path(result['role_artifacts']['path']))
            assert broker.candidate()['head'] == manifest['source_candidate']['head']
            assert broker.candidate()['head'] != initial['head']
            assert any('artifacts were bound' in finding for finding in result['findings'])
            return
        if scenario in {'outside', 'rewritten'}:
            assert result['status'] == 'blocked'
            assert raw_result['status'] == 'blocked'
            assert any(('outside allowed' if scenario == 'outside' else 'ancestry') in finding
                       for finding in result['findings'])
            return
        assert result['status'] == 'pass'
        assert broker.candidate()['head'] == initial['head']
        assert _git(broker.checkout, 'diff', '--cached', '--name-only') == 'README.md'
        assert (broker.checkout / 'README.md').read_text() == 'Changed by the offline role\n'
        manifest = read_private(Path(result['role_artifacts']['path']))
        artifact = manifest['artifacts'][0]
        original_bytes = Path(artifact['path']).read_bytes()
        assert hashlib.sha256(original_bytes).hexdigest() == artifact['sha256']
        provider_head = json.loads(original_bytes)['head']
        assert provider_head != initial['head']
        assert ('provider commit ' + provider_head) in log
        assert manifest['source_candidate'] == {
            key: result['candidate'][key] for key in ('head', 'content_sha256', 'id')}
        monkeypatch.setattr(broker, '_existing_pr', lambda: {'number': 7,
            'url': 'https://github.com/example/fixture/pull/7', 'state': 'OPEN',
            'headRefOid': broker.candidate()['head']})
        published = broker.publish(0, result['candidate'])
        assert _git(broker.checkout, 'rev-parse', 'HEAD^') == initial['head']
        assert 'Signed-off-by:' in _git(broker.checkout, 'show', '-s', '--format=%B')
        assert published['head'] == broker.candidate()['head']
    finally:
        assert RunResources(broker.spec).finalize('blocked')['resource_cleanup'] == 'confirmed'
