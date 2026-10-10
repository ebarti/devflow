"""Browser output created by earlier checks retains controller resource custody."""
from __future__ import annotations

import json
import socket
import sys
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_configured_resources import configured
from test_delivery_store import _git

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import RunResources, read_private


@pytest.mark.parametrize('kind', ['gate', 'checkout'])
def test_nested_browser_output_is_reserved_before_creation(tmp_path, kind):
    owned, resources, broker, root, _ = configured(tmp_path, kind)
    owned['policy']['browser_qa'] = {'artifact_paths': ['dist/playwright-report']}
    report = root / 'dist/playwright-report'
    broker._register_generated(root, ['dist/playwright-report'])
    assert report.parent.is_dir() and not report.exists()
    report.mkdir()
    (report / 'result.json').write_text('earlier regression output')
    broker._record_generated([report])
    before = read_private(resources.manifest)['roots'][str(report)]
    assert broker._register_generated(root, ['dist/playwright-report']) == [report]
    assert read_private(resources.manifest)['roots'][str(report)] == before
    assert before['identity']
    assert resources.finalize('blocked')['state'] == 'confirmed'
    assert not report.exists()


@pytest.mark.parametrize('violation', ['foreign', 'symlink', 'parent-symlink', 'tracked',
                                     'protected', 'escape', 'empty', 'dot', 'noncanonical'])
def test_browser_output_registration_never_adopts_or_escapes(tmp_path, violation):
    owned, resources, broker, root, _ = configured(tmp_path)
    name = {'protected': '.git/new/report', 'escape': '../outside/report',
            'empty': '', 'dot': '.', 'noncanonical': 'dist//report'}.get(
                violation, 'dist/playwright-report')
    owned['policy']['browser_qa'] = {'artifact_paths': [name]}
    sentinel = tmp_path / 'precious'
    sentinel.mkdir()
    (sentinel / 'data').write_text('SAFE')
    if violation == 'parent-symlink':
        (root / 'dist').symlink_to(sentinel, target_is_directory=True)
    elif violation in {'foreign', 'symlink', 'tracked'}:
        (root / 'dist').mkdir()
        report = root / name
        if violation == 'symlink':
            report.symlink_to(sentinel, target_is_directory=True)
        else:
            report.mkdir()
            (report / 'data').write_text('preserve')
            if violation == 'tracked':
                _git(root, 'add', name)
    before = resources.manifest.read_bytes()
    with pytest.raises(ValueError):
        broker._register_generated(root, [name])
    assert resources.manifest.read_bytes() == before
    assert (sentinel / 'data').read_text() == 'SAFE'
    if violation in {'foreign', 'tracked'}:
        assert (root / name / 'data').read_text() == 'preserve'


@pytest.mark.skipif(sys.platform != 'darwin', reason='native macOS execution authority')
def test_real_native_check_output_is_reused_by_fixture_gate(api_fixture, tmp_path):
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_preparation import prepare_authority
    from devflow_temporal.delivery_store import DeliveryStore

    path, request = api_fixture
    raw = json.loads(path.read_text())
    repository = raw['repositories']['fixture']
    source = Path(repository['source_path'])
    (source / '.gitignore').write_text('dist/\nnode_modules/\n')
    (source / 'fixture.py').write_text(
        "import os, socket, time\nfrom pathlib import Path\n"
        "report=Path('dist/playwright-report')\nreport.mkdir(parents=True, exist_ok=True)\n"
        "(report/'result.json').write_text('retained fixture output')\n"
        "ports=[int(os.environ[k]) for k in ('QA_API_PORT','QA_WEB_PORT') if k in os.environ]\n"
        "sockets=[]\nfor port in ports:\n"
        " s=socket.socket();s.bind(('127.0.0.1',port));s.listen();sockets.append(s)\n"
        "if ports: time.sleep(2)\nprint('1 passed')\n"
    )
    _git(source, 'add', '.')
    _git(source, 'commit', '-qm', 'fixture output regression')
    repository['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    sockets = [socket.socket(), socket.socket()]
    for s in sockets:
        s.bind(('127.0.0.1', 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    recipe = {'id': 'regression', 'kind': 'check', 'cwd': '.',
              'argv': [sys.executable, 'fixture.py'], 'timeout_seconds': 30}
    repository.update(prepublish_checks=[recipe], checks=[recipe],
                      required_ci=['test'], project_url='https://github.com/users/example/projects/1',
                      assignee='example', browser_qa={
                          'id': 'fixture', 'argv': recipe['argv'], 'cwd': '.',
                          'ports': dict(zip(['QA_API_PORT', 'QA_WEB_PORT'], ports, strict=True)),
                          'env': {'JOBCTRL_E2E_ISOLATED': '1'}, 'read_roots': [],
                          'artifact_paths': ['dist/playwright-report'],
                          'test_count_regex': r'(\d+) passed', 'min_tests': 1,
                          'timeout_seconds': 30})
    auth = tmp_path / 'fixture-auth.json'
    auth.write_text('{"OPENAI_API_KEY":"unusable-owned-fixture"}')
    auth.chmod(0o600)
    raw.update(provider='codex', execution_mode='trusted-local', codex_auth_path=str(auth),
               codex_bin=str(distribution('openai-codex-cli-bin').locate_file(
                   'codex_cli_bin/bin/codex')))
    raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'}
                    for role in ['intake', 'implement', 'review', 'verify']}
    path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(request)
    owned = prepare_authority(store, store.submitted_spec(request['run_id']))
    broker = DeliveryBroker(store, owned)
    candidate = broker.prepare()['candidate']
    try:
        local = broker.run_checks(0, candidate)
        assert local['state'] == 'passed'
        report = broker._gate_path('verify', 0) / 'dist/playwright-report'
        assert (report / 'result.json').exists()
        custody = read_private(RunResources(owned).manifest)['roots'][str(report)]
        assert custody['identity']
        result = broker.run_browser_qa(0, candidate)
        assert result['state'] == 'passed', result.get('diagnostic')
        assert result['cleanup'] == 'confirmed' and result['test_count'] == 1
        assert len(result['artifacts']) == 1
        assert broker.run_browser_qa(0, candidate) == result
    finally:
        assert RunResources(owned).finalize('blocked')['state'] == 'confirmed'
    assert Path(result['artifacts'][0]['path']).is_file()
    assert not report.exists()
