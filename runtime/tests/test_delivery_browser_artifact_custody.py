"""Browser output created by earlier checks retains controller resource custody."""
from __future__ import annotations

import fcntl
import hashlib
import json
import socket
import sys
from importlib.metadata import distribution
from pathlib import Path

import pytest
from test_delivery_api import api_fixture as api_fixture
from test_delivery_configured_resources import configured
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_finalized_resources,
    read_private,
    write_private,
)


@pytest.mark.skipif(sys.platform != 'darwin', reason='exclusive native preservation')
def test_failed_allocated_output_is_preserved_and_replayed_after_lost_ack(tmp_path, monkeypatch):
    from devflow_temporal import delivery_metadata_recovery as metadata
    from devflow_temporal.delivery_orphans import _disposable_work

    owned, resources, _broker, root, _ = configured(tmp_path, 'checkout')
    owned['provider'] = 'codex'
    owned['policy']['browser_qa'] = {'artifact_paths': ['dist/playwright-report']}
    owned['policy_digest'] = digest(owned['policy'])
    (root / '.gitignore').write_text('dist/\n')
    _git(root, 'add', '.')
    _git(root, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
         'commit', '-qm', 'retained output fixture')
    report = root / 'dist/playwright-report'
    report.parent.mkdir()
    resources.register(report, 'generated')
    report.mkdir()
    (report / '.last-run.json').write_text('ambiguous native role output')
    before = candidate_for(root)
    assert resources.finalize('blocked')['state'] == 'unknown'
    original = {name: (resources.root / name).read_bytes()
                for name in ('manifest.json', 'finalization.json')}
    immutable = metadata._immutable
    attempt = resources.state / 'attempts/unstarted'
    attempt.mkdir(parents=True, mode=0o700)
    owner = resources.state / 'attempt-owners/unstarted'
    owner.parent.mkdir(mode=0o700)
    owner.touch(mode=0o600)
    with owner.open('rb') as lease:
        fcntl.flock(lease.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            resources.finalize('blocked')
    assert all((resources.root / name).read_bytes() == raw for name, raw in original.items())
    assert report.is_dir()

    def lose_intent(path, value, **options):
        raise OSError('lost intent publication')

    monkeypatch.setattr(metadata, '_immutable', lose_intent)
    with pytest.raises(OSError, match='lost intent publication'):
        resources.finalize('blocked')
    assert all((resources.root / name).read_bytes() == raw for name, raw in original.items())
    assert report.is_dir()

    def lose_ack(path, value, **options):
        if path.name == 'receipt.json':
            raise OSError('lost preservation acknowledgement')
        immutable(path, value, **options)

    monkeypatch.setattr(metadata, '_immutable', lose_ack)
    failed = resources.finalize('blocked')
    assert failed['state'] == 'unknown' and not report.exists()
    manifest = read_private(resources.manifest)
    entry = manifest['roots'][str(report)]
    assert entry['identity'] is None and entry['state'] == 'allocated'
    assert entry['generated_output_preservation']['state'] == 'pending'
    row = {'cleanup': 'unknown', 'checks_json': '{}'}
    assert _disposable_work(manifest, row)
    folder = next((resources.root / 'preserved-generated-output').iterdir())
    destination = folder / 'output'
    assert (destination / '.last-run.json').read_text() == 'ambiguous native role output'
    intent = read_private(folder / 'intent.json')
    assert {name: raw.encode('utf-8') for name, raw in intent['original_bytes'].items()} == original
    assert candidate_for(root) == before
    monkeypatch.setattr(metadata, '_immutable', immutable)
    result = resources.finalize('blocked')
    assert result['state'] == result['resource_cleanup'] == 'confirmed'
    manifest = read_private(resources.manifest)
    assert manifest['roots'][str(report)]['identity'] is None
    assert _disposable_work(manifest, row)  # Projection acknowledgement still needs delivery.
    row = {'cleanup': 'confirmed', 'checks_json': json.dumps({'resource_cleanup': result})}
    assert not _disposable_work(manifest, row)
    observed = observe_finalized_resources(owned)
    assert observed['finalization_sha256'] == hashlib.sha256(
        (resources.root / 'finalization.json').read_bytes()).hexdigest()
    assert resources.finalize('blocked')['state'] == 'confirmed'
    manifest_bytes = resources.manifest.read_bytes()
    intent_bytes = (folder / 'intent.json').read_bytes()
    for changed in ('receipt', 'receipt_sha256', 'policy_digest', 'source'):
        altered = json.loads(manifest_bytes)
        marker = altered['roots'][str(report)]['generated_output_preservation']
        try:
            if changed in ('receipt', 'receipt_sha256'):
                marker[changed] = (str(resources.root / 'foreign/receipt.json')
                                   if changed == 'receipt' else '0' * 64)
            else:
                forged = json.loads(intent_bytes)
                forged['binding'][changed] = ('0' * 64 if changed == 'policy_digest'
                                               else str(root / 'dist/foreign'))
                write_private(folder / 'intent.json', forged)
                marker['intent_sha256'] = hashlib.sha256(
                    (folder / 'intent.json').read_bytes()).hexdigest()
            write_private(resources.manifest, altered)
            before_observation = resources.manifest.read_bytes()
            with pytest.raises(ValueError):
                observe_finalized_resources(owned)
            assert resources.manifest.read_bytes() == before_observation
        finally:
            (folder / 'intent.json').write_bytes(intent_bytes)
            resources.manifest.write_bytes(manifest_bytes)
    retained = destination.with_name('retained-original')
    destination.rename(retained)
    destination.mkdir()
    (destination / 'foreign').write_text('preserve replacement')
    assert resources.finalize('blocked')['state'] == 'unknown'
    assert (destination / 'foreign').read_text() == 'preserve replacement'
    destination.joinpath('foreign').unlink()
    destination.rmdir()
    retained.rename(destination)
    assert resources.finalize('blocked')['state'] == 'confirmed'
    assert candidate_for(root) == before


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
        readiness={'version': 1, 'generation': 0, 'browser_engines': ['chromium'],
                   'temporal_checks': []},
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
