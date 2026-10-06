from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_resources import private_directory


def setup(service):
    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request['run_id']))
    broker.prepare()
    return store, broker, {'spec': broker.spec, 'role': 'implement', 'iteration': 0,
                           'workspace': str(broker.checkout), 'candidate': broker.candidate()}


def test_role_can_retain_complete_original_probe_and_failure_after_scratch_cleanup(service):
    from devflow_temporal.delivery_role_evidence import allocate, seal, validate_handoff

    store, broker, request = setup(service)
    request = allocate(request, 'a' * 64)
    target = Path(request['artifact_directory'])
    (target / 'probe.py').write_text("print('original synthetic probe')\n")
    (target / 'failed.log').write_text('original import failure\n')
    result = seal(request)
    reference = result['role_artifacts']
    handoff = validate_handoff(reference, Path(broker.spec['state_dir']))
    assert {p['relative_path'] for p in handoff['artifacts']} == {'probe.py', 'failed.log'}
    assert all(Path(p['path']).read_bytes() for p in handoff['artifacts'])
    # A later resumed role can change only its authoring copy, not historical evidence.
    (target / 'failed.log').write_text('new attempt\n')
    assert 'original import failure' in Path(next(
        p['path'] for p in handoff['artifacts'] if p['relative_path'] == 'failed.log')).read_text()
    assert validate_handoff(reference, Path(broker.spec['state_dir'])) == handoff


@pytest.mark.parametrize('bad', ['symlink', 'hardlink', 'escape'])
def test_artifact_allocation_and_sealing_reject_foreign_files(service, tmp_path, bad):
    from devflow_temporal.delivery_role_evidence import allocate, seal

    _store, _broker, request = setup(service)
    request = allocate(request, 'b' * 64)
    target = Path(request['artifact_directory'])
    external = tmp_path / 'secret.txt'
    external.write_text('private')
    if bad == 'symlink':
        (target / 'probe.txt').symlink_to(external)
    elif bad == 'hardlink':
        import os
        os.link(external, target / 'probe.txt')
    else:
        request['artifact_directory'] = str(tmp_path)
    with pytest.raises(ValueError):
        seal(request)


def test_readonly_receipt_handoff_preserves_complete_logs_and_historical_binding(service):
    from devflow_temporal.delivery_role_evidence import allocate, read_context

    _store, broker, request = setup(service)
    source = Path(broker.spec['state_dir']) / 'checks/0'
    private_directory(source)
    log = source / 'test.log'
    log.write_text('first line\nactual 163 cases\nlast line\n')
    log.chmod(0o600)
    request['evidence_context'] = {'candidate': request['candidate'], 'iteration': 0,
        'checks': {'local': {'candidate_id': request['candidate']['id'], 'state': 'passed',
            'results': [{'argv': ['python', '-m', 'pytest'], 'passed': True,
                'test_count': 163, 'log': str(log),
                'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest(),
                'native_process': {'journal': 'private controller state'}}]}}}
    enriched = allocate(request, 'c' * 64)
    context = read_context(enriched)
    assert context['candidate'] == request['candidate']
    check = context['checks']['local']['results'][0]
    assert Path(check['log']).read_bytes() == log.read_bytes()
    assert 'native_process' not in check
    assert 'private controller state' not in json.dumps(context)
    assert Path(check['log']).is_relative_to(Path(broker.spec['state_dir']) / 'role-evidence')


def test_receipt_handoff_rejects_changed_or_foreign_log(service, tmp_path):
    from devflow_temporal.delivery_role_evidence import allocate

    _store, _broker, request = setup(service)
    log = tmp_path / 'foreign.log'
    log.write_text('foreign')
    log.chmod(0o600)
    request['evidence_context'] = {'checks': {'local': {'results': [
        {'log': str(log), 'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest()}]}}}
    with pytest.raises(ValueError, match='run'):
        allocate(request, 'd' * 64)


def test_review_receives_copied_original_probes_and_rejects_stale_source(service):
    from devflow_temporal.delivery_role_evidence import allocate, read_context, seal

    _store, broker, request = setup(service)
    author = allocate(request, 'e' * 64)
    (Path(author['artifact_directory']) / 'probe.py').write_text('print(42)\n')
    reference = seal(author)['role_artifacts']
    review = {**request, 'role': 'review',
              'evidence_context': {'role_artifacts': [reference]}}
    enriched = allocate(review, 'f' * 64)
    copied = read_context(enriched)['role_artifacts'][0]['retained_files'][0]
    assert Path(copied['path']).read_text() == 'print(42)\n'
    (broker.checkout / 'README.md').write_text('different source\n')
    review['candidate'] = broker.candidate()
    with pytest.raises(ValueError, match='another candidate'):
        allocate(review, '1' * 64)


def test_existing_sealed_recovery_checks_survive_current_empty_checks(service):
    from devflow_temporal.delivery_role_evidence import historical_context

    store, broker, request = setup(service)
    original = {'candidate': request['candidate'], 'iteration': 0,
                'checks': {'review': {'state': 'failed', 'detail': 'Actual measured finding'}}}
    recovery = {'state': {'checks': {}}, 'original_recovery': {'state': original}}
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET recovery_json=? WHERE run_id=?',
                   (json.dumps(recovery), broker.spec['run_id']))
    result = historical_context(store, broker.spec)
    assert result['previous_iterations'] == [original]


@pytest.mark.parametrize('retained_environment', [False, True])
def test_controller_prepares_real_locked_python_before_native_probe(service, tmp_path,
                                                                  retained_environment):
    import subprocess
    import sys

    from test_delivery_store import _git

    from devflow_temporal.delivery_store import DeliveryStore

    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    project = source / 'worker'
    project.mkdir()
    (project / 'pyproject.toml').write_text(
        '[project]\nname="probe-fixture"\nversion="0.0.1"\nrequires-python=">=3.12"\n'
        '[project.optional-dependencies]\ndev=[]\n')
    (project / 'test_probe.py').write_text('def test_case():\n    assert 1 == 1\n')
    (project / '.gitignore').write_text('.venv/\n')
    subprocess.run(['uv', 'lock', '--python', sys.executable], cwd=project,
                   env={'PATH': __import__('os').environ['PATH']},
                   capture_output=True, check=True)
    _git(source, 'add', 'worker')
    _git(source, 'commit', '-qm', 'test: locked probe fixture')
    store.config.raw['repositories']['fixture']['expected_base_sha'] = _git(
        source, 'rev-parse', 'HEAD')
    store.config.path.write_text(json.dumps(store.config.raw))
    store = DeliveryStore(store.config)
    store.submit({**request, 'base_ref': 'HEAD'})
    spec = store.spec(request['run_id'])
    spec.update(accepted_plan=json.dumps({'verification': ['Run pytest worker/test_probe.py']}),
                verification_test_paths=['worker/test_probe.py'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    retained = broker.checkout / 'worker/.venv'
    if retained_environment:
        retained.mkdir()
        (retained / 'original.txt').write_text('Retained original environment data\n')
        original_identity = retained.stat().st_ino
    before = broker.candidate()
    result = broker.run_implementation_preparation(0, before)
    assert result['state'] == 'passed'
    if retained_environment:
        assert retained.stat().st_ino == original_identity
        assert (retained / 'original.txt').read_text() == 'Retained original environment data\n'
        assert not (retained / 'bin/python').exists()
        python = Path(result['python_interpreters'][0]['interpreter'])
        assert python.is_relative_to(Path(spec['state_dir']) / 'transient/implementation-python')
        assert str(python) in result['diagnostic']
    else:
        python = broker.checkout / 'worker/.venv/bin/python'
    measured = subprocess.run([str(python), '-c',
        'import sys,test_probe; test_probe.test_case(); '
        'print(sys.prefix); print("normally imported probe")'],
        cwd=broker.checkout / 'worker', capture_output=True, text=True, check=True)
    assert str(python.parent.parent) in measured.stdout
    assert 'normally imported probe' in measured.stdout
    assert broker.candidate() == before
    assert len(result['results']) == 1
    assert result['results'][0]['argv'][1:4] == ['sync', '--locked', '--no-install-project']
    if retained_environment:
        from devflow_temporal.delivery_role_evidence import allocate, read_context
        role = {'spec': spec, 'role': 'implement', 'iteration': 0,
                'workspace': str(broker.checkout), 'candidate': before,
                'evidence_context': {'implementation_preparation': result}}
        enriched = allocate(role, '7' * 64)
        context = read_context(enriched)['implementation_preparation']
        assert context['python_interpreters'] == result['python_interpreters']
        assert context['diagnostic'] == result['diagnostic']


def test_no_dependency_plan_has_explicit_successful_noop_preparation(service):
    _store, broker, request = setup(service)
    result = broker.run_implementation_preparation(0, request['candidate'])
    assert result == {'state': 'passed', 'cleanup': 'confirmed', 'results': [],
                      'candidate_id': request['candidate']['id'], 'source_unchanged': True}


def test_prompt_and_profile_name_authorized_output_without_exposing_controller(service, tmp_path):
    from devflow_temporal.delivery_role_evidence import allocate
    from devflow_temporal.delivery_sandbox import prepare_native_role
    from devflow_temporal.role_runner import _task

    _store, broker, request = setup(service)
    auth = tmp_path / 'fixture-auth.json'
    auth.write_text('fixture auth')
    auth.chmod(0o600)
    request['spec']['provider'] = 'codex'
    request['spec']['policy'].update(host_sandbox='native-profile',
        execution_backend='native-macos', codex_auth_path=str(auth), codex_bin='/usr/bin/false')
    first = allocate(request, '2' * 64)
    attempt = Path(broker.spec['state_dir']) / 'attempts' / ('2' * 64)
    prepare_native_role(first, attempt)
    profile = Path(broker.spec['state_dir']) / 'role-homes/implement/codex/config.toml'
    content = profile.read_text()
    assert f'"{first["artifact_write_root"]}" = "write"' in content
    assert f'"{broker.spec["state_dir"]}/role-evidence" = "read"' in content
    assert f'"{broker.spec["state_dir"]}" = "read"' not in content
    second = allocate({**request, 'iteration': 1}, '3' * 64)
    prepare_native_role(second, attempt.with_name('3' * 64))
    assert profile.read_text() == content
    assert first['artifact_directory'] != second['artifact_directory']
    assert first['artifact_write_root'] == second['artifact_write_root']
    task = _task(first)
    assert first['artifact_directory'] in task.goal
    assert 'full stdout/stderr, failed attempts' in task.goal
