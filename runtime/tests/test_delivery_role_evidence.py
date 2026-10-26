from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
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


@pytest.mark.parametrize('case', ['restored', 'source-repair'])
def test_evidence_only_repair_admits_bound_payload_progress(service, case):
    from devflow_temporal.delivery_activities import _role_result
    from devflow_temporal.delivery_role_evidence import allocate, seal

    store, broker, request = setup(service)
    if case != 'no-feature-diff':
        (broker.checkout / 'README.md').write_text('Reviewed feature\n')
    candidate = broker.candidate()
    request.update(candidate=candidate, iteration=1)
    prior = allocate(request, '1' * 64)
    (Path(prior['artifact_directory']) / 'original.log').write_text('retained original\n')
    prior_ref = seal(prior)['role_artifacts']
    request.update(iteration=2, resume_session='original-session',
                   findings=['Supply omitted original investigation evidence'])
    current = allocate(request, '2' * 64)
    target = Path(current['artifact_directory'])
    (target / ('renamed.log' if case == 'renamed' else 'original.log')).write_text(
        'retained original\n')
    if case not in {'identical', 'renamed'}:
        (target / 'restored-inputs.json').write_text(
            '' if case == 'empty' else '{"original_input":42}\n')
    if case == 'stale-input':
        current['candidate'] = {**candidate, 'id': 'stale'}
    if case == 'source-repair':
        (broker.checkout / 'README.md').write_text('Corrected feature source\n')
    reference = seal(current)['role_artifacts']
    if case == 'stale-source':
        manifest_path = Path(reference['path'])
        manifest = json.loads(manifest_path.read_text())
        manifest['source_candidate']['content_sha256'] = 'stale'
        manifest_path.write_text(json.dumps(manifest))
        reference['sha256'] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    with store._connect() as db:
        for key, iteration, ref in [('1' * 64, 1, prior_ref), ('2' * 64, 2, reference)]:
            db.execute(
                """INSERT INTO delivery_attempts
                   (job_key,run_id,role,iteration,candidate_id,state,session_id,result_json,cleanup)
                   VALUES (?,?,'implement',?,?,'finished','original-session',?,'confirmed')""",
                (key, broker.spec['run_id'], iteration, candidate['id'],
                 json.dumps({'role_artifacts': ref})),
            )
    if case == 'tampered':
        manifest = json.loads(Path(reference['path']).read_text())
        Path(manifest['artifacts'][-1]['path']).write_text('changed after seal\n')
    if case == 'foreign-attempt':
        reference = prior_ref
    result = {'status': 'pass', 'session_id': 'original-session', 'cleanup': 'confirmed',
              'role_artifacts': reference, 'findings': []}
    if case == 'unknown-cleanup':
        result['cleanup'] = 'unknown'
    if case == 'wrong-session':
        request['resume_session'] = 'different-session'
    after = _role_result(request, broker, broker.checkout, None, result)
    assert after['status'] == ('pass' if case in {'restored', 'source-repair'} else 'blocked')
    assert (broker.candidate() == candidate) is (case != 'source-repair')


@pytest.mark.parametrize('bad', ['symlink', 'escape'])
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
    assert result['results'][0]['argv'][1:3] == ['sync', '--locked']
    assert '--no-install-project' not in result['results'][0]['argv']
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


def test_controller_prepares_named_node_dependencies_before_role(service, tmp_path):
    import sys

    from test_delivery_store import _git

    from devflow_temporal.delivery_store import DeliveryStore

    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    package = source / 'api'
    (package / 'test').mkdir(parents=True)
    (package / 'test/audit.test.ts').write_text('test("audit", () => {});')
    (package / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
    (source / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (source / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    _git(source, 'add', '.')
    _git(source, 'commit', '-qm', 'test: accepted Node test fixture')
    store.config.raw['repositories']['fixture']['expected_base_sha'] = _git(
        source, 'rev-parse', 'HEAD')
    store.config.path.write_text(json.dumps(store.config.raw))
    store = DeliveryStore(store.config)
    store.submit({**request, 'base_ref': 'HEAD'})
    spec = store.spec(request['run_id'])
    marker = tmp_path / 'prepared-dependencies.txt'
    dependency = {'id': 'docs-install', 'argv': [sys.executable, '-c',
        f'from pathlib import Path; Path({str(marker)!r}).write_text("prepared")', '/store']}
    spec['accepted_plan'] = json.dumps({'verification': ['Run API audit.test.ts']})
    spec['policy']['prepublish_checks'] = [dependency]
    spec['policy_digest'] = digest(spec['policy'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    before = broker.candidate()
    result = broker.run_implementation_preparation(0, before)
    assert result['state'] == 'passed'
    assert marker.read_text() == 'prepared'
    assert [r['id'] for r in result['results']] == ['docs-install']
    assert broker.candidate() == before
    assert 'verification_test_paths' not in spec


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
    request['spec']['policy_digest'] = digest(request['spec']['policy'])
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


def test_node_preparation_handoff_names_tool_paths_and_login_shell_environment(service):
    from devflow_temporal.delivery_role_evidence import allocate, read_context
    from devflow_temporal.role_runner import _task

    _, broker, request = setup(service)
    tools = {'node_interpreter': {'absolute_path': '/frozen/node22/bin/node',
             'realpath': '/frozen/node22/bin/node', 'sha256': 'a' * 64,
             'version': 'v22.21.1', 'modules_ABI': '127'},
             'corepack': {'absolute_path': '/frozen/node22/bin/corepack', 'sha256': 'b' * 64},
             'environment': {'PATH': '/frozen/node22/bin:/usr/bin',
                             'COREPACK_HOME': '/frozen/cache',
                             'npm_config_nodedir': '/frozen/node22'}}
    enriched = allocate({**request, 'evidence_context': {
        'implementation_preparation': {'node_toolchain': tools,
            'native_addon_authority': {'dependency_links': {
                'better-sqlite3@12.9.0': {'bindings': 'bindings@1.5.0'}}}}}}, '7' * 64)
    assert read_context(enriched)['implementation_preparation']['node_toolchain'] == tools
    assert read_context(enriched)['implementation_preparation']['native_addon_authority'] == {
        'dependency_links': {'better-sqlite3@12.9.0': {'bindings': 'bindings@1.5.0'}}}
    task = _task(enriched)
    assert 'node_toolchain.node_interpreter.absolute_path' in task.goal
    assert '/frozen/node22/bin/node' in task.goal
    assert 'set the recorded environment inside that shell' in task.goal


@pytest.mark.parametrize('role', ['implement', 'review', 'verify'])
def test_large_retained_history_stays_complete_without_overflowing_role_input(service, role):
    from devflow_temporal.delivery_role_evidence import allocate, read_context
    from devflow_temporal.role_runner import _task

    _, _broker, request = setup(service)
    diagnostic = 'retained-diagnostic-' + 'x' * 1_100_000
    checked = {'candidate_id': request['candidate']['id'], 'state': 'failed',
               'results': [{'id': 'failed-original-command', 'passed': False,
                            'diagnostic': diagnostic}]}
    context = {'checks': {'local': checked}, 'previous_iterations': [
        {'iteration': 1, 'checks': {'local': checked}}]}
    request = {**request, 'role': role, 'findings': ['Keep the unresolved source defect'],
               'evidence_context': context}
    if role != 'implement':
        request['check_evidence'] = checked
    enriched = allocate(request, 'a' * 64)
    reference = enriched['receipt_handoff']
    original = Path(reference['path']).read_bytes()
    assert len(original) > 1_048_576
    assert read_context(enriched) == context
    goal = _task(enriched).goal
    assert len(goal) < 100_000
    assert reference['path'] in goal and reference['sha256'] in goal
    assert 'previous_iterations' in goal and 'checks' in goal
    assert 'Keep the unresolved source defect' in goal
    assert diagnostic not in goal
    assert Path(reference['path']).read_bytes() == original
    # A small prompt cannot waive authentication of its complete evidence.
    Path(reference['path']).write_bytes(original + b' ')
    with pytest.raises(ValueError, match='handoff changed'):
        _task(enriched)


def test_rejected_same_iteration_artifacts_do_not_replace_the_accepted_producer(service):
    from devflow_temporal.delivery_role_evidence import allocate, read_context, seal
    from devflow_temporal.delivery_workflow import _implementation_artifacts

    _, broker, request = setup(service)
    rejected = allocate(request, '1' * 64)
    stale = seal(rejected)['role_artifacts']
    old_bytes = Path(stale['path']).read_bytes()
    (broker.checkout / 'README.md').write_text('Actual accepted repair\n')
    request = {**request, 'candidate': broker.candidate()}
    accepted = allocate(request, '2' * 64)
    (Path(accepted['artifact_directory']) / 'HANDOFF.md').write_text('Accepted repair evidence\n')
    current = seal(accepted)['role_artifacts']
    roles = [{'role': 'implement', 'iteration': 0, 'status': 'blocked', 'role_artifacts': stale},
             {'role': 'implement', 'iteration': 0, 'status': 'pass', 'role_artifacts': current}]
    review = {**request, 'role': 'review'}
    # The actual old boundary fails when a rejected empty seal is marked current.
    with pytest.raises(ValueError, match='another candidate content'):
        allocate({**review, 'evidence_context': {'role_artifacts': [stale, current]}}, '3' * 64)
    references = _implementation_artifacts(roles, 0)
    handoff = allocate({**review, 'evidence_context': {'role_artifacts': references}}, '4' * 64)
    assert references == [current]
    assert read_context(handoff)['role_artifacts']
    assert Path(stale['path']).read_bytes() == old_bytes
    # Never silently fall back to older evidence if the accepted producer is corrupt.
    bad = [*roles, {**roles[-1], 'role_artifacts': stale}]
    with pytest.raises(ValueError, match='another candidate content'):
        allocate({**review, 'evidence_context': {
            'role_artifacts': _implementation_artifacts(bad, 0)}}, '5' * 64)


@pytest.mark.parametrize('role', ['review', 'verify'])
def test_separate_current_checks_are_in_the_authenticated_prompt_handoff(service, role):
    from devflow_temporal.delivery_role_evidence import allocate, read_context
    from devflow_temporal.role_runner import _task

    _, _broker, request = setup(service)
    checked = {'state': 'passed', 'candidate_id': request['candidate']['id'],
               'results': [{'id': 'actual-local-check', 'passed': True,
                            'diagnostic': 'complete current receipt'}]}
    request = {**request, 'role': role, 'evidence_context': {'role_artifacts': []},
               'check_evidence': checked}
    enriched = allocate(request, 'b' * 64)
    assert read_context(enriched)['checks']['local'] == checked
    assert request['evidence_context'] == {'role_artifacts': []}
    assert 'checks.local' in _task(enriched).goal
    with pytest.raises(ValueError, match='conflicts'):
        allocate({**request, 'evidence_context': {'checks': {'local': {
            **checked, 'candidate_id': 'stale'}}}}, 'c' * 64)


@pytest.fixture
def dependency_handoff(service, monkeypatch):
    """Real receipt producer/store/checkpoints; modeled GitHub transport and role verdicts."""
    import sys
    from copy import deepcopy

    from test_delivery_feature_execution import feature_service
    from test_delivery_feature_merge import Remote
    from test_delivery_store import _git

    from devflow_temporal.delivery_feature_activities import finish_worker
    from devflow_temporal.delivery_feature_execution import (
        continue_feature,
        register_worker,
        registry,
        worker_key,
        worker_spec,
    )
    from devflow_temporal.delivery_github_contract import GitHubDelivery, ordered_chunks

    store, supplied, _ = feature_service(service, monkeypatch)
    plan = json.loads(supplied['accepted_plan'])
    final = plan['workstreams'][0]['chunks'].pop()
    final['depends_on'] = ['client']
    plan['workstreams'][1]['chunks'][0]['depends_on'] = ['model']
    plan['workstreams'].append({'id': 'final', 'title': 'Final comparison', 'issue_number': 12,
                               'acceptance': ['Compare against baseline'], 'chunks': [final]})
    supplied['accepted_plan'] = json.dumps(plan)
    store.submit(supplied)
    parent = store.effective_spec(supplied['run_id'])
    shared, token = registry(parent), parent['feature_delivery']['owner']
    issues = {stream['id']: {'id': 'I_' + str(stream['issue_number']),
              'number': stream['issue_number'], 'url':
              'https://github.com/example/fixture/issues/' + str(stream['issue_number'])}
              for stream in plan['workstreams']}
    record = {'comment_id': 11, 'comment_node_id': 'IC_11', 'manifest': {
        'version': 1, 'issue_id': 'I_feature', 'repository_id': 'R_fixture',
        'revision': 1, 'creation_key': 'create-feature', 'plan': plan,
        'workstream_issues': issues, 'publication': {'stack_id': None, 'members': []}}}
    for key, value in [('accepted-plan', {'digest': digest(plan)}),
                       ('github-record', {'comment_id': 11, 'comment_node_id': 'IC_11'}),
                       ('workstream-issues', issues)]:
        shared.checkpoint(token, key, value)
    remote = Remote(record)
    monkeypatch.setattr(GitHubDelivery, 'api', remote.api)
    # These are the original immutable worker inputs, before coordinator continuation.
    ancestor, immediate, receiver = [worker_spec(parent, chunk, issues[chunk['workstream_id']],
        kind=kind, base_sha=parent['base_sha'], base_branch='main') for chunk, kind in
        zip(ordered_chunks(plan), ['chunk', 'chunk', 'build'], strict=True)]
    sources = [ancestor, immediate]
    for source in sources:
        measurement = source['feature_worker']['chunk_id'] + ' prerequisite measurement: 42'
        source['policy']['checks'] = [{'id': 'measurement', 'argv': [sys.executable, '-c',
            'print(' + repr(measurement) + ')']}]
        source['policy_digest'] = digest(source['policy'])
    def assign(spec):
        register_worker(store, parent, spec)
        worker = spec['feature_worker']
        shared.checkpoint(token, f"assignment:{worker['chunk_id']}:{worker['kind']}", {
            'run_id': spec['run_id'], 'store_path': str(store.config.tracking_db),
            'chunk_id': worker['chunk_id'], 'kind': worker['kind']})
        broker = DeliveryBroker(store, spec)
        broker.prepare()
        (broker.checkout / 'README.md').write_text(worker['chunk_id'] + ' contribution\n')
        _git(broker.checkout, 'commit', '-am', 'feat: original worker', '--signoff')
        return broker

    source_checks = {}
    for index, source in enumerate(sources):
        number = 20 + index
        producer = assign(source)
        candidate = producer.candidate()
        local = producer.run_checks(0, candidate)
        assert local['state'] == 'passed' and len(local['results']) == 1
        checks = {'local': local, **{stage: {'state': 'passed', 'candidate_id': candidate['id']}
                                   for stage in ('prepublish', 'review', 'qa')}}
        pr = {'number': number, 'url': 'https://github.com/example/fixture/pull/' + str(number),
              'head': candidate['head'], 'state': 'OPEN', 'base': source['base_sha']}
        record['manifest']['publication']['members'].append({
            **{key: pr[key] for key in ('number', 'url', 'head')},
            'chunk_id': source['feature_worker']['chunk_id'], 'branch': source['branch'],
            'base_branch': sources[index - 1]['branch'] if index else 'main'})
        record['manifest']['publication']['stack_id'] = 42 if index else None
        with store._connect() as db:
            db.execute("INSERT INTO delivery_effects(effect_key,run_id,kind,request_json,state,"
                       "observed_json,updated_at) VALUES (?,?,'publish',?,'complete',?,?)",
                       ('publish-' + source['run_id'], source['run_id'],
                        json.dumps({'iteration': 0, 'input_candidate_id': candidate['id']}),
                        json.dumps(pr), '2026-10-16T00:00:00+00:00'))
        store.project(source['run_id'], phase='delivered', execution_state='terminal',
            event_type='delivered', message='Fixture prerequisite completed', candidate=candidate,
            pull_request=pr, checks=checks, iteration=0, protocol_revision=10,
            outcome='delivered', cleanup='confirmed', error=None)
        # Use the real completion producer for the verified checkpoint and registry retirement.
        finish_worker(parent, source['run_id'])
        source_checks[source['run_id']] = checks
    assign(receiver)
    shared.finish_worker(token, worker_key(receiver['run_id'], token), {'cleanup': 'confirmed'})
    shared.stop(token, 'stopped-original-coordinator', {})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='cancelled',outcome='cancelled',"
                   "execution_state='terminal' WHERE run_id=?", (parent['run_id'],))
        store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: {'result': {}})
    continued = continue_feature(store, parent['run_id'], {
        'command_id': 'continue-for-dependent-worker', 'expected_revision': 1})
    current = store.effective_spec(continued['run_id'])
    shared.reserve_worker(current['feature_delivery']['owner'],
                          worker_key(receiver['run_id'], current['feature_delivery']['owner']),
                          receiver['feature_worker']['workstream_id'])
    request = {'spec': receiver, 'role': 'implement', 'iteration': 1,
               'workspace': receiver['checkout'],
               'candidate': DeliveryBroker(store, receiver).candidate(),
               'resume_session': 'original-dependent-session', 'findings': ['Compare prerequisite']}
    return store, request, sources, source_checks, record, deepcopy(parent)


@pytest.mark.parametrize('role', ['implement', 'review', 'verify'])
def test_completed_dependency_reaches_original_worker_roles_after_coordinator_continuation(
        dependency_handoff, role):
    from copy import deepcopy

    from devflow_temporal.delivery_role_evidence import allocate, read_context
    from devflow_temporal.role_runner import _task
    from devflow_temporal.supervisor import DeliverySupervisor

    store, request, sources, source_checks, record, old_parent = dependency_handoff
    request = {**request, 'role': role, 'evidence_context': {'checks': {}}}
    assert request['spec']['feature_worker']['parent_run_id'] == old_parent['run_id']
    frozen = deepcopy(request)
    key = DeliverySupervisor._job_key(request)
    originals = [Path(source_checks[source['run_id']]['local']['results'][0]['log'])
                 for source in sources]
    original_bytes = [path.read_bytes() for path in originals]
    copied = allocate(request, key, store=store)
    context = read_context(copied)
    assert request == frozen
    assert context['checks'] == {}  # Historical passes never become current gates.
    dependencies = context['historical_dependencies']
    assert [item['provenance']['chunk_id'] for item in dependencies] == ['model', 'client']
    for index, (dependency, source) in enumerate(zip(dependencies, sources, strict=True)):
        checks = source_checks[source['run_id']]
        assert dependency['candidate']['id'] != request['candidate']['id']
        assert dependency['provenance'] == {
            'kind': 'completed_feature_dependency', 'historical': True,
            'run_id': source['run_id'], 'chunk_id': source['feature_worker']['chunk_id'],
            'feature_issue_id': 'I_feature',
            'accepted_plan_sha256': digest(record['manifest']['plan']),
            'source_spec_sha256': digest(source), 'checks_sha256': digest(checks),
            'publication': {'head': dependency['candidate']['head'], 'number': 20 + index}}
        log = Path(dependency['checks']['local']['results'][0]['log'])
        assert log.is_relative_to(Path(request['spec']['state_dir']) / 'role-evidence' / key)
        assert log.read_bytes() == original_bytes[index]
        assert originals[index].read_bytes() == original_bytes[index]
        assert 'prerequisite measurement: 42' in log.read_text()
    assert 'historical_dependencies' in _task(copied).goal
    assert 'native_process' not in json.dumps(context)


@pytest.mark.parametrize('drift', ['missing', 'unfinished', 'changed-checks', 'unrelated'])
def test_dependency_handoff_refuses_unbound_or_uncompleted_sources(dependency_handoff, drift):
    from devflow_temporal.delivery_role_evidence import allocate

    store, request, sources, _, _, _ = dependency_handoff
    source = sources[0]  # The immediate prerequisite stays verified; refuse its broken ancestor.
    if drift in {'missing', 'unrelated'}:
        from devflow_temporal.delivery_feature_execution import registry

        with registry(request['spec']).connect() as db:
            key = 'verified:model' if drift == 'missing' else 'assignment:model:chunk'
            db.execute('DELETE FROM execution_checkpoints WHERE checkpoint_key=?', (key,))
    else:
        with store._connect() as db:
            if drift == 'unfinished':
                db.execute("UPDATE delivery_runs SET outcome='blocked' WHERE run_id=?",
                           (source['run_id'],))
            else:
                db.execute("UPDATE delivery_runs SET checks_json='{}' WHERE run_id=?",
                           (source['run_id'],))
    with pytest.raises(ValueError, match='dependency'):
        allocate(request, '8' * 64, store=store)


def test_dependency_copy_refuses_changed_bytes_but_launched_request_keeps_frozen_handoff(
        dependency_handoff):
    from devflow_temporal.delivery_resources import write_private
    from devflow_temporal.delivery_role_evidence import allocate, read_context
    from devflow_temporal.supervisor import DeliverySupervisor

    store, request, sources, source_checks, _, _ = dependency_handoff
    checks = source_checks[sources[0]['run_id']]
    supervisor = DeliverySupervisor(store, capacity=1)
    key = supervisor._job_key(request)
    enriched = allocate(request, key, store=store)
    handoff = read_context(enriched)
    path = Path(request['spec']['state_dir']) / 'attempts' / key / 'request.json'
    write_private(path, enriched)
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,cleanup,result_json) "
                   "VALUES (?,?,'implement',1,?,'finished','confirmed','{}')",
                   (key, request['spec']['run_id'], request['candidate']['id']))
    original = Path(checks['local']['results'][0]['log'])
    original.write_text('Changed after the original launch\n')
    retained = supervisor.retained_request(request)
    assert retained == enriched and read_context(retained) == handoff
    assert supervisor._job_key(retained) == key
    with pytest.raises(ValueError, match='changed'):
        allocate({**request, 'iteration': 2}, '9' * 64, store=store)
    with pytest.raises(RuntimeError, match='request changed'):
        supervisor.retained_request({**request, 'findings': ['Different request']})


def test_readiness_enrichment_survives_launched_role_reattachment(service):
    from devflow_temporal.delivery_resources import write_private
    from devflow_temporal.supervisor import DeliverySupervisor

    store, broker, request = setup(service)
    request['spec'] = {**request['spec'], 'readiness_version': 1}
    request['evidence_context'] = {'source': 'original'}
    supervisor = DeliverySupervisor(store, capacity=1)
    key = supervisor._job_key(request)
    enriched = {**request,
                'evidence_context': {'source': 'original', 'readiness': {'state': 'passed'}},
                'readiness_worker_assignment': {'owner': {'generation': 1}}}
    path = Path(broker.spec['state_dir']) / 'attempts' / key / 'request.json'
    write_private(path, enriched)
    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                   "state,cleanup,result_json) VALUES (?,?,'implement',0,?,"
                   "'finished','confirmed','{}')",
                   (key, request['spec']['run_id'], request['candidate']['id']))
    assert supervisor.retained_request(request) == enriched
    with pytest.raises(RuntimeError, match='request changed'):
        supervisor.retained_request({**request, 'evidence_context': {'source': 'different'}})
    with pytest.raises(RuntimeError, match='request changed'):
        supervisor.retained_request({**request, 'readiness_worker_assignment': {}})
