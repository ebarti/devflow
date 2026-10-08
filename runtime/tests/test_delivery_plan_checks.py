from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_delivery_store import _git

from devflow_temporal.delivery_plan_checks import planned_checks


@pytest.fixture
def project(tmp_path, monkeypatch):
    checkout = tmp_path / 'checkout'
    project = checkout / 'worker'
    (project / 'tests').mkdir(parents=True)
    (project / 'src').mkdir()
    (project / 'tests/test_owned.py').write_text('def test_owned(): assert True\n')
    (project / 'pyproject.toml').write_text('[project]\nname="fixture"\nversion="1"\n')
    (project / 'uv.lock').write_text('version=1\n')
    _git(checkout, 'init', '-q')
    _git(checkout, 'add', '.')
    manager = tmp_path / 'uv'
    manager.write_bytes(b'fixed-manager')
    monkeypatch.setattr('devflow_temporal.delivery_plan_checks.shutil.which',
                        lambda _: str(manager))
    spec = {'accepted_plan': json.dumps({'verification': ['Run test_owned.py twice',
                                                          'Retain test_owned.py evidence']})}
    return spec, checkout, project, tmp_path / 'evidence'


def test_recipe_uses_only_named_owned_tests_locked_deps_and_retained_junit(project):
    spec, checkout, _, evidence = project
    before = _git(checkout, 'diff', '--cached')
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 2
    deps, test = checks
    assert deps['argv'][1:4] == ['sync', '--locked', '--extra']
    assert '--no-install-project' not in deps['argv']
    assert deps['generated_directories'] == ['worker/.venv']
    assert test['argv'][:4] == [str(checkout / 'worker/.venv/bin/python'), '-m',
                              'pytest', 'tests/test_owned.py']
    assert '--junitxml=' + str(evidence / test['id'] / 'pytest-artifacts/junit.xml') \
        in test['argv']
    assert test['min_tests'] == 1
    assert test['plan_provenance'] == deps['plan_provenance']
    assert set(test['plan_provenance']['metadata']) == {'pyproject.toml', 'uv.lock'}
    assert _git(checkout, 'diff', '--cached') == before


@pytest.mark.parametrize('node', [False, True])
def test_future_authorized_test_prepares_dependencies_but_verification_stays_strict(project, node):
    spec, checkout, root, evidence = project
    if node:
        root = checkout / 'api'
        (root / 'test').mkdir(parents=True)
        (root / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
        (checkout / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
        (checkout / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
        name, relative = 'future.test.ts', 'api/test/future.test.ts'
        _git(checkout, 'add', '.')
    else:
        name, relative = 'test_future.py', 'worker/tests/test_future.py'
    spec.update(accepted_plan=json.dumps({'verification': [f'Add and execute {name}']}),
                policy={'allowed_paths': [relative]})
    before = dict(spec)
    checks = planned_checks(spec, checkout, evidence, preparation=True)
    assert any(name in ' '.join(check['argv']) for check in checks)
    assert spec == before and not (checkout / relative).exists()
    with pytest.raises(ValueError, match='tracked owner'):
        planned_checks(spec, checkout, evidence)
    (checkout / relative).write_text('new feature regression')
    assert planned_checks(spec, checkout, evidence, preparation=True) == checks
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)
    _git(checkout, 'add', relative)
    final = planned_checks(spec, checkout, evidence)
    assert any(name in ' '.join(check['argv']) for check in final)


@pytest.mark.parametrize('failure', ['unauthorized', 'ambiguous', 'escape', 'symlink', 'directory',
                                     'project-link', 'lock'])
def test_future_test_preparation_rejects_unsealed_owners_and_metadata(project, failure):
    spec, checkout, root, evidence = project
    relative = 'worker/tests/test_future.py'
    spec.update(accepted_plan=json.dumps({'verification': ['Create and run test_future.py']}),
                policy={'allowed_paths': [relative]})
    if failure == 'unauthorized':
        spec['policy']['allowed_paths'] = []
    elif failure == 'ambiguous':
        spec['policy']['allowed_paths'].append('worker/other/test_future.py')
    elif failure == 'escape':
        spec['policy']['allowed_paths'] = ['../worker/tests/test_future.py']
    elif failure == 'symlink':
        (checkout / relative).symlink_to(root / 'missing')
    elif failure == 'directory':
        (checkout / relative).mkdir()
    elif failure == 'project-link':
        (root / 'tests').rename(root / 'original-tests')
        (root / 'tests').symlink_to(root / 'original-tests', target_is_directory=True)
    else:
        _git(checkout, 'rm', '--cached', 'worker/uv.lock')
    with pytest.raises((ValueError, FileNotFoundError)):
        planned_checks(spec, checkout, evidence, preparation=True)


def test_broker_prepares_only_dependencies_and_records_future_project_custody(project, monkeypatch):
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_resources import RunResources

    spec, checkout, root, evidence = project
    state = evidence.parent / 'run-fixture'
    state.mkdir(mode=0o700)
    spec.update(run_id='run-fixture', state_dir=str(state), checkout=str(checkout),
                accepted_plan=json.dumps({'verification': ['Create and run test_future.py']}),
                policy={'allowed_paths': ['worker/tests/test_future.py'],
                        'host_sandbox': 'trusted-local'})
    resources = RunResources(spec)
    original = checkout.with_name('fixture-source')
    checkout.rename(original)
    resources.register(checkout, 'checkout')
    original.rename(checkout)
    resources.created(checkout)
    broker = object.__new__(DeliveryBroker)
    broker.spec, broker.checkout = spec, checkout
    broker.evidence_dir = broker.state_dir = state
    candidate = {'id': 'a' * 64}
    monkeypatch.setattr(broker, 'candidate', lambda: candidate)
    observed = []

    def dependency_checks(_checkout, checks, _folder, _candidate, **_options):
        observed.extend(checks)
        resources.register(root / '.venv', 'generated')
        return {'state': 'passed', 'results': [{'cleanup': 'confirmed'}]}

    monkeypatch.setattr(broker, '_run_check_list', dependency_checks)
    assert broker.run_implementation_preparation(0, candidate)['state'] == 'passed'
    assert len(observed) == 1 and observed[0]['id'].startswith('planned-python-dependencies-')
    assert 'test_future.py' not in ' '.join(observed[0]['argv'])
    assert not (root / '.venv').exists() and not (root / 'tests/test_future.py').exists()


@pytest.mark.parametrize('failure', ['missing', 'duplicate', 'symlink', 'lock'])
def test_recipe_rejects_untracked_ambiguous_or_unlocked_sources(project, failure):
    spec, checkout, root, evidence = project
    if failure == 'missing':
        _git(checkout, 'rm', '--cached', 'worker/tests/test_owned.py')
    elif failure == 'duplicate':
        (root / 'test_owned.py').write_text('duplicate')
        _git(checkout, 'add', '.')
    elif failure == 'symlink':
        target = root / 'tests/test_owned.py'
        target.unlink()
        target.symlink_to(Path(__file__).resolve())
    else:
        (root / 'uv.lock').unlink()
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)


def test_freeform_plan_and_steps_cannot_supply_commands(project):
    spec, checkout, _, evidence = project
    for plan in ['Run test_owned.py', json.dumps({'steps': ['Run test_owned.py']}),
                 json.dumps({'verification': ['echo arbitrary-command']})]:
        assert planned_checks({**spec, 'accepted_plan': plan}, checkout, evidence) == []


def test_explicit_tracked_selection_executes_prose_plan_without_changing_it(project):
    spec, checkout, _, evidence = project
    spec['accepted_plan'] = json.dumps({'verification': ['Execute focused worker regressions']})
    spec['verification_test_paths'] = ['worker/tests/test_owned.py']
    before = dict(spec)
    checks = planned_checks(spec, checkout, evidence)
    assert checks[1]['argv'][3] == 'tests/test_owned.py'
    assert checks[1]['plan_provenance']['selection_sha256']
    assert spec == before


@pytest.mark.parametrize('paths', [
    ['/tmp/test_owned.py'], ['../test_owned.py'], ['worker/tests/missing.test.ts'],
    ['worker/tests/test_owned.py'] * 2, ['worker/pyproject.toml'], 'test_owned.py',
])
def test_explicit_selection_rejects_missing_or_unbounded_test_authority(project, paths):
    spec, checkout, _, evidence = project
    with pytest.raises((ValueError, FileNotFoundError)):
        planned_checks({**spec, 'verification_test_paths': paths}, checkout, evidence)


def test_explicit_selection_requires_a_structured_test_step_and_fixed_test_file(project):
    spec, checkout, root, evidence = project
    spec.update(accepted_plan=json.dumps({'verification': ['Inspect documentation']}),
                verification_test_paths=['worker/tests/test_owned.py'])
    with pytest.raises(ValueError, match='structured test step'):
        planned_checks(spec, checkout, evidence)
    spec['accepted_plan'] = json.dumps({'verification': ['Run focused tests']})
    target = root / 'tests/test_owned.py'
    target.unlink()
    target.symlink_to(Path(__file__).resolve())
    with pytest.raises(ValueError, match='fixed tracked test'):
        planned_checks(spec, checkout, evidence)


@pytest.mark.parametrize('explicit_selector', [False, True])
def test_node_selection_uses_locked_vitest_and_retained_junit(project, explicit_selector):
    spec, checkout, _, evidence = project
    package = checkout / 'api'
    (package / 'test').mkdir(parents=True)
    (package / 'test/audit.test.ts').write_text('test("audit", () => {});')
    (package / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
    (checkout / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (checkout / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    _git(checkout, 'add', '.')
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run API audit.test.ts twice and retain audit.test.ts evidence']})
    if explicit_selector:
        spec['verification_test_paths'] = ['api/test/audit.test.ts']
    before = dict(spec)
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 1
    test = checks[0]
    assert test['argv'][:6] == ['corepack', 'pnpm', 'exec', 'vitest', 'run', 'test/audit.test.ts']
    assert '--reporter=junit' in test['argv']
    assert test['cwd'] == 'api'
    assert test['min_tests'] == 1
    assert 'pnpm-lock.yaml' in test['plan_provenance']['metadata']
    assert spec == before
    (checkout / 'pnpm-lock.yaml').unlink()
    with pytest.raises(FileNotFoundError):
        planned_checks(spec, checkout, evidence)


@pytest.mark.parametrize('failure', ['missing', 'duplicate', 'symlink', 'package', 'bounded'])
def test_named_node_tests_reject_missing_ambiguous_or_unsealed_owners(project, failure):
    spec, checkout, _, evidence = project
    package = checkout / 'api'
    (package / 'test').mkdir(parents=True)
    test = package / 'test/audit.test.ts'
    test.write_text('test("audit", () => {});')
    (package / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
    (checkout / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (checkout / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    _git(checkout, 'add', '.')
    spec['accepted_plan'] = json.dumps({'verification': ['Run API audit.test.ts']})
    if failure == 'missing':
        _git(checkout, 'rm', '--cached', 'api/test/audit.test.ts')
    elif failure == 'duplicate':
        (package / 'audit.test.ts').write_text('duplicate')
        _git(checkout, 'add', '.')
    elif failure == 'symlink':
        test.unlink()
        test.symlink_to(Path(__file__).resolve())
    elif failure == 'package':
        (package / 'package.json').write_text('{}')
    else:
        spec['accepted_plan'] = json.dumps({'verification': [
            'Run ' + ' '.join(f'case{i}.test.ts' for i in range(33))]})
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)


def freeze_recipes(spec, checkout, *, stage=True):
    """Establish real admitted recipe metadata before testing its validation."""
    _git(checkout, 'config', 'user.name', 'Fixture')
    _git(checkout, 'config', 'user.email', 'fixture@example.invalid')
    if stage:
        _git(checkout, 'add', '.')
    _git(checkout, 'commit', '--allow-empty', '-qm', 'Admitted recipes')
    spec.update(source_path=str(checkout), base_sha=_git(checkout, 'rev-parse', 'HEAD'))


@pytest.mark.parametrize('foreign_environment', [False, True])
def test_mixed_recipe_registers_python_environment_before_first_creator(
    project, foreign_environment,
):
    import shutil

    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_resources import RunResources, read_private

    spec, source, _, evidence = project
    scripts = source / 'scripts'
    scripts.mkdir()
    (scripts / 'checks.toml').write_text(
        'schema_version=1\n[checks.worker]\nkind="junit"\n'
        'argv=["uv","run","--project","worker","--locked","pytest",'
        '"tests/test_owned.py","--junitxml={report_path}"]\n')
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run checks.worker JUnit recipe from scripts/checks.toml and test_owned.py.']})
    freeze_recipes(spec, source)
    state = source.parent / 'run-owned'
    state.mkdir(mode=0o700)
    checkout = source.parent / 'owned-checkout'
    spec.update(run_id='run-owned', state_dir=str(state), checkout=str(checkout),
                policy={'host_sandbox': 'trusted-local'})
    resources = RunResources(spec)
    resources.register(checkout, 'checkout')
    shutil.copytree(source, checkout)
    resources.created(checkout)
    recipe, dependencies, _ = planned_checks(spec, checkout, evidence)
    broker = object.__new__(DeliveryBroker)
    broker.spec = spec
    environment = checkout / 'worker/.venv'
    if foreign_environment:
        environment.mkdir(mode=0o700)
        with pytest.raises(ValueError, match='existing unregistered resource'):
            broker._register_generated(checkout, dependencies['generated_directories'])
        assert str(environment) not in read_private(resources.manifest)['roots']
        return
    roots = broker._register_generated(checkout, recipe.get('generated_directories', []))
    # Logical filesystem effect of the approved uv recipe; no command is launched.
    environment.mkdir(mode=0o700)
    broker._record_generated(roots)
    broker._register_generated(checkout, dependencies['generated_directories'])
    entry = read_private(resources.manifest)['roots'][str(environment)]
    assert entry['state'] == 'created'
    assert entry['identity']['inode'] == environment.stat().st_ino


@pytest.fixture
def junit_project(project):
    spec, checkout, _, evidence = project
    scripts = checkout / 'scripts'
    scripts.mkdir()
    metadata = scripts / 'checks.toml'
    metadata.write_text('schema_version=1\n[checks.scripts]\nkind="junit"\n'
                        'argv=["node","--test","--test-reporter=junit",'
                        '"--test-reporter-destination={report_path}","scripts/report.test.mjs"]\n'
                        'timeout_seconds=600\n')
    (scripts / 'report.test.mjs').write_text(
        'import test from "node:test"; test("actual case", () => {});\n')
    _git(checkout, 'add', '.')
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run the configured scripts recipe from scripts/checks.toml with owned JUnit output.']})
    freeze_recipes(spec, checkout)
    return spec, checkout, metadata, evidence


def test_real_tracked_recipe_retains_candidate_bound_junit(junit_project):
    import shutil
    import subprocess

    from devflow_temporal.delivery_check_evidence import junit_counts, retain_artifacts

    spec, checkout, metadata, evidence = junit_project
    before = dict(spec)
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 1
    check = checks[0]
    assert check['junit_required'] is True
    assert check['plan_provenance']['recipe'] == 'checks.scripts'
    assert check['plan_provenance']['metadata']['scripts/checks.toml']
    folder = evidence / check['id']
    (folder / 'pytest-artifacts').mkdir(parents=True)
    subprocess.run(check['argv'], cwd=checkout, check=True, capture_output=True)
    reference = retain_artifacts(folder, {'id': 'candidate'})
    shutil.rmtree(folder / 'pytest-artifacts')
    assert junit_counts(reference, 'candidate', evidence) == {
        'tests': 1, 'passed': 1, 'failures': 0, 'errors': 0, 'skipped': 0}
    assert spec == before  # Neither frozen plan nor configured commands changed.


def test_named_node_junit_recipe_is_not_redirected_to_vitest(junit_project):
    spec, checkout, _, evidence = junit_project
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run the configured scripts recipe from scripts/checks.toml with owned JUnit '
        'output for scripts/report.test.mjs.']})
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 1
    assert checks[0]['plan_provenance']['recipe'] == 'checks.scripts'
    spec['verification_test_paths'] = ['scripts/report.test.mjs']
    with pytest.raises(ValueError, match='tracked package owner'):
        planned_checks(spec, checkout, evidence)


def test_node_recipe_option_value_does_not_prove_named_test_execution(junit_project):
    import subprocess

    spec, checkout, metadata, evidence = junit_project
    script = checkout / 'scripts/other.mjs'
    script.write_text('import test from "node:test"; '
                      'test("scripts/report.test.mjs", () => {});\n')
    metadata.write_text(metadata.read_text().replace(
        '"--test-reporter=junit",',
        '"--test-name-pattern","scripts/report.test.mjs","--test-reporter=junit",'
    ).replace('"scripts/report.test.mjs"]', '"scripts/other.mjs"]'))
    freeze_recipes(spec, checkout)
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run the configured scripts recipe from scripts/checks.toml with owned JUnit output.']})
    recipe = planned_checks(spec, checkout, evidence)[0]
    (evidence / recipe['id'] / 'pytest-artifacts').mkdir(parents=True)
    subprocess.run(recipe['argv'], cwd=checkout, check=True, capture_output=True)
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run the configured scripts recipe from scripts/checks.toml with owned JUnit '
        'output for scripts/report.test.mjs.']})
    with pytest.raises(ValueError, match='tracked package owner'):
        planned_checks(spec, checkout, evidence)


@pytest.mark.parametrize('bad', ['untracked', 'symlink', 'no-report', 'two-reports',
                                 'timeout', 'kind', 'cwd', 'unnamed'])
def test_recipe_rejects_unsealed_or_unbounded_execution(junit_project, bad):
    spec, checkout, metadata, evidence = junit_project
    if bad == 'untracked':
        _git(checkout, 'rm', '--cached', 'scripts/checks.toml')
    elif bad == 'symlink':
        metadata.unlink()
        metadata.symlink_to(Path(__file__).resolve())
    elif bad == 'unnamed':
        spec['accepted_plan'] = json.dumps({'verification': ['Run scripts/checks.toml JUnit']})
    else:
        replacements = {'no-report': ('{report_path}', 'report.xml'),
                        'two-reports': ('{report_path}', '{report_path}{report_path}'),
                        'timeout': ('600', '0'), 'kind': ('kind="junit"', 'kind="static"'),
                        'cwd': ('timeout_seconds=600', 'cwd="../escape"')}
        old, new = replacements[bad]
        metadata.write_text(metadata.read_text().replace(old, new))
    freeze_recipes(spec, checkout, stage=bad != 'untracked')
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)


def test_named_static_recipes_preserve_exact_range_and_frozen_checks(junit_project):
    spec, checkout, metadata, evidence = junit_project
    metadata.write_text(metadata.read_text() + '\n[checks.diff]\nkind="static"\n'
                        'argv=["git","diff","--check","origin/main...HEAD"]\n'
                        '\n[checks.docs]\nkind="static"\nargv=["corepack","pnpm","docs:build"]\n')
    freeze_recipes(spec, checkout)
    spec['accepted_plan'] = json.dumps({'verification': [
        'Execute tracked scripts/checks.toml checks.diff and checks.docs.']})
    spec['policy'] = {'checks': [{'id': 'diff', 'argv': ['git', 'diff', '--check']}]}
    before = json.dumps(spec, sort_keys=True)
    checks = planned_checks(spec, checkout, evidence)
    assert [c['plan_provenance']['recipe'] for c in checks] == ['checks.diff', 'checks.docs']
    assert checks[0]['argv'] == ['git', 'diff', '--check', 'origin/main...HEAD']
    assert all(c['kind'] == 'static' and not c.get('junit_required') for c in checks)
    assert json.dumps(spec, sort_keys=True) == before


@pytest.mark.parametrize('bad', ['untracked', 'symlink', 'cwd', 'timeout', 'report'])
def test_static_recipes_reject_unowned_or_unbounded_metadata(junit_project, bad):
    spec, checkout, metadata, evidence = junit_project
    metadata.write_text('schema_version=1\n[checks.diff]\nkind="static"\n'
                        'argv=["git","diff","--check","origin/main...HEAD"]\n'
                        'timeout_seconds=30\n')
    _git(checkout, 'add', '.')
    spec['accepted_plan'] = json.dumps({'verification': [
        'Execute tracked scripts/checks.toml checks.diff.']})
    if bad == 'untracked':
        _git(checkout, 'rm', '--cached', 'scripts/checks.toml')
    elif bad == 'symlink':
        metadata.unlink()
        metadata.symlink_to(Path(__file__).resolve())
    else:
        old, new = {'cwd': ('timeout_seconds=30', 'cwd="../foreign"'),
                    'timeout': ('timeout_seconds=30', 'timeout_seconds=2701'),
                    'report': ('origin/main...HEAD', '{report_path}')}[bad]
        metadata.write_text(metadata.read_text().replace(old, new))
    freeze_recipes(spec, checkout, stage=bad != 'untracked')
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)


def test_metadata_defined_junit_shell_wrapper_remains_exact_with_static_recipes(junit_project):
    spec, checkout, metadata, evidence = junit_project
    metadata.write_text('schema_version=1\n[checks.scripts]\nkind="junit"\n'
                        'argv=["sh","-c","node --test --test-reporter=junit > {report_path}"]\n'
                        '[checks.diff]\nkind="static"\n'
                        'argv=["git","diff","--check","origin/main...HEAD"]\n')
    freeze_recipes(spec, checkout)
    spec['accepted_plan'] = json.dumps({'verification': [
        'Execute scripts/checks.toml checks.scripts with JUnit and checks.diff.']})
    checks = planned_checks(spec, checkout, evidence)
    assert checks[0]['argv'][:2] == ['sh', '-c']
    assert checks[0]['junit_required'] is True
    assert checks[1]['argv'] == ['git', 'diff', '--check', 'origin/main...HEAD']


def test_static_recipe_and_named_vitest_can_share_junit_verification_step(project):
    spec, checkout, _, evidence = project
    package = checkout / 'api'
    (package / 'test').mkdir(parents=True)
    (package / 'test/audit.test.ts').write_text('test("audit", () => {});')
    (package / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
    (checkout / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (checkout / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    (checkout / 'scripts').mkdir()
    (checkout / 'scripts/checks.toml').write_text(
        'schema_version=1\n[checks.diff]\nkind="static"\n'
        'argv=["git","diff","--check","origin/main...HEAD"]\n')
    freeze_recipes(spec, checkout)
    spec['accepted_plan'] = json.dumps({'verification': [
        'Run scripts/checks.toml checks.diff and API audit.test.ts with retained JUnit output.']})
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 2
    assert checks[0]['argv'] == ['git', 'diff', '--check', 'origin/main...HEAD']
    assert checks[1]['id'].startswith('planned-vitest-')
    assert '--reporter=junit' in checks[1]['argv']
    assert any(arg.endswith('/pytest-artifacts/junit.xml') for arg in checks[1]['argv'])


def test_cleanup_uses_recorded_custody_after_partial_test_changes(project):
    from devflow_temporal.delivery_resources import RunResources

    spec, checkout, root, evidence = project
    state = evidence.parent / 'run-fixture'
    state.mkdir(mode=0o700)
    relative = 'worker/tests/test_future.py'
    spec.update(run_id='run-fixture', state_dir=str(state), checkout=str(checkout),
                accepted_plan=json.dumps({'verification': ['Create and run test_future.py']}),
                policy={'allowed_paths': [relative], 'host_sandbox': 'trusted-local'})
    resources = RunResources(spec)
    original = checkout.with_name('fixture-source')
    checkout.rename(original)
    resources.register(checkout, 'checkout')
    original.rename(checkout)
    resources.created(checkout)
    venv = root / '.venv'
    resources.register(venv, 'generated')
    venv.mkdir()
    resources.created(venv)
    (checkout / relative).write_text('def test_future(): assert True\n')
    resources._allowed(venv, 'generated', finalizing=True)
    with resources.locked() as manifest:
        entry = manifest['roots'][str(venv)]
        original_digest = entry['accepted_plan_sha256']
        entry['accepted_plan_sha256'] = '0' * 64
        from devflow_temporal.delivery_resources import write_private
        write_private(resources.manifest, manifest)
    with pytest.raises(ValueError, match='recorded plan changed'):
        resources._allowed(venv, 'generated', finalizing=True)
    with resources.locked() as manifest:
        manifest['roots'][str(venv)]['accepted_plan_sha256'] = original_digest
        write_private(resources.manifest, manifest)
    original_plan = spec['accepted_plan']
    spec['accepted_plan'] = json.dumps({'verification': ['Run test_owned.py']})
    with pytest.raises(ValueError, match='recorded plan changed'):
        resources._allowed(venv, 'generated', finalizing=True)
    spec['accepted_plan'] = original_plan
    # Even broken candidate metadata must not erase preexisting cleanup custody.
    (root / 'uv.lock').unlink()
    resources._allowed(venv, 'generated', finalizing=True)
    with pytest.raises(ValueError):
        resources.register(venv, 'generated')
    spec['accepted_plan'] = json.dumps({'verification': ['Run test_other.py']})
    with pytest.raises(ValueError):
        resources._allowed(venv, 'generated', finalizing=True)
    spec['accepted_plan'] = json.dumps({'verification': ['Create and run test_future.py']})
    checkout.rename(original)
    checkout.mkdir()
    with pytest.raises(ValueError, match='parent was replaced'):
        resources._allowed(venv, 'generated', finalizing=True)


def test_verification_prepares_python_before_existing_consumers(project, monkeypatch):
    from devflow_temporal.delivery_broker import DeliveryBroker

    spec, checkout, _, evidence = project
    spec.update(provider='codex', policy={'host_sandbox': 'trusted-local',
                                        'checks': [{'id': 'api-rpc-test'}]})
    broker = object.__new__(DeliveryBroker)
    broker.spec, broker.evidence_dir = spec, evidence
    candidate = {'id': 'a' * 64}
    monkeypatch.setattr(broker, 'gate_checkout', lambda *_: checkout)
    observed = []

    def inspect_order(_checkout, checks, *_args, **_kwargs):
        observed.extend(checks)
        return {'state': 'passed'}

    monkeypatch.setattr(broker, '_run_check_list', inspect_order)
    assert broker.run_checks(0, candidate)['state'] == 'passed'
    assert observed[0]['id'].startswith('planned-python-dependencies-')
    assert observed[1]['id'] == 'api-rpc-test'
    assert observed[2]['id'].startswith('planned-pytest-')
