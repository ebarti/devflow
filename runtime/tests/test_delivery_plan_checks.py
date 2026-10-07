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
    assert deps['argv'][1:5] == ['sync', '--locked', '--no-install-project', '--extra']
    assert deps['generated_directories'] == ['worker/.venv']
    assert test['argv'][:4] == [str(checkout / 'worker/.venv/bin/python'), '-m',
                              'pytest', 'tests/test_owned.py']
    assert '--junitxml=' + str(evidence / test['id'] / 'pytest-artifacts/junit.xml') \
        in test['argv']
    assert test['min_tests'] == 1
    assert test['plan_provenance'] == deps['plan_provenance']
    assert set(test['plan_provenance']['metadata']) == {'pyproject.toml', 'uv.lock'}
    assert _git(checkout, 'diff', '--cached') == before


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
