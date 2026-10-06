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


def test_node_selection_uses_locked_vitest_and_retained_junit(project):
    spec, checkout, _, evidence = project
    package = checkout / 'api'
    (package / 'test').mkdir(parents=True)
    (package / 'test/audit.test.ts').write_text('test("audit", () => {});')
    (package / 'package.json').write_text(json.dumps({'devDependencies': {'vitest': '4.1.11'}}))
    (checkout / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (checkout / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    _git(checkout, 'add', '.')
    spec.update(accepted_plan=json.dumps({'verification': ['Run focused API tests']}),
                verification_test_paths=['api/test/audit.test.ts'])
    checks = planned_checks(spec, checkout, evidence)
    assert len(checks) == 1
    test = checks[0]
    assert test['argv'][:6] == ['corepack', 'pnpm', 'exec', 'vitest', 'run', 'test/audit.test.ts']
    assert '--reporter=junit' in test['argv']
    assert test['cwd'] == 'api'
    assert test['min_tests'] == 1
    assert 'pnpm-lock.yaml' in test['plan_provenance']['metadata']
    (checkout / 'pnpm-lock.yaml').unlink()
    with pytest.raises(FileNotFoundError):
        planned_checks(spec, checkout, evidence)


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
    with pytest.raises(ValueError):
        planned_checks(spec, checkout, evidence)
