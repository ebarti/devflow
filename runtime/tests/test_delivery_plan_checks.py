from __future__ import annotations

import json

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


def test_explicit_tracked_selection_executes_prose_plan_without_changing_it(project):
    spec, checkout, _, evidence = project
    spec['accepted_plan'] = json.dumps({'verification': ['Execute focused worker regressions']})
    spec['verification_test_paths'] = ['worker/tests/test_owned.py']
    before = dict(spec)
    checks = planned_checks(spec, checkout, evidence)
    assert checks[1]['argv'][3] == 'tests/test_owned.py'
    assert checks[1]['plan_provenance']['selection_sha256']
    assert spec == before


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
