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
