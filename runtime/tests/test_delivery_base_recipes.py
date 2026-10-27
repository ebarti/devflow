from __future__ import annotations

import hashlib
import json
import subprocess
import sys

import pytest
from test_delivery_store import _git

from devflow_temporal.delivery_plan_checks import planned_checks


def metadata(kind, code):
    argv = [sys.executable, '-c', code]
    if kind == 'junit':
        argv.append('{report_path}')
    return ('schema_version=1\n[checks.owned]\nkind=' + json.dumps(kind)
            + '\nargv=' + json.dumps(argv) + '\n')


@pytest.fixture
def repository(tmp_path):
    source = tmp_path / 'source'
    (source / 'scripts').mkdir(parents=True)
    _git(source, 'init', '-q')
    _git(source, 'config', 'user.name', 'Fixture')
    _git(source, 'config', 'user.email', 'fixture@example.invalid')
    return source, tmp_path / 'candidate', tmp_path / 'evidence'


def admit(source, checkout, kind):
    base_bytes = metadata(kind, 'raise SystemExit(7)').encode()
    (source / 'scripts/checks.toml').write_bytes(base_bytes)
    _git(source, 'add', '.')
    _git(source, 'commit', '-qm', 'Base recipe')
    _git(source.parent, 'clone', '-q', str(source), str(checkout))
    _git(checkout, 'config', 'user.name', 'Fixture')
    _git(checkout, 'config', 'user.email', 'fixture@example.invalid')
    spec = {'source_path': str(source), 'base_sha': _git(source, 'rev-parse', 'HEAD'),
            'accepted_plan': json.dumps({'verification': [
                'Run scripts/checks.toml checks.owned with owned JUnit output.']})}
    return spec, base_bytes


@pytest.mark.parametrize('kind', ['junit', 'static'])
@pytest.mark.parametrize('committed', [False, True])
def test_candidate_cannot_replace_the_recipe_that_judges_it(repository, kind, committed):
    source, checkout, evidence = repository
    spec, base_bytes = admit(source, checkout, kind)
    (checkout / 'scripts/checks.toml').write_text(metadata(kind, 'raise SystemExit(0)'))
    if committed:
        _git(checkout, 'commit', '-qam', 'Candidate replaces its judge')
    check = planned_checks(spec, checkout, evidence)[0]
    result = subprocess.run(check['argv'], cwd=checkout, capture_output=True)
    assert result.returncode == 7
    assert check['plan_provenance']['metadata']['scripts/checks.toml'] == hashlib.sha256(
        base_bytes).hexdigest()


def test_recipe_changes_take_effect_only_for_a_new_admitted_base(repository):
    source, checkout, evidence = repository
    spec, _ = admit(source, checkout, 'static')
    (source / 'scripts/checks.toml').write_text(metadata('static', 'raise SystemExit(0)'))
    _git(source, 'commit', '-qam', 'Merged recipe update')
    assert planned_checks(spec, checkout, evidence)[0]['argv'][2] == 'raise SystemExit(7)'
    next_spec = {**spec, 'base_sha': _git(source, 'rev-parse', 'HEAD')}
    assert planned_checks(next_spec, checkout, evidence)[0]['argv'][2] == 'raise SystemExit(0)'
