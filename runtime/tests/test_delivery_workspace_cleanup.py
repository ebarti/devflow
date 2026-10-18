"""Cleanup uses recorded dependency custody after permitted source edits."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from test_delivery_configured_resources import configured
from test_delivery_store import _git


@pytest.fixture
def owned_workspace(tmp_path):
    owned, resources, _broker, root, _ = configured(tmp_path, 'checkout')
    package = root / 'apps/web/package.json'
    package.parent.mkdir(parents=True)
    package.write_text(json.dumps({'name': 'owned-web', 'scripts': {'test': 'node test.js'}}))
    (root / 'package.json').write_text(json.dumps({'workspaces': ['apps/*']}))
    (root / '.gitignore').write_text('.venv/\nnode_modules/\n')
    _git(root, 'add', '.')
    _git(root, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
         'commit', '-qm', 'tracked workspace')
    owned['base_sha'] = _git(root, 'rev-parse', 'HEAD')
    owned['branch'] = _git(root, 'branch', '--show-current')
    modules = package.parent / 'node_modules'
    resources.generated_directory(modules)
    (modules / 'owned').write_text('generated dependency')
    package.write_text(json.dumps({'name': 'owned-web', 'scripts': {
        'test': 'node test.js', 'check-pdf-tools': 'node pdf-tools.js'}}))
    return resources, root, package, modules


def test_cleanup_keeps_source_edit_and_removes_recorded_dependencies(owned_workspace):
    resources, root, package, modules = owned_workspace
    before = package.read_bytes()
    result = resources.finalize('blocked')
    node = next(item for item in result['roots'] if item['path'] == str(modules))
    assert node['state'] == 'removed', node
    assert not modules.exists()
    assert package.read_bytes() == before and root.is_dir()


def test_changed_metadata_still_cannot_register_dependencies(owned_workspace):
    resources, _root, _package, modules = owned_workspace
    with pytest.raises(ValueError, match='tracked declaration'):
        resources.register(modules, 'generated')


def test_authorized_scripts_can_reprepare_the_same_dependency_layout(owned_workspace):
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_configured_resources import workspace_dependency_directories

    resources, root, _package, modules = owned_workspace
    resources.spec['policy'].setdefault('allowed_paths', []).append('apps/web/package.json')
    resources.spec['policy_digest'] = digest(resources.spec['policy'])
    assert 'apps/web/node_modules' in workspace_dependency_directories(
        root, policy=resources.spec['policy'])
    resources.register(modules, 'generated')
    assert resources.finalize('blocked')['state'] == 'confirmed'
    resources.generated_directory(modules)
    assert modules.is_dir()


@pytest.mark.parametrize('field', ['dependencies', 'workspaces', 'name'])
def test_authorized_scripts_do_not_admit_other_metadata_changes(owned_workspace, field):
    from devflow_temporal.delivery_configured_resources import workspace_dependency_directories

    resources, root, package, _modules = owned_workspace
    resources.spec['policy'].setdefault('allowed_paths', []).append('apps/web/package.json')
    value = json.loads(package.read_text())
    value[field] = {'unreviewed': 'change'}
    package.write_text(json.dumps(value))
    with pytest.raises(ValueError, match='tracked declaration'):
        workspace_dependency_directories(root, policy=resources.spec['policy'])


@pytest.mark.parametrize('replacement', ['directory', 'symlink'])
def test_cleanup_does_not_adopt_replacement_dependencies(owned_workspace, tmp_path, replacement):
    resources, _root, _package, modules = owned_workspace
    retained = tmp_path / 'original-dependencies'
    modules.rename(retained)
    precious = tmp_path / 'foreign-dependencies'
    precious.mkdir()
    (precious / 'keep').write_text('preserve foreign data')
    if replacement == 'symlink':
        modules.symlink_to(precious, target_is_directory=True)
    else:
        modules.mkdir()
        (modules / 'keep').write_text('preserve replacement')
    result = resources.finalize('blocked')
    assert result['state'] == 'unknown'
    assert (precious / 'keep').read_text() == 'preserve foreign data'
    assert (retained / 'owned').read_text() == 'generated dependency'
    if replacement == 'directory':
        assert (modules / 'keep').read_text() == 'preserve replacement'
