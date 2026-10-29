"""Cleanup uses recorded dependency custody after permitted source edits."""
from __future__ import annotations

import json

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


def test_removed_dependency_generation_can_bind_newly_published_metadata(owned_workspace):
    resources, root, package, modules = owned_workspace
    assert resources.finalize('blocked')['state'] == 'confirmed'
    _git(root, 'add', package.relative_to(root).as_posix())
    _git(root, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
         'commit', '-qm', 'Publish permitted package script')
    with resources.locked() as manifest:
        old = manifest['roots'][str(modules)]['workspace_declaration_sha256']
    resources.generated_directory(modules)
    with resources.locked() as manifest:
        entry = manifest['roots'][str(modules)]
        assert entry['workspace_declaration_sha256'] != old
        assert entry['generation'] == 1
    assert modules.is_dir()
    assert resources.finalize('blocked')['state'] == 'confirmed'


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
