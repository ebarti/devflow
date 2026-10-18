"""Owner amendments for directory scopes grant only explicitly named files."""
from __future__ import annotations

import copy
import hashlib
import json
from types import SimpleNamespace

import pytest

from devflow_temporal.delivery_config import DeliveryConfig, scope_amendment_config
from devflow_temporal.delivery_source_scope import outside_scope

WORKFLOW = '.github/workflows/typescript.yml'


@pytest.fixture
def amendment(tmp_path, monkeypatch):
    scope = {'version': 1, 'allowed_roots': ['apps', 'docs'],
             'allowed_files': ['scripts/checks.toml'],
             'protected_paths': ['.github', '.git', '.codex', '.agents']}
    old = {'repositories': {'fixture': {'allowed_paths': [], 'source_scope': scope}},
           'max_repairs': 10}
    new = copy.deepcopy(old)
    new['repositories']['fixture']['source_scope']['allowed_files'].append(WORKFLOW)
    new['repositories']['fixture']['source_scope']['protected_paths'].remove('.github')
    state = tmp_path / 'state/runs/run-owned'
    state.mkdir(parents=True)
    original = {'state_dir': str(state), 'repository_key': 'fixture',
                'policy': {'allowed_paths': [], 'source_scope': scope}}
    path = tmp_path / 'state/amended.json'

    def save():
        path.write_text(json.dumps(new))
        path.chmod(0o600)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    monkeypatch.setattr('devflow_temporal.delivery_execution_config.load_execution_config',
                        lambda _: SimpleNamespace(raw=copy.deepcopy(old)))
    monkeypatch.setattr(DeliveryConfig, 'load', lambda p: SimpleNamespace(raw=json.loads(p.read_text())))
    return SimpleNamespace(old=old, new=new, original=original, path=path, save=save)


def test_versioned_amendment_grants_only_the_named_ci_file(amendment):
    a = amendment
    before = copy.deepcopy(a.original)
    amended = scope_amendment_config(a.original, a.path, a.save(), [WORKFLOW])
    policy = {'allowed_paths': [], 'source_scope': amended.raw['repositories']['fixture']['source_scope']}
    paths = [WORKFLOW, '.github/workflows/release.yml', '.github/new.yml', '.github/other/config',
             '.git/config', '.codex/config.toml', '.agents/AGENTS.md', 'apps/main.ts', 'other.txt']
    old_denied = outside_scope(a.original['policy'], paths)
    new_denied = outside_scope(policy, paths)
    assert old_denied - new_denied == {WORKFLOW}
    assert a.original == before


@pytest.mark.parametrize('drift', ['roots', 'sibling', 'protected', 'budget'])
def test_versioned_amendment_rejects_unrelated_configuration_changes(amendment, drift):
    a = amendment
    scope = a.new['repositories']['fixture']['source_scope']
    if drift == 'roots':
        scope['allowed_roots'].append('.')
    elif drift == 'sibling':
        scope['allowed_files'].append('.github/workflows/release.yml')
    elif drift == 'protected':
        scope['protected_paths'].remove('.git')
    else:
        a.new['max_repairs'] += 1
    with pytest.raises(ValueError):
        scope_amendment_config(a.original, a.path, a.save(), [WORKFLOW])


@pytest.mark.parametrize('overlap', ['root', 'nested_root', 'hidden_file'])
def test_removing_a_protected_prefix_cannot_unlock_existing_authority(amendment, overlap):
    a = amendment
    old = a.old['repositories']['fixture']['source_scope']
    new = a.new['repositories']['fixture']['source_scope']
    if overlap == 'root':
        old['allowed_roots'] = new['allowed_roots'] = ['.']
    elif overlap == 'nested_root':
        old['allowed_roots'].append('.github/tools')
        new['allowed_roots'].append('.github/tools')
    else:
        old['allowed_files'].append('.github/hidden.yml')
        new['allowed_files'].insert(-1, '.github/hidden.yml')
    with pytest.raises(ValueError):
        scope_amendment_config(a.original, a.path, a.save(), [WORKFLOW])
