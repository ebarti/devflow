"""Owned primary-plugin switch/replay/rollback against the inspected public CLI contract."""
from __future__ import annotations

import importlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from test_delivery_installation import installation as installation

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def primary(installation, monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / 'desktop'))
    module = importlib.import_module('owned_plugin')
    packager = importlib.import_module('package_plugin')
    fixture = installation
    home = fixture['home']
    runtime = fixture['trusted'].parent.parent / 'runtime'
    config = fixture['trusted']
    direct = json.loads(json.dumps(fixture['entry']))
    direct['transport']['args'] = ['--config', str(config)]
    fixture['skill'].write_bytes((ROOT / 'desktop/devflow-local-delivery/SKILL.md').read_bytes())
    root = config.parent / 'fresh-marketplace'
    packager.package(root, runtime, config)
    state = {'mcp': {module.NAME: direct, 'unrelated': fixture['other']},
             'plugins': [], 'marketplaces': {}, 'effects': [], 'fail': None}

    def command(_codex, _home, *args):
        if args == ('mcp', 'list', '--json'):
            return list(state['mcp'].values())
        if args[:2] == ('mcp', 'get'):
            return state['mcp'][args[2]]
        if args == ('plugin', 'list', '--json'):
            return {'installed': state['plugins'], 'available': []}
        if args == ('plugin', 'marketplace', 'list'):
            if not state['marketplaces']:
                return 'No plugin marketplaces in scope.\n'
            return 'MARKETPLACE ROOT\n' + ''.join(
                f'{key} {value}\n' for key, value in state['marketplaces'].items())
        state['effects'].append(args)
        if args[:3] == ('plugin', 'marketplace', 'add'):
            state['marketplaces'][module.MARKET] = args[3]
        elif args[:2] == ('plugin', 'add'):
            cache = home / 'plugins/cache/devflow-local/devflow/0.1.0'
            cache.parent.mkdir(parents=True)
            shutil.copytree(root / 'plugins/devflow', cache)
            state['plugins'] = [{'pluginId': module.PLUGIN, 'name': 'devflow',
                'version': '0.1.0', 'installed': True, 'enabled': True,
                'marketplaceName': module.MARKET, 'source': {'source': 'local',
                'path': str(root / 'plugins/devflow')},
                'marketplaceSource': {'sourceType': 'local', 'source': str(root)}}]
        elif args[:2] == ('mcp', 'remove'):
            del state['mcp'][module.NAME]
        elif args[:2] == ('plugin', 'remove'):
            state['plugins'] = []
            shutil.rmtree(home / 'plugins/cache/devflow-local/devflow')
        elif args[:3] == ('plugin', 'marketplace', 'remove'):
            del state['marketplaces'][module.MARKET]
        else:
            raise AssertionError(args)
        if state['fail'] == args[0:2]:
            state['fail'] = None
            raise subprocess.TimeoutExpired(args, 30)
        return {}

    def restore(_codex, _home, executable, selected_config):
        assert executable == direct['transport']['command'] and selected_config == config
        state['mcp'][module.NAME] = direct

    monkeypatch.setattr(module, 'command', command)
    monkeypatch.setattr(module, 'install_pointer', restore)
    observed = module.snapshot('codex', home)
    request = config.parent / 'primary-request.json'
    module.save(request, {'command_id': 'primary-switch', 'marketplace_root': str(root),
        'expected_registration_sha256': module.seal(direct),
        'expected_config_sha256': module.sha(config.read_bytes()),
        'expected_skill_sha256': module.sha(fixture['skill'].read_bytes()),
        'expected_plugin_inventory_sha256': observed['plugins_sha256'],
        'expected_marketplace_inventory_sha256': observed['marketplaces_sha256']})
    return module, fixture, runtime, config, request, state


def test_primary_plugin_replay_and_guarded_rollback_preserve_unrelated_host(primary):
    module, fixture, runtime, config, request, state = primary
    settings = (fixture['home'] / 'config.toml').read_bytes()
    old_skill = fixture['skill'].read_bytes()
    first = module.activate('codex', fixture['home'], runtime, config, request)
    assert first['state'] == 'applied' and module.NAME not in state['mcp']
    assert state['plugins'][0]['enabled'] and not fixture['skill'].exists()
    effects = list(state['effects'])
    repeated = module.activate('codex', fixture['home'], runtime, config, request)
    assert repeated['existing'] and state['effects'] == effects
    assert state['mcp']['unrelated'] == fixture['other']
    assert (fixture['home'] / 'config.toml').read_bytes() == settings
    assert fixture['agents'].read_text() == 'preserved agent bytes\n'
    receipt = Path(first['rollback_manifest'])
    assert receipt.stat().st_mode & 0o777 == 0o600
    assert module.rollback('codex', fixture['home'], receipt)['state'] == 'rolled_back'
    assert fixture['skill'].read_bytes() == old_skill
    assert not state['plugins'] and not state['marketplaces']
    assert module.NAME in state['mcp']
    replay = module.activate('codex', fixture['home'], runtime, config, request)
    assert replay['state'] == 'rolled_back'


@pytest.mark.parametrize('effect', [('plugin', 'add'), ('mcp', 'remove')])
def test_uncertain_public_primary_switch_observes_before_resuming(primary, effect):
    module, fixture, runtime, config, request, state = primary
    state['fail'] = effect
    with pytest.raises(subprocess.TimeoutExpired):
        module.activate('codex', fixture['home'], runtime, config, request)
    result = module.activate('codex', fixture['home'], runtime, config, request)
    assert result['state'] == 'applied'
    assert sum(e[:2] == effect for e in state['effects']) == 1


@pytest.mark.parametrize('change', ['inventory', 'skill', 'config', 'registration'])
def test_primary_switch_refuses_changed_inspected_inputs(primary, change):
    module, fixture, runtime, config, request, state = primary
    if change == 'inventory':
        state['plugins'].append({'pluginId': 'other@foreign'})
    elif change == 'skill':
        fixture['skill'].write_text('User owned edit')
    elif change == 'config':
        module.save(config, {**json.loads(config.read_text()), 'capacity': 99})
    else:
        state['mcp'][module.NAME]['transport']['args'] = ['--config', '/foreign.json']
    with pytest.raises(ValueError):
        module.activate('codex', fixture['home'], runtime, config, request)
    assert state['effects'] == []


def test_applied_switch_conflict_never_overwrites_changed_cache(primary):
    module, fixture, runtime, config, request, state = primary
    first = module.activate('codex', fixture['home'], runtime, config, request)
    cache = fixture['home'] / 'plugins/cache/devflow-local/devflow/0.1.0/mcp.json'
    cache.write_text('User changed installed plugin')
    effects = list(state['effects'])
    with pytest.raises(ValueError, match='differs|changed'):
        module.activate('codex', fixture['home'], runtime, config, request)
    with pytest.raises(ValueError, match='differs|changed'):
        module.rollback('codex', fixture['home'], Path(first['rollback_manifest']))
    assert cache.read_text() == 'User changed installed plugin' and state['effects'] == effects


def test_primary_unknown_uses_same_narrow_stdio_default_compatibility(primary, monkeypatch):
    module, fixture, runtime, config, request, state = primary
    host_config = fixture['home'] / 'config.toml'
    host_config.write_text(host_config.read_text() +
        '[mcp_servers.node_repl]\ncommand="/usr/bin/true"\n'
        'args=[]\nstartup_timeout_sec=10\n')
    original = module.command

    def canonicalizing_command(*args):
        result = original(*args)
        if args[2:4] == ('plugin', 'add'):
            host_config.write_text(host_config.read_text().replace('args=[]\n', '').replace(
                'startup_timeout_sec=10\n', 'startup_timeout_sec=10.0\n'))
        return result

    monkeypatch.setattr(module, 'command', canonicalizing_command)
    semantic = module.unrelated_seal
    monkeypatch.setattr(module, 'unrelated_seal', lambda value, expected=None: module.seal(value))
    with pytest.raises(ValueError, match='host settings changed'):
        module.activate('codex', fixture['home'], runtime, config, request)
    manifest = fixture['home'] / '.devflow-local-delivery-upgrades/primary-switch/manifest.json'
    unknown = json.loads(manifest.read_text())
    assert unknown['state'] == 'unknown'
    monkeypatch.setattr(module, 'unrelated_seal', semantic)
    accepted = module.activate('codex', fixture['home'], runtime, config, request)
    assert accepted['state'] == 'applied'
    assert sum(effect[:2] == ('plugin', 'add') for effect in state['effects']) == 1
    assert json.loads(manifest.read_text())['last_error'] == unknown['last_error']
    assert module.rollback('codex', fixture['home'], manifest)['state'] == 'rolled_back'


def _primary_enabled_serialization(primary, monkeypatch):
    module, fixture, runtime, config, request, state = primary
    host = fixture['home'] / 'config.toml'
    host.write_text(host.read_text() + '[mcp_servers.unrelated]\n'
                    'command="/bin/true"\nenabled=true\n')
    original = module.command

    def canonicalizing_command(*args):
        result = original(*args)
        if args[2:4] == ('mcp', 'remove'):
            host.write_text(host.read_text().replace('enabled=true\n', ''))
        return result

    monkeypatch.setattr(module, 'command', canonicalizing_command)
    return host


def test_primary_enabled_default_serialization_activation_replay_and_rollback(primary, monkeypatch):
    module, fixture, runtime, config, request, state = primary
    host = _primary_enabled_serialization(primary, monkeypatch)
    before = module.snapshot('codex', fixture['home'])
    first = module.activate('codex', fixture['home'], runtime, config, request)
    manifest = Path(first['rollback_manifest'])
    assert first['state'] == 'applied' and 'enabled=true' not in host.read_text()
    assert module.activate('codex', fixture['home'], runtime, config, request)['existing']
    # The inverse representation also has the same effective public inventory.
    host.write_text(host.read_text().replace('[mcp_servers.unrelated]\n',
                                             '[mcp_servers.unrelated]\nenabled=true\n'))
    assert module.rollback('codex', fixture['home'], manifest)['state'] == 'rolled_back'
    assert module.snapshot('codex', fixture['home']) == before
    assert state['mcp']['unrelated']['enabled'] is True
    assert fixture['skill'].exists()


@pytest.mark.parametrize('change', [
    'false', 'string', 'integer', 'public-disabled', 'entry-mismatch',
])
def test_primary_enabled_default_rejects_effective_type_and_confirmation_drift(
    primary, monkeypatch, change,
):
    module, fixture, runtime, config, request, state = primary
    host = _primary_enabled_serialization(primary, monkeypatch)
    first = module.activate('codex', fixture['home'], runtime, config, request)
    manifest = Path(first['rollback_manifest'])
    if change == 'public-disabled':
        state['mcp']['unrelated']['enabled'] = False
    elif change == 'entry-mismatch':
        state['mcp']['unrelated']['name'] = 'different-server'
    else:
        literal = {'false': 'false', 'string': '"true"', 'integer': '1'}[change]
        replacement = '[mcp_servers.unrelated]\nenabled=' + literal + '\n'
        host.write_text(host.read_text().replace('[mcp_servers.unrelated]\n', replacement))
    frozen = (manifest.read_bytes(), list(state['effects']), host.read_bytes())
    with pytest.raises(ValueError):
        module.activate('codex', fixture['home'], runtime, config, request)
    with pytest.raises(ValueError):
        module.rollback('codex', fixture['home'], manifest)
    assert frozen == (manifest.read_bytes(), state['effects'], host.read_bytes())
