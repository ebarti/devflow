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


@pytest.fixture(params=['portable', 'codex'])
def primary(installation, monkeypatch, request):
    monkeypatch.syspath_prepend(str(ROOT / 'desktop'))
    module = importlib.import_module('owned_plugin')
    packager = importlib.import_module('package_plugin')
    fixture = installation
    home = fixture['home']
    runtime = fixture['trusted'].parent.parent / 'runtime'
    config = fixture['trusted']
    direct = json.loads(json.dumps(fixture['entry']))
    direct['transport']['args'] = ['--config', str(config)]
    direct_toml = (f'\n[mcp_servers.{module.NAME}]\n'
                   f'command={json.dumps(direct["transport"]["command"])}\n'
                   f'args={json.dumps(direct["transport"]["args"])}\n')
    host_config = home / 'config.toml'
    host_config.write_text(host_config.read_text() + direct_toml)
    fixture['skill'].write_bytes((ROOT / 'desktop/devflow-local-delivery/SKILL.md').read_bytes())
    root = config.parent / 'fresh-marketplace'
    package_format = request.param
    packager.package(root, runtime, config, package_format=package_format)
    state = {'mcp': {module.NAME: direct, 'unrelated': fixture['other']},
             'plugins': [], 'marketplaces': {}, 'effects': [], 'fail': None,
             'package_format': package_format, 'direct_toml': direct_toml,
             'plugin_mcp': direct, 'expose_plugin_mcp': package_format == 'codex'}

    def command(_codex, _home, *args):
        if args == ('mcp', 'list', '--json'):
            values = list(state['mcp'].values())
            if (state['plugins'] and state['expose_plugin_mcp']
                    and module.NAME not in state['mcp'] and state['plugin_mcp'] is not None):
                values.append(state['plugin_mcp'])
            return values
        if args[:2] == ('mcp', 'get'):
            if args[2] == module.NAME and module.NAME not in state['mcp']:
                return state['plugin_mcp']
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
            host_config.write_text(host_config.read_text().replace(direct_toml, ''))
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
        host_config.write_text(host_config.read_text() + direct_toml)

    monkeypatch.setattr(module, 'command', command)
    monkeypatch.setattr(module, 'install_pointer', restore)
    observed = module.snapshot('codex', home)
    request = config.parent / 'primary-request.json'
    module.save(request, {'command_id': 'primary-switch', 'marketplace_root': str(root),
        'expected_registration_sha256': module.seal(direct),
        'expected_config_sha256': module.sha(config.read_bytes()),
        'expected_skill_sha256': module.sha(fixture['skill'].read_bytes()),
        'expected_plugin_inventory_sha256': observed['plugins_sha256'],
        'expected_marketplace_inventory_sha256': observed['marketplaces_sha256'],
        **({'package_format': package_format} if package_format != 'portable' else {})})
    return module, fixture, runtime, config, request, state


@pytest.mark.parametrize('effect', [('plugin', 'add'), ('mcp', 'remove')])
def test_uncertain_public_primary_switch_observes_before_resuming(primary, effect):
    module, fixture, runtime, config, request, state = primary
    state['fail'] = effect
    with pytest.raises(subprocess.TimeoutExpired):
        module.activate('codex', fixture['home'], runtime, config, request)
    result = module.activate('codex', fixture['home'], runtime, config, request)
    assert result['state'] == 'applied'
    assert sum(e[:2] == effect for e in state['effects']) == 1


def test_primary_receipt_layout_is_bound_on_replay_and_rollback(primary):
    module, fixture, runtime, config, request, state = primary
    first = module.activate('codex', fixture['home'], runtime, config, request)
    receipt = Path(first['rollback_manifest'])
    value = json.loads(receipt.read_text())
    value['package_format'] = 'codex' if state['package_format'] == 'portable' else 'portable'
    module.save(receipt, value)
    effects = list(state['effects'])
    with pytest.raises(ValueError, match='different inputs'):
        module.activate('codex', fixture['home'], runtime, config, request)
    with pytest.raises(ValueError, match='differs'):
        module.rollback('codex', fixture['home'], receipt)
    assert effects == state['effects']
