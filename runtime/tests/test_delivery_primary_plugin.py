"""Owned primary-plugin switch/replay/rollback against the inspected public CLI contract."""
from __future__ import annotations

import asyncio
import importlib
import json
import os
import shutil
import subprocess
from importlib.metadata import distribution
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
    assert (fixture['home'] / 'config.toml').read_bytes() == settings.replace(
        state['direct_toml'].encode(), b'')
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
    cache = fixture['home'] / 'plugins/cache/devflow-local/devflow/0.1.0' / (
        '.mcp.json' if state['package_format'] == 'codex' else 'mcp.json')
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


def test_primary_live_owned_pointer_boolean_integer_change_rejects_before_effects(primary):
    module, fixture, runtime, config, request, state = primary
    first = module.activate('codex', fixture['home'], runtime, config, request)
    manifest = Path(first['rollback_manifest'])
    module.rollback('codex', fixture['home'], manifest)
    state['mcp'][module.NAME]['enabled'] = 1
    effects = list(state['effects'])
    retained = manifest.read_bytes()
    with pytest.raises(ValueError, match='changed'):
        module.activate('codex', fixture['home'], runtime, config, request)
    with pytest.raises(ValueError, match='changed'):
        module.rollback('codex', fixture['home'], manifest)
    assert state['effects'] == effects and manifest.read_bytes() == retained


def test_primary_activation_refuses_an_unselected_or_changed_layout(primary):
    module, fixture, runtime, config, request, state = primary
    value = json.loads(request.read_text())
    value['package_format'] = 'codex' if state['package_format'] == 'portable' else 'portable'
    module.save(request, value)
    with pytest.raises(ValueError, match='differs'):
        module.activate('codex', fixture['home'], runtime, config, request)
    assert state['effects'] == []


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


@pytest.mark.asyncio
async def test_pinned_sdk_automatically_discovers_selected_primary_skill_and_all_tools(
    installation, monkeypatch,
):
    """Public install and fresh automatic discovery, without transport launch or provider turn."""
    monkeypatch.syspath_prepend(str(ROOT / 'desktop'))
    module = importlib.import_module('owned_plugin')
    packager = importlib.import_module('package_plugin')
    codex = str(Path(distribution('openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
    home, config = installation['home'], installation['trusted']
    canonical_skill = (ROOT / 'desktop/devflow-local-delivery/SKILL.md').read_bytes()
    installation['skill'].write_bytes(canonical_skill)
    executable = (ROOT / '.venv/bin/devflow-delivery-mcp').resolve(strict=True)
    module.command(codex, home, 'mcp', 'add', module.NAME, '--', str(executable),
                   '--config', str(config))
    before = module.snapshot(codex, home)
    root = config.parent / 'sdk-marketplace'
    packager.package(root, ROOT, config, package_format='codex')
    request = config.parent / 'sdk-primary-request.json'
    module.save(request, {'command_id': 'sdk-primary', 'marketplace_root': str(root),
        'package_format': 'codex',
        'expected_registration_sha256': module.seal(before['direct']),
        'expected_config_sha256': module.sha(config.read_bytes()),
        'expected_skill_sha256': module.sha(installation['skill'].read_bytes()),
        'expected_plugin_inventory_sha256': before['plugins_sha256'],
        'expected_marketplace_inventory_sha256': before['marketplaces_sha256']})
    first = module.activate(codex, home, ROOT, config, request)
    receipt = Path(first['rollback_manifest'])
    assert json.loads(receipt.read_text())['package_format'] == 'codex'
    assert module.activate(codex, home, ROOT, config, request)['existing']
    stderr = config.parent / 'sdk-app-server.stderr'
    with stderr.open('wb') as errors:
        process = await asyncio.create_subprocess_exec(
            codex, 'app-server', cwd=home, env={**os.environ, 'CODEX_HOME': str(home)},
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=errors,
        )
        try:
            async def rpc(method, params, request_id):
                process.stdin.write((json.dumps({
                    'id': request_id, 'method': method, 'params': params}) + '\n').encode())
                await process.stdin.drain()
                async with asyncio.timeout(30):
                    while True:
                        line = await process.stdout.readline()
                        assert line, stderr.read_text()
                        message = json.loads(line)
                        if message.get('id') == request_id:
                            assert 'error' not in message, message
                            return message['result']

            await rpc('initialize', {
                'clientInfo': {'name': 'devflow-discovery-test', 'version': '1'},
                'capabilities': {'experimentalApi': True}}, 1)
            skills = await rpc('skills/list', {'cwds': [str(home)], 'forceReload': True}, 2)
            found = [s for group in skills['data'] for s in group['skills']
                     if s.get('pluginId') == module.PLUGIN]
            assert len(found) == 1 and found[0]['enabled'] is True
            assert Path(found[0]['path']).read_bytes() == canonical_skill
            status = await rpc('mcpServerStatus/list', {'detail': 'full', 'limit': 100}, 3)
            assert status['nextCursor'] is None
            servers = [s for s in status['data'] if s['name'] == module.NAME]
            assert len(servers) == 1 and servers[0]['pluginId'] == module.PLUGIN
            assert servers[0]['toolsError'] is None
            assert set(servers[0]['tools']) == {
                'get_service', 'start_service', 'submit_run', 'list_runs', 'get_run',
                'read_evidence',
                'answer_decision', 'cancel_run',
                'reconcile_tracker',
                'gates_only_preflight', 'admit_gates_only',
                'repair_admission_preflight', 'continue_repair',
                'continue_feature', 'merge_feature', 'revise_feature_plan',
            }
        finally:
            process.stdin.close()
            try:
                await asyncio.wait_for(process.wait(), timeout=10)
            except TimeoutError:
                process.terminate()
                await asyncio.wait_for(process.wait(), timeout=10)
    assert module.rollback(codex, home, receipt)['state'] == 'rolled_back'
    assert module.snapshot(codex, home) == before
    assert installation['skill'].exists()


@pytest.mark.parametrize('change', [
    'transport', 'disabled', 'foreign-plugin', 'missing-server', 'raw-duplicate',
])
def test_plugin_aggregate_inventory_requires_proven_owned_identity(primary, change):
    module, fixture, runtime, config, request, state = primary
    first = module.activate('codex', fixture['home'], runtime, config, request)
    manifest = Path(first['rollback_manifest'])
    state['expose_plugin_mcp'] = True
    state['plugin_mcp'] = json.loads(json.dumps(state['plugin_mcp']))
    if change == 'transport':
        state['plugin_mcp']['transport']['args'] = ['--config', '/foreign.json']
    elif change == 'disabled':
        state['plugin_mcp']['enabled'] = False
    elif change == 'foreign-plugin':
        state['plugins'][0]['pluginId'] = 'collision@foreign'
    elif change == 'missing-server':
        state['plugin_mcp'] = None
    else:
        state['mcp'][module.NAME] = state['plugin_mcp']
        host = fixture['home'] / 'config.toml'
        host.write_text(host.read_text() + state['direct_toml'])
    effects = list(state['effects'])
    if change == 'missing-server' and state['package_format'] == 'portable':
        # The old portable layout has no automatic MCP on the pinned host.
        assert module.activate('codex', fixture['home'], runtime, config, request)['existing']
    else:
        with pytest.raises(ValueError):
            module.activate('codex', fixture['home'], runtime, config, request)
        with pytest.raises(ValueError):
            module.rollback('codex', fixture['home'], manifest)
    assert effects == state['effects']
