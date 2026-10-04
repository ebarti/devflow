"""Guarded primary-plugin activation; public CLI effects and a retained direct-skill archive."""
from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import tomllib
from pathlib import Path

from owned_upgrade import (
    NAME,
    install_pointer,
    locked,
    private,
    registration,
    save,
    seal,
    sha,
    unrelated_seal,
)
from package_plugin import ENTRY, FORMATS, check_path, check_plugin, json_bytes, package_files

PLUGIN = 'devflow@devflow-local'
MARKET = 'devflow-local'


def command(codex, home, *args):
    result = subprocess.run([codex, *args], env={**os.environ, 'CODEX_HOME': str(home)},
                            text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise ValueError('public Codex command failed: ' + result.stderr.strip()[:500])
    return json.loads(result.stdout) if args[-1] == '--json' else result.stdout


def snapshot(codex, home, expected_unrelated=None):
    mcp = command(codex, home, 'mcp', 'list', '--json')
    plugins = command(codex, home, 'plugin', 'list', '--json')
    text = command(codex, home, 'plugin', 'marketplace', 'list')
    lines = text.strip().splitlines()
    empty = lines == ['No plugin marketplaces in scope.']
    if (not isinstance(mcp, list) or not isinstance(plugins, dict)
            or not isinstance(plugins.get('installed'), list)
            or not lines or (not empty and lines[0].split() != ['MARKETPLACE', 'ROOT'])):
        raise ValueError('unexpected public host inventory')
    marketplaces = {}
    for line in ([] if empty else lines[1:]):
        key, root = line.split(None, 1)
        if key in marketplaces:
            raise ValueError('duplicate marketplace identity')
        marketplaces[key] = root.strip()
    direct = (command(codex, home, 'mcp', 'get', NAME, '--json')
              if any(e.get('name') == NAME for e in mcp) else None)
    own = [e for e in plugins['installed'] if e.get('pluginId') == PLUGIN]
    if len(own) > 1:
        raise ValueError('duplicate owned plugin identity')
    config = home / 'config.toml'
    check_path(config)
    settings = tomllib.loads(config.read_text()) if config.exists() else {}
    if NAME in settings.get('mcp_servers', {}) and direct is None:
        raise ValueError('raw owned MCP entry is missing from public inventory')
    # Public MCP inventory includes automatically imported plugin servers. A
    # name match becomes plugin-provided only after the raw direct table is
    # absent; owned_state still authenticates the plugin/cache and transport.
    plugin_mcp = None
    if own and NAME not in settings.get('mcp_servers', {}):
        plugin_mcp, direct = direct, None
    for group, key in [('mcp_servers', NAME), ('plugins', PLUGIN), ('marketplaces', MARKET)]:
        settings.get(group, {}).pop(key, None)
        if not settings.get(group):
            settings.pop(group, None)
    return {'direct': direct, 'plugin': own[0] if own else None,
            **({'plugin_mcp': plugin_mcp} if plugin_mcp is not None else {}),
            'marketplace': marketplaces.get(MARKET),
            'plugins_sha256': seal(plugins), 'marketplaces_sha256': seal(marketplaces),
            'unrelated_sha256': unrelated_seal({'settings': settings,
                'mcp': sorted((e for e in mcp if e.get('name') != NAME), key=lambda e: e['name']),
                'plugins': {**plugins, 'installed': [e for e in plugins['installed']
                                                    if e.get('pluginId') != PLUGIN]},
                'marketplaces': {k: v for k, v in marketplaces.items() if k != MARKET}},
                                               expected_unrelated)}


def skill(path, expected):
    check_path(path)
    if (not path.is_dir() or sorted(p.name for p in path.iterdir()) != ['SKILL.md']
            or path.stat().st_uid != os.getuid()):
        raise ValueError('owned direct skill directory changed')
    file = path / 'SKILL.md'
    info = file.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1
            or sha(file.read_bytes()) != expected):
        raise ValueError('owned direct skill bytes or identity changed')


def package_check(root, files):
    catalog = root / '.agents/plugins/marketplace.json'
    check_path(catalog)
    expected = {'name': MARKET, 'interface': {'displayName': 'Devflow Local'}, 'plugins': [ENTRY]}
    if (catalog.read_bytes() != json_bytes(expected)
            or not check_plugin(root / 'plugins/devflow', files)):
        raise ValueError('fresh owned plugin package changed')
    owned_tree(root)


def owned_tree(root):
    check_path(root)
    for path in [root, *root.rglob('*')]:
        check_path(path)
        info = path.lstat()
        if (info.st_uid != os.getuid()
                or (not stat.S_ISDIR(info.st_mode)
                    and (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1))):
            raise ValueError('owned plugin file identity changed')


def owned_state(home, current, manifest, files):
    if (current['unrelated_sha256'] != manifest['before']['unrelated_sha256']
            or seal(current['direct']) not in (seal(None), seal(manifest['before']['direct']))
            or current['marketplace'] not in (None, manifest['marketplace_root'])):
        raise ValueError('owned installation or unrelated host settings changed')
    plugin = current['plugin']
    if manifest['state'] == 'applied' and plugin and current['direct'] is not None:
        raise ValueError('applied primary plugin has a raw direct duplicate')
    cache = home / 'plugins/cache/devflow-local/devflow'
    manifest_file = ('.codex-plugin/plugin.json'
                     if manifest.get('package_format', 'portable') == 'codex' else 'plugin.json')
    version = json.loads(files[manifest_file])['version']
    if plugin:
        if (plugin.get('enabled') is not True or plugin.get('installed') is not True
                or plugin.get('name') != 'devflow' or plugin.get('version') != version
                or plugin.get('marketplaceName') != MARKET
                or plugin.get('source') != {'source': 'local',
                    'path': str(Path(manifest['marketplace_root']) / 'plugins/devflow')}
                or plugin.get('marketplaceSource') != {'sourceType': 'local',
                                                      'source': manifest['marketplace_root']}):
            raise ValueError('owned enabled plugin registration changed')
    check_path(cache)
    if cache.exists():
        owned_tree(cache)
        if (sorted(p.name for p in cache.iterdir()) != [version]
                or not check_plugin(cache / version, files)):
            raise ValueError('owned installed plugin cache changed')
    if plugin and not cache.exists():
        raise ValueError('owned installed plugin cache is missing')
    if current.get('plugin_mcp') is not None or (
        plugin and current['direct'] is None and manifest.get('package_format') == 'codex'
    ):
        if (not plugin or current['direct'] is not None
                or seal(current.get('plugin_mcp')) != seal(manifest['before']['direct'])):
            raise ValueError('owned plugin MCP transport or enabled state changed')
    live = home / 'skills' / NAME
    archive = Path(manifest['archive'])
    if live.exists() == archive.exists():
        raise ValueError('owned direct skill archive is ambiguous')
    skill(live if live.exists() else archive, manifest['skill_sha256'])


def activate(codex, home, runtime, config, request_path):
    with locked(home):
        request = json.loads(private(request_path))
        required = {'command_id', 'marketplace_root', 'expected_registration_sha256',
                    'expected_config_sha256', 'expected_skill_sha256',
                    'expected_plugin_inventory_sha256', 'expected_marketplace_inventory_sha256'}
        if (not isinstance(request, dict)
                or set(request) not in (required, required | {'package_format'})
                or request.get('package_format', 'portable') not in FORMATS
                or not isinstance(request['command_id'], str)
                or not isinstance(request['marketplace_root'], str)
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', request['command_id'])
                or any(not isinstance(request[k], str)
                       or not re.fullmatch(r'[0-9a-f]{64}', request[k])
                       for k in required if k.startswith('expected_'))):
            raise ValueError('invalid primary-plugin activation request')
        raw = json.loads(private(config))
        selected_root = Path(request['marketplace_root'])
        check_path(selected_root)
        if not selected_root.is_absolute():
            raise ValueError('primary-plugin marketplace root must be absolute')
        root = selected_root.resolve(strict=True)
        if (raw.get('execution_mode') != 'trusted-local'
                or not root.is_relative_to(Path(raw['state_root']).resolve(strict=True))
                or sha(private(config)) != request['expected_config_sha256']):
            raise ValueError('inspected trusted config/package destination changed')
        package_format = request.get('package_format', 'portable')
        files = package_files(runtime, config, package_format)
        package_check(root, files)
        binding = seal({'request': request, 'runtime': str(runtime), 'config': str(config),
                        'home': str(home), 'package': {k: sha(v) for k, v in files.items()}})
        path = home / '.devflow-local-delivery-upgrades' / request['command_id'] / 'manifest.json'
        manifest = json.loads(private(path)) if path.exists() else None
        current = snapshot(codex, home,
                           manifest['before']['unrelated_sha256'] if manifest else None)
        if manifest is not None:
            if (manifest['command_digest'] != binding
                    or manifest.get('package_format', 'portable') != package_format):
                raise ValueError('activation command ID already binds different inputs')
            if manifest['state'] == 'rolled_back':
                if seal(current) != seal(manifest['before']):
                    raise ValueError('rolled-back installation changed before replay')
                return {'state': 'rolled_back', 'existing': True, 'rollback_manifest': str(path)}
        else:
            if (current['plugin'] or current['marketplace']
                    or not registration(current['direct'] or {},
                        (runtime / '.venv/bin/devflow-delivery-mcp').resolve(strict=True), config)
                    or seal(current['direct']) != request['expected_registration_sha256']
                    or current['plugins_sha256'] != request['expected_plugin_inventory_sha256']
                    or current['marketplaces_sha256']
                    != request['expected_marketplace_inventory_sha256']
                    or (home / 'plugins/cache/devflow-local/devflow').exists()):
                raise ValueError('inspected primary-plugin host inventory changed')
            skill(home / 'skills' / NAME, request['expected_skill_sha256'])
            manifest = {'kind': 'primary_plugin', 'state': 'prepared', 'command_digest': binding,
                        'codex_home': str(home), 'before': current, 'marketplace_root': str(root),
                        'runtime': str(runtime), 'config': str(config),
                        'config_sha256': request['expected_config_sha256'],
                        'skill_sha256': request['expected_skill_sha256'],
                        'archive': str(path.parent / 'direct-skill')}
            if package_format != 'portable':
                manifest['package_format'] = package_format
            save(path, manifest)
        owned_state(home, current, manifest, files)
        if manifest['state'] == 'applied':
            if (current['direct'] or not current['plugin'] or not current['marketplace']
                    or (home / 'skills' / NAME).exists()):
                raise ValueError('applied primary-plugin installation changed before replay')
            return {'state': 'applied', 'existing': True, 'rollback_manifest': str(path)}
        try:
            if current['marketplace'] is None:
                command(codex, home, 'plugin', 'marketplace', 'add', str(root))
            current = snapshot(codex, home, manifest['before']['unrelated_sha256'])
            owned_state(home, current, manifest, files)
            if current['plugin'] is None:
                command(codex, home, 'plugin', 'add', PLUGIN, '--json')
            current = snapshot(codex, home, manifest['before']['unrelated_sha256'])
            owned_state(home, current, manifest, files)
            if not current['plugin']:
                raise ValueError('public plugin activation was not confirmed')
            if current['direct']:
                command(codex, home, 'mcp', 'remove', NAME)
            live = home / 'skills' / NAME
            if live.exists():
                skill(live, manifest['skill_sha256'])
                live.rename(Path(manifest['archive']))
            current = snapshot(codex, home, manifest['before']['unrelated_sha256'])
            owned_state(home, current, manifest, files)
            if current['direct'] or not current['plugin'] or live.exists():
                raise ValueError('primary-plugin readback disagrees')
            manifest['state'] = 'applied'
            save(path, manifest)
        except (ValueError, OSError, subprocess.SubprocessError) as exc:
            manifest.update(state='unknown', last_error=str(exc)[:500])
            save(path, manifest)
            raise
        return {'state': 'applied', 'existing': False, 'rollback_manifest': str(path)}


def rollback(codex, home, path):
    with locked(home):
        manifest = json.loads(private(path))
        if (manifest.get('kind') != 'primary_plugin' or manifest['codex_home'] != str(home)
                or not path.resolve().is_relative_to(home / '.devflow-local-delivery-upgrades')):
            raise ValueError('primary-plugin rollback belongs to another installation')
        config, runtime = Path(manifest['config']), Path(manifest['runtime'])
        if sha(private(config)) != manifest['config_sha256']:
            raise ValueError('owned primary-plugin config changed before rollback')
        files = package_files(runtime, config, manifest.get('package_format', 'portable'))
        package_check(Path(manifest['marketplace_root']), files)
        current = snapshot(codex, home, manifest['before']['unrelated_sha256'])
        owned_state(home, current, manifest, files)
        if current['plugin']:
            command(codex, home, 'plugin', 'remove', PLUGIN, '--json')
        if current['marketplace']:
            command(codex, home, 'plugin', 'marketplace', 'remove', MARKET, '--json')
        if current['direct'] is None:
            direct = manifest['before']['direct']['transport']
            install_pointer(codex, home, direct['command'], config)
        archive = Path(manifest['archive'])
        if archive.exists():
            skill(archive, manifest['skill_sha256'])
            archive.rename(home / 'skills' / NAME)
        if (seal(snapshot(codex, home, manifest['before']['unrelated_sha256']))
                != seal(manifest['before'])):
            raise ValueError('primary-plugin rollback readback disagrees')
        manifest['state'] = 'rolled_back'
        save(path, manifest)
        return {'state': 'rolled_back', 'rollback_manifest': str(path)}
