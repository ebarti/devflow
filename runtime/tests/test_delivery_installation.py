"""Owned direct MCP pointer updates use public CLI readbacks and preserve the host."""
from __future__ import annotations

import hashlib
import importlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import pytest

NAME = 'devflow-local-delivery'
ROOT = Path(__file__).resolve().parents[2]


def _sha(content):
    return hashlib.sha256(content).hexdigest()


def _private(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    path.chmod(0o600)


@pytest.fixture
def installation(tmp_path):
    home, runtime, tools = tmp_path / 'host', tmp_path / 'runtime', tmp_path / 'bin'
    home.mkdir()
    tools.mkdir()
    (home / 'config.toml').write_text('model="gpt-6.1-sol"\nmodel_reasoning_effort="max"\n')
    agents = home / 'agents' / 'unrelated.toml'
    agents.parent.mkdir()
    agents.write_text('preserved agent bytes\n')
    for name in ('devflow-delivery', 'devflow-delivery-mcp'):
        executable = runtime / '.venv' / 'bin' / name
        executable.parent.mkdir(parents=True, exist_ok=True)
        executable.write_text('#!/bin/sh\nexit 0\n')
        executable.chmod(0o755)
    executable = runtime / '.venv' / 'bin' / 'devflow-delivery-mcp'
    original = tmp_path / 'original.json'
    raw = {'state_root': str(tmp_path / 'state'), 'roles': {'implement': {
        'model': 'gpt-6.1-sol', 'effort': 'max'}}, 'capacity': 2}
    _private(original, raw)
    trusted = tmp_path / 'state' / 'trusted-local.json'
    _private(trusted, {**raw, 'execution_mode': 'trusted-local'})
    skill = home / 'skills' / NAME / 'SKILL.md'
    skill.parent.mkdir(parents=True)
    skill.write_text('Inspected previous owned skill\n')
    entry = {'name': NAME, 'enabled': True, 'transport': {
        'type': 'stdio', 'command': str(executable),
        'args': ['--config', str(original)], 'env': {}, 'env_vars': [], 'cwd': None}}
    other = {'name': 'unrelated', 'enabled': True, 'transport': {'type': 'stdio',
             'command': '/bin/true', 'args': []}}
    registry = home / 'fixture-registry.json'
    _private(registry, {'entries': {NAME: entry, 'unrelated': other}, 'adds': []})
    codex = tools / 'codex'
    codex.write_text(
        f'#!{sys.executable}\n'
        'import json,os,pathlib,signal,sys\n'
        'p=pathlib.Path(os.environ["CODEX_HOME"])/"fixture-registry.json"\n'
        'd=json.loads(p.read_text()); args=sys.argv[2:]; name="devflow-local-delivery"\n'
        'if args[0]=="list": print(json.dumps(list(d["entries"].values())))\n'
        'elif args[0]=="get":\n'
        ' if d.pop("fail_next_get",False):\n'
        '  p.write_text(json.dumps(d)); print("Readback unavailable",file=sys.stderr)\n'
        '  sys.exit(5)\n'
        ' print(json.dumps(d["entries"][args[1]]))\n'
        'elif args[0]=="add":\n'
        ' start=args.index("--"); transport=d["entries"][name]["transport"]\n'
        ' transport.update(command=args[start+1],args=args[start+2:])\n'
        ' d["adds"].append(args[start+2:])\n'
        ' if d.pop("fail_after_add",False): d["fail_next_get"]=True\n'
        ' crash=d.pop("crash_after_add",False); p.write_text(json.dumps(d))\n'
        ' if crash: os.kill(os.getppid(),signal.SIGKILL)\n'
        ' print("Registered")\n'
    )
    codex.chmod(0o755)
    request = tmp_path / 'request.json'
    _private(request, {'command_id': 'owned-trust-upgrade',
                      'expected_registration_sha256': _sha(json.dumps(
                          entry, sort_keys=True, separators=(',', ':')).encode()),
                      'expected_config_path': str(original),
                      'expected_config_sha256': _sha(original.read_bytes()),
                      'expected_skill_sha256': _sha(skill.read_bytes())})
    command = [sys.executable, str(ROOT / 'runtime' / 'desktop' / 'install.py'),
               '--runtime-dir', str(runtime), '--config', str(trusted), '--codex-home', str(home)]
    env = {**os.environ, 'PATH': str(tools) + os.pathsep + os.environ['PATH']}
    return {'home': home, 'registry': registry, 'original': original, 'trusted': trusted,
            'request': request, 'command': command, 'env': env, 'entry': entry, 'other': other,
            'skill': skill, 'agents': agents}


def _run(fixture, *args):
    return subprocess.run([*fixture['command'], *args], env=fixture['env'],
                          capture_output=True, text=True, timeout=30)


def test_owned_pointer_upgrade_replay_conflict_and_public_rollback(installation):
    fixture = installation
    original = fixture['original'].read_bytes()
    skill = fixture['skill'].read_bytes()
    settings = (fixture['home'] / 'config.toml').read_bytes()
    first = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert first.returncode == 0, first.stderr
    receipt = json.loads(first.stdout)
    assert receipt['state'] == 'applied'
    manifest = Path(receipt['rollback_manifest'])
    assert manifest.stat().st_mode & 0o777 == 0o600
    repeated = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert repeated.returncode == 0, repeated.stderr
    assert json.loads(repeated.stdout)['existing'] is True
    current = json.loads(fixture['registry'].read_bytes())
    assert len(current['adds']) == 1
    assert current['entries'][NAME]['transport']['args'] == ['--config', str(fixture['trusted'])]
    assert current['entries']['unrelated'] == fixture['other']
    assert fixture['original'].read_bytes() == original
    assert (fixture['home'] / 'config.toml').read_bytes() == settings
    assert fixture['agents'].read_text() == 'preserved agent bytes\n'
    rolled = _run(fixture, '--rollback-owned-manifest', str(manifest))
    assert rolled.returncode == 0, rolled.stderr
    assert json.loads(fixture['registry'].read_bytes())['entries'][NAME] == fixture['entry']
    assert fixture['skill'].read_bytes() == skill
    current = json.loads(fixture['registry'].read_bytes())
    current['entries'][NAME]['transport']['args'] = ['--config', '/modified/foreign.json']
    _private(fixture['registry'], current)
    rejected = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert rejected.returncode != 0 and 'changed' in rejected.stderr
    assert json.loads(fixture['registry'].read_bytes()) == current


@pytest.mark.parametrize('failure', ['readback', 'crash'])
def test_owned_update_failure_restores_or_resumes_exact_pointer(installation, failure):
    fixture = installation
    current = json.loads(fixture['registry'].read_bytes())
    current['fail_after_add' if failure == 'readback' else 'crash_after_add'] = True
    _private(fixture['registry'], current)
    first = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert first.returncode != 0
    current = json.loads(fixture['registry'].read_bytes())
    if failure == 'readback':
        assert current['entries'][NAME] == fixture['entry']
    repeated = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert repeated.returncode == 0, repeated.stderr
    current = json.loads(fixture['registry'].read_bytes())
    assert current['entries']['unrelated'] == fixture['other']
    assert current['entries'][NAME]['transport']['args'] == ['--config', str(fixture['trusted'])]
    assert len(current['adds']) == (3 if failure == 'readback' else 1)


def test_applied_replay_rejects_unrecorded_owned_pointer_reversal(installation):
    fixture = installation
    assert _run(fixture, '--repoint-owned-request', str(fixture['request'])).returncode == 0
    current = json.loads(fixture['registry'].read_bytes())
    current['entries'][NAME] = fixture['entry']
    _private(fixture['registry'], current)
    replay = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert replay.returncode != 0 and 'changed' in replay.stderr
    assert json.loads(fixture['registry'].read_bytes()) == current


@pytest.mark.parametrize('change', ['original-config', 'owned-skill', 'roles'])
def test_pointer_upgrade_rejects_changed_inspected_inputs(installation, change):
    fixture = installation
    if change == 'owned-skill':
        fixture['skill'].write_text('Uninspected skill edit')
    else:
        path = fixture['original'] if change == 'original-config' else fixture['trusted']
        raw = json.loads(path.read_bytes())
        raw['roles']['implement']['effort'] = 'low'
        _private(path, raw)
    result = _run(fixture, '--repoint-owned-request', str(fixture['request']))
    assert result.returncode != 0
    assert json.loads(fixture['registry'].read_bytes())['adds'] == []


@pytest.fixture
def canonicalized_unknown(installation, monkeypatch):
    fixture = installation
    monkeypatch.syspath_prepend(str(ROOT / 'runtime/desktop'))
    module = importlib.import_module('owned_upgrade')
    host_config = fixture['home'] / 'config.toml'
    host_config.write_text(host_config.read_text() +
        '[mcp_servers.node_repl]\ncommand="/usr/bin/true"\n'
        'args=[]\nstartup_timeout_sec=10\n')
    fake = Path(fixture['env']['PATH'].split(os.pathsep)[0]) / 'codex'
    text = fake.read_text().replace(
        ' d["adds"].append(args[start+2:])\n',
        ' d["adds"].append(args[start+2:])\n'
        ' c=pathlib.Path(os.environ["CODEX_HOME"])/"config.toml"\n'
        ' c.write_text(c.read_text().replace("args=[]\\n", "")'
        '.replace("startup_timeout_sec=10\\n", "startup_timeout_sec=10.0\\n"))\n')
    fake.write_text(text)
    semantic = module.unrelated_seal
    monkeypatch.setattr(module, 'unrelated_seal', lambda value, expected=None: module.seal(value))
    args = (str(fake), fixture['home'], Path(fixture['entry']['transport']['command']),
            fixture['trusted'], ROOT / 'runtime/desktop/devflow-local-delivery/SKILL.md',
            fixture['request'])
    with pytest.raises(ValueError, match='public update readback'):
        module.upgrade(*args)
    manifest = (fixture['home']
                / '.devflow-local-delivery-upgrades/owned-trust-upgrade/manifest.json')
    failed = json.loads(manifest.read_text())
    assert failed['state'] == 'unknown' and 'rollback refused' in failed['rollback_error']
    monkeypatch.setattr(module, 'unrelated_seal', semantic)
    return module, fixture, args, manifest, failed


def test_legacy_unknown_default_canonicalization_replays_without_new_pointer_effect(
    canonicalized_unknown,
):
    module, fixture, args, manifest, failed = canonicalized_unknown
    registry = json.loads(fixture['registry'].read_text())
    assert len(registry['adds']) == 1
    accepted = module.upgrade(*args)
    assert accepted['state'] == 'applied'
    current = json.loads(manifest.read_text())
    assert {k: v for k, v in current.items() if k != 'state'} == {
        k: v for k, v in failed.items() if k != 'state'}
    assert module.upgrade(*args)['existing']
    assert json.loads(fixture['registry'].read_text()) == registry
    assert module.rollback(args[0], fixture['home'], manifest)['state'] == 'rolled_back'
    restored = json.loads(fixture['registry'].read_text())
    assert restored['entries'][NAME] == fixture['entry']
    assert fixture['skill'].read_text() == 'Inspected previous owned skill\n'
    assert fixture['agents'].read_text() == 'preserved agent bytes\n'


@pytest.mark.parametrize('change', ['args', 'timeout', 'boolean', 'infinite', 'command',
                                  'model', 'public-inventory'])
def test_legacy_unknown_canonicalization_refuses_meaningful_unrelated_drift(
    canonicalized_unknown, change,
):
    module, fixture, args, manifest, _failed = canonicalized_unknown
    host_config = fixture['home'] / 'config.toml'
    text = host_config.read_text()
    if change == 'args':
        text += 'args=["changed"]\n'
    elif change in {'timeout', 'boolean', 'infinite'}:
        value = {'timeout': '20.0', 'boolean': 'true', 'infinite': 'inf'}[change]
        text = text.replace('startup_timeout_sec=10.0', 'startup_timeout_sec=' + value)
    elif change == 'command':
        text = text.replace('/usr/bin/true', '/usr/bin/false')
    elif change == 'model':
        text = text.replace('gpt-6.1-sol', 'user-selected-model')
    else:
        registry = json.loads(fixture['registry'].read_text())
        registry['entries']['unrelated']['transport']['args'] = ['changed']
        _private(fixture['registry'], registry)
    host_config.write_text(text)
    before = (manifest.read_bytes(), fixture['registry'].read_bytes(), host_config.read_bytes())
    with pytest.raises(ValueError):
        module.upgrade(*args)
    with pytest.raises(ValueError):
        module.rollback(args[0], fixture['home'], manifest)
    assert before == (manifest.read_bytes(), fixture['registry'].read_bytes(),
                      host_config.read_bytes())


def test_only_stdio_toml_defaults_have_bounded_legacy_equivalence(monkeypatch):
    monkeypatch.syspath_prepend(str(ROOT / 'runtime/desktop'))
    module = importlib.import_module('owned_upgrade')
    original = {'settings': {'mcp_servers': {'node_repl': {
        'command': '/usr/bin/true', 'args': [], 'startup_timeout_sec': 10}}},
        'other_mcp': [{'transport': {'type': 'stdio', 'args': []}}]}
    after = deepcopy(original)
    entry = after['settings']['mcp_servers']['node_repl']
    entry.pop('args')
    entry['startup_timeout_sec'] = 10.0
    legacy = module.seal(original)
    assert module.unrelated_seal(after) == module.unrelated_seal(original)
    assert module.unrelated_seal(after, legacy) == legacy
    after['other_mcp'][0]['transport'].pop('args')
    assert module.unrelated_seal(after, legacy) != legacy
    many = {'settings': {'mcp_servers': {str(n): {'command': '/usr/bin/true', 'args': [],
            'startup_timeout_sec': 10.0} for n in range(7)}}}
    canonical = deepcopy(many)
    for item in canonical['settings']['mcp_servers'].values():
        item.pop('args')
    assert module.unrelated_seal(canonical, module.seal(many)) != module.seal(many)
    assert module.unrelated_seal(canonical) == module.unrelated_seal(many)


@pytest.mark.parametrize('transport', [{'command': '/bin/true'}, {'url': 'https://example.invalid/mcp'}])
def test_mcp_enabled_true_default_requires_unchanged_public_effective_inventory(
    monkeypatch, transport,
):
    monkeypatch.syspath_prepend(str(ROOT / 'runtime/desktop'))
    module = importlib.import_module('owned_upgrade')
    absent = {'settings': {'mcp_servers': {'foreign': transport}},
              'other_mcp': [{'name': 'foreign', 'enabled': True}]}
    explicit = deepcopy(absent)
    explicit['settings']['mcp_servers']['foreign']['enabled'] = True
    assert module.unrelated_seal(explicit) == module.unrelated_seal(absent)
    assert module.unrelated_seal(absent, module.seal(explicit)) == module.seal(explicit)
    assert module.unrelated_seal(explicit, module.seal(absent)) == module.seal(absent)
    for value in (False, 'true', 1, None):
        changed = deepcopy(explicit)
        changed['settings']['mcp_servers']['foreign']['enabled'] = value
        assert module.unrelated_seal(changed) != module.unrelated_seal(absent)
    for confirmation in ([], [{'name': 'foreign', 'enabled': False}],
                         [{'name': 'foreign', 'enabled': 'true'}],
                         [{'name': 'foreign', 'enabled': 1}],
                         [{'name': 'foreign', 'enabled': True}] * 2):
        before, after = deepcopy(explicit), deepcopy(absent)
        before['other_mcp'] = after['other_mcp'] = confirmation
        assert module.unrelated_seal(before) != module.unrelated_seal(after)
    public_change = deepcopy(absent)
    public_change['other_mcp'][0]['enabled'] = False
    assert module.unrelated_seal(public_change) != module.unrelated_seal(explicit)
