"""Guarded direct MCP/skill pointer update using only the public Codex CLI."""
from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
import re
import stat
import subprocess
import tomllib
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path

NAME = 'devflow-local-delivery'


def sha(content):
    return hashlib.sha256(content).hexdigest()


def seal(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), default=str).encode())


def unrelated_seal(value, expected=None):
    """Only evidenced public-CLI stdio defaults are representation-equivalent."""
    canonical = deepcopy(value)
    alternatives = []
    for entry in canonical.get('settings', {}).get('mcp_servers', {}).values():
        if not isinstance(entry, dict) or not isinstance(entry.get('command'), str):
            continue
        if 'args' not in entry or entry['args'] == []:
            entry.pop('args', None)
            alternatives.append((entry, 'args', []))
        if 'startup_timeout_sec' in entry:
            seconds = entry['startup_timeout_sec']
            if (type(seconds) not in (int, float)
                    or (isinstance(seconds, float) and not math.isfinite(seconds))):
                raise ValueError('stdio startup timeout must be finite numeric seconds')
            if isinstance(seconds, int) or seconds.is_integer():
                entry['startup_timeout_sec'] = int(seconds)
                try:
                    floating = float(seconds)
                except OverflowError:
                    continue
                if math.isfinite(floating) and floating == int(seconds):
                    alternatives.append((entry, 'startup_timeout_sec', floating))
    result = seal(canonical)
    if expected is None or result == expected:
        return result
    if seal(value) == expected:
        return expected
    # Legacy journals stored only a raw digest. Reconstruct exclusively these
    # equivalent representations, never adopt a fresh baseline. Limit work on
    # old receipts; new canonical receipts need no compatibility search.
    if len(alternatives) <= 12:
        for mask in range(1 << len(alternatives)):
            for index, (entry, key, alternate) in enumerate(alternatives):
                if key == 'args':
                    entry.pop(key, None)
                else:
                    entry[key] = int(alternate)
                if mask & (1 << index):
                    entry[key] = alternate
            if seal(canonical) == expected:
                return expected
    return result


def private(path):
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600
            or any(parent.is_symlink() for parent in path.parents)):
        raise ValueError('upgrade input/receipt must be a private owned regular file')
    return path.read_bytes()


def write(path, content):
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    if any(parent.is_symlink() for parent in path.parents) or path.is_symlink():
        raise ValueError('upgrade receipt or skill destination is linked')
    if path.exists() and path.name != 'SKILL.md':
        private(path)
    temporary = path.with_name(path.name + f'.{os.getpid()}.tmp')
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(descriptor, 'wb') as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def save(path, value):
    write(path, (json.dumps(value, sort_keys=True, indent=2) + '\n').encode())


def cli(codex, home, *args):
    result = subprocess.run([codex, 'mcp', *args],
                            env={**os.environ, 'CODEX_HOME': str(home)},
                            capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise ValueError('public MCP command failed: ' + result.stderr.strip()[:500])
    return json.loads(result.stdout) if args[-1] == '--json' else None


def snapshot(codex, home, expected_unrelated=None):
    entries = cli(codex, home, 'list', '--json')
    if not isinstance(entries, list) or any(not isinstance(e, dict) for e in entries):
        raise ValueError('unexpected public MCP inventory')
    entry = cli(codex, home, 'get', NAME, '--json')
    config = home / 'config.toml'
    settings = tomllib.loads(config.read_text()) if config.exists() else {}
    settings.get('mcp_servers', {}).pop(NAME, None)
    return entry, unrelated_seal({'settings': settings,
                        'other_mcp': sorted((e for e in entries if e['name'] != NAME),
                                            key=lambda e: e['name'])}, expected_unrelated)


def registration(entry, executable, config):
    transport = entry.get('transport', {})
    return (entry.get('name') == NAME and entry.get('enabled', True)
            and transport.get('type') == 'stdio' and transport.get('command') == str(executable)
            and transport.get('args') == ['--config', str(config)]
            and transport.get('env') in (None, {}) and transport.get('env_vars') in (None, [])
            and transport.get('cwd') is None)


def install_pointer(codex, home, executable, config):
    cli(codex, home, 'add', NAME, '--', str(executable), '--config', str(config))


@contextmanager
def locked(home):
    root = home / '.devflow-local-delivery-upgrades'
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError('upgrade journal directory is linked')
    descriptor = os.open(root / '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if info.st_uid != os.getuid() or info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError('upgrade lock is not private and owned')
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError('another owned pointer update is active') from exc
        yield
    finally:
        os.close(descriptor)


def rollback(codex, home, manifest_path):
    with locked(home):
        return _rollback(codex, home, manifest_path)


def _rollback(codex, home, manifest_path):
    from owned_drift import guard

    manifest = json.loads(private(manifest_path))
    expected_root = home / '.devflow-local-delivery-upgrades'
    if (not manifest_path.resolve().is_relative_to(expected_root.resolve())
            or manifest['codex_home'] != str(home)):
        raise ValueError('rollback receipt belongs to another host installation')
    try:
        expected = guard(codex, home, manifest_path, manifest)
    except ValueError as exc:
        raise ValueError('owned installation changed; rollback refused: ' + str(exc)) from exc
    current, unrelated = snapshot(codex, home, expected)
    old, new = manifest['before'], manifest['after']
    target = home / 'skills' / NAME / 'SKILL.md'
    if (unrelated != expected or current not in (old, new)
            or sha(target.read_bytes()) not in (manifest['old_skill_sha256'],
                                               manifest['new_skill_sha256'])
            or sha(private(Path(manifest['old_config_path']))) != manifest['old_config_sha256']):
        raise ValueError('owned installation or unrelated host settings changed; rollback refused')
    if current != old:
        guard(codex, home, manifest_path, manifest)
        install_pointer(codex, home, old['transport']['command'], manifest['old_config_path'])
        guard(codex, home, manifest_path, manifest, pointer=old)
    backup = manifest_path.parent / 'previous-SKILL.md'
    previous = private(backup)
    if sha(previous) != manifest['old_skill_sha256']:
        raise ValueError('owned skill rollback bytes changed')
    guard(codex, home, manifest_path, manifest, pointer=old)
    write(target, previous)
    guard(codex, home, manifest_path, manifest, pointer=old,
          skill_sha256=manifest['old_skill_sha256'])
    verified, unrelated_after = snapshot(codex, home, expected)
    if verified != old or unrelated_after != unrelated:
        raise ValueError('public rollback readback disagrees')
    manifest['state'] = 'rolled_back'
    save(manifest_path, manifest)
    return {'state': 'rolled_back', 'rollback_manifest': str(manifest_path)}


def upgrade(codex, home, executable, config, source_skill, request_path):
    with locked(home):
        return _upgrade(codex, home, executable, config, source_skill, request_path)


def _upgrade(codex, home, executable, config, source_skill, request_path):
    from owned_drift import guard

    request = json.loads(private(request_path))
    required = {'command_id', 'expected_registration_sha256', 'expected_config_path',
                'expected_config_sha256', 'expected_skill_sha256'}
    if (not isinstance(request, dict) or set(request) != required
            or not isinstance(request['command_id'], str)
            or not isinstance(request['expected_config_path'], str)
            or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._-]{0,127}', request['command_id'])
            or any(not isinstance(request[k], str) or not re.fullmatch(r'[0-9a-f]{64}', request[k])
                   for k in ('expected_registration_sha256', 'expected_config_sha256',
                             'expected_skill_sha256'))):
        raise ValueError('invalid owned pointer upgrade request')
    old_config = Path(request['expected_config_path'])
    if (not old_config.is_absolute()
            or sha(private(old_config)) != request['expected_config_sha256']):
        raise ValueError('frozen original config changed')
    before_config = json.loads(private(old_config))
    after_config = json.loads(private(config))
    state_root = Path(after_config['state_root']).resolve(strict=True)
    if not config.resolve(strict=True).is_relative_to(state_root):
        raise ValueError('trusted config must remain within the existing private state root')
    if (before_config.pop('execution_mode', 'native-profile') != 'native-profile'
            or after_config.pop('execution_mode', None) != 'trusted-local'
            or before_config != after_config):
        raise ValueError('owned pointer update permits only the trusted execution mode delta')
    target = home / 'skills' / NAME / 'SKILL.md'
    if target.is_symlink() or target.parent.is_symlink() or home.is_symlink():
        raise ValueError('owned skill/home is linked')
    info = target.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1:
        raise ValueError('owned skill is not an owned regular file')
    new_skill = source_skill.read_bytes()
    binding = {**request, 'home': str(home), 'executable': str(executable), 'config': str(config),
               'config_sha256': sha(private(config)), 'new_skill_sha256': sha(new_skill)}
    manifest_path = (home / '.devflow-local-delivery-upgrades'
                     / request['command_id'] / 'manifest.json')
    manifest = json.loads(private(manifest_path)) if manifest_path.exists() else None
    expected = guard(codex, home, manifest_path, manifest) if manifest else None
    current, unrelated = snapshot(codex, home, expected)
    if manifest is not None:
        if manifest['command_digest'] != seal(binding):
            raise ValueError('upgrade command ID already binds different inputs')
        if (seal(manifest['before']) != request['expected_registration_sha256']
                or not registration(manifest['after'], executable, config)):
            raise ValueError('owned pointer update receipt changed')
        if (unrelated != expected
                or current not in (manifest['before'], manifest['after'])
                or sha(target.read_bytes()) not in (
                    request['expected_skill_sha256'], sha(new_skill))):
            raise ValueError('owned installation changed after its sealed update')
        if manifest['state'] == 'applied':
            if current != manifest['after'] or sha(target.read_bytes()) != sha(new_skill):
                raise ValueError('applied owned installation changed before replay')
            return {'state': 'applied', 'existing': True, 'rollback_manifest': str(manifest_path)}
    else:
        if (not registration(current, executable, old_config)
                or seal(current) != request['expected_registration_sha256']
                or sha(target.read_bytes()) != request['expected_skill_sha256']):
            raise ValueError('inspected owned registration or skill changed')
        manifest = {'command_digest': seal(binding), 'state': 'prepared', 'codex_home': str(home),
                    'before': current, 'after': None, 'unrelated_sha256': unrelated,
                    'old_config_path': str(old_config),
                    'old_config_sha256': request['expected_config_sha256'],
                    'new_config_path': str(config), 'new_config_sha256': sha(private(config)),
                    'old_skill_sha256': request['expected_skill_sha256'],
                    'new_skill_sha256': sha(new_skill)}
        after = json.loads(json.dumps(current))
        after['transport']['args'] = ['--config', str(config)]
        manifest['after'] = after
        backup = manifest_path.parent / 'previous-SKILL.md'
        if backup.exists() and private(backup) != target.read_bytes():
            raise ValueError('owned skill rollback bytes conflict')
        write(backup, target.read_bytes())
        save(manifest_path, manifest)
    try:
        if current != manifest['after']:
            guard(codex, home, manifest_path, manifest)
            install_pointer(codex, home, executable, config)
            guard(codex, home, manifest_path, manifest, pointer=manifest['after'])
        guard(codex, home, manifest_path, manifest, pointer=manifest['after'])
        write(target, new_skill)
        guard(codex, home, manifest_path, manifest, pointer=manifest['after'],
              skill_sha256=manifest['new_skill_sha256'])
        verified, unrelated_after = snapshot(codex, home, expected)
        if verified != manifest['after'] or unrelated_after != unrelated:
            raise ValueError('public update readback disagrees or unrelated host settings changed')
        manifest['state'] = 'applied'
        save(manifest_path, manifest)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        manifest.setdefault('last_error', str(exc)[:500])
        save(manifest_path, manifest)
        try:
            _rollback(codex, home, manifest_path)
        except (ValueError, OSError, subprocess.SubprocessError) as restore_error:
            manifest['state'] = 'unknown'
            manifest.setdefault('rollback_error', str(restore_error)[:500])
            save(manifest_path, manifest)
        raise
    return {'state': 'applied', 'existing': False, 'rollback_manifest': str(manifest_path)}
