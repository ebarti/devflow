"""One explicit installed-payload-only preparation generation for stopped successors."""

from __future__ import annotations

import base64
import hashlib
import json
from copy import deepcopy
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_broker import _git
from .delivery_config import DeliveryConfig
from .delivery_native_preparation import (
    SCHEMA,
    _validate,
    native_identity,
    verify_native_spec,
)
from .delivery_policy_recovery import _prepare
from .delivery_preparation import _private_bytes, run_binding
from .delivery_resources import read_private, write_private

REQUEST_FIELDS = {"preparation_authority_path", "preparation_authority_sha256"}


def _payload_only(before, after):
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise ValueError("native renewal identity is absent or invalid")
    if (canonical_json({k: v for k, v in before.items() if k != 'runtime_payload_sha256'})
            != canonical_json({k: v for k, v in after.items() if k != 'runtime_payload_sha256'})):
        raise ValueError("native renewal changed an installed non-payload identity")


def _same_execution(before, after):
    frozen, effective = deepcopy(before), deepcopy(after)
    for value in (frozen, effective):
        value.pop('preparation', None)
        value.pop('policy_digest', None)
        for key in ('native_identity', 'codex_bin_sha256', 'environment_proof_sha256',
                    'security_binding_sha256'):
            value['policy'].pop(key, None)
    if canonical_json(frozen) != canonical_json(effective):
        raise ValueError("native renewal changed frozen execution or feature authority")
    _payload_only(before['policy']['native_identity'], after['policy']['native_identity'])


def _old_proof(spec):
    identity = spec.get('policy', {}).get('native_identity')
    prepared = spec.get('preparation')
    if not isinstance(identity, dict) or not isinstance(prepared, dict):
        raise ValueError('native renewal predecessor authority is absent')
    root = Path(spec['state_dir']).parents[1]
    path = root / 'preparation-native' / digest(identity) / 'proof.json'
    if not path.is_file():
        raise ValueError("native renewal predecessor proof is absent")
    raw = _private_bytes(path, root / 'preparation-native')
    if (prepared.get('schema') != SCHEMA or prepared.get('fingerprint') != digest(identity)
            or prepared.get('environment') != {'path': str(path),
                                               'sha256': hashlib.sha256(raw).hexdigest()}
            or spec['policy'].get('environment_proof_sha256') != hashlib.sha256(raw).hexdigest()
            or prepared.get('security_binding_sha256') != run_binding(spec)
            or spec['policy'].get('security_binding_sha256') != run_binding(spec)
            or spec['policy_digest'] != digest(spec['policy'])):
        raise ValueError("native renewal predecessor proof or binding changed")
    _validate(json.loads(raw), identity, root)


def _authority(spec, payload):
    from .delivery_gates_admission import _reference

    if not REQUEST_FIELDS <= set(payload):
        raise ValueError("stopped successor requires explicit native preparation renewal authority")
    value = _reference(payload['preparation_authority_path'],
                       payload['preparation_authority_sha256'])
    runs = value.get('runs')
    if (value.get('decision_owner') != 'main task'
            or value.get('new_user_approval_required') is not False
            or not isinstance(value.get('authority_source'), str) or not value['authority_source']
            or not isinstance(runs, list) or not 1 <= len(runs) <= 2
            or any(not isinstance(run, str) or not run for run in runs)
            or len(set(runs)) != len(runs) or spec['run_id'] not in runs
            or any(type(value.get(key)) is not int or value[key] != expected
                   for key, expected in (('max_generations_per_run', 1),
                                         ('max_total_generations', 2),
                                         ('provider_turns_for_renewal', 0),
                                         ('implementation_turns_for_renewal', 0),
                                         ('new_repair_grants_for_renewal', 0)))):
        raise ValueError("native renewal authority does not bind this original run")
    _reference(value.get('trigger_path'), value.get('trigger_sha256'))
    return value


def renew(spec, payload, command_digest):
    """Effectful only during admitted successor construction, never public inspection."""
    if spec['provider'] == 'fake':
        return spec, None
    authority = _authority(spec, payload) if REQUEST_FIELDS & set(payload) else None
    identity = native_identity(spec)
    before = spec['policy'].get('native_identity')
    _payload_only(before, identity)
    if canonical_json(before) == canonical_json(identity):
        verify_native_spec(spec)
        return spec, None
    authority = authority or _authority(spec, payload)
    if (spec['policy'].get('host_sandbox') != 'trusted-local'
            or spec.get('role_home_generation') != 'policy-1'
            or type(spec.get('terminal_tracker_version')) is not int
            or spec['terminal_tracker_version'] != 1):
        raise ValueError("native renewal requires the admitted trusted installed execution")
    _old_proof(spec)
    config = DeliveryConfig.load(Path(spec['config_path']))
    if digest(config.raw) != spec['config_digest']:
        raise ValueError("native renewal frozen configuration changed")
    source = Path(__file__).resolve().parents[3]
    revision = _git(source, 'rev-parse', 'HEAD')
    if _git(source, 'status', '--porcelain', '--untracked-files=all'):
        raise ValueError("native renewal requires clean installed reviewed source")
    root = Path(spec['state_dir']) / 'native-preparation-renewal'
    from .delivery_metadata_recovery import _immutable
    from .delivery_preparation import _lock

    with _lock(root / 'controller.lock'):
        binding = {'original_spec': spec, 'before_identity': before, 'after_identity': identity,
                   'command': payload,
                   'command_digest': command_digest, 'source_revision': revision,
                   'installed_source_root': str(source),
                   'runtime_import_path': str(Path(__file__).resolve()),
                   'config_sha256': hashlib.sha256(config.path.read_bytes()).hexdigest(),
                   'authority': authority,
                   'authority_path': payload['preparation_authority_path'],
                   'authority_sha256': payload['preparation_authority_sha256']}
        _immutable(root / 'authority.json', binding)
        path = root / 'preparation.json'
        if path.exists():
            intent = read_private(path)
            if intent.get('binding_sha256') != digest(binding):
                raise ValueError("this original run already received a different native generation")
        else:
            intent = {'binding_sha256': digest(binding), 'state': 'sealed',
                      'preparation_attempts': [], 'effective_spec': None}
            write_private(path, intent)
        effective = _prepare(spec, config, intent, path)
        _same_execution(spec, effective)
        verify_native_spec(effective)
        if (canonical_json(native_identity(effective)) != canonical_json(identity)
                or _git(source, 'rev-parse', 'HEAD') != revision
                or _git(source, 'status', '--porcelain', '--untracked-files=all')
                or hashlib.sha256(config.path.read_bytes()).hexdigest()
                != binding['config_sha256']):
            raise ValueError("native renewal installed payload or revision changed")
        proof_path = Path(effective['preparation']['environment']['path'])
        proof_raw = _private_bytes(proof_path, Path(spec['state_dir']).parents[1]
                                   / 'preparation-native')
        proof = json.loads(proof_raw)
        copies = []
        for key in ('path_control', 'observed', 'log'):
            original = proof['measurement'][key]
            raw = _private_bytes(Path(original['path']), Path(spec['state_dir']).parents[1]
                                 / 'runs')
            if hashlib.sha256(raw).hexdigest() != original['sha256']:
                raise ValueError('native renewal measured evidence changed before retention')
            target = root / ('measurement-' + key + '.json')
            retained = {'original': original, 'encoding': 'base64',
                        'content': base64.b64encode(raw).decode('ascii')}
            _immutable(target, retained)
            copies.append({'path': str(target),
                           'sha256': hashlib.sha256(target.read_bytes()).hexdigest(),
                           'original_path': original['path'],
                           'original_sha256': original['sha256']})
        _immutable(root / 'proof.json', proof, raw=proof_raw)
        receipt = {**binding, 'effective_spec': effective,
                   'retained_proof': {'path': str(root / 'proof.json'),
                                      'sha256': hashlib.sha256(proof_raw).hexdigest()},
                   'measurement_copies': copies,
                   'preparation_history': intent['preparation_attempts']}
        _immutable(root / 'generation.json', receipt)
        reference = {'path': str(root / 'generation.json'),
                     'sha256': hashlib.sha256((root / 'generation.json').read_bytes()).hexdigest()}
        return effective, reference


def effective_spec(original, recovery):
    """Read immutable lineage without treating a historical proof as current execution."""
    reference = recovery.get('native_preparation_renewal')
    if not reference:
        return original
    path = Path(original['state_dir']) / 'native-preparation-renewal/generation.json'
    if reference.get('path') != str(path):
        raise ValueError("native renewal generation left its exact owned path")
    raw = path.read_bytes()
    receipt = read_private(path)
    if (hashlib.sha256(raw).hexdigest() != reference.get('sha256')
            or canonical_json(receipt['original_spec']) != canonical_json(original)
            or canonical_json(receipt['effective_spec'])
            != canonical_json(recovery.get('execution_spec'))):
        raise ValueError("native renewal immutable generation changed")
    _same_execution(original, receipt['effective_spec'])
    retained = receipt['retained_proof']
    if (retained['path'] != str(path.with_name('proof.json'))
            or hashlib.sha256(Path(retained['path']).read_bytes()).hexdigest()
            != retained['sha256']
            or retained['sha256']
            != receipt['effective_spec']['preparation']['environment']['sha256']):
        raise ValueError('native renewal retained proof changed')
    read_private(Path(retained['path']))
    for item in receipt['measurement_copies']:
        copied = Path(item['path'])
        if (copied.parent != path.parent or copied.name not in {
                'measurement-path_control.json', 'measurement-observed.json',
                'measurement-log.json'}
                or hashlib.sha256(copied.read_bytes()).hexdigest() != item['sha256']):
            raise ValueError('native renewal retained measurement changed')
        read_private(copied)
    return receipt['effective_spec']


def verify_generation(original, reference, effective):
    value = effective_spec(original, {'native_preparation_renewal': reference,
                                      'execution_spec': effective})
    path = Path(reference['path'])
    receipt = read_private(path)
    _authority(original, receipt['command'])
    _old_proof(original)
    if (digest(DeliveryConfig.load(Path(original['config_path'])).raw) != original['config_digest']
            or hashlib.sha256(Path(original['config_path']).read_bytes()).hexdigest()
            != receipt['config_sha256']
            or receipt['installed_source_root'] != str(Path(__file__).resolve().parents[3])
            or receipt['runtime_import_path'] != str(Path(__file__).resolve())
            or _git(Path(__file__).resolve().parents[3], 'rev-parse', 'HEAD')
            != receipt['source_revision']
            or _git(Path(__file__).resolve().parents[3], 'status', '--porcelain',
                    '--untracked-files=all')):
        raise ValueError("native renewal installed source or configuration changed")
    verify_native_spec(value)
    return value
