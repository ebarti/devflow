"""Authenticate a finished controller prerequisite without a provider verdict."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from .contracts import digest
from .delivery_configured_resources import implementation_prerequisites
from .delivery_runtime_location import private_bytes


def completed_prerequisite(seal, previous, role):
    """A configured native prerequisite failed before the admitted provider turn.

    The caller still proves unchanged source, attempt/effect/session inventory,
    current work ownership and native actor cleanup. This joins the separately
    completed controller command to its closed role, never completes a provider.
    """
    spec, candidate = seal['predecessor_spec'], seal['candidate']
    if (spec.get('feature_worker') or spec.get('provider') != 'codex'
            or spec.get('policy', {}).get('host_sandbox') != 'trusted-local'):
        return None
    preparation = role.get('implementation_preparation') or {}
    results = preparation.get('results')
    if (preparation.get('candidate_id') != candidate['id']
            or not isinstance(results, list) or not 1 <= len(results) <= 64
            or any(not isinstance(item, dict) for item in results)
            or any(item.get('passed') is not True for item in results[:-1])
            or results[-1].get('passed') is not False):
        return None
    configured = [recipe for recipe in implementation_prerequisites(spec)
                  if recipe['id'] == results[-1].get('id')]
    if (len(configured) != 1 or configured[0].get('kind') == 'test'
            or configured[0]['argv'] != results[-1].get('argv')
            or str(Path(spec['checkout']) / configured[0].get('cwd', '.'))
                != results[-1].get('cwd')):
        return None
    from .delivery_stopped_resume import namespace

    root = (Path(spec['state_dir']) / namespace(previous) / 'evidence'
            / 'implementation-preparation' / str(role['iteration']))
    files, identifiers = {}, set()
    for item in results:
        identifier = item.get('id')
        cwd, argv = item.get('cwd'), item.get('argv')
        native = item.get('native_process') or {}
        if (not isinstance(identifier, str) or not identifier or identifier in {'.', '..'}
                or Path(identifier).name != identifier or identifier in identifiers
                or not isinstance(cwd, str) or not Path(cwd).is_relative_to(spec['checkout'])
                or Path(cwd).resolve(strict=True) != Path(cwd)
                or not isinstance(argv, list) or not argv
                or any(not isinstance(arg, str) or not arg for arg in argv)
                or type(item.get('exit_code')) is not int
                or (item['exit_code'] != 0 if item['passed'] else item['exit_code'] <= 0)
                or item.get('failure_kind') is not None or item.get('launched') is False
                or item.get('cleanup') != 'confirmed'
                or item.get('process_cleanup') != 'observed-native-confirmed'
                or item.get('rejected_output') is not False or item.get('rejection_causes') != []
                or item.get('evidence_failure') is not None
                or item.get('interrupted_native_processes')
                or not isinstance(native, dict) or native.get('state') != 'finished'
                or type(native.get('exit_code')) is not int
                or native.get('exit_code') != item['exit_code']
                or native.get('cleanup') != 'observed-native-confirmed'
                or native.get('monitoring_complete') is not True
                or native.get('stdio_drained') is not True
                or any(native.get(flag) is not False
                       for flag in ('cancelled', 'timed_out', 'listener_conflict'))
                or native.get('interrupted_monitor', False) is not False):
            raise ValueError('implementation prerequisite has no completed native receipt')
        identifiers.add(identifier)
        folder = root / identifier / 'native'
        journal_path, launch_path, log_path = (
            folder / name for name in ('native-process.json', 'launch.json', 'process.log'))
        if (native.get('journal') != str(journal_path)
                or native.get('log') != item.get('log') or item.get('log') != str(log_path)):
            raise ValueError('implementation prerequisite left its exact admission namespace')
        raw = {path: private_bytes(path) for path in (journal_path, launch_path, log_path)}
        journal, launch = json.loads(raw[journal_path]), json.loads(raw[launch_path])
        dependencies = item.get('dependency_preparation') or {}
        if not isinstance(dependencies, dict) or not isinstance(journal, dict) or not isinstance(
                launch, dict):
            raise ValueError('implementation prerequisite native evidence is malformed')
        command = [(dependencies.get('store')
                    if arg == '/store' else arg) for arg in argv]
        intent = journal.get('intent') or {}
        runtime = native.get('runtime_identity') or {}
        log_digest = hashlib.sha256(raw[log_path]).hexdigest()
        if (any(not isinstance(arg, str) or not arg for arg in command)
                or not isinstance(intent, dict) or not isinstance(runtime, dict)
                or journal.get('phase') != 'finished'
                or journal.get('monitoring_complete') is not True
                or journal.get('result') != native
                or journal.get('runtime_identity') != runtime or not runtime
                or runtime.get('dirty') is not False
                or runtime.get('runtime_payload_sha256')
                    != (spec['policy'].get('native_identity') or {}).get('runtime_payload_sha256')
                or intent.get('run_id') != spec['run_id']
                or intent.get('policy_digest') != spec['policy_digest']
                or intent.get('cwd') != cwd or intent.get('argv') != command
                or type(intent.get('timeout')) is not int or intent['timeout'] <= 0
                or (identifier == configured[0]['id']
                    and intent['timeout'] != configured[0].get('timeout_seconds', 600))
                or intent.get('ports') != [] or journal.get('ports') != []
                or launch.get('argv') != command
                or not isinstance(launch.get('environment'), dict)
                or intent.get('environment_sha256') != digest(launch['environment'])
                or item.get('log_sha256') != log_digest
                or native.get('log_sha256') != log_digest):
            raise ValueError('implementation prerequisite native evidence changed')
        files.update({str(path): hashlib.sha256(value).hexdigest()
                      for path, value in raw.items()})
    # Each bounded read pins its FD/name generation. Recheck the whole collection
    # after validation, so a mixed journal/launch/log observation cannot be sealed.
    if any(hashlib.sha256(private_bytes(path)).hexdigest() != expected
           for path, expected in files.items()):
        raise ValueError('implementation prerequisite evidence changed during observation')
    return {'kind': 'completed_native_implementation_prerequisite',
            'iteration': role['iteration'], 'closed_role_sha256': digest(role),
            'execution_spec_sha256': digest(spec),
            'predecessor_command_digest': previous['command_digest'], 'files': files}
