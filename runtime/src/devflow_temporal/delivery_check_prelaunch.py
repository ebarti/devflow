"""Observe a stopped planned-check registration failure without changing its history."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path

from .delivery_native_process import listeners, process_table
from .delivery_plan_checks import planned_checks
from .delivery_resources import RunResources, _ancestors, _gate_evidence_root, read_private


def observe(spec, state, previous):
    local = state.get('checks', {}).get('local')
    if (local != {'state': 'unknown', 'cleanup': 'unknown', 'reason': 'ValueError',
                  'candidate_id': state['candidate']['id']}
            or state.get('roles') != previous['state'].get('roles')):
        raise ValueError('check prelaunch recovery requires an unassessed registration failure')
    resources = RunResources(spec)
    manifest = read_private(resources.manifest)
    table = process_table()
    journals = {}
    for raw in manifest['processes']:
        path = Path(raw)
        if not path.is_relative_to(Path(spec['state_dir'])):
            raise ValueError('check prelaunch journal escaped its owned run')
        journal = read_private(path)
        if (journal.get('phase') != 'finished' or not journal.get('monitoring_complete')
                or journal.get('result', {}).get('cleanup') != 'observed-native-confirmed'
                or any(table.get(int(pid), {}).get('identity') == item['identity']
                       and not table[int(pid)]['stat'].startswith('Z')
                       for pid, item in journal.get('owned', {}).items())
                or any(listeners(port) for port in journal.get('ports', []))):
            raise ValueError('check prelaunch has a live or unconfirmed native process')
        journals[raw] = hashlib.sha256(path.read_bytes()).hexdigest()
    evidence = _gate_evidence_root(spec) / 'checks' / str(state['iteration'])
    gate = _gate_evidence_root(spec) / 'gates' / str(state['iteration']) / 'verify'
    checks = planned_checks(spec, gate, evidence)
    dependencies = [c for c in checks if c['id'].startswith('planned-python-dependencies-')]
    if len(dependencies) != 1:
        raise ValueError('check prelaunch requires one exact planned Python environment')
    dependency = dependencies[0]
    profile = (Path(spec['state_dir']) / 'transient/checks'
               / evidence.relative_to(Path(spec['state_dir'])) / dependency['id']
               / 'codex/config.toml')
    generated = gate / dependency['generated_directories'][0]
    _ancestors(profile)
    if (os.path.lexists(evidence / dependency['id'] / 'native') or os.path.lexists(generated)
            or any(Path(raw).is_relative_to(evidence / dependency['id']) for raw in journals)
            or str(generated) in manifest['roots'] or profile.is_symlink()
            or not profile.is_file()):
        raise ValueError('planned Python check was launched or acquired generated data')
    for check in spec['policy']['checks']:
        path = evidence / check['id'] / 'native/native-process.json'
        if str(path) not in journals or read_private(path)['result']['exit_code'] != 0:
            raise ValueError('configured checks lack their preceding completed native receipts')
    return {'state': 'observed-quiescent-prelaunch', 'historical_cleanup': state['cleanup'],
            'manifest_sha256': hashlib.sha256(resources.manifest.read_bytes()).hexdigest(),
            'profile_sha256': hashlib.sha256(profile.read_bytes()).hexdigest(),
            'journal_sha256': journals, 'planned_check': dependency}
