"""Observe a browser registration stop without adopting its existing output."""
from __future__ import annotations

import hashlib
import json
import os
import stat
from pathlib import Path

from .contracts import digest
from .delivery_browser_qa import _artifacts
from .delivery_native_process import listeners
from .delivery_resources import (
    RunResources,
    _ancestors,
    _gate_evidence_root,
    observe_finalized_resources,
    read_private,
)


def observe(spec, state, effects, *, resource_spec=None, evidence_root=None):
    """Authenticate an unlaunched browser effect after passed checks and review."""
    candidate = state['candidate']
    checks = state.get('checks', {})
    qa = spec['policy'].get('browser_qa')
    browser = checks.get('browser_qa')
    local = checks.get('local', {})
    review = checks.get('review', {})
    if (not qa or spec['policy'].get('host_sandbox') != 'trusted-local'
            or state.get('error') != 'browser QA child cleanup is unknown'
            or browser != {'state': 'unknown', 'cleanup': 'unknown', 'reason': 'ValueError',
                           'candidate_id': candidate['id']}
            or local.get('state') != 'passed' or local.get('source_unchanged') is not True
            or local.get('candidate_id') != candidate['id'] or not local.get('results')
            or any(r.get('passed') is not True for r in local['results'])
            or review.get('state') != 'passed' or review.get('candidate_id') != candidate['id']
            or not any(r.get('role') == 'review' and r.get('iteration') == state['iteration']
                       and r.get('status') == 'pass' and r.get('cleanup') == 'confirmed'
                       and r.get('candidate', {}).get('id') == candidate['id']
                       and not r.get('findings') for r in state.get('roles', []))):
        raise ValueError('browser prelaunch requires unchanged passed local checks and review')
    root = evidence_root or _gate_evidence_root(spec)
    folder = root / 'browser-qa' / str(state['iteration'])
    profile = folder / 'browser-qa.sb'
    _ancestors(profile)
    info = profile.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1
            or profile.read_bytes() != b'(version 1)\n(allow default)\n'
            or set(folder.iterdir()) != {profile}):
        raise ValueError('browser prelaunch profile changed or browser execution is possible')
    namespace = '' if root == Path(spec['state_dir']) else ':' + str(
        root.relative_to(Path(spec['state_dir'])).parent)
    key = f"browser_qa:{spec['run_id']}:{state['iteration']}" + namespace
    expected = {'iteration': state['iteration'], 'candidate_id': candidate['id'],
                'policy_digest': spec['policy_digest'], 'qa_config_sha256': digest(qa),
                'ports': qa['ports'], 'argv': qa['argv']}
    pending = [e for e in effects if e['state'] != 'complete' or not e['observed_json']]
    if (len(pending) != 1 or pending[0]['effect_key'] != key
            or pending[0]['run_id'] != spec['run_id'] or pending[0]['kind'] != 'browser_qa'
            or pending[0]['state'] != 'pending' or pending[0]['observed_json'] is not None
            or json.loads(pending[0]['request_json']) != expected):
        raise ValueError('browser prelaunch lost its sole unlaunched effect')
    resources = RunResources(resource_spec or spec, read_only=True)
    manifest = read_private(resources.manifest)
    if (any(Path(raw).is_relative_to(folder) for raw in manifest['processes'])
            or any(listeners(port) for port in qa['ports'].values())):
        raise ValueError('browser prelaunch has a registered launch or live fixture port')
    gate = root / 'gates' / str(state['iteration']) / 'verify'
    outputs = [gate / name for name in qa.get('artifact_paths', [])]
    unregistered = [path for path in outputs if os.path.lexists(path)
                    and str(path) not in manifest['roots']]
    if not unregistered:
        raise ValueError('browser prelaunch lacks the unregistered output failure')
    for path in unregistered:
        _ancestors(path)
        if path.is_symlink() or not path.is_dir() or path.resolve() != path:
            raise ValueError('browser prelaunch output left its owned gate')
    artifacts = _artifacts(gate, qa.get('artifact_paths', []))
    observed = observe_finalized_resources(resource_spec or spec, unknown_allowed=True)
    return {'state': 'observed-quiescent-browser-prelaunch',
            'historical_cleanup': state['cleanup'], 'evidence_root': str(root),
            'profile_sha256': hashlib.sha256(profile.read_bytes()).hexdigest(),
            'artifacts': artifacts, 'resources': observed, 'pending_effect': pending[0]}


def resolved_effect(observation, candidate):
    return {'state': 'failed', 'cleanup': 'confirmed', 'launched': False,
            'failure_kind': 'preparation', 'candidate_id': candidate['id'],
            'historical_cleanup': 'unknown',
            'prelaunch_observation_sha256': digest(observation)}
