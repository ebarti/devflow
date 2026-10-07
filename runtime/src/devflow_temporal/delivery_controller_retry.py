"""Authenticate preserved controller failures without approving a failed assessment."""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_resources import _ancestors, read_private


def observe(store, spec, state, attempts, broker):
    roles = [r for r in state.get('roles', []) if r.get('iteration') == state['iteration']]
    implementation = next((r for r in reversed(roles) if r.get('role') == 'implement'), {})
    matches = [a for a in attempts if a['role'] == 'implement'
               and a['iteration'] == state['iteration']
               and a['session_id'] == implementation.get('session_id')]
    if len(matches) != 1 or not implementation.get('session_id'):
        raise ValueError('controller retry requires its exact retained implementation attempt')
    attempt = matches[0]
    saved = json.loads(attempt['result_json'] or '{}')
    if (saved.get('status') != 'pass' or saved.get('cleanup') != 'confirmed'
            or saved.get('session_id') != implementation['session_id']
            or implementation.get('cleanup') != 'confirmed'):
        raise ValueError('controller retry cannot approve a failed implementation assessment')

    if (state.get('error') != 'implementer did not establish a pass'
            or implementation.get('status') != 'blocked'
            or implementation.get('findings') != ['implementer produced no candidate change']
            or saved.get('findings') or not re.fullmatch(r'[0-9a-f]{64}', attempt['job_key'])):
        raise ValueError('controller retry requires an authenticated controller-only failure')
    from .delivery_gates_admission import _native_result_bytes
    from .delivery_role_evidence import repair_payload_progress
    from .supervisor import DeliverySupervisor

    root = Path(spec['state_dir']) / 'attempts' / attempt['job_key']
    raw = _native_result_bytes(spec, attempt)
    enriched = {'cleanup', 'process_cleanup', 'resource_cleanup', 'native_process',
                'role_artifacts'}
    if canonical_json(json.loads(raw)) != canonical_json(
            {k: v for k, v in saved.items() if k not in enriched}):
        raise ValueError('controller retry original implementation receipt changed')
    _ancestors(root / 'request.json')
    request = read_private(root / 'request.json')
    journal = read_private(root / 'native-process.json')
    metadata = journal.get('provider_session', {})
    intent = journal.get('intent', {})
    candidate = broker.candidate()
    input_candidate = {k: v for k, v in candidate.items() if k != 'revision'}
    input_candidate['policy_digest'] = spec['policy_digest']
    if (request.get('spec') != spec or request.get('role') != 'implement'
            or request.get('iteration') != state['iteration']
            or request.get('workspace') != str(broker.checkout)
            or request.get('candidate') != input_candidate
            or request.get('candidate', {}).get('id') != attempt['candidate_id']
            or request.get('resume_session') != implementation['session_id']
            or DeliverySupervisor._job_key(request) != attempt['job_key']
            or intent.get('run_id') != spec['run_id']
            or intent.get('policy_digest') != spec['policy_digest']
            or intent.get('cwd') != request['workspace']
            or metadata.get('result_digest') != digest(saved)
            or metadata.get('session_id') != implementation['session_id']
            or metadata.get('resumed_from') != implementation['session_id']
            or metadata.get('role') != 'implement'
            or metadata.get('iteration') != state['iteration']
            or metadata.get('output_candidate') != {
                k: candidate[k] for k in ('id', 'head', 'content_sha256')}
            or journal.get('result') != saved.get('native_process')
            or journal.get('phase') != 'finished'
            or saved.get('role_artifacts') != implementation.get('role_artifacts')
            or not repair_payload_progress(store, request, saved, broker.checkout)):
        raise ValueError('controller retry lost its authentic same-session evidence progress')
    return {'cause': 'sealed_evidence_progress', 'attempt_job_key': attempt['job_key'],
            'assessment_digest': digest(saved), 'receipt_sha256': hashlib.sha256(raw).hexdigest(),
            'request_digest': digest(request), 'journal_digest': digest(journal),
            'role_artifacts': saved['role_artifacts']}
