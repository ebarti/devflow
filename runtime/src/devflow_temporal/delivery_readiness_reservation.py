"""Authenticate readiness attached to a private native feature-role request.

This observation is not a launch grant or a complete reservation verifier.
The launch owner must independently authenticate the selected activity grant,
Temporal execution, reservation generation, and native terminal evidence.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .delivery_readiness import ReadinessError, verify_receipt
from .delivery_resources import read_private
from .supervisor import DeliverySupervisor


def verify_reserved_readiness(store, spec, binding, *, pinned_request, request_sha256):
    """Revalidate a pinned private request and its immutable readiness receipt.

    The launch owner retains and rechecks the pinned descriptor and pathname at
    GO. This function never claims that the request or grant remains pinned.
    """
    job = binding.get('job_id') if isinstance(binding, dict) else None
    if (not isinstance(job, str) or not re.fullmatch(r'[0-9a-f]{64}', job)
            or spec.get('readiness_version') != 1 or not spec.get('feature_worker')
            or type(binding.get('iteration')) is not int or binding['iteration'] < 1
            or not isinstance(binding.get('owner'), dict)
            or not isinstance(binding.get('assignment'), dict)):
        raise ReadinessError('repair readiness lacks a bounded feature job binding')
    if store.effective_spec(spec['run_id']) != spec:
        raise ReadinessError('repair readiness execution authority changed')
    folder = Path(spec['state_dir']) / 'attempts' / job
    path = folder / 'request.json'
    if (not isinstance(pinned_request, bytes) or len(pinned_request) > 8 * 1024 * 1024
            or not isinstance(request_sha256, str)
            or not re.fullmatch(r'[0-9a-f]{64}', request_sha256)
            or hashlib.sha256(pinned_request).hexdigest() != request_sha256
            or read_private(path, raw=True) != pinned_request):
        raise ReadinessError('repair readiness pinned request changed')
    request = json.loads(pinned_request)
    if (not isinstance(request, dict) or request.get('spec') != spec
            or request.get('native_authorized') is not True
            or request.get('start_path') != str(folder / 'start.json')
            or request.get('result_path') != str(folder / 'result.json')
            or request.get('role') != 'implement' or DeliverySupervisor._job_key(request) != job
            or request.get('candidate') != binding.get('candidate')
            or type(binding.get('native_attempt_generation')) is not int
            or binding['native_attempt_generation'] != request.get('attempt_generation', 0)
            or type(binding.get('attempt_generation')) is not int
            or binding['attempt_generation'] != request.get('readiness_attempt', 0)
            or request.get('iteration') != binding.get('iteration')
            or spec.get('run_id') != binding.get('worker_run_id')):
        raise ReadinessError('repair readiness private role request changed')
    assignment = request.get('readiness_worker_assignment')
    expected = {'owner': binding.get('owner'), 'assignment': binding.get('assignment'),
                'workflow_id': binding.get('workflow_id'),
                'workflow_run_id': binding.get('workflow_run_id'),
                'repair_key': f"{spec['run_id']}:{binding['iteration']}"}
    if (assignment != expected or not isinstance(expected['workflow_id'], str)
            or not expected['workflow_id'] or not isinstance(expected['workflow_run_id'], str)
            or not expected['workflow_run_id']):
        raise ReadinessError('repair readiness worker handoff changed')
    receipt = (request.get('evidence_context') or {}).get('readiness')
    if (not isinstance(receipt, dict) or receipt.get('stage') != 'plan'
            or receipt.get('iteration') != binding['iteration']
            or receipt.get('attempt') != binding['attempt_generation']
            or receipt.get('receipt_sha256') != binding.get('readiness_digest')):
        raise ReadinessError('repair readiness receipt differs from the reservation')
    verify_receipt(store, spec, binding['candidate'], receipt)
    return {'readiness_digest': receipt['receipt_sha256'], 'request_sha256': request_sha256}
