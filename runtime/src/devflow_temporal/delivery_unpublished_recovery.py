"""Exact stopped-source checkpoints; a later teardown never supplies a verdict."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .contracts import digest
from .delivery_resources import _ancestors, read_private


def quarantined_implementation(spec, state, attempts, candidate):
    """Bind a lost provider's partial output for a new, separately charged turn.

    The closed role and later supervisor result are deliberately separate: the
    former may have returned unknown before independent teardown finished.
    """
    roles = [role for role in state.get('roles', []) if role.get('role') == 'implement']
    latest = roles[-1] if roles else None
    if not latest or latest.get('status') != 'recovery_unknown':
        return None
    from .delivery_native_process import interrupted_cleanup_proven
    from .supervisor import DeliverySupervisor

    matches = [a for a in attempts if a['role'] == 'implement'
               and a['iteration'] == latest.get('iteration')]
    if (spec.get('provider') != 'codex' or spec.get('feature_worker')
            or state.get('pull_request') is not None
            or state.get('error') != 'implementer did not establish a pass'
            or latest.get('iteration') != state['iteration'] or len(matches) != 1
            or latest.get('finish_reason') != 'recovery_unknown'
            or latest.get('session_id') is not None
            or latest.get('cleanup') != 'unknown' or latest.get('candidate') != candidate
            or any(role.get('session_id') for role in roles)):
        raise ValueError('interrupted implementation lacks its exact quarantined checkpoint')
    attempt = matches[0]
    folder = Path(spec['state_dir']) / 'attempts' / attempt['job_key']
    path = folder / 'native-process.json'
    _ancestors(path)
    request_raw, journal_raw = (read_private(item, raw=True)
                                for item in (folder / 'request.json', path))
    request, journal = json.loads(request_raw), json.loads(journal_raw)
    if not isinstance(request, dict) or not isinstance(journal, dict):
        raise ValueError('interrupted implementation evidence is malformed')
    result = json.loads(attempt['result_json'] or 'null')
    metadata, native = journal.get('provider_session') or {}, journal.get('result') or {}
    owned = journal.get('owned')
    if (not isinstance(metadata, dict) or not isinstance(native, dict)
            or not isinstance(owned, dict)
            or not isinstance(owned.get(str(attempt.get('pid'))), dict)):
        raise ValueError('interrupted implementation native identity is malformed')
    if (attempt['state'] != 'finished' or attempt['cleanup'] != 'confirmed'
            or attempt.get('session_id') is not None or not isinstance(result, dict)
            or result.get('status') != 'recovery_unknown'
            or result.get('finish_reason') != 'interrupted_monitor'
            or result.get('session_id') is not None or result.get('cleanup') != 'confirmed'
            or result.get('process_cleanup') != 'observed-native-confirmed'
            or result.get('native_process') != native
            or not interrupted_cleanup_proven(journal)
            or journal['cleanup_reconciliation'].get('version') != 1
            or native.get('cleanup_reconciliation') != journal['cleanup_reconciliation']
            or journal.get('monitoring_complete') is not False
            or native.get('provider_outcome') != 'unknown'
            or native.get('monitoring_complete') is not False
            or native.get('journal') != str(path)
            or os.path.lexists(folder / 'result.json')
            or request.get('spec') != spec or request.get('workspace') != spec['checkout']
            or request.get('role') != 'implement'
            or request.get('iteration') != state['iteration']
            or request.get('resume_session') is not None
            or request.get('candidate') != state.get('candidate')
            or latest.get('input_candidate_id') != attempt['candidate_id']
            or request['candidate']['id'] != attempt['candidate_id']
            or DeliverySupervisor._job_key(request) != attempt['job_key']
            or request.get('result_path') != str(folder / 'result.json')
            or attempt.get('result_path') != request['result_path']
            or request.get('start_path') != str(folder / 'start.json')
            or metadata != {
                'role': 'implement', 'iteration': state['iteration'], 'session_id': None,
                'resumed_from': None, 'output_candidate': {
                    key: candidate[key] for key in ('id', 'head', 'content_sha256')},
                'result_digest': digest(result)}
            or str(attempt.get('pid')) not in journal.get('owned', {})
            or journal['owned'][str(attempt['pid'])]['identity'] != attempt['process_identity']):
        raise ValueError('interrupted implementation supervisor/output binding changed')
    return {'job_key': attempt['job_key'], 'iteration': state['iteration'],
            'closed_role_sha256': digest(latest), 'result_sha256': digest(result),
            'request_sha256': hashlib.sha256(request_raw).hexdigest(),
            'journal_sha256': {str(path): hashlib.sha256(journal_raw).hexdigest()},
            'input_candidate': request['candidate'], 'output_candidate': candidate,
            'provider_outcome': 'unknown', 'monitoring_complete': False}


def interrupted_prepublication(seal):
    """Only an exact passed implementation may rerun interrupted prechecks for free."""
    state, spec = seal['state'], seal['predecessor_spec']
    check = state.get('checks', {}).get('prepublish') or {}
    closure = seal.get('cleanup_reconciliation') or {}
    return (not spec.get('feature_worker') and not state.get('pull_request')
            and closure.get('closure') == 'unpublished'
            and state.get('error') == 'prepublication process cleanup is unknown'
            and check.get('state') == check.get('cleanup') == 'unknown'
            and check.get('candidate_id') == seal['candidate']['id'])


def cleanup_checkpoint(store, row, spec, closed):
    """Post-intake custody is required before an unpublished claim can be released."""
    from .delivery_broker import DeliveryBroker
    from .delivery_policy_recovery import _rows, work_binding
    from .delivery_stopped_resume import _implementation_receipt

    state = closed['result']
    current, attempts, effects, _claim = _rows(store, spec['run_id'])
    plan = spec.get('accepted_plan')
    roles = [role for role in state.get('roles', []) if role.get('role') == 'implement']
    if (current != row or not isinstance(plan, str) or not plan.strip()
            or (row['accepted_plan_text'] if row['accepted_plan_text'] is not None else
                json.loads(row['request_json']).get('accepted_plan')) != plan or not roles
            or state.get('pull_request') is not None
            or any(effect['kind'] == 'publish' or effect['state'] != 'complete'
                       or effect['observed_json'] is None for effect in effects)):
        raise ValueError('unpublished cleanup requires accepted post-intake custody')
    latest = roles[-1]
    matches = [item for item in attempts if item['role'] == 'implement'
               and item['iteration'] == latest.get('iteration')]
    candidate = DeliveryBroker(store, spec).candidate()
    quarantine = quarantined_implementation(spec, state, attempts, candidate)
    if not quarantine and (len(matches) != 1
            or matches[0]['state'] != 'finished' or matches[0]['cleanup'] != 'confirmed'
            or latest.get('cleanup') != 'confirmed'
            or not _implementation_receipt(spec, latest, matches[0])
            or latest.get('candidate') != candidate or state.get('candidate') != candidate):
        raise ValueError('unpublished implementation/source custody changed')
    from .delivery_automatic_retry import unavailable

    with store._connect() as db:
        binding = work_binding(store, spec, db)
        if unavailable(store, db, spec, own_claim_allowed=True):
            raise ValueError('unpublished cleanup lost its current issue or own claim')
    return {'work_binding': binding, 'candidate': candidate,
            'attempts_sha256': digest(attempts), 'effects_sha256': digest(effects),
            **({'quarantined_implementation': quarantine} if quarantine else {})}


def quarantine_history(previous):
    """Keep exact old teardown receipts across later normal verdicts and candidates."""
    from .delivery_feature_revision_recovery import retained_recovery

    found = {}
    while previous:
        details = retained_recovery(previous)
        proof = details.get('quarantined_implementation')
        if proof:
            if (details.get('kind') != 'stopped_delivery_resume'
                    or quarantined_implementation(details['predecessor_spec'], details['state'],
                        details['attempts'], details['candidate']) != proof):
                raise ValueError('historical quarantined implementation changed')
            key = proof['closed_role_sha256']
            if key in found and found[key] != proof:
                raise ValueError('historical quarantine has conflicting custody')
            found[key] = proof
        previous = details.get('original_recovery')
    return [found[key] for key in sorted(found)]


def teardown_checkpoint(proofs):
    journals = {}
    for proof in proofs:
        for path, sha in proof['journal_sha256'].items():
            if path in journals and journals[path] != sha:
                raise ValueError('quarantined journal has conflicting generations')
            journals[path] = sha
    return {'journal_sha256': journals} if journals else None
