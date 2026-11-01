"""Typed recovery checkpoint comparisons; observations never grant execution.

Owner adapters must authenticate their own custody before constructing facts.
The evaluator is pure and can be called again when the owner holds its lock.
"""
from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Mapping
from functools import partial
from typing import Any

VERSION = 1

# These are observations, not ownership or execution grants.  Pause is reserved
# for the pause owner; an unknown state never establishes safe custody.
LIFECYCLE_STATES = frozenset({
    'pending', 'running', 'stopping', 'stopped', 'recovering', 'terminal', 'unknown',
})
PAUSE_EXTENSION_STATES = frozenset({'pausing', 'paused', 'resuming'})

# Route identities are explicit.  Each owner authenticates the full source
# checkpoint and rechecks it when acquiring admission custody.  The table does
# not infer a route from an error or authorize a transition by itself.
TRANSITION_TABLE = {
    kind: {'from': ('stopped',), 'to': 'recovering'} for kind in (
        'repair_continuation', 'stopped_delivery_resume', 'published_gate_retry',
        'prepublication_gate_retry', 'published_check_prelaunch_retry',
        'published_ci_retry', 'published_controller_retry', 'recover_publication',
        'retry_prelaunch', 'stopped_resource_closure', 'abandon_pending_resource_closure',
        'published_head_validation', 'investigation_gates_only', 'amend_scope',
    )
}
# Cancelled feature workers and revision-owned completed workers can be resumed
# only when their owner supplies the additional historical authority.
TRANSITION_TABLE['stopped_delivery_resume']['from'] = ('stopped', 'terminal')


def lifecycle_from_row(row: Mapping[str, Any]) -> str:
    """Describe a persisted projection without treating it as a custody proof."""
    phase, execution, outcome = (row.get(key) for key in ('phase', 'execution_state', 'outcome'))
    if execution == 'blocked' and phase == outcome == 'blocked':
        return 'stopped'
    if execution == 'terminal' and outcome in {'delivered', 'cancelled', 'failed'}:
        return 'terminal'
    if execution in {'stopping', 'cancelling'}:
        return 'stopping'
    if execution in {'queued', 'pending', 'running'}:
        if row.get('recovery_json'):
            return 'recovering'
        return 'running' if execution == 'running' else 'pending'
    return 'unknown'

# Public command names only; this table never selects a recovery route or grants it.
_NEXT_ACTIONS = {
    'repair_continuation': 'continue-repair',
    'stopped_delivery_resume': 'continue-repair',
    'published_gate_retry': 'continue-repair',
    'prepublication_gate_retry': 'continue-repair',
    'published_check_prelaunch_retry': 'continue-repair',
    'published_ci_retry': 'continue-repair',
    'published_controller_retry': 'continue-repair',
    'stopped_resource_closure': 'continue-repair',
    'abandon_pending_resource_closure': 'continue-repair',
    'published_head_validation': 'continue-repair',
    'investigation_gates_only': 'admit-gates-only',
    'merge_run': 'merge',
    'ordinary_ci_execution_inventory': 'ack-ci-inventory',
    'native_check_diagnostic': 'ack-native-diagnostic',
    'recover_publication': 'recover-publication',
    'retry_prelaunch': 'retry-prelaunch',
    'amend_scope': 'amend-scope',
    'continue_feature': 'continue-feature',
    'revise_feature_plan': 'revise-feature-plan',
    'terminal_tracker_recovery': 'reconcile-tracker',
    'adopt_filesystem_custody': 'adopt-filesystem-custody',
    'adopt_runtime_location': 'adopt-runtime-location',
}


class RecoveryRefused(ValueError):
    """A refusal carrying the machine-readable predicates that failed."""

    def __init__(self, assessment: dict[str, Any]):
        self.assessment = assessment
        super().__init__('recovery transition refused: ' + ', '.join(
            item['code'] for item in assessment['blockers']))


def evaluate_transition(facts: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    """Compare exact, authenticated owner facts with a requested checkpoint.

    Missing facts are unknown, never a successful proof. Owner validation is a
    separate predicate, so matching a revision or candidate is not authorization.
    """
    operation = facts.get('operation', 'unknown')
    owner = facts.get('owner', 'unknown')
    blockers = []

    def block(code, fact, action, authorization=False, satisfied=None):
        blockers.append({'code': code, 'owner': owner, 'missing_or_unknown_fact': fact,
                         'next_action': action, 'new_authorization_required': authorization,
                         'satisfied': satisfied, 'user_input_required': None})

    if facts.get('owner_proof') is not True:
        block('owner_proof_unknown', 'authenticated owner and custody proof',
              'inspect the owning recovery preflight',
              satisfied=False if facts.get('owner_proof') is False else None)
    elif not facts.get('proof_digest'):
        block('proof_digest_unknown', 'authenticated proof binding',
              'obtain a fresh authenticated checkpoint')
    comparisons = (
        ('revision', 'expected_revision', 'revision_changed'),
        ('iteration', 'expected_iteration', 'iteration_changed'),
        ('candidate_id', 'expected_candidate_id', 'candidate_changed'),
        ('candidate_head', facts.get('head_request_field', 'expected_candidate_head'),
         'candidate_head_changed'),
    )
    if facts.get('publication_required'):
        comparisons += (('publication_number', 'expected_pr_number', 'publication_changed'),)
    if 'comparisons' in facts:
        comparisons = tuple(facts['comparisons'])
    for observed_key, requested_key, code in comparisons:
        observed = facts.get(observed_key)
        requested = request.get(requested_key)
        if observed is None or requested is None:
            block(code.replace('_changed', '_unknown'), observed_key,
                  'obtain a fresh authenticated checkpoint')
        elif observed != requested:
            block(code, observed_key, 'obtain a fresh authenticated checkpoint', satisfied=False)
    for predicate in facts.get('predicates', ()):
        if predicate.get('satisfied') is not True:
            blockers.append({
                'code': predicate['code'], 'owner': predicate.get('owner', owner),
                'missing_or_unknown_fact': predicate['missing_or_unknown_fact'],
                'next_action': predicate['next_action'],
                'new_authorization_required': predicate.get('new_authorization_required', False),
                'satisfied': predicate.get('satisfied'),
                'user_input_required': predicate.get('user_input_required'),
            })
    transition = TRANSITION_TABLE.get(operation)
    observed = {key: facts.get(key) for key in (
        'revision', 'iteration', 'candidate_id', 'candidate_head', 'publication_number')}
    return {'version': VERSION, 'operation': operation,
            'normalized_lifecycle': facts.get('normalized_lifecycle', 'unknown'),
            'transition': ({'from': list(transition['from']), 'to': transition['to']}
                           if transition is not None else None),
            'eligible': not blockers, 'observed_revision': observed['revision'],
            'candidate': {'id': observed['candidate_id'], 'head': observed['candidate_head']},
            'publication': ({'number': observed['publication_number']}
                            if facts.get('publication_required') else None),
            'proof_digest': facts.get('proof_digest') if facts.get('owner_proof') is True else None,
            'supported_next_actions': (sorted({item['next_action'] for item in blockers})
                                       if blockers else
                                       [_NEXT_ACTIONS[operation]] if operation in _NEXT_ACTIONS
                                       else []),
            'blockers': blockers}


def require_transition(facts: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    assessment = evaluate_transition(facts, request)
    if not assessment['eligible']:
        raise RecoveryRefused(assessment)
    return assessment


def require_observed_predicates(operation: str, owner: str, proof: Any,
                                predicates: list[dict[str, Any]], *,
                                lifecycle: str = 'unknown', owner_proof: bool | None = True,
                                legacy_message: str | None = None
                                ) -> dict[str, Any]:
    """Refuse exact observed predicates in both preflight and owner admission.

    This helper never substitutes a digest for custody: callers must establish
    the authenticated observation and all owner-specific predicates themselves.
    """
    from .contracts import digest

    try:
        return require_transition({
            'operation': operation, 'owner': owner, 'owner_proof': owner_proof,
            'normalized_lifecycle': lifecycle, 'proof_digest': digest(proof),
            'comparisons': [], 'predicates': predicates,
        }, {})
    except RecoveryRefused as exc:
        if legacy_message is not None:
            exc.args = (legacy_message,)
        raise


def _event_backed_receipt(db, operation, run_id, request, response):
    """Bind a gate/publication command to its retained admission and event."""
    from .contracts import canonical_json

    pending = operation == 'recover_publication'
    expected_kind = 'pending_publication_retry' if pending else operation
    saved = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                       (run_id,)).fetchone()
    roots = [saved[0]] if saved and saved[0] else []
    if not pending:
        gate = db.execute('SELECT recovery_json FROM delivery_gate_admissions WHERE run_id=?',
                          (run_id,)).fetchone()
        if gate and gate[0]:
            roots.append(gate[0])
    revisions = set()
    try:
        for raw in roots:
            recovery = json.loads(raw)
            while isinstance(recovery, dict):
                if (recovery.get('kind') == expected_kind
                        and canonical_json(recovery.get('command')) == canonical_json(request)):
                    revision = recovery.get('seal', {}).get('row', {}).get('revision')
                    if type(revision) is int:
                        revisions.add(revision + 1)
                recovery = recovery.get('original_recovery')
        if len(revisions) != 1:
            return False
        event = db.execute(
            'SELECT payload_json FROM delivery_events WHERE run_id=? AND type=? AND '
            'run_revision=?',
            (run_id, 'publication_recovery_queued' if pending else 'gates_retry_queued',
             revisions.pop()),
        ).fetchall()
        return (len(event) == 1
                and canonical_json(json.loads(event[0][0])) == canonical_json(response))
    except (TypeError, ValueError, AttributeError):
        return False


def _receipt_matches_owner(db, operation, run_id, request_digest, request, response):
    """Check historical route evidence without comparing to the current workflow."""
    if not isinstance(response, dict) or response.get('run_id') != run_id:
        return False
    workflow = response.get('workflow_id')
    if not isinstance(workflow, str) or not workflow:
        return False
    if operation in {'stopped_delivery_resume', 'repair_continuation'}:
        if (response.get('phase') != 'repair_continuation_queued'
                or not isinstance(response.get('dashboard_url'), str)
                or not response['dashboard_url'].endswith('/runs/' + run_id)
                or type(response.get('authorized_through_iteration')) is not int):
            return False
        if operation == 'repair_continuation':
            maximum = request['expected_iteration'] + request['additional_iterations']
            grant = db.execute(
                'SELECT granted_iterations,maximum_iteration FROM delivery_repair_grants '
                'WHERE run_id=? AND command_id=?', (run_id, request['command_id']),
            ).fetchone()
            return (workflow == f'delivery-{run_id}-repair-continuation-1'
                    and response.get('existing') is False and grant is not None
                    and grant['granted_iterations'] == request['additional_iterations']
                    and grant['maximum_iteration'] == maximum
                    and response['authorized_through_iteration'] == maximum)
        if (workflow != f'delivery-{run_id}-resume-{request_digest[:20]}'
                or not isinstance(response.get('admission_sha256'), str)
                or not re.fullmatch(r'[0-9a-f]{64}', response['admission_sha256'])):
            return False
        try:
            events = [json.loads(item[0]) for item in db.execute(
                "SELECT payload_json FROM delivery_events WHERE run_id=? "
                "AND type='stopped_resume_queued'", (run_id,))]
        except (TypeError, ValueError):
            return False
        admitted = [item for item in events if isinstance(item, dict)
                    and item.get('command_id') == request['command_id']]
        return (len(admitted) == 1
                and type(admitted[0].get('authorized_through_iteration')) is int
                and admitted[0]['authorized_through_iteration']
                    == response['authorized_through_iteration'])
    if operation == 'recover_publication':
        valid = (response.get('phase') == 'publication_recovery_queued'
                 and response.get('implementation_authority') is False)
    elif operation in {'published_gate_retry', 'prepublication_gate_retry',
                       'published_check_prelaunch_retry', 'published_ci_retry',
                       'published_controller_retry'}:
        valid = (response.get('phase') == 'gates_retry_queued'
                 and response.get('implementation_authority') is False
                 and type(response.get('additional_iterations')) is int
                 and response['additional_iterations'] == 0
                 and response.get('candidate_id') == request['expected_candidate_id'])
    else:
        return False
    return valid and _event_backed_receipt(db, operation, run_id, request, response)


def command_receipt(operation: str, prior: Mapping[str, Any], run_id: str,
                    request_digest: str, *, request: Mapping[str, Any], db) -> dict[str, Any]:
    """Describe a prior admission only when its route-specific receipt is intact."""
    from .contracts import digest

    same = prior['request_digest'] == request_digest
    try:
        response = json.loads(prior['response_json'])
    except (TypeError, ValueError):
        response = None
    intact = (same and prior['run_id'] == run_id
              and _receipt_matches_owner(db, operation, run_id, request_digest, request, response))
    code = ('already_admitted' if intact else 'command_conflict' if not same
            else 'command_receipt_unknown')
    fact = ('an existing admission for these exact inputs' if intact else
            'a command ID bound to these exact inputs' if not same else
            'an intact saved admission receipt for this run')
    assessment = evaluate_transition({
        'operation': operation, 'owner': 'delivery_commands',
        'owner_proof': True if intact or not same else None,
        'proof_digest': digest({'run_id': prior['run_id'], 'prior': prior['request_digest'],
                                'response': prior['response_json'], 'request': request_digest}),
        'comparisons': [], 'predicates': [{
            'code': code, 'satisfied': False if intact or not same else None,
            'missing_or_unknown_fact': fact,
            'next_action': ('inspect the existing admission' if intact else
                            'inspect the saved command and obtain a fresh assessment'),
            'user_input_required': False if intact else None,
        }],
    }, {})
    assessment['disposition'] = code
    if not intact:
        exc = RecoveryRefused(assessment)
        exc.args = ('command ID already belongs to different inputs' if not same else
                    'stored command receipt is unavailable',)
        raise exc
    return assessment


def require_same_proof(operation: str, owner: str, observed_digest: str,
                       expected_digest: str) -> dict[str, Any]:
    """Recheck the owner's full seal under its admission custody."""
    return require_transition({
        'operation': operation, 'owner': owner, 'owner_proof': True,
        'normalized_lifecycle': 'stopped', 'proof_digest': observed_digest,
        'comparisons': [], 'predicates': [{
            'code': 'checkpoint_changed', 'satisfied': observed_digest == expected_digest,
            'missing_or_unknown_fact': 'unchanged authenticated checkpoint and custody',
            'next_action': 'obtain a fresh authenticated checkpoint',
        }],
    }, {})


def assess_transition(store, run_id: str, kind: str, request: dict[str, Any]) -> dict[str, Any]:
    from .delivery_execution_config import observation_mode

    try:
        with observation_mode():
            return _assess_transition(store, run_id, kind, request)
    except sqlite3.Error as exc:
        from .delivery_feature_revision_subject import FeatureRegistryObservationError

        feature = isinstance(exc, FeatureRegistryObservationError)
        return evaluate_transition({"operation": kind if isinstance(kind, str) else "unknown",
                                    "owner": "feature_registry" if feature else "tracking_store",
                                    "comparisons": [], "predicates": [{
                                        "code": ("feature_registry_proof_unknown" if feature else
                                                 "tracking_store_unavailable"),
                                        "satisfied": None,
                                        "missing_or_unknown_fact": (
                                            "stable feature registry" if feature else
                                            "stable tracking database"),
                                        "next_action": ("inspect the feature registry" if feature
                                                        else "inspect the tracking store")}]}, {})


def _assess_transition(store, run_id: str, kind: str, request: dict[str, Any]) -> dict[str, Any]:
    """Run the owner's complete preflight; do not turn unknown proof into eligibility.

    Currently only exact-checkpoint repair owners provide this contract. Other
    owners must be integrated before this endpoint can assert their eligibility.
    """
    def unknown(operation):
        return evaluate_transition({'operation': operation, 'owner': 'recovery_owner',
                                    'comparisons': []}, {})

    supported = {'stopped_delivery_resume', 'published_gate_retry',
                 'prepublication_gate_retry', 'published_check_prelaunch_retry',
                 'published_ci_retry', 'published_controller_retry'}
    if not isinstance(kind, str) or not isinstance(request, dict):
        return unknown('unknown')
    if kind in {'stopped_resource_closure', 'abandon_pending_resource_closure'}:
        from .delivery_resource_closure import assess

        return assess(store, run_id, kind, request)
    if kind == 'investigation_assessment_adjudication':
        from .delivery_investigation_adjudication import assess

        return assess(store, run_id, request)
    if kind == 'published_head_validation':
        from .delivery_publication_validation import assess

        return assess(store, run_id, request)
    if kind == 'investigation_gates_only':
        from .delivery_gates_admission import assess

        return assess(store, run_id, request)
    if kind == 'merge_run':
        from .delivery_merge_followup import assess

        return assess(store, run_id, request)
    if kind == 'continue_feature':
        from .delivery_feature_execution import assess_feature_continuation

        return assess_feature_continuation(store, run_id, request)
    if kind == 'revise_feature_plan':
        from .delivery_feature_revisions import assess_feature_revision

        return assess_feature_revision(store, run_id, request)
    if kind == 'repair_continuation' and 'continuation_kind' not in request:
        preflight = store.repair_admission_preflight
    elif kind in {'recover_publication', 'retry_prelaunch', 'amend_scope'}:
        preflight = partial(getattr(store, kind), preflight=True)
    elif kind in supported and request.get('continuation_kind') == kind:
        preflight = store.repair_admission_preflight
    elif kind == 'ordinary_ci_execution_inventory':
        from .delivery_ci_inventory import preflight as ci_preflight

        preflight = partial(ci_preflight, store)
    elif kind == 'native_check_diagnostic':
        from .delivery_native_diagnostic import preflight as diagnostic_preflight

        preflight = partial(diagnostic_preflight, store)
    else:
        return unknown(kind)
    try:
        result = preflight(run_id, request)
    except RecoveryRefused as exc:
        return exc.assessment
    except (ValueError, RuntimeError, OSError, sqlite3.Error):
        # Owner proof was not obtained. Do not classify failures by message text.
        return unknown(kind)
    if not isinstance(result, dict):
        # Historical command replay is not a fresh observation.
        return unknown(kind)
    assessment = result.get('lifecycle_assessment')
    if not isinstance(assessment, dict):
        return unknown(kind)
    return assessment
