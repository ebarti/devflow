"""Typed recovery checkpoint comparisons; observations never grant execution.

Owner adapters must authenticate their own custody before constructing facts.
The evaluator is pure and can be called again when the owner holds its lock.
"""
from __future__ import annotations

from collections.abc import Mapping
from functools import partial
from typing import Any

VERSION = 1


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

    def block(code, fact, action, authorization=False):
        blockers.append({'code': code, 'owner': owner, 'missing_or_unknown_fact': fact,
                         'next_action': action, 'new_authorization_required': authorization})

    if facts.get('owner_proof') is not True:
        block('owner_proof_unknown', 'authenticated owner and custody proof',
              'inspect the owning recovery preflight')
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
            block(code, observed_key, 'obtain a fresh authenticated checkpoint')
    for predicate in facts.get('predicates', ()):
        if predicate.get('satisfied') is not True:
            blockers.append({
                'code': predicate['code'], 'owner': predicate.get('owner', owner),
                'missing_or_unknown_fact': predicate['missing_or_unknown_fact'],
                'next_action': predicate['next_action'],
                'new_authorization_required': predicate.get('new_authorization_required', False),
            })
    observed = {key: facts.get(key) for key in (
        'revision', 'iteration', 'candidate_id', 'candidate_head', 'publication_number')}
    return {'version': VERSION, 'operation': operation,
            'normalized_lifecycle': facts.get('normalized_lifecycle', 'unknown'),
            'eligible': not blockers, 'observed_revision': observed['revision'],
            'candidate': {'id': observed['candidate_id'], 'head': observed['candidate_head']},
            'publication': ({'number': observed['publication_number']}
                            if facts.get('publication_required') else None),
            'proof_digest': facts.get('proof_digest') if facts.get('owner_proof') is True else None,
            'supported_next_actions': sorted({item['next_action'] for item in blockers}),
            'blockers': blockers}


def require_transition(facts: Mapping[str, Any], request: Mapping[str, Any]) -> dict[str, Any]:
    assessment = evaluate_transition(facts, request)
    if not assessment['eligible']:
        raise RecoveryRefused(assessment)
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
    if kind in {'recover_publication', 'retry_prelaunch', 'amend_scope'}:
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
    except (ValueError, RuntimeError, OSError):
        # Owner proof was not obtained. Do not classify failures by message text.
        return unknown(kind)
    if not isinstance(result, dict):
        # Historical command replay is not a fresh observation.
        return unknown(kind)
    assessment = result.get('lifecycle_assessment')
    if not isinstance(assessment, dict):
        return unknown(kind)
    return assessment
