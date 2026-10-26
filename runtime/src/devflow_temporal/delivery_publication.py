"""A public change summary is separate from immutable execution instructions."""

from __future__ import annotations

import re
import unicodedata

from .contracts import digest

PUBLICATION_PROOF_VERSION = 1
# Periods inside ordinary abbreviations/initialisms and ellipses are not boundaries.
_NON_BOUNDARY = re.compile(
    r"\b(?:[a-z]\.){2,}|\b(?:vs|etc|mr|mrs|ms|dr|prof|sr|jr|st|no)\.|\.{2,}", re.I
)
_BIDI_CONTROLS = {"LRE", "RLE", "LRO", "RLO", "PDF", "LRI", "RLI", "FSI", "PDI"}
_BIDI_MARKS = {"\u061c", "\u200e", "\u200f"}


def stopped_at_publication(state: dict) -> bool:
    """Historical workflow compatibility hint, never fresh admission authority.

    New runs classify it (``publication_deadline``); stops recorded before the
    classification carry historical text. Already recorded recovery inputs keep
    their replay branch. Fresh admission uses delivery_publication_proof instead.
    """
    failure = (state.get("checks") or {}).get("failure") or {}
    error = state.get("error") or ""
    return (failure.get("cause_type") == "publication_deadline"
            or error.startswith("publication unresolved: "))


def admitted_recovery_stage(spec: dict, recovery: dict, *, legacy_hint: bool) -> bool:
    """New admitted inputs bind proof; old recorded inputs keep their replay branch."""
    if 'publication_stop_proof' not in recovery:
        return legacy_hint
    return publication_stop_proof_matches(
        spec, recovery.get('state') or {}, recovery['publication_stop_proof'])


def publication_stop_proof_matches(spec, state, proof):
    """Pure matching inside an owner-authenticated admission; never origin proof."""
    candidate = state.get('candidate') if isinstance(state, dict) else None
    valid = (isinstance(proof, dict) and isinstance(candidate, dict)
            and proof.get('version') == PUBLICATION_PROOF_VERSION
            and proof.get('source') in {'closed_result', 'closed_history'}
            and proof.get('run_id') == spec.get('run_id')
            and proof.get('request_digest') == spec.get('request_digest')
            and proof.get('iteration') == state.get('iteration')
            and proof.get('candidate_id') == candidate.get('id')
            and proof.get('stop_state_sha256') == digest(state)
            and bool(proof.get('workflow_id')) and bool(proof.get('execution_run_id'))
            and bool(proof.get('closed_at')))
    if not valid:
        return False
    if proof['source'] == 'closed_result':
        checks = state.get('checks')
        failure = checks.get('failure') if isinstance(checks, dict) else None
        return (isinstance(failure, dict)
                and failure.get('classification') == 'transient'
                and failure.get('cause_type') == 'publication_deadline'
                and failure.get('stage') in {'publishing', 'publishing_pending'}
                and proof.get('failure_sha256') == digest(failure))
    return (isinstance(proof.get('history_sha256'), str)
            and len(proof['history_sha256']) == 64
            and type(proof.get('scheduled_event_id')) is int and proof['scheduled_event_id'] > 0
            and type(proof.get('outcome_event_id')) is int
            and proof['outcome_event_id'] > proof['scheduled_event_id']
            and proof.get('activity') in {'delivery_publish', 'delivery_reconcile_publish'}
            and proof.get('activity_outcome') in {'completed', 'failed', 'timed_out', 'cancelled'})


def conventional(subject: str) -> bool:
    return bool(re.fullmatch(r"[a-z][a-z0-9-]*(?:\([^()\r\n]+\))?!?: \S.*", subject))


def publication_summary(goal: str, supplied: str | None = None) -> str:
    """Admit one concise subject, never shorten a detailed execution prompt."""
    value = goal if supplied is None else supplied
    source = "goal used for publication_summary" if supplied is None else "publication_summary"

    def reject(rule: str) -> None:
        hint = ("; provide publication_summary separately for a detailed goal"
                if supplied is None else "")
        raise ValueError(f"{source} {rule}{hint}")

    if not isinstance(value, str) or not value.strip():
        reject("must be a non-empty string")
    # Refuse hidden controls before stripping: str.strip() also removes C1 NEL.
    if any(
        (unicodedata.category(ch) == "Cc" and ch not in " \t\r\n\v\f")
        or unicodedata.bidirectional(ch) in _BIDI_CONTROLS
        or ch in _BIDI_MARKS
        for ch in value
    ):
        reject("must not contain Unicode control or bidi formatting characters")
    summary = value.strip()
    if len(summary.splitlines()) != 1:
        reject("must be a single line after trimming surrounding whitespace")
    if any(unicodedata.category(ch) == "Cc" for ch in summary):
        reject("must not contain control characters within the summary")
    if re.search(r"[.!?]\s+\S", _NON_BOUNDARY.sub("abbreviation", summary)):
        reject("must not contain multiple sentences")
    if supplied is not None and not conventional(summary):
        reject("must use Conventional Commit syntax")
    if not conventional(summary):
        summary = "chore: " + summary
    if len(summary) > 120:
        reject("must be at most 120 characters including its type")
    return summary
