"""Reduce persisted gate evidence for the current attempt without replaying work."""
from __future__ import annotations

from typing import Any


_RESET = {
    "gates_retry_started", "gates_only_started", "publication_validation_started",
    "metadata_validation_started", "policy_recovery_started", "scope_amendment_started",
}
_STARTS = {
    "prepublish": _RESET | {"prepublish_checks"},
    "local_checks": _RESET | {"checks_started", "checks_retry_started"},
    "browser_qa": _RESET | {"browser_qa_started"},
    "required_ci": _RESET | {"ci_wait", "ci_retry_started"},
}
_ACTIVE_PHASE = {
    "prepublish": "prepublish_checks", "local_checks": "checks",
    "browser_qa": "browser_qa", "required_ci": "waiting_ci",
}


def evidence(db: Any, run_id: str, iteration: int, gate_specs: list[tuple]) -> dict[str, int]:
    """One aggregate per known event type, even for arbitrarily long histories.

    Preparation is run-wide; all other evidence is candidate-iteration scoped.
    An attempt can repeat in one iteration, so ordering is retained as well.
    """
    types = {completion for _, _, completion in gate_specs} | {"ci_retry_started"}
    types.update(*(starts for starts in _STARTS.values()))
    latest = {}
    for event in sorted(types):
        scope = "" if event == "tracker_start" else " AND json_extract(payload_json,'$.iteration')=?"
        values = (run_id, event) if not scope else (run_id, event, iteration)
        row = db.execute(
            "SELECT sequence FROM delivery_events WHERE run_id=? AND type=?" + scope
            + " ORDER BY sequence DESC LIMIT 1", values,
        ).fetchone()
        if row:
            latest[event] = row["sequence"]
    return latest


def state(name: str, completion: str, *, row: dict[str, Any],
          observed: str | None, latest: dict[str, int]) -> str:
    if name == "tracker":
        if observed in {"failed", "blocked", "conflict"}:
            return "failed"
        if observed == "unknown":
            return "unknown"
        return "completed" if row["outcome"] == "delivered" and observed == "consistent" else "pending"
    completed = latest.get(completion, 0)
    # A CI-only continuation performs local checks then enters CI directly.
    if name == "local_checks" and completion == "ci_wait":
        completed = max(completed, latest.get("ci_retry_started", 0))
    started = max((latest.get(event, 0) for event in _STARTS.get(name, ())), default=0)
    if started > completed and row["phase"] == _ACTIVE_PHASE.get(name):
        # A new start may carry the previous result until its command returns.
        return "pending"
    if observed in {"failed", "blocked", "conflict"}:
        return "failed"
    if observed == "unknown":
        return "unknown"
    if observed in {"pending", "running"} or started > completed:
        return "pending"
    return "completed" if completed or observed in {"passed", "consistent"} else "pending"
