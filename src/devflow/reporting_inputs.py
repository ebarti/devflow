"""Shared historical joins and interval arithmetic for deterministic reports."""

from __future__ import annotations

from datetime import UTC, datetime

from devflow.errors import WorkflowError
from devflow.validation import digest

UNKNOWN = "__unknown__"


def instant(value):
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
        return parsed.astimezone(UTC)
    except (TypeError, ValueError, AttributeError) as exc:
        raise WorkflowError("invalid_metric_time", "Reports require timezone-aware ISO timestamps") from exc


def values(collection):
    return list(collection.values()) if isinstance(collection, dict) else list(collection or [])


def records(work, kind):
    return [row for row in values(work.get("records")) if row.get("record_type") == kind]


def unique(rows, field, *, code="metric_conflict"):
    result = {}
    for row in rows:
        identity = row.get(field)
        if not identity:
            continue
        if identity in result and result[identity] != row:
            raise WorkflowError(code, f"Conflicting {field} in report inputs")
        result[identity] = row
    return result


def events(work, cutoff, kind=None):
    rows = unique(records(work, "outcome_event"), "event_id").values()
    return sorted(
        (row for row in rows if instant(row["occurred_at"]) <= cutoff
         and (kind is None or row.get("event_kind") == kind)),
        key=lambda row: (instant(row["occurred_at"]), row["event_id"]),
    )


def attempts(work, cutoff):
    result = unique(records(work, "attempt"), "attempt_id")
    current = work.get("attempt")
    if current and current.get("attempt_id"):
        result[current["attempt_id"]] = {**result.get(current["attempt_id"], {}), **current}
    return {
        key: row for key, row in result.items()
        if row.get("started_at") and instant(row["started_at"]) <= cutoff
    }


def eligibility(work, cutoff):
    """Only the admitted initial scope can establish pre-execution eligibility."""
    started = [instant(row["started_at"]) for row in attempts(work, cutoff).values()]
    contract = initial_contract(work, recorded=True)
    if not started or not contract or "autonomy_eligibility" not in contract:
        return None
    if not any(row.get("command") == "work.ready"
               and instant(row["recorded_at"]) <= min(started)
               for row in work.get("history", [])):
        return None
    return contract["autonomy_eligibility"]


def deliveries(work, cutoff):
    return sorted(
        (row for row in unique(records(work, "delivery"), "delivery_id").values()
         if row.get("status") == "verified" and instant(row["delivered_at"]) <= cutoff),
        key=lambda row: (instant(row["delivered_at"]), row["delivery_id"]),
    )


def initial_contract(work, *, recorded=False):
    candidates = [row for row in records(work, "work_contract") if row.get("scope_revision") == 1]
    if len(candidates) > 1 and any(row != candidates[0] for row in candidates[1:]):
        raise WorkflowError("metric_conflict", "Initial work scopes conflict")
    return candidates[0] if candidates else None if recorded else work.get("contract") or {}


def candidate_contract(work, candidate_id):
    candidate = next((row for row in records(work, "candidate")
                      if row.get("candidate_id") == candidate_id), None)
    if not candidate or not candidate.get("scope_hash"):
        return None
    for contract in records(work, "work_contract"):
        hashed = digest({key: value for key, value in contract.items()
                         if key not in {"title", "scope_revision"}})
        if hashed == candidate["scope_hash"]:
            return contract
    return None


def required_roles(work, candidate_id):
    contract = candidate_contract(work, candidate_id)
    tier = (contract or {}).get("risk", {}).get("tier")
    return None if tier is None else [] if tier == 0 else ["review"] if tier == 1 else ["review", "qa"]


def union_intervals(intervals, cutoff, *, lower=None):
    prepared = []
    for interval in intervals:
        start = instant(interval["started_at"])
        raw_end = instant(interval["ended_at"]) if interval.get("ended_at") else cutoff
        if raw_end < start:
            raise WorkflowError("invalid_metric_interval", "Interval ends before it begins")
        finish = min(raw_end, cutoff)
        start = max(start, lower) if lower else start
        if start < finish:
            prepared.append((start, finish))
    merged = []
    for start, finish in sorted(prepared):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(finish, merged[-1][1]))
        else:
            merged.append((start, finish))
    return merged


def union_seconds(intervals, cutoff, *, lower=None):
    return sum((finish - start).total_seconds()
               for start, finish in union_intervals(intervals, cutoff, lower=lower))


def coverage(work, kind, start, finish, cutoff):
    if start is None or finish is None:
        return False
    intervals = []
    for event in events(work, cutoff, "observation_window"):
        details = event.get("details", {})
        if kind not in details.get("complete_kinds", []):
            continue
        if (not details.get("ended_at")
                or instant(details["ended_at"]) > instant(event["occurred_at"])):
            continue
        intervals.append(details)
    if finish < start:
        raise WorkflowError("invalid_metric_interval", "Observation ends before execution")
    return bool(intervals) and union_seconds(intervals, finish, lower=start) == (
        finish - start
    ).total_seconds()


def observed_intervals(work, kind, cutoff):
    """An explicit later closure may finish an earlier open interval, without counting twice."""
    field = "interval_id" if kind == "blocking_wait" else "cycle_id"
    result, missing = {}, 0
    for event in events(work, cutoff, kind):
        details = event.get("details", {})
        if not details.get(field) or not details.get("started_at"):
            missing += 1
            continue
        prior = result.get(details[field])
        if prior and any(prior.get(key) != details.get(key)
                         for key in ("started_at", "cause", "blocking", "from_phase")):
            raise WorkflowError("metric_conflict", f"Conflicting {field} observations")
        if prior and prior.get("ended_at") and prior.get("ended_at") != details.get("ended_at"):
            raise WorkflowError("metric_conflict", f"A closed {field} changed")
        result[details[field]] = details
    return list(result.values()), missing


def phase_at(work, timestamp, *, attempt_id=None):
    phases = set()
    for interval in work.get("phase_history", []):
        if attempt_id and interval.get("attempt_id") not in {None, attempt_id}:
            continue
        start = instant(interval["started_at"])
        end = instant(interval["ended_at"]) if interval.get("ended_at") else None
        if end is not None and end < start:
            raise WorkflowError("invalid_metric_interval", "Phase ends before it begins")
        if start <= timestamp and (end is None or timestamp < end):
            phases.add(interval["phase"])
    return next(iter(phases)) if len(phases) == 1 else UNKNOWN
