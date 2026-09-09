"""Deterministic outcome reporting from explicitly recorded historical observations."""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from statistics import median

from devflow.errors import WorkflowError


def instant(value: str) -> datetime:
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError, AttributeError) as exc:
        raise WorkflowError("invalid_metric_time", "Metrics require ISO timestamps") from exc
    if result.tzinfo is None:
        raise WorkflowError("invalid_metric_time", "Metrics require timezone-aware timestamps")
    return result


def _union_seconds(intervals, cutoff):
    prepared = []
    for interval in intervals:
        start = instant(interval["started_at"])
        end = min(instant(interval["ended_at"]) if interval.get("ended_at") else cutoff, cutoff)
        if end < start:
            if start > cutoff:
                continue
            raise WorkflowError("invalid_metric_interval", "Interval ends before it begins")
        prepared.append((start, end))
    merged = []
    for start, end in sorted(prepared):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    return sum((end - start).total_seconds() for start, end in merged)


def quality_report(works: list[dict], *, cutoff: str) -> dict:
    end = instant(cutoff)
    work_ids = [work["work_id"] for work in works]
    if len(set(work_ids)) != len(work_ids):
        raise WorkflowError("duplicate_work", "A cohort contains each work outcome once")
    rows, all_defects, user_first, known_encounters, unknown_origins = (
        [],
        set(),
        set(),
        set(),
        set(),
    )
    stages, severities = Counter(), Counter()
    lead_times, mature_numerator, mature_items = [], set(), 0
    for work in works:
        records = list(work.get("records", {}).values())
        events = [
            r
            for r in records
            if r.get("record_type") == "outcome_event" and instant(r["occurred_at"]) <= end
        ]
        handoffs = [
            instant(e["occurred_at"]) for e in events if e["event_kind"] == "first_ready_handoff"
        ]
        handoff = min(handoffs) if handoffs else None
        exposures = [instant(e["occurred_at"]) for e in events if e["event_kind"] == "exposed"]
        exposure = min(exposures) if exposures else None
        eligible = work.get("contract", {}).get("kind") in {"feature", "bug"}
        mature = bool(eligible and handoff and end >= handoff + timedelta(days=30))
        mature_items += int(mature)
        after, early, within, late, post_exposure, raw, missing = (
            set(),
            set(),
            set(),
            set(),
            set(),
            set(),
            set(),
        )
        for event in events:
            if event["event_kind"] != "defect_confirmed":
                continue
            details, detected = event["details"], instant(event["occurred_at"])
            identity = details["defect_id"]
            if identity not in all_defects:
                stages[details["stage"]] += 1
                severities[details["severity"]] += 1
            all_defects.add(identity)
            attributed = details["attribution"] == "confirmed" and details["origin_candidate_id"]
            if details["attribution"] in {"unknown", "provisional", "shared"}:
                unknown_origins.add(identity)
                missing.add(identity)
            if exposure and detected >= exposure and details["first_report"]:
                post_exposure.add(identity)
            if details["detector"] != "user":
                continue
            raw.add(identity)
            if details["known_before_user_encounter"]:
                known_encounters.add(identity)
            if not details["first_report"] or details["known_before_user_encounter"]:
                continue
            user_first.add(identity)
            if handoff and detected >= handoff:
                after.add(identity)
                days = detected - handoff
                if days <= timedelta(days=7):
                    early.add(identity)
                if days <= timedelta(days=30):
                    within.add(identity)
                    if mature and attributed:
                        mature_numerator.add(identity)
                else:
                    late.add(identity)
        phases = defaultdict(list)
        for interval in work.get("phase_history", []):
            phases[interval["phase"]].append(interval)
        phase_seconds = {
            phase: _union_seconds(intervals, end) for phase, intervals in phases.items()
        }
        delivered = [
            r
            for r in records
            if r.get("record_type") == "delivery"
            and r.get("status") == "verified"
            and instant(r["delivered_at"]) <= end
        ]
        started = work.get("attempt", {}).get("started_at") if work.get("attempt") else None
        lead = None
        if started and delivered:
            lead = (
                min(instant(r["delivered_at"]) for r in delivered) - instant(started)
            ).total_seconds()
            if lead < 0:
                raise WorkflowError("invalid_metric_interval", "Delivery predates execution")
            lead_times.append(lead)
        snapshots = {
            r["snapshot_id"]: {
                "package_version": r["package_version"],
                "workflow_hash": r["workflow_hash"],
                "model_policy_hash": r["model_policy_hash"],
            }
            for r in records
            if r.get("record_type") == "workflow_snapshot"
        }
        rows.append(
            {
                "work_id": work["work_id"],
                "eligible_implementation": eligible,
                "handoff_at": handoff.isoformat() if handoff else None,
                "mature_30_day_window": mature,
                "raw_user_encounters": len(raw),
                "user_first_after_handoff": len(after),
                "user_first_within_7_days": len(early),
                "user_first_within_30_days": len(within),
                "late_user_discoveries": len(late),
                "post_exposure_first_discoveries": len(post_exposure),
                "unknown_origin_defects": len(missing),
                "phase_elapsed_seconds": phase_seconds,
                "execution_lead_seconds": lead,
                "historical_workflows": snapshots,
                "interventions": sum(e["event_kind"] == "intervention" for e in events),
                "avoidable_interventions": sum(
                    e["event_kind"] == "intervention" and e["details"]["avoidable"] for e in events
                ),
            }
        )
    rate = Decimal(100) * len(mature_numerator) / mature_items if mature_items else None
    return {
        "cutoff": cutoff,
        "population": "distinct supplied work outcomes",
        "observation_window_days": 30,
        "work_count": len(works),
        "confirmed_defects": len(all_defects),
        "user_first_discoveries": len(user_first),
        "known_defects_encountered_by_user": len(known_encounters),
        "unknown_origin_defects": len(unknown_origins),
        "by_stage": dict(stages),
        "by_severity": dict(severities),
        "mature_implementation_items": mature_items,
        "attributed_user_defects_in_mature_windows": len(mature_numerator),
        "user_found_after_handoff_rate_30_days": rate,
        "execution_lead_time": {
            "sample_count": len(lead_times),
            "p50_seconds": median(lead_times) if lead_times else None,
            "p90_seconds": sorted(lead_times)[math.ceil(len(lead_times) * 0.9) - 1]
            if lead_times
            else None,
            "p90_method": "nearest rank",
        },
        "missing_handoff_items": sum(row["handoff_at"] is None for row in rows),
        "missing_execution_lead_items": sum(row["execution_lead_seconds"] is None for row in rows),
        "works": rows,
        "limitations": [
            "Absence of a report is not evidence that the product path was used",
            "Request lead time and blocking waits need explicit source timestamps",
            "Cost and post-delivery repair allocation use the separate usage report",
        ],
    }
