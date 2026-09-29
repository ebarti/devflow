"""Non-overlapping response accounting, with explicit historical join coverage."""
from __future__ import annotations

from datetime import timedelta
from decimal import Decimal, localcontext

from devflow.adapters.responses import TOKEN_FIELDS, _timestamp
from devflow.adapters.usage import decimal
from devflow.errors import WorkflowError
from devflow.reporting_inputs import (
    UNKNOWN, attempts, deliveries, instant, observed_intervals, phase_at,
    records as work_records, unique, values,
)

COSTS = {"api_equivalent_usd": "missing_usd_responses",
         "estimated_codex_credits": "missing_credit_responses"}


def _empty():
    return {**{key: Decimal(0) for key in TOKEN_FIELDS},
            **{key: Decimal(0) for key in COSTS},
            **{key: 0 for key in COSTS.values()}}


def _add(total, row, weight):
    if not weight:
        return
    for key in TOKEN_FIELDS:
        total[key] += decimal(row[key]) * weight
    for cost, missing in COSTS.items():
        if row[cost] is None:
            total[missing] += 1
        else:
            total[cost] += decimal(row[cost]) * weight


def _finish(total):
    total["total_tokens"] = sum((total[key] for key in TOKEN_FIELDS[:-1]), Decimal(0))
    for cost, missing in COSTS.items():
        total[f"known_{cost}"] = total[cost]
        if total[missing]:
            total[cost] = None
    return total


def _breakdown():
    return {"by_task": {}, "by_role": {}, "by_phase": {}, "by_role_phase": {}}


def _group(groups, row, weight, task, role, phase):
    for key, value in (("by_task", task), ("by_role", role), ("by_phase", phase)):
        _add(groups[key].setdefault(value, _empty()), row, weight)
    _add(groups["by_role_phase"].setdefault(role, {}).setdefault(phase, _empty()), row, weight)


def _finish_groups(groups):
    for key in ("by_task", "by_role", "by_phase"):
        for total in groups[key].values():
            _finish(total)
    for phases in groups["by_role_phase"].values():
        for total in phases.values():
            _finish(total)


def usage_report(records, *, cutoff, population="portfolio", observation_window=None,
                 works=None, segments=None):
    cutoff = _timestamp(cutoff)
    end = instant(cutoff)
    unique_rows = unique(records, "response_id", code="usage_conflict")
    unique_rows = {key: row for key, row in unique_rows.items()
                   if instant(row["recorded_at"]) <= end}
    work_map = unique(values(works), "work_id", code="duplicate_work")
    segment_map = unique([*values(segments), *(segment for work in work_map.values()
                         for segment in work_records(work, "execution_segment"))], "segment_id")
    attempt_map = {}
    for work_id, work in work_map.items():
        for attempt_id, attempt in attempts(work, end).items():
            if attempt_id in attempt_map and attempt_map[attempt_id][0] != work_id:
                raise WorkflowError("metric_conflict", "Attempt belongs to multiple work outcomes")
            attempt_map[attempt_id] = (work_id, attempt)
    delivery_map = {wid: deliveries(work, end) for wid, work in work_map.items()}
    rework_map = {wid: observed_intervals(work, "rework", end)[0]
                  for wid, work in work_map.items()}
    portfolio, unallocated, allocated_works = _empty(), _empty(), {}
    groups, work_groups, missing_joins = _breakdown(), {}, []
    purpose_accounts = {key: _empty() for key in ("normal", "repair", "experiment", "unknown")}
    experiments, work_costs = {}, {}
    for wid, work in work_map.items():
        delivered = delivery_map[wid]
        delivery = instant(delivered[0]["delivered_at"]) if delivered else None
        work_costs[wid] = {
            "delivered_at": delivery.isoformat() if delivery else None,
            "observation_age_days": (end - delivery).total_seconds() / 86400 if delivery else None,
            "mature_30_day_window": bool(delivery and end >= delivery + timedelta(days=30)),
            "delivery": _empty() if delivery else None,
            "repair_through_30_days": _empty() if delivery else None,
            "cost_through_30_days": _empty() if delivery else None,
            "observed_rework": _empty(),
            "missing_purpose_allocations": 0,
        }

    with localcontext() as context:
        context.prec = 50
        for row in unique_rows.values():
            timestamp = instant(row["recorded_at"])
            task = row.get("task_id") or UNKNOWN
            segment = segment_map.get(row.get("segment_id"))
            reasons = []
            if segment is None:
                reasons.append("segment_missing")
            elif (segment.get("task_id") != task
                  or instant(segment["started_at"]) > timestamp
                  or (segment.get("ended_at") and timestamp >= instant(segment["ended_at"]))):
                reasons.append("segment_mismatch")
                segment = None
            role = segment.get("role") if segment else UNKNOWN
            bound_attempt = attempt_map.get(segment.get("attempt_id")) if segment else None
            allocated = Decimal(0)
            seen_work = set()
            for allocation in row["allocations"]:
                wid, weight = allocation["work_id"], decimal(allocation["weight"])
                if wid in seen_work or weight < 0 or weight > 1:
                    raise WorkflowError("usage_assignment", "Invalid or duplicate work allocation")
                seen_work.add(wid)
                allocated += weight
                if not weight:
                    continue
                work = work_map.get(wid)
                joined = bound_attempt if bound_attempt and bound_attempt[0] == wid else None
                phase = phase_at(work, timestamp, attempt_id=joined[1]["attempt_id"]) if (
                    work and joined) else UNKNOWN
                purpose = (joined[1].get("accounting") or {}).get("purpose", "unknown") if (
                    joined) else "unknown"
                accounting = joined[1].get("accounting", {}) if joined else {}
                if purpose not in purpose_accounts:
                    purpose = "unknown"
                join_reasons = [*reasons]
                if not work:
                    join_reasons.append("work_missing")
                if not joined:
                    join_reasons.append("attempt_missing_or_mismatched")
                if phase == UNKNOWN:
                    join_reasons.append("phase_missing_or_ambiguous")
                if purpose == "unknown":
                    join_reasons.append("purpose_missing")
                if join_reasons:
                    missing_joins.append({"response_id": row["response_id"], "work_id": wid,
                                          "weight": weight, "reasons": join_reasons})
                _add(allocated_works.setdefault(wid, _empty()), row, weight)
                _add(purpose_accounts[purpose], row, weight)
                _group(groups, row, weight, task, role, phase)
                _group(work_groups.setdefault(wid, _breakdown()), row, weight, task, role, phase)
                if purpose == "experiment":
                    identity = tuple(accounting.get(key) for key in
                                     ("experiment_id", "case_id", "arm_id"))
                    if all(identity):
                        account = experiments.setdefault(identity, _empty())
                        _add(account, row, weight)
                    else:
                        missing_joins.append({"response_id": row["response_id"], "work_id": wid,
                                              "weight": weight, "reasons": ["experiment_ids_missing"]})
                if wid in work_costs:
                    cost = work_costs[wid]
                    if purpose == "unknown":
                        cost["missing_purpose_allocations"] += 1
                    if cost["delivered_at"] and timestamp <= instant(cost["delivered_at"]):
                        _add(cost["delivery"], row, weight)
                        _add(cost["cost_through_30_days"], row, weight)
                    if any(instant(interval["started_at"]) <= timestamp
                           < (instant(interval["ended_at"]) if interval.get("ended_at") else end)
                           for interval in rework_map[wid]):
                        _add(cost["observed_rework"], row, weight)
                if purpose == "repair":
                    origin = accounting.get("origin_work_id")
                    original_cost = work_costs.get(origin)
                    if original_cost and original_cost["delivered_at"]:
                        delivery = instant(original_cost["delivered_at"])
                        if delivery < timestamp <= delivery + timedelta(days=30):
                            _add(original_cost["repair_through_30_days"], row, weight)
                            _add(original_cost["cost_through_30_days"], row, weight)
                    else:
                        missing_joins.append({"response_id": row["response_id"], "work_id": wid,
                                              "weight": weight, "reasons": ["repair_origin_missing"]})
            if allocated > 1:
                raise WorkflowError("usage_assignment", "Work allocation exceeds one")
            _add(portfolio, row, Decimal(1))
            _add(unallocated, row, 1 - allocated)
            _add(purpose_accounts["unknown"], row, 1 - allocated)
            _group(groups, row, 1 - allocated, task, role, UNKNOWN)
            if allocated < 1:
                missing_joins.append({"response_id": row["response_id"], "work_id": None,
                                      "weight": 1 - allocated, "reasons": ["unallocated"]})
        for total in (portfolio, unallocated, *allocated_works.values(), *purpose_accounts.values(),
                      *experiments.values()):
            _finish(total)
        for group in (groups, *work_groups.values()):
            _finish_groups(group)
        for cost in work_costs.values():
            for key in ("delivery", "repair_through_30_days", "cost_through_30_days", "observed_rework"):
                if cost[key] is not None:
                    _finish(cost[key])
        normal_work_ids = {
            wid for wid in work_map if any(attempt.get("accounting", {}).get("purpose") == "normal"
                                          for awid, attempt in attempt_map.values() if awid == wid)
        }
        normal_deliveries = {
            wid for wid in normal_work_ids if any(
                attempt_map.get(delivery.get("attempt_id"), (None, {}))[1].get(
                    "accounting", {}).get("purpose") == "normal"
                for delivery in delivery_map[wid])
        }
        count = len(normal_deliveries)
        ratios = {key: purpose_accounts["normal"][key] / count
                  if count and purpose_accounts["normal"][key] is not None else None for key in COSTS}
    return {
        "cutoff": cutoff, "population": population, "observation_window": observation_window,
        "unique_responses": len(unique_rows), "portfolio": portfolio,
        "work_allocations": allocated_works, "unallocated": unallocated,
        "missing_data_count": sum(row["pricing_status"] != "complete"
                                  or row["attribution_status"] != "complete"
                                  for row in unique_rows.values()),
        **groups, "work_breakdowns": work_groups, "missing_joins": missing_joins,
        "missing_join_response_count": len({row["response_id"] for row in missing_joins}),
        "purpose_accounts": purpose_accounts, "work_costs": work_costs,
        "experiment_accounts": [{"experiment_id": key[0], "case_id": key[1], "arm_id": key[2],
                                 "usage": total} for key, total in sorted(experiments.items())],
        "cost_per_delivered_outcome": {"normal_work_count": len(normal_work_ids),
                                       "delivered_outcomes": count,
                                       "unfinished_outcomes": len(normal_work_ids - normal_deliveries),
                                       "total_execution_cost": purpose_accounts["normal"], **ratios},
        "note": "Work and repair views overlap; portfolio and purpose accounts count each response "
                "once. All figures cover supplied usage only; absent usage is not zero execution "
                "cost. Reasoning is included in output. Costs are estimates, not invoices.",
    }
