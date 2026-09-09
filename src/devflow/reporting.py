"""Non-overlapping portfolio accounting with explicit unknown portions."""
from __future__ import annotations

from decimal import Decimal, localcontext

from devflow.adapters.responses import TOKEN_FIELDS, _timestamp
from devflow.adapters.usage import decimal
from devflow.errors import WorkflowError


def usage_report(records, *, cutoff, population="portfolio", observation_window=None):
    cutoff = _timestamp(cutoff)
    unique = {}
    for row in records:
        key = row["response_id"]
        if key in unique and unique[key] != row:
            raise WorkflowError("usage_conflict", "Conflicting response identity in report")
        if _timestamp(row["recorded_at"]) <= cutoff:
            unique[key] = row

    def empty():
        return {**{key: Decimal(0) for key in TOKEN_FIELDS},
                "api_equivalent_usd": Decimal(0), "estimated_codex_credits": Decimal(0),
                "missing_usd_responses": 0, "missing_credit_responses": 0}

    def add(total, row, weight):
        for key in TOKEN_FIELDS:
            total[key] += decimal(row[key]) * weight
        for cost, missing in (("api_equivalent_usd", "missing_usd_responses"),
                              ("estimated_codex_credits", "missing_credit_responses")):
            if row[cost] is None:
                if weight:
                    total[missing] += 1
            else:
                total[cost] += decimal(row[cost]) * weight

    portfolio, unallocated, works = empty(), empty(), {}
    with localcontext() as context:
        context.prec = 50
        for row in unique.values():
            add(portfolio, row, Decimal(1))
            allocated = Decimal(0)
            seen_work = set()
            for allocation in row["allocations"]:
                work, weight = allocation["work_id"], decimal(allocation["weight"])
                if work in seen_work:
                    raise WorkflowError("usage_assignment", "Duplicate work allocation")
                seen_work.add(work)
                allocated += weight
                add(works.setdefault(work, empty()), row, weight)
            if allocated > 1:
                raise WorkflowError("usage_assignment", "Work allocation exceeds one")
            add(unallocated, row, 1 - allocated)
    for total in [portfolio, unallocated, *works.values()]:
        total["total_tokens"] = sum((total[key] for key in TOKEN_FIELDS[:-1]), Decimal(0))
        for cost, missing in (("api_equivalent_usd", "missing_usd_responses"),
                              ("estimated_codex_credits", "missing_credit_responses")):
            total[f"known_{cost}"] = total[cost]
            if total[missing]:
                total[cost] = None
    return {"cutoff": cutoff, "population": population, "observation_window": observation_window,
            "unique_responses": len(unique), "portfolio": portfolio,
            "work_allocations": works, "unallocated": unallocated,
            "missing_data_count": sum(row["pricing_status"] != "complete"
                                      or row["attribution_status"] != "complete"
                                      for row in unique.values()),
            "note": "Work and repair views overlap; portfolio counts each response once. "
                    "Reasoning is included in output. Costs are estimates, not invoices."}
