"""Derived metrics with explicit denominators and missing-data counts."""
from collections import Counter, defaultdict
import json
import statistics

import state

DONE = {"done", "completed", "canceled", "cancelled"}
FIXED = {"resolved", "verified_fixed", "fixed", "closed"}


def distribution(values):
    values = sorted(v for v in values if v is not None)
    if not values:
        return {"observations": 0, "total": None, "median": None, "p95": None}
    return {"observations": len(values), "total": sum(values),
            "median": statistics.median(values), "p95": values[max(0, (95 * len(values) + 99) // 100 - 1)]}


def elapsed(start, end):
    return (state.instant(end) - state.instant(start)).total_seconds() if start and end else None


def summarize(db, work_id=None):
    args = (work_id,) if work_id else ()
    def rows(table, field="work_id"):
        clause = " WHERE " + field + "=?" if work_id else ""
        return [dict(r) for r in db.execute("SELECT * FROM " + table + clause, args)]
    works, runs = rows("works", "id"), rows("runs")
    results, findings, history = rows("results"), rows("findings"), rows("history")
    runtime = rows("runtime_events")
    sessions = [dict(r) for r in db.execute(
        "SELECT DISTINCT s.* FROM runtime_sessions s JOIN runtime_scopes b ON b.session_id=s.id"
        + (" WHERE b.work_id=?" if work_id else ""), args)]
    counts = lambda values, key: dict(Counter(str(r.get(key) or "unknown") for r in values))
    coverage = lambda values, fields: {
        key: {"known": sum(r.get(key) is not None for r in values), "records": len(values)}
        for key in fields}
    work_rows = []
    for w in works:
        work_rows.append(dict(id=w["id"], title=w["title"], status=w["status"], stage=w["stage"],
            issue=w["issue"], elapsed_seconds=elapsed(w["started_at"], w["ended_at"]),
            runs=sum(r["work_id"] == w["id"] for r in runs),
            results=sum(r["work_id"] == w["id"] for r in results),
            findings=sum(r["work_id"] == w["id"] for r in findings)))
    stage_seconds, stage_intervals = defaultdict(float), Counter()
    transitions, recovery = Counter(), Counter()
    for w in works:
        events = sorted((h for h in history if h["entity"] == "work" and h["work_id"] == w["id"]),
                        key=lambda h: (h.get("occurred_at") or h["recorded_at"], h["id"]))
        previous = None
        for h in events:
            timestamp = h.get("occurred_at") or h["recorded_at"]
            details = json.loads(h["details"]) if h["details"] else {}
            after = details.get("after", {})
            before = details.get("before", {})
            old_stage = h["previous_stage"] or before.get("stage")
            new_stage = h["stage"] or after.get("stage")
            new_status = h["status"] or after.get("status")
            if old_stage and new_stage and old_stage != new_stage:
                transitions[old_stage + " -> " + new_stage] += 1
            if "blocker" in after and after["blocker"] and after["blocker"] != before.get("blocker"):
                recovery["blocker_set"] += 1
            if before.get("status") in DONE and new_status and new_status not in DONE:
                recovery["reopened"] += 1
            if previous and (new_stage != previous[0] or new_status in DONE):
                seconds = elapsed(previous[1], timestamp)
                if seconds is not None and seconds >= 0:
                    stage_seconds[previous[0]] += seconds
                    stage_intervals[previous[0]] += 1
                previous = None
            if new_stage and new_status not in DONE and previous is None:
                previous = (new_stage, timestamp)
    tools = [r for r in runtime if r["kind"] == "tool"]
    turn_events = [r for r in runtime if r["kind"] == "turn"]
    repeated = Counter((r["session_id"], r["fingerprint"]) for r in tools if r["fingerprint"])
    parallel = []
    for r in turn_events:
        if r["started_at"] and r["ended_at"]:
            parallel += [(state.instant(r["started_at"]), 1), (state.instant(r["ended_at"]), -1)]
    active = peak = 0
    for _, delta in sorted(parallel):
        active += delta
        peak = max(peak, active)
    first_gates = {}
    for r in sorted(results, key=lambda r: (r["recorded_at"], r["id"])):
        if r["kind"] in {"review", "qa"}:
            first_gates.setdefault((r["work_id"], r["kind"]), r)
    normalized = lambda s: {"pass": "passed", "fail": "failed"}.get(s.lower(), s.lower())
    ownership = rows("claims")
    deliveries = [r for r in results if r["kind"] == "delivery"]
    return {
        "works": work_rows,
        "roles": counts(runs, "role"), "models": counts(runs, "model"), "efforts": counts(runs, "effort"),
        "run_status": counts(runs, "status"),
        "timing": {
            "work_elapsed_seconds": distribution([w["elapsed_seconds"] for w in work_rows]),
            "run_seconds": distribution([r["duration_seconds"] for r in runs]),
            "closed_stage_seconds": dict(stage_seconds),
            "closed_stage_intervals": dict(stage_intervals),
            "definition": "Recorded intervals, including waits; open intervals are not extrapolated."
        },
        "transitions": dict(transitions),
        "recovery": dict(recovery),
        "quality": {
            "first_recorded_review_qa": {
                "observations": len(first_gates),
                "passed": sum(normalized(r["status"]) == "passed" for r in first_gates.values()),
                "basis": "First retained observation per work and role; not a complete first-pass success rate."
            },
            "finding_resolution_seconds": distribution([
                elapsed(f["recorded_at"], f["updated_at"]) for f in findings if f["status"] in FIXED]),
            "result_status": dict(Counter(normalized(r["status"]) for r in results)),
        },
        "delivery": {"observations": len(deliveries), "status": counts(deliveries, "status"),
                     "work_ids": len({r["work_id"] for r in deliveries})},
        "ownership": {"claimed_work": len(ownership), "owners": len({r["owner"] for r in ownership}),
                      "claims": ownership, "basis": "Last observation, not agent health or a distributed lock."},
        "runtime": {
            "sessions": len(sessions), "session_models": counts(sessions, "model"),
            "events": counts(runtime, "kind"), "tools": counts(tools, "name"),
            "tool_status": counts(tools, "status"),
            "tool_seconds": distribution([r["duration_seconds"] for r in tools]),
            "turn_seconds": distribution([r["duration_seconds"] for r in turn_events]),
            "collector_processing_seconds": distribution([
                r["duration_seconds"] for r in runtime if r["kind"] == "collector"]),
            "repeated_tool_calls": sum(n - 1 for n in repeated.values()),
            "peak_observed_parallel_turns": peak if parallel else None,
            "latest_observation": max((s["last_seen_at"] for s in sessions if s["last_seen_at"]), default=None),
            "session_coverage": coverage(sessions, ["model", "effort", "transcript_path", "last_seen_at", "input_tokens", "output_tokens"]),
            "tool_coverage": coverage(tools, ["started_at", "ended_at", "duration_seconds"]),
            "basis": "Local hooks and worker transcripts; worker tool observations arrive at completion. Repeated calls are not necessarily retries. Shared sessions stay unallocated. Collector time excludes process startup and commit."
        },
        "coverage": {
            "works": coverage(works, ["issue", "branch", "commit", "started_at", "ended_at"]),
            "runs": coverage(runs, ["agent", "model", "effort", "started_at", "ended_at", "duration_seconds"]),
            "results": coverage(results, ["run_id", "commit", "evidence_ref"]),
            "findings": coverage(findings, ["commit", "evidence_ref", "fix_ref", "thread_ref"]),
        },
        "history": {"events": len(history), "entities": counts(history, "entity")},
        "unavailable": ["Complete workflow overhead and counterfactual savings", "Unobserved waiting and blocked time",
                        "Defect escape rate", "Human intervention intent", "Dollar cost without supplied prices"],
    }
