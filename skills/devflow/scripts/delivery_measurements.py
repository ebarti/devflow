"""Issue wall time and child execution time from the real delivery ledger.

The stdlib reader never starts services or changes the database. For older executions
without journal entries, an isolated installed-runtime decoder reads authenticated sealed
histories. Missing history remains unknown, never inferred from today's phase.
"""

import json
import os
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path


def instant(value):
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("metrics timestamps must include a timezone")
    return result


def decode(items):
    if not items:
        return {"executions": [], "errors": []}
    root = Path(__file__).resolve().parents[3]
    python = root / "runtime/.venv/bin/python"
    env = {k: v for k, v in os.environ.items() if k not in {"PYTHONPATH", "PYTHONOPTIMIZE", "TZ"}}
    env["PYTHONPATH"] = str(root / "runtime/src")
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    try:
        result = subprocess.run(
            [
                str(python) if python.exists() else sys.executable,
                "-m",
                "devflow_temporal.delivery_metric_history",
            ],
            input=json.dumps(items),
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
            check=False,
        )
        if result.returncode:
            raise ValueError(
                "Sealed-history decoder unavailable; install the runtime dependencies. "
                + result.stderr[-500:]
            )
        return json.loads(result.stdout)
    except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
        return {"executions": [], "errors": [{"reason": str(exc)}]}


def summarize(db, work_id=None, as_of=None):
    cutoff = instant(as_of) if as_of else datetime.now(UTC)
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"delivery_runs", "delivery_events"} <= tables:
        return None
    runs = {r["run_id"]: dict(r) for r in db.execute("SELECT * FROM delivery_runs")}
    specs = {k: json.loads(r["request_json"]) for k, r in runs.items()}

    def owner(rid):
        seen = set()
        while specs[rid].get("feature_worker"):
            if rid in seen:
                raise ValueError("cycle in worker parent lineage")
            seen.add(rid)
            parent = specs[rid]["feature_worker"].get("parent_run_id")
            if parent not in runs:
                return None
            rid = parent
        return runs[rid]["work_id"]

    owners = {rid: owner(rid) for rid in runs}
    selected = {
        rid: row
        for rid, row in runs.items()
        if (not work_id or owners[rid] == work_id) and instant(row["created_at"]) <= cutoff
    }
    if not selected:
        return None
    marks = ",".join("?" for _ in selected)
    events = defaultdict(list)
    for row in db.execute(
        f"SELECT * FROM delivery_events WHERE run_id IN ({marks}) ORDER BY sequence",
        tuple(selected),
    ):
        event = dict(row)
        event["payload"] = json.loads(event["payload_json"])
        events[row["run_id"]].append(event)
    closures = (
        [
            dict(r)
            for r in db.execute(
                f"SELECT * FROM delivery_closures WHERE run_id IN ({marks})", tuple(selected)
            )
        ]
        if "delivery_closures" in tables
        else []
    )
    decoded = decode(
        [
            {
                "closure": c,
                "root": str(Path(specs[c["run_id"]]["state_dir"]).parent.parent / "closures"),
            }
            for c in closures
            if specs[c["run_id"]].get("state_dir")
        ]
    )
    executions = list(decoded["executions"])
    warnings = list(decoded["errors"])
    warnings.extend(
        {"run_id": rid, "reason": "Worker parent is absent from the ledger"}
        for rid in selected
        if owners[rid] is None
    )
    matches = fallbacks = 0
    # Bind successful historical projections to the durable publication clock.
    for execution in executions:
        rid = execution["run_id"]
        admissions = [e for e in events[rid] if e["type"] in {"accepted", "worker_reserved"}]
        if "-resume-" not in execution["workflow_id"] and admissions:
            execution["start"] = admissions[0]["timestamp"]
        transitions = []
        for transition in execution["transitions"]:
            candidates = [
                e
                for e in events[rid]
                if transition["key"] is not None
                and e["payload"].get("key") == transition["key"]
                and e["type"] == transition["event_type"]
                and instant(transition["start"])
                <= instant(e["timestamp"])
                <= instant(transition["end"])
            ]
            matched = len(candidates) == 1
            clock = candidates[0]["timestamp"] if matched else transition["start"]
            matches += int(matched)
            fallbacks += int(not matched)
            transitions.append(
                {
                    **transition,
                    "time": clock,
                    "clock": "public_event" if matched else "projection_scheduled",
                }
            )
        execution["transitions"] = transitions
        execution["initial_phase"] = (
            "preparing"
            if specs[rid].get("preparation_version") == 1 or specs[rid].get("feature_worker")
            else "accepted"
        )
    covered = {h["workflow_id"] for h in executions}
    # New journal entries contain phase + execution identity; no Temporal query needed.
    for rid, row in selected.items():
        groups = defaultdict(list)
        if "delivery_phase_events" in tables:
            for journal in db.execute(
                "SELECT * FROM delivery_phase_events WHERE run_id=? ORDER BY event_sequence",
                (rid,),
            ):
                observation = dict(journal)
                groups[observation["workflow_id"]].append(
                    {**observation, "time": observation["timestamp"], "clock": "phase_journal"}
                )
        for workflow_id, transitions in groups.items():
            if workflow_id in covered:
                continue
            start = row["created_at"] if "-resume-" not in workflow_id else transitions[0]["time"]
            terminal = [
                t
                for t in transitions
                if t.get("execution_state")
                in {"terminal", "completed", "cancelled", "failed", "stopped"}
            ]
            close = terminal[0]["time"] if terminal else None
            executions.append(
                {
                    "run_id": rid,
                    "workflow_id": workflow_id,
                    "start": start,
                    "close": close,
                    "final_phase": terminal[0]["phase"] if terminal else None,
                    "initial_phase": "unknown",
                    "transitions": transitions,
                }
            )
        if (row["workflow_id"] or "delivery-" + rid) not in covered and not groups:
            executions.append(
                {
                    "run_id": rid,
                    "workflow_id": row["workflow_id"] or rid,
                    "start": row["created_at"],
                    "close": None,
                    "final_phase": None,
                    "initial_phase": "unknown",
                    "transitions": [],
                }
            )
            warnings.append(
                {"run_id": rid, "reason": "No phase-bearing events or readable sealed history"}
            )
    intervals = []

    def interval(h, phase, start, end, iteration=None, basis="execution"):
        start, end = max(start, instant(h["start"])), min(end, cutoff)
        if end <= start:
            return
        rid = h["run_id"]
        intervals.append(
            {
                "work_id": owners[rid],
                "run_id": rid,
                "workflow_id": h["workflow_id"],
                "kind": "worker" if specs[rid].get("feature_worker") else "coordinator",
                "phase": phase or "unknown",
                "iteration": iteration,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "seconds": (end - start).total_seconds(),
                "basis": basis,
            }
        )

    for h in executions:
        end = min(instant(h["close"]), cutoff) if h["close"] else cutoff
        transitions = [{"time": h["start"], "phase": h["initial_phase"], "iteration": 0}]
        transitions += h["transitions"]
        transitions = sorted(
            (t for t in transitions if instant(h["start"]) <= instant(t["time"]) < end),
            key=lambda t: instant(t["time"]),
        )
        for i, t in enumerate(transitions):
            stop = instant(transitions[i + 1]["time"]) if i + 1 < len(transitions) else end
            interval(h, t["phase"], instant(t["time"]), stop, t.get("iteration"))
    for wid in set(owners[r] for r in selected):
        parents = sorted(
            [
                h
                for h in executions
                if owners[h["run_id"]] == wid
                and not specs[h["run_id"]].get("feature_worker")
                and instant(h["start"]) <= cutoff
            ],
            key=lambda h: instant(h["start"]),
        )
        for i, h in enumerate(parents):
            if not h["close"] or instant(h["close"]) >= cutoff:
                continue
            next_start = instant(parents[i + 1]["start"]) if i + 1 < len(parents) else cutoff
            phase = h["final_phase"]
            if i + 1 < len(parents) or phase in {"blocked", "stopped"}:
                if next_start < instant(h["close"]):
                    warnings.append(
                        {"run_id": h["run_id"], "reason": "Overlapping coordinator executions"}
                    )
                else:
                    interval(h, phase, instant(h["close"]), next_start, basis="between_executions")
    by_work = {}
    for wid in sorted(set(owners[r] for r in selected), key=str):
        rows = [i for i in intervals if i["work_id"] == wid]
        summary = {"coordinator": defaultdict(float), "worker": defaultdict(float)}
        for i in rows:
            summary[i["kind"]][i["phase"]] += i["seconds"]
        parent = [i for i in rows if i["kind"] == "coordinator"]
        elapsed = (
            (
                max(instant(i["end"]) for i in parent) - min(instant(i["start"]) for i in parent)
            ).total_seconds()
            if parent
            else None
        )
        summary.update(
            elapsed_seconds=elapsed,
            unknown_seconds=sum(i["seconds"] for i in parent if i["phase"] == "unknown"),
            coordinator_reconciliation_seconds=(sum(i["seconds"] for i in parent) - elapsed)
            if elapsed is not None
            else None,
        )
        if elapsed is not None and abs(summary["coordinator_reconciliation_seconds"]) > 0.00001:
            warnings.append(
                {"work_id": wid, "reason": "Coordinator intervals do not reconcile to elapsed time"}
            )
        by_work[wid] = summary
    attempts = (
        [
            dict(r)
            for r in db.execute(
                "SELECT role,session_id,started_at,iteration,run_id FROM delivery_attempts "
                f"WHERE run_id IN ({marks})",
                tuple(selected),
            )
            if r["started_at"] and instant(r["started_at"]) < cutoff
        ]
        if "delivery_attempts" in tables
        else []
    )
    commands = []
    for execution in executions:
        rid = execution["run_id"]
        for command in execution.get("commands", []):
            if (command.get("activity_started_at")
                    and instant(command["activity_started_at"]) > cutoff):
                continue
            if command.get("started_at") and instant(command["started_at"]) > cutoff:
                continue
            row = {**command, "work_id": owners[rid], "run_id": rid,
                   "workflow_id": execution["workflow_id"]}
            if row.get("finished_at") and instant(row["finished_at"]) > cutoff:
                row.update(finished_at=None, duration_seconds=None)
            commands.append(row)
    return {
        "as_of": cutoff.isoformat(),
        "commands": commands,
        "command_timing_coverage": {
            "records": len(commands),
            "complete": sum(c.get("started_at") is not None
                            and c.get("finished_at") is not None for c in commands),
            "definition": "Observed command boundaries from sealed activity results; "
                          "historical missing timestamps remain null. Not CPU time.",
        },
        "works": by_work,
        "intervals": intervals,
        "coverage": {
            "complete": not warnings and not any(i["phase"] == "unknown" for i in intervals),
            "warnings": warnings,
            "sealed_executions": len(decoded["executions"]),
            "matched_public_events": matches,
            "projection_clock_fallbacks": fallbacks,
            "failed_projections_excluded": sum(h.get("failed_projections", 0) for h in executions),
        },
        "roles": dict(Counter(a["role"] for a in attempts)),
        "sessions_by_role": {
            role: len({a["session_id"] for a in attempts if a["role"] == role and a["session_id"]})
            for role in {a["role"] for a in attempts}
        },
        "definition": (
            "Coordinator wall time across successors, including blocked gaps. "
            "Worker seconds are summed separately, may overlap and are not CPU time. "
            "Open intervals end at as_of. Missing evidence is unknown."
        ),
    }
