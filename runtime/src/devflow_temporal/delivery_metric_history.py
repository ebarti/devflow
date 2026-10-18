"""Read sealed delivery histories for metrics. No service, database writes or recovery."""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from temporalio.client import WorkflowHistory

from .delivery_closures import _read_sealed, closure_facts
from .delivery_codec import DELIVERY_DATA_CONVERTER


async def read(item: dict) -> dict:
    record = item["closure"]
    data = _read_sealed(Path(item["root"]), record["history_sha256"])
    history = WorkflowHistory.from_json(record["workflow_id"], data.decode())
    facts = await closure_facts(history)
    for field in ("execution_run_id", "request_digest", "recovery_digest", "event_count"):
        if facts[field] != record[field]:
            raise ValueError(f"closure {field} differs from sealed record")
    if facts["status"] != record["status"] or (
        datetime.fromisoformat(facts["closed_at"])
        != datetime.fromisoformat(record["closed_at"].replace("Z", "+00:00"))
    ):
        raise ValueError("closure status or time differs from sealed record")
    if not facts["inputs"] or facts["inputs"][0].get("run_id") != record["run_id"]:
        raise ValueError("closure input belongs to a different delivery")
    events = list(history.events)
    completed = {
        e.activity_task_completed_event_attributes.scheduled_event_id: e
        for e in events
        if e.HasField("activity_task_completed_event_attributes")
    }
    projections = []
    failed = 0
    for event in events:
        if not event.HasField("activity_task_scheduled_event_attributes"):
            continue
        activity = event.activity_task_scheduled_event_attributes
        if activity.activity_type.name != "delivery_project":
            continue
        if event.event_id not in completed:
            failed += 1
            continue
        values = await DELIVERY_DATA_CONVERTER.decode(activity.input.payloads)
        request = values[0]
        projections.append(
            {
                **{
                    k: request.get(k)
                    for k in ("phase", "execution_state", "iteration", "key", "event_type")
                },
                "start": event.event_time.ToDatetime(tzinfo=UTC).isoformat(),
                "end": completed[event.event_id].event_time.ToDatetime(tzinfo=UTC).isoformat(),
            }
        )
    return {
        "run_id": record["run_id"],
        "workflow_id": record["workflow_id"],
        "execution_run_id": facts["execution_run_id"],
        "start": events[0].event_time.ToDatetime(tzinfo=UTC).isoformat(),
        "close": facts["closed_at"],
        "final_phase": (facts["result"] or {}).get("phase"),
        "transitions": projections,
        "failed_projections": failed,
    }


async def main(items):
    results, errors = [], []
    for item in items:
        try:
            results.append(await read(item))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            errors.append({"workflow_id": item["closure"]["workflow_id"], "reason": str(exc)})
    return {"executions": results, "errors": errors}


if __name__ == "__main__":
    print(json.dumps(asyncio.run(main(json.load(sys.stdin)))))
