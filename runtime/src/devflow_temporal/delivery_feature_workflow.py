"""Deterministic coordination of parallel builds and ordered stack integration."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow

from .contracts import digest


def ordered(plan):
    pending = [
        {**chunk, "workstream_id": stream["id"]}
        for stream in plan["workstreams"]
        for chunk in stream["chunks"]
    ]
    completed, result = set(), []
    while pending:
        chunk = next(item for item in pending if set(item["depends_on"]) <= completed)
        result.append(chunk)
        completed.add(chunk["id"])
        pending.remove(chunk)
    return result


def publication(record, *, complete=False):
    value = record["manifest"]["publication"]
    return {
        "stack_id": value["stack_id"],
        "scope_complete": complete,
        "plan_digest": digest(record["manifest"]["plan"]),
        "record_revision": record["manifest"]["revision"],
        "pull_requests": [{**item, "state": "OPEN"} for item in value["members"]],
    }


async def run_build(controller, spec, *, start_iteration=0, session=None, maximum_iteration=None):
    try:
        findings = []
        maximum = spec["policy"]["max_repairs"] if maximum_iteration is None else maximum_iteration
        for iteration in range(start_iteration, maximum + 1):
            if controller.cancel_requested:
                return await controller._cancelled(spec)
            controller.state["iteration"] = iteration
            controller.state["phase"] = "implement" if iteration == 0 else "repair"
            controller.state["revision"] += 1
            await controller._project(spec, "role_started", "Building a complete feature chunk")
            result = await controller._activity(
                "delivery_role",
                {
                    "spec": spec,
                    "role": "implement",
                    "iteration": iteration,
                    "candidate": controller.state["candidate"],
                    "findings": findings,
                    "resume_session": session,
                },
            )
            controller.state["roles"].append(result)
            controller.state["candidate"] = result["candidate"]
            if session is not None and result.get("session_id") != session:
                return await controller._stop(spec, "workstream repair lost its original session")
            session = result.get("session_id")
            if result.get("status") == "pass":
                break
            if result.get("status") != "findings" or result.get("cleanup") == "unknown":
                return await controller._stop(spec, "workstream execution needs recovery")
            findings = result.get("findings", [])
        else:
            return await controller._stop(spec, "workstream product repair limit exhausted")
        if controller.cancel_requested:
            return await controller._cancelled(spec)
        sealed = await controller._activity(
            "delivery_feature_seal_build", {"spec": spec, "candidate": result["candidate"]}
        )
        controller.state["checks"]["build_checkpoint"] = sealed
        controller.state.update(
            phase="delivered", execution_state="terminal", outcome="delivered", cleanup="confirmed"
        )
        controller.state["revision"] += 1
        await controller._project(
            spec, "delivered", "Implementation preserved for stack integration"
        )
        return controller.state
    except Exception as exc:
        return await controller._stop(
            spec, f"workstream build failed: {type(exc).__name__}", cause=exc
        )


async def _worker(controller, spec, chunk_id, kind, active):
    assignment = await controller._activity(
        "delivery_feature_reserve", {"spec": spec, "chunk_id": chunk_id, "kind": kind}
    )
    child = assignment["spec"]
    workflow_id = assignment["workflow_id"]
    active[child["run_id"]] = workflow_id
    if not assignment["completed"]:
        # The transactional outbox dispatches both initial and resumed workers.
        # A coordinator crash cannot lose the start or create a second workflow.
        while True:
            if controller.cancel_requested:
                raise RuntimeError("feature cancellation requested")
            observation = await controller._activity(
                "delivery_feature_worker_result", {"spec": spec, "child_id": child["run_id"]}
            )
            if observation["closed"]:
                break
            await workflow.sleep(timedelta(seconds=5))
    finished = await controller._activity(
        "delivery_feature_finish_worker", {"spec": spec, "child_id": child["run_id"]}
    )
    active.pop(child["run_id"], None)
    if finished["outcome"] != "delivered":
        raise RuntimeError("feature worker stopped with preserved recovery evidence")
    return finished


async def _coordinate_pass(controller, spec, active, builds):
    completed = set()
    try:
        opened = await controller._activity("delivery_feature_open", {"spec": spec})
        record = opened["record"]
        chunks = ordered(record["manifest"]["plan"])
        completed = {
            item["id"] for item in chunks if "verified:" + item["id"] in opened["checkpoints"]
        }
        controller.state["checks"]["repair_budget"] = opened["budget"]
        controller.state["pull_request"] = publication(record)
        controller.state["phase"] = "feature_implementation"
        controller.state["revision"] += 1
        await controller._project(spec, "feature_started", "Executing the GitHub feature plan")
        for chunk in chunks:
            if controller.cancel_requested:
                break
            if chunk["id"] in completed:
                continue
            # Ready workstreams build in separate owned checkouts. Integration
            # starts only in the canonical topological stack order below.
            for ready in chunks:
                if (
                    ready["id"] not in completed
                    and ready["id"] not in builds
                    and set(ready["depends_on"]) <= completed
                ):
                    builds[ready["id"]] = asyncio.create_task(
                        _worker(controller, spec, ready["id"], "build", active)
                    )
            await builds[chunk["id"]]
            if controller.cancel_requested:
                break
            controller.state["phase"] = "feature_integration"
            controller.state["revision"] += 1
            await controller._project(spec, "chunk_integrating", "Integrating " + chunk["title"])
            finished = await _worker(controller, spec, chunk["id"], "chunk", active)
            record = finished["record"]
            completed.add(chunk["id"])
            controller.state["pull_request"] = publication(record)
            controller.state["checks"]["repair_budget"] = finished["budget"]
            controller.state["checks"]["feature_progress"] = {
                "completed": sorted(completed),
                "total": len(chunks),
            }
            controller.state["phase"] = "feature_implementation"
            controller.state["revision"] += 1
            await controller._project(
                spec, "chunk_verified", "Verified and published " + chunk["title"]
            )
        if not controller.cancel_requested:
            controller.state["pull_request"] = publication(record, complete=True)
            controller.state["phase"] = "awaiting_merge"
            controller.state["execution_state"] = "waiting"
            controller.state["revision"] += 1
            if spec["authorized_endpoint"] != "merged":
                controller.decision_answer = None
                controller.state["decision"] = {
                    "id": spec["run_id"] + ":merge:" + str(record["manifest"]["revision"]),
                    "kind": "merge",
                    "revision": record["manifest"]["revision"],
                    "candidate_revision": controller.state["candidate_revision"],
                    "prompt": "All chunks passed their integrated checks. Merge this feature?",
                    "options": ["merge"],
                    "state": "pending",
                    "publication": controller.state["pull_request"],
                }
            await controller._project(
                spec, "feature_ready", "Feature stack is ready for authorized merge"
            )
            if spec["authorized_endpoint"] != "merged":
                await workflow.wait_condition(
                    lambda: controller.decision_answer == "merge" or controller.cancel_requested
                )
            if not controller.cancel_requested:
                controller.state["phase"] = "merging"
                controller.state["execution_state"] = "running"
                controller.state["revision"] += 1
                await controller._project(
                    spec, "feature_merging", "Merging the authorized feature stack"
                )
                while True:
                    merged = await controller._activity(
                        "delivery_feature_merge",
                        {
                            "spec": spec,
                            "publication": controller.state["pull_request"],
                            "authorization": getattr(
                                controller, "feature_merge_authorization", None
                            ),
                        },
                    )
                    controller.state["checks"]["feature_merge"] = merged
                    if merged["state"] == "confirmed":
                        break
                    if merged["state"] == "needs_integration":
                        await controller._activity("delivery_feature_begin_integration", {
                            "spec": spec, "target": merged["target"],
                        })
                        return {"reintegrate": True}
                    if controller.cancel_requested:
                        raise RuntimeError(
                            "merge was submitted; its remote result still requires readback"
                        )
                    await workflow.sleep(timedelta(seconds=30))
                receipts = {item["number"]: item for item in merged["pull_requests"]}
                for member in controller.state["pull_request"]["pull_requests"]:
                    member.update(receipts[member["number"]], state="MERGED")
                controller.state.update(
                    phase="merged",
                    execution_state="terminal",
                    outcome="delivered",
                    cleanup="confirmed",
                )
                controller.state["revision"] += 1
                await controller._project(
                    spec, "delivered", "Feature stack merged and GitHub issues closed"
                )
        if controller.cancel_requested:
            await controller._cancelled(spec)
    except Exception as exc:
        if controller.cancel_requested:
            await controller._cancelled(spec)
        else:
            await controller._stop(
                spec, f"feature delivery stopped: {type(exc).__name__}: {str(exc)[:300]}", cause=exc
            )
    return controller.state


async def coordinate(controller, spec):
    active, builds = {}, {}
    try:
        while True:
            result = await _coordinate_pass(controller, spec, active, builds)
            if not result.get("reintegrate"):
                return result
            builds.clear()
    finally:
        # No takeover while a build, check, or publication still has authority.
        # Cancel through the worker's supported update and observe its closure.
        await controller._activity(
            "delivery_feature_settle_workers", {"spec": spec, "workers": dict(active)}
        )
        if builds:
            await asyncio.gather(*builds.values(), return_exceptions=True)
        if controller.state.get("outcome"):
            terminal = {key: controller.state.get(key) for key in
                        ("phase", "execution_state", "outcome", "error", "cleanup")}
            waiting = False
            while True:
                try:
                    readback = await controller._activity(
                        "delivery_feature_settle_effects", {"spec": spec}
                    )
                except Exception as exc:
                    readback = {"state": "pending", "reason": type(exc).__name__}
                controller.state["checks"]["feature_readback"] = readback
                if readback["state"] == "confirmed":
                    break
                controller.state.update(phase="waiting_feature_readback", execution_state="waiting",
                                        outcome=None)
                controller.state["revision"] += 1
                await controller._project(
                    spec, "feature_readback_pending", "Reconciling the original GitHub operation"
                )
                waiting = True
                await workflow.sleep(timedelta(seconds=30))
            if controller.cancel_requested and terminal["outcome"] != "delivered":
                terminal.update(phase="cancelled", execution_state="terminal", outcome="cancelled")
            controller.state.update(terminal)
            if waiting:
                controller.state["revision"] += 1
                await controller._project(spec, terminal["outcome"],
                                          "Original operations settled; feature can be continued")
            await controller._activity(
                "delivery_feature_stop",
                {
                    "spec": spec,
                    "checkpoint": {
                        "state": controller.state,
                        "completed_chunks": controller.state["checks"].get(
                            "feature_progress", {}).get("completed", []),
                    },
                },
            )
