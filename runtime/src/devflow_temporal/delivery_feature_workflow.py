"""Deterministic coordination of parallel builds and ordered stack integration."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError

from .contracts import digest


class PlanningCorrectionRequired(Exception):
    def __init__(self, diagnostic):
        super().__init__("closed feature worker identified an evidenced planning defect")
        self.diagnostic = diagnostic


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
                    **controller._planning_request(spec),
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
            if controller._capture_planning_defect(spec, result):
                return await controller._stop(spec, "workstream identified a planning defect")
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
        compact_wait = workflow.patched("feature-worker-bounded-wait-v1")
        while True:
            if controller.cancel_requested:
                raise RuntimeError("feature cancellation requested")
            if compact_wait:
                pending = asyncio.create_task(controller._activity(
                    "delivery_feature_wait_worker", {
                        "config_path": spec["config_path"], "run_id": spec["run_id"],
                        "spec_digest": digest(spec), "child_id": child["run_id"],
                    },
                ))
                try:
                    await workflow.wait_condition(
                        lambda task=pending: task.done() or controller.cancel_requested
                    )
                    if controller.cancel_requested:
                        raise RuntimeError("feature cancellation requested")
                    observation = await pending
                finally:
                    if not pending.done():
                        pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            else:
                # Retain the original commands for pre-patch history replay.
                observation = await controller._activity(
                    "delivery_feature_worker_result", {"spec": spec, "child_id": child["run_id"]}
                )
            if observation["closed"]:
                break
            if not compact_wait:
                await workflow.sleep(timedelta(seconds=5))
    finished = await controller._activity(
        "delivery_feature_finish_worker", {"spec": spec, "child_id": child["run_id"]}
    )
    active.pop(child["run_id"], None)
    if finished["outcome"] != "delivered":
        if getattr(controller, "feature_plan_evolution", False) and finished.get("planning_defect"):
            from .delivery_feature_revision_roles import validate_diagnostic

            diagnostic = validate_diagnostic(finished["planning_defect"], chunk_id=chunk_id)
            raise PlanningCorrectionRequired(diagnostic)
        raise RuntimeError("feature worker stopped with preserved recovery evidence")
    return finished


async def _coordinate_pass(controller, spec, active, builds):
    completed = set()
    try:
        opened = await controller._activity("delivery_feature_open", {"spec": spec})
        spec = opened.get("spec", spec)
        controller.feature_spec = spec
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
    except PlanningCorrectionRequired as exc:
        return {"replan": exc.diagnostic}
    except Exception as exc:
        cause = getattr(exc, "cause", exc)
        if (getattr(controller, "feature_plan_evolution", False)
                and isinstance(cause, ApplicationError)
                and cause.type == "FeaturePlanningDefect" and cause.details):
            from .delivery_feature_revision_roles import validate_diagnostic

            try:
                return {"replan": validate_diagnostic(cause.details[0])}
            except (ValueError, KeyError, TypeError):
                pass
        if controller.cancel_requested:
            await controller._cancelled(spec)
        else:
            await controller._stop(
                spec, f"feature delivery stopped: {type(exc).__name__}: {str(exc)[:300]}", cause=exc
            )
    return controller.state


async def _revision_checkpoint(controller, spec, active, builds):
    controller.state["phase"] = "feature_revision_checkpoint"
    controller.state["revision"] += 1
    await controller._project(spec, "feature_revision_checkpoint",
                              "Closing known workers before a planning correction")
    await controller._activity("delivery_feature_settle_workers", {
        "spec": spec, "workers": dict(active),
    })
    if builds:
        await asyncio.gather(*builds.values(), return_exceptions=True)
    builds.clear()
    active.clear()
    readback = await controller._activity("delivery_feature_settle_effects", {"spec": spec})
    controller.state["checks"]["feature_readback"] = readback
    if readback.get("state") != "confirmed":
        raise RuntimeError("planning correction waits for original external effect readback")


async def _revise(controller, spec, active, builds, diagnostic=None):
    context = None
    adopting = False
    try:
        await _revision_checkpoint(controller, spec, active, builds)
        if controller.cancel_requested:
            await controller._cancelled(spec)
            return None
        context = await controller._activity("delivery_feature_revision_begin", {
            "spec": spec, "diagnostic": diagnostic,
        })
        if context.get("phase") == "rejected":
            raise RuntimeError("planning correction attempt was already rejected")
        controller.state["checks"]["repair_budget"] = context["budget"]
        controller.state["checks"]["plan_revision"] = {
            "revision_id": context["revision_id"], "phase": "investigating",
            "old_identity": context["old_identity"],
        }
        controller.state["phase"] = "feature_revision_intake"
        controller.state["revision"] += 1
        await controller._project(spec, "feature_revision_investigating",
                                  "Investigating the sealed planning defect")
        proposed = await controller._activity("delivery_feature_revision_propose", {
            "spec": spec, "context": context, "candidate": controller.state["candidate"],
        })
        controller.state["roles"].append(proposed)
        controller.state["usage"]["revision_intake:" + context["revision_id"]] = proposed.get(
            "usage")
        if (proposed.get("status") != "plan" or proposed.get("cleanup") != "confirmed"
                or not proposed.get("plan")):
            raise RuntimeError(proposed.get("summary") or
                               "revision intake did not establish a proposal")
        if controller.cancel_requested:
            raise RuntimeError("planning correction cancelled after proposal")
        plan = proposed["plan"]
        review_context = {**context, "proposed_plan": plan}
        controller.state["checks"]["plan_revision"].update(
            phase="proposed", proposal_sha256=digest(plan),
            affected_chunks=proposed.get("proposal_receipt", {}).get("affected_chunks", []),
        )
        controller.state["phase"] = "feature_revision_review"
        controller.state["revision"] += 1
        await controller._project(spec, "feature_revision_proposed",
                                  "Independently reviewing the smallest planning correction")
        reviewed = await controller._activity("delivery_feature_revision_review", {
            "spec": spec, "context": review_context, "candidate": controller.state["candidate"],
            "proposal_session_id": proposed.get("session_id"),
        })
        controller.state["roles"].append(reviewed)
        controller.state["usage"]["revision_review:" + context["revision_id"]] = reviewed.get(
            "usage")
        if (reviewed.get("status") != "pass" or reviewed.get("cleanup") != "confirmed"
                or reviewed.get("reviewed_plan_sha256") != digest(plan)
                or reviewed.get("findings")):
            raise RuntimeError(reviewed.get("summary") or
                               "independent plan review rejected the correction")
        if spec.get("plan_approval", "required") != "automatic":
            controller.decision_answer = None
            controller.state.update(phase="waiting_plan_revision", execution_state="waiting")
            controller.state["revision"] += 1
            controller.state["decision"] = {
                "id": spec["run_id"] + ":plan-revision:" + context["revision_id"],
                "kind": "plan_revision", "revision": context["old_identity"]["plan_revision"] + 1,
                "plan_digest": digest(plan), "plan": plan,
                "candidate_revision": controller.state["candidate_revision"],
                "prompt": ("Review this evidenced plan correction under the original "
                           "approval policy"),
                "options": ["proceed", "cancel"], "state": "pending",
            }
            await controller._project(spec, "feature_revision_approval_pending",
                                      "The original policy requires approval of this correction")
            await workflow.wait_condition(
                lambda: controller.decision_answer is not None or controller.cancel_requested
            )
            if controller.cancel_requested or controller.decision_answer != "proceed":
                raise RuntimeError("planning correction approval cancelled")
            reviewed = {**reviewed, "authorization": {
                "command": controller.feature_revision_authorization,
                "proposed_plan_sha256": digest(plan),
            }}
        if controller.cancel_requested:
            raise RuntimeError("planning correction cancelled before adoption")
        controller.state.update(phase="feature_revision_adopting", execution_state="running")
        controller.state["revision"] += 1
        await controller._project(spec, "feature_revision_adopting",
                                  "Publishing and reading back the exact plan correction")
        adopting = True
        adopted = await controller._activity("delivery_feature_revision_adopt", {
            "spec": spec, "context": context, "proposed_plan": plan, "review": reviewed,
        })
        if adopted.get("state") != "adopted":
            raise RuntimeError("planning correction adoption is not confirmed")
        revised = adopted["spec"]
        controller.feature_spec = revised
        controller.state["checks"]["repair_budget"] = adopted["budget"]
        controller.state["checks"]["plan_revision"].update(
            phase="adopted", identity=adopted["plan_identity"],
            affected_chunks=adopted["affected_chunks"],
        )
        controller.state["revision"] += 1
        await controller._project(revised, "feature_revision_adopted",
                                  "Resuming affected original workers under the recorded revision")
        return revised
    except Exception as exc:
        if context and not adopting:
            try:
                rejected = await controller._activity("delivery_feature_revision_reject", {
                    "spec": spec, "context": context,
                    "reason": str(exc)[:4000] or type(exc).__name__,
                })
                controller.state["checks"]["plan_revision"] = rejected
                if rejected.get("budget"):
                    controller.state["checks"]["repair_budget"] = rejected["budget"]
            except Exception:
                controller.state["cleanup"] = "unknown"
        if adopting:
            # Publication may already have committed. Never reject its original
            # intent or replace the plan/worker while its readback is uncertain.
            controller.state["cleanup"] = "unknown"
        await controller._stop(spec, "planning correction stopped: " + str(exc)[:600], cause=exc)
        return None


async def coordinate(controller, spec):
    active, builds = {}, {}
    controller.feature_plan_evolution = workflow.patched("feature-plan-evolution-v1")
    controller.feature_spec = spec
    try:
        if controller.feature_plan_evolution:
            pending = await controller._activity(
                "delivery_feature_revision_request", {"spec": spec})
            if pending:
                spec = await _revise(controller, spec, active, builds)
                if spec is None:
                    return controller.state
        while True:
            result = await _coordinate_pass(controller, spec, active, builds)
            spec = controller.feature_spec
            if result.get("replan"):
                spec = await _revise(controller, spec, active, builds, result["replan"])
                if spec is None:
                    return controller.state
                continue
            if not result.get("reintegrate"):
                return result
            builds.clear()
    finally:
        spec = controller.feature_spec
        # No takeover while a build, check, or publication still has authority.
        # Cancel through the worker's supported update and observe its closure.
        try:
            await controller._activity(
                "delivery_feature_settle_workers", {"spec": spec, "workers": dict(active)}
            )
        except ActivityError as exc:
            cause = exc.cause
            if (controller.state.get("outcome") not in {"blocked", "cancelled"}
                    or not isinstance(cause, ApplicationError)
                    or cause.type != "FeatureWorkerSettlementPending"
                    or not cause.details or cause.details[0].get("owner")
                    != spec["feature_delivery"]["owner"]
                    or not workflow.patched("feature-stopped-settlement-checkpoint-v1")):
                raise
            # Stop local monitors; child cleanup and draining ownership are retained.
            for task in builds.values():
                task.cancel()
            if builds:
                await asyncio.gather(*builds.values(), return_exceptions=True)
            return controller.state
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
