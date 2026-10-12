"""Deterministic coordination of parallel builds and ordered stack integration."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.exceptions import ActivityError, ApplicationError, ServerError
from temporalio.exceptions import TimeoutError as ActivityTimeoutError

from .contracts import digest
from .delivery_workflow import transient_failure

# Bounds of the coordinator paths gated on `delivery-feature-liveness-v1`.
AUTOMATIC_RECOVERIES = 2  # admitted resumes per worker
REISSUES = 4  # transport re-issues of a worker's assignment, admission or observation
OBSERVATION_RETRIES = 6
OBSERVATION_PAUSE = timedelta(seconds=30)
OBSERVATION_PAUSE_CAP = timedelta(minutes=10)
MERGE_READBACK_CAP = timedelta(minutes=15)
READBACK_INTERVAL = timedelta(seconds=30)
READBACK_SLOW_INTERVAL = timedelta(minutes=10)
READBACK_POLLS = 150  # backed-off readbacks (about a day) before stopping with the effect pending


class PlanningCorrectionRequired(Exception):
    def __init__(self, diagnostic):
        super().__init__("closed feature worker identified an evidenced planning defect")
        self.diagnostic = diagnostic


class FeatureWorkerStopped(RuntimeError):
    """A worker closed without delivering; its closure stays in the store.

    ``final`` marks a verdict the supervisor must not re-issue: a refused
    admission, an exhausted recovery bound or a failing activity's verdict."""

    def __init__(self, chunk_id, kind, *, closure=None, detail=None, final=False):
        super().__init__(f"feature {kind} worker for chunk {chunk_id} "
                         + (detail or "stopped with preserved recovery evidence"))
        self.chunk_id, self.kind, self.closure, self.final = chunk_id, kind, closure, final


class ObservationLost(RuntimeError):
    """A coordinator observation stayed lost to infrastructure past its retry bound.

    Carries the assignment it observed so the supervisor re-observes it instead
    of reserving the worker again."""

    def __init__(self, name, child_id, retries, cause, *, assignment=None):
        super().__init__(f"{name} for worker {child_id} stayed lost across {retries} "
                         f"re-issues ({cause})")
        self.name, self.child_id, self.cause_description = name, child_id, cause
        self.assignment = assignment


def _liveness(controller):
    return bool(getattr(controller, "feature_liveness_v1", False))


def _describe(exc):
    """Name the failing condition behind an SDK wrapper."""
    cause = getattr(exc, "cause", None)
    if isinstance(exc, ActivityError) and cause is not None:
        kind = getattr(cause, "type", None)
        name = kind if isinstance(kind, str) and kind else type(cause).__name__
        return f"{name}: {str(getattr(cause, 'message', cause))[:200]}"
    return f"{type(exc).__name__}: {str(exc)[:200]}"


def _transient_activity_failure(exc):
    """Only timeouts, retryable server errors and the allowlisted infrastructure
    failure types (the same set the worker side classifies transient) count;
    every other failure is a verdict about the request, ownership or GitHub."""
    cause = getattr(exc, "cause", None)
    if isinstance(cause, ActivityTimeoutError):
        return True
    if isinstance(cause, ServerError):
        return not cause.non_retryable
    if isinstance(cause, ApplicationError):
        return not cause.non_retryable and transient_failure(cause.type, cause.message)
    return False


def _resumable(closure):
    """Only a blocked closure its own controller classified as infrastructure,
    with confirmed cleanup and no planning verdict, is offered to the admission.
    Cancelled workers and inherited classifications never are."""
    return bool(closure) and closure.get("outcome") == "blocked" and (
        closure.get("cleanup") != "unknown"
    ) and (closure.get("failure") or {}).get("classification") == "transient" and (
        not closure.get("planning_defect")
    )


def _infrastructure_failure(exc):
    if isinstance(exc, FeatureWorkerStopped):
        return not exc.final and _resumable(exc.closure)
    if isinstance(exc, ObservationLost):
        return True
    return isinstance(exc, ActivityError) and _transient_activity_failure(exc)


async def _pause(controller, delay):
    """Wait without missing a cancellation."""
    try:
        await workflow.wait_condition(lambda: controller.cancel_requested, timeout=delay)
    except TimeoutError:
        pass


async def _observe(controller, resilient, spec, child_id, name, request, *, assignment=None):
    """Repeat an observation lost to infrastructure, bounded; verdicts propagate.

    A lost observation decides nothing: the worker's closure stays in the store.
    Each re-issue backs off and projects what is stuck; past the bound the
    worker's supervisor decides, so a permanent fault ends in a named stop
    instead of a loop that only grows history.
    """
    retries = 0
    while True:
        try:
            return await controller._activity(name, request)
        except ActivityError as exc:
            if not (resilient and _transient_activity_failure(exc)) or controller.cancel_requested:
                raise
            if retries >= OBSERVATION_RETRIES:
                raise ObservationLost(name, child_id, retries, _describe(exc),
                                      assignment=assignment) from exc
            retries += 1
            lost = controller.state["checks"].setdefault("feature_observation_retries", {})
            lost[child_id] = lost.get(child_id, 0) + 1
            delay = min(OBSERVATION_PAUSE * 2 ** (retries - 1), OBSERVATION_PAUSE_CAP)
            controller.state["revision"] += 1
            await controller._project(
                spec, "feature_worker_observation_lost",
                f"{name} for worker {child_id} lost ({_describe(exc)}); retry {retries} of "
                f"{OBSERVATION_RETRIES} in {int(delay.total_seconds())} s")
            await _pause(controller, delay)
            if controller.cancel_requested:
                raise RuntimeError("feature cancellation requested") from exc


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


async def run_build(controller, spec, *, start_iteration=0, session=None, maximum_iteration=None,
                    preserved=False):
    try:
        findings = []
        maximum = spec["policy"]["max_repairs"] if maximum_iteration is None else maximum_iteration
        # A preserved build already passed its implementation; it is only sealed.
        result = {"candidate": controller.state["candidate"]}
        for iteration in range(start_iteration, start_iteration if preserved else maximum + 1):
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
            if not preserved:
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


async def _worker(controller, spec, chunk_id, kind, active, *, resume_allowed=False,
                  assignment=None):
    if assignment is None:
        assignment = await controller._activity(
            "delivery_feature_reserve", {"spec": spec, "chunk_id": chunk_id, "kind": kind}
        )
    child = assignment["spec"]
    workflow_id = assignment["workflow_id"]
    active[child["run_id"]] = workflow_id
    resilient = _liveness(controller)
    closure = None
    if not assignment["completed"]:
        # The transactional outbox dispatches both initial and resumed workers.
        # A coordinator crash cannot lose the start or create a second workflow.
        compact_wait = workflow.patched("feature-worker-bounded-wait-v1")
        while True:
            if controller.cancel_requested:
                raise RuntimeError("feature cancellation requested")
            if compact_wait:
                pending = asyncio.create_task(_observe(
                    controller, resilient, spec, child["run_id"], "delivery_feature_wait_worker", {
                        "config_path": spec["config_path"], "run_id": spec["run_id"],
                        "spec_digest": digest(spec), "child_id": child["run_id"],
                    }, assignment=assignment,
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
                closure = observation.get("closure")
                break
            if not compact_wait:
                await workflow.sleep(timedelta(seconds=5))
    if resilient and resume_allowed and _resumable(closure):
        # Decided before finish_worker retires the registry assignment, which a
        # continuation could not reuse. The supervisor re-issues this assignment
        # and reserve() admits the supported continuation under its own checks.
        raise FeatureWorkerStopped(chunk_id, kind, closure=closure)
    finished = await _observe(
        controller, resilient, spec, child["run_id"], "delivery_feature_finish_worker",
        {"spec": spec, "child_id": child["run_id"]}, assignment=assignment,
    )
    active.pop(child["run_id"], None)
    if finished["outcome"] != "delivered":
        if getattr(controller, "feature_plan_evolution", False) and finished.get("planning_defect"):
            from .delivery_feature_revision_roles import validate_diagnostic

            diagnostic = validate_diagnostic(finished["planning_defect"], chunk_id=chunk_id)
            raise PlanningCorrectionRequired(diagnostic)
        if resilient:
            raise FeatureWorkerStopped(chunk_id, kind, closure=closure)
        raise RuntimeError("feature worker stopped with preserved recovery evidence")
    return finished


async def _admit(controller, spec, chunk_id, kind, closure):
    """Offer a resumable closure to the real admission; a refusal is final and named."""
    try:
        return await controller._activity(
            "delivery_feature_reserve", {"spec": spec, "chunk_id": chunk_id, "kind": kind})
    except ActivityError as refusal:
        if _transient_activity_failure(refusal):
            raise
        raise FeatureWorkerStopped(
            chunk_id, kind, closure=closure, final=True,
            detail=f"stopped ({closure.get('error') or 'blocked'}); automatic resume refused: "
                   f"{_describe(refusal)}") from refusal


async def _recovery(controller, spec, counter, key, count, bound, message):
    controller.state["checks"].setdefault(counter, {})[key] = count + 1
    controller.state["revision"] += 1
    await controller._project(spec, "feature_worker_recovering",
                              f"{message} ({count + 1} of {bound})")


async def _recovering_worker(controller, spec, chunk_id, kind, active):
    """Run one worker, re-issuing it across a bounded number of infrastructure failures.

    Gate verdicts, ownership conflicts and unknown cleanup stop it at once, naming
    the chunk and the failing activity. A resumable closure is resumed only once
    the real admission (reserve() -> resume_worker()) accepted it; a refusal
    stops the feature with the worker's own reason, its chunk and the refusal.
    Infrastructure during an observation or the admission re-issues exactly what
    the coordinator already holds (the assignment it observed, or the closure it
    offered) after a pause: a worker that was observed is never reserved again,
    so no closed worker re-enters reserve() without passing `_resumable` and the
    refusal branch first. Admitted resumes and transport re-issues are counted
    apart (`feature_worker_recoveries`, `feature_worker_reissues`), each under
    its own bound, so a transport retry never spends a resume.
    """
    key = chunk_id + ":" + kind
    closure = None  # a resumable closure awaiting the real admission
    assignment = None  # an admitted or already observed assignment to continue with
    while True:
        resumed = controller.state["checks"].get("feature_worker_recoveries", {}).get(key, 0)
        try:
            if closure is not None:
                assignment = await _admit(controller, spec, chunk_id, kind, closure)
                closure = None
                await _recovery(controller, spec, "feature_worker_recoveries", key, resumed,
                                AUTOMATIC_RECOVERIES,
                                f"Resumed the {kind} worker for {chunk_id} after an "
                                "infrastructure failure")
                resumed += 1
            return await _worker(controller, spec, chunk_id, kind, active,
                                 resume_allowed=resumed < AUTOMATIC_RECOVERIES,
                                 assignment=assignment)
        except PlanningCorrectionRequired:
            raise
        except Exception as exc:
            if controller.cancel_requested:
                raise
            if not _infrastructure_failure(exc):
                if isinstance(exc, ActivityError):
                    # A verdict from reserve, wait or finish: name the chunk and the activity.
                    raise FeatureWorkerStopped(
                        chunk_id, kind, closure=closure, final=True,
                        detail=f"{exc.activity_type} failed: {_describe(exc)}") from exc
                raise
            if isinstance(exc, FeatureWorkerStopped):
                if resumed >= AUTOMATIC_RECOVERIES:
                    raise FeatureWorkerStopped(
                        chunk_id, kind, final=True, closure=exc.closure,
                        detail=f"exhausted {resumed} automatic resumes: {exc}") from exc
                closure, assignment = exc.closure, None
                continue
            reissued = controller.state["checks"].get("feature_worker_reissues", {}).get(key, 0)
            if reissued >= REISSUES:
                cause = _describe(exc) if isinstance(exc, ActivityError) else exc
                detail = f"exhausted {reissued} re-issues: {cause}"
                if closure is not None:
                    detail = f"stopped ({closure.get('error') or 'blocked'}); " + detail
                raise FeatureWorkerStopped(
                    chunk_id, kind, final=True, detail=detail,
                    closure=closure or getattr(exc, "closure", None)) from exc
            if isinstance(exc, ObservationLost):
                assignment = exc.assignment
            held = ("admission" if closure is not None
                    else "observation" if assignment is not None else "assignment")
            await _recovery(controller, spec, "feature_worker_reissues", key, reissued, REISSUES,
                            f"Re-issuing the {kind} worker {held} for {chunk_id} after "
                            f"{_describe(exc)}")
            await _pause(controller, OBSERVATION_PAUSE)


async def _coordinate_pass(controller, spec, active, builds):
    completed = set()
    resilient = _liveness(controller)
    start = _recovering_worker if resilient else _worker
    stopped = None
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
                        start(controller, spec, ready["id"], "build", active)
                    )
            if resilient:
                try:
                    await builds[chunk["id"]]
                except PlanningCorrectionRequired:
                    raise
                except Exception as exc:
                    stopped = exc
                    break
            else:
                await builds[chunk["id"]]
            if controller.cancel_requested:
                break
            controller.state["phase"] = "feature_integration"
            controller.state["revision"] += 1
            await controller._project(spec, "chunk_integrating", "Integrating " + chunk["title"])
            if resilient:
                try:
                    finished = await start(controller, spec, chunk["id"], "chunk", active)
                except PlanningCorrectionRequired:
                    raise
                except Exception as exc:
                    stopped = exc
                    break
            else:
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
        if stopped is not None:
            # Sibling builds are independent of the stopped chunk; only the stack
            # order is blocked. Let them reach their own sealed closure before
            # stopping, so a continuation reuses that work instead of redoing it.
            running = [task for task in builds.values() if not task.done()]
            if running:
                controller.state["revision"] += 1
                await controller._project(
                    spec, "feature_worker_stopped",
                    f"{str(stopped)[:300]}; waiting for {len(running)} running build(s) "
                    "to close before stopping")
                await asyncio.gather(*running, return_exceptions=True)
            raise stopped
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
                pending_polls = 0
                delay = READBACK_INTERVAL
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
                    pending_polls += 1
                    if not resilient:
                        await workflow.sleep(READBACK_INTERVAL)
                        continue
                    if pending_polls == 2:
                        # One projection names the pending merge. History then grows
                        # only at the backed-off cadence, and a slow but successful
                        # merge still ends this feature merged.
                        numbers = ", ".join(
                            "#" + str(item["number"])
                            for item in controller.state["pull_request"]["pull_requests"])
                        controller.state["revision"] += 1
                        await controller._project(
                            spec, "feature_merging",
                            f"Stack merge submitted for {numbers}; GitHub readback still "
                            f"pending, polling with backoff up to "
                            f"{int(MERGE_READBACK_CAP.total_seconds() // 60)} minutes")
                    await workflow.sleep(delay)
                    delay = min(delay * 2, MERGE_READBACK_CAP)
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
            limit = 600 if isinstance(exc, FeatureWorkerStopped) else 300
            reason = f"feature delivery stopped: {type(exc).__name__}: {str(exc)[:limit]}"
            if resilient and cause is not exc:
                # Name the failing condition, not only the SDK's generic wrapper.
                reason += f" ({_describe(exc)})"
            await controller._stop(spec, reason, cause=exc)
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
    rejected = None
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
        if (adopted.get("state") == "rejected"
                and getattr(controller, "feature_revision_v1", False)):
            # The activity closed the attempt itself after proving nothing reached
            # GitHub, so its custody is released and nothing remote is uncertain.
            adopting = False
            rejected = adopted["plan_revision"]
            raise RuntimeError("planning correction rejected: "
                               + str(adopted.get("reason", ""))[:600])
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
        if rejected is not None:
            controller.state["checks"]["plan_revision"] = rejected
            if rejected.get("budget"):
                controller.state["checks"]["repair_budget"] = rejected["budget"]
        elif context and not adopting:
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
            polls = 0
            delay = READBACK_INTERVAL
            resilient = _liveness(controller)
            unsettled = None
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
                polls += 1
                controller.state.update(phase="waiting_feature_readback", execution_state="waiting",
                                        outcome=None)
                controller.state["revision"] += 1
                waiting = True
                if not resilient:
                    await controller._project(
                        spec, "feature_readback_pending",
                        "Reconciling the original GitHub operation"
                    )
                    await workflow.sleep(READBACK_INTERVAL)
                    continue
                effect = readback.get("effect_key") or "readback"
                reason = readback.get("reason") or "pending"
                if controller.cancel_requested or polls >= READBACK_POLLS:
                    unsettled = (effect, reason)
                    break
                # Nothing but this coordinator settles a pending external operation.
                # Back off to bound history growth, name what is stuck, and leave on
                # an operator cancel or at the bound without deciding the effect.
                await controller._project(
                    spec, "feature_readback_pending",
                    f"Original GitHub operation {effect} still unconfirmed after {polls} "
                    f"readbacks ({reason}); next readback in {int(delay.total_seconds())} s")
                await _pause(controller, delay)
                delay = min(delay * 2, READBACK_SLOW_INTERVAL)
            settled = "Original operations settled; feature can be continued"
            if unsettled is not None:
                # Never turn an unconfirmed effect into a confirmed or absent one.
                # registry.stop() refuses a pending effect, so the stop activity is
                # withheld: the owner keeps the effect and the reason names it.
                effect, reason = unsettled
                cancelled = controller.cancel_requested
                terminal = {
                    "phase": "cancelled" if cancelled else "blocked",
                    "execution_state": "terminal" if cancelled else "blocked",
                    "outcome": "cancelled" if cancelled else "blocked", "cleanup": "unknown",
                    "error": f"original GitHub operation {effect} still unconfirmed after "
                             f"{polls} readbacks ({reason}); the feature owner keeps it pending",
                }
                settled = terminal["error"]
                controller.state["checks"]["feature_stop"] = {
                    "state": "withheld", "effect_key": effect, "reason": reason}
            elif controller.cancel_requested and terminal["outcome"] != "delivered":
                terminal.update(phase="cancelled", execution_state="terminal", outcome="cancelled")
            controller.state.update(terminal)
            if waiting:
                controller.state["revision"] += 1
                await controller._project(spec, terminal["outcome"], settled)
            if unsettled is None:
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
