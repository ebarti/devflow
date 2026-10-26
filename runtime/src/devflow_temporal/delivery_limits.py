"""Workflow-owned delivery clocks and a compact, reference-only timeout handoff."""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from temporalio import workflow
from temporalio.exceptions import ApplicationError

from .contracts import digest
from .delivery_control_lineage import control_from_spec

KEYS = ("delivery_timeout_seconds", "no_progress_timeout_seconds")
SETTLEMENT = frozenset({
    "delivery_project", "delivery_finalize_resources", "delivery_terminal_tracker",
    "delivery_feature_settle_workers", "delivery_feature_settle_effects", "delivery_feature_stop",
    "delivery_merge", "delivery_feature_merge",
})


def policy(raw):
    result = {}
    for key in KEYS:
        value = raw.get(key)
        if value is not None:
            if type(value) is not int or not 1 <= value <= 86400:
                raise ValueError(key + " must be an integer from 1 to 86400, or null")
            result[key] = value
    return result


def require_supported_admission_limits(raw):
    # Frozen configurations remain readable and replayable; reject only new work.
    if raw.get("no_progress_timeout_seconds") is not None:
        raise ValueError(
            "stage-based no-progress delivery timeout is not supported; remove "
            "no_progress_timeout_seconds from the service configuration"
        )


def _progress(state):
    return digest({
        "candidate": (state.get("candidate") or {}).get("id"),
        "plan": (state.get("intake") or {}).get("accepted_plan"),
        "roles": [(r.get("role"), r.get("iteration"), r.get("status"), r.get("session_id"))
                  for r in state.get("roles", [])],
        "checks": {k: (v.get("state"), v.get("candidate_id"))
                   for k, v in state.get("checks", {}).items()
                   if k in {"baseline", "prepublish", "local", "browser_qa", "review", "qa", "ci"}
                   and isinstance(v, dict) and v.get("state") in {"passed", "failed"}},
        "publication": state.get("pull_request"),
    })


def observe(controller):
    control = getattr(controller, "delivery_limits_control", None)
    if control:
        controller.state.setdefault("checks", {})["delivery_limits_control"] = dict(control)
    if controller.delivery_clock is None:
        return
    signature = _progress(controller.state)
    clock = controller.delivery_clock
    if clock.get("signature") is None:
        clock["signature"] = signature
    elif signature != clock["signature"]:
        clock.update(signature=signature, last_progress_at=workflow.now().isoformat())
    controller.state.setdefault("checks", {})["delivery_clock"] = dict(clock)


def remaining(controller):
    if getattr(controller, "delivery_limits_disabled", False):
        return None
    clock = controller.delivery_clock
    limits = controller.delivery_limit_policy
    deadlines = []
    for key, anchor, kind in (
        (KEYS[0], clock["admitted_at"], "elapsed"),
        (KEYS[1], clock["last_progress_at"], "no_progress"),
    ):
        if key in limits:
            end = datetime.fromisoformat(anchor) + timedelta(seconds=limits[key])
            deadlines.append(((end - workflow.now()).total_seconds(), kind, end.isoformat()))
    return min(deadlines)


async def disable(controller, request):
    """A recorded operator update removes timers without interrupting active work."""
    await workflow.wait_condition(lambda: bool(controller.state))
    if (controller.state.get("outcome") is not None or controller.cancel_requested
            or controller.delivery_limit_reached or controller.delivery_limits_closing):
        raise ApplicationError("delivery is already stopping or terminal", non_retryable=True)
    if (type(request.get("expected_revision")) is not int
            or request["expected_revision"] != controller.state["revision"]):
        raise ApplicationError("stale run revision", non_retryable=True)
    reason = request.get("reason")
    command = request.get("command_id")
    if (not isinstance(reason, str) or not reason.strip() or len(reason) > 4000
            or not isinstance(command, str) or not command.strip() or len(command) > 128):
        raise ApplicationError("command ID and bounded disable reason are required",
                               non_retryable=True)
    controller.delivery_limits_disabled = True
    controller.delivery_limits_control = {
        "disabled": True, "command_id": command, "reason": reason.strip(),
        "recorded_at": workflow.now().isoformat(),
        "protocol_revision": controller.state["revision"] + 1,
    }
    controller.state.setdefault("checks", {})["delivery_limits_control"] = dict(
        controller.delivery_limits_control)
    controller.state["revision"] += 1
    return controller.state


def handoff(spec, state):
    return {
        "run_id": spec["run_id"], "work_id": spec.get("work_id"),
        "issue_url": spec.get("issue_url"), "candidate": state.get("candidate"),
        "pull_request": state.get("pull_request"), "evidence_root": spec.get("state_dir"),
        "accepted_plan_sha256": digest(spec.get("accepted_plan")),
        "implementation_sessions": sorted({r["session_id"] for r in state.get("roles", [])
                                           if r.get("role") == "implement"
                                           and r.get("session_id")}),
        "instruction": "Reuse retained work after confirmed cleanup and ownership release. "
                       "Do not create concurrent ownership or reset repair accounting.",
    }


async def expire(controller, spec, kind, deadline):
    if controller.delivery_limit_reached or controller.delivery_limits_closing:
        return
    controller.delivery_limit_reached = True
    controller.cancel_requested = True
    reason = "delivery " + kind.replace("_", "-") + " time limit reached"
    controller.state.update(phase="cancelling", execution_state="cancelling", error=reason)
    controller.state["checks"]["delivery_limit"] = {
        "kind": kind, "deadline": deadline, "observed_at": workflow.now().isoformat(),
        "handoff": handoff(spec, controller.state),
    }
    controller.state["revision"] += 1
    # The durable phase also tells native process monitors to stop before another command.
    try:
        await controller._project(spec, "delivery_limit", reason)
    except workflow.NondeterminismError:
        raise
    except Exception as exc:
        controller.state["checks"]["delivery_limit"]["projection_error"] = type(exc).__name__
    finally:
        for handle in list(controller.delivery_active_activities):
            handle.cancel()


async def _watch(controller, spec):
    await workflow.wait_condition(lambda: bool(controller.state))
    while not controller.cancel_requested and not controller.delivery_limits_closing:
        pending = remaining(controller)
        if pending is None:
            return
        delay, kind, deadline = pending
        if delay <= 0:
            await expire(controller, controller.controller_spec or spec, kind, deadline)
            return
        prior = controller.delivery_clock["last_progress_at"]
        try:
            await workflow.wait_condition(
                lambda previous=prior: controller.cancel_requested
                or controller.delivery_limits_closing
                or controller.delivery_limits_disabled
                or controller.delivery_clock["last_progress_at"] != previous,
                timeout=timedelta(seconds=delay),
            )
        except TimeoutError:
            continue


async def run(controller, spec, recovery, operation):
    control = (((recovery or {}).get("state") or {}).get("checks", {})
               .get("delivery_limits_control", {}))
    inherited = control_from_spec(spec)
    controller.delivery_limits_control = (dict(control) if control.get("disabled") is True
                                          else inherited)
    controller.delivery_limits_disabled = controller.delivery_limits_control is not None
    if spec.get("delivery_limits_version") != 1:
        return await operation(spec, recovery)
    # A sealed terminal tail permits only settlement. Restore its clock mode
    # before either the watcher or the first preflight can evaluate product time.
    # The patch leaves already recorded terminal-recovery histories replayable.
    if ((recovery or {}).get("kind") == "terminal_tracker_recovery"
            and (not workflow.in_workflow()
                 or workflow.patched("delivery-terminal-settlement-clock-v1"))):
        controller.delivery_limits_closing = True
    controller.delivery_limit_policy = policy(spec["policy"])
    if not controller.delivery_limit_policy and not controller.delivery_limits_disabled:
        raise ValueError("bounded delivery requires its frozen time policy")
    admitted = spec["delivery_admitted_at"]
    previous = ((recovery or {}).get("state") or {}).get("checks", {}).get("delivery_clock", {})
    controller.delivery_clock = {
        "admitted_at": admitted, "last_progress_at": previous.get("last_progress_at", admitted),
        "signature": previous.get("signature"),
    }
    watch = asyncio.create_task(_watch(controller, spec))
    try:
        try:
            return await operation(spec, recovery)
        except asyncio.CancelledError:
            if not controller.delivery_limit_reached:
                raise
            return await controller._cancelled(controller.controller_spec or spec)
    finally:
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
