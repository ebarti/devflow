"""Effectful activities used by the managed Temporal delivery protocol."""

from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import subprocess
import sys
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .candidate import candidate_for
from .contracts import digest
from .delivery_broker import DeliveryBroker
from .delivery_config import ContainerReadbackPending, DeliveryConfig
from .delivery_repair import RepairReadbackPending
from .delivery_store import DeliveryStore, _now
from .supervisor import get_supervisor


def _context(
    spec: dict[str, Any], *, preparation_input: bool = False
) -> tuple[DeliveryStore, DeliveryBroker]:
    config = DeliveryConfig.load(Path(spec["config_path"]))
    if digest(config.raw) != spec["config_digest"]:
        raise ValueError("service configuration changed during an active run")
    store = DeliveryStore(config)
    saved = store.effective_spec(spec["run_id"])
    if saved != spec and not (
        preparation_input and spec.get("preparation_version") == 1
        and store.submitted_spec(spec["run_id"]) == spec
    ):
        raise ValueError("Temporal input no longer matches the durable submitted run")
    return store, DeliveryBroker(store, spec)


@activity.defn(name="delivery_project")
async def delivery_project(request: dict[str, Any]) -> dict[str, Any]:
    store, _ = _context(request["spec"], preparation_input=True)
    result = store.project(
        request["spec"]["run_id"],
        phase=request["phase"],
        execution_state=request["execution_state"],
        event_type=request["event_type"],
        message=request["message"],
        candidate=request.get("candidate"),
        pull_request=request.get("pull_request"),
        checks=request.get("checks"),
        tracker=request.get("tracker"),
        usage=request.get("usage"),
        decision=request.get("decision"),
        intake=request.get("intake"),
        iteration=request.get("iteration"),
        protocol_revision=request.get("protocol_revision"),
        outcome=request.get("outcome"),
        cleanup=request.get("cleanup"),
        error=request.get("error"),
        key=request.get("key"),
    )
    return {"revision": result["revision"], "phase": result["phase"]}


@activity.defn(name="delivery_prepare")
async def delivery_prepare(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        from .delivery_preparation import prepare_authority

        store, _ = _context(request["spec"], preparation_input=True)
        effective = prepare_authority(store, request["spec"])
        prepared = DeliveryBroker(store, effective).prepare()
        if effective.get("preparation_version") == 1:
            return {**prepared, "spec": effective}
        return prepared

    try:
        return await asyncio.to_thread(execute)
    except Exception as exc:
        raise ApplicationError(
            str(exc)[:600], type="RuntimePreparationFailed", non_retryable=True
        ) from exc


@activity.defn(name="delivery_intake")
async def delivery_intake(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = _context(request["spec"])
    candidate = request["candidate"]
    if broker.candidate() != candidate:
        raise ValueError("intake checkout changed before investigation")
    result = await get_supervisor(store).run(
        {**request, "role": "intake", "workspace": str(broker.checkout)}
    )
    if broker.candidate() != candidate:
        result["status"] = "blocked"
        result.setdefault("findings", []).append("intake changed the read-only checkout")
    return {
        **result, "role": "intake", "iteration": request["iteration"],
        "input_candidate_id": candidate["id"], "candidate": candidate,
        "provider": request["spec"]["provider"],
    }


@activity.defn(name="delivery_accept_plan")
async def delivery_accept_plan(request: dict[str, Any]) -> dict[str, Any]:
    spec = request["spec"]
    config = DeliveryConfig.load(Path(spec["config_path"]))
    if digest(config.raw) != spec["config_digest"]:
        raise ValueError("service configuration changed during intake")
    store = DeliveryStore(config)
    saved = store.effective_spec(spec["run_id"])
    if {key: value for key, value in saved.items() if key != "accepted_plan"} != {
        key: value for key, value in spec.items() if key != "accepted_plan"
    }:
        raise ValueError("intake authority changed before acceptance")
    return store.accept_intake_plan(
        request["spec"]["run_id"], request["plan_revision"],
        request["plan_digest"], request["plan"],
    )


@activity.defn(name="delivery_role")
async def delivery_role(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = _context(request["spec"])
    role = request["role"]
    iteration = request["iteration"]
    candidate = request["candidate"]
    if role == "implement":
        workspace = broker.checkout
        review_diff = None
        if broker.candidate() != candidate:
            raise ValueError("implementer checkout changed before its role")
    elif role in {"review", "verify"}:
        workspace = broker.gate_checkout(role, iteration, candidate)
        review_diff = broker.gate_diff(role, iteration, candidate)
    else:
        raise ValueError("unknown delivery role")
    result = await get_supervisor(store).run(
        {**request, "workspace": str(workspace), "review_diff": review_diff}
    )
    if role == "implement":
        after = broker.candidate()
        if result.get("status") == "pass":
            changed = subprocess.run(
                [
                    "git",
                    "-C",
                    str(broker.checkout),
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=30,
            ).stdout.strip()
            if not changed:
                result["status"] = "blocked"
                result.setdefault("findings", []).append(
                    "implementer candidate has no feature diff"
                )
            elif after["id"] == candidate["id"] and not (
                request.get("continuation") and iteration == 0
            ):
                result["status"] = "blocked"
                result.setdefault("findings", []).append("implementer produced no candidate change")
    else:
        observed = candidate_for(workspace)
        source = broker.candidate()
        with Path(review_diff["path"]).open("rb") as stream:
            diff_sha = hashlib.file_digest(stream, "sha256").hexdigest()
        if (
            observed["id"] != candidate["id"]
            or source != candidate
            or diff_sha != review_diff["sha256"]
        ):
            result["status"] = "blocked"
            result.setdefault("findings", []).append(
                "candidate or controller diff changed during independent gate"
            )
        after = candidate
    return {
        **result,
        "role": role,
        "iteration": iteration,
        "input_candidate_id": candidate["id"],
        "candidate": after,
        "provider": request["spec"]["provider"],
    }


@activity.defn(name="delivery_publish")
async def delivery_publish(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        _, broker = _context(request["spec"])
        return broker.publish(request["iteration"], request["candidate"])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_reconcile_publish")
async def delivery_reconcile_publish(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        _, broker = _context(request["spec"])
        return broker.reconcile_publish(
            request["iteration"],
            request["candidate"],
            expected_head=request.get("expected_head"),
            expected_pr_number=request.get("expected_pr_number"),
        )

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_repair_preflight")
async def delivery_repair_preflight(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        try:
            store, _ = _context(request["spec"])
            if request["recovery"].get("kind") == "scope_amendment":
                store.scope_preflight(request["spec"], request["recovery"])
            elif request["recovery"].get("kind") == "precheck_prelaunch_recovery":
                store.precheck_recovery_preflight(request["spec"], request["recovery"])
            else:
                store.repair_preflight(request["spec"], request["recovery"])
        except (
            RepairReadbackPending,
            ContainerReadbackPending,
            subprocess.TimeoutExpired,
            sqlite3.OperationalError,
        ) as exc:
            return {"state": "pending", "reason": type(exc).__name__}
        return {"state": "confirmed"}

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_checks")
async def delivery_checks(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        _, broker = _context(request["spec"])
        try:
            return broker.run_checks(request["iteration"], request["candidate"])
        except Exception as exc:
            if request["spec"]["provider"] != "codex":
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_browser_qa")
async def delivery_browser_qa(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        _, broker = _context(request["spec"])
        try:
            return broker.run_browser_qa(request["iteration"], request["candidate"])
        except Exception as exc:
            if request["spec"]["provider"] != "codex":
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    # Browser/API fixtures may run for minutes. Keep the Temporal worker loop
    # available for cancellation updates and unrelated workflows while this
    # bounded child is supervised on its own thread.
    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_precheck")
async def delivery_precheck(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        _, broker = _context(request["spec"])
        try:
            return broker.run_prechecks(request["iteration"], request["candidate"])
        except Exception as exc:
            if request["spec"]["provider"] != "codex":
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_ci")
async def delivery_ci(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    return await broker.checks(request["pull_request"])


def _tracker_sync(spec: dict[str, Any], status: str, *, release: bool) -> dict[str, Any]:
    store, _ = _context(spec)
    repository = store.config.raw["repositories"][spec["repository_key"]]
    project = repository.get("project_url")
    assignee = repository.get("assignee")
    desired = (
        f"{status}; assignee @{assignee or 'unconfigured'}; "
        f"project {project or 'unconfigured'}; "
        f"claim {'released' if release else 'retained by external:devflow:' + spec['run_id']}"
    )
    if not project or not assignee:
        return {
            "state": "unconfigured",
            "desired": desired,
            "pending": True,
            "reason": "tracker project or assignee is missing",
        }
    script = store.config.helpers_dir / "github.py"
    owner = f"external:devflow:{spec['run_id']}"
    command = [
        sys.executable,
        str(script),
        "--db",
        str(store.config.tracking_db),
        "set",
        "--work-id",
        spec["work_id"],
        "--owner",
        owner,
        "--assignee",
        assignee,
        "--project",
        project,
        "--status",
        status,
    ]
    if release:
        command.append("--release")
    result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=120)
    if result.returncode:
        return {
            "state": "pending",
            "desired": desired,
            "pending": True,
            "reason": (result.stderr or result.stdout).strip()[:500],
        }
    audit = subprocess.run(
        [
            sys.executable,
            str(script),
            "--db",
            str(store.config.tracking_db),
            "audit",
            "--work-id",
            spec["work_id"],
        ],
        text=True,
        capture_output=True,
        check=False,
        timeout=120,
    )
    if audit.returncode:
        return {
            "state": "pending",
            "desired": desired,
            "pending": True,
            "reason": (audit.stderr or audit.stdout).strip()[:500],
        }
    observed = json.loads(audit.stdout)
    expected_claim = not release
    if observed.get("state") != "consistent" or bool(observed.get("claim")) != expected_claim:
        return {
            "state": "pending",
            "desired": desired,
            "observed": observed,
            "pending": True,
            "conflict": ", ".join(observed.get("reconciliation_required") or []) or None,
            "readback_at": _now(),
        }
    return {
        "state": "consistent",
        "desired": desired,
        "observed": observed,
        "pending": False,
        "readback_at": _now(),
    }


@activity.defn(name="delivery_tracker_start")
async def delivery_tracker_start(request: dict[str, Any]) -> dict[str, Any]:
    if request["spec"]["provider"] == "fake":
        return {"state": "consistent", "observed": {"fixture": True}}
    try:
        return await asyncio.to_thread(_tracker_sync, request["spec"], "in-progress", release=False)
    except (subprocess.TimeoutExpired, sqlite3.OperationalError) as exc:
        if not request.get("repair_continuation"):
            raise
        return {"state": "pending", "reason": type(exc).__name__}


@activity.defn(name="delivery_tracker")
async def delivery_tracker(request: dict[str, Any]) -> dict[str, Any]:
    return _tracker_sync(request["spec"], "in-review", release=True)


DELIVERY_ACTIVITIES = [
    delivery_project,
    delivery_prepare,
    delivery_intake,
    delivery_accept_plan,
    delivery_role,
    delivery_publish,
    delivery_reconcile_publish,
    delivery_repair_preflight,
    delivery_checks,
    delivery_browser_qa,
    delivery_precheck,
    delivery_ci,
    delivery_tracker_start,
    delivery_tracker,
]
