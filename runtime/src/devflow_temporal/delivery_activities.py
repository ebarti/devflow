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
from .delivery_config import DeliveryConfig
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
    broker = DeliveryBroker(store, spec)
    if broker.evidence_dir != broker.state_dir:
        namespace = str(broker.evidence_dir.relative_to(broker.state_dir).parent)
        from .delivery_resources import private_directory

        private_directory(broker.evidence_dir)
        broker.effect_namespace = ":" + namespace
    return store, broker


@activity.defn(name="delivery_resource_closure_readback")
async def delivery_resource_closure_readback(request: dict[str, Any]) -> dict[str, Any]:
    def execute():
        from .delivery_resource_closure import readback

        store, _ = _context(request['spec'])
        return readback(store, request['spec'], request['recovery'])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_adjudication_readback")
async def delivery_adjudication_readback(request: dict[str, Any]) -> dict[str, Any]:
    def execute():
        from .delivery_investigation_adjudication import readback

        store, _ = _context(request['spec'])
        return readback(store, request['spec'], request['recovery'])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_technical_readback")
async def delivery_technical_readback(request: dict[str, Any]) -> dict[str, Any]:
    def execute():
        from .delivery_technical_continuation import readback

        store, _ = _context(request['spec'])
        return readback(store, request['spec'], request['recovery'])

    return await asyncio.to_thread(execute)


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

    pending = asyncio.create_task(asyncio.to_thread(execute))
    try:
        while not pending.done():
            if activity.in_activity() and request["spec"].get("preparation_version") == 1:
                activity.heartbeat({"run_id": request["spec"]["run_id"], "stage": "preparing"})
            await asyncio.wait({pending}, timeout=5)
        return await pending
    except Exception as exc:
        raise ApplicationError(
            str(exc)[:600], type="RuntimePreparationFailed", non_retryable=True
        ) from exc
    finally:
        pending.cancel()


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


@activity.defn(name="delivery_finalize_resources")
async def delivery_finalize_resources(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        from .delivery_resources import RunResources

        _context(request["spec"], preparation_input=True)
        if request["spec"].get("resource_cleanup_version") != 1:
            raise ApplicationError("run has no resource finalization contract", non_retryable=True)
        receipt = RunResources(request["spec"]).finalize(
            request["outcome"], uncertain=request.get("uncertain", False)
        )
        if receipt.get("retryable"):
            raise ApplicationError(
                "owned temporary resource removal needs a bounded retry",
                type="ResourceCleanupTransient",
            )
        return receipt

    pending = asyncio.create_task(asyncio.to_thread(execute))
    try:
        while not pending.done():
            if activity.in_activity():
                activity.heartbeat(
                    {"run_id": request["spec"]["run_id"], "stage": "finalizing_resources"}
                )
            await asyncio.wait({pending}, timeout=5)
        return await pending
    finally:
        pending.cancel()


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
        request["plan_digest"], request["plan"], authorization=request.get("authorization"),
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
    constraint = request.get("title_constraint")
    with store._connect() as db:
        saved = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id=?",
            (request["spec"]["run_id"],),
        ).fetchone()[0] or "null")
    if saved and saved.get("kind") == "stopped_resource_closure":
        from .delivery_investigation_adjudication import _controller
        from .delivery_resource_closure import custody

        if role != "verify" or iteration != 4:
            raise ValueError("resource closure cannot launch a coding or review turn")
        with store._connect() as db:
            custody(db, saved)
        controller = _controller(store, request["spec"], saved["command"])
        request = {**request, "execution_role_policy": controller["active_config"]["roles"][role]}
    if constraint:
        from .delivery_title_repair import validate_source

        with store._connect() as db:
            saved = json.loads(db.execute(
                "SELECT recovery_json FROM delivery_runs WHERE run_id=?",
                (request["spec"]["run_id"],),
            ).fetchone()[0])
        if (role != "implement" or iteration != 5
                or digest(saved.get("title_constraint")) != digest(constraint)
                or request.get("resume_session") != saved["session_id"]):
            raise ValueError("title repair role does not match its sealed single turn")
        validate_source(request["spec"], constraint, completed=False)
    result = await get_supervisor(store).run(
        {**request, "workspace": str(workspace), "review_diff": review_diff}
    )
    if constraint:
        try:
            validate_source(request["spec"], constraint, completed=True)
        except (ValueError, OSError, UnicodeError) as exc:
            result["status"] = "blocked"
            result.setdefault("findings", []).append(str(exc))
    if role == "implement":
        after = broker.candidate()
        if result.get("status") == "pass":
            changed = broker._changed_paths(request["spec"]["base_sha"])
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


@activity.defn(name="delivery_metadata_readback")
async def delivery_metadata_readback(request: dict[str, Any]) -> dict[str, Any]:
    def execute():
        from .delivery_metadata_recovery import validation_readback

        store, _ = _context(request["spec"])
        return validation_readback(store, request["spec"], request["recovery"])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_gates_readback")
async def delivery_gates_readback(request: dict[str, Any]) -> dict[str, Any]:
    def execute():
        from .delivery_gates_admission import readback

        store, _ = _context(request["spec"])
        return readback(store, request["spec"], request["recovery"])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_repair_preflight")
async def delivery_repair_preflight(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        try:
            store, _ = _context(request["spec"])
            if request["recovery"].get("kind") == "execution_policy_recovery":
                from .delivery_policy_recovery import resume_preflight

                resume_preflight(store, request["spec"], request["recovery"])
            elif request["recovery"].get("kind") == "scope_amendment":
                store.scope_preflight(request["spec"], request["recovery"])
            else:
                store.repair_preflight(request["spec"], request["recovery"])
        except (
            RepairReadbackPending,
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


def _terminal_receipt(store, spec, status, release, project, assignee, desired):
    with store._connect() as db:
        db.execute("BEGIN")
        work = store.state.row(db, "works", spec["work_id"])
        sync = json.loads(work["details"] or "{}").get("github", {}).get("sync", {}) if work else {}
        intent = db.execute("SELECT * FROM reconcile_intents WHERE work_id=?",
                            (spec["work_id"],)).fetchone()
        claim = store.state.claim_for(db, spec["work_id"])
        from .delivery_policy_recovery import work_binding

        try:
            work_binding(store, spec, db)
        except ValueError as exc:
            return {"state": "pending", "pending": True, "desired": desired,
                    "reason": str(exc), "readback_at": _now()}
    owner = f"external:devflow:{spec['run_id']}"
    issue = spec["issue_url"]
    if (not work or store.state.issue_resource(work["issue"])
            != store.state.issue_resource(issue)):
        return {"state": "pending", "pending": True, "desired": desired,
                "reason": "terminal work issue differs from frozen authority",
                "readback_at": _now()}
    payload = json.loads(intent["payload"]) if intent else {}
    if intent and store.state.issue_resource(payload.get("issue", "")) != (
        store.state.issue_resource(issue)
    ):
        return {"state": "pending", "pending": True, "desired": desired,
                "reason": "terminal intent issue differs from frozen authority",
                "readback_at": _now()}
    if not intent or intent["state"] != "acknowledged":
        if claim is not None and claim["owner"] == owner:
            return None  # The owning helper can resume its observed pending intent.
        return {"state": "pending", "pending": True, "desired": desired,
                "reason": "terminal ownership or acknowledgement is unavailable",
                "readback_at": _now()}
    if (intent["owner"] != owner or payload.get("status") != status
            or bool(payload.get("release")) != release or sync.get("status") != status
            or sync.get("issue_state") != "OPEN" or sync.get("project") != project
            or payload.get("assignee") != assignee or payload.get("project") != project
            or (assignee != "@me"
                and sync.get("assignee", "").casefold() != assignee.lstrip("@").casefold())
            or not sync.get("readback_at") or bool(claim) != (not release)
            or (claim is not None and claim["owner"] != owner)):
        if claim is not None and claim["owner"] == owner:
            return None  # The preceding owned transition is not this terminal intent.
        return {"state": "pending", "pending": True, "desired": desired,
                "reason": "released terminal helper acknowledgement conflicts",
                "readback_at": _now()}
    return {"state": "consistent", "pending": False, "desired": desired,
            "observed": {"state": "consistent", "claim": claim, "expected": sync,
                         "source": "owning helper live readback and acknowledged intent"},
            "readback_at": sync["readback_at"]}


def _tracker_sync(spec: dict[str, Any], status: str, *, release: bool,
                  terminal: bool = False, reason: str | None = None) -> dict[str, Any]:
    store, _ = _context(spec)
    guarded = spec.get("terminal_tracker_version") == 1
    if guarded and not terminal:
        from .delivery_policy_recovery import work_binding

        with store._connect() as db:
            work_binding(store, spec, db)
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
    if terminal:
        prior = _terminal_receipt(store, spec, status, release, project, assignee, desired)
        if prior is not None:
            if prior["state"] != "consistent":
                return prior
            # A lost activity/helper completion may already have released the claim.
            # Observe its acknowledged intent and current remote state; never set again.
            audit = subprocess.run(
                [sys.executable, str(script), "--db", str(store.config.tracking_db),
                 "audit", "--work-id", spec["work_id"]],
                text=True, capture_output=True, check=False, timeout=120,
            )
            observed = json.loads(audit.stdout) if audit.returncode == 0 else {}
            current = _terminal_receipt(store, spec, status, release, project, assignee, desired)
            if (audit.returncode or observed.get("state") != "consistent"
                    or store.state.issue_resource(observed.get("issue", ""))
                    != store.state.issue_resource(spec["issue_url"])
                    or current != prior or observed.get("expected") != prior["observed"]["expected"]
                    or bool(observed.get("claim")) != (not release)):
                return {"state": "pending", "pending": True, "desired": desired,
                        "observed": observed, "reason": "acknowledged terminal readback is pending",
                        "readback_at": _now()}
            return {**prior, "observed": observed, "readback_at": _now()}
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
    project_status = repository.get("project_statuses", {}).get(status)
    if project_status:
        command.extend(["--project-status", project_status])
    if status == "blocked":
        command.extend(["--reason", reason or "Managed delivery stopped at a terminal boundary"])
    if release:
        command.append("--release")
    result = subprocess.run(command, text=True, capture_output=True, check=False, timeout=120)
    if guarded:
        from .delivery_policy_recovery import work_binding

        with store._connect() as db:
            work_binding(store, spec, db)
    if result.returncode:
        return {
            "state": "pending",
            "desired": desired,
            "pending": True,
            "reason": (result.stderr or result.stdout).strip()[:500],
        }
    if terminal:
        # The owning helper already performs live issue/assignee/Project readback
        # and acknowledges that exact intent atomically before releasing ownership.
        # A second remote audit after release cannot safely change the transition.
        acknowledged = json.loads(result.stdout)
        receipt = _terminal_receipt(store, spec, status, release, project, assignee, desired)
        sync = receipt.get("observed", {}).get("expected", {}) if receipt else {}
        if (not receipt or receipt["state"] != "consistent"
                or acknowledged.get("status") != status
                or acknowledged.get("assignee") != sync.get("assignee")
                or acknowledged.get("project_status") != sync.get("project_status")):
            return {"state": "pending", "pending": True, "desired": desired,
                    "reason": "terminal helper acknowledgement changed", "readback_at": _now()}
        return receipt
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
    if guarded:
        with store._connect() as db:
            work_binding(store, spec, db)
        if store.state.issue_resource(observed.get("issue", "")) != (
            store.state.issue_resource(spec["issue_url"])
        ):
            raise ValueError("tracker audit differs from frozen issue authority")
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


@activity.defn(name="delivery_terminal_tracker")
async def delivery_terminal_tracker(request: dict[str, Any]) -> dict[str, Any]:
    if request["spec"]["provider"] == "fake":
        return {"state": "consistent", "pending": False, "observed": {"fixture": True}}
    try:
        if request["status"] == "in-review" and request.get("pull_request") is not None:
            from .delivery_terminal_recovery import published_readback

            store, _ = _context(request["spec"])
            await asyncio.to_thread(published_readback, store, request["spec"],
                                    request.get("candidate"), request["pull_request"])
        return await asyncio.to_thread(
            _tracker_sync, request["spec"], request["status"], release=request["release"],
            terminal=True, reason=request.get("reason"),
        )
    except Exception as exc:
        return {
            "state": "pending", "pending": True, "reason": type(exc).__name__,
            "desired": f"{request['status']}; claim "
                       + ("released" if request["release"] else "retained pending cleanup"),
            "readback_at": _now(),
        }


@activity.defn(name="delivery_terminal_preflight")
async def delivery_terminal_preflight(request):
    def execute():
        from .delivery_terminal_recovery import preflight

        store, _ = _context(request["spec"])
        preflight(store, request["spec"], request["recovery"])
        return {"state": "confirmed"}

    return await asyncio.to_thread(execute)


DELIVERY_ACTIVITIES = [
    delivery_terminal_preflight,
    delivery_terminal_tracker,
    delivery_project,
    delivery_prepare,
    delivery_finalize_resources,
    delivery_intake,
    delivery_accept_plan,
    delivery_role,
    delivery_publish,
    delivery_reconcile_publish,
    delivery_metadata_readback,
    delivery_gates_readback,
    delivery_technical_readback,
    delivery_adjudication_readback,
    delivery_resource_closure_readback,
    delivery_repair_preflight,
    delivery_checks,
    delivery_browser_qa,
    delivery_precheck,
    delivery_ci,
    delivery_tracker_start,
    delivery_tracker,
]
