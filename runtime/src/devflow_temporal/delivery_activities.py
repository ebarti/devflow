"""Effectful activities used by the managed Temporal delivery protocol."""

from __future__ import annotations

import asyncio
import contextvars
import fcntl
import hashlib
import json
import os
import sqlite3
import stat
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from .candidate import candidate_for
from .contracts import digest
from .delivery_broker import (
    BrokerReadbackUnavailable,
    CheckCancelledBeforeLaunch,
    CheckPreparationFailure,
    DeliveryBroker,
)
from .delivery_config import DeliveryConfig
from .delivery_repair import RepairReadbackPending
from .delivery_store import DeliveryStore, _now
from .supervisor import get_supervisor

_CHECK_HEARTBEAT_INTERVAL = 5
_UNCLEAN_CHECK_SLOTS: list[int] = []


def _cancelled_check_result(request: dict[str, Any], broker: DeliveryBroker) -> dict[str, Any]:
    # Rejecting this launch cannot establish cleanup for any earlier child.
    confirmed = broker.native_cleanup_confirmed
    result = {"state": "failed" if confirmed else "unknown",
              "cleanup": "confirmed" if confirmed else "unknown",
              "cancelled": True, "results": []}
    if "candidate" in request:
        result["candidate_id"] = request["candidate"]["id"]
    else:
        result["base_sha"] = request["spec"]["base_sha"]
    return result


def _try_check_lock(path: Path) -> int | None:
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1):
            raise ValueError("check lock is not private and owned")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BlockingIOError:
        os.close(descriptor)
        return None
    except BaseException:
        os.close(descriptor)
        raise


async def _execute_check(
    request: dict[str, Any], execute, *, ports: tuple[int, ...] = (),
) -> dict[str, Any]:
    """Bound native gates across workers and join their monitor on cancellation."""
    cancelled = threading.Event()
    admitted = threading.Event()
    ports_admitted = threading.Event()

    def run():
        from .delivery_resources import private_directory

        store, broker = _context(request["spec"])
        broker.check_cancelled = cancelled.is_set
        if request["spec"]["provider"] != "codex":
            admitted.set()
            return execute(broker)
        root = store.config.state_root / "check-execution"
        private_directory(root)
        slots = store.config.raw.get("check_concurrency", 2)
        descriptor = None
        port_descriptors = []
        try:
            # Frozen port sets may overlap across runs. Acquire them in one
            # consistent order, before consuming generic check capacity.
            for port in sorted(set(ports)):
                while not cancelled.is_set() and not broker._native_cancelled():
                    handle = _try_check_lock(root / f"port-{port}.lock")
                    if handle is not None:
                        port_descriptors.append(handle)
                        break
                    cancelled.wait(0.1)
                else:
                    raise CheckCancelledBeforeLaunch(
                        "native check cancelled while queued for ports"
                    )
            ports_admitted.set()
            while not cancelled.is_set() and not broker._native_cancelled():
                for slot in range(slots):
                    handle = _try_check_lock(root / f"slot-{slot}.lock")
                    if handle is None:
                        continue
                    descriptor = handle
                    break
                if descriptor is not None:
                    if cancelled.is_set() or broker._native_cancelled():
                        raise CheckCancelledBeforeLaunch("native check cancelled before admission")
                    admitted.set()
                    result = execute(broker)
                    if not broker.native_cleanup_confirmed:
                        result = {**result, "state": "unknown", "cleanup": "unknown"}
                    return result
                cancelled.wait(0.1)
            raise CheckCancelledBeforeLaunch("native check cancelled while queued for a slot")
        except CheckCancelledBeforeLaunch:
            return _cancelled_check_result(request, broker)
        finally:
            if descriptor is not None:
                port_descriptors.append(descriptor)
            if broker.native_cleanup_confirmed:
                for handle in port_descriptors:
                    os.close(handle)
            else:
                # Unknown teardown cannot supply ports or capacity to another gate.
                _UNCLEAN_CHECK_SLOTS.extend(port_descriptors)

    execution = asyncio.get_running_loop().run_in_executor(
        None, contextvars.copy_context().run, run,
    )

    async def wait_for_executor():
        return await asyncio.shield(execution)

    pending = asyncio.create_task(wait_for_executor())
    try:
        while not pending.done():
            if activity.in_activity():
                stage = ("executing-check" if admitted.is_set() else
                         "waiting-browser-ports" if ports and not ports_admitted.is_set() else
                         "waiting-check-slot")
                activity.heartbeat({
                    "run_id": request["spec"]["run_id"],
                    "stage": stage,
                })
            await asyncio.wait({pending}, timeout=_CHECK_HEARTBEAT_INTERVAL)
        return await asyncio.shield(pending)
    except BaseException:
        cancelled.set()
        # The task can be cancelled during shutdown without stopping its
        # executor. Join that original future before returning cancellation.
        while not execution.done():
            try:
                await asyncio.shield(execution)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not execution.cancelled():
            execution.exception()
        raise


async def _with_heartbeat(operation, request: dict[str, Any], stage: str) -> dict[str, Any]:
    pending = asyncio.create_task(operation)
    try:
        while not pending.done():
            if activity.in_activity():
                activity.heartbeat({'run_id': request['spec']['run_id'], 'stage': stage})
            await asyncio.wait({pending}, timeout=5)
        return await pending
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


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
    def execute() -> dict[str, Any]:
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

    if request["spec"].get("projection_retry_version") != 1:
        return execute()
    if not isinstance(request.get("key"), str) or not request["key"].strip():
        raise ApplicationError("projection requires its original event key", non_retryable=True)
    try:
        return await asyncio.to_thread(execute)
    except Exception as exc:
        import sqlite3

        code = getattr(exc, "sqlite_errorcode", None)
        transient = isinstance(exc, sqlite3.OperationalError) and (
            (code is not None and code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})
            or str(exc).startswith(("database is locked", "database table is locked",
                                   "database schema is locked", "database is busy"))
        )
        raise ApplicationError(
            f"projection failed: {type(exc).__name__}: {str(exc)[:400]}",
            type="ProjectionContention" if transient else "ProjectionRejected",
            non_retryable=not transient,
        ) from exc


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
    return await _with_heartbeat(_run_intake(request), request, 'intake')


async def _run_intake(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = await asyncio.to_thread(_context, request["spec"])
    candidate = request["candidate"]
    if await asyncio.to_thread(broker.candidate) != candidate:
        raise ValueError("intake checkout changed before investigation")
    result = await get_supervisor(store).run(
        {**request, "role": "intake", "workspace": str(broker.checkout)}
    )
    if await asyncio.to_thread(broker.candidate) != candidate:
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
    return await _with_heartbeat(_run_role(request), request, 'role')


def _role_context(request, store, broker, supervisor):
    role = request["role"]
    iteration = request["iteration"]
    candidate = request["candidate"]
    if role == "implement":
        workspace = broker.checkout
        review_diff = None
    elif role in {"review", "verify"}:
        workspace = broker._gate_path(role, iteration)
        review_diff = None
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
    retained = supervisor.retained_request({
        **request, 'workspace': str(workspace),
        **({'review_diff': review_diff} if role == 'implement' else {}),
    })
    if role in {'review', 'verify'}:
        if retained is not None:
            review_diff = retained['review_diff']
        else:
            workspace = broker.gate_checkout(role, iteration, candidate)
            review_diff = broker.gate_diff(role, iteration, candidate)
    if role == 'implement' and retained is None:
        if broker.candidate() != candidate:
            raise ValueError('implementer checkout changed before its role')
        if constraint:
            validate_source(request['spec'], constraint, completed=False)
    return request, workspace, review_diff, retained


async def _run_role(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = await asyncio.to_thread(_context, request['spec'])
    supervisor = get_supervisor(store)
    request, workspace, review_diff, retained = await asyncio.to_thread(
        _role_context, request, store, broker, supervisor,
    )
    role, iteration, candidate = request['role'], request['iteration'], request['candidate']
    if (role == "implement" and request["spec"].get("provider") == "codex"
            and request["spec"]["policy"].get("host_sandbox") == "trusted-local"
            and retained is None):
        try:
            prerequisites = await asyncio.to_thread(
                broker.run_implementation_preparation, iteration, candidate,
            )
        except CheckCancelledBeforeLaunch:
            prerequisites = _cancelled_check_result(request, broker)
        if prerequisites.get("state") != "passed":
            return {"status": "blocked", "role": role, "iteration": iteration,
                    "candidate": candidate, "cleanup": prerequisites.get("cleanup", "unknown"),
                    "summary": "Controller dependency preparation failed before the role",
                    "findings": ["Required locked execution prerequisites are unavailable"],
                    "session_id": None, "implementation_preparation": prerequisites}
        from .delivery_role_evidence import historical_context

        context = {**historical_context(store, request["spec"]),
                   **(request.get("evidence_context") or {}),
                   "implementation_preparation": prerequisites}
        request = {**request, "evidence_context": context}
    result = await supervisor.run(
        {**request, "workspace": str(workspace), "review_diff": review_diff}
    )
    return await asyncio.to_thread(
        _role_result, request, broker, workspace, review_diff, result,
    )


def _role_result(request, broker, workspace, review_diff, result):
    role, iteration, candidate = request['role'], request['iteration'], request['candidate']
    constraint = request.get('title_constraint')
    if constraint:
        from .delivery_title_repair import validate_source

        try:
            validate_source(request["spec"], constraint, completed=True)
        except (ValueError, OSError, UnicodeError) as exc:
            result["status"] = "blocked"
            result.setdefault("findings", []).append(str(exc))
    if role == "implement":
        try:
            broker.validate_candidate_scope()
            if result.get('role_artifacts') and broker.candidate()['head'] != candidate['head']:
                raise ValueError('implementation moved HEAD after role artifacts were bound')
            broker.admit_implementation(candidate)
        except (ValueError, RuntimeError, OSError) as exc:
            result['status'] = 'blocked'
            result.setdefault('findings', []).append(str(exc))
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
                from .delivery_role_evidence import repair_payload_progress

                if not repair_payload_progress(broker.store, request, result, workspace):
                    result["status"] = "blocked"
                    result.setdefault("findings", []).append(
                        "implementer produced no candidate change")
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
        try:
            return broker.publish(request["iteration"], request["candidate"])
        except Exception as exc:
            if (request["spec"].get("publication_readback_version") == 1
                    and broker.publication_may_have_effect is False):
                raise ApplicationError(str(exc)[:600], type="PublicationRejected",
                                       non_retryable=True) from exc
            raise

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
        if request["recovery"].get("kind") == "pending_publication_retry":
            from .delivery_pending_publication import readback
        elif request["recovery"].get("kind") in {
                "published_gate_retry", "prepublication_gate_retry",
                "published_check_prelaunch_retry", "published_ci_retry",
                "published_controller_retry"}:
            from .delivery_gate_retry import readback
        else:
            from .delivery_gates_admission import readback

        store, _ = _context(request["spec"])
        return readback(store, request["spec"], request["recovery"])

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_repair_preflight")
async def delivery_repair_preflight(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        try:
            store, _ = _context(request["spec"])
            if request["recovery"].get("kind") == "stopped_delivery_resume":
                from .delivery_stopped_resume import readback

                return readback(store, request["spec"], request["recovery"])
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
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        try:
            return broker.run_checks(request["iteration"], request["candidate"])
        except CheckPreparationFailure as exc:
            return {'state': 'failed', 'cleanup': 'confirmed',
                    'candidate_id': request['candidate']['id'],
                    'source_unchanged': broker.candidate()['id'] == request['candidate']['id'],
                    'results': exc.results, 'diagnostic': str(exc)}
        except Exception as exc:
            if (request["spec"]["provider"] != "codex"
                    or isinstance(exc, CheckCancelledBeforeLaunch)):
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await _with_heartbeat(_execute_check(request, execute), request, 'checks')


@activity.defn(name="delivery_browser_qa")
async def delivery_browser_qa(request: dict[str, Any]) -> dict[str, Any]:
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        try:
            return broker.run_browser_qa(request["iteration"], request["candidate"])
        except Exception as exc:
            if (request["spec"]["provider"] != "codex"
                    or isinstance(exc, CheckCancelledBeforeLaunch)):
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
    qa = request["spec"].get("policy", {}).get("browser_qa") or {}
    return await _with_heartbeat(
        _execute_check(request, execute, ports=tuple(qa.get("ports", {}).values())),
        request, 'browser_qa',
    )


@activity.defn(name="delivery_precheck")
async def delivery_precheck(request: dict[str, Any]) -> dict[str, Any]:
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        try:
            return broker.run_prechecks(request["iteration"], request["candidate"])
        except CheckPreparationFailure as exc:
            return {'state': 'failed', 'cleanup': 'confirmed',
                    'candidate_id': request['candidate']['id'],
                    'source_unchanged': broker.candidate()['id'] == request['candidate']['id'],
                    'results': exc.results, 'diagnostic': str(exc)}
        except Exception as exc:
            if (request["spec"]["provider"] != "codex"
                    or isinstance(exc, CheckCancelledBeforeLaunch)):
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await _with_heartbeat(_execute_check(request, execute), request, 'precheck')


@activity.defn(name="delivery_baseline_checks")
async def delivery_baseline_checks(request: dict[str, Any]) -> dict[str, Any]:
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        from .delivery_baseline import run_baseline_checks

        try:
            return run_baseline_checks(broker)
        except CheckPreparationFailure as exc:
            return {"state": "failed", "results": exc.results, "diagnostic": str(exc),
                    "base_sha": request["spec"]["base_sha"]}

    return await _with_heartbeat(_execute_check(request, execute), request, 'baseline_checks')


@activity.defn(name="delivery_ci")
async def delivery_ci(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = _context(request["spec"])
    if "ci_wait_seconds" not in request["spec"].get("policy", {}):
        return await broker.checks(request["pull_request"])
    pending = asyncio.create_task(broker.checks(request["pull_request"]))
    try:
        while not pending.done():
            activity.heartbeat({"run_id": request["spec"]["run_id"], "stage": "required_ci"})
            await asyncio.wait({pending}, timeout=5)
        return await pending
    finally:
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


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


def _tracker_helper_retryable(result) -> bool:
    try:
        error = json.loads(result.stderr)
    except (ValueError, TypeError):
        return False
    return isinstance(error, dict) and error.get("error_type") in {
        "GitHubTransientError", "TimeoutExpired", "ConnectionError",
    }


def _tracker_error_retryable(exc) -> bool:
    return isinstance(exc, (
        BrokerReadbackUnavailable, subprocess.TimeoutExpired, TimeoutError, ConnectionError,
    )) or (
        isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).casefold()
    )


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
            "retryable": False,
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
                        "observed": observed, "retryable": _tracker_helper_retryable(audit),
                        "reason": "acknowledged terminal readback is pending",
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
            "retryable": _tracker_helper_retryable(result),
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
            "retryable": _tracker_helper_retryable(audit),
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
    except (
        subprocess.TimeoutExpired, sqlite3.OperationalError, TimeoutError, ConnectionError,
    ) as exc:
        if not request.get("repair_continuation") and "timeout_seconds" not in request:
            raise
        return {"state": "pending", "reason": type(exc).__name__,
                **({"retryable": _tracker_error_retryable(exc)}
                   if "timeout_seconds" in request else {})}


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
            **({"retryable": _tracker_error_retryable(exc)}
               if request["spec"].get("tracker_retry_version") == 1 else {}),
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
    delivery_baseline_checks,
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
