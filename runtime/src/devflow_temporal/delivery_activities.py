"""Effectful activities used by the managed Temporal delivery protocol."""

from __future__ import annotations

import asyncio
import contextvars
import fcntl
import hashlib
import json
import logging
import os
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput, Interceptor

from .candidate import candidate_for
from .contracts import digest
from .delivery_broker import (
    BrokerReadbackUnavailable,
    CheckCancelledBeforeLaunch,
    CheckPreparationFailure,
    CheckSuperseded,
    DeliveryBroker,
)
from .delivery_execution_config import load_execution_config
from .delivery_repair import RepairReadbackPending
from .delivery_store import DeliveryStore, _now
from .supervisor import activity_attempt_replaced, activity_retry_pending, get_supervisor

_LOG = logging.getLogger(__name__)
_CHECK_HEARTBEAT_INTERVAL = 5
_UNCLEAN_CHECK_RECHECK_INTERVAL = 5.0
# Gates with unknown teardown: their lock descriptors plus the processes and
# ports their journals recorded, retained while any of those is observably live.
_UNCLEAN_CHECK_SLOTS: list[dict[str, Any]] = []
_UNCLEAN_CHECK_LOCK = threading.Lock()
# The registered check body per (gate identity, workflow run, activity id) in
# this worker: its attempt number, supersession and completion signals. A later
# attempt of the same activity supersedes the registered body itself and waits
# for it, without relying on Temporal's throttled replacement notice.
_ACTIVE_CHECKS: dict[
    tuple[Any, ...], tuple[int, threading.Event, threading.Event, threading.Event | None]
] = {}
_ACTIVE_CHECK_LOCK = threading.Lock()


def database_contention(exc: BaseException) -> bool:
    """SQLITE_BUSY/LOCKED: another writer holds the database; that clears by itself."""
    code = getattr(exc, "sqlite_errorcode", None)
    return isinstance(exc, sqlite3.OperationalError) and (
        (code is not None and code & 0xFF in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED})
        or str(exc).startswith(("database is locked", "database table is locked",
                               "database schema is locked", "database is busy"))
    )


class _DatabaseContentionInbound(ActivityInboundInterceptor):
    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        try:
            return await super().execute_activity(input)
        except sqlite3.OperationalError as exc:
            if not database_contention(exc):
                raise
            raise ApplicationError(
                f"database busy: {str(exc)[:200]}", type="DatabaseBusy", non_retryable=False,
            ) from exc


class DatabaseContentionInterceptor(Interceptor):
    """Type SQLite contention so the workflow classifies it transient, never terminal."""

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _DatabaseContentionInbound(next)


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


def _recorded_processes(journal: Path) -> dict[int, dict[str, Any]] | None:
    """Processes a journal records, its monitor included; None when it is unreadable."""
    from .delivery_resources import read_private

    try:
        value = read_private(journal)
    except FileNotFoundError:
        return {}
    except (OSError, ValueError, KeyError, TypeError):
        return None
    owned = value.get("owned")
    recorded = {
        int(pid): identity for pid, identity in (owned.items() if isinstance(owned, dict) else ())
        if isinstance(identity, dict) and identity.get("identity")
    }
    monitor = value.get("monitor")
    if isinstance(monitor, dict) and monitor.get("identity") and "pid" in monitor:
        recorded[int(monitor["pid"])] = monitor
    return recorded


def _retain_unclean_check(
    descriptors: list[int], broker: DeliveryBroker, ports: tuple[int, ...],
) -> None:
    """Hold a gate's slot and port locks while what it recorded may still be live."""
    journals = {
        str(journal): _recorded_processes(journal) or {}
        for journal in dict.fromkeys(broker.native_journals)
    }
    with _UNCLEAN_CHECK_LOCK:
        _UNCLEAN_CHECK_SLOTS.append({
            "descriptors": list(descriptors), "ports": sorted(set(ports)), "journals": journals,
        })


def _unclean_check_settled(entry: dict[str, Any]) -> bool:
    """Proof that every process and port a retained gate recorded is gone."""
    from .delivery_native_process import listeners, process_table

    recorded: dict[int, dict[str, Any]] = {}
    for journal, snapshot in entry["journals"].items():
        # Journals are re-read so a monitor or descendant recorded after the
        # retention snapshot still counts; an unreadable journal proves nothing.
        current = _recorded_processes(Path(journal))
        if current is None:
            return False
        recorded.update(snapshot)
        recorded.update(current)
    table = process_table()
    if any(table.get(pid, {}).get("identity") == identity["identity"]
           and not table[pid]["stat"].startswith("Z")
           for pid, identity in recorded.items()):
        return False
    return not any(listeners(port) for port in entry["ports"])


def _gate_identity(request: dict[str, Any], kind: str) -> tuple[Any, ...]:
    spec = request["spec"]
    candidate = request.get("candidate") or {}
    return (kind, spec.get("run_id"), request.get("iteration"),
            candidate.get("id", spec.get("base_sha")))


def _attempt_identity() -> tuple[str, str, int] | None:
    """The workflow run, activity id and attempt this body runs for; None outside one."""
    if not activity.in_activity():
        return None
    try:
        info = activity.info()
    except RuntimeError:
        return None
    return (info.workflow_run_id, info.activity_id, info.attempt)


def _claim_gate(
    key: tuple[Any, ...], attempt: int, superseded: threading.Event, finished: threading.Event,
) -> threading.Event | None:
    """Register a later attempt of one activity; supersede the earlier body, return its `finished`.

    Only a genuine retry supersedes: the same workflow run and activity id with
    a higher attempt, which Temporal never runs concurrently. Any other overlap
    of one gate identity (another activity, or an equal attempt) keeps shared
    admission. Across workers the earlier body keeps running until the
    replacement notice reaches it.
    """
    with _ACTIVE_CHECK_LOCK:
        previous = _ACTIVE_CHECKS.get(key)
        if previous is not None and previous[0] >= attempt:
            return None
        predecessor = None
        if previous is not None:
            inherited = previous[3]
            predecessor = (inherited if inherited is not None and not inherited.is_set()
                           else previous[2])
        _ACTIVE_CHECKS[key] = (attempt, superseded, finished, predecessor)
    if previous is not None:
        previous[1].set()
    return predecessor


def _release_gate(key: tuple[Any, ...], finished: threading.Event) -> None:
    with _ACTIVE_CHECK_LOCK:
        finished.set()
        current = _ACTIVE_CHECKS.get(key)
        # A queued retry finishing does not settle the native body it inherited.
        # That predecessor's eventual release also retires a finished queued entry.
        if (current is not None and current[2].is_set()
                and (current[3] is None or current[3].is_set())):
            del _ACTIVE_CHECKS[key]


def _release_settled_check_slots() -> None:
    """Release retained gates whose recorded processes and ports are proven gone."""
    with _UNCLEAN_CHECK_LOCK:
        for entry in list(_UNCLEAN_CHECK_SLOTS):
            try:
                settled = _unclean_check_settled(entry)
            except Exception as exc:
                # Unavailable inspection proves nothing; the gate stays retained.
                _LOG.warning("Retained check slot could not be re-proven for %s: %s",
                             entry["journals"], exc)
                continue
            if settled:
                _UNCLEAN_CHECK_SLOTS.remove(entry)
                for descriptor in entry["descriptors"]:
                    os.close(descriptor)


def _bind_planning_repair(request, result):
    if (result.get("planning_defect") and request["spec"].get("feature_worker")
            and result.get("cleanup") == "confirmed"):
        from .delivery_feature_revisions import planning_defect_repair_binding

        binding = planning_defect_repair_binding(
            request["spec"], request["spec"]["run_id"], request["iteration"],
            result["planning_defect"],
        )
        if binding:
            result["planning_defect_repair"] = binding
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
    request: dict[str, Any], execute, *, kind: str, ports: tuple[int, ...] = (),
) -> dict[str, Any]:
    """Bound native gates across workers and join their monitor on cancellation."""
    cancelled = threading.Event()
    admitted = threading.Event()
    ports_admitted = threading.Event()
    # `superseded`: a newer attempt owns this gate, or Temporal replaced this
    # waiter; the body finishes only the native process it supervises.
    # `finished`: this body stopped, so the newer attempt may take the gate.
    superseded = threading.Event()
    finished = threading.Event()
    identity = _gate_identity(request, kind)
    dispatch = _attempt_identity()
    gate_key = None if dispatch is None else (identity, *dispatch[:2])
    # Admission and cancellation decide atomically whether the gate launches.
    gate = threading.Lock()

    def run():
        from .delivery_resources import private_directory

        store, broker = _context(request["spec"])
        if request["spec"]["provider"] != "codex":
            broker.check_cancelled = cancelled.is_set
            admitted.set()
            return _bind_planning_repair(request, execute(broker))
        root = store.config.state_root / "check-execution"
        private_directory(root)
        slots = store.config.raw.get("check_concurrency", 2)
        descriptor = None
        port_descriptors = []
        rechecked = float("-inf")

        def recheck() -> None:
            # Retained gates are re-proven at admission and while queued behind them.
            nonlocal rechecked
            if time.monotonic() - rechecked >= _UNCLEAN_CHECK_RECHECK_INTERVAL:
                rechecked = time.monotonic()
                _release_settled_check_slots()

        try:
            # A later attempt of this activity takes over the gate: it
            # supersedes the body this worker already runs for it and waits
            # for that body to stop, so one body and one slot serve the gate.
            predecessor = None
            if gate_key is not None:
                predecessor = _claim_gate(gate_key, dispatch[2], superseded, finished)
            while predecessor is not None and not predecessor.is_set():
                if superseded.is_set():
                    raise CheckSuperseded(
                        "native check superseded while waiting for its predecessor"
                    )
                if cancelled.is_set() or broker._native_cancelled():
                    raise CheckCancelledBeforeLaunch(
                        "native check cancelled while its replaced attempt finishes"
                    )
                predecessor.wait(0.1)
            # Only now may this body steer the broker: a broker shared with the
            # replaced attempt keeps that body's signals until it has stopped.
            broker.check_cancelled = cancelled.is_set
            broker.check_superseded = superseded.is_set
            recheck()
            # Frozen port sets may overlap across runs. Acquire them in one
            # consistent order, before consuming generic check capacity.
            for port in sorted(set(ports)):
                while not cancelled.is_set() and not broker._native_cancelled():
                    if superseded.is_set():
                        raise CheckSuperseded("native check superseded while queued for ports")
                    handle = _try_check_lock(root / f"port-{port}.lock")
                    if handle is not None:
                        port_descriptors.append(handle)
                        break
                    recheck()
                    cancelled.wait(0.1)
                else:
                    raise CheckCancelledBeforeLaunch(
                        "native check cancelled while queued for ports"
                    )
            ports_admitted.set()
            while not cancelled.is_set() and not broker._native_cancelled():
                if superseded.is_set():
                    raise CheckSuperseded("native check superseded while queued for a slot")
                for slot in range(slots):
                    handle = _try_check_lock(root / f"slot-{slot}.lock")
                    if handle is None:
                        continue
                    descriptor = handle
                    break
                if descriptor is not None:
                    with gate:
                        if superseded.is_set():
                            raise CheckSuperseded("native check superseded before admission")
                        if cancelled.is_set() or broker._native_cancelled():
                            raise CheckCancelledBeforeLaunch(
                                "native check cancelled before admission"
                            )
                        admitted.set()
                    result = execute(broker)
                    if not broker.native_cleanup_confirmed:
                        result = {**result, "state": "unknown", "cleanup": "unknown"}
                    return _bind_planning_repair(request, result)
                recheck()
                cancelled.wait(0.1)
            raise CheckCancelledBeforeLaunch("native check cancelled while queued for a slot")
        except CheckCancelledBeforeLaunch:
            return _cancelled_check_result(request, broker)
        except CheckSuperseded as exc:
            # A newer attempt owns this gate and scores it from the journals;
            # this attempt's verdict is void and must not be retried as such.
            raise ApplicationError(str(exc), type="CheckSuperseded", non_retryable=True) from exc
        finally:
            if descriptor is not None:
                port_descriptors.append(descriptor)
            if broker.native_cleanup_confirmed:
                for handle in port_descriptors:
                    os.close(handle)
            else:
                # Unknown teardown cannot supply ports or capacity to another gate
                # until the processes and ports it recorded are proven gone.
                _retain_unclean_check(port_descriptors, broker, ports)
            if gate_key is not None:
                _release_gate(gate_key, finished)

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
    except BaseException as exc:
        with gate:
            replaced = (isinstance(exc, asyncio.CancelledError) and admitted.is_set()
                        and request["spec"]["provider"] == "codex"
                        and activity_attempt_replaced() and activity_retry_pending())
            if replaced:
                superseded.set()
            else:
                cancelled.set()
        if replaced:
            # A heartbeat timeout, lost attempt or worker shutdown replaces only
            # this waiter. The admitted gate finishes the native process it
            # supervises under its durable monitor, launches nothing more, and
            # hands the gate to the retry, which scores that invocation from
            # its journal instead of a killed one. A retry on this worker takes
            # the gate as soon as it arrives; one on another worker only once
            # this notice has stopped the body. A terminal timeout or an
            # operator cancel still stops it and frees its slot.
            execution.add_done_callback(
                lambda future: future.exception() if not future.cancelled() else None
            )
            raise
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
    spec: dict[str, Any], *, preparation_input: bool = False, allow_stopped_feature: bool = False,
    terminal_settlement: bool = False,
) -> tuple[DeliveryStore, DeliveryBroker]:
    config = load_execution_config(spec)
    store = DeliveryStore(config)
    saved = store.effective_spec(spec["run_id"])
    if saved != spec and not (
        preparation_input and spec.get("preparation_version") == 1
        and store.submitted_spec(spec["run_id"]) == spec
    ):
        raise ValueError("Temporal input no longer matches the durable submitted run")
    broker = DeliveryBroker(store, spec)
    if spec.get("feature_delivery"):
        from .delivery_feature_execution import require_execution

        require_execution(store, spec, allow_stopped=allow_stopped_feature,
                          terminal_settlement=terminal_settlement)
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
        outcome = request.get('outcome')
        terminal = (outcome in {'blocked', 'cancelled', 'delivered'}
                    and request.get('phase') == outcome
                    and request.get('execution_state') == (
                        'blocked' if outcome == 'blocked' else 'terminal'))
        store, _ = _context(request["spec"], preparation_input=True,
                            terminal_settlement=terminal)
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
        transient = database_contention(exc)
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

        terminal = request.get('outcome') in {'blocked', 'cancelled', 'delivered'}
        store, _ = _context(request["spec"], preparation_input=True,
                            terminal_settlement=terminal)
        if request["spec"].get("resource_cleanup_version") != 1:
            raise ApplicationError("run has no resource finalization contract", non_retryable=True)
        uncertain = request.get('uncertain', False)
        if uncertain and terminal and request['spec'].get('feature_worker'):
            from .delivery_interrupted_recovery import terminal_finalization_uncertain

            uncertain = asyncio.run(terminal_finalization_uncertain(store, request))
        receipt = RunResources(request["spec"]).finalize(request["outcome"], uncertain=uncertain)
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
    def execute() -> dict[str, Any]:
        spec = request["spec"]
        config = load_execution_config(spec)
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

    return await asyncio.to_thread(execute)


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
            if request.get("revision_context"):
                from .delivery_feature_revision_roles import revision_diff

                review_diff = revision_diff(broker, request)
            else:
                review_diff = broker.gate_diff(role, iteration, candidate)
    if role == 'implement' and retained is None:
        if broker.candidate() != candidate:
            raise ValueError('implementer checkout changed before its role')
        if constraint:
            validate_source(request['spec'], constraint, completed=False)
    return request, workspace, review_diff, retained


async def _run_role(request: dict[str, Any]) -> dict[str, Any]:
    store, broker = await asyncio.to_thread(_context, request['spec'])
    if (request["spec"].get("feature_worker") and request["role"] == "implement"
            and request["iteration"] > 0):
        from .delivery_feature_execution import registry

        shared = registry(request["spec"])
        issue_id = request["spec"]["feature_delivery"]["owner"]["issue_id"]
        token = shared.token(shared.current(issue_id))
        await asyncio.to_thread(shared.repair, token,
                                f"{request['spec']['run_id']}:{request['iteration']}",
                                "Product repair at worker iteration " + str(request["iteration"]))
    if request.get("planning_defects_version") == 1 and request["spec"].get("feature_worker"):
        from .delivery_feature_revisions import adopted_plan_identity

        identity = await asyncio.to_thread(adopted_plan_identity, request["spec"])
        request = {**request, "planning_context": {
            "chunk_id": request["spec"]["feature_worker"]["chunk_id"],
            "plan_revision": identity["plan_revision"], "plan_sha256": identity["plan_digest"],
        }}
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
        except CheckPreparationFailure as exc:
            prerequisites = {
                'state': 'failed',
                'cleanup': ('confirmed' if broker.native_cleanup_confirmed
                            and all(item.get('cleanup') == 'confirmed' for item in exc.results)
                            else 'unknown'),
                'candidate_id': candidate['id'],
                'source_unchanged': broker.candidate() == candidate,
                'results': exc.results, 'diagnostic': str(exc),
            }
        if prerequisites.get("state") != "passed":
            return {"status": "blocked", "role": role, "iteration": iteration,
                    "candidate": candidate, "cleanup": prerequisites.get("cleanup", "unknown"),
                    "summary": "Controller dependency preparation failed before the role",
                    "findings": [prerequisites.get('diagnostic')
                                 or "Required locked execution prerequisites are unavailable"],
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
                iteration == 0 and (request.get("continuation")
                                    or broker.is_imported_feature_candidate(candidate))
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
    diagnostic = result.get("planning_defect")
    if diagnostic is not None:
        from .delivery_feature_revision_roles import validate_diagnostic

        try:
            if (request.get("planning_defects_version") != 1
                    or not request["spec"].get("feature_worker")
                    or result.get("status") == "pass" or result.get("cleanup") != "confirmed"):
                raise ValueError("planning defect has no closed nonpassing worker assessment")
            diagnostic = validate_diagnostic(
                diagnostic, chunk_id=request["spec"]["feature_worker"]["chunk_id"],
                candidate_id=after["id"],
            )
            expected = request["planning_context"]
            if any(diagnostic[key] != expected[key]
                   for key in ("chunk_id", "plan_revision", "plan_sha256")):
                raise ValueError("planning defect assessed a stale plan")
            result["planning_defect"] = diagnostic
            _bind_planning_repair(request, result)
        except (ValueError, KeyError, TypeError):
            # A malformed or stale diagnostic never turns ordinary failure text
            # into revision authority. Existing findings remain product evidence.
            result.pop("planning_defect", None)
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
        store, broker = _context(request["spec"])
        try:
            if request["spec"].get("feature_worker"):
                from .delivery_feature_publication import publish

                return publish(store, broker, request)
            return broker.publish(request["iteration"], request["candidate"])
        except Exception as exc:
            if (request["spec"].get("publication_readback_version") == 1
                    and broker.publication_may_have_effect is False):
                if (not request["spec"].get("feature_worker")
                        and isinstance(exc, (BrokerReadbackUnavailable,
                                             subprocess.TimeoutExpired))):
                    # Confirmed before any remote command took effect: the
                    # publication is retried whole instead of stopping the run
                    # on GitHub or network availability. A failure before the
                    # effect row existed leaves nothing to discard.
                    if broker.publication_effect_inserted(request["iteration"]):
                        broker.discard_unused_publication(request["iteration"])
                    raise ApplicationError(str(exc)[:600], type="PublicationTransient") from exc
                raise ApplicationError(str(exc)[:600], type="PublicationRejected",
                                       non_retryable=True) from exc
            raise

    return await asyncio.to_thread(execute)


@activity.defn(name="delivery_reconcile_publish")
async def delivery_reconcile_publish(request: dict[str, Any]) -> dict[str, Any]:
    def execute() -> dict[str, Any]:
        store, broker = _context(request["spec"])
        if request["spec"].get("feature_worker"):
            from .delivery_feature_publication import publish

            return publish(store, broker, request, reconcile=True)
        result = broker.reconcile_publish(
            request["iteration"],
            request["candidate"],
            expected_head=request.get("expected_head"),
            expected_pr_number=request.get("expected_pr_number"),
            create_missing=request["spec"].get("publication_readback_version") == 1,
        )
        return result

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
                    or isinstance(exc, (CheckCancelledBeforeLaunch, CheckSuperseded))):
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await _with_heartbeat(
        _execute_check(request, execute, kind='checks'), request, 'checks',
    )


@activity.defn(name="delivery_browser_qa")
async def delivery_browser_qa(request: dict[str, Any]) -> dict[str, Any]:
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        try:
            return broker.run_browser_qa(request["iteration"], request["candidate"])
        except Exception as exc:
            if (request["spec"]["provider"] != "codex"
                    or isinstance(exc, (CheckCancelledBeforeLaunch, CheckSuperseded))):
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
        _execute_check(request, execute, kind='browser_qa',
                       ports=tuple(qa.get("ports", {}).values())),
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
                    or isinstance(exc, (CheckCancelledBeforeLaunch, CheckSuperseded))):
                raise
            return {
                "state": "unknown",
                "cleanup": "unknown",
                "candidate_id": request["candidate"]["id"],
                "reason": type(exc).__name__,
            }

    return await _with_heartbeat(
        _execute_check(request, execute, kind='precheck'), request, 'precheck',
    )


@activity.defn(name="delivery_baseline_checks")
async def delivery_baseline_checks(request: dict[str, Any]) -> dict[str, Any]:
    def execute(broker: DeliveryBroker) -> dict[str, Any]:
        from .delivery_baseline import run_baseline_checks

        try:
            return run_baseline_checks(broker)
        except CheckPreparationFailure as exc:
            return {"state": "failed", "results": exc.results, "diagnostic": str(exc),
                    "base_sha": request["spec"]["base_sha"]}

    return await _with_heartbeat(
        _execute_check(request, execute, kind='baseline_checks'), request, 'baseline_checks',
    )


@activity.defn(name="delivery_ci")
async def delivery_ci(request: dict[str, Any]) -> dict[str, Any]:
    _, broker = await asyncio.to_thread(_context, request["spec"])
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
            or sync.get("issue_state") != ("CLOSED" if status == "done" else "OPEN")
            or sync.get("project") != project
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
    store, _ = _context(spec, terminal_settlement=terminal)
    if spec.get("project_sync_version") == 1:
        from .delivery_features import record_tracking

        return record_tracking(store, spec, status, release)
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
        feature_parent = request["spec"].get("feature_delivery") and not request["spec"].get(
            "feature_worker")
        if (request["status"] in {"in-review", "done"} and request.get("pull_request") is not None
                and not feature_parent):
            from .delivery_terminal_recovery import published_readback

            store, _ = await asyncio.to_thread(_context, request["spec"], terminal_settlement=True)
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


@activity.defn(name="delivery_merge")
async def delivery_merge(request):
    from threading import Event

    from .delivery_merge import merge_verified

    store, _ = await asyncio.to_thread(_context, request["spec"])
    cancelled = Event()
    try:
        return await _with_heartbeat(
            asyncio.to_thread(merge_verified, store, request["spec"], request, cancelled),
            request, "merge"
        )
    finally:
        # Cancelling asyncio.to_thread does not stop its underlying thread.
        # Fence any later external effect, retaining an already-sent effect as unknown.
        cancelled.set()


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
    delivery_merge,
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


def _feature_activities():
    from .delivery_feature_activities import FEATURE_ACTIVITIES

    return FEATURE_ACTIVITIES


DELIVERY_ACTIVITIES.extend(_feature_activities())
