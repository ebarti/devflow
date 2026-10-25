"""Loopback HTTP API and event replay for the managed delivery controller."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from temporalio.client import Client, WorkflowExecutionStatus, WorkflowUpdateFailedError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from .contracts import digest
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_config import DeliveryConfig
from .delivery_execution_config import transport_digest
from .delivery_preparation import execution_retired
from .delivery_store import DeliveryStore
from .delivery_workflow import DeliveryWorkflow

# Failures that prove nothing about the run and are safe to replay; the API
# answers 503 with the reason instead of a bare 500.
TRANSPORT_FAILURES = (RuntimeError, OSError, subprocess.SubprocessError)

logger = logging.getLogger(__name__)


def _dispatch_failure_reason(exc: BaseException) -> str:
    """Name the failing condition stably, so a repeat does not read as a new reason."""
    if isinstance(exc, RPCError):
        text = f"RPCError {exc.status.name}: {exc.message}"
    else:
        text = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return text[:240]


def _dispatch_backoff_remaining(item: dict, now: float) -> float:
    """Seconds before a row that failed to start may be tried again.

    The first retry is immediate: a start whose acknowledgement was lost is
    resolved by reading the remote memo on the next cycle. From the second
    failure on, each attempt doubles the wait (20 s up to 10 min), so a poison
    row cannot take every cycle while healthy rows wait behind it. A row
    re-queued by an admission (its error cleared) is due at once.
    """
    attempts = item.get("attempts") or 0
    if attempts <= 1 or item.get("last_error") is None:
        return 0.0
    try:
        updated = datetime.fromisoformat(item["outbox_updated_at"]).timestamp()
    except (KeyError, TypeError, ValueError):
        return 0.0
    return max(0.0, updated + min(600.0, 5.0 * 2 ** min(attempts, 8)) - now)


async def _dispatch_start(service: Any, client: Client, item: dict, spec: dict) -> None:
    """Start one queued run of ``service``; its failure never reaches the rows behind it.

    A row whose remote description or start is rejected deterministically
    would otherwise lead every batch (oldest first) and starve newer runs.
    A module-level function: callers drive ``dispatch_once`` on stand-in
    services as well as on ``DeliveryService``.
    """
    run_id = spec["run_id"]
    workflow_id = item["workflow_id"] or "delivery-" + run_id
    attempted = False
    try:
        recovery = json.loads(item["recovery_json"]) if item["recovery_json"] else None
        handle = client.get_workflow_handle(workflow_id)
        try:
            description = await handle.describe()
        except RPCError as exc:
            if exc.status != RPCStatusCode.NOT_FOUND:
                raise
            description = None
        if description is not None:
            remote_digest = await description.memo_value("request_digest", "unknown")
            remote_recovery = await description.memo_value("recovery_digest", None)
            if remote_digest != item["request_digest"] or remote_recovery != (
                digest(recovery) if recovery else None
            ):
                await asyncio.to_thread(
                    service.store.mark_start_conflict, run_id, workflow_id,
                    f"Temporal ID conflict: workflow {workflow_id} belongs to a "
                    "different request",
                )
                return
            await asyncio.to_thread(service.store.mark_start, run_id, accepted=True)
            return
        attempted = True
        try:
            await client.start_workflow(
                DeliveryWorkflow.run,
                args=[spec, recovery],
                id=workflow_id,
                task_queue=service.config.queue,
                id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                retry_policy=RetryPolicy(maximum_attempts=1),
                **({"execution_timeout": timedelta(hours=72),
                    "run_timeout": timedelta(minutes=14)
                    if recovery and recovery.get("kind") == "terminal_tracker_recovery"
                    else timedelta(hours=72)}
                   if spec.get("terminal_tracker_version") == 1
                   and not spec.get("feature_delivery")
                   and (spec.get("workflow_failure_boundary_version") != 1
                        or (recovery and recovery.get("kind") == "terminal_tracker_recovery"))
                   else {}),
                memo={
                    "request_digest": item["request_digest"],
                    **({"recovery_digest": digest(recovery)} if recovery else {}),
                },
            )
        except WorkflowAlreadyStartedError:
            # The next dispatch inspects the durable remote memo before acking.
            return
        await asyncio.to_thread(service.store.mark_start, run_id, accepted=True)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        reason = _dispatch_failure_reason(exc)
        logger.warning("Run %s dispatch failed: %s", run_id, reason)
        try:
            # A start that was sent is unknown until the remote memo is read.
            await asyncio.to_thread(
                service.store.record_dispatch_failure, run_id, reason, uncertain=attempted
            )
        except Exception as recording:
            logger.warning("Run %s dispatch failure not recorded (%s)",
                           run_id, type(recording).__name__)


class LocalSession:
    """Anonymous, automatically renewed browser state for CSRF protection only."""

    def __init__(self) -> None:
        self.secret = secrets.token_bytes(32)

    def session(self) -> str:
        payload = f"{int(time.time()) + 86400}.{secrets.token_urlsafe(16)}"
        mac = hmac.new(self.secret, payload.encode(), hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(f"{payload}.{mac}".encode()).decode()

    def valid(self, cookie: str | None) -> bool:
        if not cookie:
            return False
        try:
            value = base64.urlsafe_b64decode(cookie.encode()).decode()
            expiry, nonce, mac = value.split(".")
            payload = f"{expiry}.{nonce}"
            expected = hmac.new(self.secret, payload.encode(), hashlib.sha256).hexdigest()
            return int(expiry) >= int(time.time()) and hmac.compare_digest(mac, expected)
        except (ValueError, UnicodeError):
            return False

    def csrf(self, cookie: str) -> str:
        return hmac.new(self.secret, ("csrf:" + cookie).encode(), hashlib.sha256).hexdigest()


class DeliveryService:
    def __init__(self, config_path: Path) -> None:
        self.config = DeliveryConfig.load(config_path)
        self.store = DeliveryStore(self.config)
        self._seed_execution_snapshots()
        self.session = LocalSession()
        self.temporal_status = "disconnected"
        self._health_client: Client | None = None
        self.dispatch_task: asyncio.Task | None = None

    async def client(self) -> Client:
        return await Client.connect(
            self.config.temporal_address,
            namespace=self.config.raw.get("temporal_namespace", "default"),
            data_converter=DELIVERY_DATA_CONVERTER,
        )

    async def healthy_client(self) -> Client:
        try:
            if self._health_client is None:
                self._health_client = await self.client()
            healthy = await self._health_client.service_client.check_health()
        except Exception:
            self._health_client = None
            self.temporal_status = "disconnected"
            raise
        if not healthy:
            self._health_client = None
            self.temporal_status = "disconnected"
            raise RuntimeError("Temporal health check failed")
        self.temporal_status = "connected"
        return self._health_client

    def _seed_execution_snapshots(self) -> None:
        """Service start seeds snapshots explicitly and names the runs it cannot seed."""
        try:
            report = self.store.seed_execution_snapshots()
        except (OSError, ValueError) as exc:
            logger.warning("admitted configuration snapshot publication failed: %s", exc)
            return
        for item in report["unavailable"]:
            logger.warning("Run %s cannot be served by this service: %s",
                           item["run_id"], item["reason"])

    async def dispatch_once(self) -> None:
        # Store work runs off the loop: a contended database must not stall
        # the dashboard, the question pump or any other request.
        pending = await asyncio.to_thread(self.store.pending_starts)
        starts = []
        for item in pending:
            try:
                spec = await asyncio.to_thread(self.store.effective_spec, item["run_id"])
                retired = execution_retired(spec)
                blocked = None if retired else await asyncio.to_thread(
                    self.store.execution_ownership, spec
                )
            except (OSError, ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "Run %s skipped: execution specification unavailable (%s)",
                    item["run_id"], type(exc).__name__,
                )
                continue
            if retired:
                continue
            if blocked is None:
                starts.append((item, spec))
                continue
            try:
                # An owner must see why its own queued run never dispatches.
                await asyncio.to_thread(
                    self.store.record_dispatch_block, item["run_id"], spec, blocked
                )
            except Exception as exc:
                logger.warning("Run %s dispatch block not recorded (%s)",
                               item["run_id"], type(exc).__name__)
        # Retired and foreign outbox entries cannot authorize this service's transport.
        if pending and not starts:
            return
        client = await self.healthy_client()
        now = time.time()
        for item, spec in starts:
            if _dispatch_backoff_remaining(item, now) > 0:
                continue
            await _dispatch_start(self, client, item, spec)

    async def dispatch_merges_once(self):
        from .delivery_merge_followup import dispatch

        def pending_requests():
            with self.store._connect() as db:
                return db.execute("SELECT 1 FROM delivery_merge_requests "
                                  "WHERE dispatched=0 AND phase='queued'").fetchone() is not None

        pending = await asyncio.to_thread(pending_requests)
        if pending:
            await dispatch(self, await self.healthy_client())

    async def dispatch_loop(self) -> None:
        self._retry_stopped = threading.Event()
        self._retry_task = None
        try:
            await self._dispatch_loop()
        finally:
            self._retry_stopped.set()
            if self._retry_task:
                await asyncio.gather(self._retry_task, return_exceptions=True)

    async def _dispatch_loop(self) -> None:
        while True:
            try:
                from .delivery_native_diagnostic import dispatch_once as dispatch_diagnostics

                await dispatch_diagnostics(self)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning('Native diagnostic dispatch unavailable: %s', type(exc).__name__)
            try:
                await self.dispatch_merges_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Merge dispatch pending: %s", exc)
            try:
                await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.temporal_status = "disconnected"
                reason = _dispatch_failure_reason(exc)
                for item in await asyncio.to_thread(self.store.pending_starts):
                    try:
                        spec = await asyncio.to_thread(self.store.effective_spec, item["run_id"])
                        if execution_retired(spec) or not await asyncio.to_thread(
                            self.store.owns_execution, spec
                        ):
                            continue
                        # No row was reached: the reason is surfaced once, the
                        # rows stay queued and their backoff is untouched.
                        await asyncio.to_thread(
                            self.store.record_dispatch_failure, item["run_id"], reason,
                            counted=False,
                        )
                    except Exception:
                        # Unknown authority cannot authorize an outbox acknowledgement.
                        continue
            try:
                await self.dispatch_questions_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Any interrupted dispatching record becomes visible unknown on
                # the next exclusive pump, without repeating its external effect.
                pass
            try:
                await self.reconcile_closed_native_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Failed maintenance never acknowledges starts or releases claims.
                pass
            try:
                await self.reconcile_completed_projections_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Only an authenticated completed result can repair its read model.
                pass
            try:
                await self.seal_closures_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # Sealing only preserves retained histories; recovery still reads them.
                pass
            try:
                await self.reproject_features_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                # A stale feature projection waits for the next pass.
                pass
            try:
                from .delivery_automatic_retry import retry_once

                if self._retry_task is None or self._retry_task.done():
                    previous, self._retry_task = self._retry_task, None
                    if previous:
                        previous.result()
                    self._retry_task = asyncio.create_task(asyncio.to_thread(
                        retry_once, self.store, stopped=self._retry_stopped.is_set))
            except asyncio.CancelledError:
                raise
            except Exception:
                # Unknown successor eligibility never authorizes another attempt.
                pass
            await asyncio.sleep(5)

    async def reconcile_closed_native_once(self) -> None:
        from .delivery_orphans import reconcile_closed_native

        client = await self.healthy_client()
        await reconcile_closed_native(self.store, client)

    async def seal_closures_once(self) -> None:
        from .delivery_closures import seal_closed_executions

        client = await self.healthy_client()
        await seal_closed_executions(self.store, client)

    async def reconcile_completed_projections_once(self) -> None:
        from .delivery_closed_projection import reconcile_completed_projections

        client = await self.healthy_client()
        # Due candidates and backoff live in SQLite; SQLite work runs off the loop.
        await reconcile_completed_projections(self.store, client)

    async def reproject_features_once(self) -> None:
        from .delivery_features import maintain_projections

        await asyncio.to_thread(maintain_projections, self.store)

    async def dispatch_questions_once(self) -> None:
        from .delivery_question_sender import pump_blocking_questions

        await asyncio.to_thread(pump_blocking_questions, self.store)


def create_app(config_path: Path) -> FastAPI:
    service = DeliveryService(config_path)
    allowed_host = urlsplit(service.config.dashboard_url).netloc
    allowed_origin = service.config.dashboard_url.rstrip("/")

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        service.dispatch_task = asyncio.create_task(service.dispatch_loop())
        try:
            yield
        finally:
            service.dispatch_task.cancel()
            await asyncio.gather(service.dispatch_task, return_exceptions=True)
            diagnostics = list(getattr(service, 'native_diagnostic_tasks', {}).values())
            for task in diagnostics:
                task.cancel()
            await asyncio.gather(*diagnostics, return_exceptions=True)

    app = FastAPI(title="Devflow local delivery", lifespan=lifespan)
    app.state.delivery = service

    def _host(request: Request) -> None:
        if request.headers.get("host") != allowed_host:
            raise HTTPException(403, "host is not the configured loopback service")
        if not request.client or request.client.host not in {"127.0.0.1", "::1"}:
            raise HTTPException(403, "service is loopback only")

    def _origin(request: Request) -> None:
        _host(request)
        if request.headers.get("origin") != allowed_origin:
            raise HTTPException(403, "origin does not match the dashboard")

    def _mutation(request: Request) -> None:
        _origin(request)
        cookie = request.cookies.get("devflow_session")
        if not service.session.valid(cookie):
            raise HTTPException(403, "anonymous CSRF session is missing or expired")
        assert cookie is not None
        supplied = request.headers.get("x-devflow-csrf")
        if not supplied or not hmac.compare_digest(supplied, service.session.csrf(cookie)):
            raise HTTPException(403, "CSRF token is missing or invalid")

    def _anonymous_session(request: Request, response: Response) -> dict[str, Any]:
        cookie = request.cookies.get("devflow_session")
        if not service.session.valid(cookie):
            cookie = service.session.session()
            response.set_cookie(
                "devflow_session",
                cookie,
                httponly=True,
                samesite="strict",
                secure=False,
                max_age=86400,
                path="/",
            )
        assert cookie is not None
        response.headers["Cache-Control"] = "no-store"
        # Retain the old response shape for clients cached before the local upgrade.
        return {"authenticated": True, "csrf_token": service.session.csrf(cookie)}

    @app.get("/api/session")
    async def get_session(request: Request, response: Response) -> dict[str, Any]:
        _host(request)
        return _anonymous_session(request, response)

    @app.post("/api/session")
    async def legacy_session(request: Request, response: Response) -> dict[str, Any]:
        _origin(request)
        # Cached CLI/MCP callers may still POST a token; it has no authority now.
        return _anonymous_session(request, response)

    @app.get("/api/service")
    async def service_info(request: Request) -> dict[str, Any]:
        _host(request)
        try:
            await asyncio.wait_for(service.healthy_client(), timeout=2)
        except Exception:
            service.temporal_status = "disconnected"
        def active_attempts() -> int:
            with service.store._connect() as db:
                roles = db.execute(
                    """SELECT COUNT(*) FROM delivery_attempts
                       WHERE state IN ('starting','running','unknown')"""
                ).fetchone()[0]
                merges = db.execute("SELECT COUNT(*) FROM delivery_merge_requests WHERE "
                                    "phase IN ('queued','merging','waiting_ci','waiting_tracker',"
                                    "'unknown')").fetchone()[0]
                return roles + merges

        active = await asyncio.to_thread(active_attempts)
        from .delivery_dashboard import runtime_identity

        identity = runtime_identity()
        return {
            "status": "running",
            "pid": os.getpid(),
            "version": identity["release"] or (
                f"local-{identity['revision'][:8]}" if identity["revision"] else "unknown"
            ),
            "runtime_identity": identity,
            "temporal": service.temporal_status,
            # Informational for operators and tests; `restart` decides by the
            # transport the worker registered, never by what the API advertises.
            "config_digest": digest(service.config.raw),
            "transport_digest": transport_digest(service.config),
            "capacity": {"limit": service.config.raw.get("capacity", 2), "active": active},
            "policy": service.config.public_policy(),
            "repository_access": await asyncio.to_thread(
                _read_repository_access, service.store),
            "feature_projection_failures": await asyncio.to_thread(
                _projection_failures, service.store),
        }

    from .delivery_settings import read_repository_access as _read_repository_access

    def _projection_failures(store: DeliveryStore) -> list[dict[str, Any]]:
        from .delivery_features import projection_failures

        with store._connect() as db:
            return projection_failures(db)

    @app.post("/api/settings/repositories")
    async def save_repositories(request: Request) -> dict[str, Any]:
        _mutation(request)
        from .delivery_settings import save_repository_access

        try:
            return await asyncio.to_thread(
                save_repository_access, service.store, await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/runs")
    async def list_runs(
        request: Request, archived: bool = False, limit: int = Query(50, ge=1, le=100),
        cursor: str | None = Query(None, max_length=2048),
    ) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(
                service.store.list_runs_page, archived, limit=limit, cursor=cursor)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/api/project-board")
    async def project_board(request: Request) -> dict[str, Any]:
        _host(request)
        from .delivery_project_board import read

        return await asyncio.to_thread(read, service.store)

    @app.get("/api/statistics")
    async def statistics(request: Request) -> dict[str, Any]:
        _host(request)
        from .delivery_dashboard import statistics_for

        return await asyncio.to_thread(statistics_for, service.store)

    @app.post("/api/statistics/reset")
    async def statistics_reset(request: Request) -> dict[str, Any]:
        _mutation(request)
        from .delivery_dashboard import reset_statistics

        try:
            return await asyncio.to_thread(reset_statistics, service.store, await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    async def dashboard_command(request: Request, run_id: str, kind: str):
        _mutation(request)
        from .delivery_dashboard import mutate

        try:
            return await asyncio.to_thread(
                mutate, service.store, run_id, kind, await request.json()
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/archive")
    async def archive(request: Request, run_id: str):
        return await dashboard_command(request, run_id, "archive")

    @app.post("/api/runs/{run_id}/steer")
    async def steer(request: Request, run_id: str):
        return await dashboard_command(request, run_id, "steer")

    @app.post("/api/runs")
    async def submit(request: Request) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(service.store.submit, await request.json())
        except (ValueError, OSError) as exc:
            raise HTTPException(409, str(exc)) from exc
        except TRANSPORT_FAILURES as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/runs/{run_id}/issue-context")
    async def refresh_issue_context(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        from .delivery_issue_context import refresh

        try:
            context = await asyncio.to_thread(refresh, service.store, run_id, await request.json())
            return {"run_id": run_id, "context_sha256": digest(context),
                    "fetched_at": context["fetched_at"], "issue_url": context["issue"]["url"],
                    "evidence_count": len(context["evidence"]),
                    "unavailable": context["unavailable"]}
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except TRANSPORT_FAILURES as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/runs/{run_id}/revise-feature-plan")
    async def revise_feature_plan(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        from .delivery_feature_revisions import revise_feature_plan as revise_execution

        try:
            return await asyncio.to_thread(
                revise_execution, service.store, run_id, await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except TRANSPORT_FAILURES as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/runs/{run_id}/continue-feature")
    async def continue_feature(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        from .delivery_feature_execution import continue_feature as continue_execution

        try:
            return await asyncio.to_thread(
                continue_execution, service.store, run_id, await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except TRANSPORT_FAILURES as exc:
            # Temporal, git or gh were unavailable: the command is safe to replay.
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/runs/{run_id}/recover-publication")
    async def recover_publication(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.recover_publication, run_id, await request.json()
            )
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/runs/{run_id}/merge-preflight")
    async def merge_preflight(request: Request, run_id: str):
        _host(request)
        from .delivery_merge_followup import preflight

        try:
            return await asyncio.to_thread(preflight, service.store, run_id)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/merge")
    async def merge_run(request: Request, run_id: str):
        _mutation(request)
        from .delivery_merge_followup import admit

        try:
            return await asyncio.to_thread(admit, service.store, run_id, await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except TRANSPORT_FAILURES as exc:
            raise HTTPException(503, str(exc)) from exc


    @app.get("/api/runs/{run_id}/gates-only-preflight")
    async def gates_only_preflight(request: Request, run_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(service.store.gates_only_preflight, run_id)
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/admit-gates-only")
    async def admit_gates_only(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.admit_gates_only, run_id, await request.json(),
            )
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post('/api/runs/{run_id}/native-diagnostic-preflight')
    async def native_diagnostic_preflight(request: Request, run_id: str):
        _host(request)
        from .delivery_native_diagnostic import preflight

        try:
            return await asyncio.to_thread(preflight, service.store, run_id, await request.json())
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post('/api/runs/{run_id}/ack-native-diagnostic')
    async def ack_native_diagnostic(request: Request, run_id: str):
        _mutation(request)
        from .delivery_native_diagnostic import acknowledge

        try:
            return await asyncio.to_thread(acknowledge, service.store, run_id, await request.json())
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get('/api/runs/{run_id}/native-diagnostics/{command_id}')
    async def native_diagnostic(request: Request, run_id: str, command_id: str):
        _host(request)
        from .delivery_native_diagnostic import info

        try:
            return await asyncio.to_thread(info, service.store, run_id, command_id)
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/repair-admission-preflight")
    async def repair_admission_preflight(request: Request, run_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(service.store.repair_admission_preflight,
                                           run_id, await request.json())
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/continue-repair")
    async def continue_repair(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.continue_repair, run_id, await request.json()
            )
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/retry-prelaunch")
    async def retry_prelaunch(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.retry_prelaunch, run_id, await request.json()
            )
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/amend-scope")
    async def amend_scope(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(service.store.amend_scope, run_id, await request.json())
        except (ValueError, RuntimeError, OSError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/runs/{run_id}")
    async def detail(request: Request, run_id: str) -> dict[str, Any]:
        _host(request)
        try:
            value = await asyncio.to_thread(service.store.detail, run_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        evidence = await asyncio.to_thread(service.store.evidence_index, run_id, details=value)
        events = value.pop("events")
        run = {key: item for key, item in value.items() if key != "run"}
        run["sequence"] = events[-1]["sequence"] if events else 0
        return {"run": run, "events": events, "evidence": evidence}

    @app.get("/api/runs/{run_id}/evidence/{evidence_id}")
    async def evidence(request: Request, run_id: str, evidence_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(service.store.evidence, run_id, evidence_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/runs/{run_id}/evidence/{evidence_id}/content")
    async def evidence_content(request: Request, run_id: str, evidence_id: str):
        _host(request)
        try:
            value = await asyncio.to_thread(service.store.evidence, run_id, evidence_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        from fastapi.responses import Response

        content = base64.b64decode(value["base64"]) if "base64" in value else value["text"]
        return Response(content, media_type=value.get("media_type", "text/plain"),
                        headers={"X-Content-Type-Options": "nosniff"})

    @app.get("/api/runs/{run_id}/events")
    async def events(request: Request, run_id: str, after: int = 0) -> StreamingResponse:
        _host(request)
        await asyncio.to_thread(service.store.spec, run_id)
        cursor = max(after, int(request.headers.get("last-event-id", "0")))

        async def stream():
            nonlocal cursor
            feature = None
            def observe(after: int) -> tuple[Any, list[dict[str, Any]]]:
                return service.store.feature(run_id), service.store.events(run_id, after)

            while not await request.is_disconnected():
                # One off-loop read per tick; both yields below then follow
                # without a suspension point, as one observation of the run.
                current_feature, updates = await asyncio.to_thread(observe, cursor)
                current_feature = json.dumps(current_feature, sort_keys=True)
                if current_feature != feature:
                    feature = current_feature
                    # A separate event domain: do not rewrite historical run
                    # sequence numbers when only the current feature changes.
                    yield 'event: feature\ndata: {}\n\n'
                if updates:
                    for event in updates:
                        cursor = event["sequence"]
                        payload = json.dumps({"sequence": cursor, "run_id": run_id})
                        yield f"id: {cursor}\nevent: update\ndata: {payload}\n\n"
                else:
                    yield ": keepalive\n\n"
                await asyncio.sleep(1)

        return StreamingResponse(stream(), media_type="text/event-stream")

    async def _update(request: Request, run_id: str, name: str) -> dict[str, Any]:
        _mutation(request)
        payload = await request.json()
        if not isinstance(payload, dict) or not isinstance(payload.get("command_id"), str):
            raise HTTPException(400, "command ID is required")
        command_id = payload["command_id"]
        expected = (
            {"command_id", "expected_revision", "reason"}
            if name in {"cancel", "disable_delivery_limits"}
            else {"command_id", "expected_revision"} if name == "reconcile_tracker"
            else {
                "command_id",
                "expected_revision",
                "decision_id",
                "decision_revision",
                "candidate_revision",
                "answer",
            }
        )
        if name == "decision" and "response" in payload:
            expected = expected | {"response"}
        if set(payload) != expected:
            raise HTTPException(400, "mutation fields do not match the contract")
        try:
            prior = await asyncio.to_thread(
                service.store.begin_mutation, run_id, command_id, name, payload
            )
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        if prior is not None:
            return prior
        try:
            client = await service.client()
            handle = client.get_workflow_handle(
                await asyncio.to_thread(service.store.active_workflow_id, run_id)
            )
            if name == "reconcile_tracker":
                from .delivery_terminal_recovery import receipt, recover

                saved = await asyncio.to_thread(receipt, service.store, run_id, payload)
                if saved is not None:
                    await asyncio.to_thread(service.store.finish_mutation, command_id, saved)
                    return saved
                try:
                    description = await handle.describe()
                except RPCError as exc:
                    if exc.status != RPCStatusCode.NOT_FOUND:
                        raise
                    # Retention removed the closed predecessor; recover() authenticates
                    # its sealed original (a tail an update already advanced is refused).
                    response = await recover(service.store, run_id, payload, client)
                    await asyncio.to_thread(service.store.finish_mutation, command_id, response)
                    return response
                if description.status != WorkflowExecutionStatus.RUNNING:
                    # A lost acknowledged update and a fresh closed-tail command
                    # are different cases. Observe the existing update first.
                    try:
                        result = await handle.get_update_handle(command_id).result(
                            rpc_timeout=timedelta(seconds=2),
                        )
                    except RPCError as exc:
                        if exc.status != RPCStatusCode.NOT_FOUND:
                            raise
                        response = await recover(service.store, run_id, payload, client)
                        await asyncio.to_thread(
                            service.store.finish_mutation, command_id, response
                        )
                        return response
                else:
                    result = await handle.execute_update(name, payload, id=command_id)
            else:
                result = await handle.execute_update(name, payload, id=command_id)
        except WorkflowUpdateFailedError as exc:
            reason = str(exc.__cause__ or exc)
            await asyncio.to_thread(service.store.reject_mutation, command_id, reason)
            raise HTTPException(409, reason) from exc
        except RPCError as exc:
            await asyncio.to_thread(service.store.mark_mutation_unknown, command_id)
            raise HTTPException(503, type(exc).__name__) from exc
        except (ValueError, RuntimeError) as exc:
            if name == "reconcile_tracker":
                await asyncio.to_thread(service.store.mark_mutation_unknown, command_id)
            else:
                await asyncio.to_thread(service.store.reject_mutation, command_id, str(exc))
            raise HTTPException(409, str(exc)) from exc
        response = {"run_id": run_id, "phase": result["phase"], "revision": result["revision"]}
        if name == "disable_delivery_limits":
            response["delivery_limits_disabled"] = True
            await asyncio.to_thread(
                service.store.record_delivery_limits_disabled, run_id, result, command_id,
            )
        await asyncio.to_thread(service.store.finish_mutation, command_id, response)
        if name == "cancel":
            await asyncio.to_thread(
                service.store.project,
                run_id,
                phase="cancelling",
                execution_state="cancelling",
                event_type="cancel_requested",
                message="Cancellation requested",
                protocol_revision=result["revision"],
                key=f"cancel:{command_id}",
            )
        return response

    @app.post("/api/runs/{run_id}/decision")
    async def decision(request: Request, run_id: str) -> dict[str, Any]:
        return await _update(request, run_id, "decision")

    @app.post("/api/runs/{run_id}/cancel")
    async def cancel(request: Request, run_id: str) -> dict[str, Any]:
        return await _update(request, run_id, "cancel")

    @app.post("/api/runs/{run_id}/disable-limits")
    async def disable_delivery_limits(request: Request, run_id: str) -> dict[str, Any]:
        return await _update(request, run_id, "disable_delivery_limits")

    @app.post("/api/runs/{run_id}/reconcile-tracker")
    async def reconcile_tracker(request: Request, run_id: str) -> dict[str, Any]:
        return await _update(request, run_id, "reconcile_tracker")

    dist = Path(__file__).resolve().parents[2] / "ui" / "dist"
    if (dist / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @app.get("/")
    @app.get("/runs/{run_id}")
    @app.get("/new")
    @app.get("/settings")
    @app.get("/statistics")
    async def dashboard(request: Request, run_id: str | None = None):
        _host(request)
        if (dist / "index.html").is_file():
            return FileResponse(dist / "index.html")
        return HTMLResponse("<h1>Devflow delivery</h1><p>Dashboard assets are not built.</p>")

    return app
