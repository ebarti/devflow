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
from datetime import timedelta
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
from .delivery_preparation import execution_retired
from .delivery_store import DeliveryStore
from .delivery_workflow import DeliveryWorkflow

logger = logging.getLogger(__name__)


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

    async def dispatch_once(self) -> None:
        pending = self.store.pending_starts()
        starts = []
        for item in pending:
            try:
                spec = self.store.effective_spec(item["run_id"])
                owned = not execution_retired(spec) and self.store.owns_execution(spec)
            except (OSError, ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "Run %s skipped: execution specification unavailable (%s)",
                    item["run_id"], type(exc).__name__,
                )
                continue
            if owned:
                starts.append((item, spec))
        # Retired and foreign outbox entries cannot authorize this service's transport.
        if pending and not starts:
            return
        client = await self.healthy_client()
        for item, spec in starts:
            recovery = json.loads(item["recovery_json"]) if item["recovery_json"] else None
            workflow_id = item["workflow_id"] or "delivery-" + spec["run_id"]
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
                    self.store.mark_start(
                        spec["run_id"], accepted=False, error="Temporal ID conflict"
                    )
                    continue
                self.store.mark_start(spec["run_id"], accepted=True)
                continue
            try:
                await client.start_workflow(
                    DeliveryWorkflow.run,
                    args=[spec, recovery],
                    id=workflow_id,
                    task_queue=self.config.queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    **({"execution_timeout": timedelta(hours=72),
                        "run_timeout": timedelta(minutes=14)
                        if recovery and recovery.get("kind") == "terminal_tracker_recovery"
                        else timedelta(hours=72)}
                       if spec.get("terminal_tracker_version") == 1 else {}),
                    memo={
                        "request_digest": item["request_digest"],
                        **({"recovery_digest": digest(recovery)} if recovery else {}),
                    },
                )
            except WorkflowAlreadyStartedError:
                # The next dispatch inspects the durable remote memo before acking.
                continue
            self.store.mark_start(spec["run_id"], accepted=True)

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
                await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.temporal_status = "disconnected"
                for item in self.store.pending_starts():
                    try:
                        spec = self.store.effective_spec(item["run_id"])
                        if execution_retired(spec) or not self.store.owns_execution(spec):
                            continue
                    except Exception:
                        # Unknown authority cannot authorize an outbox acknowledgement.
                        continue
                    self.store.mark_start(item["run_id"], accepted=False, error=type(exc).__name__)
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
        with service.store._connect() as db:
            active = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
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
            "capacity": {"limit": service.config.raw.get("capacity", 2), "active": active},
            "policy": service.config.public_policy(),
        }

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

    @app.get("/api/statistics")
    async def statistics(request: Request) -> dict[str, Any]:
        _host(request)
        from .delivery_dashboard import statistics_for

        return await asyncio.to_thread(statistics_for, service.store)

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
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/recover-publication")
    async def recover_publication(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.recover_publication, run_id, await request.json()
            )
        except (ValueError, RuntimeError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/reconcile-published-metadata")
    async def reconcile_published_metadata(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.reconcile_published_metadata, run_id, await request.json(),
            )
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

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

    @app.get("/api/runs/{run_id}/recovery-preflight")
    async def recovery_preflight(request: Request, run_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(service.store.policy_recovery_precheck, run_id)
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/recover-execution")
    async def recover_execution(request: Request, run_id: str) -> dict[str, Any]:
        _mutation(request)
        try:
            return await asyncio.to_thread(
                service.store.recover_execution, run_id, await request.json(),
            )
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/metadata-preflight")
    async def metadata_preflight(request: Request, run_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return await asyncio.to_thread(service.store.metadata_preflight,
                                           run_id, await request.json())
        except (ValueError, RuntimeError, OSError, subprocess.SubprocessError) as exc:
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
            value = service.store.detail(run_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        events = value.pop("events")
        run = {key: item for key, item in value.items() if key != "run"}
        run["sequence"] = events[-1]["sequence"] if events else 0
        return {"run": run, "events": events, "evidence": service.store.evidence_index(run_id)}

    @app.get("/api/runs/{run_id}/evidence/{evidence_id}")
    async def evidence(request: Request, run_id: str, evidence_id: str) -> dict[str, Any]:
        _host(request)
        try:
            return service.store.evidence(run_id, evidence_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/runs/{run_id}/evidence/{evidence_id}/content")
    async def evidence_content(request: Request, run_id: str, evidence_id: str):
        _host(request)
        try:
            value = service.store.evidence(run_id, evidence_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        from fastapi.responses import Response

        content = base64.b64decode(value["base64"]) if "base64" in value else value["text"]
        return Response(content, media_type=value.get("media_type", "text/plain"),
                        headers={"X-Content-Type-Options": "nosniff"})

    @app.get("/api/runs/{run_id}/events")
    async def events(request: Request, run_id: str, after: int = 0) -> StreamingResponse:
        _host(request)
        service.store.spec(run_id)
        cursor = max(after, int(request.headers.get("last-event-id", "0")))

        async def stream():
            nonlocal cursor
            while not await request.is_disconnected():
                updates = service.store.events(run_id, cursor)
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
            if name == "cancel"
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
            prior = service.store.begin_mutation(run_id, command_id, name, payload)
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        if prior is not None:
            return prior
        try:
            client = await service.client()
            handle = client.get_workflow_handle(
                service.store.active_workflow_id(run_id)
            )
            if name == "reconcile_tracker":
                from .delivery_terminal_recovery import receipt, recover

                saved = receipt(service.store, run_id, payload)
                if saved is not None:
                    service.store.finish_mutation(command_id, saved)
                    return saved
                description = await handle.describe()
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
                        service.store.finish_mutation(command_id, response)
                        return response
                else:
                    result = await handle.execute_update(name, payload, id=command_id)
            else:
                result = await handle.execute_update(name, payload, id=command_id)
        except WorkflowUpdateFailedError as exc:
            reason = str(exc.__cause__ or exc)
            service.store.reject_mutation(command_id, reason)
            raise HTTPException(409, reason) from exc
        except RPCError as exc:
            service.store.mark_mutation_unknown(command_id)
            raise HTTPException(503, type(exc).__name__) from exc
        except (ValueError, RuntimeError) as exc:
            if name == "reconcile_tracker":
                service.store.mark_mutation_unknown(command_id)
            else:
                service.store.reject_mutation(command_id, str(exc))
            raise HTTPException(409, str(exc)) from exc
        response = {"run_id": run_id, "phase": result["phase"], "revision": result["revision"]}
        service.store.finish_mutation(command_id, response)
        if name == "cancel":
            service.store.project(
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
