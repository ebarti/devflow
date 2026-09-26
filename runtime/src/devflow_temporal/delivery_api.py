"""Loopback HTTP API and event replay for the managed delivery controller."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import secrets
import stat
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from temporalio.client import Client, WorkflowUpdateFailedError
from temporalio.common import RetryPolicy, WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from .delivery_config import DeliveryConfig
from .delivery_store import DeliveryStore
from .delivery_workflow import DeliveryWorkflow


class LocalAuth:
    def __init__(self, state_root: Path) -> None:
        self.path = state_root / "service-token"
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            metadata = self.path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_uid != os.getuid()
            ):
                raise ValueError("service token file has unsafe permissions") from None
        else:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(secrets.token_urlsafe(32) + "\n")
        self.secret = self.path.read_text(encoding="utf-8").strip()
        if len(self.secret) < 32:
            raise ValueError("service token is invalid")

    def session(self) -> str:
        payload = f"{int(time.time()) + 86400}.{secrets.token_urlsafe(16)}"
        mac = hmac.new(self.secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return base64.urlsafe_b64encode(f"{payload}.{mac}".encode()).decode()

    def valid(self, cookie: str | None) -> bool:
        if not cookie:
            return False
        try:
            value = base64.urlsafe_b64decode(cookie.encode()).decode()
            expiry, nonce, mac = value.split(".")
            payload = f"{expiry}.{nonce}"
            expected = hmac.new(self.secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
            return int(expiry) >= int(time.time()) and hmac.compare_digest(mac, expected)
        except (ValueError, UnicodeError):
            return False

    def csrf(self, cookie: str) -> str:
        return hmac.new(
            self.secret.encode(), ("csrf:" + cookie).encode(), hashlib.sha256
        ).hexdigest()


class DeliveryService:
    def __init__(self, config_path: Path) -> None:
        self.config = DeliveryConfig.load(config_path)
        self.store = DeliveryStore(self.config)
        self.auth = LocalAuth(self.config.state_root)
        self.temporal_status = "disconnected"
        self.dispatch_task: asyncio.Task | None = None

    async def client(self) -> Client:
        return await Client.connect(
            self.config.temporal_address,
            namespace=self.config.raw.get("temporal_namespace", "default"),
        )

    async def dispatch_once(self) -> None:
        pending = self.store.pending_starts()
        if not pending:
            return
        client = await self.client()
        self.temporal_status = "connected"
        for item in pending:
            spec = json.loads(item["request_json"])
            workflow_id = "delivery-" + spec["run_id"]
            handle = client.get_workflow_handle(workflow_id)
            try:
                description = await handle.describe()
            except RPCError as exc:
                if exc.status != RPCStatusCode.NOT_FOUND:
                    raise
                description = None
            if description is not None:
                remote_digest = await description.memo_value("request_digest", "unknown")
                if remote_digest != item["request_digest"]:
                    self.store.mark_start(
                        spec["run_id"], accepted=False, error="Temporal ID conflict"
                    )
                    continue
                self.store.mark_start(spec["run_id"], accepted=True)
                continue
            try:
                await client.start_workflow(
                    DeliveryWorkflow.run,
                    spec,
                    id=workflow_id,
                    task_queue=self.config.queue,
                    id_reuse_policy=WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                    retry_policy=RetryPolicy(maximum_attempts=1),
                    memo={"request_digest": item["request_digest"]},
                )
            except WorkflowAlreadyStartedError:
                # The next dispatch inspects the durable remote memo before acking.
                continue
            self.store.mark_start(spec["run_id"], accepted=True)

    async def dispatch_loop(self) -> None:
        while True:
            try:
                await self.dispatch_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.temporal_status = "disconnected"
                for item in self.store.pending_starts():
                    self.store.mark_start(item["run_id"], accepted=False, error=type(exc).__name__)
            await asyncio.sleep(5)


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

    def _session(request: Request) -> str:
        _host(request)
        cookie = request.cookies.get("devflow_session")
        if not service.auth.valid(cookie):
            raise HTTPException(401, "local session required")
        assert cookie is not None
        return cookie

    def _mutation(request: Request) -> None:
        _origin(request)
        cookie = _session(request)
        supplied = request.headers.get("x-devflow-csrf")
        if not supplied or not hmac.compare_digest(supplied, service.auth.csrf(cookie)):
            raise HTTPException(403, "CSRF token is missing or invalid")

    @app.get("/api/session")
    async def get_session(request: Request) -> dict[str, Any]:
        _host(request)
        cookie = request.cookies.get("devflow_session")
        if not service.auth.valid(cookie):
            return {"authenticated": False}
        assert cookie is not None
        return {"authenticated": True, "csrf_token": service.auth.csrf(cookie)}

    @app.post("/api/session")
    async def login(request: Request, response: Response) -> dict[str, Any]:
        _origin(request)
        body = await request.json()
        if not isinstance(body, dict) or not isinstance(body.get("token"), str):
            raise HTTPException(400, "service token is required")
        if not hmac.compare_digest(body["token"], service.auth.secret):
            raise HTTPException(401, "service token is invalid")
        cookie = service.auth.session()
        response.set_cookie(
            "devflow_session",
            cookie,
            httponly=True,
            samesite="strict",
            secure=False,
            max_age=86400,
            path="/",
        )
        return {"authenticated": True, "csrf_token": service.auth.csrf(cookie)}

    @app.get("/api/service")
    async def service_info(request: Request) -> dict[str, Any]:
        _session(request)
        with service.store._connect() as db:
            active = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
        return {
            "status": "running",
            "version": "0.2.0-local",
            "temporal": service.temporal_status,
            "capacity": {"limit": service.config.raw.get("capacity", 2), "active": active},
            "policy": service.config.public_policy(),
        }

    @app.get("/api/runs")
    async def list_runs(request: Request) -> dict[str, Any]:
        _session(request)
        return {"runs": service.store.list_runs()}

    @app.post("/api/runs")
    async def submit(request: Request) -> dict[str, Any]:
        _mutation(request)
        try:
            return service.store.submit(await request.json())
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.get("/api/runs/{run_id}")
    async def detail(request: Request, run_id: str) -> dict[str, Any]:
        _session(request)
        try:
            value = service.store.detail(run_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc
        events = value.pop("events")
        run = {key: item for key, item in value.items() if key != "run"}
        run["sequence"] = events[-1]["sequence"] if events else 0
        if run.get("protocol_revision") is not None:
            run["revision"] = run["protocol_revision"]
        return {"run": run, "events": events, "evidence": service.store.evidence_index(run_id)}

    @app.get("/api/runs/{run_id}/evidence/{evidence_id}")
    async def evidence(request: Request, run_id: str, evidence_id: str) -> dict[str, Any]:
        _session(request)
        try:
            return service.store.evidence(run_id, evidence_id)
        except ValueError as exc:
            raise HTTPException(404, str(exc)) from exc

    @app.get("/api/runs/{run_id}/events")
    async def events(request: Request, run_id: str, after: int = 0) -> StreamingResponse:
        _session(request)
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
            else {
                "command_id",
                "expected_revision",
                "decision_id",
                "decision_revision",
                "candidate_revision",
                "answer",
            }
        )
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
            result = await client.get_workflow_handle("delivery-" + run_id).execute_update(
                name, payload, id=command_id
            )
        except WorkflowUpdateFailedError as exc:
            reason = str(exc.__cause__ or exc)
            service.store.reject_mutation(command_id, reason)
            raise HTTPException(409, reason) from exc
        except RPCError as exc:
            service.store.mark_mutation_unknown(command_id)
            raise HTTPException(503, type(exc).__name__) from exc
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

    dist = Path(__file__).resolve().parents[2] / "ui" / "dist"
    if (dist / "assets").is_dir():
        app.mount("/assets", StaticFiles(directory=dist / "assets"), name="assets")

    @app.get("/")
    @app.get("/runs/{run_id}")
    @app.get("/new")
    @app.get("/settings")
    async def dashboard(_request: Request, run_id: str | None = None):
        if (dist / "index.html").is_file():
            return FileResponse(dist / "index.html")
        return HTMLResponse("<h1>Devflow delivery</h1><p>Dashboard assets are not built.</p>")

    return app
