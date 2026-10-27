"""Loopback checkpoint endpoints, sharing the service's existing CSRF boundary."""

from __future__ import annotations

import asyncio
import sqlite3
import subprocess

from fastapi import HTTPException, Request

from .delivery_checkpoints import (
    CheckpointRestoreUnknown,
    NativeCheckpointCustodyUnknown,
    create_checkpoint,
    read_checkpoint,
    restore_checkpoint,
)


def mount(app, service, mutation, host):
    async def body_for(request):
        try:
            return await request.json()
        except ValueError as exc:
            raise HTTPException(422, "checkpoint command must be valid JSON") from exc

    @app.post("/api/runs/{run_id}/checkpoints")
    async def create(run_id: str, request: Request):
        mutation(request)
        body = await body_for(request)
        if (
            not isinstance(body, dict)
            or set(body) - {"command_id", "expected_revision", "reason"}
            or not {"command_id", "expected_revision"} <= body.keys()
        ):
            raise HTTPException(422, "checkpoint requires command_id and expected_revision")
        try:
            return await asyncio.to_thread(
                create_checkpoint,
                service.store,
                run_id,
                body["command_id"],
                body["expected_revision"],
                body.get("reason", "manual"),
            )
        except NativeCheckpointCustodyUnknown as exc:
            raise HTTPException(409, {"code": exc.code, "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.get("/api/runs/{run_id}/checkpoints/{checkpoint_id}")
    async def read(run_id: str, checkpoint_id: str, request: Request):
        host(request)
        try:
            return await asyncio.to_thread(read_checkpoint, service.store, run_id, checkpoint_id)
        except (ValueError, OSError) as exc:
            raise HTTPException(409, str(exc)) from exc

    @app.post("/api/runs/{run_id}/checkpoints/{checkpoint_id}/verify-retained")
    async def verify_retained(run_id: str, checkpoint_id: str, request: Request):
        mutation(request)
        body = await body_for(request)
        if (not isinstance(body, dict) or set(body) != {"command_id", "expected_revision"}):
            raise HTTPException(
                422, "retained verification requires command_id and expected_revision")

        def verify():
            from .delivery_retained_checkpoint import RetainedCheckpointCustody

            with RetainedCheckpointCustody(service.store, run_id, checkpoint_id,
                                          body["expected_revision"]) as owner:
                return owner.acknowledge(body["command_id"])

        try:
            return await asyncio.to_thread(verify)
        except NativeCheckpointCustodyUnknown as exc:
            raise HTTPException(409, {"code": exc.code, "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except sqlite3.Error as exc:
            raise HTTPException(503, {"code": "checkpoint_observation_unavailable",
                                      "message": str(exc)}) from exc
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            raise HTTPException(503, str(exc)) from exc

    @app.post("/api/runs/{run_id}/checkpoints/{checkpoint_id}/restore")
    async def restore(run_id: str, checkpoint_id: str, request: Request):
        mutation(request)
        body = await body_for(request)
        if not isinstance(body, dict) or set(body) != {"command_id", "expected_revision"}:
            raise HTTPException(422, "checkpoint restore requires command_id and expected_revision")
        try:
            return await asyncio.to_thread(
                restore_checkpoint,
                service.store,
                run_id,
                checkpoint_id,
                body["command_id"],
                body["expected_revision"],
            )
        except NativeCheckpointCustodyUnknown as exc:
            raise HTTPException(409, {"code": exc.code, "message": str(exc)}) from exc
        except CheckpointRestoreUnknown as exc:
            raise HTTPException(409, {"code": exc.code, "message": str(exc)}) from exc
        except ValueError as exc:
            raise HTTPException(409, str(exc)) from exc
        except (OSError, RuntimeError, subprocess.SubprocessError) as exc:
            raise HTTPException(503, str(exc)) from exc
