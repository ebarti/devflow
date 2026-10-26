"""Owned ordinary published prerequisite failure and genuine worker reentry.

Provider execution, GitHub/tracker and the original closed descriptor are
controlled. Native prerequisite/checks, resource ownership/finalization, public
HTTP admission, SDK1.33 worker/history replay and zero-model boundary are real.
Repacked fixture wheels are sourced from the locked test interpreter's installed
pure-Python packages; they grant no production cache or browser capability.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from test_delivery_prerequisite_gate_recovery import stopped_prerequisite
from test_delivery_store import service as service

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_resources import read_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_native_published_prerequisite_reentry_runs_every_original_gate_once(
    service,
    monkeypatch,
    tmp_path,
):
    from temporal_test_server import local_temporal
    from temporalio import activity
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_activities import (
        delivery_checks,
        delivery_finalize_resources,
        delivery_gates_readback,
        delivery_precheck,
        delivery_project,
    )
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    async with local_temporal() as environment:
        initial, submitted = service
        config = copy.deepcopy(initial.config.raw)
        config["temporal_address"] = environment.client.service_client.config.target_host
        initial.config.path.write_text(json.dumps(config))
        owner = DeliveryStore(DeliveryConfig.load(initial.config.path))
        case, local, command, closed = stopped_prerequisite.__wrapped__(
            (owner, submitted), monkeypatch, tmp_path, SimpleNamespace(param="prerequisite")
        )
        original = copy.deepcopy(closed)
        original_native = {
            Path(item["native_process"]["journal"]): Path(
                item["native_process"]["journal"]
            ).read_bytes()
            for item in local["results"]
        }
        app = create_app(case.store.config.path)
        host = case.store.config.dashboard_url
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 10001)),
            base_url=host,
        ) as client:
            headers = {
                "Origin": host,
                "X-Devflow-CSRF": (await client.get("/api/session")).json()["csrf_token"],
            }
            preflight = await client.post(
                "/api/runs/run-1/repair-admission-preflight", json=command
            )
            assert preflight.status_code == 200, preflight.text
            response = await client.post(
                "/api/runs/run-1/continue-repair", json=command, headers=headers
            )
            assert response.status_code == 200, response.text
            admitted = response.json()
            assert admitted["additional_iterations"] == 0
            assert admitted["implementation_authority"] is False
            assert (
                await client.post("/api/runs/run-1/continue-repair", json=command, headers=headers)
            ).json() == admitted
        store = case.store
        current = store.effective_spec("run-1")
        with store._connect() as db:
            recovery = json.loads(
                db.execute("SELECT recovery_json FROM delivery_runs").fetchone()[0]
            )
        store.mark_start("run-1", accepted=True)
        calls = []

        @activity.defn(name="delivery_role")
        async def role(body):
            calls.append(body["role"])
            assert body["iteration"] == 3 and body["role"] in {"review", "verify"}
            assert body["check_evidence"]["state"] == "passed"
            assert body["check_evidence"]["candidate_id"] == recovery["candidate"]["id"]
            return {
                "role": body["role"],
                "iteration": 3,
                "status": "pass",
                "findings": [],
                "session_id": "fresh-" + body["role"],
                "finish_reason": "done",
                "cleanup": "confirmed",
                "candidate": recovery["candidate"],
            }

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start(_body):
            return {"state": "consistent"}

        @activity.defn(name="delivery_ci")
        async def ci(body):
            calls.append("ci")
            return {"state": "passed", "head": body["pull_request"]["head"]}

        @activity.defn(name="delivery_terminal_tracker")
        async def terminal_tracker(body):
            calls.append("terminal_tracker")
            assert body["release"] is True
            with store._connect() as db:
                store.state.release_work(db, current["work_id"], "external:devflow:run-1")
            return {"state": "consistent"}

        @activity.defn(name="delivery_tracker")
        async def tracker(_body):
            return {"state": "consistent"}

        async with Worker(
            environment.client,
            task_queue="owned-prerequisite-gates",
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_gates_readback,
                delivery_project,
                delivery_finalize_resources,
                delivery_precheck,
                delivery_checks,
                role,
                tracker_start,
                ci,
                terminal_tracker,
                tracker,
            ],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                args=[current, recovery],
                id=admitted["workflow_id"],
                task_queue="owned-prerequisite-gates",
            )
            result = await asyncio.wait_for(handle.result(), timeout=120)
            assert result["outcome"] == "delivered", result
            assert result["iteration"] == 3
            assert result["roles"][0] == original["result"]["roles"][0]
            assert calls == ["review", "verify", "ci", "terminal_tracker"]
            assert result["checks"]["prepublish"]["state"] == "passed"
            assert result["checks"]["local"]["state"] == "passed"
            from devflow_temporal.delivery_check_evidence import junit_counts

            planned = result["checks"]["local"]["results"][-1]
            assert (
                junit_counts(
                    planned["artifacts"], recovery["candidate"]["id"], Path(current["state_dir"])
                )["passed"]
                == 1
            )
            assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
            assert all(p.read_bytes() == b for p, b in original_native.items())
            with store._connect() as db:
                assert db.execute("SELECT COUNT(*) FROM delivery_repair_grants").fetchone()[0] == 0
                assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 1
                assert store.state.claim_for(db, current["work_id"]) is None
            journals = []
            root = Path(current["state_dir"]) / "ordinary-prerequisite-gates-admission/evidence"
            for path in root.glob("checks/3/*/native/native-process.json"):
                value = read_private(path)
                assert value["runtime_identity"]["dirty"] is False
                assert value["runtime_identity"]["revision"] == current_program()["head"]
                journals.append(
                    {
                        "path": str(path),
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                        "runtime_identity": value["runtime_identity"],
                    }
                )
            assert journals
            assert result["checks"]["local"]["results"][0]["exit_code"] == 0
            assert original["result"]["checks"]["local"]["results"][-1]["exit_code"] == 2
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
                await handle.fetch_history()
            )
            if output := os.environ.get("DEVFLOW_TEST_PREREQUISITE_NATIVE_RESULT"):
                with open(output, "x") as stream:
                    json.dump(
                        {
                            "program": current_program(),
                            "old_failed_local": local,
                            "command": command,
                            "admission": admitted,
                            "recovery": recovery,
                            "result": result,
                            "current_native_journals": journals,
                            "qualification": __doc__,
                        },
                        stream,
                        indent=2,
                    )


def current_program():
    from devflow_temporal.delivery_feature_review_context import current_runtime_identity

    measured = current_runtime_identity()
    return {
        "head": measured["revision"],
        "runtime_payload_sha256": measured["runtime_payload_sha256"],
    }
