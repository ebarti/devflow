"""Portable package safety and stdio-to-API delivery on disposable local fixtures."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shutil
import socket
from pathlib import Path

import pytest
import uvicorn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import Worker
from test_delivery_intake import intake_fixture as source_intake_fixture

from devflow_temporal.delivery_activities import (
    delivery_accept_plan,
    delivery_intake,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_workflow import DeliveryWorkflow

RUNTIME = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location(
    "package_plugin", RUNTIME / "desktop/package_plugin.py"
)
packager = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packager)


@pytest.fixture
def package_fixture(tmp_path):
    runtime = tmp_path / "runtime with spaces"
    executable = runtime / ".venv/bin/devflow-delivery-mcp"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    config = tmp_path / "private config.json"
    config.write_text("SECRET_CONFIG_CONTENT_MUST_NOT_BE_EMBEDDED")
    return tmp_path / "marketplace", runtime, config


@pytest.mark.parametrize("component", ["marketplace", ".agents/plugins"])
def test_symlink_destinations_never_escape_root(package_fixture, tmp_path, component):
    root, runtime, config = package_fixture
    outside = tmp_path / "outside"
    outside.mkdir()
    link = root if component == "marketplace" else root / component
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        packager.package(root, runtime, config)
    assert list(outside.iterdir()) == []


@pytest.mark.asyncio
async def test_generated_stdio_command_raw_goal_question_auto_plan_and_evidence(tmp_path):
    path, request = source_intake_fixture.__wrapped__(tmp_path)
    config = json.loads(path.read_text())
    config["fake_intake"] = [config["fake_intake"][0], config["fake_intake"][2]]
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    config["dashboard_url"] = f"http://127.0.0.1:{listener.getsockname()[1]}"
    path.write_text(json.dumps(config))
    app = create_app(path)
    store = app.state.delivery.store
    calls = []

    @activity.defn(name="delivery_tracker_start")
    async def tracker(_payload):
        return {"state": "consistent"}

    @activity.defn(name="delivery_role")
    async def implement(payload):
        calls.append(payload)
        return {"status": "blocked", "candidate": payload["candidate"]}

    # Only lifecycle startup is bypassed: the fixture owns its API and Temporal
    # server. The generated command uses the real stdio executable and client.
    shim = tmp_path / "startup-shim"
    shim.mkdir()
    (shim / "sitecustomize.py").write_text(
        "from devflow_temporal import delivery_control\n"
        "delivery_control.ensure_service_running = lambda config: None\n"
    )
    target = packager.package(tmp_path / "marketplace", RUNTIME, path)
    command = json.loads((target / "mcp.json").read_text())["mcpServers"]["devflow-local-delivery"]
    parameters = StdioServerParameters(
        command=command["command"], args=command["args"],
        env={"PYTHONPATH": str(shim), "PATH": os.environ["PATH"]},
    )
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "temporal.sqlite3"),
    ) as environment:
        async def client():
            return environment.client

        app.state.delivery.client = client
        server = uvicorn.Server(uvicorn.Config(app, lifespan="off", log_level="error"))
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        try:
            for _ in range(100):
                if server.started:
                    break
                await asyncio.sleep(0.01)
            assert server.started
            async with (
                stdio_client(parameters) as (read, write),
                ClientSession(read, write) as session,
            ):
                initialized = await session.initialize()
                assert "get_service" in initialized.instructions

                async def call(name, arguments=None):
                    result = await session.call_tool(name, arguments)
                    assert not result.isError, result.content
                    return json.loads(result.content[0].text)

                policy = (await call("get_service"))["policy"]
                assert policy["repositories"][0]["key"] == "fixture"
                assert "source_path" not in json.dumps(policy)
                assert policy["authorized_endpoint"] == "published_unmerged"
                receipt = await call("submit_run", {"request_json": json.dumps(request)})
                assert receipt["phase"] == "accepted"  # Fake-provider admission skips native prep.
                assert await call("submit_run", {"request_json": json.dumps(request)}) == receipt
                queue = "plugin-intake"
                async with Worker(environment.client, task_queue=queue,
                                  workflows=[DeliveryWorkflow],
                                  activities=[delivery_project, delivery_prepare, delivery_intake,
                                              delivery_accept_plan, tracker, implement]):
                    handle = await environment.client.start_workflow(
                        DeliveryWorkflow.run, store.spec("run-1"),
                        id="delivery-run-1", task_queue=queue,
                    )
                    store.mark_start("run-1", accepted=True)
                    for _ in range(200):
                        waiting = store.detail("run-1")
                        if waiting["phase"] == "waiting_question":
                            break
                        await asyncio.sleep(0.05)
                    assert waiting["phase"] == "waiting_question" and not calls
                    observed = (await call("get_run", {"run_id": "run-1"}))["run"]
                    decision = observed["decisions"][0]
                    answer = {"command_id": "answer-question",
                              "expected_revision": observed["revision"],
                              "decision_id": decision["id"],
                              "decision_revision": decision["revision"],
                              "candidate_revision": decision["candidate_revision"], "answer": "A"}
                    await call("answer_decision", {"run_id": "run-1",
                                                 "request_json": json.dumps(answer)})
                    result = await asyncio.wait_for(handle.result(), 20)
                    assert result["phase"] == "blocked"  # Deliberate role stub; no publication.
                detail = await call("get_run", {"run_id": "run-1"})
                assert detail["run"]["decisions"] == []
                assert detail["run"]["intake"]["accepted_plan"]["authorization"]
                assert len(calls) == 1 and calls[0]["role"] == "implement"
                assert (await call("list_runs"))["runs"][0]["id"] == "run-1"
                assert detail["evidence"]
                artifact = await call("read_evidence", {
                    "run_id": "run-1", "evidence_id": detail["evidence"][0]["id"],
                })
                assert artifact
        finally:
            server.should_exit = True
            await asyncio.wait_for(serving, 5)
            listener.close()
