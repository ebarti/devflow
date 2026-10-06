"""Portable package safety and stdio-to-API delivery on disposable local fixtures."""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import shutil
import socket
import sys
import tempfile
from pathlib import Path

import pytest
import uvicorn
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
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


@pytest.mark.parametrize("package_format", ["portable", "codex"])
def test_discovery_canonical_skill_explicit_paths_and_idempotency(package_fixture, package_format):
    root, runtime, config = package_fixture
    target = packager.package(root, runtime, config, package_format=package_format)
    catalog = json.loads((root / ".agents/plugins/marketplace.json").read_text())
    assert root / catalog["plugins"][0]["source"]["path"] == target
    if package_format == "portable":
        manifest = json.loads((target / "plugin.json").read_text())
        assert manifest["$schema"].endswith("/1.0.0/plugin.schema.json")
        assert manifest["extensions"]["com.openai"]["interface"]["displayName"]
        mcp = json.loads((target / "mcp.json").read_text())
        assert mcp["$schema"].endswith("/1.0.0/mcp.schema.json")
        assert not (target / ".codex-plugin").exists()
    else:
        manifest = json.loads((target / ".codex-plugin/plugin.json").read_text())
        assert manifest["skills"] == "./skills/" and manifest["mcpServers"] == "./.mcp.json"
        assert manifest["interface"]["displayName"]
        assert not (target / "plugin.json").exists() and not (target / "mcp.json").exists()
        mcp = json.loads((target / ".mcp.json").read_text())
    assert mcp["mcpServers"] == {"devflow-local-delivery": {
        "type": "stdio", "command": str(runtime / ".venv/bin/devflow-delivery-mcp"),
        "args": ["--config", str(config)],
    }}
    source = RUNTIME / "desktop/devflow-local-delivery/SKILL.md"
    assert (target / "skills/devflow-local-delivery/SKILL.md").read_bytes() == source.read_bytes()
    files = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob("*") if p.is_file()}
    assert all(b"SECRET_CONFIG" not in data for data, _mtime in files.values())
    assert packager.package(root, runtime, config, package_format=package_format) == target
    assert files == {p: (p.read_bytes(), p.stat().st_mtime_ns)
                     for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize("package_format", ["portable", "codex"])
def test_package_root_resolves_ancestor_alias_before_descendant_checks(
    package_fixture, tmp_path, package_format,
):
    _root, runtime, config = package_fixture
    physical = tmp_path / "physical parent"
    physical.mkdir()
    alias = tmp_path / "parent alias"
    alias.symlink_to(physical, target_is_directory=True)
    selected = alias / "new marketplace"
    target = packager.package(selected, runtime, config, package_format=package_format)
    assert target == physical / "new marketplace/plugins/devflow"
    before = {p: (p.read_bytes(), p.stat().st_mtime_ns)
              for p in target.parent.parent.rglob("*") if p.is_file()}
    assert packager.package(selected, runtime, config, package_format=package_format) == target
    assert packager.package(selected.resolve(), runtime, config,
                            package_format=package_format) == target
    assert before == {p: (p.read_bytes(), p.stat().st_mtime_ns)
                      for p in target.parent.parent.rglob("*") if p.is_file()}


@pytest.mark.skipif(sys.platform != 'darwin', reason='native macOS temporary-root spelling')
def test_native_temporary_package_root_uses_physical_path(package_fixture):
    _root, runtime, config = package_fixture
    with tempfile.TemporaryDirectory(prefix='devflow-package-alias-') as temporary:
        selected = Path(temporary) / 'marketplace'
        target = packager.package(selected, runtime, config)
        assert target == selected.resolve() / 'plugins/devflow'


@pytest.mark.parametrize("first,second", [("portable", "codex"), ("codex", "portable")])
def test_package_selection_never_rewrites_an_existing_other_layout(package_fixture, first, second):
    root, runtime, config = package_fixture
    packager.package(root, runtime, config, package_format=first)
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="same-name plugin differs"):
        packager.package(root, runtime, config, package_format=second)
    assert before == {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_unrelated_catalog_fields_and_plugins_preserved(package_fixture):
    root, runtime, config = package_fixture
    path = root / ".agents/plugins/marketplace.json"
    path.parent.mkdir(parents=True)
    original = {"name": "team", "interface": {"displayName": "Team"}, "custom": {"x": 1},
                "plugins": [{"name": "other", "source": {"source": "local", "path": "./other"}}]}
    path.write_text(json.dumps(original))
    packager.package(root, runtime, config)
    updated = json.loads(path.read_text())
    assert updated == {**original, "plugins": [*original["plugins"], packager.ENTRY]}


@pytest.mark.parametrize("conflict", ["catalog", "plugin", "extra", "symlink-file"])
def test_conflicts_preserve_both_destinations(package_fixture, conflict):
    root, runtime, config = package_fixture
    target = packager.package(root, runtime, config)
    if conflict == "catalog":
        path = root / ".agents/plugins/marketplace.json"
        catalog = json.loads(path.read_text())
        catalog["plugins"][0]["source"]["path"] = "./elsewhere"
        path.write_text(json.dumps(catalog))
    elif conflict == "plugin":
        (target / "plugin.json").write_text("user change")
    elif conflict == "extra":
        (target / "user.txt").write_text("user data")
    else:
        (target / "plugin.json").unlink()
        (target / "plugin.json").symlink_to(config)
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    with pytest.raises(ValueError):
        packager.package(root, runtime, config)
    assert before == {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert config.read_text() == "SECRET_CONFIG_CONTENT_MUST_NOT_BE_EMBEDDED"


@pytest.mark.parametrize("component", ["marketplace", "plugins", ".agents", ".agents/plugins"])
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


@pytest.mark.parametrize("missing", ["runtime", "executable", "config", "nonexecutable"])
def test_missing_inputs_create_no_package(package_fixture, missing):
    root, runtime, config = package_fixture
    if missing == "runtime":
        runtime = runtime / "absent"
    elif missing == "config":
        config.unlink()
    elif missing == "executable":
        (runtime / ".venv/bin/devflow-delivery-mcp").unlink()
    else:
        (runtime / ".venv/bin/devflow-delivery-mcp").chmod(0o644)
    with pytest.raises((ValueError, OSError)):
        packager.package(root, runtime, config)
    assert not root.exists()


def test_catalog_conflict_is_checked_before_plugin_creation(package_fixture):
    root, runtime, config = package_fixture
    path = root / ".agents/plugins/marketplace.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"name": "team", "plugins": [{"name": "devflow"}]}))
    with pytest.raises(ValueError, match="same-name marketplace"):
        packager.package(root, runtime, config)
    assert not (root / "plugins").exists()


def test_catalog_write_failure_rolls_back_new_plugin(package_fixture, monkeypatch):
    root, runtime, config = package_fixture

    def fail_replace(*_args):
        raise OSError("simulated catalog write failure")

    monkeypatch.setattr(packager.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        packager.package(root, runtime, config)
    assert not (root / "plugins/devflow").exists()
    assert not (root / ".agents/plugins/marketplace.json").exists()
    assert not list(root.glob(".devflow-package-*"))


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
    async with await WorkflowEnvironment.start_local(
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
