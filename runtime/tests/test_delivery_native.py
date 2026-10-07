from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture

from devflow_temporal.delivery_activities import (
    delivery_finalize_resources,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_guard import NATIVE_OVERRIDES
from devflow_temporal.delivery_native_preparation import _measure, native_identity
from devflow_temporal.delivery_native_threads import NativeThreadObservation
from devflow_temporal.delivery_preparation import (
    PreparationError,
    prepare_authority,
    verify_prepared_spec,
)
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.runtime_dependencies import locked_dependency_identity
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS boundary required")
def test_actual_native_boundary_denies_controller_and_recursive_commands(tmp_path):
    state = tmp_path / "runs/native"
    state.mkdir(parents=True, mode=0o700)
    binary = str(Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex")))
    spec = {
        "run_id": "native",
        "state_dir": str(state),
        "checkout": str(tmp_path / "checkout"),
        "policy_digest": "0" * 64,
        "policy": {
            "execution_backend": "native-macos",
            "codex_bin": binary,
            "config_overrides": NATIVE_OVERRIDES,
            "runtime_dependencies": locked_dependency_identity(),
        },
    }
    try:
        identity = native_identity(spec)
        proof = _measure(spec, identity)
        assert proof["measurement"]["process_cleanup"] == "observed-native-confirmed"
    finally:
        receipt = RunResources(spec).finalize("cancelled")
    assert receipt["resource_cleanup"] == "confirmed"
    assert not (state / "transient").exists()


@pytest.fixture
def native_configuration(intake_fixture):
    path, request = intake_fixture
    config = json.loads(path.read_text())
    config.update(
        provider="codex",
        execution_backend="native-macos",
        codex_bin=str(
            Path(distribution("openai-codex-cli-bin").locate_file("codex_cli_bin/bin/codex"))
        ),
        max_repairs=1,
    )
    config.pop("fake_intake")
    config["roles"] = {role: {"model": "gpt-6.1-sol", "effort": "max"} for role in config["roles"]}
    check = {
        "id": "native-check",
        "argv": [
            str(Path(sys.executable).resolve()),
            "-c",
            "import os,pathlib; pathlib.Path(os.environ['TMPDIR'],'used').write_text('temp'); "
            "print('2 passed')",
        ],
        "test_count_regex": r"(\d+) passed",
        "min_tests": 2,
    }
    config["repositories"]["fixture"].update(
        prepublish_checks=[check],
        checks=[check],
        required_ci=["test"],
        project_url="https://github.com/users/example/projects/1",
        assignee="example",
    )
    path.write_text(json.dumps(config))
    return DeliveryConfig.load(path), request


@pytest.fixture
def native_store(native_configuration):
    config, request = native_configuration
    store = DeliveryStore(config)
    store.submit(request)
    return store, request


def test_native_checks_share_browser_assets_without_sharing_private_homes(
    native_store, tmp_path, monkeypatch
):
    from devflow_temporal.delivery_sandbox import prepare_native_check

    store, request = native_store
    spec = store.spec(request["run_id"])
    checkout = Path(spec["checkout"])
    checkout.mkdir(parents=True)
    original_home = tmp_path / "original-home"
    browser_cache = original_home / "Library/Caches/ms-playwright"
    browser_cache.mkdir(parents=True)
    monkeypatch.setattr(Path, "home", lambda: original_home)
    environments = []
    for name in ("browser-a", "browser-b"):
        profile, environment = prepare_native_check(
            spec, checkout, Path(spec["state_dir"]) / "checks",
            {"id": name, "argv": ["node", "browser.cjs"]},
        )
        environments.append(environment)
        assert environment["PLAYWRIGHT_BROWSERS_PATH"] == str(browser_cache)
        assert Path(environment["HOME"]) != original_home
        lines = (Path(environment["CODEX_HOME"]) / "config.toml").read_text()
        assert f'{json.dumps(str(browser_cache.resolve()))} = "read"' in lines
        assert f'{json.dumps(str(original_home.resolve()))} = "read"' not in lines
        assert profile == "devflow-check"
    assert environments[0]["HOME"] != environments[1]["HOME"]


def test_native_admission_has_no_docker_policy_and_bounds_nested_launches(
    native_store, monkeypatch
):
    store, request = native_store
    spec = store.submitted_spec(request["run_id"])
    assert spec["resource_cleanup_version"] == 1
    assert "container" not in spec["policy"]
    assert spec["policy"]["config_overrides"] == NATIVE_OVERRIDES
    supervisor = DeliverySupervisor(store, capacity=1)
    role = {"spec": spec, "role": "intake", "iteration": 8, "candidate": {"id": "bounded"}}
    with pytest.raises(ValueError, match="finite turn limit"):
        supervisor._claim(role)
    monkeypatch.setenv("DEVFLOW_MANAGED_DEPTH", "1")
    with pytest.raises(ValueError, match="another Devflow"):
        supervisor._claim({**role, "iteration": 0})
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS boundary required")
def test_actual_native_preparation_cache_and_check_cleanup(native_store, monkeypatch):
    store, request = native_store
    submitted = store.submitted_spec(request["run_id"])
    first = prepare_authority(store, submitted)
    verify_prepared_spec(first)
    assert first["plan_approval"] == submitted["plan_approval"] == "automatic"
    with pytest.raises(PreparationError, match="run binding changed"):
        verify_prepared_spec({**first, "plan_approval": "required"})
    assert first["preparation"]["cache_reused"] is False
    second_request = {
        **request,
        "run_id": "run-2",
        "work_id": "work-2",
        "command_id": "submit-2",
        "branch": "feat/native-two",
        "issue_url": "https://github.com/example/fixture/issues/4",
    }
    store.submit(second_request)
    second = prepare_authority(store, store.submitted_spec("run-2"))
    assert second["preparation"]["cache_reused"] is True
    assert second["preparation"]["fingerprint"] == first["preparation"]["fingerprint"]
    assert (
        second["preparation"]["security_binding_sha256"]
        != first["preparation"]["security_binding_sha256"]
    )
    broker = DeliveryBroker(store, first)
    candidate = broker.prepare()["candidate"]
    checks = broker.run_prechecks(0, candidate)
    assert checks["state"] == "passed"
    assert checks["results"][0]["process_cleanup"] == "observed-native-confirmed"
    assert checks["results"][0]["test_count"] == 2
    transient = Path(first["state_dir"]) / "transient"
    assert transient.exists()
    receipt = RunResources(first).finalize("cancelled")
    assert receipt["state"] == "confirmed"
    assert not transient.exists() and not Path(first["checkout"]).exists()
    assert Path(checks["results"][0]["log"]).is_file()
    assert RunResources(second).finalize("cancelled")["state"] == "confirmed"
    verify_prepared_spec(second)  # shared proof references retained durable evidence


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native grant/check boundary required")
def test_authorized_gate_beyond_original_budget_runs_check_then_retries_cleanup(
    native_configuration,
):
    config, request = native_configuration
    repo = config.raw["repositories"]["fixture"]
    source = Path(repo["source_path"])
    source.joinpath(".gitignore").write_text("node_modules/\n")
    subprocess.run(["git", "-C", str(source), "add", ".gitignore"], check=True)
    subprocess.run(
        ["git", "-C", str(source), "commit", "-qm", "owned dependency fixture"], check=True
    )
    repo["expected_base_sha"] = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        text=True,
    ).strip()
    for stage in ("checks", "prepublish_checks"):
        repo[stage][0]["argv"][-1] = (
            "import pathlib; pathlib.Path('node_modules').mkdir(); "
            "pathlib.Path('node_modules/temporary').write_text('owned'); print('2 passed')"
        )
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    prepared = prepare_authority(store, store.submitted_spec(request["run_id"]))
    broker = DeliveryBroker(store, prepared)
    candidate = broker.prepare()["candidate"]
    role = {"spec": prepared, "role": "implement", "iteration": 2, "candidate": candidate}
    with pytest.raises(ValueError, match="finite turn limit"):
        DeliverySupervisor(store, capacity=1)._claim(role)
    with pytest.raises(ValueError, match="finite turn limit"):
        broker.run_checks(2, candidate)
    gate = Path(prepared["state_dir"]) / "gates/2/verify"
    assert not gate.exists()
    with store._connect() as db:
        db.execute(
            """INSERT INTO delivery_repair_grants VALUES (?,?,?,?,?,?,?,?)""",
            (
                request["run_id"],
                "controlled-native-grant",
                "fixture",
                "fixture",
                "0" * 64,
                1,
                2,
                "2026-10-02T00:00:00Z",
            ),
        )
    result = broker.run_checks(2, candidate)
    assert result["state"] == "passed" and (gate / "node_modules/temporary").is_file()
    resources = RunResources(prepared)
    assert resources.finalize("delivered")["state"] == "confirmed"
    assert not gate.exists() and not Path(prepared["checkout"]).exists()
    assert resources.finalize("delivered")["state"] == "confirmed"
    assert Path(result["results"][0]["log"]).is_file()


@pytest.mark.asyncio
async def test_native_observer_detects_sdk_spawn_item_dropped_by_kit_audit(tmp_path):
    from agent_runtime_kit.adapters.codex import _tool_audits
    from openai_codex.generated.v2_all import CollabAgentToolCallThreadItem, ThreadItem

    folder = tmp_path / "attempt"
    folder.mkdir()
    write_private(
        folder / "native-process.json",
        {"owned": {str(os.getpid()): {"identity": "controlled identity"}}},
    )
    observation = NativeThreadObservation(
        {
            "result_path": str(folder / "result.json"),
            "spec": {"run_id": "fixture"},
            "role": "implement",
            "iteration": 0,
        }
    )
    calls = []
    inventory = []

    class Client:
        async def thread_list(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                data=[SimpleNamespace(id=name) for name in inventory], next_cursor=None
            )

    client = Client()
    await observation.begin(client)
    observation.started("parent")
    assert read_private(observation.path)["parent_thread_id"] == "parent"
    item = ThreadItem(
        CollabAgentToolCallThreadItem(
            agentsStates={},
            id="spawn-event",
            receiverThreadIds=["child"],
            senderThreadId="parent",
            status="completed",
            tool="spawnAgent",
            type="collabAgentToolCall",
        )
    )
    assert _tool_audits([item]) == []  # original filtered-audit false-negative trigger
    inventory.extend(["parent", "child"])
    await observation.completed(client, SimpleNamespace(items=[item]))
    assert observation.reference()["state"] == "blocked"
    assert observation.data["new_child_thread_ids"] == ["child"]
    assert observation.data["collaboration_items"][0]["tool"] == "spawnAgent"
    assert all(
        any(kind.value == "subAgentThreadSpawn" for kind in call["source_kinds"]) for call in calls
    )
    assert {call["archived"] for call in calls} == {True, False}


@pytest.mark.asyncio
async def test_native_observer_incomplete_inventory_cannot_claim_zero_children(tmp_path):
    folder = tmp_path / "attempt"
    folder.mkdir()
    write_private(
        folder / "native-process.json",
        {"owned": {str(os.getpid()): {"identity": "controlled identity"}}},
    )
    observation = NativeThreadObservation(
        {
            "result_path": str(folder / "result.json"),
            "spec": {"run_id": "fixture"},
            "role": "implement",
            "iteration": 0,
        }
    )

    class Client:
        async def thread_list(self, **_kwargs):
            return SimpleNamespace(data=[], next_cursor="more")

    with pytest.raises(ValueError, match="bounded observation"):
        await observation.begin(Client())
    assert read_private(observation.path)["state"] == "unknown"
    assert observation.data["new_child_thread_ids"] is None


@activity.defn(name="delivery_intake")
async def blocked_intake(request):
    return {
        "status": "blocked",
        "summary": "controlled terminal cleanup fixture",
        "findings": [],
        "session_id": None,
        "usage": None,
        "cleanup": "confirmed",
    }


@workflow.defn(name="ControlledNativeTerminalFixture")
class ControlledNativeTerminalFixture:
    @workflow.run
    async def run(self, request):
        controller = DeliveryWorkflow()
        outcome = request["outcome"]
        controller.state = {
            "run_id": request["spec"]["run_id"],
            "phase": outcome,
            "execution_state": outcome,
            "outcome": outcome,
            "revision": 1,
            "iteration": 0,
            "roles": [],
            "checks": {},
            "tracker": {},
            "usage": {},
            "cleanup": "unknown" if request["uncertain"] else "none",
        }
        await controller._project(request["spec"], outcome, "controlled terminal fixture; no PR")
        return controller.state


@activity.defn(name="delivery_terminal_tracker")
async def controlled_terminal_tracker(request):
    return {"state": "consistent", "pending": False, "desired": request["status"],
            "release": request["release"], "observed": {"fixture": True}}


@pytest.mark.parametrize(
    "outcome,uncertain",
    [
        ("delivered", False),
        ("cancelled", False),
        ("blocked", False),
        ("delivered", True),
    ],
)
@pytest.mark.asyncio
async def test_real_terminal_workflow_removes_owned_temps_before_projection(
    native_store, outcome, uncertain
):
    store, request = native_store
    submitted = store.submitted_spec(request["run_id"])
    DeliveryBroker(store, submitted).prepare()
    resources = RunResources(submitted)
    scratch = resources.scratch("terminal", outcome)
    scratch.joinpath("owned-temp").write_text("temporary")
    evidence = Path(submitted["state_dir"]) / "durable-fixture.txt"
    evidence.write_text("controlled evidence; no model, tracker or PR publication")
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="controlled-native-terminal",
            workflows=[ControlledNativeTerminalFixture],
            workflow_runner=UnsandboxedWorkflowRunner(),
            activities=[delivery_project, delivery_finalize_resources, controlled_terminal_tracker],
        ):
            result = await environment.client.execute_workflow(
                ControlledNativeTerminalFixture.run,
                {"spec": submitted, "outcome": outcome, "uncertain": uncertain},
                id="controlled-native-terminal",
                task_queue="controlled-native-terminal",
            )
    receipt = result["checks"]["resource_cleanup"]
    assert store.detail(request["run_id"])["checks"]["resource_cleanup"] == receipt
    assert evidence.is_file() and Path(receipt["receipt"]).is_file()
    if uncertain:
        assert result["outcome"] == "blocked" and receipt["state"] == "unknown"
        assert scratch.exists() and Path(submitted["checkout"]).exists()
    else:
        assert result["outcome"] == outcome and receipt["state"] == "confirmed"
        assert not scratch.exists()
        assert Path(submitted["checkout"]).exists() == (outcome == "blocked")
        assert all(
            not Path(root["path"]).exists()
            for root in receipt["roots"]
            if root["state"] in {"removed", "already_absent"}
        )


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native preparation required")
@pytest.mark.asyncio
async def test_intake_turn_exhaustion_stops_workflow_and_cleans_directories(native_store):
    store, request = native_store
    calls = []

    @activity.defn(name="delivery_intake")
    async def questions(request):
        calls.append(request["iteration"])
        return {
            "status": "questions",
            "summary": "controlled repeated question fixture",
            "questions": [{"id": "fixture", "prompt": "Controlled question", "options": [],
                           "blocker": {
                               "unknown": f"External requirement {request['iteration']} is absent",
                               "evidence_checked": ["Repository has no external consumer contract"],
                               "why_no_safe_default": "Guessing cannot establish compatibility",
                           }}],
            "findings": [],
            "session_id": None,
            "usage": None,
            "cleanup": "confirmed",
        }

    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="native-turn-exhaustion",
            workflows=[DeliveryWorkflow],
            activities=[delivery_prepare, delivery_project, delivery_finalize_resources, questions,
                        controlled_terminal_tracker],
        ):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.submitted_spec(request["run_id"]),
                id="native-turn-exhaustion",
                task_queue="native-turn-exhaustion",
            )
            for turn in range(8):
                deadline = time.monotonic() + 20
                while True:
                    state = await handle.query("status")
                    pending = state.get("decision")
                    if pending and pending["id"].endswith(f":{turn}:fixture"):
                        break
                    assert time.monotonic() < deadline, state
                    await asyncio.sleep(0.03)
                await handle.execute_update(
                    "decision",
                    {
                        "command_id": f"controlled-answer-{turn}",
                        "expected_revision": state["revision"],
                        "decision_id": pending["id"],
                        "decision_revision": pending["revision"],
                        "candidate_revision": pending["candidate_revision"],
                        "answer": "fixture",
                    },
                )
            result = await asyncio.wait_for(handle.result(), 15)
    assert calls == list(range(8)) and "finite turn limit" in result["error"]
    assert result["outcome"] == "blocked"
    assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
    saved = store.spec(request["run_id"])
    assert not (Path(saved["state_dir"]) / "transient").exists()
    assert Path(saved["checkout"]).exists()  # blocked-run recovery source is deliberate
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native identity failure required")
@pytest.mark.asyncio
async def test_early_preparation_error_finalizes_registered_temporary_resources(
    native_configuration,
):
    config, request = native_configuration
    config.raw["codex_bin"] = "/usr/bin/false"
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    submitted = store.submitted_spec(request["run_id"])
    scratch = RunResources(submitted).scratch("preparation", "allocated-before-failure")
    scratch.joinpath("owned-temp").write_text("temporary")
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
    ) as environment:
        async with Worker(
            environment.client,
            task_queue="native-early-preparation-error",
            workflows=[DeliveryWorkflow],
            activities=[delivery_prepare, delivery_project, delivery_finalize_resources,
                        controlled_terminal_tracker],
        ):
            result = await environment.client.execute_workflow(
                DeliveryWorkflow.run,
                submitted,
                id="native-early-preparation-error",
                task_queue="native-early-preparation-error",
            )
    assert result["outcome"] == "blocked" and "same bundled executable" in result["error"]
    assert result["checks"]["resource_cleanup"]["state"] == "confirmed"
    assert not scratch.exists() and not Path(submitted["checkout"]).exists()
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0] == 0


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS boundary required")
@pytest.mark.asyncio
async def test_real_worker_loss_after_directory_removal_recovers_workflow_finalization(
    native_store, tmp_path
):
    store, request = native_store
    marker = tmp_path / "removed-before-worker-loss"
    worker_script = tmp_path / "crashing-worker.py"
    worker_script.write_text("""
import asyncio, os, signal, sys
from pathlib import Path
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker
from devflow_temporal.delivery_activities import delivery_prepare, delivery_project
from devflow_temporal.delivery_activities import delivery_finalize_resources as finalize
from devflow_temporal.delivery_workflow import DeliveryWorkflow
import devflow_temporal.delivery_resources as resources
real_remove = resources.remove_directory

def crash_after_remove(path, identity):
    real_remove(path, identity)
    Path(sys.argv[2]).write_text(str(os.getpid()))
    os.kill(os.getpid(), signal.SIGKILL)

resources.remove_directory = crash_after_remove

@activity.defn(name='delivery_finalize_resources')
async def finalization(request):
    return await finalize(request)

@activity.defn(name='delivery_intake')
async def intake(request):
    return {'status':'blocked','summary':'controlled terminal cleanup fixture',
            'findings':[],'session_id':None,'usage':None,'cleanup':'confirmed'}

@activity.defn(name='delivery_terminal_tracker')
async def tracker(request):
    return {'state':'consistent','pending':False,'desired':request['status'],
            'release':request['release'],'observed':{'fixture':True}}

async def main():
    client = await Client.connect(sys.argv[1])
    async with Worker(client, task_queue='native-finalize-restart',
                      workflows=[DeliveryWorkflow],
                      activities=[delivery_prepare,delivery_project,intake,finalization,tracker]):
        await asyncio.Event().wait()
asyncio.run(main())
""")
    async with local_temporal(
        dev_server_existing_path=shutil.which("temporal"),
        dev_server_database_filename=str(tmp_path / "temporal-restart.sqlite3"),
    ) as environment:
        worker_log = (tmp_path / "fixture-worker.log").open("wb")
        worker = subprocess.Popen(
            [
                sys.executable,
                "-I",
                str(worker_script),
                environment.client.service_client.config.target_host,
                str(marker),
            ],
            stdout=worker_log,
            stderr=subprocess.STDOUT,
        )
        try:
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run,
                store.submitted_spec(request["run_id"]),
                id="native-finalize-restart",
                task_queue="native-finalize-restart",
            )
            deadline = time.monotonic() + 25
            while not marker.exists():
                if worker.poll() is not None or time.monotonic() >= deadline:
                    raise AssertionError("owned fixture worker did not reach removal crash window")
                await asyncio.sleep(0.1)
            await asyncio.to_thread(worker.wait, 5)
            assert worker.returncode == -9 and int(marker.read_text()) == worker.pid
            assert not (Path(store.spec(request["run_id"])["state_dir"]) / "transient").exists()
            async with Worker(
                environment.client,
                task_queue="native-finalize-restart",
                workflows=[DeliveryWorkflow],
                activities=[
                    delivery_prepare,
                    delivery_project,
                    blocked_intake,
                    delivery_finalize_resources,
                    controlled_terminal_tracker,
                ],
            ):
                result = await asyncio.wait_for(handle.result(), 60)
            receipt = result["checks"]["resource_cleanup"]
            assert result["outcome"] == "blocked"  # controlled role failure, no provider/PR
            assert receipt["resource_cleanup"] == "confirmed"
            assert any(item["state"] == "already_absent" for item in receipt["roots"])
            assert store.detail(request["run_id"])["checks"]["resource_cleanup"] == receipt
            with store._connect() as db:
                assert db.execute("SELECT COUNT(*) FROM delivery_preparations").fetchone()[0] == 1
        finally:
            if worker.poll() is None:
                worker.terminate()
                await asyncio.to_thread(worker.wait, 5)
            worker_log.close()


@pytest.mark.skipif(sys.platform != "darwin", reason="actual native macOS browser fixture")
@pytest.mark.parametrize("iteration", [0, 2])
def test_native_browser_api_playwright_ports_children_and_directory_cleanup(
    native_configuration, monkeypatch, iteration
):
    node_root = Path.home() / ".cache/codex-runtimes/codex-primary-runtime/dependencies/node"
    node = node_root / "bin/node"
    library = node_root / "node_modules/playwright"
    browsers = sorted(
        (Path.home() / "Library/Caches/ms-playwright").glob(
            "chromium_headless_shell-*/chrome-headless-shell-mac-arm64/chrome-headless-shell"
        ),
        reverse=True,
    )
    if not node.is_file() or not library.is_dir() or not browsers:
        pytest.skip("installed local Playwright/browser fixture dependencies required")
    browser = browsers[0]
    ports = []
    for _ in range(2):
        with socket.socket() as lease:
            lease.bind(("127.0.0.1", 0))
            ports.append(lease.getsockname()[1])
    assert ports[0] != ports[1]
    config, request = native_configuration
    repository = config.raw["repositories"]["fixture"]
    source = Path(repository["source_path"])
    program = source / "native-qa.cjs"
    (source / ".gitignore").write_text("qa-artifacts/\nnode_modules/\n")
    program.write_text(
        """
const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { chromium } = require(LIBRARY);
const apiPort = Number(process.env.QA_API_PORT);
const webPort = Number(process.env.QA_WEB_PORT);
const api = http.createServer((request,response) => {
  response.setHeader('Access-Control-Allow-Origin', 'http://127.0.0.1:'+webPort);
  response.setHeader('Content-Type', 'application/json');
  response.end(JSON.stringify({message:'Ready from API'}));
});
const web = http.createServer((request,response) => response.end(
  `<button onclick="fetch('http://127.0.0.1:${apiPort}/state').then(r=>r.json())
   .then(x=>document.querySelector('p').textContent=x.message)">Check API</button><p>Waiting</p>`
));
const detached = spawn(process.execPath, ['-e','setTimeout(()=>{},30000)'],
                       {detached:true,stdio:'ignore'});
fs.writeFileSync(path.join(process.env.TMPDIR,'detached-pid'),String(detached.pid));
async function main() {
  await Promise.all([new Promise(resolve=>api.listen(apiPort,'127.0.0.1',resolve)),
                     new Promise(resolve=>web.listen(webPort,'127.0.0.1',resolve))]);
  const browser = await chromium.launch({headless:true,executablePath:BROWSER});
  const page = await browser.newPage();
  await page.goto('http://127.0.0.1:'+webPort);
  await page.getByRole('button',{name:'Check API'}).click();
  await page.getByText('Ready from API').waitFor();
  assert.equal(await page.locator('p').textContent(),'Ready from API');
  const response = await page.request.get('http://127.0.0.1:'+apiPort+'/state');
  assert.deepEqual(await response.json(),{message:'Ready from API'});
  fs.mkdirSync('qa-artifacts');
  await page.screenshot({path:'qa-artifacts/browser.png'});
  await browser.close();
  await new Promise(resolve=>setTimeout(resolve,1000));
  console.log('2 passed');
  process.exit(0); // broker must stop the observed detached child and ports
}
main().catch(error=>{console.error(error);process.exit(1)});
""".replace("LIBRARY", json.dumps(str(library))).replace("BROWSER", json.dumps(str(browser)))
    )
    for args in (("add", "native-qa.cjs", ".gitignore"), ("commit", "-qm", "native QA fixture")):
        subprocess.run(["git", "-C", str(source), *args], check=True)
    repository["expected_base_sha"] = subprocess.check_output(
        ["git", "-C", str(source), "rev-parse", "HEAD"], text=True
    ).strip()
    qa = {
        "id": "native-browser",
        "argv": [str(node), "native-qa.cjs"],
        "ports": {"QA_API_PORT": ports[0], "QA_WEB_PORT": ports[1]},
        "env": {"JOBCTRL_E2E_ISOLATED": "1"},
        "read_roots": [str(node_root.resolve()), str(browser.parent.parent.resolve())],
        "test_count_regex": r"(\d+) passed",
        "min_tests": 2,
        "timeout_seconds": 60,
        "artifact_paths": ["qa-artifacts"],
    }
    repository["browser_qa"] = qa
    config.path.write_text(json.dumps(config.raw))
    store = DeliveryStore(config)
    store.submit(request)
    if iteration == 2:
        with store._connect() as db:
            db.execute(
                """INSERT INTO delivery_repair_grants VALUES (?,?,?,?,?,?,?,?)""",
                (
                    request["run_id"],
                    "controlled-browser-grant",
                    "fixture",
                    "fixture",
                    "0" * 64,
                    1,
                    2,
                    "2026-10-02T00:00:00Z",
                ),
            )
    prepared = prepare_authority(store, store.submitted_spec(request["run_id"]))
    broker = DeliveryBroker(store, prepared)
    candidate = broker.prepare()["candidate"]
    with pytest.raises(ValueError, match="finite turn limit"):
        broker.run_browser_qa(3, candidate)
    result = broker.run_browser_qa(iteration, candidate)
    assert result["state"] == "passed", result.get("diagnostic")
    assert result["test_count"] == 2 and result["process_cleanup"] == "observed-native-confirmed"
    assert len(result["native_process"]["observed_owned_pids"]) >= 3
    assert set(result["native_process"]["observed_listeners"]) == {str(port) for port in ports}
    assert broker.run_browser_qa(iteration, candidate) == result
    receipt = RunResources(prepared).finalize("cancelled")
    assert receipt["resource_cleanup"] == "confirmed"
    assert not (Path(prepared["state_dir"]) / "transient").exists()
    assert not Path(result["scratch"]).exists()
    assert not (Path(prepared["state_dir"]) / "gates" / str(iteration) / "verify").exists()
    assert Path(result["log"]).is_file() and Path(result["artifacts"][0]["path"]).is_file()
    for port in ports:
        with socket.socket() as lease:
            lease.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            lease.bind(("127.0.0.1", port))
            lease.listen()
