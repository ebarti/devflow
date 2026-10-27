from __future__ import annotations

import asyncio
import json
import os
import shutil
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
from test_delivery_store import submit_historical_admission

from devflow_temporal.delivery_activities import (
    delivery_finalize_resources,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_threads import NativeThreadObservation
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow


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
        readiness={"version": 1, "generation": 0, "browser_engines": [],
                   "temporal_checks": []},
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


@pytest.fixture
def native_historical_store(native_configuration, monkeypatch):
    config, request = native_configuration
    store = DeliveryStore(config)
    submit_historical_admission(store, request, monkeypatch)
    return store, request


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
    [("delivered", False), ("delivered", True)],
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
async def test_intake_turn_exhaustion_stops_workflow_and_cleans_directories(
    native_historical_store,
):
    store, request = native_historical_store
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
    native_historical_store, tmp_path
):
    from fixtures.activity_liveness_workflow import FinalizationClock

    store, request = native_historical_store
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
sys.path.insert(0, sys.argv[3])
from fixtures.activity_liveness_workflow import FinalizationClock
import devflow_temporal.delivery_resources as resources
real_remove = resources.remove_directory

def crash_after_remove(path, identity, **kwargs):
    real_remove(path, identity, **kwargs)
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
                      workflows=[DeliveryWorkflow], interceptors=[FinalizationClock()],
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
                str(Path(__file__).parent),
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
                interceptors=[FinalizationClock()],
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
