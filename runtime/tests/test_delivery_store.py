from __future__ import annotations

import asyncio
import json
import shutil
import socket
import subprocess
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from devflow_temporal.delivery_activities import delivery_prepare, delivery_project, delivery_role
from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_workflow import DeliveryWorkflow
from devflow_temporal.supervisor import DeliverySupervisor


def _git(path: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(path), *args], check=True, capture_output=True, text=True
    )
    return completed.stdout.strip()


@pytest.fixture
def service(tmp_path: Path) -> tuple[DeliveryStore, dict]:
    source = tmp_path / "source"
    source.mkdir()
    (source / "README.md").write_text("Test repository\n")
    _git(source, "init", "-q")
    _git(source, "config", "user.name", "Delivery Test")
    _git(source, "config", "user.email", "delivery@example.invalid")
    _git(source, "add", "README.md")
    _git(source, "commit", "-qm", "Fixture")
    origin = tmp_path / "origin.git"
    subprocess.run(["git", "init", "--bare", "-q", str(origin)], check=True)
    _git(source, "remote", "add", "origin", str(origin))
    root = Path(__file__).resolve().parents[2]
    configuration = {
        "version": 1,
        "tracking_db": str(tmp_path / "workflow.sqlite3"),
        "state_root": str(tmp_path / "service-state"),
        "helpers_dir": str(root / "skills" / "devflow" / "scripts"),
        "codex_bin": "/usr/bin/false",
        "provider": "fake",
        "repositories": {
            "fixture": {
                "source_path": str(source),
                "origin_url": str(origin),
                "github_repo": "example/fixture",
                "base_ref": "HEAD",
                "expected_base_sha": _git(source, "rev-parse", "HEAD"),
                "allowed_paths": ["README.md"],
            }
        },
        "roles": {
            "implement": {"model": "fixture", "effort": "low"},
            "review": {"model": "fixture", "effort": "low"},
            "verify": {"model": "fixture", "effort": "low"},
        },
    }
    path = tmp_path / "service.json"
    path.write_text(json.dumps(configuration))
    request = {
        "command_id": "command-1",
        "run_id": "run-1",
        "work_id": "work-1",
        "issue_url": "https://github.com/example/fixture/issues/3",
        "repository_key": "fixture",
        "goal": "Change a fixture",
        "accepted_plan": "Make one bounded edit and test it.",
        "base_ref": "HEAD",
        "branch": "feat/fixture",
        "authorized_endpoint": "published_unmerged",
    }
    return DeliveryStore(DeliveryConfig.load(path)), request


def test_submit_claim_and_temporal_outbox_are_atomic_and_idempotent(service):
    store, request = service
    first = store.submit(request)
    assert first["phase"] == "accepted"
    assert first["existing"] is False
    assert len(store.pending_starts()) == 1
    with store._connect() as db:
        claim = store.state.claim_for(db, request["work_id"])
        assert claim["owner"] == "external:devflow:run-1"
    assert store.submit(request) == first
    replay = store.submit({**request, "command_id": "command-2"})
    assert replay["existing"] is True
    assert len(store.pending_starts()) == 1

    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "goal": "A different change"})
    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "command_id": "command-3", "goal": "A different change"})


def test_projection_event_is_idempotent_and_detail_is_factual(service):
    store, request = service
    store.submit(request)
    store.mark_start("run-1", accepted=True)
    store.project(
        "run-1",
        phase="implement",
        execution_state="running",
        event_type="role_started",
        message="Implementer started",
        key="implement:0",
    )
    before = store.detail("run-1")
    store.project(
        "run-1",
        phase="implement",
        execution_state="running",
        event_type="role_started",
        message="Implementer started",
        key="implement:0",
    )
    after = store.detail("run-1")
    assert after["revision"] == before["revision"]
    assert len(after["events"]) == len(before["events"])
    assert after["roles"] == []
    assert after["pull_request"] is None
    assert after["usage"] == {}


@pytest.mark.asyncio
@pytest.mark.skipif(not Path("/usr/bin/sandbox-exec").is_file(), reason="macOS Seatbelt required")
async def test_supervisor_launches_one_sandboxed_fake_role_and_replays_receipt(service):
    store, request = service
    store.submit(request)
    spec = store.spec(request["run_id"])
    broker = DeliveryBroker(store, spec)
    candidate = broker.prepare()["candidate"]
    supervisor = DeliverySupervisor(store, capacity=1)
    role_request = {
        "spec": spec,
        "role": "implement",
        "iteration": 0,
        "candidate": candidate,
        "findings": [],
        "resume_session": None,
        "workspace": str(broker.checkout),
    }
    first = await supervisor.run(role_request)
    assert first["status"] == "pass"
    assert first["session_id"] == "fake:run-1:implement"
    assert (broker.checkout / "devflow-fake-change.txt").is_file()
    assert await supervisor.run(role_request) == first
    with store._connect() as db:
        attempts = db.execute("SELECT state,cleanup FROM delivery_attempts").fetchall()
    assert [tuple(row) for row in attempts] == [("finished", "confirmed")]


@pytest.mark.asyncio
@pytest.mark.skipif(
    not Path("/usr/bin/sandbox-exec").is_file() or not shutil.which("temporal"),
    reason="macOS Seatbelt and local Temporal CLI required",
)
async def test_real_temporal_finding_repairs_same_session_with_new_gates(service):
    original_store, request = service
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    config = json.loads(original_store.config.path.read_text())
    config.update(
        {
            "temporal_address": f"127.0.0.1:{port}",
            "queue": "delivery-real-temporal-test",
            "max_repairs": 1,
            "fake_findings": {"review": [0]},
        }
    )
    config["repositories"]["fixture"]["allowed_paths"].append("devflow-fake-change.txt")
    original_store.config.path.write_text(json.dumps(config))
    service_runtime = DeliveryService(original_store.config.path)
    server = await asyncio.create_subprocess_exec(
        "temporal",
        "server",
        "start-dev",
        "--headless",
        "--ip",
        "127.0.0.1",
        "--port",
        str(port),
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        client = None
        for _ in range(100):
            try:
                client = await Client.connect(f"127.0.0.1:{port}")
                break
            except Exception as exc:
                if server.returncode is not None:
                    raise RuntimeError("Temporal dev server exited during test startup") from exc
                await asyncio.sleep(0.1)
        assert client is not None

        @activity.defn(name="delivery_publish")
        async def publish_stub(payload):
            broker = DeliveryBroker(service_runtime.store, payload["spec"])
            _git(broker.checkout, "add", "devflow-fake-change.txt")
            _git(broker.checkout, "commit", "-qm", f"fake candidate {payload['iteration']}")
            candidate = broker.candidate()
            return {
                "number": 1,
                "url": "https://example.invalid/pull/1",
                "state": "OPEN",
                "head": candidate["head"],
                "base": payload["spec"]["base_sha"],
                "candidate": candidate,
            }

        @activity.defn(name="delivery_checks")
        async def checks_stub(payload):
            return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

        @activity.defn(name="delivery_precheck")
        async def precheck_stub(payload):
            return {"state": "passed", "candidate_id": payload["candidate"]["id"]}

        @activity.defn(name="delivery_ci")
        async def ci_stub(payload):
            return {"state": "passed", "head": payload["pull_request"]["head"]}

        @activity.defn(name="delivery_tracker")
        async def tracker_stub(_payload):
            return {"state": "consistent", "observed": {"fixture": True}}

        @activity.defn(name="delivery_tracker_start")
        async def tracker_start_stub(_payload):
            return {"state": "consistent", "observed": {"fixture": True}}

        async with Worker(
            client,
            task_queue=config["queue"],
            workflows=[DeliveryWorkflow],
            activities=[
                delivery_project,
                delivery_prepare,
                delivery_role,
                publish_stub,
                precheck_stub,
                checks_stub,
                ci_stub,
                tracker_start_stub,
                tracker_stub,
            ],
        ):
            service_runtime.store.submit(request)
            await service_runtime.dispatch_once()
            result = await asyncio.wait_for(
                client.get_workflow_handle("delivery-run-1").result(), timeout=30
            )
        assert result["outcome"] == "delivered"
        assert result["iteration"] == 1
        detail = service_runtime.store.detail("run-1")
        assert detail["phase"] == "delivered"
        assert [role["role"] for role in detail["roles"]] == [
            "implement",
            "review",
            "implement",
            "review",
            "verify",
        ]
        assert detail["roles"][0]["session_id"] == detail["roles"][2]["session_id"]
        assert detail["pull_request"]["number"] == 1
        assert detail["pull_request"]["head"] == detail["candidate"]["head"]
        assert detail["checks"]["ci"]["state"] == "passed"
    finally:
        server.terminate()
        await server.wait()
