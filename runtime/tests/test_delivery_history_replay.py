"""Recorded delivery histories must replay on their designated source artifact."""

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from historical_replay import replay_designated_history
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from devflow_temporal.delivery_workflow import DeliveryWorkflow

FIXTURES = Path(__file__).parent / "fixtures"
HISTORIES = sorted(FIXTURES.glob("*.json")) + sorted((FIXTURES / "order").glob("*history.json"))


@pytest.mark.asyncio
@pytest.mark.parametrize("path", HISTORIES, ids=lambda path: path.stem)
async def test_recorded_delivery_history_replays_on_designated_artifact(path, tmp_path):
    await replay_designated_history(path, tmp_path)


def test_original_counterexample_history_bytes_remain_unchanged():
    provenance = json.loads((FIXTURES / "order/provenance.json").read_text())
    for name, expected in provenance["history_sha256"].items():
        assert hashlib.sha256((FIXTURES / "order" / name).read_bytes()).hexdigest() == expected
    old, newer = [json.loads((FIXTURES / "order" / name).read_text()) for name in (
        "pre66-gates-only-history.json", "c04-gates-only-history.json")]
    assert old["events"][0]["workflowExecutionStartedEventAttributes"]["input"] == (
        newer["events"][0]["workflowExecutionStartedEventAttributes"]["input"])


def _load_routing_probe():
    spec = importlib.util.spec_from_file_location(
        "retained_routing_probe", FIXTURES / "order/retained-routing-probe.py")
    probe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(probe)
    return probe


@pytest.mark.asyncio
@pytest.mark.parametrize("delayed", ["registration", "membership", "not-found", "poller-version",
                                    "response-version", "activity-registration"])
async def test_retained_routing_waits_beyond_poller_presence(monkeypatch, tmp_path, delayed):
    """Logical RPC fixture: no server, worker, process or recorded history is created."""
    from contextlib import asynccontextmanager

    from temporalio.api.deployment.v1 import WorkerDeploymentOptions, WorkerDeploymentVersionInfo
    from temporalio.api.taskqueue.v1 import PollerInfo
    from temporalio.api.workflowservice.v1 import (
        DescribeTaskQueueResponse,
        DescribeWorkerDeploymentVersionResponse,
    )
    from temporalio.service import RPCError, RPCStatusCode

    probe = _load_routing_probe()
    version = probe.WorkerDeploymentVersion(deployment_name="delivery-retained",
                                             build_id="original")
    description_reads = 0
    queue_reads = 0
    update_calls = 0

    async def describe_queue(request):
        nonlocal queue_reads
        queue_reads += 1
        return DescribeTaskQueueResponse(pollers=[PollerInfo(
            identity="retained-versioned", deployment_options=WorkerDeploymentOptions(
                deployment_name="delivery-retained", build_id=(
                    "wrong" if delayed == "poller-version" and queue_reads <= 2 else "original")))])

    async def describe_version(request):
        nonlocal description_reads
        assert request.deployment_version == version
        description_reads += 1
        if delayed == "not-found" and description_reads == 1:
            raise RPCError("Version not registered yet", RPCStatusCode.NOT_FOUND, b"")
        queues = [] if delayed == "registration" and description_reads == 1 else [
            DescribeWorkerDeploymentVersionResponse.VersionTaskQueue(
                name="retained-routing", type=kind) for kind in (
                    probe.TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                    probe.TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)
            if not (delayed == "activity-registration" and description_reads == 1 and
                    kind == probe.TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)]
        return DescribeWorkerDeploymentVersionResponse(
            worker_deployment_version_info=WorkerDeploymentVersionInfo(
                deployment_version=(probe.WorkerDeploymentVersion(
                    deployment_name="delivery-retained", build_id="wrong")
                    if delayed == "response-version" and description_reads == 1 else version)),
            version_task_queues=queues)

    async def update_options(request):
        nonlocal update_calls
        update_calls += 1
        # The old fixture calls this before reading registration at all.
        if description_reads == 0 or delayed == "membership" and update_calls == 1:
            raise RPCError("Pinned version 'delivery-retained:original' is not present in "
                           "task queue 'retained-routing' of type 'Workflow'",
                           RPCStatusCode.FAILED_PRECONDITION, b"")
        assert request.workflow_execution_options.versioning_override.pinned.version == version

    class Handle:
        id = "retained-routing"
        first_execution_run_id = "owned-logical-run"

        async def query(self, _):
            return {"revision": 2, "checks": {"terminal_tracker_checkpoint": {"waiting": True}}}

        async def fetch_history(self):
            return SimpleNamespace(to_json=lambda: "{}")

        async def execute_update(self, *args):
            assert description_reads >= 2

        async def result(self):
            return {"outcome": "delivered"}

        async def describe(self):
            return SimpleNamespace(raw_description=SimpleNamespace(workflow_execution_info=
                SimpleNamespace(versioning_info=SimpleNamespace(versioning_override=
                    probe.VersioningOverride(pinned=probe.VersioningOverride.PinnedOverride(
                        version=version))))))

    async def start_workflow(*args, **kwargs):
        return Handle()

    client = SimpleNamespace(start_workflow=start_workflow, workflow_service=SimpleNamespace(
        describe_task_queue=describe_queue, describe_worker_deployment_version=describe_version,
        update_workflow_execution_options=update_options))

    @asynccontextmanager
    async def environment():
        yield SimpleNamespace(client=client)

    @asynccontextmanager
    async def worker(*args, **kwargs):
        yield

    class LogicalReplayer:
        def __init__(self, **kwargs):
            pass

        async def replay_workflow(self, _):
            pass

    monkeypatch.setattr(probe, "local_temporal", environment)
    monkeypatch.setattr(probe, "Worker", worker)
    monkeypatch.setattr(probe, "Replayer", LogicalReplayer)
    await probe.main(tmp_path / "logical")
    assert update_calls == (2 if delayed == "membership" else 1)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing-membership", "other-precondition", "network",
                                    "read-network", "wrong-poller", "wrong-registration"])
async def test_retained_routing_readiness_is_bounded_and_rejects_unrelated_errors(failure):
    from temporalio.api.deployment.v1 import WorkerDeploymentOptions, WorkerDeploymentVersionInfo
    from temporalio.api.taskqueue.v1 import PollerInfo
    from temporalio.api.workflowservice.v1 import (
        DescribeTaskQueueResponse,
        DescribeWorkerDeploymentVersionResponse,
    )
    from temporalio.service import RPCError, RPCStatusCode

    probe = _load_routing_probe()
    version = probe.WorkerDeploymentVersion(deployment_name="delivery-retained",
                                           build_id="original")
    updates = []
    error = RPCError("Pinned version 'delivery-retained:original' is not present in "
                     "task queue 'retained-routing' of type 'Workflow'" if failure ==
                     "missing-membership" else "Unrelated rejected or unknown outcome",
                     RPCStatusCode.UNAVAILABLE if "network" in failure else
                     RPCStatusCode.FAILED_PRECONDITION, b"")

    async def describe_queue(request):
        return DescribeTaskQueueResponse(pollers=[PollerInfo(
            identity="retained-versioned", deployment_options=WorkerDeploymentOptions(
                deployment_name="delivery-retained",
                build_id="wrong" if failure == "wrong-poller" else "original"))])

    async def describe_version(request):
        if failure == "read-network":
            raise error
        return DescribeWorkerDeploymentVersionResponse(
            worker_deployment_version_info=WorkerDeploymentVersionInfo(deployment_version=version),
            version_task_queues=[DescribeWorkerDeploymentVersionResponse.VersionTaskQueue(
                name="wrong" if failure == "wrong-registration" else "retained-routing", type=kind)
                for kind in (probe.TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                             probe.TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY)])

    async def update_options(request):
        updates.append(request)
        raise error

    client = SimpleNamespace(workflow_service=SimpleNamespace(describe_task_queue=describe_queue,
        describe_worker_deployment_version=describe_version,
        update_workflow_execution_options=update_options))
    expected = (TimeoutError if failure in {"missing-membership", "wrong-poller",
                                          "wrong-registration"} else RPCError)
    with pytest.raises(expected) as raised:
        await probe.pin_registered_retained_version(client,
            SimpleNamespace(id="retained-routing", first_execution_run_id="owned-logical-run"),
            timeout=0.02)
    if expected is RPCError:
        assert raised.value is error
    assert len(updates) == (0 if failure in {"wrong-poller", "wrong-registration",
                                           "read-network"} else 1)


@pytest.mark.asyncio
async def test_actual_unversioned_execution_moves_to_retained_artifact_without_history_edit(
    tmp_path,
):
    archive = FIXTURES / "order/c04-source.tar.gz"
    with tarfile.open(archive) as artifact:
        artifact.extractall(tmp_path, filter="data")
    source_before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in (tmp_path / "runtime/src").rglob("*.py")}
    result = subprocess.run(
        [sys.executable, "-B", str(FIXTURES / "order/retained-routing-probe.py"),
         str(tmp_path / "observed")],
        env={**os.environ, "PYTHONPATH": os.pathsep.join((
            str(tmp_path / "runtime/src"), str(Path(__file__).parent.resolve())))},
        capture_output=True, text=True, check=False, timeout=40,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in (tmp_path / "runtime/src").rglob("*.py")} == source_before
    for name in ("before.json", "final.json"):
        history = WorkflowHistory.from_json("observed-retained", (
            tmp_path / "observed" / name).read_text())
        with pytest.raises(NondeterminismError, match="TMPRL1100"):
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


@pytest.mark.asyncio
async def test_external_adapter_registers_original_artifact_without_changing_source(tmp_path):
    import asyncio
    import signal

    from temporal_test_server import local_temporal
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import DescribeTaskQueueRequest

    with tarfile.open(FIXTURES / "order/c04-source.tar.gz") as artifact:
        artifact.extractall(tmp_path, filter="data")
    source = tmp_path / "runtime/src"
    before = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in source.rglob("*.py")}
    state = tmp_path / "state"
    state.mkdir(mode=0o700)
    async with local_temporal() as environment:
        config = tmp_path / "config.json"
        config.write_text(json.dumps({"version": 1, "provider": "fake",
            "state_root": str(state), "tracking_db": str(tmp_path / "tracking.sqlite3"),
            "helpers_dir": str(Path(__file__).resolve().parents[2] / "skills/devflow/scripts"),
            "codex_bin": "/usr/bin/true", "repositories": {"fixture": {"base_ref": "main"}},
            "roles": {name: {} for name in ("implement", "review", "verify")},
            "temporal_address": environment.client.service_client.config.target_host,
            "queue": "adapter-readiness"}))
        frozen_bytes = config.read_bytes()
        process = subprocess.Popen([sys.executable, "-B", str(
            Path(__file__).resolve().parents[2] / "scripts/run-versioned-worker.py"),
            "--runtime-src", str(source), "--config", str(config),
            "--deployment-name", "adapter-delivery", "--deployment-build-id", "original"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        marker = state / "worker-ready.json"
        try:
            async with asyncio.timeout(10):
                while not marker.exists():
                    assert process.poll() is None, process.communicate()
                    await asyncio.sleep(0.01)
            ready = json.loads(marker.read_text())
            assert ready["pid"] == process.pid
            for queue_type in (TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                               TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY):
                queue = await environment.client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(namespace="default",
                        task_queue=TaskQueue(name="adapter-readiness"), task_queue_type=queue_type))
                matching = [p for p in queue.pollers if p.identity.startswith(
                    f"devflow-{process.pid}-")]
                assert matching
                assert matching[0].deployment_options.build_id == "original"
                assert matching[0].deployment_options.deployment_name == "adapter-delivery"
            assert config.read_bytes() == frozen_bytes
        finally:
            process.send_signal(signal.SIGINT)
            await asyncio.to_thread(process.communicate, timeout=10)
        assert not marker.exists()
    assert {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in source.rglob("*.py")} == before
