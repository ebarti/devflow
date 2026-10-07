"""Recorded delivery histories must replay on their designated source artifact."""

import hashlib
import json
import os
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from temporalio.workflow import NondeterminismError

from devflow_temporal.delivery_workflow import DeliveryWorkflow

FIXTURES = Path(__file__).parent / "fixtures"
RETAINED = {"delivery-after-check-order-history.json",
            "delivery-published-checkpoint-history.json", "c04-gates-only-history.json"}
HISTORIES = sorted(FIXTURES.glob("*.json")) + sorted((FIXTURES / "order").glob("*history.json"))


@pytest.mark.asyncio
@pytest.mark.parametrize("path", HISTORIES, ids=lambda path: path.stem)
async def test_recorded_delivery_history_replays_on_designated_artifact(path, tmp_path):
    history = WorkflowHistory.from_json("delivery-replay", path.read_text())
    if path.name not in RETAINED:
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
        return
    # These unmarked checks-first histories cannot replay on the marker-only worker.
    # Do not change their events to manufacture a universal compatibility claim.
    with pytest.raises(NondeterminismError, match="TMPRL1100"):
        await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    archive = FIXTURES / "order/c04-source.tar.gz"
    provenance = json.loads((FIXTURES / "order/provenance.json").read_text())
    assert hashlib.sha256(archive.read_bytes()).hexdigest() == provenance["source_archive_sha256"]
    with tarfile.open(archive) as artifact:
        artifact.extractall(tmp_path, filter="data")
    result = subprocess.run(
        [sys.executable, "-B", "-c", '''
import asyncio, sys
from pathlib import Path
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer
from devflow_temporal.delivery_workflow import DeliveryWorkflow
asyncio.run(Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
    WorkflowHistory.from_json("retained-replay", Path(sys.argv[1]).read_text())))
''', str(path.resolve())],
        env={**os.environ, "PYTHONPATH": str(tmp_path / "runtime/src")},
        capture_output=True, text=True, check=False, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_original_counterexample_history_bytes_remain_unchanged():
    provenance = json.loads((FIXTURES / "order/provenance.json").read_text())
    for name, expected in provenance["history_sha256"].items():
        assert hashlib.sha256((FIXTURES / "order" / name).read_bytes()).hexdigest() == expected
    old, newer = [json.loads((FIXTURES / "order" / name).read_text()) for name in (
        "pre66-gates-only-history.json", "c04-gates-only-history.json")]
    assert old["events"][0]["workflowExecutionStartedEventAttributes"]["input"] == (
        newer["events"][0]["workflowExecutionStartedEventAttributes"]["input"])


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
