"""Worker deployment options do not upgrade frozen admission inputs."""

import asyncio
import json
import os
from collections.abc import Sequence
from types import SimpleNamespace

import pytest
from temporalio.common import VersioningBehavior
from temporalio.worker import WorkerDeploymentConfig, WorkerDeploymentVersion

from devflow_temporal import delivery_control as control


@pytest.mark.asyncio
async def test_worker_uses_explicit_pinned_deployment_without_changing_config(
    tmp_path, monkeypatch,
):
    from devflow_temporal.delivery_config import DeliveryConfig

    raw = {"temporal_address": "127.0.0.1:1", "temporal_queue": "fixture",
           "state_root": str(tmp_path), "temporal_namespace": "default"}
    config = DeliveryConfig(tmp_path / "config.json", raw)
    before = json.dumps(raw, sort_keys=True)
    created = []

    class Worker:
        def __init__(self, *args, **kwargs):
            created.append(kwargs)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            pass

    async def describe(_request, **_kwargs):
        return SimpleNamespace(pollers=[SimpleNamespace(identity=created[0]["identity"])])

    async def connect(*_args, **_kwargs):
        return SimpleNamespace(namespace="default", workflow_service=SimpleNamespace(
            describe_task_queue=describe))

    monkeypatch.setattr(control, "Worker", Worker)
    monkeypatch.setattr(control.Client, "connect", connect)
    deployment = WorkerDeploymentConfig(
        WorkerDeploymentVersion("delivery", "candidate"), True, VersioningBehavior.PINNED)
    task = asyncio.create_task(control.worker(config, deployment=deployment))
    marker = tmp_path / "worker-ready.json"
    try:
        async with asyncio.timeout(2):
            while not marker.exists():
                if task.done():
                    await task
                await asyncio.sleep(0.001)
        assert created[0]["deployment_config"] == deployment
        ready = json.loads(marker.read_text())
        assert ready["pid"] == os.getpid()
        assert ready["deployment"] == {"name": "delivery", "build_id": "candidate"}
        assert json.dumps(config.raw, sort_keys=True) == before
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not marker.exists()


@pytest.mark.asyncio
async def test_real_deployment_worker_registers_both_pollers_before_ready(tmp_path):
    from temporalio.testing import WorkflowEnvironment

    from devflow_temporal.delivery_config import DeliveryConfig

    async with await WorkflowEnvironment.start_local() as environment:
        raw = {"temporal_address": environment.client.service_client.config.target_host,
               "queue": "versioned-readiness", "state_root": str(tmp_path)}
        config = DeliveryConfig(tmp_path / "config.json", raw)
        deployment = control._worker_deployment("delivery-readiness", "fixture-v1")
        task = asyncio.create_task(control.worker(config, deployment=deployment))
        marker = tmp_path / "worker-ready.json"
        try:
            async with asyncio.timeout(10):
                while not marker.exists():
                    if task.done():
                        await task
                    await asyncio.sleep(0.01)
            assert json.loads(marker.read_text())["deployment"] == {
                "name": "delivery-readiness", "build_id": "fixture-v1"}
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        assert not marker.exists()


@pytest.mark.asyncio
async def test_new_delivery_is_pinned_and_records_order_marker_before_checks():
    from datetime import timedelta

    from temporalio import activity
    from temporalio.api.enums.v1 import TaskQueueType
    from temporalio.api.taskqueue.v1 import TaskQueue
    from temporalio.api.workflowservice.v1 import (
        DescribeTaskQueueRequest,
        SetWorkerDeploymentCurrentVersionRequest,
    )
    from temporalio.common import RawValue
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Replayer, Worker

    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    calls = []
    candidate = {"id": "candidate-1", "head": "head-1"}

    @activity.defn(dynamic=True)
    async def fake_activity(args: Sequence[RawValue]):
        assert isinstance(args[0], RawValue)
        payload = activity.payload_converter().from_payload(args[0].payload)
        name = activity.info().activity_type
        if name == "delivery_role":
            calls.append(payload["role"])
            return {"status": "pass", "session_id": "fake:" + payload["role"],
                    "candidate": candidate}
        if name == "delivery_checks":
            calls.append("checks")
        if name == "delivery_browser_qa":
            calls.append("browser_qa")
            return {"state": "passed", "cleanup": "confirmed", "receipt": "/owned/receipt",
                    "receipt_sha256": "a" * 64, "log": "/owned/log", "log_sha256": "b" * 64}
        return {"state": "confirmed" if name == "delivery_gates_readback" else
                "consistent" if name in {"delivery_tracker", "delivery_tracker_start"} else
                "passed", "candidate": candidate, "revision": 1}

    spec = {"run_id": "pinned-order", "provider": "fake",
            "policy": {"max_repairs": 0, "browser_qa": {"id": "owned-browser"}}}
    publication = {"candidate": candidate, "head": candidate["head"]}
    state = {"run_id": spec["run_id"], "revision": 1, "iteration": 0,
             "candidate_revision": 1, "candidate": candidate, "pull_request": publication,
             "roles": [{"role": "implement", "iteration": 0, "session_id": "fake:implement"}],
             "checks": {}, "findings": [], "usage": {}, "decision": None}
    recovery = {"kind": "investigation_gates_only", "execution_spec": spec,
                "command": {"additional_iterations": 0}, "state": state,
                "candidate": candidate, "publication": publication,
                "seal": {"session_id": "fake:implement"}, "semantic": {}}
    deployment = control._worker_deployment("delivery-order", "marked-v1")
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(environment.client, task_queue="pinned-order", identity="order-worker",
                          workflows=[DeliveryWorkflow], activities=[fake_activity],
                          deployment_config=deployment):
            async with asyncio.timeout(10):
                for queue_type in (TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW,
                                   TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY):
                    while True:
                        queue = await environment.client.workflow_service.describe_task_queue(
                            DescribeTaskQueueRequest(namespace="default",
                                task_queue=TaskQueue(name="pinned-order"),
                                task_queue_type=queue_type))
                        if any(p.identity == "order-worker" for p in queue.pollers):
                            break
                        await asyncio.sleep(0.01)
            await environment.client.workflow_service.set_worker_deployment_current_version(
                SetWorkerDeploymentCurrentVersionRequest(namespace="default",
                    deployment_name="delivery-order", build_id="marked-v1"))
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id="pinned-order",
                task_queue="pinned-order", execution_timeout=timedelta(seconds=15))
            result = await handle.result()
            history = await handle.fetch_history()
            description = await handle.describe()
    assert result["outcome"] == "delivered"
    assert calls[:4] == ["checks", "review", "browser_qa", "verify"]
    version = description.raw_description.workflow_execution_info.versioning_info
    assert version.behavior == VersioningBehavior.PINNED
    assert version.deployment_version.build_id == "marked-v1"
    patch_event = next(e.event_id for e in history.events if e.HasField(
        "marker_recorded_event_attributes") and b"local-checks-before-review-v1" in
        b"".join(p.data for values in e.marker_recorded_event_attributes.details.values()
                 for p in values.payloads))
    check_event = next(e.event_id for e in history.events if e.HasField(
        "activity_task_scheduled_event_attributes") and
        e.activity_task_scheduled_event_attributes.activity_type.name == "delivery_checks")
    assert patch_event < check_event
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


@pytest.mark.parametrize("name,build", [("delivery", None), (None, "version"),
                                       ("bad.name", "version"), ("delivery", "bad version")])
def test_invalid_or_partial_deployment_options_are_rejected(name, build):
    with pytest.raises(ValueError, match="together as bounded identifiers"):
        control._worker_deployment(name, build)


def test_default_worker_options_do_not_enable_versioning():
    assert control._worker_deployment(None, None) is None


def test_starting_different_version_preserves_ready_worker(tmp_path, monkeypatch):
    from devflow_temporal.delivery_config import DeliveryConfig

    config = DeliveryConfig(tmp_path / "config.json", {"state_root": str(tmp_path),
        "service_start_timeout": 1})
    deployment = control._worker_deployment("delivery", "new")
    existing = {"config_path": str(config.path), "processes": {"worker": {
        "pid": 1, "identity": "fixture", "deployment": {"name": "delivery", "build_id": "old"}}}}
    monkeypatch.setattr(control, "_ports", lambda _config: None)
    monkeypatch.setattr(control, "_read_manifest", lambda _config: existing)
    monkeypatch.setattr(control, "_owned", lambda _process: True)
    monkeypatch.setattr(control, "_ready", lambda *_args: True)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("restarted ready worker"))
    monkeypatch.setattr(control, "_start", lambda *_args, **_kwargs: pytest.fail("new worker launched"))
    with pytest.raises(ValueError, match="running worker deployment differs"):
        control.ensure_service_running(config, deployment=deployment)
    assert existing["processes"]["worker"]["deployment"]["build_id"] == "old"
