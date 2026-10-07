"""Run only on an extracted original artifact against an owned disposable server."""

import asyncio
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from google.protobuf.field_mask_pb2 import FieldMask
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.api.common.v1 import WorkflowExecution
from temporalio.api.deployment.v1 import WorkerDeploymentVersion
from temporalio.api.enums.v1 import TaskQueueType
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflow.v1 import VersioningOverride, WorkflowExecutionOptions
from temporalio.api.workflowservice.v1 import (
    DescribeTaskQueueRequest,
    DescribeWorkerDeploymentVersionRequest,
    UpdateWorkflowExecutionOptionsRequest,
)
from temporalio.common import RawValue, VersioningBehavior
from temporalio.service import RPCError, RPCStatusCode
from temporalio.worker import Replayer, Worker, WorkerDeploymentConfig
from temporalio.worker import WorkerDeploymentVersion as DeploymentVersion

from devflow_temporal.delivery_workflow import DeliveryWorkflow

candidate = {"id": "candidate-1", "head": "head-1"}
consistent = False


async def pin_registered_retained_version(client, handle, *, timeout=10):
    """Poller presence precedes registration and matching membership propagation."""
    version = WorkerDeploymentVersion(deployment_name="delivery-retained", build_id="original")
    kinds = {TaskQueueType.TASK_QUEUE_TYPE_WORKFLOW, TaskQueueType.TASK_QUEUE_TYPE_ACTIVITY}
    override = VersioningOverride(pinned=VersioningOverride.PinnedOverride(
        behavior=VersioningOverride.PINNED_OVERRIDE_BEHAVIOR_PINNED, version=version))
    request = UpdateWorkflowExecutionOptionsRequest(namespace="default",
        workflow_execution=WorkflowExecution(
            workflow_id=handle.id, run_id=handle.first_execution_run_id),
        workflow_execution_options=WorkflowExecutionOptions(versioning_override=override),
        update_mask=FieldMask(paths=["versioning_override"]))
    membership_missing = ("Pinned version 'delivery-retained:original' is not present in "
                          "task queue 'retained-routing' of type 'Workflow'")
    async with asyncio.timeout(timeout):
        while True:
            ready = True
            for kind in kinds:
                queue = await client.workflow_service.describe_task_queue(
                    DescribeTaskQueueRequest(namespace="default",
                        task_queue=TaskQueue(name="retained-routing"), task_queue_type=kind))
                ready &= any(p.identity == "retained-versioned" and
                             p.deployment_options.deployment_name == version.deployment_name and
                             p.deployment_options.build_id == version.build_id
                             for p in queue.pollers)
            try:
                description = await client.workflow_service.describe_worker_deployment_version(
                    DescribeWorkerDeploymentVersionRequest(namespace="default",
                        deployment_version=version))
            except RPCError as error:
                if error.status != RPCStatusCode.NOT_FOUND:
                    raise
                ready = False
            else:
                ready &= description.worker_deployment_version_info.deployment_version == version
                ready &= kinds <= {q.type for q in description.version_task_queues
                                   if q.name == "retained-routing"}
            if ready:
                try:
                    await client.workflow_service.update_workflow_execution_options(request)
                except RPCError as error:
                    # This exact precondition is checked before applying the override.
                    # Network/timeout/other errors have unknown outcomes and are not retried.
                    if (error.status != RPCStatusCode.FAILED_PRECONDITION or
                            error.message != membership_missing):
                        raise
                else:
                    return
            await asyncio.sleep(0.1)


@activity.defn(dynamic=True)
async def fake_activity(args: Sequence[RawValue]):
    payload = activity.payload_converter().from_payload(args[0].payload)
    name = activity.info().activity_type
    if name == "delivery_role":
        return {"status": "pass", "session_id": "fake:" + payload["role"],
                "candidate": candidate}
    if name == "delivery_terminal_tracker":
        return {"state": "consistent" if consistent else "pending"}
    if name == "delivery_browser_qa":
        return {"state": "passed", "cleanup": "confirmed", "receipt": "/owned/receipt",
                "receipt_sha256": "a" * 64, "log": "/owned/log", "log_sha256": "b" * 64}
    return {"state": "confirmed" if name == "delivery_gates_readback" else
            "consistent" if name in {"delivery_tracker", "delivery_tracker_start"} else
            "passed", "candidate": candidate, "revision": 1}


async def main(output):
    global consistent
    spec = {"run_id": "retained-routing", "provider": "fake", "terminal_tracker_version": 1,
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
    async with local_temporal() as environment:
        # Original, unversioned artifact really records its checks-first history.
        async with Worker(environment.client, task_queue="retained-routing",
                          workflows=[DeliveryWorkflow], activities=[fake_activity]):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id="retained-routing",
                task_queue="retained-routing")
            async with asyncio.timeout(15):
                while True:
                    checkpoint = await handle.query("status")
                    if checkpoint.get("checks", {}).get("terminal_tracker_checkpoint", {}).get(
                            "waiting"):
                        break
                    await asyncio.sleep(0.01)
            before = await handle.fetch_history()
        deployment = WorkerDeploymentConfig(DeploymentVersion("delivery-retained", "original"),
                                            True, VersioningBehavior.PINNED)
        # Sequential bootstrap: unchanged old artifact; no current worker polls this queue.
        async with Worker(environment.client, task_queue="retained-routing",
                          workflows=[DeliveryWorkflow], activities=[fake_activity],
                          deployment_config=deployment, identity="retained-versioned"):
            await pin_registered_retained_version(environment.client, handle)
            consistent = True
            await handle.execute_update("reconcile_tracker", {
                "expected_revision": checkpoint["revision"], "reason": "Fixture tracker ready"})
            async with asyncio.timeout(10):
                result = await handle.result()
            final = await handle.fetch_history()
            description = await handle.describe()
    assert result["outcome"] == "delivered"
    version = description.raw_description.workflow_execution_info.versioning_info
    assert version.versioning_override.pinned.version.build_id == "original"
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(final)
    output.mkdir()
    (output / "before.json").write_text(before.to_json())
    (output / "final.json").write_text(final.to_json())
    (output / "summary.json").write_text(json.dumps({"outcome": result["outcome"],
        "pinned_override": "delivery-retained.original", "actual_old_source": True,
        "native_activity_validation": "not exercised: fixture activities"}, indent=2))


if __name__ == "__main__":
    asyncio.run(main(Path(sys.argv[1])))
