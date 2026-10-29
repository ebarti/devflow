"""A durable merge workflow with narrowly scoped conflict resolution."""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError


@workflow.defn(name="DevflowMergeWorkflow")
class MergeWorkflow:
    @workflow.run
    async def run(self, spec: dict, grant_sha256: str) -> dict:
        request = {"spec": spec, "merge_authorization": grant_sha256}
        if workflow.patched("ordinary-merge-execution-v1"):
            request["merge_execution"] = {
                "workflow_id": workflow.info().workflow_id,
                "execution_run_id": workflow.info().run_id,
            }
        while True:
            try:
                result = await workflow.execute_activity(
                    "delivery_merge_followup",
                    request,
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(seconds=30),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
            except ActivityError as exc:
                return await workflow.execute_activity(
                    "delivery_merge_followup_failed",
                    {**request, "reason": str(exc)},
                    start_to_close_timeout=timedelta(minutes=1),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            if result["state"] == "resolve_conflicts":
                try:
                    result = await workflow.execute_activity(
                        "delivery_resolve_merge_conflicts",
                        {**request, "conflict": result},
                        start_to_close_timeout=timedelta(seconds=sum(
                            spec["policy"]["roles"][role].get("timeout_seconds", 7200)
                            for role in ("implement", "review")
                        ) + 300),
                        heartbeat_timeout=timedelta(seconds=30),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
                except ActivityError as exc:
                    return await workflow.execute_activity(
                        "delivery_merge_followup_failed", {**request, "reason": str(exc)},
                        start_to_close_timeout=timedelta(minutes=1),
                        retry_policy=RetryPolicy(maximum_attempts=3),
                    )
            if result["state"] != "pending":
                return result
            await workflow.sleep(timedelta(seconds=15))
