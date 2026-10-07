from temporalio import workflow

from devflow_temporal.delivery_workflow import DeliveryWorkflow


@workflow.defn
class ActivityLivenessWorkflow(DeliveryWorkflow):
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await self._activity(
            payload['activity_name'], payload['request'], hours=payload.get('hours', 2),
        )
