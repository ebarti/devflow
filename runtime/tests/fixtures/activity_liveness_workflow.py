from datetime import timedelta

from temporalio import workflow
from temporalio.worker import Interceptor, WorkflowInboundInterceptor, WorkflowOutboundInterceptor

from devflow_temporal.delivery_workflow import DeliveryWorkflow


class FeatureWaitClockOutbound(WorkflowOutboundInterceptor):
    """Scale only the lost-wait fixture clock after the real policy is produced."""

    def start_activity(self, input):
        if input.activity == 'delivery_feature_wait_worker':
            assert input.heartbeat_timeout == timedelta(seconds=60)
            assert input.start_to_close_timeout >= timedelta(seconds=360)
            assert input.schedule_to_close_timeout is None
            input.heartbeat_timeout = timedelta(seconds=3)
            input.start_to_close_timeout = timedelta(seconds=12)
        return super().start_activity(input)


class FeatureWaitClockInbound(WorkflowInboundInterceptor):
    def init(self, outbound):
        super().init(FeatureWaitClockOutbound(outbound))


class FeatureWaitClock(Interceptor):
    def workflow_interceptor_class(self, input):
        return FeatureWaitClockInbound


class FinalizationClockOutbound(WorkflowOutboundInterceptor):
    """Keep real finalization retries, scaling only the worker-loss fixture heartbeat."""

    def start_activity(self, input):
        if input.activity == 'delivery_finalize_resources':
            assert input.heartbeat_timeout == timedelta(seconds=30)
            input.heartbeat_timeout = timedelta(seconds=3)
        return super().start_activity(input)


class FinalizationClockInbound(WorkflowInboundInterceptor):
    def init(self, outbound):
        super().init(FinalizationClockOutbound(outbound))


class FinalizationClock(Interceptor):
    def workflow_interceptor_class(self, input):
        return FinalizationClockInbound


@workflow.defn
class ActivityLivenessWorkflow(DeliveryWorkflow):
    @workflow.run
    async def run(self, payload: dict) -> dict:
        return await self._activity(
            payload['activity_name'], payload['request'], hours=payload.get('hours', 2),
        )
