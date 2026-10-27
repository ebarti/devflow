from __future__ import annotations

import sqlite3
from datetime import timedelta

import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
)
from temporalio.testing import ActivityEnvironment
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    UnsandboxedWorkflowRunner,
    Worker,
)
from test_delivery_store import service as service

from devflow_temporal.delivery_activities import DatabaseContentionInterceptor
from devflow_temporal.delivery_workflow import _failure_classification


@pytest.mark.asyncio
@pytest.mark.parametrize('message', ['database is locked', 'database table is locked: runs'])
async def test_worker_interceptor_types_sqlite_contention_as_transient(message):
    class Failing(ActivityInboundInterceptor):
        def __init__(self, error):
            self.error = error

        async def execute_activity(self, input):
            raise self.error

    inbound = DatabaseContentionInterceptor().intercept_activity(
        Failing(sqlite3.OperationalError(message)))
    request = ExecuteActivityInput(fn=_locked_store_probe, args=[], executor=None, headers={})
    with pytest.raises(ApplicationError) as failure:
        await ActivityEnvironment().run(inbound.execute_activity, request)
    assert failure.value.type == 'DatabaseBusy' and not failure.value.non_retryable
    assert 'database busy' in str(failure.value)
    other = DatabaseContentionInterceptor().intercept_activity(
        Failing(sqlite3.OperationalError('no such table: delivery_runs')))
    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        await ActivityEnvironment().run(other.execute_activity, request)


@activity.defn(name='locked_store_probe')
async def _locked_store_probe() -> None:
    raise sqlite3.OperationalError('database is locked')


@workflow.defn
class _ContentionProbe:
    @workflow.run
    async def run(self) -> dict:
        try:
            await workflow.execute_activity(
                _locked_store_probe, start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
        except ActivityError as exc:
            return _failure_classification(exc, None)
        return {'classification': 'none'}


@pytest.mark.asyncio
async def test_single_attempt_activity_hitting_a_locked_database_is_transient():
    async with local_temporal() as environment:
        async with Worker(
            environment.client, task_queue='database-contention',
            workflows=[_ContentionProbe], activities=[_locked_store_probe],
            interceptors=[DatabaseContentionInterceptor()],
            workflow_runner=UnsandboxedWorkflowRunner(),
        ):
            result = await environment.client.execute_workflow(
                _ContentionProbe.run, id='database-contention-probe',
                task_queue='database-contention',
            )
    assert result == {'classification': 'transient', 'cause_type': 'DatabaseBusy'}
