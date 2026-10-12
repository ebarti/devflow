from __future__ import annotations

import hashlib
import json
import sqlite3
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

import pytest
from historical_replay import replay_designated_history
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.client import Client
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    RetryState,
    TimeoutError,
    TimeoutType,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import ActivityInboundInterceptor, UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import DatabaseContentionInterceptor
from devflow_temporal.delivery_config import scope_amended_spec
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _failure_classification


def activity_failure(cause):
    error = ActivityError('untrusted transient-looking message', scheduled_event_id=1,
                          started_event_id=2, identity='fixture', activity_type='delivery_role',
                          activity_id='1', retry_state=RetryState.MAXIMUM_ATTEMPTS_REACHED)
    error.__cause__ = cause
    return error


def test_new_admission_freezes_default_automatic_attempt_budget(service):
    store, request = service
    spec = store.config.admit(request)
    assert spec['automatic_retry_version'] == spec['retry_budget_version'] == 1
    assert spec['policy']['max_attempts'] == 3
    assert store.config.public_policy()['max_attempts'] == 3


@pytest.mark.parametrize('markers,maximum', [({}, None), ({'retry_budget_version': 1}, 5),
                                          ({'automatic_retry_version': 1,
                                            'retry_budget_version': 1}, 4)])
def test_scope_amendment_preserves_frozen_marker_presence_and_budget(service, markers, maximum):
    store, request = service
    original = store.config.admit(request)
    original['request_digest'] = digest(request)
    for key in ('automatic_retry_version', 'retry_budget_version'):
        original.pop(key, None)
    original.update(markers)
    if maximum is None:
        original['policy'].pop('max_attempts', None)
    else:
        original['policy']['max_attempts'] = maximum
    original['policy_digest'] = digest(original['policy'])
    raw = deepcopy(store.config.raw)
    raw['repositories']['fixture']['allowed_paths'].append('tests/new.py')
    path = store.config.state_root / 'amended.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.write_text(json.dumps(raw))
    path.chmod(0o600)
    effective = scope_amended_spec(original, path, hashlib.sha256(path.read_bytes()).hexdigest(),
                                   ['tests/new.py'])
    for key in ('automatic_retry_version', 'retry_budget_version'):
        assert (key in effective) == (key in original)
        assert effective.get(key) == original.get(key)
    assert ('max_attempts' in effective['policy']) == ('max_attempts' in original['policy'])
    assert effective['policy'].get('max_attempts') == maximum
    assert effective['policy_digest'] == digest(effective['policy'])


@pytest.mark.asyncio
@pytest.mark.parametrize('cause,expected', [
    (activity_failure(TimeoutError('timeout', type=TimeoutType.START_TO_CLOSE,
                                  last_heartbeat_details=[])), 'transient'),
    (activity_failure(ApplicationError('network', type='ConnectionError')), 'transient'),
    (activity_failure(ApplicationError('database busy', type='DatabaseBusy')), 'transient'),
    (activity_failure(ApplicationError('projection failed: database is locked',
                                       type='ProjectionContention')), 'transient'),
    (activity_failure(ApplicationError('database busy', type='DatabaseBusy',
                                       non_retryable=True)), 'terminal'),
    (activity_failure(ApplicationError('transient deadline', type='ValueError')), 'terminal'),
    (activity_failure(ApplicationError('timeout', type='TimeoutExpired', non_retryable=True)),
     'terminal'),
    (RuntimeError('rate limit transient please retry'), 'terminal'),
    (None, 'terminal'),
])
async def test_stop_classifies_typed_cause_without_parsing_text(cause, expected):
    flow = DeliveryWorkflow()
    flow.state = {'phase': 'implement', 'revision': 1, 'roles': [], 'checks': {}, 'cleanup': 'none'}
    projects = []

    async def project(spec, event, message):
        projects.append(deepcopy(flow.state))

    flow._project = project
    result = await flow._stop({'automatic_retry_version': 1}, 'untrusted retry me', cause=cause)
    assert result['checks']['failure']['classification'] == expected
    assert result['checks']['failure']['stage'] == 'implement'
    assert projects[-1]['checks']['failure'] == result['checks']['failure']
    assert result['outcome'] == 'blocked'


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
    with pytest.raises(ApplicationError) as failure:
        await inbound.execute_activity(None)
    assert failure.value.type == 'DatabaseBusy' and not failure.value.non_retryable
    assert 'database busy' in str(failure.value)
    other = DatabaseContentionInterceptor().intercept_activity(
        Failing(sqlite3.OperationalError('no such table: delivery_runs')))
    with pytest.raises(sqlite3.OperationalError, match='no such table'):
        await other.execute_activity(None)


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


@pytest.mark.asyncio
async def test_old_budget_input_keeps_original_stop_payload():
    flow = DeliveryWorkflow()
    flow.state = {'phase': 'implement', 'revision': 1, 'roles': [], 'checks': {}, 'cleanup': 'none'}

    async def project(*args):
        pass

    flow._project = project
    result = await flow._stop({'retry_budget_version': 1}, 'legacy stopped')
    assert result['checks'] == {}


@pytest.mark.asyncio
async def test_legacy_recovery_keeps_old_stop_payload_even_with_new_original_marker():
    flow = DeliveryWorkflow()
    flow.original_execution = False
    flow.state = {'phase': 'implement', 'revision': 1, 'roles': [], 'checks': {}, 'cleanup': 'none'}

    async def project(*args):
        pass

    flow._project = project
    result = await flow._stop({'automatic_retry_version': 1}, 'legacy recovery',
                              cause=activity_failure(ApplicationError('network',
                                                                      type='ConnectionError')))
    assert result['checks'] == {}


@pytest.mark.asyncio
@pytest.mark.parametrize('code,expected', [
    ('ci_deadline', 'transient'), ('tracker_readback', 'transient'),
    ('publication_deadline', 'transient'), ('agent says transient', 'terminal'),
])
async def test_only_known_controller_results_can_classify_transient(code, expected):
    flow = DeliveryWorkflow()
    flow.state = {'phase': 'waiting_ci', 'revision': 1, 'roles': [], 'checks': {},
                  'cleanup': 'unknown'}

    async def project(*args):
        pass

    flow._project = project
    result = await flow._stop({'automatic_retry_version': 1}, 'deadline', controller_cause=code)
    assert result['checks']['failure']['classification'] == expected
    assert result['cleanup'] == 'unknown'


async def original_run(*, version=True, budget=True, failure='pending', capture=None):
    spec = {'run_id': 'failure-fixture', 'provider': 'fake',
            'policy': {'max_repairs': 0, 'browser_qa': None}}
    if budget:
        spec['retry_budget_version'] = 1
        spec['policy']['max_attempts'] = 3
    if version:
        spec['automatic_retry_version'] = 1
    candidate = {'id': 'candidate', 'head': 'candidate-head'}

    @activity.defn(name='delivery_project')
    async def project(_request):
        return {}

    @activity.defn(name='delivery_prepare')
    async def prepare(_request):
        return {'candidate': candidate}

    @activity.defn(name='delivery_tracker_start')
    async def tracker(_request):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_role')
    async def role(request):
        if failure == 'transport' and request['role'] == 'review':
            raise ConnectionError('fixture transport unavailable')
        return {'status': 'pass', 'session_id': request['role'], 'candidate': candidate}

    @activity.defn(name='delivery_precheck')
    async def precheck(_request):
        return {'state': 'passed'}

    @activity.defn(name='delivery_publish')
    async def publish(_request):
        return {'number': 1, 'head': candidate['head'], 'candidate': candidate}

    @activity.defn(name='delivery_checks')
    async def checks(_request):
        return {'state': 'passed'}

    @activity.defn(name='delivery_ci')
    async def ci(_request):
        return {'state': failure}

    async with await WorkflowEnvironment.start_local() as env:
        client = await Client.connect(env.client.service_client.config.target_host,
                                      namespace=env.client.namespace,
                                      identity='failure-fixture-client')
        async with Worker(client, task_queue='failure-fixture', workflows=[DeliveryWorkflow],
                          identity='failure-fixture-worker', activities=[
                              project, prepare, tracker, role, precheck, publish, checks, ci]):
            handle = await client.start_workflow(DeliveryWorkflow.run, spec,
                                                id='failure-fixture', task_queue='failure-fixture')
            result = await handle.result()
            if capture:
                Path(capture).write_text((await handle.fetch_history()).to_json())
    return result


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,expected', [('pending', 'transient'), ('failed', 'terminal'),
                                            ('stale', 'terminal'), ('transport', 'transient')])
async def test_actual_original_workflow_classifies_controller_failure(failure, expected):
    result = await original_run(failure=failure)
    assert result['outcome'] == 'blocked'
    assert result['checks']['failure']['classification'] == expected


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['delivery-failure-c04-history.json',
                                 'delivery-failure-budget-history.json'])
async def test_actual_previous_source_original_history_replays(name, tmp_path):
    path = Path(__file__).parent / 'fixtures' / name
    await replay_designated_history(path, tmp_path, 'failure-fixture')
