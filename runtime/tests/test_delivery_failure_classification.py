from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest
from historical_replay import replay_designated_history
from temporalio import activity
from temporalio.client import Client
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    RetryState,
    TimeoutError,
    TimeoutType,
)
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_config import scope_amended_spec
from devflow_temporal.delivery_workflow import DeliveryWorkflow


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
