from __future__ import annotations

import asyncio
from copy import deepcopy
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from devflow_temporal.delivery_workflow import DeliveryWorkflow

CANDIDATE = {'id': 'candidate-1', 'head': 'head-1'}
SPEC = {'run_id': 'qa-findings', 'provider': 'fake', 'policy': {},
        'resource_cleanup_version': 1, 'terminal_tracker_version': 1}
STATE = {'run_id': SPEC['run_id'], 'revision': 1, 'iteration': 4,
         'candidate_revision': 1, 'candidate': CANDIDATE,
         'pull_request': {'candidate': CANDIDATE, 'head': 'head-1', 'number': 1},
         'roles': [{'role': 'verify', 'iteration': 4, 'status': 'findings',
                    'findings': ['A required behavior failed'], 'candidate': CANDIDATE}],
         'checks': {'qa': {'state': 'failed'}}, 'findings': ['A required behavior failed']}
RECOVERY = {'kind': 'investigation_assessment_adjudication', 'execution_spec': SPEC,
            'maximum_iteration': 4, 'command': {'additional_iterations': 0}, 'state': STATE}


@activity.defn(name='delivery_adjudication_readback')
async def readback(_request):
    return {'state': 'adjudicated', 'raw_status': 'findings'}


@activity.defn(name='delivery_ci')
async def ci(_request):
    return {'state': 'passed'}


@activity.defn(name='delivery_project')
async def project(_request):
    return {}


@activity.defn(name='delivery_finalize_resources')
async def finalize(_request):
    return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
            'resource_cleanup': 'confirmed'}


@activity.defn(name='delivery_terminal_tracker')
async def tracker(_request):
    return {'state': 'consistent'}


@pytest.mark.asyncio
async def test_new_adjudication_cannot_deliver_while_qa_has_findings():
    async with await WorkflowEnvironment.start_local() as env:
        async with Worker(env.client, task_queue='qa-findings', workflows=[DeliveryWorkflow],
                          activities=[readback, ci, project, finalize, tracker],
                          identity='qa-fixture-worker'):
            result = await env.client.execute_workflow(
                DeliveryWorkflow.run, args=[deepcopy(SPEC), deepcopy(RECOVERY)],
                id='qa-findings', task_queue='qa-findings',
            )
    assert result['outcome'] == 'blocked'
    assert result['checks']['qa']['state'] == 'failed'
    assert result['roles'] == STATE['roles']
    assert result['findings'] == STATE['findings']
    assert result['cleanup'] == 'confirmed'
    assert result['tracker']['state'] == 'consistent'


@pytest.mark.asyncio
@pytest.mark.parametrize('name', ['delivery-adjudication-history.json',
                                 'delivery-adjudication-waiting-ci-history.json'])
async def test_recorded_legacy_adjudication_replays(name):
    path = Path(__file__).parent / 'fixtures' / name
    history = WorkflowHistory.from_json('qa-replay', path.read_text())
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


@pytest.mark.asyncio
async def test_terminal_transition_after_workflow_worker_restart_still_blocks_findings():
    started, release = asyncio.Event(), asyncio.Event()

    @activity.defn(name='delivery_ci')
    async def waiting_ci(_request):
        started.set()
        await release.wait()
        return {'state': 'passed'}

    async with await WorkflowEnvironment.start_local() as env:
        # The activity worker retains CI while the workflow worker is replaced.
        async with Worker(env.client, task_queue='qa-restart',
                          activities=[readback, waiting_ci, project, finalize, tracker],
                          identity='qa-activity-fixture-worker'):
            async with Worker(env.client, task_queue='qa-restart', workflows=[DeliveryWorkflow],
                              identity='qa-first-fixture-worker'):
                handle = await env.client.start_workflow(
                    DeliveryWorkflow.run, args=[deepcopy(SPEC), deepcopy(RECOVERY)],
                    id='qa-restart', task_queue='qa-restart',
                )
                await asyncio.wait_for(started.wait(), 30)
            async with Worker(env.client, task_queue='qa-restart', workflows=[DeliveryWorkflow],
                              identity='qa-next-fixture-worker'):
                release.set()
                result = await handle.result()
    assert result['outcome'] == 'blocked'
    assert result['checks']['ci']['state'] == 'passed'
    assert result['checks']['qa']['state'] == 'failed'
    assert result['roles'] == STATE['roles']
    assert result['cleanup'] == 'confirmed'
