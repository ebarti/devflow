"""Readiness refusals stop the real workflow before a product repair."""
import shutil

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.parametrize('failed_activity,stage', [
    ('delivery_precheck', 'prepublish'),
    ('delivery_checks', 'local'),
    ('delivery_browser_qa', 'browser_qa'),
])
async def test_real_workflow_stops_for_gate_readiness_without_repair(
    tmp_path, failed_activity, stage,
):
    candidate = {'id': 'fixture-candidate', 'head': 'fixture-head'}
    calls = []
    receipt = {'state': 'failed', 'receipt': '/owned/readiness.json', 'receipt_sha256': 'a' * 64}

    def stub(name):
        @activity.defn(name=name)
        async def execute(payload):
            calls.append((name, payload.get('role'), payload.get('iteration')))
            if name == failed_activity:
                return {'state': 'failed', 'cleanup': 'confirmed',
                        'failure_kind': 'readiness_preparation', 'launched': False,
                        'candidate_id': candidate['id'], 'source_unchanged': True,
                        'readiness': receipt}
            if name == 'delivery_prepare':
                return {'candidate': candidate}
            if name == 'delivery_environment_readiness':
                return {'state': 'passed'}
            if name == 'delivery_role':
                return {'status': 'pass', 'candidate': candidate, 'session_id': 'fixture-session'}
            if name == 'delivery_publish':
                return {'candidate': candidate, 'head': candidate['head']}
            if name in {'delivery_tracker_start', 'delivery_tracker'}:
                return {'state': 'consistent'}
            if name == 'delivery_browser_qa':
                return {'state': 'passed', 'cleanup': 'confirmed', 'receipt': '/fixture/qa',
                        'receipt_sha256': 'b' * 64, 'log': '/fixture/log', 'log_sha256': 'c' * 64}
            return {'state': 'passed', 'cleanup': 'confirmed'}
        return execute

    names = ('delivery_prepare', 'delivery_environment_readiness', 'delivery_project',
             'delivery_tracker_start', 'delivery_tracker', 'delivery_role', 'delivery_precheck',
             'delivery_checks', 'delivery_browser_qa', 'delivery_publish', 'delivery_ci')
    spec = {'run_id': 'readiness-' + stage, 'provider': 'fake', 'readiness_version': 1,
            'policy': {'max_repairs': 2, 'browser_qa': {'id': 'browser'},
                       'prepublish_checks': [{'id': 'fixture'}]}}
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
        dev_server_database_filename=str(tmp_path / 'temporal.sqlite3'),
    ) as env:
        async with Worker(env.client, task_queue=spec['run_id'], workflows=[DeliveryWorkflow],
                          activities=[stub(name) for name in names]):
            result = await (await env.client.start_workflow(
                DeliveryWorkflow.run, spec, id=spec['run_id'], task_queue=spec['run_id'])).result()
    assert result['outcome'] == 'blocked'
    assert result['checks'][stage]['readiness'] == receipt
    assert result['checks']['readiness'] == receipt
    assert result['iteration'] == 0 and result['findings'] == []
    assert len([call for call in calls if call[:2] == ('delivery_role', 'implement')]) == 1
