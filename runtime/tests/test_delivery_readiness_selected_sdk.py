"""Public native admission and selected-SDK readiness before any role."""
import asyncio
import json
import sys

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import controlled_terminal_tracker
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal import delivery_execution_authority as authority
from devflow_temporal.delivery_activities import (
    DatabaseContentionInterceptor,
    delivery_environment_readiness,
    delivery_finalize_resources,
    delivery_prepare,
    delivery_project,
)
from devflow_temporal.delivery_api import _dispatch_start, create_app
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.mark.skipif(sys.platform != 'darwin', reason='real native profile required')
@pytest.mark.asyncio
async def test_public_admission_selected_sdk_refuses_missing_tool_before_role(
    native_configuration, tmp_path,
):
    config, request = native_configuration
    proofs, roles = [], []
    async with local_temporal() as env:
        raw = json.loads(config.path.read_text())
        raw['temporal_address'] = env.client.service_client.config.target_host
        check = {'id': 'missing-node', 'argv': [str(tmp_path / 'missing' / 'node'), '--version']}
        raw['repositories']['fixture'].update(checks=[check], prepublish_checks=[check])
        config.path.write_text(json.dumps(raw))
        app = create_app(config.path)
        service = app.state.delivery
        transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 1234))

        @activity.defn(name='delivery_environment_readiness')
        async def observed_readiness(payload):
            proofs.append(authority.activity_authority())
            return await delivery_environment_readiness(payload)

        @activity.defn(name='delivery_role')
        async def forbidden_role(payload):
            roles.append(payload)
            raise AssertionError('missing tool must not launch a product role')

        async with httpx.AsyncClient(transport=transport, base_url=config.dashboard_url) as client:
            session = await client.get('/api/session')
            headers = {'Origin': config.dashboard_url,
                       'X-Devflow-CSRF': session.json()['csrf_token']}
            accepted = await client.post('/api/runs', json=request, headers=headers)
            assert accepted.status_code == 200, accepted.text
            spec = service.store.submitted_spec(request['run_id'])
            assert spec['readiness_version'] == spec['execution_authority_version'] == 1
            async with Worker(
                env.client, task_queue=service.config.queue,
                workflow_runner=UnsandboxedWorkflowRunner(), workflows=[DeliveryWorkflow],
                activities=[delivery_prepare, delivery_project, observed_readiness,
                            delivery_finalize_resources, controlled_terminal_tracker,
                            forbidden_role],
                interceptors=[DatabaseContentionInterceptor()],
            ):
                await _dispatch_start(service, env.client, service.store.pending_starts()[0], spec)
                handle = env.client.get_workflow_handle('delivery-' + request['run_id'])
                result = await asyncio.wait_for(handle.result(), timeout=90)
                history = await handle.fetch_history()
            detail = await client.get('/api/runs/' + request['run_id'])
            assert detail.status_code == 200
            assert result['outcome'] == 'blocked' and result['roles'] == [] and not roles
            receipt = result['checks']['readiness']
            assert receipt['state'] == 'unknown'
            assert receipt['observations']['tools']['state'] == 'unknown'
            assert receipt['receipt'] and receipt['receipt_sha256']
            assert len(proofs) == 1 and proofs[0]['workflow_id'] == handle.id
            with service.store._connect() as db:
                authority.require_current(db, spec, proofs[0], require_claim=False)
                assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
            scheduled = [event.activity_task_scheduled_event_attributes.activity_type.name
                         for event in history.events
                         if event.HasField('activity_task_scheduled_event_attributes')]
            assert 'delivery_environment_readiness' in scheduled
            assert 'delivery_role' not in scheduled

@pytest.mark.skipif(sys.platform != 'darwin', reason='real native profile required')
@pytest.mark.asyncio
async def test_public_selected_sdk_blocks_unverified_planned_vitest_before_product_role(
    native_configuration, tmp_path,
):
    import subprocess
    from pathlib import Path

    from devflow_temporal.delivery_activities import delivery_accept_plan, delivery_role

    config, request = native_configuration
    source = Path(config.raw['repositories']['fixture']['source_path'])
    (source / 'api' / 'test').mkdir(parents=True)
    (source / 'api' / 'test' / 'audit.test.ts').write_text('test("audit", () => {});\n')
    (source / 'api' / 'package.json').write_text('{"devDependencies":{"vitest":"4.1.11"}}')
    (source / 'package.json').write_text('{"packageManager":"pnpm@10.33.3"}')
    (source / 'pnpm-lock.yaml').write_text('lockfileVersion: 9.0\n')
    subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(source), 'commit', '-qm', 'tracked test fixture'], check=True)
    base = subprocess.check_output(
        ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
    role_results = []
    async with local_temporal() as env:
        raw = json.loads(config.path.read_text())
        raw['temporal_address'] = env.client.service_client.config.target_host
        check = {'id': 'fixture-check', 'argv': ['git', 'diff', '--check']}
        raw['repositories']['fixture'].update(
            expected_base_sha=base, checks=[check], prepublish_checks=[check])
        config.path.write_text(json.dumps(raw))
        app = create_app(config.path)
        service = app.state.delivery
        transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 1234))

        @activity.defn(name='delivery_intake')
        async def intake(_payload):
            return {'status': 'plan', 'summary': 'Controlled accepted plan', 'cleanup': 'confirmed',
                    'plan': {'scope': 'Update README', 'steps': ['Update README'],
                             'verification': ['Run audit.test.ts using Vitest'],
                             'acceptance': ['README is updated']}}

        @activity.defn(name='delivery_tracker_start')
        async def tracker_start(_payload):
            return {'state': 'consistent'}

        @activity.defn(name='delivery_role')
        async def observe_role(payload):
            result = await delivery_role(payload)
            role_results.append(result)
            return result

        async with httpx.AsyncClient(transport=transport, base_url=config.dashboard_url) as client:
            session = await client.get('/api/session')
            headers = {'Origin': config.dashboard_url,
                       'X-Devflow-CSRF': session.json()['csrf_token']}
            accepted = await client.post('/api/runs', json=request, headers=headers)
            assert accepted.status_code == 200, accepted.text
            spec = service.store.submitted_spec(request['run_id'])
            async with Worker(
                env.client, task_queue=service.config.queue,
                workflow_runner=UnsandboxedWorkflowRunner(), workflows=[DeliveryWorkflow],
                activities=[delivery_prepare, delivery_project, delivery_environment_readiness,
                            delivery_finalize_resources, controlled_terminal_tracker, intake,
                            delivery_accept_plan, tracker_start, observe_role],
                interceptors=[DatabaseContentionInterceptor()],
            ):
                await _dispatch_start(service, env.client, service.store.pending_starts()[0], spec)
                handle = env.client.get_workflow_handle('delivery-' + request['run_id'])
                result = await asyncio.wait_for(handle.result(), timeout=90)
            assert result['outcome'] == 'blocked'
            assert len(role_results) == 1 and role_results[0]['status'] == 'blocked', result
            receipt = role_results[0]['readiness']
            tools = receipt['observations']['tools']
            assert receipt['state'] in {'unknown', 'failed'}
            assert tools['check_id'].startswith('planned-vitest-')
            assert tools['state'] in {'unknown', 'failed'}
            assert receipt['receipt'] and receipt['receipt_sha256']
            with service.store._connect() as db:
                assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
