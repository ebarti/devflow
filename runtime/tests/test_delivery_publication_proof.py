"""Publication-stage authority comes from typed closure or actual history, not text."""
from __future__ import annotations

import asyncio
import copy
import json
import lzma
import os
import zlib
from datetime import timedelta
from types import MethodType

import httpx
import pytest
from temporal_test_server import local_temporal
from temporalio import activity, workflow
from temporalio.api.common.v1 import Payload
from temporalio.exceptions import ActivityError, ApplicationError
from temporalio.worker import UnsandboxedWorkflowRunner, Worker
from test_delivery_publication_liveness import run_publication_probe
from test_delivery_publication_self_heal import pending_v1
from test_delivery_store import service as service

from devflow_temporal import delivery_closures as closures
from devflow_temporal import delivery_publication_proof as proof
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_lifecycle import RecoveryRefused
from devflow_temporal.delivery_store import DeliveryStore


@activity.defn(name='delivery_publish')
async def owned_failed_publication(_request):
    raise ApplicationError('owned transport stopped', non_retryable=True)


@activity.defn(name='delivery_checks')
async def owned_failed_check(_request):
    raise ApplicationError('owned later check stopped', non_retryable=True)


@workflow.defn(name='DevflowDeliveryWorkflow')
class RetainedPublicationHistory:
    # Controlled retained producer; the real owner reads its exact server history.
    retained_state = {}
    last_activity = 'delivery_publish'

    @workflow.run
    async def run(self, spec, recovery=None):
        state = copy.deepcopy(self.retained_state)
        try:
            await workflow.execute_activity(
                self.last_activity,
                {'spec': spec, 'iteration': state['iteration'], 'candidate': state['candidate']},
                start_to_close_timeout=timedelta(seconds=5),
            )
        except ActivityError:
            pass
        return state


def closed_fixture(spec):
    state = {'run_id': spec['run_id'], 'phase': 'blocked', 'execution_state': 'blocked',
             'outcome': 'blocked', 'iteration': 0, 'revision': 3,
             'candidate': {'id': 'a' * 64, 'head': 'b' * 40}, 'checks': {'failure': {
                 'classification': 'transient', 'cause_type': 'publication_deadline',
                 'stage': 'publishing_pending'}}, 'error': 'display text is not authority'}
    return {'workflow_id': 'delivery-' + spec['run_id'], 'execution_run_id': 'owned-execution',
            'request_digest': spec['request_digest'], 'recovery_digest': None,
            'closed_at': '2026-10-26T00:00:00+00:00', 'result': state}


def test_typed_closed_proof_binds_current_stage_without_history(service, monkeypatch):
    store, request = service
    store.submit(request)
    spec = store.spec(request['run_id'])
    closed = closed_fixture(spec)

    async def unavailable(*_args):
        raise AssertionError('typed controller result needs no historical text inference')

    monkeypatch.setattr(proof, '_history', unavailable)
    observed = proof.observe(store, spec, closed)
    assert observed['source'] == 'closed_result'
    assert proof.matches(spec, closed['result'], observed)
    assert not proof.matches(spec, {**closed['result'], 'iteration': 1}, observed)


def test_matching_english_prefix_without_evidence_stays_typed_unknown(service, monkeypatch):
    store, request = service
    store.submit(request)
    spec = store.spec(request['run_id'])
    closed = closed_fixture(spec)
    closed['result']['checks'] = {}
    closed['result']['error'] = 'publication unresolved: ActivityError'

    async def unavailable(*_args):
        raise OSError('exact original history unavailable')

    monkeypatch.setattr(proof, '_history', unavailable)
    with pytest.raises(RecoveryRefused) as rejected:
        proof.observe(store, spec, closed)
    blocker = rejected.value.assessment['blockers'][0]
    assert blocker['code'] == 'publication_stage_unproven'
    assert blocker['satisfied'] is None


@pytest.mark.parametrize('encoding,compress', [(b'binary/zlib', zlib.compress),
                                              (b'binary/xz', lzma.compress)])
def test_compressed_payload_expansion_is_bounded_before_codec_decode(
    service, encoding, compress,
):
    store, request = service
    store.submit(request)
    closed = closed_fixture(store.spec(request['run_id']))
    raw = Payload(data=b'a' * (proof.MAX_PAYLOAD_BYTES + 1)).SerializeToString()
    compressed = Payload(metadata={'encoding': encoding}, data=compress(raw))
    assert compressed.ByteSize() < 10_000
    with pytest.raises(RecoveryRefused) as rejected:
        proof._bounded_payloads([compressed], closed)
    assert rejected.value.assessment['blockers'][0]['code'] == (
        'publication_history_bound_exceeded')


@pytest.mark.asyncio
async def test_actual_current_controller_supplies_typed_publication_stage():
    # Authenticated closure bindings are covered independently. This executes the
    # current controller rather than inferring the field from its helper alone.
    state, _calls, _events = await run_publication_probe(
        failure='deadline', readback_seconds=2)
    failure = state['checks']['failure']
    assert failure['classification'] == 'transient'
    assert failure['cause_type'] == 'publication_deadline'
    assert failure['stage'] == 'publishing_pending'


@pytest.mark.asyncio
@pytest.mark.parametrize('later_check', [False, True], ids=['genuine-legacy-stop', 'false-prefix'])
async def test_public_assessment_uses_real_closed_stage_without_mutation(
    service, monkeypatch, later_check,
):
    original, request = service
    async with local_temporal() as env:
        raw = json.loads(original.config.path.read_text())
        raw['temporal_address'] = env.client.service_client.config.target_host
        original.config.path.write_text(json.dumps(raw))
        bound = DeliveryStore(DeliveryConfig.load(original.config.path))
        store, broker, state, command, _stop = pending_v1.__wrapped__((bound, request), monkeypatch)
        # Restore the real owner observer after the older component fixture.
        monkeypatch.setattr('devflow_temporal.delivery_pending_publication.publication_stop_proof',
                            proof.observe)
        state['checks'].pop('failure', None)
        state['error'] = ('publication unresolved: ActivityError' if later_check
                          else 'Changed display wording on a genuine publication stop')
        store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                      message=state['error'], candidate=state['candidate'], checks=state['checks'],
                      iteration=state['iteration'], protocol_revision=state['revision'],
                      outcome='blocked', cleanup='confirmed', error=state['error'])
        spec = store.spec('run-1')
        monkeypatch.setattr(RetainedPublicationHistory, 'retained_state', copy.deepcopy(state))
        monkeypatch.setattr(RetainedPublicationHistory, 'last_activity',
                            'delivery_checks' if later_check else 'delivery_publish')
        monkeypatch.setattr(store, '_completed_temporal_result',
                            MethodType(DeliveryStore._completed_temporal_result, store))
        monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                            lambda *_args, **_kwargs: {})
        async with Worker(env.client, task_queue='publication-stage-proof',
                          workflows=[RetainedPublicationHistory],
                          activities=[owned_failed_publication, owned_failed_check],
                          workflow_runner=UnsandboxedWorkflowRunner()):
            handle = await env.client.start_workflow(
                RetainedPublicationHistory.run, args=[spec, None], id='delivery-run-1',
                task_queue='publication-stage-proof',
                memo={'request_digest': spec['request_digest']})
            assert await asyncio.wait_for(handle.result(), 15) == state
        app = create_app(store.config.path)
        app.state.delivery.store = store
        with store._connect() as db:
            before = list(db.iterdump())
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                     base_url=store.config.dashboard_url) as browser:
            response = await browser.post('/api/runs/run-1/assess-transition', json={
                'kind': 'recover_publication', 'request': command})
        assert response.status_code == 200
        assessed = response.json()
        if later_check:
            assert not assessed['eligible']
            assert assessed['blockers'][0]['code'] == 'publication_stage_changed'
            assert assessed['blockers'][0]['satisfied'] is False
        else:
            assert assessed['eligible'], assessed
            assert assessed['proof_digest']
        with store._connect() as db:
            assert list(db.iterdump()) == before
            assert not db.execute('SELECT 1 FROM delivery_closures').fetchone()
        if not later_check:
            # Sealing is an explicit owner operation after the read-only assertion.
            history = await handle.fetch_history()
            facts = await closures.closure_facts(history)
            closures.seal(store, history, facts, run_id='run-1', source='temporal')
            closed = await asyncio.to_thread(store._completed_temporal_result, 'run-1',
                                            preserve=False)
            observed = await asyncio.to_thread(proof.observe, store, spec, closed)
            assert observed['source'] == 'closed_history'
            assert observed['history_sha256'] == closures.history_sha256(history)
            assert proof.matches(spec, state, observed)
            before = digest(closed)
            with monkeypatch.context() as bounded:
                bounded.setattr(proof, 'MAX_HISTORY_EVENTS', 1)
                with pytest.raises(RecoveryRefused) as rejected:
                    await asyncio.to_thread(proof.observe, store, spec, closed)
            assert rejected.value.assessment['blockers'][0]['code'] == (
                'publication_history_bound_exceeded')
            assert digest(closed) == before
            record = closures.sealed_record(store, closed['workflow_id'],
                                            execution_run_id=closed['execution_run_id'])
            artifact = (store.config.state_root / 'closures'
                        / (record['history_sha256'] + '.json.xz'))
            artifact.chmod(0o644)
            try:
                with pytest.raises(RecoveryRefused) as rejected:
                    await asyncio.to_thread(proof.observe, store, spec, closed)
                assert rejected.value.assessment['blockers'][0]['code'] == (
                    'publication_stage_unproven')
                assert artifact.stat().st_mode & 0o777 == 0o644
            finally:
                artifact.chmod(0o600)
            alias = artifact.with_suffix('.owned-alias')
            os.link(artifact, alias)
            try:
                with pytest.raises(RecoveryRefused):
                    await asyncio.to_thread(proof.observe, store, spec, closed)
            finally:
                alias.unlink()
