from __future__ import annotations

import json
from copy import deepcopy

import pytest
from test_delivery_gate_retry import unpublished as unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.fixture
def pending(service, monkeypatch):
    # This component fixture controls the closed owner boundary. Actual stage
    # authentication is qualified by test_delivery_publication_proof.py.
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication.publication_stop_proof',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication.read_publication_closed',
                        lambda store, spec, **_kwargs:
                        (store._completed_temporal_result(spec['run_id']), None))
    store, _request = service
    original_admit = type(store.config).admit

    def historical_admit(config, request, **kwargs):
        spec = original_admit(config, request, **kwargs)
        spec.pop('publication_readback_version')
        spec.pop('publication_readback_seconds')
        return spec

    monkeypatch.setattr(type(store.config), 'admit', historical_admit)
    store, broker, original, gate_request = unpublished.__wrapped__(service, monkeypatch)

    def renew(spec, *_args):
        renewed = deepcopy(spec)
        renewed['policy']['measured_runtime'] = 'renewed'
        renewed['policy_digest'] = digest(renewed['policy'])
        return renewed

    monkeypatch.setattr('devflow_temporal.delivery_gate_retry.prepare_runtime', renew)
    store.continue_repair('run-1', gate_request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        previous = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
        ).fetchone()[0])
    broker = DeliveryBroker(store, spec)
    checked_candidate = broker.candidate()
    broker._effect('publish:run-1:0', 'publish', {
        'iteration': 0, 'input_candidate_id': original['candidate']['id'],
    })
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-sqm', 'docs: document the owned feature')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    state = deepcopy(original)
    state.update(error='publication unresolved: ActivityError', revision=13,
                 candidate=checked_candidate)
    state['checks']['prepublish'].update(state='passed', results=[
        {'id': 'packaging', 'passed': True, 'cleanup': 'confirmed', 'exit_code': 0}])
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=state['error'], candidate=state['candidate'], pull_request=None,
                  checks=state['checks'], iteration=0, protocol_revision=13, outcome='blocked',
                  cleanup='confirmed', error=state['error'])
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
        workflow = db.execute("SELECT workflow_id FROM delivery_runs WHERE run_id='run-1'")
        workflow_id = workflow.fetchone()[0]
    closed = {'workflow_id': workflow_id, 'request_digest': spec['request_digest'],
              'recovery_digest': digest(previous), 'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_args, **_kw: closed)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _, **_kw: {})
    request = {'command_id': 'retry-publication-1', 'expected_revision': 13,
               'expected_candidate_id': state['candidate']['id'],
               'expected_head': broker.candidate()['head'], 'expected_pr_number': 0}
    return store, broker, state, request


@pytest.mark.parametrize('drift', ['source', 'effect'])
def test_pending_retry_rejects_changed_pushed_source_or_failed_checkpoint(pending, drift):
    store, broker, state, request = pending
    if drift == 'source':
        (broker.checkout / 'README.md').write_text('Changed after checking\n')
    elif drift == 'head':
        request['expected_head'] = 'a' * 40
    elif drift == 'checks':
        state['checks']['prepublish']['results'][0]['passed'] = False
    else:
        with store._connect() as db:
            db.execute("UPDATE delivery_effects SET request_json='{}' WHERE kind='publish'")
    with pytest.raises(ValueError):
        store.recover_publication('run-1', request)


@pytest.mark.asyncio
async def test_public_pending_assessment_agrees_with_admission(pending):
    import httpx

    from devflow_temporal.delivery_api import create_app

    store, _, _, request = pending
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 10001))
    async with httpx.AsyncClient(transport=transport, base_url=host) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        path = '/api/runs/run-1/'
        assessment = await client.post(path + 'assess-transition', json={
            'kind': 'recover_publication', 'request': request})
        assert assessment.status_code == 200 and assessment.json()['eligible']
        stale = {**request, 'expected_revision': request['expected_revision'] + 1}
        refused = await client.post(path + 'assess-transition', json={
            'kind': 'recover_publication', 'request': stale})
        admission = await client.post(path + 'recover-publication', json=stale, headers=headers)
        assert admission.status_code == 409 and admission.json()['detail'] == refused.json()
        assert refused.json()['blockers'][0]['code'] == 'revision_changed'
        accepted = await client.post(path + 'recover-publication', json=request, headers=headers)
        assert accepted.status_code == 200 and not accepted.json()['implementation_authority']
