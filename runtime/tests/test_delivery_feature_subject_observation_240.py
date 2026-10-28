"""An owner observation cannot mix registry facts across a remote read."""

from copy import deepcopy

import pytest
from test_delivery_feature_revisions import feature as feature
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_store import service as service

from devflow_temporal.delivery_execution_config import observation_mode
from devflow_temporal.delivery_feature_revision_subject import (
    FeatureRegistryObservationError,
    ObservationBroker,
)
from devflow_temporal.delivery_github_contract import GitHubDelivery


def test_subject_remote_read_cannot_accept_mixed_owner_registry(feature, monkeypatch):
    store, parent, shared, record, _diagnostic = feature
    spec = deepcopy(parent)
    spec['feature_worker'] = {'kind': 'chunk', 'chunk_id': 'model', 'workstream_id': 'core'}
    spec['issue_url'] = 'https://github.com/example/fixture/issues/10'
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.publication_child_binding',
                        lambda *_args: {'workstream_id': 'core', 'issue_url': spec['issue_url']})
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.live_members',
                        lambda *_args: [])
    calls = []

    def changed(*_args):
        calls.append(True)
        with shared.connect() as db:
            db.execute('UPDATE execution_claims SET generation=generation+1 WHERE issue_id=?',
                       (parent['feature_delivery']['owner']['issue_id'],))
        return deepcopy(record)

    monkeypatch.setattr(GitHubDelivery, 'load_record', changed)
    broker = ObservationBroker(store, spec)
    with pytest.raises(FeatureRegistryObservationError):
        with observation_mode():
            assert broker._record() == record
    assert calls


@pytest.mark.asyncio
async def test_public_preflight_returns_registry_unknown_after_subject_race(feature, monkeypatch):
    import httpx

    from devflow_temporal import delivery_feature_execution
    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_lifecycle import evaluate_transition

    store, parent, shared, record, _diagnostic = feature
    spec = deepcopy(parent)
    spec['feature_worker'] = {'kind': 'chunk', 'chunk_id': 'model', 'workstream_id': 'core'}
    spec['issue_url'] = 'https://github.com/example/fixture/issues/10'
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.publication_child_binding',
                        lambda *_args: {'workstream_id': 'core', 'issue_url': spec['issue_url']})
    monkeypatch.setattr('devflow_temporal.delivery_feature_publication.live_members',
                        lambda *_args: [])

    def changed(*_args):
        with shared.connect() as db:
            db.execute('UPDATE execution_claims SET generation=generation+1 WHERE issue_id=?',
                       (parent['feature_delivery']['owner']['issue_id'],))
        return deepcopy(record)

    monkeypatch.setattr(GitHubDelivery, 'load_record', changed)
    broker = ObservationBroker(store, spec)

    def owner_observation(*_args):
        broker._record()
        return evaluate_transition({'operation': 'continue_feature', 'owner': 'feature_registry',
                                    'owner_proof': True, 'proof_digest': 'proof',
                                    'comparisons': []}, {})

    monkeypatch.setattr(delivery_feature_execution, 'assess_feature_continuation',
                        owner_observation)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    origin = store.config.dashboard_url.rstrip('/')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=origin) as client:
        response = await client.post(
            f"/api/runs/{parent['run_id']}/assess-transition",
            json={'kind': 'continue_feature', 'request': {}},
        )
    assert response.status_code == 200
    assessment = response.json()
    assert not assessment['eligible']
    blocker = next(b for b in assessment['blockers']
                   if b['code'] == 'feature_registry_proof_unknown')
    assert blocker['owner'] == 'feature_registry'
    assert blocker['satisfied'] is None
