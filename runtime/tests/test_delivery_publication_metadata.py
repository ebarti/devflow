from __future__ import annotations

import json

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_broker
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.fixture
def publisher(service, monkeypatch):
    store, request = service
    request["goal"] = "refactor(profile): share pure production and demo coaching policy"
    store.submit(request)
    broker = DeliveryBroker(store, store.spec(request["run_id"]))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    monkeypatch.setattr(broker, "_existing_pr", lambda: {
        "number": 7, "url": "https://example.invalid/pull/7", "state": "OPEN",
        "headRefOid": _git(broker.checkout, "rev-parse", "HEAD"),
        "title": request["goal"],
    })
    return broker


@pytest.mark.parametrize("title", ["Implement refactor(profile): change", ""])
def test_actual_pr_readback_refuses_nonconventional_title(publisher, monkeypatch, title):
    broker = publisher
    found = {"number": 7, "url": "https://example.invalid/pull/7", "state": "OPEN",
             "isDraft": False, "headRefName": broker.spec["branch"], "baseRefName": "HEAD",
             "headRefOid": broker.spec["base_sha"], "title": title}
    monkeypatch.setattr(delivery_broker, "_run", lambda *_args, **_kwargs: json.dumps([found]))
    with pytest.raises(ValueError, match="PR title"):
        DeliveryBroker._existing_pr(broker)


def test_detailed_goal_requires_a_separate_publication_summary(service):
    store, request = service
    goal = ('Investigate Unicode comparison behavior. Own only the architecture document. '
            'Preserve evidence and publish the design without implementing production behavior.')
    request['goal'] = goal
    with pytest.raises(ValueError, match='publication_summary'):
        store.submit(request)
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 0
        assert store.state.claim_for(db, request['work_id']) is None
    request['publication_summary'] = 'docs: investigate Unicode comparison behavior'
    store.submit(request)
    assert store.spec(request['run_id'])['goal'] == goal
    assert store.spec(request['run_id'])['publication_summary'] == request['publication_summary']
    assert store.submit(request)['existing'] is False
    with pytest.raises(ValueError, match='different inputs'):
        store.submit({**request, 'publication_summary': 'feat: implement Unicode comparison'})
