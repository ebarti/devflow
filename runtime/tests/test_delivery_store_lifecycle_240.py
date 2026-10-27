"""Owner preflight uses the same sealed store admission as execution."""


import pytest
from test_delivery_store import _scope_amendment_fixture

pytest_plugins = ["test_delivery_store"]


def _counts(store):
    with store._connect() as db:
        return tuple(
            db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in ("delivery_commands", "delivery_scope_amendments", "delivery_events")
        )


@pytest.mark.parametrize(
    "field,value", [("expected_revision", 999), ("expected_candidate_id", "f" * 64)]
)
def test_scope_stale_request_never_mutates(service, monkeypatch, field, value):
    store, request = service
    _original, _state, command, _closed = _scope_amendment_fixture(store, request, monkeypatch)
    command[field] = value
    before = _counts(store)
    for preflight in (True, False):
        with pytest.raises(ValueError):
            store.amend_scope("run-1", command, preflight=preflight)
        assert _counts(store) == before


@pytest.mark.asyncio
async def test_default_repair_public_assessment_and_admission_agree(service, monkeypatch):
    import httpx
    from test_delivery_store import _failed_published_repair_fixture

    from devflow_temporal.delivery_api import create_app

    store, request = service
    _spec, _state, command = _failed_published_repair_fixture(store, request, monkeypatch)
    app = create_app(store.config.path)
    app.state.delivery.store = store
    host = store.config.dashboard_url
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=('127.0.0.1', 10001)), base_url=host,
    ) as client:
        token = (await client.get('/api/session')).json()['csrf_token']
        headers = {'Origin': host, 'X-Devflow-CSRF': token}
        base = '/api/runs/run-1/'
        observed = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': command})
        assert observed.status_code == 200 and observed.json()['eligible']
        stale = {**command, 'expected_revision': command['expected_revision'] + 1}
        preview = await client.post(base + 'assess-transition', json={
            'kind': 'repair_continuation', 'request': stale})
        actual = await client.post(base + 'continue-repair', json=stale, headers=headers)
        assert actual.status_code == 409 and actual.json()['detail'] == preview.json()
        assert preview.json()['blockers'][0]['code'] == 'revision_changed'
        accepted = await client.post(base + 'continue-repair', json=command, headers=headers)
        assert accepted.status_code == 200 and accepted.json()['authorized_through_iteration'] == 4
