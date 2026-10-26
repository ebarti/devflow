from __future__ import annotations

import asyncio
import threading

import httpx
import pytest

from devflow_temporal.delivery_api import create_app
from test_delivery_api import api_fixture



@pytest.mark.asyncio
async def test_public_operation_survives_caller_and_deduplicates(api_fixture, monkeypatch):
    path, _ = api_fixture
    started, release = threading.Event(), threading.Event()
    calls = []

    def slow(store, run_id):
        calls.append(run_id)
        started.set()
        assert release.wait(10)
        return {'run_id': run_id, 'observation': 'complete'}

    monkeypatch.setattr('devflow_temporal.delivery_merge_followup.preflight', slow)
    app = create_app(path)
    transport = httpx.ASGITransport(app=app, client=('127.0.0.1', 1234))
    envelope = {'operation_id': 'operation-1', 'run_id': 'run-1',
                'kind': 'merge-preflight', 'request': {}}
    async with httpx.AsyncClient(transport=transport, base_url='http://127.0.0.1:18770') as client:
        response = await client.post('/api/operations', json=envelope)
        assert response.status_code == 202
        assert await asyncio.to_thread(started.wait, 5)
        duplicates = await asyncio.gather(*[client.post('/api/operations', json=envelope) for _ in range(5)])
        assert all(item.status_code == 202 for item in duplicates)
        conflict = await client.post('/api/operations', json={**envelope, 'request': {'other': 1}})
        assert conflict.status_code == 409
        assert (await client.get('/api/operations/operation-1')).json()['state'] == 'running'
        release.set()
        for _ in range(100):
            result = (await client.get('/api/operations/operation-1')).json()
            if result['state'] == 'succeeded':
                break
            await asyncio.sleep(.02)
        assert result['result'] == {'run_id': 'run-1', 'observation': 'complete'}
        assert result['authority'] == 'current_authority_unverified'
        assert (await client.post('/api/operations', json=envelope)).json()['result'] == result['result']
        assert calls == ['run-1']
