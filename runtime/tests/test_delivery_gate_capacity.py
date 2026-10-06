"""Concurrent workflows must not multiply heavyweight host test process trees."""
import asyncio
import threading
from types import SimpleNamespace

from devflow_temporal import delivery_activities as activities


def test_mixed_check_batches_share_admission_without_blocking_worker_loop(tmp_path, monkeypatch):
    ready = threading.Barrier(4)
    release = threading.Event()
    entered = threading.Event()
    guard = threading.Lock()
    observed = {'active': 0, 'maximum': 0, 'completed': 0}
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path))

    def run(*args):
        with guard:
            observed['active'] += 1
            observed['maximum'] = max(observed['maximum'], observed['active'])
        entered.set()
        try:
            assert release.wait(5)
            return {'state': 'passed', 'cleanup': 'confirmed'}
        finally:
            with guard:
                observed['active'] -= 1
                observed['completed'] += 1

    broker = SimpleNamespace(run_checks=run, run_prechecks=run, run_browser_qa=run)

    def context(spec):
        ready.wait(timeout=5)
        return store, broker

    monkeypatch.setattr(activities, '_context', context)

    async def execute():
        request = {'spec': {'provider': 'codex'}, 'iteration': 0, 'candidate': {'id': 'checked'}}
        tasks = [asyncio.create_task(fn(request)) for fn in (
            activities.delivery_precheck, activities.delivery_checks,
            activities.delivery_browser_qa, activities.delivery_precheck)]
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            await asyncio.sleep(0)
        finally:
            release.set()
        assert all(r['state'] == 'passed' for r in await asyncio.gather(*tasks))

    asyncio.run(execute())
    assert observed['completed'] == 4
    assert observed['maximum'] == 1
