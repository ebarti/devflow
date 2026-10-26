"""Publication custody follows actual bodies, including a killed Python parent."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_publication_execution import execution


@pytest.fixture
def publication(service, monkeypatch, tmp_path):
    store, request = service
    store.submit(request)
    spec = store.spec(request['run_id'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Owned publication edit\n')
    checked = broker.candidate()
    bindir = tmp_path / 'owned-bin'
    bindir.mkdir()
    fake = bindir / 'gh'
    fake.write_text('#!' + sys.executable + '\n' + '''import json,os,subprocess,sys,time
from pathlib import Path
root=Path(os.environ['PUBLICATION_FIXTURE'])
def event(kind):
    with (root/'events.jsonl').open('a') as out:
        out.write(json.dumps({'kind':kind,'pid':os.getpid()})+'\\n')
if sys.argv[1:3]==['pr','list']:
    if os.environ.get('PUBLICATION_LIST_FAIL') and not (root/'list-failed').exists():
        (root/'list-failed').touch()
        event('list-start')
        time.sleep(1)
        event('list-end')
        raise SystemExit('owned query transport failed before mutation')
    print((root/'receipt.json').read_text() if (root/'receipt.json').exists() else '[]')
elif sys.argv[1:3]==['pr','create']:
    event('create-start')
    if os.environ.get('PUBLICATION_FAIL'):
        event('create-end')
        raise SystemExit('lost or failed create response; outcome unconfirmed')
    time.sleep(1)
    receipt=json.loads(os.environ['PUBLICATION_RECEIPT'])
    receipt[0]['headRefOid']=subprocess.check_output(['/usr/bin/git','rev-parse','HEAD'],text=True).strip()
    (root/'receipt.json').write_text(json.dumps(receipt))
    event('create-end')
    print('https://example.invalid/pull/1')
else:
    raise SystemExit('unexpected fixture command')
''')
    fake.chmod(0o700)
    receipt = [{
        'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
        'isDraft': False, 'baseRefName': broker._publication_base_ref(),
        'headRefName': spec['branch'], 'headRefOid': None,
        'title': 'chore: Change a fixture',
    }]
    monkeypatch.setenv('PATH', str(bindir) + os.pathsep + os.environ['PATH'])
    monkeypatch.setenv('PUBLICATION_FIXTURE', str(tmp_path))
    monkeypatch.setenv('PUBLICATION_RECEIPT', json.dumps(receipt))
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda _spec: (store, DeliveryBroker(store, spec)))
    return store, spec, broker, checked, tmp_path, receipt


def events(root):
    path = root / 'events.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def until(root, kind, child=None):
    deadline = time.monotonic() + 30
    while not any(item['kind'] == kind for item in events(root)):
        if child is not None and child.poll() is not None:
            pytest.fail('owned publication child exited: ' + (root / 'child-stderr').read_text())
        assert time.monotonic() < deadline, 'owned publication process did not reach ' + kind
        await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_cancelled_activity_waiter_cannot_admit_a_second_publication_body(publication):
    store, spec, broker, checked, root, _receipt = publication
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    first = asyncio.create_task(delivery_activities.delivery_publish(request))
    await until(root, 'create-start')
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    for call in (delivery_activities.delivery_publish,
                 delivery_activities.delivery_reconcile_publish):
        result = await asyncio.wait_for(call(request), timeout=.5)
        assert result == {'state': 'pending', 'reason': 'publication_execution_active'}
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects WHERE kind='publish'").fetchone()[0] == 'pending'
    await until(root, 'create-end')
    # The original body can still be acknowledging the exact receipt.
    for _ in range(100):
        result = await delivery_activities.delivery_reconcile_publish(request)
        if result.get('number'):
            break
        await asyncio.sleep(.02)
    assert result['head'] == _git(broker.checkout, 'rev-parse', 'HEAD')
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1


@pytest.mark.asyncio
async def test_lost_waiter_then_proven_pre_mutation_failure_can_restart_original_request(
    publication, monkeypatch,
):
    store, spec, _broker, checked, root, _receipt = publication
    monkeypatch.setenv('PUBLICATION_LIST_FAIL', '1')
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    first = asyncio.create_task(delivery_activities.delivery_publish(request))
    await until(root, 'list-start')
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert await delivery_activities.delivery_publish(request) == {
        'state': 'pending', 'reason': 'publication_execution_active'}
    await until(root, 'list-end')
    for _ in range(100):
        result = await delivery_activities.delivery_reconcile_publish(request)
        if result.get('number'):
            break
        await asyncio.sleep(.02)
    assert result['number'] == 1
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects WHERE kind='publish'").fetchone()[0] == 'complete'


@pytest.mark.asyncio
async def test_child_cli_keeps_publication_lease_when_python_owner_is_killed(publication):
    _store, spec, broker, checked, root, _receipt = publication
    payload = root / 'child-request.json'
    payload.write_text(json.dumps({'spec': spec, 'candidate': checked}))
    script = '''import json,sys
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_broker import DeliveryBroker
r=json.load(open(sys.argv[1]));s=r['spec']
b=DeliveryBroker(DeliveryStore(DeliveryConfig.load(Path(s['config_path']))),s)
b.publish(0,r['candidate'])
'''
    with (root / 'child-stderr').open('w') as errors:
        child = subprocess.Popen([sys.executable, '-B', '-c', script, str(payload)],
                                 stdout=subprocess.DEVNULL, stderr=errors)
    try:
        await until(root, 'create-start', child)
        child.kill()
        child.wait(timeout=5)
        # This is another process and descriptor, not a process-local mutex test.
        assert broker.publish(0, checked) == {
            'state': 'pending', 'reason': 'publication_execution_active'}
        if any(item['kind'] == 'create-start' for item in events(root)):
            await until(root, 'create-end')
        for _ in range(100):
            result = broker.reconcile_publish(0, checked, create_missing=True)
            if result.get('number'):
                break
            await asyncio.sleep(.02)
        assert result['number'] == 1
        assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        await until(root, 'create-end')


def test_released_executor_is_not_proof_that_unknown_create_did_nothing(publication, monkeypatch):
    _store, _spec, broker, checked, root, receipt = publication
    monkeypatch.setenv('PUBLICATION_FAIL', '1')
    with pytest.raises(RuntimeError, match='unconfirmed'):
        broker.publish(0, checked)
    for _ in range(2):
        assert broker.reconcile_publish(0, checked, create_missing=True) == {
            'state': 'pending', 'reason': 'original_create_readback'}
        assert broker.publish(0, checked) == {
            'state': 'pending', 'reason': 'original_create_readback'}
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    receipt[0]['headRefOid'] = _git(broker.checkout, 'rev-parse', 'HEAD')
    (root / 'receipt.json').write_text(json.dumps(receipt))
    assert broker.reconcile_publish(0, checked, create_missing=True)['number'] == 1


def test_actual_missing_executable_can_retry_creation_without_repeating_push(
    publication, monkeypatch,
):
    _store, _spec, broker, checked, root, _receipt = publication
    real = broker._create_owned_pr
    first = True

    def create(base):
        nonlocal first
        if first:
            first = False
            # No executable exists in this owned path. This reaches real Popen,
            # and cannot fall through to the account's installed gh executable.
            with monkeypatch.context() as missing:
                missing.setenv('PATH', str(root / 'no-executables'))
                return real(base)
        return real(base)

    monkeypatch.setattr(broker, '_create_owned_pr', create)
    with pytest.raises(FileNotFoundError):
        broker.publish(0, checked)
    assert events(root) == []
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    assert _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + broker.spec['branch']).split()[0] == head
    assert broker.reconcile_publish(0, checked, create_missing=True)['head'] == head
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1


def test_legacy_pending_without_executor_journal_requires_readback(publication):
    store, _spec, broker, checked, root, _receipt = publication
    broker._effect(broker.publication_effect_key(0), 'publish', {
        'iteration': 0, 'input_candidate_id': checked['id']})
    assert broker.publish(0, checked) == {
        'state': 'pending', 'reason': 'original_commit_readback'}
    assert events(root) == []
    assert _git(broker.checkout, 'rev-parse', 'HEAD') == checked['head']
    with store._connect() as db:
        assert db.execute('SELECT legacy_unknown FROM delivery_publication_executors').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_copied_async_context_does_not_transfer_live_thread_ownership(publication):
    _store, _spec, broker, checked, _root, _receipt = publication
    with execution(broker, 0, checked):
        result = await asyncio.to_thread(broker.publish, 0, checked)
        assert result == {'state': 'pending', 'reason': 'publication_execution_active'}


def test_replaced_lock_inode_cannot_grant_a_second_generation(publication):
    _store, _spec, broker, checked, _root, _receipt = publication
    with execution(broker, 0, checked) as owner:
        owner.path.unlink()
        with pytest.raises((ValueError, FileNotFoundError)):
            owner.check()
        # The retained journal fences even a new independent descriptor/inode.
        # Clear the inherited context to model another process.
        import contextvars
        def replacement():
            with execution(broker, 0, checked):
                pytest.fail('a replaced inode acquired publication authority')
        with pytest.raises(ValueError, match='lock generation'):
            contextvars.Context().run(replacement)
