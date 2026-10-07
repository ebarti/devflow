"""One original publication, bounded transport readback and frozen workflow compatibility."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from temporalio import activity
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker
from test_delivery_store import service as _service_fixture

from devflow_temporal import delivery_broker, delivery_config
from devflow_temporal.delivery_broker import BrokerReadbackUnavailable, DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def service(tmp_path):
    return _service_fixture.__wrapped__(tmp_path)


async def run_publication_probe(*, versioned=True, failure='lost_initial', history_path=None):
    calls = {'publish': 0, 'reconcile': 0, 'implement': 0}
    events = []
    pending = asyncio.Event()
    before = {'id': 'before', 'head': 'base'}
    checked = {'id': 'checked', 'head': 'base'}
    final = {'id': 'published', 'head': 'original-head'}
    publication = {'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                   'head': 'original-head', 'candidate': final}
    spec = {'run_id': 'publication-probe', 'provider': 'fake',
            'policy': {'max_repairs': 0, 'browser_qa': None}}
    if versioned:
        spec.update(publication_readback_version=1, publication_readback_seconds=30)
    if failure in {'deadline', 'cancel', 'semantic', 'pre_mutation'}:
        spec.update(resource_cleanup_version=1, terminal_tracker_version=1)

    @activity.defn(name='delivery_project')
    async def project(payload):
        events.append(payload['event_type'])
        if payload['event_type'] == 'publication_pending':
            pending.set()
        return {'revision': len(events)}

    @activity.defn(name='delivery_prepare')
    async def prepare(_payload):
        return {'candidate': before}

    @activity.defn(name='delivery_tracker_start')
    async def tracker_start(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_role')
    async def role(payload):
        if payload['role'] == 'implement':
            calls['implement'] += 1
        return {'status': 'pass', 'session_id': payload['role'], 'cleanup': 'confirmed',
                'candidate': checked if payload['role'] == 'implement' else final}

    @activity.defn(name='delivery_precheck')
    async def precheck(_payload):
        return {'state': 'passed'}

    @activity.defn(name='delivery_publish')
    async def publish(_payload):
        calls['publish'] += 1
        if failure == 'pre_mutation':
            raise ApplicationError('candidate rejected before remote mutation',
                                   type='PublicationRejected', non_retryable=True)
        if failure == 'failed_mutation':
            raise RuntimeError('mutation command failed; its remote outcome is unconfirmed')
        if failure == 'lost_initial':
            # The original remote effect already exists; only its activity completion is lost.
            raise subprocess.TimeoutExpired(['gh', 'pr', 'create'], 90)
        return {'state': 'pending', 'reason': 'pr_head_readback', 'head': 'original-head'}

    @activity.defn(name='delivery_reconcile_publish')
    async def reconcile(payload):
        calls['reconcile'] += 1
        assert payload['candidate'] == checked
        if failure == 'pre_mutation':
            raise ValueError('original publication head has not been observed')
        if failure == 'semantic':
            raise ValueError('publication resolved to a different PR')
        if failure in {'deadline', 'cancel'}:
            raise subprocess.TimeoutExpired(['gh', 'pr', 'list'], 60)
        if failure == 'read_timeout' and calls['reconcile'] == 1:
            raise subprocess.TimeoutExpired(['gh', 'pr', 'list'], 60)
        return publication

    @activity.defn(name='delivery_checks')
    async def checks(_payload):
        return {'state': 'passed'}

    @activity.defn(name='delivery_ci')
    async def ci(_payload):
        return {'state': 'passed'}

    @activity.defn(name='delivery_tracker')
    async def tracker(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_finalize_resources')
    async def finalize_resources(payload):
        if failure == 'pre_mutation':
            assert payload['uncertain'] is False
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        assert payload['uncertain'] is True
        return {'state': 'unknown', 'process_cleanup': 'unknown', 'resource_cleanup': 'unknown'}

    @activity.defn(name='delivery_terminal_tracker')
    async def terminal_tracker(payload):
        assert payload['release'] is (failure == 'pre_mutation')
        return {'state': 'consistent'}

    activities = [project, prepare, tracker_start, role, precheck, publish,
                  reconcile, checks, ci, tracker, finalize_resources, terminal_tracker]
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(environment.client, task_queue='publication-probe',
                          workflows=[DeliveryWorkflow], activities=activities):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id='publication-probe', task_queue='publication-probe')
            if failure == 'cancel':
                with environment.auto_time_skipping_disabled():
                    await asyncio.wait_for(pending.wait(), timeout=10)
                    status = await handle.query('status')
                    await handle.execute_update('cancel', {
                        'expected_revision': status['revision'], 'reason': 'cancel readback',
                    })
            result = await handle.result()
            history = await handle.fetch_history()
            if history_path:
                Path(history_path).write_text(history.to_json())
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    return result, calls, events


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['lost_initial', 'read_timeout', 'failed_mutation'])
async def test_publication_transport_failure_reconciles_original_inside_run(failure):
    result, calls, events = await run_publication_probe(failure=failure)
    assert result['outcome'] == 'delivered'
    assert calls['publish'] == calls['implement'] == 1
    assert calls['reconcile'] == (2 if failure == 'read_timeout' else 1)
    assert result['pull_request']['head'] == 'original-head'
    assert 'published' in events


@pytest.mark.asyncio
async def test_legacy_publication_history_keeps_original_commands():
    path = os.environ.get('PUBLICATION_C04_HISTORY_OUTPUT')
    result, calls, _events = await run_publication_probe(versioned=False,
                                                       failure='read_timeout', history_path=path)
    assert result['outcome'] == 'blocked'
    assert calls == {'publish': 1, 'reconcile': 1, 'implement': 1}


@pytest.mark.asyncio
@pytest.mark.parametrize('failure,outcome', [('deadline', 'blocked'), ('cancel', 'cancelled'),
                                             ('semantic', 'blocked')])
async def test_publication_wait_is_bounded_cancellable_and_semantic_errors_terminal(
    failure, outcome,
):
    result, calls, _events = await run_publication_probe(failure=failure)
    assert result['outcome'] == outcome
    assert result['cleanup'] == 'unknown'
    assert result['checks']['resource_cleanup']['state'] == 'unknown'
    assert calls['publish'] == calls['implement'] == 1
    assert calls['reconcile'] <= 3
    if failure == 'semantic':
        assert calls['reconcile'] == 1


@pytest.mark.asyncio
async def test_untouched_c04_publication_history_replays_with_changed_workflow():
    # Captured by the unmodified c04f00e workflow before changing its publication commands.
    path = Path(__file__).parent / 'fixtures' / 'publication-readback-c04.json'
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(
        WorkflowHistory.from_json('publication-probe', path.read_text()))


def published_broker(
    service, monkeypatch, *, lost_completion=False, precommitted=False, inflight_push=False,
):
    store, request = service
    store.submit(request)
    spec = store.spec(request['run_id'])
    spec.update(publication_readback_version=1, publication_readback_seconds=30)
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    delivery_broker._git(broker.checkout, 'config', 'user.name', 'Delivery Test')
    delivery_broker._git(broker.checkout, 'config', 'user.email', 'delivery@example.invalid')
    (broker.checkout / 'README.md').write_text('Owned edit\n')
    if precommitted:
        delivery_broker._git(broker.checkout, 'add', 'README.md')
        delivery_broker._git(broker.checkout, 'commit', '--signoff', '-m', 'chore: Checked edit')
    checked = broker.candidate()
    state = {'created': False, 'unavailable': False, 'calls': []}
    real_run = delivery_broker._run

    def run(argv, **kwargs):
        state['calls'].append(list(argv))
        if inflight_push and 'push' in argv:
            subprocess.run([sys.executable, '-c', 'import time; time.sleep(2)'], timeout=.02)
        if argv[:3] == ['gh', 'pr', 'create']:
            state['created'] = True
            if lost_completion:
                state['unavailable'] = True
            return 'https://example.invalid/pull/1'
        if argv[:3] == ['gh', 'pr', 'list']:
            if state['unavailable']:
                # An actual subprocess exceeds its deadline, not a mocked result.
                subprocess.run([sys.executable, '-c', 'import time; time.sleep(2)'], timeout=.02)
            if not state['created']:
                return '[]'
            return json.dumps([{
                'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                'isDraft': False, 'baseRefName': broker._publication_base_ref(),
                'headRefName': spec['branch'],
                'headRefOid': delivery_broker._git(broker.checkout, 'rev-parse', 'HEAD'),
                'title': 'chore: Change a fixture',
            }])
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', run)
    if lost_completion or inflight_push:
        error_type = subprocess.TimeoutExpired if inflight_push else BrokerReadbackUnavailable
        with pytest.raises(error_type):
            broker.publish(0, checked)
        published = None
    else:
        published = broker.publish(0, checked)
    return broker, checked, published, state


def test_slow_pr_readback_keeps_original_effect_and_mutations(service, monkeypatch):
    broker, checked, published, state = published_broker(service, monkeypatch)
    mutations = [args for args in state['calls'] if 'push' in args or 'commit' in args
                 or args[:3] == ['gh', 'pr', 'create']]
    state['unavailable'] = True
    with pytest.raises(BrokerReadbackUnavailable) as error:
        broker.reconcile_publish(0, checked)
    assert isinstance(error.value.__cause__, subprocess.TimeoutExpired)
    state['unavailable'] = False
    assert broker.reconcile_publish(0, checked) == published
    assert mutations == [args for args in state['calls'] if 'push' in args or 'commit' in args
                         or args[:3] == ['gh', 'pr', 'create']]
    with broker.store._connect() as db:
        saved = db.execute("SELECT state,observed_json FROM delivery_effects "
                           "WHERE effect_key='publish:run-1:0'").fetchone()
        assert saved['state'] == 'complete'
        assert json.loads(saved['observed_json']) == published
        assert broker.store.state.claim_for(db, broker.spec['work_id'])['owner'] == \
            'external:devflow:run-1'


def test_failed_permission_query_does_not_prove_remote_authority(service, monkeypatch):
    broker, checked, _published, _state = published_broker(service, monkeypatch)
    real_run = delivery_broker._run

    def forbidden(argv, **kwargs):
        if argv[:3] == ['gh', 'pr', 'list']:
            raise RuntimeError('command failed (1): gh pr: HTTP 403: Resource not accessible')
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', forbidden)
    with pytest.raises(BrokerReadbackUnavailable):
        broker.reconcile_publish(0, checked)


@pytest.mark.parametrize('drift', ['head', 'pr', 'base', 'draft', 'multiple'])
def test_original_publication_identity_drift_is_terminal(service, monkeypatch, drift):
    broker, checked, published, _state = published_broker(service, monkeypatch)
    real_run = delivery_broker._run

    def changed(argv, **kwargs):
        raw = real_run(argv, **kwargs)
        if argv[:3] != ['gh', 'pr', 'list']:
            return raw
        prs = json.loads(raw)
        if drift == 'head':
            prs[0]['headRefOid'] = 'a' * 40
        elif drift == 'pr':
            prs[0]['number'] += 1
        elif drift == 'base':
            prs[0]['baseRefName'] = 'unowned'
        elif drift == 'draft':
            prs[0]['isDraft'] = True
        else:
            prs.append(prs[0])
        return json.dumps(prs)

    monkeypatch.setattr(delivery_broker, '_run', changed)
    with pytest.raises((ValueError, RuntimeError)):
        broker.reconcile_publish(0, checked, expected_head=published['head'],
                                 expected_pr_number=published['number'])


@pytest.mark.parametrize('value', [True, 29, 3601, '900'])
def test_publication_deadline_admission_is_bounded(service, value):
    store, request = service
    store.config.raw['publication_readback_seconds'] = value
    with pytest.raises(ValueError, match='publication_readback_seconds'):
        store.config.admit(request)


@pytest.mark.parametrize('versioned', [False, True])
def test_scope_amendment_preserves_original_publication_fields(service, monkeypatch, versioned):
    store, request = service
    store.submit(request)
    original = store.spec(request['run_id'])
    original.pop('publication_readback_version')
    original.pop('publication_readback_seconds')
    if versioned:
        original.update(publication_readback_version=1, publication_readback_seconds=77)
    monkeypatch.setattr(delivery_config, 'scope_amendment_config', lambda *args: store.config)
    monkeypatch.setattr('devflow_temporal.delivery_preparation.require_native_execution',
                        lambda spec: None)
    effective = delivery_config.scope_amended_spec(original, store.config.path, 'unused', ['extra'])
    for key in ('publication_readback_version', 'publication_readback_seconds'):
        assert (key in effective) == (key in original)
        assert effective.get(key) == original.get(key)


def test_lost_completion_reconciles_pending_original_without_republication(service, monkeypatch):
    broker, checked, _published, state = published_broker(
        service, monkeypatch, lost_completion=True
    )
    with broker.store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects "
                          "WHERE effect_key='publish:run-1:0'").fetchone()[0] == 'pending'
    mutations = [args for args in state['calls'] if 'push' in args or 'commit' in args
                 or args[:3] == ['gh', 'pr', 'create']]
    state['unavailable'] = False
    published = broker.reconcile_publish(0, checked)
    assert published['number'] == 1
    assert published['head'] == delivery_broker._git(broker.checkout, 'rev-parse', 'HEAD')
    assert mutations == [args for args in state['calls'] if 'push' in args or 'commit' in args
                         or args[:3] == ['gh', 'pr', 'create']]


def test_no_original_effect_cannot_publish_during_reconciliation(service, monkeypatch):
    broker, checked, _published, state = published_broker(service, monkeypatch)
    with broker.store._connect() as db:
        db.execute("DELETE FROM delivery_effects WHERE kind='publish'")
    previous = len(state['calls'])
    with pytest.raises(ValueError, match='publication effect does not match'):
        broker.reconcile_publish(0, checked)
    assert not any('push' in args or 'commit' in args or args[:3] == ['gh', 'pr', 'create']
                   for args in state['calls'][previous:])


@pytest.mark.parametrize('failure', [subprocess.TimeoutExpired(['git', 'ls-remote'], 120),
                                     RuntimeError('HTTP 503: unavailable')])
def test_known_remote_transport_failure_is_retryable_without_mutation(
    service, monkeypatch, failure,
):
    broker, checked, _published, state = published_broker(service, monkeypatch)
    real_run = delivery_broker._run

    def unavailable(argv, **kwargs):
        if 'ls-remote' in argv:
            raise failure
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', unavailable)
    previous = len(state['calls'])
    with pytest.raises(BrokerReadbackUnavailable):
        broker.reconcile_publish(0, checked)
    assert not any('push' in args or 'commit' in args or args[:3] == ['gh', 'pr', 'create']
                   for args in state['calls'][previous:])


def test_lost_completion_freezes_head_before_remote_mutation(service, monkeypatch):
    broker, checked, _published, state = published_broker(
        service, monkeypatch, lost_completion=True
    )
    with broker.store._connect() as db:
        original = json.loads(db.execute("SELECT observed_json FROM delivery_effects "
                                        "WHERE effect_key='publish:run-1:0'").fetchone()[0])
    delivery_broker._git(broker.checkout, 'commit', '--amend', '--signoff', '-m', 'chore: Rewrite')
    assert delivery_broker._git(broker.checkout, 'rev-parse', 'HEAD') != original['head']
    state['unavailable'] = False
    with pytest.raises(ValueError, match='published checkout head changed'):
        broker.reconcile_publish(0, checked)


def test_inflight_original_push_waits_without_repeating_mutation(service, monkeypatch):
    broker, checked, _published, state = published_broker(
        service, monkeypatch, inflight_push=True
    )
    previous = len(state['calls'])
    result = broker.reconcile_publish(0, checked)
    assert result == {'state': 'pending', 'reason': 'original_push_readback',
                      'head': delivery_broker._git(broker.checkout, 'rev-parse', 'HEAD')}
    assert not any('push' in args or 'commit' in args or args[:3] == ['gh', 'pr', 'create']
                   for args in state['calls'][previous:])


def test_precommitted_original_candidate_readback_completes(service, monkeypatch):
    broker, checked, _published, state = published_broker(
        service, monkeypatch, lost_completion=True, precommitted=True
    )
    state['unavailable'] = False
    published = broker.reconcile_publish(0, checked)
    assert published['state'] == 'OPEN'
    assert published['head'] == checked['head']


def test_original_push_already_accepted_then_remote_rewind_is_terminal(service, monkeypatch):
    broker, checked, _published, state = published_broker(
        service, monkeypatch, lost_completion=True
    )
    real_run = delivery_broker._run

    def rewound(argv, **kwargs):
        if 'ls-remote' in argv:
            return checked['head'] + '\trefs/heads/' + broker.spec['branch']
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', rewound)
    state['unavailable'] = False
    with pytest.raises(ValueError, match='remote feature branch differs'):
        broker.reconcile_publish(0, checked)


def test_original_completion_can_arrive_during_readonly_reconciliation(service, monkeypatch):
    broker, checked, published, _state = published_broker(service, monkeypatch)
    with broker.store._connect() as db:
        db.execute("UPDATE delivery_effects SET state='pending',observed_json=? "
                   "WHERE effect_key='publish:run-1:0'",
                   (json.dumps({'head': published['head'], 'number': published['number'],
                                'remote_confirmed': True}),))
    real_run = delivery_broker._run

    def acknowledge_original(argv, **kwargs):
        result = real_run(argv, **kwargs)
        if 'ls-remote' in argv:
            broker._finish_effect('publish:run-1:0', published)
        return result

    monkeypatch.setattr(delivery_broker, '_run', acknowledge_original)
    assert broker.reconcile_publish(0, checked) == published


@pytest.mark.asyncio
async def test_confirmed_pre_mutation_rejection_stops_without_readback_or_retaining_claim():
    result, calls, events = await run_publication_probe(failure='pre_mutation')
    assert result['outcome'] == 'blocked'
    assert result['checks']['resource_cleanup']['state'] == 'confirmed'
    assert calls['publish'] == 1
    assert calls['reconcile'] == 0
    assert 'publication_pending' not in events
