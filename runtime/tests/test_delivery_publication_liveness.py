"""One original publication, bounded transport readback and frozen workflow compatibility."""
from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
from pathlib import Path

import pytest
from temporalio import activity, workflow
from temporalio.client import WorkflowHistory
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
from test_delivery_store import service as _service_fixture

from devflow_temporal import delivery_broker
from devflow_temporal.delivery_broker import BrokerReadbackUnavailable, DeliveryBroker
from devflow_temporal.delivery_workflow import DeliveryWorkflow


@pytest.fixture
def service(tmp_path):
    return _service_fixture.__wrapped__(tmp_path)


@contextlib.asynccontextmanager
async def marker_replayed_false(name):
    """An execution recorded before ``name`` replays the marker False."""
    real = workflow.patched
    workflow.patched = lambda marker: False if marker == name else real(marker)
    try:
        yield
    finally:
        workflow.patched = real


async def run_publication_probe(*, versioned=True, failure='lost_initial', history_path=None,
                                readback_seconds=30, marker=True):
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
        spec.update(publication_readback_version=1,
                    publication_readback_seconds=readback_seconds)
    # A readback deadline cannot establish that the original body/request ended.
    certain = failure in {'pre_mutation', 'pre_push_exhausted'}
    if failure in {'deadline', 'cancel', 'semantic', 'pre_mutation', 'pre_push_exhausted'}:
        spec.update(resource_cleanup_version=1, terminal_tracker_version=1)
    if failure in {'deadline', 'pre_push_transient', 'pre_push_exhausted'}:
        spec.update(automatic_retry_version=1)

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
        if failure == 'pre_push_exhausted' or (
                failure == 'pre_push_transient' and calls['publish'] <= 5):
            # Confirmed before any remote mutation; outlasts one activity's attempts.
            raise ApplicationError('owned branch PR readback unavailable',
                                   type='PublicationTransient')
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
        if certain:
            assert payload['uncertain'] is False
            return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                    'resource_cleanup': 'confirmed'}
        assert payload['uncertain'] is True
        return {'state': 'unknown', 'process_cleanup': 'unknown', 'resource_cleanup': 'unknown'}

    @activity.defn(name='delivery_terminal_tracker')
    async def terminal_tracker(payload):
        assert payload['release'] is certain
        return {'state': 'consistent'}

    activities = [project, prepare, tracker_start, role, precheck, publish,
                  reconcile, checks, ci, tracker, finalize_resources, terminal_tracker]
    async with (await WorkflowEnvironment.start_time_skipping() as environment,
                marker_replayed_false('delivery-publication-self-heal-v1') if not marker
                else contextlib.nullcontext()):
        async with Worker(environment.client, task_queue='publication-probe',
                          workflows=[DeliveryWorkflow], activities=activities,
                          **({} if marker else
                             {'workflow_runner': UnsandboxedWorkflowRunner()})):
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


BASE146 = sorted((Path(__file__).parent / 'fixtures').glob('publication-base146-*.json'))


@pytest.mark.asyncio
@pytest.mark.parametrize('path', BASE146, ids=lambda path: path.stem)
async def test_histories_recorded_at_the_liveness_base_replay_without_this_marker(path):
    # Recorded by this probe on 9aa75b9 (`delivery-liveness-v2` True, before this
    # change set); the changed workflow replays them on their recorded path.
    from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER

    history = WorkflowHistory.from_json('publication-probe', path.read_text())
    markers = []
    for event in history.events:
        if not event.HasField('marker_recorded_event_attributes'):
            continue
        details = event.marker_recorded_event_attributes.details
        for key in ('patch-data', 'patch_id'):
            if key in details:
                value = (await DELIVERY_DATA_CONVERTER.decode(details[key].payloads))[0]
                markers.append(value['id'] if isinstance(value, dict) else value)
    assert 'delivery-liveness-v2' in markers
    assert 'delivery-publication-self-heal-v1' not in markers
    await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)


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


def test_no_original_effect_cannot_publish_during_reconciliation(service, monkeypatch):
    broker, checked, _published, state = published_broker(service, monkeypatch)
    with broker.store._connect() as db:
        db.execute("DELETE FROM delivery_effects WHERE kind='publish'")
    previous = len(state['calls'])
    with pytest.raises(ValueError, match='publication effect does not match'):
        broker.reconcile_publish(0, checked)
    assert not any('push' in args or 'commit' in args or args[:3] == ['gh', 'pr', 'create']
                   for args in state['calls'][previous:])
