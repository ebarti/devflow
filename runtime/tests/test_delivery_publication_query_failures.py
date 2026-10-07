"""Read-only command failures cannot establish remote publication authority."""
from __future__ import annotations

import json
import sys

import pytest
from temporalio.exceptions import ApplicationError
from test_delivery_publication_liveness import published_broker
from test_delivery_publication_liveness import service as service

from devflow_temporal import delivery_activities, delivery_broker
from devflow_temporal.delivery_broker import BrokerReadbackUnavailable, DeliveryBroker
from devflow_temporal.delivery_repair import RepairReadbackPending, published_identity

MESSAGES = [
    'ssh: connect to host github.com port 22: Operation timed out',
    "Failed to connect to github.com port 443: Couldn't connect to server",
    'HTTP 500: Internal Server Error',
    'error connecting to api.github.com',
    'GraphQL: Something went wrong; this may be the result of a timeout',
    'HTTP 403: Resource not accessible',
    'unknown query error with agent-controlled diagnostic text',
]


def failing_process(message, real_run):
    return real_run([sys.executable, '-c',
                     'import sys; sys.stderr.write(sys.argv[1]); sys.exit(1)', message])


@pytest.mark.parametrize('query', ['branch', 'pr'])
@pytest.mark.parametrize('message', MESSAGES)
def test_failed_read_only_process_is_unavailable_not_authority_conflict(
    service, monkeypatch, query, message,
):
    broker, checked, _published, state = published_broker(service, monkeypatch)
    real_run = delivery_broker._run

    def run(argv, **kwargs):
        if ('ls-remote' in argv if query == 'branch' else argv[:3] == ['gh', 'pr', 'list']):
            return failing_process(message, real_run)
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', run)
    previous = len(state['calls'])
    with pytest.raises(BrokerReadbackUnavailable):
        broker.reconcile_publish(0, checked)
    assert not any('push' in argv or 'commit' in argv or argv[:3] == ['gh', 'pr', 'create']
                   for argv in state['calls'][previous:])


def test_real_owned_pr_query_remains_pending_in_repair_preflight(service, monkeypatch):
    broker, _checked, published, _state = published_broker(service, monkeypatch)
    real_run = delivery_broker._run

    def run(argv, **kwargs):
        if argv[:3] == ['gh', 'pr', 'list']:
            return failing_process('HTTP 500: Internal Server Error', real_run)
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', run)
    with pytest.raises(RepairReadbackPending):
        published_identity(broker, broker.candidate(), published)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['candidate', 'committed_candidate', 'permission'])
async def test_activity_confirms_pre_mutation_rejection_without_unknown_effect(
    service, monkeypatch, failure,
):
    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec('run-1'))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    if failure in {'candidate', 'committed_candidate'}:
        (broker.checkout / 'outside.md').write_text('Outside the frozen edit scope\n')
        if failure == 'committed_candidate':
            delivery_broker._git(broker.checkout, 'add', 'outside.md')
            delivery_broker._git(broker.checkout, 'commit', '--signoff',
                                 '-m', 'docs: add an out-of-scope fixture')
    else:
        (broker.checkout / 'README.md').write_text('Owned edit\n')
    checked = broker.candidate()
    commands = []
    real_run = delivery_broker._run

    def run(argv, **kwargs):
        commands.append(argv)
        if failure == 'permission' and 'ls-remote' in argv:
            raise PermissionError('readback is denied before any remote mutation')
        if argv[:3] == ['gh', 'pr', 'list']:
            return '[]'
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', run)
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (store, broker))
    with pytest.raises(ApplicationError) as error:
        await delivery_activities.delivery_publish({
            'spec': broker.spec, 'iteration': 0, 'candidate': checked,
        })
    assert error.value.type == 'PublicationRejected'
    assert error.value.non_retryable
    assert broker.publication_may_have_effect is False
    assert not any('push' in argv or argv[:3] == ['gh', 'pr', 'create'] for argv in commands)
    with store._connect() as db:
        row = db.execute('SELECT state,observed_json FROM delivery_effects WHERE kind=?',
                         ('publish',)).fetchone()
        if failure in {'candidate', 'committed_candidate'}:
            assert 'outside allowed paths: outside.md' in str(error.value)
            assert row is None
        else:
            assert row['state'] == 'pending'
            observed = json.loads(row['observed_json'])
            assert observed['head'] == delivery_broker._git(broker.checkout, 'rev-parse', 'HEAD')
            assert observed['remote_confirmed'] is False


@pytest.mark.asyncio
async def test_failed_mutation_process_preserves_uncertain_original_effect(service, monkeypatch):
    store, request = service
    store.submit(request)
    broker = DeliveryBroker(store, store.spec('run-1'))
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    (broker.checkout / 'README.md').write_text('Owned edit\n')
    checked = broker.candidate()
    commands = []
    real_run = delivery_broker._run

    def run(argv, **kwargs):
        commands.append(argv)
        if argv[:3] == ['gh', 'pr', 'create']:
            return failing_process('HTTP 403: Resource not accessible', real_run)
        if argv[:3] == ['gh', 'pr', 'list']:
            return '[]'
        return real_run(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', run)
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (store, broker))
    with pytest.raises(RuntimeError, match='HTTP 403') as error:
        await delivery_activities.delivery_publish({
            'spec': broker.spec, 'iteration': 0, 'candidate': checked,
        })
    assert not isinstance(error.value, ApplicationError)
    previous = len(commands)
    assert broker.reconcile_publish(0, checked)['state'] == 'pending'
    assert not any('push' in argv or argv[:3] == ['gh', 'pr', 'create']
                   for argv in commands[previous:])
    with store._connect() as db:
        row = db.execute('SELECT state,observed_json FROM delivery_effects WHERE kind=?',
                         ('publish',)).fetchone()
    assert row['state'] == 'pending'
    assert json.loads(row['observed_json'])['remote_confirmed'] is True


@pytest.mark.asyncio
async def test_prior_lost_effect_cannot_be_reclassified_as_pre_mutation_rejection(
    service, monkeypatch,
):
    broker, checked, _published, state = published_broker(service, monkeypatch,
                                                        lost_completion=True)
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (broker.store, broker))
    previous = len(state['calls'])
    with pytest.raises(BrokerReadbackUnavailable):
        await delivery_activities.delivery_publish({
            'spec': broker.spec, 'iteration': 0, 'candidate': checked,
        })
    assert broker.publication_may_have_effect is True
    assert not any('push' in argv or argv[:3] == ['gh', 'pr', 'create']
                   for argv in state['calls'][previous:])


@pytest.mark.asyncio
async def test_scope_rejection_preserves_prior_uncertain_publication(service, monkeypatch):
    broker, checked, _published, state = published_broker(service, monkeypatch,
                                                        lost_completion=True)
    with broker.store._connect() as db:
        original = dict(db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                   ("publish:run-1:0",)).fetchone())
    (broker.checkout / 'outside.md').write_text('New out-of-scope edit after a lost effect\n')
    monkeypatch.setattr(delivery_activities, '_context', lambda _spec: (broker.store, broker))
    previous = len(state['calls'])
    with pytest.raises(ValueError, match='outside allowed paths: outside.md') as error:
        await delivery_activities.delivery_publish({
            'spec': broker.spec, 'iteration': 0, 'candidate': checked,
        })
    assert not isinstance(error.value, ApplicationError)
    assert broker.publication_may_have_effect is True
    assert not any('push' in argv or 'commit' in argv or argv[:3] == ['gh', 'pr', 'create']
                   for argv in state['calls'][previous:])
    with broker.store._connect() as db:
        retained = dict(db.execute("SELECT * FROM delivery_effects WHERE effect_key=?",
                                   ("publish:run-1:0",)).fetchone())
    assert retained == original and retained['state'] == 'pending'
    assert json.loads(retained['observed_json'])['remote_confirmed'] is True
