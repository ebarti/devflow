"""Publication converges across transient failures instead of stranding the run."""
from __future__ import annotations

import asyncio
import json
import subprocess
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from temporalio.exceptions import ApplicationError
from test_delivery_gate_retry import unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_broker
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_publication import stopped_at_publication
from devflow_temporal.delivery_workflow import DeliveryWorkflow


class FakeGitHub:
    """gh answers for one owned branch; git commands run for real."""

    def __init__(self, broker, *, list_failures=0, create_failures=0,
                 create_error='HTTP 502 Bad Gateway', ls_remote_failure=None):
        self.broker = broker
        self.list_failures = list_failures
        self.create_failures = create_failures
        self.create_error = create_error
        self.ls_remote_failure = ls_remote_failure
        self.created = False
        self.commands = []
        self.real = delivery_broker._run

    def __call__(self, argv, **kwargs):
        self.commands.append(list(argv))
        if argv[:3] == ['gh', 'pr', 'list']:
            if self.list_failures:
                self.list_failures -= 1
                raise RuntimeError('command failed (1): gh pr: HTTP 502 Bad Gateway')
            if not self.created:
                return '[]'
            return json.dumps([{
                'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                'isDraft': False, 'baseRefName': self.broker._publication_base_ref(),
                'headRefName': self.broker.spec['branch'],
                'headRefOid': _git(self.broker.checkout, 'rev-parse', 'HEAD'),
                'title': 'chore: Change a fixture',
            }])
        if argv[:3] == ['gh', 'pr', 'create']:
            if self.create_failures:
                self.create_failures -= 1
                if 'already exists' in self.create_error:
                    # A lost earlier attempt created it; GitHub refuses a second one.
                    self.created = True
                raise RuntimeError('command failed (1): gh pr: ' + self.create_error)
            self.created = True
            return 'https://example.invalid/pull/1'
        if 'ls-remote' in argv and self.ls_remote_failure is not None:
            failure, self.ls_remote_failure = self.ls_remote_failure, None
            raise failure
        return self.real(argv, **kwargs)

    def mutations(self):
        return [argv for argv in self.commands
                if 'push' in argv or argv[:3] == ['gh', 'pr', 'create']]

    def creations(self):
        return [argv for argv in self.commands if argv[:3] == ['gh', 'pr', 'create']]


def prepared(service, monkeypatch, **github):
    store, request = service
    store.submit(request)
    spec = store.spec('run-1')
    assert spec['publication_readback_version'] == 1
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    _git(broker.checkout, 'config', 'user.name', 'Delivery Test')
    _git(broker.checkout, 'config', 'user.email', 'delivery@example.invalid')
    (broker.checkout / 'README.md').write_text('Owned edit\n')
    gh = FakeGitHub(broker, **github)
    monkeypatch.setattr(delivery_broker, '_run', gh)
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda _spec: (store, DeliveryBroker(store, spec)))
    return store, spec, broker, broker.candidate(), gh


def effect_row(store):
    with store._connect() as db:
        row = db.execute("SELECT state,observed_json FROM delivery_effects "
                         "WHERE kind='publish'").fetchone()
    return None if row is None else (row['state'], json.loads(row['observed_json'] or 'null'))


def remote_head(broker):
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + broker.spec['branch'])
    return remote.split()[0] if remote else None


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['pr_list', 'ls_remote', 'ls_remote_timeout'])
async def test_pre_push_transport_failure_is_retryable_and_the_retry_publishes(
    service, monkeypatch, failure,
):
    github = {'list_failures': 1} if failure == 'pr_list' else {
        'ls_remote_failure': subprocess.TimeoutExpired(['git', 'ls-remote'], 120)
        if failure == 'ls_remote_timeout'
        else RuntimeError('command failed (128): git -c: fatal: unable to access origin'),
    }
    store, spec, broker, checked, gh = prepared(service, monkeypatch, **github)
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    with pytest.raises(ApplicationError) as error:
        await delivery_activities.delivery_publish(request)
    assert error.value.type == 'PublicationTransient'
    assert error.value.non_retryable is False
    assert gh.mutations() == []
    assert remote_head(broker) is None
    # Nothing remote happened, so the next attempt starts from the same state.
    assert effect_row(store) is None
    committed = _git(broker.checkout, 'rev-parse', 'HEAD') != checked['head']
    assert committed is (failure != 'pr_list')
    published = await delivery_activities.delivery_publish(request)
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    assert published['number'] == 1 and published['head'] == head
    assert _git(broker.checkout, 'rev-list', '--count', spec['base_sha'] + '..HEAD') == '1'
    assert remote_head(broker) == head
    assert len(gh.creations()) == 1
    assert effect_row(store)[0] == 'complete'
    assert published['candidate']['content_sha256'] == checked['content_sha256']


@pytest.mark.asyncio
async def test_deterministic_pre_push_refusal_stays_rejected(service, monkeypatch):
    store, spec, broker, checked, gh = prepared(service, monkeypatch)
    real = gh.__call__

    def closed_pr(argv, **kwargs):
        if argv[:3] == ['gh', 'pr', 'list']:
            return json.dumps([{
                'number': 4, 'url': 'https://example.invalid/pull/4', 'state': 'CLOSED',
                'isDraft': False, 'baseRefName': broker._publication_base_ref(),
                'headRefName': spec['branch'], 'headRefOid': checked['head'],
                'title': 'chore: Change a fixture',
            }])
        return real(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', closed_pr)
    with pytest.raises(ApplicationError) as error:
        await delivery_activities.delivery_publish(
            {'spec': spec, 'iteration': 0, 'candidate': checked})
    assert error.value.type == 'PublicationRejected'
    assert error.value.non_retryable is True
    assert 'not the authorized open regular PR' in str(error.value)
    assert remote_head(broker) is None
    assert effect_row(store) == ('pending', None)


@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['create_failed', 'already_exists'])
async def test_reconcile_creates_the_missing_pr_for_the_pushed_head(
    service, monkeypatch, failure,
):
    store, spec, broker, checked, gh = prepared(service, monkeypatch, create_failures=1)
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    with pytest.raises(RuntimeError, match='HTTP 502'):
        await delivery_activities.delivery_publish(request)
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    assert remote_head(broker) == head
    state, observed = effect_row(store)
    assert state == 'pending'
    assert observed == {'head': head, 'number': None, 'remote_confirmed': True}
    if failure == 'already_exists':
        gh.create_failures, gh.create_error = 1, 'a pull request for this branch already exists'
    pushes = len(gh.mutations())
    published = await delivery_activities.delivery_reconcile_publish(
        {**request, 'expected_head': None, 'expected_pr_number': None})
    assert published['number'] == 1 and published['head'] == head
    assert effect_row(store)[0] == 'complete'
    creation = gh.creations()[-1]
    assert creation[creation.index('--head') + 1] == spec['branch']
    assert creation[creation.index('--base') + 1] == broker._publication_base_ref()
    assert len(gh.creations()) == 2
    # The reconciliation created the PR and nothing else.
    assert len(gh.mutations()) == pushes + 1
    assert _git(broker.checkout, 'rev-parse', 'HEAD') == head


def test_admission_readback_never_creates_the_missing_pr(service, monkeypatch):
    store, spec, broker, checked, gh = prepared(service, monkeypatch, create_failures=1)
    with pytest.raises(RuntimeError, match='HTTP 502'):
        asyncio.run(delivery_activities.delivery_publish(
            {'spec': spec, 'iteration': 0, 'candidate': checked}))
    before = len(gh.commands)
    observed = broker.reconcile_publish(0, checked, complete=False)
    assert observed == {'state': 'pending', 'reason': 'pr_head_readback',
                        'head': _git(broker.checkout, 'rev-parse', 'HEAD')}
    assert not any(argv[:3] == ['gh', 'pr', 'create'] for argv in gh.commands[before:])
    assert effect_row(store)[0] == 'pending'


def test_pr_creation_refused_as_existing_reads_back_inside_publish(service, monkeypatch):
    store, spec, broker, checked, gh = prepared(
        service, monkeypatch, create_failures=1,
        create_error='a pull request for this branch already exists')
    published = asyncio.run(delivery_activities.delivery_publish(
        {'spec': spec, 'iteration': 0, 'candidate': checked}))
    assert published['number'] == 1
    assert len(gh.creations()) == 1
    assert effect_row(store)[0] == 'complete'


def test_discarding_is_limited_to_a_confirmed_unused_effect(service, monkeypatch):
    store, spec, broker, checked, _gh = prepared(service, monkeypatch)
    broker._effect('publish:run-1:0', 'publish', {'iteration': 0,
                                                  'input_candidate_id': checked['id']})
    later = DeliveryBroker(store, spec)
    later.publication_may_have_effect = False
    with pytest.raises(ValueError, match='cannot be discarded'):
        later.discard_unused_publication(0)
    assert effect_row(store) == ('pending', None)


@pytest.mark.parametrize('state,expected', [
    ({'error': 'publication unresolved: ActivityError'}, True),
    ({'error': 'publication unresolved: ApplicationError'}, True),
    ({'error': 'publication unresolved: readback deadline exhausted'}, True),
    ({'error': 'repair limit exhausted',
      'checks': {'failure': {'cause_type': 'publication_deadline'}}}, True),
    ({'error': 'repair limit exhausted', 'checks': {'failure': {'cause_type': 'ci_deadline'}}},
     False),
    ({'error': 'publication retry failed: ValueError'}, False),
    ({}, False),
])
def test_stopped_at_publication_keys_on_classification_or_historical_reasons(state, expected):
    assert stopped_at_publication(state) is expected


NEW_REASON = 'publication unresolved: readback deadline exhausted'


@pytest.fixture
def pending_v1(service, monkeypatch):
    """A current admission stopped after its push with no PR, effect bound to the head."""
    store, broker, original, _gate_request = unpublished.__wrapped__(service, monkeypatch)
    spec = store.effective_spec('run-1')
    assert spec['publication_readback_version'] == 1
    broker = DeliveryBroker(store, spec)
    checked = broker.candidate()
    broker._effect('publish:run-1:0', 'publish', {
        'iteration': 0, 'input_candidate_id': original['candidate']['id'],
    })
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-sqm', 'docs: document the owned feature')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    pushed = _git(broker.checkout, 'rev-parse', 'HEAD')
    broker._bind_pending_publication('publish:run-1:0', pushed, None, remote_confirmed=True)
    state = deepcopy(original)
    state.update(error=NEW_REASON, revision=13, candidate=checked)
    state['checks']['prepublish'].update(state='passed', results=[
        {'id': 'packaging', 'passed': True, 'cleanup': 'confirmed', 'exit_code': 0}])
    state['checks']['failure'] = {
        'classification': 'transient', 'cause_type': 'publication_deadline',
        'stage': 'publishing_pending', 'reason': NEW_REASON,
    }

    def stop(error):
        state['error'] = error
        if error != NEW_REASON:
            state['checks'].pop('failure', None)
        store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                      message=error, candidate=checked, pull_request=None,
                      checks=state['checks'], iteration=0, protocol_revision=13,
                      outcome='blocked', cleanup='confirmed', error=error)

    stop(NEW_REASON)
    with store._connect() as db:
        store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
    closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': 'closed-original',
              'request_digest': spec['request_digest'], 'recovery_digest': None,
              'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: closed)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _: {})
    request = {'command_id': 'retry-publication-1', 'expected_revision': 13,
               'expected_candidate_id': checked['id'], 'expected_head': pushed,
               'expected_pr_number': 0}
    return store, broker, state, request, stop


@pytest.mark.parametrize('error', [
    NEW_REASON,
    'publication unresolved: ApplicationError',   # readback deadline before classification
    'publication unresolved: ActivityError',      # historical stop reason
])
def test_current_admission_pending_publication_is_admitted_by_effect_identity(pending_v1,
                                                                             error):
    store, broker, state, request, stop = pending_v1
    stop(error)
    admitted = store.recover_publication('run-1', request)
    assert admitted['phase'] == 'publication_recovery_queued'
    assert admitted['implementation_authority'] is False
    assert store.recover_publication('run-1', request) == admitted
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
    assert recovery['seal']['candidate']['head'] == request['expected_head']
    assert recovery['state']['error'] == error


@pytest.mark.parametrize('drift', ['other_stop', 'bound_head', 'bound_number'])
def test_current_admission_pending_publication_keeps_identity_refusals(pending_v1, drift):
    store, broker, state, request, stop = pending_v1
    if drift == 'other_stop':
        stop('repair limit exhausted')
    else:
        with store._connect() as db:
            observed = {'head': 'a' * 40 if drift == 'bound_head' else request['expected_head'],
                        'number': 9 if drift == 'bound_number' else None,
                        'remote_confirmed': True}
            db.execute("UPDATE delivery_effects SET observed_json=? WHERE kind='publish'",
                       (json.dumps(observed),))
    with pytest.raises(ValueError):
        store.recover_publication('run-1', request)


def test_current_admission_resume_completes_the_effect_without_publishing_again(
    pending_v1, monkeypatch,
):
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now',
                        lambda: datetime.now(UTC))
    store, broker, state, request, _stop = pending_v1
    store.recover_publication('run-1', request)
    spec = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute(
            "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
    flow = DeliveryWorkflow()
    flow.publication_v1 = True
    calls = []
    publication = {'number': 7, 'url': 'https://example.invalid/pull/7', 'state': 'OPEN',
                   'head': request['expected_head'], 'base': spec['base_sha'],
                   'candidate': broker.candidate()}

    async def execute(name, body, **_kw):
        calls.append((name, body.get('role')))
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_reconcile_publish':
            assert body['expected_head'] == request['expected_head']
            assert body['candidate'] == state['candidate']
            return publication
        if name == 'delivery_role':
            return {'status': 'pass', 'role': body['role'], 'candidate': broker.candidate(),
                    'session_id': 'independent-' + body['role']}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*_args):
        pass

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_pending_publication(spec, recovery))
    assert result['outcome'] == 'delivered'
    assert result['pull_request'] == publication
    assert ('delivery_publish', None) not in calls
    assert ('delivery_role', 'implement') not in calls
    assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']
    assert calls.index(('delivery_reconcile_publish', None)) < \
        calls.index(('delivery_role', 'review'))
    assert digest(result['candidate']) == digest(publication['candidate'])
