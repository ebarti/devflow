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
@pytest.mark.parametrize('failure', ['pr_list', 'ls_remote_timeout'])
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


@pytest.mark.parametrize(
    "state,expected", [({"error": "publication unresolved: ActivityError"}, True), ({}, False)]
)
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


@pytest.mark.parametrize('error', [NEW_REASON, 'publication unresolved: ActivityError'])
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


@pytest.mark.parametrize('drift', ['other_stop', 'bound_number'])
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


def test_discard_leaves_a_row_adopted_by_a_later_attempt(service, monkeypatch):
    store, spec, _broker, checked, gh = prepared(service, monkeypatch)
    key = 'publish:run-1:0'
    timed_out = DeliveryBroker(store, spec)
    timed_out._effect(key, 'publish', {'iteration': 0, 'input_candidate_id': checked['id']})
    timed_out.publication_may_have_effect = False
    adopted = DeliveryBroker(store, spec)
    gh.ls_remote_failure = RuntimeError('command failed (128): git -c: fatal: unable to access')
    with pytest.raises(delivery_broker.BrokerReadbackUnavailable):
        adopted.publish(0, checked)
    assert adopted.publication_may_have_effect is True
    state, observed = effect_row(store)
    assert state == 'pending' and observed['remote_confirmed'] is False
    # The timed-out thread reports its pre-mutation failure after the adoption.
    timed_out.discard_unused_publication(0)
    assert effect_row(store) == (state, observed)
    assert DeliveryBroker(store, spec).publish(0, checked)['number'] == 1


def test_reconcile_adopts_the_pending_effect_it_depends_on(service, monkeypatch):
    store, spec, _broker, checked, _gh = prepared(service, monkeypatch)
    inserter = DeliveryBroker(store, spec)
    inserter._effect('publish:run-1:0', 'publish', {'iteration': 0,
                                                    'input_candidate_id': checked['id']})
    _git(inserter.checkout, 'add', 'README.md')
    _git(inserter.checkout, 'commit', '--signoff', '-m', 'chore: Checked edit')
    head = _git(inserter.checkout, 'rev-parse', 'HEAD')
    inserter._bind_pending_publication('publish:run-1:0', head, None)
    # Its push never happened and the attempt timed out; the thread lives on.
    inserter.publication_may_have_effect = False
    reader = DeliveryBroker(store, spec)
    assert reader.reconcile_publish(0, checked) == {
        'state': 'pending', 'reason': 'original_push_readback', 'head': head}
    inserter.discard_unused_publication(0)
    state, observed = effect_row(store)
    assert state == 'pending' and observed['head'] == head
    assert DeliveryBroker(store, spec).publish(0, checked)['number'] == 1
    assert effect_row(store)[0] == 'complete'


@pytest.mark.asyncio
async def test_transport_failure_before_the_effect_row_is_transient_without_discard(
    service, monkeypatch,
):
    store, spec, broker, checked, gh = prepared(service, monkeypatch)
    real = gh.__call__
    failures = {'left': 1}

    def flaky_git(argv, **kwargs):
        if failures['left'] and argv[0] == 'git' and 'rev-list' in argv:
            failures['left'] -= 1
            raise subprocess.TimeoutExpired(argv, 120)
        return real(argv, **kwargs)

    monkeypatch.setattr(delivery_broker, '_run', flaky_git)
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    with pytest.raises(ApplicationError) as error:
        await delivery_activities.delivery_publish(request)
    assert error.value.type == 'PublicationTransient'
    assert error.value.non_retryable is False
    assert effect_row(store) is None and gh.mutations() == []
    published = await delivery_activities.delivery_publish(request)
    assert published['number'] == 1
    assert effect_row(store)[0] == 'complete'


@pytest.mark.asyncio
async def test_real_deadline_stop_is_admitted_and_completed_without_an_implementation_turn(
    service, monkeypatch,
):
    """A current admission, the real publish/reconcile/project activities and the
    default readback window: GitHub refuses PR creation until the run stops."""
    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker

    from devflow_temporal.delivery_activities import (
        delivery_gates_readback,
        delivery_project,
        delivery_publish,
        delivery_reconcile_publish,
    )

    store, request = service
    original_admit = type(store.config).admit

    def closing_admit(config, supplied):
        # The fixture provider is fake; production admissions finalize resources
        # and reconcile the tracker at every stop, which is where cleanup and the
        # claim release come from.
        admitted = original_admit(config, supplied)
        admitted.update(resource_cleanup_version=1, terminal_tracker_version=1)
        return admitted

    monkeypatch.setattr(type(store.config), 'admit', closing_admit)
    store.submit(request)
    spec = store.spec('run-1')
    assert spec['publication_readback_seconds'] == 900
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    _git(broker.checkout, 'config', 'user.name', 'Delivery Test')
    _git(broker.checkout, 'config', 'user.email', 'delivery@example.invalid')
    base_candidate = broker.candidate()
    (broker.checkout / 'README.md').write_text('Owned edit\n')
    checked = broker.candidate()
    gh = FakeGitHub(broker, create_failures=10 ** 6)
    monkeypatch.setattr(delivery_broker, '_run', gh)
    monkeypatch.setattr(delivery_broker.time, 'sleep', lambda _seconds: None)
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda given, **_kw: (store, DeliveryBroker(store, given)))
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _: {})
    # The fake provider has no native resource manifest to archive at admission.
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication.preserve_resources',
                        lambda *_args, **_kwargs: None)
    store.mark_start('run-1', accepted=True)
    seen = {'roles': [], 'uncertain': [], 'release': []}

    @activity.defn(name='delivery_prepare')
    async def prepare(_payload):
        return {'candidate': base_candidate}

    @activity.defn(name='delivery_tracker_start')
    async def tracker_start(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_role')
    async def role(payload):
        seen['roles'].append(payload['role'])
        candidate = checked if payload['role'] == 'implement' else (
            DeliveryBroker(store, payload['spec']).candidate())
        return {'status': 'pass', 'role': payload['role'], 'iteration': payload['iteration'],
                'session_id': payload['role'], 'cleanup': 'confirmed', 'candidate': candidate}

    @activity.defn(name='delivery_precheck')
    async def precheck(payload):
        return {'state': 'passed', 'source_unchanged': True,
                'candidate_id': payload['candidate']['id'],
                'results': [{'id': 'packaging', 'passed': True, 'cleanup': 'confirmed',
                             'exit_code': 0}]}

    @activity.defn(name='delivery_checks')
    async def checks(payload):
        return {'state': 'passed', 'candidate_id': payload['candidate']['id'],
                'cleanup': 'confirmed'}

    @activity.defn(name='delivery_ci')
    async def ci(_payload):
        return {'state': 'passed'}

    @activity.defn(name='delivery_tracker')
    async def tracker(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_finalize_resources')
    async def finalize_resources(payload):
        seen['uncertain'].append(payload['uncertain'])
        if payload['uncertain']:
            return {'state': 'unknown', 'process_cleanup': 'unknown',
                    'resource_cleanup': 'unknown'}
        return {'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
                'resource_cleanup': 'confirmed'}

    @activity.defn(name='delivery_terminal_tracker')
    async def terminal_tracker(payload):
        seen['release'].append(payload['release'])
        return {'state': 'consistent'}

    activities = [delivery_project, delivery_publish, delivery_reconcile_publish,
                  delivery_gates_readback, prepare, tracker_start, role, precheck, checks, ci,
                  tracker, finalize_resources, terminal_tracker]
    queue = 'publication-real-stop'
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(environment.client, task_queue=queue, workflows=[DeliveryWorkflow],
                          activities=activities):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id='delivery-run-1', task_queue=queue)
            stopped = await handle.result()
            pushed = _git(broker.checkout, 'rev-parse', 'HEAD')
            assert stopped['outcome'] == 'blocked'
            assert stopped['error'] == 'publication unresolved: readback deadline exhausted'
            assert stopped['checks']['failure']['classification'] == 'transient'
            assert stopped['checks']['failure']['cause_type'] == 'publication_deadline'
            assert stopped['cleanup'] == 'confirmed'
            assert seen['uncertain'] == [False] and seen['release'] == [True]
            assert stopped['pull_request'] is None
            assert remote_head(broker) == pushed and gh.created is False
            assert effect_row(store) == ('pending', {'head': pushed, 'number': None,
                                                     'remote_confirmed': True})
            with store._connect() as db:
                row = dict(db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
                # The terminal tracker releases the claim for codex providers.
                store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
            assert (row['phase'], row['cleanup'], row['error']) == (
                'blocked', 'confirmed', stopped['error'])
            closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': handle.result_run_id,
                      'closed_at': '2026-10-12T00:00:00Z',
                      'request_digest': spec['request_digest'], 'recovery_digest': None,
                      'result': stopped}
            monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: closed)
            admitted = store.recover_publication('run-1', {
                'command_id': 'after-the-real-stop', 'expected_revision': stopped['revision'],
                'expected_candidate_id': stopped['candidate']['id'], 'expected_head': pushed,
                'expected_pr_number': 0})
            assert admitted['phase'] == 'publication_recovery_queued'
            assert admitted['implementation_authority'] is False
            gh.create_failures = 0
            store.mark_start('run-1', accepted=True)
            with store._connect() as db:
                recovery = json.loads(db.execute(
                    "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
                ).fetchone()[0])
            effective = store.effective_spec('run-1')
            resumed = await environment.client.execute_workflow(
                DeliveryWorkflow.run, args=[effective, recovery], id=admitted['workflow_id'],
                task_queue=queue)
    assert resumed['outcome'] == 'delivered'
    assert resumed['pull_request']['number'] == 1
    assert resumed['pull_request']['head'] == pushed
    assert seen['roles'] == ['implement', 'review', 'verify']
    assert gh.created is True
    assert effect_row(store)[0] == 'complete'
    assert _git(broker.checkout, 'rev-list', '--count', spec['base_sha'] + '..HEAD') == '1'
