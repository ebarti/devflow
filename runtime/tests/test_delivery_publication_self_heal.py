"""Publication converges across transient failures instead of stranding the run."""
from __future__ import annotations

import asyncio
import json
import subprocess
from copy import deepcopy

import pytest
from test_delivery_gate_retry import unpublished
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_broker
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_publication_execution import execution
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
                if isinstance(self.create_error, BaseException):
                    raise self.create_error
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


NEW_REASON = 'publication unresolved: readback deadline exhausted'


@pytest.fixture
def pending_v1(service, monkeypatch):
    """A current admission stopped after its push with no PR, effect bound to the head."""
    # Component scope: exact owner stage is controlled here. The real closed
    # history/public boundary is exercised in test_delivery_publication_proof.py.
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication.publication_stop_proof',
                        lambda *_args, **_kwargs: None)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication.read_publication_closed',
                        lambda store, spec, **_kwargs:
                        (store._completed_temporal_result(spec['run_id']), None))
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


@pytest.mark.asyncio
async def test_real_deadline_stop_is_admitted_and_completed_without_an_implementation_turn(
    service, monkeypatch,
):
    """A current admission, the real publish/reconcile/project activities and the
    default readback window: a PR-create response is lost until the run stops."""
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
        return await asyncio.to_thread(
            DeliveryBroker(store, payload['spec']).run_resources().finalize,
            payload['outcome'], uncertain=payload['uncertain'])

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
            assert stopped['cleanup'] == 'unknown'
            assert seen['uncertain'] == [True] and seen['release'] == [False]
            assert stopped['pull_request'] is None
            assert remote_head(broker) == pushed and gh.created is False
            assert effect_row(store) == ('pending', {'head': pushed, 'number': None,
                                                     'remote_confirmed': True})
            with store._connect() as db:
                row = dict(db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
            assert (row['phase'], row['cleanup'], row['error']) == (
                'blocked', 'unknown', stopped['error'])
            closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': handle.result_run_id,
                      'closed_at': '2026-10-12T00:00:00Z',
                      'request_digest': spec['request_digest'], 'recovery_digest': None,
                      'result': stopped}
            monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: closed)
            # This transport fixture controls closed-owner readback. The real
            # bounded reader is qualified by test_delivery_publication_proof.py.
            monkeypatch.setattr(
                'devflow_temporal.delivery_pending_publication.read_publication_closed',
                lambda *_a, **_k: (closed, None))
            recover = {
                'command_id': 'after-the-real-stop', 'expected_revision': stopped['revision'],
                'expected_candidate_id': stopped['candidate']['id'], 'expected_head': pushed,
                'expected_pr_number': 0}
            mutations = len(gh.mutations())
            with pytest.raises(ValueError, match='exact completed PR readback'):
                await asyncio.to_thread(store.recover_publication, 'run-1', recover)
            assert len(gh.mutations()) == mutations
            # Later transport readback discovers the original request's PR. It
            # is evidence, not permission to issue another create request.
            gh.created = True
            with execution(broker, stopped['iteration'], stopped['candidate']):
                with pytest.raises(ValueError, match='executor or remote outcome remains unknown'):
                    await asyncio.to_thread(store.recover_publication, 'run-1', recover)
            # A real live root user is independently refused even after exact
            # PR readback; the publication lease alone is not native cleanup.
            actor = subprocess.Popen(['/bin/sleep', '30'], cwd=broker.checkout)
            try:
                with pytest.raises(ValueError, match='live root user|unreadable lease'):
                    await asyncio.to_thread(store.recover_publication, 'run-1', recover)
            finally:
                actor.terminate()
                actor.wait(timeout=5)
            from devflow_temporal.delivery_lifecycle import assess_transition

            with store._connect() as db:
                effect_before = dict(db.execute(
                    "SELECT * FROM delivery_effects WHERE kind='publish'").fetchone())
                executor_before = dict(db.execute(
                    "SELECT * FROM delivery_publication_executors").fetchone())
            observation = await asyncio.to_thread(
                assess_transition, store, 'run-1', 'recover_publication', recover)
            assert observation['eligible'] is True
            with store._connect() as db:
                current_effect = dict(db.execute(
                    "SELECT * FROM delivery_effects WHERE kind='publish'").fetchone())
                assert current_effect == effect_before
                assert dict(db.execute(
                    "SELECT * FROM delivery_publication_executors").fetchone()) == executor_before
            assert len(gh.mutations()) == mutations
            admitted = await asyncio.to_thread(store.recover_publication, 'run-1', recover)
            assert admitted['phase'] == 'publication_recovery_queued'
            assert admitted['implementation_authority'] is False
            store.mark_start('run-1', accepted=True)
            with store._connect() as db:
                recovery = json.loads(db.execute(
                    "SELECT recovery_json FROM delivery_runs WHERE run_id='run-1'"
                ).fetchone()[0])
            assert digest(recovery['state']) == digest(stopped)
            assert recovery['state']['cleanup'] == 'unknown'
            retained_cleanup = (broker.state_dir / recovery['namespace'] /
                                'predecessor-resources' / 'finalization.json')
            assert json.loads(retained_cleanup.read_text())['state'] == 'unknown'
            effective = store.effective_spec('run-1')
            resumed = await environment.client.execute_workflow(
                DeliveryWorkflow.run, args=[effective, recovery], id=admitted['workflow_id'],
                task_queue=queue)
    assert resumed['outcome'] == 'delivered'
    assert resumed['pull_request']['number'] == 1
    assert resumed['pull_request']['head'] == pushed
    assert seen['roles'] == ['implement', 'review', 'verify']
    assert gh.created is True
    assert len(gh.creations()) == 1
    assert effect_row(store)[0] == 'complete'
    # Successful terminal cleanup may remove the worktree. Its published Git
    # object remains in the owned source repository for exact mutation evidence.
    assert _git(broker.source, 'rev-list', '--count', spec['base_sha'] + '..' + pushed) == '1'
    assert resumed['cleanup'] == 'confirmed'


def test_observation_retains_publication_journal_and_refuses_active_executor(service, monkeypatch):
    store, _spec, broker, checked, gh = prepared(service, monkeypatch, create_failures=1)
    with pytest.raises(RuntimeError, match='HTTP 502'):
        asyncio.run(delivery_activities.delivery_publish(
            {'spec': broker.spec, 'iteration': 0, 'candidate': checked}))
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    gh.created = True  # Controlled later remote readback; no hosted write.
    with store._connect() as db:
        before_effect = dict(db.execute('SELECT * FROM delivery_effects WHERE kind=?',
                                        ('publish',)).fetchone())
    with execution(broker, 0, checked):
        assert broker.observe_publish(0, checked, expected_head=head, expected_pr_number=1) == {
            'state': 'pending', 'reason': 'publication_execution_active'}
    # Establish the post-lease journal baseline: observation itself must not rewrite it.
    with store._connect() as db:
        before_executor = dict(db.execute(
            'SELECT * FROM delivery_publication_executors').fetchone())
    gh.commands.clear()
    result = broker.observe_publish(0, checked, expected_head=head, expected_pr_number=1)
    assert result['number'] == 1 and result['head'] == head
    assert gh.mutations() == []
    with store._connect() as db:
        assert dict(db.execute('SELECT * FROM delivery_effects WHERE kind=?',
                               ('publish',)).fetchone()) == before_effect
        assert dict(db.execute(
            'SELECT * FROM delivery_publication_executors').fetchone()) == before_executor
