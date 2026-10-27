"""Published runs heal after transport and controller stops without a model turn."""
from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.exceptions import ActivityError, ApplicationError
from test_delivery_gate_retry import stopped as stopped
from test_delivery_metadata_recovery import published as published
from test_delivery_publication_self_heal import FakeGitHub, remote_head
from test_delivery_publication_self_heal import pending_v1 as pending_v1
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_broker, delivery_workflow
from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_gate_retry import (
    CI_KIND,
    KIND,
    TRANSIENT_GATE_RETRIES,
)
from devflow_temporal.delivery_stopped_resume import KIND as RESUME_KIND
from devflow_temporal.delivery_workflow import DeliveryWorkflow

TRANSPORT_STOPS = [
    'review activity failed: ActivityError',
    'verify activity failed: ActivityError',
    'checks activity failed: ActivityError',
]
PAST = (datetime.now(UTC) - timedelta(hours=1)).isoformat()


def activity_error(cause):
    error = ActivityError('activity failed', scheduled_event_id=1, started_event_id=2,
                          identity='worker', activity_type='delivery_role', activity_id='1',
                          retry_state=None)
    error.__cause__ = cause
    return error


def stop_transiently(store, state, error, *, stage='checks', controller=False,
                     cause_type='ActivityTimeoutError', keep_roles=False):
    """Project the published run as stopped by a classified transport/controller failure."""
    state['error'] = error
    if not keep_roles:
        state['roles'] = [r for r in state['roles'] if r.get('role') == 'implement']
    state['checks'] = {k: v for k, v in state['checks'].items()
                       if k not in {'failure', 'controller_failure'}}
    if controller:
        # The boundary records the structured failure and classifies the stop.
        state['checks']['controller_failure'] = {
            'version': 1, 'stage': stage, 'error_type': 'KeyError', 'message': 'missing key'}
        cause_type = 'controller_failure'
    state['checks']['failure'] = {'classification': 'transient', 'cause_type': cause_type,
                                  'stage': stage, 'reason': error}
    with store._connect() as db:
        # The historical fixture predates automatic retry; current admissions carry it.
        submitted = json.loads(db.execute(
            "SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
        submitted['automatic_retry_version'] = 1
        db.execute("UPDATE delivery_runs SET error=?,checks_json=?,updated_at=?,request_json=? "
                   "WHERE run_id='run-1'", (error, canonical_json(state['checks']), PAST,
                                            json.dumps(submitted)))
        db.execute("DELETE FROM delivery_attempts WHERE job_key='old-qa'")


def stop_again(store, state, closed, error, **kwargs):
    """The admitted continuation itself stops again; its closed result binds the chain."""
    with store._connect() as db:
        row = db.execute("SELECT workflow_id,recovery_json,work_id FROM delivery_runs "
                         "WHERE run_id='run-1'").fetchone()
    recovery = json.loads(row['recovery_json'])
    state['revision'] += 7
    stop_transiently(store, state, error, keep_roles=True, **kwargs)
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=error, candidate=state['candidate'], pull_request=state['pull_request'],
                  checks=state['checks'], iteration=state['iteration'],
                  protocol_revision=state['revision'], outcome='blocked', cleanup='confirmed',
                  error=error)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET updated_at=? WHERE run_id='run-1'", (PAST,))
        store.state.release_work(db, row['work_id'], 'external:devflow:run-1')
    closed.update(workflow_id=row['workflow_id'], recovery_digest=digest(recovery))
    return recovery


def current_recovery(store):
    with store._connect() as db:
        row = db.execute("SELECT recovery_json,phase,workflow_id FROM delivery_runs "
                         "WHERE run_id='run-1'").fetchone()
    return json.loads(row['recovery_json']), row['phase'], row['workflow_id']


def clear_waits(store):
    """Skip the in-process per-run backoff; the persisted one stays in force."""
    store._automatic_retry_waits = {}
    store._automatic_continuation_waits = {}


def run_count(store):
    with store._connect() as db:
        return db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0]


@pytest.mark.parametrize('error', TRANSPORT_STOPS)
def test_classified_transport_stop_after_publish_admits_fresh_gates_only(stopped, error):
    store, broker, state, _, request = stopped
    stop_transiently(store, state, error)
    before = broker.candidate()
    result = store.continue_repair('run-1', {**request, 'command_id': 'transient-1'})
    assert result['workflow_id'] == 'delivery-run-1-transient-gates-retry-1'
    assert result['implementation_authority'] is False
    assert result['additional_iterations'] == 0
    assert store.continue_repair('run-1', {**request, 'command_id': 'transient-1'}) == result
    recovery, phase, _ = current_recovery(store)
    assert phase == 'gates_retry_queued'
    assert recovery['kind'] == KIND
    assert recovery['execution_spec']['gate_retry_stage'] == 'transient'
    assert recovery['execution_spec']['gate_retry_generation'] == 1
    assert recovery['publication']['number'] == 7
    assert broker.candidate() == before
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()
    assert run_count(store) == 1






def test_transient_gate_retries_are_bounded_per_run(stopped):
    store, _, state, closed, request = stopped
    stop_transiently(store, state, TRANSPORT_STOPS[0])
    for generation in range(1, TRANSIENT_GATE_RETRIES + 1):
        payload = {**request, 'command_id': f'transient-{generation}',
                   'expected_revision': state['revision']}
        result = store.continue_repair('run-1', payload)
        assert result['workflow_id'] == f'delivery-run-1-transient-gates-retry-{generation}'
        stop_again(store, state, closed, TRANSPORT_STOPS[1])
    with pytest.raises(ValueError, match='bounded transient gate retries'):
        store.continue_repair('run-1', {**request, 'command_id': 'transient-too-many',
                                        'expected_revision': state['revision']})
    assert retry.retry_once(store) == []


def test_service_loop_admits_the_gates_retry_automatically_bounded_and_without_a_new_run(
    stopped, monkeypatch,
):
    store, broker, state, closed, _ = stopped
    stop_transiently(store, state, TRANSPORT_STOPS[0])
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET updated_at=? WHERE run_id='run-1'",
                   (datetime.now(UTC).isoformat(),))
    # Persisted backoff: a stop this recent is not retried yet.
    assert retry.retry_once(store) == []
    clear_waits(store)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET updated_at=? WHERE run_id='run-1'", (PAST,))
    admitted = retry.retry_once(store)
    assert [item['phase'] for item in admitted] == ['gates_retry_queued']
    assert admitted[0]['workflow_id'] == 'delivery-run-1-transient-gates-retry-1'
    assert admitted[0]['implementation_authority'] is False
    recovery, _, _ = current_recovery(store)
    assert recovery['command']['command_id'].startswith('automatic-gates-')
    assert run_count(store) == 1
    clear_waits(store)
    assert retry.retry_once(store) == []
    stop_again(store, state, closed, TRANSPORT_STOPS[2])
    clear_waits(store)
    second = retry.retry_once(store)
    assert [item['workflow_id'] for item in second] == [
        'delivery-run-1-transient-gates-retry-2']
    stop_again(store, state, closed, TRANSPORT_STOPS[0])
    clear_waits(store)
    assert retry.retry_once(store) == []
    assert run_count(store) == 1
    assert broker.candidate() == state['candidate']


def waiting_for_ci(store, state, *, recorded=True):
    candidate = state['candidate']
    state['roles'] = [r for r in state['roles'] if r.get('role') == 'implement']
    state['roles'].extend({'role': role, 'iteration': state['iteration'], 'status': 'pass',
                           'session_id': 'passed-' + role, 'candidate': candidate,
                           'cleanup': 'confirmed'} for role in ('review', 'verify'))
    state['checks'] = {key: {'state': 'passed', 'candidate_id': candidate['id']}
                       for key in ('review', 'qa', 'local')}
    if recorded:
        state['checks']['ci'] = {'state': 'pending', 'reason': 'CI deadline reached',
                                 'checks': {}}
        error, cause = 'required CI did not confirm this PR head', 'ci_deadline'
    else:
        error, cause = 'CI observation failed: ActivityError', 'ActivityTimeoutError'
    stop_transiently(store, state, error, stage='waiting_ci', cause_type=cause,
                     keep_roles=True)








def test_pending_publication_deadline_is_completed_by_the_service_loop(pending_v1):
    store, broker, state, request, _stop = pending_v1
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    spec = {**json.loads(row['request_json']), 'automatic_retry_version': 1}
    admit, generation, payload = retry.published_continuation(store, row, spec)
    assert admit == store.recover_publication and generation == 1
    assert payload['expected_head'] == request['expected_head']
    assert payload['expected_pr_number'] == 0
    assert payload['command_id'].startswith('automatic-publication-')
    assert admit('run-1', payload)['phase'] == 'publication_recovery_queued'
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    assert retry.published_continuation(store, row, spec) is None








class Abort(Exception):
    pass


def resume_probe(flow, launched, monkeypatch):
    async def execute(name, body, **_kw):
        launched.setdefault('calls', []).append((name, body.get('role')))
        if name == 'delivery_role':
            launched.update(body)
            raise Abort('stop after recording the first role request')
        if name == 'delivery_repair_preflight':
            return {'state': 'confirmed'}
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(_spec, event, _message):
        launched.setdefault('events', []).append(event)

    async def stop(_spec, reason, **_kw):
        flow.state.update(outcome='blocked', error=reason)
        return flow.state

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_stop', stop)
    monkeypatch.setattr(delivery_workflow.workflow, 'patched', lambda _name: True)


def admitted_resume(store, broker, state, monkeypatch):
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    store.continue_repair('run-1', {
        'continuation_kind': RESUME_KIND, 'command_id': 'resume-after-transport',
        'expected_revision': state['revision'], 'expected_iteration': state['iteration'],
        'expected_candidate_id': broker.candidate()['id'],
        'expected_candidate_head': broker.candidate()['head'], 'additional_iterations': 1})
    recovery, _, _ = current_recovery(store)
    return store.effective_spec('run-1'), recovery






def test_interrupted_repair_resumes_with_the_findings_it_was_repairing(stopped, monkeypatch):
    store, _, state, _, _ = stopped
    spec = store.spec('run-1')
    # verify(1) recorded findings; the repair turn 2 was lost to transport before it
    # produced anything. The resume hands over those findings, never the error.
    assert [r['findings'] for r in state['roles'] if r.get('role') == 'verify'] == [
        ['Receipt omits source hash semantics']]
    state['iteration'] = 2
    state['checks'] = {'failure': {'classification': 'transient',
                                   'cause_type': 'ActivityTimeoutError', 'stage': 'repair',
                                   'reason': 'implementer activity failed: ActivityError'}}
    state['error'] = 'implementer activity failed: ActivityError'
    recovery = {'execution_spec': spec, 'maximum_iteration': state['iteration'] + 1,
                'state': state, 'execution_candidate': state['candidate'],
                'session_id': 'original-session', 'command': {'additional_iterations': 1}}
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = True
    launched = {}
    resume_probe(flow, launched, monkeypatch)
    # The role activity's failure is absorbed by the run; the request is what matters.
    asyncio.run(flow._resume_stopped(spec, recovery))
    assert launched['role'] == 'implement'
    assert launched['iteration'] == state['iteration'] + 1
    assert launched['findings'] == ['Receipt omits source hash semantics']




STALL = 'publication unresolved: remote unavailable before push'


def stalled_repair(service, monkeypatch, stage):
    """A published run whose repair iteration passed and whose publication stalled.

    stage: 'uncommitted' (lost before the local commit), 'committed' (lost before
    the push) or 'pushed' (pushed, PR head not read back)."""
    from copy import deepcopy

    from test_delivery_gate_retry import unpublished

    store, broker, original, _gate_request = unpublished.__wrapped__(service, monkeypatch)
    spec = store.effective_spec('run-1')
    broker = DeliveryBroker(store, spec)
    broker._effect('publish:run-1:0', 'publish', {
        'iteration': 0, 'input_candidate_id': original['candidate']['id']})
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-sqm', 'docs: document the owned feature')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    first = broker.candidate()
    publication = {'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                   'head': first['head'], 'base': spec['base_sha'], 'candidate': first}
    broker._finish_effect('publish:run-1:0', publication)

    def existing(self, **_kwargs):
        remote = _git(self.source, 'ls-remote', 'origin', 'refs/heads/' + spec['branch'])
        return {'number': 1, 'url': publication['url'], 'state': 'OPEN', 'isDraft': False,
                'headRefName': spec['branch'], 'baseRefName': broker._publication_base_ref(),
                'headRefOid': remote.split()[0], 'title': 'chore: Change a fixture'}

    monkeypatch.setattr(DeliveryBroker, '_existing_pr', existing)
    (broker.checkout / 'README.md').write_text('Owned repair edit\n')
    candidate = broker.candidate()
    assert candidate['head'] == publication['head']
    state = deepcopy(original)
    state.update(iteration=1, candidate=candidate, pull_request=publication, revision=21,
                 error=STALL, cleanup='confirmed')
    state['roles'].append({'role': 'implement', 'iteration': 1, 'status': 'pass',
                           'session_id': 'original-implementation', 'cleanup': 'confirmed',
                           'candidate': candidate})
    state['checks'] = {
        'prepublish': {'state': 'passed', 'source_unchanged': True,
                       'candidate_id': candidate['id'],
                       'results': [{'id': 'packaging', 'passed': True, 'cleanup': 'confirmed',
                                    'exit_code': 0}]},
        'failure': {'classification': 'transient', 'cause_type': 'publication_deadline',
                    'stage': 'publishing_pending', 'reason': STALL}}
    store.project('run-1', phase='blocked', execution_state='blocked', event_type='blocked',
                  message=STALL, candidate=candidate, pull_request=publication,
                  checks=state['checks'], iteration=1, protocol_revision=21, outcome='blocked',
                  cleanup='confirmed', error=STALL)
    with store._connect() as db:
        submitted = json.loads(db.execute(
            "SELECT request_json FROM delivery_runs WHERE run_id='run-1'").fetchone()[0])
        submitted['automatic_retry_version'] = 1
        db.execute("UPDATE delivery_runs SET request_json=?,updated_at=? WHERE run_id='run-1'",
                   (json.dumps(submitted), PAST))
    if stage in {'committed', 'pushed'}:
        _git(broker.checkout, 'add', 'README.md')
        _git(broker.checkout, 'commit', '-sqm', 'docs: repair the owned feature')
    if stage == 'pushed':
        broker._effect('publish:run-1:1', 'publish', {'iteration': 1,
                                                      'input_candidate_id': candidate['id']})
        _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
        broker._bind_pending_publication('publish:run-1:1',
                                         _git(broker.checkout, 'rev-parse', 'HEAD'), 1,
                                         remote_confirmed=True)
    closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': 'closed-original',
              'request_digest': spec['request_digest'], 'recovery_digest': None,
              'result': state}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_kw: closed)
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _: {})
    return store, broker, state, closed, candidate


def completion_request(broker, state, candidate, command_id='complete-repair-publication'):
    return {'command_id': command_id, 'expected_revision': state['revision'],
            'expected_candidate_id': candidate['id'],
            'expected_head': _git(broker.checkout, 'rev-parse', 'HEAD'),
            'expected_pr_number': 0}














def test_superseded_stop_is_never_revived_by_the_service_loop(service, monkeypatch):
    store, _broker, _state, _closed, _candidate = stalled_repair(service, monkeypatch,
                                                                'uncommitted')
    _store, request = service
    newer = store.submit({**request, 'run_id': 'run-2', 'command_id': 'command-2',
                          'branch': 'feat/fixture-2'})
    assert newer['existing'] is False
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    assert retry.published_continuation(store, row, json.loads(row['request_json'])) is None
    clear_waits(store)
    assert retry.retry_once(store) == []


class RemoteFakeGitHub(FakeGitHub):
    """gh answers whose PR head follows origin, as GitHub's does."""

    def __call__(self, argv, **kwargs):
        if argv[:3] == ['gh', 'pr', 'list'] and not self.list_failures and self.created:
            self.commands.append(list(argv))
            remote = _git(self.broker.source, 'ls-remote', 'origin',
                          'refs/heads/' + self.broker.spec['branch'])
            return json.dumps([{
                'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                'isDraft': False, 'baseRefName': self.broker._publication_base_ref(),
                'headRefName': self.broker.spec['branch'],
                'headRefOid': remote.split()[0] if remote else '',
                'title': 'chore: Change a fixture',
            }])
        return super().__call__(argv, **kwargs)


class RealDelivery:
    """A current admission on the time-skipping server with the real publish,
    reconcile, projection, readback and (optionally) finalization activities;
    roles, checks and CI are scripted, so every stop is one the real controller
    produces and every admission reads the state it projected."""

    queue = 'real-delivery'

    def __init__(self, service, monkeypatch, *, script=None):
        self.script = script or {}
        self.monkeypatch = monkeypatch
        self.seen = {'roles': [], 'ci_heads': [], 'uncertain': [], 'release': [],
                     'checks': 0, 'ci': 0}
        self.service = service

    def _setup(self, temporal_address):
        from devflow_temporal.delivery_config import DeliveryConfig
        from devflow_temporal.delivery_store import DeliveryStore

        original, request = self.service
        raw = json.loads(original.config.path.read_text())
        raw['temporal_address'] = temporal_address
        original.config.path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(original.config.path))
        monkeypatch = self.monkeypatch
        original_admit = type(store.config).admit

        def closing_admit(config, supplied):
            # The fixture provider is fake; production admissions finalize resources
            # and reconcile the tracker at every stop.
            admitted = original_admit(config, supplied)
            admitted.update(resource_cleanup_version=1, terminal_tracker_version=1)
            return admitted

        monkeypatch.setattr(type(store.config), 'admit', closing_admit)
        store.submit(request)
        self.store, self.spec = store, store.spec('run-1')
        self.broker = DeliveryBroker(store, self.spec)
        self.broker.prepare()
        self.broker.state_dir.mkdir(parents=True, exist_ok=True)
        _git(self.broker.checkout, 'config', 'user.name', 'Delivery Test')
        _git(self.broker.checkout, 'config', 'user.email', 'delivery@example.invalid')
        self.base_candidate = self.broker.candidate()
        self.gh = RemoteFakeGitHub(self.broker)
        monkeypatch.setattr(delivery_broker, '_run', self.gh)
        monkeypatch.setattr(delivery_broker.time, 'sleep', lambda _seconds: None)
        monkeypatch.setattr(delivery_activities, '_context',
                            lambda given, **_kw: (store, DeliveryBroker(store, given)))
        for module in ('delivery_pending_publication', 'delivery_gate_retry'):
            monkeypatch.setattr(f'devflow_temporal.{module}._stopped_cleanup', lambda _: {})
        # The closure's tracker helper releases the claim it is asked to release.
        monkeypatch.setattr(retry, '_tracker_sync', self._tracker)
        store.mark_start('run-1', accepted=True)

    def _tracker(self, spec, status, *, release, terminal, reason=None):
        if spec.get('project_sync_version') == 1:
            from devflow_temporal.delivery_features import record_tracking

            return record_tracking(self.store, spec, status, release)
        if release:
            with self.store._connect() as db:
                self.store.state.release_work(db, spec['work_id'],
                                              'external:devflow:' + spec['run_id'])
        return {'state': 'consistent', 'pending': False}

    def activities(self):
        from temporalio import activity

        from devflow_temporal.delivery_activities import (
            delivery_finalize_resources,
            delivery_gates_readback,
            delivery_project,
            delivery_publish,
            delivery_reconcile_publish,
        )

        seen, script, store, base = self.seen, self.script, self.store, self.base_candidate

        @activity.defn(name='delivery_project')
        async def project(payload):
            # The real projection, with a scripted failure window.
            if 'project' in script:
                script['project'](payload)
            return await delivery_project(payload)

        @activity.defn(name='delivery_prepare')
        async def prepare(_payload):
            return {'candidate': base}

        @activity.defn(name='delivery_tracker_start')
        async def tracker_start(_payload):
            return {'state': 'consistent'}

        @activity.defn(name='delivery_repair_preflight')
        async def repair_preflight(_payload):
            return {'state': 'confirmed'}

        @activity.defn(name='delivery_role')
        async def role(payload):
            seen['roles'].append((payload['role'], payload['iteration']))
            working = DeliveryBroker(store, payload['spec'])
            if payload['role'] == 'implement':
                seen.setdefault('findings', []).append(list(payload.get('findings') or []))
                (working.checkout / 'README.md').write_text(
                    f"Owned edit {payload['iteration']}\n")
                if 'implement' in script:
                    script['implement'](payload['iteration'], self)
            # The repair resumes the implementer's session; independent roles are fresh.
            session = ('implementer' if payload['role'] == 'implement'
                       else f"{payload['role']}-{payload['iteration']}")
            result = {'status': 'pass', 'role': payload['role'],
                      'iteration': payload['iteration'], 'session_id': session,
                      'cleanup': 'confirmed', 'candidate': working.candidate()}
            findings = script.get('review', lambda _iteration: None)(payload['iteration']) \
                if payload['role'] == 'review' else None
            if findings:
                result.update(status='findings', findings=findings)
            return result

        @activity.defn(name='delivery_precheck')
        async def precheck(payload):
            return {'state': 'passed', 'source_unchanged': True,
                    'candidate_id': payload['candidate']['id'],
                    'results': [{'id': 'packaging', 'passed': True, 'cleanup': 'confirmed',
                                 'exit_code': 0}]}

        @activity.defn(name='delivery_checks')
        async def checks(payload):
            seen['checks'] += 1
            if 'checks' in script:
                script['checks'](seen['checks'])
            return {'state': 'passed', 'candidate_id': payload['candidate']['id'],
                    'cleanup': 'confirmed'}

        @activity.defn(name='delivery_ci')
        async def ci(payload):
            seen['ci'] += 1
            seen['ci_heads'].append(payload['pull_request']['head'])
            if 'ci' in script:
                return script['ci'](seen['ci'])
            return {'state': 'passed'}

        @activity.defn(name='delivery_tracker')
        async def tracker(_payload):
            return {'state': 'consistent'}

        @activity.defn(name='delivery_finalize_resources')
        async def finalize(payload):
            # The real finalization, with its uncertainty recorded for the assertions.
            seen['uncertain'].append(payload['uncertain'])
            return await delivery_finalize_resources(payload)

        @activity.defn(name='delivery_terminal_tracker')
        async def terminal_tracker(payload):
            seen['release'].append(payload['release'])
            return {'state': 'consistent'}

        return [project, delivery_publish, delivery_reconcile_publish,
                delivery_gates_readback, prepare, tracker_start, repair_preflight, role,
                precheck, checks, ci, tracker, terminal_tracker, finalize]

    @contextlib.asynccontextmanager
    async def running(self):
        from temporalio.testing import WorkflowEnvironment
        from temporalio.worker import Worker

        async with await WorkflowEnvironment.start_time_skipping() as environment:
            # Bind the owned server before admission freezes execution configuration.
            self._setup(environment.client.service_client.config.target_host)
            async with Worker(environment.client, task_queue=self.queue,
                              workflows=[DeliveryWorkflow],
                              activities=self.activities()):
                self.client = environment.client
                yield self

    async def start(self):
        handle = await self.client.start_workflow(
            DeliveryWorkflow.run, self.spec, id='delivery-run-1', task_queue=self.queue,
            memo={'request_digest': self.spec['request_digest']})
        result = await handle.result()
        self.closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': handle.result_run_id,
                       'closed_at': '2026-10-12T00:00:00Z',
                       'request_digest': self.spec['request_digest'], 'recovery_digest': None,
                       'result': result}
        self.monkeypatch.setattr(self.store, '_completed_temporal_result',
                                 lambda *_a, **_k: self.closed)
        return result

    def row(self):
        with self.store._connect() as db:
            return dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())

    def claim(self):
        with self.store._connect() as db:
            return self.store.state.claim_for(db, self.spec['work_id'])

    def age(self, *, release=False):
        with self.store._connect() as db:
            if release:
                # The terminal tracker releases the claim for codex providers.
                self.store.state.release_work(db, self.spec['work_id'],
                                              'external:devflow:run-1')
            db.execute("UPDATE delivery_runs SET updated_at=? WHERE run_id='run-1'", (PAST,))

    async def continue_run(self):
        self.store.mark_start('run-1', accepted=True)
        recovery, _, workflow_id = current_recovery(self.store)
        effective = self.store.effective_spec('run-1')
        result = await self.client.execute_workflow(
            DeliveryWorkflow.run, args=[effective, recovery], id=workflow_id,
            task_queue=self.queue, memo={'request_digest': effective['request_digest'],
                                        'recovery_digest': digest(recovery)})
        return result, recovery


def interrupt_repair_publication(iteration, run):
    if iteration == 1:
        # GitHub becomes unreachable right before the repair's publication.
        run.gh.list_failures = 10 ** 6


@pytest.mark.asyncio
@pytest.mark.parametrize('path', ['automatic', 'stopped_resume'])
async def test_stalled_repair_publication_heals_through_the_real_stop(service, monkeypatch,
                                                                       path):
    """Iteration 0 publishes; the repair's publication loses GitHub before its push
    for longer than the readback window. The service loop completes it on the
    existing PR, and an operator's stopped resume publishes it before any gate;
    neither spends an implementation turn nor runs CI on another head."""
    script = {'implement': interrupt_repair_publication,
              'review': lambda iteration: ['Needs a second edit'] if iteration == 0 else None}
    async with RealDelivery(service, monkeypatch, script=script).running() as run:
        assert run.store.config.temporal_address == run.client.service_client.config.target_host
        stopped = await run.start()
        first_head = stopped['pull_request']['head']
        assert stopped['outcome'] == 'blocked' and stopped['iteration'] == 1
        assert stopped['error'] == STALL
        assert stopped['checks']['failure']['cause_type'] == 'publication_deadline'
        assert stopped['cleanup'] == 'confirmed'
        assert run.seen['uncertain'] == [False] and run.seen['release'] == [True]
        assert stopped['pull_request']['number'] == 1
        assert remote_head(run.broker) == first_head
        assert _git(run.broker.checkout, 'rev-parse', 'HEAD') == first_head
        assert (run.broker.checkout / 'README.md').read_text() == 'Owned edit 1\n'
        row = run.row()
        assert (row['phase'], row['cleanup'], row['iteration']) == ('blocked', 'confirmed', 1)
        with run.store._connect() as db:
            assert db.execute("SELECT COUNT(*) FROM delivery_effects WHERE kind='publish' "
                              "AND state='pending'").fetchone()[0] == 0
        run.age(release=True)
        run.gh.list_failures = 0
        if path == 'automatic':
            clear_waits(run.store)
            admitted = retry.retry_once(run.store)
            assert [item['phase'] for item in admitted] == ['publication_recovery_queued']
            assert admitted[0]['implementation_authority'] is False
        else:
            admitted = run.store.continue_repair('run-1', {
                'continuation_kind': RESUME_KIND, 'command_id': 'resume-the-stalled-repair',
                'expected_revision': stopped['revision'], 'expected_iteration': 1,
                'expected_candidate_id': stopped['candidate']['id'],
                'expected_candidate_head': stopped['candidate']['head'],
                'additional_iterations': 1})
            assert admitted['phase'] == 'repair_continuation_queued'
        assert run_count(run.store) == 1
        resumed, recovery = await run.continue_run()
    # Delivery finalizes the checkout away; origin and the shared object store remain.
    repaired = remote_head(run.broker)
    assert resumed['outcome'] == 'delivered'
    assert resumed['pull_request']['number'] == 1
    assert resumed['pull_request']['head'] == repaired != first_head
    assert _git(run.broker.source, 'rev-list', '--count',
                run.spec['base_sha'] + '..' + repaired) == '2'
    assert len(run.gh.creations()) == 1
    assert run.seen['roles'] == [('implement', 0), ('review', 0), ('verify', 0),
                                 ('implement', 1), ('review', 1), ('verify', 1)]
    # CI only ever observed the gated candidate, which is the delivered head.
    assert run.seen['ci_heads'] == [repaired]
    assert recovery['kind'] == ('pending_publication_retry' if path == 'automatic'
                                else RESUME_KIND)
    with run.store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects WHERE effect_key=?",
                          ('publish:run-1:1',)).fetchone()[0] == 'complete'


def lose_first_checks(call):
    if call == 1:
        raise ApplicationError('local check runner lost', type='TimeoutExpired')


def break_first_ci_observation(call):
    # A malformed observation makes the controller itself fail after publish.
    return 'passed' if call == 1 else {'state': 'passed'}




async def run_ci_continuation_probe(*, marker):
    """A published_ci_retry continuation whose observation ends at the deadline."""
    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Replayer, UnsandboxedWorkflowRunner, Worker
    from test_delivery_publication_liveness import marker_replayed_false

    spec = {'run_id': 'ci-probe', 'provider': 'fake',
            'policy': {'max_repairs': 0, 'browser_qa': None, 'ci_wait_seconds': 60},
            'automatic_retry_version': 1, 'publication_readback_version': 1}
    candidate = {'id': 'c' * 64, 'head': 'a' * 40}
    publication = {'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                   'head': 'a' * 40, 'candidate': candidate}
    state = {'run_id': 'ci-probe', 'revision': 9, 'iteration': 0, 'phase': 'blocked',
             'execution_state': 'blocked', 'outcome': 'blocked', 'cleanup': 'confirmed',
             'error': 'required CI did not confirm this PR head', 'candidate': candidate,
             'pull_request': publication, 'candidate_revision': 1, 'roles': [],
             'checks': {'ci': {'state': 'pending'}}, 'usage': {}, 'findings': []}
    recovery = {'kind': CI_KIND, 'execution_spec': spec, 'command': {'additional_iterations': 0},
                'state': state, 'candidate': candidate, 'publication': publication}
    events = []

    @activity.defn(name='delivery_project')
    async def project(payload):
        events.append(payload['event_type'])
        return {'revision': len(events)}

    @activity.defn(name='delivery_gates_readback')
    async def gates_readback(_payload):
        return {}

    @activity.defn(name='delivery_tracker_start')
    async def tracker_start(_payload):
        return {'state': 'consistent'}

    @activity.defn(name='delivery_ci')
    async def ci(_payload):
        return {'state': 'pending', 'reason': 'CI deadline reached'}

    @activity.defn(name='delivery_tracker')
    async def tracker(_payload):
        return {'state': 'consistent'}

    activities = [project, gates_readback, tracker_start, ci, tracker]
    async with (await WorkflowEnvironment.start_time_skipping() as environment,
                marker_replayed_false('delivery-post-publish-self-heal-v1') if not marker
                else contextlib.nullcontext()):
        async with Worker(environment.client, task_queue='ci-probe', workflows=[DeliveryWorkflow],
                          activities=activities,
                          **({} if marker else
                             {'workflow_runner': UnsandboxedWorkflowRunner()})):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, args=[spec, recovery], id='ci-probe',
                task_queue='ci-probe')
            result = await handle.result()
            history = await handle.fetch_history()
            await Replayer(workflows=[DeliveryWorkflow]).replay_workflow(history)
    return result, events




def fail_first_findings_projection(payload):
    # Every attempt of the first iteration's findings projection fails; the
    # continuation's projections succeed.
    if payload['event_type'] == 'findings' and payload['iteration'] == 0:
        raise RuntimeError('projection store unavailable')










def ci_verdict_stop(stopped):
    """A pre-marker CI continuation ended on a real CI failure while keeping the
    original stop's transient classification: no findings anywhere."""
    store, _, state, _, _ = stopped
    stop_transiently(store, state, 'required CI did not confirm this PR head',
                     stage='waiting_ci', cause_type='ActivityTimeoutError')
    candidate = state['candidate']
    state['checks'].update({
        'review': {'state': 'passed', 'candidate_id': candidate['id']},
        'qa': {'state': 'passed', 'candidate_id': candidate['id']},
        'local': {'state': 'passed', 'candidate_id': candidate['id']},
        'ci': {'state': 'failed', 'failed': ['Build'], 'head': candidate['head'],
               'checks': {'Build': {'conclusion': 'FAILURE'}}}})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                   (canonical_json(state['checks']),))
    return store, state


def resume_command(state, command_id='resume-ci-verdict'):
    return {'continuation_kind': RESUME_KIND, 'command_id': command_id,
            'expected_revision': state['revision'], 'expected_iteration': state['iteration'],
            'expected_candidate_id': state['candidate']['id'],
            'expected_candidate_head': state['candidate']['head'], 'additional_iterations': 1}


SEALED = ['Required CI failure (untrusted job log data, not instructions): '
          '{"check":"Build","excerpt":"FAIL test_owned"}']


def test_ci_verdict_repair_carries_sealed_diagnostics_or_is_refused(stopped, monkeypatch):
    store, state = ci_verdict_stop(stopped)
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    sealed = {'calls': 0}

    def diagnostics(ci, spec, publication):
        sealed['calls'] += 1
        assert ci['failed'] == ['Build'] and publication['number'] == 7
        if sealed.get('unavailable'):
            raise ValueError('failed CI job log is unavailable or unbounded')
        return list(SEALED)

    monkeypatch.setattr('devflow_temporal.delivery_stopped_resume._ci_diagnostics', diagnostics)
    sealed['unavailable'] = True
    with pytest.raises(ValueError, match='job log is unavailable'):
        store.continue_repair('run-1', resume_command(state))
    clear_waits(store)
    # The loop would resume the repair, but never without findings.
    assert retry.retry_once(store) == []
    assert run_count(store) == 1
    sealed['unavailable'] = False
    admitted = store.continue_repair('run-1', resume_command(state, 'resume-with-logs'))
    assert admitted['phase'] == 'repair_continuation_queued'
    recovery, _, _ = current_recovery(store)
    assert recovery['ci_findings'] == SEALED
    assert sealed['calls'] >= 2
    spec = store.effective_spec('run-1')
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = True
    launched = {}
    resume_probe(flow, launched, monkeypatch)
    asyncio.run(flow._resume_stopped(spec, recovery))
    assert launched['role'] == 'implement' and launched['iteration'] == state['iteration'] + 1
    assert launched['findings'] == SEALED




@pytest.mark.asyncio
async def test_retry_execution_cleanup_recovers_again_without_reusing_old_archive(
    service, monkeypatch,
):
    def lose_twice(call):
        if call <= 2:
            raise ApplicationError("gate transport lost", type="TimeoutExpired")

    async with RealDelivery(service, monkeypatch, script={"checks": lose_twice}).running() as run:
        result = await run.start()
        original_head = result["pull_request"]["head"]
        for generation in (1, 2):
            assert result["outcome"] == "blocked" and result["cleanup"] == "unknown"
            run.age()
            clear_waits(run.store)
            admitted = retry.retry_once(run.store)
            assert len(admitted) == 1, run.store.events("run-1")
            assert admitted[0]["implementation_authority"] is False
            result, recovery = await run.continue_run()
            row = run.row()
            run.closed = {**run.closed, "workflow_id": row["workflow_id"],
                          "execution_run_id": "continued-" + str(generation),
                          "recovery_digest": digest(recovery), "result": result}
        assert result["outcome"] == "delivered"
        assert result["pull_request"]["head"] == original_head
        assert run.seen["roles"].count(("implement", 0)) == 1
        assert run_count(run.store) == 1
        from pathlib import Path
        archive = Path(run.spec["state_dir"]) / "resources" / "predecessor-resources"
        assert (archive / "finalization.json").is_file()
        assert len(list(archive.glob("*/finalization.json"))) == 1
