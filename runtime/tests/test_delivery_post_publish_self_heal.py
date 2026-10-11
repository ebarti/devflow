"""Published runs heal after transport and controller stops without a model turn."""
from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest
from temporalio.exceptions import ActivityError, ApplicationError
from test_delivery_gate_retry import stopped as stopped
from test_delivery_metadata_recovery import published as published
from test_delivery_publication_self_heal import pending_v1 as pending_v1
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities, delivery_broker, delivery_workflow
from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_gate_retry import (
    CI_KIND,
    CI_OBSERVATION_RETRIES,
    CONTROLLER_KIND,
    KIND,
    PRELAUNCH_KIND,
    TRANSIENT_GATE_RETRIES,
)
from devflow_temporal.delivery_stopped_resume import KIND as RESUME_KIND
from devflow_temporal.delivery_workflow import DeliveryWorkflow, _failure_classification

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
        state['checks']['controller_failure'] = {
            'version': 1, 'stage': stage, 'error_type': 'KeyError', 'message': 'missing key'}
    else:
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


def test_controller_failure_stop_after_publish_is_visible_to_gate_admission(stopped):
    store, _, state, _, request = stopped
    stop_transiently(store, state, 'controller failed: KeyError', controller=True)
    result = store.continue_repair('run-1', {**request, 'command_id': 'controller-transient'})
    assert result['workflow_id'] == 'delivery-run-1-transient-gates-retry-1'
    assert result['implementation_authority'] is False


@pytest.mark.parametrize('error', TRANSPORT_STOPS)
def test_unclassified_transport_string_is_still_refused_by_every_gate_retry(stopped, error):
    store, _, state, _, request = stopped
    stop_transiently(store, state, error)
    state['checks'].pop('failure')
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET checks_json=? WHERE run_id='run-1'",
                   (canonical_json(state['checks']),))
    for kind in (KIND, CI_KIND, PRELAUNCH_KIND, CONTROLLER_KIND):
        with pytest.raises(ValueError):
            store.continue_repair('run-1', {**request, 'continuation_kind': kind,
                                            'command_id': 'string-' + kind})
    assert retry.retry_once(store) == []


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


def test_ci_observation_repeats_automatically_while_the_head_is_unchanged(stopped):
    store, _, state, closed, request = stopped
    waiting_for_ci(store, state)
    sizes = []
    for generation in range(1, CI_OBSERVATION_RETRIES + 1):
        clear_waits(store)
        admitted = retry.retry_once(store)
        assert [item['workflow_id'] for item in admitted] == [
            f'delivery-run-1-ci-retry-{generation}']
        recovery, _, _ = current_recovery(store)
        assert recovery['kind'] == CI_KIND
        assert recovery['execution_spec']['gate_retry_generation'] == generation
        sizes.append(len(canonical_json(recovery)))
        stop_again(store, state, closed, 'required CI did not confirm this PR head',
                   stage='waiting_ci', cause_type='ci_deadline')
    # The retained chain is referenced by digest, so the record grows linearly.
    assert sizes[-1] < 2 * sizes[0] * CI_OBSERVATION_RETRIES
    clear_waits(store)
    assert retry.retry_once(store) == []
    with pytest.raises(ValueError, match='bounded CI observation retries'):
        store.continue_repair('run-1', {**request, 'continuation_kind': CI_KIND,
                                        'command_id': 'ci-too-many',
                                        'expected_revision': state['revision']})
    assert run_count(store) == 1


def test_second_operator_ci_observation_is_admitted_after_another_deadline(stopped):
    store, _, state, closed, request = stopped
    waiting_for_ci(store, state)
    first = store.continue_repair('run-1', {**request, 'continuation_kind': CI_KIND,
                                            'command_id': 'ci-1'})
    assert first['workflow_id'] == 'delivery-run-1-ci-retry-1'
    stop_again(store, state, closed, 'required CI did not confirm this PR head',
               stage='waiting_ci', cause_type='ci_deadline')
    second = store.continue_repair('run-1', {**request, 'continuation_kind': CI_KIND,
                                             'command_id': 'ci-2',
                                             'expected_revision': state['revision']})
    assert second['workflow_id'] == 'delivery-run-1-ci-retry-2'


def test_lost_ci_observation_without_a_recorded_state_is_observed_again(stopped):
    store, _, state, _, request = stopped
    waiting_for_ci(store, state, recorded=False)
    assert 'ci' not in state['checks']
    result = store.continue_repair('run-1', {**request, 'continuation_kind': CI_KIND,
                                             'command_id': 'ci-lost'})
    assert result['workflow_id'] == 'delivery-run-1-ci-retry-1'
    assert result['implementation_authority'] is False


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


def test_tracker_and_unpublished_transient_stops_get_no_published_continuation(stopped):
    store, _, state, _, _ = stopped
    stop_transiently(store, state, 'tracker readback remains pending or conflicting',
                     stage='tracker', cause_type='tracker_readback')
    with store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
    spec = {**json.loads(row['request_json']), 'automatic_retry_version': 1}
    assert retry.published_continuation(store, row, spec) is None
    assert retry.published_continuation(store, {**row, 'pr_json': None}, spec) is None


def test_controller_failure_classifies_transient_with_its_own_cause():
    assert _failure_classification(KeyError('head'), 'controller_failure') == {
        'classification': 'transient', 'cause_type': 'controller_failure'}
    assert _failure_classification(KeyError('head'), None) == {'classification': 'terminal'}


@pytest.mark.parametrize('post_publish_v1,original',
                         [(True, False), (False, False), (False, True)])
def test_stop_records_the_classification_for_recovery_executions(monkeypatch, post_publish_v1,
                                                                 original):
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = post_publish_v1
    flow.original_execution = original
    flow.state = {'phase': 'checks', 'revision': 3, 'roles': [], 'checks': {},
                  'cleanup': 'none', 'cancel_requested': False}
    projected = []

    async def project(_spec, event, _message):
        projected.append(event)

    monkeypatch.setattr(flow, '_project', project)
    cause = activity_error(ApplicationError('lost', type='TimeoutExpired'))
    result = asyncio.run(flow._stop({'automatic_retry_version': 1},
                                    'review activity failed: ActivityError', cause=cause))
    assert projected == ['blocked'] and result['outcome'] == 'blocked'
    if post_publish_v1 or original:
        assert result['checks']['failure'] == {
            'classification': 'transient', 'cause_type': 'TimeoutExpired', 'stage': 'checks',
            'reason': 'review activity failed: ActivityError'}
    else:
        assert 'failure' not in result['checks']


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


@pytest.mark.parametrize('controller', [False, True])
def test_stopped_resume_reruns_gates_instead_of_handing_the_error_to_the_implementer(
    stopped, monkeypatch, controller,
):
    store, broker, state, _, _ = stopped
    error = 'controller failed: KeyError' if controller else TRANSPORT_STOPS[0]
    stop_transiently(store, state, error, controller=controller)
    spec, recovery = admitted_resume(store, broker, state, monkeypatch)
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = True
    launched = {}
    resume_probe(flow, launched, monkeypatch)
    # The role activity's failure is absorbed by the run; the request is what matters.
    asyncio.run(flow._resume_stopped(spec, recovery))
    assert launched['role'] == 'review'
    assert launched['iteration'] == state['iteration']
    assert error not in json.dumps(launched.get('findings'))
    assert ('delivery_role', 'implement') not in launched['calls']
    assert ('delivery_checks', None) in launched['calls']
    assert 'failure' not in flow.state['checks']
    assert 'controller_failure' not in flow.state['checks']


def test_recorded_stopped_resume_keeps_its_implementation_turn_without_the_marker(
    stopped, monkeypatch,
):
    store, broker, state, _, _ = stopped
    stop_transiently(store, state, TRANSPORT_STOPS[0])
    spec, recovery = admitted_resume(store, broker, state, monkeypatch)
    flow = DeliveryWorkflow()
    launched = {}
    resume_probe(flow, launched, monkeypatch)
    # The role activity's failure is absorbed by the run; the request is what matters.
    asyncio.run(flow._resume_stopped(spec, recovery))
    assert launched['role'] == 'implement'
    assert launched['iteration'] == state['iteration'] + 1
    assert launched['findings'] == [TRANSPORT_STOPS[0]]


def test_interrupted_implementation_resumes_without_the_transport_error_as_findings(
    stopped, monkeypatch,
):
    store, _, state, _, _ = stopped
    spec = store.spec('run-1')
    state['roles'] = [r for r in state['roles'] if r.get('iteration') < state['iteration']]
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
    assert launched['findings'] == []


@pytest.mark.parametrize('outcome', ['pending', 'lost'])
def test_ci_resume_classifies_its_own_deadline_and_lost_observation(stopped, monkeypatch,
                                                                    outcome):
    store, _, state, _, _ = stopped
    waiting_for_ci(store, state)
    spec = {**store.spec('run-1'), 'automatic_retry_version': 1}
    recovery = {'execution_spec': spec, 'command': {'additional_iterations': 0},
                'state': state, 'candidate': state['candidate'],
                'publication': state['pull_request']}
    flow = DeliveryWorkflow()
    flow.post_publish_v1 = True
    flow.original_execution = False

    async def execute(name, body, **_kw):
        if name == 'delivery_ci':
            if outcome == 'lost':
                raise activity_error(ApplicationError('lost', type='TimeoutExpired'))
            return {'state': 'pending', 'reason': 'CI deadline reached'}
        return {'state': 'consistent'}

    async def project(*_args):
        pass

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_ci(spec, recovery))
    assert result['outcome'] == 'blocked'
    failure = result['checks']['failure']
    assert failure['classification'] == 'transient'
    assert failure['cause_type'] == ('ci_deadline' if outcome == 'pending' else 'TimeoutExpired')
    assert failure['stage'] == 'waiting_ci'


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
                           'session_id': 'repair-session', 'cleanup': 'confirmed',
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


@pytest.mark.parametrize('stage', ['uncommitted', 'committed', 'pushed'])
def test_stalled_repair_publication_is_admitted_for_completion_by_identity(
    service, monkeypatch, stage,
):
    store, broker, state, _closed, candidate = stalled_repair(service, monkeypatch, stage)
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    assert (head == candidate['head']) is (stage == 'uncommitted')
    admitted = store.recover_publication('run-1', completion_request(broker, state, candidate))
    assert admitted['phase'] == 'publication_recovery_queued'
    assert admitted['implementation_authority'] is False
    assert admitted['workflow_id'] == 'delivery-run-1-publication-retry-1'
    recovery, _, _ = current_recovery(store)
    assert recovery['seal']['pushed'] is (stage == 'pushed')
    assert recovery['seal']['published']['number'] == 1
    assert recovery['seal']['candidate']['head'] == head
    assert recovery['state']['pull_request']['number'] == 1
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()


@pytest.mark.parametrize('drift', ['pr_number', 'remote', 'content', 'rewound_push'])
def test_stalled_repair_publication_keeps_its_identity_refusals(service, monkeypatch, drift):
    stage = 'pushed' if drift == 'rewound_push' else 'committed'
    store, broker, state, _closed, candidate = stalled_repair(service, monkeypatch, stage)
    if drift == 'pr_number':
        monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_a, **_kw: {
            'number': 2, 'url': 'https://example.invalid/pull/2', 'state': 'OPEN',
            'isDraft': False, 'headRefOid': candidate['head']})
    elif drift in {'remote', 'rewound_push'}:
        # origin shows a foreign head, or no longer the push this run confirmed.
        foreign = candidate['head'] if drift == 'rewound_push' else 'f' * 40
        monkeypatch.setattr('devflow_temporal.delivery_pending_publication._git',
                            lambda path, *args: f'{foreign}\trefs/heads/feat/fixture'
                            if 'ls-remote' in args else _git(path, *args))
    else:
        (broker.checkout / 'README.md').write_text('Changed after checking\n')
    with pytest.raises(ValueError):
        store.recover_publication('run-1', completion_request(broker, state, candidate))


def test_service_loop_completes_a_stalled_repair_publication_within_bounds(service,
                                                                           monkeypatch):
    from devflow_temporal.delivery_pending_publication import PUBLICATION_RETRIES

    store, broker, state, closed, candidate = stalled_repair(service, monkeypatch,
                                                             'uncommitted')
    for generation in range(1, PUBLICATION_RETRIES + 1):
        clear_waits(store)
        admitted = retry.retry_once(store)
        assert [item['phase'] for item in admitted] == ['publication_recovery_queued']
        assert admitted[0]['workflow_id'] == f'delivery-run-1-publication-retry-{generation}'
        recovery, _, _ = current_recovery(store)
        assert recovery['command']['command_id'].startswith('automatic-publication-')
        assert recovery['command']['expected_head'] == candidate['head']
        assert recovery['namespace'] == 'publication-retry' + (
            f'-{generation}' if generation > 1 else '')
        assert run_count(store) == 1
        stop_again(store, state, closed, STALL, stage='publishing_pending',
                   cause_type='publication_deadline')
    clear_waits(store)
    assert retry.retry_once(store) == []
    with pytest.raises(ValueError, match='bounded publication retries'):
        store.recover_publication('run-1', completion_request(broker, state, candidate,
                                                              command_id='one-too-many'))


def test_pending_publication_resume_publishes_the_unpushed_repair(service, monkeypatch):
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.patched', lambda _: True)
    monkeypatch.setattr('devflow_temporal.delivery_workflow.workflow.now',
                        lambda: datetime.now(UTC))
    store, broker, state, _closed, candidate = stalled_repair(service, monkeypatch, 'committed')
    store.recover_publication('run-1', completion_request(broker, state, candidate))
    spec = store.effective_spec('run-1')
    recovery, _, _ = current_recovery(store)
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    published = {'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
                 'head': head, 'base': spec['base_sha'],
                 'candidate': DeliveryBroker(store, spec).candidate()}
    flow = DeliveryWorkflow()
    flow.publication_v1 = flow.post_publish_v1 = True
    calls = []

    async def execute(name, body, **_kw):
        calls.append((name, body.get('role')))
        if name in {'delivery_tracker_start', 'delivery_tracker'}:
            return {'state': 'consistent'}
        if name == 'delivery_publish':
            assert body['iteration'] == 1 and body['candidate'] == state['candidate']
            return published
        if name == 'delivery_role':
            return {'status': 'pass', 'role': body['role'], 'candidate': published['candidate'],
                    'session_id': 'independent-' + body['role']}
        return {'state': 'passed', 'cleanup': 'confirmed'}

    async def project(*_args):
        pass

    monkeypatch.setattr(flow, '_activity', execute)
    monkeypatch.setattr(flow, '_project', project)
    result = asyncio.run(flow._resume_pending_publication(spec, recovery))
    assert result['outcome'] == 'delivered'
    assert result['pull_request'] == published
    assert ('delivery_publish', None) in calls
    assert ('delivery_reconcile_publish', None) not in calls
    assert ('delivery_role', 'implement') not in calls
    assert [role for name, role in calls if name == 'delivery_role'] == ['review', 'verify']


@pytest.mark.asyncio
async def test_stalled_repair_publication_heals_through_the_real_stop(service, monkeypatch):
    """Iteration 0 publishes; the repair's publication loses GitHub before its push
    for longer than the readback window; the service loop completes it on the
    existing PR without an implementation turn."""
    from temporalio import activity
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    from test_delivery_publication_self_heal import FakeGitHub, remote_head

    from devflow_temporal.delivery_activities import (
        delivery_gates_readback,
        delivery_project,
        delivery_publish,
        delivery_reconcile_publish,
    )

    store, request = service
    original_admit = type(store.config).admit

    def closing_admit(config, supplied):
        admitted = original_admit(config, supplied)
        admitted.update(resource_cleanup_version=1, terminal_tracker_version=1)
        return admitted

    monkeypatch.setattr(type(store.config), 'admit', closing_admit)
    store.submit(request)
    spec = store.spec('run-1')
    assert spec['automatic_retry_version'] == 1 and spec['publication_readback_seconds'] == 900
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.state_dir.mkdir(parents=True, exist_ok=True)
    _git(broker.checkout, 'config', 'user.name', 'Delivery Test')
    _git(broker.checkout, 'config', 'user.email', 'delivery@example.invalid')
    base_candidate = broker.candidate()
    gh = FakeGitHub(broker)
    monkeypatch.setattr(delivery_broker, '_run', gh)
    monkeypatch.setattr(delivery_broker.time, 'sleep', lambda _seconds: None)
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda given, **_kw: (store, DeliveryBroker(store, given)))
    monkeypatch.setattr('devflow_temporal.delivery_pending_publication._stopped_cleanup',
                        lambda _: {})
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
        seen['roles'].append((payload['role'], payload['iteration']))
        working = DeliveryBroker(store, payload['spec'])
        if payload['role'] == 'implement':
            (working.checkout / 'README.md').write_text(
                f"Owned edit {payload['iteration']}\n")
            if payload['iteration'] == 1:
                # GitHub becomes unreachable right before the repair's publication.
                gh.list_failures = 10 ** 6
        # The repair resumes the implementer's session; independent roles are fresh.
        session = ('implementer' if payload['role'] == 'implement'
                   else f"{payload['role']}-{payload['iteration']}")
        result = {'status': 'pass', 'role': payload['role'], 'iteration': payload['iteration'],
                  'session_id': session, 'cleanup': 'confirmed',
                  'candidate': working.candidate()}
        if payload['role'] == 'review' and payload['iteration'] == 0:
            result.update(status='findings', findings=['Needs a second edit'])
        return result

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
    queue = 'stalled-repair-publication'
    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with Worker(environment.client, task_queue=queue, workflows=[DeliveryWorkflow],
                          activities=activities):
            handle = await environment.client.start_workflow(
                DeliveryWorkflow.run, spec, id='delivery-run-1', task_queue=queue)
            stopped = await handle.result()
            first_head = stopped['pull_request']['head']
            assert stopped['outcome'] == 'blocked' and stopped['iteration'] == 1
            assert stopped['error'] == STALL
            assert stopped['checks']['failure']['cause_type'] == 'publication_deadline'
            assert stopped['cleanup'] == 'confirmed'
            assert seen['uncertain'] == [False] and seen['release'] == [True]
            assert stopped['pull_request']['number'] == 1
            assert remote_head(broker) == first_head
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == first_head
            assert (broker.checkout / 'README.md').read_text() == 'Owned edit 1\n'
            with store._connect() as db:
                row = dict(db.execute(
                    "SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
                store.state.release_work(db, spec['work_id'], 'external:devflow:run-1')
                db.execute("UPDATE delivery_runs SET updated_at=? WHERE run_id='run-1'",
                           (PAST,))
                assert db.execute("SELECT COUNT(*) FROM delivery_effects WHERE kind='publish' "
                                  "AND state='pending'").fetchone()[0] == 0
            assert (row['phase'], row['cleanup'], row['iteration']) == ('blocked', 'confirmed', 1)
            closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': handle.result_run_id,
                      'closed_at': '2026-10-12T00:00:00Z',
                      'request_digest': spec['request_digest'], 'recovery_digest': None,
                      'result': stopped}
            monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: closed)
            gh.list_failures = 0
            clear_waits(store)
            admitted = retry.retry_once(store)
            assert [item['phase'] for item in admitted] == ['publication_recovery_queued']
            assert admitted[0]['implementation_authority'] is False
            assert run_count(store) == 1
            store.mark_start('run-1', accepted=True)
            recovery, _, workflow_id = current_recovery(store)
            assert recovery['command']['command_id'].startswith('automatic-publication-')
            assert recovery['seal']['pushed'] is False
            effective = store.effective_spec('run-1')
            resumed = await environment.client.execute_workflow(
                DeliveryWorkflow.run, args=[effective, recovery], id=workflow_id,
                task_queue=queue)
    repaired = _git(broker.checkout, 'rev-parse', 'HEAD')
    assert resumed['outcome'] == 'delivered'
    assert resumed['pull_request']['number'] == 1
    assert resumed['pull_request']['head'] == repaired != first_head
    assert remote_head(broker) == repaired
    assert _git(broker.checkout, 'rev-list', '--count', spec['base_sha'] + '..HEAD') == '2'
    assert len(gh.creations()) == 1
    assert seen['roles'] == [('implement', 0), ('review', 0), ('implement', 1),
                             ('review', 1), ('verify', 1)]
    with store._connect() as db:
        assert db.execute("SELECT state FROM delivery_effects WHERE effect_key=?",
                          ('publish:run-1:1',)).fetchone()[0] == 'complete'
