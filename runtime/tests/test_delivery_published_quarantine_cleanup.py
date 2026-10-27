"""Published cleanup retains authenticated old unknowns across later completion."""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_closed_cleanup_projection import _maintenance
from test_delivery_quarantined_authority import admitted
from test_delivery_quarantined_authority import native_quarantine as native_quarantine
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_policy_recovery import reconciled_cleanup
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_sandbox import _native_role_home
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def published_after_unknown(native_quarantine, monkeypatch):
    case = native_quarantine
    response, current, recovery = admitted(case)
    case.store.mark_start('run-1', accepted=True)
    original_unknown = case.journal.read_bytes()
    broker = DeliveryBroker(case.store, current)
    before = broker.candidate()
    (broker.checkout / 'README.md').write_text('Owned later completed implementation\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-qm', 'Complete owned later iteration')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + current['branch'])
    candidate = broker.candidate()
    request = {'spec': current, 'role': 'implement', 'iteration': 1,
               'candidate': before, 'workspace': current['checkout'], 'resume_session': None,
               'native_authorized': True}
    key = DeliverySupervisor._job_key(request)
    folder = Path(current['state_dir']) / 'attempts' / key
    request.update(result_path=str(folder / 'result.json'), start_path=str(folder / 'start.json'))
    home = _native_role_home(request)
    session = 'owned-later-implementation-session'
    (home / 'codex/sessions').mkdir(mode=0o700, parents=True)
    (home / 'codex/sessions' / (session + '.jsonl')).write_text('Owned retained session\n')
    launch = {'argv': ['owned-provider'], 'environment': {'CODEX_HOME': str(home / 'codex')}}
    intent = {'run_id': 'run-1', 'policy_digest': current['policy_digest'],
              'cwd': current['checkout'], 'argv': launch['argv'],
              'environment_sha256': digest(launch['environment']), 'timeout': 10, 'ports': []}
    outcome = {'journal': str(folder / 'native-process.json'), 'state': 'finished',
               'exit_code': 0, 'cleanup': 'observed-native-confirmed',
               'monitoring_complete': True, 'stdio_drained': True,
               'cancelled': False, 'timed_out': False}
    raw = {'status': 'pass', 'summary': 'Owned later completion', 'findings': [],
           'finish_reason': 'done', 'session_id': session}
    result = {**raw, 'cleanup': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
              'resource_cleanup': 'pending_workflow_finalization', 'native_process': outcome}
    journal = {'phase': 'finished', 'monitoring_complete': True, 'intent': intent,
               'ports': [], 'owned': {'17021': {'identity': 'owned-later-provider'},
                                      '17023': {'identity': 'owned-later-monitor'}},
               'monitor': {'pid': 17023, 'identity': 'owned-later-monitor'}, 'result': outcome,
               'provider_session': {'role': 'implement', 'iteration': 1, 'session_id': session,
                   'resumed_from': None, 'result_digest': digest(result), 'output_candidate': {
                       k: candidate[k] for k in ('id', 'head', 'content_sha256')}}}
    for name, value in [('request.json', request), ('result.json', raw), ('launch.json', launch),
                        ('start.json', {'pid': 17021}), ('native-process.json', journal)]:
        write_private(folder / name, value)
    for name in ('native-process.lock', 'native-monitor.lock'):
        folder.joinpath(name).touch(mode=0o600)
    registry = RunResources(current)
    registry.process(folder / 'native-process.json')
    with case.store._connect() as db:
        db.execute('INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,'
                   'state,session_id,result_json,cleanup,pid,process_identity,result_path) VALUES '
                   "(?,'run-1','implement',1,?,'finished',?,?,'confirmed',17021,"
                   "'owned-later-provider',?)",
                   (key, before['id'], session, canonical_json(result), request['result_path']))
    finalization = registry.finalize('blocked')
    assert finalization['state'] == 'confirmed'
    publication = {'number': 12, 'url': 'https://github.com/example/fixture/pull/12',
                   'head': candidate['head'], 'candidate': candidate,
                   'base': current['base_sha'], 'state': 'OPEN'}
    state = copy.deepcopy(case.state)
    state.update(revision=39, iteration=1, candidate=candidate, pull_request=publication,
                 cleanup='unknown', error='terminal tracker synchronization conflicted',
                 tracker={'state': 'conflicted', 'pending': False})
    state['roles'].append({**result, 'role': 'implement', 'iteration': 1,
                           'input_candidate_id': before['id'], 'candidate': candidate})
    state['checks'] = {'resource_cleanup': {'state': 'unknown', 'process_cleanup': 'unknown',
                                           'resource_cleanup': 'unknown', 'reason': 'ValueError'}}
    case.store.project('run-1', phase='blocked', execution_state='blocked', outcome='blocked',
        protocol_revision=39, iteration=1, candidate=candidate, pull_request=publication,
        event_type='blocked', message=state['error'], error=state['error'], cleanup='confirmed',
        tracker=state['tracker'], checks={**state['checks'], 'resource_cleanup': finalization})
    closed = {'workflow_id': response['workflow_id'], 'execution_run_id': 'later-completed',
              'closed_at': '2026-10-25T12:00:00+00:00', 'request_digest': current['request_digest'],
              'recovery_digest': digest(recovery), 'result': state}
    monkeypatch.setattr(case.store, '_completed_temporal_result', lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_: {
        'number': 12, 'url': publication['url'], 'headRefOid': candidate['head']})
    return SimpleNamespace(case=case, store=case.store, current=current, recovery=recovery,
                           state=state, closed=closed, candidate=candidate, registry=registry,
                           original_unknown=original_unknown, later=folder, publication=publication)


def test_public_published_cleanup_preserves_old_unknown_and_replays(published_after_unknown):
    value = published_after_unknown
    before = copy.deepcopy(value.closed)
    result = reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)
    assert result['changed'] and result['cleanup'] == 'confirmed'
    replay = reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)
    assert replay['changed'] is False
    assert value.closed == before and value.case.journal.read_bytes() == value.original_unknown
    with value.store._connect() as db:
        assert value.store.state.claim_for(db, value.current['work_id']) is None
        records = [json.loads(r[0]) for r in db.execute(
            "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'")]
        saved = [r for r in records if r.get('closure') == 'published']
        assert len(saved) == 1
        proof = saved[0]
        assert len(proof['historical_quarantines']) == 1
        assert proof['quarantined_cleanup']['journal_sha256'] == {
            str(value.case.journal): hashlib.sha256(value.original_unknown).hexdigest()}
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 2
    assert read_private(value.case.journal)['monitoring_complete'] is False
    assert read_private(value.case.journal)['result']['provider_outcome'] == 'unknown'
    command = {'continuation_kind': resume.KIND, 'command_id': 'next-owned-finite',
               'expected_revision': 39, 'expected_iteration': 1,
               'expected_candidate_id': value.candidate['id'],
               'expected_candidate_head': value.candidate['head'], 'additional_iterations': 1}
    assert value.store.repair_admission_preflight('run-1', command)[
        'authorized_through_iteration'] == 2


def _later_compact_maintenance(value):
    assert reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)['changed']
    with value.store._connect() as db:
        anchor = json.loads(db.execute(
            "SELECT payload_json FROM delivery_events WHERE "
            "type='resource_cleanup_reconciled' AND "
            "json_extract(payload_json,'$.closure')='published'").fetchone()[0])
    # A fresh registered transient creates a genuinely new finalization receipt.
    # Use the same real finalizer + compact store.project producer exercised by
    # ordinary closed-run maintenance, without replacing resource observations.
    scratch = value.registry.scratch('maintenance', 'after-published-cleanup')
    (scratch / 'owned.txt').write_text('Owned maintenance scratch\n')
    closed = value.closed
    receipt = _maintenance(value.store, value.current, SimpleNamespace(
        id=closed['workflow_id'], run_id=closed['execution_run_id'],
        close_time=datetime.fromisoformat(closed['closed_at'])), uncertain=False)
    assert receipt['state'] == receipt['resource_cleanup'] == 'confirmed'
    assert receipt['receipt_sha256'] != anchor['resource_cleanup']['receipt_sha256']
    assert not scratch.exists()
    with value.store._connect() as db:
        row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
        latest = json.loads(db.execute(
            "SELECT payload_json FROM delivery_events WHERE "
            "type='resource_cleanup_reconciled' ORDER BY sequence DESC LIMIT 1").fetchone()[0])
    assert set(latest) == {'key', 'iteration', 'candidate_id'}
    assert latest['iteration'] == 1 and latest['candidate_id'] is None
    return row, anchor, receipt


def test_public_published_cleanup_replays_after_compact_maintenance(published_after_unknown):
    value = published_after_unknown
    before = copy.deepcopy(value.closed)
    row, anchor, receipt = _later_compact_maintenance(value)
    assert reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)[
        'changed'] is False
    proof = reconciled_cleanup(value.store, row, value.state, closed=value.closed)
    assert proof['maintenance_chain']['anchor_sha256'] == digest(anchor)
    assert proof['resource_cleanup'] == receipt
    for field in ('historical_quarantines', 'quarantined_cleanup', 'native_observation'):
        assert proof[field] == anchor[field]
    command = {'continuation_kind': resume.KIND, 'command_id': 'finite-after-maintenance',
               'expected_revision': 39, 'expected_iteration': 1,
               'expected_candidate_id': value.candidate['id'],
               'expected_candidate_head': value.candidate['head'], 'additional_iterations': 1}
    assert value.store.repair_admission_preflight('run-1', command)[
        'authorized_through_iteration'] == 2
    assert value.closed == before and value.case.journal.read_bytes() == value.original_unknown
    assert read_private(value.case.journal)['monitoring_complete'] is False
    assert read_private(value.case.journal)['result']['provider_outcome'] == 'unknown'
