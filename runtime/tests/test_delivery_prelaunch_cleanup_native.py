"""Owned native preparation/registration/finalizer/public cleanup, no provider call.

Only GitHub publication and the closed workflow descriptor are controlled seams.
The imported bundle, native preparation, failed command, no-launch supervisor,
resources, local tracking/claim closure and public reconciliation are real.
"""
from __future__ import annotations

import asyncio
import copy
import json
import sys
from datetime import UTC, datetime
from importlib.metadata import distribution
from pathlib import Path

from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_resources import RunResources, read_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


def test_actual_native_preparation_no_launch_history_then_public_cleanup(service, monkeypatch):
    owner, submission = service
    config = copy.deepcopy(owner.config.raw)
    config.update(provider='codex', execution_mode='trusted-local', max_repairs=10,
                  codex_bin=str(distribution('openai-codex-cli-bin').locate_file(
                      'codex_cli_bin/bin/codex')))
    config['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'max'}
                       for role in ('implement', 'review', 'verify')}
    prerequisite = {'id': 'owned-failed-prerequisite', 'kind': 'command',
        'argv': [str(Path(sys.executable).resolve()), '-c',
                 'print("Owned locked dependency unavailable before pytest");raise SystemExit(2)'],
        'timeout_seconds': 10}
    config['repositories']['fixture'].update(project_url='https://github.com/users/test/projects/1',
        assignee='test', required_ci=['owned-ci'], checks=[prerequisite],
        prepublish_checks=[{'id': 'owned-prepublish', 'argv': ['/usr/bin/true']}], browser_qa={
            'id': 'owned-browser', 'argv': ['/usr/bin/false'], 'cwd': '.',
            'ports': {'QA_API_PORT': 43127, 'QA_WEB_PORT': 43128}, 'timeout_seconds': 10,
            'env': {'JOBCTRL_E2E_ISOLATED': '1'}, 'artifact_paths': [],
            'test_count_regex': r'(\d+) passed', 'min_tests': 1})
    owner.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(owner.config.path))
    store.submit(submission)
    store.mark_start('run-1', accepted=True)
    spec = prepare_authority(store, store.submitted_spec('run-1'))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    broker.checkout.joinpath('README.md').write_text('Owned published source\n')
    _git(broker.checkout, 'add', 'README.md')
    _git(broker.checkout, 'commit', '-qm', 'Publish owned cleanup fixture')
    _git(broker.checkout, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    candidate = broker.candidate()
    supervisor = DeliverySupervisor(store, capacity=2)
    roles = []
    for iteration in (0, 1, 2):
        workspace = broker.gate_checkout('verify', iteration, candidate)
        request = {'spec': spec, 'role': 'verify', 'iteration': iteration,
                   'candidate': candidate, 'workspace': str(workspace), 'resume_session': None,
                   'review_diff': broker.gate_diff('verify', iteration, candidate)}
        result = asyncio.run(supervisor.run(request))
        assert result['findings'] == [
            'configured browser QA receipt is required for verification']
        assert result['finish_reason'] == 'prelaunch' and result['session_id'] is None
        roles.append({**result, 'role': 'verify', 'iteration': iteration, 'candidate': candidate})
    failed = broker._run_check_list(broker.checkout, spec['policy']['checks'],
                                    broker.evidence_dir / 'checks/3', candidate)
    assert failed['state'] == 'failed' and failed['results'][0]['exit_code'] == 2
    journal = failed['results'][0]['native_process']['journal']
    assert read_private(Path(journal))['monitoring_complete'] is True
    registry = RunResources(spec)
    assert registry.finalize('blocked')['state'] == 'confirmed'
    before_manifest = read_private(registry.manifest)
    assert journal in before_manifest['processes']
    with store._connect() as db:
        attempts = [dict(r) for r in db.execute('SELECT * FROM delivery_attempts')]
    assert len(attempts) == 3
    missing = {str(Path(r['result_path']).with_name('native-process.json')) for r in attempts}
    assert not missing & set(before_manifest['processes'])
    assert all(not Path(p).exists() for p in missing)
    publication = {'number': 12, 'url': 'https://github.com/example/fixture/pull/12',
                   'head': candidate['head'], 'candidate': candidate,
                   'base': spec['base_sha'], 'state': 'OPEN'}
    checks = {'local': failed, 'resource_cleanup': {
        'state': 'unknown', 'process_cleanup': 'unknown', 'resource_cleanup': 'unknown',
        'reason': 'ActivityError'}}
    state = {'run_id': 'run-1', 'revision': 55, 'iteration': 3, 'phase': 'blocked',
             'outcome': 'blocked', 'execution_state': 'blocked', 'cleanup': 'unknown',
             'candidate': candidate, 'pull_request': publication, 'roles': roles,
             'checks': checks, 'error': 'local command failed', 'tracker': None}
    store.project('run-1', phase='blocked', execution_state='blocked', outcome='blocked',
        protocol_revision=55, iteration=3, cleanup='unknown', error=state['error'],
        candidate=candidate, pull_request=publication, checks=checks,
        event_type='blocked', message='Owned closed finalizer placeholder')
    closed = {'workflow_id': 'delivery-run-1', 'execution_run_id': 'owned-closed-native',
              'closed_at': datetime.now(UTC).isoformat(), 'request_digest': spec['request_digest'],
              'recovery_digest': None, 'result': state}
    original = copy.deepcopy(closed)
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: closed)
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_a: {
        'number': 12, 'url': publication['url'], 'headRefOid': candidate['head']})
    assert reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=3)['changed']
    assert not reconcile_attempt(store, run_id='run-1', role='cleanup', iteration=3)['changed']
    assert closed == original
    with store._connect() as db:
        assert [dict(r) for r in db.execute('SELECT * FROM delivery_attempts')] == attempts
        assert store.state.claim_for(db, spec['work_id']) is None
        anchor = json.loads(db.execute("SELECT payload_json FROM delivery_events WHERE "
            "type='resource_cleanup_reconciled' AND "
            "json_extract(payload_json,'$.closure')='published'").fetchone()[0])
    assert set(anchor['native_observation']['prelaunch_attempts']) == missing
    assert set(read_private(registry.manifest)['processes']) == set(before_manifest['processes'])
    assert all(not Path(p).exists() for p in missing)
    assert read_private(Path(journal))['monitoring_complete'] is True
    # This is cleanup evidence, never an implementation pass, browser pass or retry grant.
    assert state['checks']['local']['state'] == 'failed'
    assert all(role['status'] == 'blocked' for role in roles)
