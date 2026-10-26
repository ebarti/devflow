"""Completed no-launch reservations retain their typed producer and original custody.

Unit cases use opaque actor/port tables; the producer, DB, requests, leases,
resource registry, finalizer and public cleanup readers are real owned objects.
"""
from __future__ import annotations

import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path

import pytest
from test_delivery_published_quarantine_cleanup import (
    _later_compact_maintenance,
)
from test_delivery_published_quarantine_cleanup import (
    published_after_unknown as published_after_unknown,
)
from test_delivery_quarantined_authority import native_quarantine as native_quarantine
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal import delivery_native_process as native
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_orphans import reconcile_attempt
from devflow_temporal.delivery_resources import (
    RunResources,
    observe_completed_resources,
    read_private,
    write_private,
)
from devflow_temporal.supervisor import DeliverySupervisor


def _prelaunch(store, spec, broker, iteration=0):
    """Use the existing server producer, without fabricating a native journal."""
    candidate = broker.candidate()
    workspace = broker.gate_checkout('verify', iteration, candidate)
    request = {'spec': spec, 'role': 'verify', 'iteration': iteration,
               'workspace': str(workspace), 'candidate': candidate, 'resume_session': None}
    owner = DeliverySupervisor(store, capacity=2)
    key, old = owner._claim(request)
    assert old is None
    folder = Path(spec['state_dir']) / 'attempts' / key
    request.update(native_authorized=True, result_path=str(folder / 'result.json'),
                   start_path=str(folder / 'start.json'))
    write_private(folder / 'request.json', request)
    lease = owner._owner_lock(spec, key)
    assert lease is not None
    try:
        with store._connect() as db:
            row = dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                                  (key,)).fetchone())
        result = owner._native_failure(request, key, row,
            'configured browser QA receipt is required for verification', launch_absent=True)
    finally:
        os.close(lease)
    assert result['finish_reason'] == 'prelaunch' and result['cleanup'] == 'confirmed'
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                              (key,)).fetchone())
    assert not (folder / 'native-process.json').exists()
    return row, request, folder


@pytest.fixture
def no_launch(service, monkeypatch):
    store, submission = service
    store.submit(submission)
    spec = store.spec('run-1')
    spec.update(provider='codex', resource_cleanup_version=1)
    spec['policy'].update(execution_backend='native-macos', host_sandbox='trusted-local')
    spec['policy_digest'] = digest(spec['policy'])
    # Neutral historical-controller fixture; no provider identity is impersonated.
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=?', (canonical_json(spec),))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    row, request, folder = _prelaunch(store, spec, broker)
    monkeypatch.setattr(native, 'process_table', lambda: {})
    monkeypatch.setattr(native, 'listeners', lambda _port: set())
    assert RunResources(spec).finalize('blocked')['state'] == 'confirmed'
    return store, spec, row, request, folder


def test_completed_typed_prelaunch_needs_no_fictitious_registration(no_launch):
    _store, spec, row, request, folder = no_launch
    registry = RunResources(spec)
    original = {p: p.read_bytes() for p in [folder / 'request.json', registry.manifest,
                                           registry.state / 'resources/finalization.json']}
    assert request['workspace'] in read_private(registry.manifest)['roots']
    assert read_private(registry.manifest)['roots'][request['workspace']]['identity']
    executions = {}
    from devflow_temporal.delivery_resources import _gate_roots
    _gate_roots(spec, execution_specs=executions)
    assert request['spec'] in executions.values()
    proof = observe_completed_resources(spec, [row])
    assert proof['journal_sha256'] == {}
    saved = proof['prelaunch_attempts'][str(folder / 'native-process.json')]
    assert saved['attempt_sha256'] == digest(row)
    assert saved['request_sha256'] == hashlib.sha256(original[folder / 'request.json']).hexdigest()
    assert saved['result_sha256'] == digest(json.loads(row['result_json']))
    assert saved['leases']['native-monitor.lock'] is None
    assert all(path.read_bytes() == raw for path, raw in original.items())
    assert read_private(folder / 'request.json') == request
    assert not (folder / 'native-process.json').exists()


@pytest.mark.parametrize('drift', [
    'unknown', 'untyped', 'pass', 'session', 'pid', 'process_identity', 'usage', 'forged_native',
    'wrong_role', 'wrong_iteration', 'wrong_input', 'foreign_spec', 'unauthorized',
    'wrong_result_path', 'wrong_start_path', 'foreign_workspace', 'missing_request',
    'missing_owner', 'owner_link', 'owner_busy', 'native_busy', 'journal', 'launch',
    'request_fifo', 'request_mode', 'request_link', 'request_symlink',
    'ready', 'start', 'result', 'cancel',
])
def test_absence_or_null_session_cannot_replace_original_prelaunch_authority(no_launch, drift):
    _store, spec, row, request, folder = no_launch
    row = copy.deepcopy(row)
    result = json.loads(row['result_json'])
    held = None
    if drift == 'unknown':
        result['status'] = result['finish_reason'] = 'recovery_unknown'
    elif drift == 'untyped':
        result.pop('finish_reason')
    elif drift == 'pass':
        result['status'] = 'pass'
    elif drift in {'session', 'pid', 'process_identity'}:
        row['session_id' if drift == 'session' else drift] = 'unexpected-provider'
    elif drift == 'usage':
        result['usage'] = {'tokens': 1}
    elif drift == 'forged_native':
        result['native_process'] = {'provider_outcome': 'unknown', 'cleanup': 'confirmed'}
    elif drift.startswith('wrong_') or drift in {'foreign_spec', 'unauthorized',
                                                'foreign_workspace'}:
        if drift == 'wrong_role':
            request['role'] = 'review'
        elif drift == 'wrong_iteration':
            request['iteration'] = 1
        elif drift == 'wrong_input':
            request['candidate']['id'] = digest('other-input')
        elif drift == 'foreign_spec':
            request['spec'] = {**spec, 'request_digest': digest('foreign')}
        elif drift == 'unauthorized':
            request['native_authorized'] = False
        elif drift == 'foreign_workspace':
            request['workspace'] = str(folder)
        else:
            request['result_path' if drift == 'wrong_result_path' else 'start_path'] += '.other'
        write_private(folder / 'request.json', request)
    elif drift == 'missing_request':
        (folder / 'request.json').unlink()
    elif drift.startswith('request_'):
        path = folder / 'request.json'
        if drift == 'request_fifo':
            path.unlink()
            os.mkfifo(path, mode=0o600)
        elif drift == 'request_mode':
            path.chmod(0o644)
        elif drift == 'request_link':
            os.link(path, path.with_suffix('.linked'))
        else:
            path.rename(path.with_suffix('.original'))
            path.symlink_to(path.with_suffix('.original'))
    elif drift in {'missing_owner', 'owner_link', 'owner_busy'}:
        path = DeliverySupervisor._owner_lock_path(spec, row['job_key'])
        if drift == 'missing_owner':
            path.unlink()
        elif drift == 'owner_link':
            os.link(path, path.with_suffix('.linked'))
        else:
            held = path.open('rb')
            fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    elif drift == 'native_busy':
        path = folder / 'native-monitor.lock'
        path.touch(mode=0o600)
        held = path.open('rb')
        fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        name = 'native-process.json' if drift == 'journal' else (
            drift if drift == 'cancel' else drift + '.json')
        write_private(folder / name, {'opaque': 'possible prior launch'})
    row['result_json'] = canonical_json(result)
    try:
        with pytest.raises((ValueError, OSError)):
            observe_completed_resources(spec, [row])
    finally:
        if held is not None:
            held.close()


def test_request_change_while_native_roots_are_observed_refuses(no_launch, monkeypatch):
    _store, spec, row, request, folder = no_launch
    original = RunResources._allowed

    def replace_after_leases(self, path, kind, **kwargs):
        request['candidate']['id'] = digest('changed-during-observation')
        write_private(folder / 'request.json', request)
        return original(self, path, kind, **kwargs)

    monkeypatch.setattr(RunResources, '_allowed', replace_after_leases)
    with pytest.raises(ValueError, match='evidence generation changed'):
        observe_completed_resources(spec, [row])


def test_public_cleanup_preserves_prelaunch_rows_and_replays_after_maintenance(
    published_after_unknown,
):
    value = published_after_unknown
    broker = DeliveryBroker(value.store, value.current)
    attempts = [_prelaunch(value.store, value.current, broker, i)
                for i in (0, 1)]
    RunResources(value.current).finalize('blocked')
    before = {str(folder / 'request.json'): (folder / 'request.json').read_bytes()
              for _row, _request, folder in attempts}
    _row, anchor, _receipt = _later_compact_maintenance(value)
    assert len(anchor['native_observation']['prelaunch_attempts']) == 2
    assert reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)[
        'changed'] is False
    assert all(Path(path).read_bytes() == raw for path, raw in before.items())
    with value.store._connect() as db:
        assert value.store.state.claim_for(db, value.current['work_id']) is None
        for row, _request, folder in attempts:
            assert dict(db.execute('SELECT * FROM delivery_attempts WHERE job_key=?',
                                   (row['job_key'],)).fetchone()) == row
            assert not (folder / 'native-process.json').exists()
    assert value.case.journal.read_bytes() == value.original_unknown


@pytest.mark.parametrize('when', ['before_tracker', 'after_tracker', 'replay'])
def test_public_cleanup_cannot_ack_changed_no_launch_authority(
    published_after_unknown, monkeypatch, when,
):
    value = published_after_unknown
    _row, request, folder = _prelaunch(
        value.store, value.current, DeliveryBroker(value.store, value.current))
    RunResources(value.current).finalize('blocked')

    def drift():
        request['workspace'] += '-changed'
        write_private(folder / 'request.json', request)

    if when == 'replay':
        reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)
        drift()
    elif when == 'before_tracker':
        old = retry.RunResources.finalize

        def finalize(*args, **kwargs):
            result = old(*args, **kwargs)
            drift()
            return result
        monkeypatch.setattr(retry.RunResources, 'finalize', finalize)
    else:
        old = retry._tracker_sync

        def tracker(*args, **kwargs):
            result = old(*args, **kwargs)
            drift()
            return result
        monkeypatch.setattr(retry, '_tracker_sync', tracker)
    with pytest.raises(ValueError):
        reconcile_attempt(value.store, run_id='run-1', role='cleanup', iteration=1)
