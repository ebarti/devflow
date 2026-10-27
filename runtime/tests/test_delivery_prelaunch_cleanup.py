"""Completed no-launch reservations retain their typed producer and original custody.

Unit cases use opaque actor/port tables; the producer, DB, requests, leases,
resource registry, finalizer and public cleanup readers are real owned objects.
"""
from __future__ import annotations

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
