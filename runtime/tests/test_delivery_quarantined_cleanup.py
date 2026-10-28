"""Real resource readers keep unknown provider verdicts separate from proven teardown."""
from __future__ import annotations

import hashlib
import json

import pytest
from test_delivery_completed_resources import completed as completed
from test_delivery_store import service as service

from devflow_temporal import delivery_resources as resources
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_resources import (
    observe_completed_resources,
    observe_finalized_resources,
    read_private,
    write_private,
)


@pytest.fixture
def quarantined(completed):
    spec, row, path, journal, request, table, ports = completed
    proof = {'kind': 'interrupted_monitor', 'version': 1, 'previous_phase': 'authorized',
             'intent_sha256': digest(journal['intent']), 'owned_sha256': digest(journal['owned'])}
    native = {'journal': str(path), 'cleanup': 'observed-native-confirmed',
              'provider_outcome': 'unknown', 'interrupted_monitor': True,
              'monitoring_complete': False}
    journal.update(monitoring_complete=False, result=native, cleanup_reconciliation=proof)
    result = {'status': 'recovery_unknown', 'finish_reason': 'interrupted_monitor',
              'cleanup': 'confirmed', 'session_id': None, 'native_process': native}
    row.update(session_id=None, result_json=json.dumps(result))
    journal['provider_session'].update(session_id=None, result_digest=digest(result))
    write_private(path, journal)
    final_path = path.parent.parent.parent / 'resources/finalization.json'
    final = read_private(final_path)
    final.update(state='confirmed', resource_cleanup='confirmed',
                 process_cleanup='observed-native-confirmed')
    final['processes'][0].update(cleanup='observed-native-confirmed', owned_ports_clear=True)
    write_private(final_path, final)
    checkpoint = {'journal_sha256': {str(path): hashlib.sha256(path.read_bytes()).hexdigest()}}
    return spec, row, path, journal, request, table, ports, checkpoint


def test_unknown_provider_observation_requires_explicit_exact_teardown_checkpoint(quarantined):
    spec, row, path, journal, _request, _table, _ports, checkpoint = quarantined
    before = path.read_bytes()
    with pytest.raises(ValueError):
        observe_completed_resources(spec, [row])
    with pytest.raises(ValueError):
        observe_finalized_resources(spec)
    assert observe_completed_resources(spec, [row], quarantined=checkpoint)['journal_sha256'] == (
        checkpoint['journal_sha256'])
    assert observe_finalized_resources(spec, quarantined=checkpoint)['journal_sha256'] == (
        checkpoint['journal_sha256'])
    assert path.read_bytes() == before
    assert journal['monitoring_complete'] is False
    assert journal['result']['provider_outcome'] == 'unknown'


def test_quarantine_hash_authenticates_the_same_bytes_used_for_actor_observation(
    quarantined, monkeypatch,
):
    spec, row, path, journal, _request, table, _ports, checkpoint = quarantined
    journal['owned']['21'] = {'identity': 'live-child'}
    journal['cleanup_reconciliation']['owned_sha256'] = digest(journal['owned'])
    write_private(path, journal)
    checkpoint['journal_sha256'][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    before = path.read_bytes()
    table[21] = {'identity': 'live-child', 'stat': 'S'}
    altered = json.loads(before)
    del altered['owned']['21']
    altered['cleanup_reconciliation']['owned_sha256'] = digest(altered['owned'])
    original = resources.read_private
    def racing_read(target, *, raw=False):
        # A different observed generation cannot borrow the physical path's hash.
        if target == path:
            return json.dumps(altered).encode() if raw else altered
        return original(target, raw=raw)
    monkeypatch.setattr(resources, 'read_private', racing_read)
    with pytest.raises(ValueError, match='teardown binding changed'):
        observe_completed_resources(spec, [row], quarantined=checkpoint)
    assert path.read_bytes() == before
