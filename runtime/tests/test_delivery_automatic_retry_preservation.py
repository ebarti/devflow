"""Exact-byte preservation with opaque finalization, no native actors or process control."""
import hashlib
import json
from pathlib import Path

import pytest
from test_delivery_automatic_retry_cleanup import later_cleanup as later_cleanup
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.delivery_resources import write_private


def files(store, request):
    root = Path(store.spec(request['run_id'])['state_dir']) / 'resources'
    return root, {name: (root / name).read_bytes()
                  for name in ('manifest.json', 'finalization.json')}


def assert_preserved(store, request, original):
    root, _ = files(store, request)
    archive = root / 'predecessor-resources'
    for name, raw in original.items():
        assert (archive / name).read_bytes() == raw
    with store._connect() as db:
        records = [json.loads(row[0]) for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'")]
    observed = next(row['predecessor_resources'] for row in records
                    if 'predecessor_resources' in row)
    assert observed == {name: hashlib.sha256(raw).hexdigest() for name, raw in original.items()}


@pytest.mark.parametrize('interrupt', ['refusal', 'crash'])
def test_original_bytes_survive_finalize_then_refusal_or_crash(later_cleanup, interrupt):
    store, request, historical, calls, external = later_cleanup
    _root, original = files(store, request)
    external['ready'] = True
    if interrupt == 'crash':
        external['crash_finalizing'] = True
    else:
        observations = [0]
        def refuse_second():
            observations[0] += 1
            if observations[0] == 2:
                raise ValueError('opaque publication readback unavailable after cleanup')
        external['on_publication'] = refuse_second
    saved = json.dumps(historical, sort_keys=True)
    assert retry.retry_once(store) == []
    assert ('finalize', 'blocked', False) in calls
    assert not any(item[0] == 'tracker' for item in calls)
    assert_preserved(store, request, original)
    external.pop('on_publication', None)
    store._automatic_retry_waits.clear()
    assert len(retry.retry_once(store)) == 1
    assert_preserved(store, request, original)
    assert json.dumps(historical, sort_keys=True) == saved
    with store._connect() as db:
        records = [json.loads(row[0]) for row in db.execute(
            "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'")]
    assert sum('predecessor_resources' in row for row in records) == 1


@pytest.mark.parametrize('name', ['manifest.json', 'finalization.json'])
def test_changed_archived_original_refuses_before_further_cleanup(later_cleanup, name):
    store, request, _historical, calls, external = later_cleanup
    root, original = files(store, request)
    external.update(ready=True, crash_finalizing=True)
    assert retry.retry_once(store) == []
    assert_preserved(store, request, original)
    write_private(root / 'predecessor-resources' / name, {'changed': True})
    count = calls.count(('finalize', 'blocked', False))
    store._automatic_retry_waits.clear()
    assert retry.retry_once(store) == []
    assert calls.count(('finalize', 'blocked', False)) == count
    assert not any(item[0] == 'tracker' for item in calls)


def test_pre_effect_event_failure_preserves_bytes_without_cleanup(later_cleanup, monkeypatch):
    import sqlite3
    store, request, _historical, calls, external = later_cleanup
    root, original = files(store, request)
    external['ready'] = True
    event = store._event
    def fail_observation(*args, **kwargs):
        if args[3] == 'resource_cleanup_reconciled':
            raise sqlite3.OperationalError('opaque observation transaction refused')
        return event(*args, **kwargs)
    monkeypatch.setattr(store, '_event', fail_observation)
    assert retry.retry_once(store) == []
    assert not any(item[0] in {'finalize', 'tracker'} for item in calls)
    for name, raw in original.items():
        assert (root / name).read_bytes() == raw
        assert (root / 'predecessor-resources' / name).read_bytes() == raw
    monkeypatch.setattr(store, '_event', event)
    store._automatic_retry_waits.clear()
    assert len(retry.retry_once(store)) == 1
    assert_preserved(store, request, original)


@pytest.mark.parametrize('name', ['manifest.json', 'finalization.json'])
def test_changed_original_during_readback_refuses_before_archive_effect(later_cleanup, name):
    store, request, _historical, calls, external = later_cleanup
    root, _original = files(store, request)
    external['ready'] = True
    external['on_publication'] = lambda: write_private(root / name, {'changed': True})
    assert retry.retry_once(store) == []
    assert not any(item[0] in {'finalize', 'tracker'} for item in calls)
    assert not (root / 'predecessor-resources').exists()
