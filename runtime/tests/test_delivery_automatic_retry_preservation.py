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


@pytest.mark.parametrize('drift', [None, 'producer_hash', 'latest_hash'])
def test_published_cleanup_keeps_historical_bytes_after_owner_refresh(later_cleanup, drift):
    store, request, historical, calls, external = later_cleanup
    root, original = files(store, request)
    old_receipt = {**json.loads(original['finalization.json']),
                   'receipt': str(root / 'finalization.json'),
                   'receipt_sha256': hashlib.sha256(original['finalization.json']).hexdigest()}
    historical['result']['checks']['resource_cleanup'] = old_receipt
    if drift == 'producer_hash':
        old_receipt['receipt_sha256'] = '0' * 64
    write_private(root / 'finalization.json', {'state': 'confirmed', 'outcome': 'blocked'})
    latest = (root / 'finalization.json').read_bytes()
    latest_hash = hashlib.sha256(latest).hexdigest()
    current = {**json.loads(latest), 'receipt': str(root / 'finalization.json'),
               'receipt_sha256': latest_hash if drift != 'latest_hash' else '1' * 64}
    with store._connect() as db:
        checks = json.loads(db.execute('SELECT checks_json FROM delivery_runs').fetchone()[0])
        checks['resource_cleanup'] = current
        db.execute('UPDATE delivery_runs SET checks_json=?', (json.dumps(checks),))
        row = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
    external.update(ready=True, hash=latest_hash)
    before = json.dumps(historical, sort_keys=True)
    if drift:
        with pytest.raises(ValueError):
            retry._close_cleanup(store, row, store.spec(request['run_id']), historical,
                                 lambda: False, published=True)
        assert not any(item[0] in {'finalize', 'tracker'} for item in calls)
        assert not (root / 'predecessor-resources').exists()
    else:
        result = retry._close_cleanup(store, row, store.spec(request['run_id']), historical,
                                      lambda: False, published=True)
        assert result['cleanup'] == 'confirmed'
        archive = root / 'predecessor-resources'
        assert (archive / 'finalization.json').read_bytes() == original['finalization.json']
        assert (archive / 'observed-finalization.json').read_bytes() == latest
    assert json.dumps(historical, sort_keys=True) == before
