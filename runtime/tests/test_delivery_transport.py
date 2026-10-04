from __future__ import annotations

import hashlib
import random
import string
from copy import deepcopy
from pathlib import Path

import pytest
from temporalio.converter import DataConverter

from devflow_temporal import delivery_native_preparation as native
from devflow_temporal import delivery_transport_adoption as adoption
from devflow_temporal.delivery_broker import _git
from devflow_temporal.delivery_codec import DELIVERY_DATA_CONVERTER
from devflow_temporal.delivery_preparation import PreparationError
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.payload import payload_digest


@pytest.mark.asyncio
async def test_long_distance_recovery_history_fits_and_preserves_exact_payload():
    block = ''.join(random.Random(41).choices(string.ascii_letters + string.digits, k=160_000))
    history = {'retained_history': [{'sealed_json': block, 'iteration': i} for i in range(90)]}
    original = await DataConverter.default.encode([history])
    compressed = await DELIVERY_DATA_CONVERTER.encode([history])
    assert len(original[0].data) > 14_000_000
    assert compressed[0].metadata['encoding'] == b'binary/xz'
    assert len(compressed[0].data) < 2 * 1024 * 1024
    assert await DELIVERY_DATA_CONVERTER.decode(compressed) == [history]
    decoded = await DELIVERY_DATA_CONVERTER.payload_codec.decode(compressed)
    assert decoded[0].SerializeToString() == original[0].SerializeToString()


@pytest.fixture
def installation(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    package = source / 'runtime/src/devflow_temporal'
    package.mkdir(parents=True)
    (package / 'delivery_codec.py').write_text('old transport\n')
    (package / 'role_runner.py').write_text('native execution must stay exact\n')
    for args in [('init', '-q'), ('config', 'user.name', 'Transport Test'),
                 ('config', 'user.email', 'transport@example.invalid'), ('add', '.'),
                 ('commit', '-qm', 'Retained runtime')]:
        _git(source, *args)
    before = {'revision': _git(source, 'rev-parse', 'HEAD'),
              'payload_sha256': payload_digest(package)}
    (package / 'delivery_codec.py').write_text('new lossless transport\n')
    _git(source, 'add', '.')
    _git(source, 'commit', '-qm', 'Transport update')
    revision = _git(source, 'rev-parse', 'HEAD')
    after = {'revision': revision, 'tree': _git(source, 'rev-parse', 'HEAD^{tree}'),
             'payload_sha256': payload_digest(package), 'published_head': revision,
             'source_review': 'PASS', 'required_ci': 'SUCCESS'}
    spec = {'run_id': 'queued', 'state_dir': str(tmp_path / 'state/runs/queued')}
    path = Path(spec['state_dir']) / 'transport-adoption.json'
    retained = Path(spec['state_dir']) / 'retained-manifest.json'
    write_private(retained, {'processes': {'historical': True}})
    receipt = {'schema': 'devflow-queued-transport-adoption-v1', 'owner': 'root',
               'run_id': spec['run_id'], 'source_root': str(source),
               'before': before, 'after': after,
               'historical_service_manifest': {'path': str(retained),
                   'sha256': hashlib.sha256(retained.read_bytes()).hexdigest()}}
    write_private(path, receipt)
    monkeypatch.setattr(adoption, '__file__', str(package / 'delivery_transport_adoption.py'))
    return source, package, spec, path, receipt


def test_adoption_authenticates_old_payload_and_keeps_original_controller(installation):
    _source, _package, spec, path, receipt = installation
    original_bytes = path.read_bytes()
    before = receipt['before']
    assert adoption.transport_adoption(spec, before['payload_sha256']) == receipt
    original = {'runtime_payload_sha256': before['payload_sha256'],
                'source_revision': before['revision'],
                'service_manifest_sha256': receipt['historical_service_manifest']['sha256']}
    retained_original = deepcopy(original)
    current, raw = adoption.controller_installation(spec, original)
    assert current['source_revision'] == receipt['after']['revision']
    assert hashlib.sha256(raw).hexdigest() == original['service_manifest_sha256']
    assert original == retained_original
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize('change', ['role-source', 'dirty-source', 'old-payload', 'ci', 'mode'])
def test_adoption_rejects_drift_even_with_current_receipt(installation, change):
    source, package, spec, path, receipt = installation
    if change == 'role-source':
        (package / 'role_runner.py').write_text('different execution\n')
        _git(source, 'add', '.')
        _git(source, 'commit', '-qm', 'Changed role')
        revision = _git(source, 'rev-parse', 'HEAD')
        receipt['after'].update(revision=revision, published_head=revision,
                               tree=_git(source, 'rev-parse', 'HEAD^{tree}'),
                               payload_sha256=payload_digest(package))
    elif change == 'dirty-source':
        (package / 'role_runner.py').write_text('uncommitted execution\n')
    elif change == 'old-payload':
        receipt['before']['payload_sha256'] = 'f' * 64
    elif change == 'ci':
        receipt['after']['required_ci'] = 'PENDING'
    write_private(path, receipt)
    if change == 'mode':
        path.chmod(0o644)
    with pytest.raises(ValueError):
        adoption.transport_adoption(spec, receipt['before']['payload_sha256'])


def test_native_identity_adoption_cannot_accept_host_or_permission_drift(monkeypatch, tmp_path):
    frozen = {'runtime_payload_sha256': 'old', 'codex_bin_sha256': 'binary',
              'config_overrides': ['sealed']}
    spec = {'state_dir': str(tmp_path / 'state/runs/queued'),
            'policy': {'native_identity': frozen},
            'preparation': {'schema': native.SCHEMA, 'fingerprint': 'invalid'}}
    calls = []
    monkeypatch.setattr(adoption, 'transport_adoption', lambda *_args: calls.append(True))
    current = {**frozen, 'runtime_payload_sha256': 'new', 'config_overrides': ['changed']}
    monkeypatch.setattr(native, 'native_identity', lambda _spec: current)
    with pytest.raises(PreparationError):
        native.verify_native_spec(spec)
    assert not calls
    current['config_overrides'] = ['sealed']
    with pytest.raises(PreparationError):
        native.verify_native_spec(spec)
    assert calls == [True]  # A receipt never replaces the original proof validation.
