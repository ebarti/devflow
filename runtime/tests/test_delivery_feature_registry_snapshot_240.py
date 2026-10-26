"""Registry proofs read authenticated bytes even through pathname substitution."""

import os
import shutil
import sqlite3
from pathlib import Path

import pytest
from test_delivery_feature_handoff_240 import feature as feature

from devflow_temporal.delivery_feature_handoff import (
    observe_handoff,
    readonly_feature_spec,
    readonly_registry,
)
from devflow_temporal.delivery_feature_revision_subject import _ReadOnlyRegistry


@pytest.mark.parametrize('reader', ['view', 'spec', 'handoff', 'revision_subject'])
def test_registry_rename_aba_cannot_supply_substituted_proof(feature, monkeypatch, reader):
    store, shared, _snapshot, token = feature
    shared.stop(token, 'closed', {})
    source = shared.path
    alternate = source.with_name('alternate.sqlite3')
    saved = source.with_name('held.sqlite3')
    shutil.copyfile(source, alternate)
    alternate.chmod(0o600)
    with sqlite3.connect(alternate) as db:
        db.execute('UPDATE execution_claims SET generation=generation+1')
    spec = {'run_id': 'run-1', 'feature_delivery': {'owner': token, 'registry': str(source)}}
    real_connect = sqlite3.connect
    swaps = []

    def substituted(path, *args, **kwargs):
        if 'devflow-feature-observation-' in str(path) and not swaps:
            swaps.append(True)
            os.rename(source, saved)
            os.rename(alternate, source)
            try:
                return real_connect(path, *args, **kwargs)
            finally:
                os.rename(source, alternate)
                os.rename(saved, source)
        return real_connect(path, *args, **kwargs)

    monkeypatch.setattr(sqlite3, 'connect', substituted)
    try:
        if reader == 'view':
            with readonly_registry(spec) as view:
                assert view.current(token['issue_id'])['generation'] == token['generation']
        elif reader == 'spec':
            assert readonly_feature_spec(store, 'run-1')['feature_delivery']['owner'] == token
        elif reader == 'handoff':
            assessment = observe_handoff(store, 'run-1', {'expected_revision': 7},
                                         operation='continue_feature')
            assert not assessment['eligible']
            assert 'feature_owner_changed' not in {b['code'] for b in assessment['blockers']}
        else:
            with _ReadOnlyRegistry(source).connect() as db:
                row = db.execute('SELECT generation FROM execution_claims WHERE issue_id=?',
                                 (token['issue_id'],)).fetchone()
                assert row[0] == token['generation']
    except ValueError as exc:
        assert 'changed during observation' in str(exc)
    assert swaps
    assert shared.current(token['issue_id'])['generation'] == token['generation']
    assert not Path(str(source) + '-wal').exists()


@pytest.mark.parametrize('suffix', ['-wal', '-journal'])
@pytest.mark.parametrize('reader', ['view', 'revision_subject'])
def test_dangling_registry_sidecar_is_unknown(feature, suffix, reader):
    _store, shared, _snapshot, token = feature
    sidecar = Path(str(shared.path) + suffix)
    sidecar.symlink_to(sidecar.with_name('missing-target'))
    spec = {'feature_delivery': {'owner': token, 'registry': str(shared.path)}}
    with pytest.raises(ValueError):
        if reader == 'view':
            with readonly_registry(spec):
                pytest.fail('a dangling journal cannot be treated as absent')
        else:
            with _ReadOnlyRegistry(shared.path).connect():
                pytest.fail('a dangling journal cannot be treated as absent')
    assert sidecar.is_symlink()
