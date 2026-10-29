"""Fixture authority controls; the production historical reader stays unchanged."""
from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import portable_capture_reader as fixture
import pytest

from devflow_temporal import delivery_retained_closures as historical


def digest(data):
    return hashlib.sha256(data).hexdigest()


@pytest.fixture
def portable_case(tmp_path, monkeypatch):
    stage = tmp_path / 'stage'
    source = stage / 'tools/cli/temporal'
    source.parent.mkdir(parents=True)
    executable = (b'#!/bin/sh\necho "temporal version 1.9.1 '
                  b'(Server 1.32.0, UI 2.54.1)"\n')
    archive_bytes = b'owned fixture archive'
    source.write_bytes(executable)
    pin = {'repository': 'temporalio/cli', 'tag': 'v1.9.1',
           'archive': 'owned-cli.tar.gz', 'archive_sha256': digest(archive_bytes),
           'member': 'temporal', 'binary_sha256': digest(executable),
           'binary_size': len(executable)}
    monkeypatch.setattr(fixture, 'TOOLS', {'cli': pin})
    # Unit-only version seams; do not change the interpreter's global sys module.
    monkeypatch.setattr(fixture, 'sys', SimpleNamespace(version_info=(3, 12, 13)))
    monkeypatch.setattr(fixture, 'version', lambda name: '1.33.0' if name == 'temporalio' else '')
    archive = stage / pin['archive']
    archive.write_bytes(archive_bytes)
    url = f"https://github.com/{pin['repository']}/releases/download/{pin['tag']}/{pin['archive']}"
    metadata_path = stage / 'cli-release.json'
    metadata = {'assets': [{'name': pin['archive'], 'digest': 'sha256:' + pin['archive_sha256'],
                            'browser_download_url': url, 'size': len(archive_bytes)}]}
    metadata_raw = json.dumps(metadata).encode()
    metadata_path.write_bytes(metadata_raw)
    receipt = {'schema': 'jobctrl.temporal-test-prerequisites/v1',
               'acquisition_facts': {'official_metadata': [
                   {'path': str(metadata_path), 'sha256': digest(metadata_raw)}]},
               'tools': {'cli': {'official_repository': pin['repository'], 'pinned_tag': pin['tag'],
                                 'asset_name': pin['archive'], 'asset_url': url,
                                 'published_digest': {'algorithm': 'sha256',
                                                      'value': pin['archive_sha256']},
                                 'archive': {'path': str(archive), 'size': len(archive_bytes),
                                             'sha256': pin['archive_sha256']},
                                 'extraction': {'exact_member': pin['member']},
                                 'binary': {'path': str(source), 'sha256': digest(executable),
                                            'size': len(executable)}}}}
    (stage / 'temporal-prerequisites.provenance.v1.json').write_text(json.dumps(receipt))
    binary = fixture.prepare(tmp_path / 'reader', stage)
    monkeypatch.setenv('DEVFLOW_OWNED_CAPTURE_READER', str(binary))
    state = tmp_path / 'state'
    state.mkdir(mode=0o700)
    log = state / 'temporal.log'
    log.write_text(historical.READER_HEADER + '\n')
    log.chmod(0o600)
    config = SimpleNamespace(raw={'temporal_bin': str(binary)}, state_root=state)
    declared = {'configured_path': str(binary), 'resolved_path': str(binary),
                'sha256': pin['binary_sha256'], 'version': '1.9.1', 'server_version': '1.32.0',
                'historical_start_binary_sha256': None}
    return binary, config, declared


def test_fixture_reader_is_explicit_and_does_not_change_historical_reader(portable_case):
    _binary, config, declared = portable_case
    profile = fixture.reader(config, declared)['fixture_profile']
    assert profile == 'owned-official-temporal-reader-v1'
    with pytest.raises(ValueError, match='explicit configured copy-only'):
        historical._reader(config, declared)
    with pytest.raises(ValueError, match='authority differs'):
        fixture.reader(config, {**declared, 'sha256': '0' * 64})


@pytest.mark.parametrize('changed', ['binary', 'metadata', 'receipt-archive', 'configured',
                                     'symlink'])
def test_fixture_reader_refuses_changed_bound_authority(portable_case, changed):
    binary, config, declared = portable_case
    if changed == 'binary':
        binary.chmod(0o755)
        binary.write_bytes(binary.read_bytes() + b'!')
        binary.chmod(0o555)
    elif changed == 'metadata':
        metadata = binary.parent.parent / '.brew/temporal.rb'
        metadata.write_bytes(metadata.read_bytes() + b' ')
    elif changed == 'receipt-archive':
        path = binary.parent.parent / 'INSTALL_RECEIPT.json'
        receipt = json.loads(path.read_text())
        receipt['tools']['cli']['archive']['sha256'] = '0' * 64
        path.write_text(json.dumps(receipt))
    elif changed == 'configured':
        config.raw['temporal_bin'] = '/untrusted/temporal'
    else:
        moved = binary.with_name('original')
        binary.rename(moved)
        binary.symlink_to(moved)
    with pytest.raises(ValueError):
        fixture.reader(config, declared)
