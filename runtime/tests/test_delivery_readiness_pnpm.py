"""Synthetic frozen registry inventory; verifies actual package bytes and closure."""
import base64
import hashlib
import json

import pytest

from devflow_temporal import delivery_readiness_pnpm as proof


def test_loaded_playwright_closure_matches_every_frozen_registry_file(tmp_path, monkeypatch):
    checkout, store = tmp_path / 'checkout', tmp_path / 'store'
    checkout.mkdir()
    store.mkdir()
    version = '1.0.0'
    lock = {'packages': {}}
    paths = []
    for name in ('@playwright/test', 'playwright', 'playwright-core'):
        target = name.replace('/', '+') + '@' + version
        package = checkout / 'node_modules/.pnpm' / target / 'node_modules' / name
        package.mkdir(parents=True)
        files = {'package.json': json.dumps({'name': name, 'version': version}).encode(),
                 'index.js': ('module.exports = ' + repr(name) + ';').encode()}
        records = {}
        for relative, content in files.items():
            (package / relative).write_bytes(content)
            hashed = base64.b64encode(hashlib.sha512(content).digest()).decode()
            records[relative] = {'integrity': 'sha512-' + hashed}
        tar_hash = hashlib.sha512(name.encode()).digest()
        integrity = 'sha512-' + base64.b64encode(tar_hash).decode()
        lock['packages'][name + '@' + version] = {'resolution': {'integrity': integrity}}
        fingerprint = tar_hash.hex()[:64]
        index = store / 'v10/index' / fingerprint[:2] / (fingerprint[2:] + '-' + target + '.json')
        index.parent.mkdir(parents=True, exist_ok=True)
        index.write_text(json.dumps({'name': name, 'version': version, 'files': records}))
        paths.append(package / 'index.js')
    monkeypatch.setattr(proof, '_fetch_authority', lambda *_: (store, lock))
    receipt = proof.authenticate_playwright({}, checkout, {}, paths[0], [str(p) for p in paths])
    assert [item['name'] for item in receipt['packages']] == [
        '@playwright/test', 'playwright', 'playwright-core']
    shims = []
    for package, relative in zip(paths[:2],
            ('node_modules/.bin/playwright', 'node_modules/.bin/playwright-core'), strict=True):
        shim = package.parent / relative
        shim.parent.mkdir(parents=True)
        shim.write_text('#!/bin/sh\nexit 0\n')
        shim.chmod(0o755)
        shims.append(shim)
    receipt = proof.authenticate_playwright({}, checkout, {}, paths[0], [str(p) for p in paths])
    assert all(item['generated_shims'] for item in receipt['packages'][:2])
    with pytest.raises(proof.PackageProofError, match='outside its authenticated closure'):
        proof.authenticate_playwright({}, checkout, {}, paths[0], [str(shims[0])])
    foreign = checkout / 'foreign.js'
    foreign.write_text('module.exports = 0')
    with pytest.raises(proof.PackageProofError, match='outside its authenticated closure'):
        proof.authenticate_playwright({}, checkout, {}, paths[0], [str(foreign)])
    original = paths[1].read_bytes()
    paths[1].write_bytes(original + b'changed')
    with pytest.raises(proof.PackageProofError, match='differs from its registry inventory'):
        proof.authenticate_playwright({}, checkout, {}, paths[0], [str(p) for p in paths])
    paths[1].write_bytes(original)
    (paths[2].parent / 'untracked.js').write_text('extra')
    with pytest.raises(proof.PackageProofError, match='unverified source'):
        proof.authenticate_playwright({}, checkout, {}, paths[0], [str(p) for p in paths])


def test_fetch_proof_rejects_candidate_setup_and_foreign_runtime(tmp_path, monkeypatch):
    state = tmp_path / 'run'
    receipt = state / 'dependency-preparation' / 'native-0' / 'receipt.json'
    journal = receipt.parent / 'process' / 'native-process.json'
    process = {'journal': str(journal), 'exit_code': 0,
               'cleanup': 'observed-native-confirmed', 'monitoring_complete': True,
               'stdio_drained': True, 'cancelled': False, 'timed_out': False,
               'listener_conflict': False}
    saved = {'state': 'passed', 'request': {}, 'native_process': process,
             'candidate_setup_executed': True}
    def preparation():
        raw = json.dumps(saved).encode()
        return {**saved, 'receipt': str(receipt),
                'receipt_sha256': hashlib.sha256(raw).hexdigest()}
    observed = preparation()
    monkeypatch.setattr(proof, 'read_private', lambda path, raw=False: (
        json.dumps(saved).encode() if raw else {'phase': 'finished', 'result': process,
                                                'runtime_identity': {}}))
    with pytest.raises(proof.PackageProofError, match='differs from its receipt'):
        proof._fetch_authority({'state_dir': str(state)}, tmp_path, observed)
    saved['candidate_setup_executed'] = False
    observed = preparation()
    with pytest.raises(proof.PackageProofError, match='journal or runtime changed'):
        proof._fetch_authority({'state_dir': str(state)}, tmp_path, observed)
