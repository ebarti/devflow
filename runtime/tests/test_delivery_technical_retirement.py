"""Retired ingress refuses effects while retained workflow inputs stay readable."""
from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest
from historical_replay import replay_designated_history
from test_delivery_store import service as service

from devflow_temporal import delivery_technical_integration as integration
from devflow_temporal.delivery_broker import _git
from devflow_temporal.delivery_resources import read_private, write_private


def _snapshot(store):
    with store._connect() as db:
        tables = [row[0] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        rows = {name: [tuple(row) for row in db.execute('SELECT * FROM "' + name + '"')]
                for name in tables}
    source = Path(store.config.raw["repositories"]["fixture"]["source_path"])
    files = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
             for root in (source, store.config.state_root)
             for path in root.rglob('*') if path.is_file()}
    return rows, files


@pytest.mark.parametrize('kind', ['accepted_technical_successor', 'unknown', None, 1, [], {}])
@pytest.mark.parametrize('preflight', [False, True])
def test_retired_or_malformed_continuation_refuses_before_any_effect(service, kind, preflight):
    store, supplied = service
    store.submit(supplied)
    before = _snapshot(store)
    try:
        with pytest.raises(ValueError, match='unsupported repair continuation kind'):
            store.continue_repair('run-1', {
                'command_id': 'retired-technical',
                'continuation_kind': kind,
                'additional_iterations': 0,
            }, preflight=preflight)
    finally:
        assert _snapshot(store) == before


def test_already_admitted_technical_outbox_keeps_its_inputs_and_start_acknowledgement(service):
    store, supplied = service
    store.submit(supplied)
    # This tests the retained outbox projection contract, not new admission.
    # Workflow execution compatibility is separately proved by the old histories.
    recovery = {'kind': 'accepted_technical_successor', 'retained': 'original fixture inputs'}
    original_request = store.pending_starts()[0]['request_json']
    recovery_json = json.dumps(recovery)
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET phase='technical_successor_queued',"
                   "workflow_id='delivery-run-1-technical-1',recovery_json=? WHERE run_id='run-1'",
                   (recovery_json,))
    pending = store.pending_starts()
    assert len(pending) == 1
    assert pending[0]['workflow_id'] == 'delivery-run-1-technical-1'
    assert pending[0]['request_json'] == original_request
    assert pending[0]['recovery_json'] == recovery_json
    before = _snapshot(store)
    with pytest.raises(ValueError, match='unsupported repair continuation kind'):
        store.continue_repair('run-1', {'continuation_kind': recovery['kind'],
                                      'additional_iterations': 0})
    assert _snapshot(store) == before
    store.mark_start('run-1', accepted=True)
    assert store.pending_starts() == []
    with store._connect() as db:
        row = db.execute("SELECT phase,request_json,recovery_json FROM delivery_runs").fetchone()
        assert tuple(row) == ('technical_preflight', original_request, recovery_json)


@pytest.mark.asyncio
@pytest.mark.parametrize('name,workflow_id', [
    ('technical-c04-checks-completed-history.json', 'technical-checks'),
    ('technical-c04-suspended-history.json', 'technical-review'),
    ('technical-c04-review-completed-history.json', 'technical-review'),
])
async def test_original_completed_and_suspended_technical_histories_replay(
    name, workflow_id, tmp_path,
):
    path = Path(__file__).parent / 'fixtures' / name
    original = path.read_bytes()
    await replay_designated_history(path, tmp_path, workflow_id)
    assert path.read_bytes() == original


@pytest.mark.parametrize('drift', [None, 'tree', 'parents', 'subject', 'signer',
                                  'signoff', 'unsigned', 'receipt', 'ref', 'base'])
def test_retained_signed_integration_reads_and_rejects_tampering_without_effects(tmp_path, drift):
    # Build an ordinary signed fixture merge directly with Git. The retired
    # target-specific builder is not copied or called by this reader test.
    repo = tmp_path / 'source'
    repo.mkdir()
    _git(repo, 'init', '-q', '-b', 'main')
    _git(repo, 'config', 'user.name', 'Reader Fixture')
    _git(repo, 'config', 'user.email', 'fixture@example.invalid')
    (repo / 'README.md').write_text('Original fixture\n')
    _git(repo, 'add', '.')
    _git(repo, 'commit', '-qm', 'test: fixture base')
    base = _git(repo, 'rev-parse', 'HEAD')
    _git(repo, 'checkout', '-qb', 'fix/owned')
    (repo / 'owned.md').write_text('Owned fixture\n')
    _git(repo, 'add', '.')
    _git(repo, 'commit', '-qm', 'test: owned fixture')
    owned = _git(repo, 'rev-parse', 'HEAD')
    _git(repo, 'checkout', '-q', 'main')
    (repo / 'main.md').write_text('Main fixture\n')
    _git(repo, 'add', '.')
    _git(repo, 'commit', '-qm', 'test: main fixture')
    main = _git(repo, 'rev-parse', 'HEAD')
    tree = _git(repo, 'rev-parse', 'HEAD^{tree}')
    key = tmp_path / 'signing-key'
    subprocess.run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)],
                   check=True, capture_output=True)
    allowed = tmp_path / 'allowed-signers'
    allowed.write_text('fixture@example.invalid ' + key.with_suffix('.pub').read_text())
    _git(repo, 'config', 'gpg.format', 'ssh')
    _git(repo, 'config', 'user.signingkey', str(key))
    _git(repo, 'config', 'gpg.ssh.allowedSignersFile', str(allowed))
    signer = 'Reader Fixture <fixture@example.invalid>'
    subject = 'chore: retained fixture merge'
    plan = {'tree': tree, 'old_head': owned, 'main': main, 'subject': subject, 'signer': signer}
    if drift == 'signer':
        _git(repo, 'config', 'user.name', 'Different Fixture')
    commit_tree = base + '^{tree}' if drift == 'tree' else tree
    parents = [main, owned] if drift == 'parents' else [owned, main]
    message = ('chore: altered fixture' if drift == 'subject' else subject)
    if drift != 'signoff':
        message += '\n\nSigned-off-by: ' + signer
    head = _git(repo, 'commit-tree', *([] if drift == 'unsigned' else ['-S']),
                commit_tree, '-p', parents[0], '-p', parents[1], '-m', message)
    ref = 'refs/devflow/technical/fixture/integration'
    _git(repo, 'update-ref', ref, main if drift == 'ref' else head)
    state = tmp_path / 'state'
    receipt = state / 'technical-successor/integration.json'
    write_private(receipt, {'head': head, 'ref': ref, **plan})
    if drift == 'receipt':
        write_private(receipt, {**read_private(receipt), 'subject': 'unbound metadata'})
    spec = {'checkout': str(repo), 'state_dir': str(state),
            'base_sha': base if drift == 'base' else main}
    before = {str(path): path.read_bytes() for root in (repo, state)
              for path in root.rglob('*') if path.is_file()}
    if drift:
        with pytest.raises(ValueError):
            integration.readback(spec, {'integration': plan})
    else:
        integration.readback(spec, {'integration': plan})
        integration.readback(spec, {'integration': plan})
    assert {str(path): path.read_bytes() for root in (repo, state)
            for path in root.rglob('*') if path.is_file()} == before
