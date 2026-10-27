from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_resources import owned_worktree, spec

from devflow_temporal.delivery_check_evidence import retain_artifacts
from devflow_temporal.delivery_resources import RunResources, read_private


def test_nested_workspace_dependencies_and_declared_build_output_are_owned(tmp_path):
    import json
    import subprocess

    from devflow_temporal.candidate import candidate_for
    from devflow_temporal.delivery_broker import DeliveryBroker

    owned, resources, source, checkout = owned_worktree(tmp_path)
    (checkout / '.gitignore').write_text('node_modules/\ndist/\n')
    (checkout / 'package.json').write_text(json.dumps({'workspaces': ['packages/*']}))
    workspace = checkout / 'packages/client'
    workspace.mkdir(parents=True)
    (workspace / 'package.json').write_text('{"name":"owned-client"}')
    nested_fixture = workspace / 'fixtures/sample'
    nested_fixture.mkdir(parents=True)
    (nested_fixture / 'package.json').write_text('{"name":"fixture-only"}')
    subprocess.run(['git', '-C', str(checkout), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(checkout), 'commit', '-qm', 'Tracked workspace fixture'],
                   check=True, capture_output=True)
    owned['policy']['checks'] = [{'id': 'build', 'generated_directories': ['dist']}]
    broker = DeliveryBroker.__new__(DeliveryBroker)
    broker.spec = owned
    candidate = candidate_for(checkout)
    outputs = broker._register_generated(checkout, ['node_modules', 'dist'])
    assert workspace / 'node_modules' in outputs
    assert nested_fixture / 'node_modules' not in outputs
    build = checkout / 'dist'
    identities = read_private(resources.manifest)['roots']
    assert build.is_dir() and identities[str(build)]['identity']
    assert all(not output.exists() and identities[str(output)]['identity'] is None
               for output in outputs if output != build)
    for output in outputs:
        output.mkdir(exist_ok=True)
        (output / 'small-generated-file').write_text('disposable')
    broker._record_generated(outputs)
    assert read_private(resources.manifest)['roots'][str(build)] == identities[str(build)]
    assert resources.finalize('blocked')['state'] == 'confirmed'
    assert all(not output.exists() for output in outputs)
    assert candidate_for(checkout) == candidate
    with pytest.raises(ValueError):
        broker._register_generated(checkout, ['packages/client/fixtures/sample/node_modules'])


def test_failed_artifact_copy_is_disposed_and_cannot_pass_after_raw_disposal(tmp_path, monkeypatch):
    from devflow_temporal import delivery_check_evidence

    raw = tmp_path / 'pytest-artifacts'
    raw.mkdir()
    for name in ['a.txt', 'b.txt']:
        (raw / name).write_text('small retained evidence')
    actual_read = delivery_check_evidence._owned_bytes

    def failed_read(path):
        if path == raw / 'b.txt':
            raise OSError('Synthetic disk failure after the first retained copy')
        return actual_read(path)

    monkeypatch.setattr(delivery_check_evidence, '_owned_bytes', failed_read)
    with pytest.raises(OSError, match='Synthetic disk failure'):
        retain_artifacts(tmp_path, {'id': 'candidate'})
    assert not (tmp_path / 'artifacts').exists()
    shutil.rmtree(raw)
    with pytest.raises(ValueError, match='Synthetic disk failure'):
        retain_artifacts(tmp_path, {'id': 'candidate'})
    assert not raw.exists()


def test_confirmed_finalizer_before_projection_acknowledgement_is_reconciled(
    native_configuration,
):
    from devflow_temporal import delivery_orphans
    from devflow_temporal.delivery_store import DeliveryStore

    config, request = native_configuration
    store = DeliveryStore(config)
    store.submit(request)
    owned = store.spec(request['run_id'])
    resources = RunResources(owned)
    resources.scratch('checks', 'allocation-only')
    store.project(owned['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  error='Original worker stopped', cleanup='unknown', checks={},
                  event_type='blocked', message='Finalization acknowledgement was lost')
    assert resources.finalize('blocked')['resource_cleanup'] == 'confirmed'
    with store._connect() as db:
        before = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
    assert delivery_orphans._unfinished_rows(store) == [before]
    delivery_orphans._reconcile(store, before, owned, {'status': 'TERMINATED'})
    with store._connect() as db:
        after = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
    assert after['cleanup'] == 'confirmed' and after['error'] == before['error']
    assert after['outcome'] == before['outcome']


@pytest.mark.parametrize('guard', ['live-monitor', 'unjournaled-lease'])
def test_check_output_disposal_requires_released_original_native_leases(tmp_path, guard):
    import fcntl
    import os
    import sys

    from devflow_temporal.delivery_native_process import NativeProcess, process_table
    from devflow_temporal.delivery_resources import write_private

    owned = spec(tmp_path)
    resources = RunResources(owned)
    raw = resources.check_artifacts(Path(owned['state_dir']) / 'checks/0/owned/pytest-artifacts')
    folder = raw.parent / 'native'
    descriptor = None
    original = None
    if guard == 'unjournaled-lease':
        folder.mkdir(mode=0o700)
        journal = None
        lock = folder / 'native-monitor.lock'
        descriptor = os.open(lock, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
    else:
        native = NativeProcess(owned, folder, argv=[sys.executable, '-c', 'print("one")'],
                               cwd=tmp_path, environment={'PATH': '/usr/bin:/bin'}, timeout=5)
        assert native.run()['cleanup'] == 'observed-native-confirmed'
        journal = native.journal
        original = read_private(journal)
        if guard == 'live-monitor':
            changed = {**original, 'monitor': {'pid': os.getpid(), **process_table()[os.getpid()]}}
            write_private(journal, changed)
        else:
            descriptor = os.open(folder / 'native-process.lock', os.O_RDONLY)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
    try:
        receipt = resources.cleanup_check_artifacts(raw, [journal] if journal else [])
        assert receipt['state'] == 'failed_unknown' and raw.is_dir()
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if original is not None:
            write_private(journal, original)
    disposal = resources.cleanup_check_artifacts(raw, [journal] if journal else [])
    assert disposal['state'] == 'removed'



def test_clean_recorded_gate_preserves_unique_commit_after_original_candidate_rollback(tmp_path):
    import subprocess

    owned, resources, source, checkout = owned_worktree(tmp_path)
    (checkout / 'file').write_text('unique committed candidate')
    subprocess.run(['git', '-C', str(checkout), 'commit', '-qam', 'Unique candidate fixture'],
                   check=True, capture_output=True)
    unique = subprocess.check_output(['git', '-C', str(checkout), 'rev-parse', 'HEAD'],
                                     text=True).strip()
    gate = Path(owned['state_dir']) / 'gates/0/verify'
    gate.parent.mkdir(parents=True)
    resources.register(gate, 'gate')
    subprocess.run(['git', '-C', str(source), 'worktree', 'add', '--detach', str(gate), unique],
                   check=True, capture_output=True)
    resources.created(gate, head=unique)
    subprocess.run(['git', '-C', str(checkout), 'reset', '--hard', owned['base_sha']],
                   check=True, capture_output=True)
    receipt = resources.finalize('blocked')
    observation = next(item for item in receipt['roots'] if item['path'] == str(gate))
    assert observation['state'] == 'retained'
    assert 'preserved candidate proof' in observation['reason']
    assert gate.exists() and (gate / 'file').read_text() == 'unique committed candidate'
    assert (checkout / 'file').read_text() == 'base'
