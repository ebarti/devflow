from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_resources import owned_worktree, spec

from devflow_temporal.delivery_check_evidence import retain_artifacts
from devflow_temporal.delivery_resources import RunResources, read_private


def test_retained_manifest_replays_without_recreating_raw_fixture_output(tmp_path):
    raw = tmp_path / 'pytest-artifacts'
    raw.mkdir()
    (raw / 'junit.xml').write_text('<testsuites><testcase name="owned"/></testsuites>')
    reference = retain_artifacts(tmp_path, {'id': 'candidate'})
    shutil.rmtree(raw)
    assert retain_artifacts(tmp_path, {'id': 'candidate'}) == reference
    assert not raw.exists()


def test_artifact_inventory_failure_does_not_leave_partial_retained_copies(tmp_path):
    raw = tmp_path / 'pytest-artifacts'
    raw.mkdir()
    (raw / 'a.txt').write_text('small selected output')
    with (raw / 'z.txt').open('wb') as output:
        output.truncate(51 * 1024 * 1024)  # Sparse input, never a bulk fixture copy.
    with pytest.raises(ValueError, match='finite evidence limit'):
        retain_artifacts(tmp_path, {'id': 'candidate'})
    assert not (tmp_path / 'artifacts').exists()
    assert not (tmp_path / 'artifacts.json').exists()


def test_missing_disposable_execution_scratch_can_resume_after_external_removal(
    tmp_path, monkeypatch,
):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    short = tmp_path / 'short'
    monkeypatch.setattr(RunResources, '_execution_scratch_root', lambda _: short)
    scratch = resources.execution_scratch('checks', 'fixture')
    (scratch / 'socket-placeholder').write_text('disposable')
    session = Path(owned['state_dir']) / 'session.json'
    session.write_text('durable context')
    shutil.rmtree(short)
    assert resources.execution_scratch('checks', 'fixture') == scratch
    assert scratch.is_dir()
    assert read_private(resources.manifest)['roots'][str(short)]['generation'] == 1
    assert session.read_text() == 'durable context'
    assert resources.finalize('blocked')['state'] == 'confirmed'


@pytest.mark.parametrize('legacy', [False, True])
def test_historical_clean_gate_is_removed_and_original_candidate_session_survive(
    tmp_path, monkeypatch, legacy,
):
    import subprocess

    from devflow_temporal import delivery_resources

    owned, resources, source, checkout = owned_worktree(tmp_path)
    gate = Path(owned['state_dir']) / 'gates/0/verify'
    gate.parent.mkdir(parents=True)
    resources.register(gate, 'gate')
    subprocess.run(['git', '-C', str(source), 'worktree', 'add', '--detach', str(gate)],
                   check=True, capture_output=True)
    resources.created(gate, head=owned['base_sha'])
    if legacy:
        from devflow_temporal.delivery_resources import write_private

        with resources.locked() as manifest:
            manifest['roots'][str(gate)].pop('head')
            write_private(resources.manifest, manifest)
    monkeypatch.setattr(delivery_resources, '_gate_evidence_root',
                        lambda _: Path(owned['state_dir']) / 'later/evidence')
    session = Path(owned['state_dir']) / 'session.json'
    session.write_text('durable original provider session')
    receipt = resources.finalize('blocked')
    assert receipt['resource_cleanup'] == 'confirmed'
    assert not gate.exists()
    assert checkout.is_dir() and session.read_text() == 'durable original provider session'
    assert resources.finalize('blocked')['resource_cleanup'] == 'confirmed'


def test_nested_workspace_dependencies_and_declared_build_output_are_owned(tmp_path):
    import json
    import subprocess

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
    outputs = broker._register_generated(checkout, ['node_modules', 'dist'])
    assert workspace / 'node_modules' in outputs
    assert nested_fixture / 'node_modules' not in outputs
    for output in outputs:
        output.mkdir()
        (output / 'small-generated-file').write_text('disposable')
    broker._record_generated(outputs)
    assert resources.finalize('blocked')['state'] == 'confirmed'
    assert all(not output.exists() for output in outputs)
    with pytest.raises(ValueError):
        broker._register_generated(checkout, ['packages/client/fixtures/sample/node_modules'])


@pytest.mark.parametrize('stage', ['checks/0/owned/native', 'dependency-preparation/native-0'])
def test_closed_check_or_preparation_without_provider_attempt_reaches_finalization(
    native_configuration, stage,
):
    import json
    import sys

    from devflow_temporal import delivery_orphans
    from devflow_temporal.delivery_native_process import NativeProcess
    from devflow_temporal.delivery_store import DeliveryStore

    config, request = native_configuration
    store = DeliveryStore(config)
    store.submit(request)
    owned = store.spec(request['run_id'])
    resources = RunResources(owned)
    scratch = resources.scratch('checks', 'owned')
    folder = Path(owned['state_dir']) / stage
    native = NativeProcess(owned, folder, argv=[sys.executable, '-c', 'print("finished")'],
                           cwd=scratch, environment={'PATH': '/usr/bin:/bin'}, timeout=5).run()
    assert native['cleanup'] == 'observed-native-confirmed'
    session = Path(owned['state_dir']) / 'session.json'
    session.write_text('durable original session')
    store.project(owned['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  error='Original worker stopped', cleanup='unknown', checks={},
                  event_type='blocked', message='Synthetic completed native interruption')
    with store._connect() as db:
        before = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
    assert delivery_orphans._unfinished_rows(store) == [before]
    delivery_orphans._reconcile(store, before, owned, {'status': 'TERMINATED'})
    with store._connect() as db:
        after = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
    assert after['cleanup'] == 'confirmed'
    assert after['outcome'] == 'blocked' and after['error'] == 'Original worker stopped'
    assert not scratch.exists() and session.read_text() == 'durable original session'
    assert json.loads(after['checks_json'])['resource_cleanup']['state'] == 'confirmed'
    # A settled unchanged manifest should not be scanned again on every pump.
    assert delivery_orphans._unfinished_rows(store) == []


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


def test_new_aggregate_bound_does_not_invalidate_authenticated_historical_manifest(
    tmp_path, monkeypatch,
):
    from devflow_temporal import delivery_check_evidence

    raw = tmp_path / 'pytest-artifacts'
    raw.mkdir()
    for name in ['a.txt', 'b.txt']:
        (raw / name).write_text('123456')
    monkeypatch.setattr(delivery_check_evidence, 'MAX_ARTIFACT_BYTES', 16)
    reference = retain_artifacts(tmp_path, {'id': 'candidate'})
    shutil.rmtree(raw)
    monkeypatch.setattr(delivery_check_evidence, 'MAX_ARTIFACT_BYTES', 8)
    assert retain_artifacts(tmp_path, {'id': 'candidate'}) == reference
    fresh = tmp_path / 'fresh/pytest-artifacts'
    fresh.mkdir(parents=True)
    for name in ['a.txt', 'b.txt']:
        (fresh / name).write_text('123456')
    with pytest.raises(ValueError, match='finite evidence limit'):
        retain_artifacts(fresh.parent, {'id': 'candidate'})
    assert not (fresh.parent / 'artifacts').exists()


@pytest.mark.parametrize('order', [False, True])
def test_check_artifact_admission_accepts_each_authenticated_continuation_root(
    tmp_path, monkeypatch, order,
):
    from devflow_temporal import delivery_resources

    owned = spec(tmp_path)
    state = Path(owned['state_dir'])
    continuation = state / 'continued/evidence'
    roots = [state, continuation] if order else [continuation, state]
    monkeypatch.setattr(delivery_resources, '_gate_roots', lambda _: (continuation, roots))
    raw = continuation / 'checks/0/owned/pytest-artifacts'
    resources = RunResources(owned)
    assert resources.check_artifacts(raw) == raw
    assert raw.exists()
    resources._allowed(raw, 'check-artifacts', finalizing=True)


@pytest.mark.parametrize('replacement', ['directory', 'symlink'])
def test_disposable_raw_root_replacement_is_refused(tmp_path, replacement):
    owned = spec(tmp_path)
    resources = RunResources(owned)
    raw = resources.check_artifacts(Path(owned['state_dir']) / 'checks/0/owned/pytest-artifacts')
    original = raw.with_name('original')
    raw.rename(original)
    sentinel = tmp_path / 'precious'
    sentinel.mkdir()
    (sentinel / 'data').write_text('SAFE')
    if replacement == 'symlink':
        raw.symlink_to(sentinel, target_is_directory=True)
    else:
        raw.mkdir()
        (raw / 'data').write_text('replaced, not adopted')
    receipt = resources.cleanup_check_artifacts(raw, [])
    assert receipt['state'] == 'failed_unknown'
    assert (sentinel / 'data').read_text() == 'SAFE'
    assert original.is_dir() and raw.exists()


@pytest.mark.parametrize('outcome', [0, 1])
def test_repeated_finished_checks_dispose_raw_database_files_and_keep_verdict(
    tmp_path, outcome,
):
    import sys

    from devflow_temporal.delivery_check_evidence import junit_counts
    from devflow_temporal.delivery_native_process import NativeProcess

    owned = spec(tmp_path)
    resources = RunResources(owned)
    raw = resources.check_artifacts(Path(owned['state_dir']) / 'checks/0/owned/pytest-artifacts')
    report = '<testsuites><testcase name="owned"/></testsuites>'
    script = (
        'from pathlib import Path; import sys; p=Path(sys.argv[1]); '
        '(p/"jobs.db").write_bytes(b"disposable synthetic database"); '
        f'(p/"junit.xml").write_text({report!r}); sys.exit(int(sys.argv[2]))'
    )
    process = NativeProcess(owned, raw.parent / 'native', argv=[sys.executable, '-c', script,
                            str(raw), str(outcome)], cwd=tmp_path,
                            environment={'PATH': '/usr/bin:/bin'}, timeout=5)
    native = process.run()
    assert native['exit_code'] == outcome
    reference = retain_artifacts(raw.parent, {'id': 'candidate'})
    assert reference['count'] == 1
    assert resources.cleanup_check_artifacts(raw, [process.journal])['state'] == 'removed'
    assert not raw.exists()
    for _ in range(2):
        assert resources.check_artifacts(raw) == raw
        assert process.run() == native
        assert retain_artifacts(raw.parent, {'id': 'candidate'}) == reference
        disposal = resources.cleanup_check_artifacts(raw, [process.journal])
        assert disposal['state'] == 'already_absent'
        assert not raw.exists()
    assert junit_counts(reference, 'candidate', Path(owned['state_dir']))['passed'] == 1
    assert resources.finalize('blocked')['state'] == 'confirmed'


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



def test_missing_execution_scratch_refuses_an_original_unjournaled_check_lease(
    tmp_path, monkeypatch,
):
    import fcntl
    import os

    owned = spec(tmp_path)
    resources = RunResources(owned)
    short = tmp_path / 'short'
    monkeypatch.setattr(RunResources, '_execution_scratch_root', lambda _: short)
    scratch = resources.execution_scratch('checks', 'owned')
    raw = resources.check_artifacts(Path(owned['state_dir']) / 'checks/0/owned/pytest-artifacts')
    folder = raw.parent / 'native'
    folder.mkdir(mode=0o700)
    descriptor = os.open(folder / 'native-monitor.lock', os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        shutil.rmtree(short)
        with pytest.raises(BlockingIOError):
            resources.execution_scratch('checks', 'owned')
        assert not short.exists()
        assert read_private(resources.manifest)['roots'][str(short)].get('generation', 0) == 0
    finally:
        os.close(descriptor)
    assert resources.execution_scratch('checks', 'owned') == scratch
    assert read_private(resources.manifest)['roots'][str(short)]['generation'] == 1
    assert resources.finalize('blocked')['resource_cleanup'] == 'confirmed'
