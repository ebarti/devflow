"""Finalization retains historical root authority across compact revision transports."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_feature_revision_recovery as revision
from devflow_temporal import delivery_orphans as orphans
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_feature_revision_recovery import compact_recovery
from devflow_temporal.delivery_resources import (
    RunResources,
    _gate_roots,
    private_directory,
    write_private,
)
from devflow_temporal.delivery_stopped_resume import namespace


@pytest.fixture(params=[False, True], ids=['full', 'compact'])
def chain(tmp_path, monkeypatch, request):
    state = tmp_path / 'runs' / 'worker'
    private_directory(state)
    database = tmp_path / 'workflow.sqlite3'
    raw = {'fixture': 'frozen-config'}
    spec = {'run_id': 'worker', 'state_dir': str(state),
            'config_path': str(tmp_path / 'config.json'), 'config_digest': digest(raw),
            'request_digest': 'original', 'checkout': str(tmp_path / 'checkout'),
            'policy': {'max_repairs': 10}}
    spec['policy_digest'] = digest(spec['policy'])
    # Only config loading is a seam; real canonical rows, compact envelopes,
    # custody files and every ancestor command authenticate the ownership chain.
    monkeypatch.setattr('devflow_temporal.delivery_execution_config.load_execution_config',
        lambda _: SimpleNamespace(raw=raw, path=Path(spec['config_path']),
                                   state_root=tmp_path, tracking_db=database))

    def admission(name, iteration, previous):
        command = {'command_id': name, 'additional_iterations': 1}
        old = {'run_id': spec['run_id'], 'iteration': iteration, 'roles': []}
        return {'kind': 'stopped_delivery_resume', 'command': command,
                'command_digest': digest(command), 'execution_spec': spec,
                'execution_candidate': None, 'maximum_iteration': iteration + 1,
                'session_id': None, 'feature_plan_revision': None,
                'predecessor_result_digest': digest(old), 'state': old,
                'original_recovery': previous}

    previous = admission('old', 1, None)
    full = admission('new', 2, previous)
    private_directory(state / namespace(full))
    current = compact_recovery(full, state / namespace(full)) if request.param else full
    with sqlite3.connect(database) as db:
        db.execute('CREATE TABLE delivery_runs('
                   'run_id,request_digest,recovery_json,request_json,accepted_plan_text)')
        db.execute('CREATE TABLE delivery_preparations('
                   'run_id,submitted_spec_digest,effective_spec_digest,effective_spec_json)')
        db.execute('CREATE TABLE delivery_commands(command_id,run_id,request_digest,response_json)')
        for recovery in (previous, current):
            write_private(state / namespace(recovery) / 'admission.json', recovery)
            db.execute('INSERT INTO delivery_commands VALUES(?,?,?,?)',
                (recovery['command']['command_id'], spec['run_id'], recovery['command_digest'],
                 json.dumps({'admission_sha256': digest(recovery)})))
        db.execute('INSERT INTO delivery_runs VALUES(?,?,?,?,?)',
                   (spec['run_id'], spec['request_digest'], json.dumps(current),
                    json.dumps(spec), None))
    return spec, previous, current, database


def test_finalization_recognizes_old_gates_and_generated_children(chain):
    spec, previous, current, _ = chain
    state = Path(spec['state_dir'])
    old_root = state / namespace(previous) / 'evidence'
    current_root = state / namespace(current) / 'evidence'
    selected, roots = _gate_roots(spec)
    assert selected == current_root
    assert roots == {state, old_root, current_root}
    registry = RunResources(spec)
    gate = old_root / 'gates' / '1' / 'review'
    private_directory(gate.parent)
    registry.register(gate, 'gate')
    private_directory(gate)
    registry.created(gate)
    registry.register(gate / 'node_modules', 'generated')
    private_directory(gate / 'node_modules')
    registry.created(gate / 'node_modules')
    # These are the same entry guards used by finalization, including recorded
    # parent identity; a foreign similarly shaped directory remains unauthorized.
    registry._allowed(gate, 'gate', finalizing=True)
    registry._allowed(gate / 'node_modules', 'generated', finalizing=True)
    with pytest.raises(ValueError, match='outside its registered run boundary'):
        registry._allowed(state / 'stopped-resumes/foreign/evidence/gates/1/review',
                          'gate', finalizing=True)


def test_historical_command_and_admission_are_still_required(chain):
    spec, previous, _, database = chain
    path = Path(spec['state_dir']) / namespace(previous) / 'admission.json'
    original = path.read_bytes()
    write_private(path, {**previous, 'session_id': 'changed'})
    with pytest.raises(ValueError, match='immutable command authority'):
        _gate_roots(spec)
    path.write_bytes(original)
    with sqlite3.connect(database) as db:
        db.execute('DELETE FROM delivery_commands WHERE command_id=?',
                   (previous['command']['command_id'],))
    with pytest.raises(ValueError, match='immutable command authority'):
        _gate_roots(spec)


def test_compact_custody_tampering_cannot_grant_old_roots(chain):
    spec, _, current, database = chain
    if 'custody_ref' not in current:
        return
    memo = {}
    _gate_roots(spec, retained_memo=memo)
    hydrated = revision.retained_recovery(current, memo=memo)
    hydrated['original_recovery']['execution_spec']['checkout'] = '/foreign'
    assert revision.retained_recovery(current, memo=memo)['original_recovery'][
        'execution_spec']['checkout'] == spec['checkout']
    path = Path(current['custody_ref']['path'])
    original = path.read_bytes()
    value = json.loads(original)
    value['original_recovery']['execution_spec']['checkout'] = '/foreign'
    write_private(path, value)
    with pytest.raises(ValueError, match='custody bytes changed'):
        _gate_roots(spec, retained_memo=memo)
    path.write_bytes(original)
    current['custody_ref']['path'] = str(path.with_name('foreign.json'))
    with sqlite3.connect(database) as db:
        db.execute('UPDATE delivery_runs SET recovery_json=?', (json.dumps(current),))
    with pytest.raises(ValueError, match='sealed namespace'):
        _gate_roots(spec, retained_memo=memo)


def _process_paths(spec, root):
    paths = []
    launch = {'argv': ['opaque-fixture'], 'environment': {}}
    for name in ('first', 'second', 'third'):
        path = root / name / 'native-process.json'
        write_private(path, {
            'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
                       'argv': launch['argv'], 'environment_sha256': digest({}), 'ports': []},
            'phase': 'finished', 'ports': [], 'owned': {'17017': {'identity': 'opaque-child'}},
            'monitor': {'pid': 17019, 'identity': 'opaque-monitor'},
        })
        write_private(path.with_name('launch.json'), launch)
        paths.append(path)
    return paths


def test_observation_batch_materializes_history_once_and_keeps_fresh_checks(chain, monkeypatch):
    spec, _, current, _ = chain
    root, _ = _gate_roots(spec)
    paths = _process_paths(spec, root)
    materialized, namespaces, observed, reconciled = [], [], [], []
    compact_view, gate_roots = revision.compact_recovery_view, orphans._gate_roots

    def materialize(value, path):
        materialized.append(path)
        return compact_view(value, path)

    def namespace_checks(*args, **kwargs):
        namespaces.append(True)
        return gate_roots(*args, **kwargs)

    def process_readback():
        observed.append(True)
        return {}

    def reconcile(path):
        reconciled.append(path)
        return {'cleanup': 'observed-native-confirmed'}

    monkeypatch.setattr(revision, 'compact_recovery_view', materialize)
    monkeypatch.setattr(orphans, '_gate_roots', namespace_checks)
    monkeypatch.setattr(orphans, 'process_table', process_readback)
    monkeypatch.setattr(orphans, 'reconcile_process', reconcile)
    for batch in (1, 2):
        assert len(list(orphans._observe_registered_processes(spec, paths))) == len(paths)
        assert len(materialized) == (batch if 'custody_ref' in current else 0)
        assert len(namespaces) == len(observed) == len(reconciled) == batch * len(paths)
        assert reconciled[-len(paths):] == paths


def test_observation_batch_refuses_admission_drift_before_next_reconciliation(chain, monkeypatch):
    spec, _, current, _ = chain
    root, _ = _gate_roots(spec)
    paths = _process_paths(spec, root)
    reconciled = []

    def reconcile(path):
        reconciled.append(path)
        write_private(Path(spec['state_dir']) / namespace(current) / 'admission.json',
                      {**current, 'session_id': 'changed-after-first-journal'})
        return {'cleanup': 'observed-native-confirmed'}

    monkeypatch.setattr(orphans, 'process_table', lambda: {})
    monkeypatch.setattr(orphans, 'reconcile_process', reconcile)
    with pytest.raises(ValueError, match='immutable command authority'):
        list(orphans._observe_registered_processes(spec, paths))
    assert reconciled == paths[:1]
