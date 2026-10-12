"""Finalization retains historical root authority across compact revision transports."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

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
        db.execute('CREATE TABLE delivery_runs(run_id,request_digest,recovery_json)')
        db.execute('CREATE TABLE delivery_commands(command_id,run_id,request_digest,response_json)')
        for recovery in (previous, current):
            write_private(state / namespace(recovery) / 'admission.json', recovery)
            db.execute('INSERT INTO delivery_commands VALUES(?,?,?,?)',
                (recovery['command']['command_id'], spec['run_id'], recovery['command_digest'],
                 json.dumps({'admission_sha256': digest(recovery)})))
        db.execute('INSERT INTO delivery_runs VALUES(?,?,?)',
                   (spec['run_id'], spec['request_digest'], json.dumps(current)))
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
    path = Path(current['custody_ref']['path'])
    original = path.read_bytes()
    value = json.loads(original)
    value['original_recovery']['execution_spec']['checkout'] = '/foreign'
    write_private(path, value)
    with pytest.raises(ValueError, match='custody bytes changed'):
        _gate_roots(spec)
    path.write_bytes(original)
    current['custody_ref']['path'] = str(path.with_name('foreign.json'))
    with sqlite3.connect(database) as db:
        db.execute('UPDATE delivery_runs SET recovery_json=?', (json.dumps(current),))
    with pytest.raises(ValueError, match='sealed namespace'):
        _gate_roots(spec)
