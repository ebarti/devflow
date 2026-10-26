"""CI-inventory-only reuse of an already admitted historical quarantine.

This proves immutable membership, never a provider verdict or new launch.
Current resource consumers separately observe every actor, lease and root.
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from .contracts import canonical_json, digest
from .delivery_ci_inventory_contract import APPROVED_PROFILE_SHA256, approved_profile

FIELD = 'ci_inventory_historical_teardown'


def context(spec, previous, profile, candidate, publication, *, current=None):
    """Authenticate the canonical chain before deriving any exception hashes.

No physical checkout or kernel observation is needed for historical hydration.
An effectful caller supplies its exact current resource execution separately.
"""
    from .delivery_ci_inventory import _subject
    from .delivery_execution_config import load_execution_config
    from .delivery_feature_revision_recovery import retained_recovery
    from .delivery_resources import _gate_roots, read_private
    from .delivery_stopped_resume import custody, namespace
    from .delivery_unpublished_recovery import quarantine_history

    approved_profile(profile)
    _subject(spec, candidate, publication, profile)
    config = load_execution_config(spec)
    with closing(sqlite3.connect(config.tracking_db.as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        row = db.execute('SELECT request_digest,recovery_json FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
        if not row or row['request_digest'] != spec['request_digest']:
            raise ValueError('CI teardown lost its canonical original run')
        head = json.loads(row['recovery_json']) if row['recovery_json'] else None
        latest = current or (head or {}).get('execution_spec') or spec
        bindings = {}
        authority = _gate_roots(latest, execution_specs=bindings)
        if spec not in bindings.values():
            raise ValueError('CI teardown predecessor is not an admitted execution')
        # _gate_roots authenticates the current root and each retained namespace.
        # Locate the exact proposed predecessor, never a caller-supplied branch.
        ancestor = head
        while ancestor is not None and ancestor != previous:
            ancestor = retained_recovery(ancestor).get('original_recovery')
        if ancestor != previous:
            raise ValueError('CI teardown is outside the canonical retained admission chain')
        witnesses = []
        ancestor = previous
        while ancestor:
            details = retained_recovery(ancestor)
            if details.get('kind') == 'stopped_delivery_resume':
                admitted = custody(db, ancestor)
                if admitted not in bindings.values():
                    raise ValueError('CI teardown lost its admitted historical member')
                command = db.execute('SELECT request_digest,response_json FROM delivery_commands '
                                     'WHERE run_id=? AND command_id=?',
                                     (spec['run_id'], details['command']['command_id'])).fetchone()
                path = Path(admitted['state_dir']) / namespace(ancestor) / 'admission.json'
                raw = read_private(path, raw=True)
                if json.loads(raw) != ancestor or not command:
                    raise ValueError('CI teardown immutable admission changed')
                witnesses.append({'admission_sha256': digest(ancestor),
                    'command_id': details['command']['command_id'],
                    'command_digest': command['request_digest'],
                    'response_sha256': digest(json.loads(command['response_json']))})
            ancestor = details.get('original_recovery')
        proofs = quarantine_history(previous)
        # The typed owner above enforces version1, original request/input/output,
        # result/session and raw journal proof. An absent chain grants nothing.
        result = ({'schema': FIELD + '/v1', 'profile_sha256': APPROVED_PROFILE_SHA256,
                   'subject_sha256': digest(profile['subject']),
                   'predecessor_spec_sha256': digest(spec),
                   'chain_root_sha256': digest(previous), 'admissions': witnesses,
                   'quarantines': proofs} if proofs else None)
        after = {}
        if (_gate_roots(latest, execution_specs=after) != authority or after != bindings
                or db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                              (spec['run_id'],)).fetchone()[0] != row['recovery_json']):
            raise ValueError('CI teardown canonical custody changed during observation')
    return result


def snapshot_cleanup(store, spec, previous, candidate, publication, profile=None):
    """Only the inventory owner before ACK, or its durable ACK afterwards."""
    from .delivery_ci_inventory import _saved, archive
    from .delivery_stopped_resume import observed_preparation_cleanup
    from .delivery_unpublished_recovery import teardown_checkpoint

    if profile is None:
        if not _saved(store, spec['run_id']):
            return None
        _, _, profile = archive(store, spec['run_id'])
    proof = context(spec, previous, profile, candidate, publication, current=spec)
    if proof is None:
        return None
    observed = observed_preparation_cleanup(
        spec, resource_spec=spec, quarantined=teardown_checkpoint(proof['quarantines']))
    if context(spec, previous, profile, candidate, publication, current=spec) != proof:
        raise ValueError('CI teardown historical authority changed during physical observation')
    return {'context': proof, 'resources_sha256': observed}


def validate(store, recovery, profile):
    """Pure historical custody; current launch permission is validated elsewhere."""
    before = recovery['seal']['original_spec']
    expected = recovery['seal'].get(FIELD)
    actual = context(before, recovery['original_recovery'], profile,
                     recovery['seal']['candidate'], recovery['seal']['publication'])
    if actual != expected:
        raise ValueError('CI teardown differs from its immutable admitted context')
    return actual


def for_resources(spec):
    """An effectful resource reader requires the actual normal zero-CI ACK."""
    if not spec.get('config_path') or spec.get('gate_retry_stage') != 'ci':
        return None
    from .delivery_ci_inventory import FIELD as INVENTORY
    from .delivery_ci_inventory import authority
    from .delivery_execution_config import load_execution_config
    from .delivery_store import DeliveryStore
    from .delivery_unpublished_recovery import teardown_checkpoint

    store = DeliveryStore(load_execution_config(spec), read_only=True)
    with store._connect() as db:
        row = db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                         (spec['run_id'],)).fetchone()
    recovery = json.loads(row[0]) if row and row[0] else None
    if not recovery or not recovery.get('seal', {}).get(INVENTORY):
        return None
    if (recovery.get('kind') != 'published_ci_retry'
            or recovery.get('command', {}).get('additional_iterations') != 0
            or canonical_json(recovery.get('execution_spec')) != canonical_json(spec)):
        raise ValueError('CI resource teardown lost its current zero-model admission')
    _, _, profile = authority(store, recovery)
    proof = validate(store, recovery, profile)
    return teardown_checkpoint(proof['quarantines']) if proof else None
