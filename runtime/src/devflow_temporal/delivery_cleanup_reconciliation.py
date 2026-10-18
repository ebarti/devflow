"""Owner observation of cleanup for an ordinary, already closed published run."""
from __future__ import annotations

import json

from .contracts import digest


def reconcile_stopped_cleanup(store, run_id: str, iteration: int) -> dict:
    """Close only native resources and the claim; never retry or change a verdict."""
    from .delivery_automatic_retry import _close_cleanup
    from .delivery_interrupted_recovery import installation_recovery_lease
    from .delivery_policy_recovery import reconciled_cleanup

    with installation_recovery_lease(store.config):
        with store._connect() as db:
            saved = db.execute('SELECT * FROM delivery_runs WHERE run_id=?', (run_id,)).fetchone()
        if saved is None:
            raise ValueError('cleanup reconciliation run is absent')
        row = dict(saved)
        spec = store.effective_spec(run_id)
        if (spec.get('provider') != 'codex' or not store.owns_execution(spec)
                or spec.get('policy', {}).get('execution_backend') != 'native-macos'
                or spec.get('feature_worker') or spec.get('feature_delivery')
                or row['phase'] != row['outcome'] or row['phase'] != 'blocked'
                or row['execution_state'] != 'blocked' or not row['pr_json']
                or type(iteration) is not int or row['iteration'] != iteration
                or row['cleanup'] not in {'unknown', 'confirmed'}):
            raise ValueError('cleanup reconciliation requires an owned closed published run')
        closed = store._completed_temporal_result(run_id, workflow_id=row['workflow_id'])
        state = closed['result']
        recovery = json.loads(row['recovery_json']) if row['recovery_json'] else None
        checks = json.loads(row['checks_json'] or '{}')
        if (closed['workflow_id'] != row['workflow_id']
                or not closed['execution_run_id'] or not closed['closed_at']
                or closed['request_digest'] != row['request_digest']
                or closed['recovery_digest'] != (digest(recovery) if recovery else None)
                or state.get('run_id') != run_id or state.get('revision') != row['protocol_revision']
                or state.get('cleanup') != 'unknown'
                or any(state.get(key) != row[key] for key in
                       ('phase', 'outcome', 'execution_state', 'iteration', 'error'))
                or {k: v for k, v in state.get('checks', {}).items() if k != 'resource_cleanup'}
                != {k: v for k, v in checks.items() if k != 'resource_cleanup'}
                or state.get('candidate') != json.loads(row['candidate_json'])
                or state.get('pull_request') != json.loads(row['pr_json'])):
            raise ValueError('cleanup reconciliation lost its exact closed execution')
        with store._connect() as db:
            if (db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND state!='complete'",
                           (run_id,)).fetchone()
                    or db.execute("SELECT 1 FROM delivery_attempts WHERE run_id=? AND state!='finished'",
                                  (run_id,)).fetchone()
                    or db.execute("SELECT 1 FROM delivery_mutations WHERE run_id=? AND "
                                  "(state!='complete' OR kind='cancel')", (run_id,)).fetchone()):
                raise ValueError('cleanup reconciliation has an unfinished effect or role')
        outcome = {'state': 'finished', 'cleanup': 'confirmed', 'changed': False,
                   'run_id': run_id, 'iteration': iteration}
        if row['cleanup'] == 'confirmed':
            if reconciled_cleanup(store, row, state, closed=closed) is not None:
                return outcome
            # Generic maintenance may have observed cleanup before binding its
            # claim closure to this producer. Independently perform that closure.
        if state.get('tracker') != json.loads(row['tracker_json'] or 'null'):
            raise ValueError('cleanup reconciliation tracker changed before closure')
        updated = _close_cleanup(store, row, spec, closed, lambda: False, published=True)
        return {**outcome, 'cleanup': updated['cleanup'], 'changed': True}
