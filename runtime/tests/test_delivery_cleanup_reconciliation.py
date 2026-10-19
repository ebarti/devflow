"""An owner can observe later cleanup without changing a stopped verdict."""
from __future__ import annotations

import copy

import pytest
from test_delivery_browser_reconciliation import recovery as recovery


@pytest.fixture
def stopped(recovery):
    r = recovery
    r.spec['policy'] = {'execution_backend': 'native-macos'}
    r.state['cleanup'] = 'unknown'
    r.state['error'] = 'implementer did not establish a pass'
    with r.store._connect() as db:
        db.execute('UPDATE delivery_runs SET error=?', (r.state['error'],))
        db.execute("UPDATE delivery_effects SET state='complete'")
    return r


def test_cleanup_observation_preserves_failure_and_never_runs_another_role(stopped):
    from devflow_temporal.delivery_cleanup_reconciliation import reconcile_stopped_cleanup

    before = copy.deepcopy(stopped.closed)
    result = reconcile_stopped_cleanup(stopped.store, 'run-owned', 2)
    assert result == {'state': 'finished', 'cleanup': 'confirmed', 'changed': True,
                      'run_id': 'run-owned', 'iteration': 2}
    assert stopped.calls == ['cleanup']
    assert stopped.closed == before


@pytest.mark.parametrize('drift', ['active', 'foreign_owner', 'iteration', 'closed_identity',
                                    'closed_verdict', 'candidate', 'checks', 'feature',
                                    'other_effect', 'mutation', 'unfinished_role'])
def test_cleanup_refuses_changed_authority_before_effects(stopped, drift):
    from devflow_temporal.delivery_cleanup_reconciliation import reconcile_stopped_cleanup

    r = stopped
    if drift == 'foreign_owner':
        r.store.owns_execution = lambda _: False
    elif drift == 'closed_identity':
        r.closed['request_digest'] = 'different'
    elif drift == 'closed_verdict':
        r.state['outcome'] = 'delivered'
    elif drift == 'candidate':
        r.state['candidate'] = {'id': 'different'}
    elif drift == 'checks':
        r.state['checks'] = {'review': {'state': 'passed'}}
    elif drift == 'feature':
        r.spec['feature_worker'] = {'kind': 'chunk'}
    else:
        with r.store._connect() as db:
            if drift == 'active':
                db.execute("UPDATE delivery_runs SET phase='repair'")
            elif drift == 'iteration':
                db.execute('UPDATE delivery_runs SET iteration=3')
            elif drift == 'other_effect':
                db.execute("UPDATE delivery_effects SET state='pending'")
            elif drift == 'mutation':
                db.execute("INSERT INTO delivery_mutations VALUES ('run-owned','pending','cancel')")
            elif drift == 'unfinished_role':
                db.execute("INSERT INTO delivery_attempts VALUES ('run-owned','running')")
    with r.store._connect() as db:
        before = db.serialize()
    with pytest.raises(ValueError):
        reconcile_stopped_cleanup(r.store, 'run-owned', 2)
    assert r.calls == []
    with r.store._connect() as db:
        assert db.serialize() == before


def test_confirmed_cleanup_without_its_record_is_independently_closed(stopped, monkeypatch):
    from devflow_temporal import delivery_policy_recovery
    from devflow_temporal.delivery_cleanup_reconciliation import reconcile_stopped_cleanup

    with stopped.store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='confirmed'")
    monkeypatch.setattr(delivery_policy_recovery, 'reconciled_cleanup', lambda *a, **kw: None)
    result = reconcile_stopped_cleanup(stopped.store, 'run-owned', 2)
    assert result['changed'] is True
    assert stopped.calls == ['cleanup']


def test_confirmed_cleanup_with_its_record_is_idempotent(stopped):
    from devflow_temporal.delivery_cleanup_reconciliation import reconcile_stopped_cleanup

    with stopped.store._connect() as db:
        db.execute("UPDATE delivery_runs SET cleanup='confirmed'")
    result = reconcile_stopped_cleanup(stopped.store, 'run-owned', 2)
    assert result['state'] == 'finished' and result['changed'] is False
    assert stopped.calls == []
