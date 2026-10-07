"""Finite fresh attempts for closed, unpublished original transient failures."""
from __future__ import annotations

import json
import re
import sqlite3
import subprocess
import time
from copy import deepcopy
from pathlib import Path

from .contracts import digest
from .delivery_activities import _tracker_sync
from .delivery_broker import DeliveryBroker, _git
from .delivery_resources import (
    RunResources,
    observe_completed_resources,
    observe_finalized_resources,
)


def eligible(row, spec):
    checks = json.loads(row['checks_json'] or '{}')
    return (
        spec.get('automatic_retry_version') == spec.get('retry_budget_version') == 1
        and row['outcome'] == row['execution_state'] == row['phase'] == 'blocked'
        and row['cleanup'] in {'unknown', 'confirmed'} and row['pr_json'] is None
        and row['recovery_json'] is None and not spec.get('continuation')
        and checks.get('failure', {}).get('classification') == 'transient'
        and bool(row['accepted_plan_text'] or spec.get('accepted_plan'))
    )


def unavailable(store, db, spec, *, own_claim_allowed=False):
    """Cheap eligibility repeated inside the ordinary admission transaction."""
    issue = store.state.issue_resource(spec['issue_url'])
    latest = next((row['run_id'] for row in db.execute(
        'SELECT run_id,issue_url FROM delivery_runs ORDER BY rowid DESC')
        if store.state.issue_resource(row['issue_url']) == issue), None)
    claim = store.state.claim_for(db, spec['work_id'])
    return (
        latest != spec['run_id']
        or (claim is not None and not (own_claim_allowed
            and claim['owner'] == 'external:devflow:' + spec['run_id']))
        or db.execute("SELECT 1 FROM delivery_mutations WHERE run_id=? AND "
                      "(state IN ('pending','unknown') OR (kind='cancel' AND state='complete'))",
                      (spec['run_id'],)).fetchone() is not None
        or db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND "
                      "(kind='publish' OR state != 'complete')",
                      (spec['run_id'],)).fetchone() is not None
        or db.execute("SELECT 1 FROM delivery_attempts WHERE run_id=? AND state != 'finished'",
                      (spec['run_id'],)).fetchone() is not None
        or db.execute("SELECT 1 FROM delivery_outbox WHERE run_id=? AND state != 'sent'",
                      (spec['run_id'],)).fetchone() is not None
    )


def _current(store, row, spec, stopped):
    with store._connect() as db:
        current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                             (spec['run_id'],)).fetchone()
        if (current is None or dict(current) != row or stopped()
                or not store.owns_execution(spec)
                or unavailable(store, db, spec, own_claim_allowed=True)):
            raise ValueError('closed predecessor changed before cleanup or tracker effect')
        return [dict(item) for item in db.execute(
            'SELECT * FROM delivery_attempts WHERE run_id=?', (spec['run_id'],))]


def _close_cleanup(store, row, spec, closed, stopped):
    """Keep historical failure immutable; record separately authenticated later cleanup."""
    attempts = _current(store, row, spec, stopped)
    execution = store.intake_execution_spec(spec['run_id'])
    before = observe_completed_resources(execution, attempts)
    unpublished(store, spec)
    if _current(store, row, spec, stopped) != attempts:
        raise ValueError('original finished attempts changed before finalization')
    receipt = RunResources(execution, read_only=True).finalize('blocked')
    observed = observe_finalized_resources(execution)
    if (not (receipt.get('state') == receipt.get('resource_cleanup') == 'confirmed')
            or receipt.get('process_cleanup') != 'observed-native-confirmed'
            or observed['finalization_sha256'] != receipt.get('receipt_sha256')
            or before['journal_sha256'] != observed['journal_sha256']):
        raise ValueError('fresh original cleanup remains unconfirmed')
    if _current(store, row, spec, stopped) != attempts:
        raise ValueError('original completed attempts changed before tracker closure')
    unpublished(store, spec)
    if _current(store, row, spec, stopped) != attempts:
        raise ValueError('original attempts changed during publication readback')
    tracker = _tracker_sync(execution, 'blocked', release=True, terminal=True,
                            reason=closed['result'].get('error') or 'Retained transient failure')
    if tracker.get('state') != 'consistent' or tracker.get('pending') is not False:
        raise ValueError('terminal tracker closure remains unacknowledged')
    checks = json.loads(row['checks_json'])
    with store._connect() as db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                             (spec['run_id'],)).fetchone()
        if (dict(current) != row or stopped() or unavailable(store, db, spec)
                or [dict(item) for item in db.execute(
                    'SELECT * FROM delivery_attempts WHERE run_id=?',
                    (spec['run_id'],))] != attempts):
            raise ValueError('predecessor changed after terminal tracker acknowledgement')
        # The old checks remain in the existing append-only event transaction.
        store._event(db, spec['run_id'], row['revision'] + 1, 'resource_cleanup_reconciled',
            'Owner retry maintenance independently confirmed later cleanup and claim closure',
            {'original_checks': checks, 'closed_workflow': {
                key: value for key, value in closed.items()
                if key != 'result'}, 'original_result_digest': digest(closed['result']),
             'resource_cleanup': receipt, 'tracker': tracker,
             'original_attempts': attempts, 'native_observation': observed})
        db.execute('UPDATE delivery_runs SET checks_json=?,cleanup=?,tracker_json=?,revision=? '
                   'WHERE run_id=?', (json.dumps({**checks, 'resource_cleanup': receipt}),
                    'confirmed', json.dumps(tracker), row['revision'] + 1, spec['run_id']))
        updated = dict(db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                                 (spec['run_id'],)).fetchone())
    return updated


def _reuse_cleanup(store, row, spec, closed, stopped):
    """Reuse only an authenticated later closure, without projecting it again."""
    checks = json.loads(row['checks_json'])
    with store._connect() as db:
        if unavailable(store, db, spec):
            return False
        records = [json.loads(item[0]) for item in db.execute(
            "SELECT payload_json FROM delivery_events WHERE run_id=? "
            "AND type='resource_cleanup_reconciled' ORDER BY rowid DESC", (spec['run_id'],))]
    prior = next((item for item in records
                  if item.get('original_result_digest') == digest(closed['result'])
                  and item.get('closed_workflow') == {key: value for key, value in closed.items()
                                                       if key != 'result'}
                  and item.get('resource_cleanup') == checks.get('resource_cleanup')
                  and item.get('tracker') == json.loads(row['tracker_json'] or '{}')), None)
    if prior is None:
        return False
    attempts = _current(store, row, spec, stopped)
    execution = store.intake_execution_spec(spec['run_id'])
    fresh = observe_completed_resources(execution, attempts)
    observed = observe_finalized_resources(execution)
    if (attempts != prior['original_attempts']
            or fresh['journal_sha256'] != prior['native_observation']['journal_sha256']
            or observed['finalization_sha256'] != checks['resource_cleanup']['receipt_sha256']):
        raise ValueError('later cleanup changed after its acknowledged closure')
    _current(store, row, spec, stopped)
    with store._connect() as db:
        if unavailable(store, db, spec):
            raise ValueError('released original claim changed before tracker readback')
    tracker = _tracker_sync(execution, 'blocked', release=True, terminal=True,
                            reason=closed['result'].get('error') or 'Retained transient failure')
    if tracker.get('state') != 'consistent' or tracker.get('pending') is not False:
        raise ValueError('acknowledged later tracker closure is unobservable')
    _current(store, row, spec, stopped)
    return True


def unpublished(store, spec):
    """Absence must be observed successfully before cleanup or claim release."""
    with store._connect() as db:
        if db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND kind='publish'",
                      (spec['run_id'],)).fetchone():
            raise ValueError('publication was attempted')
    source = Path(spec['source_path'])
    if _git(source, 'remote', 'get-url', 'origin') != spec['origin_url']:
        raise ValueError('configured origin changed')
    if _git(source, 'ls-remote', '--heads', 'origin', 'refs/heads/' + spec['branch']):
        raise ValueError('predecessor branch exists')
    if DeliveryBroker(store, spec)._read_owned_pr() is not None:
        raise ValueError('predecessor PR exists')


def fresh_unpublished_base(store, spec):
    unpublished(store, spec)
    source = Path(spec['source_path'])
    branch = spec.get('publication_base_ref')
    if not branch:
        raise ValueError('fresh attempt needs an approved named publication branch')
    remote = _git(source, 'ls-remote', '--heads', 'origin', 'refs/heads/' + branch)
    lines = remote.splitlines()
    if len(lines) != 1 or lines[0].split()[1] != 'refs/heads/' + branch:
        raise ValueError('fresh base is unobservable')
    base = lines[0].split()[0]
    repository = store.config.raw['repositories'][spec['repository_key']]
    if repository.get('expected_base_sha') not in (None, base):
        raise ValueError('pinned base moved from accepted plan')
    reference = spec['base_ref']
    if re.fullmatch(r'[0-9a-fA-F]{40}', reference):
        if reference.lower() != base:
            raise ValueError('pinned commit moved from approved current origin branch')
    elif reference in {branch, 'refs/heads/' + branch, 'origin/' + branch,
                       'refs/remotes/origin/' + branch}:
        reference = 'refs/remotes/origin/' + branch
        _git(source, 'fetch', '--no-write-fetch-head', 'origin',
             'refs/heads/' + branch + ':' + reference)
    else:
        raise ValueError('configured base is not the approved named branch')
    if _git(source, 'rev-parse', reference) != base:
        raise ValueError('configured base does not equal approved current origin branch')
    return base


def validate_transaction(store, db, spec, expected):
    previous = db.execute('SELECT * FROM delivery_runs WHERE run_id=?',
                          (spec['supersedes_run_id'],)).fetchone()
    if previous is None or dict(previous) != expected['row']:
        raise ValueError('predecessor changed after closure observation')
    old = json.loads(previous['request_json'])
    if not eligible(previous, old) or not store.owns_execution(old):
        raise ValueError('predecessor no longer eligible')
    if unavailable(store, db, old) or expected.get('stopped', lambda: False)():
        raise ValueError('predecessor is no longer the latest safe issue attempt')
    if spec['policy'] != old['policy']:
        raise ValueError('fresh attempt changed frozen execution policy')
    if spec['base_sha'] != expected['base_sha']:
        raise ValueError('fresh base changed after readback')
    spec['accepted_plan'] = previous['accepted_plan_text'] or old['accepted_plan']
    spec['intake_required'] = False


def retry_once(store, *, stopped=lambda: False):
    """Per-row read-only uncertainty isolation; admissions use the ordinary outbox."""
    with store._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM delivery_runs WHERE outcome='blocked' AND recovery_json IS NULL")]
    waits = getattr(store, '_automatic_retry_waits', {})
    store._automatic_retry_waits = waits
    admitted = []
    for row in rows:
        if stopped():
            break
        delay, due = waits.get(row['run_id'], (5, 0))
        if time.monotonic() < due:
            continue
        waits[row['run_id']] = (min(delay * 2, 300), time.monotonic() + delay)
        try:
            spec = json.loads(row['request_json'])
            if not eligible(row, spec) or not store.owns_execution(spec):
                continue
            with store._connect() as db:
                if unavailable(store, db, spec, own_claim_allowed=True):
                    continue
                issue = store.state.issue_resource(spec['issue_url'])
                count = sum(store.state.issue_resource(item[0]) == issue for item in db.execute(
                    'SELECT issue_url FROM delivery_runs'))
                if (count >= spec['policy']['max_attempts'] and row['cleanup'] == 'confirmed'
                        and store.state.claim_for(db, spec['work_id']) is None):
                    continue
            closed = store._completed_temporal_result(
                spec['run_id'], workflow_id=row['workflow_id'])
            result = closed['result']
            checks = json.loads(row['checks_json'])
            if (closed['request_digest'] != row['request_digest']
                    or closed['recovery_digest'] is not None
                    or not closed['execution_run_id'] or not closed['closed_at']
                    or closed['workflow_id'] != (row['workflow_id'] or 'delivery-' + spec['run_id'])
                    or result.get('run_id') != spec['run_id']
                    or result.get('phase') != 'blocked'
                    or result.get('pull_request') is not None
                    or result.get('outcome') != 'blocked'
                    or result.get('execution_state') != 'blocked'
                    or result.get('cleanup') not in {'unknown', 'confirmed'}
                    or result.get('checks', {}).get('failure') != checks['failure']
                    or not isinstance(result.get('checks', {}).get('resource_cleanup'), dict)
                    or result['checks']['resource_cleanup'].get('state')
                    != result.get('cleanup')
                    or result['checks']['resource_cleanup'].get('resource_cleanup')
                    != result.get('cleanup')):
                continue
            execution = store.intake_execution_spec(spec['run_id'])
            with store._connect() as db:
                retained_claim = store.state.claim_for(db, spec['work_id']) is not None
            original_confirmed = (result.get('cleanup') == row['cleanup'] == 'confirmed'
                                  and result['checks']['resource_cleanup']
                                  == checks.get('resource_cleanup'))
            if original_confirmed and not retained_claim:
                observed = observe_finalized_resources(execution)
                expected_cleanup = checks.get('resource_cleanup', {}).get('receipt_sha256')
                if (not expected_cleanup or row['cleanup'] != 'confirmed'
                        or observed['finalization_sha256'] != expected_cleanup):
                    continue
            elif not _reuse_cleanup(store, row, spec, closed, stopped):
                row = _close_cleanup(store, row, spec, closed, stopped)
            with store._connect() as db:
                issue = store.state.issue_resource(spec['issue_url'])
                count = sum(store.state.issue_resource(item[0]) == issue for item in db.execute(
                    'SELECT issue_url FROM delivery_runs'))
                if count >= spec['policy']['max_attempts'] or unavailable(store, db, spec):
                    continue
            base = fresh_unpublished_base(store, spec)
            identity = digest({'run_id': spec['run_id'],
                               'request_digest': row['request_digest']})[:24]
            supplied = {key: deepcopy(spec[key]) for key in (
                'work_id', 'issue_url', 'repository_key', 'goal', 'base_ref', 'authorized_endpoint',
                'origin_thread_id', 'recovery_key') if key in spec}
            supplied.update(command_id='automatic-' + identity, run_id='run-' + identity,
                            supersedes_run_id=spec['run_id'], branch='fix/automatic-' + identity,
                            accepted_plan=row['accepted_plan_text'] or spec['accepted_plan'])
            admitted.append(store.submit(supplied, _automatic={
                'row': row, 'base_sha': base, 'stopped': stopped}))
        except (ValueError, KeyError, TypeError, OSError, RuntimeError,
                subprocess.SubprocessError, sqlite3.Error):
            # Wait for conclusive fresh observations; no absence inferred from errors.
            continue
    return admitted
