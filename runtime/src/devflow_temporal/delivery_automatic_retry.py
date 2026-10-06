"""Finite fresh attempts for closed, unpublished original transient failures."""
from __future__ import annotations

import json
import sqlite3
import subprocess
from copy import deepcopy
from pathlib import Path

from .contracts import digest
from .delivery_broker import DeliveryBroker, _git
from .delivery_resources import observe_finalized_resources


def eligible(row, spec):
    checks = json.loads(row['checks_json'] or '{}')
    cleanup = checks.get('resource_cleanup', {})
    return (
        spec.get('automatic_retry_version') == spec.get('retry_budget_version') == 1
        and row['outcome'] == row['execution_state'] == row['phase'] == 'blocked'
        and row['cleanup'] == 'confirmed' and row['pr_json'] is None
        and row['recovery_json'] is None and not spec.get('continuation')
        and checks.get('failure', {}).get('classification') == 'transient'
        and cleanup.get('state') == cleanup.get('resource_cleanup') == 'confirmed'
        and cleanup.get('process_cleanup') == 'observed-native-confirmed'
        and bool(row['accepted_plan_text'] or spec.get('accepted_plan'))
    )


def fresh_unpublished_base(store, spec):
    """Absence must be observed successfully; an uncertain write is never retried."""
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
    if spec['base_ref'] in {'origin/' + branch, 'refs/remotes/origin/' + branch}:
        _git(source, 'fetch', '--no-write-fetch-head', 'origin',
             'refs/heads/' + branch + ':refs/remotes/origin/' + branch)
    if _git(source, 'rev-parse', spec['base_ref']) != base:
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
    if any(json.loads(row[0]).get('supersedes_run_id') == old['run_id']
           for row in db.execute('SELECT request_json FROM delivery_runs')):
        raise ValueError('predecessor already has a direct successor')
    if db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND kind='publish'",
                  (old['run_id'],)).fetchone() or db.execute(
            "SELECT 1 FROM delivery_attempts WHERE run_id=? AND state != 'finished'",
            (old['run_id'],)).fetchone():
        raise ValueError('predecessor effect or actor changed')
    if db.execute("SELECT 1 FROM delivery_outbox WHERE run_id=? AND state != 'sent'",
                  (old['run_id'],)).fetchone():
        raise ValueError('predecessor dispatch is unresolved')
    if store.state.claim_for(db, old['work_id']) is not None:
        raise ValueError('predecessor claim has not been released')
    if spec['policy'] != old['policy']:
        raise ValueError('fresh attempt changed frozen execution policy')
    if spec['base_sha'] != expected['base_sha']:
        raise ValueError('fresh base changed after readback')
    spec['accepted_plan'] = previous['accepted_plan_text'] or old['accepted_plan']
    spec['intake_required'] = False


def retry_once(store):
    """Per-row read-only uncertainty isolation; admissions use the ordinary outbox."""
    with store._connect() as db:
        rows = [dict(row) for row in db.execute(
            "SELECT * FROM delivery_runs WHERE outcome='blocked' AND recovery_json IS NULL")]
    admitted = []
    for row in rows:
        try:
            spec = json.loads(row['request_json'])
            if not eligible(row, spec) or not store.owns_execution(spec):
                continue
            with store._connect() as db:
                if any(json.loads(item[0]).get('supersedes_run_id') == spec['run_id']
                       for item in db.execute('SELECT request_json FROM delivery_runs')):
                    continue
                issue = store.state.issue_resource(spec['issue_url'])
                count = sum(store.state.issue_resource(item[0]) == issue for item in db.execute(
                    'SELECT issue_url FROM delivery_runs'))
                if count >= spec['policy']['max_attempts']:
                    continue
                if store.state.claim_for(db, spec['work_id']) is not None:
                    continue
                if db.execute("SELECT 1 FROM delivery_effects WHERE run_id=? AND kind='publish'",
                              (spec['run_id'],)).fetchone():
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
                    or result.get('cleanup') != 'confirmed'
                    or result.get('checks', {}).get('failure') != checks['failure']
                    or result.get('checks', {}).get('resource_cleanup')
                    != checks['resource_cleanup']):
                continue
            observe_finalized_resources(spec)
            base = fresh_unpublished_base(store, spec)
            identity = digest({'run_id': spec['run_id'],
                               'request_digest': row['request_digest']})[:24]
            supplied = {key: deepcopy(spec[key]) for key in (
                'work_id', 'issue_url', 'repository_key', 'goal', 'base_ref', 'authorized_endpoint',
                'origin_thread_id', 'recovery_key') if key in spec}
            supplied.update(command_id='automatic-' + identity, run_id='run-' + identity,
                            supersedes_run_id=spec['run_id'], branch='fix/automatic-' + identity,
                            accepted_plan=row['accepted_plan_text'] or spec['accepted_plan'])
            admitted.append(store.submit(supplied, _automatic={'row': row, 'base_sha': base}))
        except (ValueError, KeyError, TypeError, OSError, RuntimeError,
                subprocess.SubprocessError, sqlite3.Error):
            # Wait for conclusive fresh observations; no absence inferred from errors.
            continue
    return admitted
