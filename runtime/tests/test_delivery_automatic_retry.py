from __future__ import annotations

import copy
import json
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest
from test_delivery_store import service as service

from devflow_temporal import delivery_automatic_retry as retry
from devflow_temporal.delivery_store import DeliveryStore


def stopped(service, monkeypatch):
    store, request = service
    store.submit(request)
    store.mark_start(request["run_id"], accepted=True)
    failure = {'classification': 'transient', 'stage': 'preparing',
               'reason': 'transport unavailable', 'cause_type': 'TimeoutError'}
    checks = {'failure': failure, 'resource_cleanup': {
        'state': 'confirmed', 'process_cleanup': 'observed-native-confirmed',
        'resource_cleanup': 'confirmed', 'receipt_sha256': 'fixture-finalization'}}
    store.project(request['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='retained failure', outcome='blocked',
                  cleanup='confirmed', checks=checks)
    with store._connect() as db:
        store.state.release_work(db, request['work_id'], 'external:devflow:' + request['run_id'])
    spec = store.spec(request['run_id'])
    closed = {'workflow_id': 'delivery-' + request['run_id'], 'execution_run_id': 'execution',
              'closed_at': '2026-10-07T00:00:00+00:00', 'request_digest': spec['request_digest'],
              'recovery_digest': None, 'result': {'run_id': request['run_id'], 'outcome': 'blocked',
              'phase': 'blocked', 'execution_state': 'blocked',
              'cleanup': 'confirmed', 'checks': checks}}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result', lambda *_a, **_k: closed)
    monkeypatch.setattr(retry, 'observe_finalized_resources',
                        lambda *_a, **_k: {'finalization_sha256': 'fixture-finalization'})
    monkeypatch.setattr(retry, 'fresh_unpublished_base', lambda *_a: spec['base_sha'])
    return store, request, closed


def test_transient_closed_run_gets_one_fresh_attempt_with_original_plan(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    before = store.spec(request['run_id'])
    results = retry.retry_once(store)
    assert len(results) == 1
    new = store.spec(results[0]['run_id'])
    assert new['supersedes_run_id'] == request['run_id']
    assert new['accepted_plan'] == before['accepted_plan']
    assert new['policy'] == before['policy']
    assert new['checkout'] != before['checkout']
    assert new['state_dir'] != before['state_dir']
    assert new['branch'] != before['branch']
    assert 'continuation' not in new
    with store._connect() as db:
        assert db.execute('SELECT outcome FROM delivery_runs WHERE run_id=?',
                          (request['run_id'],)).fetchone()[0] == 'blocked'
    assert retry.retry_once(store) == []
    assert len(store.pending_starts()) == 1


@pytest.mark.parametrize('obstacle', ['terminal', 'legacy', 'pr', 'recovery', 'cleanup',
                                     'claim', 'effect', 'closure', 'failure_mismatch'])
def test_unknown_or_unsafe_predecessor_never_admits(service, monkeypatch, obstacle):
    store, request, closed = stopped(service, monkeypatch)
    with store._connect() as db:
        if obstacle == 'terminal':
            checks = json.loads(db.execute('SELECT checks_json FROM delivery_runs').fetchone()[0])
            checks['failure']['classification'] = 'terminal'
            db.execute('UPDATE delivery_runs SET checks_json=?', (json.dumps(checks),))
        if obstacle == 'legacy':
            spec = store.spec(request['run_id'])
            spec.pop('automatic_retry_version')
            db.execute('UPDATE delivery_runs SET request_json=?', (json.dumps(spec),))
        if obstacle == 'pr':
            db.execute('UPDATE delivery_runs SET pr_json=?', ('{"number":1}',))
        if obstacle == 'recovery':
            db.execute('UPDATE delivery_runs SET recovery_json=?', ('{"kind":"legacy"}',))
        if obstacle == 'cleanup':
            db.execute("UPDATE delivery_runs SET cleanup='unknown'")
        if obstacle == 'claim':
            store.state.claim_work(db, request['work_id'], 'foreign', 'test')
        if obstacle == 'effect':
            db.execute("INSERT INTO delivery_effects VALUES "
                       "('publish',?,'publish','{}','pending',NULL,'now')", (request['run_id'],))
    if obstacle == 'closure':
        monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                            lambda *_a, **_k: (_ for _ in ()).throw(ValueError('not closed')))
    if obstacle == 'failure_mismatch':
        closed['result']['checks'] = copy.deepcopy(closed['result']['checks'])
        closed['result']['checks']['failure']['stage'] = 'other'
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


def test_concurrent_retry_admits_only_one_successor(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda _: retry.retry_once(store), range(2)))
    with store._connect() as db:
        rows = [json.loads(row[0]) for row in db.execute('SELECT request_json FROM delivery_runs')]
        assert len(rows) == 2
        assert sum(row.get('supersedes_run_id') == request['run_id'] for row in rows) == 1


def test_transaction_rechecks_changed_predecessor(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    def changed(_store, spec):
        with _store._connect() as db:
            db.execute('UPDATE delivery_runs SET revision=revision+1')
        return spec['base_sha']
    monkeypatch.setattr(retry, 'fresh_unpublished_base', changed)
    assert retry.retry_once(store) == []
    assert len(store.pending_starts()) == 0


def test_exhausted_budget_never_creates_new_attempt(service, monkeypatch):
    store, request = service
    raw = copy.deepcopy(store.config.raw)
    raw['max_attempts'] = 1
    store.config.path.write_text(json.dumps(raw))
    from devflow_temporal.delivery_config import DeliveryConfig
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    store, request, _ = stopped((store, request), monkeypatch)
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.parametrize('obstacle', [None, 'branch', 'pr', 'lost_publish', 'github', 'pin'])
def test_fresh_base_requires_actual_git_absence_and_successful_github_readback(
    service, monkeypatch, obstacle,
):
    import subprocess
    from pathlib import Path

    from devflow_temporal.delivery_broker import _git
    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    _git(source, 'branch', '-M', 'main')
    _git(source, 'push', 'origin', 'main')
    raw = copy.deepcopy(store.config.raw)
    raw['repositories']['fixture']['base_ref'] = 'origin/main'
    store.config.path.write_text(json.dumps(raw))
    from devflow_temporal.delivery_config import DeliveryConfig
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    store.submit({**request, 'base_ref': 'origin/main'})
    spec = store.spec(request['run_id'])
    spec['publication_base_ref'] = 'main'
    monkeypatch.setattr(retry.DeliveryBroker, '_read_owned_pr',
                        lambda _self: {'number': 1} if obstacle == 'pr' else None)
    if obstacle == 'github':
        monkeypatch.setattr(retry.DeliveryBroker, '_read_owned_pr',
                            lambda _self: (_ for _ in ()).throw(RuntimeError('unavailable')))
    if obstacle == 'branch':
        _git(source, 'push', 'origin', 'HEAD:refs/heads/' + spec['branch'])
    if obstacle == 'lost_publish':
        with store._connect() as db:
            db.execute("INSERT INTO delivery_effects VALUES "
                       "('publish',?,'publish','{}','pending',NULL,'now')", (request['run_id'],))
    if obstacle == 'pin':
        (source / 'README.md').write_text('new approved base\n')
        _git(source, 'commit', '-am', 'next base')
        _git(source, 'push', 'origin', 'main')
    if obstacle:
        with pytest.raises((ValueError, RuntimeError, subprocess.SubprocessError)):
            retry.fresh_unpublished_base(store, spec)
    else:
        assert retry.fresh_unpublished_base(store, spec) == spec['base_sha']


@pytest.mark.parametrize('field', ['request_digest', 'recovery_digest', 'workflow_id',
                                   'execution_run_id', 'closed_at', 'result_pr', 'result_run'])
def test_closed_execution_identity_and_publication_are_not_projection_guesses(
    service, monkeypatch, field,
):
    store, request, closed = stopped(service, monkeypatch)
    if field == 'result_pr':
        closed['result']['pull_request'] = {'number': 1}
    elif field == 'result_run':
        closed['result']['run_id'] = 'foreign'
    else:
        closed[field] = None if field in {'execution_run_id', 'closed_at'} else 'foreign'
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_service_loop_admits_closed_transient_successor_without_an_operator(
    service, monkeypatch,
):
    import asyncio

    from devflow_temporal.delivery_api import DeliveryService
    store, request, _ = stopped(service, monkeypatch)
    app = object.__new__(DeliveryService)
    app.store = store
    async def idle():
        pass
    real_sleep = asyncio.sleep
    async def end(_seconds):
        for _ in range(100):
            with store._connect() as db:
                if db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 2:
                    raise asyncio.CancelledError
            await real_sleep(.01)
        raise AssertionError('background retry did not admit its eligible successor')
    monkeypatch.setattr(app, 'dispatch_once', idle)
    monkeypatch.setattr(app, 'dispatch_questions_once', idle)
    monkeypatch.setattr(asyncio, 'sleep', end)
    with pytest.raises(asyncio.CancelledError):
        await app.dispatch_loop()
    with store._connect() as db:
        rows = [json.loads(row[0]) for row in db.execute('SELECT request_json FROM delivery_runs')]
        assert len(rows) == 2
        assert rows[1]['supersedes_run_id'] == request['run_id']


def test_actual_cleanup_must_match_closed_controller_finalization_hash(service, monkeypatch):
    store, request, _ = stopped(service, monkeypatch)
    monkeypatch.setattr(retry, 'observe_finalized_resources',
                        lambda *_a: {'finalization_sha256': 'changed-finalization'})
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


@pytest.mark.parametrize('outcome', ['delivered', 'blocked', None])
def test_newer_canonical_issue_admission_prevents_stale_retry(service, monkeypatch, outcome):
    store, request, _ = stopped(service, monkeypatch)
    store.submit({**request, 'command_id': 'new', 'run_id': 'new', 'branch': 'fix/new',
                  'issue_url': request['issue_url'].replace('/3', '/03')})
    with store._connect() as db:
        db.execute("UPDATE delivery_runs SET outcome=? WHERE run_id='new'", (outcome,))
        store.state.release_work(db, request['work_id'], 'external:devflow:new')
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda *_a, **_k: pytest.fail('stale issue reached readback'))
    assert retry.retry_once(store) == []


@pytest.mark.parametrize('obstacle', ['mutation_pending', 'mutation_unknown', 'cancel_complete',
                                     'effect_pending', 'effect_unknown', 'attempt', 'outbox',
                                     'missing_plan', 'continuation'])
def test_unsafe_state_is_filtered_before_remote_observation(service, monkeypatch, obstacle):
    store, request, _ = stopped(service, monkeypatch)
    with store._connect() as db:
        if obstacle.startswith('mutation') or obstacle == 'cancel_complete':
            state = (obstacle.removeprefix('mutation_')
                     if obstacle.startswith('mutation') else 'complete')
            db.execute('INSERT INTO delivery_mutations '
                       '(command_id,run_id,kind,request_digest,state) '
                       'VALUES (?,?,?,?,?)',
                       ('intent', request['run_id'], 'cancel', 'digest', state))
        elif obstacle.startswith('effect'):
            db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                       ('tracker', request['run_id'], 'tracker', '{}',
                        obstacle.removeprefix('effect_'), None, 'now'))
        elif obstacle == 'outbox':
            db.execute("UPDATE delivery_outbox SET state='pending'")
        elif obstacle == 'attempt':
            # Reuse the real supervisor's ordinary attempt record shape.
            from devflow_temporal.supervisor import DeliverySupervisor
            DeliverySupervisor(store, capacity=1)._claim({
                'spec': store.spec(request['run_id']), 'role': 'implement', 'iteration': 0,
                'candidate': {'id': 'fixture', 'head': 'fixture', 'worktree_sha256': 'fixture'}})
        else:
            spec = store.spec(request['run_id'])
            if obstacle == 'missing_plan':
                spec['accepted_plan'] = ''
            else:
                spec['continuation'] = {'original': 'fixture'}
            db.execute('UPDATE delivery_runs SET request_json=?', (json.dumps(spec),))
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda *_a, **_k: pytest.fail('unsafe row reached expensive readback'))
    assert retry.retry_once(store) == []


@pytest.mark.parametrize('change', ['newer_issue', 'mutation_unknown', 'effect_pending'])
def test_transaction_rechecks_late_issue_and_operator_effects(service, monkeypatch, change):
    store, request, _ = stopped(service, monkeypatch)
    def race(_store, spec):
        if change == 'newer_issue':
            store.submit({**request, 'command_id': 'new', 'run_id': 'new', 'branch': 'fix/new'})
            with store._connect() as db:
                store.state.release_work(db, request['work_id'], 'external:devflow:new')
        else:
            with store._connect() as db:
                if change == 'mutation_unknown':
                    db.execute('INSERT INTO delivery_mutations '
                               '(command_id,run_id,kind,request_digest,state) '
                               "VALUES ('cancel',?,'cancel','digest','unknown')",
                               (request['run_id'],))
                else:
                    db.execute('INSERT INTO delivery_effects VALUES (?,?,?,?,?,?,?)',
                               ('late', request['run_id'], 'tracker', '{}', 'pending', None, 'now'))
        return spec['base_sha']
    monkeypatch.setattr(retry, 'fresh_unpublished_base', race)
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert not any(json.loads(row[0]).get('supersedes_run_id') == request['run_id']
                       for row in db.execute('SELECT request_json FROM delivery_runs'))


def test_unknown_readbacks_have_bounded_per_run_backoff(service, monkeypatch):
    store, _request, _ = stopped(service, monkeypatch)
    clock = [0]
    calls = []
    from types import SimpleNamespace
    monkeypatch.setattr(retry, 'time', SimpleNamespace(monotonic=lambda: clock[0]), raising=False)
    def unknown(*_a, **_k):
        calls.append(clock[0])
        raise ValueError('unavailable')
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result', unknown)
    for _ in range(4):
        assert retry.retry_once(store) == []
    assert calls == [0]
    for delay in [5, 10, 20, 40, 80, 160, 300, 300]:
        clock[0] += delay
        assert retry.retry_once(store) == []
        retry.retry_once(store)
    assert calls == [0, 5, 15, 35, 75, 155, 315, 615, 915]


@pytest.mark.asyncio
async def test_slow_retry_observer_cannot_block_dispatch_or_outlive_stop(service, monkeypatch):
    import asyncio
    import time

    from devflow_temporal.delivery_api import DeliveryService
    store, request, _ = stopped(service, monkeypatch)
    app = object.__new__(DeliveryService)
    app.store = store
    dispatched = []
    observers = []
    def slow(_store, *, stopped=lambda: False):
        observers.append('started')
        deadline = time.monotonic() + .15
        while not stopped() and time.monotonic() < deadline:
            time.sleep(.005)
        observers.append('stopped')
    monkeypatch.setattr(retry, 'retry_once', slow)
    async def dispatch():
        dispatched.append(True)
    async def questions():
        pass
    real_sleep = asyncio.sleep
    async def tick(_seconds):
        await real_sleep(.01)
        if len(dispatched) >= 3:
            raise asyncio.CancelledError
    monkeypatch.setattr(app, 'dispatch_once', dispatch)
    monkeypatch.setattr(app, 'dispatch_questions_once', questions)
    monkeypatch.setattr(asyncio, 'sleep', tick)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(app.dispatch_loop(), timeout=.5)
    assert len(dispatched) == 3 and observers == ['started', 'stopped']
    assert app._retry_task.done()
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1


def test_automatic_generations_spend_one_original_total_ceiling(service, monkeypatch):
    store, request, closed = stopped(service, monkeypatch)
    closures = {request['run_id']: closed}
    monkeypatch.setattr(DeliveryStore, '_completed_temporal_result',
                        lambda _store, run_id, **_k: closures[run_id])
    current = request['run_id']
    for _ in range(2):
        admitted = retry.retry_once(store)
        assert len(admitted) == 1
        current = admitted[0]['run_id']
        spec = store.spec(current)
        store.mark_start(current, accepted=True)
        store.project(current, phase='blocked', execution_state='blocked', outcome='blocked',
                      event_type='blocked', message='retained transient', cleanup='confirmed',
                      checks=closed['result']['checks'])
        with store._connect() as db:
            store.state.release_work(db, spec['work_id'], 'external:devflow:' + current)
        closures[current] = {**closed, 'workflow_id': 'delivery-' + current,
                             'request_digest': spec['request_digest'],
                             'result': {**closed['result'], 'run_id': current}}
    assert retry.retry_once(store) == []
    with store._connect() as db:
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 3


@pytest.mark.parametrize('reference', ['main', 'refs/heads/main', 'origin/main', 'sha'])
def test_moved_named_base_is_fetched_without_moving_local_worktree(service, monkeypatch, reference):
    from pathlib import Path

    from test_delivery_store import _git

    from devflow_temporal.delivery_config import DeliveryConfig
    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    _git(source, 'branch', '-M', 'main')
    _git(source, 'push', 'origin', 'main')
    old = _git(source, 'rev-parse', 'HEAD')
    base_ref = old if reference == 'sha' else reference
    raw = copy.deepcopy(store.config.raw)
    repository = raw['repositories']['fixture']
    repository['base_ref'] = base_ref
    repository.pop('expected_base_sha')
    store.config.path.write_text(json.dumps(raw))
    store = DeliveryStore(DeliveryConfig.load(store.config.path))
    request = {**request, 'base_ref': base_ref}
    fresh = retry.fresh_unpublished_base
    store, request, _ = stopped((store, request), monkeypatch)
    monkeypatch.setattr(retry, 'fresh_unpublished_base', fresh)
    spec = store.spec(request['run_id'])
    spec['publication_base_ref'] = 'main'
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=?', (json.dumps(spec),))
    monkeypatch.setattr(retry.DeliveryBroker, '_read_owned_pr', lambda _self: None)
    other = source.parent / 'other'
    _git(source, 'clone', str(source.parent / 'origin.git'), str(other))
    _git(other, 'checkout', 'main')
    (other / 'README.md').write_text('approved updated base\n')
    _git(other, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
         'commit', '-am', 'updated base')
    _git(other, 'push', 'origin', 'main')
    new = _git(other, 'rev-parse', 'HEAD')
    (source / 'README.md').write_text('precious local uncommitted change\n')
    if reference == 'sha':
        with pytest.raises(ValueError):
            retry.fresh_unpublished_base(store, spec)
        assert retry.retry_once(store) == []
        assert not store.pending_starts()
    else:
        assert retry.fresh_unpublished_base(store, spec) == new
        admitted = retry.retry_once(store)
        assert len(admitted) == 1
        successor = store.spec(admitted[0]['run_id'])
        assert successor['base_sha'] == new and successor['base_ref'] == base_ref
        assert len(store.pending_starts()) == 1
    assert _git(source, 'rev-parse', 'HEAD') == old
    assert (source / 'README.md').read_text() == 'precious local uncommitted change\n'


@pytest.mark.skipif(sys.platform != "darwin", reason="actual macOS cleanup identity inspection")
def test_retry_observes_real_cleanup_using_accepted_intake_plan(service, monkeypatch):
    from pathlib import Path

    from test_delivery_store import _git

    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_resources import RunResources
    observe = retry.observe_finalized_resources
    fixture_store, _request = service
    admit = fixture_store.config.admit
    def trusted_fixture(_self, request, **kwargs):
        spec = admit(request, **kwargs)
        spec['policy']['host_sandbox'] = 'trusted-local'
        spec['policy_digest'] = digest(spec['policy'])
        return spec
    monkeypatch.setattr(DeliveryConfig, 'admit', trusted_fixture)
    store, request, closed = stopped(service, monkeypatch)
    original = store.spec(request['run_id'])
    original['accepted_plan'] = ''
    plan = json.dumps({'verification': ['Run test_owned.py']})
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=?,accepted_plan_text=?',
                   (json.dumps(original), plan))
    execution = store.intake_execution_spec(request['run_id'])
    root = Path(execution['checkout'])
    root.parent.mkdir(parents=True, exist_ok=True)
    resources = RunResources(execution)
    resources.register(root, 'checkout')
    root.mkdir()
    resources.created(root)
    project = root / 'worker'
    (project / 'tests').mkdir(parents=True)
    (project / 'tests/test_owned.py').write_text('def test_owned(): assert True\n')
    (project / 'pyproject.toml').write_text('[project]\nname="fixture"\nversion="1"\n')
    (project / 'uv.lock').write_text('version=1\n')
    _git(root, 'init', '-q')
    _git(root, 'add', '.')
    broker = DeliveryBroker.__new__(DeliveryBroker)
    broker.spec = execution
    generated = broker._register_generated(root, ['worker/.venv'])
    (project / '.venv').mkdir()
    broker._record_generated(generated)
    final = resources.finalize('blocked')
    assert final['state'] == 'confirmed'
    with pytest.raises(ValueError, match='generated environment recorded plan changed'):
        observe(original)
    monkeypatch.setattr(retry, 'observe_finalized_resources', observe)
    closed['result']['checks']['resource_cleanup'] = final
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET checks_json=?',
                   (json.dumps(closed['result']['checks']),))
    admitted = retry.retry_once(store)
    assert len(admitted) == 1
    assert store.spec(admitted[0]['run_id'])['accepted_plan'] == plan


@pytest.mark.parametrize('reference', ['foreign', 'pin'])
def test_private_fresh_ref_cannot_change_configured_branch_or_sha_pin(service, reference):
    from pathlib import Path

    from test_delivery_store import _git

    from devflow_temporal.delivery_config import DeliveryConfig
    store, request = service
    source = Path(store.config.raw['repositories']['fixture']['source_path'])
    _git(source, 'branch', '-M', 'main')
    ref = _git(source, 'rev-parse', 'HEAD') if reference == 'pin' else 'main'
    store.config.raw['repositories']['fixture']['base_ref'] = ref
    store.config.path.write_text(json.dumps(store.config.raw))
    config = DeliveryConfig.load(store.config.path)
    with pytest.raises(ValueError, match='configured named branch'):
        config.admit({**request, 'base_ref': ref}, _base_ref='refs/remotes/origin/foreign')


def test_fresh_attempt_preserves_frozen_publication_summary(service, monkeypatch):
    store, request = service
    request.update(goal="Investigate the fixture. Publish only the documented design.",
                   publication_summary="docs: investigate fixture behavior")
    store, request, _ = stopped(service, monkeypatch)
    original = store.spec(request["run_id"])
    results = retry.retry_once(store)
    assert len(results) == 1
    fresh = store.spec(results[0]["run_id"])
    assert fresh["goal"] == original["goal"]
    assert fresh["publication_summary"] == original["publication_summary"]
    assert fresh["accepted_plan"] == original["accepted_plan"]
    assert fresh["policy"] == original["policy"]
