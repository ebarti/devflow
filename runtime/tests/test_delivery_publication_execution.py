"""Publication custody follows actual bodies, including a killed Python parent."""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_activities
from devflow_temporal.delivery_broker import DeliveryBroker


@pytest.fixture
def publication(service, monkeypatch, tmp_path):
    store, request = service
    store.submit(request)
    spec = store.spec(request['run_id'])
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    (broker.checkout / 'README.md').write_text('Owned publication edit\n')
    checked = broker.candidate()
    bindir = tmp_path / 'owned-bin'
    bindir.mkdir()
    fake = bindir / 'gh'
    fake.write_text('#!' + sys.executable + '\n' + '''import json,os,subprocess,sys,time
from pathlib import Path
root=Path(os.environ['PUBLICATION_FIXTURE'])
def event(kind):
    with (root/'events.jsonl').open('a') as out:
        out.write(json.dumps({'kind':kind,'pid':os.getpid()})+'\\n')
if sys.argv[1:3]==['pr','list']:
    if os.environ.get('PUBLICATION_LIST_FAIL') and not (root/'list-failed').exists():
        (root/'list-failed').touch()
        event('list-start')
        time.sleep(1)
        event('list-end')
        raise SystemExit('owned query transport failed before mutation')
    print((root/'receipt.json').read_text() if (root/'receipt.json').exists() else '[]')
elif sys.argv[1:3]==['pr','create']:
    event('create-start')
    if os.environ.get('PUBLICATION_FAIL'):
        event('create-end')
        raise SystemExit('lost or failed create response; outcome unconfirmed')
    time.sleep(1)
    receipt=json.loads(os.environ['PUBLICATION_RECEIPT'])
    receipt[0]['headRefOid']=subprocess.check_output(['/usr/bin/git','rev-parse','HEAD'],text=True).strip()
    (root/'receipt.json').write_text(json.dumps(receipt))
    event('create-end')
    print('https://example.invalid/pull/1')
else:
    raise SystemExit('unexpected fixture command')
''')
    fake.chmod(0o700)
    receipt = [{
        'number': 1, 'url': 'https://example.invalid/pull/1', 'state': 'OPEN',
        'isDraft': False, 'baseRefName': broker._publication_base_ref(),
        'headRefName': spec['branch'], 'headRefOid': None,
        'title': 'chore: Change a fixture',
    }]
    monkeypatch.setenv('PATH', str(bindir) + os.pathsep + os.environ['PATH'])
    monkeypatch.setenv('PUBLICATION_FIXTURE', str(tmp_path))
    monkeypatch.setenv('PUBLICATION_RECEIPT', json.dumps(receipt))
    monkeypatch.setattr(delivery_activities, '_context',
                        lambda _spec: (store, DeliveryBroker(store, spec)))
    return store, spec, broker, checked, tmp_path, receipt


def running_publication_authority(store, spec):
    """Install a controlled SDK actor for the still-disabled running protocol."""
    from devflow_temporal import delivery_execution_authority as execution
    from devflow_temporal import delivery_pause_context as context
    from devflow_temporal.contracts import canonical_json

    run_id = spec['run_id']
    workflow_id = 'delivery-' + run_id
    spec['pause_control_version'] = 1
    with store._connect() as db:
        db.execute('UPDATE delivery_runs SET request_json=? WHERE run_id=?',
                   (canonical_json(spec), run_id))
    reservation = execution.reserve(store, store.pending_starts()[0], spec)
    authority = execution.bind(store, reservation, 'fixture-sdk-run')
    with store._connect() as db:
        db.execute("INSERT INTO delivery_pause_control VALUES "
                   "(?,'running',0,0,?,'fixture-sdk-run',1,NULL,?)",
                   (run_id, workflow_id, '2026-10-29T00:00:00+00:00'))
        db.execute('INSERT INTO delivery_pause_actors VALUES (?,?,?,1,?)',
                   (run_id, workflow_id, 'fixture-sdk-run', run_id))
        db.execute("INSERT INTO delivery_pause_actor_status VALUES (?,?,'running')",
                   (run_id, workflow_id))
    return authority, context.PauseContext(run_id, workflow_id, 'fixture-sdk-run', {run_id: 0})


def events(root):
    path = root / 'events.jsonl'
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


async def until(root, kind, child=None):
    deadline = time.monotonic() + 30
    while not any(item['kind'] == kind for item in events(root)):
        if child is not None and child.poll() is not None:
            pytest.fail('owned publication child exited: ' + (root / 'child-stderr').read_text())
        assert time.monotonic() < deadline, 'owned publication process did not reach ' + kind
        await asyncio.sleep(.02)


@pytest.mark.asyncio
async def test_lost_waiter_then_proven_pre_mutation_failure_can_restart_original_request(
    publication, monkeypatch,
):
    store, spec, _broker, checked, root, _receipt = publication
    monkeypatch.setenv('PUBLICATION_LIST_FAIL', '1')
    request = {'spec': spec, 'iteration': 0, 'candidate': checked}
    first = asyncio.create_task(delivery_activities.delivery_publish(request))
    await until(root, 'list-start')
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert await delivery_activities.delivery_publish(request) == {
        'state': 'pending', 'reason': 'publication_execution_active'}
    await until(root, 'list-end')
    for _ in range(100):
        result = await delivery_activities.delivery_reconcile_publish(request)
        if result.get('number'):
            break
        await asyncio.sleep(.02)
    assert result['number'] == 1
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    with store._connect() as db:
        row = db.execute("SELECT state FROM delivery_effects WHERE kind='publish'").fetchone()
        assert row[0] == 'complete'


def test_actual_missing_executable_can_retry_creation_without_repeating_push(
    publication, monkeypatch,
):
    _store, _spec, broker, checked, root, _receipt = publication
    real = broker._create_owned_pr
    first = True

    def create(base):
        nonlocal first
        if first:
            first = False
            # No executable exists in this owned path. This reaches real Popen,
            # and cannot fall through to the account's installed gh executable.
            with monkeypatch.context() as missing:
                missing.setenv('PATH', str(root / 'no-executables'))
                return real(base)
        return real(base)

    monkeypatch.setattr(broker, '_create_owned_pr', create)
    with pytest.raises(FileNotFoundError):
        broker.publish(0, checked)
    assert events(root) == []
    head = _git(broker.checkout, 'rev-parse', 'HEAD')
    remote = _git(broker.source, 'ls-remote', 'origin', 'refs/heads/' + broker.spec['branch'])
    assert remote.split()[0] == head
    assert broker.reconcile_publish(0, checked, create_missing=True)['head'] == head
    assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1


@pytest.mark.asyncio
async def test_child_cli_keeps_publication_lease_when_python_owner_is_killed(publication):
    _store, spec, broker, checked, root, _receipt = publication
    payload = root / 'child-request.json'
    payload.write_text(json.dumps({'spec': spec, 'candidate': checked}))
    script = '''import json,sys
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.delivery_broker import DeliveryBroker
r=json.load(open(sys.argv[1]));s=r['spec']
b=DeliveryBroker(DeliveryStore(DeliveryConfig.load(Path(s['config_path']))),s)
b.publish(0,r['candidate'])
'''
    with (root / 'child-stderr').open('w') as errors:
        child = subprocess.Popen([sys.executable, '-B', '-c', script, str(payload)],
                                 stdout=subprocess.DEVNULL, stderr=errors)
    try:
        await until(root, 'create-start', child)
        child.kill()
        child.wait(timeout=5)
        # This is another process and descriptor, not a process-local mutex test.
        assert broker.publish(0, checked) == {
            'state': 'pending', 'reason': 'publication_execution_active'}
        if any(item['kind'] == 'create-start' for item in events(root)):
            await until(root, 'create-end')
        for _ in range(100):
            result = broker.reconcile_publish(0, checked, create_missing=True)
            if result.get('number'):
                break
            await asyncio.sleep(.02)
        assert result['number'] == 1
        assert len([item for item in events(root) if item['kind'] == 'create-start']) == 1
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        await until(root, 'create-end')


def test_publication_handoff_preserves_staged_work_across_a_pause(publication, monkeypatch):
    """A fenced commit cannot start and a later continuation does not repeat staging."""
    import threading
    from contextlib import contextmanager
    from types import SimpleNamespace

    from temporalio.exceptions import ApplicationError
    from temporalio.worker import ExecuteActivityInput

    from devflow_temporal import delivery_execution_authority as execution
    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_context as context
    from devflow_temporal.delivery_broker import BrokerReadbackUnavailable
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    store, spec, broker, candidate, root, _receipt = publication
    run_id = spec['run_id']
    workflow_id = 'delivery-' + run_id
    # Public running-pause admission remains disabled; SDK history is controlled.
    authority, binding = running_publication_authority(store, spec)
    original_handoff = context.publication_handoff
    reached = []
    entered, accepted = threading.Event(), threading.Event()

    def request_pause():
        entered.set()
        with pause.root_mutex(store, run_id), store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE delivery_pause_control SET state='pause_requested',"
                       "revision=1,epoch=1 WHERE root_id=?", (run_id,))
        accepted.set()

    @contextmanager
    def pause_after_stage(owner, step):
        requester = None
        try:
            with original_handoff(owner, step) as handoff:
                if step == 'stage':
                    # The concurrent request must wait until Popen has been handed
                    # off under the shared root mutex, before it can close the fence.
                    requester = threading.Thread(target=request_pause)
                    requester.start()
                    assert entered.wait(5)
                    assert not accepted.wait(.1)
                yield handoff
            reached.append(step)
            if requester is not None:
                assert accepted.wait(5)
        finally:
            if requester is not None:
                requester.join(5)
                assert not requester.is_alive()

    monkeypatch.setattr(context, 'publication_handoff', pause_after_stage)
    token = execution._ACTIVITY_AUTHORITY.set(authority)
    original_head = _git(broker.checkout, 'rev-parse', 'HEAD')
    # Exercise the actual interceptor and activity; SDK selection is controlled,
    # while the persisted actor and publication handoff checks remain real.
    class InvokeActivity:
        async def execute_activity(self, input):
            return await input.fn(*input.args)

    info = SimpleNamespace(workflow_type='DevflowDeliveryWorkflow', workflow_id=workflow_id,
                           workflow_run_id='fixture-sdk-run')
    monkeypatch.setattr(delivery_activities.activity, 'info', lambda: info)

    authenticated_calls = []

    async def authenticated(_store, _spec):
        authenticated_calls.append(_spec['run_id'])
        return authority

    monkeypatch.setattr(execution, 'authenticate_activity', authenticated)
    inbound = delivery_activities._DatabaseContentionInbound(InvokeActivity())
    request = {'spec': spec, 'iteration': 0, 'candidate': candidate,
               '_pause_epochs': {run_id: 0}}
    input = ExecuteActivityInput(fn=delivery_activities.delivery_publish, args=[request],
                                 executor=None, headers={})
    try:
        with pytest.raises(ApplicationError) as denied:
            asyncio.run(inbound.execute_activity(input))
        assert authenticated_calls == [run_id]
        assert denied.value.type == 'PauseDeferred' and denied.value.non_retryable
        assert denied.value.details == ({'root_id': run_id, 'operation_key': 'publish:run-1:0',
                                         'step': 'commit', 'epoch': 1},)
        assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
        assert _git(broker.checkout, 'diff', '--cached', '--name-only') == 'README.md'
        assert events(root) == []
        with store._connect() as db:
            mutations = json.loads(db.execute(
                'SELECT mutations_json FROM delivery_publication_executors').fetchone()[0])
            assert mutations['stage']['state'] == 'complete'
            assert len(mutations['stage']['evidence']['stage_sha256']) == 64
            assert mutations['commit']['state'] == 'not_started'
            assert db.execute("SELECT COUNT(*) FROM delivery_pause_denials "
                              "WHERE root_id=? AND step='commit'", (run_id,)).fetchone()[0] == 1

        # Reconstruct the store; a new process cannot treat the denial as a
        # completed commit or launch the old epoch while the fence is closed.
        reopened = DeliveryStore(DeliveryConfig.load(store.config.path))
        reopened_broker = DeliveryBroker(reopened, spec)
        with context.activate(binding), pytest.raises(pause.PauseDeferred):
            reopened_broker.publish(0, candidate)
        assert events(root) == []
        with pause.root_mutex(reopened, run_id), reopened._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            # Stand in for the future authenticated checkpoint/resume owner.
            db.execute("UPDATE delivery_pause_control SET state='running',revision=2 "
                       "WHERE root_id=?", (run_id,))
        resumed = context.PauseContext(run_id, workflow_id, 'fixture-sdk-run', {run_id: 1})

        # A completed staging step is not authority to commit a different
        # index, even if the worktree still has the original candidate bytes.
        original_blob = _git(broker.checkout, 'rev-parse', ':README.md')
        altered = root / 'altered-staged-content'
        altered.write_text('Unexpected staged content\n')
        altered_blob = _git(broker.checkout, 'hash-object', '-w', str(altered))
        _git(broker.checkout, 'update-index', '--cacheinfo', '100644', altered_blob, 'README.md')
        try:
            with context.activate(resumed), pytest.raises(ValueError, match='staged publication'):
                reopened_broker.publish(0, candidate)
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
            assert events(root) == []
        finally:
            _git(broker.checkout, 'update-index', '--cacheinfo',
                 '100644', original_blob, 'README.md')

        # A staged deletion leaves the file untracked, so the ordinary unstaged
        # diff is empty even though committing it would drop reviewed content.
        _git(broker.checkout, 'update-index', '--force-remove', 'README.md')
        try:
            assert (broker.checkout / 'README.md').is_file()
            assert not _git(broker.checkout, 'diff', '--name-only')
            with context.activate(resumed), pytest.raises(ValueError, match='staged publication'):
                reopened_broker.publish(0, candidate)
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
            assert events(root) == []
        finally:
            _git(broker.checkout, 'update-index', '--add', '--cacheinfo',
                 '100644', original_blob, 'README.md')

        # Assume-unchanged also hides mismatched staged bytes from git diff.
        _git(broker.checkout, 'update-index', '--cacheinfo', '100644', altered_blob, 'README.md')
        _git(broker.checkout, 'update-index', '--assume-unchanged', 'README.md')
        try:
            assert not _git(broker.checkout, 'diff', '--name-only')
            with context.activate(resumed), pytest.raises(ValueError, match='staged publication'):
                reopened_broker.publish(0, candidate)
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
            assert events(root) == []
        finally:
            _git(broker.checkout, 'update-index', '--no-assume-unchanged', 'README.md')
            _git(broker.checkout, 'update-index', '--cacheinfo',
                 '100644', original_blob, 'README.md')

        real_create = reopened_broker._create_owned_pr
        first_create = True

        def create(base):
            nonlocal first_create
            if first_create:
                first_create = False
                with monkeypatch.context() as missing:
                    missing.setenv('PATH', str(root / 'no-executables'))
                    return real_create(base)
            return real_create(base)

        monkeypatch.setattr(reopened_broker, '_create_owned_pr', create)
        with context.activate(resumed):
            with pytest.raises(FileNotFoundError):
                reopened_broker.publish(0, candidate)
            with reopened._connect() as db:
                assert db.execute("SELECT COUNT(*) FROM delivery_pause_steps "
                                  "WHERE root_id=? AND step='create'", (run_id,)).fetchone()[0] == 0
            # A real launched create with a lost outcome is not a no-launch
            # failure. It must be read back before any new create is allowed.
            monkeypatch.setenv('PUBLICATION_FAIL', '1')
            with pytest.raises(BrokerReadbackUnavailable):
                reopened_broker.reconcile_publish(0, candidate, create_missing=True)
            monkeypatch.delenv('PUBLICATION_FAIL')
            assert reopened_broker.reconcile_publish(0, candidate, create_missing=True) == {
                'state': 'pending', 'reason': 'original_create_readback'}
            assert [item['kind'] for item in events(root)].count('create-start') == 1
            head = _git(broker.checkout, 'rev-parse', 'HEAD')
            # Controlled remote readback finds the original PR after the lost reply.
            (root / 'receipt.json').write_text(json.dumps([dict(_receipt[0], headRefOid=head)]))
            result = reopened_broker.reconcile_publish(0, candidate, create_missing=True)
        assert result['number'] == 1
        assert reached.count('stage') == 1
        assert [item['kind'] for item in events(root)].count('create-start') == 1
    finally:
        execution._ACTIVITY_AUTHORITY.reset(token)


def test_unknown_stage_readback_never_relaunches_or_commits(publication, monkeypatch):
    """A completed Git add with unavailable index proof stays an unknown handoff."""
    from devflow_temporal import delivery_broker as broker_module
    from devflow_temporal import delivery_execution_authority as execution
    from devflow_temporal import delivery_pause_context as context
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    store, spec, broker, candidate, root, _receipt = publication
    authority, binding = running_publication_authority(store, spec)
    original_head = _git(broker.checkout, 'rev-parse', 'HEAD')
    original_observe = broker_module._staged_index_evidence

    observations = []

    def unavailable(checkout):
        observations.append(True)
        if len(observations) == 1:
            return original_observe(checkout)
        raise OSError('controlled index readback failure')

    token = execution._ACTIVITY_AUTHORITY.set(authority)
    try:
        # Git can otherwise ignore reviewed worktree bytes during the first add.
        _git(broker.checkout, 'update-index', '--assume-unchanged', 'README.md')
        try:
            with context.activate(binding), pytest.raises(ValueError, match='index semantics'):
                broker.publish(0, candidate)
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
            assert events(root) == []
        finally:
            _git(broker.checkout, 'update-index', '--no-assume-unchanged', 'README.md')
        monkeypatch.setattr(broker_module, '_staged_index_evidence', unavailable)
        with context.activate(binding), pytest.raises(OSError, match='index readback'):
            broker.publish(0, candidate)
        with store._connect() as db:
            mutations = json.loads(db.execute(
                'SELECT mutations_json FROM delivery_publication_executors').fetchone()[0])
            assert mutations['stage']['state'] == 'unknown'
        assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
        assert events(root) == []
        monkeypatch.setattr(broker_module, '_staged_index_evidence', original_observe)
        reopened = DeliveryStore(DeliveryConfig.load(store.config.path))
        with context.activate(binding):
            assert DeliveryBroker(reopened, spec).publish(0, candidate) == {
                'state': 'pending', 'reason': 'original_stage_readback'}
        assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
        assert events(root) == []
    finally:
        execution._ACTIVITY_AUTHORITY.reset(token)


def test_already_staged_deletion_is_bound_before_pause(publication, monkeypatch):
    """A deletion with no remaining stageable path still has an exact index seal."""
    from contextlib import contextmanager

    from devflow_temporal import delivery_execution_authority as execution
    from devflow_temporal import delivery_pause as pause
    from devflow_temporal import delivery_pause_context as context

    store, spec, broker, _candidate, root, _receipt = publication
    original_blob = _git(broker.checkout, 'rev-parse', 'HEAD:README.md')
    (broker.checkout / 'README.md').unlink()
    _git(broker.checkout, 'add', '-u', '--', 'README.md')
    candidate = broker.candidate()
    original_head = _git(broker.checkout, 'rev-parse', 'HEAD')
    authority, binding = running_publication_authority(store, spec)
    original_handoff = context.publication_handoff

    @contextmanager
    def close_after_stage(owner, step):
        with original_handoff(owner, step) as handoff:
            yield handoff
        if step == 'stage':
            with pause.root_mutex(store, spec['run_id']), store._connect() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("UPDATE delivery_pause_control SET state='pause_requested',"
                           "revision=1,epoch=1 WHERE root_id=?", (spec['run_id'],))

    monkeypatch.setattr(context, 'publication_handoff', close_after_stage)
    token = execution._ACTIVITY_AUTHORITY.set(authority)
    try:
        with context.activate(binding), pytest.raises(pause.PauseDeferred) as denied:
            broker.publish(0, candidate)
        assert denied.value.step == 'commit'
        with store._connect() as db:
            mutations = json.loads(db.execute(
                'SELECT mutations_json FROM delivery_publication_executors').fetchone()[0])
            assert len(mutations['stage']['evidence']['stage_sha256']) == 64
            db.execute("UPDATE delivery_pause_control SET state='running',revision=2 "
                       "WHERE root_id=?", (spec['run_id'],))
        resumed = context.PauseContext(spec['run_id'], binding.workflow_id,
                                       binding.execution_run_id, {spec['run_id']: 1})
        # Reintroducing the old blob would discard the reviewed deletion.
        _git(broker.checkout, 'update-index', '--add', '--cacheinfo',
             '100644', original_blob, 'README.md')
        try:
            with context.activate(resumed), pytest.raises(ValueError, match='staged publication'):
                broker.publish(0, candidate)
            assert _git(broker.checkout, 'rev-parse', 'HEAD') == original_head
            assert events(root) == []
        finally:
            _git(broker.checkout, 'update-index', '--force-remove', 'README.md')
        with context.activate(resumed):
            result = broker.publish(0, candidate)
        assert result['number'] == 1
        assert not _git(broker.checkout, 'ls-tree', '--name-only', 'HEAD', 'README.md')
        assert [item['kind'] for item in events(root)].count('create-start') == 1
    finally:
        execution._ACTIVITY_AUTHORITY.reset(token)


def test_child_handoff_stays_fenced_when_only_parent_resumes(service, tmp_path):
    """A parent resume cannot authorize a child that retains its own pause token."""
    from devflow_temporal import delivery_pause as pause
    from devflow_temporal.delivery_config import DeliveryConfig
    from devflow_temporal.delivery_store import DeliveryStore

    store, request = service
    store.submit(request)
    parent, child = request['run_id'], 'fixture-child'
    workflow, sdk_run = 'fixture-child-workflow', 'fixture-child-sdk-run'
    with store._connect() as db:
        for root in (parent, child):
            db.execute("INSERT INTO delivery_pause_control VALUES "
                       "(?,'running',0,0,?,?,1,NULL,?)",
                       (root, workflow, sdk_run, '2026-10-29T00:00:00+00:00'))
            db.execute('INSERT INTO delivery_pause_actors VALUES (?,?,?,1,?)',
                       (root, workflow, sdk_run, child))
            db.execute("INSERT INTO delivery_pause_actor_status VALUES (?,?,'running')",
                       (root, workflow))
        db.execute("UPDATE delivery_pause_control SET state='paused',revision=1,epoch=1 "
                   "WHERE root_id=?", (child,))

    def validate(db, roots, actor_workflow, actor_run, generation):
        assert db.in_transaction
        if (set(roots) != {parent, child} or actor_workflow != workflow
                or actor_run != sdk_run or generation != 1):
            raise ValueError('fixture graph identity changed')
        actors = db.execute('SELECT root_id,actor_run_id FROM delivery_pause_actors '
                            'WHERE workflow_id=?', (workflow,)).fetchall()
        if {(row['root_id'], row['actor_run_id']) for row in actors} != {
                (parent, child), (child, child)}:
            raise ValueError('fixture graph membership changed')

    def admit(current, bindings):
        return pause.launch_admission_graph(
            current, bindings, 'child-work', 'launch', workflow_id=workflow,
            execution_run_id=sdk_run, owner_generation=1, validate_graph=validate)

    marker = tmp_path / 'child-launched'
    with pytest.raises(pause.PauseDeferred) as denied:
        with admit(store, {parent: 0, child: 0}):
            pytest.fail('independently paused child was launched')
    assert denied.value.root_id == child and not marker.exists()
    with store._connect() as db:
        db.execute("UPDATE delivery_pause_control SET state='paused',epoch=1 WHERE root_id=?",
                   (parent,))
        # Parent resume is independent of the child's closed token.
        db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id=?", (parent,))
    reopened = DeliveryStore(DeliveryConfig.load(store.config.path))
    with pytest.raises(pause.PauseDeferred) as denied:
        with admit(reopened, {parent: 1, child: 1}):
            pytest.fail('parent resume cleared the child pause')
    assert denied.value.root_id == child and not marker.exists()
    with reopened._connect() as db:
        db.execute("UPDATE delivery_pause_control SET state='running' WHERE root_id=?", (child,))
        db.execute("UPDATE delivery_pause_actor_status SET state='resume_pending' "
                   "WHERE root_id=? AND workflow_id=?", (child, workflow))
    with pytest.raises(pause.PauseDeferred):
        with admit(reopened, {parent: 1, child: 1}):
            pytest.fail('child launch preceded its resume acknowledgement')
    with reopened._connect() as db:
        db.execute("UPDATE delivery_pause_actor_status SET state='running' "
                   "WHERE root_id=? AND workflow_id=?", (child, workflow))
    with admit(reopened, {parent: 1, child: 1}):
        process = subprocess.Popen([sys.executable, '-c',
                                    'from pathlib import Path; import sys; '
                                    'Path(sys.argv[1]).write_text("started")', str(marker)])
    assert process.wait(timeout=5) == 0 and marker.read_text() == 'started'
    with pytest.raises(ValueError, match='already admitted'):
        with admit(reopened, {parent: 1, child: 1}):
            pytest.fail('same unknown handoff was launched twice')
