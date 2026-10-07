from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys
import time
from importlib.metadata import distribution
from pathlib import Path

import pytest
from fixtures.activity_liveness_workflow import ActivityLivenessWorkflow
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from test_delivery_activity_liveness import CHECK_PROGRAM, WORKER_DRIVER
from test_delivery_api import api_fixture as api_fixture
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration
from test_delivery_policy_recovery import preserved as preserved
from test_delivery_role_reattach import FIXED_NATIVE_PROVIDER

from devflow_temporal.delivery_api import DeliveryService
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_native_process import process_table, stop_observed
from devflow_temporal.delivery_resources import read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore


@pytest.mark.asyncio
async def test_closed_native_role_is_reconciled_after_worker_sigkill_without_replacement(
    api_fixture, tmp_path,
):
    path, submission = api_fixture
    async with await WorkflowEnvironment.start_local(
        dev_server_existing_path=shutil.which('temporal'),
    ) as environment:
        raw = json.loads(path.read_text())
        raw.update(provider='codex', execution_mode='trusted-local',
            temporal_address=environment.client.service_client.config.target_host,
            queue='activity-worker-loss', codex_bin=str(distribution(
                'openai-codex-cli-bin').locate_file('codex_cli_bin/bin/codex')))
        raw['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'high'}
                        for role in raw['roles']}
        repository = raw['repositories']['fixture']
        source = Path(repository['source_path'])
        (source / 'liveness.py').write_text(CHECK_PROGRAM)
        for args in [('add', 'liveness.py'), ('commit', '-qm', 'test: native orphan fixture')]:
            subprocess.run(['git', '-C', str(source), *args], check=True)
        repository['expected_base_sha'] = subprocess.check_output(
            ['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
        state = Path(raw['state_root']) / 'runs' / submission['run_id']
        check = {'id': 'fixture-check', 'kind': 'test', 'argv': [sys.executable,
            'liveness.py', str(state)], 'test_count_regex': r'(\d+) passed', 'min_tests': 1}
        repository.update(required_ci=['test'], assignee='example', checks=[check],
            prepublish_checks=[check], baseline_check_ids=[check['id']],
            project_url='https://github.com/users/example/projects/1')
        path.write_text(json.dumps(raw))
        store = DeliveryStore(DeliveryConfig.load(path))
        store.submit(submission)
        spec = store.spec(submission['run_id'])
        broker = DeliveryBroker(store, spec)
        broker.prepare()
        request = {'spec': spec, 'role': 'implement', 'iteration': 0,
                   'candidate': broker.candidate()}
        state = Path(spec['state_dir'])
        input_path, driver, provider = [tmp_path / name for name in
                                        ['input.json', 'worker.py', 'provider.py']]
        input_path.write_text(json.dumps(request))
        driver.write_text(WORKER_DRIVER)
        provider.write_text(FIXED_NATIVE_PROVIDER)
        log_path = tmp_path / 'original-worker.log'
        with log_path.open('wb') as log:
            worker = subprocess.Popen([sys.executable, '-I', str(driver), str(input_path),
                str(provider), environment.client.service_client.config.target_host,
                str(Path(__file__).parent)], stdout=log, stderr=subprocess.STDOUT)
            try:
                handle = await environment.client.start_workflow(
                    ActivityLivenessWorkflow.run, {'activity_name': 'delivery_role',
                        'request': request}, id='delivery-'+spec['run_id'],
                    task_queue=store.config.queue,
                    memo={'request_digest': spec['request_digest']},
                )
                deadline = time.monotonic()+20
                while not (state / 'started').exists():
                    assert worker.poll() is None, log_path.read_text()
                    assert time.monotonic()<deadline, log_path.read_text()
                    await asyncio.sleep(0.05)
                worker.kill()
                await asyncio.to_thread(worker.wait, 5)
                await handle.terminate(reason='No surviving or replacement role observer')
                with pytest.raises(WorkflowFailureError):
                    await handle.result()
                closed = await handle.describe()
                assert closed.close_time and closed.run_id
                # Only the original detached monitor/provider remains. Complete it
                # without starting any worker or replacing the native command.
                (state / 'release').touch()
                journal_path = next(state.rglob('native-process.json'))
                deadline = time.monotonic()+15
                while True:
                    journal = read_private(journal_path)
                    if journal.get('phase') == 'finished':
                        owned = {int(pid): value for pid, value in journal['owned'].items()}
                        if journal.get('monitor'):
                            monitor = journal['monitor']
                            owned[monitor['pid']] = monitor
                        observed = process_table()
                        if not any(observed.get(pid, {}).get('identity') == value['identity']
                                   and not observed[pid]['stat'].startswith('Z')
                                   for pid, value in owned.items()):
                            break
                    assert time.monotonic()<deadline, journal
                    await asyncio.sleep(0.05)
                with store._connect() as db:
                    before = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
                assert before['state'] in {'starting', 'running'} and before['cleanup'] == 'none'
                from devflow_temporal.delivery_orphans import _complete_attempt

                # A closed workflow alone cannot authorize a changed original journal.
                original_journal = read_private(journal_path)
                for field, wrong in [('cwd', '/unrelated'), ('policy_digest', 'changed'),
                                     ('timeout', 999999), ('ports', [1])]:
                    changed = json.loads(json.dumps(original_journal))
                    changed['intent'][field] = wrong
                    write_private(journal_path, changed)
                    with pytest.raises(ValueError):
                        _complete_attempt(store, spec, before)
                    with store._connect() as db:
                        assert db.execute('SELECT state FROM delivery_attempts').fetchone()[0] \
                            == before['state']
                write_private(journal_path, original_journal)
                from devflow_temporal.supervisor import get_supervisor

                original_observation = get_supervisor(store)._mark_unknown(
                    before['job_key'], 'Original lost observer')
                with store._connect() as db:
                    original_unknown = dict(db.execute(
                        'SELECT * FROM delivery_attempts').fetchone())
                store.project(spec['run_id'], phase='blocked', execution_state='blocked',
                    event_type='blocked', message='Original workflow terminated', outcome='blocked',
                    error='Original worker was killed', cleanup='unknown',
                    checks={'original_role': original_observation})
                service = DeliveryService(path)
                service._health_client = environment.client
                await service.dispatch_once()
                await service.reconcile_closed_native_once()
                with store._connect() as db:
                    after = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
                    run = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
                assert after['state'] == 'finished' and after['cleanup'] == 'confirmed', after
                assert after['session_id'] == 'fixed-native-session'
                assert run['outcome'] == 'blocked' and run['error'] == 'Original worker was killed'
                assert run['cleanup'] == 'confirmed'
                import httpx

                from devflow_temporal.delivery_api import create_app

                transport = httpx.ASGITransport(app=create_app(path), client=("127.0.0.1", 10001))
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://127.0.0.1:18770"
                ) as browser:
                    listed = (await browser.get("/api/runs")).json()["runs"][0]
                    detailed = (await browser.get("/api/runs/" + spec["run_id"])).json()["run"]
                for projected in (listed, detailed):
                    assert projected["cleanup"] == projected["cleanup_recorded"] == "confirmed"
                assert any(
                    event["payload"].get("original_attempts") == [original_unknown]
                    for event in store.events(spec["run_id"])
                )
                assert original_unknown['state'] == 'unknown'
                assert original_unknown['result_json'] is None
                assert original_unknown['cleanup'] == 'unknown'
                assert json.loads(run['checks_json'])['original_role'] == original_observation
                assert original_observation['status'] == 'recovery_unknown'
                assert (state / 'invocations').read_text().splitlines() == ['one']
                assert len(list(state.rglob('native-process.json'))) == 1
            finally:
                if worker.poll() is None:
                    worker.kill()
                    await asyncio.to_thread(worker.wait, 5)
                for journal_path in state.rglob('native-process.json'):
                    journal = read_private(journal_path)
                    owned = {int(pid): value for pid, value in journal.get('owned', {}).items()}
                    if journal.get('monitor'):
                        monitor = journal['monitor']
                        owned[monitor['pid']] = monitor
                    assert stop_observed(owned)


@pytest.mark.asyncio
@pytest.mark.parametrize('uncertain', [
    'running', 'continued_as_new', 'no_close_time', 'wrong_id', 'wrong_request',
    'wrong_recovery', 'describe_error', 'foreign_owner', 'changed_config', 'non_native',
])
async def test_uncertain_closed_row_does_not_reconcile_or_block_the_next_owner(
    api_fixture, monkeypatch, uncertain,
):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from temporalio.client import WorkflowExecutionStatus

    from devflow_temporal import delivery_orphans

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    for index in range(2):
        store.submit({**submission, 'command_id': f'submit-{index}', 'run_id': f'run-{index}',
                      'work_id': f'work-{index}', 'branch': f'feat/fixture-{index}',
                      'issue_url': f'https://github.com/example/fixture/issues/{index+1}'})
        with store._connect() as db:
            db.execute('INSERT INTO delivery_attempts '
                       '(job_key,run_id,role,iteration,candidate_id,state) VALUES (?,?,?,?,?,?)',
                       (f'attempt-{index}', f'run-{index}', 'implement', 0, 'input', 'starting'))
    specs = {f'run-{index}': store.effective_spec(f'run-{index}') for index in range(2)}
    for spec in specs.values():
        spec['provider'] = 'codex'
        spec['policy']['execution_backend'] = 'native-macos'
    if uncertain == 'non_native':
        specs['run-0']['policy']['execution_backend'] = 'other'
    monkeypatch.setattr(store, 'effective_spec', lambda run: specs[run])

    def owns(spec):
        if spec['run_id'] == 'run-0':
            if uncertain == 'changed_config':
                raise ValueError('original frozen configuration was edited')
            if uncertain == 'foreign_owner':
                return False
        return True

    monkeypatch.setattr(store, 'owns_execution', owns)
    reconciled = []
    monkeypatch.setattr(delivery_orphans, '_reconcile',
                        lambda store, row, spec, closed: reconciled.append(row['run_id']))

    class Handle:
        def __init__(self, workflow_id):
            self.id = workflow_id

        async def describe(self):
            first = self.id == 'delivery-run-0'
            if first and uncertain == 'describe_error':
                raise RuntimeError('transient unavailable describe')
            status = WorkflowExecutionStatus.TERMINATED
            if first and uncertain == 'running':
                status = WorkflowExecutionStatus.RUNNING
            elif first and uncertain == 'continued_as_new':
                status = WorkflowExecutionStatus.CONTINUED_AS_NEW

            async def memo_value(name, default):
                if name == 'request_digest':
                    if first and uncertain == 'wrong_request':
                        return 'changed'
                    return specs[self.id.removeprefix('delivery-')]['request_digest']
                if first and uncertain == 'wrong_recovery':
                    return 'unmatched-recovery'
                return default

            return SimpleNamespace(
                id='changed-id' if first and uncertain == 'wrong_id' else self.id,
                run_id='actual-closed-execution', status=status, memo_value=memo_value,
                close_time=None if first and uncertain == 'no_close_time' else datetime.now(UTC))

    client = SimpleNamespace(get_workflow_handle=Handle, namespace='default',
        service_client=SimpleNamespace(config=SimpleNamespace(
            target_host=store.config.temporal_address)))
    await delivery_orphans.reconcile_closed_native(store, client)
    assert reconciled == ['run-1']
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_attempts "
                          "WHERE state='starting' AND cleanup='none'").fetchone()[0] == 2


@pytest.mark.asyncio
async def test_maintenance_rejects_foreign_transport_before_observing_any_row(api_fixture):
    from types import SimpleNamespace

    from devflow_temporal.delivery_orphans import reconcile_closed_native

    path, _ = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    client = SimpleNamespace(namespace='default', service_client=SimpleNamespace(
        config=SimpleNamespace(target_host='127.0.0.1:1')))
    with pytest.raises(ValueError, match='transport differs'):
        await reconcile_closed_native(store, client)


def _resolved_orphan_fixture(store, spec, monkeypatch):
    """Isolate post-authentication guards with a real unfinished store row.

    Journal authentication is covered by the real SIGKILL test; this completion
    double only provides its confirmed transition to the guards under test.
    """
    from devflow_temporal import delivery_orphans

    with store._connect() as db:
        db.execute("INSERT INTO delivery_attempts "
                   "(job_key,run_id,role,iteration,candidate_id,state,cleanup) "
                   "VALUES ('orphan',?,'implement',0,'original','unknown','unknown')",
                   (spec['run_id'],))
    observed = []

    def complete(actual_store, actual_spec, attempt, on_observed):
        assert actual_store is store and actual_spec == spec
        assert attempt['state'] == 'unknown' and attempt['cleanup'] == 'unknown'
        observed.append(attempt['job_key'])
        receipt = {'cleanup': 'observed-native-confirmed'}
        on_observed(receipt)
        with store._connect() as db:
            db.execute("UPDATE delivery_attempts SET state='finished',cleanup='confirmed' "
                       "WHERE job_key=?", (attempt['job_key'],))
        return receipt

    monkeypatch.setattr(delivery_orphans, '_complete_attempt', complete)
    return observed


@pytest.mark.parametrize('table', ['delivery_effects', 'delivery_mutations'])
@pytest.mark.parametrize('state', ['pending', 'unknown'])
def test_uncertain_effect_preserves_cleanup_and_claim(api_fixture, monkeypatch, table, state):
    from devflow_temporal import delivery_orphans

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission['run_id'])
    store.project(spec['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='Original failure', outcome='blocked',
                  error='Original failure', cleanup='unknown')
    with store._connect() as db:
        if table == 'delivery_effects':
            db.execute('INSERT INTO delivery_effects '
                       '(effect_key,run_id,kind,request_json,state,updated_at) '
                       'VALUES (?,?,?,?,?,?)',
                       ('unknown-publication', spec['run_id'], 'publish', '{}', state, 'original'))
        else:
            db.execute('INSERT INTO delivery_mutations '
                       '(command_id,run_id,kind,request_digest,state) VALUES (?,?,?,?,?)',
                       ('unknown-write', spec['run_id'], 'publish', 'original', state))
        original = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        claims = [dict(row) for row in db.execute('SELECT * FROM claims')]
    completed = _resolved_orphan_fixture(store, spec, monkeypatch)
    observed = []

    class Resources:
        def __init__(self, _spec, *, read_only):
            self.manifest = path

        def locked(self):
            from contextlib import contextmanager

            @contextmanager
            def manifest():
                yield {'processes': ['registered.json']}
            return manifest()

        def finalize(self, outcome, *, uncertain):
            observed.append((outcome, uncertain))
            cleanup = 'unknown' if uncertain else 'confirmed'
            return {'state': cleanup, 'resource_cleanup': cleanup, 'process_cleanup': cleanup}

    monkeypatch.setattr(delivery_orphans, 'RunResources', Resources)
    monkeypatch.setattr(delivery_orphans, '_observe_registered_process',
                        lambda spec, path: {'cleanup': 'observed-native-confirmed'})
    delivery_orphans._reconcile(store, original, spec, {'status': 'TERMINATED'})
    assert completed == ['orphan']
    assert observed == [('blocked', True)]
    with store._connect() as db:
        run = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        assert run['cleanup'] == 'unknown' and run['error'] == 'Original failure'
        assert run['outcome'] == 'blocked'
        assert [dict(row) for row in db.execute('SELECT * FROM claims')] == claims
        assert db.execute(f'SELECT state FROM {table}').fetchone()[0] == state
        assert db.execute("SELECT state,cleanup FROM delivery_attempts "
                          "WHERE job_key='orphan'").fetchone()[:] == ('finished', 'confirmed')


def test_cleanup_cannot_apply_a_previous_execution_snapshot(api_fixture):
    from devflow_temporal.delivery_orphans import _unchanged

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    with store._connect() as db:
        original = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        db.execute('UPDATE delivery_runs SET workflow_id=?', ('new-execution',))
    with pytest.raises(ValueError, match='execution changed'):
        _unchanged(store, original)


@pytest.mark.parametrize('cleanup', ['confirmed', 'unknown'])
def test_finished_historical_role_is_not_completed_again(api_fixture, monkeypatch, cleanup):
    from contextlib import contextmanager

    from devflow_temporal import delivery_orphans

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission['run_id'])
    store.project(spec['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='Original failure', outcome='blocked',
                  error='Original failure', cleanup='unknown')
    original_result = json.dumps({'status': 'failed', 'error': 'Retained role failure'})
    with store._connect() as db:
        db.execute('INSERT INTO delivery_attempts '
                   '(job_key,run_id,role,iteration,candidate_id,state,result_json,cleanup) '
                   'VALUES (?,?,?,?,?,?,?,?)',
                   ('historical', spec['run_id'], 'intake', 0, 'original', 'finished',
                    original_result, cleanup))
        original = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        attempt = dict(db.execute('SELECT * FROM delivery_attempts').fetchone())
    # A later accepted plan must not cause immutable earlier role results to be reprocessed.
    spec = {**spec, 'accepted_plan': {'text': 'Current accepted plan'}}
    observed = []

    def repeat_completion(*_):
        raise ValueError('historical original request differs from current accepted plan')

    class Resources:
        def __init__(self, _spec, *, read_only):
            self.manifest = path

        @contextmanager
        def locked(self):
            yield {'processes': ['historical-process.json']}

        def finalize(self, outcome, *, uncertain):
            observed.append(('finalize', outcome, uncertain))
            return {'state': 'confirmed', 'resource_cleanup': 'confirmed',
                    'process_cleanup': 'confirmed'}

    monkeypatch.setattr(delivery_orphans, '_complete_attempt', repeat_completion)
    monkeypatch.setattr(delivery_orphans, 'RunResources', Resources)
    monkeypatch.setattr(delivery_orphans, '_observe_registered_process',
                        lambda spec, path: observed.append(('observe', str(path))))
    delivery_orphans._reconcile(store, original, spec, {'status': 'TERMINATED'})
    assert observed == []
    with store._connect() as db:
        assert dict(db.execute('SELECT * FROM delivery_attempts').fetchone()) == attempt
        run = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        assert run == original
        assert run['outcome'] == 'blocked' and run['error'] == 'Original failure'


@pytest.mark.parametrize('retained_manifest', [False, True])
def test_missing_native_resource_ownership_remains_unknown(
    api_fixture, monkeypatch, retained_manifest,
):
    from devflow_temporal import delivery_orphans
    from devflow_temporal.delivery_resources import RunResources

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission['run_id'])
    store.project(spec['run_id'], phase='blocked', execution_state='blocked',
                  event_type='blocked', message='Original failure', outcome='blocked',
                  error='Original failure', cleanup='unknown')
    completed = _resolved_orphan_fixture(store, spec, monkeypatch)
    with store._connect() as db:
        db.execute('INSERT INTO delivery_attempts '
                   '(job_key,run_id,role,iteration,candidate_id,state,result_json,cleanup) '
                   'VALUES (?,?,?,?,?,?,?,?)',
                   ('completed', spec['run_id'], 'implement', 0, 'original', 'finished',
                    '{"status":"failed"}', 'confirmed'))
        original = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        claims = [tuple(row) for row in db.execute('SELECT * FROM claims')]
        retained = dict(db.execute("SELECT * FROM delivery_attempts "
                                   "WHERE job_key='completed'").fetchone())
    resources = RunResources(spec)
    assert not resources.manifest.exists()
    if retained_manifest:
        with resources.locked() as manifest:
            write_private(resources.manifest, manifest)
    delivery_orphans._reconcile(store, original, spec, {'status': 'TERMINATED'})
    assert completed == ['orphan']
    with store._connect() as db:
        current = dict(db.execute('SELECT * FROM delivery_runs').fetchone())
        assert current['cleanup'] == 'unknown'
        assert current['outcome'] == original['outcome'] and current['error'] == original['error']
        receipt = json.loads(current['checks_json'])['resource_cleanup']
        reason = ('original native process ownership is missing' if retained_manifest
                  else 'original native resource ownership manifest is missing')
        assert receipt['reason'] == reason
        assert [tuple(row) for row in db.execute('SELECT * FROM claims')] == claims
        assert dict(db.execute("SELECT * FROM delivery_attempts "
                               "WHERE job_key='completed'").fetchone()) == retained


def test_registered_monitor_cannot_claim_cleanup_while_its_owned_child_is_alive(
    api_fixture, monkeypatch, tmp_path,
):
    import os

    from devflow_temporal import delivery_orphans
    from devflow_temporal.contracts import digest

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.spec(submission['run_id'])
    folder = Path(spec['state_dir']) / 'attempts' / 'live-native-fixture'
    journal_path = folder / 'native-process.json'
    child = subprocess.Popen([sys.executable, '-I', '-c', 'import time; time.sleep(30)'],
                             cwd=tmp_path)
    try:
        table = process_table()
        journal = {'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
                   'argv': child.args, 'environment_sha256': digest({}), 'ports': []},
                   'phase': 'finished', 'monitoring_complete': True, 'ports': [],
                   'owned': {str(child.pid): table[child.pid]},
                   'monitor': {'pid': os.getpid(), **table[os.getpid()]}}
        write_private(journal_path, journal)
        write_private(folder / 'launch.json', {'argv': child.args, 'environment': {}})
        reconciled = []
        monkeypatch.setattr(delivery_orphans, 'reconcile_process',
                            lambda path: reconciled.append(path))
        with pytest.raises(ValueError, match='remain active'):
            delivery_orphans._observe_registered_process(spec, journal_path)
        assert not reconciled and child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


@pytest.mark.parametrize("attempt_state", ["finished", "unknown"])
def test_closed_maintenance_keeps_legacy_confirmed_projection(api_fixture, attempt_state):
    from devflow_temporal import delivery_orphans
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_resources import RunResources

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission["run_id"])
    folder = Path(spec["state_dir"]) / "attempts" / "legacy"
    write_private(folder / "launch.json", {"argv": ["legacy"], "environment": {}})
    write_private(
        folder / "native-process.json",
        {  # pre-#70 shape: no 'monitor'
            "intent": {
                "run_id": spec["run_id"],
                "policy_digest": spec["policy_digest"],
                "argv": ["legacy"],
                "cwd": "/",
                "environment_sha256": digest({}),
                "timeout": 60,
                "ports": [],
            },
            "phase": "finished",
            "monitoring_complete": True,
            "ports": [],
            "result": {},
            "owned": {
                "999999": {
                    "ppid": 1,
                    "pgid": 999999,
                    "stat": "S",
                    "identity": "Thu Oct  2 10:00:00 2026",
                }
            },
        },
    )
    resources = RunResources(spec)
    resources.process(folder / "native-process.json")
    receipt = resources.finalize("blocked")
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts "
            "(job_key,run_id,role,iteration,candidate_id,state,result_json,cleanup) "
            "VALUES ('legacy',?,'implement',0,'original',?,'{}','confirmed')",
            (spec["run_id"], attempt_state),
        )
    store.project(
        spec["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="implementer did not establish a pass",
        outcome="blocked",
        cleanup="none",
        checks={"resource_cleanup": receipt},
        error="implementer did not establish a pass",
    )
    initial_cleanup = "confirmed" if attempt_state == "finished" else "none"
    assert store.list_runs()[0]["cleanup"] == initial_cleanup
    with store._connect() as db:
        before = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    events = store.events(spec["run_id"])
    delivery_orphans._reconcile(store, before, spec, {"status": "COMPLETED"})
    with store._connect() as db:
        after = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
    assert after == before
    assert store.events(spec["run_id"]) == events
    assert store.list_runs()[0]["cleanup"] == initial_cleanup


@pytest.mark.asyncio
async def test_closed_maintenance_keeps_historical_policy_recovery(preserved):
    from datetime import UTC, datetime
    from types import SimpleNamespace

    from temporalio.client import WorkflowExecutionStatus

    from devflow_temporal.delivery_orphans import reconcile_closed_native

    store, spec, _payload, state = preserved
    run_id = spec["run_id"]
    # Prospective policy admission is retired; existing grants remain validated
    # by the historical reader and publicly retain their original provenance.
    effective = store.effective_spec(run_id)
    submitted = store.submitted_spec(run_id)
    policy = store.detail(run_id)["execution_policy_recovery"]
    events = store.events(run_id)
    root = Path(spec["state_dir"])
    frozen_files = {
        path.relative_to(root): (path.read_bytes(), path.stat().st_mode)
        for path in root.rglob("*")
        if path.is_file()
    }
    with store._connect() as db:
        before = dict(
            db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        )
        historical_rows = {
            table: [
                dict(row)
                for row in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY rowid", (run_id,)
                )
            ]
            for table in ("delivery_policy_recoveries", "delivery_attempts", "delivery_effects")
        }
    recovery = json.loads(before["recovery_json"])
    assert recovery["kind"] == "execution_policy_recovery"
    assert effective == recovery["effective_spec"]
    assert effective["policy"]["host_sandbox"] == "trusted-local"
    assert policy["precheck_sha256"] == recovery["seal"]["precheck_sha256"]
    assert policy["session_id"] == recovery["session_id"]
    assert policy["predecessor_workflow_id"] == recovery["predecessor_workflow_id"]
    assert policy["predecessor_execution_run_id"] == recovery["predecessor_execution_run_id"]
    assert policy["authorized_through_iteration"] == recovery["maximum_iteration"]
    assert policy["preserved_checks"] == state["checks"]
    assert policy["preserved_error"] == state["error"]

    class Handle:
        def __init__(self, workflow_id):
            self.id = workflow_id

        async def describe(self):
            async def memo_value(name, default):
                return before["request_digest"] if name == "request_digest" else default

            return SimpleNamespace(
                id=self.id,
                run_id="closed-execution",
                memo_value=memo_value,
                status=WorkflowExecutionStatus.COMPLETED,
                close_time=datetime.now(UTC),
            )

    client = SimpleNamespace(
        get_workflow_handle=Handle,
        namespace=store.config.raw.get("temporal_namespace", "default"),
        service_client=SimpleNamespace(
            config=SimpleNamespace(target_host=store.config.temporal_address)
        ),
    )
    await reconcile_closed_native(store, client)
    with store._connect() as db:
        after = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone())
        assert historical_rows == {
            table: [
                dict(row)
                for row in db.execute(
                    f"SELECT * FROM {table} WHERE run_id=? ORDER BY rowid", (run_id,)
                )
            ]
            for table in historical_rows
        }
    assert after == before
    assert store.events(run_id) == events
    assert store.effective_spec(run_id) == effective
    assert store.submitted_spec(run_id) == submitted
    assert store.detail(run_id)["execution_policy_recovery"] == policy
    assert frozen_files == {
        path.relative_to(root): (path.read_bytes(), path.stat().st_mode)
        for path in root.rglob("*")
        if path.is_file()
    }


@pytest.mark.parametrize("interruption", [None, "acknowledgement", "changed-execution"])
def test_prior_unknown_attempt_survives_second_unresolved_pass(
    api_fixture, monkeypatch, interruption,
):
    from contextlib import contextmanager

    from devflow_temporal import delivery_orphans

    path, submission = api_fixture
    store = DeliveryStore(DeliveryConfig.load(path))
    store.submit(submission)
    spec = store.effective_spec(submission["run_id"])
    store.project(
        spec["run_id"],
        phase="blocked",
        execution_state="blocked",
        event_type="blocked",
        message="Original failure",
        outcome="blocked",
        error="Original failure",
        cleanup="unknown",
    )
    with store._connect() as db:
        db.execute(
            "INSERT INTO delivery_attempts "
            "(job_key,run_id,role,iteration,candidate_id,state,cleanup) "
            "VALUES ('orphan',?,'implement',0,'input','unknown','unknown')",
            (spec["run_id"],),
        )
        db.execute(
            "INSERT INTO delivery_mutations (command_id,run_id,kind,request_digest,state) "
            "VALUES ('pending-write',?,'publish','original','pending')",
            (spec["run_id"],),
        )
        unknown = dict(db.execute("SELECT * FROM delivery_attempts").fetchone())

    def complete(store, spec, row, observed):  # authenticated observation precedes transition
        observed({"cleanup": "observed-native-confirmed"})
        assert any(event["payload"].get("original_attempts") == [unknown]
                   for event in store.events(spec["run_id"]))
        with store._connect() as db:
            db.execute(
                "UPDATE delivery_attempts SET state='finished',cleanup='confirmed',"
                "result_json='{}' "
                "WHERE job_key=?",
                (row["job_key"],),
            )
        if interruption == "acknowledgement":
            raise RuntimeError("Completion acknowledgement lost after commit")
        if interruption == "changed-execution":
            with store._connect() as db:
                db.execute("UPDATE delivery_runs SET protocol_revision=1")
        return {"cleanup": "observed-native-confirmed"}

    class Resources:
        def __init__(self, _spec, *, read_only):
            self.manifest = path

        @contextmanager
        def locked(self):
            yield {"processes": ["registered.json"]}

        def finalize(self, outcome, *, uncertain):
            return {"state": "unknown", "resource_cleanup": "unknown", "process_cleanup": "unknown"}

    monkeypatch.setattr(delivery_orphans, "_complete_attempt", complete)
    monkeypatch.setattr(delivery_orphans, "RunResources", Resources)
    monkeypatch.setattr(
        delivery_orphans, "_observe_registered_process", lambda s, p: {"journal": str(p)}
    )
    for _ in range(2):  # the run stays selected while cleanup is unknown
        with store._connect() as db:
            original = dict(db.execute("SELECT * FROM delivery_runs").fetchone())
        if interruption == "changed-execution" and _ == 0:
            with pytest.raises(ValueError, match="execution changed"):
                delivery_orphans._reconcile(store, original, spec, {"status": "TERMINATED"})
        else:
            delivery_orphans._reconcile(store, original, spec, {"status": "TERMINATED"})
    with store._connect() as db:
        payloads = [
            json.loads(r[0])
            for r in db.execute(
                "SELECT payload_json FROM delivery_events WHERE type='resource_cleanup_reconciled'"
            )
        ]
    assert any(p.get("original_attempts") == [unknown] for p in payloads)
