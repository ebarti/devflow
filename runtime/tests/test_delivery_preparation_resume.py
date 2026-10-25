"""Owned native preparation command, real controller return and public admission.

Only the GitHub and completed-history descriptor are controlled fixture seams.
No original state or stored provider result is used by this neutral fixture.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from importlib.metadata import distribution
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_delivery_store import _git
from test_delivery_store import service as service

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_activities import delivery_repair_preflight, delivery_role
from devflow_temporal.delivery_baseline import run_baseline_checks
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_preparation import prepare_authority
from devflow_temporal.delivery_preparation_recovery import completed_prerequisite
from devflow_temporal.delivery_resources import RunResources, read_private, write_private
from devflow_temporal.delivery_store import DeliveryStore
from devflow_temporal.supervisor import DeliverySupervisor


@pytest.fixture
def native_preparation_stop(service, monkeypatch, tmp_path):
    owner, request = service
    source = Path(owner.config.raw['repositories']['fixture']['source_path'])
    source.joinpath('.gitignore').write_text('.venv/\n')
    source.joinpath('pyproject.toml').write_text(
        '[project]\nname="owned-locked-preparation"\nversion="0.1.0"\nrequires-python=">=3.12"\n')
    subprocess.run([shutil.which('uv'), 'lock', '--offline', '--python',
                    str(Path(sys.executable).resolve()), '--project', str(source)],
                   check=True, capture_output=True, text=True)
    _git(source, 'add', '.gitignore', 'pyproject.toml', 'uv.lock')
    _git(source, 'commit', '-qm', 'Declare owned locked environment')
    ready = tmp_path / 'synthetic-cache-ready'
    ready.write_text('Owned setup is initially available\n')
    # This fixed operator recipe checks a neutral owned cache prerequisite, then
    # performs a real locked/offline uv sync in the declared exact checkout.
    program = (
        'import pathlib,subprocess,sys; ready=pathlib.Path(sys.argv[1]); '
        'print("owned cache ready",ready.exists()); '
        'sys.exit(1) if not ready.exists() else None; '
        'sys.exit(subprocess.run([sys.argv[2],"sync","--offline","--frozen",'
        '"--no-dev","--python",sys.executable],check=False).returncode)'
    )
    recipe = {'id': 'owned-locked-environment', 'kind': 'command',
              'argv': [str(Path(sys.executable).resolve()), '-c', program,
                       str(ready), shutil.which('uv')], 'timeout_seconds': 30,
              'generated_directories': ['.venv']}
    config = copy.deepcopy(owner.config.raw)
    config.update(provider='codex', execution_mode='trusted-local', max_repairs=10,
                  codex_bin=str(distribution('openai-codex-cli-bin').locate_file(
                      'codex_cli_bin/bin/codex')))
    config['roles'] = {role: {'model': 'gpt-6.1-sol', 'effort': 'max'}
                       for role in ('implement', 'review', 'verify')}
    config['repositories']['fixture'].update(
        expected_base_sha=_git(source, 'rev-parse', 'HEAD'),
        prepublish_checks=[recipe], checks=[{'id': 'owned-check', 'argv': ['/usr/bin/true']}],
        baseline_check_ids=[recipe['id']], required_ci=['owned-ci'],
        project_url='https://github.com/users/example/projects/1', assignee='example')
    owner.config.path.write_text(json.dumps(config))
    store = DeliveryStore(DeliveryConfig.load(owner.config.path))
    store.submit(request)
    store.mark_start('run-1', accepted=True)
    spec = prepare_authority(store, store.submitted_spec('run-1'))
    broker = DeliveryBroker(store, spec)
    broker.prepare()
    baseline = run_baseline_checks(broker)
    assert baseline['state'] == 'passed'
    ready.unlink()
    monkeypatch.setattr(DeliveryBroker, '_existing_pr', lambda *_a, **_k: None)
    monkeypatch.setattr(DeliveryStore, '_ensure_no_remote_pr', staticmethod(lambda *_a: None))

    async def forbid_provider(*_args, **_kwargs):
        raise AssertionError('No provider may launch during this preparation failure fixture')
    monkeypatch.setattr(DeliverySupervisor, 'run', forbid_provider)

    def native_role(current, iteration):
        result = asyncio.run(delivery_role({'spec': current, 'role': 'implement',
            'iteration': iteration, 'candidate': DeliveryBroker(store, current).candidate(),
            'resume_session': None}))
        p = result['implementation_preparation']
        assert result['status'] == 'blocked' and result['session_id'] is None
        assert p['state'] == 'failed' and p['cleanup'] == 'confirmed'
        assert p['source_unchanged'] is True and len(p['results']) == 1
        child = p['results'][0]['native_process']
        assert child['state'] == 'finished' and child['exit_code'] == 1
        assert child['monitoring_complete'] is True
        assert child['cleanup'] == 'observed-native-confirmed'
        with store._connect() as db:
            assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
        return result

    initial = native_role(spec, 0)
    closed = {}
    monkeypatch.setattr(store, '_completed_temporal_result', lambda *_a, **_k: closed)

    def close(current, role, iteration, protocol, previous=None):
        receipt = RunResources(current).finalize('blocked')
        assert receipt['state'] == 'confirmed'
        state = copy.deepcopy(previous['state']) if previous else {
            'run_id': 'run-1', 'roles': [], 'checks': {'baseline': baseline},
            'pull_request': None, 'usage': {}, 'findings': []}
        state.update(phase='blocked', execution_state='blocked', outcome='blocked',
            iteration=iteration, revision=protocol, cleanup='confirmed',
            error='implementer did not establish a pass',
            candidate=DeliveryBroker(store, current).candidate())
        state['roles'].append(role)
        state['checks']['resource_cleanup'] = receipt
        store.project('run-1', phase='blocked', execution_state='blocked', outcome='blocked',
            iteration=iteration, protocol_revision=protocol, cleanup='confirmed',
            error=state['error'], candidate=state['candidate'], checks=state['checks'],
            event_type='blocked', message='Owned controller preparation stopped before provider')
        with store._connect() as db:
            store.state.release_work(db, current['work_id'], 'external:devflow:run-1')
            row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='run-1'").fetchone())
        closed.clear()
        closed.update(workflow_id=row['workflow_id'] or 'delivery-run-1',
            execution_run_id=f'owned-closed-preparation-{iteration}',
            closed_at=datetime.now(UTC).isoformat(), request_digest=current['request_digest'],
            recovery_digest=digest(previous) if previous else None, result=state)
        return state

    state = close(spec, initial, 0, 9)
    def command(iteration, protocol, name, additional):
        current_candidate = state['candidate']
        return {'continuation_kind': resume.KIND, 'command_id': name,
            'expected_revision': protocol, 'expected_iteration': iteration,
            'expected_candidate_id': current_candidate['id'],
            'expected_candidate_head': current_candidate['head'],
            'additional_iterations': additional}
    first = command(0, 9, 'first-positive-finite', 1)
    assert store.repair_admission_preflight('run-1', first)['authorized_through_iteration'] == 1
    store.continue_repair('run-1', first)
    current = store.effective_spec('run-1')
    store.mark_start('run-1', accepted=True)
    with store._connect() as db:
        previous = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
    second = native_role(current, 1)
    state = close(current, second, 1, 15, previous)
    return SimpleNamespace(store=store, state=state, previous=previous, current=current,
        role=second, recipe=recipe, ready=ready, command=command)


def test_native_prerequisite_failure_retains_only_its_unused_provider_turn(
    native_preparation_stop, monkeypatch,
):
    case = native_preparation_stop
    store, state = case.store, case.state
    command = case.command(1, 15, 'retry-unused-provider-turn', 0)
    original = copy.deepcopy(state)
    row = case.role['implementation_preparation']['results'][-1]
    journal = Path(row['native_process']['journal'])
    original_journal = journal.read_bytes()
    assert 'launched' not in row and 'failure_kind' not in row
    preflight = store.repair_admission_preflight('run-1', command)
    assert preflight['authorized_through_iteration'] == 1
    assert state == original

    checkout = Path(case.current['checkout'])
    retained_checkout = checkout.with_name(checkout.name + '-owned-held-source')

    def source_refused():
        with pytest.raises((ValueError, OSError, subprocess.CalledProcessError)) as caught:
            store.repair_admission_preflight('run-1', command)
        if isinstance(caught.value, subprocess.CalledProcessError):
            assert caught.value.returncode == 128
            assert caught.value.cmd[:3] == ['git', '-C', str(checkout)]
            assert caught.value.cmd[3:] == ['rev-parse', 'HEAD']

    checkout.rename(retained_checkout)
    try:
        source_refused()
        checkout.symlink_to(retained_checkout, target_is_directory=True)
        source_refused()
        checkout.unlink()
    finally:
        if checkout.is_symlink():
            checkout.unlink()
        retained_checkout.rename(checkout)

    # Current work and provider inventory remain independent mandatory guards.
    with store._connect() as db:
        store.state.claim_work(db, case.current['work_id'], 'foreign-owner', 'Owned contradiction')
    with pytest.raises(ValueError):
        store.repair_admission_preflight('run-1', command)
    with store._connect() as db:
        store.state.release_work(db, case.current['work_id'], 'foreign-owner')
        db.execute("INSERT INTO delivery_attempts (job_key,run_id,role,iteration,candidate_id,"
                   "state,cleanup,result_json) VALUES "
                   "('unexpected-provider','run-1','implement',1,?,"
                   "'finished','confirmed','{}')", (state['candidate']['id'],))
    with pytest.raises(ValueError):
        store.repair_admission_preflight('run-1', command)
    with store._connect() as db:
        db.execute("DELETE FROM delivery_attempts WHERE job_key='unexpected-provider'")

    # Literal setup completion alone cannot adopt a newly created stopped-run
    # environment. Its unregistered generation still fails resource custody.
    case.ready.write_text('Owned cache repaired\n')
    setup = subprocess.run(case.recipe['argv'], cwd=case.current['checkout'],
                           check=False, capture_output=True, text=True, timeout=30)
    assert setup.returncode == 0, setup.stderr
    assert DeliveryBroker(store, case.current).candidate() == state['candidate']
    with pytest.raises(ValueError, match='root identity changed|recreated'):
        store.repair_admission_preflight('run-1', command)
    # Remove only this test's just-created, unregistered environment, restoring
    # the absent generation. This is deliberately not an operator repair route.
    shutil.rmtree(Path(case.current['checkout']) / '.venv')
    assert store.repair_admission_preflight('run-1', command)['authorized_through_iteration'] == 1

    response = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == response
    current = store.effective_spec('run-1')
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs').fetchone()[0])
        assert db.execute('SELECT COUNT(*) FROM delivery_runs').fetchone()[0] == 1
        assert db.execute('SELECT COUNT(*) FROM delivery_attempts').fetchone()[0] == 0
        assert db.execute('SELECT COUNT(*) FROM delivery_repair_grants').fetchone()[0] == 0
    assert recovery['maximum_iteration'] == recovery['pending_repair']['iteration'] == 1
    assert recovery['state'] == original
    proof = recovery['pending_repair']['implementation_preparation']
    assert proof['closed_role_sha256'] == digest(case.role)
    assert proof['files'][str(journal)] == hashlib.sha256(original_journal).hexdigest()
    assert asyncio.run(delivery_repair_preflight({'spec': current, 'recovery': recovery})) == {
        'state': 'confirmed'}

    changed = read_private(journal)
    changed['intent']['timeout'] += 1
    write_private(journal, changed)
    with pytest.raises(ValueError):
        asyncio.run(delivery_repair_preflight({'spec': current, 'recovery': recovery}))
    journal.write_bytes(original_journal)
    assert asyncio.run(delivery_repair_preflight({'spec': current, 'recovery': recovery})) == {
        'state': 'confirmed'}

    class ProviderBoundaryReached(Exception):
        pass

    reached = []

    async def provider_boundary(_self, request):
        reached.append(request)
        raise ProviderBoundaryReached

    monkeypatch.setattr(DeliverySupervisor, 'run', provider_boundary)
    store.mark_start('run-1', accepted=True)
    with pytest.raises(ProviderBoundaryReached):
        asyncio.run(delivery_role({'spec': current, 'role': 'implement', 'iteration': 1,
            'candidate': recovery['execution_candidate'], 'resume_session': None}))
    assert len(reached) == 1 and reached[0]['iteration'] == 1
    assert reached[0]['evidence_context']['implementation_preparation']['state'] == 'passed'
    assert current['policy']['max_repairs'] == 10 and recovery['maximum_iteration'] == 1
    assert state == original and journal.read_bytes() == original_journal
    assert RunResources(current).finalize('blocked')['state'] == 'confirmed'


@pytest.fixture
def prerequisite_receipt(tmp_path):
    state, checkout = tmp_path / 'run', tmp_path / 'checkout'
    state.mkdir(mode=0o700)
    checkout.mkdir(mode=0o700)
    recipe = {'id': 'locked-python', 'kind': 'command', 'argv': ['uv', 'sync', '--offline'],
              'timeout_seconds': 30, 'generated_directories': ['.venv']}
    policy = {'host_sandbox': 'trusted-local', 'prepublish_checks': [recipe],
              'baseline_checks': [recipe],
              'native_identity': {'runtime_payload_sha256': digest('owned native payload')}}
    spec = {'run_id': 'owned', 'provider': 'codex', 'state_dir': str(state),
            'checkout': str(checkout), 'policy': policy, 'policy_digest': digest(policy)}
    previous = {'command_digest': digest('owned command')}
    folder = (state / resume.namespace(previous) / 'evidence/implementation-preparation/1'
              / recipe['id'] / 'native')
    folder.mkdir(mode=0o700, parents=True)
    log = folder / 'process.log'
    log.write_bytes(b'Owned unavailable locked prerequisite\n')
    log.chmod(0o600)
    log_digest = hashlib.sha256(log.read_bytes()).hexdigest()
    native = {'state': 'finished', 'exit_code': 1, 'cleanup': 'observed-native-confirmed',
              'journal': str(folder / 'native-process.json'), 'log': str(log),
              'log_sha256': log_digest, 'monitoring_complete': True, 'stdio_drained': True,
              'timed_out': False, 'cancelled': False, 'listener_conflict': False,
              'runtime_identity': {'runtime_payload_sha256': policy['native_identity'][
                  'runtime_payload_sha256'], 'dirty': False}}
    item = {'id': recipe['id'], 'argv': recipe['argv'], 'cwd': str(checkout), 'passed': False,
            'exit_code': 1, 'cleanup': 'confirmed', 'process_cleanup': native['cleanup'],
            'rejected_output': False, 'rejection_causes': [], 'log': str(log),
            'log_sha256': log_digest, 'native_process': native}
    role = {'role': 'implement', 'iteration': 1, 'status': 'blocked', 'session_id': None,
            'candidate': {'id': digest('owned candidate')}, 'cleanup': 'confirmed',
            'implementation_preparation': {'state': 'failed', 'cleanup': 'confirmed',
                'candidate_id': digest('owned candidate'), 'source_unchanged': True,
                'results': [item]}}
    launch = {'argv': recipe['argv'], 'environment': {'HOME': '/owned-private-home'}}
    journal = {'phase': 'finished', 'monitoring_complete': True, 'ports': [], 'owned': {},
               'intent': {'run_id': spec['run_id'], 'policy_digest': spec['policy_digest'],
                   'argv': recipe['argv'], 'cwd': str(checkout), 'timeout': 30, 'ports': [],
                   'environment_sha256': digest(launch['environment'])},
               'runtime_identity': native['runtime_identity'], 'result': native}
    write_private(folder / 'native-process.json', journal)
    write_private(folder / 'launch.json', launch)
    seal = {'predecessor_spec': spec, 'candidate': role['candidate']}
    return SimpleNamespace(seal=seal, previous=previous, role=role, item=item, native=native,
                           journal=journal, launch=launch, folder=folder, recipe=recipe)


def test_configured_native_prerequisite_proof_binds_every_original_byte(prerequisite_receipt):
    c = prerequisite_receipt
    proof = completed_prerequisite(c.seal, c.previous, c.role)
    assert proof['closed_role_sha256'] == digest(c.role)
    assert proof['execution_spec_sha256'] == digest(c.seal['predecessor_spec'])
    assert proof['predecessor_command_digest'] == c.previous['command_digest']
    assert set(proof['files']) == {str(c.folder / name) for name in (
        'native-process.json', 'launch.json', 'process.log')}


def test_historical_prerequisite_metadata_outlives_its_checkout(prerequisite_receipt):
    c = prerequisite_receipt
    checkout = Path(c.seal['predecessor_spec']['checkout'])
    checkout.rmdir()
    # Historical custody authenticates ended execution metadata. Independent
    # current snapshot/readback still needs the actual source before admission.
    proof = completed_prerequisite(c.seal, c.previous, c.role)
    assert proof['closed_role_sha256'] == digest(c.role)


@pytest.mark.parametrize('drift', ['relative', 'noncanonical', 'parent'])
def test_prerequisite_cwd_metadata_remains_canonical(prerequisite_receipt, drift):
    c = prerequisite_receipt
    spec = c.seal['predecessor_spec']
    if drift == 'relative':
        spec['checkout'] = 'relative-checkout'
    elif drift == 'noncanonical':
        spec['checkout'] += '//'
    else:
        c.recipe['cwd'] = '../checkout'
        c.item['cwd'] += '/../checkout'
    with pytest.raises(ValueError):
        completed_prerequisite(c.seal, c.previous, c.role)


@pytest.mark.parametrize('drift', ['signal', 'timeout', 'cancelled', 'stdio', 'monitor',
                                  'interrupted', 'listener', 'unknown'])
def test_unfinished_native_prerequisite_never_proves_a_pending_turn(prerequisite_receipt, drift):
    c = prerequisite_receipt
    if drift == 'signal':
        c.item['exit_code'] = c.native['exit_code'] = -15
    elif drift == 'unknown':
        c.native['state'] = 'unknown'
    else:
        field = {'timeout': 'timed_out', 'stdio': 'stdio_drained', 'monitor': 'monitoring_complete',
                 'listener': 'listener_conflict', 'interrupted': 'interrupted_monitor'}.get(
                     drift, drift)
        c.native[field] = drift not in {'stdio', 'monitor'}
    write_private(c.folder / 'native-process.json', c.journal)
    with pytest.raises(ValueError):
        completed_prerequisite(c.seal, c.previous, c.role)


@pytest.mark.parametrize('drift', ['recipe', 'test', 'candidate', 'multiple_failures', 'foreign'])
def test_only_the_exact_configured_prerequisite_is_eligible(prerequisite_receipt, drift):
    c = prerequisite_receipt
    if drift == 'recipe':
        c.recipe['argv'] = ['uv', 'sync', '--different']
    elif drift == 'test':
        c.recipe['kind'] = 'test'
    elif drift == 'candidate':
        c.role['implementation_preparation']['candidate_id'] = digest('different candidate')
    elif drift == 'multiple_failures':
        c.role['implementation_preparation']['results'].append(copy.deepcopy(c.item))
    else:
        c.item['id'] = 'foreign-prerequisite'
    assert completed_prerequisite(c.seal, c.previous, c.role) is None


@pytest.mark.parametrize('drift', ['journal', 'launch', 'log', 'path', 'policy', 'timeout',
                                  'runtime', 'linked', 'mode', 'oversize'])
def test_native_prerequisite_receipts_are_fresh_and_private(prerequisite_receipt, drift):
    c = prerequisite_receipt
    if drift == 'journal':
        c.journal['phase'] = 'authorized'
    elif drift == 'launch':
        c.launch['environment']['HOME'] += '-changed'
    elif drift == 'log':
        (c.folder / 'process.log').write_bytes(b'Changed output\n')
    elif drift == 'path':
        c.native['journal'] = str(c.folder.parent / 'different/native-process.json')
    elif drift == 'policy':
        c.journal['intent']['policy_digest'] = digest('different policy')
    elif drift == 'timeout':
        c.journal['intent']['timeout'] += 1
    elif drift == 'runtime':
        c.native['runtime_identity']['runtime_payload_sha256'] = digest('different payload')
    elif drift == 'mode':
        (c.folder / 'process.log').chmod(0o644)
    elif drift == 'oversize':
        (c.folder / 'process.log').write_bytes(b'x' * (4 * 1024 * 1024 + 1))
    else:
        log = c.folder / 'process.log'
        saved = c.folder / 'retained.log'
        log.rename(saved)
        log.symlink_to(saved)
    write_private(c.folder / 'native-process.json', c.journal)
    write_private(c.folder / 'launch.json', c.launch)
    with pytest.raises(ValueError):
        completed_prerequisite(c.seal, c.previous, c.role)


def test_native_prerequisite_collection_cannot_change_during_observation(
    prerequisite_receipt, monkeypatch,
):
    from devflow_temporal import delivery_preparation_recovery as recovery

    c = prerequisite_receipt
    original = recovery.private_bytes
    changed = False

    def after_launch(path):
        nonlocal changed
        raw = original(path)
        if Path(path).name == 'launch.json' and not changed:
            changed = True
            c.journal['intent']['timeout'] += 1
            write_private(c.folder / 'native-process.json', c.journal)
        return raw

    monkeypatch.setattr(recovery, 'private_bytes', after_launch)
    with pytest.raises(ValueError, match='during observation'):
        completed_prerequisite(c.seal, c.previous, c.role)


