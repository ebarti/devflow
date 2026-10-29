from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path

import pytest
from test_delivery_stopped_resume import project
from test_delivery_store import _git
from test_delivery_store import service as service
from test_delivery_unused_budget_resume import fixed_stop

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json, digest
from devflow_temporal.delivery_broker import DeliveryBroker
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def stopped_feature_scope(service, monkeypatch):
    from test_delivery_feature_execution import feature_service

    from devflow_temporal import delivery_feature_activities as activities
    from devflow_temporal.delivery_activities import _bind_planning_repair
    from devflow_temporal.delivery_feature_execution import (
        register_worker,
        registry,
        worker_key,
        worker_spec,
    )
    from devflow_temporal.delivery_github_contract import ordered_chunks

    owner, request = service
    source = owner.config.path.parent / 'source'
    for name in ('contract.test.ts', 'exemptions.json'):
        (source / name).write_text('Original owning file\n')
    _git(source, 'add', 'contract.test.ts', 'exemptions.json')
    _git(source, 'commit', '-qm', 'Tracked omitted owners')
    _git(source, 'branch', '-M', 'main')
    _git(source, 'push', 'origin', 'main')
    raw = copy.deepcopy(owner.config.raw)
    raw['repositories']['fixture']['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    owner.config.path.write_text(json.dumps(raw))
    store, request, _ = feature_service(
        (DeliveryStore(DeliveryConfig.load(owner.config.path)), request), monkeypatch)
    plan = json.loads(request['accepted_plan'])
    plan.update(version=2, final_gates=[])
    for stream in plan['workstreams']:
        for chunk in stream['chunks']:
            chunk['expected_paths'] = chunk.pop('allowed_paths')
            chunk['gates'] = []
    request['accepted_plan'] = canonical_json(plan)
    store.submit(request)
    parent = store.effective_spec(request['run_id'])
    shared, token = registry(parent), parent['feature_delivery']['owner']
    shared.checkpoint(token, 'accepted-plan', {'digest': digest(plan)})
    chunk = ordered_chunks(plan)[0]
    issue = {'id': 'I_child', 'number': 10, 'url': 'https://github.com/example/fixture/issues/10'}
    _git(source, 'checkout', '-b', 'prerequisite')
    _git(source, 'commit', '--allow-empty', '-qm', 'Completed prerequisite chunk')
    dependency_head = _git(source, 'rev-parse', 'HEAD')
    _git(source, 'checkout', 'main')
    child = worker_spec(parent, chunk, issue, kind='build',
                        base_sha=dependency_head, base_branch='prerequisite')
    register_worker(store, parent, child)
    shared.checkpoint(token, 'assignment:' + chunk['id'] + ':build', {
        'run_id': child['run_id'], 'store_path': token['store_path'],
        'chunk_id': chunk['id'], 'kind': 'build'})
    broker = DeliveryBroker(store, child)
    broker.prepare()
    input_candidate = broker.candidate()
    (broker.checkout / 'README.md').write_text('Retained partial implementation\n')
    candidate = broker.candidate()
    evidence = broker.state_dir / 'scope-finding.txt'
    evidence.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    evidence.write_text('The two tracked owning files were omitted from authority.\n')
    diagnostic = {'version': 1, 'kind': 'planning_defect', 'category': 'dependency',
                  'chunk_id': chunk['id'], 'plan_revision': 1, 'plan_sha256': digest(plan),
                  'candidate_id': candidate['id'], 'detail': evidence.read_text(),
                  'evidence': [{'path': str(evidence),
                                'sha256': hashlib.sha256(evidence.read_bytes()).hexdigest()}]}
    receipt = {'status': 'blocked', 'session_id': 'original-implementation',
               'finish_reason': 'done', 'cleanup': 'confirmed',
               'findings': ['contract.test.ts and exemptions.json require owning-file edits'],
               'native_process': {'state': 'finished', 'exit_code': 0,
                                  'cleanup': 'observed-native-confirmed',
                                  'monitoring_complete': True, 'stdio_drained': True},
               'summary': 'Retained native assessment requires omitted owning files',
               'planning_defect': diagnostic}
    initial = {**copy.deepcopy(receipt), 'status': 'findings',
               'planning_defect': {**copy.deepcopy(diagnostic),
                                   'candidate_id': input_candidate['id']}}
    initial_role = {key: value for key, value in initial.items() if key != 'planning_defect'}
    initial_role.update(role='implement', iteration=0, candidate=candidate,
                        input_candidate_id=input_candidate['id'])
    shared.repair(token, child['run_id'] + ':1', 'Product repair at worker iteration 1')
    role = _bind_planning_repair({'spec': child, 'iteration': 1}, copy.deepcopy(receipt))
    assert role.get('planning_defect_repair') and 'planning_defect_repair' not in receipt
    role.update(role='implement', iteration=1, candidate=candidate,
                input_candidate_id=candidate['id'])
    state = {'run_id': child['run_id'], 'phase': 'blocked', 'outcome': 'blocked',
             'execution_state': 'blocked', 'cleanup': 'confirmed', 'revision': 9, 'iteration': 1,
             'candidate': candidate, 'pull_request': None,
             'roles': [initial_role, role],
             'checks': {'planning_defect': diagnostic},
             'error': 'workstream identified a planning defect'}
    store.project(child['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='confirmed', event_type='blocked', message=state['error'],
                  error=state['error'], iteration=1, protocol_revision=9,
                  candidate=candidate, checks=state['checks'])
    with store._connect() as db:
        for iteration, native, admitted in ((0, initial, input_candidate), (1, receipt, candidate)):
            db.execute("INSERT INTO delivery_attempts(job_key,run_id,role,iteration,candidate_id,"
                       "state,session_id,result_json,cleanup) "
                       "VALUES (?,?,'implement',?,?,'finished',?,?,'confirmed')",
                       ('original-build-' + str(iteration), child['run_id'], iteration,
                        admitted['id'], receipt['session_id'], canonical_json(native)))
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    shared.finish_worker(token, worker_key(child['run_id'], token), {'cleanup': 'confirmed'})
    shared.stop(token, 'original-feature-stop', {})
    store.project(parent['run_id'], phase='blocked', execution_state='blocked', outcome='blocked',
                  cleanup='confirmed', event_type='blocked', message='Retained build needs scope')
    with store._connect() as db:
        store.state.release_work(db, parent['work_id'], 'external:devflow:' + parent['run_id'])
        revision = db.execute('SELECT revision FROM delivery_runs WHERE run_id=?',
                              (parent['run_id'],)).fetchone()[0]
    # Temporal transport and provider execution are modeled; admission, receipts,
    # Git custody, permission checks, reservations and seed import are real.
    def closed(run_id, **_):
        return {'result': state if run_id == child['run_id'] else {},
                'workflow_id': 'delivery-' + run_id, 'execution_run_id': 'closed-' + run_id,
                'request_digest': store.submitted_spec(run_id)['request_digest'],
                'recovery_digest': None}
    monkeypatch.setattr(store, '_completed_temporal_result', closed)
    monkeypatch.setattr(store, '_ensure_no_remote_pr', lambda *_: None)
    monkeypatch.setattr(activities, '_context', lambda spec: (store, DeliveryBroker(store, spec)))
    record = {'manifest': {'plan': plan, 'publication': {'members': []},
                          'workstream_issues': {chunk['workstream_id']: issue}}}
    monkeypatch.setattr(activities, 'current_record', lambda _: record)
    amended = copy.deepcopy(store.config.raw)
    added = ['contract.test.ts', 'exemptions.json']
    amended['repositories']['fixture']['allowed_paths'] += added
    path = store.config.state_root / 'feature-scope.json'
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    command = {'command_id': 'continue-feature-scope', 'expected_revision': revision,
               'added_paths': added, 'amended_config_path': str(path),
               'amended_config_sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
    return store, parent, child, broker, state, command


async def test_stopped_feature_scope_continues_original_build_and_normal_integration(
    stopped_feature_scope, monkeypatch,
):
    import httpx

    from devflow_temporal import delivery_feature_activities as activities
    from devflow_temporal.delivery_api import create_app
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry
    from devflow_temporal.delivery_feature_execution import continue_feature, registry
    from devflow_temporal.delivery_feature_scope import MARKER
    from devflow_temporal.delivery_github_contract import GitHubDelivery
    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    store, parent, child, broker, state, command = stopped_feature_scope
    original = copy.deepcopy(child)
    before = copy.deepcopy(state)
    with store._connect() as db:
        native_attempt = dict(db.execute('SELECT * FROM delivery_attempts '
                                         'WHERE run_id=? AND iteration=1',
                                         (child['run_id'],)).fetchone())
    for absent in (False, True):
        ordinary = copy.deepcopy(before['roles'][-1])
        ordinary.pop('planning_defect_repair')
        native = json.loads(native_attempt['result_json'])
        ordinary['planning_defect'] = native['planning_defect'] = None
        if absent:
            ordinary.pop('planning_defect')
            native.pop('planning_defect')
        assert resume._implementation_receipt(
            child, ordinary, {**native_attempt, 'result_json': canonical_json(native)})
    shared = registry(parent)
    registry_before = {p: p.read_bytes() for p in shared.path.parent.iterdir() if p.is_file()}
    real_open = os.open
    def readonly_open(path, flags, *args, **kwargs):
        if Path(path).parent == shared.path.parent:
            assert not flags & (os.O_CREAT | os.O_RDWR | os.O_WRONLY | os.O_TRUNC)
        return real_open(path, flags, *args, **kwargs)
    with monkeypatch.context() as readonly:
        readonly.setattr(os, 'open', readonly_open)
        readonly.setattr(ExecutionRegistry, '__init__',
                         lambda *_: pytest.fail('read-only receipt initialized registry schema'))
        observed = resume.review_snapshot(store, child['run_id'])
    assert observed['session_id'] == 'original-implementation'
    assert observed['candidate'] == before['candidate']
    assert {p: p.read_bytes() for p in shared.path.parent.iterdir()
            if p.is_file()} == registry_before
    original_owner = shared.current('I_feature')
    remote = copy.deepcopy(parent['feature_delivery']['snapshot'])
    changed = json.loads(parent['accepted_plan'])
    changed['acceptance'].append('Unapproved remote outcome')
    remote['delivery'] = {'manifest': {'plan': changed}}
    def inventory():
        with store._connect() as db:
            return [db.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                    for table in ('delivery_runs', 'delivery_commands', 'delivery_outbox')]
    original_inventory = inventory()
    app = create_app(store.config.path)
    app.state.delivery.store = store
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url=store.config.dashboard_url) as client:
        session = (await client.get('/api/session')).json()
        client.headers.update({'origin': store.config.dashboard_url,
                               'x-devflow-csrf': session['csrf_token']})
        with monkeypatch.context() as drift:
            drift.setattr(GitHubDelivery, 'snapshot', lambda *_: copy.deepcopy(remote))
            refused = await client.post('/api/runs/' + parent['run_id'] + '/continue-feature',
                                        json=command)
        assert refused.status_code == 409 and 'accepted feature plan' in refused.text
        assert inventory() == original_inventory
        assert shared.current('I_feature') == original_owner
        accepted_remote = copy.deepcopy(remote)
        accepted_remote['delivery']['manifest']['plan'] = json.loads(parent['accepted_plan'])
        with monkeypatch.context() as accepted_snapshot:
            accepted_snapshot.setattr(GitHubDelivery, 'snapshot',
                                      lambda *_: copy.deepcopy(accepted_remote))
            response = await client.post('/api/runs/' + parent['run_id'] + '/continue-feature',
                                         json=command)
        assert response.status_code == 200, response.text
        admitted = response.json()
        assert (await client.post('/api/runs/' + parent['run_id'] + '/continue-feature',
                                  json=command)).json() == admitted
        stale = await client.post('/api/runs/' + parent['run_id'] + '/continue-feature',
                                  json={**command, 'added_paths': ['contract.test.ts']})
        assert stale.status_code == 409
    successor = store.effective_spec(admitted['run_id'])
    assert successor['accepted_plan'] == parent['accepted_plan']
    assert successor['plan_approval'] == parent['plan_approval']
    resumed = activities.reserve(successor, child['feature_worker']['chunk_id'], 'build')
    effective = resumed['spec']
    assert effective['run_id'] == child['run_id']
    assert store.submitted_spec(child['run_id']) == original
    assert effective['accepted_plan'] == child['accepted_plan']
    assert effective['base_sha'] == child['base_sha']
    assert effective['base_sha'] != parent['base_sha']
    assert effective['feature_plan_revision'] == child['feature_plan_revision']
    assert effective['policy'] == {**child['policy'],
                                  'allowed_paths': ['README.md', *command['added_paths']]}
    with store._connect() as db:
        recovery = json.loads(db.execute('SELECT recovery_json FROM delivery_runs WHERE run_id=?',
                                        (child['run_id'],)).fetchone()[0])
    assert recovery['state'] == before and recovery['session_id'] == 'original-implementation'
    assert recovery['execution_candidate'] == {**broker.candidate(),
                                               'policy_digest': effective['policy_digest']}
    assert resume.readback(store, effective, recovery) == {'state': 'confirmed'}
    flow = DeliveryWorkflow()
    async def confirmed(*_):
        return True
    async def activity(name, payload):
        if name == 'delivery_tracker_start':
            return {'state': 'consistent'}
        if name == 'delivery_role':
            assert payload['resume_session'] == 'original-implementation'
            assert payload['iteration'] == 2
            assert payload['candidate'] == recovery['execution_candidate']
            builder = DeliveryBroker(store, payload['spec'])
            for name in command['added_paths']:
                (builder.checkout / name).write_text('Corrected owning file\n')
            return {'role': 'implement', 'iteration': 2, 'status': 'pass',
                    'session_id': payload['resume_session'], 'cleanup': 'confirmed',
                    'candidate': builder.candidate()}
        assert name == 'delivery_feature_seal_build'
        return activities.seal_build(payload['spec'], payload['candidate'])
    async def project(*_):
        pass
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_planning_request', lambda _: {})
    completed = await flow._resume_stopped(effective, recovery)
    assert completed['outcome'] == 'delivered'
    seed = completed['checks']['build_checkpoint']
    shared, token = registry(successor), successor['feature_delivery']['owner']
    from devflow_temporal.delivery_feature_execution import worker_key
    shared.finish_worker(token, worker_key(child['run_id'], token), {'cleanup': 'confirmed'})
    with store._connect() as db:
        store.state.release_work(db, child['work_id'], 'external:devflow:' + child['run_id'])
    integration = activities.reserve(successor, child['feature_worker']['chunk_id'], 'chunk')
    imported = DeliveryBroker(store, integration['spec'])
    imported.prepare()
    assert integration['spec'][MARKER] == successor[MARKER]
    assert integration['spec']['feature_worker']['seed'] == seed
    assert imported.validate_candidate_scope() == {'README.md', *command['added_paths']}
    for name in command['added_paths']:
        assert (imported.checkout / name).read_text() == 'Corrected owning file\n'
    assert registry(successor).budget('I_feature')['used'] == 1
    # An ordinary later continuation inherits the same guard and sealed receipt.
    integration_spec = integration['spec']
    shared.finish_worker(token, worker_key(integration_spec['run_id'], token),
                         {'cleanup': 'confirmed'})
    shared.stop(token, 'stopped-scope-descendant', {})
    store.project(successor['run_id'], phase='blocked', execution_state='blocked',
                  outcome='blocked',
                  cleanup='confirmed', event_type='blocked', message='Stopped scoped descendant')
    with store._connect() as db:
        for spec in (integration_spec, successor):
            store.state.release_work(db, spec['work_id'], 'external:devflow:' + spec['run_id'])
        revision = db.execute('SELECT revision FROM delivery_runs WHERE run_id=?',
                              (successor['run_id'],)).fetchone()[0]
    stopped_owner, stopped_inventory = shared.current('I_feature'), inventory()
    with monkeypatch.context() as drift:
        drift.setattr(GitHubDelivery, 'snapshot', lambda *_: copy.deepcopy(remote))
        with pytest.raises(ValueError, match='accepted feature plan'):
            continue_feature(store, successor['run_id'], {
                'command_id': 'scope-descendant', 'expected_revision': revision})
    assert inventory() == stopped_inventory and shared.current('I_feature') == stopped_owner
    assert store.effective_spec(successor['run_id'])[MARKER] == successor[MARKER]


@pytest.fixture
def scope_stop(service, monkeypatch):
    owner, request = service
    source = owner.config.path.parent / 'source'
    (source / 'contract.test.ts').write_text(
        "expect(parseRequest({name: 'fixture'}).success).toBe(true)\n")
    (source / 'another.test.ts').write_text('Existing second owning test\n')
    _git(source, 'add', 'contract.test.ts', 'another.test.ts')
    _git(source, 'commit', '-qm', 'Existing contract test')
    config = copy.deepcopy(owner.config.raw)
    config['repositories']['fixture']['expected_base_sha'] = _git(source, 'rev-parse', 'HEAD')
    owner.config.path.write_text(json.dumps(config))
    owner = DeliveryStore(DeliveryConfig.load(owner.config.path))
    store, broker, state, command = fixed_stop.__wrapped__((owner, request), monkeypatch)
    (broker.checkout / 'README.md').write_text('Authentic uncommitted implementation\n')
    candidate = broker.candidate()
    receipt = {'status': 'findings', 'session_id': 'original-implementation',
               'finish_reason': 'done', 'cleanup': 'confirmed',
               'findings': ['contract.test.ts must validate the new required request field']}
    state['roles'] = [{**receipt, 'role': 'implement', 'iteration': 1, 'candidate': candidate}]
    state['error'] = 'implementer did not establish a pass'
    project(store, state)
    with store._connect() as db:
        db.execute('UPDATE delivery_attempts SET result_json=?,session_id=?',
                   (canonical_json(receipt), receipt['session_id']))
    command['expected_candidate_id'] = candidate['id']
    amended = copy.deepcopy(store.config.raw)
    amended['repositories']['fixture']['allowed_paths'].append('contract.test.ts')
    path = broker.state_dir.parents[1] / 'amended.json'
    path.write_text(json.dumps(amended))
    path.chmod(0o600)
    import hashlib
    command.update(added_paths=['contract.test.ts'], amended_config_path=str(path),
                   amended_config_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return store, broker, state, command


@pytest.mark.parametrize('drift', ['hash', 'hash-type'])
def test_scope_resume_rejects_unrelated_authority_or_unowned_file(scope_stop, drift, monkeypatch):
    import hashlib
    from pathlib import Path

    store, broker, state, command = scope_stop
    path = Path(command['amended_config_path'])
    config = json.loads(path.read_text())
    if drift in {'checks', 'budget', 'model'}:
        if drift == 'checks':
            config['repositories']['fixture']['required_ci'] = ['different-check']
        elif drift == 'budget':
            config['max_repairs'] = 3
        else:
            config['roles']['implement']['model'] = 'different'
        path.write_text(json.dumps(config))
        command['amended_config_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif drift == 'hash':
        command['amended_config_sha256'] = 'a' * 64
    elif drift == 'hash-type':
        command['amended_config_sha256'] = 1
    elif drift in {'missing', 'wildcard'}:
        name = 'missing.test.ts' if drift == 'missing' else '*.ts'
        command['added_paths'] = [name]
        config['repositories']['fixture']['allowed_paths'] = ['README.md', name]
        path.write_text(json.dumps(config))
        command['amended_config_sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
    elif drift == 'already-allowed':
        command['added_paths'] = ['README.md']
    elif drift in {'changed', 'symlink'}:
        target = broker.checkout / 'contract.test.ts'
        if drift == 'changed':
            target.write_text('Unauthorized post-stop edit\n')
        else:
            target.unlink()
            target.symlink_to(broker.checkout / 'README.md')
    elif drift in {'non-finding', 'session'}:
        role = state['roles'][0]
        role['status' if drift == 'non-finding' else 'session_id'] = (
            'pass' if drift == 'non-finding' else None)
    elif drift == 'oversized':
        command['additional_iterations'] = 2
    else:
        del command['amended_config_sha256']
    monkeypatch.setattr(resume, 'prepare_runtime',
                        lambda *_: pytest.fail('invalid scope prepared native runtime'))
    with pytest.raises(ValueError):
        store.continue_repair('run-1', command)
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                             (command['command_id'],)).fetchone()
        assert store.state.claim_for(db, broker.spec['work_id']) is None
