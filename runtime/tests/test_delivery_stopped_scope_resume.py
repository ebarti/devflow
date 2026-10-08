from __future__ import annotations

import copy
import json

import pytest
from test_delivery_stopped_resume import project, saved
from test_delivery_store import _git
from test_delivery_store import service as service
from test_delivery_unused_budget_resume import fixed_stop

from devflow_temporal import delivery_stopped_resume as resume
from devflow_temporal.contracts import canonical_json
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore


@pytest.fixture
def scope_stop(service, monkeypatch):
    owner, request = service
    source = owner.config.path.parent / 'source'
    (source / 'contract.test.ts').write_text('expect(endpoints).toHaveLength(145)\n')
    _git(source, 'add', 'contract.test.ts')
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
               'findings': ['contract.test.ts expects 145 endpoints but the new contract has 149']}
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


def test_scope_resume_retains_original_failure_session_and_fixed_budget(scope_stop, monkeypatch):
    store, broker, state, command = scope_stop
    original = copy.deepcopy(store.submitted_spec('run-1'))
    before = copy.deepcopy(state)
    with monkeypatch.context() as check:
        check.setattr(resume, 'prepare_runtime',
                      lambda *_: pytest.fail('preflight prepared native runtime'))
        result = store.repair_admission_preflight('run-1', command)
    assert result['preflight'] is True
    assert result['added_paths'] == ['contract.test.ts']
    assert result['authorized_through_iteration'] == 2
    assert store.submitted_spec('run-1') == original
    admitted = store.continue_repair('run-1', command)
    assert store.continue_repair('run-1', command) == admitted
    effective = store.effective_spec('run-1')
    recovery = saved(store)
    assert recovery['state'] == before
    assert recovery['predecessor_spec']['policy']['allowed_paths'] == ['README.md']
    assert effective['policy']['allowed_paths'] == ['README.md', 'contract.test.ts']
    assert effective['policy']['max_repairs'] == 2
    assert effective['accepted_plan'] == broker.spec['accepted_plan']
    assert effective['request_digest'] == broker.spec['request_digest']
    assert effective['authorized_endpoint'] == broker.spec['authorized_endpoint']
    assert recovery['session_id'] == 'original-implementation'
    assert recovery['execution_candidate']['id'] == command['expected_candidate_id']
    assert recovery['maximum_iteration'] == 2
    assert store.submitted_spec('run-1') == original
    assert resume.readback(store, effective, recovery) == {'state': 'confirmed'}
    assert store.detail('run-1')['scope_amendment']['added_paths'] == ['contract.test.ts']
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM delivery_runs').fetchone()[0] == 1
        assert not db.execute('SELECT 1 FROM delivery_repair_grants').fetchone()


@pytest.mark.parametrize('drift', ['hash', 'checks', 'budget', 'model', 'missing',
                                  'changed', 'already-allowed', 'wildcard', 'symlink',
                                  'non-finding', 'session', 'oversized', 'partial-fields',
                                  'hash-type'])
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


def test_scope_config_drift_after_admission_is_rejected_at_readback(scope_stop):
    from pathlib import Path

    store, _, _, command = scope_stop
    store.continue_repair('run-1', command)
    recovery = saved(store)
    effective = store.effective_spec('run-1')
    path = Path(command['amended_config_path'])
    config = json.loads(path.read_text())
    config['repositories']['fixture']['allowed_paths'].append('unapproved.ts')
    path.write_text(json.dumps(config))
    with pytest.raises(ValueError):
        resume.readback(store, effective, recovery)


def test_scope_preparation_drift_cannot_admit_work(scope_stop, monkeypatch):
    from pathlib import Path

    store, broker, _, command = scope_stop
    def prepare(spec, *_):
        assert spec['policy']['allowed_paths'] == ['README.md', 'contract.test.ts']
        Path(command['amended_config_path']).write_text('{}')
        return spec
    monkeypatch.setattr(resume, 'prepare_runtime', prepare)
    with pytest.raises(ValueError):
        store.continue_repair('run-1', command)
    with store._connect() as db:
        assert not db.execute('SELECT 1 FROM delivery_commands WHERE command_id=?',
                             (command['command_id'],)).fetchone()
        assert store.state.claim_for(db, broker.spec['work_id']) is None


def test_scope_resume_dispatches_original_session_to_all_normal_iterations(scope_stop, monkeypatch):
    import asyncio

    from devflow_temporal.delivery_workflow import DeliveryWorkflow

    store, _, _, command = scope_stop
    store.continue_repair('run-1', command)
    spec, recovery = store.effective_spec('run-1'), saved(store)
    flow = DeliveryWorkflow()
    async def confirmed(*_):
        return True
    async def activity(name, body, **_):
        assert name == 'delivery_tracker_start'
        assert body['spec'] == spec
        return {'state': 'consistent'}
    async def project(*_):
        pass
    async def iterations(actual_spec, **body):
        assert actual_spec == spec
        assert body['prior_implementer_session'] == 'original-implementation'
        assert body['start_iteration'] == 2
        assert body['authorized_max_iteration'] == 2
        assert not body.get('verify_only', False)
        assert body['recovery'] is None
        assert body['repair_findings'][1] == recovery['state']['roles'][0]['findings'][0]
        return {'outcome': 'normal-iterations'}
    monkeypatch.setattr(flow, '_confirm_repair_preflight', confirmed)
    monkeypatch.setattr(flow, '_activity', activity)
    monkeypatch.setattr(flow, '_project', project)
    monkeypatch.setattr(flow, '_run_iterations', iterations)
    assert asyncio.run(flow._resume_stopped(spec, recovery)) == {'outcome': 'normal-iterations'}
