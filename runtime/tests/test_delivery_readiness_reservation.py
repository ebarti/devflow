"""Readiness observation for a private native repair request, not a GO grant."""
import hashlib
from types import SimpleNamespace

import pytest

from devflow_temporal import delivery_readiness_reservation as readiness
from devflow_temporal.delivery_resources import write_private
from devflow_temporal.supervisor import DeliverySupervisor


def test_reservation_readiness_authenticates_exact_private_role_request(tmp_path, monkeypatch):
    spec = {'run_id': 'worker-1', 'state_dir': str(tmp_path), 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk'}, 'policy_digest': 'a' * 64}
    owner = {'issue_id': 'issue', 'run_id': 'coordinator', 'generation': 1}
    assignment = {'worker_key': 'worker-1', 'generation': 1}
    candidate = {'id': 'b' * 64}
    receipt = {'state': 'passed', 'stage': 'plan', 'iteration': 1, 'attempt': 0,
               'receipt_sha256': 'c' * 64}
    handoff = {'owner': owner, 'assignment': assignment, 'workflow_id': 'workflow',
               'workflow_run_id': 'temporal-run', 'repair_key': 'worker-1:1'}
    request = {'spec': spec, 'role': 'implement', 'iteration': 1, 'candidate': candidate,
               'readiness_worker_assignment': handoff,
               'evidence_context': {'readiness': receipt}, 'native_authorized': True}
    job = DeliverySupervisor._job_key(request)
    folder = tmp_path / 'attempts' / job
    request.update(start_path=str(folder / 'start.json'), result_path=str(folder / 'result.json'))
    write_private(folder / 'request.json', request)
    binding = {'job_id': job, 'candidate': candidate, 'iteration': 1, 'worker_run_id': 'worker-1',
               'attempt_generation': 0, 'native_attempt_generation': 0,
               'owner': owner, 'assignment': assignment, 'workflow_id': 'workflow',
               'workflow_run_id': 'temporal-run', 'readiness_digest': 'c' * 64}
    store = SimpleNamespace(effective_spec=lambda _run_id: spec)
    observed = []
    monkeypatch.setattr(readiness, 'verify_receipt', lambda *args: observed.append(args))
    pinned = (folder / 'request.json').read_bytes()
    request_sha256 = hashlib.sha256(pinned).hexdigest()
    proof = readiness.verify_reserved_readiness(
        store, spec, binding, pinned_request=pinned, request_sha256=request_sha256)
    assert proof == {'readiness_digest': 'c' * 64, 'request_sha256': request_sha256}
    assert observed == [(store, spec, candidate, receipt)]
    for key, value in [('candidate', {'id': 'other'}), ('workflow_run_id', 'other'),
                       ('attempt_generation', 1), ('attempt_generation', True),
                       ('native_attempt_generation', 1), ('native_attempt_generation', True),
                       ('readiness_digest', 'd' * 64), ('job_id', '../foreign')]:
        with pytest.raises(readiness.ReadinessError):
            readiness.verify_reserved_readiness(store, spec, {**binding, key: value},
                                                pinned_request=pinned,
                                                request_sha256=request_sha256)
    request['start_path'] = str(tmp_path / 'foreign')
    write_private(folder / 'request.json', request)
    with pytest.raises(readiness.ReadinessError, match='pinned request changed'):
        readiness.verify_reserved_readiness(store, spec, binding, pinned_request=pinned,
                                            request_sha256=request_sha256)
    changed = (folder / 'request.json').read_bytes()
    with pytest.raises(readiness.ReadinessError, match='private role request changed'):
        readiness.verify_reserved_readiness(store, spec, binding, pinned_request=changed,
                                            request_sha256=hashlib.sha256(changed).hexdigest())


def test_reservation_readiness_revalidates_real_owned_receipt(tmp_path, monkeypatch):
    import subprocess

    from devflow_temporal.candidate import candidate_for
    from devflow_temporal.contracts import digest
    from devflow_temporal.delivery_readiness import describe_requirements, prepare_and_probe
    from devflow_temporal.delivery_resources import RunResources

    root = tmp_path / 'candidate'
    root.mkdir()
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Fixture'], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'fixture@example.invalid'],
                   check=True)
    (root / 'uv.lock').write_text('version = 1\n')
    subprocess.run(['git', '-C', str(root), 'add', 'uv.lock'], check=True)
    subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
    state = tmp_path / 'worker-1'
    state.mkdir()
    policy = {'readiness': {'version': 1, 'generation': 0, 'browser_engines': [],
                            'temporal_checks': []}, 'checks': [], 'prepublish_checks': []}
    spec = {'run_id': 'worker-1', 'state_dir': str(state), 'checkout': str(root),
            'provider': 'fake', 'readiness_version': 1, 'feature_worker': {'chunk_id': 'chunk'},
            'accepted_plan': '', 'policy': policy, 'policy_digest': digest(policy)}
    resources = RunResources(spec)
    staged = root.with_name('staged-candidate')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)
    candidate = candidate_for(root)
    class Broker:
        native_cleanup_confirmed = True
        def __init__(self, store, admitted):
            self.store, self.spec = store, admitted
            self.checkout, self.evidence_dir = root, state
        def candidate(self): return candidate_for(root)
        def run_resources(self): return RunResources(spec)
        def run_implementation_preparation(self, *_args, **_kwargs):
            return {'state': 'passed', 'results': [], 'candidate_id': candidate['id'],
                    'source_unchanged': True}
    monkeypatch.setattr('devflow_temporal.delivery_broker.DeliveryBroker', Broker)
    monkeypatch.setattr('devflow_temporal.delivery_configured_resources.implementation_prerequisites',
                        lambda _spec: [])
    monkeypatch.setattr('devflow_temporal.delivery_plan_checks.planned_checks',
                        lambda *_args, **_kwargs: [])
    store = SimpleNamespace(effective_spec=lambda _run_id: spec)
    requirements = describe_requirements(spec, root, accepted_plan='')
    receipt = prepare_and_probe(store, spec, candidate, requirements, 'plan', iteration=1)
    assert receipt['state'] == 'passed'
    owner, assignment = {'generation': 1}, {'worker_key': 'worker-1'}
    request = {'spec': spec, 'role': 'implement', 'iteration': 1, 'candidate': candidate,
               'readiness_worker_assignment': {'owner': owner, 'assignment': assignment,
                   'workflow_id': 'workflow', 'workflow_run_id': 'temporal-run',
                   'repair_key': 'worker-1:1'},
               'evidence_context': {'readiness': receipt}, 'native_authorized': True}
    job = DeliverySupervisor._job_key(request)
    folder = state / 'attempts' / job
    request.update(start_path=str(folder / 'start.json'), result_path=str(folder / 'result.json'))
    write_private(folder / 'request.json', request)
    pinned = (folder / 'request.json').read_bytes()
    binding = {'job_id': job, 'candidate': candidate, 'iteration': 1, 'worker_run_id': 'worker-1',
               'attempt_generation': 0, 'native_attempt_generation': 0,
               'owner': owner, 'assignment': assignment, 'workflow_id': 'workflow',
               'workflow_run_id': 'temporal-run', 'readiness_digest': receipt['receipt_sha256']}
    proof = readiness.verify_reserved_readiness(
        store, spec, binding, pinned_request=pinned,
        request_sha256=hashlib.sha256(pinned).hexdigest())
    assert proof['readiness_digest'] == receipt['receipt_sha256']
    (root / 'uv.lock').write_text('version = 2\n')
    with pytest.raises(readiness.ReadinessError):
        readiness.verify_reserved_readiness(
            store, spec, binding, pinned_request=pinned,
            request_sha256=hashlib.sha256(pinned).hexdigest())
