"""Frozen readiness declaration and stale-observation controls."""

import json
import subprocess
from pathlib import Path

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_readiness import (
    ReadinessError,
    binding,
    derive_declaration,
    describe_requirements,
    validate_declaration,
    verify_receipt,
)


@pytest.fixture
def owned_repo(tmp_path):
    root = tmp_path / 'candidate'
    root.mkdir()
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Fixture'], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'fixture@example.invalid'],
                   check=True)
    (root / 'uv.lock').write_text('version = 1\n')
    subprocess.run(['git', '-C', str(root), 'add', 'uv.lock'], check=True)
    subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
    policy = {'readiness': {'version': 1, 'generation': 0, 'browser_engines': [],
                            'temporal_checks': []}, 'prepublish_checks': [], 'checks': []}
    spec = {'checkout': str(root), 'policy': policy, 'accepted_plan': ''}
    return root, spec


def test_explicit_requirements_preserve_frozen_browser_and_test_recipes():
    browser = {'argv': ['corepack', 'pnpm', 'playwright', 'test']}
    check = {'id': 'temporal', 'kind': 'test', 'argv': ['pytest']}
    declaration = {'version': 1, 'generation': 2, 'browser_engines': ['chromium'],
                   'temporal_checks': ['temporal'],
                   'temporal_provenance': {'check_id': 'temporal',
                       'python_project': 'workers/automation',
                       'provenance_path': '/private/tmp/provenance.json',
                       'provenance_sha256': 'a' * 64}}
    assert (validate_declaration(declaration, {'browser_qa': browser, 'checks': [check]})
            == declaration)
    for invalid in ({**declaration, 'browser_engines': []},
                    {**declaration, 'temporal_checks': ['foreign']},
                    {**declaration, 'browser_engines': [{}]},
                    {**declaration, 'generation': True},
                    {**declaration, 'extra': 'command'}):
        with pytest.raises(ReadinessError):
            validate_declaration(invalid, {'browser_qa': browser, 'checks': [check]})


def test_binding_rejects_candidate_plan_policy_and_resource_drift(owned_repo):
    root, spec = owned_repo
    candidate = candidate_for(root)
    requirements = describe_requirements(spec, root, accepted_plan='')
    first = binding(spec, candidate, requirements, resource_generation=0)
    assert first['metadata']['uv.lock']
    assert binding(spec, candidate, requirements, resource_generation=1) != first
    with pytest.raises(ReadinessError, match='plan differs'):
        describe_requirements(spec, root, accepted_plan='different')
    spec['policy']['readiness']['generation'] = 1
    with pytest.raises(ReadinessError, match='policy differs'):
        binding(spec, candidate, requirements, resource_generation=0)
    spec['policy']['readiness']['generation'] = 0
    (root / 'uv.lock').write_text('version = 2\n')
    with pytest.raises(ReadinessError, match='candidate changed'):
        binding(spec, candidate, requirements, resource_generation=0)


def test_unverified_hash_cannot_grant_a_readiness_receipt(owned_repo):
    root, spec = owned_repo
    candidate = candidate_for(root)
    requirements = describe_requirements(spec, root)
    observed = binding(spec, candidate, requirements, resource_generation=0)
    receipt = {'version': 1, 'binding': observed,
               'observations': {'configured': {'state': 'passed', 'cleanup': 'confirmed',
                                                'evidence_sha256': digest('claimed')}}}
    from types import SimpleNamespace

    with pytest.raises(ReadinessError, match='no authenticated pass'):
        verify_receipt(SimpleNamespace(), spec, candidate, receipt, requirements)


def test_shared_missing_node_is_rechecked_and_invalidated_after_remediation(tmp_path, owned_repo):
    from types import SimpleNamespace

    from devflow_temporal.delivery_readiness import shared_missing_node

    root, spec = owned_repo
    requirements = describe_requirements(spec, root)
    tools = tmp_path / 'tools'
    tools.mkdir()
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path / 'service'))
    environment = {'PATH': str(tools)}
    first = shared_missing_node(store, requirements, candidate_for(root), environment)
    assert first['state'] == 'failed' and first['reused'] is False
    later = shared_missing_node(store, requirements, candidate_for(root), environment)
    assert later['state'] == 'failed' and later['reused'] is True
    node = tools / 'node'
    node.write_text('#!/bin/sh\nexit 0\n')
    node.chmod(0o700)
    assert shared_missing_node(store, requirements, candidate_for(root), environment) is None
    assert Path(first['evidence']).is_file()  # The old failure remains historical evidence.


def test_configured_prerequisite_failure_is_observed_before_role(owned_repo, tmp_path):
    import sys

    from devflow_temporal.delivery_broker import DeliveryBroker
    from devflow_temporal.delivery_readiness import probe_configured

    root, spec = owned_repo
    check = {'id': 'offline-install', 'argv': [sys.executable, '-c',
                                               'import sys; sys.exit(17)', '/store']}
    spec.update(provider='fake', state_dir=str(tmp_path / 'state'))
    spec['policy'].update(host_sandbox='trusted-local', prepublish_checks=[check])
    state = Path(spec['state_dir'])
    state.mkdir()

    class Broker:
        native_cleanup_confirmed = True
        _run_check_list = DeliveryBroker._run_check_list

        def __init__(self):
            self.checkout, self.spec, self.evidence_dir = root, spec, state

        def candidate(self):
            return candidate_for(root)

        def _prepare_generated_outputs(self, _checkout):
            pass

    broker = Broker()
    candidate = broker.candidate()
    result = probe_configured(broker, describe_requirements(spec, root), candidate, attempt=1)
    assert result['state'] == 'failed' and result['cleanup'] == 'confirmed'
    assert result['result']['results'][0]['exit_code'] == 17
    assert result['candidate'] == candidate == broker.candidate()


def test_temporal_requirement_without_admitted_tools_cannot_claim_readiness(owned_repo):
    from devflow_temporal.delivery_readiness import probe_temporal_preparation

    root, spec = owned_repo
    check = {'id': 'temporal-test', 'kind': 'test', 'argv': ['pytest']}
    spec['policy']['checks'] = [check]
    spec['policy']['readiness']['temporal_checks'] = ['temporal-test']
    spec['policy']['readiness']['temporal_provenance'] = {
        'check_id': 'temporal-test', 'python_project': 'workers/automation',
        'provenance_path': '/private/tmp/provenance.json', 'provenance_sha256': 'a' * 64}
    candidate = candidate_for(root)

    class Broker:
        checkout = root
        def __init__(self): self.spec = spec
        def candidate(self): return candidate_for(root)

    result = probe_temporal_preparation(Broker(), describe_requirements(spec, root), candidate,
                                        iteration=0)
    assert result['state'] == 'failed' and result['launched'] is False
    assert 'admitted tool binding' in result['reason']
    assert result['candidate'] == candidate


def test_resource_binding_rejects_replaced_owned_root(owned_repo, tmp_path):
    from devflow_temporal.delivery_readiness import resource_binding
    from devflow_temporal.delivery_resources import RunResources

    root, spec = owned_repo
    state = tmp_path / 'run-resource-fixture'
    state.mkdir()
    spec.update(state_dir=str(state), run_id=state.name)
    resources = RunResources(spec)
    staged = root.with_name('unregistered-candidate')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)
    transient = resources.scratch('readiness', 'binding')
    assert transient.is_dir()
    observed = resource_binding(spec)
    assert observed[str(root)]['kind'] == 'checkout'
    original = state / 'transient'
    saved = state / 'saved-transient'
    original.rename(saved)
    original.mkdir(mode=0o700)
    try:
        with pytest.raises(ReadinessError, match='resource identity changed'):
            resource_binding(spec)
    finally:
        original.rmdir()
        saved.rename(original)
    assert resource_binding(spec) == observed


def test_coordinator_never_grants_dispatch_from_unverified_observations(
    owned_repo, tmp_path, monkeypatch
):
    from devflow_temporal.delivery_readiness import prepare_and_probe
    from devflow_temporal.delivery_resources import RunResources

    root, spec = owned_repo
    state = tmp_path / 'run-coordinator'
    state.mkdir()
    spec.update(run_id=state.name, state_dir=str(state), provider='fake', readiness_version=1)
    resources = RunResources(spec)
    staged = root.with_name('coordinator-candidate')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)

    class Broker:
        native_cleanup_confirmed = True

        def __init__(self, store, admitted):
            self.store, self.spec = store, admitted
            self.checkout, self.evidence_dir = root, state

        def candidate(self): return candidate_for(root)
        def run_resources(self): return RunResources(spec)
        def _prepare_generated_outputs(self, _checkout): pass

    monkeypatch.setattr('devflow_temporal.delivery_broker.DeliveryBroker', Broker)
    requirements = describe_requirements(spec, root)
    candidate = candidate_for(root)
    from types import SimpleNamespace

    store = SimpleNamespace(effective_spec=lambda _run_id: spec)
    result = prepare_and_probe(store, spec, candidate, requirements, 'configured')
    assert result['observations']['configured']['state'] == 'passed'
    assert result['state'] == 'passed'
    verify_receipt(store, spec, candidate, result, requirements)
    assert Path(result['receipt']).is_file()
    assert result['binding']['resources'][str(root)]['kind'] == 'checkout'
    assert candidate_for(root) == candidate
    receipt_file = Path(result['receipt'])
    receipt_file.write_text('{}\n')
    with pytest.raises(ReadinessError, match='receipt changed'):
        verify_receipt(store, spec, candidate, result, requirements)


def test_authenticate_shared_failure_reobserves_remediation(owned_repo, tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal.delivery_readiness import authenticate_missing_node, shared_missing_node

    root, spec = owned_repo
    spec.update(run_id='fixture', provider='fake')
    spec['policy'].update(browser_qa={'argv': ['playwright', 'test']}, toolchain_roots=[])
    spec['policy']['readiness']['browser_engines'] = ['chromium']
    requirements = describe_requirements(spec, root)
    candidate = candidate_for(root)
    tools = tmp_path / 'isolated-tools'
    tools.mkdir()
    monkeypatch.setattr('devflow_temporal.delivery_sandbox._native_path', lambda _roots: str(tools))
    store = SimpleNamespace(config=SimpleNamespace(state_root=tmp_path / 'shared'),
                            effective_spec=lambda _run_id: spec)
    result = shared_missing_node(store, requirements, candidate, {'PATH': str(tools)})
    evidence = {'path': result['evidence'], 'sha256': result['evidence_sha256']}
    original = Path(evidence['path']).read_bytes()
    missing = authenticate_missing_node(store, spec, candidate, requirements, evidence)
    assert missing['state'] == 'still_missing' and missing['node'] is None
    executable = tools / 'node'
    executable.write_text('#!/bin/sh\nexit 0\n')
    executable.chmod(0o700)
    corrected = authenticate_missing_node(store, spec, candidate, requirements, evidence)
    assert corrected['state'] == 'resolved' and corrected['node']['path'] == str(executable)
    assert Path(evidence['path']).read_bytes() == original
    with pytest.raises(ReadinessError, match='evidence changed'):
        authenticate_missing_node(store, spec, candidate, requirements,
                                  {**evidence, 'sha256': '0' * 64})
    (root / 'uv.lock').write_text('candidate drift\n')
    with pytest.raises(ReadinessError, match='candidate or browser recipe changed'):
        authenticate_missing_node(store, spec, candidate, requirements, evidence)


@pytest.mark.asyncio
async def test_plan_failure_never_debits_or_dispatches_a_new_product_turn(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_feature_execution, delivery_readiness

    candidate = {'id': 'candidate'}
    spec = {'run_id': 'run-1', 'provider': 'codex', 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk-1'},
            'feature_delivery': {'owner': {'issue_id': 'issue-1'}},
            'policy': {'host_sandbox': 'trusted-local'}, 'accepted_plan': 'plan'}
    broker = SimpleNamespace(checkout=tmp_path)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    monkeypatch.setattr(delivery_activities, '_role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    monkeypatch.setattr(delivery_activities, 'get_supervisor', lambda _: object())
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'exact': True})
    readiness = {'state': 'failed', 'observations': {'browser': {'cleanup': 'confirmed'}},
                 'receipt': '/owned/receipt', 'receipt_sha256': 'a' * 64}
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_k: readiness)
    def forbidden_registry(_):
        raise AssertionError('a failed readiness observation must not debit the product turn')
    monkeypatch.setattr(delivery_feature_execution, 'registry', forbidden_registry)
    monkeypatch.setattr(delivery_activities, '_readiness_worker_assignment',
                        lambda *_: (object(), {'generation': 1}, {'worker': 'original'}))
    monkeypatch.setattr(delivery_activities, '_readiness_workflow_id', lambda *_: 'workflow-1')
    result = await delivery_activities._run_role({'spec': spec, 'role': 'implement',
                                                   'iteration': 2, 'candidate': candidate})
    assert result['status'] == 'blocked' and result['session_id'] is None
    assert result['readiness'] == readiness and result['candidate'] == candidate


def test_fresh_temporal_authority_is_separate_from_recovery_marker(tmp_path, monkeypatch):
    from devflow_temporal import delivery_temporal_prerequisites as temporal

    declaration = {'check_id': 'temporal-test', 'python_project': 'workers/automation',
                   'provenance_path': str(tmp_path / 'proof.json'),
                   'provenance_sha256': 'a' * 64}
    check = {'id': 'temporal-test', 'kind': 'test', 'argv': ['pytest']}
    policy = {'host_sandbox': 'trusted-local', 'checks': [check],
              'readiness': {'temporal_checks': ['temporal-test'],
                            'temporal_provenance': declaration}}
    spec = {'readiness_version': 1, 'provider': 'codex', 'policy': policy,
            'policy_digest': digest(policy), 'state_dir': str(tmp_path / 'runs' / 'fixture')}
    metadata = {'pyproject.toml': 'p', 'uv.lock': 'l'}
    source = {'source_facts': {'pyproject_sha256': 'p', 'uv_lock_sha256': 'l'}}
    monkeypatch.setattr(temporal, '_reference', lambda *_a, **_k: json.dumps(source).encode())
    monkeypatch.setattr(temporal, '_project', lambda *_: metadata)
    monkeypatch.setattr(temporal, '_tools', lambda _: {'pinned': True})
    monkeypatch.setattr(temporal, '_evidence', lambda _: [])
    result = temporal.fresh_authority(spec)
    assert result['declaration'] == declaration and result['check'] == check
    assert temporal.MARKER not in spec
    spec['policy']['readiness']['temporal_checks'] = ['different']
    with pytest.raises(ValueError, match='frozen admission'):
        temporal.fresh_authority(spec)


def test_gate_workspace_binding_rejects_candidate_and_owner_drift(owned_repo, tmp_path):
    from devflow_temporal.delivery_readiness import _validate_workspace, resource_binding
    from devflow_temporal.delivery_resources import RunResources

    root, spec = owned_repo
    state = tmp_path / 'run-gate-fixture'
    state.mkdir()
    spec.update(state_dir=str(state), run_id=state.name)
    resources = RunResources(spec)
    staged = root.with_name('staged-source')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)
    resources.scratch('readiness', 'binding')
    candidate = candidate_for(root)
    gate = state / 'gates' / '0' / 'verify'
    gate.parent.mkdir(parents=True, mode=0o700)
    resources.register(gate, 'gate')
    subprocess.run(['git', '-C', str(root), 'worktree', 'add', '--detach', str(gate), 'HEAD'],
                   check=True, capture_output=True)
    resources.created(gate, head=candidate['head'])
    _validate_workspace(spec, gate, candidate, 0)
    assert resource_binding(spec, workspace=gate)[str(gate)]['kind'] == 'gate'
    (gate / 'uv.lock').write_text('changed gate\n')
    with pytest.raises(ReadinessError, match='lost candidate or resource custody'):
        _validate_workspace(spec, gate, candidate, 0)
    subprocess.run(['git', '-C', str(root), 'worktree', 'remove', '--force', str(gate)], check=True)


def test_owned_python_and_node_graphs_detect_content_drift(owned_repo, tmp_path):
    import sys

    from devflow_temporal.delivery_readiness import (
        _node_environment_fingerprint,
        _python_environment_fingerprint,
    )
    from devflow_temporal.delivery_resources import RunResources

    root, spec = owned_repo
    (root / 'pyproject.toml').write_text('[project]\nname = "fixture"\nversion = "1.0"\n')
    subprocess.run(['git', '-C', str(root), 'add', 'pyproject.toml'], check=True)
    subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'project metadata'], check=True)
    check = {'id': 'install', 'argv': ['uv', 'sync'], 'generated_directories': ['.venv']}
    spec['policy'].update(host_sandbox='trusted-local', baseline_checks=[check],
                          prepublish_checks=[check])
    state = tmp_path / 'run-graph'
    state.mkdir()
    spec.update(state_dir=str(state), run_id=state.name)
    resources = RunResources(spec)
    staged = root.with_name('graph-source')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)
    env = root / '.venv'
    resources.register(env, 'generated')
    (env / 'bin').mkdir(parents=True)
    (env / 'bin/python').symlink_to(Path(sys.executable).resolve())
    (env / 'pyvenv.cfg').write_text('home = fixture\n')
    module = env / 'fixture.py'
    module.write_text('VALUE = 1\n')
    resources.created(env)
    before = _python_environment_fingerprint(spec, '.', {})
    module.write_text('VALUE = 2\n')
    changed = _python_environment_fingerprint(spec, '.', {})
    assert changed['content_sha256'] != before['content_sha256']
    modules = root / 'node_modules'
    resources.register(modules, 'generated')
    modules.mkdir()
    (modules / 'fixture.js').write_text('module.exports = 1;')
    resources.created(modules)
    before_node = _node_environment_fingerprint(spec, root)
    (modules / 'fixture.js').write_text('module.exports = 2;')
    changed_node = _node_environment_fingerprint(spec, root)
    assert changed_node['content_sha256'] != before_node['content_sha256']


@pytest.mark.asyncio
async def test_workflow_records_configured_refusal_before_intake_and_roles():
    from copy import deepcopy

    from test_delivery_baseline_remediation import ProbeWorkflow, evidence

    spec, baseline = evidence()
    spec['readiness_version'] = 1
    refusal = {'state': 'failed', 'receipt': '/owned/readiness.json',
               'receipt_sha256': 'a' * 64, 'reason': 'isolated prerequisite missing',
               'observations': {'browser': {'cleanup': 'confirmed'}}}

    class ReadinessWorkflow(ProbeWorkflow):
        async def _activity(self, name, payload):
            self.calls.append((name, deepcopy(payload)))
            if name == 'delivery_prepare':
                return {'candidate': {'id': 'fixture', 'head': 'head'}}
            if name == 'delivery_environment_readiness':
                return refusal
            raise AssertionError('failed readiness must prevent all later activities')

    workflow = ReadinessWorkflow(baseline)
    result = await workflow.run(spec)
    assert [name for name, _ in workflow.calls] == ['delivery_prepare',
                                                   'delivery_environment_readiness']
    assert result['outcome'] == 'blocked' and result['iteration'] == 0
    assert result['roles'] == [] and result['checks']['readiness'] == refusal


@pytest.mark.asyncio
async def test_feature_build_readiness_refusal_preserves_pending_iteration():
    from devflow_temporal.delivery_feature_workflow import run_build

    class Controller:
        cancel_requested = False
        def __init__(self):
            self.state = {'iteration': 1, 'candidate': {'id': 'unchanged'}, 'revision': 0,
                          'checks': {}, 'roles': [], 'cleanup': 'none'}
            self.calls = []
        def _planning_request(self, _spec): return {}
        async def _project(self, _spec, event, _message): self.calls.append(event)
        async def _activity(self, name, _payload):
            self.calls.append(name)
            return {'status': 'blocked', 'session_id': None, 'cleanup': 'confirmed',
                    'candidate': self.state['candidate'],
                    'readiness': {'state': 'failed', 'receipt_sha256': 'b' * 64}}
        async def _stop(self, _spec, reason):
            self.state.update(outcome='blocked', error=reason)
            return self.state

    controller = Controller()
    spec = {'readiness_version': 1, 'policy': {'max_repairs': 3}}
    result = await run_build(controller, spec, start_iteration=2, session='existing-session')
    assert result['outcome'] == 'blocked' and result['iteration'] == 1
    assert result['roles'] == [] and result['checks']['readiness']['pending_iteration'] == 2
    assert controller.calls == ['readiness_preparing', 'delivery_role']


def test_native_admission_requires_declared_unknown_test_requirements(native_configuration):
    config, request = native_configuration
    config.raw['repositories']['fixture'].pop('readiness')
    with pytest.raises(ReadinessError, match="declare test backend requirements"):
        config.admit(request)
    declaration = {'version': 1, 'generation': 0, 'browser_engines': [], 'temporal_checks': []}
    config.raw['repositories']['fixture']['readiness'] = declaration
    fresh = config.admit(request)
    assert fresh['readiness_version'] == 1 and fresh['policy']['readiness'] == declaration


def test_default_readiness_only_derives_known_no_backend_recipes():
    declaration = {'version': 1, 'generation': 0, 'browser_engines': [], 'temporal_checks': []}
    actual = derive_declaration({'checks': [{'id': 'lint', 'argv': ['ruff', 'check', '.']}]})
    assert actual == declaration
    with pytest.raises(ReadinessError, match='browser QA engines'):
        derive_declaration({'browser_qa': {'argv': ['playwright', 'test']}})
    with pytest.raises(ReadinessError, match='test backend requirements'):
        derive_declaration({'checks': [{'id': 'test', 'kind': 'test'}]})
    with pytest.raises(ReadinessError, match='unknown check requirements'):
        derive_declaration({'checks': [{'id': 'opaque', 'argv': ['custom-build']}]})


@pytest.mark.parametrize('failure_origin', ['tools', 'temporal'])
def test_temporal_preparation_error_is_retained_as_unknown_attempt(
    owned_repo, tmp_path, monkeypatch, failure_origin
):
    from types import SimpleNamespace

    from devflow_temporal import delivery_readiness as readiness
    from devflow_temporal.delivery_resources import RunResources

    root, spec = owned_repo
    state = tmp_path / 'temporal-observation'
    state.mkdir()
    spec.update(run_id=state.name, state_dir=str(state), provider='fake', readiness_version=1)
    check = {'id': 'temporal-test', 'kind': 'test', 'argv': ['pytest']}
    spec['policy']['checks'] = [check]
    spec['policy']['readiness'].update(
        temporal_checks=['temporal-test'],
        temporal_provenance={'check_id': 'temporal-test', 'python_project': 'worker',
                             'provenance_path': '/private/tmp/provenance.json',
                             'provenance_sha256': 'a' * 64})
    resources = RunResources(spec)
    staged = root.with_name('temporal-candidate')
    root.rename(staged)
    resources.register(root, 'checkout')
    staged.rename(root)
    resources.created(root)

    class Broker:
        def __init__(self, store, admitted):
            self.store, self.spec = store, admitted
            self.checkout, self.evidence_dir = root, state
        def candidate(self): return candidate_for(root)
        def run_resources(self): return RunResources(spec)

    monkeypatch.setattr('devflow_temporal.delivery_broker.DeliveryBroker', Broker)
    monkeypatch.setattr(readiness, 'probe_configured', lambda *_a, **_kw: {
        'state': 'passed', 'result': {'results': []}})
    def missing(*_args, **_kwargs):
        raise FileNotFoundError('fixture missing pinned server')
    monkeypatch.setattr(readiness, 'probe_tools', missing if failure_origin == 'tools'
                        else lambda *_a, **_kw: {'state': 'passed'})
    monkeypatch.setattr(readiness, 'probe_temporal_preparation', missing)
    store = SimpleNamespace(effective_spec=lambda _run_id: spec)
    requirements = describe_requirements(spec, root)
    candidate = candidate_for(root)
    first = readiness.prepare_and_probe(store, spec, candidate, requirements, 'configured')
    assert first['state'] == 'unknown'
    assert first['observations'][failure_origin]['error_type'] == 'FileNotFoundError'
    assert first['observations'][failure_origin]['cleanup'] == 'unknown'
    assert Path(first['receipt']).is_file()
    second = readiness.prepare_and_probe(store, spec, candidate, requirements, 'configured',
                                         attempt=1)
    assert second['receipt'] != first['receipt']
    assert Path(first['receipt']).is_file() and Path(second['receipt']).is_file()
    with pytest.raises(ReadinessError, match='no authenticated pass'):
        verify_receipt(store, spec, candidate, first, requirements)


@pytest.mark.asyncio
async def test_changed_worker_owner_cannot_debit_after_readiness(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_readiness, delivery_role_evidence
    from devflow_temporal.delivery_execution_registry import OwnershipConflict

    candidate = {'id': 'candidate'}
    spec = {'run_id': 'run-1', 'provider': 'codex', 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk-1'},
            'policy': {'host_sandbox': 'trusted-local'}, 'accepted_plan': 'plan'}
    broker = SimpleNamespace(checkout=tmp_path)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    monkeypatch.setattr(delivery_activities, '_role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    monkeypatch.setattr(delivery_activities, 'get_supervisor', lambda _: object())
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'exact': True})
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_k: {
        'state': 'passed', 'observations': {'plan': {'result': {}}}})
    monkeypatch.setattr(delivery_readiness, 'verify_receipt', lambda *_: None)
    monkeypatch.setattr(delivery_role_evidence, 'historical_context', lambda *_: {})
    class Registry:
        def repair(self, *_args):
            raise AssertionError('a changed owner must not allocate a repair debit')
    shared = Registry()
    observations = iter([(shared, {'generation': 1}, {'worker': 'original'}),
                         (shared, {'generation': 2}, {'worker': 'successor'})])
    monkeypatch.setattr(delivery_activities, '_readiness_worker_assignment',
                        lambda *_: next(observations))
    monkeypatch.setattr(delivery_activities, '_readiness_workflow_id', lambda *_: 'workflow-1')
    with pytest.raises(OwnershipConflict, match='assignment changed during readiness'):
        await delivery_activities._run_role({'spec': spec, 'role': 'implement',
                                             'iteration': 2, 'candidate': candidate})


def test_worker_owner_must_remain_stable_during_authentication(monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_feature_execution
    from devflow_temporal.delivery_execution_registry import OwnershipConflict

    class Registry:
        @staticmethod
        def token(row):
            return {'generation': row['generation']}

    shared = Registry()
    owner = {'state': 'active', 'store_path': '/private/tmp/owned', 'generation': 1}
    successor = {**owner, 'generation': 2}
    assignments = iter([(shared, owner, {'worker': 'original'}),
                        (shared, successor, {'worker': 'successor'})])
    monkeypatch.setattr(delivery_feature_execution, 'require_execution', lambda *_: None)
    monkeypatch.setattr(delivery_feature_execution, '_worker_assignment',
                        lambda *_: next(assignments))
    store = SimpleNamespace(config=SimpleNamespace(tracking_db='/private/tmp/owned'))
    with pytest.raises(OwnershipConflict, match='changed while authenticated'):
        delivery_activities._readiness_worker_assignment(store, {})


@pytest.mark.asyncio
async def test_supervisor_receives_owner_binding_without_changing_its_result(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_readiness, delivery_role_evidence

    spec = {'run_id': 'run-1', 'provider': 'codex', 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk-1'},
            'policy': {'host_sandbox': 'trusted-local'}, 'accepted_plan': 'plan'}
    broker = SimpleNamespace(checkout=tmp_path)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    monkeypatch.setattr(delivery_activities, '_role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'exact': True})
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_k: {
        'state': 'passed', 'observations': {'plan': {'result': {}}}})
    monkeypatch.setattr(delivery_readiness, 'verify_receipt', lambda *_: None)
    monkeypatch.setattr(delivery_role_evidence, 'historical_context', lambda *_: {})
    token, assignment = {'generation': 1}, {'worker': 'original'}
    monkeypatch.setattr(delivery_activities, '_readiness_worker_assignment',
                        lambda *_: (object(), token, assignment))
    monkeypatch.setattr(delivery_activities, '_readiness_workflow_id', lambda *_: 'workflow-1')
    saved = {'status': 'blocked', 'session_id': None, 'candidate': {'id': 'candidate'}}
    observed = {}
    class Supervisor:
        async def run(self, request):
            observed.update(request)
            return dict(saved)
    monkeypatch.setattr(delivery_activities, 'get_supervisor', lambda _: Supervisor())
    monkeypatch.setattr(delivery_activities, '_role_result', lambda *_args: _args[-1])
    result = await delivery_activities._run_role({'spec': spec, 'role': 'implement',
                                                   'iteration': 0, 'candidate': saved['candidate']})
    assert result == saved
    assert observed['readiness_worker_assignment'] == {
        'owner': token, 'assignment': assignment, 'repair_key': None,
        'workflow_id': 'workflow-1', 'workflow_run_id': None}
    assert observed['evidence_context']['readiness']['state'] == 'passed'


def test_workflow_binding_rejects_superseded_activity(monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities
    from devflow_temporal.delivery_execution_registry import OwnershipConflict

    store = SimpleNamespace(active_workflow_id=lambda _run: 'current-workflow')
    monkeypatch.setattr(delivery_activities.activity, 'in_activity', lambda: True)
    monkeypatch.setattr(delivery_activities.activity, 'info',
                        lambda: SimpleNamespace(workflow_id='old-workflow'))
    with pytest.raises(OwnershipConflict, match='superseded workflow'):
        delivery_activities._readiness_workflow_id(store, {'run_id': 'run-1'})


@pytest.mark.asyncio
async def test_final_readiness_drift_refuses_before_repair_debit(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_readiness, delivery_role_evidence

    candidate = {'id': 'candidate'}
    spec = {'run_id': 'run-1', 'provider': 'codex', 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk-1'},
            'policy': {'host_sandbox': 'trusted-local'}, 'accepted_plan': 'plan'}
    broker = SimpleNamespace(checkout=tmp_path)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    monkeypatch.setattr(delivery_activities, '_role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    monkeypatch.setattr(delivery_activities, 'get_supervisor', lambda _: object())
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'exact': True})
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_k: {
        'state': 'passed', 'observations': {'plan': {'result': {}}}})
    calls = []
    def verify(*_args):
        calls.append('verify')
        if len(calls) == 2:
            raise ReadinessError('candidate or tool drift')
    monkeypatch.setattr(delivery_readiness, 'verify_receipt', verify)
    monkeypatch.setattr(delivery_role_evidence, 'historical_context', lambda *_: {})
    class Registry:
        def repair(self, *_args, **_kwargs):
            raise AssertionError('drift must not debit a product turn')
    shared = Registry()
    monkeypatch.setattr(delivery_activities, '_readiness_worker_assignment',
                        lambda *_: (shared, {'generation': 1}, {'worker': 'original'}))
    monkeypatch.setattr(delivery_activities, '_readiness_workflow_id', lambda *_: 'workflow-1')
    with pytest.raises(ReadinessError, match='drift'):
        await delivery_activities._run_role({'spec': spec, 'role': 'implement',
                                             'iteration': 2, 'candidate': candidate})
    assert calls == ['verify', 'verify']


def test_gate_readiness_failure_has_explicit_unlaunched_preparation_result(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_readiness

    candidate = {'id': 'candidate'}
    spec = {'readiness_version': 1, 'accepted_plan': 'plan'}
    broker = SimpleNamespace(checkout=tmp_path, store=object(), candidate=lambda: candidate,
                             gate_checkout=lambda *_: tmp_path)
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'recipe': 'fixed'})
    receipt = {'state': 'failed', 'observations': {'browser': {'cleanup': 'confirmed'}},
               'receipt': '/owned/receipt', 'receipt_sha256': 'a' * 64}
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_kw: receipt)
    result = delivery_activities._check_readiness(
        broker, {'spec': spec, 'candidate': candidate, 'iteration': 1}, 101)
    assert result['failure_kind'] == 'readiness_preparation' and result['launched'] is False
    assert result['readiness'] == receipt and result['cleanup'] == 'confirmed'


@pytest.mark.asyncio
@pytest.mark.parametrize('case', ['unpaid', 'premature_marker', 'malformed_marker',
                                  'null_marker', 'paid', 'delegated', 'initial',
                                  'initial_bad_marker'])
async def test_feature_repair_requires_paid_key_or_accounting_capability(
    tmp_path, monkeypatch, case
):
    from types import SimpleNamespace

    from devflow_temporal import delivery_activities, delivery_readiness, delivery_role_evidence
    from devflow_temporal.delivery_execution_registry import ExecutionRegistry, OwnershipConflict

    shared = ExecutionRegistry(tmp_path / 'registry' / 'execution.sqlite3')
    snapshot = {'issue': {'id': 'issue', 'url': 'https://github.com/o/r/issues/1',
                          'repository_id': 'repo'}, 'body': 'Feature', 'plan': None}
    token = shared.claim(snapshot, 'coordinator', '/owned/tracking.sqlite3')
    shared.reserve_worker(token, 'worker-1', 'workstream-1')
    with shared.connect() as db:
        assignment = dict(db.execute('SELECT * FROM execution_workers').fetchone())
    if case == 'paid':
        shared.repair(token, 'worker-1:1', 'Product repair at worker iteration 1',
                      worker_assignment=assignment)
    spec = {'run_id': 'worker-1', 'provider': 'codex', 'readiness_version': 1,
            'feature_worker': {'chunk_id': 'chunk'},
            'policy': {'host_sandbox': 'trusted-local'}, 'accepted_plan': 'plan'}
    if case in {'premature_marker', 'delegated', 'initial'}:
        spec['feature_repair_reservation_version'] = 1
    if case in {'malformed_marker', 'null_marker', 'initial_bad_marker'}:
        spec['feature_repair_reservation_version'] = True if case != 'null_marker' else None
    broker = SimpleNamespace(checkout=tmp_path)
    monkeypatch.setattr(delivery_activities, '_context', lambda _: (object(), broker))
    monkeypatch.setattr(delivery_activities, '_role_context',
                        lambda request, *_: (request, tmp_path, None, None))
    monkeypatch.setattr(delivery_readiness, 'describe_requirements', lambda *_: {'exact': True})
    monkeypatch.setattr(delivery_readiness, 'prepare_and_probe', lambda *_a, **_k: {
        'state': 'passed', 'observations': {'plan': {'result': {}}}})
    monkeypatch.setattr(delivery_readiness, 'verify_receipt', lambda *_: None)
    monkeypatch.setattr(delivery_role_evidence, 'historical_context', lambda *_: {})
    monkeypatch.setattr(delivery_activities, '_readiness_worker_assignment',
                        lambda *_: (shared, token, assignment))
    monkeypatch.setattr(delivery_activities, '_readiness_workflow_id', lambda *_: 'workflow-1')
    calls = []
    class Supervisor:
        async def run(self, request):
            calls.append(('run', request))
            return {'status': 'blocked'}
    supervisor = Supervisor()
    if case == 'delegated':
        def require(request, retained):
            calls.append(('require', request, retained))
        supervisor.require_feature_repair_reservation = require
    monkeypatch.setattr(delivery_activities, 'get_supervisor', lambda _: supervisor)
    monkeypatch.setattr(delivery_activities, '_role_result', lambda *_args: _args[-1])
    request = {'spec': spec, 'role': 'implement',
               'iteration': 0 if case in {'initial', 'initial_bad_marker'} else 1,
               'candidate': {'id': 'candidate'}}
    if case == 'unpaid':
        with pytest.raises(OwnershipConflict, match='no original repair debit'):
            await delivery_activities._run_role(request)
    elif case == 'premature_marker':
        with pytest.raises(ValueError, match='integration is unavailable'):
            await delivery_activities._run_role(request)
    elif case in {'malformed_marker', 'null_marker', 'initial_bad_marker'}:
        with pytest.raises(ValueError, match='unsupported feature repair reservation'):
            await delivery_activities._run_role(request)
    else:
        assert (await delivery_activities._run_role(request))['status'] == 'blocked'
    assert shared.budget(token['issue_id'])['used'] == (1 if case == 'paid' else 0)
    assert [item[0] for item in calls] == (
        ['run'] if case in {'paid', 'initial'}
        else ['require', 'run'] if case == 'delegated' else [])
    if case == 'delegated':
        assert calls[0][1]['readiness_worker_assignment']['repair_key'] == 'worker-1:1'


@pytest.mark.parametrize(('domains', 'allowed'), [
    (['registry.npmjs.org'], True),
    (['example.org', 'registry.npmjs.org'], True),
    ([], False),
    (['registry.npmjs.org.attacker.example'], False),
    (['attacker.example/registry.npmjs.org'], False),
    ('https://registry.npmjs.org.attacker.example', False),
    ('registry.npmjs.org', False),
    ({'registry.npmjs.org': True}, False),
    (None, False),
])
def test_native_fetch_requires_exact_domain_list(domains, allowed):
    from devflow_temporal.delivery_readiness import _native_fetch_allowed

    spec = {'policy': {'host_sandbox': 'native-profile'}}
    check = {'argv': ['corepack', 'pnpm', 'install', '/store'], 'network_domains': domains}
    assert _native_fetch_allowed(spec, {'configured': [check]}) is allowed
    assert _native_fetch_allowed(spec, {'configured': [{'argv': ['git', 'status']}]})
    assert _native_fetch_allowed({'policy': {'host_sandbox': 'trusted-local'}},
                                 {'configured': [check]})


def test_native_fetch_requires_every_store_prerequisite_to_admit_registry():
    from devflow_temporal.delivery_readiness import _native_fetch_allowed

    spec = {'policy': {'host_sandbox': 'native-profile'}}
    checks = [{'argv': ['pnpm', '/store'], 'network_domains': ['registry.npmjs.org']},
              {'argv': ['pnpm', '/store'], 'network_domains': ['example.org']}]
    assert not _native_fetch_allowed(spec, {'configured': checks})
