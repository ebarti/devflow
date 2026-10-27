"""Frozen readiness declaration and stale-observation controls."""

import subprocess

import pytest
from test_delivery_intake import intake_fixture as intake_fixture
from test_delivery_native import native_configuration as native_configuration

from devflow_temporal.candidate import candidate_for
from devflow_temporal.delivery_readiness import (
    ReadinessError,
    binding,
    describe_requirements,
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
