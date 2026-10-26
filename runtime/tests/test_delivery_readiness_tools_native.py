"""Owned native tool observations; no provider or product role is launched."""
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_readiness import describe_requirements, probe_tools
from devflow_temporal.delivery_readiness_verify import tools as verify_tools
from devflow_temporal.delivery_resources import RunResources


@pytest.mark.skipif(shutil.which('node') is None, reason='fixture needs Node')
@pytest.mark.parametrize('unsupported', [False, True])
def test_check_tool_observation_uses_owned_native_profile(tmp_path, monkeypatch, unsupported):
    node = Path(shutil.which('node')).resolve(strict=True)
    root = tmp_path / 'candidate'
    root.mkdir()
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Fixture'], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'fixture@example.invalid'],
                   check=True)
    (root / 'README').write_text('fixture\n')
    subprocess.run(['git', '-C', str(root), 'add', 'README'], check=True)
    subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
    state = tmp_path / 'service' / 'runs' / 'fixture'
    state.mkdir(parents=True, mode=0o700)
    check = {'id': 'test-tool', 'argv': ['sh', '-c', 'echo fixture'] if unsupported
             else ['node', '--version']}
    policy = {'host_sandbox': 'trusted-local', 'execution_backend': 'native-macos',
              'codex_bin': '/usr/bin/true', 'toolchain_roots': [str(node.parent.parent)],
              'checks': [check], 'prepublish_checks': [],
              'readiness': {'version': 1, 'generation': 0, 'browser_engines': [],
                            'temporal_checks': []}}
    spec = {'run_id': 'fixture', 'state_dir': str(state), 'checkout': str(root),
            'provider': 'codex', 'accepted_plan': '', 'policy': policy,
            'policy_digest': digest(policy)}
    candidate = candidate_for(root)
    requirements = describe_requirements(spec, root)
    class Broker:
        checkout, evidence_dir = root, state
        def __init__(self, *_): self.spec = spec
        def candidate(self): return candidate_for(root)
        def run_resources(self): return RunResources(spec)
        def _native_cancelled(self): return False
        def _run_native_check(self, process): return process.run()
    broker = Broker()
    try:
        result = probe_tools(broker, requirements, candidate, iteration=0, attempt=1)
        if unsupported:
            assert result['state'] == 'unknown' and result['items'] == []
        else:
            assert result['state'] == 'passed'
            assert result['items'][0]['native_process']['exit_code'] == 0
            monkeypatch.setattr('devflow_temporal.delivery_broker.DeliveryBroker', Broker)
            receipt = {'stage': 'configured', 'workspace': str(root), 'iteration': 0, 'attempt': 1,
                       'observations': {'tools': result}}
            verify_tools(SimpleNamespace(), spec, candidate, requirements, receipt)
    finally:
        assert RunResources(spec).finalize('blocked')['state'] == 'confirmed'
