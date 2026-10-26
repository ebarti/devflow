"""Real native process and private browser environment; controlled Playwright package."""
import shutil
import subprocess
from pathlib import Path

import pytest

from devflow_temporal.candidate import candidate_for
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_readiness import describe_requirements, probe_browser
from devflow_temporal.delivery_resources import RunResources


@pytest.mark.skipif(shutil.which('node') is None, reason='fixture needs Node')
@pytest.mark.parametrize('missing', [True, False])
@pytest.mark.parametrize('bundled', [True, False])
def test_browser_probe_uses_native_qa_environment(tmp_path, monkeypatch, missing, bundled):
    node = Path(shutil.which('node')).resolve(strict=True)
    root = tmp_path / 'candidate'
    root.mkdir()
    subprocess.run(['git', 'init', '-q', str(root)], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.name', 'Fixture'], check=True)
    subprocess.run(['git', '-C', str(root), 'config', 'user.email', 'fixture@example.invalid'],
                   check=True)
    (root / '.gitignore').write_text('node_modules/\n')
    subprocess.run(['git', '-C', str(root), 'add', '.gitignore'], check=True)
    subprocess.run(['git', '-C', str(root), 'commit', '-qm', 'fixture'], check=True)
    package = root / 'node_modules' / '@playwright' / 'test'
    package.mkdir(parents=True)
    launch = ("throw Error('controlled missing isolated browser')" if missing else
              "if(options.executablePath!='/usr/bin/true')throw Error('unbound executable');"
              "return {close:async()=>{}}")
    (package / 'index.js').write_text(
        "module.exports={chromium:{executablePath:()=>'/usr/bin/true',"
        "launch:async(options)=>{" + launch + "}}};\n"
    )
    playwright = root / 'node_modules' / 'playwright'
    playwright.mkdir()
    (playwright / 'index.js').write_text('module.exports = {}')
    core = root / 'node_modules' / 'playwright-core'
    registry = core / 'lib/server/registry'
    registry.mkdir(parents=True)
    (core / 'index.js').write_text('module.exports = {}')
    implementation = "{findExecutable:()=>({executablePath:()=>'/usr/bin/true'})}"
    if bundled:
        (core / 'package.json').write_text(
            '{"exports":{".":"./index.js","./lib/coreBundle":"./lib/coreBundle.js"}}')
        (core / 'lib/coreBundle.js').write_text(
            'module.exports={registry:{registry:' + implementation + '}}')
    else:
        (registry / 'index.js').write_text('module.exports={registry:' + implementation + '}')
    state = tmp_path / 'service' / 'runs' / 'fixture-run'
    state.mkdir(parents=True, mode=0o700)
    qa = {'argv': ['node', 'playwright', 'test'], 'cwd': '.',
          'ports': {'QA_API_PORT': 14001, 'QA_WEB_PORT': 14002},
          'env': {'JOBCTRL_E2E_ISOLATED': '1'}, 'timeout_seconds': 30}
    policy = {'host_sandbox': 'trusted-local', 'execution_backend': 'native-macos',
              'codex_bin': '/usr/bin/true', 'toolchain_roots': [str(node.parent.parent)],
              'checks': [], 'prepublish_checks': [], 'browser_qa': qa,
              'readiness': {'version': 1, 'generation': 0, 'browser_engines': ['chromium'],
                            'temporal_checks': []}}
    spec = {'run_id': 'fixture-run', 'state_dir': str(state), 'checkout': str(root),
            'provider': 'codex', 'accepted_plan': '', 'policy': policy,
            'policy_digest': digest(policy)}
    candidate = candidate_for(root)
    requirements = describe_requirements(spec, root)
    monkeypatch.setattr('devflow_temporal.delivery_preparation.verify_prepared_spec',
                        lambda _: None)
    # Controlled package proof; the browser command and native process are real.
    monkeypatch.setattr('devflow_temporal.delivery_readiness_pnpm.authenticate_playwright',
                        lambda *_: {'packages': []})


    class Broker:
        checkout = root
        def __init__(self):
            from types import SimpleNamespace
            self.spec = spec
            self.store = SimpleNamespace(config=SimpleNamespace(state_root=state.parent.parent))
        def candidate(self): return candidate_for(root)
        def _native_cancelled(self): return False
        def _run_native_check(self, process): return process.run()

    try:
        result = probe_browser(Broker(), requirements, candidate, iteration=0, attempt=1)
        assert result['cleanup'] == 'confirmed'
        if missing:
            assert result['state'] == 'failed' and result['native_process']['exit_code'] != 0
            log = Path(result['native_process']['log']).read_text()
            assert 'controlled missing isolated browser' in log
        else:
            assert result['state'] == 'passed'
            assert result['native_process']['exit_code'] == 0
            assert result['binaries']['chromium']['path'] == str(Path('/usr/bin/true').resolve())
        assert candidate_for(root) == candidate
    finally:
        assert RunResources(spec).finalize('blocked')['state'] == 'confirmed'
