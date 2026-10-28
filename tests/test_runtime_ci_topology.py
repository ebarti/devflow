"""Exercise the hosted Mac runtime scheduler without running its costly suites."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class RuntimeCITopologyTests(unittest.TestCase):
    def test_location_fixture_uses_verified_managed_python(self):
        workflow = (ROOT / '.github/workflows/check.yml').read_text()
        block = workflow.split('      - name: Prepare exact Python for native location fixtures\n', 1)[1]
        block = block.split('      - name: Prepare disposable native SDK and official tool fixtures', 1)[0]
        script = '\n'.join(line[10:] for line in block.split('        run: |\n', 1)[1].splitlines())
        with tempfile.TemporaryDirectory(prefix='devflow-ci-python-') as directory:
            root = Path(directory)
            uv, python, env_file, log = (root / name for name in ('uv', 'fixture-python', 'env', 'log'))
            uv.write_text("""#!/bin/bash
printf '%s\\n' \"$*\" >> \"$STUB_LOG\"
if [ \"$1 $2\" = 'python find' ]; then printf '%s\\n' \"$STUB_PYTHON\"; fi
""")
            python.write_text("""#!/bin/bash
[ \"$1\" = '-c' ] || exit 1
[ \"$2\" = 'import sys; assert sys.version_info[:3] == (3, 12, 13)' ] || exit 1
[ \"${STUB_BAD_VERSION:-0}\" = 0 ] || exit 1
""")
            uv.chmod(0o755)
            python.chmod(0o755)
            env = {**os.environ, 'PATH': str(root) + os.pathsep + os.environ['PATH'],
                   'STUB_LOG': str(log), 'STUB_PYTHON': str(python), 'GITHUB_ENV': str(env_file)}
            result = subprocess.run(['/bin/bash', '-e', '-c', script], env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(log.read_text().splitlines(), [
                'python install --managed-python 3.12.13',
                'python find --managed-python --no-python-downloads 3.12.13',
            ])
            self.assertEqual(env_file.read_text(),
                             f'DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON={python}\n')
            env_file.unlink()
            env['STUB_BAD_VERSION'] = '1'
            result = subprocess.run(['/bin/bash', '-e', '-c', script], env=env,
                                    capture_output=True, text=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(env_file.exists())

    def test_two_parents_cover_all_partitions_and_propagate_failure(self):
        workflow = (ROOT / '.github/workflows/check.yml').read_text()
        block = workflow.split(
            '      - name: Run assigned Mac source qualification then runtime modules\n', 1
        )[1].split('        working-directory: runtime\n', 1)[0]
        script = block.split('        run: |\n', 1)[1]
        script = '\n'.join(line[10:] for line in script.splitlines())
        self.assertIn('shard: [0, 1]', workflow)
        self.assertIn('--verify runtime/ci-results --count 4', workflow)
        with tempfile.TemporaryDirectory(prefix='devflow-ci-topology-') as directory:
            root = Path(directory)
            home = root / 'home'
            home.mkdir()
            stub = root / 'stub.py'
            stub.write_text('''\
import fcntl, json, os, sys, time
from pathlib import Path
path, key = os.environ['STUB_LOG'], sys.argv[1]
if key.startswith('source-'):
    fixture = Path(os.environ['DEVFLOW_PRODUCTION_FIXTURE_PARENT'])
    assert fixture.is_dir() and fixture.parent == Path(os.environ['HOME'])
    if os.environ.get('STUB_FAIL') == 'cleanup':
        (fixture / 'retained').touch()
    if key == 'source-install':
        assert os.environ['DEVFLOW_PYTHON'] == os.environ['STUB_SOURCE_PYTHON']
    else:
        assert os.environ['DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON'] == os.environ['STUB_SOURCE_PYTHON']
else:
    assert os.environ['DEVFLOW_PYTHON'] == os.environ['STUB_NATIVE_PYTHON']
    assert os.environ['DEVFLOW_PRODUCTION_FIXTURE_PARENT'] == 'original-native-fixture-parent'
    assert len(sys.argv) == 3
    base = Path(sys.argv[2])
    assert base.parent == Path(os.environ['RUNNER_TEMP'])
    assert base.name == f'devflow-pytest-{key}.noindex'
    with open(os.environ['STUB_ROOT_LOG'], 'a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps([key, str(base)]) + '\\n')
for event in ('start', 'end'):
    if event == 'end':
        time.sleep(.03)
    with open(path, 'a') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        stream.write(json.dumps([event, key]) + '\\n')
        stream.flush()
if key == os.environ.get('STUB_FAIL'):
    sys.exit(7)
''')
            runner = f'{shlex.quote(sys.executable)} {shlex.quote(str(stub))}'
            substitutions = {
                'env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \\\n    DEVFLOW_PRODUCTION_FIXTURE_PARENT="$fixture_parent" \\\n    DEVFLOW_PYTHON="$DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON" sh ../scripts/check-install.sh':
                    f'env DEVFLOW_PRODUCTION_FIXTURE_PARENT="$fixture_parent" '
                    f'DEVFLOW_PYTHON="$DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON" {runner} source-install',
                'env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \\\n      DEVFLOW_PRODUCTION_FIXTURE_PARENT="$fixture_parent" \\\n      "$DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON" -B -m unittest discover -s tests -p \'test_reconcile.py\' -v':
                    f'env DEVFLOW_PRODUCTION_FIXTURE_PARENT="$fixture_parent" {runner} source-reconcile',
                '.venv/bin/python runtime_ci.py --index "$index" --count 4 \\\n    --basetemp "$RUNNER_TEMP/devflow-pytest-$index.noindex"':
                    f'{runner} "$index" "$RUNNER_TEMP/devflow-pytest-$index.noindex"',
            }
            for original, replacement in substitutions.items():
                self.assertIn(original, script)
                script = script.replace(original, replacement)
            all_indices, all_sources, all_roots = [], [], []
            for host in range(2):
                log = root / f'{host}.jsonl'
                root_log = root / f'{host}-roots.jsonl'
                source = 'source-reconcile' if host == 0 else 'source-install'
                failures = [None, str(host), str(host + 2), source, 'cleanup']
                for failed_index in failures:
                    log.unlink(missing_ok=True)
                    root_log.unlink(missing_ok=True)
                    env = {**os.environ, 'STUB_LOG': str(log), 'STUB_ROOT_LOG': str(root_log),
                           'RUNNER_TEMP': str(root),
                           'HOME': str(home),
                           'DEVFLOW_TEST_RUNTIME_LOCATION_PYTHON': sys.executable,
                           'STUB_SOURCE_PYTHON': sys.executable,
                           'DEVFLOW_PYTHON': 'original-native-interpreter',
                           'STUB_NATIVE_PYTHON': 'original-native-interpreter',
                           'DEVFLOW_PRODUCTION_FIXTURE_PARENT': 'original-native-fixture-parent'}
                    env.pop('STUB_FAIL', None)
                    if failed_index is not None:
                        env['STUB_FAIL'] = failed_index
                    result = subprocess.run(
                        ['/bin/bash', '-c', script.replace('${{ matrix.shard }}', str(host))],
                        cwd=ROOT / 'runtime', env=env, capture_output=True, text=True, timeout=10,
                    )
                    self.assertEqual(result.returncode, 0 if failed_index is None else 1,
                                     result.stdout + result.stderr)
                    events = [json.loads(line) for line in log.read_text().splitlines()]
                    active = peak = 0
                    starts = []
                    for event, key in events:
                        active += 1 if event == 'start' else -1
                        peak = max(peak, active)
                        if event == 'start':
                            starts.append(key)
                    self.assertEqual(active, 0)
                    self.assertLessEqual(peak, 2)
                    indices = sorted(int(key) for key in starts if key.isdigit())
                    self.assertEqual(indices, [host, host + 2])
                    roots = [json.loads(line) for line in root_log.read_text().splitlines()]
                    self.assertEqual(sorted(roots), [[str(i), str(root / f'devflow-pytest-{i}.noindex')]
                                                     for i in [host, host + 2]])
                    self.assertEqual(set(starts), {source, str(host), str(host + 2)})
                    self.assertEqual(starts.count(source), 1)
                    last_source = max(i for i, (_, key) in enumerate(events)
                                      if key.startswith('source-'))
                    first_partition = min(i for i, (_, key) in enumerate(events) if key.isdigit())
                    self.assertLess(last_source, first_partition)
                    retained = list(home.glob('devflow-install-fixtures.*'))
                    self.assertEqual(len(retained), 1 if failed_index == 'cleanup' else 0)
                    for fixture in retained:
                        self.assertEqual([p.name for p in fixture.iterdir()], ['retained'])
                        shutil.rmtree(fixture)
                    if failed_index is None:
                        all_indices.extend(indices)
                        all_roots.extend(path for _, path in roots)
                        all_sources.extend(key for key in starts if key.startswith('source-'))
            self.assertEqual(sorted(all_indices), list(range(4)))
            self.assertEqual(sorted(all_sources), ['source-install', 'source-reconcile'])
            self.assertEqual(len(set(all_roots)), 4)


if __name__ == '__main__':
    unittest.main()
