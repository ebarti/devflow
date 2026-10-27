"""Exercise the hosted Mac runtime scheduler without running its costly suites."""

from __future__ import annotations

import json
import os
import shlex
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
            '      - name: Run assigned runtime modules and host 0 Mac source qualification\n', 1
        )[1].split('        working-directory: runtime\n', 1)[0]
        script = block.split('        run: |\n', 1)[1]
        script = '\n'.join(line[10:] for line in script.splitlines())
        self.assertIn('shard: [0, 1]', workflow)
        self.assertIn('--verify runtime/ci-results --count 4', workflow)
        with tempfile.TemporaryDirectory(prefix='devflow-ci-topology-') as directory:
            root = Path(directory)
            stub = root / 'stub.py'
            stub.write_text('''\
import fcntl, json, os, sys, time
path, key = os.environ['STUB_LOG'], sys.argv[1]
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
                'env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \\\n    sh ../scripts/check-install.sh': f'{runner} source-install',
                'env GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 \\\n      python3.12 -B -m unittest discover -s tests -p \'test_reconcile.py\' -v':
                    f'{runner} source-reconcile',
                '.venv/bin/python runtime_ci.py --index "$index" --count 4 \\\n    --basetemp "$RUNNER_TEMP/devflow-pytest-$index"': f'{runner} "$index"',
            }
            for original, replacement in substitutions.items():
                self.assertIn(original, script)
                script = script.replace(original, replacement)
            all_indices = []
            for host in range(2):
                log = root / f'{host}.jsonl'
                failures = [None, str(host), str(host + 2)]
                if host == 0:
                    failures.extend(['source-install', 'source-reconcile'])
                for failed_index in failures:
                    log.unlink(missing_ok=True)
                    env = {**os.environ, 'STUB_LOG': str(log), 'RUNNER_TEMP': str(root)}
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
                    if host == 0:
                        self.assertEqual(set(starts), {'source-install', 'source-reconcile', '0', '2'})
                        last_source = max(i for i, (_, key) in enumerate(events)
                                          if key.startswith('source-'))
                        first_partition = min(i for i, (_, key) in enumerate(events) if key.isdigit())
                        self.assertLess(last_source, first_partition)
                    if failed_index is None:
                        all_indices.extend(indices)
            self.assertEqual(sorted(all_indices), list(range(4)))


if __name__ == '__main__':
    unittest.main()
