"""Independent public-entry qualification of source custody in a disposable owner."""
import fcntl
import json
import os
import signal
import subprocess
import time
import unittest
from pathlib import Path

from test_production_generation import GenerationFixture


def wait_for_file(path, timeout):
    deadline = time.monotonic() + timeout
    while not path.exists() and time.monotonic() < deadline:
        time.sleep(.05)
    return path.exists()


def child_identity(pid, required):
    """Observe only this fixture's uniquely named child, including birth time."""
    result = subprocess.run(['ps', '-ww', '-p', str(pid), '-o', 'stat=', '-o', 'lstart=', '-o', 'command='],
                            capture_output=True, text=True, timeout=5, check=False)
    if result.returncode and result.stderr.strip():
        raise AssertionError('child process identity observation failed: ' + result.stderr.strip())
    if not result.stdout.strip():
        return None
    if result.returncode:
        raise AssertionError('child process identity observation was incomplete')
    fields = result.stdout.strip().split(None, 6)
    if len(fields) < 7 or fields[0].startswith('Z'):
        return None
    if not all(str(value) in fields[6] for value in required):
        raise AssertionError('child command no longer matches the owned fixture')
    return ' '.join(fields[1:6])


def stop_owned_child(pid, birth, required):
    """Never signal a child whose observed command and start identity differ."""
    for sig in (signal.SIGTERM, signal.SIGKILL):
        current = child_identity(pid, required)
        if current is None:
            return
        if current != birth:
            raise AssertionError('child start identity changed; refusing to signal it')
        try:
            os.kill(pid, sig)
        except ProcessLookupError:
            return
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            current = child_identity(pid, required)
            if current is None:
                return
            if current != birth:
                raise AssertionError('child identity changed during cleanup')
            time.sleep(.05)
    raise AssertionError('owned child did not exit after cleanup')


def release_owned_child(release, child_record, parent_record, required, *, registration_timeout=45, grace=10):
    # Launch may have succeeded even if its parent failed or its reply was lost.
    release.touch()
    deadline = time.monotonic() + registration_timeout
    while not child_record.exists() and not parent_record.exists() and time.monotonic() < deadline:
        time.sleep(.05)
    record = child_record if child_record.exists() else parent_record
    if not record.exists():
        raise AssertionError('child launch outcome is unknown: no owned PID record')
    identity = json.loads(record.read_text())
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        observed = child_identity(identity['pid'], required)
        if observed is None:
            return
        if observed != identity['birth']:
            raise AssertionError('child identity changed; refusing cleanup signal')
        time.sleep(.05)
    stop_owned_child(identity['pid'], identity['birth'], required)


def finish_direct_process(process):
    """Bound all waits and reap this direct child even on a readiness failure."""
    try:
        process.communicate('\n', timeout=5)
    except subprocess.TimeoutExpired:
        process.terminate()
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)


class SourceCustodyPublicProcesses(GenerationFixture):
    def test_source_custody_at_process_and_child_boundaries(self):
        _request, root = self.prepare_and_apply()
        allocated = self.command('--allocate-namespace', self.config, root=root)
        config = Path(allocated['config'])
        witness = json.loads(Path(allocated['witness']).read_text())
        database = Path(witness['database'])
        birth = json.loads((root.parent.parent.parent / '.production-generation-births' / (root.parent.name + '.json')).read_text())
        bootstrap = Path(birth['startup']['directory'])
        manifest = json.loads((bootstrap / 'manifest.json').read_text())
        entry = [manifest['base_python'], '-I', '-S', '-B', str(bootstrap / 'production-entry.py'), str(root), '--python']
        raw = [str(root / 'runtime/.venv/bin/python'), '-I', '-B']
        scope = ("import pathlib,sys;from devflow_temporal.delivery_config import DeliveryConfig;"
                 "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                 "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));\n"
                 "with fresh_namespace_scope(c) as s:\n s.verify();print('SCOPE_OK',flush=True)")
        def run(command, *args):
            return subprocess.run([*command, *args], env=self.env, capture_output=True, text=True, timeout=90, check=False)
        def accepted(result):
            self.assertEqual(result.returncode, 0, result.stdout + '\n' + result.stderr)
            self.assertIn('SCOPE_OK', result.stdout)
        def refused(result):
            self.assertNotEqual(result.returncode, 0, result.stdout + '\n' + result.stderr)
            self.assertNotIn('SCOPE_OK', result.stdout)
        accepted(run(entry, '-c', scope, str(config)))

        before = database.read_bytes()
        refused(run(raw, '-c', scope, str(config)))
        self.assertEqual(database.read_bytes(), before)

        # Pre-imported altered module restored before admission: a late clean-tree
        # observation cannot authenticate the live interpreter's module cache.
        source = root / 'runtime/src/devflow_temporal/delivery_config.py'
        original = source.read_bytes()
        backup = self.home / 'source-original'
        backup.write_bytes(original)
        source.write_bytes(original + b'\nSTALE_SCOPE_TEST = True\n')
        stale = ("import pathlib,sys;import devflow_temporal.delivery_config as d;"
                 "assert d.STALE_SCOPE_TEST;pathlib.Path(sys.argv[2]).write_bytes(pathlib.Path(sys.argv[3]).read_bytes());"
                 "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                 "c=d.DeliveryConfig.load(pathlib.Path(sys.argv[1]));\n"
                 "with fresh_namespace_scope(c): print('SCOPE_OK',flush=True)")
        try:
            refused(run(raw, '-c', stale, str(config), str(source), str(backup)))
        finally:
            source.write_bytes(original)
        self.assertEqual(database.read_bytes(), before)
        accepted(run(entry, '-c', scope, str(config)))

        # Persistent in-place dirty source must be refused before even --help.
        try:
            source.write_bytes(original + b'\n# dirty owned fixture\n')
            refused(run(entry, '-c', scope, str(config)))
            ordinary = run([str(root / 'runtime/.venv/bin/devflow-delivery')], '--config', str(self.config), '--help')
            self.assertEqual(ordinary.returncode, 0, ordinary.stderr)
            self.assertIn('usage:', ordinary.stdout.lower())
            namespace_console = run([str(root / 'runtime/.venv/bin/devflow-delivery')], '--config', str(config), '--help')
            self.assertNotEqual(namespace_console.returncode, 0, namespace_console.stdout + namespace_console.stderr)
            self.assertEqual(database.read_bytes(), before)
        finally:
            source.write_bytes(original)

        # Source custody covers runtime inputs, not unrelated installer files.
        installer = root / 'scripts/install-agents.py'
        installer_before = installer.read_bytes()
        try:
            installer.write_bytes(installer_before + b'\n# unrelated installer fixture\n')
            accepted(run(entry, '-c', scope, str(config)))
            console = run([str(root / 'runtime/.venv/bin/devflow-delivery')], '--config', str(config), '--help')
            self.assertEqual(console.returncode, 0, console.stdout + console.stderr)
            self.assertIn('usage:', console.stdout.lower())
            self.assertEqual(database.read_bytes(), before)
        finally:
            installer.write_bytes(installer_before)

        # Produce a timestamp-valid evil cache, then restore exact tracked source.
        # Normal -B may still read it, so a qualified interpreter must not.
        marker = self.home / 'stale-bytecode-executed'
        stamp = source.stat()
        evil = f"from pathlib import Path\nPath({str(marker)!r}).write_text('executed')\n".encode()
        self.assertLess(len(evil), len(original))
        try:
            source.write_bytes(evil + b' ' * (len(original) - len(evil)))
            os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            built = run(raw, '-c', 'import py_compile,sys;py_compile.compile(sys.argv[1],doraise=True)', str(source))
            self.assertEqual(built.returncode, 0, built.stderr)
            source.write_bytes(original)
            os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            accepted(run(entry, '-c', scope, str(config)))
            self.assertFalse(marker.exists(), 'qualified startup executed stale __pycache__')
        finally:
            source.write_bytes(original)
            os.utime(source, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))

        # Site customizations run before -c even in isolated mode. A changed
        # .pth must be rejected by the outer controller before it can execute.
        sites = list((root / 'runtime/.venv/lib').glob('python*/site-packages'))
        self.assertEqual(len(sites), 1)
        pth = sites[0] / 'custody-fixture.pth'
        pth_marker = self.home / 'pth-executed'
        pth.write_text(f"import pathlib;pathlib.Path({str(pth_marker)!r}).write_text('executed')\n")
        try:
            refused(run(entry, '-c', scope, str(config)))
            self.assertFalse(pth_marker.exists(), 'unverified .pth executed before source bootstrap')
        finally:
            pth.unlink()
        accepted(run(entry, '-c', scope, str(config)))

        # Legacy sourceless bytecode can be imported outside __pycache__ even
        # with pycache_prefix; do not allow unmeasured package-root imports.
        rogue = root / 'runtime/src/devflow_temporal/custody_fixture.pyc'
        build_source = self.home / 'custody-fixture.py'
        build_source.write_text('VALUE = 1\n')
        built = run(raw, '-c', 'import py_compile,sys;py_compile.compile(sys.argv[1],cfile=sys.argv[2],doraise=True)', str(build_source), str(rogue))
        self.assertEqual(built.returncode, 0, built.stderr)
        try:
            refused(run(entry, '-c', scope, str(config)))
        finally:
            rogue.unlink()
        accepted(run(entry, '-c', scope, str(config)))

        # A valid parent witness must not be accepted by a new interpreter.
        child = ("import os,subprocess,sys;"
                 "fds=tuple(int(os.environ[k]) for k in ('DEVFLOW_SOURCE_START_FD','DEVFLOW_INSTALLATION_LOCK_FD'));"
                 "p=subprocess.run([sys.executable,'-I','-B','-X','pycache_prefix=/dev/null','-c',sys.argv[1],sys.argv[2]],"
                 "env=os.environ.copy(),pass_fds=fds,capture_output=True,text=True);"
                 "print('CHILD',p.returncode,p.stdout,p.stderr);"
                 "sys.exit(0 if p.returncode!=0 and 'SCOPE_OK' not in p.stdout else 1)")
        child_result = run(entry, '-c', child, scope, str(config))
        self.assertEqual(child_result.returncode, 0, child_result.stdout + child_result.stderr)
        self.assertIn('CHILD', child_result.stdout)

        # Exercise the same managed handoff as delivery_control._launch, which
        # makes a new process witness before the child's application imports.
        child_ok = ("import pathlib,subprocess,sys;"
                    "from devflow_temporal.delivery_config import DeliveryConfig;"
                    "from devflow_temporal.delivery_installation_owner import subprocess_command,subprocess_options;"
                    "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));"
                    "opts=subprocess_options(c);"
                    "cmd=subprocess_command(c,[sys.executable,'-c',sys.argv[2],sys.argv[1]]);"
                    "p=subprocess.run(cmd,capture_output=True,text=True,**opts);"
                    "print(p.stdout,p.stderr);sys.exit(p.returncode)")
        accepted(run(entry, '-c', child_ok, str(config), scope))

        # The managed child outlives a graceful parent exit and must hold its
        # own SH, rather than depending on the lifetime of the parent process.
        ready, release, finished = (self.home / name for name in
                                    ('child-ready', 'child-release', 'child-finished'))
        child_record = self.home / 'managed-child.pid'
        parent_record = self.home / 'managed-parent-child.pid'
        child_wait = ("import json,os,pathlib,subprocess,sys,time;"
                      "identity=' '.join(subprocess.check_output(['ps','-p',str(os.getpid()),'-o','lstart='],text=True).split());"
                      "record=pathlib.Path(sys.argv[2]);stage=record.with_name(record.name+'.tmp');"
                      "stage.write_text(json.dumps({'pid':os.getpid(),'birth':identity}));stage.replace(record);"
                      "from devflow_temporal.delivery_config import DeliveryConfig;"
                      "from devflow_temporal.delivery_installation_owner import fresh_namespace_scope;"
                      "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));\n"
                      "with fresh_namespace_scope(c) as s:\n"
                      " s.verify();pathlib.Path(sys.argv[5]).write_text('ready');\n"
                      " deadline=time.monotonic()+45\n"
                      " while not pathlib.Path(sys.argv[3]).exists() and time.monotonic()<deadline: time.sleep(.05)\n"
                      " if not pathlib.Path(sys.argv[3]).exists(): raise TimeoutError('release missing')\n"
                      " s.verify()\n"
                      "pathlib.Path(sys.argv[4]).write_text('finished')")
        parent = ("import json,pathlib,subprocess,sys;"
                  "from devflow_temporal.delivery_config import DeliveryConfig;"
                  "from devflow_temporal.delivery_installation_owner import subprocess_command,subprocess_options;"
                  "c=DeliveryConfig.load(pathlib.Path(sys.argv[1]));opts=subprocess_options(c);"
                  "cmd=subprocess_command(c,[sys.executable,'-c',sys.argv[2],*sys.argv[1:2],*sys.argv[3:7]]);"
                  "p=subprocess.Popen(cmd,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,"
                  "stderr=open(sys.argv[7],'w'),start_new_session=True,**opts);"
                  "identity=' '.join(subprocess.check_output(['ps','-p',str(p.pid),'-o','lstart='],text=True).split());"
                  "record=pathlib.Path(sys.argv[8]);stage=record.with_name(record.name+'.tmp');"
                  "stage.write_text(json.dumps({'pid':p.pid,'birth':identity}));stage.replace(record);"
                  "print(p.pid,flush=True)")
        error_log = self.home / 'managed-child.err'
        required = (str(config), str(child_record), str(release), str(finished), str(ready))
        try:
            parent_result = run(entry, '-c', parent, str(config), child_wait, str(child_record),
                                str(release), str(finished), str(ready), str(error_log), str(parent_record))
            self.assertEqual(parent_result.returncode, 0, parent_result.stdout + parent_result.stderr)
            self.assertTrue(parent_result.stdout.strip().isdigit(), parent_result.stdout)
            self.assertTrue(wait_for_file(ready, 30),
                            error_log.read_text() if error_log.exists() else 'child did not start')
            identity = json.loads(child_record.read_text())
            self.assertEqual(identity['pid'], int(parent_result.stdout.strip()))
            self.assertEqual(child_identity(identity['pid'], required), identity['birth'])
            lock = os.open(root.parent.parent.parent / '.production-install.lock', os.O_RDWR | os.O_NOFOLLOW)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lock)
        finally:
            release_owned_child(release, child_record, parent_record, required)
        self.assertTrue(finished.exists(), error_log.read_text() if error_log.exists() else 'child did not finish')
        lock = os.open(root.parent.parent.parent / '.production-install.lock', os.O_RDWR | os.O_NOFOLLOW)
        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        self.fail('managed child did not release SH after exit')
                    time.sleep(.05)
        finally:
            os.close(lock)

        # A live admitted process holds the owner SH against a real competing EX.
        parent_ready = self.home / 'live-parent-ready'
        waiting = scope + "\npathlib.Path(sys.argv[2]).write_text('ready')\ninput()"
        process = subprocess.Popen([*entry, '-c', waiting, str(config), str(parent_ready)], env=self.env,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(wait_for_file(parent_ready, 30), 'live parent did not become ready within 30 seconds')
            lock = os.open(root.parent.parent.parent / '.production-install.lock', os.O_RDWR | os.O_NOFOLLOW)
            try:
                with self.assertRaises(BlockingIOError):
                    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            finally:
                os.close(lock)
        finally:
            finish_direct_process(process)
        self.assertEqual(process.returncode, 0)
        self.assertEqual(database.read_bytes(), before)

if __name__ == '__main__':
    unittest.main()
