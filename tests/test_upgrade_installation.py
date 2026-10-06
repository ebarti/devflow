"""Exercise real historical installers and public update entry points in owned roots."""

import contextlib
import importlib.util
import io
import json
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
V022 = "e966cf89e057abc9a2629faf957a2ec175599b53"
GUARDED = "115d17d"
INSTALLERS = ("install.sh", "install-agents.py", "install-service-entry.py", "install-migration.py", "install-rollback.py")


class UpgradeInstallation(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="devflow-upgrade-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "source"
        self.skills, self.home = self.root / "skills", self.root / "codex"
        self.home.mkdir()
        self.env = dict(os.environ, DEVFLOW_PYTHON=sys.executable,
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        self.git("clone", "--shared", str(ROOT), str(self.source), cwd=self.root)
        self.data = self.home / "user.sqlite3"
        self.data.write_bytes(b"user data, never an installer migration target")
        self.config = self.home / "config.toml"
        self.config.write_text('model = "foreign"\n')
        self.hooks = self.home / "hooks.json"
        self.foreign = {"hooks": [{"command": "foreign hook"}], "matcher": "foreign"}
        self.hooks.write_text(json.dumps({"custom": 42, "hooks": {
            "Stop": [self.foreign], "ForeignEvent": [{"hooks": []}, {"matcher": "metadata"}]}}))

    def git(self, *args, cwd=None):
        return subprocess.run(["git", *args], cwd=cwd or self.source, env=self.env,
                              check=True, capture_output=True, text=True).stdout.strip()

    def command(self, script, *args):
        return subprocess.run(["sh", str(script), *map(str, args)], env=self.env,
                              capture_output=True, text=True, check=False)

    def historical(self, revision):
        self.git("checkout", "--detach", revision)
        result = self.command(self.source / "scripts/install.sh", self.skills, self.home)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.old_head = self.git("rev-parse", "HEAD")
        self.candidate()

    def candidate(self):
        self.git("checkout", "--detach", self.git("-C", str(ROOT), "rev-parse", "HEAD"))
        # Include local installer edits for RED and local focused validation;
        # the source helpers and historical Git objects remain authentic.
        for name in INSTALLERS:
            if (ROOT / "scripts" / name).exists():
                shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)

    def snapshot(self):
        return {
            str(p.relative_to(self.root)): ("link", os.readlink(p), p.lstat().st_uid)
            if p.is_symlink() else ("file", p.read_bytes(), p.stat().st_mode)
            for folder in (self.skills, self.home)
            for p in folder.rglob("*") if p.is_symlink() or p.is_file()
        }

    def install(self, *args):
        return self.command(self.source / "scripts/install.sh", *args, self.skills, self.home)

    def assert_upgraded(self):
        self.assertTrue((self.skills / "devflow-local-delivery/SKILL.md").is_file())
        self.assertFalse((self.skills / "devflow/SKILL.md").exists())
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
        self.assertTrue((self.source / "skills/devflow/SKILL.md").is_file())
        for name in (self.source / "skills").iterdir():
            if name.name != "devflow":
                self.assertFalse(os.path.lexists(self.skills / name.name))
        self.assertFalse((self.home / ".devflow-install.json").exists())
        self.assertFalse((self.home / ".devflow-hook.py").exists())
        self.assertEqual(self.data.read_bytes(), b"user data, never an installer migration target")
        self.assertEqual(self.config.read_text(), 'model = "foreign"\n')
        hooks = json.loads(self.hooks.read_text())
        self.assertEqual(hooks["custom"], 42)
        self.assertIn(self.foreign, hooks["hooks"]["Stop"])
        self.assertEqual(hooks["hooks"]["ForeignEvent"], [{"hooks": []}, {"matcher": "metadata"}])
        self.assertNotIn("Record Devflow metrics", self.hooks.read_text())
        self.assertFalse((self.home / "LaunchAgents").exists())
        self.assertEqual(len(list((self.home / "agents").glob("*.toml"))), 4)
        self.assertTrue(all(not p.is_symlink() for p in (self.home / "agents").glob("*.toml")))

    def assert_forward_drift_preserved(self, boundary):
        self.historical(GUARDED)
        modules = {}
        for name in ("install-rollback", "install-service-entry"):
            spec = importlib.util.spec_from_file_location(name, self.source / "scripts" / (name + ".py"))
            modules[name] = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modules[name])
        rollback, service = modules["install-rollback"], modules["install-service-entry"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        target = {"pointer": self.skills / "devflow", "retired": self.skills / "devflow-reviewing",
                  "hooks": self.hooks}[boundary]
        payload = ("foreign forward bytes at " + boundary).encode()
        injected = []
        unlink, replace, exchange, load = Path.unlink, os.replace, rollback.exchange, service.load

        def inject(path):
            if path == target and not injected:
                unlink(target)
                target.write_bytes(payload)
                injected.append(rollback.identity(target))

        def before_unlink(path, *args, **kwargs):
            inject(path)
            return unlink(path, *args, **kwargs)

        def before_replace(stage, destination):
            inject(stage if boundary == "retired" else destination)
            return replace(stage, destination)

        def before_exchange(stage, destination):
            inject(destination)
            return exchange(stage, destination)

        def owning_load(name):
            if name == "install-rollback":
                return rollback
            module = load(name)
            if name == "install-agents":
                module.rollback_module = lambda: rollback
            return module

        error = None
        with mock.patch.object(service, "load", owning_load), \
                mock.patch.object(Path, "unlink", before_unlink), \
                mock.patch.object(os, "replace", before_replace), \
                mock.patch.object(rollback, "exchange", before_exchange):
            try:
                service.install(self.skills, self.home, False, backup)
            except ValueError as exc:
                error = exc
        self.assertTrue(injected, "actual forward mutation boundary was not exercised")
        self.assertIsNotNone(error, "forward migration accepted and deleted concurrent foreign bytes")
        self.assertIn(str(target), str(error))
        self.assertIn(str(backup), str(error))
        with self.assertRaisesRegex(ValueError, "retained backup"):
            rollback.restore(backup)
        preserved = [p for p in backup.rglob("*") if not p.is_symlink() and p.is_file()
                     and p.read_bytes() == payload]
        self.assertTrue(preserved, "foreign bytes and full backup were not retained after rollback")
        self.assertEqual(preserved[0].lstat().st_ino, injected[0]["node"][1])
        self.assertTrue((backup / "snapshot.json").is_file())
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
        self.assertEqual(self.data.read_bytes(), b"user data, never an installer migration target")

    def test_forward_pointer_exchange_preserves_concurrent_foreign_bytes(self):
        self.assert_forward_drift_preserved("pointer")

    def test_forward_retired_link_capture_preserves_concurrent_foreign_bytes(self):
        self.assert_forward_drift_preserved("retired")

    def test_forward_hook_write_exchange_preserves_concurrent_foreign_bytes(self):
        self.assert_forward_drift_preserved("hooks")

    def test_true_v022_install_upgrade_and_idempotent_replay(self):
        self.historical(V022)
        self.assertFalse((self.home / ".devflow-install.json").exists())
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_upgraded()
        before = self.snapshot()
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_guarded_install_migrates_only_recorded_unchanged_files(self):
        self.historical(GUARDED)
        self.assertTrue((self.home / ".devflow-install.json").is_file())
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assert_upgraded()

    def test_modified_hook_refused_before_effects_even_with_force(self):
        self.historical(GUARDED)
        guard = self.home / ".devflow-hook.py"
        guard.write_bytes(guard.read_bytes() + b"\n# user's change\n")
        before = self.snapshot()
        result = self.install("--force")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(guard), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_modified_agent_and_unowned_guard_alias_refuse_unchanged(self):
        self.historical(GUARDED)
        agent = self.home / "agents/devflow-implementer.toml"
        original = agent.read_bytes()
        agent.write_bytes(original + b"\n# user customization\n")
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(agent), result.stderr)
        self.assertEqual(self.snapshot(), before)
        agent.write_bytes(original)
        self.candidate()
        hook = self.home / ".devflow-hook.py"
        foreign = self.home / "foreign-guard.py"
        foreign.write_bytes(hook.read_bytes())
        hook.unlink()
        hook.symlink_to(foreign)
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(hook), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_pin_hash_tampering_and_modified_interpreter_refuse(self):
        self.historical(GUARDED)
        pin = self.home / ".devflow-install.json"
        original = pin.read_bytes()
        saved = json.loads(original)
        saved["files"]["skills/devflow/SKILL.md"] = "0" * 64
        pin.write_text(json.dumps(saved))
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(pin), result.stderr)
        self.assertEqual(self.snapshot(), before)
        pin.write_bytes(original)
        self.candidate()
        hooks = json.loads(self.hooks.read_text())
        item = hooks["hooks"]["Stop"][-1]["hooks"][0]
        item["command"] = item["command"].replace(str(Path(sys.executable).resolve()), "/bin/sh")
        self.hooks.write_text(json.dumps(hooks))
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.hooks), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_modified_status_on_owned_guard_command_refused(self):
        self.historical(GUARDED)
        hooks = json.loads(self.hooks.read_text())
        hooks["hooks"]["Stop"][-1]["hooks"][0]["statusMessage"] = "Customized metrics"
        self.hooks.write_text(json.dumps(hooks))
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.hooks), result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.assertTrue((self.home / ".devflow-hook.py").is_file())

    def test_modified_matcher_on_owned_hook_group_refused(self):
        self.historical(GUARDED)
        hooks = json.loads(self.hooks.read_text())
        hooks["hooks"]["Stop"][-1]["matcher"] = "Customized matcher"
        self.hooks.write_text(json.dumps(hooks))
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.hooks), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_modified_historical_source_skill_preserved(self):
        self.historical(V022)
        skill = self.source / "skills/devflow/SKILL.md"
        skill.write_bytes(skill.read_bytes() + b"\nuser source change\n")
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(skill), result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.assertTrue(skill.read_bytes().endswith(b"user source change\n"))

    def test_unowned_skill_pointer_and_changed_hook_refuse(self):
        self.historical(V022)
        link = self.skills / "devflow-reviewing"
        link.unlink()
        link.symlink_to(ROOT / "skills/devflow-reviewing")
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(link), result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.candidate()
        link.unlink()
        link.symlink_to(self.source / "skills/devflow-reviewing")
        hooks = json.loads(self.hooks.read_text())
        hooks["hooks"]["Stop"][-1]["hooks"][0]["command"] += " --custom"
        self.hooks.write_text(json.dumps(hooks))
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(self.hooks), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_preflight_refusal_does_not_remove_foreign_post_capture_data(self):
        self.historical(GUARDED)
        foreign = self.skills / ".devflow-helpers/scripts"
        wrapper = self.root / "python-fixture"
        code = f"from pathlib import Path; p=Path({str(foreign)!r}); p.parent.mkdir(); p.write_bytes(b'foreign post-capture data')"
        wrapper.write_text(
            '#!/bin/sh\ncase "$2" in */install-service-entry.py)\n'
            + shlex.quote(sys.executable) + " -c " + shlex.quote(code) + '\n;; esac\nexec '
            + shlex.quote(sys.executable) + ' "$@"\n')
        wrapper.chmod(0o700)
        self.env["DEVFLOW_PYTHON"] = str(wrapper)
        before = self.snapshot()
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(str(foreign.parent), result.stderr)
        self.assertTrue(foreign.is_file(), "rollback deleted foreign post-capture data")
        self.assertEqual(foreign.read_bytes(), b"foreign post-capture data")
        after = self.snapshot()
        after.pop(str(foreign.relative_to(self.root)))
        self.assertEqual(after, before)
        self.assertEqual(self.git("rev-parse", "HEAD"), self.old_head)

    def test_post_effect_foreign_replacement_is_preserved_with_retained_backup(self):
        self.historical(GUARDED)
        modules = {}
        for name in ("install-rollback", "install-service-entry"):
            spec = importlib.util.spec_from_file_location(name, self.source / "scripts" / (name + ".py"))
            modules[name] = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modules[name])
        rollback = modules["install-rollback"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        modules["install-service-entry"].install(self.skills, self.home, False, backup)
        foreign = self.skills / ".devflow-helpers/scripts"
        foreign.unlink()
        foreign.write_bytes(b"foreign data created after installer effect")
        foreign_agent = self.home / "agents/devflow-implementer.toml"
        foreign_agent.unlink()
        foreign_agent.write_bytes(b"foreign agent created after installer effect")
        with self.assertRaisesRegex(ValueError, "rollback drift preserved") as error:
            rollback.restore(backup)
        self.assertIn(str(foreign), str(error.exception))
        self.assertIn(str(foreign_agent), str(error.exception))
        self.assertEqual(foreign_agent.read_bytes(), b"foreign agent created after installer effect")
        self.assertEqual(foreign.read_bytes(), b"foreign data created after installer effect")
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
        self.assertEqual(os.readlink(self.skills / "devflow"), str(self.source / "skills/devflow"))
        self.assertTrue((backup / "snapshot.json").is_file())
        self.assertEqual(self.data.read_bytes(), b"user data, never an installer migration target")

    def test_foreign_swap_at_cleanup_syscall_is_captured_and_preserved(self):
        self.historical(GUARDED)
        modules = {}
        for name in ("install-rollback", "install-service-entry"):
            spec = importlib.util.spec_from_file_location(name, self.source / "scripts" / (name + ".py"))
            modules[name] = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modules[name])
        rollback = modules["install-rollback"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        modules["install-service-entry"].install(self.skills, self.home, False, backup)
        foreign = self.skills / ".devflow-helpers/scripts"
        unlink, replace = Path.unlink, os.replace
        injected = []

        def inject(path):
            if Path(path) == foreign and not injected:
                unlink(foreign)
                foreign.write_bytes(b"foreign replacement at cleanup syscall")
                injected.append(str(path))

        def swap_before_unlink(path, *args, **kwargs):
            inject(path)
            return unlink(path, *args, **kwargs)

        def swap_before_capture(source, target):
            inject(source)
            return replace(source, target)

        error = None
        with mock.patch.object(Path, "unlink", swap_before_unlink), \
                mock.patch.object(os, "replace", swap_before_capture):
            try:
                rollback.restore(backup)
            except ValueError as exc:
                error = exc
        self.assertTrue(injected, "actual cleanup boundary was not exercised")
        self.assertTrue(foreign.is_file(), "cleanup deleted the foreign replacement")
        self.assertEqual(foreign.read_bytes(), b"foreign replacement at cleanup syscall")
        self.assertIn(str(foreign), str(error))
        self.assertIn("rollback drift preserved", str(error))
        self.assertTrue((backup / "snapshot.json").is_file())
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())

    def test_foreign_write_during_rollback_is_preserved_and_backup_retained(self):
        self.historical(GUARDED)
        modules = {}
        for name in ("install-rollback", "install-service-entry"):
            spec = importlib.util.spec_from_file_location(name, self.source / "scripts" / (name + ".py"))
            modules[name] = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(modules[name])
        rollback = modules["install-rollback"]
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        modules["install-service-entry"].install(self.skills, self.home, False, backup)
        foreign = self.skills / ".devflow-helpers/scripts"
        replace, exchange = os.replace, rollback.exchange
        injected = []

        def write_after_pointer_restore(operation, stage, target):
            operation(stage, target)
            if target == self.skills / "devflow":
                foreign.unlink()
                foreign.write_bytes(b"foreign write during rollback")
                injected.append(str(target))

        error = None
        with mock.patch.object(os, "replace", lambda a, b: write_after_pointer_restore(replace, a, b)), \
                mock.patch.object(rollback, "exchange", lambda a, b: write_after_pointer_restore(exchange, a, b)):
            try:
                rollback.restore(backup)
            except ValueError as exc:
                error = exc
        self.assertTrue(injected, "foreign writer was not exercised")
        self.assertTrue(foreign.is_file(), "rollback deleted data written during restore")
        self.assertEqual(foreign.read_bytes(), b"foreign write during rollback")
        self.assertIn(str(foreign), str(error))
        self.assertIn("rollback drift preserved", str(error))
        self.assertTrue((backup / "snapshot.json").is_file())
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
        self.assertEqual(self.data.read_bytes(), b"user data, never an installer migration target")

    def test_rollback_keeps_public_helper_available_at_each_mutation(self):
        self.historical(GUARDED)
        spec = importlib.util.spec_from_file_location(
            "upgrade_rollback", self.source / "scripts/install-rollback.py")
        rollback = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(rollback)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        before = self.snapshot()
        service_spec = importlib.util.spec_from_file_location(
            "upgrade_service", self.source / "scripts/install-service-entry.py")
        service = importlib.util.module_from_spec(service_spec)
        service_spec.loader.exec_module(service)
        service.install(self.skills, self.home, False, backup)
        helper = self.skills / "devflow/scripts/state.py"
        observations = []
        unlink, rmdir, replace, exchange = Path.unlink, Path.rmdir, os.replace, rollback.exchange

        def observe(operation, *args, **kwargs):
            self.assertTrue(helper.is_file(), f"helper unavailable before {operation.__name__}")
            result = operation(*args, **kwargs)
            observations.append(str(args[0]))
            self.assertTrue(helper.is_file(), f"helper unavailable after {operation.__name__}: {args[0]}")
            return result

        with mock.patch.object(Path, "unlink", lambda *a, **kw: observe(unlink, *a, **kw)), \
                mock.patch.object(Path, "rmdir", lambda *a, **kw: observe(rmdir, *a, **kw)), \
                mock.patch.object(os, "replace", lambda *a, **kw: observe(replace, *a, **kw)), \
                mock.patch.object(rollback, "exchange", lambda *a, **kw: observe(exchange, *a, **kw)):
            rollback.restore(backup)
        self.assertTrue(observations)
        self.assertEqual(self.snapshot(), before)

    def test_failure_after_migration_restores_owned_files_and_helpers(self):
        self.historical(GUARDED)
        before = self.snapshot()
        agent_installer = self.source / "scripts/install-agents.py"
        agent_installer.write_text(agent_installer.read_text().replace(
            'if mode == "preflight":', 'if mode == "apply":\n        fail("injected failure")\n    if mode == "preflight":'))
        result = self.install()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("injected failure", result.stderr)
        self.assertEqual(self.snapshot(), before)
        self.assertFalse((self.skills / ".devflow-helpers").exists())

    def test_public_old_updater_restores_source_on_zero_effect_capture_refusal(self):
        # The published old updater execs the candidate installer; it cannot
        # perform checkout recovery itself. This tag exists only in our fixture.
        origin = self.root / "origin"
        self.git("clone", "--shared", str(ROOT), str(origin), cwd=self.root)
        self.git("-C", str(origin), "tag", "fixture-capture-refusal")
        self.git("remote", "set-url", "origin", str(origin))
        self.git("checkout", "--detach", V022)
        installed = self.command(self.source / "scripts/install.sh", self.skills, self.home)
        self.assertEqual(installed.returncode, 0, installed.stderr)
        old = self.git("rev-parse", "HEAD")
        before = self.snapshot()
        marker = self.root / "observer-ran"
        observer = self.root / "observer-python"
        body = """import json, os, pathlib, runpy, sys
original = sys.argv[1:]
arguments = original[1:] if original and original[0] == "-B" else original
if len(arguments) > 1 and pathlib.Path(arguments[0]).name == "install-rollback.py" and arguments[1] == "capture":
    target = pathlib.Path(arguments[4]) / "hooks.json"
    inode, fdopen, injected = target.lstat().st_ino, os.fdopen, []
    class Reader:
        def __init__(self, stream): self.stream = stream
        def __enter__(self): return self
        def __exit__(self, *args): return self.stream.__exit__(*args)
        def fileno(self): return self.stream.fileno()
        def read(self):
            raw = self.stream.read()
            if not injected and os.fstat(self.fileno()).st_ino == inode:
                hooks = json.loads(raw)
                hooks["foreign_capture_note"] = "ordinary update during public old-updater capture"
                target.write_text(json.dumps(hooks))
                injected.append(True)
                pathlib.Path(MARKER).write_text("observed")
            return raw
    os.fdopen = lambda descriptor, *a, **kw: Reader(fdopen(descriptor, *a, **kw))
    sys.argv = arguments
    runpy.run_path(arguments[0], run_name="__main__")
else:
    os.execv(sys.executable, [sys.executable, *original])
"""
        observer.write_text("#!" + sys.executable + "\n" + body.replace("MARKER", repr(str(marker))))
        observer.chmod(0o700)
        self.env["DEVFLOW_PYTHON"] = str(observer)
        refused = self.command(self.source / "scripts/update.sh", "fixture-capture-refusal", self.skills, self.home)
        self.assertNotEqual(refused.returncode, 0)
        self.assertTrue(marker.is_file(), "ordinary capture writer was not exercised")
        self.assertIn("installer file changed while reading", refused.stderr)
        self.assertIn(str(self.hooks), refused.stderr)
        self.assertIn("foreign_capture_note", json.loads(self.hooks.read_text()))
        after = self.snapshot()
        key = str(self.hooks.relative_to(self.root))
        before.pop(key)
        after.pop(key)
        self.assertEqual(after, before, "zero-effect capture refusal changed installation destinations")
        self.assertEqual(self.git("rev-parse", "HEAD"), old, "historical updater left the refused source active")

    def test_public_old_updater_to_candidate_then_normal_update_and_failed_upgrade(self):
        # The updater requires a tag fetched from origin. These two refs exist
        # only in this test's private repository, never in the source or GitHub.
        origin = self.root / "origin"
        self.git("clone", "--shared", str(ROOT), str(origin), cwd=self.root)
        for name in INSTALLERS:
            if (ROOT / "scripts" / name).exists():
                shutil.copyfile(ROOT / "scripts" / name, origin / "scripts" / name)
        self.git("-C", str(origin), "add", "scripts")
        self.git("-C", str(origin), "-c", "user.name=Installer fixture", "-c",
                 "user.email=fixture@example.invalid", "commit", "--allow-empty", "-m", "installer fixture")
        candidate = self.git("-C", str(origin), "rev-parse", "HEAD")
        self.git("-C", str(origin), "tag", "fixture-candidate", candidate)
        self.git("-C", str(origin), "tag", "fixture-replay", candidate)
        self.git("remote", "set-url", "origin", str(origin))
        self.git("checkout", "--detach", V022)
        old = self.command(self.source / "scripts/install.sh", self.skills, self.home)
        self.assertEqual(old.returncode, 0, old.stderr)
        result = self.command(self.source / "scripts/update.sh", "fixture-candidate", self.skills, self.home)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD"), candidate)
        self.assert_upgraded()
        before = self.snapshot()
        replay = self.command(self.source / "scripts/update.sh", "fixture-replay", self.skills, self.home)
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(self.snapshot(), before)
        (origin / "scripts/install-service-entry.py").write_text('raise SystemExit("fixture install failure")\n')
        self.git("-C", str(origin), "add", "scripts/install-service-entry.py")
        self.git("-C", str(origin), "-c", "user.name=Installer fixture", "-c",
                 "user.email=fixture@example.invalid", "commit", "-m", "broken fixture")
        self.git("-C", str(origin), "tag", "fixture-broken")
        failed = self.command(self.source / "scripts/update.sh", "fixture-broken", self.skills, self.home)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn("fixture install failure", failed.stderr)
        self.assertEqual(self.git("rev-parse", "HEAD"), candidate)
        self.assertEqual(self.snapshot(), before)


if __name__ == "__main__":
    unittest.main()
