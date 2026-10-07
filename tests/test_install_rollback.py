"""Observe rollback's real filesystem boundaries without running a service."""

import base64
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]


class InstallRollback(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="devflow-rollback-test-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "source"
        subprocess.run(["git", "clone", "--quiet", "--shared", str(ROOT), str(self.source)], check=True)
        subprocess.run(["git", "-C", str(self.source), "checkout", "--quiet", "--detach", "HEAD"], check=True)
        self.skills, self.home = self.root / "skills", self.root / "codex"
        self.skills.mkdir()
        self.home.mkdir()
        spec = importlib.util.spec_from_file_location("test_rollback", ROOT / "scripts/install-rollback.py")
        self.rollback = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.rollback)
        self.compatibility = self.skills / "devflow"
        self.compatibility.symlink_to(self.source / "skills/devflow")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(self.source, self.skills, self.home)
        self.backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, self.backup, True)
        self.helper = self.skills / ".devflow-helpers"
        self.helper.mkdir()
        self.rollback.created(self.backup, self.helper)
        for name in ("scripts", "references"):
            (self.helper / name).symlink_to(self.source / "skills/devflow" / name)
            self.rollback.created(self.backup, self.helper / name)
        stage = self.skills / "pointer"
        stage.symlink_to(self.helper)
        self.rollback.remember(self.backup, self.compatibility, self.rollback.identity(stage))
        os.replace(stage, self.compatibility)

    def test_native_exchange_preserves_foreign_pointer_swapped_at_boundary(self):
        exchange = self.rollback.exchange
        injected = []
        foreign = b"foreign pointer payload at native exchange"

        def swap_before_exchange(stage, target):
            if target == self.compatibility:
                target.unlink()
                target.write_bytes(foreign)
                injected.append(self.rollback.identity(target))
            exchange(stage, target)

        with mock.patch.object(self.rollback, "exchange", swap_before_exchange):
            with self.assertRaisesRegex(ValueError, "captured at") as error:
                self.rollback.restore(self.backup)
        self.assertTrue(injected)
        captured = [p for p in self.backup.glob("restore-*") if not p.is_symlink() and p.read_bytes() == foreign]
        self.assertEqual(len(captured), 1)
        self.assertEqual(self.rollback.identity(captured[0]), injected[0])
        self.assertIn(str(captured[0]), str(error.exception))
        self.assertTrue((self.backup / "snapshot.json").is_file())
        self.assertTrue((self.compatibility / "scripts/state.py").is_file())

    def test_capture_preserves_foreign_object_swapped_at_move_boundary(self):
        target = self.helper / "scripts"
        replace = os.replace
        injected = []

        def swap_before_capture(source, destination):
            if source == target:
                target.unlink()
                target.write_bytes(b"foreign replacement at atomic capture")
                injected.append(str(target))
            replace(source, destination)

        with mock.patch.object(os, "replace", swap_before_capture):
            with self.assertRaisesRegex(ValueError, "captured at"):
                self.rollback.restore(self.backup)
        self.assertTrue(injected)
        self.assertEqual(target.read_bytes(), b"foreign replacement at atomic capture")
        self.assertTrue((self.backup / "snapshot.json").is_file())
        self.assertTrue((self.compatibility / "scripts/state.py").is_file())

    def test_continuous_helper_access_around_native_exchange_and_capture(self):
        helper = self.compatibility / "scripts/state.py"
        replace, exchange = os.replace, self.rollback.exchange
        observations = []

        def observe(operation, *args):
            self.assertTrue(helper.is_file())
            operation(*args)
            self.assertTrue(helper.is_file())
            observations.append(str(args[0]))

        with mock.patch.object(os, "replace", lambda *a: observe(replace, *a)), \
                mock.patch.object(self.rollback, "exchange", lambda *a: observe(exchange, *a)):
            self.rollback.restore(self.backup)
        self.assertTrue(observations)
        self.assertFalse(self.backup.exists())
        self.assertFalse(self.helper.exists())
        self.assertEqual(os.readlink(self.compatibility), str(self.source / "skills/devflow"))

    def test_agent_write_authenticates_displaced_foreign_file(self):
        target = self.home / "agents/devflow-implementer.toml"
        target.parent.mkdir()
        target.write_bytes(b"old installed agent")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        spec = importlib.util.spec_from_file_location("test_agents", ROOT / "scripts/install-agents.py")
        agents = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(agents)
        agents.ROLLBACK = backup
        agents.rollback_module = lambda: self.rollback
        exchange = self.rollback.exchange
        injected = []

        def replace_before_exchange(stage, destination):
            if destination == target:
                target.unlink()
                target.write_bytes(b"foreign agent at atomic writer boundary")
                injected.append(target.lstat().st_ino)
            exchange(stage, destination)

        with mock.patch.object(self.rollback, "exchange", replace_before_exchange):
            with self.assertRaisesRegex(ValueError, "forward installer drift preserved"):
                agents.atomic_write(target, b"new installed agent")
        with self.assertRaisesRegex(ValueError, "retained backup"):
            self.rollback.restore(backup)
        captured = [p for p in backup.glob("forward-*") if p.read_bytes() == b"foreign agent at atomic writer boundary"]
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].lstat().st_ino, injected[0])
        self.assertEqual(target.read_bytes(), b"old installed agent")

    def test_absent_agent_write_never_overwrites_concurrent_creation(self):
        target = self.home / "agents/devflow-reviewer.toml"
        target.parent.mkdir()
        stage = self.home / "stage"
        stage.write_bytes(b"new installed agent")
        link = os.link
        injected = []

        def create_before_link(source, destination, **kwargs):
            if destination == target:
                target.write_bytes(b"foreign concurrently created agent")
                injected.append(True)
            return link(source, destination, **kwargs)

        with mock.patch.object(os, "link", create_before_link):
            with self.assertRaises(FileExistsError):
                self.rollback.effect(self.backup, target, stage)
        with self.assertRaisesRegex(ValueError, "retained backup"):
            self.rollback.restore(self.backup)
        self.assertTrue(injected)
        self.assertEqual(target.read_bytes(), b"foreign concurrently created agent")
        self.assertTrue((self.backup / "snapshot.json").is_file())

    def test_agent_apply_without_snapshot_refuses_before_effects(self):
        spec = importlib.util.spec_from_file_location("unbound_agents", ROOT / "scripts/install-agents.py")
        agents = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(agents)
        before = list(self.home.iterdir())
        arguments = [str(ROOT / "scripts/install-agents.py"), "apply", str(self.source),
                     str(self.skills), str(self.home), "false"]
        with mock.patch.object(sys, "argv", arguments):
            with self.assertRaisesRegex(ValueError, "installer snapshot; run scripts/install.sh"):
                agents.main()
        self.assertEqual(list(self.home.iterdir()), before)
        self.assertFalse((self.home / "agents").exists())

    def test_capture_binds_saved_file_bytes_and_identity_to_one_observation(self):
        target = self.home / "hooks.json"
        target.write_text('{"hooks": {}}')
        inode = target.lstat().st_ino
        read, fdopen, mkdtemp = Path.read_bytes, os.fdopen, tempfile.mkdtemp
        injected, backups = [], []

        def update_after_read(raw):
            if not injected:
                updated = json.loads(raw)
                updated["foreign_capture_note"] = "ordinary update during capture"
                target.write_text(json.dumps(updated))
                injected.append(True)
            return raw

        def path_read(path):
            raw = read(path)
            return update_after_read(raw) if path == target else raw

        class Reader:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def fileno(self):
                return self.stream.fileno()

            def read(self):
                raw = self.stream.read()
                return update_after_read(raw) if os.fstat(self.fileno()).st_ino == inode else raw

        def opened(descriptor, *args, **kwargs):
            return Reader(fdopen(descriptor, *args, **kwargs))

        def private_directory(*args, **kwargs):
            directory = mkdtemp(*args, **kwargs)
            backups.append(Path(directory))
            self.addCleanup(shutil.rmtree, directory, True)
            return directory

        error = None
        output = io.StringIO()
        with mock.patch.object(Path, "read_bytes", path_read), \
                mock.patch.object(os, "fdopen", opened), \
                mock.patch.object(tempfile, "mkdtemp", private_directory), \
                contextlib.redirect_stdout(output):
            try:
                self.rollback.capture(self.source, self.skills, self.home)
            except ValueError as exc:
                error = exc
        self.assertTrue(injected, "ordinary update at the actual byte observation was not exercised")
        if error:
            self.assertIn(str(target), str(error))
        else:
            saved = json.loads((backups[0] / "snapshot.json").read_text())[str(target)]
            self.assertEqual(hashlib.sha256(base64.b64decode(saved["bytes"])).hexdigest(),
                             saved["before"]["sha256"], "old bytes were bound to a newer file identity")
        self.assertEqual(json.loads(target.read_text())["foreign_capture_note"], "ordinary update during capture")
        self.assertTrue((self.compatibility / "scripts/state.py").is_file())

    def test_unreadable_and_fifo_displaced_objects_remain_unresolved(self):
        for kind in ("unreadable", "fifo"):
            for retire in (False, True):
                with self.subTest(kind=kind, retire=retire):
                    target = self.home / "agents/devflow-implementer.toml"
                    target.parent.mkdir(exist_ok=True)
                    target.unlink(missing_ok=True)
                    target.write_bytes(b"old installed agent")
                    output = io.StringIO()
                    with contextlib.redirect_stdout(output):
                        self.rollback.capture(self.source, self.skills, self.home)
                    backup = Path(output.getvalue().strip())
                    self.addCleanup(shutil.rmtree, backup, True)
                    stage = self.home / "stage"
                    stage.write_bytes(b"new installed agent")
                    replace, exchange = os.replace, self.rollback.exchange

                    def plant(target=target, kind=kind):
                        target.unlink()
                        if kind == "fifo":
                            os.mkfifo(target)
                        else:
                            target.write_bytes(b"foreign unreadable bytes")
                            target.chmod(0o200)

                    def before_exchange(left, right, target=target, plant=plant, exchange=exchange):
                        if right == target:
                            plant()
                        return exchange(left, right)

                    def before_replace(left, right, target=target, plant=plant, replace=replace):
                        if left == target:
                            plant()
                        return replace(left, right)

                    with mock.patch.object(self.rollback, "exchange", before_exchange), \
                            mock.patch.object(os, "replace", before_replace):
                        with self.assertRaisesRegex(ValueError, "forward installer drift preserved"):
                            self.rollback.effect(backup, target, None if retire else stage)
                    saved = json.loads((backup / "snapshot.json").read_text())[str(target)]
                    captured = next(backup.glob("forward-*"))
                    self.assertIn("authentication failed", saved["drift"])
                    self.assertEqual(saved["displaced_node"][1], captured.lstat().st_ino)
                    with self.assertRaisesRegex(ValueError, "captured at.*retained backup"):
                        self.rollback.restore(backup)
                    self.assertTrue(os.path.lexists(captured))
                    if kind == "unreadable":
                        captured.chmod(0o600)
                        self.assertEqual(captured.read_bytes(), b"foreign unreadable bytes")
                    self.assertEqual(backup.parent, self.home, "retained data was left in the OS temporary directory")

    def test_displaced_file_write_during_authentication_retains_updated_bytes(self):
        target = self.home / "agents/devflow-implementer.toml"
        target.parent.mkdir()
        target.write_bytes(b"old installed agent")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        stage = self.home / "stage"
        stage.write_bytes(b"new installed agent")
        exchange, fdopen = self.rollback.exchange, os.fdopen
        foreign_inode, changed = [], []
        updated = b"foreign bytes updated during authentication"

        def replace_before_exchange(left, right):
            target.unlink()
            target.write_bytes(b"foreign original bytes")
            foreign_inode.append(target.lstat().st_ino)
            return exchange(left, right)

        class Reader:
            def __init__(self, stream):
                self.stream = stream

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return self.stream.__exit__(*args)

            def fileno(self):
                return self.stream.fileno()

            def read(self):
                raw = self.stream.read()
                if foreign_inode and os.fstat(self.fileno()).st_ino == foreign_inode[0] and not changed:
                    next(backup.glob("forward-*")).write_bytes(updated)
                    changed.append(True)
                return raw

        with mock.patch.object(self.rollback, "exchange", replace_before_exchange), \
                mock.patch.object(os, "fdopen", lambda *a, **kw: Reader(fdopen(*a, **kw))):
            with self.assertRaises(ValueError):
                self.rollback.effect(backup, target, stage)
        self.assertTrue(changed, "the captured file was not updated through its open descriptor")
        with self.assertRaisesRegex(ValueError, "captured at.*retained backup"):
            self.rollback.restore(backup)
        captured = next(backup.glob("forward-*"))
        self.assertEqual(captured.read_bytes(), updated)
        self.assertEqual(captured.lstat().st_ino, foreign_inode[0])

    def test_distinct_target_device_refuses_before_effects(self):
        original = Path.stat
        before = sorted(p.name for p in self.home.iterdir())

        def distinct_device(path, *args, **kwargs):
            info = original(path, *args, **kwargs)
            if path == self.skills:
                values = list(info)
                values[2] += 1
                return os.stat_result(values)
            return info

        with mock.patch.object(Path, "stat", distinct_device):
            with self.assertRaisesRegex(ValueError, "requires one filesystem"):
                self.rollback.capture(self.source, self.skills, self.home)
        self.assertEqual(sorted(p.name for p in self.home.iterdir()), before)
        self.assertTrue((self.compatibility / "scripts/state.py").is_file())

    def test_cross_filesystem_and_unsupported_exchange_refuse_before_effects(self):
        import errno
        for error in (errno.EXDEV, errno.ENOTSUP):
            with self.subTest(error=error):
                before = sorted(p.name for p in self.home.iterdir())
                with mock.patch.object(self.rollback, "exchange", side_effect=OSError(error, os.strerror(error))):
                    with self.assertRaisesRegex(ValueError, "atomic exchange unsupported"):
                        self.rollback.capture(self.source, self.skills, self.home)
                self.assertEqual(sorted(p.name for p in self.home.iterdir()), before)
                self.assertTrue((self.compatibility / "scripts/state.py").is_file())

    def test_destination_restore_error_still_restores_owned_prior_checkout(self):
        import errno
        prior = subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip()
        subprocess.run(["git", "-C", str(self.source), "-c", "core.hooksPath=/dev/null",
                        "-c", "commit.gpgsign=false", "-c", "user.name=Test",
                        "-c", "user.email=test@example.invalid", "commit", "--allow-empty",
                        "--quiet", "-m", "isolated installer candidate"], check=True)
        current = subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip()
        for revision in (prior, current):
            subprocess.run(["git", "-C", str(self.source), "checkout", "--quiet", "--detach", revision], check=True)
        self.compatibility.unlink()
        self.compatibility.symlink_to(self.source / "skills/devflow")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(self.source, self.skills, self.home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        stage = self.skills / "pointer"
        stage.symlink_to(self.helper)
        self.rollback.effect(backup, self.compatibility, stage)
        with mock.patch.object(self.rollback, "exchange", side_effect=OSError(errno.EXDEV, "injected restore failure")):
            with self.assertRaisesRegex(ValueError, "destination recovery failed.*retained backup"):
                self.rollback.restore(backup)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip(), prior)
        self.assertTrue((backup / "snapshot.json").is_file())

    def test_aliased_install_and_canonical_replay_record_every_effect(self):
        for name in ("install.sh", "install-service-entry.py", "install-rollback.py", "install-agents.py"):
            shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)
        alias = self.root / "alias"
        alias.symlink_to(self.root)
        home = self.root / "fresh-codex"
        home.mkdir()
        for location in (alias, self.root):
            installed = subprocess.run(["sh", str(self.source / "scripts/install.sh"),
                                        str(location / "fresh-skills"), str(location / "fresh-codex")],
                                       env=dict(os.environ, DEVFLOW_PYTHON=sys.executable), capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
        self.assertTrue((self.root / "fresh-skills/devflow/scripts/state.py").is_file())
        self.assertTrue((self.root / "fresh-skills/devflow-local-delivery/SKILL.md").is_file())

    def test_umask002_install_replay_uses_protected_backup_ancestor(self):
        for name in ("install.sh", "install-service-entry.py", "install-rollback.py", "install-agents.py"):
            shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)
        skills, home = self.root / "fresh-skills", self.root / "fresh-codex"
        for _ in range(2):
            installed = subprocess.run(["sh", str(self.source / "scripts/install.sh"), str(skills), str(home)],
                                       umask=0o002, env=dict(os.environ, DEVFLOW_PYTHON=sys.executable),
                                       capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
            self.assertEqual(home.stat().st_mode & 0o777, 0o775)
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(self.source, skills, home)
        backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, backup, True)
        self.assertEqual(backup.parent, self.root)
        self.assertEqual(backup.stat().st_mode & 0o777, 0o700)
        home.chmod(0o777)
        before = sorted(path.name for path in home.iterdir())
        with self.assertRaisesRegex(ValueError, "backup parent must be owned and protected"):
            self.rollback.capture(self.source, skills, home)
        self.assertEqual(sorted(path.name for path in home.iterdir()), before)

    def test_symlinked_agent_directory_installs_and_refuses_foreign_drift(self):
        for name in ("install.sh", "install-service-entry.py", "install-rollback.py", "install-agents.py"):
            shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)
        skills, home, agents = self.root / "fresh-skills", self.root / "fresh-codex", self.root / "agents-real"
        home.mkdir()
        agents.mkdir()
        (home / "agents").symlink_to(agents)
        foreign = agents / "custom.toml"
        foreign.write_bytes(b"foreign agent data")
        for _ in range(2):
            installed = subprocess.run(["sh", str(self.source / "scripts/install.sh"), str(skills), str(home)],
                                       env=dict(os.environ, DEVFLOW_PYTHON=sys.executable), capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
        self.assertEqual(len(list(agents.glob("devflow-*.toml"))), 4)
        target = agents / "devflow-implementer.toml"
        target.write_bytes(b"user changed the installed agent")
        refused = subprocess.run(["sh", str(self.source / "scripts/install.sh"), str(skills), str(home)],
                                 env=dict(os.environ, DEVFLOW_PYTHON=sys.executable), capture_output=True, text=True)
        self.assertEqual(refused.returncode, 1, refused.stderr)
        self.assertEqual(target.read_bytes(), b"user changed the installed agent")
        self.assertEqual(foreign.read_bytes(), b"foreign agent data")
        self.assertEqual(os.readlink(home / "agents"), str(agents))

    def test_restore_continues_after_read_or_exchange_error_and_reports_conflicts(self):
        import errno
        for failure in ("read", "exchange"):
            with self.subTest(failure=failure):
                targets = [self.home / "agents" / ("devflow-" + name + ".toml")
                           for name in ("coordinator", "implementer", "reviewer")]
                targets[0].parent.mkdir(exist_ok=True)
                for target in targets:
                    target.unlink(missing_ok=True)
                for target in targets[:2]:
                    target.write_bytes(b"original agent")
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.rollback.capture(self.source, self.skills, self.home)
                backup = Path(output.getvalue().strip())
                self.addCleanup(shutil.rmtree, backup, True)
                for target in targets:
                    stage = self.root / "agent-stage"
                    stage.write_bytes(b"new installed agent")
                    self.rollback.effect(backup, target, stage)
                    stage.unlink(missing_ok=True)
                targets[0].write_bytes(b"foreign agent update")
                if failure == "read":
                    targets[1].chmod(0o200)
                exchange = self.rollback.exchange

                def fail_exchange(left, right, target=targets[1], failure=failure, exchange=exchange):
                    if failure == "exchange" and right == target:
                        raise OSError(errno.EXDEV, "injected isolated restore error", str(right))
                    return exchange(left, right)

                with mock.patch.object(self.rollback, "exchange", fail_exchange):
                    with self.assertRaisesRegex(ValueError, "retained backup") as error:
                        self.rollback.restore(backup)
                self.assertIn(str(targets[0]), str(error.exception))
                self.assertIn(str(targets[1]), str(error.exception))
                self.assertFalse(targets[2].exists(), "an independent owned path was skipped after recovery failed")
                self.assertEqual(targets[0].read_bytes(), b"foreign agent update")
                self.assertTrue((backup / "snapshot.json").exists())
                targets[1].chmod(0o600)

    def test_pointer_restore_error_preserves_helper_dependencies_and_recovers_agents(self):
        import errno
        target = self.home / "agents/devflow-reviewer.toml"
        target.parent.mkdir()
        stage = self.root / "agent-stage"
        stage.write_bytes(b"new installed agent")
        self.rollback.effect(self.backup, target, stage)
        stage.unlink(missing_ok=True)
        exchange = self.rollback.exchange

        def pointer_failure(left, right):
            if right == self.compatibility:
                raise OSError(errno.EXDEV, "injected pointer recovery error", str(right))
            return exchange(left, right)

        with mock.patch.object(self.rollback, "exchange", pointer_failure):
            with self.assertRaisesRegex(ValueError, "retained backup"):
                self.rollback.restore(self.backup)
        self.assertTrue((self.compatibility / "scripts/state.py").is_file())
        self.assertFalse(target.exists(), "independent owned agents were not recovered")
        self.assertTrue((self.backup / "snapshot.json").is_file())

    def test_group_backup_recovery_restores_public_old_updater_checkout(self):
        for name in ("install.sh", "install-service-entry.py", "install-rollback.py", "install-agents.py"):
            shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)
        for key, value in (("core.hooksPath", "/dev/null"), ("commit.gpgsign", "false"),
                           ("gc.auto", "0"), ("maintenance.auto", "false"),
                           ("user.name", "Test"), ("user.email", "test@example.invalid")):
            subprocess.run(["git", "-C", str(self.source), "config", key, value], check=True)
        subprocess.run(["git", "-C", str(self.source), "add", "scripts"], check=True)
        subprocess.run(["git", "-C", str(self.source), "commit", "--quiet", "--allow-empty", "-m", "private candidate"], check=True)
        subprocess.run(["git", "-C", str(self.source), "tag", "fixture-candidate"], check=True)
        checkout = self.root / "old-checkout"
        subprocess.run(["git", "clone", "--quiet", "--no-local", str(self.source), str(checkout)], check=True)
        baseline = "e966cf89e057abc9a2629faf957a2ec175599b53"
        subprocess.run(["git", "-C", str(checkout), "checkout", "--quiet", "--detach", baseline], check=True)
        skills, home = self.root / "old-skills", self.root / "old-codex"
        home.mkdir(mode=0o775)
        home.chmod(0o775)
        env = dict(os.environ, DEVFLOW_PYTHON=sys.executable, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        installed = subprocess.run(["sh", str(checkout / "scripts/install.sh"), str(skills), str(home)],
                                   env=env, capture_output=True, text=True)
        self.assertEqual(installed.returncode, 0, installed.stderr)
        hooks = (home / "hooks.json").read_bytes()
        user = home / "user.sqlite3"
        user.write_bytes(b"untouched user data")
        updated = subprocess.run(["sh", str(checkout / "scripts/update.sh"), "fixture-candidate", str(skills), str(home)],
                                 env=env, capture_output=True, text=True)
        self.assertEqual(updated.returncode, 1, updated.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip(), baseline)
        self.assertEqual((home / "hooks.json").read_bytes(), hooks)
        self.assertEqual(user.read_bytes(), b"untouched user data")
        self.assertTrue((skills / "devflow/scripts/state.py").is_file())

    def test_backup_creation_error_recovers_source_without_destination_effects(self):
        prior = subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip()
        subprocess.run(["git", "-C", str(self.source), "-c", "core.hooksPath=/dev/null",
                        "-c", "commit.gpgsign=false", "-c", "user.name=Test",
                        "-c", "user.email=test@example.invalid", "commit", "--allow-empty",
                        "--quiet", "-m", "isolated installer candidate"], check=True)
        current = subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip()
        for revision in (prior, current):
            subprocess.run(["git", "-C", str(self.source), "checkout", "--quiet", "--detach", revision], check=True)
        self.compatibility.unlink()
        self.compatibility.symlink_to(self.source / "skills/devflow")
        before = sorted(path.name for path in self.home.iterdir())
        with mock.patch.object(tempfile, "mkdtemp", side_effect=PermissionError("private backup creation refused")):
            with self.assertRaisesRegex(PermissionError, "private backup creation refused"):
                self.rollback.capture(self.source, self.skills, self.home)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.source), "rev-parse", "HEAD"], text=True).strip(), prior)
        self.assertEqual(sorted(path.name for path in self.home.iterdir()), before)
        self.assertEqual(os.readlink(self.compatibility), str(self.source / "skills/devflow"))

    def test_unsupported_host_refuses_without_replacement_fallback(self):
        before = os.readlink(self.compatibility)
        with mock.patch.object(sys, "platform", "unsupported"):
            with self.assertRaisesRegex(ValueError, "requires native"):
                self.rollback.exchange_function()
        self.assertEqual(os.readlink(self.compatibility), before)


if __name__ == "__main__":
    unittest.main()
