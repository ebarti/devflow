"""Observe rollback's real filesystem boundaries without running a service."""

import base64
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import shutil
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
        self.skills, self.home = self.root / "skills", self.root / "codex"
        self.skills.mkdir()
        self.home.mkdir()
        spec = importlib.util.spec_from_file_location("test_rollback", ROOT / "scripts/install-rollback.py")
        self.rollback = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.rollback)
        self.compatibility = self.skills / "devflow"
        self.compatibility.symlink_to(ROOT / "skills/devflow")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(ROOT, self.skills, self.home)
        self.backup = Path(output.getvalue().strip())
        self.addCleanup(shutil.rmtree, self.backup, True)
        self.helper = self.skills / ".devflow-helpers"
        self.helper.mkdir()
        self.rollback.created(self.backup, self.helper)
        for name in ("scripts", "references"):
            (self.helper / name).symlink_to(ROOT / "skills/devflow" / name)
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
        self.assertEqual(os.readlink(self.compatibility), str(ROOT / "skills/devflow"))

    def test_agent_write_authenticates_displaced_foreign_file(self):
        target = self.home / "agents/devflow-implementer.toml"
        target.parent.mkdir()
        target.write_bytes(b"old installed agent")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.rollback.capture(ROOT, self.skills, self.home)
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
        arguments = [str(ROOT / "scripts/install-agents.py"), "apply", str(ROOT),
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
                self.rollback.capture(ROOT, self.skills, self.home)
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

    def test_unsupported_host_refuses_without_replacement_fallback(self):
        before = os.readlink(self.compatibility)
        with mock.patch.object(sys, "platform", "unsupported"):
            with self.assertRaisesRegex(ValueError, "requires native"):
                self.rollback.exchange_function()
        self.assertEqual(os.readlink(self.compatibility), before)


if __name__ == "__main__":
    unittest.main()
