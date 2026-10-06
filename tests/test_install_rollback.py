"""Observe rollback's real filesystem boundaries without running a service."""

import contextlib
import importlib.util
import io
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

    def test_unsupported_host_refuses_without_replacement_fallback(self):
        before = os.readlink(self.compatibility)
        with mock.patch.object(sys, "platform", "unsupported"):
            with self.assertRaisesRegex(ValueError, "requires native"):
                self.rollback.exchange_function()
        self.assertEqual(os.readlink(self.compatibility), before)


if __name__ == "__main__":
    unittest.main()
