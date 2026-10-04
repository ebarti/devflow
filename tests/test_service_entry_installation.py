"""The normal installer exposes one service entry and preserves retired helpers."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ServiceEntryInstallation(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.skills, self.home = self.root / "skills", self.root / "codex"
        self.home.mkdir()
        self.config = self.home / "config.toml"
        self.config.write_text("foreign desktop settings unchanged\n")
        self.hooks = self.home / "hooks.json"
        self.hooks.write_text('{"hooks":{"Stop":[{"hooks":[{"command":"foreign"}]}]}}')
        self.env = dict(os.environ, DEVFLOW_PYTHON=sys.executable)

    def install(self):
        return subprocess.run(
            ["sh", str(ROOT / "scripts/install.sh"), str(self.skills), str(self.home)],
            env=self.env,
            capture_output=True,
            text=True,
            check=False,
        )

    def snapshot(self):
        return {
            str(p.relative_to(self.root)): ("link", os.readlink(p))
            if p.is_symlink()
            else ("file", p.read_bytes(), p.stat().st_mode)
            for p in self.root.rglob("*")
            if p.is_symlink() or p.is_file()
        }

    def test_fresh_service_install_and_replay_do_not_restore_agent_skills_or_hooks(
        self,
    ):
        hooks, config = self.hooks.read_bytes(), self.config.read_bytes()
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.skills / "devflow-local-delivery/SKILL.md").exists())
        self.assertFalse((self.skills / "devflow/SKILL.md").exists())
        self.assertTrue((self.skills / "devflow/scripts/state.py").exists())
        self.assertEqual(len(list((self.home / "agents").glob("*.toml"))), 4)
        self.assertEqual(self.hooks.read_bytes(), hooks)
        self.assertEqual(self.config.read_bytes(), config)
        self.assertFalse((self.home / ".devflow-hook.py").exists())
        self.assertFalse((self.home / ".devflow-install.json").exists())
        self.assertFalse((self.home / "LaunchAgents").exists())
        before = self.snapshot()
        replay = self.install()
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_existing_plugin_and_retained_old_helper_realpaths_remain_exact(self):
        old = self.root / "retained-authentic"
        shutil.copytree(ROOT / "skills/devflow", old)
        (old / "SKILL.md").unlink()
        reference = old / "references/implementation-worker.md"
        reference.write_text("Retained instructions for already-running work\n")
        retained_reference = reference.read_bytes()
        self.skills.mkdir()
        (self.skills / "devflow").symlink_to(old)
        cache = (
            self.home
            / "plugins/cache/devflow-local/devflow/current/skills/devflow-local-delivery"
        )
        shutil.copytree(ROOT / "runtime/desktop/devflow-local-delivery", cache)
        before = (self.skills / "devflow/scripts/state.py").resolve()
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((self.skills / "devflow/scripts/state.py").resolve(), before)
        self.assertEqual(reference.read_bytes(), retained_reference)
        self.assertFalse((self.skills / "devflow-local-delivery").exists())
        self.assertTrue((cache / "SKILL.md").exists())
        snapshot = self.snapshot()
        self.assertEqual(self.install().returncode, 0)
        self.assertEqual(self.snapshot(), snapshot)

    def test_legacy_registration_and_foreign_helpers_refuse_before_effects(self):
        for adverse in ("old-skill", "pin", "helper", "service"):
            with self.subTest(adverse=adverse):
                if self.skills.exists():
                    shutil.rmtree(self.skills)
                self.skills.mkdir()
                if adverse == "pin":
                    (self.home / ".devflow-install.json").write_text("retained old pin")
                elif adverse == "old-skill":
                    (self.skills / "devflow").symlink_to(ROOT / "skills/devflow")
                elif adverse == "helper":
                    (self.skills / "devflow").mkdir()
                    (self.skills / "devflow/scripts").mkdir()
                    (self.skills / "devflow/scripts/state.py").write_text(
                        "foreign helper"
                    )
                else:
                    (self.skills / "devflow-local-delivery").symlink_to(
                        ROOT / "skills/devflow"
                    )
                before = self.snapshot()
                self.assertNotEqual(self.install().returncode, 0)
                self.assertEqual(self.snapshot(), before)
                (self.home / ".devflow-install.json").unlink(missing_ok=True)


if __name__ == "__main__":
    unittest.main()
