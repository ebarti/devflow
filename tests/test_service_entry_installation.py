"""The normal installer exposes one service entry and preserves retired helpers."""

import contextlib
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

    def historical_helper(self, *, current_state=True):
        old = self.root / "retained-git-source"
        subprocess.run(
            [
                "git",
                "-c",
                "gc.auto=0",
                "-c",
                "maintenance.auto=false",
                "clone",
                "--no-local",
                "--no-checkout",
                str(ROOT),
                str(old),
            ],
            check=True,
            capture_output=True,
        )
        for key, value in (("gc.auto", "0"), ("maintenance.auto", "false"),
                           ("core.hooksPath", "/dev/null"), ("commit.gpgsign", "false")):
            subprocess.run(["git", "-C", str(old), "config", key, value], check=True)
        revision = "a9a2c07d086c466e94d4fd6eb73c7b7250f090f3"
        subprocess.run(
            ["git", "-C", str(old), "checkout", "--detach", revision],
            check=True,
            capture_output=True,
        )
        origin = subprocess.check_output(
            ["git", "-C", str(ROOT), "config", "--get", "remote.origin.url"],
            text=True,
        ).strip()
        subprocess.run(["git", "-C", str(old), "remote", "set-url", "origin", origin], check=True)
        if current_state:
            shutil.copyfile(
                ROOT / "skills/devflow/scripts/state.py", old / "skills/devflow/scripts/state.py"
            )
        compatibility = self.root / "owned-compat/devflow"
        compatibility.mkdir(parents=True)
        for name in ("scripts", "references"):
            (compatibility / name).symlink_to(old / "skills/devflow" / name)
        self.skills.mkdir()
        (self.skills / "devflow").symlink_to(compatibility)
        cache = self.home / "plugins/cache/devflow-local/devflow/current/skills"
        shutil.copytree(
            ROOT / "runtime/desktop/devflow-local-delivery", cache / "devflow-local-delivery"
        )
        return old, compatibility

    def test_owned_symlinked_historical_helper_upgrade_and_replay(self):
        old, compatibility = self.historical_helper()
        prior = {p.name: p.read_bytes() for p in (old / "skills/devflow/scripts").iterdir()}
        reference = os.readlink(compatibility / "references")
        root_link = os.readlink(self.skills / "devflow")
        hooks, config = self.hooks.read_bytes(), self.config.read_bytes()
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.skills / "devflow/scripts").resolve(), ROOT / "skills/devflow/scripts"
        )
        self.assertEqual(os.readlink(compatibility / "references"), reference)
        self.assertEqual(os.readlink(self.skills / "devflow"), root_link)
        self.assertEqual(
            {p.name: p.read_bytes() for p in (old / "skills/devflow/scripts").iterdir()}, prior
        )
        self.assertEqual(self.hooks.read_bytes(), hooks)
        self.assertEqual(self.config.read_bytes(), config)
        before = self.snapshot()
        replay = self.install()
        self.assertEqual(replay.returncode, 0, replay.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_complete_old_git_helper_upgrade_preserves_old_files(self):
        old, compatibility = self.historical_helper(current_state=False)
        before = {p.name: p.read_bytes() for p in (old / "skills/devflow/scripts").iterdir()}
        result = self.install()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            os.readlink(compatibility / "scripts"), str(ROOT / "skills/devflow/scripts")
        )
        self.assertEqual(
            {p.name: p.read_bytes() for p in (old / "skills/devflow/scripts").iterdir()}, before
        )

    def test_unknown_modified_helper_refuses_with_path_before_effects(self):
        old, _ = self.historical_helper()
        changed = old / "skills/devflow/scripts/github.py"
        changed.write_text("user-owned edits must remain")
        before = self.snapshot()
        result = self.install()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn(str(changed), result.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_foreign_origin_extra_file_and_aliased_helper_refuse(self):
        old, compatibility = self.historical_helper()
        original = (old / "skills/devflow/scripts/github.py").read_bytes()
        scripts = old / "skills/devflow/scripts"
        for adverse in ("origin", "extra", "alias", "hardlink", "references", "real-directory"):
            with self.subTest(adverse=adverse):
                if adverse == "origin":
                    subprocess.run(
                        ["git", "-C", str(old), "remote", "set-url", "origin", "foreign"],
                        check=True,
                    )
                elif adverse == "extra":
                    (scripts / "user.py").write_text("preserved foreign file")
                elif adverse in ("alias", "hardlink"):
                    (scripts / "github.py").unlink()
                    donor = self.root / "foreign-github.py"
                    donor.write_bytes(original)
                    if adverse == "alias":
                        (scripts / "github.py").symlink_to(donor)
                    else:
                        os.link(donor, scripts / "github.py")
                elif adverse == "references":
                    (compatibility / "references").unlink()
                    (compatibility / "references").symlink_to(ROOT / "skills/devflow/references")
                else:
                    (compatibility / "scripts").unlink()
                    shutil.copytree(scripts, compatibility / "scripts")
                before = self.snapshot()
                result = self.install()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("preserved", result.stderr)
                self.assertEqual(self.snapshot(), before)
                if adverse == "origin":
                    origin = subprocess.check_output(
                        ["git", "-C", str(ROOT), "config", "--get", "remote.origin.url"], text=True
                    ).strip()
                    subprocess.run(
                        ["git", "-C", str(old), "remote", "set-url", "origin", origin], check=True
                    )
                elif adverse == "extra":
                    (scripts / "user.py").unlink()
                elif adverse in ("alias", "hardlink"):
                    (scripts / "github.py").unlink()
                    (scripts / "github.py").write_bytes(original)
                    donor.unlink()
                elif adverse == "references":
                    (compatibility / "references").unlink()
                    (compatibility / "references").symlink_to(old / "skills/devflow/references")
                else:
                    shutil.rmtree(compatibility / "scripts")
                    (compatibility / "scripts").symlink_to(scripts)

    def service_modules(self):
        spec = importlib.util.spec_from_file_location(
            "helper_service", ROOT / "scripts/install-service-entry.py"
        )
        service = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(service)
        rollback = service.load("install-rollback")
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            rollback.capture(ROOT, self.skills, self.home)
        return service, rollback, Path(output.getvalue().strip())

    def test_migration_staging_collision_preserves_foreign_path(self):
        spec = importlib.util.spec_from_file_location(
            "stage_migration", ROOT / "scripts/install-migration.py"
        )
        migration = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration)
        self.skills.mkdir()
        stage = self.skills / (".devflow-helper-pointer-" + str(os.getpid()))
        for kind in ("file", "symlink"):
            with self.subTest(kind=kind):
                if kind == "file":
                    stage.write_bytes(b"foreign staging bytes")
                else:
                    stage.symlink_to("foreign-target")
                helper = self.skills / (".owned-helper-" + kind)
                rollback = mock.Mock()
                with self.assertRaises(FileExistsError):
                    migration.apply(
                        {"links": [self.skills / "devflow"], "helper": helper},
                        ROOT, self.skills, mock.Mock(), rollback, "unused-backup",
                    )
                rollback.effect.assert_not_called()
                self.assertTrue(os.path.lexists(stage), "foreign staging path was removed")
                if kind == "file":
                    self.assertEqual(stage.read_bytes(), b"foreign staging bytes")
                else:
                    self.assertEqual(os.readlink(stage), "foreign-target")
                stage.unlink()
                shutil.rmtree(helper)

    def test_helper_exchange_rolls_back_without_missing_helper_interval(self):
        _, compatibility = self.historical_helper()
        before = self.snapshot()
        service, rollback, backup = self.service_modules()
        path = compatibility / "scripts"
        snapshot = json.loads((backup / "snapshot.json").read_text())
        self.assertIn(str(path), snapshot)
        original_load, original_run, original_exchange = (
            service.load,
            subprocess.run,
            rollback.exchange,
        )
        observations = []

        def exchange(left, right):
            self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
            original_exchange(left, right)
            self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())
            observations.append(str(right))

        def run(args, **kwargs):
            if "apply" in args and any(str(a).endswith("install-agents.py") for a in args):
                raise subprocess.CalledProcessError(1, args)
            return original_run(args, **kwargs)

        with (
            mock.patch.object(
                service,
                "load",
                side_effect=lambda name: (
                    rollback if name == "install-rollback" else original_load(name)
                ),
            ),
            mock.patch.object(subprocess, "run", side_effect=run),
            mock.patch.object(rollback, "exchange", side_effect=exchange),
        ):
            with self.assertRaises(subprocess.CalledProcessError):
                service.install(self.skills, self.home, False, backup)
            rollback.restore(backup)
        self.assertEqual(observations, [str(path), str(path)])
        self.assertEqual(self.snapshot(), before)

    def test_concurrent_helper_pointer_replacement_preserves_foreign_bytes(self):
        _, compatibility = self.historical_helper()
        service, rollback, backup = self.service_modules()
        path = compatibility / "scripts"
        original_load, original_exchange = service.load, rollback.exchange
        foreign = b"foreign replacement at native exchange"

        def exchange(left, right):
            path.unlink()
            path.write_bytes(foreign)
            original_exchange(left, right)

        with (
            mock.patch.object(
                service,
                "load",
                side_effect=lambda name: (
                    rollback if name == "install-rollback" else original_load(name)
                ),
            ),
            mock.patch.object(rollback, "exchange", side_effect=exchange),
        ):
            with self.assertRaisesRegex(ValueError, "forward installer drift preserved"):
                service.install(self.skills, self.home, False, backup)
        with self.assertRaisesRegex(ValueError, "retained backup"):
            rollback.restore(backup)
        self.assertTrue(backup.exists())
        captured = list(backup.glob("forward-*"))
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0].read_bytes(), foreign)
        self.assertTrue((self.skills / "devflow/scripts/state.py").is_file())

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
