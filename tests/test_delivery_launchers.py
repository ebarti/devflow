"""Scoped installer command routing, retained legacy custody and guarded rollback."""

import hashlib
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


class DeliveryLaunchers(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "checkout with spaces"
        self.bin = self.root / "local/bin"
        self.bin.mkdir(parents=True)
        self.runtime = self.source / "runtime"
        self.entries = self.runtime / ".venv/bin"
        self.entries.mkdir(parents=True)
        (self.source / ".gitignore").write_text("runtime/.venv/\n")
        (self.runtime / "pyproject.toml").write_text(
            '[project.scripts]\ndevflow-delivery="devflow_temporal.delivery_control:main"\n'
            'devflow-delivery-mcp="devflow_temporal.delivery_mcp:main"\n')
        scripts = self.source / "scripts"
        scripts.mkdir()
        for name in ("install.sh", "install-delivery-launchers.py"):
            shutil.copyfile(ROOT / "scripts" / name, scripts / name)
        for name in ("devflow-delivery", "devflow-delivery-mcp"):
            entry = self.entries / name
            entry.write_text(f'#!{sys.executable}\nimport json,sys\n'
                             f'print(json.dumps({{"entry":{name!r},"argv":sys.argv[1:]}}))\n')
            entry.chmod(0o755)
        self.config = self.root / "service config.json"
        self.config.write_text('{"service":"preserved"}\n')
        self.config.chmod(0o600)
        self.env = dict(os.environ, DEVFLOW_PYTHON=sys.executable,
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        self.git("init", "-q")
        self.git("config", "user.name", "Fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("add", ".")
        self.git("commit", "-qm", "fixture: installed canonical root")
        spec = importlib.util.spec_from_file_location("launcher_fixture", scripts / "install-delivery-launchers.py")
        self.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.module)
        self.release = self.bin.parent / "share/devflow/releases" / self.module.REVISION
        (self.release / "scripts").mkdir(parents=True)
        for original, fixture in [("scripts/devflow", "devflow"), (".devflow-release.json", "marker.json"), ("pyproject.toml", "pyproject.toml")]:
            shutil.copyfile(ROOT / "tests/fixtures/legacy-cli-3c1363b" / fixture, self.release / original)
        (self.release / "scripts/devflow").chmod(0o755)
        (self.bin / "devflow").symlink_to(self.release / "scripts/devflow")
        self.request = {
            "schema_version": 1, "command_id": "global-1", "bin_dir": str(self.bin),
            "runtime_dir": str(self.runtime), "source_revision": self.git("rev-parse", "HEAD"),
            "source_tree": self.git("rev-parse", "HEAD^{tree}"), "config_path": str(self.config),
            "config_sha256": sha(self.config.read_bytes()),
            "entry_sha256": {name: sha((self.entries / name).read_bytes()) for name in self.module.NAMES[1:]},
            "expected_before": {name: self.module.link_state(self.bin / name) for name in self.module.NAMES},
        }
        self.request_file = self.root / "request.json"
        self.write_request()
        self.foreign = self.root / "codex"
        self.foreign.mkdir()
        for name in ("config.toml", "hooks.json", "agents.toml", "skills.txt"):
            (self.foreign / name).write_text("untouched host state " + name)

    def git(self, *args):
        return subprocess.check_output(["git", "-C", str(self.source), *args], env=self.env, text=True).strip()

    def write_request(self):
        self.request_file.write_text(json.dumps(self.request))
        self.request_file.chmod(0o600)

    def cli(self, operation, file=None, expected_hash=None):
        file = file or self.request_file
        flag = "--request" if operation in ("apply", "preflight") else "--manifest"
        return subprocess.run(["sh", str(self.source / "scripts/install.sh"), "--delivery-launchers",
                               operation, flag, str(file), "--sha256", expected_hash or sha(file.read_bytes())],
                              env=self.env, capture_output=True, text=True, check=False)

    def snapshot(self):
        return {str(p.relative_to(self.root)): ("link", os.readlink(p)) if p.is_symlink()
                else ("file", sha(p.read_bytes()), p.stat().st_mode) for p in self.root.rglob("*")
                if p.is_file() or p.is_symlink()}

    def test_original_legacy_to_canonical_route_and_immutable_rollback(self):
        (self.release / "scripts/devflow").chmod(0o555)
        before = self.snapshot()
        preview = self.cli("preflight")
        self.assertEqual(preview.returncode, 0, preview.stderr)
        self.assertEqual(before, self.snapshot())
        result = self.cli("apply")
        self.assertEqual(result.returncode, 0, result.stderr)
        response = json.loads(result.stdout)
        manifest = Path(response["manifest"])
        manifest_hash = response["manifest_sha256"]
        for name in self.module.NAMES:
            executed = subprocess.run([str(self.bin / name), "status", "--id", "preserved-run"],
                                      env=self.env, capture_output=True, text=True, check=True)
            routed = json.loads(executed.stdout)
            self.assertEqual(routed["entry"], "devflow-delivery" if name == "devflow" else name)
            self.assertEqual(routed["argv"], ["--config", str(self.config), "status", "--id", "preserved-run"])
        applied = self.snapshot()
        self.assertEqual(self.cli("apply").returncode, 0)
        self.assertEqual(applied, self.snapshot())
        self.assertEqual(json.loads(self.cli("status", manifest, manifest_hash).stdout)["state"], "applied")
        rollback = self.cli("rollback", manifest, manifest_hash)
        self.assertEqual(rollback.returncode, 0, rollback.stderr)
        retained = self.snapshot()
        self.assertEqual(self.cli("rollback", manifest, manifest_hash).returncode, 0)
        self.assertEqual(retained, self.snapshot())
        self.assertEqual(os.readlink(self.bin / "devflow"), self.request["expected_before"]["devflow"]["target"])
        self.assertFalse(os.path.lexists(self.bin / "devflow-delivery"))
        self.assertFalse(os.path.lexists(self.bin / "devflow-delivery-mcp"))
        self.assertEqual(read := self.module.read(manifest, mode=0o600), manifest.read_bytes())
        self.assertEqual(sha(read), manifest_hash)
        for key, value in before.items():
            self.assertEqual(retained[key], value, key)

    def test_foreign_alias_drift_refuse_before_any_creation(self):
        for change in ("foreign", "legacy-bytes", "marker", "entry", "config", "source", "bin-alias", "hash", "uid"):
            with self.subTest(change=change):
                # Each adverse gets a fresh clean fixture; changes are caller setup only.
                original_request = json.loads(json.dumps(self.request))
                patches = []
                altered = None
                if change == "foreign":
                    (self.bin / "devflow-delivery").symlink_to("/bin/true")
                elif change in ("legacy-bytes", "marker", "entry", "config", "source"):
                    altered = {"legacy-bytes": self.release / "scripts/devflow", "marker": self.release / ".devflow-release.json",
                               "entry": self.entries / "devflow-delivery", "config": self.config,
                               "source": self.runtime / "pyproject.toml"}[change]
                    original = altered.read_bytes()
                    altered.write_bytes(original + b"\nchanged")
                elif change == "bin-alias":
                    alias = self.root / "alias"
                    alias.symlink_to(self.bin)
                    self.request["bin_dir"] = str(alias)
                elif change == "hash":
                    self.request["config_sha256"] = "0" * 64
                else:
                    patches = [patch.object(self.module.os, "getuid", return_value=os.getuid() + 1)]
                self.write_request()

                before = self.snapshot()
                if patches:
                    with patches[0], self.assertRaises(ValueError):
                        self.module.apply(self.request)
                else:
                    for operation in ("preflight", "apply"):
                        self.assertNotEqual(self.cli(operation).returncode, 0)
                self.assertEqual(before, self.snapshot())
                self.assertFalse((self.bin.parent / "share/devflow/delivery-launchers").exists())
                if altered:
                    altered.write_bytes(original)
                if change == "foreign":
                    (self.bin / "devflow-delivery").unlink()
                if change == "bin-alias":
                    (self.root / "alias").unlink()
                self.request = original_request
                self.write_request()

    def test_legacy_permission_modes_preserve_read_only_release(self):
        entry = self.release / "scripts/devflow"
        for mode in (0o555, 0o755, 0o444, 0o777, 0o4755):
            with self.subTest(mode=oct(mode)):
                entry.chmod(mode)
                before = self.snapshot()
                preview = self.cli("preflight")
                self.assertEqual(preview.returncode == 0, mode in (0o555, 0o755), preview.stderr)
                self.assertEqual(before, self.snapshot())

    def test_lost_response_after_one_link_resumes_without_overwriting_legacy_history(self):
        original = self.module.replace_link
        calls = []
        def interrupted(*args):
            original(*args)
            calls.append(args[0].name)
            raise RuntimeError("lost response after actual first owned link")
        with patch.object(self.module, "replace_link", interrupted), self.assertRaisesRegex(RuntimeError, "lost response"):
            self.module.apply(self.request)
        self.assertEqual(calls, ["devflow"])
        journal = self.bin.parent / "share/devflow/delivery-launchers/global-1"
        before = (journal / "manifest.json").read_bytes()
        self.assertEqual(self.module.apply(self.request)["state"], "applied")
        self.assertEqual((journal / "manifest.json").read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
