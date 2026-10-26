"""Production adoption through public entry points, without touching a real install.

The expensive fixture requires DEVFLOW_PRODUCTION_FIXTURE_PARENT to name an
owned durable test directory. A system temporary HOME would correctly be refused
by the production contract. Its disposable *source* remains in OS temp storage.
"""

import importlib.util
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


class InstallationDependencies(unittest.TestCase):
    def setUp(self):
        self.paths = module("installation-paths")

    def test_indirect_temporary_and_managed_dependencies_are_refused(self):
        for path in ("/private/tmp/owned/config.json", "/tmp/owned/source",
                     "/Users/fixture/.codex/worktrees/abcd/source",
                     "/Users/fixture/.codex-workspaces/source"):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, "temporary or managed|canonical"):
                self.paths.durable(path)

    def test_development_cannot_target_production_registrations(self):
        with patch.dict(os.environ, DEVFLOW_INSTALL_MODE="development"):
            for relative in (".agents/skills", ".codex", ".local/bin", "Library/LaunchAgents"):
                with self.subTest(relative=relative), self.assertRaisesRegex(ValueError, "production"):
                    self.paths.development(Path.home() / relative)

    def test_custom_managed_roots_are_resolved_before_dependency_checks(self):
        with patch.dict(os.environ, DEVFLOW_MANAGED_WORKTREE_ROOTS='["/Users/fixture/custom-cleanup"]'):
            with self.assertRaisesRegex(ValueError, "configured managed worktree root"):
                self.paths.durable("/Users/fixture/custom-cleanup/owned/runtime")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            alias = root / "alias"
            actual = root / "managed"
            actual.mkdir()
            alias.symlink_to(actual)
            with patch.dict(os.environ, DEVFLOW_MANAGED_WORKTREE_ROOTS=json.dumps([str(alias)])):
                with self.assertRaisesRegex(ValueError, "configured managed worktree root"):
                    self.paths.durable(actual / "runtime")

    def test_production_rejects_shared_git_storage_before_runtime_provisioning(self):
        # Use a lexical durable name with actual fixture storage replaced only
        # at the path-policy boundary; this test exercises Git's real alternates.
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary).resolve() / "shared"
            subprocess.run(["git", "clone", "--shared", str(ROOT), str(source)],
                           check=True, capture_output=True)
            with patch.object(self.paths, "durable", side_effect=lambda value, *args: Path(value)):
                with self.assertRaisesRegex(ValueError, "external object store"):
                    self.paths.source(source)

    def test_ambient_git_storage_redirects_do_not_become_installation_dependencies(self):
        with patch.dict(os.environ, GIT_OBJECT_DIRECTORY="/private/tmp/disposable-objects",
                        GIT_ALTERNATE_OBJECT_DIRECTORIES="/private/tmp/other-objects",
                        GIT_DIR="/private/tmp/other-repository/.git"):
            observed = self.paths.git(ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir")
            self.assertEqual(Path(observed), Path(subprocess.check_output(
                ["git", "-C", str(ROOT), "rev-parse", "--path-format=absolute", "--git-common-dir"],
                text=True, env=self.paths.git_environment()).strip()))

    def test_outer_identity_matches_existing_volume_proof_and_bounds_renumbering(self):
        identity = module("production-entry")
        with tempfile.TemporaryDirectory() as temporary:
            folder = Path(temporary).resolve()
            actual = identity.directory_identity(folder)
            proof = subprocess.check_output([sys.executable, "-I", "-B", "-c",
                "import json,sys;sys.path.insert(0,sys.argv[1]);"
                "from devflow_temporal.delivery_runtime_installation import _directory;"
                "print(json.dumps(_directory(sys.argv[2])))", str(ROOT / "runtime/src"), str(folder)],
                text=True)
            self.assertEqual(actual, json.loads(proof))
        before = {**actual, "volume_uuid": "owned-volume", "birthtime": 123.0}
        renumbered = {**before, "device": before["device"] + 1}
        self.assertTrue(identity.same_identity(before, renumbered))
        for changed in ({**renumbered, "volume_uuid": "other-volume"},
                        {**renumbered, "birthtime": 124.0},
                        {**renumbered, "inode": before["inode"] + 1}):
            self.assertFalse(identity.same_identity(before, changed))
        self.assertFalse(identity.same_identity(
            [before["device"], before["inode"], before["uid"]], renumbered))


@unittest.skipUnless(os.environ.get("DEVFLOW_PRODUCTION_FIXTURE_PARENT"),
                     "set an owned durable fixture parent to run real production provisioning")
class ProductionInstallation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture_parent = Path(os.environ["DEVFLOW_PRODUCTION_FIXTURE_PARENT"]).resolve(strict=True)
        cls.fixture = tempfile.TemporaryDirectory(prefix="production-install-", dir=fixture_parent)
        cls.addClassCleanup(cls.fixture.cleanup)
        cls.home = Path(cls.fixture.name).resolve()
        cls.setup = tempfile.TemporaryDirectory(prefix="devflow-disposable-origin-")
        cls.addClassCleanup(cls.setup.cleanup)
        cls.origin = Path(cls.setup.name).resolve() / "source"
        releases = cls.releases = cls.home / "releases.git"
        subprocess.run(["git", "clone", "--bare", "--no-local", str(ROOT), str(releases)],
                       check=True, capture_output=True)
        subprocess.run(["git", "clone", "--no-local", str(ROOT), str(cls.origin)],
                       check=True, capture_output=True)
        subprocess.run(["git", "-C", str(cls.origin), "remote", "set-url", "origin", str(releases)],
                       check=True, capture_output=True)
        # Clone the exact working candidate into the fixture without committing or
        # changing the shared checkout. Both releases and setup use this snapshot.
        paths = ("scripts", "runtime/src/devflow_temporal/delivery_config.py",
                 "runtime/src/devflow_temporal/delivery_installation_owner.py")
        patch_bytes = subprocess.check_output(["git", "-C", str(ROOT), "diff", "--binary", "HEAD", "--", *paths])
        if patch_bytes:
            subprocess.run(["git", "-C", str(cls.origin), "apply", "--binary"], input=patch_bytes,
                           check=True, capture_output=True)
        new_owner = ROOT / paths[-1]
        fixture_owner = cls.origin / paths[-1]
        if new_owner.exists() and not fixture_owner.exists():
            shutil.copy2(new_owner, fixture_owner)
        subprocess.run(["git", "-C", str(cls.origin), "add", "--", *paths], check=True, capture_output=True)
        if subprocess.check_output(["git", "-C", str(cls.origin), "diff", "--cached", "--name-only"]):
            subprocess.run(["git", "-C", str(cls.origin), "-c", "user.name=Fixture",
                            "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false",
                            "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture candidate"],
                           check=True, capture_output=True)
        subprocess.run(["git", "-C", str(cls.origin), "push", "origin", "HEAD:refs/heads/fixture-candidate"],
                       check=True, capture_output=True)
        subprocess.run(["git", "--git-dir", str(releases), "symbolic-ref", "HEAD", "refs/heads/fixture-candidate"],
                       check=True, capture_output=True)
        # The controller used during setup is deliberately disposable too.
        # The installed runtime must bind to its durable base interpreter.
        setup_venv = Path(cls.setup.name) / "setup-venv"
        subprocess.run([sys.executable, "-m", "venv", str(setup_venv)], check=True, capture_output=True)
        cls.setup_python = setup_venv / "bin/python"
        cls.env = {**os.environ, "HOME": str(cls.home), "DEVFLOW_PYTHON": str(cls.setup_python),
                   "DEVFLOW_INSTALL_MODE": "production", "UV_LINK_MODE": "copy",
                   "UV_CACHE_DIR": str(cls.home / "uv-cache"),
                   "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
        cls.env.pop("CODEX_HOME", None)
        cls.target = cls.home / ".local/share/devflow/developer-workflow"
        cls.skills = cls.home / "skills"
        cls.codex = cls.home / "codex"
        # Reproduce the old successful registration first. The production
        # adopter must migrate every link and copied-agent ownership record.
        trial = subprocess.run(["sh", str(cls.origin / "scripts/install.sh"), "--development",
                                str(cls.skills), str(cls.codex)],
                               env=cls.env, text=True, capture_output=True)
        if trial.returncode:
            raise AssertionError(trial.stdout + "\n" + trial.stderr)
        result = subprocess.run(["sh", str(cls.origin / "scripts/install.sh"), "--force",
                                 str(cls.skills), str(cls.codex)], env=cls.env,
                                text=True, capture_output=True)
        if result.returncode:
            raise AssertionError(result.stdout + "\n" + result.stderr)
        cls.install_output = result.stdout
        # Seed actual admitted run state while the setup checkout still exists.
        # The owner repository and every state file are private fixture data.
        repository = cls.home / "owned-repository"
        repository.mkdir(mode=0o700)
        subprocess.run(["git", "init", "-q", str(repository)], check=True)
        subprocess.run(["git", "-C", str(repository), "config", "user.name", "Fixture"], check=True)
        subprocess.run(["git", "-C", str(repository), "config", "user.email", "fixture@example.invalid"], check=True)
        subprocess.run(["git", "-C", str(repository), "remote", "add", "origin", str(cls.releases)], check=True)
        (repository / "README.md").write_text("fixture\n")
        subprocess.run(["git", "-C", str(repository), "add", "README.md"], check=True)
        subprocess.run(["git", "-C", str(repository), "commit", "-qm", "fixture"], check=True, env=cls.env)
        sha = subprocess.check_output(["git", "-C", str(repository), "rev-parse", "HEAD"], text=True).strip()
        state = cls.home / "owned-state"
        state.mkdir(mode=0o700)
        cls.config = cls.home / "owner.json"
        # Reserve a non-listening port throughout the test so the real dispatcher
        # cannot connect to any existing Temporal service or start fixture work.
        cls.unavailable_temporal = socket.socket()
        cls.unavailable_temporal.bind(("127.0.0.1", 0))
        cls.addClassCleanup(cls.unavailable_temporal.close)
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            cls.port = listener.getsockname()[1]
        cls.config.write_text(json.dumps({"version": 1, "state_root": str(state),
            "tracking_db": str(state / "tracking.db"),
            "execution_registry": str(state / "ownership.sqlite3"),
            "helpers_dir": str(cls.target / "skills/devflow/scripts"),
            "codex_bin": "/usr/bin/false", "provider": "fake",
            "temporal_address": f"127.0.0.1:{cls.unavailable_temporal.getsockname()[1]}",
            "dashboard_url": f"http://127.0.0.1:{cls.port}",
            "repositories": {"fixture": {"source_path": str(repository),
                "origin_url": str(cls.releases), "github_repo": "example/fixture",
                "base_ref": "HEAD", "expected_base_sha": sha, "allowed_paths": ["README.md"]}},
            "roles": {key: {"model": "fixture", "effort": "low"} for key in
                ("implement", "review", "verify")}}))
        cls.config.chmod(0o600)
        seed = r'''
import json, sys
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
s = DeliveryStore(DeliveryConfig.load(Path(sys.argv[1])))
r = {"command_id": "owned-command", "run_id": "owned-run", "work_id": "owned-work",
     "issue_url": "https://github.com/example/fixture/issues/3", "repository_key": "fixture",
     "goal": "Change the fixture", "accepted_plan": "Make one bounded edit and test it.",
     "base_ref": "HEAD", "branch": "feat/fixture", "authorized_endpoint": "published_unmerged"}
s.submit(r)
s.project("owned-run", phase="blocked", execution_state="blocked", event_type="fixture_checkpoint",
          message="Owned checkpoint", candidate={"head": sys.argv[2], "id": "a" * 64}, iteration=1)
print(json.dumps({"spec": s.submitted_spec("owned-run"), "events": s.events("owned-run"),
                  "detail": s.detail("owned-run"), "outbox": s.pending_starts()}, sort_keys=True))
'''
        seeded = subprocess.run([str(cls.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                                 seed, str(cls.config), sha], env=cls.env, text=True, capture_output=True)
        if seeded.returncode:
            raise AssertionError(seeded.stdout + "\n" + seeded.stderr)
        cls.persisted = json.loads(seeded.stdout)
        if not cls.persisted["outbox"] or not cls.persisted["spec"]["policy"].get("max_repairs"):
            raise AssertionError("fixture did not persist its start journal and repair budget")
        # Remove the entire disposable setup ancestor before any installed reads.
        shutil.rmtree(cls.setup.name)
        cls.env["DEVFLOW_PYTHON"] = str(cls.target / "runtime/.venv/bin/python")

    def test_cli_imports_and_api_restart_after_setup_checkout_removed(self):
        target = self.target
        self.assertFalse(self.origin.exists())
        self.assertFalse(self.setup_python.exists())
        self.assertTrue((target / ".git").is_dir())
        self.assertFalse((target / ".git/objects/info/alternates").exists())
        for path in (self.skills / "devflow/scripts", self.skills / "devflow/references",
                     self.skills / "devflow-local-delivery"):
            self.assertTrue(path.resolve(strict=True).is_relative_to(target))
        cli = target / "runtime/.venv/bin/devflow-delivery"
        result = subprocess.run([str(cli), "--help"], env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        port = self.port
        config = self.config
        # Exercise the real public app factory and socket twice. No Temporal or
        # provider is substituted; the owned non-listening Temporal port prevents
        # the real pending-start dispatcher from reaching an external service.
        command = [str(target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            "import sys,uvicorn;from pathlib import Path;"
            "from devflow_temporal.delivery_api import create_app;"
            "uvicorn.run(create_app(Path(sys.argv[1])),host='127.0.0.1',port=int(sys.argv[2]))",
            str(config), str(port)]
        for _ in range(2):
            with tempfile.TemporaryFile() as logs:
                process = subprocess.Popen(command, env=self.env, stdout=logs, stderr=logs)
                try:
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        if process.poll() is not None:
                            logs.seek(0)
                            self.fail(logs.read().decode())
                        try:
                            with urlopen(f"http://127.0.0.1:{port}/api/session", timeout=1) as response:
                                self.assertEqual(response.status, 200)
                                self.assertIn("devflow_session", response.headers.get("Set-Cookie", ""))
                            with urlopen(f"http://127.0.0.1:{port}/api/runs/owned-run", timeout=2) as response:
                                actual = json.load(response)
                            self.assertEqual(actual["run"]["candidate"], self.persisted["detail"]["candidate"])
                            self.assertEqual(actual["events"], self.persisted["events"])
                            self.assertEqual(actual["run"]["candidate"]["policy_digest"],
                                             self.persisted["spec"]["policy_digest"])
                            self.assertTrue(any(event["type"] == "fixture_checkpoint" for event in actual["events"]))
                            break
                        except (URLError, TimeoutError):
                            time.sleep(0.1)
                    else:
                        self.fail("owned API did not become ready")
                finally:
                    process.terminate()
                    process.wait(timeout=10)
        # Independently reload the durable admission and pending-start journal.
        readback = r'''
import json, sys
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
s = DeliveryStore(DeliveryConfig.load(Path(sys.argv[1])))
print(json.dumps({"spec": s.submitted_spec("owned-run"), "events": s.events("owned-run"),
                  "detail": s.detail("owned-run"), "outbox": s.pending_starts()}, sort_keys=True))
'''
        loaded = subprocess.run([str(target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                                 readback, str(config)], env=self.env, text=True, capture_output=True)
        self.assertEqual(loaded.returncode, 0, loaded.stderr)
        self.assertEqual(json.loads(loaded.stdout), self.persisted)
        replay = subprocess.run(["sh", str(target / "scripts/install.sh"),
                                 str(self.skills), str(self.codex)], env=self.env,
                                text=True, capture_output=True)
        self.assertEqual(replay.returncode, 0, replay.stderr)

    def test_failed_update_preserves_previous_owner_and_private_state(self):
        marker = self.home / "preserved-budget.json"
        marker.write_bytes(b'{"grant":"owned","remaining":7}\n')
        before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
        # Publish a failing release only into this fixture's bare origin. The
        # update crosses the source/environment boundary before its real import
        # proof rejects the release and recovers the previous owner.
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "candidate"
            subprocess.run(["git", "clone", str(self.releases), str(candidate)],
                           check=True, capture_output=True)
            (candidate / "runtime/src/devflow_temporal/delivery_api.py").write_text(
                'raise RuntimeError("owned failing release import")\n')
            project = candidate / "runtime/pyproject.toml"
            project.write_text(project.read_text().replace('  "pyyaml>=6,<7",\n', ""))
            # Change the frozen dependency graph directly: PyYAML is only a
            # direct dependency here, and uv sync must actually remove it. This
            # avoids requiring registry metadata just to construct the release.
            lock = candidate / "runtime/uv.lock"
            locked = lock.read_text()
            self.assertEqual(locked.count('    { name = "pyyaml" },\n'), 1)
            self.assertEqual(locked.count('    { name = "pyyaml", specifier = ">=6,<7" },\n'), 1)
            lock.write_text(locked.replace('    { name = "pyyaml" },\n', "")
                            .replace('    { name = "pyyaml", specifier = ">=6,<7" },\n', ""))
            for key, value in (("user.name", "Fixture"), ("user.email", "fixture@example.invalid")):
                subprocess.run(["git", "-C", str(candidate), "config", key, value], check=True)
            for arguments in (("add", "."), ("commit", "-qm", "fixture: failing release"),
                              ("tag", "owned-failing-release"), ("push", "origin", "owned-failing-release")):
                subprocess.run(["git", "-C", str(candidate), *arguments],
                               check=True, capture_output=True)
        python = self.target / "runtime/.venv/bin/python"
        before_packages = subprocess.check_output([str(python), "-I", "-c",
            "import importlib.metadata,json;print(json.dumps(sorted((d.metadata['Name'],d.version) "
            "for d in importlib.metadata.distributions())))"], env=self.env, text=True)
        subprocess.run([str(python), "-I", "-c", "import yaml"], env=self.env, check=True)
        tools = self.home / "offline-tools"
        tools.mkdir(mode=0o700)
        uv = tools / "uv"
        real_uv = shutil.which("uv", path=self.env["PATH"])
        self.assertIsNotNone(real_uv)
        uv.write_text('#!/bin/sh\n' + repr(real_uv) + ' "$@" || exit $?\n'
                      + repr(str(python)) + ' -I -c "import yaml" >/dev/null 2>&1 || '
                      + ': > "$HOME/candidate-dependency-removed"\n')
        uv.chmod(0o755)
        offline_env = {**self.env, "UV_OFFLINE": "1", "PATH": str(tools) + os.pathsep + self.env["PATH"]}
        result = subprocess.run(["sh", str(self.target / "scripts/update.sh"),
                                 "owned-failing-release", str(self.skills), str(self.codex)],
                                env=offline_env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("owned failing release import", result.stderr)
        self.assertTrue((self.home / "candidate-dependency-removed").exists(),
                        "candidate provisioning did not actually remove the old dependency")
        subprocess.run([str(python), "-I", "-c", "import yaml"], env=offline_env, check=True)
        restored_packages = subprocess.check_output([str(python), "-I", "-c",
            "import importlib.metadata,json;print(json.dumps(sorted((d.metadata['Name'],d.version) "
            "for d in importlib.metadata.distributions())))"], env=offline_env, text=True)
        self.assertEqual(restored_packages, before_packages)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target),
                                                 "rev-parse", "HEAD"]), before)
        self.assertEqual(marker.read_bytes(), b'{"grant":"owned","remaining":7}\n')
        self.assertEqual(json.loads((self.target.parent / ".production-update.json").read_text())["phase"],
                         "rolled_back")
        result = subprocess.run([str(self.target / "runtime/.venv/bin/devflow-delivery"), "--help"],
                                env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_unfinished_install_has_one_owner_and_retains_state_on_rollback(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            subprocess.run(["git", "clone", str(self.releases), str(source)],
                           check=True, capture_output=True)
            home = self.home / "interrupted-home"
            home.mkdir(mode=0o700)
            tools = home / "tools"
            tools.mkdir(mode=0o700)
            entered = home / "provisioning-entered"
            uv = tools / "uv"
            uv.write_text('#!/bin/sh\nprintf "%s\\n" "$$" > "$HOME/provisioning-child"\n'
                          ': > "$HOME/provisioning-entered"\nexec /bin/sleep 60\n')
            uv.chmod(0o755)
            env = {**self.env, "HOME": str(home), "PATH": str(tools) + os.pathsep + self.env["PATH"]}
            command = ["sh", str(source / "scripts/install.sh"), str(home / "skills"), str(home / "codex")]
            with tempfile.TemporaryFile() as logs:
                process = subprocess.Popen(command, env=env, stdout=logs, stderr=logs, start_new_session=True)
                try:
                    deadline = time.monotonic() + 15
                    while not entered.exists() and process.poll() is None and time.monotonic() < deadline:
                        time.sleep(0.05)
                    self.assertTrue(entered.exists(), "the owned provisioning boundary was not reached")
                    concurrent = subprocess.run(command, env=env, text=True, capture_output=True, timeout=10)
                    self.assertNotEqual(concurrent.returncode, 0)
                    self.assertIn("temporarily unavailable", concurrent.stderr.lower())
                    # Kill only the controller. A still-running provisioner
                    # must retain the exclusive lease until its real exit.
                    os.kill(process.pid, signal.SIGKILL)
                    process.wait(timeout=10)
                    os.kill(int((home / "provisioning-child").read_text()), 0)
                    rejected = subprocess.run(["sh", str(source / "scripts/install.sh"), "--rollback"],
                        env=env, text=True, capture_output=True, timeout=10)
                    self.assertNotEqual(rejected.returncode, 0)
                    self.assertIn("temporarily unavailable", rejected.stderr.lower())
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=10)
            target = home / ".local/share/devflow/developer-workflow"
            allowance = target / "preserved-allowance.json"
            allowance.write_bytes(b'{"owner":"fixture","remaining":7}\n')
            result = subprocess.run(["sh", str(source / "scripts/install.sh"), "--rollback"],
                                    env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            state = json.loads((target.parent / ".production-install.json").read_text())
            self.assertEqual(state["phase"], "rolled_back")
            self.assertFalse(target.exists())
            self.assertEqual((Path(state["retained"]) / allowance.name).read_bytes(),
                             b'{"owner":"fixture","remaining":7}\n')

    def test_retained_recovery_command_survives_setup_interpreter_deletion(self):
        before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
        paths = module("installation-paths")
        published = paths.published_entry(self.target, "devflow-delivery")
        with tempfile.TemporaryDirectory() as temporary:
            setup = Path(temporary)
            candidate = setup / "candidate"
            subprocess.run(["git", "clone", str(self.releases), str(candidate)], check=True, capture_output=True)
            for key, value in (("user.name", "Fixture"), ("user.email", "fixture@example.invalid")):
                subprocess.run(["git", "-C", str(candidate), "config", key, value], check=True)
            subprocess.run(["git", "-C", str(candidate), "commit", "--allow-empty", "-qm", "recovery candidate"],
                           env=self.env, check=True, capture_output=True)
            subprocess.run(["git", "-C", str(candidate), "tag", "recovery-crash"], check=True)
            subprocess.run(["git", "-C", str(candidate), "push", "origin", "recovery-crash"],
                           check=True, capture_output=True)
            setup_venv = setup / "controller-venv"
            subprocess.run([sys.executable, "-m", "venv", str(setup_venv)], check=True, capture_output=True)
            setup_python = setup_venv / "bin/python"
            crash = r'''
import importlib.util, os, sys
from pathlib import Path
source = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("update_production", source / "scripts/update-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = m.load
def load(name, *args, **kwargs):
    loaded = original(name, *args, **kwargs)
    if name == "install-production": loaded.provision = lambda target: os._exit(88)
    return loaded
m.load = load
m.update(["recovery-crash", sys.argv[2], sys.argv[3]])
'''
            crashed = subprocess.run([str(setup_python), "-B", "-c", crash, str(self.target),
                                      str(self.skills), str(self.codex)], env=self.env,
                                     text=True, capture_output=True)
            self.assertEqual(crashed.returncode, 88, crashed.stderr)
            state = json.loads((self.target.parent / ".production-update.json").read_text())
            recovery = state["recovery_command"]
            self.assertFalse(Path(recovery[0]).resolve().is_relative_to(setup.resolve()))
            blocked = subprocess.run([str(published), "--help"], env=self.env,
                                     text=True, capture_output=True)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("update is unfinished", blocked.stderr)
            direct = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                "from pathlib import Path;from devflow_temporal.delivery_config import DeliveryConfig;"
                "import sys;DeliveryConfig.load(Path(sys.argv[1]))", str(self.config)],
                env=self.env, text=True, capture_output=True)
            self.assertNotEqual(direct.returncode, 0)
            self.assertIn("update is unfinished", direct.stderr)
        self.assertFalse(setup_python.exists())
        tools = self.home / "no-network-tools"
        tools.mkdir(mode=0o700)
        uv = tools / "uv"
        uv.write_text("#!/bin/sh\nexit 99\n")
        uv.chmod(0o755)
        env = {**self.env, "UV_OFFLINE": "1", "PATH": str(tools) + os.pathsep + self.env["PATH"]}
        restored = subprocess.run(recovery, env=env, text=True, capture_output=True)
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"]), before)
        state = json.loads((self.target.parent / ".production-update.json").read_text())
        self.assertEqual(state["phase"], "rolled_back")
        cli = subprocess.run([str(self.target / "runtime/.venv/bin/devflow-delivery"), "--help"],
                             env=env, text=True, capture_output=True)
        self.assertEqual(cli.returncode, 0, cli.stderr)

    def test_concurrent_configuration_loads_keep_lifetime_update_exclusion(self):
        # Exercise the public SDK load in independent processes. They retain
        # ownership before the subsequent store access, closing load→admit races.
        program = r'''
import sys, time
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_store import DeliveryStore
config = DeliveryConfig.load(Path(sys.argv[1]))
print("loaded", flush=True)
while not Path(sys.argv[2]).exists(): time.sleep(0.01)
assert DeliveryStore(config).detail("owned-run")["run_id"] == "owned-run"
'''
        release = self.home / "release-config-readers"
        processes = [subprocess.Popen([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            program, str(self.config), str(release)], env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(6)]
        try:
            for process in processes:
                self.assertEqual(process.stdout.readline().strip(), "loaded")
            before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
            blocked = subprocess.run(["sh", str(self.target / "scripts/update.sh"),
                "owned-missing-release", str(self.skills), str(self.codex)],
                env=self.env, text=True, capture_output=True, timeout=10)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("temporarily unavailable", blocked.stderr.lower())
            self.assertEqual(subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"]), before)
        finally:
            release.touch()
            for process in processes:
                out, err = process.communicate(timeout=20)
                self.assertEqual(process.returncode, 0, out + err)

    def test_store_birth_precedes_first_write_and_missing_birth_store_stays_unknown(self):
        paths = module("installation-paths")
        config = self.home / "cold-owner.json"
        raw = json.loads(self.config.read_text())
        state = self.home / "cold-state"
        state.mkdir(mode=0o700)
        database = state / "tracking.db"
        raw.update(state_root=str(state), tracking_db=str(database),
                   execution_registry=str(state / "ownership.sqlite3"))
        config.write_text(json.dumps(raw))
        config.chmod(0o600)
        python = str(self.target / "runtime/.venv/bin/python")
        program = ("from pathlib import Path;from devflow_temporal.delivery_config import DeliveryConfig;"
                   "import sys;config=DeliveryConfig.load(Path(sys.argv[1]));")
        observed = subprocess.run([python, "-I", "-B", "-c", program, str(config)],
                                  env=self.env, text=True, capture_output=True)
        self.assertEqual(observed.returncode, 0, observed.stderr)
        with patch.dict(os.environ, self.env):
            inventory = paths.read_inventory(self.target)
            self.assertNotIn(str(database), inventory["store_births"])
            self.assertFalse(database.exists())
            paths.require_update_inventory(self.target)
        written = subprocess.run([python, "-I", "-B", "-c",
            program + "from devflow_temporal.delivery_store import DeliveryStore;DeliveryStore(config)", str(config)],
            env=self.env, text=True, capture_output=True)
        self.assertEqual(written.returncode, 0, written.stderr)
        with patch.dict(os.environ, self.env):
            self.assertIn(str(database), paths.read_inventory(self.target)["store_births"])
            self.assertTrue(database.exists())
            retained = state / "retained.db"
            database.rename(retained)
            try:
                with self.assertRaisesRegex(ValueError, "absence does not prove no admitted work"):
                    paths.require_update_inventory(self.target)
            finally:
                retained.rename(database)

        failed_config = self.home / "failed-first-write.json"
        failed_database = state / "failed-first-write.db"
        raw["tracking_db"] = str(failed_database)
        failed_config.write_text(json.dumps(raw))
        failed_config.chmod(0o600)
        # Crash before the real state helper's first O_CREAT for this database,
        # not a success substitute. Its intent must survive with no SQLite file.
        failed = subprocess.run([python, "-I", "-B", "-c", program + r'''
import os
original = os.open
def opened(path, flags, *args, **kwargs):
    if str(path) == str(config.tracking_db) and flags & os.O_CREAT: os._exit(87)
    return original(path, flags, *args, **kwargs)
os.open = opened
from devflow_temporal.delivery_store import DeliveryStore
DeliveryStore(config)
''',
            str(failed_config)], env=self.env, text=True, capture_output=True)
        self.assertEqual(failed.returncode, 87, failed.stderr)
        with patch.dict(os.environ, self.env):
            value = paths.read_inventory(self.target)
            self.assertIn(str(failed_database), value["store_births"])
            self.assertFalse(failed_database.exists())
            with self.assertRaisesRegex(ValueError, "absence does not prove no admitted work"):
                paths.require_update_inventory(self.target)
        # Finish only this fixture's failed first write so later independent
        # tests see a reconciled owned database rather than a deliberately lost one.
        resumed = subprocess.run([python, "-I", "-B", "-c", program +
            "from devflow_temporal.delivery_store import DeliveryStore;DeliveryStore(config)",
            str(failed_config)], env=self.env, text=True, capture_output=True)
        self.assertEqual(resumed.returncode, 0, resumed.stderr)

    def test_missing_inventory_cannot_be_recreated_by_observation(self):
        paths = module("installation-paths")
        inventory = paths.inventory_path(self.target)
        retained = inventory.with_suffix(".retained")
        inventory.rename(retained)
        try:
            result = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                "from pathlib import Path;from devflow_temporal.delivery_config import DeliveryConfig;"
                "import sys;DeliveryConfig.load(Path(sys.argv[1]))", str(self.config)],
                env=self.env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(inventory.exists())
        finally:
            retained.rename(inventory)

    def test_observation_context_never_registers_or_creates_write_intent(self):
        paths = module("installation-paths")
        inventory = paths.inventory_path(self.target)
        before_inventory = inventory.read_bytes()
        lock = self.target.parent / ".production-inventory.lock"
        retained_lock = lock.with_suffix(".retained")
        lock.rename(retained_lock)
        before_config = self.config.read_bytes()
        program = r'''
import sys
from pathlib import Path
from devflow_temporal import delivery_execution_config as execution
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_installation_owner import before_store_write
# The composed #240 SDK supplies this exact context predicate. This lower-layer
# fixture checks its boundary; public assessment is qualified on composition.
execution.is_observation_mode = lambda: True
config = DeliveryConfig.load(Path(sys.argv[1]))
try: before_store_write(config)
except ValueError as exc:
    assert "observational" in str(exc)
else: raise AssertionError("observation obtained a writable store intent")
'''
        try:
            observed = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                program, str(self.config)], env=self.env, text=True, capture_output=True)
            self.assertEqual(observed.returncode, 0, observed.stderr)
            raw = json.loads(before_config)
            raw["capacity"] = 3
            self.config.write_text(json.dumps(raw))
            changed = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                program, str(self.config)], env=self.env, text=True, capture_output=True)
            self.assertNotEqual(changed.returncode, 0)
            self.assertIn("custody is unknown during observation", changed.stderr)
            self.assertEqual(inventory.read_bytes(), before_inventory)
            self.assertFalse(lock.exists())
        finally:
            self.config.write_bytes(before_config)
            retained_lock.rename(lock)

    def test_controller_child_retains_custody_before_import_after_parent_death(self):
        ready = self.home / "child-before-import"
        release = self.home / "release-child-import"
        child_program = r'''
import os, sys, time
from pathlib import Path
Path(sys.argv[1]).write_text(str(os.getpid()))
while not Path(sys.argv[2]).exists(): time.sleep(0.01)
import site
site.main()
from devflow_temporal.delivery_config import DeliveryConfig
DeliveryConfig.load(Path(sys.argv[3]))
Path(sys.argv[1]).with_name("child-import-completed").touch()
'''
        parent_program = r'''
import json, sys, time
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_control import _launch
config = DeliveryConfig.load(Path(sys.argv[1]))
child = _launch(config, "api", [sys.executable, "-I", "-S", "-B", "-c", sys.argv[4], *sys.argv[2:4], sys.argv[1]])
print(json.dumps(child), flush=True)
time.sleep(60)
'''
        parent = subprocess.Popen([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            parent_program, str(self.config), str(ready), str(release), child_program],
            env=self.env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        child_pid = None
        try:
            line = parent.stdout.readline()
            self.assertTrue(line, parent.stderr.read() if parent.poll() is not None else "child not launched")
            child_pid = json.loads(line)["pid"]
            deadline = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(ready.exists())
            self.assertEqual(int(ready.read_text()), child_pid)
            os.kill(parent.pid, signal.SIGKILL)
            parent.wait(timeout=10)
            os.kill(child_pid, 0)
            blocked = subprocess.run(["sh", str(self.target / "scripts/update.sh"), "owned-missing-release",
                str(self.skills), str(self.codex)], env=self.env, text=True, capture_output=True, timeout=10)
            self.assertNotEqual(blocked.returncode, 0)
            self.assertIn("temporarily unavailable", blocked.stderr.lower())
        finally:
            release.touch()
            if parent.poll() is None:
                parent.kill()
            parent.communicate(timeout=10)
            if child_pid:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    # Wait for the actual inherited open-file description to
                    # settle, rather than inferring exit from a projection.
                    import fcntl
                    descriptor = os.open(self.target.parent / ".production-install.lock", os.O_RDWR)
                    try:
                        try:
                            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            time.sleep(0.02)
                    finally:
                        os.close(descriptor)
                else:
                    os.killpg(child_pid, signal.SIGKILL)
                    self.fail("owned child did not release installation custody")
                self.assertTrue((self.home / "child-import-completed").exists(),
                                (self.home / "owned-state/api.log").read_text())

    def test_public_assessment_observes_changed_config_without_owner_writes(self):
        program = r'''
import asyncio, hashlib, json, os, stat, sys, threading
from copy import deepcopy
from pathlib import Path
import httpx
from devflow_temporal.contracts import digest
from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_execution_config import is_observation_mode
from devflow_temporal.delivery_installation_owner import before_store_write
from devflow_temporal.delivery_store import DeliveryStore
config_path = Path(sys.argv[1]); owner = Path(sys.argv[2])
config = DeliveryConfig.load(config_path)
store = DeliveryStore(config)
# A controlled legacy admission exercises the real fallback reader. Its absent
# historical snapshot and changed live configuration must remain unknown. This
# fixture grants no execution and does not claim a native continuation success.
with store._connect() as db:
    row = dict(db.execute("SELECT * FROM delivery_runs WHERE run_id='owned-run'").fetchone())
    spec = json.loads(row['request_json'])
    spec.update(run_id='legacy-observed-run', command_id='legacy-observed-command',
                work_id='legacy-observed-work', state_dir=str(config.state_root / 'runs/legacy-observed-run'))
    spec.pop('retry_budget_version', None)
    spec['policy'].pop('max_attempts', None)
    spec['policy_digest'] = digest(spec['policy'])
    row.update(run_id=spec['run_id'], work_id=spec['work_id'],
               request_json=json.dumps(spec), request_digest=digest(spec))
    columns = list(row)
    db.execute('INSERT INTO delivery_runs (' + ','.join(columns) + ') VALUES (' +
               ','.join('?' for _ in columns) + ')', [row[key] for key in columns])
app = create_app(config_path)
snapshot = config.state_root / 'configuration-snapshots' / (spec['config_digest'] + '.json')
retained = snapshot.with_suffix('.retained')
snapshot.rename(retained)
lock = owner / '.production-inventory.lock'
retained_lock = lock.with_suffix('.retained')
lock.rename(retained_lock)
original = config_path.read_bytes()
changed = json.loads(original); changed['capacity'] = 3
config_path.write_text(json.dumps(changed))
def fingerprint(paths):
    result = {}
    for path in paths:
        info = path.lstat()
        result[str(path)] = (stat.S_IMODE(info.st_mode), info.st_size, info.st_mtime_ns,
                            hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None)
    return result
def owned_files():
    return [*config.state_root.rglob('*'), *owner.glob('.production-*.json'),
            *(owner / '.production-configurations').rglob('*')]
before = fingerprint(owned_files())
loads, writes = [], []
def profile(frame, event, arg):
    if event == 'call' and is_observation_mode():
        if frame.f_code is DeliveryConfig.load.__func__.__code__: loads.append(True)
        if frame.f_code is before_store_write.__code__: writes.append(True)
threading.setprofile(profile)
async def assess():
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                  base_url=config.dashboard_url) as client:
        response = await client.post('/api/runs/legacy-observed-run/assess-transition', json={
            'kind': 'repair_continuation', 'request': {
                'command_id': 'observe-only', 'expected_revision': row['revision'],
                'expected_iteration': row['iteration'], 'expected_candidate_id': 'a' * 64,
                'expected_pr_number': 1, 'expected_pr_head': spec['base_sha'],
                'additional_iterations': 1}})
        assert response.status_code == 200, response.text
        observed = response.json()
        assert observed['eligible'] is False and observed['proof_digest'] is None, observed
        return observed
try:
    result = asyncio.run(assess())
    assert loads, 'public assessment did not reach the actual observational config loader'
    assert not writes, 'observation snapshot entered the normal writable-store hook'
    assert not lock.exists(), 'public assessment created an owner inventory lock'
    assert fingerprint(owned_files()) == before, 'public assessment changed owned inventory/state files'
    assert config_path.read_bytes() == json.dumps(changed).encode()
    print(json.dumps({'observational_config_loads': len(loads), 'write_intents': len(writes),
                      'assessment': result}))
finally:
    threading.setprofile(None)
    config_path.write_bytes(original)
    retained.rename(snapshot)
    retained_lock.rename(lock)
'''
        result = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            program, str(self.config), str(self.target.parent)], env=self.env,
            text=True, capture_output=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertGreater(json.loads(result.stdout)["observational_config_loads"], 0)
        self.assertEqual(json.loads(result.stdout)["write_intents"], 0)

    def test_normal_child_and_parent_exit_preserve_other_runtime_holders(self):
        home = self.home
        child_program = r'''
import os, sys, time
from pathlib import Path
ready, release, complete, config = map(Path, sys.argv[1:])
ready.write_text(str(os.getpid()))
while not release.exists(): time.sleep(0.01)
import site
site.main()
from devflow_temporal.delivery_config import DeliveryConfig
DeliveryConfig.load(config)
complete.touch()
'''
        parent_program = r'''
import json, os, sys, time
from pathlib import Path
from devflow_temporal.delivery_config import DeliveryConfig
from devflow_temporal.delivery_control import _launch
config = DeliveryConfig.load(Path(sys.argv[1]))
home, code = Path(sys.argv[2]), sys.argv[3]
children = []
for index in (1, 2):
    children.append(_launch(config, 'api-' + str(index), [sys.executable, '-I', '-S', '-B', '-c', code,
        str(home / ('normal-ready-' + str(index))), str(home / ('normal-release-' + str(index))),
        str(home / ('normal-complete-' + str(index))), str(config.path)]))
print(json.dumps(children), flush=True)
pid, status = os.waitpid(children[0]['pid'], 0)
assert pid == children[0]['pid'] and status == 0
(home / 'normal-first-collected').touch()
while not (home / 'normal-parent-exit').exists(): time.sleep(0.01)
'''
        parent = subprocess.Popen([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            parent_program, str(self.config), str(home), child_program], env=self.env,
            text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        children = []
        def wait_for(path):
            deadline = time.monotonic() + 15
            while not path.exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue(path.exists(), "owned runtime handoff did not reach " + path.name)

        def update_is_excluded():
            result = subprocess.run(["sh", str(self.target / "scripts/update.sh"), "owned-missing-release",
                str(self.skills), str(self.codex)], env=self.env, text=True, capture_output=True, timeout=10)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("temporarily unavailable", result.stderr.lower())

        try:
            line = parent.stdout.readline()
            self.assertTrue(line, parent.stderr.read() if parent.poll() is not None else "no child receipt")
            children = json.loads(line)
            for index in (1, 2):
                wait_for(home / ("normal-ready-" + str(index)))
            (home / "normal-release-1").touch()
            wait_for(home / "normal-first-collected")
            self.assertTrue((home / "normal-complete-1").exists())
            self.assertIsNone(parent.poll())
            update_is_excluded()  # Normal child exit must not unlock its parent/sibling.
            (home / "normal-parent-exit").touch()
            out, err = parent.communicate(timeout=15)
            self.assertEqual(parent.returncode, 0, out + err)
            self.assertFalse((home / "normal-complete-2").exists())
            update_is_excluded()  # Graceful parent exit leaves the waiting child custodian.
            (home / "normal-release-2").touch()
            wait_for(home / "normal-complete-2")
            import fcntl
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                descriptor = os.open(self.target.parent / ".production-install.lock", os.O_RDWR)
                try:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        time.sleep(0.02)
                finally:
                    os.close(descriptor)
            else:
                self.fail("normal trusted runtime exits did not release their installation lease")
        finally:
            for index in (1, 2):
                (home / ("normal-release-" + str(index))).touch()
            (home / "normal-parent-exit").touch()
            if parent.poll() is None:
                parent.kill()
            parent.communicate(timeout=10)

    def test_registration_rollback_authenticates_owner_before_foreign_code(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            subprocess.run(["git", "clone", str(self.releases), str(source)], check=True, capture_output=True)
            home = self.home / "registration-crash-home"
            home.mkdir(mode=0o700)
            env = {**self.env, "HOME": str(home)}
            crash = r'''
import importlib.util, os, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location("install_production", Path(sys.argv[1]) / "scripts/install-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = m.save
def save(path, state):
    original(path, state)
    if state.get("phase") == "registering": os._exit(90)
m.save = save
m.install([str(Path.home() / "skills"), str(Path.home() / "codex")])
'''
            failed = subprocess.run([sys.executable, "-B", "-c", crash, str(source)],
                env=env, text=True, capture_output=True)
            self.assertEqual(failed.returncode, 90, failed.stderr)
            target = home / ".local/share/devflow/developer-workflow"
            owned = target.with_name("retained-owned-source")
            target.rename(owned)
            subprocess.run(["git", "clone", str(self.releases), str(target)], check=True, capture_output=True)
            foreign_imported = home / "foreign-imported"
            (target / "scripts/install-rollback.py").write_text(
                "from pathlib import Path\nPath(" + repr(str(foreign_imported)) + ").touch()\n"
                "def restore(*args): raise AssertionError('foreign restore executed')\n")
            command = ["sh", str(source / "scripts/install.sh"), "--rollback"]
            refused = subprocess.run(command, env=env, text=True, capture_output=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("owner changed", refused.stderr)
            self.assertFalse(foreign_imported.exists())
            journal = target.parent / ".production-install.json"
            state = json.loads(journal.read_text())
            self.assertEqual(state["phase"], "registering")
            self.assertTrue(Path(state["backup"]).is_dir())
            shutil.rmtree(target)  # Only the foreign tree created by this test.
            owned.rename(target)
            restored = subprocess.run(command, env=env, text=True, capture_output=True)
            self.assertEqual(restored.returncode, 0, restored.stderr)
            state = json.loads(journal.read_text())
            self.assertEqual(state["phase"], "rolled_back")
            self.assertTrue((Path(state["retained"]) / ".git").is_dir())

    def test_acknowledgement_in_custom_state_root_refuses_update(self):
        # The real store load in setUpClass registered this configuration and
        # its custom state root with the independently owned installation.
        receipts = self.home / "owned-state/runtime-installations"
        receipts.mkdir(mode=0o700)
        current = receipts / "current.json"
        current.write_text('{"fixture":"acknowledged"}\n')
        current.chmod(0o600)
        before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
        try:
            result = subprocess.run(["sh", str(self.target / "scripts/update.sh"),
                                     "owned-missing-release", str(self.skills), str(self.codex)],
                                    env=self.env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("acknowledged", result.stderr.lower())
            self.assertEqual(subprocess.check_output(["git", "-C", str(self.target),
                                                      "rev-parse", "HEAD"]), before)
            self.assertEqual(current.read_text(), '{"fixture":"acknowledged"}\n')
        finally:
            current.unlink()

    def test_service_registration_refuses_disposable_search_path(self):
        temporal = self.home / "fixture-temporal"
        temporal.write_text("#!/bin/sh\nexit 0\n")
        temporal.chmod(0o755)
        with tempfile.TemporaryDirectory() as disposable:
            # Build the actual service registration plan only: no launchctl or
            # real service is invoked. The disposable PATH must be rejected.
            planner = r'''
import importlib.util, sys
from argparse import Namespace
from pathlib import Path
root, config, temporal, home = map(Path, sys.argv[1:])
spec = importlib.util.spec_from_file_location("delivery_service", root / "scripts/delivery-service.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
args = Namespace(devflow_delivery=str(root / "runtime/.venv/bin/devflow-delivery"),
                 runtime=str(root / "runtime"), temporal=str(temporal),
                 launch_agents=str(home / "launch-agents"), interval=60)
m.agent(args, "fixture.service", config, {}, home / "owned-state")
'''
            env = {**self.env, "PATH": os.pathsep.join(("/usr/bin", "/bin", disposable))}
            result = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
                                     planner, str(self.target), str(self.config), str(temporal), str(self.home)],
                                    env=env, text=True, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("temporary or managed", result.stderr)

    def test_owned_durable_helper_alias_preserves_configuration_bytes(self):
        alias = self.home / "owned-helper-alias"
        alias.symlink_to(self.target / "skills/devflow/scripts")
        config = self.home / "alias-owner.json"
        raw = json.loads(self.config.read_text())
        raw["helpers_dir"] = str(alias)
        config.write_text(json.dumps(raw))
        config.chmod(0o600)
        before = config.read_bytes()
        result = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            "from pathlib import Path;from devflow_temporal.delivery_config import DeliveryConfig;"
            "import sys;DeliveryConfig.load(Path(sys.argv[1]))", str(config)],
            env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(config.read_bytes(), before)
        bad = self.home / "owned-helper-tree"
        bad.mkdir(mode=0o700)
        (bad / "linked-child").symlink_to(self.target / "skills/devflow/scripts")
        raw["helpers_dir"] = str(bad)
        config.write_text(json.dumps(raw))
        paths = module("installation-paths")
        with patch.dict(os.environ, self.env), self.assertRaisesRegex(ValueError, "linked dependencies"):
            paths.configuration(config)
        # Preserve the registered prior observation without leaving an invalid
        # live fixture configuration behind.
        config.write_bytes(before)

    def test_unknown_worker_listing_refuses_update_before_mutation(self):
        tools = self.home / "unknown-process-tools"
        tools.mkdir(mode=0o700)
        ps = tools / "ps"
        ps.write_text("#!/bin/sh\nexit 1\n")
        ps.chmod(0o755)
        env = {**self.env, "PATH": str(tools) + os.pathsep + self.env["PATH"]}
        before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
        result = subprocess.run(["sh", str(self.target / "scripts/update.sh"),
                                 "owned-missing-release", str(self.skills), str(self.codex)],
                                env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("delivery-workers", result.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target),
                                                  "rev-parse", "HEAD"]), before)

    def test_unknown_live_worker_identity_refuses_update(self):
        tools = self.home / "unknown-identity-tools"
        tools.mkdir(mode=0o700)
        observed = self.home / "observed-worker"
        observed.mkdir(mode=0o700)
        config = observed / "config.json"
        config.write_text(json.dumps({"state_root": str(observed)}))
        (observed / "worker-ready.json").write_text(json.dumps({"pid": os.getpid(),
            "config_path": str(config), "interpreter": str(self.target / "runtime/.venv/bin/python"),
            "identity": "fixture start identity"}))
        ps = tools / "ps"
        # Model a live worker PID whose identity lookup returns no start time;
        # the test process is real and os.kill(pid, 0) proves it is still alive.
        command = (f"{os.getpid()} {self.target}/runtime/.venv/bin/python -m "
                   f"devflow_temporal.delivery_control --config {config} worker")
        ps.write_text("#!/bin/sh\nif [ \"$1\" = -axo ]; then printf '%s\\n' "
                      + repr(command) + "; fi\nexit 0\n")
        ps.chmod(0o755)
        env = {**self.env, "PATH": str(tools) + os.pathsep + self.env["PATH"]}
        before = subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"])
        result = subprocess.run(["sh", str(self.target / "scripts/update.sh"),
                                 "owned-missing-release", str(self.skills), str(self.codex)],
                                env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("identity observation unavailable", result.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target),
                                                  "rev-parse", "HEAD"]), before)

    def test_update_receipt_publication_crash_remains_blocked_and_restores_observation(self):
        receipt = self.target.parent / ".production-install.json"
        before = json.loads(receipt.read_text())
        published = module("installation-paths").published_entry(self.target, "devflow-delivery")
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "candidate"
            subprocess.run(["git", "clone", str(self.releases), str(candidate)], check=True, capture_output=True)
            for key, value in (("user.name", "Fixture"), ("user.email", "fixture@example.invalid")):
                subprocess.run(["git", "-C", str(candidate), "config", key, value], check=True)
            for arguments in (("commit", "--allow-empty", "-qm", "receipt crash candidate"),
                              ("tag", "receipt-crash"), ("push", "origin", "receipt-crash")):
                subprocess.run(["git", "-C", str(candidate), *arguments], check=True, capture_output=True)
        crash = r'''
import importlib.util, os, sys
from pathlib import Path
root = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("update_production", root / "scripts/update-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = m.load
def load(name, *args, **kwargs):
    result = original(name, *args, **kwargs)
    if name == "install-production":
        saved = result.save
        def save(path, value):
            saved(path, value)
            if path.name == ".production-install.json" and value["revision"] != sys.argv[4]: os._exit(89)
        result.save = save
    return result
m.load = load
m.update(["receipt-crash", sys.argv[2], sys.argv[3]])
'''
        failed = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c", crash,
            str(self.target), str(self.skills), str(self.codex), before["revision"]],
            env=self.env, text=True, capture_output=True)
        self.assertEqual(failed.returncode, 89, failed.stderr)
        update = json.loads((self.target.parent / ".production-update.json").read_text())
        self.assertEqual(update["phase"], "registering")
        self.assertNotEqual(json.loads(receipt.read_text())["revision"], before["revision"])
        blocked = subprocess.run([str(published), "--help"], env=self.env, text=True, capture_output=True)
        self.assertNotEqual(blocked.returncode, 0)
        self.assertIn("update is unfinished", blocked.stderr)
        restored = subprocess.run(update["recovery_command"], env=self.env, text=True, capture_output=True)
        self.assertEqual(restored.returncode, 0, restored.stderr)
        self.assertEqual(json.loads(receipt.read_text()), before)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"],
                                               text=True).strip(), before["revision"])

    def test_requested_install_crash_refuses_foreign_target_and_can_roll_back(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            subprocess.run(["git", "clone", str(self.releases), str(source)], check=True, capture_output=True)
            home = self.home / "requested-crash-home"
            home.mkdir(mode=0o700)
            env = {**self.env, "HOME": str(home)}
            # Kill precisely after the durable requested journal and before any
            # directory allocation. This runs the real installer entry point.
            crash = r'''
import importlib.util, os, sys
from pathlib import Path
source = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("install_production", source / "scripts/install-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = m.save
def save(path, state):
    original(path, state)
    if state.get("phase") == "requested": os._exit(86)
m.save = save
m.install([str(Path.home() / "skills"), str(Path.home() / "codex")])
'''
            crashed = subprocess.run([sys.executable, "-B", "-c", crash, str(source)], env=env,
                                     text=True, capture_output=True)
            self.assertEqual(crashed.returncode, 86, crashed.stderr)
            target = home / ".local/share/devflow/developer-workflow"
            self.assertFalse(target.exists())
            target.mkdir(mode=0o700)
            marker = target / "foreign"
            marker.write_text("preserve foreign owner\n")
            command = ["sh", str(source / "scripts/install.sh"), str(home / "skills"), str(home / "codex")]
            replay = subprocess.run(command, env=env, text=True, capture_output=True)
            self.assertNotEqual(replay.returncode, 0)
            self.assertEqual(marker.read_text(), "preserve foreign owner\n")
            rolled_back = subprocess.run(["sh", str(source / "scripts/install.sh"), "--rollback"],
                                         env=env, text=True, capture_output=True)
            self.assertEqual(rolled_back.returncode, 0, rolled_back.stderr)
            self.assertEqual(marker.read_text(), "preserve foreign owner\n")
            state = json.loads((target.parent / ".production-install.json").read_text())
            self.assertEqual(state["phase"], "rolled_back")

    def test_allocation_crash_preserves_unknown_stage_and_replays(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            subprocess.run(["git", "clone", str(self.releases), str(source)], check=True, capture_output=True)
            home = self.home / "allocation-crash-home"
            home.mkdir(mode=0o700)
            env = {**self.env, "HOME": str(home)}
            crash = r'''
import importlib.util, os, sys
from pathlib import Path
source = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("install_production", source / "scripts/install-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = Path.mkdir
def mkdir(self, *args, **kwargs):
    result = original(self, *args, **kwargs)
    if self.name.startswith(".production-adopt-"): os._exit(87)
    return result
Path.mkdir = mkdir
m.install([str(Path.home() / "skills"), str(Path.home() / "codex")])
'''
            crashed = subprocess.run([sys.executable, "-B", "-c", crash, str(source)], env=env,
                                     text=True, capture_output=True)
            self.assertEqual(crashed.returncode, 87, crashed.stderr)
            owner = home / ".local/share/devflow"
            journal = owner / ".production-install.json"
            state = json.loads(journal.read_text())
            stage = Path(state["stage"])
            self.assertTrue(stage.is_dir())
            self.assertNotIn("target_identity", state)
            (stage / "uncertain-owner").write_text("preserve this data\n")
            result = subprocess.run(["sh", str(source / "scripts/install.sh"),
                                     str(home / "skills"), str(home / "codex")],
                                    env=env, text=True, capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            state = json.loads(journal.read_text())
            self.assertEqual(state["phase"], "verified")
            self.assertIn(str(stage), state["unknown_allocations"])
            self.assertEqual((stage / "uncertain-owner").read_text(), "preserve this data\n")
            self.assertTrue((owner / "developer-workflow/.git").is_dir())

    def test_requested_crash_rollback_preserves_foreign_file_and_alias(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "source"
            subprocess.run(["git", "clone", str(self.releases), str(source)], check=True, capture_output=True)
            crash = r'''
import importlib.util, os, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location("install_production", Path(sys.argv[1]) / "scripts/install-production.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
original = m.save
def save(path, state):
    original(path, state)
    if state.get("phase") == "requested": os._exit(86)
m.save = save
m.install([str(Path.home() / "skills"), str(Path.home() / "codex")])
'''
            for kind in ("file", "alias"):
                with self.subTest(kind=kind):
                    home = self.home / ("requested-foreign-" + kind)
                    home.mkdir(mode=0o700)
                    env = {**self.env, "HOME": str(home)}
                    failed = subprocess.run([sys.executable, "-B", "-c", crash, str(source)],
                        env=env, text=True, capture_output=True)
                    self.assertEqual(failed.returncode, 86, failed.stderr)
                    target = home / ".local/share/devflow/developer-workflow"
                    if kind == "file":
                        target.write_text("preserve foreign file\n")
                        retained = target
                    else:
                        retained = home / "foreign-file"
                        retained.write_text("preserve foreign alias target\n")
                        target.symlink_to(retained)
                    before = retained.read_bytes()
                    result = subprocess.run(["sh", str(source / "scripts/install.sh"), "--rollback"],
                        env=env, text=True, capture_output=True)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertEqual(retained.read_bytes(), before)
                    self.assertEqual(target.is_symlink(), kind == "alias")
                    state = json.loads((target.parent / ".production-install.json").read_text())
                    self.assertEqual(state["phase"], "rolled_back")
                    self.assertIn(str(target), state["preserved_unattributed"])

    def test_indirect_environment_and_configuration_dependencies_are_rejected(self):
        paths = module("installation-paths")
        with patch.dict(os.environ, self.env):
            with self.assertRaisesRegex(ValueError, "temporary or managed"):
                paths.configuration(Path(tempfile.gettempdir()).resolve() / "owner.json")
            site = next((self.target / "runtime/.venv/lib").glob("python*/site-packages"))
            pth = site / "unowned-disposable.pth"
            pth.write_text(str(self.origin / "runtime/src") + "\n")
            try:
                with self.assertRaisesRegex(ValueError, "temporary or managed"):
                    paths.runtime(self.target)
            finally:
                pth.unlink()

    def test_z_source_changing_update_of_stopped_fake_owner(self):
        # Positive normal upgrade: changing a tracked document changes both the
        # commit and tree while preserving real persisted fake-run state.
        with tempfile.TemporaryDirectory() as temporary:
            candidate = Path(temporary) / "candidate"
            subprocess.run(["git", "clone", str(self.releases), str(candidate)], check=True, capture_output=True)
            document = candidate / "README.md"
            document.write_text(document.read_text() + "\nOwned release fixture.\n")
            for key, value in (("user.name", "Fixture"), ("user.email", "fixture@example.invalid")):
                subprocess.run(["git", "-C", str(candidate), "config", key, value], check=True)
            for arguments in (("add", "README.md"), ("commit", "-qm", "fixture release"),
                              ("tag", "owned-good-release"), ("push", "origin", "owned-good-release")):
                subprocess.run(["git", "-C", str(candidate), *arguments], check=True, capture_output=True)
            revision = subprocess.check_output(["git", "-C", str(candidate), "rev-parse", "HEAD"])
        result = subprocess.run(["sh", str(self.target / "scripts/update.sh"), "owned-good-release",
            str(self.skills), str(self.codex)], env=self.env, text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(self.target), "rev-parse", "HEAD"]), revision)
        paths = module("installation-paths")
        current = paths.published_entry(self.target, "devflow-delivery")
        ready = subprocess.run([str(current), "--help"], env=self.env, text=True, capture_output=True)
        self.assertEqual(ready.returncode, 0, ready.stderr)
        self.assertIn("usage:", ready.stdout)
        persisted = subprocess.run([str(self.target / "runtime/.venv/bin/python"), "-I", "-B", "-c",
            "import json,sys;from pathlib import Path;"
            "from devflow_temporal.delivery_config import DeliveryConfig;"
            "from devflow_temporal.delivery_store import DeliveryStore;"
            "s=DeliveryStore(DeliveryConfig.load(Path(sys.argv[1])));"
            "print(json.dumps(s.submitted_spec('owned-run'),sort_keys=True))", str(self.config)],
            env=self.env, text=True, capture_output=True)
        self.assertEqual(persisted.returncode, 0, persisted.stderr)
        self.assertEqual(json.loads(persisted.stdout), self.persisted["spec"])


if __name__ == "__main__":
    unittest.main()
