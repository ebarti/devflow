"""The delivery stack's launchd supervisor: staged privately, activated explicitly."""
import json
import os
from pathlib import Path
import plistlib
import shutil
import stat
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/delivery-service.py"
PREFIX = "com.ebarti.devflow.delivery."
FAKE_LAUNCHCTL = '''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
p = Path(os.environ["FAKE_LAUNCH_STATE"])
s = json.loads(p.read_text())
a = sys.argv[1:]
s["calls"].append(a[0])
if a[0] == "print":
    if not s["loaded"]:
        print("Could not find service", file=sys.stderr); p.write_text(json.dumps(s)); sys.exit(1)
elif a[0] == "bootout":
    s["loaded"] = False
elif a[0] == "bootstrap":
    if s["fail_next_bootstrap"]:
        s["fail_next_bootstrap"] = False
        p.write_text(json.dumps(s)); print("bootstrap failed", file=sys.stderr); sys.exit(1)
    s["loaded"] = True
    s["program"] = __import__("plistlib").loads(Path(a[-1]).read_bytes())["ProgramArguments"]
p.write_text(json.dumps(s))
'''


def fake_executable(path, body="#!/bin/sh\nexit 0\n"):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    path.chmod(0o755)
    return path


class DeliveryService(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="devflow-delivery-service-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        # Never the real ~/Library/LaunchAgents: every call names this directory.
        self.agents = self.root / "Library/LaunchAgents"
        self.state = self.root / "state"
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"state_root": str(self.state)}))
        self.runtime = self.root / "source/runtime"
        self.entry = fake_executable(self.runtime / ".venv/bin/devflow-delivery")
        self.temporal = fake_executable(self.root / "tools/temporal")
        self.env = dict(os.environ, DEVFLOW_LAUNCH_AGENTS=str(self.agents),
                        GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")

    def script(self, *args, env=None):
        return subprocess.run([sys.executable, "-B", str(SCRIPT), "--launch-agents", str(self.agents),
                               *map(str, args)], env=env or self.env, text=True, capture_output=True)

    def install_args(self):
        return ["install", "--config", self.config, "--devflow-delivery", self.entry,
                "--temporal", self.temporal]

    def test_install_stages_under_the_state_root_without_touching_launchd(self):
        staged = self.script(*self.install_args())
        self.assertEqual(staged.returncode, 0, staged.stderr)
        result = json.loads(staged.stdout)
        self.assertFalse(result["active"])
        path = Path(result["staged"])
        self.assertEqual(path.parent, self.state / "launchd")
        self.assertTrue(path.name.startswith(PREFIX))
        # Nothing launchd reads at the next login exists.
        self.assertFalse(self.agents.exists())
        data = plistlib.loads(path.read_bytes())
        self.assertEqual(data["ProgramArguments"],
                         [str(self.entry), "--config", str(self.config), "supervise"])
        self.assertEqual(data["DevflowRuntime"], str(self.runtime))
        self.assertEqual(data["ProcessType"], "Standard")
        self.assertEqual(data["KeepAlive"], {"SuccessfulExit": False})
        self.assertTrue(data["RunAtLoad"])
        self.assertEqual(data["StartInterval"], 60)
        self.assertTrue(data["AbandonProcessGroup"])
        self.assertTrue(data["DevflowManaged"])
        self.assertEqual(data["DevflowConfig"], str(self.config))
        search = data["EnvironmentVariables"]["PATH"].split(os.pathsep)
        self.assertEqual(search[:2], [str(self.entry.parent), str(self.temporal.parent)])
        for folder in (self.state, self.state / "launchd"):
            self.assertEqual(stat.S_IMODE(folder.stat().st_mode), 0o700)
        for file in (path, self.state / "launchd.log", self.state / "launchd.error.log"):
            self.assertEqual(stat.S_IMODE(file.stat().st_mode), 0o600)
        self.assertEqual(data["StandardOutPath"], str(self.state / "launchd.log"))
        again = self.script(*self.install_args())
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(plistlib.loads(path.read_bytes()), data)
        inspected = json.loads(self.script("inspect", "--config", self.config).stdout)
        self.assertEqual((inspected["installed"], inspected["active"]), (True, False))
        # A staged agent never counts against an upgrade of its checkout.
        self.assertEqual(self.script("active", "--runtime", self.runtime).stdout.strip(), "[]")
        removed = self.script("uninstall", "--config", self.config, "--no-stop")
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertEqual(json.loads(removed.stdout)["removed"], [str(path)])
        self.assertFalse(path.exists())

    def test_install_refuses_relative_config_and_missing_executables_before_writing(self):
        relative = self.script("install", "--config", "config.json", "--devflow-delivery", self.entry,
                               "--temporal", self.temporal)
        self.assertEqual(relative.returncode, 1)
        self.assertIn("absolute", relative.stderr)
        missing = self.script("install", "--config", self.config, "--devflow-delivery",
                              self.root / "bin/absent", "--temporal", self.temporal)
        self.assertEqual(missing.returncode, 1)
        self.assertIn("devflow-delivery", missing.stderr)
        self.assertFalse(self.agents.exists())
        self.assertFalse(self.state.exists())

    def test_activation_installs_into_launchd_and_restores_the_prior_agent_on_failure(self):
        if sys.platform != "darwin":
            self.skipTest("launchd activation is a macOS entry point")
        launch_state = self.root / "launch.json"
        launch_state.write_text(json.dumps({"loaded": False, "calls": [], "fail_next_bootstrap": False}))
        binary = fake_executable(self.root / "fake-launchctl", FAKE_LAUNCHCTL)
        env = dict(self.env, FAKE_LAUNCH_STATE=str(launch_state))
        base = ["--launchctl", binary]
        active = self.script(*base, *self.install_args(), "--activate", env=env)
        self.assertEqual(active.returncode, 0, active.stderr)
        result = json.loads(active.stdout)
        self.assertTrue(result["active"])
        path = Path(result["service"])
        self.assertEqual(path.parent, self.agents)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(path.read_bytes(), Path(result["staged"]).read_bytes())
        state = json.loads(launch_state.read_text())
        self.assertTrue(state["loaded"])
        self.assertEqual(state["calls"], ["bootstrap"])
        prior = path.read_bytes()
        self.assertEqual(state["program"], plistlib.loads(prior)["ProgramArguments"])
        # An activated agent for this checkout blocks its upgrade until uninstalled.
        blocking = self.script("active", "--runtime", self.runtime, env=env)
        self.assertEqual(blocking.returncode, 3)
        self.assertEqual([item["config"] for item in json.loads(blocking.stdout)], [str(self.config)])
        self.assertIn("uninstall --config", blocking.stderr)
        self.assertEqual(self.script("active", "--runtime", self.root / "elsewhere",
                                     env=env).stdout.strip(), "[]")
        state["fail_next_bootstrap"] = True
        launch_state.write_text(json.dumps(state))
        failed = self.script(*base, *self.install_args(), "--interval", "120", "--activate", env=env)
        self.assertEqual(failed.returncode, 1)
        self.assertIn("bootstrap failed", failed.stderr)
        self.assertEqual(path.read_bytes(), prior)
        recovered = json.loads(launch_state.read_text())
        self.assertTrue(recovered["loaded"])
        self.assertEqual(recovered["program"], plistlib.loads(prior)["ProgramArguments"])
        self.assertEqual(recovered["calls"][1:], ["print", "bootout", "bootstrap", "print", "bootstrap"])
        # The staged copy moved on; inspect names the drift until the next activation.
        self.assertTrue(json.loads(self.script("inspect", "--config", self.config,
                                               env=env).stdout)["stale_active"])
        removed = self.script(*base, "uninstall", "--config", self.config, env=env)
        self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertEqual(len(json.loads(removed.stdout)["removed"]), 2)
        self.assertFalse(json.loads(launch_state.read_text())["loaded"])
        self.assertFalse(path.exists())
        self.assertEqual(json.loads(self.script(*base, "uninstall", "--config", self.config,
                                                env=env).stdout)["removed"], [])

    def test_update_refuses_while_an_activated_agent_supervises_the_checkout(self):
        source = self.runtime.parent
        (source / "scripts").mkdir(parents=True)
        for name in ("update.sh", "delivery-service.py", "delivery-workers.py"):
            shutil.copyfile(ROOT / "scripts" / name, source / "scripts" / name)
        env = dict(self.env, DEVFLOW_PYTHON=sys.executable)
        staged = self.script(*self.install_args(), env=env)
        self.assertEqual(staged.returncode, 0, staged.stderr)
        allowed = subprocess.run(["sh", str(source / "scripts/update.sh"), "fixture-tag"],
                                 env=env, text=True, capture_output=True)
        self.assertNotIn("Uninstall the launchd agent", allowed.stderr)
        # Activate without launchctl: the activated copy is what launchd reads at login.
        self.agents.mkdir(parents=True)
        shutil.copyfile(json.loads(staged.stdout)["staged"],
                        self.agents / Path(json.loads(staged.stdout)["staged"]).name)
        refused = subprocess.run(["sh", str(source / "scripts/update.sh"), "fixture-tag"],
                                 env=env, text=True, capture_output=True)
        self.assertEqual(refused.returncode, 1, refused.stderr)
        self.assertIn("Uninstall the launchd agent", refused.stderr)
        self.assertIn(f"uninstall --config {self.config}", refused.stderr)

    def test_install_records_the_runtime_behind_a_canonical_launcher(self):
        # install-delivery-launchers.py links bin/devflow-delivery to a journal
        # entry that execs the checkout's venv entry: argv[0] never names the checkout.
        journal = self.root / "launchers/share/devflow/delivery-launchers/cmd-1"
        (journal / "entries").mkdir(parents=True)
        # The journal's manifest is {"request": <request>, "bin_identity": ...}.
        (journal / "manifest.json").write_text(json.dumps({
            "request": {"schema_version": 1, "command_id": "cmd-1",
                        "runtime_dir": str(self.runtime), "config_path": str(self.config)},
            "bin_identity": {"device": 1, "inode": 2}}))
        # This wrapper's exec target tells nothing about the checkout; the journal does.
        opaque = fake_executable(self.root / "tools/devflow-delivery")
        wrapper = fake_executable(
            journal / "entries/devflow-delivery",
            "#!/bin/sh\n# Canonical delivery entry; devflow aliases devflow-delivery.\n"
            f"exec '{opaque}' --config '{self.config}' \"$@\"\n")
        link = self.root / "launchers/bin/devflow-delivery"
        link.parent.mkdir(parents=True)
        link.symlink_to(wrapper)
        launcher_args = ["install", "--config", self.config, "--devflow-delivery", link,
                         "--temporal", self.temporal]
        installed = self.script(*launcher_args)
        self.assertEqual(installed.returncode, 0, installed.stderr)
        staged = Path(json.loads(installed.stdout)["staged"])
        data = plistlib.loads(staged.read_bytes())
        self.assertEqual(data["ProgramArguments"][0], str(wrapper))
        self.assertEqual(data["DevflowRuntime"], str(self.runtime))
        # Activated (copied, no launchctl), the agent counts against this checkout only.
        self.agents.mkdir(parents=True)
        shutil.copyfile(staged, self.agents / (data["Label"] + ".plist"))
        blocking = self.script("active", "--runtime", self.runtime)
        self.assertEqual(blocking.returncode, 3, blocking.stderr)
        self.assertEqual([item["runtime"] for item in json.loads(blocking.stdout)],
                         [[str(self.runtime)]])
        elsewhere = self.root / "elsewhere/runtime"
        self.assertEqual(self.script("active", "--runtime", elsewhere).stdout.strip(), "[]")
        # Without the journal, only a wrapper that execs a venv entry names the
        # checkout; otherwise --runtime must. A wrapper bound to another
        # configuration is refused.
        (journal / "manifest.json").unlink()
        underived = self.script(*launcher_args)
        self.assertEqual(underived.returncode, 1)
        self.assertIn("supply --runtime", underived.stderr)
        named = self.script(*launcher_args, "--runtime", self.runtime)
        self.assertEqual(named.returncode, 0, named.stderr)
        wrapper.write_text(f"#!/bin/sh\nexec '{self.entry}' --config '{self.config}' \"$@\"\n")
        again = self.script(*launcher_args)
        self.assertEqual(again.returncode, 0, again.stderr)
        self.assertEqual(plistlib.loads(staged.read_bytes())["DevflowRuntime"], str(self.runtime))
        other = fake_executable(
            journal / "entries/other",
            f"#!/bin/sh\nexec '{self.entry}' --config '{self.root}/other.json' \"$@\"\n")
        refused = self.script("install", "--config", self.config, "--devflow-delivery", other,
                              "--temporal", self.temporal)
        self.assertEqual(refused.returncode, 1)
        self.assertIn("bound to another configuration", refused.stderr)

    def test_active_rederives_the_runtime_and_reports_ambiguous_agents(self):
        staged = self.script(*self.install_args())
        self.assertEqual(staged.returncode, 0, staged.stderr)
        data = plistlib.loads(Path(json.loads(staged.stdout)["staged"]).read_bytes())
        self.agents.mkdir(parents=True)
        elsewhere = self.root / "elsewhere/runtime"
        # A recorded runtime pointing elsewhere does not hide an entry that execs
        # this checkout's venv: the entry decides where a worker would launch.
        repointed = {**data, "DevflowRuntime": str(elsewhere), "Label": PREFIX + "repointed000"}
        (self.agents / (repointed["Label"] + ".plist")).write_bytes(plistlib.dumps(repointed))
        here = self.script("active", "--runtime", self.runtime)
        self.assertEqual(here.returncode, 3, here.stderr)
        self.assertEqual([item["label"] for item in json.loads(here.stdout)], [repointed["Label"]])
        self.assertEqual(json.loads(here.stdout)[0]["runtime"],
                         sorted([str(elsewhere), str(self.runtime)]))
        self.assertEqual([item["label"] for item in
                          json.loads(self.script("active", "--runtime", elsewhere).stdout)],
                         [repointed["Label"]])
        # An agent with no runtime and an entry that tells nothing blocks every checkout.
        opaque = fake_executable(self.root / "tools/opaque")
        unknown = {key: value for key, value in data.items() if key != "DevflowRuntime"}
        unknown.update(Label=PREFIX + "unknown00000",
                       ProgramArguments=[str(opaque), "--config", str(self.config), "supervise"])
        (self.agents / (unknown["Label"] + ".plist")).write_bytes(plistlib.dumps(unknown))
        far = self.root / "another/runtime"
        self.assertEqual([item["label"] for item in
                          json.loads(self.script("active", "--runtime", far).stdout)],
                         [unknown["Label"]])
        # A plist under the Devflow label that Devflow does not own is reported, not skipped.
        foreign = self.agents / (PREFIX + "foreign000000.plist")
        foreign.write_bytes(plistlib.dumps({"Label": PREFIX + "foreign000000",
                                            "ProgramArguments": ["/usr/bin/true"]}))
        reported = self.script("active", "--runtime", far)
        self.assertEqual(reported.returncode, 3)
        self.assertEqual([item["service"] for item in json.loads(reported.stdout)
                          if item.get("error")], [str(foreign)])
        self.assertIn("not owned by Devflow", reported.stderr)


if __name__ == "__main__":
    unittest.main()
