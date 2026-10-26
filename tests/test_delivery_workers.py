"""The upgrade refuses while a registered worker runs from the checkout it would change."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/delivery-workers.py"


def start_identity(pid, *, canonical):
    env = {**os.environ, "LC_ALL": "C", "TZ": "UTC0"} if canonical else None
    return subprocess.run(["ps", "-p", str(pid), "-o", "lstart="], capture_output=True,
                          text=True, env=env).stdout.strip()


class DeliveryWorkers(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="devflow-delivery-workers-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "source"
        (self.source / "scripts").mkdir(parents=True)
        for name in ("update.sh", "delivery-workers.py"):
            shutil.copyfile(ROOT / "scripts" / name, self.source / "scripts" / name)
        self.state = self.root / "state"
        self.state.mkdir()
        self.config = self.root / "config.json"
        self.config.write_text(json.dumps({"state_root": str(self.state)}))
        # A stand-in module keeps the worker's exact argv shape without the runtime.
        package = self.root / "fake-runtime/devflow_temporal"
        package.mkdir(parents=True)
        (package / "__init__.py").write_text("")
        (package / "delivery_control.py").write_text("import time\ntime.sleep(60)\n")
        self.env = dict(os.environ, DEVFLOW_PYTHON=sys.executable, DEVFLOW_INSTALL_MODE="development", GIT_CONFIG_GLOBAL=os.devnull,
                        GIT_CONFIG_NOSYSTEM="1")
        self.worker = subprocess.Popen(
            [sys.executable, "-m", "devflow_temporal.delivery_control", "--config", str(self.config),
             "worker"], env=dict(self.env, PYTHONPATH=str(package.parent)), start_new_session=True)
        self.addCleanup(self.worker.wait)
        self.addCleanup(self.worker.kill)

    def register(self, **fields):
        marker = {"pid": self.worker.pid, "config_path": str(self.config),
                  "identity": start_identity(self.worker.pid, canonical=True),
                  "interpreter": str(self.source / "runtime/.venv/bin/python"),
                  "source_root": str(self.source / "runtime"), **fields}
        (self.state / "worker-ready.json").write_text(json.dumps(marker))

    def running(self, runtime):
        return subprocess.run([sys.executable, "-B", str(SCRIPT), "--runtime", str(runtime)],
                              text=True, capture_output=True)

    def update(self):
        return subprocess.run(["sh", str(self.source / "scripts/update.sh"), "fixture-tag"],
                              env=self.env, text=True, capture_output=True)

    def test_update_refuses_while_a_registered_worker_runs_from_the_checkout(self):
        self.register()
        refused = self.update()
        self.assertEqual(refused.returncode, 1, refused.stderr)
        self.assertIn(f"--config {self.config} stop", refused.stderr)
        self.assertIn("Stop the running delivery stack", refused.stderr)
        live = self.running(self.source / "runtime")
        self.assertEqual(live.returncode, 3, live.stderr)
        self.assertEqual([item["pid"] for item in json.loads(live.stdout)], [self.worker.pid])
        # Another checkout's runtime does not claim this worker.
        other = self.running(self.root / "elsewhere/runtime")
        self.assertEqual((other.returncode, other.stdout.strip()), (0, "[]"))
        # A marker whose process is gone no longer refuses the upgrade.
        self.worker.kill()
        self.worker.wait()
        gone = self.update()
        self.assertNotIn("Stop the running delivery stack", gone.stderr)

    def test_detection_trusts_the_marker_not_the_process_table_argv(self):
        # The worker's argv[0] is the test interpreter, outside the checkout; the
        # recorded interpreter and source root decide.
        self.register(interpreter=str(self.root / "elsewhere/bin/python"),
                      source_root=str(self.root / "elsewhere/runtime"))
        self.assertEqual(self.running(self.source / "runtime").stdout.strip(), "[]")
        self.register(interpreter=str(self.root / "elsewhere/bin/python"))
        self.assertEqual(self.running(self.source / "runtime").returncode, 3)
        # A marker written before identities were canonical still proves the worker.
        self.register(identity=start_identity(self.worker.pid, canonical=False))
        self.assertEqual(self.running(self.source / "runtime").returncode, 3)
        # A mismatched generation cannot prove that the visible worker is safe
        # to overwrite; preserve custody until its registration is reconciled.
        self.register(identity="Thu Jan  1 00:00:00 1970")
        unknown = self.running(self.source / "runtime")
        self.assertEqual(unknown.returncode, 1)
        self.assertIn("does not match", unknown.stderr)

    def test_visible_worker_with_unavailable_registration_remains_unknown(self):
        unknown = self.running(self.source / "runtime")
        self.assertEqual(unknown.returncode, 1)
        self.assertIn("registration is unavailable", unknown.stderr)


if __name__ == "__main__":
    unittest.main()
