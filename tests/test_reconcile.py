"""Controlled CLI, hook, and service replay without real GitHub mutation."""
import json
import hashlib
import io
import os
from pathlib import Path
import plistlib
import select
import shlex
import shutil
import sqlite3
import stat
import subprocess
import sys
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "skills/devflow/scripts"
sys.path.insert(0, str(SCRIPTS))
import state  # noqa: E402
import reconcile  # noqa: E402

ISSUE = "https://github.com/owner/repo/issues/12"
PROJECT = "https://github.com/users/owner/projects/1"
RUN = "https://github.com/owner/repo/actions/runs/99"

FAKE_GH = '''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
path = Path(os.environ["FAKE_GH_STATE"])
s = json.loads(path.read_text())
a = sys.argv[1:]
if s.get("fail_read"):
    print("API unavailable", file=sys.stderr); sys.exit(1)
if a[:2] == ["issue", "view"]:
    out = s["issue"]
elif a[:2] == ["issue", "edit"]:
    s["issue"]["assignees"].append({"login": a[-1]})
    s["writes"].append("assignee")
    out = ""
elif a[:2] == ["run", "view"]:
    out = s["run"]
elif a[:2] == ["api", "graphql"]:
    q = next(x[6:] for x in a if x.startswith("query="))
    if "projectV2(number:" in q:
        options = [{"id": key, "name": name} for key, name in s["options"].items()]
        out = {"data": {"user": {"projectV2": {"id": "P", "url": s["project"],
            "closed": False, "field": {"id": "F", "options": options}}}}}
    elif "projectItems(first:100)" in q:
        nodes = ([{"id": "ITEM", "project": {"id": "P"},
                   "fieldValueByName": {"optionId": s["option"], "name": s["options"][s["option"]]}}]
                 if s["item"] else [])
        out = {"data": {"node": {"projectItems": {"nodes": nodes}}}}
    elif "addProjectV2ItemById" in q:
        s["item"] = True
        s["writes"].append("add_item")
        out = {"data": {"addProjectV2ItemById": {"item": {"id": "ITEM"}}}}
    elif "updateProjectV2ItemFieldValue" in q:
        s["option"] = next(x.split("=",1)[1] for x in a if x.startswith("option="))
        s["writes"].append("status")
        out = {"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "ITEM"}}}}
        if s.get("fail_after_status"):
            path.write_text(json.dumps(s)); print("uncertain write", file=sys.stderr); sys.exit(1)
    else:
        out = {"data": {"node": {"project": {"id": "P"},
            "fieldValueByName": {"optionId": s["option"], "name": s["options"][s["option"]]}}}}
else:
    raise SystemExit("unknown fake gh call: " + str(a))
path.write_text(json.dumps(s))
print(json.dumps(out) if isinstance(out, dict) else out)
'''


class ReconcileCLI(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.db = self.root / "workflow.sqlite3"
        self.remote_path = self.root / "remote.json"
        self.remote_path.write_text(json.dumps({
            "project": PROJECT, "issue": {"id": "ISSUE", "url": ISSUE, "title": "Work",
                                         "state": "OPEN", "assignees": [{"login": "owner"}]},
            "item": True, "option": "I", "options": {"I": "In progress", "R": "In review",
                "B": "Blocked", "D": "Done", "P": "Paused"}, "writes": [],
            "run": {"status": "in_progress", "conclusion": None, "url": RUN}}))
        binpath = self.root / "bin"
        binpath.mkdir()
        gh = binpath / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(0o755)
        self.gh = gh
        self.env = dict(os.environ, PATH=str(binpath) + os.pathsep + os.environ["PATH"],
                        FAKE_GH_STATE=str(self.remote_path))
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.record(db, "work", dict(id="w1", title="Work", issue=ISSUE,
                                           repository="github.com/owner/repo", status="active"))
            state.claim_work(db, "w1", "root-1")

    def cli(self, script, *args, payload=None, check=True):
        path = SCRIPTS / script if script != "reconcile-service.py" else ROOT / "scripts" / script
        cmd = [sys.executable, "-B", str(path)]
        if script != "reconcile-service.py":
            cmd += ["--db", str(self.db)]
        result = subprocess.run(cmd + list(args), input=json.dumps(payload) if payload else None,
                                text=True, capture_output=True, env=self.env, timeout=15)
        if check:
            self.assertEqual(result.returncode, 0, result.stderr)
        return result

    def remote(self):
        return json.loads(self.remote_path.read_text())

    def save_remote(self, update):
        data = self.remote()
        update(data)
        self.remote_path.write_text(json.dumps(data))

    def record(self, work_id="w1"):
        with state.connect(self.db) as db:
            return state.row(db, "works", work_id), reconcile.intent(db, work_id), state.claim_for(db, work_id)

    def set(self, status="in-progress", release=False, extra=(), check=True):
        args = ["set", "--work-id", "w1", "--owner", "root-1", "--assignee", "owner",
                "--project", PROJECT, "--status", status]
        if status == "blocked":
            args += ["--reason", "Await external run"]
        if release:
            args.append("--release")
        return self.cli("github.py", *(args + list(extra)), check=check)

    def test_duplicate_and_repeated_same_drift_have_one_write_per_change(self):
        self.set()
        self.set()
        work, intent, _ = self.record()
        self.assertEqual(intent["revision"], 1)
        self.assertEqual(self.remote()["writes"], [])
        for expected in (1, 2):
            self.save_remote(lambda s: s.update(option="B"))
            self.cli("reconcile.py", "once")
            self.assertEqual(self.remote()["option"], "I")
            self.assertEqual(self.remote()["writes"].count("status"), expected)
        with state.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM results WHERE kind='github'").fetchone()[0], 3)
            history = db.execute("SELECT COUNT(*) FROM history").fetchone()[0]
        self.cli("reconcile.py", "once")
        self.assertEqual(self.remote()["writes"].count("status"), 2)
        with state.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM history").fetchone()[0], history)

    def test_uncertain_partial_write_replays_without_second_mutation(self):
        self.save_remote(lambda s: s.update(fail_after_status=True))
        failed = self.set("blocked", check=False)
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(self.remote()["writes"], ["status"])
        self.assertEqual(self.record()[1]["state"], "pending")
        self.save_remote(lambda s: s.update(fail_after_status=False))
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE reconcile_intents SET next_attempt_at=NULL")
        self.cli("reconcile.py", "once")
        self.assertEqual(self.remote()["writes"], ["status"])
        self.assertEqual(self.record()[1]["state"], "acknowledged")

    def test_owner_stop_waits_for_child_then_releases(self):
        self.set()
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.bind(db, "child", ["w1"], "devflow-coordinator", "root-1")
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "Interrupt", "turn_id": "a"})
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd", "turn_id": "a"})
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[2]["owner"], "root-1")
        self.assertEqual(self.remote()["option"], "I")
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "agent_id": "child",
                          "hook_event_name": "SubagentStop", "turn_id": "b"})
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE reconcile_intents SET next_attempt_at=NULL")
        self.cli("reconcile.py", "once")
        self.assertIsNone(self.record()[2])
        self.assertEqual(self.remote()["option"], "B")

    def test_same_owner_native_resume_fences_old_recovery(self):
        self.set()
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd", "turn_id": "old"})
        old = self.record()[1]["claim_token"]
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "UserPromptSubmit", "turn_id": "new"})
        with state.connect(self.db) as db:
            self.assertNotEqual(old, reconcile.owner_token(db, state.claim_for(db, "w1")))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "needs_decision")
        self.assertEqual(self.record()[2]["owner"], "root-1")
        self.assertEqual(self.remote()["option"], "I")

    def test_new_owner_supersedes_recovery_without_remote_write(self):
        self.set()
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd"})
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.release_work(db, "w1", "root-1")
            state.claim_work(db, "w1", "root-2")
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "needs_decision")
        self.assertEqual(self.record()[2]["owner"], "root-2")
        self.assertEqual(self.remote()["writes"], [])

    def test_api_outage_persists_attempt_and_retry(self):
        self.set()
        self.save_remote(lambda s: s.update(fail_read=True))
        self.cli("reconcile.py", "once")
        intent = self.record()[1]
        self.assertEqual(intent["kind"], "probe")
        self.assertEqual(intent["state"], "pending")
        self.assertGreaterEqual(intent["attempts"], 1)
        self.assertIn("API unavailable", intent["last_error"])
        self.assertTrue(intent["next_attempt_at"])
        self.assertEqual(self.remote()["writes"], [])

    def test_direct_transition_survives_mapping_read_outage(self):
        self.save_remote(lambda s: s.update(fail_read=True))
        failed = self.set("blocked", check=False)
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(json.loads(self.record()[1]["payload"])["status"], "blocked")
        self.save_remote(lambda s: s.update(fail_read=False))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "acknowledged")
        self.assertEqual(self.remote()["option"], "B")

    def test_direct_unknown_mapping_is_one_decision(self):
        self.save_remote(lambda s: s["options"].pop("B"))
        self.assertEqual(self.set("blocked", check=False).returncode, 1)
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "needs_decision")
        self.assertEqual(self.remote()["writes"], [])

    def test_explicit_retry_after_mapping_repair_uses_new_revision(self):
        self.save_remote(lambda s: s["options"].pop("B"))
        self.assertEqual(self.set("blocked", check=False).returncode, 1)
        self.cli("reconcile.py", "once")
        old = self.record()[1]
        self.assertEqual(old["state"], "needs_decision")
        self.save_remote(lambda s: s["options"].update(B="Blocked"))
        self.set("blocked")
        self.assertGreater(self.record()[1]["revision"], old["revision"])
        self.assertEqual(self.record()[1]["state"], "acknowledged")
        self.assertEqual(self.remote()["option"], "B")

    def test_external_terminal_success_and_failure_remain_open_and_actionable(self):
        for conclusion, expected in (("success", "R"), ("failure", "B")):
            with self.subTest(conclusion=conclusion):
                self.set("blocked", release=True, extra=("--await-url", RUN, "--follow-up", "review run"))
                self.save_remote(lambda s: s["run"].update(status="completed", conclusion=conclusion))
                self.cli("reconcile.py", "once")
                work, intent, claim = self.record()
                self.assertIsNone(claim)
                self.assertEqual(self.remote()["issue"]["state"], "OPEN")
                self.assertEqual(self.remote()["option"], expected)
                self.assertEqual(intent["state"], "needs_decision")
                self.assertIn("review" if conclusion == "success" else "inspect", intent["next_action"])
                self.assertNotIn("Await external", work["blocker"] or "")
                tracking = json.loads(work["details"])["github"]
                self.assertNotIn("await", tracking)
                self.assertEqual(tracking["sync"]["project_status"], self.remote()["options"][expected])
                # Reset this work for the other outcome with an explicit owner event.
                if conclusion == "success":
                    with state.connect(self.db) as db, db:
                        db.execute("BEGIN IMMEDIATE")
                        state.claim_work(db, "w1", "root-1")
                    self.save_remote(lambda s: s["run"].update(status="in_progress", conclusion=None))

    def test_completed_external_run_supersedes_prior_decision_intent(self):
        self.set("blocked", release=True, extra=("--await-url", RUN, "--follow-up", "review run"))
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("UPDATE reconcile_intents SET state='needs_decision',next_action='old decision'")
        self.save_remote(lambda s: s["run"].update(status="completed", conclusion="success"))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.remote()["option"], "R")
        self.assertEqual(self.record()[1]["kind"], "external_transition")
        self.assertEqual(self.record()[1]["state"], "needs_decision")

    def test_closed_issue_converges_without_closing_any_issue(self):
        self.set("blocked", release=True)
        self.save_remote(lambda s: s["issue"].update(state="CLOSED"))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[0]["status"], "done")
        self.assertEqual(self.remote()["option"], "D")
        self.assertEqual(self.remote()["writes"].count("close_issue"), 0)

    def test_closed_issue_keeps_live_owner_claim(self):
        self.set()
        self.save_remote(lambda s: s["issue"].update(state="CLOSED"))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "needs_decision")
        self.assertEqual(self.record()[2]["owner"], "root-1")
        self.assertEqual(self.remote()["option"], "I")

    def test_interrupted_claimed_owner_converges_already_closed_issue(self):
        self.set()
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd"})
        self.save_remote(lambda s: s["issue"].update(state="CLOSED"))
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["kind"], "closed_convergence")
        self.assertEqual(self.record()[1]["state"], "pending")
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[0]["status"], "done")
        self.assertIsNone(self.record()[2])
        self.assertEqual(self.remote()["option"], "D")

    def test_missing_mapping_api_failure_and_read_only_preview(self):
        self.set()
        self.save_remote(lambda s: s["options"].pop("B"))
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd"})
        self.cli("reconcile.py", "once")
        self.assertEqual(self.record()[1]["state"], "needs_decision")
        before = self.db.read_bytes()
        lock = self.db.with_suffix(self.db.suffix + ".reconcile.lock")
        lock.unlink(missing_ok=True)
        preview = self.cli("reconcile.py", "once", "--dry-run")
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(lock.exists())
        self.assertEqual(json.loads(preview.stdout)["state"], "preview")
        self.assertEqual(self.remote()["writes"], [])

    def test_preview_does_not_migrate_legacy_database(self):
        self.set()
        with sqlite3.connect(self.db) as db:
            db.execute("DROP TABLE reconcile_cursor")
            db.execute("DROP TABLE reconcile_intents")
            db.execute("PRAGMA user_version=4")
        before = self.db.read_bytes()
        lock = self.db.with_suffix(self.db.suffix + ".reconcile.lock")
        lock.unlink(missing_ok=True)
        preview = self.cli("reconcile.py", "once", "--dry-run")
        self.assertEqual(json.loads(preview.stdout)["records"][0]["eligibility"], "managed")
        self.assertEqual(self.db.read_bytes(), before)
        self.assertFalse(lock.exists())

    def test_preview_reports_queued_owner_stop_remote_and_claim_impact(self):
        self.set()
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd"})
        before = self.db.read_bytes()
        preview = json.loads(self.cli("reconcile.py", "once", "--dry-run").stdout)["records"][0]
        self.assertEqual(preview["pending_intent"]["kind"], "owner_stop")
        self.assertEqual(preview["pending_intent"]["desired_project_status"], "Blocked")
        self.assertIn("Project Status", preview["possible_remote_writes"])
        self.assertIn("release terminal owner claim after readback", preview["possible_local_actions"])
        self.assertEqual(self.db.read_bytes(), before)
        self.assertEqual(self.remote()["writes"], [])

    def test_default_preview_reports_truncated_coverage(self):
        self.set()
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            for index in range(1, 26):
                state.record(db, "work", {"id": f"legacy-{index:02d}", "title": "Legacy",
                                           "issue": f"https://github.com/owner/repo/issues/{index + 100}",
                                           "repository": "github.com/owner/repo", "status": "active",
                                           "details": {"github": {"project": PROJECT}}})
        result = json.loads(self.cli("reconcile.py", "once", "--dry-run").stdout)
        self.assertEqual(result["coverage"]["total_project_records"], 26)
        self.assertEqual(result["coverage"]["returned"], 20)
        self.assertTrue(result["coverage"]["truncated"])
        self.assertEqual(len(json.loads(self.cli("reconcile.py", "once", "--dry-run", "--limit", "100").stdout)["records"]), 26)
        legacy = next(row for row in result["records"] if row["work_id"] == "legacy-01")
        self.assertEqual(legacy["audit_state"], "unknown")
        self.assertIsNone(legacy["local_claim_owner"])
        self.assertEqual(legacy["remote_observed"]["project_status"], "In progress")

    def test_fair_cursor_and_legacy_migration(self):
        self.set()
        with state.connect(self.db) as db, db:
            db.execute("BEGIN IMMEDIATE")
            saved = json.loads(state.row(db, "works", "w1")["details"] or "{}")
            for i in range(2, 27):
                state.record(db, "work", dict(id=f"w{i:02d}", title="Work", issue=ISSUE.replace("12", str(i)),
                                               repository="github.com/owner/repo", status="active", details=saved))
        # A small limit rotates and persists its position; later work is not starved.
        positions = []
        for _ in range(4):
            self.cli("reconcile.py", "once", "--limit", "8")
            with state.connect(self.db) as db:
                positions.append(db.execute("SELECT last_work_id FROM reconcile_cursor").fetchone()[0])
        self.assertEqual(positions, ["w09", "w16", "w24", "w07"])
        with sqlite3.connect(self.db) as db:
            db.execute("DROP TABLE reconcile_cursor")
            db.execute("ALTER TABLE runtime_sessions DROP COLUMN generation")
            db.execute("PRAGMA user_version=5")
        with state.connect(self.db) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 7)
            self.assertEqual(state.row(db, "works", "w26")["title"], "Work")

    def test_service_install_run_uninstall_in_owned_home(self):
        agents = self.root / "Library/LaunchAgents"
        home = self.root / "codex"
        script = ROOT / "scripts/reconcile-service.py"
        base = [sys.executable, "-B", str(script), "--launch-agents", str(agents)]
        install = subprocess.run(base + ["install", "--db", str(self.db), "--codex-home", str(home),
                                         "--gh", str(self.gh), "--no-start"], text=True, capture_output=True)
        self.assertEqual(install.returncode, 0, install.stderr)
        path = agents / "com.ebarti.devflow.reconcile.plist"
        data = plistlib.loads(path.read_bytes())
        self.assertEqual(data["ProgramArguments"][4], str(self.db.resolve()))
        self.assertEqual(data["EnvironmentVariables"]["DEVFLOW_GH"], str(self.gh.resolve()))
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE((home / "logs/devflow-reconcile.log").stat().st_mode), 0o600)
        self.cli("reconcile.py", "once")
        uninstall = subprocess.run(base + ["uninstall", "--no-stop"], text=True, capture_output=True)
        self.assertEqual(uninstall.returncode, 0, uninstall.stderr)
        self.assertFalse(path.exists())

    def test_daemon_entrypoint_runs_one_bounded_pass_without_agent(self):
        self.set()
        process = subprocess.Popen([sys.executable, "-B", str(SCRIPTS / "reconcile.py"),
                                    "--db", str(self.db), "daemon", "--interval", "15", "--limit", "1"],
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready, "daemon did not complete its first pass")
            self.assertEqual(json.loads(process.stdout.readline())["state"], "completed")
            self.assertEqual(self.remote()["writes"], [])
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_daemon_waits_for_activation_without_migrating_and_missing_db_without_exit(self):
        manifest = self.root / "manifest.json"
        marker = self.root / "active.json"
        manifest.write_text("{}")
        before = self.db.read_bytes()
        command = [sys.executable, "-B", str(SCRIPTS / "reconcile.py"), "--db", str(self.db),
                   "--manifest", str(manifest), "--activation-marker", str(marker),
                   "--activation-token", "next-release", "daemon", "--interval", "15"]
        process = subprocess.Popen(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready)
            self.assertEqual(json.loads(process.stdout.readline())["state"], "waiting_for_activation")
            self.assertIsNone(process.poll())
            self.assertEqual(self.db.read_bytes(), before)
        finally:
            process.terminate()
            process.communicate(timeout=5)
        missing = self.root / "missing.sqlite3"
        process = subprocess.Popen([sys.executable, "-B", str(SCRIPTS / "reconcile.py"), "--db", str(missing),
                                    "daemon", "--interval", "15"], text=True, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, env=self.env)
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready)
            self.assertEqual(json.loads(process.stdout.readline())["state"], "no_database")
            self.assertIsNone(process.poll())
            self.assertFalse(missing.exists())
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_daemon_runs_after_matching_installation_activation(self):
        manifest = self.root / "manifest.json"
        marker = self.root / "active.json"
        manifest.write_text(json.dumps({"source": str(ROOT), "git": shutil.which("git"),
                                        "head": subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip(),
                                        "files": {"skills/devflow/scripts/reconcile.py":
                                                  hashlib.sha256((SCRIPTS / "reconcile.py").read_bytes()).hexdigest()}}))
        marker.write_text(json.dumps({"token": "activated"}))
        process = subprocess.Popen([sys.executable, "-B", str(SCRIPTS / "reconcile.py"), "--db", str(self.db),
                                    "--manifest", str(manifest), "--activation-marker", str(marker),
                                    "--activation-token", "activated", "daemon", "--interval", "15"],
                                   text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.env)
        try:
            ready, _, _ = select.select([process.stdout], [], [], 5)
            self.assertTrue(ready)
            self.assertEqual(json.loads(process.stdout.readline())["state"], "completed")
        finally:
            process.terminate()
            process.communicate(timeout=5)

    def test_service_activation_and_failed_upgrade_restore_prior_service(self):
        agents = self.root / "Library/LaunchAgents"
        home = self.root / "codex"
        launch_state = self.root / "launch.json"
        launch_state.write_text(json.dumps({"loaded": False, "calls": [], "fail_next_bootstrap": False}))
        binary = self.root / "fake-launchctl"
        binary.write_text('''#!/usr/bin/env python3
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
''')
        binary.chmod(0o755)
        env = dict(self.env, FAKE_LAUNCH_STATE=str(launch_state))
        script = ROOT / "scripts/reconcile-service.py"
        base = [sys.executable, "-B", str(script), "--launch-agents", str(agents),
                "--launchctl", str(binary)]
        install = ["install", "--db", str(self.db), "--codex-home", str(home), "--gh", str(self.gh)]
        staged = subprocess.run(base + install + ["--no-start"], env=env, text=True, capture_output=True)
        self.assertEqual(staged.returncode, 0, staged.stderr)
        first = subprocess.run(base + install, env=env, text=True, capture_output=True)
        self.assertEqual(first.returncode, 0, first.stderr)
        self.assertTrue(json.loads(launch_state.read_text())["loaded"])
        path = agents / "com.ebarti.devflow.reconcile.plist"
        prior = path.read_bytes()
        marker = home / ".devflow-reconcile-active.json"
        prior_marker = marker.read_bytes()
        config = json.loads(launch_state.read_text())
        config["fail_next_bootstrap"] = True
        launch_state.write_text(json.dumps(config))
        failed = subprocess.run(base + install + ["--interval", "120"], env=env, text=True, capture_output=True)
        self.assertEqual(failed.returncode, 1)
        self.assertEqual(path.read_bytes(), prior)
        self.assertEqual(marker.read_bytes(), prior_marker)
        recovered = json.loads(launch_state.read_text())
        self.assertTrue(recovered["loaded"])
        self.assertEqual(recovered["program"], plistlib.loads(prior)["ProgramArguments"])
        for _ in range(2):
            removed = subprocess.run(base + ["uninstall"], env=env, text=True, capture_output=True)
            self.assertEqual(removed.returncode, 0, removed.stderr)
        self.assertFalse(path.exists())
        self.assertFalse(marker.exists())

    def exercise_full_upgrade(self, baseline):
        origin, checkout = self.root / "origin", self.root / "checkout"
        origin.mkdir()
        archive = subprocess.check_output(["git", "-C", str(ROOT), "archive", "--format=tar", baseline])
        with tarfile.open(fileobj=io.BytesIO(archive)) as packed:
            packed.extractall(origin, filter="data")
        subprocess.run(["git", "init", "-q", str(origin)], check=True)
        for key, value in (("user.name", "Test"), ("user.email", "test@example.invalid")):
            subprocess.run(["git", "-C", str(origin), "config", key, value], check=True)
        subprocess.run(["git", "-C", str(origin), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(origin), "commit", "-qm", "old release"], check=True)
        subprocess.run(["git", "-C", str(origin), "tag", "v1"], check=True)
        tracked = subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z"]).decode().split("\0")
        for name in set(filter(None, tracked)) | {"scripts/install-rollback.py"}:
            target = origin / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / name, target)
        subprocess.run(["git", "-C", str(origin), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(origin), "commit", "-qm", "new release"], check=True)
        subprocess.run(["git", "-C", str(origin), "tag", "v2"], check=True)
        subprocess.run(["git", "clone", "-q", str(origin), str(checkout)], check=True)
        subprocess.run(["git", "-C", str(checkout), "checkout", "-q", "--detach", "v1"], check=True)
        home = self.root / "home"
        codex = home / ".codex"
        temporary = self.root / "temporary"
        temporary.mkdir()
        launch_state = self.root / "launch.json"
        launch_state.write_text(json.dumps({"calls": [], "observed": None}))
        launchctl = self.root / "launchctl"
        launchctl.write_text('''#!/usr/bin/env python3
import json, os, plistlib, subprocess, sys
from pathlib import Path
p = Path(os.environ["FAKE_LAUNCH_STATE"])
s = json.loads(p.read_text())
a = sys.argv[1:]
s["calls"].append(a[0])
if a[0] == "print":
    p.write_text(json.dumps(s)); print("Could not find service", file=sys.stderr); sys.exit(1)
if a[0] == "bootstrap":
    arguments = plistlib.loads(Path(a[-1]).read_bytes())["ProgramArguments"]
    if os.environ.get("FAKE_BOOTSTRAP_OK") == "1":
        s["program"] = arguments
        s["loaded"] = True
        p.write_text(json.dumps(s)); sys.exit(0)
    child = subprocess.Popen(arguments, env=os.environ, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        s["observed"] = json.loads(child.stdout.readline())["state"]
    finally:
        child.terminate(); child.communicate(timeout=5)
    p.write_text(json.dumps(s)); print("uncertain bootstrap failure", file=sys.stderr); sys.exit(1)
p.write_text(json.dumps(s))
''')
        launchctl.chmod(0o755)
        env = dict(self.env, HOME=str(home), CODEX_HOME=str(codex),
                   XDG_STATE_HOME=str(self.root / "state"), DEVFLOW_PYTHON=sys.executable,
                   DEVFLOW_LAUNCHCTL=str(launchctl), FAKE_LAUNCH_STATE=str(launch_state),
                   TMPDIR=str(temporary))
        old_install = subprocess.run(["sh", str(checkout / "scripts/install.sh")], env=env,
                                     text=True, capture_output=True, timeout=30)
        self.assertEqual(old_install.returncode, 0, old_install.stderr)
        database = self.root / "state/devflow/workflow.sqlite3"
        seed = subprocess.run([sys.executable, "-B", "-c", "import state; state.connect(__import__('sys').argv[1]).close()",
                               str(database)], env=dict(env, PYTHONPATH=str(checkout / "skills/devflow/scripts")),
                              text=True, capture_output=True, timeout=15)
        self.assertEqual(seed.returncode, 0, seed.stderr)
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 4)
        prior_head = subprocess.check_output(["git", "-C", str(origin), "rev-parse", "v1"], text=True).strip()
        logs_target = self.root / "logs-target"
        logs_target.mkdir()
        (codex / "logs").symlink_to(logs_target)
        late = subprocess.run(["sh", str(checkout / "scripts/update.sh"), "v2"], env=env,
                              text=True, capture_output=True, timeout=90)
        self.assertNotEqual(late.returncode, 0, late.stdout + late.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip(),
                         prior_head)
        self.assertTrue((codex / "logs").is_symlink())
        self.assertEqual(list(temporary.glob("devflow-install-rollback-*")), [])
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 4)
        (codex / "logs").unlink()
        before = {str(path.relative_to(codex)): path.read_bytes() for path in codex.rglob("*") if path.is_file()}
        failed = subprocess.run(["sh", str(checkout / "scripts/update.sh"), "v2"], env=env,
                                text=True, capture_output=True, timeout=90)
        self.assertNotEqual(failed.returncode, 0, failed.stdout + failed.stderr)
        self.assertEqual(subprocess.check_output(["git", "-C", str(checkout), "rev-parse", "HEAD"], text=True).strip(),
                         prior_head)
        self.assertEqual(json.loads(launch_state.read_text())["observed"], "waiting_for_activation",
                         failed.stdout + failed.stderr)
        self.assertEqual(list(temporary.glob("devflow-install-rollback-*")), [])
        for name, expected in before.items():
            actual = (codex / name).read_bytes()
            if name == ".devflow-install.json" and actual != expected:
                original, restored = json.loads(expected), json.loads(actual)
                changed = {key for key in original["files"] | restored["files"]
                           if original["files"].get(key) != restored["files"].get(key)}
                self.fail(f"manifest changed files: {changed}; checkout status: " +
                          subprocess.check_output(["git", "-C", str(checkout), "status", "--short"], text=True) +
                          "\nupdate output:\n" + failed.stdout + failed.stderr)
            self.assertEqual(actual, expected, name)
        self.assertFalse((codex / ".devflow-reconcile-active.json").exists())
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 4)
        installed_guard = codex / ".devflow-hook.py"
        if installed_guard.exists():
            guard = subprocess.run([sys.executable, "-B", str(installed_guard), "--check"],
                                   env=env, text=True, capture_output=True, timeout=15)
            self.assertEqual(guard.returncode, 0, guard.stdout + guard.stderr)
        else:
            hooks = json.loads((codex / "hooks.json").read_text())
            command = hooks["hooks"]["SessionEnd"][0]["hooks"][0]["command"]
            self.assertEqual(Path(shlex.split(command)[-2]).resolve(),
                             (checkout / "skills/devflow/scripts/telemetry.py").resolve())
        activated = subprocess.run(["sh", str(checkout / "scripts/update.sh"), "v2"],
                                   env=dict(env, FAKE_BOOTSTRAP_OK="1"), text=True, capture_output=True, timeout=90)
        self.assertEqual(activated.returncode, 0, activated.stdout + activated.stderr)
        self.assertEqual(list(temporary.glob("devflow-install-rollback-*")), [])
        installed_guard = codex / ".devflow-hook.py"
        guard = subprocess.run([sys.executable, "-B", str(installed_guard), "--check"],
                               env=env, text=True, capture_output=True, timeout=15)
        self.assertEqual(guard.returncode, 0, guard.stdout + guard.stderr)
        service = plistlib.loads((home / "Library/LaunchAgents/com.ebarti.devflow.reconcile.plist").read_bytes())
        marker = json.loads((codex / ".devflow-reconcile-active.json").read_text())
        self.assertEqual(marker["token"], service["DevflowToken"])
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 4)
        child = subprocess.Popen(service["ProgramArguments"], env=env, text=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            ready, _, _ = select.select([child.stdout], [], [], 5)
            self.assertTrue(ready)
            self.assertEqual(json.loads(child.stdout.readline())["state"], "completed")
        finally:
            child.terminate()
            child.communicate(timeout=5)
        with sqlite3.connect(database) as db:
            self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 7)

    def test_first_upgrade_from_legacy_updater_rollback_then_activation(self):
        self.exercise_full_upgrade("e966cf89e057abc9a2629faf957a2ec175599b53")

    def test_upgrade_from_installed_lifecycle_checkout_rollback_then_activation(self):
        self.exercise_full_upgrade("60de52a37c9774f188376904517fb6318246adf9")


if __name__ == "__main__":
    unittest.main()
