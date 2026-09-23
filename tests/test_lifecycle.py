"""Lifecycle behavior through the shipped CLI and hook entry points."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "skills/devflow/scripts"
sys.path.insert(0, str(SCRIPTS))
import state  # noqa: E402

ISSUE = "https://github.com/owner/repo/issues/12"
PROJECT = "https://github.com/users/owner/projects/1"
RUN = "https://github.com/owner/repo/actions/runs/99"

FAKE_GH = '''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
p = Path(os.environ["FAKE_GH_STATE"])
s = json.loads(p.read_text())
a = sys.argv[1:]
if s.get("fail"):
    print("fake API failure", file=sys.stderr); sys.exit(1)
if a[:2] == ["issue", "view"]:
    out = s["issue"]
elif a[:2] == ["issue", "edit"]:
    s["issue"]["assignees"].append({"login": a[-1]}); out = ""
elif a[:2] == ["run", "view"]:
    assert a[2:5] == ["99", "--repo", "github.com/owner/repo"], a
    out = s["run"]
elif a[:2] == ["api", "graphql"]:
    query = next(x[6:] for x in a if x.startswith("query="))
    if "projectV2(number:" in query:
        out = {"data": {"user": {"projectV2": {"id": "P", "url": "https://github.com/users/owner/projects/1", "closed": False,
            "field": {"id": "F", "options": [{"id": "B", "name": "Blocked"}, {"id": "D", "name": "Done"}]}}}}}
    elif "addProjectV2ItemById" in query:
        out = {"data": {"addProjectV2ItemById": {"item": {"id": "ITEM"}}}}
    elif "updateProjectV2ItemFieldValue" in query:
        s["option"] = next(x.split("=",1)[1] for x in a if x.startswith("option="))
        out = {"data": {"updateProjectV2ItemFieldValue": {"projectV2Item": {"id": "ITEM"}}}}
    elif "projectItems(first:100)" in query:
        out = {"data": {"node": {"projectItems": {"nodes": [
            {"id": "ITEM", "project": {"id": "P"}, "fieldValueByName": {"optionId": s["option"], "name": s["option_name"]}}]}}}}
    else:
        out = {"data": {"node": {"project": {"id": "P"}, "fieldValueByName": {"optionId": s["option"], "name": s["option_name"]}}}}
else:
    raise SystemExit("unknown fake gh call: " + str(a))
p.write_text(json.dumps(s))
print(json.dumps(out) if isinstance(out, dict) else out)
'''


class LifecycleCLI(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.db_path = self.root / "workflow.sqlite3"
        self.gh_state = self.root / "gh.json"
        self.gh_state.write_text(json.dumps({
            "issue": {"id": "ISSUE", "url": ISSUE, "title": "Work", "state": "OPEN",
                      "assignees": [{"login": "owner"}]},
            "option": "B", "option_name": "Blocked",
            "run": {"status": "in_progress", "conclusion": None, "url": RUN}}))
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        gh = bin_dir / "gh"
        gh.write_text(FAKE_GH)
        gh.chmod(0o755)
        self.env = dict(os.environ, PATH=str(bin_dir) + os.pathsep + os.environ["PATH"],
                        FAKE_GH_STATE=str(self.gh_state))
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.record(db, "work", dict(id="w1", title="Work", issue=ISSUE,
                                           repository="github.com/owner/repo", status="active"))
            state.claim_work(db, "w1", "root-1")

    def cli(self, script, *args, payload=None):
        return subprocess.run([sys.executable, "-B", str(SCRIPTS / script), "--db", str(self.db_path), *args],
                              input=json.dumps(payload) if payload is not None else None,
                              text=True, capture_output=True, env=self.env)

    def set_blocked(self, release=False):
        args = ["set", "--work-id", "w1", "--owner", "root-1", "--assignee", "owner",
                "--status", "blocked", "--reason", "Await signed release", "--project", PROJECT,
                "--await-url", RUN, "--follow-up", "root-1 checks this run at next resume"]
        if release:
            args.append("--release")
        result = self.cli("github.py", *args)
        self.assertEqual(result.returncode, 0, result.stderr)

    def audit(self):
        result = self.cli("github.py", "audit", "--work-id", "w1")
        return result, json.loads(result.stdout) if result.stdout else None

    def test_pending_and_completed_actions_and_read_only_replay(self):
        self.set_blocked(release=True)
        before = hashlib.sha256(self.db_path.read_bytes()).hexdigest()
        first, report = self.audit()
        second, replay = self.audit()
        self.assertEqual((first.returncode, second.returncode), (0, 0))
        self.assertEqual(report, replay)
        self.assertEqual(report["state"], "consistent")
        self.assertEqual(report["expected"]["item_id"], "ITEM")
        self.assertTrue(report["expected"]["readback_at"])
        self.assertEqual(hashlib.sha256(self.db_path.read_bytes()).hexdigest(), before)
        stopped = self.cli("telemetry.py", "hook", payload={"session_id": "root-1",
                            "hook_event_name": "Stop", "turn_id": "t1"})
        self.assertNotIn("decision", json.loads(stopped.stdout))
        remote = json.loads(self.gh_state.read_text())
        remote["run"].update(status="completed", conclusion="success")
        self.gh_state.write_text(json.dumps(remote))
        finished, report = self.audit()
        self.assertEqual(finished.returncode, 1)
        self.assertIn("awaited_run_completed", report["reconciliation_required"])
        self.assertEqual(report["issue_state"], "OPEN")  # success did not accept or close the issue

    def test_mismatch_and_api_failure_never_pass(self):
        self.set_blocked(release=True)
        remote = json.loads(self.gh_state.read_text())
        remote["issue"]["assignees"] = []
        remote["option"] = "D"
        self.gh_state.write_text(json.dumps(remote))
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("assignee_mismatch", report["reconciliation_required"])
        self.assertIn("project_status_mismatch", report["reconciliation_required"])
        remote["fail"] = True
        self.gh_state.write_text(json.dumps(remote))
        failed, _ = self.audit()
        self.assertEqual(failed.returncode, 1)
        self.assertIn("fake API failure", failed.stderr)

    def test_renamed_project_option_with_same_id_requires_reconciliation(self):
        self.set_blocked(release=True)
        remote = json.loads(self.gh_state.read_text())
        remote["option_name"] = "Done"
        self.gh_state.write_text(json.dumps(remote))
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("project_status_mismatch", report["reconciliation_required"])

    def test_active_issue_without_claim_requires_reconciliation(self):
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.release_work(db, "w1", "root-1")
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("active_work_unclaimed", report["reconciliation_required"])

    def test_legacy_sync_unknown_and_done_closed_state_mismatch(self):
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("legacy_sync_expectation", report["unknown"])
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.update(db, "work", dict(id="w1", status="done"), None)
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("done_issue_open", report["reconciliation_required"])

    def test_legacy_release_run_is_audited_without_implying_acceptance(self):
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.update(db, "work", dict(id="w1", status="blocked",
                                           details={"release_run": RUN, "github": {"project": PROJECT}}), None)
        remote = json.loads(self.gh_state.read_text())
        remote["run"].update(status="completed", conclusion="success")
        self.gh_state.write_text(json.dumps(remote))
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("awaited_run_completed", report["reconciliation_required"])
        self.assertIn("legacy_sync_expectation", report["unknown"])
        self.assertEqual(report["project_observed"]["fieldValueByName"]["optionId"], "B")

    def test_root_stop_interrupt_leaf_and_other_owner(self):
        stop = {"session_id": "root-1", "hook_event_name": "Stop", "turn_id": "t1"}
        result = self.cli("telemetry.py", "hook", payload=stop)
        self.assertEqual(result.returncode, 0)
        self.assertEqual(json.loads(result.stdout)["decision"], "block")
        active = self.cli("telemetry.py", "hook", payload=dict(stop, stop_hook_active=True))
        self.assertNotIn("decision", json.loads(active.stdout))
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.bind(db, "child-1", ["w1"], "devflow-coordinator", "root-1")
            state.bind(db, "leaf-1", ["w1"], "devflow-implementer", "child-1")
            state.record(db, "work", dict(id="w2", title="Other", issue="https://github.com/owner/repo/issues/13",
                                           repository="github.com/owner/repo", status="active"))
            state.claim_work(db, "w2", "root-2")
        for actor in ("child-1", "leaf-1"):
            result = self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "agent_id": actor,
                              "hook_event_name": "Stop", "turn_id": "t2"})
            self.assertNotIn("decision", json.loads(result.stdout))
            self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "agent_id": actor,
                     "hook_event_name": "Interrupt", "turn_id": "t2"})
        with state.connect(self.db_path) as db:
            self.assertEqual(state.row(db, "works", "w1")["status"], "active")
            self.assertEqual(state.row(db, "works", "w2")["status"], "active")
        interrupted = self.cli("telemetry.py", "hook", payload={"session_id": "root-1",
                               "hook_event_name": "Interrupt", "turn_id": "t3"})
        self.assertEqual(interrupted.returncode, 0)
        with state.connect(self.db_path) as db:
            self.assertEqual(state.row(db, "works", "w1")["status"], "blocked")
            self.assertEqual(state.claim_for(db, "w1")["owner"], "root-1")
            self.assertEqual(state.row(db, "works", "w2")["status"], "active")
            self.assertEqual(state.claim_for(db, "w2")["owner"], "root-2")
        # A closed owner remains auditable without expiring or silently releasing its claim.
        self.cli("telemetry.py", "hook", payload={"session_id": "root-1", "hook_event_name": "SessionEnd"})
        result, report = self.audit()
        self.assertEqual(result.returncode, 1)
        self.assertIn("stopped_owner_retains_claim", report["reconciliation_required"])

    def test_unresolved_issue_creation_blocks_root_stop_without_blocking_local_work(self):
        with state.connect(self.db_path) as db, db:
            db.execute("BEGIN IMMEDIATE")
            state.record(db, "work", dict(id="pending", title="Pending issue", status="starting",
                                           details={"github": {"create_pending": True, "project": PROJECT}}))
            state.claim_work(db, "pending", "root-pending")
            state.record(db, "work", dict(id="local", title="Local work", status="active"))
            state.claim_work(db, "local", "root-local")
        pending = self.cli("telemetry.py", "hook", payload={"session_id": "root-pending",
                           "hook_event_name": "Stop", "turn_id": "t1"})
        self.assertEqual(json.loads(pending.stdout)["decision"], "block")
        self.assertIn("creation", json.loads(pending.stdout)["reason"])
        local = self.cli("telemetry.py", "hook", payload={"session_id": "root-local",
                         "hook_event_name": "Stop", "turn_id": "t1"})
        self.assertNotIn("decision", json.loads(local.stdout))

    def test_local_only_owner_interrupt_and_end_preserve_claim_and_mark_blocked(self):
        for event in ("Interrupt", "SessionEnd"):
            work_id, owner = "local-" + event, "root-" + event
            with state.connect(self.db_path) as db, db:
                db.execute("BEGIN IMMEDIATE")
                state.record(db, "work", dict(id=work_id, title="Local work", status="active"))
                state.claim_work(db, work_id, owner)
            response = self.cli("telemetry.py", "hook", payload={"session_id": owner,
                                "hook_event_name": event, "turn_id": "t1"})
            self.assertEqual(response.returncode, 0)
            with state.connect(self.db_path) as db:
                work = state.row(db, "works", work_id)
                self.assertEqual(work["status"], "blocked")
                self.assertIn("reconciliation", work["blocker"])
                self.assertEqual(state.claim_for(db, work_id)["owner"], owner)


if __name__ == "__main__":
    unittest.main()
