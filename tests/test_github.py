"""Issue creation never duplicates: the attempt is persisted before the network call."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills" / "devflow" / "scripts"))

import github  # noqa: E402
import state  # noqa: E402

PROJECT = "https://github.com/users/owner/projects/1"


class CreateIssueTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.db = state.connect(Path(directory.name) / "workflow.sqlite3")
        self.addCleanup(self.db.close)
        self.args = SimpleNamespace(work_id="w1", owner="task-a", repo="owner/repo", title="Add retry cap",
                                    body_file="/dev/null", assignee="@me", source_ref=None)

    def test_failed_create_is_recorded_and_never_retried_blindly(self):
        calls = []

        def failing_gh(*command, as_json=False):
            calls.append(command)
            raise RuntimeError("network down")

        with mock.patch.object(github, "gh", failing_gh):
            with self.assertRaisesRegex(RuntimeError, "network down"):
                github.create_issue(self.db, self.args, PROJECT)
        self.assertEqual(calls[0][:2], ("issue", "create"))
        tracking = json.loads(state.row(self.db, "works", "w1")["details"])["github"]
        self.assertEqual(tracking, {"project": PROJECT, "create_pending": True})
        self.assertEqual(state.claim_for(self.db, "w1")["owner"], "task-a")

        with mock.patch.object(github, "gh", failing_gh):
            with self.assertRaisesRegex(ValueError, "do not create again"):
                github.create_issue(self.db, self.args, PROJECT)
        self.assertEqual(len(calls), 1)

    def test_successful_create_binds_the_issue_and_replays_without_network(self):
        calls = []

        def creating_gh(*command, as_json=False):
            calls.append(command)
            return "https://github.com/owner/repo/issues/12"

        with mock.patch.object(github, "gh", creating_gh):
            first = github.create_issue(self.db, self.args, PROJECT)
            second = github.create_issue(self.db, self.args, PROJECT)
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)
        work = state.row(self.db, "works", "w1")
        self.assertEqual((work["issue"], work["repository"]), (first, "github.com/owner/repo"))
        self.assertEqual(state.claim_for(self.db, "w1")["owner"], "task-a")
        with self.assertRaisesRegex(ValueError, "already claimed"):
            github.create_issue(self.db, SimpleNamespace(**dict(vars(self.args), owner="task-b")), PROJECT)


if __name__ == "__main__":
    unittest.main()
