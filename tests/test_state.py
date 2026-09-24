"""Contracts of the state helper: exclusive claims, creation replay, schema migration."""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills" / "devflow" / "scripts"))

import state  # noqa: E402


class StateCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path = Path(directory.name) / "workflow.sqlite3"
        self.db = state.connect(self.path)
        self.addCleanup(self.db.close)
        self.db.execute("BEGIN IMMEDIATE")


class ClaimTests(StateCase):
    def test_one_owner_per_canonical_issue(self):
        state.record(self.db, "work", {"id": "w1", "title": "one", "issue": "https://github.com/Owner/Repo/issues/7"})
        state.record(self.db, "work", {"id": "w2", "title": "two", "issue": "https://GITHUB.com/owner/repo/issues/007"})
        first = state.claim_work(self.db, "w1", "task-a")
        self.assertFalse(first["replayed"])
        self.assertEqual(first["claim"]["resource"], "issue:github.com/owner/repo/issues/7")
        with self.assertRaisesRegex(ValueError, "already claimed"):
            state.claim_work(self.db, "w1", "task-b")
        with self.assertRaisesRegex(ValueError, "already claimed"):
            state.claim_work(self.db, "w2", "task-b")  # the same issue spelled differently
        self.assertTrue(state.claim_work(self.db, "w1", "task-a")["replayed"])
        with self.assertRaisesRegex(ValueError, "ownership mismatch"):
            state.release_work(self.db, "w1", "task-b")
        with self.assertRaisesRegex(ValueError, "release ownership"):
            state.update(self.db, "work", {"id": "w1", "issue": "https://github.com/owner/repo/issues/8"}, None)
        self.assertTrue(state.release_work(self.db, "w1", "task-a")["released"])
        self.assertFalse(state.claim_work(self.db, "w2", "task-b")["replayed"])
        self.assertEqual(state.scope(self.db, "task-b"), ["w2"])  # claiming binds the owner's session


class ReplayTests(StateCase):
    def test_same_facts_replay_and_different_facts_fail(self):
        state.record(self.db, "work", {"id": "w1", "title": "one"})
        state.record(self.db, "work", {"id": "w2", "title": "two"})
        facts = {"id": "u1", "work_id": "w1", "input_tokens": 100, "cached_input_tokens": 40, "output_tokens": 10}
        self.assertFalse(state.record(self.db, "usage", dict(facts))["replayed"])
        self.assertTrue(state.record(self.db, "usage", dict(facts))["replayed"])
        with self.assertRaisesRegex(ValueError, "different facts"):
            state.record(self.db, "usage", dict(facts, input_tokens=99))
        with self.assertRaisesRegex(ValueError, "different facts"):
            state.record(self.db, "usage", dict(facts, work_id="w2"))
        with self.assertRaisesRegex(ValueError, "exceeds one"):
            state.record(self.db, "usage", {"id": "u2", "allocations": [
                {"work_id": "w1", "weight": 0.6}, {"work_id": "w2", "weight": 0.6}]})
        with self.assertRaises(sqlite3.IntegrityError):
            state.record(self.db, "usage", {"id": "u3", "input_tokens": 10, "cached_input_tokens": 20})
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)


class MigrationTests(unittest.TestCase):
    def test_schema_two_database_upgrades_to_seven_and_keeps_rows(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "workflow.sqlite3"
        schema = (ROOT / "skills" / "devflow" / "scripts" / "schema.sql").read_text()
        statements, statement = [], ""
        for line in schema.splitlines(True):
            statement += line
            if sqlite3.complete_statement(statement):
                statements.append(statement)
                statement = ""
        legacy = sqlite3.connect(path)
        for text in statements:
            if all(name not in text for name in ("claims", "runtime_", "reconcile_")):  # schema 2 predates these
                legacy.execute(text)
        legacy.execute(f"PRAGMA application_id={state.APP_ID}")
        legacy.execute("PRAGMA user_version=2")
        legacy.execute("INSERT INTO works(id,title,status,created_at,updated_at) VALUES "
                       "('w1','kept','active','2026-01-01T00:00:00+00:00','2026-01-01T00:00:00+00:00')")
        legacy.commit()
        legacy.close()

        db = state.connect(path)
        self.addCleanup(db.close)
        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 7)
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertTrue({"claims", "runtime_sessions", "runtime_scopes", "runtime_events",
                         "reconcile_intents", "reconcile_cursor"} <= tables)
        self.assertEqual(state.row(db, "works", "w1")["title"], "kept")
        db.execute("BEGIN IMMEDIATE")
        self.assertEqual(state.claim_work(db, "w1", "task-a")["claim"]["resource"], "work:w1")
        db.commit()
        again = state.connect(path)
        self.addCleanup(again.close)
        self.assertEqual(again.execute("PRAGMA user_version").fetchone()[0], 7)


if __name__ == "__main__":
    unittest.main()
