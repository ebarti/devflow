"""Runtime collection: the transcript adapter, hook attribution and timestamp ordering."""
import json
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "skills" / "devflow" / "scripts"))

import state  # noqa: E402
import telemetry  # noqa: E402


def transcript_stamp(base, seconds):
    """Codex transcripts use millisecond precision with a Z suffix, unlike state.now()."""
    moment = state.instant(base) + timedelta(seconds=seconds)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


class TelemetryCase(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.db = state.connect(self.root / "workflow.sqlite3")
        self.addCleanup(self.db.close)
        self.db.execute("BEGIN IMMEDIATE")
        state.record(self.db, "work", {"id": "w1", "title": "one", "status": "active"})
        telemetry.bind(self.db, "s1", ["w1"], "coordinator")

    def session(self, session_id="s1"):
        return dict(self.db.execute("SELECT * FROM runtime_sessions WHERE id=?", (session_id,)).fetchone())

    def rows(self, query, *args):
        return [dict(r) for r in self.db.execute(query, args)]


class TranscriptTests(TelemetryCase):
    def test_counter_deltas_skipped_lines_resets_partial_lines_and_replay(self):
        session = self.session()

        def stamp(seconds):
            return transcript_stamp(session["bound_at"], seconds)

        def usage(at, turn, **counts):
            return json.dumps({"type": "event_msg", "timestamp": stamp(at), "payload": {
                "type": "token_count", "turn_id": turn, "info": {"total_token_usage": counts}}})

        lines = [
            json.dumps({"type": "turn_context", "timestamp": stamp(-10),
                        "payload": {"model": "gpt-5.6-sol", "effort": "high", "turn_id": "t0"}}),
            usage(-9, "t0", input_tokens=100, cached_input_tokens=20, output_tokens=10, reasoning_output_tokens=4),
            "{not JSON: this line may have carried the last counter before binding",
            usage(1, "t1", input_tokens=1100, cached_input_tokens=220, output_tokens=110, reasoning_output_tokens=44),
            usage(2, "t1", input_tokens=1150, cached_input_tokens=230, output_tokens=125, reasoning_output_tokens=49),
            usage(3, "t2", input_tokens=5, cached_input_tokens=0, output_tokens=1, reasoning_output_tokens=0),
        ]
        partial = json.dumps({"type": "event_msg", "timestamp": stamp(4),
                              "payload": {"type": "task_complete", "turn_id": "t2"}})
        complete = "\n".join(lines) + "\n"
        path = self.root / "session.jsonl"
        path.write_text(complete + partial[:-5])

        telemetry.collect_transcript(self.db, session, str(path))

        # The pre-binding counter is a baseline, the skipped line invalidates it, the first counter
        # after binding re-baselines without charging the hidden 1,000 tokens, and only the next
        # delta becomes usage.
        recorded = self.rows("SELECT input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens "
                             "FROM usage ORDER BY recorded_at")
        self.assertEqual(recorded, [
            {"input_tokens": 50, "cached_input_tokens": 10, "output_tokens": 15, "reasoning_output_tokens": 5}])
        self.assertEqual(self.rows("SELECT work_id,weight FROM usage_allocations"), [{"work_id": "w1", "weight": 1.0}])
        gaps = {r["kind"]: r for r in self.rows(
            "SELECT kind,status,source_ref FROM runtime_events "
            "WHERE kind IN ('transcript_gap','counter_baseline','counter_reset')")}
        self.assertEqual({kind: row["status"] for kind, row in gaps.items()},
                         {"transcript_gap": "skipped", "counter_baseline": "gap", "counter_reset": "gap"})
        offset = len(lines[0]) + 1 + len(lines[1]) + 1
        self.assertTrue(gaps["transcript_gap"]["source_ref"].endswith("#byte=" + str(offset)))
        session = self.session()
        self.assertEqual((session["model"], session["effort"]), ("gpt-5.6-sol", "high"))
        self.assertEqual(session["cursor"], len(complete.encode()))  # the partial line waits
        self.assertEqual(session["input_tokens"], 5)  # counters follow the reset

        path.write_text(complete + partial + "\n")
        telemetry.collect_transcript(self.db, self.session(), str(path))
        self.assertEqual(self.session()["cursor"], len((complete + partial + "\n").encode()))
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM usage").fetchone()[0], 1)  # no replay duplicates
        self.assertEqual(state.row(self.db, "runtime_events", "turn:s1:t2")["status"], "completed")


class OrderingTests(TelemetryCase):
    def test_turn_boundaries_compare_instants_not_strings(self):
        session = self.session()
        started = transcript_stamp(session["bound_at"], 10)[:-5] + "Z"  # second precision, Z suffix
        telemetry.turn(self.db, session, "t1", started)
        ended = (state.instant(started) + timedelta(milliseconds=500)).isoformat()  # +00:00 suffix
        telemetry.turn(self.db, session, "t1", ended, "completed")
        row = state.row(self.db, "runtime_events", "turn:s1:t1")
        self.assertEqual((row["started_at"], row["ended_at"]), (started, ended))
        self.assertAlmostEqual(row["duration_seconds"], 0.5)


class HookTests(TelemetryCase):
    def test_hooks_attribute_tools_and_turns_to_the_bound_work(self):
        events = [
            {"session_id": "s1", "hook_event_name": "UserPromptSubmit", "turn_id": "t1"},
            {"session_id": "s1", "hook_event_name": "PreToolUse", "turn_id": "t1", "tool_use_id": "call-1",
             "tool_name": "shell", "tool_input": {"command": "pytest -q"}},
            {"session_id": "s1", "hook_event_name": "PostToolUse", "turn_id": "t1", "tool_use_id": "call-1",
             "tool_name": "shell", "tool_input": {"command": "pytest -q"}, "tool_response": {"exit_code": 1}},
            {"session_id": "s1", "hook_event_name": "Stop", "turn_id": "t1"},
        ]
        for payload in events:
            telemetry.handle(self.db, payload)
        tool = state.row(self.db, "runtime_events", "tool:s1:call-1")
        self.assertEqual((tool["work_id"], tool["name"], tool["status"]), ("w1", "shell", "failed"))
        self.assertIsNotNone(tool["duration_seconds"])
        self.assertEqual(len(tool["fingerprint"]), 64)
        self.assertNotIn("pytest", json.dumps(tool))  # content-free: only a fingerprint is stored
        run = state.row(self.db, "runs", "runtime:s1:t1")
        self.assertEqual((run["work_id"], run["role"], run["status"]), ("w1", "coordinator", "completed"))

        before = self.db.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0]
        telemetry.handle(self.db, {"session_id": "nobody", "hook_event_name": "Stop", "turn_id": "t9"})
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM runtime_events").fetchone()[0], before)

        state.record(self.db, "work", {"id": "w2", "title": "two"})
        telemetry.bind(self.db, "s2", ["w1", "w2"], "coordinator")
        telemetry.handle(self.db, {"session_id": "s2", "hook_event_name": "PreToolUse", "turn_id": "t1",
                                   "tool_use_id": "call-2", "tool_name": "shell", "tool_input": {}})
        self.assertIsNone(state.row(self.db, "runtime_events", "tool:s2:call-2")["work_id"])  # two issues stay unallocated


class BoundaryTests(TelemetryCase):
    def test_coordinator_sessions_cannot_modify_the_repository(self):
        state.claim_work(self.db, "w1", "root-1")  # claiming binds root-1 as the coordinator
        telemetry.bind(self.db, "worker-1", ["w1"], "devflow-implementer")

        def call(session, tool, tool_input, use_id):
            return telemetry.handle(self.db, {"session_id": session, "hook_event_name": "PreToolUse", "turn_id": "t1",
                                              "tool_use_id": use_id, "tool_name": tool, "tool_input": tool_input})

        denied = call("root-1", "apply_patch", {"patch": "*** Begin Patch"}, "c1")
        self.assertEqual(denied["hookSpecificOutput"]["permissionDecision"], "deny")
        self.assertEqual(state.row(self.db, "runtime_events", "boundary:root-1:c1")["status"], "denied")
        self.assertIsNone(state.row(self.db, "runtime_events", "tool:root-1:c1"))
        for use_id, command in (("c2", "git -C /repo commit -m x"), ("c3", "sed -i '' 's/a/b/' src/x.py"),
                                ("c4", "pytest -q > report.txt"), ("c5", "cat notes | tee docs/x.md")):
            self.assertIsNotNone(call("root-1", "shell", {"command": command}, use_id), command)
        for use_id, command in (("c6", "pytest -q 2>&1 | tail -5 > /tmp/out.txt"),
                                ("c7", 'python3.12 state.py record result --id r1 --work-id w1 --kind qa --status passed --summary "a > b"'),
                                ("c8", "git status && git log --oneline -3 && git worktree add ../wt feature")):
            self.assertIsNone(call("root-1", "shell", {"command": command}, use_id), command)
        self.assertIsNone(call("worker-1", "apply_patch", {"patch": "*** Begin Patch"}, "c9"))
        self.assertEqual(state.row(self.db, "runtime_events", "tool:worker-1:c9")["status"], "started")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM runtime_events WHERE kind='boundary'").fetchone()[0], 5)


if __name__ == "__main__":
    unittest.main()
