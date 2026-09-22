"""Runtime collection: the transcript adapter, hook attribution and timestamp ordering."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TELEMETRY_SCRIPT = ROOT / "skills" / "devflow" / "scripts" / "telemetry.py"
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
                        "payload": {"model": "gpt-6-sol", "effort": "high", "turn_id": "t0"}}),
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
        self.assertEqual((session["model"], session["effort"]), ("gpt-6-sol", "high"))
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
    def test_batch_coordinator_usage_stays_unallocated_until_a_leaf_is_bound(self):
        state.record(self.db, "work", {"id": "w2", "title": "two"})
        for work in ("w1", "w2"):
            state.claim_work(self.db, work, "main-1")
        telemetry.handle(self.db, {"session_id": "main-1", "hook_event_name": "SubagentStart",
                                   "agent_id": "batch-1", "agent_type": "devflow-coordinator"})
        self.assertEqual(telemetry.scope(self.db, "batch-1"), ["w1", "w2"])
        telemetry.handle(self.db, {"session_id": "main-1", "agent_id": "batch-1", "turn_id": "t1",
                                   "hook_event_name": "PreToolUse", "tool_use_id": "dispatch",
                                   "tool_name": "spawn_agent", "tool_input": {}})
        self.assertIsNone(state.row(self.db, "runtime_events", "tool:batch-1:dispatch")["work_id"])
        telemetry.handle(self.db, {"session_id": "batch-1", "hook_event_name": "SubagentStart",
                                   "agent_id": "leaf-1", "agent_type": "devflow-implementer"})
        self.assertEqual(telemetry.scope(self.db, "leaf-1"), [])
        telemetry.bind(self.db, "leaf-1", ["w2"], "devflow-implementer", "batch-1")
        telemetry.handle(self.db, {"session_id": "main-1", "agent_id": "leaf-1", "turn_id": "t2",
                                   "hook_event_name": "PreToolUse", "tool_use_id": "edit",
                                   "tool_name": "apply_patch", "tool_input": {}})
        self.assertEqual(state.row(self.db, "runtime_events", "tool:leaf-1:edit")["work_id"], "w2")

    def test_native_nested_hooks_attribute_the_actor_and_resume_same_workers(self):
        state.claim_work(self.db, "w1", "main-1")
        self.db.commit()

        def hook(agent, role, event, **fields):
            payload = dict(session_id="main-1", agent_id=agent, agent_type=role,
                           hook_event_name=event, turn_id=agent + "-turn", **fields)
            result = subprocess.run([sys.executable, "-B", str(TELEMETRY_SCRIPT),
                                     "--db", str(self.root / "workflow.sqlite3"), "hook"],
                                    input=json.dumps(payload), capture_output=True, text=True, check=True,
                                    env=dict(os.environ, PYTHONPATH=str(ROOT / "skills/devflow/scripts")))
            return json.loads(result.stdout)

        transcripts = {}
        roles = {"execution-1": "devflow-coordinator", "worker-1": "devflow-implementer"}
        for parent, child in [("main-1", "execution-1"), ("execution-1", "worker-1")]:
            transcript = self.root / (child + ".jsonl")
            # Native hooks share the root session_id; rollout metadata names the immediate parent.
            transcript.write_text(json.dumps({"type": "session_meta", "payload": {
                "session_id": "main-1", "id": child, "parent_thread_id": parent,
                "source": {"subagent": {"thread_spawn": {"parent_thread_id": parent,
                                                         "agent_role": roles[child]}}}}}) + "\n")
            transcripts[child] = str(transcript)
            self.assertEqual(hook(child, roles[child], "SubagentStart", transcript_path=str(transcript)), {})

        repository = self.root / "native-repository"
        repository.mkdir()
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        subprocess.run(["git", "init", "-q", str(repository)], env=env, check=True)
        (repository / "app.txt").write_text("candidate\n")
        for child, expected in [("execution-1", ""), ("worker-1", "app.txt\n")]:
            with self.subTest(child=child):
                response = hook(child, roles[child], "PreToolUse", transcript_path=transcripts[child],
                                tool_use_id="stage", tool_name="Bash", tool_input={"command": "git add app.txt"})
                if response.get("hookSpecificOutput", {}).get("permissionDecision") != "deny":
                    subprocess.run(["git", "add", "app.txt"], cwd=repository, env=env, check=True)
                self.assertEqual(subprocess.check_output(["git", "diff", "--cached", "--name-only"],
                                                         cwd=repository, env=env, text=True), expected)
        self.assertEqual(self.session("worker-1")["parent_id"], "execution-1")
        self.assertEqual(telemetry.scope(self.db, "worker-1"), ["w1"])
        self.assertEqual(state.row(self.db, "runtime_events", "tool:worker-1:stage")["work_id"], "w1")
        self.assertIsNone(state.row(self.db, "runtime_events", "boundary:main-1:stage"))

        state.release_work(self.db, "w1", "main-1")
        self.db.commit()
        for child in ("worker-1", "execution-1"):
            hook(child, roles[child], "SubagentStop", agent_transcript_path=transcripts[child])
            self.assertIsNotNone(self.session(child)["closed_at"])
        # Activity without a reclaimed assignment must remain outside collection.
        self.assertEqual(hook("execution-1", roles["execution-1"], "PreToolUse",
                              tool_use_id="unrelated", tool_name="apply_patch", tool_input={}), {})
        self.assertIsNotNone(self.session("execution-1")["closed_at"])

        state.claim_work(self.db, "w1", "main-1")
        self.db.commit()
        for child in ("execution-1", "worker-1"):
            response = hook(child, roles[child], "PreToolUse", transcript_path=transcripts[child],
                            tool_use_id="resume", tool_name="apply_patch", tool_input={})
            self.assertIsNone(self.session(child)["closed_at"])
            self.assertEqual(response.get("hookSpecificOutput", {}).get("permissionDecision"),
                             "deny" if child == "execution-1" else None)

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
    def test_nested_execution_coordinator_keeps_scope_and_delegates_writes(self):
        state.claim_work(self.db, "w1", "main-1")
        for parent, child, role in [("main-1", "execution-1", "devflow-coordinator"),
                                    ("execution-1", "worker-1", "devflow-implementer")]:
            telemetry.handle(self.db, {"hook_event_name": "SubagentStart", "session_id": parent,
                                       "agent_id": child, "agent_type": role})
        self.assertEqual(telemetry.scope(self.db, "worker-1"), ["w1"])
        self.assertEqual(self.session("worker-1")["parent_id"], "execution-1")
        self.assertEqual(state.claim_for(self.db, "w1")["owner"], "main-1")

        repository = self.root / "nested-repository"
        repository.mkdir()
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        subprocess.run(["git", "init", "-q", str(repository)], env=env, check=True)
        (repository / "app.txt").write_text("candidate\n")

        for session, expected in [("execution-1", ""), ("worker-1", "app.txt\n")]:
            with self.subTest(session=session):
                response = telemetry.handle(self.db, {
                    "session_id": session, "hook_event_name": "PreToolUse", "turn_id": "t1",
                    "tool_use_id": "stage", "tool_name": "Bash", "tool_input": {"command": "git add app.txt"}})
                if response is None:
                    subprocess.run(["git", "add", "app.txt"], cwd=repository, env=env, check=True)
                staged = subprocess.check_output(["git", "diff", "--cached", "--name-only"],
                                                 cwd=repository, env=env, text=True)
                self.assertEqual(staged, expected)
        boundary = state.row(self.db, "runtime_events", "boundary:execution-1:stage")
        self.assertIsNotNone(boundary)
        self.assertEqual(boundary["work_id"], "w1")
        self.assertEqual(state.row(self.db, "runtime_events", "tool:worker-1:stage")["work_id"], "w1")

    def test_later_dry_run_cannot_allow_an_earlier_index_write(self):
        state.claim_work(self.db, "w1", "root-1")
        repository = self.root / "repository"
        repository.mkdir()
        env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM="1")
        subprocess.run(["git", "init", "-q", str(repository)], env=env, check=True)
        (repository / "app.txt").write_text("candidate\n")
        command = "git add app.txt && git clean -n"
        result = telemetry.handle(self.db, {
            "session_id": "root-1", "hook_event_name": "PreToolUse", "turn_id": "t1",
            "tool_use_id": "compound-write", "tool_name": "Bash", "tool_input": {"command": command}})
        if result is None:
            subprocess.run(["/bin/sh", "-c", command], cwd=repository, env=env, check=True)
        staged = subprocess.check_output(["git", "diff", "--cached", "--name-only"],
                                         cwd=repository, env=env, text=True)
        self.assertEqual(staged, "", "the hook allowed the compound command to stage app.txt")
        self.assertEqual(result["hookSpecificOutput"]["permissionDecision"], "deny")

    def test_git_inspection_flags_are_scoped_to_the_command_and_options(self):
        commands = ["git add app.txt;git clean -n", "git add app.txt\ngit clean -n",
                    "git add app.txt || git clean -n", "git add app.txt | git clean -n",
                    "git add -- -n", "git tag -- -n", "git apply --stat --apply fix.patch",
                    "git apply --check fix.patch && git add app.txt"]
        for command in commands:
            with self.subTest(command=command):
                self.assertIsNotNone(telemetry.boundary_violation("Bash", {"command": command}))

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
        denied = ["git -C /repo commit -m x", "sed -i '' 's/a/b/' src/x.py", "pytest -q > report.txt",
                  "cat notes | tee docs/x.md", "git tag v1.2.0", "git tag -d v1.1.0", "git stash", "git stash pop",
                  "git clean -fd", "git apply fix.patch", "git add -A"]
        allowed = ["pytest -q 2>&1 | tail -5 > /tmp/out.txt",
                   'python3.12 state.py record result --id r1 --work-id w1 --kind qa --status passed --summary "a > b"',
                   "git status && git log --oneline -3 && git worktree add ../wt feature",
                   "git tag --list", "git tag", "git tag -l 'v*'", "git tag --contains abc123", "git stash list",
                   "git stash show -p stash@{0}", "git clean -nd", "git add --dry-run .", "git apply --check fix.patch",
                   "git add --dry-run .;git clean -n", "git tag --list -- -n",
                   "gh pr merge 36 --squash --match-head-commit abc123", "gh stack merge 7 --yes --squash"]
        for number, command in enumerate(denied, start=100):
            self.assertIsNotNone(call("root-1", "shell", {"command": command}, f"d{number}"), command)
        for number, command in enumerate(allowed, start=200):
            self.assertIsNone(call("root-1", "shell", {"command": command}, f"a{number}"), command)
        self.assertIsNone(call("worker-1", "apply_patch", {"patch": "*** Begin Patch"}, "c9"))
        self.assertEqual(state.row(self.db, "runtime_events", "tool:worker-1:c9")["status"], "started")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM runtime_events WHERE kind='boundary'").fetchone()[0],
                         len(denied) + 1)

    def test_denial_survives_transcript_collection_failure(self):
        state.claim_work(self.db, "w1", "root-1")
        transcript = self.root / "root.jsonl"
        transcript.write_text("")
        self.db.execute("UPDATE runtime_sessions SET transcript_path=?, cursor=999 WHERE id='root-1'", (str(transcript),))
        self.db.commit()  # release the lock for the hook process
        payload = {"session_id": "root-1", "hook_event_name": "PreToolUse", "turn_id": "t1", "tool_use_id": "c1",
                   "tool_name": "apply_patch", "tool_input": {"patch": "*** Begin Patch"}, "transcript_path": str(transcript)}
        completed = subprocess.run(
            [sys.executable, "-B", str(ROOT / "skills" / "devflow" / "scripts" / "telemetry.py"),
             "--db", str(self.root / "workflow.sqlite3"), "hook"],
            input=json.dumps(payload), text=True, capture_output=True, check=True)
        output = json.loads(completed.stdout)
        self.assertEqual(output["hookSpecificOutput"]["permissionDecision"], "deny")
        self.assertIn("transcript truncated", output["systemMessage"])
        self.assertEqual(state.row(self.db, "runtime_events", "boundary:root-1:c1")["status"], "denied")


if __name__ == "__main__":
    unittest.main()
