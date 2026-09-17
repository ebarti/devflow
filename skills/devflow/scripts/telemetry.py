#!/usr/bin/env python3.12
"""Collect content-free local runtime observations for explicitly bound work."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shlex
import sqlite3
import sys
import time

import state
from state import bind, scope

EVENTS = ("SessionStart", "UserPromptSubmit", "PreToolUse", "PostToolUse",
          "PermissionRequest", "PreCompact", "PostCompact", "SubagentStart",
          "SubagentStop", "Stop", "Interrupt", "SessionEnd")
TOKEN_FIELDS = ("input_tokens", "cached_input_tokens", "cache_write_tokens",
                "output_tokens", "reasoning_output_tokens")


def identity(*parts):
    return hashlib.sha256(state.encode(parts).encode()).hexdigest()


def event(db, session, key, kind, timestamp, turn_id=None, **values):
    work_ids = scope(db, session)
    item = dict(id=key, session_id=session, work_id=work_ids[0] if len(work_ids) == 1 else None,
                kind=kind, turn_id=turn_id, started_at=timestamp, **values)
    existing = state.row(db, "runtime_events", key)
    if existing:
        starts = [x for x in (existing["started_at"], timestamp) if x]
        item["started_at"] = min(starts, key=state.instant) if starts else None
        for name in ("ended_at", "status", "name", "fingerprint", "source_ref"):
            if item.get(name) is None:
                item[name] = existing[name]
        if existing["ended_at"] and item.get("ended_at"):
            item["ended_at"] = max(existing["ended_at"], item["ended_at"], key=state.instant)
    if item.get("ended_at") and item.get("started_at"):
        item["duration_seconds"] = max(0, (state.instant(item["ended_at"]) -
                                          state.instant(item["started_at"])).total_seconds())
    if existing:
        fields = [k for k in item if k != "id"]
        db.execute("UPDATE runtime_events SET " + ",".join(k + "=?" for k in fields) +
                   " WHERE id=?", (*[item[k] for k in fields], key))
    else:
        state.insert(db, "runtime_events", item)
    return state.row(db, "runtime_events", key)


def turn(db, session, turn_id, timestamp, status=None):
    if not turn_id:
        return None
    key = "turn:" + session["id"] + ":" + turn_id
    existing = state.row(db, "runtime_events", key)
    if status is None and existing and existing["ended_at"]:
        if state.instant(timestamp) <= state.instant(existing["ended_at"]):
            status = existing["status"]
            timestamp = existing["ended_at"]
        else:
            db.execute("UPDATE runtime_events SET ended_at=NULL,duration_seconds=NULL WHERE id=?", (key,))
    row = event(db, session["id"], key, "turn",
                timestamp, turn_id, status=status or "active",
                ended_at=timestamp if status else None, source_ref=session["transcript_path"])
    work_ids = scope(db, session["id"])
    run_id = "runtime:" + session["id"] + ":" + turn_id
    if len(work_ids) == 1:
        values = dict(id=run_id, work_id=work_ids[0], agent=session["id"],
                      role=session["role"], model=session["model"], effort=session["effort"],
                      status=row["status"], started_at=row["started_at"], ended_at=row["ended_at"],
                      duration_seconds=row["duration_seconds"], source_ref=session["transcript_path"],
                      details=state.encode({"basis": "observed turn interval, including tool waits"}))
        saved = state.row(db, "runs", run_id)
        if not saved:
            state.insert(db, "runs", dict(values, recorded_at=state.now()))
        else:
            # A stop followed by a continuation keeps one turn identity.
            columns = [k for k in values if k not in ("id", "work_id")]
            db.execute("UPDATE runs SET " + ",".join(k + "=?" for k in columns) +
                       " WHERE id=?", (*[values[k] for k in columns], run_id))
        return run_id
    return None


def tokens(db, session, payload, timestamp, position, turn_id):
    raw = (payload.get("info") or {}).get("total_token_usage")
    if not isinstance(raw, dict):
        return
    values = {k: raw.get("cache_write_input_tokens" if k == "cache_write_tokens" else k)
              for k in TOKEN_FIELDS}
    if any(v is not None and (type(v) is not int or v < 0) for v in values.values()):
        raise ValueError("unsupported token counter")
    if values["input_tokens"] is None or values["output_tokens"] is None:
        return
    prior = {k: session[k] for k in TOKEN_FIELDS}
    after_binding = state.instant(timestamp) >= state.instant(session["bound_at"])
    reset = any(prior[k] is not None and values[k] is not None and values[k] < prior[k]
                for k in TOKEN_FIELDS)
    if reset and after_binding:
        event(db, session["id"], identity(session["id"], position, "reset"), "counter_reset",
              timestamp, turn_id, status="gap", source_ref=session["transcript_path"])
    if after_binding and not reset:
        delta = {k: None if values[k] is None or (prior["input_tokens"] is not None and prior[k] is None)
                 else values[k] - (prior[k] or 0)
                 for k in TOKEN_FIELDS}
        if any(v for v in delta.values() if v is not None):
            run_id = turn(db, session, turn_id, timestamp)
            work_ids = scope(db, session["id"])
            state.record(db, "usage", dict(
                id="runtime:" + session["id"] + ":" + str(position),
                run_id=run_id, agent=session["id"], model=session["model"],
                **delta, recorded_at=timestamp,
                allocations=[{"work_id": work_ids[0], "weight": 1.0}] if len(work_ids) == 1 else [],
                source_ref=session["transcript_path"] + "#byte=" + str(position),
                details={"basis": "cumulative counter delta", "collector": "codex-jsonl-v1"}))
    session.update(values)


def collect_tool(db, session, payload, timestamp, turn_id, position):
    kind = payload.get("type")
    calls = {"function_call", "custom_tool_call"}
    completions = {"function_call_output", "custom_tool_call_output"}
    call_id = payload.get("call_id")
    if kind not in calls | completions or not call_id:
        return
    key = "tool:" + session["id"] + ":" + call_id
    saved = state.row(db, "runtime_events", key)
    if saved and saved["ended_at"]:
        return  # Preserve completed hook observations and make replay a no-op.
    source = session["transcript_path"] + "#byte=" + str(position)
    if kind in calls:
        name = payload.get("name")
        event(db, session["id"], key, "tool", timestamp, turn_id, name=name,
              fingerprint=identity(name, payload.get("arguments", payload.get("input"))),
              status="started", source_ref=source)
    else:
        output = payload.get("output")
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except ValueError:
                pass
        event(db, session["id"], key, "tool", None, turn_id,
              ended_at=timestamp, status=tool_status(output), source_ref=source)


def collect_transcript(db, session, transcript, collect_tools=False):
    if not transcript:
        return
    path = Path(transcript).expanduser()
    if session["transcript_path"] not in (None, str(path)):
        raise ValueError("transcript changed; recorded cursor retained")
    session["transcript_path"] = str(path)
    cursor = session["cursor"]
    with path.open("rb") as stream:
        if path.stat().st_size < cursor:
            raise ValueError("transcript truncated; recorded cursor retained")
        stream.seek(cursor)
        active_turn = session["turn_id"]
        while True:
            position = stream.tell()
            line = stream.readline()
            if not line or not line.endswith(b"\n"):
                break
            try:
                item = json.loads(line)
                if not isinstance(item, dict):
                    raise ValueError("transcript line is not an object")
            except ValueError:
                # One malformed line must not stall collection: record the gap and move on.
                event(db, session["id"], identity(session["id"], position, "malformed"), "transcript_gap",
                      state.now(), active_turn, status="skipped",
                      source_ref=session["transcript_path"] + "#byte=" + str(position))
                cursor = stream.tell()
                continue
            payload = item.get("payload") or {}
            timestamp = item.get("timestamp")
            if item.get("type") == "turn_context":
                session["model"] = payload.get("model") or session["model"]
                session["effort"] = payload.get("effort") or payload.get("reasoning_effort") or session["effort"]
                active_turn = payload.get("turn_id") or active_turn
            if (collect_tools and item.get("type") == "response_item" and timestamp
                    and state.instant(timestamp) >= state.instant(session["bound_at"])):
                collect_tool(db, session, payload, timestamp, active_turn, position)
            if item.get("type") == "event_msg":
                name = payload.get("type")
                active_turn = payload.get("turn_id") or active_turn
                if name == "token_count":
                    tokens(db, session, payload, timestamp, position, active_turn)
                elif name in ("task_started", "task_complete", "task_completed", "turn_aborted"):
                    if timestamp and state.instant(timestamp) >= state.instant(session["bound_at"]):
                        turn(db, session, active_turn, timestamp,
                             {"task_complete": "completed", "task_completed": "completed",
                              "turn_aborted": "interrupted"}.get(name))
            cursor = stream.tell()
    db.execute("""UPDATE runtime_sessions SET transcript_path=?,cursor=?,model=?,effort=?,turn_id=?,
        input_tokens=?,cached_input_tokens=?,cache_write_tokens=?,output_tokens=?,
        reasoning_output_tokens=? WHERE id=?""",
        (str(path), cursor, session["model"], session["effort"], active_turn,
         *[session[k] for k in TOKEN_FIELDS], session["id"]))


def tool_status(response):
    if isinstance(response, dict):
        if response.get("isError") is True or response.get("error"):
            return "failed"
        code = response.get("exit_code")
        if type(code) is int:
            return "passed" if code == 0 else "failed"
        # Transport success does not prove the invoked command/product passed.
        return "returned"
    return "returned"


def handle(db, payload):
    started = time.monotonic()
    parent = payload.get("session_id")
    saved = db.execute("SELECT * FROM runtime_sessions WHERE id=?", (parent,)).fetchone()
    if not saved or saved["closed_at"]:
        return
    session = dict(saved)
    name = payload.get("hook_event_name")
    stamp = state.now()
    turn_id = payload.get("turn_id")
    if name in ("SubagentStart", "SubagentStop"):
        child = payload.get("agent_id")
        if child:
            child_saved = db.execute("SELECT id FROM runtime_sessions WHERE id=?", (child,)).fetchone()
            if not child_saved and len(scope(db, parent)) != 1:
                event(db, parent, identity(child, name, turn_id), name, stamp, turn_id,
                      name=payload.get("agent_type"), status="unallocated")
                return
            if not child_saved:
                bind(db, child, scope(db, parent), payload.get("agent_type"), parent)
            session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (child,)).fetchone())
            if name == "SubagentStop":
                collect_transcript(db, session, payload.get("agent_transcript_path"), collect_tools=True)
                # The parent's turn ID is not the child's turn ID.
                session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (child,)).fetchone())
                turn(db, session, session["turn_id"], stamp, "completed")
            event(db, child, identity(child, name, session["turn_id"], session["cursor"]), name, stamp,
                  name=payload.get("agent_type"), status="stopped" if name == "SubagentStop" else "started")
            db.execute("UPDATE runtime_sessions SET last_seen_at=? WHERE id=?", (stamp, child))
            if name == "SubagentStop":
                close_if_released(db, child, stamp)
    else:
        session["model"] = payload.get("model") or session["model"]
        db.execute("UPDATE runtime_sessions SET model=? WHERE id=?", (session["model"], parent))
        collect_transcript(db, session, payload.get("transcript_path"))
        session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (parent,)).fetchone())
        if name in ("PreToolUse", "PostToolUse"):
            use_id = payload.get("tool_use_id")
            if not use_id:
                raise ValueError("tool hook lacks tool_use_id")
            signature = identity(payload.get("tool_name"), payload.get("tool_input"))
            tool_key = "tool:" + parent + ":" + use_id
            tool_started = stamp if name == "PreToolUse" else None
            previous = state.row(db, "runtime_events", tool_key)
            if not previous or previous["ended_at"] is None:
                event(db, parent, tool_key, "tool", tool_started, turn_id,
                      name=payload.get("tool_name"), fingerprint=signature,
                      ended_at=stamp if name == "PostToolUse" else None,
                      status=tool_status(payload.get("tool_response")) if name == "PostToolUse" else "started")
        else:
            event(db, parent, identity(parent, name, turn_id, payload.get("source"), session["cursor"]), name, stamp,
                  turn_id, name=payload.get("source") or payload.get("trigger"))
        if name in ("Stop", "Interrupt"):
            turn(db, session, turn_id, stamp, "completed" if name == "Stop" else "interrupted")
            close_if_released(db, parent, stamp)
        elif name == "SessionEnd":
            db.execute("UPDATE runtime_sessions SET closed_at=? WHERE id=?", (stamp, parent))
        elif name in ("PreToolUse", "UserPromptSubmit"):
            turn(db, session, turn_id, stamp)
    db.execute("UPDATE runtime_sessions SET last_seen_at=? WHERE id=?", (stamp, parent))
    # Runtime collector overhead is measured directly, not inferred from coordinator usage.
    event(db, parent, identity(parent, name, turn_id, payload.get("tool_use_id"), stamp),
          "collector", stamp, turn_id, duration_seconds=time.monotonic() - started, status="completed")


def close_if_released(db, session_id, timestamp):
    active = db.execute("""SELECT 1 FROM claims c JOIN runtime_scopes s ON s.work_id=c.work_id
        WHERE s.session_id=? LIMIT 1""", (session_id,)).fetchone()
    if not active:
        db.execute("UPDATE runtime_sessions SET closed_at=? WHERE id=?", (timestamp, session_id))


def install(codex_home):
    path = Path(codex_home).expanduser() / "hooks.json"
    original = json.loads(path.read_text()) if path.exists() else {}
    hooks = original.setdefault("hooks", {})
    command = shlex.join([str(Path(sys.executable).resolve()), "-B",
                          str(Path(__file__).absolute()), "hook"])
    for name in EVENTS:
        groups = hooks.setdefault(name, [])
        for group in groups:
            group["hooks"] = [h for h in group.get("hooks", [])
                              if h.get("statusMessage") != "Record Devflow metrics"]
        groups[:] = [g for g in groups if g.get("hooks")]
        groups.append({"hooks": [{"type": "command", "command": command, "timeout": 3,
                                  "statusMessage": "Record Devflow metrics"}]})
    path.parent.mkdir(parents=True, exist_ok=True)
    # Preserve other hooks and atomically replace only the merged configuration.
    temporary = path.with_suffix(".json.tmp-" + str(os.getpid()))
    temporary.write_text(json.dumps(original, indent=2) + "\n")
    temporary.chmod(0o600)
    temporary.replace(path)
    return {"hooks_file": str(path), "events": list(EVENTS),
            "activation": "Review and trust these hooks with /hooks before using them."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=str(Path(os.environ.get("XDG_STATE_HOME",
                        str(Path.home() / ".local/state"))) / "devflow/workflow.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    binding = commands.add_parser("bind")
    binding.add_argument("--session-id", required=True)
    binding.add_argument("--work-id", required=True, action="append")
    binding.add_argument("--role")
    installing = commands.add_parser("install")
    installing.add_argument("--codex-home", default=os.environ.get("CODEX_HOME", str(Path.home() / ".codex")))
    commands.add_parser("hook")
    args = parser.parse_args()
    try:
        if args.command == "install":
            result = install(args.codex_home)
        elif args.command == "hook":
            payload = json.load(sys.stdin)
            if not isinstance(payload, dict):
                raise ValueError("hook input must be an object")
            # Unrelated conversations do not initialize or migrate a metrics database.
            if not Path(args.db).expanduser().exists():
                print("{}")
                return 0
            with closing(sqlite3.connect(Path(args.db).expanduser().resolve().as_uri() + "?mode=ro", uri=True)) as check:
                present = check.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='runtime_sessions'").fetchone()
                bound = present and check.execute("SELECT 1 FROM runtime_sessions WHERE id=? AND closed_at IS NULL", (payload.get("session_id"),)).fetchone()
            if not bound:
                print("{}")
                return 0
            with closing(state.connect(args.db)) as db, db:
                db.execute("BEGIN IMMEDIATE")
                handle(db, payload)
            result = {}
        else:
            with closing(state.connect(args.db)) as db, db:
                db.execute("BEGIN IMMEDIATE")
                result = bind(db, args.session_id, args.work_id, args.role)
        print(state.encode(result))
        return 0
    except (ValueError, OSError, sqlite3.Error) as exc:
        if args.command == "hook":
            print(state.encode({"systemMessage": "Devflow metrics collection failed: " + str(exc)}))
            return 0
        print(state.encode({"error": str(exc)}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
