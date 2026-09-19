#!/usr/bin/env python3.12
"""Collect content-free local runtime observations for explicitly bound work."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
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
# Neither the main claim holder nor its execution coordinator changes the repository;
# recognize both roles without restricting their leaf workers.
COORDINATOR_ROLES = {"coordinator", "devflow-coordinator"}
EDIT_TOOL_WORDS = ("patch", "write", "edit", "create_file")
MUTATING_GIT = {"commit", "push", "merge", "rebase", "cherry-pick", "revert", "apply", "am",
                "reset", "restore", "stash", "clean", "rm", "mv", "add", "tag"}
COMMAND_STARTS = {"&&", "||", "|", ";", "&", "\n", "(", ")", "{", "}", "then", "do"}
SAFE_REDIRECT_PREFIXES = ("/dev/", "/tmp/", "$TMPDIR", "${TMPDIR")


def read_only_git(arguments):
    """True for the inspection forms of otherwise mutating Git subcommands."""
    subcommand, options = arguments[0], arguments[1:]
    operands = []
    if "--" in options:
        end = options.index("--")
        options, operands = options[:end], options[end + 1:]
    if subcommand == "tag":
        listing = ("-l", "--list", "-n", "--contains", "--no-contains", "--points-at", "--merged", "--no-merged")
        return (not operands and not any(not option.startswith("-") for option in options)) or any(
            option == flag or option.startswith(flag + "=") or (flag == "-n" and re.match(r"^-n\d*$", option))
            for option in options for flag in listing)
    if subcommand == "stash":
        return bool(options) and options[0] in ("list", "show")
    if subcommand in ("clean", "add", "rm", "mv"):
        return any(option == "--dry-run" or re.match(r"^-[a-zA-Z]*n", option) for option in options)
    if subcommand == "apply":
        return "--apply" not in options and any(
            option in ("--check", "--stat", "--numstat", "--summary") for option in options)
    return False


def command_boundary(token):
    return token in COMMAND_STARTS or bool(token) and all(char in ";&|()\n" for char in token)


def command_text(tool_input):
    if isinstance(tool_input, dict):
        for key in ("command", "cmd", "script"):
            value = tool_input.get(key)
            if isinstance(value, list):
                return " ".join(str(item) for item in value)
            if isinstance(value, str):
                return value
    return None


def boundary_violation(tool_name, tool_input):
    """Return why a coordinator may not make this tool call, or None when it is allowed."""
    lowered = (tool_name or "").lower()
    if any(word in lowered for word in EDIT_TOOL_WORDS):
        return f"{tool_name} edits files; dispatch the devflow-implementer worker"
    command = command_text(tool_input)
    if not command:
        return None
    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|()\n")
        lexer.whitespace = " \t\r"
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        tokens = command.split()
    for index, token in enumerate(tokens):
        leading = index == 0 or command_boundary(tokens[index - 1])
        if token == "git":
            rest = []
            for argument in tokens[index + 1:]:
                if command_boundary(argument):
                    break
                rest.append(argument)
            while rest and rest[0].startswith("-"):
                rest = rest[2:] if rest[0] in ("-C", "-c", "--git-dir", "--work-tree") else rest[1:]
            if rest and rest[0] in MUTATING_GIT and not read_only_git(rest):
                return f"git {rest[0]} changes the repository; dispatch the devflow-implementer worker"
        if leading and token in ("patch", "tee"):
            return f"{token} writes files; dispatch the devflow-implementer worker"
        if leading and token in ("sed", "perl"):
            for argument in tokens[index + 1:]:
                if command_boundary(argument):
                    break
                if re.match(r"^-[a-zA-Z]*i", argument) or argument.startswith("--in-place"):
                    return f"{token} in-place edits belong to the devflow-implementer worker"
        redirect = re.match(r"^(\d*)(>{1,2})(.*)$", token)
        if redirect:
            target = redirect.group(3) or (tokens[index + 1] if index + 1 < len(tokens) else "")
            if not target.startswith("&") and not target.startswith(SAFE_REDIRECT_PREFIXES):
                return f"shell redirection writes {target or 'a file'}; coordinators only write under temporary paths"
    return None


def boundary_decision(db, payload):
    """Why a bound main/execution coordinator's PreToolUse is denied. Read-only."""
    if payload.get("hook_event_name") != "PreToolUse":
        return None
    saved = db.execute("SELECT role, closed_at FROM runtime_sessions WHERE id=?",
                       (payload.get("session_id"),)).fetchone()
    if not saved or saved[1] or saved[0] not in COORDINATOR_ROLES:
        return None
    return boundary_violation(payload.get("tool_name"), payload.get("tool_input"))


def child_binding(db, payload):
    """Resolve an unbound/resumed child from native hook and rollout identities. Read-only."""
    child = payload.get("agent_id")
    if not child:
        return None
    saved = db.execute("SELECT parent_id,role,closed_at FROM runtime_sessions WHERE id=?", (child,)).fetchone()
    if saved and not saved[2]:
        return None
    if saved and payload.get("hook_event_name") not in ("SubagentStart", "UserPromptSubmit", "PreToolUse"):
        return None
    parent, role = (saved[0], saved[1]) if saved else (None, payload.get("agent_type"))
    transcript = payload.get("agent_transcript_path") or payload.get("transcript_path")
    if not parent and transcript:
        try:
            with Path(transcript).expanduser().open() as stream:
                item = json.loads(stream.readline())
            meta = item.get("payload") or {}
            if item.get("type") != "session_meta" or meta.get("id") != child:
                return None
            source = meta.get("source") or {}
            spawned = source.get("subagent", {}).get("thread_spawn", {}) if isinstance(source, dict) else {}
            parent = meta.get("parent_thread_id") or spawned.get("parent_thread_id")
            role = role or meta.get("agent_role") or spawned.get("agent_role")
        except (OSError, ValueError, AttributeError):
            return None
    if not parent and not transcript:
        # Legacy hosts supplied the immediate parent and no child transcript at start.
        parent = payload.get("session_id")
    if not parent or parent == child:
        return None
    active_parent = db.execute("SELECT 1 FROM runtime_sessions WHERE id=? AND closed_at IS NULL", (parent,)).fetchone()
    if not active_parent:
        return None
    work_ids = scope(db, child) if saved else scope(db, parent)
    if (not work_ids or (len(work_ids) != 1 and role not in COORDINATOR_ROLES)
            or not set(work_ids).issubset(scope(db, parent))):
        return None  # A batch needs explicit per-child attribution.
    if saved and not db.execute("SELECT 1 FROM claims WHERE work_id IN (" +
                               ",".join("?" for _ in work_ids) + ")", work_ids).fetchone():
        return None  # Do not reopen a finished assignment for unrelated later activity.
    return dict(session_id=child, work_ids=work_ids, role=role, parent_id=parent)


def record_boundary(db, session_id, payload, stamp):
    use_id = payload.get("tool_use_id") or stamp
    event(db, session_id, "boundary:" + session_id + ":" + str(use_id), "boundary", stamp, payload.get("turn_id"),
          name=payload.get("tool_name"), fingerprint=identity(payload.get("tool_name"), payload.get("tool_input")),
          status="denied", ended_at=stamp)


def deny(reason):
    message = "Devflow boundary: " + reason
    return {"hookSpecificOutput": {"hookEventName": "PreToolUse", "permissionDecision": "deny",
                                   "permissionDecisionReason": message},
            "systemMessage": message}


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
    if prior["input_tokens"] is None and db.execute(
            "SELECT 1 FROM runtime_events WHERE session_id=? AND kind='transcript_gap' LIMIT 1",
            (session["id"],)).fetchone():
        # A skipped line broke counter continuity: this counter is the new baseline and allocates
        # nothing, so usage hidden by the skipped line stays unknown instead of charging the work.
        if after_binding:
            event(db, session["id"], identity(session["id"], position, "baseline"), "counter_baseline",
                  timestamp, turn_id, status="gap",
                  source_ref=session["transcript_path"] + "#byte=" + str(position))
        session.update(values)
        return
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
                # One malformed line must not stall collection: record the gap and move on. The
                # skipped line may have carried a counter, so the checkpoint is uncertain until
                # the next counter re-establishes it without allocating usage.
                event(db, session["id"], identity(session["id"], position, "malformed"), "transcript_gap",
                      state.now(), active_turn, status="skipped",
                      source_ref=session["transcript_path"] + "#byte=" + str(position))
                for key in TOKEN_FIELDS:
                    session[key] = None
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


def handle(db, payload, reason=None):
    started = time.monotonic()
    response = None
    inherited = child_binding(db, payload)
    if inherited:
        bind(db, **inherited)
    # Native hooks share the root session_id across the tree; agent_id identifies the actor.
    session_id = payload.get("agent_id") or payload.get("session_id")
    payload = dict(payload, session_id=session_id)
    saved = db.execute("SELECT * FROM runtime_sessions WHERE id=?", (session_id,)).fetchone()
    if not saved or saved["closed_at"]:
        return None
    session = dict(saved)
    name = payload.get("hook_event_name")
    stamp = state.now()
    turn_id = payload.get("turn_id")
    if name in ("SubagentStart", "SubagentStop"):
        child = payload.get("agent_id")
        if child:
            session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (child,)).fetchone())
            if name == "SubagentStop":
                collect_transcript(db, session, payload.get("agent_transcript_path"), collect_tools=True)
                # Use the child's collected turn; legacy stop hooks may carry the parent's turn ID.
                session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (child,)).fetchone())
                turn(db, session, session["turn_id"], stamp, "completed")
            event(db, child, identity(child, name, session["turn_id"], session["cursor"]), name, stamp,
                  name=payload.get("agent_type"), status="stopped" if name == "SubagentStop" else "started")
            db.execute("UPDATE runtime_sessions SET last_seen_at=? WHERE id=?", (stamp, child))
            if name == "SubagentStop":
                close_if_released(db, child, stamp)
    else:
        if name in ("PreToolUse", "PostToolUse") and not payload.get("tool_use_id"):
            raise ValueError("tool hook lacks tool_use_id")
        if name == "PreToolUse":
            # The boundary is decided and recorded before any collection, so a collector
            # failure cannot let a coordinator edit through.
            if reason is None:
                reason = boundary_decision(db, payload)
            if reason:
                record_boundary(db, session_id, payload, stamp)
                response = deny(reason)
        session["model"] = payload.get("model") or session["model"]
        db.execute("UPDATE runtime_sessions SET model=? WHERE id=?", (session["model"], session_id))
        collect_transcript(db, session, payload.get("transcript_path"))
        session = dict(db.execute("SELECT * FROM runtime_sessions WHERE id=?", (session_id,)).fetchone())
        if name in ("PreToolUse", "PostToolUse"):
            if not response:
                use_id = payload.get("tool_use_id")
                signature = identity(payload.get("tool_name"), payload.get("tool_input"))
                tool_key = "tool:" + session_id + ":" + use_id
                tool_started = stamp if name == "PreToolUse" else None
                previous = state.row(db, "runtime_events", tool_key)
                if not previous or previous["ended_at"] is None:
                    event(db, session_id, tool_key, "tool", tool_started, turn_id,
                          name=payload.get("tool_name"), fingerprint=signature,
                          ended_at=stamp if name == "PostToolUse" else None,
                          status=tool_status(payload.get("tool_response")) if name == "PostToolUse" else "started")
        else:
            event(db, session_id, identity(session_id, name, turn_id, payload.get("source"), session["cursor"]), name, stamp,
                  turn_id, name=payload.get("source") or payload.get("trigger"))
        if name in ("Stop", "Interrupt"):
            turn(db, session, turn_id, stamp, "completed" if name == "Stop" else "interrupted")
            close_if_released(db, session_id, stamp)
        elif name == "SessionEnd":
            db.execute("UPDATE runtime_sessions SET closed_at=? WHERE id=?", (stamp, session_id))
        elif name in ("PreToolUse", "UserPromptSubmit"):
            turn(db, session, turn_id, stamp)
    db.execute("UPDATE runtime_sessions SET last_seen_at=? WHERE id=?", (stamp, session_id))
    # Runtime collector overhead is measured directly, not inferred from coordinator usage.
    event(db, session_id, identity(session_id, name, turn_id, payload.get("tool_use_id"), stamp),
          "collector", stamp, turn_id, duration_seconds=time.monotonic() - started, status="completed")
    return response

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
                inherited = child_binding(check, payload) if present else None
                actor = payload.get("agent_id") or payload.get("session_id")
                actor_payload = dict(payload, session_id=actor)
                bound = present and check.execute("SELECT 1 FROM runtime_sessions WHERE id=? AND closed_at IS NULL", (actor,)).fetchone()
                reason = boundary_decision(check, actor_payload) if bound else None
                if inherited:
                    bound = True
                    if payload.get("hook_event_name") == "PreToolUse" and inherited["role"] in COORDINATOR_ROLES:
                        reason = boundary_violation(payload.get("tool_name"), payload.get("tool_input"))
            if not bound:
                print("{}")
                return 0
            # The boundary is decided before collection and its denial survives any collector failure.
            result = deny(reason) if reason else {}
            try:
                with closing(state.connect(args.db)) as db, db:
                    db.execute("BEGIN IMMEDIATE")
                    if inherited:
                        bind(db, **inherited)
                    handle(db, payload, reason)
            except (ValueError, OSError, sqlite3.Error) as exc:
                failure = "Devflow metrics collection failed: " + str(exc)
                result["systemMessage"] = (result["systemMessage"] + " " + failure) if result.get("systemMessage") else failure
                if reason:
                    try:
                        with closing(state.connect(args.db)) as db, db:
                            db.execute("BEGIN IMMEDIATE")
                            if inherited:
                                bind(db, **inherited)
                            record_boundary(db, actor, actor_payload, state.now())
                    except (ValueError, OSError, sqlite3.Error):
                        pass  # The denial stands even when it cannot be recorded.
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
