"""Durable presentation preferences, bounded steering and observed run cohorts."""

from __future__ import annotations

import hashlib
import json
import math
import statistics
import subprocess
from datetime import datetime
from functools import lru_cache
from pathlib import Path

from .contracts import canonical_json, digest


@lru_cache(maxsize=1)
def runtime_identity() -> dict:
    root = Path(__file__).resolve().parents[3]

    def git(*args):
        return (
            subprocess.check_output(
                ["git", "-C", str(root), *args],
                stderr=subprocess.DEVNULL,
                timeout=5,
            )
            .decode()
            .strip()
        )

    try:
        revision = git("rev-parse", "HEAD")
        patch = git("diff", "--binary", "HEAD")
        untracked = git("ls-files", "--others", "--exclude-standard").splitlines()
        changes = hashlib.sha256(patch.encode())
        for name in sorted(untracked):
            file = root / name
            if file.is_file() and not file.is_symlink():
                changes.update(name.encode())
                changes.update(hashlib.sha256(file.read_bytes()).digest())
        dirty = bool(patch or untracked)
        try:
            tag = git("describe", "--tags", "--exact-match", "HEAD")
        except subprocess.SubprocessError:
            tag = None
        return {
            "revision": revision,
            "release": tag if not dirty else None,
            "dirty": dirty,
            "local_digest": changes.hexdigest() if dirty else None,
        }
    except (OSError, subprocess.SubprocessError):
        return {"revision": None, "release": None, "dirty": None, "local_digest": None}


def initialize(db):
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_dashboard_state (
        run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
        archived INTEGER NOT NULL DEFAULT 0,
        identity_json TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_dashboard_commands (
        command_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
        request_digest TEXT NOT NULL, response_json TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_steering (
        id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
        message TEXT NOT NULL, created_at TEXT NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS delivery_steering_launches (
        job_key TEXT PRIMARY KEY REFERENCES delivery_attempts(job_key),
        run_id TEXT NOT NULL, role TEXT NOT NULL, notes_json TEXT NOT NULL)""")


def presentation(db, run_id):
    row = db.execute(
        "SELECT archived,identity_json FROM delivery_dashboard_state WHERE run_id=?", (run_id,)
    ).fetchone()
    return {
        "archived": bool(row and row[0]),
        "runtime_identity": json.loads(row[1]) if row and row[1] else None,
    }


def steering_open(db, row):
    return (
        row["outcome"] is None
        and row["execution_state"]
        not in {
            "terminal",
            "blocked",
            "cancelled",
            "cancelling",
            "waiting_tracker",
        }
        and row["phase"] not in {"verify", "ci_wait", "tracker", "delivered"}
        and not db.execute(
            "SELECT 1 FROM delivery_attempts WHERE run_id=? AND role='verify' "
            "AND iteration=? LIMIT 1",
            (row["run_id"], row["iteration"]),
        ).fetchone()
    )


def mutate(store, run_id, kind, payload):
    from .delivery_store import _now

    fields = {"command_id", "expected_revision", "archived" if kind == "archive" else "message"}
    if not isinstance(payload, dict) or set(payload) != fields:
        raise ValueError("dashboard command fields do not match the contract")
    if not isinstance(payload["command_id"], str) or not payload["command_id"].strip():
        raise ValueError("command ID is required")
    if type(payload["expected_revision"]) is not int:
        raise ValueError("observed projection revision is required")
    if kind == "archive" and type(payload["archived"]) is not bool:
        raise ValueError("archived must be a boolean")
    message = payload.get("message")
    if kind == "steer" and (not isinstance(message, str) or not 1 <= len(message.strip()) <= 4000):
        raise ValueError("steering instructions must contain 1–4000 characters")
    bound = digest({"run_id": run_id, "kind": kind, **payload})
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        prior = db.execute(
            "SELECT request_digest,response_json FROM delivery_dashboard_commands "
            "WHERE command_id=?",
            (payload["command_id"],),
        ).fetchone()
        if prior:
            if prior[0] != bound:
                raise ValueError("command ID already belongs to different inputs")
            return json.loads(prior[1])
        row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("run ID not found")
        if row["revision"] != payload["expected_revision"]:
            raise ValueError("run state changed; refresh before sending this command")
        if kind == "archive":
            active = db.execute(
                "SELECT 1 FROM delivery_attempts WHERE run_id=? "
                "AND (state!='finished' OR cleanup='unknown') LIMIT 1",
                (run_id,),
            ).fetchone()
            if (
                not (
                    row["outcome"] or row["execution_state"] in {"terminal", "blocked", "cancelled"}
                )
                or active
            ):
                raise ValueError("only stopped tasks can be archived or restored")
            db.execute(
                "INSERT INTO delivery_dashboard_state(run_id,archived) VALUES (?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET archived=excluded.archived",
                (run_id, payload["archived"]),
            )
            response = {"run_id": run_id, "archived": payload["archived"]}
        else:
            from .delivery_preparation import execution_retired

            if execution_retired(json.loads(row["request_json"])) or not steering_open(db, row):
                raise ValueError("steering is closed: final QA or terminal work has no next role")
            size = db.execute(
                "SELECT COALESCE(SUM(length(message)),0) FROM delivery_steering WHERE run_id=?",
                (run_id,),
            ).fetchone()[0]
            if size + len(message.strip()) > 16000:
                raise ValueError("this run has reached its steering context limit")
            cursor = db.execute(
                "INSERT INTO delivery_steering(run_id,message,created_at) VALUES (?,?,?)",
                (run_id, message.strip(), _now()),
            )
            response = {"run_id": run_id, "steering_id": cursor.lastrowid, "state": "queued"}
        store._event(
            db,
            run_id,
            row["revision"],
            kind,
            "Task archived"
            if kind == "archive" and payload["archived"]
            else "Task restored"
            if kind == "archive"
            else "Steering queued for the next role launch",
            response,
        )
        db.execute(
            "INSERT INTO delivery_dashboard_commands VALUES (?,?,?,?)",
            (payload["command_id"], run_id, bound, canonical_json(response)),
        )
        return response


def steering_history(db, run_id):
    notes = [
        dict(row)
        for row in db.execute(
            "SELECT id,message,created_at FROM delivery_steering WHERE run_id=? ORDER BY id",
            (run_id,),
        )
    ]
    launches = db.execute(
        "SELECT role,job_key,notes_json FROM delivery_steering_launches WHERE run_id=?", (run_id,)
    ).fetchall()
    for note in notes:
        note["included_in"] = [
            {"role": row[0], "job_key": row[1]}
            for row in launches
            if note["id"] in {n["id"] for n in json.loads(row[2])}
        ]
    return notes


def launch_steering(store, request, job_key):
    """Freeze each launch snapshot once; replay never silently changes actor input."""
    with store._connect() as db:
        db.execute("BEGIN IMMEDIATE")
        row = db.execute(
            "SELECT notes_json FROM delivery_steering_launches WHERE job_key=?", (job_key,)
        ).fetchone()
        if row:
            notes = json.loads(row[0])
        else:
            prior_path = Path(request["spec"]["state_dir"]) / "attempts" / job_key / "request.json"
            if prior_path.exists() or prior_path.is_symlink():
                from .delivery_resources import read_private

                prior = read_private(prior_path)
                if (prior["spec"]["run_id"], prior["role"]) != (
                    request["spec"]["run_id"],
                    request["role"],
                ):
                    raise ValueError("retained role input belongs to another launch")
                # Older launches predate the dashboard table. Their saved input wins.
                notes = prior.get("steering", [])
            else:
                notes = [
                    {"id": row[0], "message": row[1]}
                    for row in db.execute(
                        "SELECT id,message FROM delivery_steering WHERE run_id=? ORDER BY id",
                        (request["spec"]["run_id"],),
                    )
                ]
            db.execute(
                "INSERT INTO delivery_steering_launches VALUES (?,?,?,?)",
                (job_key, request["spec"]["run_id"], request["role"], canonical_json(notes)),
            )
            if notes:
                run_id = request["spec"]["run_id"]
                revision = db.execute(
                    "SELECT revision FROM delivery_runs WHERE run_id=?", (run_id,)
                ).fetchone()[0]
                store._event(
                    db,
                    run_id,
                    revision,
                    "steering_bound",
                    f"Steering bound to {request['role']} launch input",
                    {"job_key": job_key, "note_ids": [note["id"] for note in notes]},
                )
        return {**request, "steering": notes} if notes else request


def _number(value):
    return type(value) in {int, float} and math.isfinite(value) and value >= 0


def statistics_for(store):
    """All durable runs, including archives and failures; no UI-list truncation."""
    with store._connect() as db:
        runs = [dict(row) for row in db.execute("SELECT * FROM delivery_runs")]
        identities = {
            row[0]: json.loads(row[1])
            for row in db.execute(
                "SELECT run_id,identity_json FROM delivery_dashboard_state "
                "WHERE identity_json IS NOT NULL",
            )
        }
        attempts = {}
        for row in db.execute("SELECT run_id,role,iteration,result_json FROM delivery_attempts"):
            attempts.setdefault(row[0], []).append(dict(row))
    groups = {}
    for run in runs:
        identity = identities.get(run["run_id"]) or {}
        key = (
            identity.get("release"),
            identity.get("revision"),
            identity.get("local_digest"),
            json.loads(run["request_json"]).get("provider", "unknown"),
        )
        groups.setdefault(key, []).append(run)
    cohorts = []
    for key, group in groups.items():
        terminal = [
            r
            for r in group
            if r["outcome"] in {"delivered", "blocked", "cancelled"}
            or r["execution_state"] in {"terminal", "blocked", "cancelled"}
        ]
        delivered = [r for r in terminal if r["outcome"] == "delivered"]
        first_pass = [
            r
            for r in delivered
            if r["iteration"] == 0
            and not r["recovery_json"]
            and not json.loads(r["request_json"]).get("supersedes_run_id")
            and all(a["iteration"] == 0 for a in attempts.get(r["run_id"], []))
        ]
        durations = []
        for run in terminal:
            try:
                seconds = (
                    datetime.fromisoformat(run["updated_at"])
                    - datetime.fromisoformat(run["created_at"])
                ).total_seconds()
                if seconds >= 0:
                    durations.append(seconds)
            except (TypeError, ValueError):
                pass
        tokens, costs, by_role = [], [], {}
        all_attempts = [a for r in group for a in attempts.get(r["run_id"], [])]
        for attempt in all_attempts:
            usage = json.loads(attempt["result_json"] or "{}").get("usage") or {}
            role = by_role.setdefault(
                attempt["role"], {"attempts": 0, "observed_tokens": 0, "token_observations": 0}
            )
            role["attempts"] += 1
            if _number(usage.get("total_tokens")):
                tokens.append(usage["total_tokens"])
                role["observed_tokens"] += usage["total_tokens"]
                role["token_observations"] += 1
            if _number(usage.get("cost_usd")):
                costs.append(usage["cost_usd"])
        cohorts.append(
            {
                "release": key[0],
                "revision": key[1],
                "local_digest": key[2],
                "provider": key[3],
                "runs": len(group),
                "terminal": len(terminal),
                "active": len(group) - len(terminal),
                "delivered": len(delivered),
                "blocked": sum(r["outcome"] == "blocked" for r in terminal),
                "cancelled": sum(r["outcome"] == "cancelled" for r in terminal),
                "unknown_outcomes": sum(
                    r["outcome"]
                    not in {
                        "delivered",
                        "blocked",
                        "cancelled",
                    }
                    for r in terminal
                ),
                "first_pass_delivered": len(first_pass),
                "success_rate": len(delivered) / len(terminal) if terminal else None,
                "median_duration_seconds": statistics.median(durations) if durations else None,
                "duration_observations": len(durations),
                "attempts": len(all_attempts),
                "token_observations": len(tokens),
                "observed_tokens": sum(tokens) if tokens else None,
                "cost_observations": len(costs),
                "observed_cost_usd": sum(costs) if costs else None,
                "repairs": sum(
                    a["role"] == "implement" and a["iteration"] > 0 for a in all_attempts
                ),
                "roles": by_role,
            }
        )
    return {
        "total_runs": len(runs),
        "cohorts": cohorts,
        "definitions": "Success rate = delivered / all terminal runs "
        "(including blocked, cancelled and unknown outcomes). "
        "First pass = delivered at iteration zero without recovery or superseding another run. "
        "Duration = admission to last workflow observation, including waits. "
        "Tokens and costs cover only observed role attempts; missing values are unknown. "
        "Archived tasks remain included. Historical unrecorded identities stay unknown. "
        "Simulated providers are separate cohorts, not evidence of native delivery. "
        "Cohort differences describe the sample and do not establish causation.",
    }
