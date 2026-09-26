"""SQLite command/outbox, claim, and event projection for local deliveries."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .contracts import canonical_json, digest
from .delivery_config import DeliveryConfig


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True)
    except FileExistsError:
        pass
    else:
        os.chmod(path, 0o700)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != os.getuid()
    ):
        raise ValueError("service state root must be an owned private directory (0700)")


class DeliveryStore:
    def __init__(self, config: DeliveryConfig) -> None:
        self.config = config
        _private_directory(config.state_root)
        helper = config.helpers_dir / "state.py"
        if not helper.is_file():
            raise ValueError("configured Devflow state helper is missing")
        module_spec = importlib.util.spec_from_file_location("devflow_delivery_state", helper)
        if module_spec is None or module_spec.loader is None:
            raise ValueError("cannot load configured Devflow state helper")
        module = importlib.util.module_from_spec(module_spec)
        module_spec.loader.exec_module(module)
        self.state = module
        with self._connect() as db:
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_runs (
                    run_id TEXT PRIMARY KEY,
                    request_digest TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    work_id TEXT NOT NULL,
                    issue_url TEXT NOT NULL,
                    repository_key TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    execution_state TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    protocol_revision INTEGER,
                    candidate_revision INTEGER NOT NULL DEFAULT 0,
                    candidate_json TEXT,
                    pr_json TEXT,
                    checks_json TEXT,
                    tracker_json TEXT,
                    usage_json TEXT,
                    decision_json TEXT,
                    outcome TEXT,
                    error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(delivery_runs)")}
            if "decision_json" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN decision_json TEXT")
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_commands (
                    command_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    request_digest TEXT NOT NULL,
                    response_json TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_outbox (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    updated_at TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    timestamp TEXT NOT NULL,
                    type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    run_revision INTEGER NOT NULL,
                    payload_json TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE INDEX IF NOT EXISTS delivery_events_run
                   ON delivery_events(run_id, sequence)"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_attempts (
                    job_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    role TEXT NOT NULL,
                    iteration INTEGER NOT NULL,
                    candidate_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    pid INTEGER,
                    process_identity TEXT,
                    session_id TEXT,
                    result_json TEXT,
                    result_path TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    cleanup TEXT NOT NULL DEFAULT 'none'
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_effects (
                    effect_key TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    kind TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    observed_json TEXT,
                    updated_at TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_mutations (
                    command_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    kind TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    state TEXT NOT NULL,
                    response_json TEXT
                )"""
            )

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        db = self.state.connect(self.config.tracking_db)
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def _event(
        db: sqlite3.Connection,
        run_id: str,
        revision: int,
        event_type: str,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> int:
        cursor = db.execute(
            """INSERT INTO delivery_events
               (run_id, timestamp, type, message, run_revision, payload_json)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (run_id, _now(), event_type, message, revision, canonical_json(payload or {})),
        )
        return int(cursor.lastrowid)

    def submit(self, supplied: dict[str, Any]) -> dict[str, Any]:
        run_id = supplied.get("run_id")
        command_id = supplied.get("command_id")
        if not isinstance(run_id, str) or not isinstance(command_id, str):
            raise ValueError("run ID and command ID are required")
        request_digest = digest(
            {key: value for key, value in supplied.items() if key != "command_id"}
        )
        dashboard_url = f"{self.config.dashboard_url}/runs/{run_id}"
        # Replay reads the original frozen receipt even if the allowed base or
        # service policy has moved since the first accepted command.
        with self._connect() as db:
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior[0] != request_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior[1])
            prior_run = db.execute(
                "SELECT request_digest,phase FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if prior_run:
                if prior_run[0] != request_digest:
                    raise ValueError("run ID already belongs to different inputs")
                return {
                    "run_id": run_id,
                    "dashboard_url": dashboard_url,
                    "existing": True,
                    "phase": prior_run[1],
                }
        spec = self.config.admit(supplied)
        spec["request_digest"] = request_digest
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            command = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if command:
                if command[0] != request_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(command[1])
            existing = db.execute(
                "SELECT request_digest,phase FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if existing:
                if existing[0] != request_digest:
                    raise ValueError("run ID already belongs to different inputs")
                response = {
                    "run_id": run_id,
                    "dashboard_url": dashboard_url,
                    "existing": True,
                    "phase": existing[1],
                }
                db.execute(
                    "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                    (command_id, run_id, request_digest, canonical_json(response)),
                )
                return response
            work = self.state.row(db, "works", spec["work_id"])
            if work is None:
                self.state.record(
                    db,
                    "work",
                    {
                        "id": spec["work_id"],
                        "title": spec["goal"].splitlines()[0][:200],
                        "repository": "github.com/" + spec["github_repo"],
                        "issue": spec["issue_url"],
                        "status": "starting",
                    },
                )
            elif not work["issue"] or (
                self.state.issue_resource(work["issue"])
                != self.state.issue_resource(spec["issue_url"])
            ):
                raise ValueError("work ID is bound to another issue")
            superseded = spec.get("supersedes_run_id")
            if superseded:
                previous = db.execute(
                    """SELECT work_id,issue_url,repository_key,outcome,pr_json
                       FROM delivery_runs WHERE run_id=?""",
                    (superseded,),
                ).fetchone()
                attempts = db.execute(
                    "SELECT COUNT(*) FROM delivery_attempts WHERE run_id=?", (superseded,)
                ).fetchone()[0]
                if (
                    previous is None
                    or (previous["work_id"], previous["issue_url"], previous["repository_key"])
                    != (spec["work_id"], spec["issue_url"], spec["repository_key"])
                    or previous["outcome"] != "blocked"
                    or previous["pr_json"] is not None
                    or attempts
                ):
                    raise ValueError("only a blocked pre-role run without a PR may be superseded")
                old_owner = f"external:devflow:{superseded}"
                claim = self.state.claim_for(db, spec["work_id"])
                if claim is None or claim["owner"] != old_owner:
                    raise ValueError("superseded run no longer owns this work")
                self.state.release_work(db, spec["work_id"], old_owner)
                db.execute(
                    "UPDATE runtime_sessions SET closed_at=? WHERE id=? AND closed_at IS NULL",
                    (self.state.now(), old_owner),
                )
            self.state.claim_work(db, spec["work_id"], f"external:devflow:{run_id}", dashboard_url)
            timestamp = _now()
            db.execute(
                """INSERT INTO delivery_runs
                   (run_id,request_digest,request_json,work_id,issue_url,repository_key,
                    phase,execution_state,revision,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,'accepted','queued',1,?,?)""",
                (
                    run_id,
                    request_digest,
                    canonical_json(spec),
                    spec["work_id"],
                    spec["issue_url"],
                    spec["repository_key"],
                    timestamp,
                    timestamp,
                ),
            )
            db.execute(
                "INSERT INTO delivery_outbox(run_id,state,updated_at) VALUES (?,'pending',?)",
                (run_id, timestamp),
            )
            self._event(db, run_id, 1, "accepted", "Run accepted and queued for Temporal", {})
            response = {
                "run_id": run_id,
                "dashboard_url": dashboard_url,
                "existing": False,
                "phase": "accepted",
            }
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, request_digest, canonical_json(response)),
            )
            return response

    def spec(self, run_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            return json.loads(row[0])

    def pending_starts(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                """SELECT r.run_id,r.request_digest,r.request_json FROM delivery_runs r
                   JOIN delivery_outbox o ON o.run_id=r.run_id
                   WHERE o.state IN ('pending','unknown') ORDER BY r.created_at"""
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_start(self, run_id: str, *, accepted: bool, error: str | None = None) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT revision,phase FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            if row[1] != "accepted":
                if accepted:
                    # The worker may project a phase before the dispatcher has
                    # recorded start_workflow's acknowledgement.
                    db.execute(
                        """UPDATE delivery_outbox SET state='sent',last_error=NULL,
                           updated_at=? WHERE run_id=?""",
                        (_now(), run_id),
                    )
                return
            new_phase = "preparing" if accepted else "accepted"
            new_state = "running" if accepted else "pending_temporal"
            revision = row[0] + 1
            db.execute(
                """UPDATE delivery_runs SET phase=?,execution_state=?,revision=?,updated_at=?
                   WHERE run_id=?""",
                (new_phase, new_state, revision, _now(), run_id),
            )
            db.execute(
                """UPDATE delivery_outbox SET state=?,attempts=attempts+1,
                   last_error=?,updated_at=? WHERE run_id=?""",
                ("sent" if accepted else "unknown", error, _now(), run_id),
            )
            self._event(
                db,
                run_id,
                revision,
                "temporal_accepted" if accepted else "temporal_pending",
                "Temporal accepted the run" if accepted else "Temporal dispatch remains pending",
                {"error": error} if error else {},
            )

    def project(
        self,
        run_id: str,
        *,
        phase: str,
        execution_state: str,
        event_type: str,
        message: str,
        candidate: dict[str, Any] | None = None,
        pull_request: dict[str, Any] | None = None,
        checks: dict[str, Any] | None = None,
        tracker: dict[str, Any] | None = None,
        usage: dict[str, Any] | None = None,
        decision: dict[str, Any] | None = None,
        protocol_revision: int | None = None,
        outcome: str | None = None,
        error: str | None = None,
        key: str | None = None,
    ) -> dict[str, Any]:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            if row["outcome"] is not None and outcome != row["outcome"]:
                # A late cancel-request projection cannot overwrite the final
                # workflow result after its update was accepted.
                return dict(row)
            if (
                key is not None
                and db.execute(
                    "SELECT 1 FROM delivery_events WHERE run_id=? AND type=? AND payload_json=?",
                    (run_id, event_type, canonical_json({"key": key})),
                ).fetchone()
            ):
                return dict(row)
            revision = row["revision"] + 1
            current_candidate = json.loads(row["candidate_json"]) if row["candidate_json"] else None
            candidate_revision = row["candidate_revision"]
            if candidate and candidate != current_candidate:
                candidate_revision += 1
            values = {
                "phase": phase,
                "execution_state": execution_state,
                "revision": revision,
                "protocol_revision": (
                    protocol_revision if protocol_revision is not None else row["protocol_revision"]
                ),
                "candidate_revision": candidate_revision,
                "candidate_json": canonical_json(candidate)
                if candidate is not None
                else row["candidate_json"],
                "pr_json": canonical_json(pull_request)
                if pull_request is not None
                else row["pr_json"],
                "checks_json": canonical_json(checks) if checks is not None else row["checks_json"],
                "tracker_json": canonical_json(tracker)
                if tracker is not None
                else row["tracker_json"],
                "usage_json": canonical_json(usage) if usage is not None else row["usage_json"],
                "decision_json": canonical_json(decision),
                "outcome": outcome if outcome is not None else row["outcome"],
                "error": error if error is not None else row["error"],
                "updated_at": _now(),
            }
            assignments = ",".join(f"{field}=?" for field in values)
            db.execute(
                f"UPDATE delivery_runs SET {assignments} WHERE run_id=?",
                (*values.values(), run_id),
            )
            self._event(db, run_id, revision, event_type, message, {"key": key} if key else {})
            return dict(
                db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            )

    def events(self, run_id: str, after: int = 0) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                """SELECT sequence,timestamp,type,message,run_revision,payload_json
                   FROM delivery_events WHERE run_id=? AND sequence>?
                   ORDER BY sequence LIMIT 200""",
                (run_id, after),
            ).fetchall()
            return [
                {**dict(row), "payload": json.loads(row["payload_json"]), "evidence_refs": []}
                for row in rows
            ]

    def begin_mutation(
        self, run_id: str, command_id: str, kind: str, payload: dict[str, Any]
    ) -> dict[str, Any] | None:
        request_digest = digest(payload)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            run = db.execute(
                "SELECT outcome FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if run is None:
                raise ValueError("run ID not found")
            prior = db.execute(
                """SELECT run_id,kind,request_digest,state,response_json
                   FROM delivery_mutations WHERE command_id=?""",
                (command_id,),
            ).fetchone()
            if prior:
                if (prior[0], prior[1], prior[2]) != (run_id, kind, request_digest):
                    raise ValueError("command ID already belongs to another mutation")
                if prior[3] == "rejected":
                    raise ValueError(json.loads(prior[4])["error"])
                return json.loads(prior[4]) if prior[3] == "complete" else None
            if run["outcome"] is not None:
                raise ValueError("run is already terminal")
            db.execute(
                """INSERT INTO delivery_mutations
                   (command_id,run_id,kind,request_digest,state)
                   VALUES (?,?,?,?,'pending')""",
                (command_id, run_id, kind, request_digest),
            )
            return None

    def finish_mutation(self, command_id: str, response: dict[str, Any]) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_mutations SET state='complete',response_json=?
                   WHERE command_id=?""",
                (canonical_json(response), command_id),
            )

    def reject_mutation(self, command_id: str, reason: str) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "UPDATE delivery_mutations SET state='rejected',response_json=? WHERE command_id=?",
                (canonical_json({"error": reason}), command_id),
            )

    def mark_mutation_unknown(self, command_id: str) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                """UPDATE delivery_mutations SET state='unknown'
                   WHERE command_id=? AND state='pending'""",
                (command_id,),
            )

    def list_runs(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT * FROM delivery_runs ORDER BY updated_at DESC LIMIT 100"
            ).fetchall()
            return [self._compact(dict(row)) for row in rows]

    @staticmethod
    def _compact(row: dict[str, Any]) -> dict[str, Any]:
        spec = json.loads(row["request_json"])
        return {
            "id": row["run_id"],
            "run_id": row["run_id"],
            "work_id": row["work_id"],
            "title": spec["goal"].splitlines()[0][:120],
            "issue": row["issue_url"],
            "repository": row["repository_key"],
            "phase": row["phase"],
            "execution_state": row["execution_state"],
            "updated_at": row["updated_at"],
            "revision": row["revision"],
            "protocol_revision": row["protocol_revision"],
            "authorized_endpoint": spec["authorized_endpoint"],
            "outcome": row["outcome"],
        }

    def detail(self, run_id: str) -> dict[str, Any]:
        with self._connect() as db:
            saved = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            if saved is None:
                raise ValueError("run ID not found")
            row = dict(saved)
            attempts = [
                dict(item)
                for item in db.execute(
                    """SELECT job_key,role,iteration,state,session_id,result_json,cleanup
                       FROM delivery_attempts WHERE run_id=? ORDER BY iteration,role""",
                    (run_id,),
                )
            ]
        compact = self._compact(row)
        candidate = json.loads(row["candidate_json"]) if row["candidate_json"] else None
        roles = []
        for attempt in attempts:
            result = json.loads(attempt["result_json"]) if attempt["result_json"] else {}
            roles.append(
                {
                    "role": attempt["role"],
                    "iteration": attempt["iteration"],
                    "state": attempt["state"],
                    "session_id": attempt["session_id"],
                    "usage": result.get("usage"),
                    "summary": result.get("summary"),
                    "findings": result.get("findings", []),
                    "cleanup": attempt["cleanup"],
                }
            )
        with self._connect() as db:
            active = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
        event_types = {event["type"] for event in self.events(run_id)}
        gates = [
            {
                "id": name,
                "label": label,
                "state": "completed" if completion in event_types else "pending",
            }
            for name, label, completion in (
                ("prepare", "Prepare", "role_started"),
                ("prepublish", "Before PR checks", "candidate_ready"),
                ("publish", "Publish", "published"),
                ("local_checks", "Local checks", "ci_wait"),
                ("required_ci", "Required CI", "tracker_started"),
                ("tracker", "Tracker", "delivered"),
            )
        ]
        return {
            **compact,
            "run": compact,
            "phase_gates": gates,
            "roles": roles,
            "capacity": {"limit": self.config.raw.get("capacity", 2), "active": active},
            "queued": row["execution_state"] == "queued",
            "cleanup": "unknown" if any(a["cleanup"] == "unknown" for a in attempts) else "none",
            "candidate": {**candidate, "revision": row["candidate_revision"]}
            if candidate
            else None,
            "pull_request": json.loads(row["pr_json"]) if row["pr_json"] else None,
            "checks": json.loads(row["checks_json"]) if row["checks_json"] else {},
            "tracker": json.loads(row["tracker_json"]) if row["tracker_json"] else {},
            "usage": json.loads(row["usage_json"]) if row["usage_json"] else {},
            "decisions": [json.loads(row["decision_json"])]
            if row["decision_json"] and json.loads(row["decision_json"]) is not None
            else [],
            "events": self.events(run_id),
            "error": row["error"],
        }

    def evidence_index(self, run_id: str) -> list[dict[str, Any]]:
        spec = self.spec(run_id)
        root = Path(spec["state_dir"])
        indexed: list[dict[str, Any]] = []
        with self._connect() as db:
            attempts = db.execute(
                "SELECT job_key,role,iteration FROM delivery_attempts WHERE run_id=?",
                (run_id,),
            ).fetchall()
        for attempt in attempts:
            path = root / "attempts" / attempt["job_key"] / "process.log"
            if path.is_file():
                indexed.append(
                    {
                        "id": f"role-{attempt['role']}-{attempt['iteration']}",
                        "label": f"{attempt['role']} process log, attempt {attempt['iteration']}",
                        "path": path,
                    }
                )
        details = self.detail(run_id)
        for result in details.get("checks", {}).get("local", {}).get("results", []):
            path = Path(result["log"])
            indexed.append({"id": f"check-{result['id']}", "label": result["id"], "path": path})
        recovery = root / "recovery" / "provenance.json"
        if recovery.is_file():
            indexed.append(
                {"id": "recovery-provenance", "label": "Recovery provenance", "path": recovery}
            )
        safe = []
        for item in indexed:
            path = item["path"]
            if (
                path.is_symlink()
                or not path.is_file()
                or root.resolve() not in path.resolve().parents
            ):
                continue
            safe.append(
                {
                    "id": item["id"],
                    "label": item["label"],
                    "bytes": path.stat().st_size,
                    "url": f"/api/runs/{run_id}/evidence/{item['id']}",
                }
            )
        return safe

    def evidence(self, run_id: str, evidence_id: str) -> dict[str, Any]:
        spec = self.spec(run_id)
        root = Path(spec["state_dir"])
        known = {item["id"] for item in self.evidence_index(run_id)}
        if evidence_id not in known:
            raise ValueError("evidence ID is not indexed for this run")
        if evidence_id == "recovery-provenance":
            path = root / "recovery" / "provenance.json"
        elif evidence_id.startswith("role-"):
            with self._connect() as db:
                attempts = db.execute(
                    "SELECT job_key,role,iteration FROM delivery_attempts WHERE run_id=?",
                    (run_id,),
                ).fetchall()
            matched = [
                row for row in attempts if evidence_id == f"role-{row['role']}-{row['iteration']}"
            ]
            if len(matched) != 1:
                raise ValueError("evidence identity is ambiguous")
            path = root / "attempts" / matched[0]["job_key"] / "process.log"
        else:
            results = self.detail(run_id).get("checks", {}).get("local", {}).get("results", [])
            matched = [item for item in results if evidence_id == f"check-{item['id']}"]
            if len(matched) != 1:
                raise ValueError("evidence identity is ambiguous")
            path = Path(matched[0]["log"])
        if path.is_symlink() or root.resolve() not in path.resolve().parents:
            raise ValueError("evidence path escaped its run state")
        data = path.read_bytes()
        if len(data) > 1024 * 1024:
            raise ValueError("evidence exceeds the local read limit")
        return {
            "id": evidence_id,
            "sha256": hashlib.sha256(data).hexdigest(),
            "bytes": len(data),
            "text": data.decode("utf-8", errors="replace"),
        }
