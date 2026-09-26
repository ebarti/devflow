"""SQLite command/outbox, claim, and event projection for local deliveries."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import re
import sqlite3
import stat
import subprocess
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from temporalio.client import Client, WorkflowExecutionStatus

from .candidate import candidate_for
from .contracts import canonical_json, digest
from .delivery_config import DeliveryConfig
from .delivery_continuation import (
    continuation_authority,
    selected_manifest,
    session_state_digest,
)


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
                    iteration INTEGER NOT NULL DEFAULT 0,
                    protocol_revision INTEGER,
                    candidate_revision INTEGER NOT NULL DEFAULT 0,
                    candidate_json TEXT,
                    pr_json TEXT,
                    checks_json TEXT,
                    tracker_json TEXT,
                    usage_json TEXT,
                    decision_json TEXT,
                    outcome TEXT,
                    cleanup TEXT NOT NULL DEFAULT 'none',
                    error TEXT,
                    workflow_id TEXT,
                    recovery_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )"""
            )
            columns = {row[1] for row in db.execute("PRAGMA table_info(delivery_runs)")}
            if "iteration" not in columns:
                db.execute(
                    "ALTER TABLE delivery_runs ADD COLUMN iteration INTEGER NOT NULL DEFAULT 0"
                )
            if "decision_json" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN decision_json TEXT")
            if "cleanup" not in columns:
                db.execute(
                    "ALTER TABLE delivery_runs ADD COLUMN cleanup TEXT NOT NULL DEFAULT 'none'"
                )
            if "workflow_id" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN workflow_id TEXT")
            if "recovery_json" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN recovery_json TEXT")
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
        temporal_result = None
        superseded = spec.get("supersedes_run_id")
        if superseded:
            # A blocked projection can precede Temporal's terminal close. Read
            # the completed execution before BEGIN IMMEDIATE; recheck DB state,
            # frozen bytes and claim under that transaction below.
            with self._connect() as db:
                has_attempt = db.execute(
                    "SELECT 1 FROM delivery_attempts WHERE run_id=? LIMIT 1", (superseded,)
                ).fetchone()
            if has_attempt:
                temporal_result = self._completed_temporal_result(superseded)
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
            if superseded:
                previous = db.execute(
                    """SELECT work_id,issue_url,repository_key,phase,outcome,execution_state,
                              cleanup,error,pr_json,checks_json,request_digest,request_json
                       FROM delivery_runs WHERE run_id=?""",
                    (superseded,),
                ).fetchone()
                attempts = db.execute(
                    "SELECT * FROM delivery_attempts WHERE run_id=?", (superseded,)
                ).fetchall()
                if (
                    previous is None
                    or (previous["work_id"], previous["issue_url"], previous["repository_key"])
                    != (spec["work_id"], spec["issue_url"], spec["repository_key"])
                    or previous["outcome"] != "blocked"
                    or previous["execution_state"] != "blocked"
                    or previous["pr_json"] is not None
                ):
                    raise ValueError("only a blocked unpublished run may be superseded")
                prior_spec = json.loads(previous["request_json"])
                if prior_spec["branch"] == spec["branch"]:
                    raise ValueError("superseded run retains the owned branch; choose a new branch")
                if attempts:
                    spec["continuation"] = self._post_role_continuation(
                        db, spec, prior_spec, previous, attempts, temporal_result
                    )
                elif (spec["policy"].get("recovery") or {}).get("continuation"):
                    raise ValueError("pre-role supersede cannot import a role session")
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

    def _completed_temporal_result(self, run_id: str) -> dict[str, Any]:
        """Read a closed workflow from Temporal, never from caller-authored JSON."""

        async def read() -> dict[str, Any]:
            client = await Client.connect(
                self.config.temporal_address,
                namespace=self.config.raw.get("temporal_namespace", "default"),
            )
            handle = client.get_workflow_handle("delivery-" + run_id)
            description = await handle.describe()
            if (
                description.status != WorkflowExecutionStatus.COMPLETED
                or not description.close_time
            ):
                raise ValueError("continuation predecessor Temporal workflow is not closed")
            return {
                "workflow_id": description.id,
                "execution_run_id": description.run_id,
                "closed_at": description.close_time.isoformat(),
                "request_digest": await description.memo_value("request_digest", "unknown"),
                "result": await client.get_workflow_handle(
                    description.id, run_id=description.run_id
                ).result(),
            }

        try:
            # submit() is shared by synchronous CLI and the async API route.
            with ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(
                    lambda: asyncio.run(asyncio.wait_for(read(), timeout=30))
                ).result(timeout=35)
        except Exception as exc:
            raise ValueError("continuation predecessor Temporal closure is unproven") from exc

    def recover_publication(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Queue one same-run continuation after a pushed PR outlived readback.

        The completed Temporal result is the authority for the checkpoint.
        Git and GitHub are checked twice: before this transaction and by the
        read-only recovery activity before any downstream gate can start.
        """
        from .delivery_broker import DeliveryBroker

        required = {
            "command_id",
            "expected_revision",
            "expected_candidate_id",
            "expected_head",
            "expected_pr_number",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("publication recovery fields do not match the contract")
        command_id = supplied["command_id"]
        head = supplied["expected_head"]
        pr_number = supplied["expected_pr_number"]
        if (
            not isinstance(command_id, str)
            or not command_id
            or type(supplied["expected_revision"]) is not int
            or not isinstance(supplied["expected_candidate_id"], str)
            or not isinstance(head, str)
            or not re.fullmatch(r"[0-9a-f]{40}", head)
            or type(pr_number) is not int
            or pr_number < 1
        ):
            raise ValueError("invalid publication recovery identity")
        request_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != request_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            raise ValueError("run ID not found")
        spec = json.loads(row["request_json"])
        closed = self._completed_temporal_result(run_id)
        state = closed["result"]
        candidate = state.get("candidate") if isinstance(state, dict) else None
        precheck = state.get("checks", {}).get("prepublish", {}) if isinstance(state, dict) else {}
        roles = state.get("roles", []) if isinstance(state, dict) else []
        iteration = state.get("iteration") if isinstance(state, dict) else None
        previous_pr = state.get("pull_request") if isinstance(state, dict) else None
        if (
            closed["workflow_id"] != f"delivery-{run_id}"
            or closed["request_digest"] != row["request_digest"]
            or not isinstance(state, dict)
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("error") != "publication unresolved: ActivityError"
            or state.get("cleanup") != "none"
            or state.get("revision") != supplied["expected_revision"]
            or type(iteration) is not int
            or iteration < 0
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or candidate.get("head") == head
            or not isinstance(precheck, dict)
            or precheck.get("state") != "passed"
            or precheck.get("candidate_id") != candidate["id"]
            or precheck.get("source_unchanged") is not True
            or not isinstance(precheck.get("results"), list)
            or not precheck["results"]
            or any(
                not isinstance(item, dict)
                or item.get("passed") is not True
                or item.get("cleanup") != "confirmed"
                for item in precheck["results"]
            )
            or not isinstance(roles, list)
            or not roles
            or roles[-1].get("role") != "implement"
            or roles[-1].get("iteration") != iteration
            or roles[-1].get("status") != "pass"
            or roles[-1].get("cleanup") != "confirmed"
            or roles[-1].get("candidate", {}).get("id") != candidate["id"]
            or not roles[-1].get("session_id")
            or (
                previous_pr is not None
                and (
                    not isinstance(previous_pr, dict)
                    or previous_pr.get("number") != pr_number
                    or previous_pr.get("head") != candidate["head"]
                )
            )
        ):
            raise ValueError("closed Temporal result is not a recoverable publication")
        broker = DeliveryBroker(self, spec)
        observed = broker.reconcile_publish(
            iteration,
            candidate,
            expected_head=head,
            expected_pr_number=pr_number,
            complete=False,
        )
        if observed.get("state") == "pending":
            raise ValueError("published head has not read back at the expected PR")
        workflow_id = f"delivery-{run_id}-publish-recovery-1"
        recovery = {
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "state": state,
            "expected_head": head,
            "expected_pr_number": pr_number,
            "observed_publication": observed,
        }
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "publication_recovery_queued",
            "workflow_id": workflow_id,
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != request_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            effects = db.execute(
                "SELECT effect_key,kind,request_json,state FROM delivery_effects WHERE run_id=?",
                (run_id,),
            ).fetchall()
            attempts = db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            key = f"publish:{run_id}:{iteration}"
            pending = [item for item in effects if item["state"] == "pending"]
            if (
                current is None
                or current["request_json"] != row["request_json"]
                or current["outcome"] != "blocked"
                or current["phase"] != "blocked"
                or current["error"] != state["error"]
                or current["cleanup"] != "none"
                or current["protocol_revision"] != state["revision"]
                or current["recovery_json"] is not None
                or current["workflow_id"] is not None
                or json.loads(current["candidate_json"] or "null") != candidate
                or json.loads(current["checks_json"] or "{}").get("prepublish") != precheck
                or json.loads(current["pr_json"] or "null") != previous_pr
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or len(pending) != 1
                or pending[0]["effect_key"] != key
                or pending[0]["kind"] != "publish"
                or pending[0]["request_json"]
                != canonical_json(
                    {
                        "iteration": iteration,
                        "input_candidate_id": candidate["id"],
                    }
                )
                or any(item["state"] not in {"complete", "pending"} for item in effects)
                or len(attempts) != len(roles)
                or any(
                    item["state"] in {"starting", "running", "unknown"}
                    or item["cleanup"] != "confirmed"
                    for item in attempts
                )
            ):
                raise ValueError("publication recovery lost its frozen run or effect")
            revision = current["revision"] + 1
            db.execute(
                """UPDATE delivery_runs SET phase='publication_recovery_queued',
                   execution_state='queued',outcome=NULL,error=NULL,revision=?,
                   workflow_id=?,recovery_json=?,updated_at=? WHERE run_id=?""",
                (revision, workflow_id, canonical_json(recovery), _now(), run_id),
            )
            db.execute(
                """UPDATE delivery_outbox SET state='pending',last_error=NULL,updated_at=?
                   WHERE run_id=?""",
                (_now(), run_id),
            )
            self._event(
                db,
                run_id,
                revision,
                "publication_recovery_queued",
                "Existing PR head verified; resuming remaining gates",
                {
                    "iteration": iteration,
                    "candidate_id": candidate["id"],
                    "expected_head": head,
                    "pr_number": pr_number,
                    "predecessor_execution_run_id": closed["execution_run_id"],
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, request_digest, canonical_json(response)),
            )
        return response

    def _post_role_continuation(
        self,
        db: sqlite3.Connection,
        spec: dict[str, Any],
        prior_spec: dict[str, Any],
        previous: sqlite3.Row,
        attempts: list[sqlite3.Row],
        temporal_result: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Freeze one finished post-role result before transferring its claim."""

        recovery = spec["policy"].get("recovery") or {}
        control = recovery.get("continuation")
        required = {
            "from_run_id",
            "attempt_job_key",
            "session_id",
            "candidate_id",
            "history_result_path",
            "history_result_sha256",
            "source_manifest_sha256",
            "session_state_sha256",
        }
        if not isinstance(control, dict) or set(control) != required:
            raise ValueError("post-role continuation requires exact private evidence")
        old_id = prior_spec["run_id"]
        if (
            control["from_run_id"] != old_id
            or spec.get("supersedes_run_id") != old_id
            or spec["goal"] != prior_spec["goal"]
            or spec["accepted_plan"] != prior_spec["accepted_plan"]
            or spec["authorized_endpoint"] != prior_spec["authorized_endpoint"]
            or spec["base_sha"] != prior_spec["base_sha"]
            or spec["source_path"] != prior_spec["source_path"]
            or Path(recovery["source_path"]).resolve(strict=True)
            != Path(prior_spec["checkout"]).resolve(strict=True)
            or continuation_authority(spec["policy"])
            != continuation_authority(prior_spec["policy"])
            or previous["phase"] != "blocked"
            or previous["cleanup"] != "none"
            or previous["error"] != "implementer did not establish a pass"
            or previous["checks_json"] not in (None, "{}")
            or len(attempts) != 1
        ):
            raise ValueError("continuation changed authority or predecessor identity")
        old_state = Path(prior_spec["state_dir"])
        if any(
            (old_state / name).exists() or (old_state / name).is_symlink()
            for name in (
                "prechecks",
                "checks",
                "browser-qa",
                "gate-evidence",
            )
        ):
            raise ValueError("continuation predecessor crossed a broker check gate")
        attempt = attempts[0]
        if (
            attempt["role"] != "implement"
            or attempt["iteration"] != 0
            or attempt["job_key"] != control["attempt_job_key"]
            or attempt["state"] != "finished"
            or attempt["cleanup"] != "confirmed"
            or not attempt["finished_at"]
            or attempt["session_id"] != control["session_id"]
            or not attempt["result_json"]
        ):
            raise ValueError("continuation predecessor role is not finished and contained")
        effects = db.execute(
            "SELECT kind,state FROM delivery_effects WHERE run_id=?", (old_id,)
        ).fetchall()
        if not effects or any(
            row["kind"] not in {"prepare", "dependency-preparation"} or row["state"] != "complete"
            for row in effects
        ):
            raise ValueError("continuation predecessor has an unresolved external effect")
        history_path = Path(control["history_result_path"])
        info = history_path.lstat()
        history_bytes = history_path.read_bytes()
        if (
            not history_path.is_absolute()
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or hashlib.sha256(history_bytes).hexdigest() != control["history_result_sha256"]
        ):
            raise ValueError("continuation Temporal result evidence changed")
        history = json.loads(history_bytes)
        role = history.get("role_result")
        raw = json.loads(attempt["result_json"])
        live = temporal_result or {}
        live_result = live.get("result")
        if (
            history.get("workflow_id") != f"delivery-{old_id}"
            or history.get("run_id") != old_id
            or history.get("outcome") != "blocked"
            or not isinstance(role, dict)
            or role.get("role") != "implement"
            or role.get("iteration") != 0
            or role.get("status") != "blocked"
            or role.get("cleanup") != "confirmed"
            or role.get("session_id") != control["session_id"]
            or any(role.get(key) != value for key, value in raw.items())
            or live.get("workflow_id") != f"delivery-{old_id}"
            or not isinstance(live.get("execution_run_id"), str)
            or not live["execution_run_id"]
            or live.get("request_digest") != previous["request_digest"]
            or not isinstance(live_result, dict)
            or live_result.get("run_id") != old_id
            or live_result.get("phase") != "blocked"
            or live_result.get("outcome") != "blocked"
            or live_result.get("cleanup") != previous["cleanup"]
            or live_result.get("error") != previous["error"]
            or live_result.get("roles") != [role]
            or not isinstance(live.get("closed_at"), str)
            or datetime.fromisoformat(live["closed_at"])
            <= datetime.fromisoformat(attempt["finished_at"])
        ):
            raise ValueError("continuation history differs from closed Temporal result")
        candidate = role.get("candidate")
        source = Path(prior_spec["checkout"])
        if (
            not isinstance(candidate, dict)
            or candidate.get("id") != control["candidate_id"]
            or candidate.get("base_sha") != spec["base_sha"]
            or any(candidate.get(key) != value for key, value in candidate_for(source).items())
        ):
            raise ValueError("continuation source differs from the completed activity candidate")
        manifest = selected_manifest(source, recovery["paths"])
        if digest(manifest) != control["source_manifest_sha256"]:
            raise ValueError("continuation recovered file manifest changed")
        finished_ns = int(datetime.fromisoformat(attempt["finished_at"]).timestamp() * 1e9)
        if any(
            item.get("type") == "file" and int(item["mtime_ns"]) > finished_ns
            for item in manifest.values()
        ):
            raise ValueError("continuation source was edited after the role finished")
        home = Path(prior_spec["state_dir"]) / "role-homes" / "implement"
        if session_state_digest(home, control["session_id"]) != control["session_state_sha256"]:
            raise ValueError("continuation provider session state changed")
        self._ensure_no_remote_pr(spec["github_repo"], prior_spec["branch"])
        return {
            "from_run_id": old_id,
            "attempt_job_key": attempt["job_key"],
            "session_id": control["session_id"],
            "candidate_id": candidate["id"],
            "source_manifest_sha256": control["source_manifest_sha256"],
            "session_state_sha256": control["session_state_sha256"],
            "history_result_sha256": control["history_result_sha256"],
            "workflow_execution_run_id": live["execution_run_id"],
            "workflow_closed_at": live["closed_at"],
            "findings": role.get("findings", []),
        }

    @staticmethod
    def _ensure_no_remote_pr(repository: str, branch: str) -> None:
        remote = subprocess.run(
            [
                "gh",
                "pr",
                "list",
                "--repo",
                repository,
                "--head",
                branch,
                "--state",
                "all",
                "--json",
                "number",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        if remote.returncode or json.loads(remote.stdout) != []:
            raise ValueError("continuation predecessor has a remote pull request")

    def spec(self, run_id: str) -> dict[str, Any]:
        with self._connect() as db:
            row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            return json.loads(row[0])

    def active_workflow_id(self, run_id: str) -> str:
        with self._connect() as db:
            row = db.execute(
                "SELECT workflow_id FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            return row[0] or f"delivery-{run_id}"

    def pending_starts(self) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                """SELECT r.run_id,r.request_digest,r.request_json,r.workflow_id,
                          r.recovery_json FROM delivery_runs r
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
            if row[1] not in {"accepted", "publication_recovery_queued"}:
                if accepted:
                    # The worker may project a phase before the dispatcher has
                    # recorded start_workflow's acknowledgement.
                    db.execute(
                        """UPDATE delivery_outbox SET state='sent',last_error=NULL,
                           updated_at=? WHERE run_id=?""",
                        (_now(), run_id),
                    )
                return
            new_phase = (
                "publishing"
                if row[1] == "publication_recovery_queued" and accepted
                else "preparing"
                if accepted
                else row[1]
            )
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
        iteration: int | None = None,
        protocol_revision: int | None = None,
        outcome: str | None = None,
        cleanup: str | None = None,
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
            if key is not None and any(
                json.loads(item[0]).get("key") == key
                for item in db.execute(
                    "SELECT payload_json FROM delivery_events WHERE run_id=? AND type=?",
                    (run_id, event_type),
                )
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
                "iteration": iteration if iteration is not None else row["iteration"],
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
                "cleanup": cleanup if cleanup is not None else row["cleanup"],
                "error": error if error is not None else row["error"],
                "updated_at": _now(),
            }
            assignments = ",".join(f"{field}=?" for field in values)
            db.execute(
                f"UPDATE delivery_runs SET {assignments} WHERE run_id=?",
                (*values.values(), run_id),
            )
            self._event(
                db,
                run_id,
                revision,
                event_type,
                message,
                {
                    "key": key,
                    "iteration": values["iteration"],
                    "candidate_id": candidate["id"] if candidate else None,
                },
            )
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
            "issue_url": row["issue_url"],
            "repository": row["repository_key"],
            "phase": row["phase"],
            "execution_state": row["execution_state"],
            "updated_at": row["updated_at"],
            # Public command revisions follow Temporal's deterministic
            # protocol. The SQLite projection has an independent sequence.
            "revision": row["protocol_revision"],
            "projection_revision": row["revision"],
            "protocol_revision": row["protocol_revision"],
            "iteration": row["iteration"],
            "authorized_endpoint": spec["authorized_endpoint"],
            "outcome": row["outcome"],
            "cleanup": row["cleanup"],
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
        spec = json.loads(row["request_json"])
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
        events = self.events(run_id)
        event_types = {event["type"] for event in events}
        current_event_types = {
            event["type"]
            for event in events
            if event["payload"].get("iteration") == row["iteration"]
        }
        checks = json.loads(row["checks_json"]) if row["checks_json"] else {}
        tracker = json.loads(row["tracker_json"]) if row["tracker_json"] else {}
        observed_gate_states = {
            "prepublish": checks.get("prepublish", {}).get("state"),
            "browser_qa": checks.get("browser_qa", {}).get("state"),
            "local_checks": checks.get("local", {}).get("state"),
            "required_ci": checks.get("ci", {}).get("state"),
            "tracker": tracker.get("state"),
        }

        def gate_state(name: str, completion: str) -> str:
            relevant_events = event_types if name == "prepare" else current_event_types
            if completion in relevant_events or observed_gate_states.get(name) in {
                "passed",
                "consistent",
            }:
                return "completed"
            if observed_gate_states.get(name) in {"failed", "blocked", "conflict"}:
                return "failed"
            if observed_gate_states.get(name) == "unknown":
                return "unknown"
            return "pending"

        gate_specs = [
            ("prepare", "Prepare", "tracker_start"),
            ("prepublish", "Before PR checks", "candidate_ready"),
            ("publish", "Publish", "published"),
            (
                "local_checks",
                "Local checks",
                "browser_qa_started" if spec["policy"].get("browser_qa") else "ci_wait",
            ),
        ]
        if spec["policy"].get("browser_qa"):
            gate_specs.append(("browser_qa", "Browser / API QA", "browser_qa_passed"))
        gate_specs.extend(
            (
                ("required_ci", "Required CI", "tracker_started"),
                ("tracker", "Tracker", "delivered"),
            )
        )
        gates = [
            {
                "id": name,
                "label": label,
                "state": gate_state(name, completion),
            }
            for name, label, completion in gate_specs
        ]
        return {
            **compact,
            "run": compact,
            "phase_gates": gates,
            "roles": roles,
            "capacity": {"limit": self.config.raw.get("capacity", 2), "active": active},
            "queued": row["execution_state"] == "queued",
            "cleanup": "unknown"
            if row["cleanup"] == "unknown" or any(a["cleanup"] == "unknown" for a in attempts)
            else row["cleanup"],
            "candidate": {
                **candidate,
                "revision": row["candidate_revision"],
                "base_sha": spec["base_sha"],
                "policy_digest": spec["policy_digest"],
            }
            if candidate
            else None,
            "pull_request": json.loads(row["pr_json"]) if row["pr_json"] else None,
            "checks": checks,
            "tracker": tracker,
            "usage": json.loads(row["usage_json"]) if row["usage_json"] else {},
            "decisions": [json.loads(row["decision_json"])]
            if row["decision_json"] and json.loads(row["decision_json"]) is not None
            else [],
            "events": events,
            "error": row["error"],
        }

    def _evidence_items(self, run_id: str) -> list[dict[str, Any]]:
        spec = self.spec(run_id)
        root = Path(spec["state_dir"])
        indexed: list[dict[str, Any]] = []
        with self._connect() as db:
            attempts = db.execute(
                """SELECT job_key,role,iteration,process_identity,result_json
                   FROM delivery_attempts WHERE run_id=?""",
                (run_id,),
            ).fetchall()
            browser_effects = {
                row["effect_key"]: json.loads(row["observed_json"])
                for row in db.execute(
                    """SELECT effect_key,observed_json FROM delivery_effects
                       WHERE run_id=? AND kind='browser_qa' AND state='complete'
                       AND observed_json IS NOT NULL""",
                    (run_id,),
                )
            }
        for attempt in attempts:
            folder = root / "attempts" / attempt["job_key"]
            result = json.loads(attempt["result_json"]) if attempt["result_json"] else {}
            contained_log = folder / "container" / "container.log"
            intent = folder / "container" / "container-intent.json"
            if intent.is_symlink():
                continue
            contained = (
                intent.is_file()
                or bool(result.get("container_id") or result.get("container_log_sha256"))
                or bool(re.fullmatch(r"[0-9a-f]{64}", attempt["process_identity"] or ""))
            )
            path = contained_log if contained else folder / "process.log"
            if path.is_file():
                indexed.append(
                    {
                        "id": f"role-{attempt['role']}-{attempt['iteration']}",
                        "label": f"{attempt['role']} log, attempt {attempt['iteration']}",
                        "path": path,
                        "expected_sha256": result.get("container_log_sha256")
                        if path == contained_log
                        else None,
                        "limit": 20 * 1024 * 1024 if path == contained_log else 1024 * 1024,
                    }
                )
        details = self.detail(run_id)
        for result in details.get("checks", {}).get("local", {}).get("results", []):
            path = Path(result["log"])
            indexed.append(
                {
                    "id": f"check-{result['id']}",
                    "label": result["id"],
                    "path": path,
                    "expected_sha256": result.get("log_sha256"),
                    "limit": 20 * 1024 * 1024 if path.name == "container.log" else 1024 * 1024,
                }
            )
        for folder in sorted((root / "browser-qa").glob("[0-9]*")):
            if not folder.name.isdecimal():
                continue
            receipt = folder / "receipt.json"
            effect = browser_effects.get(f"browser_qa:{run_id}:{folder.name}")
            projected = (
                details.get("checks", {}).get("browser_qa")
                if int(folder.name) == details["iteration"]
                else None
            )
            if not isinstance(projected, dict) or not projected.get("receipt_sha256"):
                projected = None
            if effect is not None and not isinstance(effect, dict):
                continue
            if (
                effect is not None
                and projected is not None
                and any(
                    effect.get(field) != projected.get(field)
                    for field in ("receipt", "receipt_sha256", "log", "log_sha256")
                )
            ):
                continue
            binding = effect if effect is not None else projected
            contained_log = folder / "container" / "container.log"
            intent = folder / "container" / "container-intent.json"
            if intent.is_symlink():
                continue
            contained = intent.is_file() or (
                binding is not None and binding.get("log") == str(contained_log)
            )
            path = contained_log if contained else folder / "browser-qa.log"
            if binding is not None:
                expected_receipt = binding.get("receipt_sha256")
                if (
                    not isinstance(expected_receipt, str)
                    or binding.get("receipt") != str(receipt)
                    or binding.get("log") != str(path)
                    or binding.get("iteration", int(folder.name)) != int(folder.name)
                    or receipt.is_symlink()
                    or not receipt.is_file()
                    or root.resolve() not in receipt.resolve().parents
                    or receipt.stat().st_size > 1024 * 1024
                    or hashlib.sha256(receipt.read_bytes()).hexdigest() != expected_receipt
                ):
                    continue
                indexed.append(
                    {
                        "id": f"browser-qa-{folder.name}-receipt",
                        "label": f"browser QA receipt, iteration {folder.name}",
                        "path": receipt,
                        "expected_sha256": expected_receipt,
                        "limit": 1024 * 1024,
                    }
                )
            if path.is_file():
                indexed.append(
                    {
                        "id": f"browser-qa-{folder.name}-log",
                        "label": f"browser QA log, iteration {folder.name}",
                        "path": path,
                        "expected_sha256": binding.get("log_sha256") if binding else None,
                        "limit": 20 * 1024 * 1024 if path == contained_log else 1024 * 1024,
                    }
                )
        recovery = root / "recovery" / "provenance.json"
        if recovery.is_file():
            indexed.append(
                {
                    "id": "recovery-provenance",
                    "label": "Recovery provenance",
                    "path": recovery,
                    "limit": 1024 * 1024,
                }
            )
        safe: list[dict[str, Any]] = []
        for item in indexed:
            path = item["path"]
            if (
                path.is_symlink()
                or not path.is_file()
                or root.resolve() not in path.resolve().parents
                or path.stat().st_size > item["limit"]
            ):
                continue
            safe.append(item)
        return safe

    def evidence_index(self, run_id: str) -> list[dict[str, Any]]:
        safe = []
        for item in self._evidence_items(run_id):
            path = item["path"]
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
        matched = [item for item in self._evidence_items(run_id) if item["id"] == evidence_id]
        if len(matched) != 1:
            raise ValueError("evidence ID is not indexed for this run")
        item = matched[0]
        path = item["path"]
        root = Path(self.spec(run_id)["state_dir"])
        if path.is_symlink() or root.resolve() not in path.resolve().parents:
            raise ValueError("evidence path escaped its run state")
        data = path.read_bytes()
        if len(data) > item["limit"]:
            raise ValueError("evidence exceeds the local read limit")
        observed_sha256 = hashlib.sha256(data).hexdigest()
        if item.get("expected_sha256") and item["expected_sha256"] != observed_sha256:
            raise ValueError("evidence differs from its immutable result")
        return {
            "id": evidence_id,
            "sha256": observed_sha256,
            "bytes": len(data),
            "text": data.decode("utf-8", errors="replace"),
        }
