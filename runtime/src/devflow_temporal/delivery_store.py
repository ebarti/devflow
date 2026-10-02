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
from .delivery_codec import DELIVERY_DATA_CONVERTER
from .delivery_config import DeliveryConfig, scope_amended_spec, scope_amendment_config
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
                    intake_json TEXT,
                    accepted_plan_text TEXT,
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
            if "intake_json" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN intake_json TEXT")
            if "accepted_plan_text" not in columns:
                db.execute("ALTER TABLE delivery_runs ADD COLUMN accepted_plan_text TEXT")
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
                """CREATE TABLE IF NOT EXISTS delivery_preparations (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    submitted_spec_digest TEXT NOT NULL,
                    effective_spec_digest TEXT NOT NULL,
                    effective_spec_json TEXT NOT NULL,
                    prepared_at TEXT NOT NULL
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
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_repair_grants (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT NOT NULL UNIQUE,
                    predecessor_workflow_id TEXT NOT NULL,
                    predecessor_execution_run_id TEXT NOT NULL,
                    predecessor_result_digest TEXT NOT NULL,
                    granted_iterations INTEGER NOT NULL,
                    maximum_iteration INTEGER NOT NULL,
                    granted_at TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_repair_grant_extensions (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    grant_number INTEGER NOT NULL CHECK (grant_number = 2),
                    command_id TEXT NOT NULL UNIQUE,
                    predecessor_workflow_id TEXT NOT NULL,
                    predecessor_execution_run_id TEXT NOT NULL,
                    predecessor_result_digest TEXT NOT NULL,
                    review_job_key TEXT NOT NULL,
                    review_receipt_sha256 TEXT NOT NULL,
                    effective_policy_digest TEXT NOT NULL,
                    grant_record_digest TEXT NOT NULL,
                    amendment_record_digest TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    pr_head TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    granted_iterations INTEGER NOT NULL CHECK (granted_iterations = 2),
                    maximum_iteration INTEGER NOT NULL,
                    granted_at TEXT NOT NULL
                )"""
            )
            extension_columns = {
                row[1] for row in db.execute(
                    "PRAGMA table_info(delivery_repair_grant_extensions)"
                )
            }
            for name, kind in (
                ("grant_record_digest", "TEXT"),
                ("amendment_record_digest", "TEXT"),
                ("candidate_id", "TEXT"),
                ("pr_number", "INTEGER"),
                ("pr_head", "TEXT"),
                ("session_id", "TEXT"),
            ):
                if name not in extension_columns:
                    db.execute(
                        f"ALTER TABLE delivery_repair_grant_extensions ADD COLUMN {name} {kind}"
                    )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_repair_grant_thirds (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    grant_number INTEGER NOT NULL CHECK (grant_number = 3),
                    command_id TEXT NOT NULL UNIQUE,
                    predecessor_workflow_id TEXT NOT NULL,
                    predecessor_execution_run_id TEXT NOT NULL,
                    predecessor_result_digest TEXT NOT NULL,
                    review_job_key TEXT NOT NULL,
                    review_receipt_sha256 TEXT NOT NULL,
                    effective_policy_digest TEXT NOT NULL,
                    grant_record_digest TEXT NOT NULL,
                    amendment_record_digest TEXT NOT NULL,
                    prior_extension_digest TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    pr_head TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    operator_brief_digest TEXT NOT NULL,
                    granted_iterations INTEGER NOT NULL CHECK (granted_iterations = 2),
                    maximum_iteration INTEGER NOT NULL,
                    granted_at TEXT NOT NULL
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_repair_grant_successors (
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    grant_number INTEGER NOT NULL CHECK (grant_number >= 4),
                    command_id TEXT NOT NULL UNIQUE,
                    predecessor_workflow_id TEXT NOT NULL,
                    predecessor_execution_run_id TEXT NOT NULL,
                    predecessor_result_digest TEXT NOT NULL,
                    review_job_key TEXT NOT NULL,
                    review_receipt_sha256 TEXT NOT NULL,
                    review_container_log_sha256 TEXT NOT NULL,
                    effective_policy_digest TEXT NOT NULL,
                    ancestor_row_digests_json TEXT NOT NULL,
                    prior_grant_digest TEXT NOT NULL,
                    candidate_id TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    pr_head TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    operator_brief_digest TEXT NOT NULL,
                    granted_iterations INTEGER NOT NULL CHECK (granted_iterations IN (1, 2)),
                    maximum_iteration INTEGER NOT NULL,
                    granted_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, grant_number)
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_scope_amendments (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT NOT NULL UNIQUE,
                    predecessor_workflow_id TEXT NOT NULL,
                    predecessor_execution_run_id TEXT NOT NULL,
                    predecessor_result_digest TEXT NOT NULL,
                    predecessor_attempt_job_key TEXT NOT NULL,
                    predecessor_attempt_result_sha256 TEXT NOT NULL,
                    original_policy_digest TEXT NOT NULL,
                    effective_policy_digest TEXT NOT NULL,
                    added_paths_json TEXT NOT NULL,
                    maximum_iteration INTEGER NOT NULL,
                    authorized_at TEXT NOT NULL
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
                              cleanup,error,pr_json,checks_json,request_digest,request_json,
                              accepted_plan_text
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
                prior_spec = self._prepared_original(db, json.loads(previous["request_json"]))
                if previous["accepted_plan_text"] is not None:
                    prior_spec["accepted_plan"] = previous["accepted_plan_text"]
                if prior_spec["branch"] == spec["branch"]:
                    raise ValueError("superseded run retains the owned branch; choose a new branch")
                if attempts:
                    if spec.get("intake_required") and (
                        spec["policy"].get("recovery") or {}
                    ).get("continuation"):
                        if previous["accepted_plan_text"] is None:
                            raise ValueError("continuation predecessor has no accepted plan")
                        spec["accepted_plan"] = previous["accepted_plan_text"]
                        spec["intake_required"] = False
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
            initial_phase = "preparing" if spec.get("preparation_version") == 1 else "accepted"
            db.execute(
                """INSERT INTO delivery_runs
                   (run_id,request_digest,request_json,work_id,issue_url,repository_key,
                    phase,execution_state,revision,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,'queued',1,?,?)""",
                (
                    run_id,
                    request_digest,
                    canonical_json(spec),
                    spec["work_id"],
                    spec["issue_url"],
                    spec["repository_key"],
                    initial_phase,
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
                "phase": initial_phase,
            }
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, request_digest, canonical_json(response)),
            )
            return response

    def _completed_temporal_result(
        self, run_id: str, *, workflow_id: str | None = None
    ) -> dict[str, Any]:
        """Read a closed workflow from Temporal, never from caller-authored JSON."""

        async def read() -> dict[str, Any]:
            client = await Client.connect(
                self.config.temporal_address,
                namespace=self.config.raw.get("temporal_namespace", "default"),
                data_converter=DELIVERY_DATA_CONVERTER,
            )
            handle = client.get_workflow_handle(workflow_id or "delivery-" + run_id)
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
                "recovery_digest": await description.memo_value("recovery_digest", None),
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
        spec = self.spec(run_id)
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
            or state.get("cleanup") not in {"none", "pending_publication_readback"}
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
                "SELECT effect_key,kind,request_json,state,observed_json "
                "FROM delivery_effects WHERE run_id=?",
                (run_id,),
            ).fetchall()
            attempts = db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            key = f"publish:{run_id}:{iteration}"
            target = [item for item in effects if item["effect_key"] == key]
            if (
                current is None
                or current["request_json"] != row["request_json"]
                or current["outcome"] != "blocked"
                or current["phase"] != "blocked"
                or current["error"] != state["error"]
                or current["cleanup"] != state["cleanup"]
                or current["protocol_revision"] != state["revision"]
                or current["recovery_json"] is not None
                or current["workflow_id"] is not None
                or json.loads(current["candidate_json"] or "null") != candidate
                or json.loads(current["checks_json"] or "{}").get("prepublish") != precheck
                or json.loads(current["pr_json"] or "null") != previous_pr
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or len(target) != 1
                or target[0]["state"] not in {"pending", "complete"}
                or target[0]["kind"] != "publish"
                or target[0]["request_json"]
                != canonical_json(
                    {
                        "iteration": iteration,
                        "input_candidate_id": candidate["id"],
                    }
                )
                or (
                    target[0]["state"] == "complete"
                    and target[0]["observed_json"] != canonical_json(observed)
                )
                or any(
                    item["state"] != "complete" for item in effects if item["effect_key"] != key
                )
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

    def _precheck_recovery_readback(
        self, spec: dict[str, Any], recovery: dict[str, Any], *, queued: bool
    ) -> None:
        """Prove the scope-amended role ended before a single check could start."""
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_amendment_lineage_cleanup,
            published_identity,
        )

        run_id = spec["run_id"]
        scope = recovery.get("scope_recovery")
        state = recovery.get("state")
        candidate = recovery.get("candidate")
        role = state.get("roles", [])[-1] if isinstance(state, dict) else None
        if (
            recovery.get("kind") != "precheck_prelaunch_recovery"
            or not isinstance(scope, dict)
            or scope.get("kind") != "scope_amendment"
            or scope.get("effective_spec") != spec
            or self.effective_spec(run_id) != spec
            or not isinstance(role, dict)
            or state.get("candidate") != candidate
            or role.get("candidate") != candidate
            or role.get("role") != "implement"
            or role.get("iteration") != recovery.get("iteration")
            or role.get("status") != "pass"
            or role.get("finish_reason") != "done"
            or role.get("cleanup") != "confirmed"
            or role.get("session_id") != recovery.get("session_id")
            or role.get("session_id") != scope.get("session_id")
            or role.get("container_id") != recovery.get("role_container_id")
            or recovery.get("iteration") != scope.get("maximum_iteration")
            or recovery.get("predecessor_result_digest") != digest(state)
            or recovery.get("pull_request") != state.get("pull_request")
            or state.get("checks") != {
                "prepublish": {
                    "candidate_id": candidate.get("id") if isinstance(candidate, dict) else None,
                    "cleanup": "unknown",
                    "reason": "ContainerUnknown",
                    "state": "unknown",
                }
            }
        ):
            raise ValueError("closed precheck checkpoint changed")
        original = self.spec(run_id)
        if (
            original["policy_digest"] == spec["policy_digest"]
            or "dependency-preparation/container-intent.json"
            not in scope.get("old_container_intents", {})
        ):
            raise ValueError("precheck recovery has no original-policy dependency collision")
        with self._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
        if (
            row is None
            or amendment is None
            or grant is None
            or row["request_json"] != canonical_json(
                self.submitted_spec(run_id)
                if original.get("preparation_version") == 1 else original
            )
            or row["recovery_json"] != canonical_json(recovery if queued else scope)
            or row["workflow_id"] != (
                f"delivery-{run_id}-precheck-recovery-1" if queued
                else recovery["predecessor_workflow_id"]
            )
            or row["phase"] not in (
                {"precheck_recovery_queued", "repair_preflight", "prepublish_checks"}
                if queued else {"blocked"}
            )
            or row["execution_state"] not in (
                {"queued", "running"} if queued else {"blocked"}
            )
            or (not queued and (
                row["outcome"] != "blocked"
                or row["cleanup"] != "unknown"
                or row["error"] != "prepublication container cleanup is unknown"
                or row["protocol_revision"] != state["revision"]
            ))
            or row["iteration"] != recovery["iteration"]
            or json.loads(row["candidate_json"] or "null") != candidate
            or json.loads(row["pr_json"] or "null") != recovery["pull_request"]
            or (not queued and json.loads(row["checks_json"] or "{}") != state["checks"])
            or claim is None
            or claim["owner"] != f"external:devflow:{run_id}"
            or amendment["effective_policy_digest"] != spec["policy_digest"]
            or amendment["original_policy_digest"] != original["policy_digest"]
            or amendment["maximum_iteration"] != recovery["iteration"]
            or amendment["predecessor_workflow_id"] != scope["predecessor_workflow_id"]
            or amendment["predecessor_execution_run_id"]
            != scope["predecessor_execution_run_id"]
            or amendment["predecessor_result_digest"] != digest(scope["state"])
            or amendment["predecessor_attempt_job_key"] != scope["attempt_job_key"]
            or amendment["predecessor_attempt_result_sha256"]
            != scope["attempt_result_sha256"]
            or json.loads(amendment["added_paths_json"]) != scope["added_paths"]
            or grant["maximum_iteration"] + 1 != recovery["iteration"]
            or any(item["state"] != "finished" or item["cleanup"] != "confirmed"
                   for item in attempts)
            or any(item["state"] != "complete" or item["observed_json"] is None
                   for item in effects)
            or any(item["effect_key"].endswith(f":{recovery['iteration']}")
                   for item in effects)
        ):
            raise ValueError("precheck recovery lost the frozen run or claim")
        current_role = [
            item for item in attempts
            if item["role"] == "implement" and item["iteration"] == recovery["iteration"]
        ]
        if len(current_role) != 1 or current_role[0]["job_key"] != recovery["role_job_key"]:
            raise ValueError("amended implementation attempt is ambiguous")
        attempt = current_role[0]
        expected_job_key = digest({
            "run_id": run_id,
            "role": "implement",
            "iteration": recovery["iteration"],
            "candidate_id": scope["amended_candidate"]["id"],
            "policy_digest": spec["policy_digest"],
        })
        receipt = Path(spec["state_dir"]) / "attempts" / attempt["job_key"] / "result.json"
        if (
            attempt["job_key"] != expected_job_key
            or attempt["candidate_id"] != scope["amended_candidate"]["id"]
            or attempt["session_id"] != recovery["session_id"]
            or attempt["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
        ):
            raise ValueError("amended implementation receipt is missing")
        info = receipt.stat()
        result_bytes = receipt.read_bytes()
        raw_result = json.loads(result_bytes)
        result = json.loads(attempt["result_json"] or "null")
        if (
            info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or hashlib.sha256(result_bytes).hexdigest() != recovery["role_receipt_sha256"]
            or not isinstance(result, dict)
            or result_bytes != (
                json.dumps(raw_result, sort_keys=True, indent=2) + "\n"
            ).encode()
            or raw_result != {
                key: value for key, value in result.items()
                if key not in {"cleanup", "container_id", "container_log_sha256"}
            }
            or any(role.get(key) != value for key, value in result.items())
            or result.get("container_id") != recovery["role_container_id"]
        ):
            raise ValueError("amended implementation receipt changed")
        root = Path(spec["state_dir"])
        prechecks = root / "prechecks" / str(recovery["iteration"])
        meta = prechecks.lstat()
        if (
            not stat.S_ISDIR(meta.st_mode)
            or meta.st_uid != os.getuid()
            or stat.S_IMODE(meta.st_mode) != 0o700
            or any(prechecks.iterdir())
            or any(
                (root / folder / str(recovery["iteration"])).exists()
                for folder in ("checks", "browser-qa", "gate-evidence", "gates")
            )
            or (root / f"dependency-preparation-{spec['policy_digest']}").exists()
        ):
            raise ValueError("prepublication execution already started or is ambiguous")
        broker = DeliveryBroker(self, spec)
        if broker.candidate() != candidate:
            raise ValueError("amended candidate changed after role completion")
        published_identity(broker, candidate, recovery["pull_request"])
        intent_sha = confirmed_amendment_lineage_cleanup(
            original, spec, scope["old_container_intents"], recovery["role_intent"]
        )
        if intent_sha != recovery["role_intent_sha256"]:
            raise ValueError("amended role container intent changed")

    def recover_precheck_prelaunch(
        self, run_id: str, supplied: dict[str, Any]
    ) -> dict[str, Any]:
        """Queue one same-run precheck retry after a proven prelaunch collision."""
        required = {
            "command_id", "expected_revision", "expected_iteration",
            "expected_candidate_id", "expected_pr_number", "expected_pr_head",
            "expected_session_id", "expected_policy_digest",
            "expected_execution_run_id",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("precheck recovery fields do not match the contract")
        if (
            not isinstance(supplied["command_id"], str)
            or not supplied["command_id"]
            or type(supplied["expected_revision"]) is not int
            or type(supplied["expected_iteration"]) is not int
            or type(supplied["expected_pr_number"]) is not int
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
            or not isinstance(supplied["expected_session_id"], str)
            or not isinstance(supplied["expected_policy_digest"], str)
            or not isinstance(supplied["expected_execution_run_id"], str)
        ):
            raise ValueError("invalid precheck recovery identity")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            attempts_snapshot = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_snapshot = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if row is None:
            raise ValueError("run ID not found")
        scope = json.loads(row["recovery_json"] or "null")
        if not isinstance(scope, dict) or scope.get("kind") != "scope_amendment":
            raise ValueError("run has no eligible scope-amendment predecessor")
        spec = self.effective_spec(run_id)
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        candidate = state.get("candidate") if isinstance(state, dict) else None
        roles = state.get("roles") if isinstance(state, dict) else None
        role = roles[-1] if isinstance(roles, list) and roles else None
        pr = state.get("pull_request") if isinstance(state, dict) else None
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["execution_run_id"] != supplied["expected_execution_run_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != digest(scope)
            or not isinstance(state, dict)
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("cleanup") != "unknown"
            or state.get("error") != "prepublication container cleanup is unknown"
            or state.get("revision") != supplied["expected_revision"]
            or state.get("iteration") != supplied["expected_iteration"]
            or state.get("iteration") != scope.get("maximum_iteration")
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or candidate.get("policy_digest") != supplied["expected_policy_digest"]
            or candidate.get("policy_digest") != spec["policy_digest"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or candidate.get("head") != pr.get("head")
            or not isinstance(role, dict)
            or role.get("role") != "implement"
            or role.get("iteration") != state["iteration"]
            or role.get("status") != "pass"
            or role.get("finish_reason") != "done"
            or role.get("cleanup") != "confirmed"
            or role.get("session_id") != supplied["expected_session_id"]
            or role.get("session_id") != scope.get("session_id")
            or role.get("candidate") != candidate
        ):
            raise ValueError("closed Temporal result is not a prelaunch precheck collision")
        with self._connect() as db:
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND role='implement' "
                "AND iteration=?", (run_id, state["iteration"]),
            ).fetchall()
        if len(attempts) != 1:
            raise ValueError("amended implementation attempt is ambiguous")
        attempt = attempts[0]
        receipt = Path(spec["state_dir"]) / "attempts" / attempt["job_key"] / "result.json"
        intent = f"attempts/{attempt['job_key']}/container/container-intent.json"
        if not receipt.is_file() or not (Path(spec["state_dir"]) / intent).is_file():
            raise ValueError("amended implementation evidence is missing")
        recovery = {
            "kind": "precheck_prelaunch_recovery",
            "scope_recovery": scope,
            "effective_spec": spec,
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state,
            "iteration": state["iteration"],
            "candidate": candidate,
            "pull_request": pr,
            "session_id": role["session_id"],
            "role_job_key": attempt["job_key"],
            "role_receipt_sha256": hashlib.sha256(receipt.read_bytes()).hexdigest(),
            "role_intent": intent,
            "role_intent_sha256": hashlib.sha256(
                (Path(spec["state_dir"]) / intent).read_bytes()
            ).hexdigest(),
            "role_container_id": json.loads(attempt["result_json"])["container_id"],
        }
        self._precheck_recovery_readback(spec, recovery, queued=False)
        workflow_id = f"delivery-{run_id}-precheck-recovery-1"
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "precheck_recovery_queued",
            "workflow_id": workflow_id,
            "iteration": state["iteration"],
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts_now = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_now = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            if (
                current is None
                or tuple(current) != tuple(row)
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or [tuple(item) for item in attempts_now]
                != [tuple(item) for item in attempts_snapshot]
                or [tuple(item) for item in effects_now]
                != [tuple(item) for item in effects_snapshot]
            ):
                raise ValueError("precheck recovery lost its frozen run or ownership")
            revision = current["revision"] + 1
            db.execute(
                """UPDATE delivery_runs SET phase='precheck_recovery_queued',
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
                db, run_id, revision, "precheck_recovery_queued",
                "Sealed amended role and stopped containers; resuming prechecks without a role",
                {
                    "candidate_id": candidate["id"],
                    "iteration": state["iteration"],
                    "role_job_key": attempt["job_key"],
                    "session_id": role["session_id"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (supplied["command_id"], run_id, command_digest, canonical_json(response)),
            )
        return response

    def precheck_recovery_preflight(
        self, spec: dict[str, Any], recovery: dict[str, Any]
    ) -> None:
        self._precheck_recovery_readback(spec, recovery, queued=True)

    def scope_preflight(self, spec: dict[str, Any], recovery: dict[str, Any]) -> None:
        """Recheck the sealed amendment before the sole enlarged-scope role."""
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import confirmed_container_cleanup, published_identity

        run_id = spec["run_id"]
        if recovery.get("kind") != "scope_amendment" or self.effective_spec(run_id) != spec:
            raise ValueError("scope amendment effective authority changed")
        # External Docker image readback belongs at this cancellable boundary,
        # not in ordinary projection or outbox dispatch.
        admitted = scope_amended_spec(
            self.intake_execution_spec(run_id), Path(recovery["amended_config_path"]),
            recovery["amended_config_sha256"], recovery["added_paths"],
        )
        if admitted != spec:
            raise ValueError("scope amendment image or policy changed")
        with self._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT state,observed_json FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if (
            row is None
            or amendment is None
            or grant is None
            or row["recovery_json"] != canonical_json(recovery)
            or row["workflow_id"] != f"delivery-{run_id}-scope-amendment-1"
            or row["phase"] not in {
                "scope_amendment_queued", "repair_preflight", "tracker_start", "repair"
            }
            or row["execution_state"] not in {"queued", "running"}
            or amendment["predecessor_workflow_id"]
            != recovery["predecessor_workflow_id"]
            or amendment["predecessor_execution_run_id"]
            != recovery["predecessor_execution_run_id"]
            or amendment["predecessor_result_digest"] != digest(recovery["state"])
            or amendment["predecessor_attempt_job_key"] != recovery["attempt_job_key"]
            or amendment["predecessor_attempt_result_sha256"]
            != recovery["attempt_result_sha256"]
            or amendment["original_policy_digest"]
            != self.spec(run_id)["policy_digest"]
            or amendment["effective_policy_digest"] != spec["policy_digest"]
            or amendment["maximum_iteration"] != recovery["maximum_iteration"]
            or grant["maximum_iteration"] + 1 != recovery["maximum_iteration"]
            or claim is None
            or claim["owner"] != f"external:devflow:{run_id}"
            or any(item["state"] != "finished" or item["cleanup"] != "confirmed"
                   for item in attempts)
            or any(item["state"] != "complete" or item["observed_json"] is None
                   for item in effects)
        ):
            raise ValueError("scope amendment or owned resources changed before resume")
        original = self.spec(run_id)
        if row["request_json"] != canonical_json(
                self.submitted_spec(run_id)
                if original.get("preparation_version") == 1 else original
            ):
            raise ValueError("original request changed before scope repair")
        confirmed_container_cleanup(original)
        if self._container_intent_inventory(original) != recovery["old_container_intents"]:
            raise ValueError("predecessor container inventory changed")
        with self._connect() as db:
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE job_key=? AND run_id=?",
                (amendment["predecessor_attempt_job_key"], run_id),
            ).fetchone()
        receipt_path = Path(attempt["result_path"] or "") if attempt else Path()
        receipt_info = receipt_path.lstat() if attempt and receipt_path.is_absolute() else None
        if (
            attempt is None
            or attempt["state"] != "finished"
            or attempt["cleanup"] != "confirmed"
            or attempt["session_id"] != recovery["session_id"]
            or receipt_path
            != Path(original["state_dir"]) / "attempts" / attempt["job_key"] / "result.json"
            or receipt_info is None
            or not stat.S_ISREG(receipt_info.st_mode)
            or receipt_info.st_uid != os.getuid()
            or stat.S_IMODE(receipt_info.st_mode) != 0o600
            or hashlib.sha256(receipt_path.read_bytes()).hexdigest()
            != amendment["predecessor_attempt_result_sha256"]
        ):
            raise ValueError("predecessor role receipt changed before scope repair")
        old_broker = DeliveryBroker(self, original)
        if old_broker.candidate() != recovery["source_candidate"]:
            raise ValueError("post-role source candidate changed")
        published_identity(
            old_broker, recovery["source_candidate"], recovery["state"]["pull_request"]
        )
        if DeliveryBroker(self, spec).candidate() != recovery["amended_candidate"]:
            raise ValueError("amended candidate changed before repair")

    def continue_repair(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Spend one explicit, bounded grant on a closed failed gate of this run."""
        if (
            isinstance(supplied, dict)
            and type(supplied.get("grant_number")) is int
            and supplied["grant_number"] >= 4
        ):
            return self._continue_later_repair(run_id, supplied)
        if isinstance(supplied, dict) and supplied.get("grant_number") == 3:
            return self._continue_third_repair(run_id, supplied)
        if isinstance(supplied, dict) and supplied.get("grant_number") == 2:
            return self._continue_amended_repair(run_id, supplied)
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_container_cleanup,
            failed_gate_diagnostics,
            published_identity,
        )

        required = {
            "command_id",
            "expected_revision",
            "expected_iteration",
            "expected_candidate_id",
            "expected_pr_number",
            "expected_pr_head",
            "additional_iterations",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("repair continuation fields do not match the contract")
        command_id = supplied["command_id"]
        if (
            not isinstance(command_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", command_id)
            or any(
                type(supplied[key]) is not int
                for key in (
                    "expected_revision",
                    "expected_iteration",
                    "expected_pr_number",
                    "additional_iterations",
                )
            )
            or supplied["expected_revision"] < 1
            or supplied["expected_iteration"] < 0
            or supplied["expected_pr_number"] < 1
            or supplied["additional_iterations"] not in (1, 2)
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
        ):
            raise ValueError("invalid repair continuation identity or grant")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            granted = db.execute(
                "SELECT 1 FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None:
            raise ValueError("run ID not found")
        if granted:
            raise ValueError("this run already received its one repair grant")
        spec = self.spec(run_id)
        if digest(DeliveryConfig.load(self.config.path).raw) != spec["config_digest"]:
            raise ValueError("frozen service configuration changed before repair grant")
        current_workflow_id = row["workflow_id"] or f"delivery-{run_id}"
        previous_recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        closed = self._completed_temporal_result(run_id, workflow_id=current_workflow_id)
        state = closed["result"]
        if not isinstance(state, dict):
            raise ValueError("closed Temporal result has no terminal state")
        candidate = state.get("candidate")
        pr = state.get("pull_request")
        roles = state.get("roles")
        checks = state.get("checks")
        iteration = state.get("iteration")
        if (
            closed["workflow_id"] != current_workflow_id
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"]
            != (digest(previous_recovery) if previous_recovery is not None else None)
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("cleanup") != "none"
            or state.get("revision") != supplied["expected_revision"]
            or type(iteration) is not int
            or iteration != supplied["expected_iteration"]
            or iteration + supplied["additional_iterations"] > spec["policy"]["max_repairs"] + 2
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(state.get("candidate"), dict)
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or not isinstance(roles, list)
            or not isinstance(checks, dict)
        ):
            raise ValueError("closed Temporal result does not authorize repair continuation")
        implementation = next(
            (
                role
                for role in reversed(roles)
                if role.get("role") == "implement" and role.get("iteration") == iteration
            ),
            None,
        )
        if (
            implementation is None
            or implementation.get("status") != "pass"
            or implementation.get("cleanup") != "confirmed"
            or not implementation.get("session_id")
            or any(
                role.get("session_id") != implementation["session_id"]
                for role in roles
                if role.get("role") == "implement"
            )
            or any(
                role.get("cleanup") != "confirmed"
                or role.get("finish_reason") == "recovery_unknown"
                for role in roles
            )
        ):
            raise ValueError("original implementer session or role cleanup is unconfirmed")
        for key in ("prepublish", "local", "browser_qa"):
            result = checks.get(key)
            if result is None:
                continue
            if not isinstance(result, dict) or result.get("state") == "unknown":
                raise ValueError("a check has an unknown outcome")
            if result.get("cleanup") == "unknown" or (
                key == "browser_qa" and result.get("cleanup") != "confirmed"
            ):
                raise ValueError("a check has unconfirmed cleanup")
            if any(
                not isinstance(item, dict) or item.get("cleanup") != "confirmed"
                for item in result.get("results", [])
            ):
                raise ValueError("a check child has unconfirmed cleanup")
        findings = failed_gate_diagnostics(state, spec)
        if not findings:
            raise ValueError("failed gate supplied no repair diagnostics")
        broker = DeliveryBroker(self, spec)
        observed_pr = published_identity(broker, candidate, pr)
        confirmed_container_cleanup(spec)
        workflow_id = f"delivery-{run_id}-repair-continuation-1"
        recovery = {
            "kind": "repair_continuation",
            "predecessor_workflow_id": current_workflow_id,
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state,
            "candidate": candidate,
            "pull_request": observed_pr,
            "session_id": implementation["session_id"],
            "findings": findings,
            "additional_iterations": supplied["additional_iterations"],
            "maximum_iteration": iteration + supplied["additional_iterations"],
        }
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_continuation_queued",
            "workflow_id": workflow_id,
            "authorized_through_iteration": recovery["maximum_iteration"],
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT role,iteration,state,session_id,cleanup "
                "FROM delivery_attempts WHERE run_id=?",
                (run_id,),
            ).fetchall()
            effects = db.execute(
                "SELECT kind,state,observed_json FROM delivery_effects WHERE run_id=?",
                (run_id,),
            ).fetchall()
            if (
                current is None
                or current["request_json"] != row["request_json"]
                or current["workflow_id"] != row["workflow_id"]
                or current["recovery_json"] != row["recovery_json"]
                or current["phase"] != "blocked"
                or current["outcome"] != "blocked"
                or current["execution_state"] != "blocked"
                or current["cleanup"] != "none"
                or current["error"] != state["error"]
                or current["protocol_revision"] != state["revision"]
                or current["iteration"] != iteration
                or json.loads(current["candidate_json"] or "null") != candidate
                or json.loads(current["pr_json"] or "null") != pr
                or json.loads(current["checks_json"] or "{}") != checks
                or db.execute(
                    "SELECT 1 FROM delivery_repair_grants WHERE run_id=?", (run_id,)
                ).fetchone()
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or len(attempts) != len(roles)
                or any(
                    item["state"] != "finished" or item["cleanup"] != "confirmed"
                    for item in attempts
                )
                or sorted(
                    (item["role"], item["iteration"], item["session_id"])
                    for item in attempts
                )
                != sorted(
                    (item["role"], item["iteration"], item["session_id"])
                    for item in roles
                )
                or not any(item["kind"] == "publish" for item in effects)
                or any(
                    item["state"] != "complete" or item["observed_json"] is None
                    for item in effects
                )
            ):
                raise ValueError("repair continuation lost its frozen run or ownership")
            revision = current["revision"] + 1
            db.execute(
                """INSERT INTO delivery_repair_grants VALUES (?,?,?,?,?,?,?,?)""",
                (
                    run_id,
                    command_id,
                    current_workflow_id,
                    closed["execution_run_id"],
                    digest(state),
                    supplied["additional_iterations"],
                    recovery["maximum_iteration"],
                    _now(),
                ),
            )
            db.execute(
                """UPDATE delivery_runs SET phase='repair_continuation_queued',
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
                "repair_continuation_queued",
                "Explicit bounded repair grant queued after failed gate",
                {
                    "iteration": iteration,
                    "authorized_through_iteration": recovery["maximum_iteration"],
                    "candidate_id": candidate["id"],
                    "pr_number": pr["number"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "diagnostics_digest": digest(findings),
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, command_digest, canonical_json(response)),
            )
        return response

    def _continue_amended_repair(
        self, run_id: str, supplied: dict[str, Any]
    ) -> dict[str, Any]:
        """Record exactly one numbered, two-iteration grant after the amended review."""
        from .delivery_repair import failed_gate_diagnostics

        required = {
            "command_id", "grant_number", "expected_revision", "expected_iteration",
            "expected_candidate_id", "expected_pr_number", "expected_pr_head",
            "expected_session_id", "expected_policy_digest",
            "expected_execution_run_id", "expected_review_receipt_sha256",
            "additional_iterations",
        }
        if (
            set(supplied) != required
            or not isinstance(supplied["command_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", supplied["command_id"])
            or type(supplied["grant_number"]) is not int
            or supplied["grant_number"] != 2
            or type(supplied["expected_revision"]) is not int
            or type(supplied["expected_iteration"]) is not int
            or type(supplied["expected_pr_number"]) is not int
            or type(supplied["additional_iterations"]) is not int
            or supplied["additional_iterations"] != 2
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
            or not isinstance(supplied["expected_session_id"], str)
            or not supplied["expected_session_id"]
            or not isinstance(supplied["expected_policy_digest"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_policy_digest"])
            or not isinstance(supplied["expected_execution_run_id"], str)
            or not supplied["expected_execution_run_id"]
            or not isinstance(supplied["expected_review_receipt_sha256"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_review_receipt_sha256"])
        ):
            raise ValueError("numbered repair grant fields do not match the contract")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            prior_command = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if prior_command:
                if prior_command["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior_command["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            prior_grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            prior_amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            attempts_snapshot = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_snapshot = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if row is None or prior_grant is None or prior_amendment is None:
            raise ValueError("numbered grant requires the original grant and amendment")
        prior = json.loads(row["recovery_json"] or "null")
        scope = self._scope_recovery(prior)
        if (
            not isinstance(prior, dict)
            or prior.get("kind") != "precheck_prelaunch_recovery"
            or not isinstance(scope, dict)
        ):
            raise ValueError("numbered grant requires the closed amended precheck recovery")
        spec = self.effective_spec(run_id)
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        roles = state.get("roles") if isinstance(state, dict) else None
        review = roles[-1] if isinstance(roles, list) and roles else None
        candidate = state.get("candidate") if isinstance(state, dict) else None
        pr = state.get("pull_request") if isinstance(state, dict) else None
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != digest(prior)
            or closed["execution_run_id"] != supplied["expected_execution_run_id"]
            or not isinstance(state, dict)
            or state.get("revision") != supplied["expected_revision"]
            or state.get("iteration") != supplied["expected_iteration"]
            or state.get("iteration") != scope.get("maximum_iteration")
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or spec["policy_digest"] != supplied["expected_policy_digest"]
            or prior.get("session_id") != supplied["expected_session_id"]
            or not isinstance(review, dict)
            or review.get("role") != "review"
            or review.get("iteration") != state["iteration"]
        ):
            raise ValueError("numbered grant does not match the closed reviewed candidate")
        review_job_key = digest({
            "run_id": run_id, "role": "review", "iteration": state["iteration"],
            "candidate_id": candidate["id"], "policy_digest": spec["policy_digest"],
        })
        with self._connect() as db:
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                (run_id, review_job_key),
            ).fetchone()
        if attempt is None:
            raise ValueError("finished independent review attempt is missing")
        receipt = Path(spec["state_dir"]) / "attempts" / review_job_key / "result.json"
        if (
            attempt["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
            or hashlib.sha256(receipt.read_bytes()).hexdigest()
            != supplied["expected_review_receipt_sha256"]
        ):
            raise ValueError("finished independent review receipt changed")
        saved_result = json.loads(attempt["result_json"] or "null")
        if not isinstance(saved_result, dict):
            raise ValueError("finished independent review result is missing")
        findings = failed_gate_diagnostics(state, spec)
        recovery = {
            "kind": "repair_continuation",
            "grant_number": 2,
            "prior_recovery": prior,
            "effective_spec": spec,
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state,
            "candidate": candidate,
            "pull_request": pr,
            "session_id": prior["session_id"],
            "findings": findings,
            "review_summary": review["summary"],
            "review_job_key": review_job_key,
            "review_receipt_sha256": supplied["expected_review_receipt_sha256"],
            "review_container_id": saved_result.get("container_id"),
            "review_container_log_sha256": saved_result.get("container_log_sha256"),
            "grant_record_digest": digest(dict(prior_grant)),
            "amendment_record_digest": digest(dict(prior_amendment)),
            "additional_iterations": 2,
            "maximum_iteration": state["iteration"] + 2,
        }
        recovery["additional_intents"] = self._amended_repair_intents(spec, recovery)
        self._amended_repair_readback(spec, recovery, queued=False)
        workflow_id = f"delivery-{run_id}-repair-continuation-2"
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_continuation_queued",
            "workflow_id": workflow_id,
            "grant_number": 2,
            "authorized_through_iteration": recovery["maximum_iteration"],
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if replay:
                if replay["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(replay["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts_now = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_now = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            grant_now = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            amendment_now = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            extension = db.execute(
                "SELECT 1 FROM delivery_repair_grant_extensions WHERE run_id=?", (run_id,)
            ).fetchone()
            if (
                current is None
                or tuple(current) != tuple(row)
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or extension is not None
                or grant_now is None
                or digest(dict(grant_now)) != recovery["grant_record_digest"]
                or amendment_now is None
                or digest(dict(amendment_now)) != recovery["amendment_record_digest"]
                or [tuple(item) for item in attempts_now]
                != [tuple(item) for item in attempts_snapshot]
                or [tuple(item) for item in effects_now]
                != [tuple(item) for item in effects_snapshot]
            ):
                raise ValueError("numbered grant lost its frozen run or ownership")
            revision = current["revision"] + 1
            db.execute(
                """INSERT INTO delivery_repair_grant_extensions
                   (run_id,grant_number,command_id,predecessor_workflow_id,
                    predecessor_execution_run_id,predecessor_result_digest,
                    review_job_key,review_receipt_sha256,effective_policy_digest,
                    grant_record_digest,amendment_record_digest,candidate_id,
                    pr_number,pr_head,session_id,granted_iterations,
                    maximum_iteration,granted_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, 2, supplied["command_id"], closed["workflow_id"],
                    closed["execution_run_id"], digest(state), review_job_key,
                    recovery["review_receipt_sha256"], spec["policy_digest"],
                    recovery["grant_record_digest"], recovery["amendment_record_digest"],
                    candidate["id"], pr["number"], pr["head"], recovery["session_id"],
                    2, recovery["maximum_iteration"], _now(),
                ),
            )
            db.execute(
                """UPDATE delivery_runs SET phase='repair_continuation_queued',
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
                db, run_id, revision, "repair_continuation_queued",
                "Explicit numbered repair grant queued after amended independent review",
                {
                    "grant_number": 2,
                    "iteration": state["iteration"],
                    "authorized_through_iteration": recovery["maximum_iteration"],
                    "candidate_id": candidate["id"],
                    "pr_number": pr["number"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "diagnostics_digest": digest(findings),
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (supplied["command_id"], run_id, command_digest, canonical_json(response)),
            )
        return response

    def _amended_repair_readback(
        self, spec: dict[str, Any], recovery: dict[str, Any], *, queued: bool
    ) -> None:
        """Recheck the closed review, same authority and exact mixed PID inventory."""
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_amendment_lineage_cleanup,
            failed_gate_diagnostics,
            published_identity,
        )

        run_id = spec["run_id"]
        prior = recovery.get("prior_recovery")
        scope = self._scope_recovery(prior)
        state = recovery.get("state")
        original = self.spec(run_id)
        if (
            recovery.get("kind") != "repair_continuation"
            or recovery.get("grant_number") != 2
            or not isinstance(prior, dict)
            or prior.get("kind") != "precheck_prelaunch_recovery"
            or not isinstance(scope, dict)
            or not isinstance(state, dict)
            or self.effective_spec(run_id) != spec
            or prior.get("effective_spec") != spec
            or recovery.get("effective_spec") != spec
            or original["policy_digest"] == spec["policy_digest"]
            or recovery.get("maximum_iteration") != scope.get("maximum_iteration", -3) + 2
            or recovery.get("additional_iterations") != 2
            or recovery.get("session_id") != prior.get("session_id")
            or recovery.get("candidate") != state.get("candidate")
            or recovery.get("pull_request") != state.get("pull_request")
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("cleanup") != "none"
            or state.get("error") != "repair limit exhausted"
            or state.get("iteration") != scope.get("maximum_iteration")
            or not isinstance(state.get("candidate"), dict)
            or state["candidate"].get("policy_digest") != spec["policy_digest"]
            or state["candidate"].get("head") != state.get("pull_request", {}).get("head")
            or state.get("checks", {}).get("prepublish", {}).get("state") != "passed"
            or state["checks"]["prepublish"].get("source_unchanged") is not True
            or state["checks"]["prepublish"].get("candidate_id")
            != prior.get("candidate", {}).get("id")
            or state["checks"].get("review") != {
                "candidate_id": state["candidate"]["id"],
                "detail": recovery.get("review_summary"),
                "state": "failed",
            }
            or set(state["checks"]) != {"prepublish", "review"}
            or not isinstance(state.get("roles"), list)
            or len(state["roles"]) < 2
        ):
            raise ValueError("closed amended review is not eligible for a second grant")
        implementation, review = state["roles"][-2:]
        if (
            implementation.get("role") != "implement"
            or implementation.get("iteration") != state["iteration"]
            or implementation.get("status") != "pass"
            or implementation.get("finish_reason") != "done"
            or implementation.get("cleanup") != "confirmed"
            or implementation.get("session_id") != recovery["session_id"]
            or implementation.get("candidate") != prior.get("candidate")
            or review.get("role") != "review"
            or review.get("iteration") != state["iteration"]
            or review.get("status") != "findings"
            or review.get("finish_reason") != "done"
            or review.get("cleanup") != "confirmed"
            or review.get("candidate") != state["candidate"]
            or review.get("summary") != recovery["review_summary"]
            or review.get("session_id") == recovery["session_id"]
            or review.get("findings") != recovery.get("findings")
            or failed_gate_diagnostics(state, spec) != recovery.get("findings")
        ):
            raise ValueError("second grant does not bind the failed independent review")
        try:
            closed = self._completed_temporal_result(
                run_id, workflow_id=recovery["predecessor_workflow_id"]
            )
        except ValueError as exc:
            from temporalio.service import RPCError

            from .delivery_repair import RepairReadbackPending

            if isinstance(exc.__cause__, (RPCError, TimeoutError, ConnectionError, OSError)):
                raise RepairReadbackPending("Temporal closure readback unavailable") from exc
            raise
        if (
            closed["workflow_id"] != recovery["predecessor_workflow_id"]
            or closed["execution_run_id"] != recovery["predecessor_execution_run_id"]
            or closed["request_digest"] != original["request_digest"]
            or closed["recovery_digest"] != digest(prior)
            or digest(closed["result"]) != recovery["predecessor_result_digest"]
            or closed["result"] != state
        ):
            raise ValueError("second grant predecessor is not a closed Temporal review")
        expected_job_key = digest({
            "run_id": run_id, "role": "review", "iteration": state["iteration"],
            "candidate_id": state["candidate"]["id"],
            "policy_digest": spec["policy_digest"],
        })
        root = Path(spec["state_dir"])
        with self._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            extension = db.execute(
                "SELECT * FROM delivery_repair_grant_extensions WHERE run_id=?", (run_id,)
            ).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        review_attempts = [
            item for item in attempts
            if item["role"] == "review" and item["iteration"] == state["iteration"]
        ]
        if (
            row is None
            or grant is None
            or amendment is None
            or row["request_json"] != canonical_json(
                self.submitted_spec(run_id)
                if original.get("preparation_version") == 1 else original
            )
            or row["recovery_json"] != canonical_json(recovery if queued else prior)
            or row["workflow_id"] != (
                f"delivery-{run_id}-repair-continuation-2" if queued
                else recovery["predecessor_workflow_id"]
            )
            or row["phase"] not in (
                {"repair_continuation_queued", "repair_preflight", "tracker_start", "repair"}
                if queued else {"blocked"}
            )
            or row["execution_state"] not in (
                {"queued", "running"} if queued else {"blocked"}
            )
            or (not queued and (
                row["outcome"] != "blocked"
                or row["cleanup"] != "none"
                or row["error"] != state["error"]
                or row["protocol_revision"] != state["revision"]
            ))
            or row["iteration"] != state["iteration"]
            or json.loads(row["candidate_json"] or "null") != state["candidate"]
            or json.loads(row["pr_json"] or "null") != state["pull_request"]
            or json.loads(row["checks_json"] or "{}") != state["checks"]
            or grant["maximum_iteration"] + 1 != amendment["maximum_iteration"]
            or amendment["maximum_iteration"] != state["iteration"]
            or amendment["original_policy_digest"] != original["policy_digest"]
            or amendment["effective_policy_digest"] != spec["policy_digest"]
            or json.loads(amendment["added_paths_json"]) != scope["added_paths"]
            or digest(dict(grant)) != recovery.get("grant_record_digest")
            or digest(dict(amendment)) != recovery.get("amendment_record_digest")
            or (queued and (
                extension is None
                or extension["grant_number"] != 2
                or extension["predecessor_workflow_id"]
                != recovery["predecessor_workflow_id"]
                or extension["predecessor_execution_run_id"]
                != recovery["predecessor_execution_run_id"]
                or extension["predecessor_result_digest"]
                != recovery["predecessor_result_digest"]
                or extension["review_job_key"] != recovery["review_job_key"]
                or extension["review_receipt_sha256"]
                != recovery["review_receipt_sha256"]
                or extension["effective_policy_digest"] != spec["policy_digest"]
                or extension["grant_record_digest"] != recovery["grant_record_digest"]
                or extension["amendment_record_digest"]
                != recovery["amendment_record_digest"]
                or extension["candidate_id"] != state["candidate"]["id"]
                or extension["pr_number"] != state["pull_request"]["number"]
                or extension["pr_head"] != state["pull_request"]["head"]
                or extension["session_id"] != recovery["session_id"]
                or extension["granted_iterations"] != 2
                or extension["maximum_iteration"] != recovery["maximum_iteration"]
            ))
            or (not queued and extension is not None)
            or claim is None
            or claim["owner"] != f"external:devflow:{run_id}"
            or any(item["state"] != "finished" or item["cleanup"] != "confirmed"
                   for item in attempts)
            or any(item["state"] != "complete" or item["observed_json"] is None
                   for item in effects)
            or len(review_attempts) != 1
            or recovery["review_job_key"] != expected_job_key
            or review_attempts[0]["job_key"] != expected_job_key
        ):
            raise ValueError("second grant lost its frozen run, claim or authority")
        attempt = review_attempts[0]
        receipt = root / "attempts" / expected_job_key / "result.json"
        if (
            attempt["candidate_id"] != state["candidate"]["id"]
            or attempt["session_id"] != review["session_id"]
            or attempt["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
            or receipt.stat().st_uid != os.getuid()
            or stat.S_IMODE(receipt.stat().st_mode) != 0o600
            or hashlib.sha256(receipt.read_bytes()).hexdigest()
            != recovery["review_receipt_sha256"]
        ):
            raise ValueError("second grant review receipt changed")
        raw = json.loads(receipt.read_text(encoding="utf-8"))
        saved = json.loads(attempt["result_json"] or "null")
        if (
            not isinstance(saved, dict)
            or raw != {key: value for key, value in saved.items()
                       if key not in {"cleanup", "container_id", "container_log_sha256"}}
            or any(review.get(key) != value for key, value in saved.items())
            or saved.get("container_id") != recovery["review_container_id"]
            or saved.get("container_log_sha256")
            != recovery["review_container_log_sha256"]
        ):
            raise ValueError("second grant review result changed")
        extra = self._amended_repair_intents(spec, recovery)
        if extra != recovery.get("additional_intents"):
            raise ValueError("second grant amended execution inventory changed")
        broker = DeliveryBroker(self, spec)
        if broker.candidate() != state["candidate"]:
            raise ValueError("second grant candidate changed after review")
        published_identity(broker, state["candidate"], state["pull_request"])
        role_sha = confirmed_amendment_lineage_cleanup(
            original, spec, scope["old_container_intents"], prior["role_intent"],
            additional_intents=extra,
        )
        if role_sha != prior["role_intent_sha256"]:
            raise ValueError("second grant changed the completed implementation container")

    @staticmethod
    def _amended_repair_intents(
        spec: dict[str, Any], recovery: dict[str, Any]
    ) -> dict[str, str]:
        """Bind only the amended preparation, prechecks and failed review."""
        state = recovery["state"]
        iteration = state["iteration"]
        precheck = state["checks"]["prepublish"]
        results = precheck["results"]
        configured = spec["policy"]["prepublish_checks"]
        if (
            not isinstance(results, list)
            or [item.get("id") for item in results]
            != [item.get("id") for item in configured]
            or len({item.get("id") for item in results}) != len(results)
        ):
            raise ValueError("amended precheck results differ from the frozen gate list")
        root = Path(spec["state_dir"])
        expected = {
            f"dependency-preparation-{spec['policy_digest']}/container-intent.json",
            f"attempts/{recovery['review_job_key']}/container/container-intent.json",
        }
        for result in results:
            key = f"prechecks/{iteration}/{result['id']}/container/container-intent.json"
            expected.add(key)
            log = root / f"prechecks/{iteration}/{result['id']}/container/container.log"
            identity = log.parent / "container-id.json"
            if (
                result.get("cleanup") != "confirmed"
                or result.get("passed") is not True
                or result.get("log") != str(log)
                or log.is_symlink()
                or not log.is_file()
                or log.stat().st_uid != os.getuid()
                or stat.S_IMODE(log.stat().st_mode) != 0o600
                or hashlib.sha256(log.read_bytes()).hexdigest() != result.get("log_sha256")
                or identity.is_symlink()
                or not identity.is_file()
                or json.loads(identity.read_text(encoding="utf-8")).get("container_id")
                != result.get("container_id")
            ):
                raise ValueError("amended precheck receipt or log changed")
        review_log = root / "attempts" / recovery["review_job_key"] / "container" / "container.log"
        review_identity = review_log.parent / "container-id.json"
        if (
            review_log.is_symlink()
            or not review_log.is_file()
            or review_log.stat().st_uid != os.getuid()
            or stat.S_IMODE(review_log.stat().st_mode) != 0o600
            or hashlib.sha256(review_log.read_bytes()).hexdigest()
            != recovery["review_container_log_sha256"]
            or review_identity.is_symlink()
            or not review_identity.is_file()
            or json.loads(review_identity.read_text(encoding="utf-8")).get("container_id")
            != recovery["review_container_id"]
        ):
            raise ValueError("amended review container receipt or log changed")
        observed: dict[str, str] = {}
        for relative in expected:
            path = root / relative
            try:
                info = path.lstat()
            except OSError as exc:
                raise ValueError("amended container intent is missing") from exc
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != os.getuid()
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise ValueError("amended container intent is not private")
            observed[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        return observed

    def _later_repair_intents(
        self, spec: dict[str, Any], recovery: dict[str, Any]
    ) -> dict[str, str]:
        """Seal the completed one- or two-turn interval since the prior grant."""
        prior = recovery["prior_recovery"]
        state = recovery["state"]
        first = prior["state"]["iteration"] + 1
        last = state["iteration"]
        if (
            last != prior["maximum_iteration"]
            or last - first + 1 != prior["additional_iterations"]
            or prior["additional_iterations"] not in (1, 2)
            or not isinstance(prior.get("additional_intents"), dict)
            or not isinstance(state.get("roles"), list)
        ):
            raise ValueError("later grant has no bounded earlier execution inventory")
        root = Path(spec["state_dir"])
        extra = dict(prior["additional_intents"])
        previous = prior["state"]["candidate"]
        checks = spec["policy"]["prepublish_checks"]
        with self._connect() as db:
            for iteration in range(first, last + 1):
                roles = [
                    item for item in state["roles"]
                    if item.get("iteration") == iteration
                ]
                if [item.get("role") for item in roles] != ["implement", "review"]:
                    raise ValueError("third grant has an unsealed intervening role")
                implementation, review = roles
                if (
                    implementation.get("input_candidate_id") != previous["id"]
                    or review.get("input_candidate_id")
                    != review.get("candidate", {}).get("id")
                    or review.get("candidate", {}).get("id") is None
                    or implementation.get("session_id") != recovery["session_id"]
                    or implementation.get("status") != "pass"
                    or review.get("status") != "findings"
                ):
                    raise ValueError("third grant changed an intervening review")
                for role in roles:
                    identity = {
                        "run_id": spec["run_id"], "role": role["role"],
                        "iteration": iteration,
                        "candidate_id": role["input_candidate_id"],
                        "policy_digest": spec["policy_digest"],
                    }
                    job_key = digest(identity)
                    attempt = db.execute(
                        "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                        (spec["run_id"], job_key),
                    ).fetchone()
                    receipt = root / "attempts" / job_key / "result.json"
                    log = receipt.parent / "container" / "container.log"
                    if (
                        attempt is None
                        or attempt["role"] != role["role"]
                        or attempt["iteration"] != iteration
                        or attempt["candidate_id"] != role["input_candidate_id"]
                        or attempt["session_id"] != role["session_id"]
                        or attempt["state"] != "finished"
                        or attempt["cleanup"] != "confirmed"
                        or attempt["result_path"] != str(receipt)
                        or receipt.is_symlink()
                        or not receipt.is_file()
                        or receipt.stat().st_uid != os.getuid()
                        or stat.S_IMODE(receipt.stat().st_mode) != 0o600
                        or log.is_symlink()
                        or not log.is_file()
                        or log.stat().st_uid != os.getuid()
                        or stat.S_IMODE(log.stat().st_mode) != 0o600
                        or hashlib.sha256(log.read_bytes()).hexdigest()
                        != role.get("container_log_sha256")
                    ):
                        raise ValueError("third grant role receipt or log changed")
                    raw = json.loads(receipt.read_text(encoding="utf-8"))
                    saved = json.loads(attempt["result_json"] or "null")
                    if (
                        not isinstance(saved, dict)
                        or raw != {
                            key: value for key, value in saved.items()
                            if key not in {"cleanup", "container_id", "container_log_sha256"}
                        }
                        or any(role.get(key) != value for key, value in saved.items())
                    ):
                        raise ValueError("third grant role result changed")
                    relative = f"attempts/{job_key}/container/container-intent.json"
                    intent = root / relative
                    if (
                        intent.is_symlink()
                        or not intent.is_file()
                        or intent.stat().st_uid != os.getuid()
                        or stat.S_IMODE(intent.stat().st_mode) != 0o600
                        or relative in extra
                    ):
                        raise ValueError("third grant role intent changed")
                    extra[relative] = hashlib.sha256(intent.read_bytes()).hexdigest()
                for check in checks:
                    relative = (
                        f"prechecks/{iteration}/{check['id']}/container/"
                        "container-intent.json"
                    )
                    intent = root / relative
                    if (
                        intent.is_symlink()
                        or not intent.is_file()
                        or intent.stat().st_uid != os.getuid()
                        or stat.S_IMODE(intent.stat().st_mode) != 0o600
                        or relative in extra
                    ):
                        raise ValueError("third grant precheck intent changed")
                    extra[relative] = hashlib.sha256(intent.read_bytes()).hexdigest()
                previous = review["candidate"]
        if previous != state["candidate"]:
            raise ValueError("third grant candidate or intent identity changed")
        return extra

    @staticmethod
    def _numbered_recovery_chain(
        recovery: dict[str, Any]
    ) -> tuple[dict[int, dict[str, Any]], dict[str, Any]]:
        """Normalize immutable numbered recoveries without projecting old ones anew."""
        number = recovery.get("grant_number")
        if type(number) is not int or number < 3:
            raise ValueError("later grant has no numbered predecessor")
        chain: dict[int, dict[str, Any]] = {}
        cursor: Any = recovery
        for expected in range(number, 1, -1):
            if (
                not isinstance(cursor, dict)
                or cursor.get("kind") != "repair_continuation"
                or cursor.get("grant_number") != expected
            ):
                raise ValueError("numbered repair ancestry skips or repeats a grant")
            chain[expected] = cursor
            cursor = cursor.get("prior_recovery")
        if not isinstance(cursor, dict) or cursor.get("kind") != "precheck_prelaunch_recovery":
            raise ValueError("numbered repair ancestry has no amended predecessor")
        return chain, cursor

    @staticmethod
    def _numbered_authority_rows(
        db: sqlite3.Connection, run_id: str, through: int
    ) -> tuple[dict[int, sqlite3.Row], sqlite3.Row, list[dict[str, Any]]]:
        """Read full legacy ancestry and the consecutive append-only successors."""
        tables = (
            (1, "delivery_repair_grants"),
            (2, "delivery_repair_grant_extensions"),
            (3, "delivery_repair_grant_thirds"),
        )
        rows = {
            number: db.execute(f"SELECT * FROM {table} WHERE run_id=?", (run_id,)).fetchone()
            for number, table in tables
        }
        amendment = db.execute(
            "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
        ).fetchone()
        successors = db.execute(
            "SELECT * FROM delivery_repair_grant_successors WHERE run_id=? ORDER BY grant_number",
            (run_id,),
        ).fetchall()
        if (
            amendment is None
            or any(rows[number] is None for number in range(1, min(through, 3) + 1))
            or rows[3] is not None and through < 3
            or [item["grant_number"] for item in successors]
            != list(range(4, through + 1))
        ):
            raise ValueError("numbered repair authority rows are missing or nonconsecutive")
        rows.update({item["grant_number"]: item for item in successors})
        vector = [
            {"source": "grant1", "sha256": digest(dict(rows[1]))},
            {"source": "scope_amendment", "sha256": digest(dict(amendment))},
            {"source": "grant2", "sha256": digest(dict(rows[2]))},
        ]
        for number in range(3, through + 1):
            vector.append({"source": f"grant{number}", "sha256": digest(dict(rows[number]))})
        return rows, amendment, vector

    def _later_repair_readback(
        self, spec: dict[str, Any], recovery: dict[str, Any], *, queued: bool
    ) -> None:
        """Reprove the numbered chain, failed review, and stopped owned work."""
        from temporalio.service import RPCError

        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            RepairReadbackPending,
            confirmed_amendment_lineage_cleanup,
            failed_gate_diagnostics,
            published_identity,
        )

        run_id = spec["run_id"]
        chain, amended = self._numbered_recovery_chain(recovery)
        number = recovery["grant_number"]
        prior = chain[number - 1]
        scope = amended.get("scope_recovery")
        state = recovery.get("state")
        original = self.spec(run_id)
        brief = recovery.get("operator_brief")
        prior_brief = prior.get("operator_brief") if number >= 4 else None
        checks = state.get("checks") if isinstance(state, dict) else None
        if (
            recovery.get("kind") != "repair_continuation"
            or not isinstance(scope, dict)
            or scope.get("kind") != "scope_amendment"
            or not isinstance(state, dict)
            or not isinstance(checks, dict)
            or self.effective_spec(run_id) != spec
            or recovery.get("effective_spec") != spec
            or prior.get("effective_spec") != spec
            or recovery.get("additional_iterations") not in (
                (2,) if number == 3 else (1, 2)
            )
            or recovery.get("maximum_iteration")
            != prior.get("maximum_iteration", -2) + recovery["additional_iterations"]
            or recovery.get("session_id") != prior.get("session_id")
            or recovery.get("candidate") != state.get("candidate")
            or recovery.get("pull_request") != state.get("pull_request")
            or recovery.get("grant_record_digest") != prior.get("grant_record_digest")
            or recovery.get("amendment_record_digest")
            != prior.get("amendment_record_digest")
            or not isinstance(brief, dict)
            or not isinstance(brief.get("criteria"), list)
            or not brief["criteria"]
            or digest(brief) != recovery.get("operator_brief_digest")
            or (number >= 4 and (
                not isinstance(prior_brief, dict)
                or not isinstance(prior_brief.get("criteria"), list)
                or brief["criteria"][:len(prior_brief["criteria"])]
                != prior_brief["criteria"]
            ))
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("cleanup") != "none"
            or state.get("error") != "repair limit exhausted"
            or state.get("iteration") != prior.get("maximum_iteration")
            or not isinstance(state.get("candidate"), dict)
            or state["candidate"].get("policy_digest") != spec["policy_digest"]
            or state["candidate"].get("head")
            != state.get("pull_request", {}).get("head")
            or checks.get("prepublish", {}).get("state") != "passed"
            or checks["prepublish"].get("source_unchanged") is not True
            or checks.get("review", {}).get("state") != "failed"
            or set(checks) != {"prepublish", "review"}
            or not isinstance(state.get("roles"), list)
            or len(state["roles"]) < 2
        ):
            raise ValueError("closed review is not eligible for a later grant")
        for ancestor_number in range(3, number + 1):
            current_recovery = chain[ancestor_number]
            predecessor = chain[ancestor_number - 1]
            current_pr = current_recovery.get("pull_request")
            predecessor_pr = predecessor.get("pull_request")
            if (
                current_recovery.get("state", {}).get("iteration")
                != predecessor.get("maximum_iteration")
                or current_recovery.get("effective_spec") != spec
                or current_recovery.get("session_id") != predecessor.get("session_id")
                or current_recovery.get("maximum_iteration")
                != predecessor.get("maximum_iteration", -2)
                + current_recovery.get("additional_iterations", -1)
                or not isinstance(current_pr, dict)
                or not isinstance(predecessor_pr, dict)
                or any(
                    current_pr.get(key) != predecessor_pr.get(key)
                    for key in ("number", "base", "url")
                )
            ):
                raise ValueError("numbered repair ancestry changed frozen authority")
        implementation, review = state["roles"][-2:]
        if (
            implementation.get("role") != "implement"
            or implementation.get("iteration") != state["iteration"]
            or implementation.get("status") != "pass"
            or implementation.get("finish_reason") != "done"
            or implementation.get("cleanup") != "confirmed"
            or implementation.get("session_id") != recovery["session_id"]
            or state["checks"]["prepublish"].get("candidate_id")
            != implementation.get("candidate", {}).get("id")
            or review.get("role") != "review"
            or review.get("iteration") != state["iteration"]
            or review.get("status") != "findings"
            or review.get("finish_reason") != "done"
            or review.get("cleanup") != "confirmed"
            or review.get("candidate") != state["candidate"]
            or review.get("session_id") == recovery["session_id"]
            or review.get("summary") != recovery.get("review_summary")
            or review.get("findings") != recovery.get("findings")
            or not review.get("findings")
            or state["checks"]["review"] != {
                "candidate_id": state["candidate"]["id"],
                "detail": review["summary"],
                "state": "failed",
            }
            or failed_gate_diagnostics(state, spec) != recovery["findings"]
        ):
            raise ValueError("third grant does not bind the failed independent review")
        try:
            closed = self._completed_temporal_result(
                run_id, workflow_id=recovery["predecessor_workflow_id"]
            )
        except ValueError as exc:
            if isinstance(exc.__cause__, (RPCError, TimeoutError, ConnectionError, OSError)):
                raise RepairReadbackPending("Temporal closure readback unavailable") from exc
            raise
        if (
            closed["workflow_id"] != recovery["predecessor_workflow_id"]
            or closed["execution_run_id"] != recovery["predecessor_execution_run_id"]
            or closed["request_digest"] != original["request_digest"]
            or closed["recovery_digest"] != digest(prior)
            or digest(closed["result"]) != recovery["predecessor_result_digest"]
            or closed["result"] != state
        ):
            raise ValueError("third grant predecessor is not a closed Temporal review")
        root = Path(spec["state_dir"])
        with self._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            rows, amendment, vector = self._numbered_authority_rows(
                db, run_id, number if queued else number - 1
            )
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        grant, second = rows[1], rows[2]
        if (
            row is None
            or row["request_json"] != canonical_json(
                self.submitted_spec(run_id)
                if original.get("preparation_version") == 1 else original
            )
            or row["recovery_json"] != canonical_json(recovery if queued else prior)
            or row["workflow_id"] != (
                f"delivery-{run_id}-repair-continuation-{number}"
                if queued else recovery["predecessor_workflow_id"]
            )
            or row["phase"] not in (
                {"repair_continuation_queued", "repair_preflight", "tracker_start", "repair"}
                if queued else {"blocked"}
            )
            or row["execution_state"] not in (
                {"queued", "running"} if queued else {"blocked"}
            )
            or (not queued and (
                row["outcome"] != "blocked"
                or row["cleanup"] != "none"
                or row["error"] != state["error"]
                or row["protocol_revision"] != state["revision"]
            ))
            or row["iteration"] != state["iteration"]
            or json.loads(row["candidate_json"] or "null") != state["candidate"]
            or json.loads(row["pr_json"] or "null") != state["pull_request"]
            or json.loads(row["checks_json"] or "{}") != state["checks"]
            or grant["maximum_iteration"] + 1 != amendment["maximum_iteration"]
            or amendment["original_policy_digest"] != original["policy_digest"]
            or amendment["effective_policy_digest"] != spec["policy_digest"]
            or json.loads(amendment["added_paths_json"]) != scope["added_paths"]
            or amendment["maximum_iteration"] != scope["maximum_iteration"]
            or vector[0]["sha256"] != recovery.get("grant_record_digest")
            or vector[1]["sha256"] != recovery.get("amendment_record_digest")
            or second["grant_number"] != 2
            or second["maximum_iteration"] != chain[2]["maximum_iteration"]
            or second["effective_policy_digest"] != spec["policy_digest"]
            or second["session_id"] != recovery["session_id"]
            or second["grant_record_digest"] != vector[0]["sha256"]
            or second["amendment_record_digest"] != vector[1]["sha256"]
            or claim is None
            or claim["owner"] != f"external:devflow:{run_id}"
            or any(item["state"] != "finished" or item["cleanup"] != "confirmed"
                   for item in attempts)
            or any(item["state"] != "complete" or item["observed_json"] is None
                   for item in effects)
        ):
            raise ValueError("later grant lost its frozen run, claim or authority")
        second_recovery = chain[2]
        if any(
            second[field] != second_recovery[recovery_field]
            for field, recovery_field in (
                ("predecessor_workflow_id", "predecessor_workflow_id"),
                ("predecessor_execution_run_id", "predecessor_execution_run_id"),
                ("predecessor_result_digest", "predecessor_result_digest"),
                ("review_job_key", "review_job_key"),
                ("review_receipt_sha256", "review_receipt_sha256"),
                ("candidate_id", "candidate_id"),
            ) if recovery_field in second_recovery
        ):
            raise ValueError("later grant lost its frozen run, claim or authority")
        if (
            second["candidate_id"] != second_recovery["candidate"]["id"]
            or second["pr_number"] != second_recovery["pull_request"]["number"]
            or second["pr_head"] != second_recovery["pull_request"]["head"]
            or second["granted_iterations"] != second_recovery["additional_iterations"]
        ):
            raise ValueError("later grant lost its frozen run, claim or authority")
        for ancestor_number in range(3, number + 1):
            ancestor = chain[ancestor_number]
            predecessor = chain[ancestor_number - 1]
            ancestor_row = rows.get(ancestor_number)
            prior_vector = vector[:ancestor_number]
            if (
                ancestor.get("grant_record_digest") != vector[0]["sha256"]
                or ancestor.get("amendment_record_digest") != vector[1]["sha256"]
                or (ancestor_number == 3 and
                    ancestor.get("prior_extension_digest") != vector[2]["sha256"])
                or (ancestor_number >= 4 and (
                    ancestor.get("ancestor_row_digests") != prior_vector
                    or ancestor.get("prior_grant_digest") != prior_vector[-1]["sha256"]
                ))
                or ancestor.get("candidate") != ancestor.get("state", {}).get("candidate")
                or ancestor.get("pull_request") != ancestor.get("state", {}).get("pull_request")
                or ancestor.get("state", {}).get("iteration") != predecessor["maximum_iteration"]
            ):
                raise ValueError("numbered grant ancestry changed")
            if ancestor_row is None:
                if queued or ancestor_number != number:
                    raise ValueError("numbered grant row is missing")
                continue
            if (
                ancestor_row["grant_number"] != ancestor_number
                or ancestor_row["predecessor_workflow_id"] != ancestor["predecessor_workflow_id"]
                or ancestor_row["predecessor_execution_run_id"]
                != ancestor["predecessor_execution_run_id"]
                or ancestor_row["predecessor_result_digest"]
                != ancestor["predecessor_result_digest"]
                or ancestor_row["review_job_key"] != ancestor["review_job_key"]
                or ancestor_row["review_receipt_sha256"]
                != ancestor["review_receipt_sha256"]
                or ancestor_row["effective_policy_digest"] != spec["policy_digest"]
                or ancestor_row["candidate_id"] != ancestor["candidate"]["id"]
                or ancestor_row["pr_number"] != ancestor["pull_request"]["number"]
                or ancestor_row["pr_head"] != ancestor["pull_request"]["head"]
                or ancestor_row["session_id"] != ancestor["session_id"]
                or ancestor_row["operator_brief_digest"]
                != ancestor["operator_brief_digest"]
                or ancestor_row["granted_iterations"] != ancestor["additional_iterations"]
                or ancestor_row["maximum_iteration"] != ancestor["maximum_iteration"]
                or (ancestor_number == 3 and (
                    ancestor_row["grant_record_digest"] != vector[0]["sha256"]
                    or ancestor_row["amendment_record_digest"] != vector[1]["sha256"]
                    or ancestor_row["prior_extension_digest"] != vector[2]["sha256"]
                ))
                or (ancestor_number >= 4 and (
                    ancestor_row["ancestor_row_digests_json"] != canonical_json(prior_vector)
                    or ancestor_row["prior_grant_digest"] != prior_vector[-1]["sha256"]
                    or ancestor_row["review_container_log_sha256"]
                    != ancestor["review_container_log_sha256"]
                ))
            ):
                raise ValueError("numbered grant row changed after authorization")
        expected_job_key = digest({
            "run_id": run_id, "role": "review", "iteration": state["iteration"],
            "candidate_id": state["candidate"]["id"],
            "policy_digest": spec["policy_digest"],
        })
        receipt = root / "attempts" / expected_job_key / "result.json"
        review_attempts = [
            item for item in attempts
            if item["role"] == "review" and item["iteration"] == state["iteration"]
        ]
        if (
            recovery.get("review_job_key") != expected_job_key
            or len(review_attempts) != 1
            or review_attempts[0]["job_key"] != expected_job_key
            or review_attempts[0]["candidate_id"] != state["candidate"]["id"]
            or review_attempts[0]["session_id"] != review["session_id"]
            or review_attempts[0]["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
            or receipt.stat().st_uid != os.getuid()
            or stat.S_IMODE(receipt.stat().st_mode) != 0o600
            or hashlib.sha256(receipt.read_bytes()).hexdigest()
            != recovery["review_receipt_sha256"]
        ):
            raise ValueError("third grant review receipt changed")
        raw = json.loads(receipt.read_text(encoding="utf-8"))
        saved = json.loads(review_attempts[0]["result_json"] or "null")
        if (
            not isinstance(saved, dict)
            or raw != {
                key: value for key, value in saved.items()
                if key not in {"cleanup", "container_id", "container_log_sha256"}
            }
            or any(review.get(key) != value for key, value in saved.items())
            or saved.get("container_id") != recovery["review_container_id"]
            or saved.get("container_log_sha256")
            != recovery["review_container_log_sha256"]
        ):
            raise ValueError("third grant review result changed")
        extra = self._later_repair_intents(spec, recovery)
        if extra != recovery.get("additional_intents"):
            raise ValueError("third grant amended execution inventory changed")
        latest = self._amended_repair_intents(spec, recovery)
        if any(extra.get(key) != value for key, value in latest.items()):
            raise ValueError("third grant latest precheck receipt changed")
        broker = DeliveryBroker(self, spec)
        if broker.candidate() != state["candidate"]:
            raise ValueError("third grant candidate changed after review")
        published_identity(broker, state["candidate"], state["pull_request"])
        earlier = amended
        role_sha = confirmed_amendment_lineage_cleanup(
            original, spec, scope["old_container_intents"], earlier["role_intent"],
            additional_intents=extra,
        )
        if role_sha != earlier["role_intent_sha256"]:
            raise ValueError("third grant changed the original amended role")

    def _continue_third_repair(
        self, run_id: str, supplied: dict[str, Any]
    ) -> dict[str, Any]:
        """Record one final two-iteration grant without changing grant two's row."""
        from .delivery_repair import failed_gate_diagnostics

        required = {
            "command_id", "grant_number", "expected_revision", "expected_iteration",
            "expected_candidate_id", "expected_pr_number", "expected_pr_head",
            "expected_session_id", "expected_policy_digest",
            "expected_execution_run_id", "expected_review_receipt_sha256",
            "expected_prior_grant_digest", "additional_iterations", "operator_brief",
        }
        brief = supplied.get("operator_brief")
        if (
            set(supplied) != required
            or not isinstance(supplied["command_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", supplied["command_id"])
            or type(supplied["grant_number"]) is not int
            or supplied["grant_number"] != 3
            or type(supplied["expected_revision"]) is not int
            or type(supplied["expected_iteration"]) is not int
            or type(supplied["expected_pr_number"]) is not int
            or type(supplied["additional_iterations"]) is not int
            or supplied["additional_iterations"] != 2
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
            or not isinstance(supplied["expected_session_id"], str)
            or not supplied["expected_session_id"]
            or not isinstance(supplied["expected_policy_digest"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_policy_digest"])
            or not isinstance(supplied["expected_execution_run_id"], str)
            or not supplied["expected_execution_run_id"]
            or not isinstance(supplied["expected_review_receipt_sha256"], str)
            or not re.fullmatch(
                r"[0-9a-f]{64}", supplied["expected_review_receipt_sha256"]
            )
            or not isinstance(supplied["expected_prior_grant_digest"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_prior_grant_digest"])
            or not isinstance(brief, dict)
            or set(brief) != {"label", "criteria"}
            or not isinstance(brief["label"], str)
            or not brief["label"].strip()
            or len(brief["label"]) > 120
            or not isinstance(brief["criteria"], list)
            or not 1 <= len(brief["criteria"]) <= 5
            or any(
                not isinstance(item, str) or not item.strip() or len(item) > 1200
                for item in brief["criteria"]
            )
        ):
            raise ValueError("third repair grant fields or operator brief are invalid")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            replay = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if replay:
                if replay["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(replay["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            second = db.execute(
                "SELECT * FROM delivery_repair_grant_extensions WHERE run_id=?", (run_id,)
            ).fetchone()
            attempts_snapshot = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_snapshot = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if row is None or grant is None or amendment is None or second is None:
            raise ValueError("third grant requires the full earlier authority chain")
        prior = json.loads(row["recovery_json"] or "null")
        if (
            not isinstance(prior, dict)
            or prior.get("kind") != "repair_continuation"
            or prior.get("grant_number") != 2
            or digest(dict(second)) != supplied["expected_prior_grant_digest"]
        ):
            raise ValueError("third grant does not bind the prior numbered grant")
        spec = self.effective_spec(run_id)
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        roles = state.get("roles") if isinstance(state, dict) else None
        review = roles[-1] if isinstance(roles, list) and roles else None
        candidate = state.get("candidate") if isinstance(state, dict) else None
        pr = state.get("pull_request") if isinstance(state, dict) else None
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != digest(prior)
            or closed["execution_run_id"] != supplied["expected_execution_run_id"]
            or not isinstance(state, dict)
            or state.get("revision") != supplied["expected_revision"]
            or state.get("iteration") != supplied["expected_iteration"]
            or state.get("iteration") != prior["maximum_iteration"]
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or spec["policy_digest"] != supplied["expected_policy_digest"]
            or prior.get("session_id") != supplied["expected_session_id"]
            or not isinstance(review, dict)
            or review.get("role") != "review"
            or review.get("iteration") != state["iteration"]
        ):
            raise ValueError("third grant does not match the closed reviewed candidate")
        review_job_key = digest({
            "run_id": run_id, "role": "review", "iteration": state["iteration"],
            "candidate_id": candidate["id"], "policy_digest": spec["policy_digest"],
        })
        with self._connect() as db:
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                (run_id, review_job_key),
            ).fetchone()
        receipt = Path(spec["state_dir"]) / "attempts" / review_job_key / "result.json"
        if (
            attempt is None
            or attempt["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
            or receipt.stat().st_uid != os.getuid()
            or stat.S_IMODE(receipt.stat().st_mode) != 0o600
            or hashlib.sha256(receipt.read_bytes()).hexdigest()
            != supplied["expected_review_receipt_sha256"]
        ):
            raise ValueError("third grant review receipt changed")
        saved = json.loads(attempt["result_json"] or "null")
        if not isinstance(saved, dict):
            raise ValueError("third grant review result is missing")
        findings = failed_gate_diagnostics(state, spec)
        if not findings or findings != review.get("findings"):
            raise ValueError("third grant has no sealed failed-gate diagnostics")
        recovery = {
            "kind": "repair_continuation", "grant_number": 3,
            "prior_recovery": prior, "effective_spec": spec,
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state, "candidate": candidate, "pull_request": pr,
            "session_id": prior["session_id"], "findings": findings,
            "review_summary": review["summary"],
            "review_job_key": review_job_key,
            "review_receipt_sha256": supplied["expected_review_receipt_sha256"],
            "review_container_id": saved.get("container_id"),
            "review_container_log_sha256": saved.get("container_log_sha256"),
            "grant_record_digest": digest(dict(grant)),
            "amendment_record_digest": digest(dict(amendment)),
            "prior_extension_digest": digest(dict(second)),
            "operator_brief": brief, "operator_brief_digest": digest(brief),
            "additional_iterations": 2, "maximum_iteration": state["iteration"] + 2,
        }
        recovery["additional_intents"] = self._later_repair_intents(spec, recovery)
        self._later_repair_readback(spec, recovery, queued=False)
        workflow_id = f"delivery-{run_id}-repair-continuation-3"
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_continuation_queued", "workflow_id": workflow_id,
            "grant_number": 3,
            "authorized_through_iteration": recovery["maximum_iteration"],
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if replay:
                if replay["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(replay["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts_now = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_now = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            grant_now = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            amendment_now = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            second_now = db.execute(
                "SELECT * FROM delivery_repair_grant_extensions WHERE run_id=?", (run_id,)
            ).fetchone()
            third = db.execute(
                "SELECT 1 FROM delivery_repair_grant_thirds WHERE run_id=?", (run_id,)
            ).fetchone()
            if (
                current is None
                or tuple(current) != tuple(row)
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or third is not None
                or grant_now is None
                or digest(dict(grant_now)) != recovery["grant_record_digest"]
                or amendment_now is None
                or digest(dict(amendment_now)) != recovery["amendment_record_digest"]
                or second_now is None
                or digest(dict(second_now)) != recovery["prior_extension_digest"]
                or [tuple(item) for item in attempts_now]
                != [tuple(item) for item in attempts_snapshot]
                or [tuple(item) for item in effects_now]
                != [tuple(item) for item in effects_snapshot]
            ):
                raise ValueError("third grant lost its frozen run or ownership")
            revision = current["revision"] + 1
            db.execute(
                """INSERT INTO delivery_repair_grant_thirds
                   (run_id,grant_number,command_id,predecessor_workflow_id,
                    predecessor_execution_run_id,predecessor_result_digest,
                    review_job_key,review_receipt_sha256,effective_policy_digest,
                    grant_record_digest,amendment_record_digest,prior_extension_digest,
                    candidate_id,pr_number,pr_head,session_id,operator_brief_digest,
                    granted_iterations,maximum_iteration,granted_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, 3, supplied["command_id"], closed["workflow_id"],
                    closed["execution_run_id"], digest(state), review_job_key,
                    recovery["review_receipt_sha256"], spec["policy_digest"],
                    recovery["grant_record_digest"], recovery["amendment_record_digest"],
                    recovery["prior_extension_digest"], candidate["id"], pr["number"],
                    pr["head"], recovery["session_id"], recovery["operator_brief_digest"],
                    2, recovery["maximum_iteration"], _now(),
                ),
            )
            db.execute(
                """UPDATE delivery_runs SET phase='repair_continuation_queued',
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
                db, run_id, revision, "repair_continuation_queued",
                "Explicit third bounded repair grant queued after independent review",
                {
                    "grant_number": 3, "iteration": state["iteration"],
                    "authorized_through_iteration": recovery["maximum_iteration"],
                    "candidate_id": candidate["id"], "pr_number": pr["number"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "diagnostics_digest": digest(findings),
                    "operator_brief_digest": recovery["operator_brief_digest"],
                    "prior_extension_digest": recovery["prior_extension_digest"],
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (supplied["command_id"], run_id, command_digest, canonical_json(response)),
            )
        return response

    def _continue_later_repair(
        self, run_id: str, supplied: dict[str, Any]
    ) -> dict[str, Any]:
        """Append one explicitly numbered successor using the shared grant proof."""
        from .delivery_repair import failed_gate_diagnostics

        required = {
            "command_id", "grant_number", "expected_revision", "expected_iteration",
            "expected_candidate_id", "expected_pr_number", "expected_pr_head",
            "expected_session_id", "expected_policy_digest",
            "expected_execution_run_id", "expected_review_receipt_sha256",
            "expected_prior_grant_digest", "additional_iterations", "operator_brief",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("later repair grant fields do not match the contract")
        brief = supplied["operator_brief"]
        number = supplied["grant_number"]
        increment = supplied["additional_iterations"]
        if (
            not isinstance(supplied["command_id"], str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", supplied["command_id"])
            or type(number) is not int or number < 4
            or type(increment) is not int or increment not in (1, 2)
            or any(type(supplied[key]) is not int or supplied[key] < 1 for key in (
                "expected_revision", "expected_pr_number"
            ))
            or type(supplied["expected_iteration"]) is not int
            or supplied["expected_iteration"] < 0
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
            or not isinstance(supplied["expected_session_id"], str)
            or not supplied["expected_session_id"]
            or not isinstance(supplied["expected_policy_digest"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_policy_digest"])
            or not isinstance(supplied["expected_execution_run_id"], str)
            or not supplied["expected_execution_run_id"]
            or any(
                not isinstance(supplied[key], str)
                or not re.fullmatch(r"[0-9a-f]{64}", supplied[key])
                for key in ("expected_review_receipt_sha256", "expected_prior_grant_digest")
            )
            or not isinstance(brief, dict)
            or set(brief) != {"label", "criteria"}
            or not isinstance(brief["label"], str)
            or not brief["label"].strip()
            or len(brief["label"]) > 120
            or not isinstance(brief["criteria"], list)
            or not 1 <= len(brief["criteria"]) <= 32
            or any(
                not isinstance(item, str) or not item.strip() or len(item) > 1200
                for item in brief["criteria"]
            )
        ):
            raise ValueError("later repair grant identity, increment or brief is invalid")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            replay = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if replay:
                if replay["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(replay["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            rows, _amendment, ancestry = self._numbered_authority_rows(db, run_id, number - 1)
            attempts_snapshot = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_snapshot = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if row is None:
            raise ValueError("run ID not found")
        prior = json.loads(row["recovery_json"] or "null")
        if (
            not isinstance(prior, dict)
            or prior.get("kind") != "repair_continuation"
            or prior.get("grant_number") != number - 1
            or ancestry[-1]["sha256"] != supplied["expected_prior_grant_digest"]
            or not isinstance(prior.get("operator_brief"), dict)
            or not isinstance(prior["operator_brief"].get("criteria"), list)
            or len(brief["criteria"]) <= len(prior["operator_brief"]["criteria"])
            or brief["criteria"][:len(prior["operator_brief"]["criteria"])]
            != prior["operator_brief"]["criteria"]
        ):
            raise ValueError("later grant does not bind the complete prior authority")
        spec = self.effective_spec(run_id)
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        roles = state.get("roles") if isinstance(state, dict) else None
        review = roles[-1] if isinstance(roles, list) and roles else None
        candidate = state.get("candidate") if isinstance(state, dict) else None
        pr = state.get("pull_request") if isinstance(state, dict) else None
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != digest(prior)
            or closed["execution_run_id"] != supplied["expected_execution_run_id"]
            or not isinstance(state, dict)
            or state.get("revision") != supplied["expected_revision"]
            or state.get("iteration") != supplied["expected_iteration"]
            or state.get("iteration") != prior["maximum_iteration"]
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or spec["policy_digest"] != supplied["expected_policy_digest"]
            or prior.get("session_id") != supplied["expected_session_id"]
            or not isinstance(review, dict)
            or review.get("role") != "review"
            or review.get("iteration") != state["iteration"]
        ):
            raise ValueError("later grant does not match the closed reviewed candidate")
        review_job_key = digest({
            "run_id": run_id, "role": "review", "iteration": state["iteration"],
            "candidate_id": candidate["id"], "policy_digest": spec["policy_digest"],
        })
        with self._connect() as db:
            attempt = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=? AND job_key=?",
                (run_id, review_job_key),
            ).fetchone()
        receipt = Path(spec["state_dir"]) / "attempts" / review_job_key / "result.json"
        if (
            attempt is None
            or attempt["result_path"] != str(receipt)
            or receipt.is_symlink()
            or not receipt.is_file()
            or receipt.stat().st_uid != os.getuid()
            or stat.S_IMODE(receipt.stat().st_mode) != 0o600
            or hashlib.sha256(receipt.read_bytes()).hexdigest()
            != supplied["expected_review_receipt_sha256"]
        ):
            raise ValueError("later grant review receipt changed")
        saved = json.loads(attempt["result_json"] or "null")
        if not isinstance(saved, dict):
            raise ValueError("later grant review result is missing")
        findings = failed_gate_diagnostics(state, spec)
        if not findings or findings != review.get("findings"):
            raise ValueError("later grant has no sealed failed-review diagnostics")
        recovery = {
            "kind": "repair_continuation", "grant_number": number,
            "prior_recovery": prior, "effective_spec": spec,
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state, "candidate": candidate, "pull_request": pr,
            "session_id": prior["session_id"], "findings": findings,
            "review_summary": review["summary"],
            "review_job_key": review_job_key,
            "review_receipt_sha256": supplied["expected_review_receipt_sha256"],
            "review_container_id": saved.get("container_id"),
            "review_container_log_sha256": saved.get("container_log_sha256"),
            "grant_record_digest": ancestry[0]["sha256"],
            "amendment_record_digest": ancestry[1]["sha256"],
            "ancestor_row_digests": ancestry,
            "prior_grant_digest": ancestry[-1]["sha256"],
            "operator_brief": brief, "operator_brief_digest": digest(brief),
            "additional_iterations": increment,
            "maximum_iteration": state["iteration"] + increment,
        }
        recovery["additional_intents"] = self._later_repair_intents(spec, recovery)
        self._later_repair_readback(spec, recovery, queued=False)
        workflow_id = f"delivery-{run_id}-repair-continuation-{number}"
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_continuation_queued", "workflow_id": workflow_id,
            "grant_number": number,
            "authorized_through_iteration": recovery["maximum_iteration"],
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            replay = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (supplied["command_id"],),
            ).fetchone()
            if replay:
                if replay["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(replay["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            _rows_now, _amendment_now, ancestry_now = self._numbered_authority_rows(
                db, run_id, number - 1
            )
            claim = self.state.claim_for(db, spec["work_id"])
            attempts_now = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects_now = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            if (
                current is None or tuple(current) != tuple(row)
                or ancestry_now != ancestry
                or claim is None or claim["owner"] != f"external:devflow:{run_id}"
                or [tuple(item) for item in attempts_now]
                != [tuple(item) for item in attempts_snapshot]
                or [tuple(item) for item in effects_now]
                != [tuple(item) for item in effects_snapshot]
            ):
                raise ValueError("later grant lost its frozen run or ownership")
            db.execute(
                """INSERT INTO delivery_repair_grant_successors
                   (run_id,grant_number,command_id,predecessor_workflow_id,
                    predecessor_execution_run_id,predecessor_result_digest,
                    review_job_key,review_receipt_sha256,review_container_log_sha256,
                    effective_policy_digest,ancestor_row_digests_json,prior_grant_digest,
                    candidate_id,pr_number,pr_head,session_id,operator_brief_digest,
                    granted_iterations,maximum_iteration,granted_at)
                   VALUES (:run_id,:grant_number,:command_id,:predecessor_workflow_id,
                    :predecessor_execution_run_id,:predecessor_result_digest,
                    :review_job_key,:review_receipt_sha256,:review_container_log_sha256,
                    :effective_policy_digest,:ancestor_row_digests_json,:prior_grant_digest,
                    :candidate_id,:pr_number,:pr_head,:session_id,:operator_brief_digest,
                    :granted_iterations,:maximum_iteration,:granted_at)""",
                {
                    "run_id": run_id, "grant_number": number,
                    "command_id": supplied["command_id"],
                    "predecessor_workflow_id": closed["workflow_id"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "predecessor_result_digest": digest(state),
                    "review_job_key": review_job_key,
                    "review_receipt_sha256": recovery["review_receipt_sha256"],
                    "review_container_log_sha256": recovery["review_container_log_sha256"],
                    "effective_policy_digest": spec["policy_digest"],
                    "ancestor_row_digests_json": canonical_json(ancestry),
                    "prior_grant_digest": ancestry[-1]["sha256"],
                    "candidate_id": candidate["id"], "pr_number": pr["number"],
                    "pr_head": pr["head"], "session_id": recovery["session_id"],
                    "operator_brief_digest": recovery["operator_brief_digest"],
                    "granted_iterations": increment,
                    "maximum_iteration": recovery["maximum_iteration"],
                    "granted_at": _now(),
                },
            )
            revision = current["revision"] + 1
            db.execute(
                """UPDATE delivery_runs SET phase='repair_continuation_queued',
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
                db, run_id, revision, "repair_continuation_queued",
                "Explicit numbered repair grant queued after independent review",
                {
                    "grant_number": number, "iteration": state["iteration"],
                    "authorized_through_iteration": recovery["maximum_iteration"],
                    "candidate_id": candidate["id"], "pr_number": pr["number"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "diagnostics_digest": digest(findings),
                    "operator_brief_digest": recovery["operator_brief_digest"],
                    "prior_grant_digest": ancestry[-1]["sha256"],
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (supplied["command_id"], run_id, command_digest, canonical_json(response)),
            )
        return response

    def retry_prelaunch(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Retry one proved no-process repair launch under its existing grant."""
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_container_cleanup,
            current_head_ci_evidence,
            published_identity,
        )

        required = {
            "command_id",
            "expected_revision",
            "expected_iteration",
            "expected_candidate_id",
            "expected_pr_number",
            "expected_pr_head",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("prelaunch retry fields do not match the contract")
        command_id = supplied["command_id"]
        if (
            not isinstance(command_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", command_id)
            or type(supplied["expected_revision"]) is not int
            or type(supplied["expected_iteration"]) is not int
            or type(supplied["expected_pr_number"]) is not int
            or supplied["expected_revision"] < 1
            or supplied["expected_iteration"] < 1
            or supplied["expected_pr_number"] < 1
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
        ):
            raise ValueError("invalid prelaunch retry identity")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
        if row is None or grant is None or not row["recovery_json"]:
            raise ValueError("prelaunch retry has no original repair grant")
        spec = self.spec(run_id)
        original = json.loads(row["recovery_json"])
        if (
            original.get("kind") != "repair_continuation"
            or digest(DeliveryConfig.load(self.config.path).raw) != spec["config_digest"]
        ):
            raise ValueError("prelaunch retry changed the frozen repair authority")
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        if not isinstance(state, dict):
            raise ValueError("closed prelaunch result is missing")
        candidate = state.get("candidate")
        pr = state.get("pull_request")
        roles = state.get("roles")
        iteration = state.get("iteration")
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"] != digest(original)
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("cleanup") != "none"
            or state.get("error") != "implementer did not establish a pass"
            or state.get("revision") != supplied["expected_revision"]
            or type(iteration) is not int
            or iteration != supplied["expected_iteration"]
            or iteration != grant["maximum_iteration"]
            or iteration != original.get("maximum_iteration")
            or iteration > spec["policy"]["max_repairs"] + 2
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or not isinstance(roles, list)
            or len(roles) < 2
            or state.get("checks") != {}
            or grant["predecessor_workflow_id"] != original.get("predecessor_workflow_id")
            or grant["predecessor_execution_run_id"]
            != original.get("predecessor_execution_run_id")
            or grant["predecessor_result_digest"] != digest(original.get("state"))
            or grant["granted_iterations"] != original.get("additional_iterations")
        ):
            raise ValueError("closed result does not authorize a prelaunch retry")
        failed, review = roles[-1], roles[-2]
        session = original.get("session_id")
        if (
            not isinstance(failed, dict)
            or failed.get("role") != "implement"
            or failed.get("iteration") != iteration
            or failed.get("status") != "blocked"
            or failed.get("finish_reason") != "prelaunch"
            or failed.get("cleanup") != "confirmed"
            or failed.get("session_id") is not None
            or not isinstance(review, dict)
            or review.get("role") != "review"
            or review.get("iteration") != iteration - 1
            or review.get("status") != "findings"
            or review.get("cleanup") != "confirmed"
            or not isinstance(review.get("findings"), list)
            or not review["findings"]
            or not isinstance(session, str)
            or not session
            or any(
                role.get("session_id") != session
                for role in roles
                if role.get("role") == "implement" and role.get("session_id")
            )
            or any(role.get("cleanup") != "confirmed" for role in roles)
        ):
            raise ValueError("failed role or sealed review findings cannot be retried")
        with self._connect() as db:
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
        failed_attempts = [
            item
            for item in attempts
            if item["role"] == "implement" and item["iteration"] == iteration
        ]
        if len(failed_attempts) != 1:
            raise ValueError("failed prelaunch attempt is ambiguous")
        failed_attempt = failed_attempts[0]
        receipt = json.loads(failed_attempt["result_json"] or "null")
        folder = Path(spec["state_dir"]) / "attempts" / failed_attempt["job_key"]
        try:
            metadata = folder.lstat()
        except OSError as exc:
            raise ValueError("failed prelaunch attempt directory is unavailable") from exc
        if (
            failed_attempt["state"] != "finished"
            or failed_attempt["cleanup"] != "confirmed"
            or failed_attempt["session_id"] is not None
            or failed_attempt["pid"] is not None
            or failed_attempt["process_identity"] is not None
            or failed_attempt["result_path"] != str(folder / "result.json")
            or not isinstance(receipt, dict)
            or receipt.get("status") != "blocked"
            or receipt.get("finish_reason") != "prelaunch"
            or receipt.get("session_id") is not None
            or receipt.get("cleanup") != "confirmed"
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != os.getuid()
            or any(folder.iterdir())
            or len(attempts) != len(roles)
            or any(
                item["state"] != "finished" or item["cleanup"] != "confirmed"
                for item in attempts
            )
        ):
            raise ValueError("prelaunch provider absence or cleanup is unproven")
        broker = DeliveryBroker(self, spec)
        observed_pr = published_identity(broker, candidate, pr)
        confirmed_container_cleanup(spec)
        ci_evidence = current_head_ci_evidence(broker, observed_pr)
        review_findings = list(review["findings"])
        findings = [*review_findings, *ci_evidence["diagnostics"]]
        workflow_id = f"delivery-{run_id}-repair-prelaunch-retry-1"
        recovery = {
            "kind": "repair_prelaunch_retry",
            "original_recovery": original,
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "failed_job_key": failed_attempt["job_key"],
            "state": state,
            "candidate": candidate,
            "pull_request": observed_pr,
            "session_id": session,
            "review_findings": review_findings,
            "ci_evidence": ci_evidence,
            "findings": findings,
            "maximum_iteration": iteration,
        }
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_prelaunch_retry_queued",
            "workflow_id": workflow_id,
            "authorized_through_iteration": iteration,
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            prior = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if prior:
                if prior["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(prior["response_json"])
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            current_grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            current_attempts = db.execute(
                "SELECT job_key,state,cleanup,result_json,session_id,pid,process_identity "
                "FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT kind,state,observed_json FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            if (
                current is None
                or current["request_json"] != row["request_json"]
                or current["workflow_id"] != row["workflow_id"]
                or current["recovery_json"] != row["recovery_json"]
                or current["phase"] != "blocked"
                or current["outcome"] != "blocked"
                or current["execution_state"] != "blocked"
                or current["cleanup"] != "none"
                or current["error"] != state["error"]
                or current["protocol_revision"] != state["revision"]
                or current["iteration"] != iteration
                or json.loads(current["candidate_json"] or "null") != candidate
                or json.loads(current["pr_json"] or "null") != pr
                or json.loads(current["checks_json"] or "{}") != state["checks"]
                or current_grant is None
                or dict(current_grant) != dict(grant)
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or len(current_attempts) != len(attempts)
                or sorted(tuple(item) for item in current_attempts)
                != sorted(
                    (
                        item["job_key"], item["state"], item["cleanup"], item["result_json"],
                        item["session_id"], item["pid"], item["process_identity"]
                    )
                    for item in attempts
                )
                or not any(item["kind"] == "publish" for item in effects)
                or any(
                    item["state"] != "complete" or item["observed_json"] is None
                    for item in effects
                )
                or any(folder.iterdir())
            ):
                raise ValueError("prelaunch retry lost its frozen run or ownership")
            revision = current["revision"] + 1
            db.execute(
                """UPDATE delivery_runs SET phase='repair_prelaunch_retry_queued',
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
                "repair_prelaunch_retry_queued",
                "Sealed no-process repair attempt queued under the existing grant",
                {
                    "iteration": iteration,
                    "candidate_id": candidate["id"],
                    "pr_number": pr["number"],
                    "failed_job_key": failed_attempt["job_key"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "diagnostics_digest": digest(findings),
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, command_digest, canonical_json(response)),
            )
        return response

    @staticmethod
    def _container_intent_inventory(spec: dict[str, Any]) -> dict[str, str]:
        root = Path(spec["state_dir"])
        return {
            str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(root.rglob("container-intent.json"))
        }

    def amend_scope(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Authorize one contained original-session turn for omitted test files."""
        from .delivery_broker import DeliveryBroker, _git
        from .delivery_repair import (
            confirmed_container_cleanup,
            current_head_ci_evidence,
            published_identity,
        )

        required = {
            "command_id", "expected_revision", "expected_iteration",
            "expected_candidate_id", "expected_pr_number", "expected_pr_head",
            "added_paths", "amended_config_path", "amended_config_sha256",
        }
        if not isinstance(supplied, dict) or set(supplied) != required:
            raise ValueError("scope amendment fields do not match the contract")
        command_id = supplied["command_id"]
        added = supplied["added_paths"]
        if (
            not isinstance(command_id, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", command_id)
            or type(supplied["expected_revision"]) is not int
            or type(supplied["expected_iteration"]) is not int
            or type(supplied["expected_pr_number"]) is not int
            or supplied["expected_revision"] < 1
            or supplied["expected_iteration"] < 0
            or supplied["expected_pr_number"] < 1
            or not isinstance(supplied["expected_candidate_id"], str)
            or not re.fullmatch(r"[0-9a-f]{64}", supplied["expected_candidate_id"])
            or not isinstance(supplied["expected_pr_head"], str)
            or not re.fullmatch(r"[0-9a-f]{40}", supplied["expected_pr_head"])
            or not isinstance(added, list)
            or not 1 <= len(added) <= 2
            or any(not isinstance(path, str) for path in added)
            or added != sorted(set(added))
            or any(
                Path(path).is_absolute()
                or not Path(path).parts
                or path != Path(path).as_posix()
                or any(part in {".", "..", ".git", ".codex"} for part in Path(path).parts)
                for path in added
            )
            or not isinstance(supplied["amended_config_path"], str)
            or not isinstance(supplied["amended_config_sha256"], str)
        ):
            raise ValueError("invalid scope amendment identity")
        command_digest = digest({"run_id": run_id, **supplied})
        with self._connect() as db:
            command = db.execute(
                "SELECT request_digest,response_json FROM delivery_commands WHERE command_id=?",
                (command_id,),
            ).fetchone()
            if command:
                if command["request_digest"] != command_digest:
                    raise ValueError("command ID already belongs to different inputs")
                return json.loads(command["response_json"])
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            amendment = db.execute(
                "SELECT 1 FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
            attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
        if row is None or grant is None or amendment:
            raise ValueError("scope amendment requires one exhausted original repair grant")
        original = self.spec(run_id)
        if row["accepted_plan_text"] is not None:
            original["accepted_plan"] = row["accepted_plan_text"]
        prior_recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        closed = self._completed_temporal_result(run_id, workflow_id=row["workflow_id"])
        state = closed["result"]
        roles = state.get("roles") if isinstance(state, dict) else None
        last = roles[-1] if isinstance(roles, list) and roles else None
        candidate = last.get("candidate") if isinstance(last, dict) else None
        pr = state.get("pull_request") if isinstance(state, dict) else None
        iteration = supplied["expected_iteration"]
        if (
            closed["workflow_id"] != row["workflow_id"]
            or closed["request_digest"] != row["request_digest"]
            or closed["recovery_digest"]
            != (digest(prior_recovery) if prior_recovery is not None else None)
            or state.get("run_id") != run_id
            or state.get("phase") != "blocked"
            or state.get("execution_state") != "blocked"
            or state.get("outcome") != "blocked"
            or state.get("cleanup") != "none"
            or state.get("error") != "implementer did not establish a pass"
            or state.get("checks") != {}
            or state.get("revision") != supplied["expected_revision"]
            or state.get("iteration") != iteration
            or iteration != grant["maximum_iteration"]
            or iteration != original["policy"]["max_repairs"] + 2
            or not isinstance(last, dict)
            or last.get("role") != "implement"
            or last.get("iteration") != iteration
            or last.get("status") != "findings"
            or last.get("finish_reason") != "done"
            or last.get("cleanup") != "confirmed"
            or not isinstance(last.get("session_id"), str)
            or not last["session_id"]
            or not isinstance(last.get("findings"), list)
            or not last["findings"]
            or not isinstance(candidate, dict)
            or candidate.get("id") != supplied["expected_candidate_id"]
            or not isinstance(pr, dict)
            or pr.get("number") != supplied["expected_pr_number"]
            or pr.get("head") != supplied["expected_pr_head"]
            or candidate.get("head") != pr["head"]
            or row["phase"] != "blocked"
            or row["execution_state"] != "blocked"
            or row["outcome"] != "blocked"
            or row["cleanup"] != "none"
            or row["error"] != state["error"]
            or row["protocol_revision"] != state["revision"]
            or row["iteration"] != iteration
            or json.loads(row["pr_json"] or "null") != pr
            or json.loads(row["checks_json"] or "{}") != {}
            or not attempts
            or any(item["state"] != "finished" or item["cleanup"] != "confirmed"
                   for item in attempts)
            or any(item["state"] != "complete" or item["observed_json"] is None
                   for item in effects)
            or not any(item["kind"] == "publish" for item in effects)
            or any(item["effect_key"].endswith(f":{iteration}") for item in effects)
        ):
            raise ValueError("closed implementation result cannot amend this scope")
        # A prelaunch retry leaves a finished generation-zero attempt beside
        # the generation-one provider result. Bind the closed Temporal tail
        # to the exact durable attempt rather than selecting by iteration.
        generation = (
            1 if prior_recovery and prior_recovery.get("kind") == "repair_prelaunch_retry"
            else 0
        )
        attempt_identity = {
            "run_id": run_id,
            "role": "implement",
            "iteration": iteration,
            "candidate_id": state["candidate"]["id"],
            "policy_digest": original["policy_digest"],
        }
        if generation:
            attempt_identity["attempt_generation"] = generation
        expected_job_key = hashlib.sha256(canonical_json(attempt_identity).encode()).hexdigest()
        attempt = next((item for item in attempts if item["job_key"] == expected_job_key), None)
        if attempt is None or attempt["session_id"] != last["session_id"]:
            raise ValueError("finished implementation session is not durable")
        if (
            attempt["role"] != "implement"
            or attempt["iteration"] != iteration
            or attempt["candidate_id"] != attempt_identity["candidate_id"]
        ):
            raise ValueError("finished implementation attempt identity changed")
        result_path = Path(attempt["result_path"] or "")
        expected_receipt = (
            Path(original["state_dir"]) / "attempts" / attempt["job_key"] / "result.json"
        )
        if result_path != expected_receipt:
            raise ValueError("finished implementation receipt left its owned attempt")
        info = result_path.lstat()
        result_bytes = result_path.read_bytes()
        raw_result = json.loads(result_bytes)
        saved_result = json.loads(attempt["result_json"] or "null")
        if (
            not result_path.is_absolute()
            or not stat.S_ISREG(info.st_mode)
            or info.st_uid != os.getuid()
            or stat.S_IMODE(info.st_mode) != 0o600
            or not isinstance(raw_result, dict)
            or not isinstance(saved_result, dict)
            or raw_result
            != {
                key: value for key, value in saved_result.items()
                if key not in {"cleanup", "container_id", "container_log_sha256"}
            }
            or any(saved_result.get(key) != last.get(key)
                   for key in ("status", "session_id", "cleanup", "finish_reason", "findings"))
        ):
            raise ValueError("finished implementation receipt changed")
        for folder in ("prechecks", "checks", "browser-qa", "gate-evidence", "gates"):
            if (Path(original["state_dir"]) / folder / str(iteration)).exists():
                raise ValueError("implementation already crossed a broker gate")
        original_broker = DeliveryBroker(self, original)
        if original_broker.candidate() != candidate:
            raise ValueError("post-role candidate changed after Temporal closure")
        changed = original_broker._changed_paths()
        if not changed or changed - set(original["policy"]["allowed_paths"]):
            raise ValueError("post-role edits exceeded the original file scope")
        for path in added:
            try:
                tracked = _git(
                    original_broker.checkout, "ls-files", "--error-unmatch", "--", path
                )
            except RuntimeError as exc:
                raise ValueError("added test path is not tracked") from exc
            target = original_broker.checkout / path
            if path in changed or tracked != path or not target.is_file() or target.is_symlink():
                raise ValueError("added test path was already changed or is not tracked")
            parent = target.parent
            while parent != original_broker.checkout:
                info = parent.lstat()
                if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
                    raise ValueError("added test path has an unsafe ancestor")
                parent = parent.parent
        observed_pr = published_identity(original_broker, candidate, pr)
        confirmed_container_cleanup(original)
        intents = self._container_intent_inventory(original)
        effective = scope_amended_spec(
            original, Path(supplied["amended_config_path"]),
            supplied["amended_config_sha256"], added,
        )
        amended_candidate = DeliveryBroker(self, effective).candidate()
        if any(amended_candidate.get(key) != candidate.get(key)
               for key in ("head", "content_sha256", "base_sha", "environment_digest", "id")):
            raise ValueError("amended policy changed the implementation bytes")
        ci_evidence = current_head_ci_evidence(original_broker, pr)
        findings = [*last["findings"][:8], *ci_evidence["diagnostics"]]
        if not findings:
            raise ValueError("scope amendment lacks sealed repair diagnostics")
        workflow_id = f"delivery-{run_id}-scope-amendment-1"
        recovery = {
            "kind": "scope_amendment",
            "predecessor_workflow_id": closed["workflow_id"],
            "predecessor_execution_run_id": closed["execution_run_id"],
            "predecessor_closed_at": closed["closed_at"],
            "predecessor_result_digest": digest(state),
            "state": state,
            "source_candidate": candidate,
            "amended_candidate": amended_candidate,
            "pull_request": observed_pr,
            "session_id": last["session_id"],
            "attempt_job_key": attempt["job_key"],
            "attempt_result_sha256": hashlib.sha256(result_bytes).hexdigest(),
            "old_container_intents": intents,
            "ci_evidence": ci_evidence,
            "findings": findings,
            "added_paths": added,
            "amended_config_path": effective["config_path"],
            "amended_config_sha256": supplied["amended_config_sha256"],
            "effective_spec": effective,
            "maximum_iteration": iteration + 1,
        }
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "scope_amendment_queued",
            "workflow_id": workflow_id,
            "authorized_through_iteration": iteration + 1,
            "existing": False,
        }
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            current = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            current_attempts = db.execute(
                "SELECT * FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            current_effects = db.execute(
                "SELECT * FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            claim = self.state.claim_for(db, original["work_id"])
            if (
                current is None
                or current["request_json"] != row["request_json"]
                or current["recovery_json"] != row["recovery_json"]
                or current["workflow_id"] != row["workflow_id"]
                or current["protocol_revision"] != state["revision"]
                or current["phase"] != "blocked"
                or current["outcome"] != "blocked"
                or current["cleanup"] != "none"
                or [tuple(item) for item in current_attempts]
                != [tuple(item) for item in attempts]
                or [tuple(item) for item in current_effects]
                != [tuple(item) for item in effects]
                or claim is None
                or claim["owner"] != f"external:devflow:{run_id}"
                or db.execute(
                    "SELECT 1 FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
                ).fetchone()
            ):
                raise ValueError("scope amendment lost its frozen run or claim")
            revision = current["revision"] + 1
            db.execute(
                """INSERT INTO delivery_scope_amendments VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    run_id, command_id, closed["workflow_id"], closed["execution_run_id"],
                    digest(state), attempt["job_key"], recovery["attempt_result_sha256"],
                    original["policy_digest"], effective["policy_digest"],
                    canonical_json(added), iteration + 1, _now(),
                ),
            )
            db.execute(
                """UPDATE delivery_runs SET phase='scope_amendment_queued',
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
                db, run_id, revision, "scope_amendment_queued",
                "Two-file-or-smaller scope amendment queued for one original-session repair",
                {
                    "added_paths": added,
                    "source_candidate_id": candidate["id"],
                    "effective_policy_digest": effective["policy_digest"],
                    "predecessor_execution_run_id": closed["execution_run_id"],
                    "authorized_through_iteration": iteration + 1,
                },
            )
            db.execute(
                "INSERT INTO delivery_commands VALUES (?,?,?,?)",
                (command_id, run_id, command_digest, canonical_json(response)),
            )
        return response

    def repair_preflight(self, spec: dict[str, Any], recovery: dict[str, Any]) -> None:
        """Recheck the frozen command before a resumed implementer can execute."""
        if (
            recovery.get("kind") == "repair_continuation"
            and type(recovery.get("grant_number")) is int
            and recovery["grant_number"] >= 3
        ):
            self._later_repair_readback(spec, recovery, queued=True)
            return
        if recovery.get("kind") == "repair_continuation" and recovery.get("grant_number") == 2:
            self._amended_repair_readback(spec, recovery, queued=True)
            return
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import confirmed_container_cleanup, published_identity

        run_id = spec["run_id"]
        retry = recovery.get("kind") == "repair_prelaunch_retry"
        original = recovery.get("original_recovery") if retry else recovery
        if not isinstance(original, dict) or original.get("kind") != "repair_continuation":
            raise ValueError("repair preflight has no original grant")
        expected_workflow = (
            f"delivery-{run_id}-repair-prelaunch-retry-1"
            if retry
            else f"delivery-{run_id}-repair-continuation-1"
        )
        with self._connect() as db:
            row = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (run_id,)).fetchone()
            grant = db.execute(
                "SELECT * FROM delivery_repair_grants WHERE run_id=?", (run_id,)
            ).fetchone()
            claim = self.state.claim_for(db, spec["work_id"])
            attempts = db.execute(
                "SELECT state,cleanup FROM delivery_attempts WHERE run_id=?", (run_id,)
            ).fetchall()
            effects = db.execute(
                "SELECT state FROM delivery_effects WHERE run_id=?", (run_id,)
            ).fetchall()
            failed_attempt = (
                db.execute(
                    "SELECT * FROM delivery_attempts WHERE job_key=? AND run_id=?",
                    (recovery.get("failed_job_key"), run_id),
                ).fetchone()
                if retry
                else None
            )
        if (
            row is None
            or grant is None
            or row["request_json"] != canonical_json(
                self.submitted_spec(run_id) if spec.get("preparation_version") == 1 else spec
            )
            or row["workflow_id"] != expected_workflow
            or row["recovery_json"] != canonical_json(recovery)
            or row["phase"]
            not in {
                "repair_continuation_queued",
                "repair_prelaunch_retry_queued",
                "repair_preflight",
                "tracker_start",
                "repair",
            }
            or row["execution_state"] not in {"queued", "running"}
            or grant["predecessor_workflow_id"] != original["predecessor_workflow_id"]
            or grant["predecessor_execution_run_id"]
            != original["predecessor_execution_run_id"]
            or grant["predecessor_result_digest"] != digest(original["state"])
            or grant["granted_iterations"] != original["additional_iterations"]
            or grant["maximum_iteration"] != original["maximum_iteration"]
            or grant["maximum_iteration"] != recovery["maximum_iteration"]
            or claim is None
            or claim["owner"] != f"external:devflow:{run_id}"
            or any(
                item["state"] != "finished" or item["cleanup"] != "confirmed"
                for item in attempts
            )
            or any(item["state"] != "complete" for item in effects)
        ):
            raise ValueError("repair grant or owned resources changed before resume")
        if retry:
            folder = Path(spec["state_dir"]) / "attempts" / recovery["failed_job_key"]
            try:
                metadata = folder.lstat()
            except OSError as exc:
                raise ValueError("failed prelaunch attempt directory is unavailable") from exc
            receipt = (
                json.loads(failed_attempt["result_json"] or "null")
                if failed_attempt
                else None
            )
            if (
                failed_attempt is None
                or failed_attempt["role"] != "implement"
                or failed_attempt["iteration"] != recovery["maximum_iteration"]
                or failed_attempt["state"] != "finished"
                or failed_attempt["cleanup"] != "confirmed"
                or failed_attempt["session_id"] is not None
                or failed_attempt["pid"] is not None
                or failed_attempt["process_identity"] is not None
                or not isinstance(receipt, dict)
                or receipt.get("finish_reason") != "prelaunch"
                or not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o700
                or any(folder.iterdir())
            ):
                raise ValueError("failed prelaunch attempt changed before retry")
        published_identity(
            DeliveryBroker(self, spec),
            recovery["candidate"],
            recovery["state"]["pull_request"],
        )
        confirmed_container_cleanup(spec)

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
        prior_authority = prior_spec["policy"]
        if prior_spec.get("preparation_version") == 1:
            # Admission compares configured authority; measured identities are
            # validated again when the successor prepares its own boundary.
            prior_authority = json.loads(previous["request_json"])["policy"]
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
            != continuation_authority(prior_authority)
            or previous["phase"] != "blocked"
            or previous["cleanup"] != "none"
            or previous["error"] != "implementer did not establish a pass"
            or previous["checks_json"] not in (None, "{}")
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
        implement_attempts = [item for item in attempts if item["role"] == "implement"]
        intake_attempts = [item for item in attempts if item["role"] == "intake"]
        if (
            len(implement_attempts) != 1
            or len(attempts) != 1 + len(intake_attempts)
            or (not prior_spec.get("intake_required") and intake_attempts)
            or (prior_spec.get("intake_required") and not intake_attempts)
        ):
            raise ValueError("continuation predecessor role inventory changed")
        attempt = implement_attempts[0]
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
        live_roles = live_result.get("roles") if isinstance(live_result, dict) else None
        if not isinstance(live_roles, list) or len(live_roles) != len(attempts):
            raise ValueError("continuation history differs from closed Temporal result")
        prior_intake_roles = live_roles[:-1]
        for intake_attempt, intake_role in zip(
            sorted(intake_attempts, key=lambda item: item["iteration"]),
            prior_intake_roles,
            strict=True,
        ):
            saved = (
                json.loads(intake_attempt["result_json"])
                if intake_attempt["result_json"] else None
            )
            if (
                intake_attempt["state"] != "finished"
                or intake_attempt["cleanup"] != "confirmed"
                or not intake_attempt["finished_at"]
                or not isinstance(saved, dict)
                or not isinstance(intake_role, dict)
                or intake_role.get("role") != "intake"
                or intake_role.get("iteration") != intake_attempt["iteration"]
                or intake_role.get("input_candidate_id") != intake_attempt["candidate_id"]
                or not isinstance(intake_role.get("candidate"), dict)
                or intake_role["candidate"].get("id") != intake_attempt["candidate_id"]
                or intake_role.get("session_id") != intake_attempt["session_id"]
                or any(intake_role.get(key) != value for key, value in saved.items())
            ):
                raise ValueError("continuation intake history differs from its receipt")
        if prior_spec.get("intake_required"):
            intake_row = db.execute(
                "SELECT intake_json FROM delivery_runs WHERE run_id=?", (old_id,)
            ).fetchone()
            intake = json.loads(intake_row[0]) if intake_row and intake_row[0] else None
            accepted = intake.get("accepted_plan") if isinstance(intake, dict) else None
            if (
                not isinstance(accepted, dict)
                or digest(accepted.get("content")) != accepted.get("digest")
                or json.dumps(accepted["content"], sort_keys=True, indent=2)
                != previous["accepted_plan_text"]
                or prior_spec["accepted_plan"] != previous["accepted_plan_text"]
            ):
                raise ValueError("continuation accepted intake plan changed")
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
            or live_roles[-1] != role
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
            return self._prepared_original(db, json.loads(row[0]))

    def submitted_spec(self, run_id: str) -> dict[str, Any]:
        """Read immutable admission input, including after preparation freezes."""

        with self._connect() as db:
            row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            return json.loads(row[0])

    @staticmethod
    def _prepared_original(db: sqlite3.Connection, original: dict[str, Any]) -> dict[str, Any]:
        if original.get("preparation_version") != 1:
            return original
        row = db.execute(
            "SELECT * FROM delivery_preparations WHERE run_id=?", (original["run_id"],)
        ).fetchone()
        if row is None:
            return original
        effective = json.loads(row["effective_spec_json"])
        if (
            row["submitted_spec_digest"] != digest(original)
            or row["effective_spec_digest"] != digest(effective)
        ):
            raise ValueError("durable preparation authority changed")
        return effective

    def prepared_spec(self, run_id: str) -> dict[str, Any] | None:
        original = self.submitted_spec(run_id)
        with self._connect() as db:
            effective = self._prepared_original(db, original)
        return effective if effective.get("preparation") else None

    def freeze_preparation(self, submitted: dict[str, Any], effective: dict[str, Any]) -> dict:
        """Atomically append one prepared authority without changing request/history."""

        from .delivery_preparation import verify_prepared_spec

        verify_prepared_spec(effective)
        unchanged = {key: value for key, value in effective.items()
                     if key not in {"policy", "policy_digest", "preparation"}}
        if unchanged != {key: value for key, value in submitted.items()
                         if key not in {"policy", "policy_digest"}}:
            raise ValueError("preparation changed the submitted run identity")
        measured_fields = {
            "container",
            "host_sandbox",
            "kit_revision",
            "environment_proof_sha256",
            "security_binding_sha256",
        }
        if submitted["policy"].get("execution_backend") == "native-macos":
            measured_fields.update({"native_identity", "codex_bin_sha256"})
        original_authority = {
            key: value for key, value in submitted["policy"].items() if key not in measured_fields
        }
        prepared_authority = {
            key: value for key, value in effective["policy"].items() if key not in measured_fields
        }
        if prepared_authority != original_authority:
            raise ValueError("preparation changed repository or execution authority")
        configured_container = submitted["policy"]["container"]
        prepared_container = effective["policy"]["container"]
        if submitted["policy"].get("execution_backend") != "native-macos":
            for key, default in (("memory", "2g"), ("cpus", "2"), ("pids_limit", 256)):
                if prepared_container.get(key) != configured_container.get(key, default):
                    raise ValueError("preparation changed configured container limits")
            if prepared_container.get("docker_bin") != configured_container.get("docker_bin"):
                raise ValueError("preparation changed the configured Docker executable")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (submitted["run_id"],)
            ).fetchone()
            if row is None or json.loads(row[0]) != submitted:
                raise ValueError("preparation input differs from the durable admission")
            frozen = self._prepared_original(db, submitted)
            if frozen.get("preparation"):
                if frozen != effective:
                    raise ValueError("preparation already froze a different effective result")
                return frozen
            db.execute(
                "INSERT INTO delivery_preparations VALUES (?,?,?,?,?)",
                (submitted["run_id"], digest(submitted), digest(effective),
                 canonical_json(effective), _now()),
            )
        return effective

    def preparation_progress(self, run_id: str, stage: str, message: str) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT revision,phase,outcome FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None or row["outcome"] is not None or row["phase"] != "preparing":
                return
            revision = row["revision"] + 1
            db.execute(
                "UPDATE delivery_runs SET revision=?,updated_at=? WHERE run_id=?",
                (revision, _now(), run_id),
            )
            self._event(db, run_id, revision, "preparation_" + stage, message, {"stage": stage})

    def intake_execution_spec(self, run_id: str) -> dict[str, Any]:
        """Read the frozen request with its single accepted intake plan."""

        with self._connect() as db:
            row = db.execute(
                "SELECT request_json,accepted_plan_text FROM delivery_runs WHERE run_id=?",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            spec = self._prepared_original(db, json.loads(row["request_json"]))
            if row["accepted_plan_text"] is not None:
                spec["accepted_plan"] = row["accepted_plan_text"]
            return spec

    def accept_intake_plan(
        self, run_id: str, revision: int, plan_digest: str, plan: dict[str, Any]
    ) -> dict[str, Any]:
        """Bind exactly the reviewed plan without changing repository authority."""

        if digest(plan) != plan_digest or type(revision) is not int:
            raise ValueError("accepted plan does not match the reviewed revision")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                """SELECT request_json,intake_json,accepted_plan_text
                   FROM delivery_runs WHERE run_id=?""",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            original = self._prepared_original(db, json.loads(row["request_json"]))
            if not original.get("intake_required"):
                raise ValueError("run was submitted with an accepted plan")
            intake = json.loads(row["intake_json"]) if row["intake_json"] else None
            if (
                not isinstance(intake, dict)
                or not intake.get("plans")
                or intake["plans"][-1].get("revision") != revision
                or intake["plans"][-1].get("digest") != plan_digest
                or intake["plans"][-1].get("content") != plan
            ):
                raise ValueError("plan changed before acceptance")
            accepted = json.dumps(plan, sort_keys=True, indent=2)
            if row["accepted_plan_text"] is not None:
                if row["accepted_plan_text"] != accepted:
                    raise ValueError("another plan was already accepted")
            else:
                intake["accepted_plan"] = {
                    "revision": revision, "digest": plan_digest, "content": plan
                }
                db.execute(
                    "UPDATE delivery_runs SET accepted_plan_text=?,intake_json=? WHERE run_id=?",
                    (accepted, canonical_json(intake), run_id),
                )
        return {**original, "accepted_plan": accepted}

    @staticmethod
    def _scope_recovery(recovery: dict[str, Any] | None) -> dict[str, Any] | None:
        """Resolve the sole amendment through explicitly numbered continuations."""
        if not isinstance(recovery, dict):
            return None
        expected = None
        while (
            isinstance(recovery, dict)
            and recovery.get("kind") == "repair_continuation"
            and type(recovery.get("grant_number")) is int
            and recovery["grant_number"] >= 2
        ):
            number = recovery["grant_number"]
            if expected is not None and number != expected:
                raise ValueError("numbered repair ancestry skips or repeats a grant")
            expected = number - 1
            recovery = recovery.get("prior_recovery")
        if expected is not None and expected != 1:
            raise ValueError("numbered repair ancestry is incomplete")
        if isinstance(recovery, dict) and recovery.get("kind") == "precheck_prelaunch_recovery":
            recovery = recovery.get("scope_recovery")
        if isinstance(recovery, dict) and recovery.get("kind") == "scope_amendment":
            return recovery
        return None

    def effective_spec(self, run_id: str) -> dict[str, Any]:
        """Read an explicit amended authority while preserving request_json."""

        with self._connect() as db:
            row = db.execute(
                """SELECT request_json,recovery_json,accepted_plan_text
                   FROM delivery_runs WHERE run_id=?""",
                (run_id,),
            ).fetchone()
            if row is None:
                raise ValueError("run ID not found")
            original = self._prepared_original(db, json.loads(row["request_json"]))
            if row["accepted_plan_text"] is not None:
                original["accepted_plan"] = row["accepted_plan_text"]
            recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
            scope = self._scope_recovery(recovery)
            if scope is None:
                return original
            amendment = db.execute(
                "SELECT * FROM delivery_scope_amendments WHERE run_id=?", (run_id,)
            ).fetchone()
        if (
            amendment is None
            or amendment["effective_policy_digest"]
            != scope["effective_spec"]["policy_digest"]
            or amendment["original_policy_digest"] != original["policy_digest"]
            or json.loads(amendment["added_paths_json"]) != scope["added_paths"]
            or amendment["maximum_iteration"] != scope["maximum_iteration"]
        ):
            raise ValueError("scope amendment authority is not durable")
        amended = scope_amendment_config(
            original,
            Path(scope["amended_config_path"]),
            scope["amended_config_sha256"],
            scope["added_paths"],
        )
        effective = scope["effective_spec"]
        if (
            not isinstance(effective, dict)
            or effective.get("config_digest") != digest(amended.raw)
            or effective.get("config_path") != str(amended.path)
            or effective.get("policy", {}).get("allowed_paths")
            != amended.raw["repositories"][original["repository_key"]]["allowed_paths"]
        ):
            raise ValueError("scope amendment effective authority changed")
        return effective

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
            if row[1] not in {
                "accepted",
                "publication_recovery_queued",
                "repair_continuation_queued",
                "repair_prelaunch_retry_queued",
                "scope_amendment_queued",
                "precheck_recovery_queued",
            }:
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
                else "repair"
                if row[1] in {
                    "repair_continuation_queued", "repair_prelaunch_retry_queued",
                    "scope_amendment_queued", "precheck_recovery_queued",
                }
                and accepted
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
        intake: dict[str, Any] | None = None,
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
                "intake_json": canonical_json(intake)
                if intake is not None else row["intake_json"],
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
            if (
                event_type == "cancelled"
                and outcome == "cancelled"
                and cleanup == "confirmed_after_role_boundary"
                and json.loads(row["request_json"]).get("intake_required")
                and row["accepted_plan_text"] is None
                and json.loads(values["intake_json"] or "{}").get("accepted_plan") is None
                and json.loads(values["tracker_json"] or "{}") == {}
                and values["pr_json"] is None
            ):
                attempts = db.execute(
                    "SELECT role,state,cleanup FROM delivery_attempts WHERE run_id=?", (run_id,)
                ).fetchall()
                effects = db.execute(
                    "SELECT kind,state FROM delivery_effects WHERE run_id=?", (run_id,)
                ).fetchall()
                if all(
                    attempt["role"] == "intake"
                    and attempt["state"] == "finished"
                    and attempt["cleanup"] == "confirmed"
                    for attempt in attempts
                ) and all(
                    effect["kind"] == "prepare" and effect["state"] == "complete"
                    for effect in effects
                ):
                    owner = f"external:devflow:{run_id}"
                    claim = self.state.claim_for(db, row["work_id"])
                    if claim is not None and claim["owner"] == owner:
                        self.state.release_work(db, row["work_id"], owner)
                        db.execute(
                            "UPDATE runtime_sessions SET closed_at=? "
                            "WHERE id=? AND closed_at IS NULL",
                            (self.state.now(), owner),
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
        spec = self.spec(run_id)
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        scope_recovery = self._scope_recovery(recovery)
        scope_amendment = (
            {
                "added_paths": scope_recovery["added_paths"],
                "original_policy_digest": spec["policy_digest"],
                "effective_policy_digest": scope_recovery["effective_spec"]["policy_digest"],
                "authorized_through_iteration": scope_recovery["maximum_iteration"],
                "predecessor_execution_run_id": scope_recovery["predecessor_execution_run_id"],
            }
            if scope_recovery and scope_recovery.get("kind") == "scope_amendment"
            else None
        )
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
                    "requested_model": result.get("requested_model"),
                    "requested_effort": result.get("requested_effort"),
                    "reported_model": result.get("reported_model"),
                    "usage": result.get("usage"),
                    "summary": result.get("summary"),
                    "findings": result.get("findings", []),
                    "cleanup": attempt["cleanup"],
                }
            )
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
        completion_types = tuple(sorted({completion for _, _, completion in gate_specs}))
        with self._connect() as db:
            active = db.execute(
                """SELECT COUNT(*) FROM delivery_attempts
                   WHERE state IN ('starting','running','unknown')"""
            ).fetchone()[0]
            # events() pages oldest-first for SSE replay; detail shows the newest activity.
            recent_event_rows = db.execute(
                """SELECT sequence,timestamp,type,message,run_revision,payload_json
                   FROM delivery_events WHERE run_id=?
                   ORDER BY sequence DESC LIMIT 200""",
                (run_id,),
            ).fetchall()
            # Gate evidence must include relevant events outside the display window.
            gate_event_rows = db.execute(
                f"""SELECT type,payload_json FROM delivery_events
                    WHERE run_id=? AND type IN ({','.join('?' for _ in completion_types)})""",
                (run_id, *completion_types),
            ).fetchall()
        events = [
            {**dict(event), "payload": json.loads(event["payload_json"]), "evidence_refs": []}
            for event in reversed(recent_event_rows)
        ]
        event_types = {event["type"] for event in gate_event_rows}
        current_event_types = {
            event["type"]
            for event in gate_event_rows
            if json.loads(event["payload_json"]).get("iteration") == row["iteration"]
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
            if name == "tracker":
                tracker_state = observed_gate_states[name]
                if tracker_state in {"failed", "blocked", "conflict"}:
                    return "failed"
                if tracker_state == "unknown":
                    return "unknown"
                return (
                    "completed"
                    if row["outcome"] == "delivered" and tracker_state == "consistent"
                    else "pending"
                )
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
                "policy_digest": candidate.get("policy_digest", spec["policy_digest"]),
            }
            if candidate
            else None,
            "pull_request": json.loads(row["pr_json"]) if row["pr_json"] else None,
            "scope_amendment": scope_amendment,
            "preparation": spec.get("preparation"),
            "checks": checks,
            "tracker": tracker,
            "usage": json.loads(row["usage_json"]) if row["usage_json"] else {},
            "decisions": [json.loads(row["decision_json"])]
            if row["decision_json"] and json.loads(row["decision_json"]) is not None
            else [],
            "intake": json.loads(row["intake_json"]) if row["intake_json"] else None,
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
