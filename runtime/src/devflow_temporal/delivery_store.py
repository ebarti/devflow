"""SQLite command/outbox, claim, and event projection for local deliveries."""

from __future__ import annotations

import asyncio
import base64
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
from .delivery_preparation import execution_retired


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
                """CREATE TABLE IF NOT EXISTS delivery_resource_closures (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT UNIQUE NOT NULL,
                    intent_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    response_json TEXT
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_technical_successors (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT UNIQUE NOT NULL,
                    intent_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    response_json TEXT
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
            mutation_columns = {
                row[1] for row in db.execute("PRAGMA table_info(delivery_mutations)")
            }
            for name, kind in (("decision_id", "TEXT"), ("decision_revision", "INTEGER")):
                if name not in mutation_columns:
                    db.execute(f"ALTER TABLE delivery_mutations ADD COLUMN {name} {kind}")
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_question_notifications (
                    notification_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL REFERENCES delivery_runs(run_id),
                    decision_id TEXT NOT NULL,
                    decision_revision INTEGER NOT NULL,
                    thread_id TEXT,
                    question_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    receipt_json TEXT,
                    updated_at TEXT NOT NULL,
                    UNIQUE (run_id,decision_id,decision_revision)
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_policy_recoveries (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT NOT NULL UNIQUE,
                    original_spec_digest TEXT NOT NULL,
                    recovery_digest TEXT NOT NULL,
                    maximum_iteration INTEGER NOT NULL
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
                """CREATE TABLE IF NOT EXISTS delivery_metadata_recoveries (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT NOT NULL UNIQUE,
                    command_digest TEXT NOT NULL,
                    grant_json TEXT NOT NULL,
                    state TEXT NOT NULL CHECK (state IN ('pending','queued'))
                )"""
            )
            db.execute(
                """CREATE TABLE IF NOT EXISTS delivery_gate_admissions (
                    run_id TEXT PRIMARY KEY REFERENCES delivery_runs(run_id),
                    command_id TEXT NOT NULL UNIQUE,
                    recovery_json TEXT NOT NULL
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

            from .delivery_dashboard import initialize

            initialize(db)

    def policy_recovery_precheck(self, run_id: str) -> dict:
        from .delivery_policy_recovery import precheck

        return precheck(self, run_id)[0]

    def recover_execution(self, run_id: str, supplied: dict) -> dict:
        from .delivery_policy_recovery import recover

        return recover(self, run_id, supplied)

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
            from .delivery_preparation import require_native_execution

            require_native_execution(self.spec(superseded))
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
                if "origin_thread_id" not in supplied and prior_spec.get("origin_thread_id"):
                    spec["origin_thread_id"] = prior_spec["origin_thread_id"]
                if spec.get("origin_thread_id") != prior_spec.get("origin_thread_id"):
                    raise ValueError("continuation changed the originating thread")
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
            from .delivery_dashboard import runtime_identity

            db.execute("INSERT INTO delivery_dashboard_state(run_id,identity_json) VALUES (?,?)",
                       (run_id, canonical_json(runtime_identity())))
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
        if isinstance(supplied, dict) and supplied.get('expected_pr_number') == 0:
            from .delivery_pending_publication import admit

            return admit(self, run_id, supplied)
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec(run_id))
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

    def metadata_preflight(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        from .delivery_metadata_recovery import reconcile

        return reconcile(self, run_id, supplied, preflight=True)

    def repair_admission_preflight(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        return self.continue_repair(run_id, supplied, preflight=True)

    def reconcile_published_metadata(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        from .delivery_metadata_recovery import reconcile

        return reconcile(self, run_id, supplied)

    def gates_only_preflight(self, run_id: str) -> dict[str, Any]:
        from .delivery_gates_admission import preflight

        return preflight(self, run_id)

    def admit_gates_only(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        from .delivery_gates_admission import admit

        return admit(self, run_id, supplied)


    def scope_preflight(self, spec: dict[str, Any], recovery: dict[str, Any]) -> None:
        """Recheck the sealed amendment before the sole enlarged-scope role."""
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import confirmed_native_cleanup, published_identity

        run_id = spec["run_id"]
        if recovery.get("kind") != "scope_amendment" or self.effective_spec(run_id) != spec:
            raise ValueError("scope amendment effective authority changed")
        admitted = scope_amended_spec(
            self.intake_execution_spec(run_id), Path(recovery["amended_config_path"]),
            recovery["amended_config_sha256"], recovery["added_paths"],
        )
        if admitted != spec:
            raise ValueError("scope amendment native identity or policy changed")
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
        if confirmed_native_cleanup(original) != recovery["native_cleanup_digest"]:
            raise ValueError("predecessor native ownership evidence changed")
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

    def continue_repair(
        self, run_id: str, supplied: dict[str, Any], *, preflight: bool = False,
    ) -> dict[str, Any]:
        """Spend one explicit, bounded grant on a closed failed gate of this run."""
        if (isinstance(supplied, dict)
                and supplied.get('continuation_kind') in {
                    'published_gate_retry', 'prepublication_gate_retry',
                    'published_check_prelaunch_retry', 'published_ci_retry'}):
            from .delivery_gate_retry import admit

            return admit(self, run_id, supplied, preflight=preflight)
        if (isinstance(supplied, dict)
                and supplied.get('continuation_kind') == 'abandon_pending_resource_closure'):
            from .delivery_resource_closure import abandon_pending

            return abandon_pending(self, run_id, supplied, preflight=preflight)
        if (isinstance(supplied, dict)
                and supplied.get('continuation_kind') == 'stopped_resource_closure'):
            from .delivery_resource_closure import admit

            return admit(self, run_id, supplied, preflight=preflight)
        if (isinstance(supplied, dict)
                and supplied.get('continuation_kind') == 'investigation_assessment_adjudication'):
            from .delivery_investigation_adjudication import admit

            return admit(self, run_id, supplied, preflight=preflight)
        if isinstance(supplied, dict) and 'continuation_kind' in supplied:
            from .delivery_technical_continuation import continue_technical

            return continue_technical(self, run_id, supplied, preflight=preflight)
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec(run_id))
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_native_cleanup,
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
        cause_specific = isinstance(supplied, dict) and set(supplied) == required | {
            "authority_path", "authority_sha256",
        }
        if not isinstance(supplied, dict) or (set(supplied) != required and not cause_specific):
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
        spec = self.effective_spec(run_id)
        finalized = (not cause_specific and spec.get("resource_cleanup_version") == 1
                     and row["cleanup"] == "confirmed")
        stopped_claim = cause_specific or finalized
        if digest(DeliveryConfig.load(Path(spec["config_path"])).raw) != spec["config_digest"]:
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
            or state.get("cleanup") not in ({"none", "confirmed"} if stopped_claim else {"none"})
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
        cleanup_digest = confirmed_native_cleanup(spec)
        title_constraint = None
        if cause_specific:
            from .delivery_title_repair import prepare

            title_constraint = prepare(
                self, spec, state, previous_recovery, supplied, implementation["session_id"],
            )
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
        if cause_specific:
            recovery.update(original_recovery=previous_recovery, effective_spec=spec,
                            title_constraint=title_constraint, cleanup_digest=cleanup_digest)
        response = {
            "run_id": run_id,
            "dashboard_url": f"{self.config.dashboard_url}/runs/{run_id}",
            "phase": "repair_continuation_queued",
            "workflow_id": workflow_id,
            "authorized_through_iteration": recovery["maximum_iteration"],
            "existing": False,
        }
        if finalized:
            recovery.update(original_recovery=previous_recovery, predecessor_spec=spec,
                            finalized_checkpoint=True, cleanup_digest=cleanup_digest)
        if preflight:
            from .delivery_policy_recovery import work_binding

            with self._connect() as db:
                work_binding(self, spec, db)
                claim = self.state.claim_for(db, spec["work_id"])
                if (claim is not None if stopped_claim else
                        claim is None or claim["owner"] != f"external:devflow:{run_id}"):
                    raise ValueError("repair preflight claim authority changed")
            return {**response, "preflight": True, "diagnostics": findings,
                    "title_constraint_sha256": (
                        digest(title_constraint) if title_constraint else None
                    )}
        if finalized:
            from .delivery_gate_retry import prepare_runtime
            from .delivery_metadata_recovery import _immutable, preserve_resources
            from .delivery_resources import private_directory

            root = Path(spec["state_dir"]) / "repair-continuation"
            private_directory(root)
            prepared = prepare_runtime(spec, root, command_digest, digest(recovery))
            # Resume the original implementation home; independent gates get new namespaces.
            original_home = self.spec(run_id).get("role_home_generation")
            if original_home is None:
                prepared.pop("role_home_generation", None)
            else:
                prepared["role_home_generation"] = original_home
            recovery["execution_spec"] = prepared
            recovery["execution_candidate"] = {**candidate,
                                                 "policy_digest": prepared["policy_digest"]}
            _immutable(root / "admission.json", recovery)
            preserve_resources(root, spec)
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
                or current["cleanup"] not in (
                    {"none", "confirmed"} if stopped_claim else {"none"}
                )
                or current["error"] != state["error"]
                or current["protocol_revision"] != state["revision"]
                or current["iteration"] != iteration
                or json.loads(current["candidate_json"] or "null") != candidate
                or json.loads(current["pr_json"] or "null") != pr
                or json.loads(current["checks_json"] or "{}") != checks
                or db.execute(
                    "SELECT 1 FROM delivery_repair_grants WHERE run_id=?", (run_id,)
                ).fetchone()
                or (claim is not None if stopped_claim else
                    claim is None or claim["owner"] != f"external:devflow:{run_id}")
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
            if stopped_claim:
                from .delivery_policy_recovery import work_binding

                work_binding(self, spec, db)
                self.state.claim_work(db, spec["work_id"], f"external:devflow:{run_id}",
                                      self.config.dashboard_url)
                work_binding(self, spec, db)
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


    def retry_prelaunch(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Retry one proved no-process repair launch under its existing grant."""
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec(run_id))
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import (
            confirmed_native_cleanup,
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
        confirmed_native_cleanup(spec)
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


    def amend_scope(self, run_id: str, supplied: dict[str, Any]) -> dict[str, Any]:
        """Authorize one native original-session turn for omitted test files."""
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec(run_id))
        from .delivery_broker import DeliveryBroker, _git
        from .delivery_repair import (
            confirmed_native_cleanup,
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
                if key not in {"cleanup", "process_cleanup", "resource_cleanup", "native_process"}
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
        cleanup_digest = confirmed_native_cleanup(original)
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
            "native_cleanup_digest": cleanup_digest,
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
        from .delivery_broker import DeliveryBroker
        from .delivery_repair import confirmed_native_cleanup, published_identity

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
        if original.get("finalized_checkpoint"):
            if (self.effective_spec(run_id) != spec
                    or original.get("execution_spec") != spec
                    or confirmed_native_cleanup(spec) != original["cleanup_digest"]):
                raise ValueError("finalized repair source or cleanup authority changed")
        if original.get("title_constraint"):
            from .delivery_policy_recovery import work_binding
            from .delivery_title_repair import validate_source

            if (canonical_json(self.effective_spec(run_id)) != canonical_json(spec)
                    or canonical_json(original["effective_spec"]) != canonical_json(spec)
                    or confirmed_native_cleanup(spec) != original["cleanup_digest"]):
                raise ValueError("title repair effective or resource authority changed")
            with self._connect() as db:
                work_binding(self, spec, db)
            validate_source(spec, original["title_constraint"], completed=False)
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
            recovery.get("execution_candidate", recovery["candidate"]),
            recovery["state"]["pull_request"],
        )
        confirmed_native_cleanup(spec)

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
            or (
                prior_spec.get("intake_required")
                and spec.get("plan_approval", "required")
                != prior_spec.get("plan_approval", "required")
            )
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
        accepted_intake_plan = None
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
            if prior_spec.get("plan_approval") == "automatic":
                if accepted.get("authorization") != {
                    "source": "run_authorization", "command_id": prior_spec["command_id"],
                    "request_digest": prior_spec["request_digest"],
                    "policy_digest": prior_spec["policy_digest"],
                    "authorized_endpoint": prior_spec["authorized_endpoint"],
                }:
                    raise ValueError("continuation accepted intake plan authorization changed")
                accepted_intake_plan = accepted
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
            **({"accepted_intake_plan": accepted_intake_plan} if accepted_intake_plan else {}),
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
            "host_sandbox", "native_identity", "codex_bin_sha256",
            "environment_proof_sha256", "security_binding_sha256",
        }
        original_authority = {
            key: value for key, value in submitted["policy"].items() if key not in measured_fields
        }
        prepared_authority = {
            key: value for key, value in effective["policy"].items() if key not in measured_fields
        }
        if prepared_authority != original_authority:
            raise ValueError("preparation changed repository or execution authority")
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
        self, run_id: str, revision: int, plan_digest: str, plan: dict[str, Any],
        *, authorization: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Bind the exact plan under human review or frozen run authorization."""

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
            automatic = original.get("plan_approval", "required") == "automatic"
            expected_authorization = {
                "source": "run_authorization", "command_id": original["command_id"],
                "request_digest": original["request_digest"],
                "policy_digest": original["policy_digest"],
                "authorized_endpoint": original["authorized_endpoint"],
            } if automatic else None
            if authorization != expected_authorization:
                raise ValueError("plan acceptance does not match frozen run authorization")
            intake = json.loads(row["intake_json"]) if row["intake_json"] else None
            if (
                not isinstance(intake, dict)
                or not intake.get("plans")
                or intake["plans"][-1].get("revision") != revision
                or intake["plans"][-1].get("digest") != plan_digest
                or intake["plans"][-1].get("content") != plan
            ):
                raise ValueError("plan changed before acceptance")
            if automatic and (
                intake["plans"][-1].get("authorization") != authorization
                or any(
                    question.get("state") != "answered" for question in intake.get("questions", [])
                )
            ):
                raise ValueError("plan authorization changed or clarification remains pending")
            acceptance = {"revision": revision, "digest": plan_digest, "content": plan}
            if automatic:
                acceptance["authorization"] = authorization
            accepted = json.dumps(plan, sort_keys=True, indent=2)
            if row["accepted_plan_text"] is not None:
                if row["accepted_plan_text"] != accepted or (
                    automatic and intake.get("accepted_plan") != acceptance
                ):
                    raise ValueError("another plan was already accepted")
            else:
                intake["accepted_plan"] = acceptance
                if automatic:
                    intake["plans"][-1]["state"] = "accepted"
                db.execute(
                    "UPDATE delivery_runs SET accepted_plan_text=?,intake_json=? WHERE run_id=?",
                    (accepted, canonical_json(intake), run_id),
                )
        return {**original, "accepted_plan": accepted}

    @staticmethod
    def _scope_recovery(recovery: dict[str, Any] | None) -> dict[str, Any] | None:
        """Read the sole native scope amendment without restoring retired execution."""
        while recovery and (recovery.get("kind") in {
            "terminal_tracker_recovery", "published_metadata_recovery", "investigation_gates_only",
            "accepted_technical_successor", "investigation_assessment_adjudication",
            "stopped_resource_closure",
        } or (recovery.get("kind") == "repair_continuation" and recovery.get("title_constraint"))):
            recovery = recovery["original_recovery"]
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
            if recovery and recovery.get('kind') == 'pending_publication_retry':
                from .delivery_pending_publication import effective_spec

                return effective_spec(self, original, recovery)
            if recovery and recovery.get('kind') == 'repair_continuation' and recovery.get(
                    'finalized_checkpoint'):
                from .delivery_gate_retry import effective_repair

                return effective_repair(self, recovery)
            if recovery and recovery.get('kind') in {
                    'published_gate_retry', 'prepublication_gate_retry',
                    'published_check_prelaunch_retry', 'published_ci_retry'}:
                from .delivery_gate_retry import effective_spec

                return effective_spec(self, original, recovery)
            renewals = []

            def renewed(spec):
                from .delivery_native_renewal import effective_spec as renewed_spec

                for renewal in reversed(renewals):
                    if renewal.get('kind') == 'stopped_resource_closure':
                        from .delivery_resource_closure import effective_spec as closure_spec

                        spec = closure_spec(self, spec, renewal)
                    elif renewal.get('kind') == 'accepted_technical_successor':
                        from .delivery_technical_continuation import (
                            effective_spec as technical_spec,
                        )

                        spec = technical_spec(self, spec, renewal)
                    else:
                        spec = renewed_spec(spec, renewal)
                return spec

            while recovery and recovery.get("kind") in {
                "terminal_tracker_recovery", "published_metadata_recovery",
                "investigation_gates_only", "accepted_technical_successor",
                "investigation_assessment_adjudication", "stopped_resource_closure",
            }:
                if recovery.get('kind') == 'stopped_resource_closure':
                    from .delivery_resource_closure import custody

                    custody(db, recovery)
                if recovery.get('kind') == 'investigation_assessment_adjudication':
                    from .delivery_investigation_adjudication import custody

                    custody(db, recovery)
                if recovery.get('native_preparation_renewal'):
                    renewals.append(recovery)
                recovery = recovery["original_recovery"]
            if (recovery and recovery.get("kind") == "repair_continuation"
                    and recovery.get("title_constraint")):
                recovery = recovery["original_recovery"]
                while recovery and recovery.get("kind") in {
                    "terminal_tracker_recovery", "published_metadata_recovery",
                    "investigation_gates_only", "accepted_technical_successor",
                    "investigation_assessment_adjudication", "stopped_resource_closure",
                }:
                    if recovery.get('kind') == 'stopped_resource_closure':
                        from .delivery_resource_closure import custody

                        custody(db, recovery)
                    if recovery.get('kind') == 'investigation_assessment_adjudication':
                        from .delivery_investigation_adjudication import custody

                        custody(db, recovery)
                    if recovery.get('native_preparation_renewal'):
                        renewals.append(recovery)
                    recovery = recovery["original_recovery"]
            if isinstance(recovery, dict) and recovery.get("kind") == "execution_policy_recovery":
                from .delivery_policy_recovery import effective_spec

                grant = db.execute(
                    "SELECT * FROM delivery_policy_recoveries WHERE run_id=?", (run_id,),
                ).fetchone()
                return renewed(effective_spec(self, original, recovery, grant))
            scope = self._scope_recovery(recovery)
            historical = (
                original.get("provider") == "codex"
                and original["policy"].get("execution_backend") != "native-macos"
            )
            if scope is None or historical:
                return renewed(original)
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
        return renewed(effective)

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
                "terminal_tracker_recovery_queued",
                "metadata_validation_queued",
                "gates_only_queued",
                "technical_successor_queued",
                "investigation_adjudication_queued",
                "resource_closure_queued",
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
                "waiting_tracker"
                if row[1] == "terminal_tracker_recovery_queued" and accepted
                else "metadata_validation"
                if row[1] == "metadata_validation_queued" and accepted
                else "gates_only"
                if row[1] == "gates_only_queued" and accepted
                else "resource_closure_preflight"
                if row[1] == "resource_closure_queued" and accepted
                else "adjudication_preflight"
                if row[1] == "investigation_adjudication_queued" and accepted
                else "technical_preflight"
                if row[1] == "technical_successor_queued" and accepted
                else "publishing"
                if row[1] == "publication_recovery_queued" and accepted
                else "repair"
                if row[1] in {
                    "repair_continuation_queued", "repair_prelaunch_retry_queued",
                    "scope_amendment_queued",
                }
                and accepted
                else "preparing"
                if accepted
                else row[1]
            )
            new_state = "waiting_tracker" if accepted and new_phase == "waiting_tracker" else (
                "running" if accepted else "pending_temporal"
            )
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
                "error": error if error is not None or event_type == "delivered"
                or (checks or {}).get("terminal_tracker_checkpoint", {}).get("state") == "confirmed"
                else row["error"],
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
            if event_type == "question_pending" and json.loads(row["request_json"]).get(
                "blocking_questions_version"
            ) == 1:
                from .delivery_questions import valid_blocker

                if not decision or decision.get("kind") != "question" or not valid_blocker(
                    decision.get("blocker")
                ):
                    raise ValueError("question notification requires a justified blocking decision")
                thread = json.loads(row["request_json"]).get("origin_thread_id")
                notification_id = digest({"run_id": run_id, "decision_id": decision["id"],
                                          "decision_revision": decision["revision"]})
                db.execute(
                    """INSERT OR IGNORE INTO delivery_question_notifications
                       VALUES (?,?,?,?,?,?,?,NULL,?)""",
                    (notification_id, run_id, decision["id"], decision["revision"], thread,
                     canonical_json(decision), "pending" if thread else "unavailable", _now()),
                )
                saved = db.execute(
                    "SELECT question_json,thread_id FROM delivery_question_notifications "
                    "WHERE notification_id=?", (notification_id,),
                ).fetchone()
                if (
                    saved["question_json"] != canonical_json(decision)
                    or saved["thread_id"] != thread
                ):
                    raise ValueError("blocking question notification identity changed")
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
        from .delivery_preparation import require_native_execution

        require_native_execution(self.spec(run_id))
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
                   (command_id,run_id,kind,request_digest,state,decision_id,decision_revision)
                   VALUES (?,?,?,?,'pending',?,?)""",
                (command_id, run_id, kind, request_digest,
                 payload.get("decision_id"), payload.get("decision_revision")),
            )
            return None

    @staticmethod
    def _question_is_current(db: sqlite3.Connection, item: dict) -> bool:
        run = db.execute("SELECT * FROM delivery_runs WHERE run_id=?", (item["run_id"],)).fetchone()
        spec = json.loads(run["request_json"]) if run else {}
        question = json.loads(item["question_json"])
        pending = json.loads(run["decision_json"] or "null") if run else None
        answered = db.execute(
            """SELECT 1 FROM delivery_mutations WHERE run_id=? AND state='complete'
               AND (kind='cancel' OR (kind='decision'
                    AND decision_id=? AND decision_revision=?))""",
            (item["run_id"], item["decision_id"], item["decision_revision"]),
        ).fetchone()
        return bool(
            run and run["outcome"] is None and run["phase"] == "waiting_question"
            and run["execution_state"] == "waiting" and pending == question
            and spec.get("blocking_questions_version") == 1
            and spec.get("origin_thread_id") == item["thread_id"]
            and not answered
        )

    @staticmethod
    def _question_command_inflight(db: sqlite3.Connection, item: dict) -> bool:
        return db.execute(
            """SELECT 1 FROM delivery_mutations WHERE run_id=? AND state IN ('pending','unknown')
               AND (kind='cancel' OR (kind='decision'
                    AND decision_id=? AND decision_revision=?))""",
            (item["run_id"], item["decision_id"], item["decision_revision"]),
        ).fetchone() is not None

    def _owns_question_notification(self, db: sqlite3.Connection, item: dict) -> bool:
        row = db.execute("SELECT request_json FROM delivery_runs WHERE run_id=?",
                         (item["run_id"],)).fetchone()
        spec = json.loads(row[0]) if row else {}
        return (
            spec.get("config_path") == str(self.config.path)
            and spec.get("state_dir") == str(self.config.state_root / "runs" / item["run_id"])
        )

    def _question_notification_state(
        self, db: sqlite3.Connection, notification_id: str, state: str, receipt: dict | None = None
    ) -> None:
        row = db.execute("SELECT run_id,state FROM delivery_question_notifications "
                         "WHERE notification_id=?", (notification_id,)).fetchone()
        if row is None or row["state"] == state:
            return
        db.execute("UPDATE delivery_question_notifications SET state=?,receipt_json=?,updated_at=? "
                   "WHERE notification_id=?", (state, canonical_json(receipt) if receipt else None,
                                              _now(), notification_id))
        db.execute("UPDATE delivery_runs SET revision=revision+1,updated_at=? WHERE run_id=?",
                   (_now(), row["run_id"]))
        revision = db.execute("SELECT revision FROM delivery_runs WHERE run_id=?",
                              (row["run_id"],)).fetchone()[0]
        self._event(db, row["run_id"], revision, "question_notification",
                    f"Blocking question callback {state}",
                    {"notification_id": notification_id, "state": state})

    def abandon_question_notifications(self) -> None:
        """Called only under exclusive sender ownership; abandoned sends are uncertain."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for row in db.execute("SELECT notification_id,run_id "
                                  "FROM delivery_question_notifications "
                                  "WHERE state='dispatching'").fetchall():
                if not self._owns_question_notification(db, dict(row)):
                    continue
                self._question_notification_state(
                    db, row[0], "unknown",
                    {"reason": "sender stopped before acknowledgement was recorded"},
                )

    def claim_question_notification(self) -> dict | None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            for row in db.execute(
                "SELECT * FROM delivery_question_notifications WHERE state='pending' "
                "ORDER BY updated_at,notification_id"
            ).fetchall():
                item = dict(row)
                if not self._owns_question_notification(db, item):
                    continue
                if self._question_command_inflight(db, item):
                    continue
                if not self._question_is_current(db, item):
                    self._question_notification_state(db, item["notification_id"], "suppressed")
                    continue
                self._question_notification_state(db, item["notification_id"], "dispatching")
                return item
        return None

    def question_notification_current(self, item: dict) -> bool:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM delivery_question_notifications "
                             "WHERE notification_id=?", (item["notification_id"],)).fetchone()
            if row is None or row[0] != "dispatching":
                return False
            if self._question_command_inflight(db, item):
                self._question_notification_state(db, item["notification_id"], "pending")
                return False
            if self._question_is_current(db, item):
                return True
            self._question_notification_state(db, item["notification_id"], "suppressed")
            return False

    def finish_question_notification(self, notification_id: str, state: str, receipt: dict) -> None:
        if state not in {"queued", "failed", "unknown"}:
            raise ValueError("invalid question notification result")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT state FROM delivery_question_notifications "
                             "WHERE notification_id=?", (notification_id,)).fetchone()
            if row is not None and row[0] == "dispatching":
                self._question_notification_state(db, notification_id, state, receipt)

    def question_notifications(self, run_id: str) -> list[dict]:
        with self._connect() as db:
            return [{**dict(row), "receipt": json.loads(row["receipt_json"] or "null")}
                    for row in db.execute("SELECT * FROM delivery_question_notifications "
                                          "WHERE run_id=? ORDER BY updated_at,notification_id",
                                          (run_id,))]

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

    def list_runs(self, archived: bool = False) -> list[dict[str, Any]]:
        with self._connect() as db:
            rows = db.execute(
                "SELECT r.* FROM delivery_runs r LEFT JOIN delivery_dashboard_state d "
                "ON r.run_id=d.run_id WHERE COALESCE(d.archived,0)=? ORDER BY r.updated_at DESC",
                (int(archived),),
            ).fetchall()
            return [self._compact(dict(row)) for row in rows]

    def _compact(self, row: dict[str, Any]) -> dict[str, Any]:
        spec = json.loads(row["request_json"])
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        technical = False
        while recovery and (recovery.get("kind") in {
            "terminal_tracker_recovery", "published_metadata_recovery", "investigation_gates_only",
            "accepted_technical_successor", "investigation_assessment_adjudication",
            "stopped_resource_closure",
        } or (recovery.get("kind") == "repair_continuation" and recovery.get("title_constraint"))):
            technical = technical or recovery.get("kind") == "accepted_technical_successor"
            recovery = recovery["original_recovery"]
        effective = (
            recovery.get("effective_spec", spec)
            if isinstance(recovery, dict)
            and recovery.get("kind") in {"scope_amendment", "execution_policy_recovery"}
            else spec
        )
        if not isinstance(effective, dict):
            effective = spec
        if technical:
            effective = self.effective_spec(row["run_id"])
        from .delivery_resources import projected_cleanup

        with self._connect() as db:
            from .delivery_dashboard import presentation

            dashboard_state = presentation(db, row["run_id"])
            active = db.execute(
                "SELECT COUNT(*) FROM delivery_attempts WHERE run_id=? "
                "AND (state!='finished' OR cleanup='unknown')", (row["run_id"],),
            ).fetchone()[0]
        cleanup = projected_cleanup(
            effective, json.loads(row["checks_json"] or "{}"), row["cleanup"],
            terminal=(not active and row["execution_state"] in {
                "terminal", "blocked", "cancelled", "waiting_tracker",
            }),
        )
        return {
            **dashboard_state,
            "id": row["run_id"],
            "run_id": row["run_id"],
            "work_id": row["work_id"],
            "title": spec["goal"].splitlines()[0][:120],
            "issue": row["issue_url"],
            "issue_url": row["issue_url"],
            "repository": row["repository_key"],
            "phase": row["phase"],
            "execution_state": row["execution_state"],
            "execution_retired": execution_retired(spec) or execution_retired(effective),
            "updated_at": row["updated_at"],
            # Public command revisions follow Temporal's deterministic
            # protocol. The SQLite projection has an independent sequence.
            "revision": row["protocol_revision"],
            "projection_revision": row["revision"],
            "protocol_revision": row["protocol_revision"],
            "iteration": row["iteration"],
            "authorized_endpoint": spec["authorized_endpoint"],
            "outcome": row["outcome"],
            "cleanup": cleanup,
            "cleanup_recorded": row["cleanup"],
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
        from .delivery_dashboard import steering_history, steering_open

        with self._connect() as db:
            steering = steering_history(db, run_id)
            can_steer = not compact["execution_retired"] and steering_open(db, row)
        spec = self.effective_spec(run_id)
        recovery = json.loads(row["recovery_json"]) if row["recovery_json"] else None
        adjudication = (recovery if recovery and recovery.get("kind")
                        == "investigation_assessment_adjudication" else None)
        terminal_recovery = (
            recovery if recovery and recovery.get("kind") == "terminal_tracker_recovery" else None
        )
        metadata_recovery = recovery if recovery and recovery.get("kind") == (
            "published_metadata_recovery"
        ) else None
        gates_admission = recovery if recovery and recovery.get("kind") == (
            "investigation_gates_only"
        ) else None
        technical_successor = None
        while recovery and (recovery.get("kind") in {
            "terminal_tracker_recovery", "published_metadata_recovery", "investigation_gates_only",
            "accepted_technical_successor", "investigation_assessment_adjudication",
            "stopped_resource_closure",
        } or (recovery.get("kind") == "repair_continuation" and recovery.get("title_constraint"))):
            if recovery.get("kind") == "accepted_technical_successor":
                technical_successor = recovery
            recovery = recovery["original_recovery"]
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
                    "attempt_id": attempt["job_key"],
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
            own_unconfirmed = db.execute(
                "SELECT COUNT(*) FROM delivery_attempts WHERE run_id=? "
                "AND (state!='finished' OR cleanup='unknown')", (run_id,),
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
        from .delivery_resources import projected_cleanup

        cleanup = projected_cleanup(
            spec, checks, row["cleanup"], terminal=(not own_unconfirmed
                                                 and row["execution_state"] in {
                "terminal", "blocked", "cancelled", "waiting_tracker",
            }),
        )
        return {
            **compact,
            "run": compact,
            "phase_gates": gates,
            "roles": roles,
            "capacity": {"limit": self.config.raw.get("capacity", 2), "active": active},
            "metadata_reconciliation": {
                "old_head": metadata_recovery["old_head"],
                "new_head": metadata_recovery["new_head"],
                "mapping_sha256": digest(metadata_recovery["mapping"]),
                "provider_turns": 0,
            } if metadata_recovery else None,
            "gates_only_admission": {
                "input_candidate_id": gates_admission["seal"]["input_candidate_id"],
                "after_candidate_id": gates_admission["candidate"]["id"],
                "iteration": gates_admission["seal"]["iteration"],
                "precheck_sha256": digest(gates_admission["seal"]),
                "implementation_authority": False,
            } if gates_admission else None,
            "queued": row["execution_state"] == "queued",
            "cleanup": "unknown"
            if row["cleanup"] == "unknown" or any(a["cleanup"] == "unknown" for a in attempts)
            else cleanup,
            "cleanup_recorded": row["cleanup"],
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
            "investigation_adjudication": ({
                "raw_status": "findings",
                "raw_findings": adjudication["authority"]["raw_qa"]["findings"],
                "disposition": adjudication["authority"]["accepted_disposition"],
                "authority_sha256": adjudication["command"]["authority_sha256"],
                "historical_runtime": adjudication["authority"]["original_runtime_sha"],
                "controller_source": adjudication["controller"]["source_revision"],
                "additional_native_execution": False,
            } if adjudication else None),
            "technical_successor": ({
                'intent_sha256': technical_successor['intent_sha256'],
                'resume_stage': technical_successor['resume_stage'],
                'maximum_iteration': technical_successor['maximum_iteration'],
                'additional_implementation_turns': 0,
                'native_preparation_renewal': technical_successor['native_preparation_renewal'],
                'integration': technical_successor['integration'],
            } if technical_successor else None),
            "terminal_tracker_recovery": {
                "reconciliation_only": True,
                "command_id": terminal_recovery["command_id"],
                "predecessor_workflow_id": terminal_recovery["closed"]["workflow_id"],
                "predecessor_execution_run_id": terminal_recovery["closed"]["execution_run_id"],
                "predecessor_status": terminal_recovery["closed"]["status"],
                "closed_history_sha256": terminal_recovery["closed"]["history_sha256"],
                "spec_sha256": terminal_recovery["spec_sha256"],
                "seal": terminal_recovery["seal"],
            } if terminal_recovery else None,
            "execution_policy_recovery": {
                "precheck_sha256": recovery["seal"]["precheck_sha256"],
                "original_mode": "native-profile", "effective_mode": "trusted-local",
                "predecessor_workflow_id": recovery["predecessor_workflow_id"],
                "predecessor_execution_run_id": recovery["predecessor_execution_run_id"],
                "session_id": recovery["session_id"],
                "authorized_through_iteration": recovery["maximum_iteration"],
                "preserved_checks": recovery["state"]["checks"],
                "preserved_error": recovery["state"]["error"],
                "preparation_history": recovery.get("preparation_history", []),
            } if recovery and recovery.get("kind") == "execution_policy_recovery" else None,
            "preparation": spec.get("preparation"),
            "checks": checks,
            "tracker": tracker,
            "steering": steering,
            "can_steer": can_steer,
            "usage": json.loads(row["usage_json"]) if row["usage_json"] else {},
            "decisions": [json.loads(row["decision_json"])]
            if row["decision_json"] and json.loads(row["decision_json"]) is not None
            else [],
            "intake": json.loads(row["intake_json"]) if row["intake_json"] else None,
            "question_notifications": self.question_notifications(run_id),
            "events": events,
            "error": row["error"],
        }

    def _evidence_items(self, run_id: str) -> list[dict[str, Any]]:
        spec = self.spec(run_id)
        root = Path(spec["state_dir"])
        indexed: list[dict[str, Any]] = []
        for namespace, names in (
            ("metadata-reconciliation", ("intent.json", "original-ref.json", "rewritten-ref.json",
                                          "publication.json")),
            ("gates-admission", ("admission.json",)),
            ("technical-successor", ("intent.json", "closure-intent.json",
                                      "closure-finalization.json", "closure.json",
                                      "integration.json",
                                      "admission.json")),
            ("technical-successor/native-generation", (
                "authority.json", "preparation.json", "generation.json", "proof.json",
                "measurement-path_control.json", "measurement-observed.json",
                "measurement-log.json",
            )),
            ("native-preparation-renewal", ("authority.json", "preparation.json",
                                             "generation.json", "proof.json",
                                             "measurement-path_control.json",
                                             "measurement-observed.json", "measurement-log.json")),
        ):
            for name in names:
                path = root / namespace / name
                if path.is_file() and not path.is_symlink():
                    indexed.append({"id": namespace + "-" + name.removesuffix(".json"),
                                    "label": namespace + ": " + name,
                                    "path": path, "limit": 20 * 1024 * 1024})
            for name in ("manifest.json", "finalization.json"):
                path = root / namespace / "predecessor-resources" / name
                if path.is_file() and not path.is_symlink():
                    indexed.append({"id": namespace + "-predecessor-" + name,
                                    "label": namespace + " original cleanup " + name,
                                    "path": path, "limit": 4 * 1024 * 1024})
        actor_root = root / "technical-successor/resume-actors"
        if actor_root.is_dir() and not actor_root.is_symlink():
            for path in sorted(actor_root.glob('*.json')):
                if path.is_file() and not path.is_symlink():
                    indexed.append({"id": "technical-resume-actor-" + path.stem,
                                    "label": "Technical controller resume observation",
                                    "path": path, "limit": 64 * 1024})
        for generation in ("native-preparation-renewal", "technical-successor/native-generation"):
            renewal_intent = root / generation / "preparation.json"
            if not renewal_intent.is_file() or renewal_intent.is_symlink():
                continue
            from .delivery_resources import read_private

            observed = read_private(renewal_intent)
            for index, attempt in enumerate(observed.get("preparation_attempts", [])[:2]):
                probe = root / generation / "probes" / str(index) / run_id
                if attempt.get("spec", {}).get("state_dir") != str(probe):
                    raise ValueError("native renewal evidence left its owned generation")
                for relative in ("resources/manifest.json", "resources/finalization.json",
                                 "native-preparation/trusted-local/observed.json",
                                 "native-preparation/trusted-local/path-control/process.log",
                                 "native-preparation/trusted-local/path-control/native-process.json",
                                 "native-preparation/trusted-local/trusted-local/process.log",
                                 "native-preparation/trusted-local/trusted-local/native-process.json"):
                    path = probe / relative
                    if path.is_file() and not path.is_symlink():
                        prefix = ("native-renewal" if generation == "native-preparation-renewal"
                                  else "technical-native")
                        indexed.append({"id": prefix + "-probe-" + str(index) + "-"
                                        + relative.replace("/", "-"),
                                        "label": "Native renewal probe " + str(index) + ": "
                                        + relative, "path": path, "limit": 4 * 1024 * 1024})
        policy_intent = root / "policy-recovery" / "intent.json"
        if policy_intent.is_file() and not policy_intent.is_symlink():
            indexed.append({"id": "execution-policy-recovery-intent",
                            "label": "Policy recovery preparation intent and retained failures",
                            "path": policy_intent, "limit": 2 * 1024 * 1024})
        with self._connect() as db:
            attempts = db.execute(
                """SELECT job_key,role,iteration,process_identity,result_json
                   FROM delivery_attempts WHERE run_id=? ORDER BY rowid""",
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
        role_ids: set[str] = set()
        for attempt in attempts:
            role_id = f"role-{attempt['role']}-{attempt['iteration']}"
            evidence_id = (role_id if role_id not in role_ids
                           else f"{role_id}-{attempt['job_key']}")
            role_ids.add(role_id)
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
                        "id": evidence_id,
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
        for stage in ("local", "prepublish"):
            for result in details.get("checks", {}).get(stage, {}).get("results", []):
                artifact = result.get("artifacts")
                if isinstance(artifact, dict):
                    from .delivery_check_evidence import verify_manifest

                    manifest = verify_manifest(artifact, artifact["candidate_id"], root)
                    for index, item in enumerate(manifest["artifacts"]):
                        indexed.append({
                            "id": f"artifact-{stage}-{details['iteration']}-{result['id']}-{index}",
                            "label": item["relative_path"], "path": Path(item["path"]),
                            "expected_sha256": item["sha256"], "limit": 50 * 1024 * 1024,
                        })
                    indexed.append({
                        "id": f"artifacts-{stage}-{details['iteration']}-{result['id']}",
                        "label": f"Synthetic artifacts: {result['id']}",
                        "path": Path(artifact["path"]),
                        "expected_sha256": artifact["sha256"], "limit": 4 * 1024 * 1024,
                    })
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
        current_browser = details.get("checks", {}).get("browser_qa", {})
        if isinstance(current_browser, dict):
            for field, label in (("receipt", "receipt"), ("log", "log")):
                path = Path(current_browser[field]) if current_browser.get(field) else None
                if (path is not None and current_browser.get(field + "_sha256")
                        and not any(item["path"] == path for item in indexed)):
                    indexed.append({"id": "current-browser-" + label,
                                    "label": "Current browser QA " + label,
                                    "path": path,
                                    "expected_sha256": current_browser[field + "_sha256"],
                                    "limit": 20 * 1024 * 1024})
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
            "text": data.decode("utf-8", errors="replace")
            if path.suffix not in {".png", ".pdf"}
            else "Binary synthetic artifact; use content_url",
            **({"base64": base64.b64encode(data).decode(),
                "media_type": "image/png" if path.suffix == ".png" else "application/pdf"}
               if path.suffix in {".png", ".pdf"} else {}),
            "content_url": f"/api/runs/{run_id}/evidence/{evidence_id}/content",
        }
