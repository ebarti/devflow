"""Transaction coordinator. External mutations are returned as durable intents only."""

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from devflow.adapters.sqlite_store import SQLiteStore
from devflow.admission import (
    BOOKKEEPING,
    TrustedIntakeVerifier,
    execution_admission,
    requested_admission,
)
from devflow.domain.rules import (
    PERMISSIONS,
    active,
    authority,
    blank,
    next_actions,
    transition,
    unblocked,
)
from devflow.errors import WorkflowError
from devflow.validation import canonical_json, digest, validate_record


class WorkflowService:
    def __init__(self, state_dir: Path, *, trusted_verifier: TrustedIntakeVerifier | None = None,
                 repository: str | None = None):
        self.store = SQLiteStore(state_dir)
        self.trusted_verifier = trusted_verifier
        self.repository = repository

    def preflight(self, command, request):
        if command in {"work.ready", "work.amend"}:
            return requested_admission(
                blank(request.get("work_id")), request, self.trusted_verifier,
                datetime.now(timezone.utc), repository=self.repository,
            )
        if not isinstance(request.get("work_id"), str) or not request["work_id"]:
            raise WorkflowError("invalid_request", "A nonempty work_id is required")
        return self.require_execution(self.snapshot(request["work_id"]), operation={
            "host.prepare": "create_tasks", "check.run": "check",
            "workspace.register": "edit", "candidate.capture": "edit",
        }.get(command))

    def require_execution(self, state, *, operation=None):
        return execution_admission(
            state, state.get("contract") or {}, state.get("admission_id"),
            self.trusted_verifier, datetime.now(timezone.utc),
            repository=self.repository,
            operation=operation,
        )

    def require_action_dispatch(self, state, action, expected_revision, *, reconcile=False):
        """Validate the current intent before external dispatch or native handoff."""
        if action["status"] == "invalidated":
            raise WorkflowError("stale_action", "Action was invalidated")
        if action["status"] == "failed":
            raise WorkflowError(
                "retry_required", "Use action retry to re-admit a definitely failed action"
            )
        if type(expected_revision) is not int or state["revision"] != expected_revision:
            raise WorkflowError("stale_revision", "Work changed before action dispatch")
        if action["payload"]["scope_hash"] != state["scope_hash"] or (
            action["payload"]["candidate_id"] != state["candidate_id"]
        ):
            raise WorkflowError("stale_action", "Action scope or candidate changed")
        if not reconcile:
            attempt = active(state)
            if attempt["status"] != "active" or action["attempt_id"] != attempt["attempt_id"]:
                raise WorkflowError("invalid_state", "Action needs its current active attempt")
            if action["status"] != "prepared":
                raise WorkflowError("reconcile_required", "Uncertain actions permit recovery only")
            unblocked(state)
            permission = PERMISSIONS[action["operation"]]
            self.require_execution(state, operation=permission)
            authority(state, datetime.now(timezone.utc), permission)

    def permitted_actions(self, state):
        actions = next_actions(state)
        if state["lifecycle"] in {"backlog", "done", "canceled"}:
            return actions
        try:
            self.require_execution(state)
        except WorkflowError as exc:
            # Reads still expose uncertain-action recovery without suggesting execution.
            return [a for a in actions if a["kind"] == "reconcile_action"
                    and a.get("action", {}).get("status") != "prepared"] + [
                {"kind": "request_user_action", "reason": exc.code}
            ]
        return actions

    def put_artifact(self, content: bytes):
        return self.store.put_artifact(content)

    def snapshot(self, work_id):
        return self.store.read(work_id)

    def list_works(self, repository):
        with closing(self.store.connect()) as db:
            rows = db.execute("SELECT state FROM works ORDER BY work_id").fetchall()
        states = [json.loads(row[0]) for row in rows]
        return [
            {key: state[key] for key in ("work_id", "revision", "lifecycle", "phase", "candidate_id")}
            for state in states
            if (state.get("authority") or {}).get("repository") == repository
        ]

    def next(self, work_id):
        state = self.snapshot(work_id)
        return {"work_id": work_id, "revision": state["revision"], "actions": self.permitted_actions(state)}

    def execute(self, command: str, request: dict):
        if command == "work.prepare":
            record = request.get("record", {})
            try:
                validate_record(record, "work_contract")
            except WorkflowError as exc:
                return {"ready": False, "missing": [str(exc)], "authority_required": True}
            return {"ready": False, "missing": [], "authority_required": True, "record": record}
        if command == "next":
            return self.next(request["work_id"])
        for key in ("operation_id", "work_id", "expected_revision"):
            if key not in request:
                raise WorkflowError("invalid_request", f"Missing {key}")
        if not isinstance(request["operation_id"], str) or not request["operation_id"]:
            raise WorkflowError("invalid_request", "operation_id must be nonempty")
        if type(request["expected_revision"]) is not int or request["expected_revision"] < 0:
            raise WorkflowError(
                "invalid_request", "expected_revision must be a nonnegative integer"
            )
        if command not in BOOKKEEPING:
            self.preflight(command, request)
        payload_hash = digest({"command": command, "request": request})
        with self.store.transaction() as db:
            operation = db.execute(
                "SELECT payload_hash,result FROM operations WHERE operation_id=?",
                (request["operation_id"],),
            ).fetchone()
            if operation:
                if operation[0] != payload_hash:
                    raise WorkflowError(
                        "operation_conflict",
                        "Operation ID was already used with a different payload",
                    )
                return json.loads(operation[1], parse_float=Decimal)
            row = db.execute(
                "SELECT state FROM works WHERE work_id=?", (request["work_id"],)
            ).fetchone()
            state = json.loads(row[0], parse_float=Decimal) if row else blank(request["work_id"])
            if state["revision"] != request["expected_revision"]:
                raise WorkflowError(
                    "stale_revision",
                    f"Expected revision {request['expected_revision']}; current revision {state['revision']}",
                )
            dependencies = {
                row[0]: json.loads(row[1], parse_float=Decimal)["lifecycle"]
                for row in db.execute("SELECT work_id,state FROM works")
            }
            try:
                updated, details = transition(
                    state, command, request, datetime.now(timezone.utc), dependencies,
                    trusted_verifier=self.trusted_verifier, repository=self.repository
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise WorkflowError(
                    "invalid_request", f"Malformed {command} request: {exc}"
                ) from exc
            # Historical evidence remains auditable, but its loss must not prevent cancellation
            # or a later defect report. Proof admissions recheck the current candidate's artifacts.
            proof_commands = {
                "gate.record",
                "fix.record",
                "deliver",
                "action.begin",
                "work.reconcile",
            }
            for key, record in updated["records"].items():
                if "artifact_hash" in record and (
                    key not in state["records"]
                    or (
                        command in proof_commands
                        and record.get("candidate_id") == updated["candidate_id"]
                    )
                ):
                    self.store.require_artifact(record["artifact_hash"])
            if command == "usage.record":
                usage = request["record"]
                known_work_ids = set(dependencies)
                if any(a["work_id"] not in known_work_ids for a in usage["allocations"]):
                    raise WorkflowError("unknown_work", "Usage allocation references unknown work")
                prior = db.execute(
                    "SELECT payload FROM records WHERE record_key=?",
                    (f"usage:{usage['response_id']}",),
                ).fetchall()
                if any(json.loads(row[0], parse_float=Decimal) != usage for row in prior):
                    raise WorkflowError(
                        "usage_conflict",
                        "Response identity already has a different portfolio record",
                    )
            self.store.persist_records(db, request["work_id"], updated["records"])
            if updated["lifecycle"] == "active":
                attempt = updated["attempt"]
                try:
                    db.execute(
                        "INSERT INTO claims VALUES (?,?,?,?) ON CONFLICT(work_id) DO NOTHING",
                        (
                            request["work_id"],
                            updated["authority"]["repository"],
                            attempt["attempt_id"],
                            attempt["host_id"],
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise WorkflowError(
                        "already_claimed", "This repository already has an active outcome"
                    ) from exc
                claim = db.execute(
                    "SELECT attempt_id,host_id FROM claims WHERE work_id=?", (request["work_id"],)
                ).fetchone()
                if tuple(claim) != (attempt["attempt_id"], attempt["host_id"]):
                    raise WorkflowError("already_claimed", "Another attempt or host owns the claim")
            else:
                db.execute("DELETE FROM claims WHERE work_id=?", (request["work_id"],))
            db.execute(
                "INSERT INTO works VALUES (?,?,?) ON CONFLICT(work_id) DO UPDATE SET revision=excluded.revision,state=excluded.state",
                (request["work_id"], updated["revision"], canonical_json(updated)),
            )
            result = {
                "work_id": request["work_id"],
                "revision": updated["revision"],
                "lifecycle": updated["lifecycle"],
                "phase": updated["phase"],
                "scope_hash": updated["scope_hash"],
                "admission_id": updated.get("admission_id"),
                "authority_id": (updated.get("authority") or {}).get("authority_id"),
                "candidate_id": updated["candidate_id"],
                "actions": self.permitted_actions(updated),
                **details,
            }
            db.execute(
                "INSERT INTO operations VALUES (?,?,?)",
                (request["operation_id"], payload_hash, canonical_json(result)),
            )
        return result
