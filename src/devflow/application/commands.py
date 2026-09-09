"""Transaction coordinator. External mutations are returned as durable intents only."""

import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from devflow.adapters.sqlite_store import SQLiteStore
from devflow.domain.rules import blank, next_actions, transition
from devflow.errors import WorkflowError
from devflow.validation import canonical_json, digest, validate_record


class WorkflowService:
    def __init__(self, state_dir: Path):
        self.store = SQLiteStore(state_dir)

    def put_artifact(self, content: bytes):
        return self.store.put_artifact(content)

    def snapshot(self, work_id):
        return self.store.read(work_id)

    def next(self, work_id):
        state = self.snapshot(work_id)
        return {"work_id": work_id, "revision": state["revision"], "actions": next_actions(state)}

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
                    state, command, request, datetime.now(timezone.utc), dependencies
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
                "candidate_id": updated["candidate_id"],
                "actions": next_actions(updated),
                **details,
            }
            db.execute(
                "INSERT INTO operations VALUES (?,?,?)",
                (request["operation_id"], payload_hash, canonical_json(result)),
            )
        return result
