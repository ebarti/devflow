"""Small, restartable issue capture around gh, before an execution attempt exists.

Append-only operation facts use the existing SQLite schema so ongoing attempts
can retain their pinned runtime. GitHub owns the issue; this journal owns only
the request identity, dispatch uncertainty and observed issue identity.
"""

from __future__ import annotations

import json
import re
from contextlib import closing
from datetime import UTC, datetime

from devflow.adapters.github import GitHubRepository
from devflow.errors import WorkflowError
from devflow.execution import _action_lock
from devflow.validation import canonical_json, digest


def _key(repository, work_id):
    if not isinstance(work_id, str) or not work_id or len(work_id) > 256:
        raise WorkflowError("invalid_request", "A stable work_id of at most 256 characters is required")
    return "backlog:" + digest({"repository": repository, "work_id": work_id}) + ":"


def _read(store, key):
    with closing(store.connect()) as db:
        row = db.execute(
            "SELECT result FROM operations WHERE operation_id LIKE ? ORDER BY operation_id DESC LIMIT 1",
            (key + "%",),
        ).fetchone()
    return _decode(row[0]) if row else None


def _decode(payload):
    state = json.loads(payload)
    if state.get("kind") != "backlog_capture" or state.get("schema_version") != 1:
        raise WorkflowError("unsupported_capture", "Cannot interpret this saved backlog capture")
    return state


def _append(store, key, state, **changes):
    state = {
        **state, **changes,
        "sequence": state.get("sequence", 0) + 1,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    with store.transaction() as db:
        db.execute(
            "INSERT INTO operations VALUES (?,?,?)",
            (key + f"{state['sequence']:016d}", state["payload_hash"], canonical_json(state)),
        )
    return state


def list_captures(store, repository):
    """Discover recoverable requests without relying on a conversation or request file."""
    with closing(store.connect()) as db:
        rows = db.execute(
            "SELECT result FROM operations WHERE operation_id LIKE 'backlog:%' ORDER BY operation_id"
        ).fetchall()
    current = {}
    for row in rows:
        state = _decode(row[0])
        if state["repository"] == repository:
            current[state["work_id"]] = state
    return list(current.values())


def capture(store, repository, request, *, action="capture", github_factory=GitHubRepository):
    if not re.fullmatch(r"github:[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
        raise WorkflowError("unsupported_remote", "Backlog capture requires a bound GitHub repository")
    if action == "list":
        return {"captures": list_captures(store, repository)}
    if action not in {"capture", "show", "retry"}:
        raise WorkflowError("unknown_command", "Unknown backlog command")
    work_id = request.get("work_id")
    key = _key(repository, work_id)
    with _action_lock(store.root, key):
        state = _read(store, key)
        supplied = any(k in request for k in ("title", "body", "issue_number"))
        if supplied:
            title, body, number = request.get("title"), request.get("body"), request.get("issue_number")
            if number is not None and (type(number) is not int or number < 1):
                raise WorkflowError("invalid_request", "issue_number must be a positive integer")
            if number is None and (
                not isinstance(title, str) or not title.strip() or len(title) > 256
                or not isinstance(body, str) or not body.strip() or len(body) > 65000
                or "<!-- devflow-backlog:" in body
            ):
                raise WorkflowError("invalid_request", "Capture needs a short title and sanitized issue body")
            payload = {"title": title, "body": body, "issue_number": number}
            payload_hash = digest(payload)
            if state and state["payload_hash"] != payload_hash:
                raise WorkflowError("capture_conflict", "This work already has a different capture request; reuse it")
        elif not state:
            raise WorkflowError("unknown_capture", "No saved capture exists for this work")
        if not state:
            if action != "capture":
                raise WorkflowError("unknown_capture", "No saved capture exists for this work")
            state = _append(store, key, {
                "kind": "backlog_capture", "schema_version": 1,
                "repository": repository, "work_id": work_id,
                "payload": payload, "payload_hash": payload_hash, "status": "prepared",
                "capture_id": key.split(":")[1],
            })
        if action == "show" or state["status"] == "confirmed":
            return state
        if action == "retry":
            if state["status"] != "failed" or not state.get("no_mutation"):
                raise WorkflowError("reconcile_required", "Only a proven unsent/rejected capture can be retried")
            state = _append(store, key, state, status="prepared", no_mutation=True)
        elif state["status"] == "failed":
            raise WorkflowError("retry_required", "Correct the prerequisite, then use backlog retry")

        remote = github_factory(*repository[7:].split("/"))
        uncertain = state["status"] in {"dispatched", "ambiguous"}
        try:
            result = remote.reconcile_backlog_issue(
                state["capture_id"], issue_number=state["payload"]["issue_number"]
            )
            if result is None:
                if uncertain:
                    raise WorkflowError(
                        "ambiguous_backlog", "Issue creation is uncertain; reconcile later, never create a replacement"
                    )
                state = _append(store, key, state, status="dispatched", no_mutation=False)
                result = remote.create_backlog_issue(
                    state["capture_id"], title=state["payload"]["title"], body=state["payload"]["body"]
                )
        except WorkflowError as exc:
            no_mutation = not uncertain and (
                state["status"] == "prepared" or exc.details.get("no_mutation") is True
            )
            _append(
                store, key, state, status="failed" if no_mutation else "ambiguous",
                no_mutation=no_mutation, error_code=exc.code,
            )
            raise
        return _append(store, key, state, status="confirmed", issue=result, no_mutation=False)
