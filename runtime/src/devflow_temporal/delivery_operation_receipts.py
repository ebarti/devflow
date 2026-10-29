"""Read exact existing domain receipts without reissuing their commands."""

from __future__ import annotations

import json

from .contracts import digest

_PHASES = {
    "recover-publication": {"publication_recovery_queued"},
    "admit-gates-only": {"gates_only_queued"},
    "continue-repair": {
        "repair_continuation_queued",
        "gates_retry_queued",
        "resource_closure_queued",
        "pending_resource_closure_abandoned",
    },
    "retry-prelaunch": {"repair_prelaunch_retry_queued"},
    "amend-scope": {"scope_amendment_queued"},
}


def receipt(store, operation):
    kind, run_id, payload = operation["kind"], operation["run_id"], operation["request"]
    command_id = payload.get("command_id")
    if not isinstance(command_id, str):
        return None
    if kind in {"pause", "resume"}:
        # A stored acknowledgement is historical evidence of intent, not a
        # fresh observation that settlement or activation has completed.
        keys = {"command_id", "expected_revision"} | ({"epoch"} if kind == "resume" else set())
        revision = payload.get("expected_revision")
        epoch = payload.get("epoch")
        if (set(payload) != keys or not command_id or len(command_id) > 512
                or operation.get("domain_command_id") != command_id
                or operation.get("request_digest") != digest({
                    "run_id": run_id, "kind": kind, "request": payload})
                or type(revision) is not int or revision < 0
                or (kind == "resume" and (type(epoch) is not int or epoch < 1))):
            return None
        expected = {"action": kind, "root_id": run_id, "expected_revision": revision}
        if kind == "resume":
            expected["epoch"] = epoch
        with store._connect() as db:
            row = db.execute(
                "SELECT root_id,request_digest,response_json FROM delivery_pause_commands "
                "WHERE command_id=?", (command_id,),
            ).fetchone()
        if row is None or row["root_id"] != run_id or row["request_digest"] != digest(expected):
            return None
        response = json.loads(row["response_json"])
        state = "pause_requested" if kind == "pause" else "resuming"
        if (not isinstance(response, dict)
                or set(response) != {"root_id", "state", "revision", "epoch"}
                or response["root_id"] != run_id or response["state"] != state
                or type(response["revision"]) is not int or response["revision"] != revision + 1
                or type(response["epoch"]) is not int or response["epoch"] < 1
                or (kind == "resume" and response["epoch"] != epoch)):
            return None
        return response
    if kind == "issue-context":
        if set(payload) != {"command_id"}:
            return None
        with store._connect() as db:
            row = db.execute(
                "SELECT * FROM delivery_issue_context WHERE command_id=? AND run_id=?",
                (command_id, run_id),
            ).fetchone()
        if row is None:
            return None
        from .delivery_issue_context import _restore

        context = _restore(row, store.effective_spec(run_id))
        return {
            "run_id": run_id,
            "context_sha256": digest(context),
            "fetched_at": context["fetched_at"],
            "issue_url": context["issue"]["url"],
            "evidence_count": len(context["evidence"]),
            "unavailable": context["unavailable"],
        }
    if kind not in _PHASES and kind not in {"revise-feature-plan", "continue-feature"}:
        return None
    # These domain digests merge run_id into the payload. An extra caller
    # run_id could otherwise collide with a valid receipt despite invalid shape.
    if "run_id" in payload:
        return None
    with store._connect() as db:
        row = db.execute(
            "SELECT * FROM delivery_commands WHERE command_id=?", (command_id,)
        ).fetchone()
        if row is None:
            return None
        response = json.loads(row["response_json"])
        if not isinstance(response, dict):
            return None
        if kind == "continue-feature":
            successor = "run-feature-" + digest({"run": run_id, "command": payload})[:20]
            if row["run_id"] != successor or response.get("run_id") != successor:
                return None
            successor_row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (successor,)
            ).fetchone()
            if successor_row is None:
                return None
            predecessor_row = db.execute(
                "SELECT request_json FROM delivery_runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if predecessor_row is None:
                return None
            predecessor = (
                json.loads(predecessor_row["request_json"])
                .get("feature_delivery", {})
                .get("owner", {})
            )
            successor_owner = (
                json.loads(successor_row["request_json"])
                .get("feature_delivery", {})
                .get("owner", {})
            )
            if (
                not isinstance(predecessor, dict)
                or not isinstance(successor_owner, dict)
                or predecessor.get("run_id") != run_id
                or successor_owner.get("run_id") != successor
                or not predecessor.get("issue_id")
                or not predecessor.get("store_path")
                or successor_owner.get("issue_id") != predecessor.get("issue_id")
                or not successor_owner.get("store_path")
                or type(predecessor.get("generation")) is not int
                or successor_owner.get("generation") != predecessor["generation"] + 1
            ):
                return None
            return response
        if row["run_id"] != run_id:
            return None
        expected = (
            digest({"run_id": run_id, "request": payload})
            if kind == "revise-feature-plan"
            else digest({"run_id": run_id, **payload})
        )
        if row["request_digest"] != expected:
            return None
        if kind == "revise-feature-plan":
            if (
                response.get("predecessor_run_id") != run_id
                or response.get("revision_phase") != "requested"
            ):
                return None
        elif response.get("run_id") != run_id or response.get("phase") not in _PHASES[kind]:
            return None
        return response
