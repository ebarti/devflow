"""Deterministic Temporal state machine. All filesystem and model work is an activity."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy


@workflow.defn(name="DevflowIssueWorkflow")
class IssueWorkflow:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}
        self.cancel_requested = False
        self.decision_answer: str | None = None

    @workflow.run
    async def run(self, spec: dict[str, Any]) -> dict[str, Any]:
        self.state = {
            "run_id": spec["run_id"],
            "input_digest": spec["input_digest"],
            "phase": "waiting_decision" if spec["require_decision"] else "implement",
            "outcome": None,
            "revision": 1,
            "decision_id": f"{spec['run_id']}:start" if spec["require_decision"] else None,
            "candidate": spec["initial_candidate"],
            "roles": [],
            "findings": [],
            "cleanup": "none",
        }
        if spec["require_decision"]:
            await workflow.wait_condition(
                lambda: self.decision_answer is not None or self.cancel_requested
            )
            if self.cancel_requested:
                return self._cancelled()
            if self.decision_answer != "proceed":
                return self._blocked("decision declined")
            self.state["phase"] = "implement"

        for role in ("implement", "review", "verify"):
            if self.cancel_requested:
                return self._cancelled()
            self.state["phase"] = role
            requested_candidate = self.state["candidate"]
            try:
                result = await workflow.execute_activity(
                    "run_role",
                    {"spec": spec, "role": role, "candidate": requested_candidate},
                    start_to_close_timeout=timedelta(hours=2),
                    retry_policy=RetryPolicy(maximum_attempts=1),
                )
            except Exception as exc:
                if self.cancel_requested:
                    return self._cancelled()
                return self._blocked(f"{role} activity failed: {type(exc).__name__}")
            self.state["roles"].append(result)
            if self.cancel_requested:
                return self._cancelled()
            if result.get("input_candidate_id") != requested_candidate["id"]:
                return self._blocked(f"{role} result is bound to another candidate")
            if result.get("status") != "pass":
                self.state["findings"].extend(result.get("findings") or [])
                return self._blocked(f"{role} did not pass")
            if role == "implement":
                produced = result.get("candidate")
                if not isinstance(produced, dict) or not produced.get("id"):
                    return self._blocked("implementer did not produce a candidate")
                self.state["candidate"] = produced
            elif result.get("candidate", {}).get("id") != requested_candidate["id"]:
                return self._blocked(f"{role} observed changed candidate content")
        self.state["phase"] = "completed"
        self.state["outcome"] = "completed"
        return self.state

    def _blocked(self, reason: str) -> dict[str, Any]:
        self.state["phase"] = "blocked"
        self.state["outcome"] = "blocked"
        self.state["findings"].append(reason)
        return self.state

    def _cancelled(self) -> dict[str, Any]:
        self.state["phase"] = "cancelled"
        self.state["outcome"] = "cancelled"
        self.state["cleanup"] = "unknown_after_activity" if self.state["roles"] else "none"
        return self.state

    @workflow.query(name="status")
    def status(self) -> dict[str, Any]:
        return self.state

    @workflow.update(name="decision")
    def decision(self, request: dict[str, Any]) -> dict[str, Any]:
        if self.state.get("phase") != "waiting_decision":
            raise ValueError("no decision is pending")
        if request.get("decision_id") != self.state["decision_id"]:
            raise ValueError("decision ID does not match")
        if request.get("revision") != self.state["revision"]:
            raise ValueError("decision revision does not match")
        answer = request.get("answer")
        if answer not in ("proceed", "decline"):
            raise ValueError("answer must be proceed or decline")
        self.decision_answer = answer
        self.state["decision_id"] = None
        self.state["revision"] += 1
        return self.state

    @workflow.update(name="cancel")
    def cancel(self, reason: str) -> dict[str, Any]:
        if self.state.get("outcome") is not None:
            raise ValueError("run is already terminal")
        if not reason.strip():
            raise ValueError("cancellation reason is required")
        self.cancel_requested = True
        self.state["phase"] = "cancelling"
        self.state["revision"] += 1
        self.state["cleanup"] = "pending_role_completion"
        return self.state
