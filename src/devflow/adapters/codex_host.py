"""Native host bridge: prepare briefs and validate externally observed receipts.

This module deliberately makes no calls to desktop storage, HTTP or subprocesses.
The active owner invokes supported native tools and submits their observations.
Inventory entries for reconciliation must include the original prompt and context,
retrieved by a supported task read; inventory titles alone cannot identify a task.
"""

from __future__ import annotations

import hashlib
import json

from devflow.errors import WorkflowError


class NativeHostBridge:
    @staticmethod
    def marker(assignment: dict) -> str:
        identity = {
            key: assignment[key]
            for key in ("assignment_id", "action_id", "attempt_id", "owner_task_id")
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        return f"[devflow-assignment:{digest}]"

    def prepare_assignment(self, assignment: dict, brief: str) -> dict:
        if assignment.get("status") != "prepared" or assignment.get("task_id"):
            raise WorkflowError("assignment_state", "Only a prepared unbound assignment can launch")
        if assignment.get("client_id"):
            raise WorkflowError("pending_task", "Pending setup must be reconciled before launching")
        context = {
            key: assignment[key]
            for key in (
                "assignment_id",
                "action_id",
                "attempt_id",
                "candidate_id",
                "role",
                "owner_task_id",
            )
        }
        prompt = f"{self.marker(assignment)}\n{json.dumps(context, sort_keys=True)}\n\n{brief}"
        return {
            "operation": "launch_role",
            "action_id": assignment["action_id"],
            "assignment_id": assignment["assignment_id"],
            "prompt": prompt,
            "marker": self.marker(assignment),
            "context": context,
            "native_tool": "create_thread",
            "requires_native_owner": True,
        }

    def record_launch(self, assignment: dict, response: dict) -> dict:
        """Accept native threadId or queued clientThreadId, never conflate them."""
        if assignment.get("status") not in {"prepared", "pending_setup", "running"}:
            raise WorkflowError(
                "assignment_state", "Assignment cannot accept a launch receipt in this state"
            )
        task_id, client_id = response.get("threadId"), response.get("clientThreadId")
        if client_id and assignment.get("client_id") not in (None, client_id):
            raise WorkflowError(
                "task_identity_conflict", "Receipt changed the pending client identity"
            )
        if task_id and client_id and task_id == client_id:
            raise WorkflowError("invalid_host_receipt", "A client ID is not an executable task ID")
        if task_id:
            if not isinstance(task_id, str) or task_id == assignment.get("client_id"):
                raise WorkflowError(
                    "invalid_host_receipt", "Actual native task identity is required"
                )
            if assignment.get("task_id") not in (None, task_id):
                raise WorkflowError(
                    "task_identity_conflict", "Assignment is already bound to another task"
                )
            return assignment | {
                "task_id": task_id,
                # The pending identity remains in receipt/assignment history; a
                # runnable assignment carries only the actual native task identity.
                "client_id": None,
                "status": "running",
            }
        if client_id:
            if not isinstance(client_id, str) or assignment.get("task_id"):
                raise WorkflowError("invalid_host_receipt", "Invalid pending setup receipt")
            if assignment.get("client_id") not in (None, client_id):
                raise WorkflowError(
                    "task_identity_conflict", "Assignment already has a pending setup"
                )
            return assignment | {"task_id": None, "client_id": client_id, "status": "pending_setup"}
        raise WorkflowError(
            "ambiguous_host_action", "No actual or pending task identity; reconcile inventory"
        )

    def reconcile_launch(self, assignment: dict, inventory: list[dict]) -> dict:
        marker = self.marker(assignment)
        matches = [
            item
            for item in inventory
            if marker in item.get("prompt", "")
            and item.get("assignment_id") == assignment["assignment_id"]
            and item.get("attempt_id") == assignment["attempt_id"]
            and item.get("candidate_id") == assignment["candidate_id"]
            and item.get("owner_task_id") == assignment["owner_task_id"]
        ]
        if len(matches) != 1:
            raise WorkflowError(
                "ambiguous_host_action", "Inventory does not uniquely identify the assignment"
            )
        return self.record_launch(assignment, matches[0])

    @staticmethod
    def wait_target(assignment: dict, *, cursor: str | None = None) -> dict:
        task_id = assignment.get("task_id")
        if not task_id or task_id == assignment.get("client_id"):
            raise WorkflowError(
                "pending_task", "A final task ID is required before waiting or messaging"
            )
        target = {"threadId": task_id}
        if cursor:
            target["afterCursor"] = cursor
        return target

    @staticmethod
    def validate_result(assignment: dict, result: dict, *, observed_task_id: str) -> dict:
        if not assignment.get("task_id") or assignment["task_id"] != observed_task_id:
            raise WorkflowError(
                "producer_mismatch", "Result did not come from the registered native task"
            )
        if result.get("producer_task_id", observed_task_id) != observed_task_id:
            raise WorkflowError(
                "producer_mismatch", "Claimed result producer differs from native observation"
            )
        if result.get("attempt_id", assignment["attempt_id"]) != assignment["attempt_id"]:
            raise WorkflowError("result_mismatch", "Result attempt does not match assignment")
        for key in ("assignment_id", "candidate_id"):
            if result.get(key) != assignment.get(key):
                raise WorkflowError("result_mismatch", f"Result {key} does not match assignment")
        if result.get("producer_role", result.get("role")) != assignment["role"]:
            raise WorkflowError(
                "producer_mismatch", "Result role does not match registered assignment"
            )
        return result
