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


def observed_agent_status(value):
    """Normalize observed control state without copying completed message contents."""
    if isinstance(value, str) and value in {"running", "interrupted"}:
        return value
    if isinstance(value, dict) and set(value) == {"completed"} and isinstance(value["completed"], str):
        return "completed"
    raise WorkflowError("invalid_agent_status", "Unsupported native agent status observation")


def require_native_coordinator(assignment, current_task_id):
    if assignment.get("host_kind") == "subagent" and current_task_id != assignment["owner_task_id"]:
        raise WorkflowError("coordinator_mismatch", "Native control requires the recorded coordinator UUID")


class NativeHostBridge:
    @staticmethod
    def marker(assignment: dict) -> str:
        identity = {
            key: assignment[key]
            for key in ("assignment_id", "action_id", "attempt_id", "owner_task_id")
        }
        digest = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        return f"[devflow-assignment:{digest}]"

    def prepare_assignment(self, assignment: dict, brief: str, *, recovery=None) -> dict:
        if assignment.get("host_kind") == "subagent":
            return self.prepare_subagent(assignment, brief, recovery=recovery)
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
        if assignment.get("host_kind") == "subagent":
            if assignment.get("status") not in {"prepared", "pending_startup"}:
                raise WorkflowError("assignment_state", "Subagent launch is not pending")
            if response.get("task_name") != assignment["agent_name"]:
                raise WorkflowError("ambiguous_host_action", "Expected canonical agent path missing")
            return assignment | {"status": "pending_startup", "task_id": None}
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
        if assignment.get("host_kind") == "subagent":
            matches = [item for item in inventory
                       if item.get("agent_name") == assignment["agent_name"]]
            if len(matches) != 1:
                raise WorkflowError("ambiguous_host_action", "Agent path is not uniquely observed")
            return self.record_launch(assignment, {"task_name": matches[0]["agent_name"]})
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
        if assignment.get("host_kind") == "subagent":
            return {"native_tool": "wait_agent", "arguments": {"timeout_ms": 60000},
                    "agent_name": assignment["agent_name"],
                    "inventory_tool": "list_agents"}
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
        if assignment.get("host_kind") == "subagent" and (
            not assignment.get("startup_observation")
            or assignment.get("status") not in {"running", "completed", "blocked"}
        ):
            raise WorkflowError("startup_unverified", "Product results require confirmed startup")
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

    def prepare_subagent(self, assignment: dict, brief: str, *, recovery=None) -> dict:
        from devflow.model_policy import validate_role_policy

        policy = assignment["role_policy"]
        validate_role_policy(policy)
        if assignment.get("task_id"):
            if assignment.get("status") != "ready" or not assignment.get("startup_observation"):
                raise WorkflowError("assignment_state", "Only verified ready agents may receive work")
            context = {key: assignment[key] for key in (
                "assignment_id", "attempt_id", "candidate_id", "scope_hash", "role",
                "owned_paths", "workspace_reference"
            )}
            context["assignment_action_id"] = assignment.get("continuation_of", assignment["action_id"])
            if assignment.get("workflow_snapshot_id"):
                context["workflow_snapshot_id"] = assignment["workflow_snapshot_id"]
            instruction = "Startup is verified. Begin the bounded assignment below. "
            if assignment.get("result_recovery_id"):
                if not recovery or recovery["recovery_id"] != assignment["result_recovery_id"]:
                    raise WorkflowError("invalid_result_recovery", "Recovery requires its durable original record")
                instruction = (
                    "Serialization recovery only for your completed original result. Do not rerun product "
                    "checks, edit product files, repair findings or begin a new review round. Return bare "
                    "corrected gate JSON. Preserve every original identity, verdict, finding, timestamp "
                    "and limitation exactly. Only supplement evidence_ids with actual already recorded "
                    "candidate evidence; limitations may append a correction explanation. Do not erase "
                    "historical limitations. If evidence cannot be identified, report that limitation.\n"
                )
                brief = json.dumps(recovery)
            elif assignment.get("continuation_of"):
                instruction = ("Resume this interrupted activation and preserve its original gate identity. "
                               "This continues the same candidate and review round. Reason: "
                               + assignment["continuation_reason"] + "\n")
            return {
                "native_tool": "followup_task", "operation": "send_role",
                "assignment_id": assignment["assignment_id"], "action_id": assignment["action_id"],
                "arguments": {"target": assignment["agent_name"], "message": (
                    self.marker(assignment) + "\n" + json.dumps(context) + "\n" + instruction
                    + "Do not spawn agents. Preserve others' edits in this shared checkout.\n\n" + brief
                )},
            }
        if assignment.get("status") != "prepared":
            raise WorkflowError("reconcile_required", "An uncertain spawn must be reconciled")
        context = {key: assignment[key] for key in (
            "assignment_id", "attempt_id", "candidate_id", "owner_task_id", "agent_name", "role"
        )}
        startup = (
            "Startup handshake only. Do not edit product files, run product checks, or begin the "
            "assignment yet. Return your CODEX_THREAD_ID and the exact native session JSONL "
            "source for allowlisted session_meta identity and turn_context model/effort. "
            "The coordinator must independently verify startup and send a follow-up before "
            "product work begins. Do not spawn agents. You share the checkout; preserve others' edits."
        )
        return {
            "native_tool": "spawn_agent", "operation": "launch_role",
            "assignment_id": assignment["assignment_id"], "action_id": assignment["action_id"],
            "arguments": {"task_name": assignment["task_name"], "agent_type": "default",
                          "fork_turns": "none", "model": policy["model"],
                          "reasoning_effort": policy["reasoning_effort"],
                          "message": f"{self.marker(assignment)}\n{json.dumps(context)}\n{startup}"},
        }
