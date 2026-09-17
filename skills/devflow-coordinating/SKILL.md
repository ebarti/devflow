---
name: devflow-coordinating
description: Carry a defined development request or bounded batch through execution, track its outcomes, and recover interrupted work.
---

# Coordinate the outcome

Use the user's request and target project instructions to choose the next useful action. Reuse the [work record](../devflow/references/state.md) for the same outcome; record scope, progress, evidence and the concrete next step. Do not scan or resume a backlog merely because the skill loaded.

Claim implementation work through [issue ownership](../devflow/references/ownership.md) before dispatching it. Maintain the issue's assignee and its existing Project's Status, and release ownership when the task stops. For a requested batch, a coordinator may own several issues and keep independent ready work moving concurrently with separate work IDs/worktrees. Respect dependencies and host capacity. One issue keeps one coordinating owner across its delegated roles.

Delegate every implementation change, including small fixes and review/QA repairs, to a **Sol / high** subagent. The coordinator owns scope, planning, dispatch, verification and delivery; it does not implement changes itself. Spawn each implementation worker with these literal arguments, adding its `task_name` and `message`:

```json
{
  "agent_type": "worker",
  "model": "gpt-5.6-sol",
  "reasoning_effort": "high",
  "fork_turns": "none"
}
```

Use the configurable `worker` role, not a fixed `implementer`, `fixer` or QA role that can override the model. Do not omit these arguments or inherit the coordinator's model. If spawning fails, report the failure instead of implementing directly or silently substituting another model. Coordinator, review and verification models remain unchanged.

Give each worker a concise brief: one observable outcome, owned files/modules, required context and dependencies, and exact checks or manual steps with expected results. Include the relevant [implementation instructions](../devflow-implementing/SKILL.md). Tell workers they share the codebase and must preserve others' edits. Delegate only independent work concurrently; do not create agents just to satisfy stages.

Metrics hooks attach to the claiming coordinator and inherit its issue for child tasks when there is exactly one issue. For a multi-issue coordinator, bind each child explicitly with `telemetry.py bind --session-id CHILD_ID --work-id WORK_ID --role ROLE`. Shared coordinator usage remains unallocated. Keep recording semantic check/review outcomes and findings; tool completion alone cannot establish product acceptance. Use `state.py metrics --work-id WORK_ID` to inspect coverage before the final report.

Use [implementation](../devflow-implementing/SKILL.md), [review](../devflow-reviewing/SKILL.md), [verification](../devflow-verifying/SKILL.md) and [delivery](../devflow-delivering/SKILL.md) as the request and project policy require. Preserve the user's acceptance conditions through delegation. Before reporting completion, match each required behavior and mode to observed evidence; review approval or passing checks cannot replace an unexercised product scenario. Continue authorized verification. Record actual role outcomes before repairs obscure them, linking findings to fixes and follow-up evidence.

When the user requests continuation, read the existing record and inspect the actual checkout, artifacts and external state. Reconcile uncertain actions before retrying them; a saved intention is not proof of completion. Resume the same outcome and preserve previous results. Report what is implemented, checked, published or blocked with the evidence and next action needed.
