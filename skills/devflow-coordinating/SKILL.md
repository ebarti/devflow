---
name: devflow-coordinating
description: Carry a defined development request or bounded batch through execution, track its outcomes, and recover interrupted work.
---

# Coordinate the outcome

Carry a defined request or bounded batch to its requested endpoint. The coordinator owns scope, planning, dispatch, verification and delivery; every implementation change, including small fixes and review or QA repairs, runs in the [implementation worker](../devflow/references/implementation-worker.md), never in the coordinator.

## Steps

1. **Establish the outcome.** Use the user's request and target project instructions to choose the next useful action. Reuse the [work record](../devflow/references/state.md) for the same outcome; record scope, progress, evidence and the concrete next step. Do not scan or resume a backlog merely because the skill loaded.

2. **Claim before dispatching.** Follow [issue ownership](../devflow/references/ownership.md): claim the issue, maintain its assignee and existing Project Status, and release ownership when the task stops. For a requested batch, a coordinator may own several issues and keep independent ready work moving concurrently with separate work IDs and worktrees, respecting dependencies and host capacity. One issue keeps one coordinating owner across its delegated roles.

3. **Choose the agent.** Reuse an existing agent when the [worker reference](../devflow/references/implementation-worker.md#reuse) allows it; otherwise spawn the implementation worker with the arguments, repository override rule and prerequisites defined there. Do not omit those arguments or let a worker inherit the coordinator's model. On a transient spawn failure, wait or reuse a suitable worker as the reference describes; on a configuration failure, stop and ask the user. Never implement directly or substitute a model silently. Coordinator, review and verification models remain unchanged.

4. **Brief the worker** as the reference specifies: the statement that it is the implementation worker for this work ID and must not delegate, one observable outcome, owned files or modules, required context and dependencies, and exact checks or manual steps with expected results. Include the relevant [implementation instructions](../devflow-implementing/SKILL.md). Tell workers they share the codebase and must preserve others' edits. Delegate only independent work concurrently; do not create agents just to satisfy stages.

5. **Keep metrics attributable.** Hooks attach to the claiming coordinator and inherit its issue for child tasks when there is exactly one issue. For a multi-issue coordinator, bind each child explicitly with `telemetry.py bind --session-id CHILD_ID --work-id WORK_ID --role ROLE`; shared coordinator usage remains unallocated. Keep recording semantic check and review outcomes and findings; tool completion alone cannot establish product acceptance. Use `state.py metrics --work-id WORK_ID` to inspect coverage before the final report.

6. **Verify and deliver.** Use [review](../devflow-reviewing/SKILL.md), [verification](../devflow-verifying/SKILL.md) and [delivery](../devflow-delivering/SKILL.md) as the request and project policy require, routing every repair back to the implementation worker. Preserve the user's acceptance conditions through delegation. Before reporting completion, match each required behavior and mode to observed evidence; review approval or passing checks cannot replace an unexercised product scenario. Continue authorized verification. Record actual role outcomes before repairs obscure them, linking findings to fixes and follow-up evidence.

7. **Continue interrupted work on request.** Read the existing record and inspect the actual checkout, artifacts and external state. Reconcile uncertain actions before retrying them; a saved intention is not proof of completion. Resume the same outcome and preserve previous results. Report what is implemented, checked, published or blocked with the evidence and the next action needed.

## Example brief

```text
You are the implementation worker for work ID retry-fix (gpt-5.6-sol, effort high). Do not delegate implementation.
Issue: https://github.com/OWNER/REPO/issues/12. Worktree: /path/to/worktrees/retry-fix, shared with other agents; preserve their edits.
Outcome: client.fetch() retries a timed-out request at most twice with backoff, then raises RetryExhausted.
Owned files: src/client/retry.py, tests/test_retry.py. Do not edit other modules.
Context: retries currently repeat forever; the reproduction in the issue times out after 60 s.
Checks: `pytest tests/test_retry.py -q` passes, and the new test fails on the previous commit.
Report: changed files, each check with its observed output, and anything left unverified.
```
