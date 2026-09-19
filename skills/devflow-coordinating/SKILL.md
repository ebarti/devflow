---
name: devflow-coordinating
description: Run the Devflow workflow for a defined development request or bounded batch: own the outcome, dispatch the Devflow agents, gate on evidence, and recover interrupted work.
---

# Coordinate the outcome

The coordinator is the user's own session. It runs the workflow and owns the outcome; it does not do the workflow's work. Its whole tool surface is: read files, run read-only inspection commands, run the Devflow helpers (`state.py`, `github.py`, `telemetry.py`), spawn and steer the [Devflow agents](../devflow/references/agents.md), and talk to the user. It never edits files, never commits, pushes or merges, never runs product checks as evidence, and never chooses a model for an agent. For a session that holds a claim, the installed hook enforces the repository part of that boundary: edit tools, mutating Git commands, in-place edits and redirects outside temporary paths are denied and recorded as `boundary` events.

Coordination is bookkeeping and judgment about evidence, so run feature threads on `gpt-5.6-sol` at high or xhigh; the planner, reviewer and verifier bring `gpt-6-astra` where judgment matters, and the implementer runs Sol / high.

## Steps

1. **Establish the outcome with the user.** Use the request and the target project's instructions to state the outcome, acceptance conditions, scope and constraints in observable terms; ask only what materially changes the work. Reuse the [work record](../devflow/references/state.md) for the same outcome; record scope, progress, evidence and the concrete next step. Do not scan or resume a backlog merely because the skill loaded.

2. **Claim before dispatching.** Follow [issue ownership](../devflow/references/ownership.md): claim the issue, maintain its assignee and existing Project Status, and release ownership when the task stops. For a requested batch, a coordinator may own several issues and keep independent ready work moving concurrently with separate work IDs and worktrees, respecting dependencies and host capacity. One issue keeps one coordinating owner across its delegated roles.

3. **Get a sharp plan.** Delegate planning to `devflow-planner` with the outcome, acceptance conditions, constraints and evidence; for an ambiguous bug or request, delegate the investigation to `devflow-definer` first. A trivial change whose slice and checks you can state in a few sentences needs no planner. Ratify the returned plan against the acceptance conditions and the user's constraints, put consequential open choices to the user, and never dispatch a slice whose checks and expected results are not written down.

4. **Choose the agent.** Every delegated action runs as its [Devflow agent type](../devflow/references/agents.md): `devflow-definer`, `devflow-planner`, `devflow-implementer`, `devflow-reviewer`, `devflow-verifier` and `devflow-deliverer`. Reuse an existing agent when the next task is the same or closely related work in the same role, work ID and worktree, as the [worker reference](../devflow/references/implementation-worker.md#reuse) describes; otherwise spawn by type with `fork_turns: "none"`. Never pass a model or effort, use a generic role, or let an agent inherit the coordinator's model. On a transient spawn failure, wait or reuse a suitable agent; on a configuration failure, stop and ask the user. Never do the delegated work yourself.

5. **Brief the worker** as the reference specifies: the statement that it is the implementation worker for this work ID and must not delegate, one observable outcome, owned files or modules, required context and dependencies, and exact checks or manual steps with expected results. Include the relevant [implementation instructions](../devflow-implementing/SKILL.md). Tell workers they share the codebase and must preserve others' edits. Delegate only independent work concurrently; do not create agents just to satisfy stages.

6. **Keep metrics attributable.** Hooks attach to the claiming coordinator and inherit its issue for child tasks when there is exactly one issue. For a multi-issue coordinator, bind each child explicitly with `telemetry.py bind --session-id CHILD_ID --work-id WORK_ID --role ROLE`; shared coordinator usage remains unallocated. Keep recording semantic check and review outcomes and findings; tool completion alone cannot establish product acceptance. Use `state.py metrics --work-id WORK_ID` to inspect coverage before the final report.

7. **Gate on evidence.** Dispatch `devflow-reviewer` and `devflow-verifier` as the request and project policy require, and route every repair back to the implementation worker. Preserve the user's acceptance conditions through delegation. Before reporting completion, match each required behavior and mode to observed evidence; review approval or passing checks cannot replace an unexercised product scenario. Continue authorized verification. Record actual role outcomes before repairs obscure them, linking findings to fixes and follow-up evidence.

8. **Deliver within authorization.** Dispatch `devflow-deliverer` for the requested endpoint only; preparing is not permission to push, and a published PR is not product success. Report implemented, verified, published and merged status separately.

9. **Continue interrupted work on request.** Read the existing record and inspect the actual checkout, artifacts and external state. Reconcile uncertain actions before retrying them; a saved intention is not proof of completion. Resume the same outcome and preserve previous results. Report what is implemented, checked, published or blocked with the evidence and the next action needed.

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
