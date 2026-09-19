---
name: devflow-coordinating
description: Carry a defined request through implementation, early PR publication, review, verification and the authorized merge.
---

# Coordinate the outcome

Use the user's session to define scope, dispatch work, assess evidence and maintain records. Delegate product edits and checks to the appropriate [agent](../devflow/references/agents.md). Read files and inspect Git/GitHub directly. Run the record and issue helpers, publish authorized review comments, and perform the final authorized PR or stack merge. Do not edit product files, commit, push or run product checks yourself. The installed hook catches common repository writes; it is a backstop, not workflow enforcement.

## Steps

1. **State the outcome.** Record observable acceptance conditions, scope and explicit limits. Ask only for information that changes the work; reuse the existing work record for continuation. A question or plan-only request creates no implementation work.

2. **Claim the issue.** Follow [ownership](../devflow/references/ownership.md). Maintain the assignee and existing Project Status. Keep the issue in progress while implementation continues, even after its PR opens. Release the claim when the task stops.

3. **Choose the next useful action.** Handle simple clarification yourself. Use the definer when ambiguity needs repository or evidence investigation, and the planner for consequential choices or slicing. A small, clear change needs only a concise implementation brief. Do not dispatch agents to fill mandatory stages. Split features into coherent reviewable PRs. Sequential features or slices started before earlier work merges join the same gh stack, including logically independent features; see [PR workflow](../devflow/references/pr-workflow.md).

4. **Select and reuse an agent.** Use the named role in the [agents reference](../devflow/references/agents.md). Reuse the original implementer for repairs and the original reviewer/verifier for rechecks when work ID, worktree and role remain compatible. Spawn a new agent by type with `fork_turns: "none"` when needed; the installed definition selects model and effort. Brief it with context it cannot otherwise inherit. Follow the [worker reference](../devflow/references/implementation-worker.md) for reuse and spawn failures.

5. **Supply role-specific inputs.** Every brief includes work ID, repository/worktree, outcome, relevant context, project instructions, explicit limits and your agent ID or path for replies. Add the inputs below; state unknowns instead of making them up.

   | Role | Additional inputs |
   | --- | --- |
   | Definer | Original request, observed behavior, evidence and unresolved questions |
   | Planner | Accepted conditions, constraints, affected paths and existing unmerged branches/PRs |
   | Implementer | Owned files/modules, branch/base or existing PR and stack order, dependencies, checks with expected results, publication limits |
   | Reviewer | Base/head SHAs or complete snapshot, acceptance conditions, review scope and prior findings to recheck |
   | Verifier | Candidate identity, acceptance scenarios, environment/build setup, check limits and original failure evidence |

   Include the relevant role skill. Tell agents that the checkout is shared and they must preserve others' edits. Parallel work needs disjoint ownership and separate worktrees; sequential unmerged work keeps its branch chain.

   **Handle questions as they arrive.** Agents send questions to you or return them with partial findings; you own the user conversation. Answer from existing context when possible, otherwise ask the user yourself. Relay the answer and any changed constraints to the same agent with `agents.send_message` if it is running, or `agents.followup_task` if it has finished. Record accepted decisions in the brief. Keep independent work moving, but do not dispatch work that depends on an unresolved required decision. A partial report is not a completed definition.

6. **Publish during implementation.** The implementer commits the first meaningful change and opens its PR immediately, then pushes repairs to that same PR. Do not defer publication until review or QA. Preserve explicit local-only/no-commit/no-push instructions. Record the returned PR, base/head SHAs and stack order. A published head must contain the changes being reviewed; dirty work needs an explicit complete snapshot.

7. **Review and verify the current candidate.** Dispatch only the roles and checks required by the request, risk and project policy. Keep findings tied to the checked revision. Route repairs to the implementer; rebase/resubmit affected upper stack layers and reassess evidence when the code or bases change. Match required behavior to observed evidence, including the identity of the build exercised. Do not replace a missing product scenario with green unrelated tests.

8. **Record results and handle comments.** Agents return reports; you record runs, model/effort, checks, findings and publication references with the [state helper](../devflow/references/state.md). Maintain the tracker yourself. For several owned issues, bind each child to its work ID with `telemetry.py bind`; shared coordinator usage stays unallocated. Publish comments only when authorized, using inline locations when requested. Resolve a finding only after checking its original trigger and repair evidence, then read back its thread state.

9. **Merge when requested.** Follow [merging](../devflow-merging/SKILL.md) directly; there is no delivery agent or separate publication stage. Inspect the current PR or stack and its evidence before mutation. Report queued separately from merged. Release the claim and reconcile the issue at the requested endpoint.

10. **Recover from actual state.** After interruption, read the existing record and inspect the checkout, PR heads, stack bases, checks and artifacts. Reconcile an uncertain push, create or merge before retrying. Resume the same work and retain earlier findings and evidence.

## Implementation brief

```text
Work: retry-fix. Worktree: /worktrees/retry-fix; preserve collaborators' changes.
Outcome: client.fetch() retries a timeout twice, then raises RetryExhausted.
Owns: src/client/retry.py, tests/test_retry.py. Context: issue #12 reproduces an infinite retry.
Branch: fix/retry. Base: feat/client (unmerged PR #41); append this feature to that gh stack.
Publish: open the PR after the first meaningful commit; push subsequent repairs to it. No merge requested.
Checks: pytest tests/test_retry.py -q passes; the regression fails on the previous candidate. No full suite.
Return: PR/stack, base/head SHAs, changed scope, check results and remaining gaps.
```
