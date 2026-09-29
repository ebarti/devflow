---
name: devflow-coordinating
description: Hand an inspected plan to an execution coordinator, or carry that plan through implementation, early PR publication, review, verification and the authorized merge.
---

# Execute the plan

## Main task: inspect, plan, hand off

Perform [inspection](../devflow-defining-work/SKILL.md) and [planning](../devflow-planning/SKILL.md) in the main Astra task, including user questions. Keep a small change's plan short. A discussion or plan-only request ends there.

For authorized implementation, create or reuse the [work record](../devflow/references/state.md) and [issue claim](../devflow/references/ownership.md) under the main task's actual host ID. Save the inspected plan and its evidence. Resolve the installed helper paths and absolute database path once; the execution coordinator uses that same database and owner ID.

Spawn one `devflow-coordinator` with `fork_turns: "none"` for the bounded request or batch. Its [definition](../devflow/references/agents.md) selects Sol/high. Supply:

- work IDs, repository/worktrees and project instructions;
- accepted outcome, acceptance conditions, inspection evidence and design rationale;
- ordered slices, owned files/modules, dependencies and exact checks with expected results;
- existing branches/PRs, their bases and stack order;
- explicit limits and the authorized endpoint: local change, published PR/stack or merge;
- claim owner's actual host task ID, absolute helper/database paths and the main agent's reply target.

The coordinator inherits the main task's permissions for records and tracker operations; it does not receive broader permissions. Verify required record access as part of starting the work. Leaf workers have their own roles and return reports. During execution, the coordinator is the sole writer of that work's records and tracker updates. The main task retains the user conversation and material design decisions.

Wait for completion or a material escalation instead of supervising each worker turn. Follow the [coordination cadence](#coordination-cadence) while the coordinator runs. Forward new user constraints to the same coordinator. Answer escalations from the existing context or ask the user directly, then send the decision to the running coordinator or resume it with `agents.followup_task`. When it has released a claim, reacquire the same work for the main task before resuming. Do not spawn a replacement for an ordinary clarification or repair.

On return, check the consolidated report against the requested endpoint and inspect the deciding remote state or artifact. Do not repeat current review and verification. Reconcile records/claims yourself if interruption prevented the coordinator from doing so. Standalone review, verification and merge requests use the direct routes in [Devflow](../devflow/SKILL.md); they need no execution coordinator unless implementation is requested.

## Coordination cadence

Main task to coordinator and coordinator to worker communication follows one cadence. Work autonomously. Send no routine inter-agent progress during the first 30 minutes of ongoing work. After that, send at most one useful routine update in each subsequent 30-minute interval; an interval ending does not require an update. Do not narrate each step or PR-publication milestone, use repeated short status polls or pokes, or treat silence or a wait-tool timeout as failure or permission to request status.

Send completion, actionable blockers or material decisions, required candidate/results/repair handoffs, and responses to explicit user steering or status requests immediately. The interval must not delay functional collaboration, user-facing commentary, work-record updates or GitHub state transitions.

## Execution coordinator: finish the assignment

1. **Resume actual state.** Read the plan, work record, claim, checkout and existing PR/stack. Confirm the supplied owner and database; keep the same work IDs. Run the [read-only issue audit](../devflow/references/ownership.md) for each linked work; inspect stopped or failed child/owner runtime and any tracked Actions run before deciding whether ownership or approval remains valid. Reconcile an uncertain create, push or merge before retrying. Missing record access or a missing decision is a concrete escalation, not permission to invent a new database or expand scope.

2. **Dispatch the next useful worker.** Spawn only the implementer, reviewer or verifier by [agent type](../devflow/references/agents.md), with `fork_turns: "none"` and a self-contained brief. Reuse compatible original workers for repairs and rechecks. Never spawn another coordinator or delegate inspection/planning. Work within host capacity; run roles sequentially when slots are limited.

   Every brief includes work ID, worktree, outcome, acceptance conditions, relevant plan/evidence, project instructions, explicit limits, your reply target and the [coordination cadence](#coordination-cadence). Add the role-specific inputs:

   | Role | Additional inputs |
   | --- | --- |
   | Implementer | Owned files/modules, branch/base or existing PR, stack order, dependencies, checks and expected results, publication limits |
   | Reviewer | Base/head SHAs or complete snapshot, design rationale, review scope, prior findings and original triggers |
   | Verifier | Candidate identity, scenarios and expected results, environment/build setup, original failure evidence |

   Include the relevant role skill. The checkout is shared: workers must preserve others' edits. Parallel implementation needs disjoint ownership and separate worktrees. Use the [worker reference](../devflow/references/implementation-worker.md) for reuse and spawn failures.

3. **Publish during implementation.** The implementer commits the first meaningful change and opens its non-draft PR immediately, then pushes repairs to that same PR. Respect explicit local-only/no-commit/no-push instructions. Sequential features or slices started before prior work merges join the same gh stack, including logically independent features; follow [PR workflow](../devflow/references/pr-workflow.md). Record observed PR/base/head SHAs and stack order. Keep the issue in progress while implementation continues; move it to in review when the candidate is ready.

4. **Close the repair loop.** Dispatch only the review and verification required by the plan, risk and project policy. Supply the original acceptance conditions as well as the implementation report. Keep required independence. Route actionable findings to the original implementer; rebase/resubmit affected upper stack layers and send the changed candidate to the original reviewer/verifier. Reassess affected evidence when code or bases change. An unverified required scenario remains incomplete. Escalate a demonstrated flaw in the accepted plan to the main task; routine repairs stay here.

5. **Keep records and comments current.** Record runs, observed model/effort, results, findings and publication references with the supplied [state helper](../devflow/references/state.md). Update the issue with the supplied owner ID. Leaf workers write no records. Publish comments only when authorized and inline when requested. Resolve a finding only after checking its original trigger and repair evidence, then read back the thread state. For a batch, explicitly bind each child to its work ID when the actual runtime session ID is available; do not invent IDs or usage attribution.

6. **Handle real blockers.** Settle worker questions from the plan and evidence. Send the main task only a missing user decision, material design/scope change, inaccessible prerequisite, or repair loop without evidenced progress. Include the decision, evidence, recommendation and affected work. Continue independent work; if none remains, retain partial results and return the blocker. Never call user-input tools, silently change the plan or keep retrying unchanged failures.

7. **Finish at the authorized endpoint.** Perform the [PR or stack merge](../devflow-merging/SKILL.md) directly only if authorized. Otherwise stop with the requested local candidate or current published PRs. Read back the resulting state and run the issue audit before releasing the claim. Record each semantic status decision once with `github.py set --release`; the service retries mechanical synchronization and observes tracked Actions after agents stop. A terminal Actions run becomes an actionable review or repair state, not acceptance or issue closure. Preserve actual review and QA handoffs. Return one consolidated report with candidates, PR/stack state, acceptance evidence, review/verification results, open findings and remaining work. A queued merge is not merged.

Do not edit product files, commit, push or run product checks yourself. Use the leaf workers for those actions. Keep routine worker messages and rechecks here; the main task needs the final report and material escalations, not a narration of every step.
