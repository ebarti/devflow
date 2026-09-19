---
name: devflow-verifying
description: Reproduce a defect, exercise product behavior, run project checks, or verify a repair with observable evidence.
---

# Verify the outcome

Delegated verification runs as the `devflow-verifier` [agent](../devflow/references/agents.md), which runs commands and writes evidence only. Verification produces evidence about the candidate; it does not change the candidate. Repairs and regression-test changes run in the [implementation worker](../devflow/references/implementation-worker.md).

## Steps

1. **Start from acceptance.** Start from the user's acceptance conditions and current candidate. Use the target project's existing commands and QA policies, respecting explicit scope and check limits.

2. **Exercise the real path.** For a product-use claim, exercise the actual application entry point in each required mode and inspect the user-visible or persisted result. Include failure and recovery behavior when relevant. Passing checks, healthy services and simulated dependencies do not establish unexercised product behavior.

3. **Isolate and clean up.** Use owned isolated resources and preserve unrelated data and processes. Tests sharing a runtime or database must restore their owned state; verify cleanup and affected neighboring scenarios.

4. **Record what happened.** Record the candidate and environment, setup, actions, expected and observed result, and any simulated dependencies.

5. **Verify repairs at the trigger.** For repairs, reproduce the original trigger, verify the invariant and retain failed output.

6. **Protect behavior.** Add regression coverage where the project requires it or where it materially protects behavior, routing the test change, like any repair, through [coordinating](../devflow-coordinating/SKILL.md) to the implementation worker.

7. **Report per condition.** For each acceptance condition, report verified, failed or unverified with its evidence. Continue required verification within the authorized scope; if blocked, name the unexercised scenario and the blocker.

8. **Retain results.** Retain useful artifacts at durable locations and return their references with the results and findings; the coordinator records them with the [shared helper](../devflow/references/state.md). A delegated verifier writes evidence, not Devflow records. Return the observed outcomes and limits; an unverified required scenario keeps verification incomplete regardless of other passing checks.

## Example report

```text
Candidate 3f2a1c9, local dev server, real HTTP client, mocked upstream (simulated dependency).
1. Timed-out request retried twice then RetryExhausted: verified (log retained at /path/to/evidence/retry-1.log).
2. Successful first attempt sends immediately: failed (observed 2 s delay; finding retry-fix-f1).
3. Behavior against the real upstream: unverified (staging token unavailable).
For the coordinator to record: result retry-fix-qa-1, kind qa, status failed, commit 3f2a1c9, evidence /path/to/evidence.
```
