---
name: devflow-implementing
description: Implement a defined feature or repair a defect in the requested repository scope.
---

# Implement the change

Implementation runs in the delegated [implementation worker](../devflow/references/implementation-worker.md). Your brief names you as that worker for a work ID and states the model and effort you were spawned with; rely on the brief rather than guessing your own configuration, and do the assigned work yourself without spawning another implementer. If your brief and your assignment disagree, report the mismatch to the coordinator before editing. Without such a brief you are not the worker: a direct implementation request follows [coordinating](../devflow-coordinating/SKILL.md), which claims the issue and reuses or spawns the worker.

## Steps

1. **Read first.** Read the owning code, documentation and project rules. Confirm the working branch and dirty state; preserve unrelated work and collaborators' edits.

2. **Stay in scope.** Use the coordinator's work ID and assigned file scope. Never compete with another task's active claim; report results to the owning coordinator.

3. **Find the invariant.** For defects, reproduce or trace the failing invariant before editing. Follow the data through its source, transformations and consumers; fix the owning layer. A cosmetic change cannot establish a missing persistence or integrity guarantee.

4. **Change the minimum.** Make the smallest coherent change that satisfies the requested behavior. Update affected contracts and documentation.

5. **Check proportionally.** Run the target project's applicable checks in proportion to the change, respecting explicit user limits. Use [verification](../devflow-verifying/SKILL.md) when product behavior needs direct evidence; distinguish a blocked check from a proven defect.

6. **Record the run.** Use the [shared helper](../devflow/references/state.md) to record the actual run with the model and effort from your brief, commit and evidence references, unresolved findings and useful continuation context. Preserve failed evidence.

7. **Report.** Return the candidate, changed scope, checks performed with observed results, and unresolved limits. Keep failed or unperformed checks explicit; do not invent a mandatory extra stage.

## Example report

```text
Candidate: 3f2a1c9 on retry-fix. Changed: src/client/retry.py (backoff and attempt cap), tests/test_retry.py (new).
Checks: `pytest tests/test_retry.py -q` passed (3 tests); the new test fails on 9b7d0e2 as expected.
Not done: the integration suite needs the staging token and was not run.
Run recorded: state.py record run --id retry-fix-impl-1 --work-id retry-fix --role implementer --model gpt-5.6-sol --effort high --status completed
```
