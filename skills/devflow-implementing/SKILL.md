---
name: devflow-implementing
description: Implement a defined feature or repair a defect in the requested repository scope.
---

# Implement the change

Implementation runs in the `devflow-implementer` agent, the delegated [implementation worker](../devflow/references/implementation-worker.md). Your brief names you as that worker for a work ID and states the model and effort of the installed definition; rely on the brief rather than guessing your own configuration, and do the assigned work yourself without spawning another agent. If your brief and your assignment disagree, report the mismatch to the coordinator before editing. Without such a brief you are not the worker: a direct implementation request follows [coordinating](../devflow-coordinating/SKILL.md), which claims the issue and reuses or spawns the worker.

## Steps

1. **Read first.** Read the owning code, documentation and project rules. Confirm the working branch and dirty state; preserve unrelated work and collaborators' edits.

2. **Stay in scope.** Use the coordinator's work ID and assigned file scope. Never compete with another task's active claim; report results to the owning coordinator.

3. **Find the invariant.** For defects, reproduce or trace the failing invariant before editing. Follow the data through its source, transformations and consumers; fix the owning layer. A cosmetic change cannot establish a missing persistence or integrity guarantee.

4. **Change the minimum.** Make the smallest coherent change that satisfies the requested behavior. Update affected contracts and documentation.

5. **Check proportionally.** Run the target project's applicable checks in proportion to the change, respecting explicit user limits. Use [verification](../devflow-verifying/SKILL.md) when product behavior needs direct evidence; distinguish a blocked check from a proven defect.

6. **Hand over the record.** Return the commit, evidence references, unresolved findings and useful continuation context; the coordinator records the run, with the model and effort from your brief, and its results and findings with the [shared helper](../devflow/references/state.md). A delegated worker writes no Devflow records. Preserve failed evidence at durable paths.

7. **Report.** Return the candidate, changed scope, checks performed with observed results, and unresolved limits. Keep failed or unperformed checks explicit; do not invent a mandatory extra stage.

## Example report

```text
Candidate: 3f2a1c9 on retry-fix. Changed: src/client/retry.py (backoff and attempt cap), tests/test_retry.py (new).
Checks: `pytest tests/test_retry.py -q` passed (3 tests); the new test fails on 9b7d0e2 as expected.
Not done: the integration suite needs the staging token and was not run.
For the coordinator to record: run retry-fix-impl-1, role implementer, gpt-5.6-sol / high, completed at 3f2a1c9.
```
