---
name: devflow-reviewing
description: Review an existing code change for actionable defects, contract violations and meaningful coverage, or verify a claimed repair.
---

# Review the candidate

Delegated review runs as the read-only `devflow-reviewer` [agent](../devflow/references/agents.md). Review does not change the candidate: findings go back to the coordinator, and repairs run in the [implementation worker](../devflow/references/implementation-worker.md).

## Steps

1. **Establish the target.** Identify the worktree, base/head SHAs or complete dirty snapshot, and relevant project contracts. Review that candidate rather than assuming HEAD contains uncommitted work.

2. **Trace the change.** Follow changed behavior through callers and consumers, including realistic failure paths. Seek evidence that could disprove the implementation's claim; passing tests alone do not establish the claimed coverage. For prompts, skills and workflows, walk a concrete request through its inputs, instructions, decisions, handoffs and completion evidence. Flag demonstrated contradictions, missing task guidance and omitted work with the same evidence standard as code defects. Respect explicit review and test limits.

3. **Report findings.** Report actionable findings with severity, location, concrete trigger, impact and supporting evidence. Separate confirmed defects from uncertainties and optional improvements. State what was inspected and what remains unverified. Follow project rules for required independence and checks; do not manufacture a review gate.

4. **Retain the result.** Return the review result and findings tied to the candidate SHAs or snapshot; the coordinator records them with the [shared helper](../devflow/references/state.md) against the actual reviewer run. A delegated reviewer is read-only and writes no Devflow records. Preserve earlier findings when verifying a repair; report the fix and new evidence instead of silently replacing the original judgement.

5. **Route repairs.** Return implementation repairs to [coordinating](../devflow-coordinating/SKILL.md) for the implementation worker; do not edit the candidate yourself.

6. **Verify repairs.** Check a claimed repair against the original trigger and relevant adjacent behavior before marking its finding resolved.

7. **Publish within scope.** The coordinator publishes authorized review comments and resolves verified threads, using inline locations when requested. The delegated reviewer returns the comment text and precise locations without publishing. Return concise findings and limits to the requesting agent or user.

## Example finding

```text
[high] src/client/retry.py:42 sleeps for one backoff interval before the first attempt.
Trigger: call fetch() against a healthy server. Impact: every request waits before sending.
Evidence: tests/test_retry.py::test_first_attempt_is_immediate fails at 3f2a1c9.
For the coordinator to record: finding retry-fix-f1, severity high, status open, commit 3f2a1c9.
```
