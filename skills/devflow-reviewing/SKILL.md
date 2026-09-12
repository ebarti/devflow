---
name: devflow-reviewing
description: Review an existing code change for actionable defects, contract violations and meaningful coverage, or verify a claimed repair.
---

# Review the candidate

Establish the actual diff and relevant project contracts. Trace changed behavior through callers and consumers, including realistic failure paths. Seek evidence that could disprove the implementation's claim; passing tests alone do not establish the claimed coverage.

Report actionable findings with severity, location, concrete trigger, impact and supporting evidence. Separate confirmed defects from uncertainties and optional improvements. State what was inspected and what remains unverified. Follow project rules for required independence and checks; do not manufacture a review gate.

Retain the review result and findings in the [shared helper](../devflow/references/state.md), tied to the candidate commit and actual reviewer run when known. Preserve earlier findings when verifying a repair; record the fix and new evidence instead of silently replacing the original judgement.

Publish review comments only within authorized external scope. Verify a claimed repair against the original trigger and relevant adjacent behavior before marking its finding resolved. Return concise findings and limits to the requesting agent or user.
