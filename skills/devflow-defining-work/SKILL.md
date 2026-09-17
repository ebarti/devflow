---
name: devflow-defining-work
description: Clarify a development outcome, investigate an ambiguous bug report, or explore a design before implementation.
---

# Define the work

## Steps

1. **Gather the evidence.** Read the relevant code, owning documentation and user-provided evidence.

2. **Describe the outcome observably.** State current behavior, desired behavior, boundaries and what would demonstrate success. Separate confirmed facts from hypotheses.

3. **Trace unclear bugs.** For an unclear bug, trace the reported behavior to its source and identify the invariant before proposing a fix. Ask only for missing information that materially affects the solution; continue useful investigation meanwhile.

4. **Match the output to the request.** Design-only discussion remains discussion. A clear action request proceeds within its authorized scope without another approval. Use [planning](../devflow-planning/SKILL.md) when consequential choices need a plan, or [coordinating](../devflow-coordinating/SKILL.md) for a bounded change; implementation itself runs in the [implementation worker](../devflow/references/implementation-worker.md).

5. **Claim substantive work.** For substantive authorized implementation, follow [issue ownership](../devflow/references/ownership.md): reuse or create its issue, assign an accountable user and claim it for the actual owning task. Preserve explicit local-only or alternative-tracker scope. Keep the outcome and context in the same [work record](../devflow/references/state.md). Loading this skill alone creates nothing.

## Example

```text
Reported: "uploads sometimes fail". Confirmed: uploads over 8 MB get 413 from the proxy (proxy log, 3 cases). Hypothesis: the app never chunks.
Desired: uploads up to 100 MB succeed through the proxy. Boundary: no proxy changes. Success: a 50 MB upload completes in the staging UI.
Open question that changes the design: is resumable upload required, or is chunking enough?
```
