---
name: devflow-defining-work
description: Clarify a development outcome, investigate an ambiguous bug report, or explore a design before implementation.
---

# Define the work

## Steps

1. **Inspect the evidence.** In the main task, read the relevant code, owning documentation and user-provided evidence. Trace the production path and its callers rather than relying on names or summaries. Cite the files, lines or observations that support the conclusion.

2. **Describe the outcome observably.** State current behavior, desired behavior, boundaries and what would demonstrate success. Separate confirmed facts from hypotheses.

3. **Resolve uncertainty.** For a bug, identify the trigger and violated invariant, test competing explanations against the evidence, and state the supported cause or the missing probe. Ask the user directly only when the answer changes the outcome or design and cannot be found in the available context. Explain the decision and supported options. Keep independent inspection moving while awaiting the answer; leave dependent work unassigned.

4. **Match the output to the request.** Return the current behavior, desired behavior, boundaries, success evidence, confirmed facts and unresolved questions. Design-only discussion remains discussion. For authorized implementation, use [planning](../devflow-planning/SKILL.md) to turn the inspection into an executable brief, then [hand it off](../devflow-coordinating/SKILL.md). A small fix needs only a short plan.

5. **Retain decisions.** Include the evidence, accepted decisions and remaining limits in the plan. For authorized implementation, retain them in the same [work record](../devflow/references/state.md). Discussion and loading this skill create no issue, claim or execution agent.

## Example

```text
Reported: "uploads sometimes fail". Confirmed: uploads over 8 MB get 413 from the proxy (proxy log, 3 cases). Hypothesis: the app never chunks.
Desired: uploads up to 100 MB succeed through the proxy. Boundary: no proxy changes. Success: a 50 MB upload completes in the staging UI.
Question for the user: is resumable upload required, or is chunking enough?
Why: resumability changes the upload protocol and persistence. Blocks: final design and acceptance conditions.
Continue: inspect the current proxy and upload limits while awaiting the answer.
```
