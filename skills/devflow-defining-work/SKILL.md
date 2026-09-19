---
name: devflow-defining-work
description: Clarify a development outcome, investigate an ambiguous bug report, or explore a design before implementation.
---

# Define the work

The coordinator owns requirements conversation and asks the user directly when simple clarification is enough. Delegate to the read-only `devflow-definer` [agent](../devflow/references/agents.md) when code, logs or other evidence need investigation. The delegated definer returns findings and questions to the coordinator; it does not ask the user directly.

## Steps

1. **Gather the evidence.** Read the relevant code, owning documentation and user-provided evidence.

2. **Describe the outcome observably.** State current behavior, desired behavior, boundaries and what would demonstrate success. Separate confirmed facts from hypotheses.

3. **Trace unclear bugs and route questions.** Trace the reported behavior to its source and identify the invariant before proposing a fix. Settle unknowns from evidence where possible. A delegated definer sends material questions to the coordinator's supplied agent ID/path with `agents.send_message`, including why they matter, supported options and the work they block. It continues independent investigation, then returns partial findings if only blocked work remains. If messaging is unavailable, it returns the questions immediately. It never calls user-input tools or waits indefinitely for a direct user reply. The coordinator asks only what the existing context cannot answer, then relays the answer to the same definer.

4. **Match the output to the request.** Design-only discussion remains discussion. A clear action request proceeds within its authorized scope without another approval. Use [planning](../devflow-planning/SKILL.md) when consequential choices need a plan, or [coordinating](../devflow-coordinating/SKILL.md) for a bounded change; implementation itself runs in the [implementation worker](../devflow/references/implementation-worker.md).

5. **Record through the coordinator.** For substantive authorized implementation, the coordinator follows [issue ownership](../devflow/references/ownership.md): it reuses or creates the issue, assigns an accountable user and claims it for the actual owning task. Preserve explicit local-only or alternative-tracker scope. The definer returns evidence and accepted decisions for the coordinator to retain in the same [work record](../devflow/references/state.md). Loading this skill alone creates nothing.

## Example

```text
Reported: "uploads sometimes fail". Confirmed: uploads over 8 MB get 413 from the proxy (proxy log, 3 cases). Hypothesis: the app never chunks.
Desired: uploads up to 100 MB succeed through the proxy. Boundary: no proxy changes. Success: a 50 MB upload completes in the staging UI.
Question for coordinator: is resumable upload required, or is chunking enough?
Why: resumability changes the upload protocol and persistence. Blocks: final design and acceptance conditions.
Continue: inspect the current proxy and upload limits while the coordinator obtains the answer.
```
