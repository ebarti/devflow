---
name: devflow-delivering
description: Publish, merge, release or install a requested development outcome, close verified review findings, or reconcile interrupted delivery.
---

# Deliver the outcome

Delegated delivery runs as the `devflow-deliverer` [agent](../devflow/references/agents.md), which uses Git and `gh` within the authorized endpoint and never edits product code.

## Steps

1. **Establish the endpoint.** Establish the requested endpoint and current candidate. Follow target project policies and the user's existing authorization. Complete authorized preparation without another approval gate; do not expand a request to push into permission to merge, release or deploy.

2. **Use the project's tools and conventions.** Use Git, `gh`, host tools and project commands directly. Preserve unrelated changes and use the project's commit and PR conventions. Describe the concrete problem, resulting behavior, relevant checks and material limits. Keep private artifacts and credentials out of public output.

3. **Read back after each action.** Inspect the actual remote or installation state after each meaningful delivery action. When an interrupted action may have succeeded, read its state before retrying. Report what actually happened, including commit, PR or artifact references and any remaining next step; the coordinator records it in the [shared helper](../devflow/references/state.md). A delegated deliverer writes no Devflow records.

4. **Keep ownership current.** Report the state the coordinator must reflect in [issue ownership and status](../devflow/references/ownership.md): a published candidate is in review, paused or blocked work is identified explicitly, and an issue is done only after its verified outcome and authorized closure. The coordinator updates the tracker and releases its claim when it stops; delegated delivery reports back to that owner.

5. **Close threads with evidence.** For authorized review-thread closure, retain the finding, verify its fix, reply and resolve as required, then read back the thread state. A code change a thread still needs goes back to [coordinating](../devflow-coordinating/SKILL.md) for the [implementation worker](../devflow/references/implementation-worker.md).

6. **Report status separately.** Report implemented, verified, published and merged status separately. State which requested behaviors were observed and which remain unverified, including simulated dependencies. Mark work complete only when the requested endpoint and required verification are observed; a published PR does not establish product success.

## Example status

```text
Implemented: 3f2a1c9. Verified: retry cap and backoff (real client, simulated upstream). Published: PR #48, checks green. Merged: no, merge was not requested.
Issue #12: in review, claim released. Unverified: behavior against the real upstream.
```
