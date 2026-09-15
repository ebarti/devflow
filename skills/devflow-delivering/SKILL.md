---
name: devflow-delivering
description: Publish, merge, release or install a requested development outcome, close verified review findings, or reconcile interrupted delivery.
---

# Deliver the outcome

Establish the requested endpoint and current candidate. Follow target project policies and the user's existing authorization. Complete authorized preparation without another approval gate; do not expand a request to push into permission to merge, release or deploy.

Use Git, `gh`, host tools and project commands directly. Preserve unrelated changes and use the project's commit and PR conventions. Describe the concrete problem, resulting behavior, relevant checks and material limits. Keep private artifacts and credentials out of public output.

Inspect the actual remote or installation state after each meaningful delivery action. When an interrupted action may have succeeded, read its state before retrying. Record what actually happened in the [shared helper](../devflow/references/state.md), including commit, PR/artifact references and any remaining next step.

Keep [issue ownership and status](../devflow/references/ownership.md) current: a published candidate is in review, paused or blocked work is identified explicitly, and an issue is done only after its verified outcome and authorized closure. Release the coordinating task's claim when it stops; delegated delivery reports back to that owner.

For authorized review-thread closure, retain the finding, verify its fix, reply and resolve as required, then read back the thread state. Report implemented, verified, published and merged status separately. State which requested behaviors were observed and which remain unverified, including simulated dependencies. Mark work complete only when the requested endpoint and required verification are observed; a published PR does not establish product success.
