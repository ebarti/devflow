---
name: devflow-implementing
description: Implement or repair a bounded activated development assignment. Use for code changes and defect repair after scope, ownership and producer identity are established.
---

# Implement the bounded change

Input: an activated verified assignment with work/scope identity, acceptance, exact base, owned paths, constraints and check plan. Output: a producer-bound candidate, findings and implementation completion. A bootstrap child reports metadata and waits; it does not inspect the product yet.

Confirm branch/worktree ownership and dirty state. Preserve unrelated files and others' edits. Read owning docs/code and implement the smallest coherent change. Use the available systematic-debugging skill for defects; identify the invariant and trace source, transform, persistence and presentation before editing. Cosmetic masking cannot repair missing audit data.

Add regression proof and required owning documentation. Run targeted development probes while editing. If another contract/path must change, notify coordinating and persist an amendment before crossing the ownership boundary.

If admitted edits change the workflow pin, profile or captured instructions,
pause before candidate capture. Return actual BLOCKED partial output under this
activation so coordinating can import it, observe availability and amend the
snapshot. Resume through a newly recorded activation of the same worker before
capturing proof under the new policy; never rewrite the old activation's snapshot.

Commit the intended change and capture the clean candidate using the verified assignment/producer. Return candidate identity and implementation evidence for `host result`. The coordinator must durably admit completion **before** registered `check run` or independent gate recording. Then [devflow-verifying](../devflow-verifying/SKILL.md) executes required recipes on the frozen candidate. Do not reverse this order or relabel an unregistered probe as a registered check.

Include `assignment_action_id` from the actual native handoff context in each
implementation result, including BLOCKED partial output. Reused activations
require this binding; a late result from an earlier activation cannot update
the current one. Preserve already imported historical results as recorded.

Record every confirmed code finding, including self-detected defects, with identity, invariant, severity, anchor and evidence. Return limitations accurately. Preserve failed candidates/results. Reuse this assignment for repairs only after the previous independent round's results are imported.

Handoff to [devflow-coordinating](../devflow-coordinating/SKILL.md) with assignment/action/scope/candidate IDs, changed paths, probes, findings and result. Do not award yourself independent review/QA gates or publish through an unjournaled shell command.
