---
name: devflow-verifying
description: Execute meaningful checks and operational/product QA, reproduce a defect, or independently verify a repair. Use when work needs evidence beyond a review or a claimed green test.
---

# Verify the outcome

Input: acceptance, owning contracts, risk/check plan, exact candidate and owned fixtures. Output: actual execution evidence and, for independent QA, its producer-bound PASS/FAIL/BLOCKED.

Direct test/reproduction requests start here within scope. A managed gate needs verified activation through [coordinating](../devflow-coordinating/SKILL.md); a bootstrap child reports metadata and waits. QA identity differs from coordinator, implementer and reviewer.

Check the coverage map against owning contracts. Include mirrored registries, schema consumers and parity requirements when affected. If coverage is missing, return to [planning](../devflow-planning/SKILL.md) and amend it; do not narrow proof to selected green recipes.

For managed implementation, require candidate capture and durable `host result` before registered `check run`. Run the smallest applicable recipes/scenarios; high-risk changes retain the relevant full matrix. Tier 0 uses static checks without invented independent gates. Record process outcomes separately from executed assertions.

Use [retained check artifacts](references/check-evidence.md) for later inspection and handoff. Supply the evidence ID, hash and explicit state root; temporary paths in argv are not enduring evidence locators. Preserve the original artifact when extracting report text into an owned private file.

Exercise the real product/operational path in owned isolated resources. Record setup, actions, expected/observed result, candidate/environment and evidence. Probe failure/recovery where the contract depends on them. A screenshot or green unit suite alone cannot establish persistence, transport or integration.

Missing capability, inaccessible fixture, failed launch or skipped required assertion means FAIL/BLOCKED. Preserve unrelated data/processes and failed evidence. A human-found major regression needs a meaningful fixture or explicit checklist item. QA does not edit implementation while judging it.

Return findings and independent fix observations, then the original verdict with work/scope/candidate/assignment IDs, producer, activation `assignment_action_id`, executed evidence and limits. The coordinator imports every status before repair/rerun; late results cannot satisfy newer candidates. Reuse QA for affected repairs.

Handoff to [coordinating](../devflow-coordinating/SKILL.md) for import and repair/delivery. Do not convert unavailable proof into PASS.

Save the original bare gate-result JSON unchanged as a private artifact before import. The imported record adds only `producer_result_artifact_hash`; the file itself contains no self-hash. The importer checks every original field, including verdict and limits.
