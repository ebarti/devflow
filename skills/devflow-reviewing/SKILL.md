---
name: devflow-reviewing
description: Independently review an existing code candidate or verify a claimed repair. Use for direct review requests and activated review gates, including all PASS, FAIL and BLOCKED results.
---

# Review independently

Input: a specific candidate/PR and acceptance/contracts; for a managed gate, its verified independent assignment and activation action. Output: evidence, every confirmed finding, fix observations, limits and a candidate-bound verdict.

A direct read-only review begins here without inventing implementation. For a managed gate, use [coordinating](../devflow-coordinating/SKILL.md) to establish the entry phase and verified role. A bootstrap child reports metadata and waits. The reviewer differs from coordinator, implementer and QA, and does not repair the candidate being judged.

Confirm repository, base/head/tree, scope and activation. Review correctness, security, data integrity, compatibility and meaningful coverage. Try to prove a suspected defect harmless before confirming it. Record severity, invariant, exact anchor, trigger, impact and evidence for every confirmed finding, including lower severities.

Inspect [retained check artifacts](../devflow-verifying/references/check-evidence.md) by their recorded hashes and explicit state root. Read their original output/JUnit fields; paths in command argv may have been removed after execution. Reuse unchanged-candidate evidence without rerunning a passing check to recreate its temporary report.

Verify repairs from the candidate and regression evidence. Return independent fix observations before final gate evaluation; a resolved GitHub flag is not technical fix proof.

Produce the original structured PASS/FAIL/BLOCKED with work/scope/candidate/assignment IDs, `assignment_action_id`, producer, evidence, findings and limitations. PASS requires applicable evidence and no unresolved blockers. Every status must be imported through `gate record` before repair or advancement. A late result remains bound to its original activation as history.

Handoff to [coordinating](../devflow-coordinating/SKILL.md) for durable import, then [implementing](../devflow-implementing/SKILL.md) for repairs or [delivering](../devflow-delivering/SKILL.md) for publication. Reuse this reviewer for affected reruns. A technical verdict grants no merge authority.

Save the original bare gate-result JSON unchanged as a private artifact before import. The imported record adds only `producer_result_artifact_hash`; the file itself contains no self-hash. The importer checks every original field, including verdict and limits.
