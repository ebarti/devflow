---
name: devflow-verifying
description: Reproduce a defect, exercise product behavior, run project checks, or verify a repair with observable evidence.
---

# Verify the outcome

Start from the user's acceptance conditions and current candidate. Use the target project's existing commands and QA policies, respecting explicit scope and check limits. For a product-use claim, exercise the actual application entry point in each required mode and inspect the user-visible or persisted result. Include failure/recovery behavior when relevant.

Record the candidate/environment, setup, actions, expected and observed result, and any simulated dependencies. Use owned isolated resources and preserve unrelated data and processes. Tests sharing a runtime or database must restore their owned state; verify cleanup and affected neighboring scenarios. Passing checks, healthy services and simulated dependencies do not establish unexercised product behavior.

For each acceptance condition, report verified, failed or unverified with its evidence. Continue required verification within the authorized scope; if blocked, name the unexercised scenario and the blocker. For repairs, reproduce the original trigger, verify the invariant and retain failed output. Add regression coverage where the project requires it or it materially protects behavior.

Record meaningful results and findings with the [shared helper](../devflow/references/state.md). Retain useful artifacts at durable locations and record their references. Return the observed outcomes and limits; an unverified required scenario keeps verification incomplete regardless of other passing checks.
