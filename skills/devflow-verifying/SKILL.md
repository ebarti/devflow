---
name: devflow-verifying
description: Reproduce a defect, exercise product behavior, run project checks, or verify a repair with observable evidence.
---

# Verify the outcome

Identify the behavior and candidate being checked. Use the target project's existing commands and QA policies directly. Choose checks that can falsify the claim, covering affected integration and failure/recovery paths when relevant. Respect the user's requested scope and explicit check limits.

For product scenarios, record setup, actions, expected and observed behavior, candidate/environment and useful evidence. Use owned isolated resources and preserve unrelated data and processes. Tests that share a runtime or database must restore their owned state; verify cleanup and the affected neighboring scenarios. A screenshot, successful launch or green unit suite proves only what it actually exercised.

Distinguish a product failure from an unavailable tool, blocked environment or skipped assertion. Preserve failed output and explain the evidence boundary. For repairs, reproduce the original trigger and verify the relevant invariant; add regression coverage where the project requires it or it materially protects behavior.

Record each meaningful result and finding with the [shared helper](../devflow/references/state.md). Retain useful artifacts at durable locations and record references, rather than claiming temporary paths remain available. Return the observed outcome and limits; do not turn missing proof into a passing result.
