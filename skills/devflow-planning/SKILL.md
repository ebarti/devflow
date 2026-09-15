---
name: devflow-planning
description: Plan architecture, dependencies, implementation slices and verification for a consequential development change or a plan-only request.
---

# Plan the change

Use the accepted outcome and inspect the affected production paths, contracts and dependencies. Compare alternatives only where they change correctness, complexity, compatibility or operational risk. Prefer the smallest coherent design.

Split consequential work into slices with one observable outcome, owned files/modules, dependencies and concise verification steps with expected results. Map the user's acceptance conditions to observable results, including each required mode and relevant failure/recovery path. For product-use claims, identify the actual entry point to exercise and label simulated dependencies; simulation cannot satisfy a requirement to exercise the real path. Follow the target project's verification policy; Devflow adds no package-wide gate.

Keep the plan proportional. A small fix may need only a few sentences; a migration needs ordering and recovery. Preserve accepted decisions and mark unresolved facts explicitly. A plan-only request produces the plan without initiating implementation or external work.

For ongoing work, save the plan or its durable reference in the [work record](../devflow/references/state.md). When implementation is already authorized, proceed through [coordinating](../devflow-coordinating/SKILL.md) or [implementing](../devflow-implementing/SKILL.md) without another approval step.
