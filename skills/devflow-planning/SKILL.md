---
name: devflow-planning
description: Plan architecture, dependencies, implementation slices and verification for a consequential development change or a plan-only request.
---

# Plan the change

Use the accepted outcome and inspect the affected production paths, contracts and dependencies. Compare alternatives only where they change correctness, complexity, compatibility or operational risk. Prefer the smallest coherent design.

Choose implementation slices with clear ownership and dependencies. Connect each material acceptance condition to a project check or observable product scenario, including failure/recovery behavior when it matters. Follow the target project's verification policy; Devflow adds no package-wide gate.

Keep the plan proportional. A small fix may need only a few sentences; a migration needs ordering and recovery. Preserve accepted decisions and mark unresolved facts explicitly. A plan-only request produces the plan without initiating implementation or external work.

For ongoing work, save the plan or its durable reference in the [work record](../devflow/references/state.md). When implementation is already authorized, proceed through [coordinating](../devflow-coordinating/SKILL.md) or [implementing](../devflow-implementing/SKILL.md) without another approval step.
