---
name: devflow-planning
description: Plan architecture, dependencies, implementation slices and verification coverage for accepted development work. Use for plan-only requests and consequential changes before coordination.
---

# Plan development work

Input: a defined outcome or admitted scope and owning code/contracts. Output: the smallest coherent plan covering architecture decisions, ordered slices, risk, documentation owners and verification.

For a plan-only request, prepare the plan without admitting execution, creating branches or publishing. For authorized work, do not insert another approval gate. Reuse an accepted sufficient plan. A tiny editorial change proceeds with applicable static checks and its endpoint.

Inspect production roots and changed boundaries. Trace shared contracts across producers and consumers: event, schema, generated type or registry changes require their mirrored owners and parity checks when the project contract requires them. A recipe name, green fixture or changed-file list does not prove this coverage.

Make a compact coverage map: acceptance/invariant → owning contract/path → executable recipe or operational scenario → required independent role. Include failure/recovery behavior and mutable fixture ownership. Record justified omissions; required security, privacy, data-integrity and product-path checks cannot be waived by selecting fewer recipes. Amend the admitted profile/snapshot before executing changed recipes.

Choose slices by dependency and reviewability. Use an approved stack for dependent changes; respect independently released phases and cumulative QA rules. Keep optional improvements outside the outcome. Share bounded ownership with each worker.

If a defect invalidates a planning assumption, preserve failing evidence and return here for the necessary scope/check change. Implementation findings are repaired through implementing; workflow/transport failures are reconciled by their owning stage. Do not restart the work item to clear a failure.

Handoff: send scope, coverage map, exact base/dependencies and endpoint to [devflow-coordinating](../devflow-coordinating/SKILL.md). Send unresolved definition decisions to [devflow-defining-work](../devflow-defining-work/SKILL.md). Persist the coverage map as a private artifact and put its reference in the contract context; list its recipes, scenarios and documentation owners in verification. The coordinator admits or amends these outputs before dependent execution.
