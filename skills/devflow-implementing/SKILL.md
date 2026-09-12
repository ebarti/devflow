---
name: devflow-implementing
description: Implement a defined feature or repair a defect in the requested repository scope.
---

# Implement the change

Read the owning code, documentation and project rules. Confirm the working branch and dirty state; preserve unrelated work and collaborators' edits. Make the smallest coherent change that satisfies the requested behavior.

For defects, reproduce or trace the failing invariant before editing. Follow the data through its source, transformations and consumers; fix the owning layer. A cosmetic change cannot establish a missing persistence or integrity guarantee.

Update affected contracts and documentation. Run the target project's applicable checks in proportion to the change, respecting explicit user limits. Use [verification](../devflow-verifying/SKILL.md) when product behavior needs direct evidence; distinguish a blocked check from a proven defect.

Use the [shared helper](../devflow/references/state.md) to record the actual run, commit/evidence references, unresolved findings and useful continuation context. Preserve failed evidence. Hand off the concrete candidate and limits when review or delivery is part of the requested work; do not invent a mandatory extra stage.
