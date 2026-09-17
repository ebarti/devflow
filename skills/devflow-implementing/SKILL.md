---
name: devflow-implementing
description: Implement a defined feature or repair a defect in the requested repository scope.
---

# Implement the change

Execute implementation only as a delegated **Sol / high** worker (`gpt-5.6-sol`, effort `high`). A direct implementation request first follows [coordinating](../devflow-coordinating/SKILL.md) to claim the issue and reuse or spawn that worker. If an existing worker has the wrong model or effort, return to the coordinator for replacement before editing. A correctly configured implementation worker performs its assigned work directly and does not spawn another implementer.

Read the owning code, documentation and project rules. Confirm the working branch and dirty state; preserve unrelated work and collaborators' edits. Make the smallest coherent change that satisfies the requested behavior.

Use the coordinator's work ID and assigned file scope. Never compete with another task's active claim; report results to the owning coordinator.

For defects, reproduce or trace the failing invariant before editing. Follow the data through its source, transformations and consumers; fix the owning layer. A cosmetic change cannot establish a missing persistence or integrity guarantee.

Update affected contracts and documentation. Run the target project's applicable checks in proportion to the change, respecting explicit user limits. Use [verification](../devflow-verifying/SKILL.md) when product behavior needs direct evidence; distinguish a blocked check from a proven defect.

Use the [shared helper](../devflow/references/state.md) to record the actual run, commit/evidence references, unresolved findings and useful continuation context. Preserve failed evidence. Return the candidate, changed scope, checks performed with observed results, and unresolved limits. Keep failed or unperformed checks explicit; do not invent a mandatory extra stage.
