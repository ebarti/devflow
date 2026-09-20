---
name: devflow-implementing
description: Implement a scoped feature or repair, open its PR early and push subsequent fixes to that PR.
---

# Implement the change

Direct implementation requests enter through [coordinating](../devflow-coordinating/SKILL.md): the main task inspects and plans, then its execution coordinator supplies the [worker brief](../devflow/references/implementation-worker.md). As the assigned implementation worker, use that outcome, worktree, file scope, branch/base and checks; perform the following steps without spawning agents.

1. **Inspect before editing.** Read the owning code, project instructions and relevant contracts. Confirm branch and dirty state. Preserve collaborators' edits and unrelated files. For a defect, reproduce or trace the failing invariant and fix the owning layer.

2. **Make a coherent change.** Satisfy the assigned behavior without unrelated refactors, upgrades or formatting. Update affected tests, contracts and documentation within the assigned scope. Return a concrete scope gap if the correct repair exceeds it.

3. **Open the PR early.** For GitHub work, commit and publish the first meaningful change immediately. Use a non-draft PR with a description of implemented behavior, remaining work and checks actually performed. Reuse an existing PR instead of opening another for repairs. Follow [PR workflow](../devflow/references/pr-workflow.md); explicit local-only/no-commit/no-push limits take precedence.

4. **Build the stack as work progresses.** Split a feature into reviewable slices. Start each sequential feature or slice from the previous unmerged branch and publish it in the same gh stack, even when features are logically independent. Each PR targets its predecessor. Repair a lower layer on its own branch, then rebase and resubmit affected upper layers; coordinate any shared branches before rewriting them.

5. **Check within scope.** Run the agreed checks and appropriate project checks permitted by the user's limits. Do not run tests for a no-tests request. Record commands, expected/observed behavior, failed checks and unperformed checks honestly. Keep evidence at durable paths.

6. **Push repairs and identify the candidate.** Commit further fixes to the same branch, push them to the same PR, update its description and read back base/head SHAs. Verify the remote head contains the reported changes. Separate dirty/untracked work from the committed candidate. With no-commit scope, return the base SHA, worktree, full changed/untracked file list and a durable patch/snapshot with a content hash covering the assigned changes.

7. **Hand off.** Return candidate identity, PR/stack references, changed scope, check outcomes and remaining work. The coordinator records results and updates the issue. Do not merge, release or deploy as part of implementation unless separately assigned.
