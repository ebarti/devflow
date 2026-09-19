---
name: devflow-merging
description: Merge an authorized PR or gh stack after checking its current revisions and evidence; reconcile interrupted merges.
---

# Merge the PR or stack

The execution coordinator performs an authorized merge directly. For a standalone merge-only request, the main task performs it directly without spawning an execution coordinator. Implementation has already opened the PRs and pushed repairs. This skill creates no delivery agent and is not a publication stage.

1. **Resolve the target and authorization.** Identify the requested PR or stack, including exactly which members may merge. Reuse the user's existing authorization. A request to implement or publish does not authorize a merge. Inspect remote state first when a previous attempt may have succeeded.

2. **Check the current revisions.** Read every in-scope PR's head, base, checks, review state and unresolved findings. Match the relevant review/verification evidence to those revisions and the accepted scope. A changed head/base requires reassessment of affected evidence; send any code repair to the original implementer. Do not turn this into another full review or duplicate test run when evidence is current.

3. **Use the correct merge command.** For a standalone PR, use `gh pr merge PR --match-head-commit HEAD_SHA` with the repository's merge method. For a stack, use the installed gh-stack skill and `gh stack view --json`, then `gh stack merge STACK_OR_LAST_PR --yes` with the intended method. That target includes all preceding members: verify the included set before calling it. Never merge stacked PRs individually with `gh pr merge`. The stack command has no equivalent head-match flag; read its current heads immediately before merging and rely on GitHub's required checks and protections, without claiming an atomic evidence lock.

4. **Read back the result.** Verify the remote state for every requested PR. A merge-queue submission is queued, not merged. Record merge commits when present; do not report completion from a successful command alone. Inspect an uncertain result before retrying.

5. **Reconcile ownership.** Update the [issue and claim](../devflow/references/ownership.md) and retain references in the [work record](../devflow/references/state.md). Close an issue only when its requested outcome and closure are satisfied. A subset merged from a larger stack does not complete the remaining work.

Report the target, observed heads, merged/queued/blocked state, merge commits and any remaining action. Release, deployment and installation are separate explicit requests, not automatic follow-up stages.
