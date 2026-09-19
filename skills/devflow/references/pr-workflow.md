# PR creation and sequential stacks

Implementation owns commits, pushes, PR creation and PR updates. Open a non-draft PR after the first meaningful commit, while work continues. Do not wait for the whole feature, review or QA. State unfinished work and check results in the description; publication is not a claim of completion. Respect explicit local-only, no-commit and no-push limits.

## First PR and subsequent fixes

Inspect the branch, remote and existing PR before creating anything. Push the branch, reuse its open PR or create one with an explicit base, head, title and body file. The body describes the concrete problem, resulting behavior, current checks and remaining work. Keep private artifacts out of public text. Read back the URL, base and head SHA.

Commit repairs to the same branch and push them to the same PR. New commits are sufficient; do not rewrite history merely because a fix was requested. Read back uncertain pushes or PR creation before retrying, and update the description as the implementation changes. Review/verification evidence identifies the base and head assessed; changed code or bases may require rechecking affected behavior.

## One stack for sequential unmerged work

Split features into coherent reviewable PRs when useful. If the next feature or slice starts while the previous one remains unmerged, branch from that previous branch and add the new PR to the same gh stack. This applies to logically independent features developed sequentially too. For `main -> feature-a -> feature-b`, PR A targets main and PR B targets feature-a. Parallel work from separate bases is a separate case, with separate worktrees and ownership.

Use the installed gh-stack skill for command mechanics and inspect the actual local stack state. The sequential-work rule above decides stack membership, including independent features; do not apply a generic recommendation to separate unrelated features when they are being built sequentially on unmerged work. All commands must be non-interactive:

- Adopt the first existing branch with `gh stack init --base TRUNK FIRST_BRANCH`, or continue the existing stack. Do not create a competing stack over branches already tracked elsewhere.
- Start the next layer from the previous unmerged layer with `gh stack add NEXT_BRANCH`.
- After its first meaningful commit, publish with `gh stack submit --auto --open`. It pushes branches, creates or updates PRs and sets their bases. `--open` avoids the command's default draft creation with `--auto`.
- Inspect with `gh stack view --json` and read back PR heads and bases. Replace generated titles/descriptions with accurate ones where needed.
- When branches already exist or have separate worktrees, manage Git branches directly and use `gh stack link --open FIRST_PR NEXT_PR` in bottom-to-top order; append with `gh stack link --open STACK_NUMBER NEXT_PR`. Verify topology before changing an existing base.
- Fix a lower layer on its own branch, then use `gh stack rebase --upstack` and resubmit. Preserve others' commits and coordinate shared branches. Recheck evidence affected by rewritten heads/bases.

After earlier layers merge, sync/prune the stack using the gh-stack skill before deciding the next base. Missing gh-stack, authentication or repository support is a publication blocker to report; do not silently replace a requested stack with unrelated standalone PRs.

## Local-only candidates

Keep the same branch order without publishing. If commits are allowed, identify the candidate by base and head SHA. If commits are forbidden, identify it by worktree, base SHA and an immutable snapshot of all assigned tracked and untracked changes; provide its content hash and full file list. Do not present HEAD as containing uncommitted work. Review and verification must use that snapshot, and later edits invalidate its evidence.

## Completion

The coordinator records publication as it happens. Keep the issue in progress while implementation continues; move it to in review when the candidate is ready for review. Final delivery is the authorized [PR or stack merge](../../devflow-merging/SKILL.md), performed directly by the coordinator.
