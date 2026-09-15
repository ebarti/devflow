# Issue ownership and parallel work

One coordinating task owns changes and tracker updates for an issue. GitHub shows
the accountable user and work status; the local record identifies the actual host
task, its locator, branch and latest ownership observation. Reviewers and delegated
workers report to that owner and use the same work ID without claiming it again.

## Start

For substantive implementation in a GitHub repository, reuse the relevant issue
or create one with `gh issue create --assignee @me --body-file ...`, unless the
user requests local-only work or the project explicitly uses another tracker.
Keep its outcome and acceptance conditions concise. Reconcile an uncertain create
before retrying. Discussion, read-only review and untouched backlog need no claim.

Run the bundled `github.py` beside `state.py`:

```sh
python3 ~/.agents/skills/devflow/scripts/github.py start https://github.com/OWNER/REPO/issues/NUMBER \
  --work-id WORK_ID --owner HOST_TASK_ID --source-ref HOST_TASK_LOCATOR
```

Use the actual, globally unique host task/coordinator ID, retained across retries;
a generic name such as `/root` cannot distinguish separate tasks. Task locators
stay in local storage. The helper creates or reuses the work record, atomically
claims the issue, assigns the authenticated GitHub user, sets `status: in progress`
and verifies the remote result. `--assignee LOGIN` selects another authorized user.
Use `start --status in-review` for an existing candidate, or `start --status done`
to reconcile an already closed issue when its previous claim was released.
Record the branch in the work record once it exists.
Other assignees and non-status labels are preserved; missing status labels are
created without changing existing label definitions. No prerequisites are checked.

If another owner holds the claim, inspect that task and continue other independent
authorized work. Do not create a new work ID to bypass ownership of the same issue.

## Update and release

The owner updates status at actual transitions:

```sh
python3 ~/.agents/skills/devflow/scripts/github.py set --work-id WORK_ID --owner HOST_TASK_ID \
  --status in-review --release
```

| Status | Meaning and ownership |
| --- | --- |
| `in-progress` | Implementation is underway; retain the claim |
| `in-review` | A candidate awaits or undergoes review; use `--release` when the owning task stops |
| `blocked` | Supply `--reason`; release when ending the task |
| `paused` | Work is paused; the helper releases ownership |
| `done` | Required outcome is verified and the issue is already closed; ownership is released |

Close the issue through `gh` only within the authorized delivery scope. A published
PR leaves the issue in review. The helper does not create, close or reopen issues.
On `--release`, active local work becomes waiting; completion of a narrower requested
endpoint can still be recorded separately. A failed GitHub update stays visible in
the local record and retains the claim for reconciliation and retry by its owner.

## Concurrency and interruption

Independent issues use separate work IDs and worktrees. A coordinator may own
several issues and dispatch their workers concurrently; each issue has one owner.
Dependencies determine which items are ready. Within one issue,
delegate independent slices with disjoint file ownership. Keep ready work moving
while other items wait for checks, review or input, within the host's capacity.

```sh
python3 ~/.agents/skills/devflow/scripts/state.py work list --claimed
```

This lists owners and last observations, not live process health. Claims have no
automatic expiry. On interruption, inspect the host task and current GitHub state.
After confirming the old owner has stopped, release its claim with
`state.py work release --id WORK_ID --owner PREVIOUS_HOST_TASK_ID`, then resume with
the same work ID and the new owner. An active owner must release before handoff.

Local-only work uses `state.py work claim --id WORK_ID --owner HOST_TASK_ID` and
`work release` directly. Atomic exclusion covers tasks sharing one SQLite database.
Separate hosts/databases require coordination through GitHub; labels and assignees
are not a distributed lock. Status remains the last reported observation after an
abrupt interruption; no heartbeat or background scheduler is installed.
