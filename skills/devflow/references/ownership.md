# Issue ownership and parallel work

One coordinating task owns changes and tracker updates for an issue. GitHub shows
the accountable user and the existing Project's Status; the local record identifies the host
task, its locator, branch and latest ownership observation. Reviewers and delegated
workers report to that owner and use the same work ID without claiming it again.

## Start

For substantive implementation, reuse the relevant issue and existing GitHub
Project, unless the request is local-only or uses another tracker. Find the Project
in the task context or repository/issue links; use `gh project list` and
`gh project field-list` to resolve its URL and Status options when needed. Ask only
if the intended Project is ambiguous. Discussion, read-only review and untouched
backlog need no claim.

Run the bundled `github.py` beside `state.py` in the selected installation. The examples use the default location:

```sh
python3.12 ~/.agents/skills/devflow/scripts/github.py start https://github.com/OWNER/REPO/issues/NUMBER \
  --project https://github.com/users/OWNER/projects/NUMBER --work-id WORK_ID --owner HOST_TASK_ID
```

When an issue is needed, use the same helper's `create` command with `--repo`,
`--title`, `--body-file`, `--project`, `--work-id` and `--owner`. Keep its outcome
and acceptance conditions concise. Creation, assignment, Project membership and
Status updates share one path. Retrying the same work ID reuses its saved issue;
an interrupted creation without a saved URL stops for reconciliation. Find the
created issue and resume with `start ISSUE_URL`; do not issue another create.

Use the actual, globally unique host task/coordinator ID, retained across retries;
a generic name such as `/root` cannot distinguish separate tasks. Task locators
supplied through `--source-ref` stay local. The helper claims the issue, assigns the
authenticated user, adds it to the selected Project and verifies its Status.
`--assignee LOGIN` selects another authorized user. The Project URL is retained
in the work record. Use `--project-status 'OPTION'` when the board's existing
option differs from the work status; that mapping is retained for this work.
Use `start --status in-review` for an existing candidate, or `start --status done`
to reconcile an already closed issue when its previous claim was released.
Record the branch once it exists. Other assignees, issue labels and Project fields
are preserved. The helper does not create Projects or Status options.

If another owner holds the claim, inspect that task and continue other independent
authorized work. Do not create a new work ID to bypass ownership of the same issue.

## Update and release

The owner updates status at actual transitions:

```sh
python3.12 ~/.agents/skills/devflow/scripts/github.py set --work-id WORK_ID --owner HOST_TASK_ID \
  --status in-review --release
```

| Status | Meaning and ownership |
| --- | --- |
| `in-progress` | Implementation is underway; retain the claim |
| `in-review` | A candidate awaits or undergoes review; use `--release` when the owning task stops |
| `blocked` | Supply `--reason`; release when ending the task |
| `paused` | Work is paused; the helper releases ownership |
| `done` | Required outcome is verified and the issue is already closed; ownership is released |

Close the issue through `gh` only within the authorized completion scope. Opening
a PR early leaves the issue in progress while implementation continues. Move it
to in review when the candidate is ready for review; publication alone does not
change its status or establish completion. The helper does not close or reopen issues.
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
python3.12 ~/.agents/skills/devflow/scripts/state.py work list --claimed
```

This lists owners and last observations, not live process health. Claims have no
automatic expiry. On interruption, inspect the host task and current GitHub state.
After confirming the old owner has stopped, release its claim with
`state.py work release --id WORK_ID --owner PREVIOUS_HOST_TASK_ID`, then resume with
the same work ID and the new owner. An active owner must release before handoff.

Local-only work uses `state.py work claim --id WORK_ID --owner HOST_TASK_ID` and
`work release` directly. Atomic exclusion covers tasks sharing one SQLite database.
Separate hosts/databases require coordination through GitHub; Project Status and assignees
are not a distributed lock. Status remains the last reported observation after an
abrupt interruption; no heartbeat or background scheduler is installed.
