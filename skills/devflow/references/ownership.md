# Issue ownership and parallel work

The main task holds one claim per issue. Its execution coordinator acts for that
owner and maintains records and tracker updates while running, using the supplied
owner ID and shared database. The main task handles the initial claim and recovery;
do not run competing tracker updates from both levels. GitHub shows the accountable
user and existing Project's Status. Leaf workers use the same work ID and never
claim it again or update the tracker.

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

Use the main task's actual, globally unique host ID, retained across retries;
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
endpoint can still be recorded separately. A failed GitHub update retains a durable
intent with its error and bounded retry. The service reads back before retrying;
unknown mapping or semantics remain one explicit decision.

Run `github.py audit --work-id WORK_ID` before resuming or ending an owned issue.
It reads the local work, claim and owner runtime plus the live issue, assignee and
selected Project item. A successful sync stores its expected issue state, assignee,
Project/item/Status IDs, Status name and readback time in `details.github.sync`.
Audit compares both the Status ID and current name, and flags active linked work
whose claim has been released. Older records
without that metadata report `unknown` while observing any discoverable selected
Project item; an audit never treats missing history as
a pass. The command changes neither GitHub nor SQLite and exits nonzero for an
unknown or reconciliation-required result. The service retries mechanical
differences from recorded intent. Use `github.py set` once for a new semantic
status or mapping, then audit its readback. A failed API read also exits nonzero.

For an external Actions handoff, set `--status blocked --reason REASON` with
`--await-url ACTIONS_RUN_URL --follow-up 'OWNER checks when TRIGGER occurs'`.
The run URL and concrete follow-up survive in the work record. The service checks
the run without model polling. Terminal success moves to configured In review;
failure moves to configured Blocked with the observed conclusion. Both require
issue, assignee and Project readback and retain an `external_outcome` next action
until the owner records a semantic decision. Success does not approve or close the
issue. Legacy `details.release_run` URLs remain read-only compatibility evidence.

## Concurrency and interruption

Independent issues use separate work IDs and worktrees. A main task may own
several issues and hand a bounded batch to one execution coordinator; each issue has one owner.
Dependencies determine which items are ready. Within one issue,
delegate independent slices with disjoint file ownership. Keep ready work moving
while other items wait for checks, review or input, within the host's capacity.

```sh
python3.12 ~/.agents/skills/devflow/scripts/state.py work list --claimed
```

This lists owners and last observations, not live process health. Claims have no
automatic expiry. A normal root Stop does not start a repeated reconciliation
conversation. Observed root Interrupt or SessionEnd blocks claimed work locally
and queues linked managed issues; hooks make no network call. The service waits
for terminal root and descendant evidence before releasing a claim. A new prompt
from the same root session reopens its runtime generation and fences old recovery.
A hard crash or missing hook remains unknown. Unresolved issue creation stays
`create_pending` until its actual outcome is bound. An active owner must release
before handoff.

If an execution coordinator returned blocked and released its claim, the main task
reclaims the same work before resuming that coordinator. A reply target such as
`/root` routes messages but is not an ownership ID. Pass both values explicitly.

Local-only work uses `state.py work claim --id WORK_ID --owner HOST_TASK_ID` and
`work release` directly. Atomic exclusion covers tasks sharing one SQLite database.
Separate hosts/databases require coordination through GitHub; Project Status and assignees
are not a distributed lock. Status remains the last reported observation after an
abrupt interruption without terminal evidence. The launchd service drains managed
issue intents on macOS; `reconcile.py once` is available elsewhere. It never
calls a model or infers acceptance from PR or CI state.
