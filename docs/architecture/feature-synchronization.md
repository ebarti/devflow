# Feature state and GitHub synchronization

The feature's current status is canonical. GitHub Project Status is an exact mirror of
that value; delivery phase, historical outcome, publication receipt, and execution
ownership are separate facts. GitHub supplies current PR lifecycle facts. Observing a
merge does not rewrite a historical delivery outcome or authorize closing an issue.

## Root cause

The observed failure was a merged PR whose delivery receipt still said OPEN, whose
work was waiting, and whose Project said In review, while audit reported consistent.

1. PR receipts are written at publication, so later merges were invisible.
2. Run cards and audit read those receipts and the saved Project mapping as current truth.
3. Audit compared the Project to that mapping, and skipped local status comparison for waiting.
4. The failure appeared after external PR changes and after claim release changed active to waiting.
5. There was no independently reconciled current feature record. Execution ownership,
   publication history, and business status were serving different purposes under one label.

The fix belongs at the state/event and external-projection boundaries. Runtime
transactions record feature changes durably. A separate synchronizer observes PRs
and projects current feature status. Remote availability must not govern execution
ownership or consume delivery repair cycles.

## Contract

- Each runtime database keeps its original immutable run specifications and receipts.
- Local state transitions and their feature events commit together. Publication receipts
  remain available independently of current PR observations.
- The synchronizer is a separate process with an explicit list of runtime configurations.
  It takes exclusive locks for every source database and Project, preventing overlapping consumers.
- For an issue with attempts in several sources, the most recently admitted run owns the
  feature. Updating an older run does not steal ownership. The worker publishes the same
  selected feature view to every participating source.
- The worker consumes committed changes, coalescing obsolete projections, with durable
  retry state. An acknowledged mirror requires remote readback of the exact status and
  assignee. A failed mirror remains pending and does not change the feature's status.
- Known PRs are checked about every five minutes, including historical receipts. Their
  observation schedule survives restarts. No agents or product repairs are dispatched.
- PR state, head changes, Project drift, duplicate events, and worker restarts reconcile
  idempotently. Unknown remote observations retain the last known fact with an error and
  timestamp; they never imply successful synchronization.
- New deliveries acknowledge local tracking transitions without waiting for GitHub.
  Historical run data is not rewritten to adopt new execution policies.
- Project Status option names match the local feature status literally. Creating missing
  options preserves the existing option IDs and values; there are no status aliases.

Post-publication merge authorization, stack merge execution, repair-budget changes,
and learning retrospectives are separate changes. This component observes merge facts
but does not merge PRs or start deliveries.

## Running the independent worker

Create a private JSON configuration listing every runtime that can own the same
features. Do not run separate consumers with disjoint owner lists for the same Project.
The list is intentionally explicit; the worker never scans arbitrary local databases.

```json
{"version":1,"owners":["/absolute/runtime/service.json"],"pr_interval_seconds":300}
```

Use `devflow-project-sync --config /absolute/project-sync.json --once` for one
reconciliation or omit `--once` for the durable loop. The loop checks only committed
local state every two seconds. GitHub observation and drift checks default to five
minutes. Project write failures back off from 15 seconds to one hour.

On macOS, install from the reviewed runtime checkout using its virtualenv Python:
`python scripts/project-sync-service.py install --config /absolute/project-sync.json`.
The service starts independently of the delivery controller and starts again at login
through launchd. Its exact source revision and configuration digest are activation
fences. Before upgrading either source checkout or backing up its databases, stop this
service as well as the delivery writers. Reinstall it after the reviewed upgrade;
a stale service cannot silently start a new revision. `inspect` reports its saved
configuration and `stop` stops the owned service without deleting evidence.

Historical compatibility: a resumed workflow admitted before asynchronous tracking
retains its original activity contract while active. The projector yields Project
ownership to that workflow until it stops, then resumes the canonical mirror. Fresh
admissions always use asynchronous tracking. This exception preserves replay and
recovery without modifying frozen specifications or historical acknowledgements.
