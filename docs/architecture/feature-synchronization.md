# Feature state and GitHub synchronization

The feature's current status is canonical. GitHub Project Status is an exact mirror of
that value; delivery phase, historical outcome, publication receipt, and execution
ownership are separate facts. GitHub supplies current issue completion and PR lifecycle facts. Observing a
merge does not rewrite a historical delivery outcome or authorize closing an issue.

## Contract

- Each runtime database keeps its original immutable run specifications and receipts.
- Local state transitions and their feature events commit together. Publication receipts
  remain available independently of current PR observations.
- After committing, Devflow sends a nonblocking local notification to the independent
  sync service. It never waits for a service acknowledgement or a GitHub response.
  Notification failure cannot fail the delivery; the durable event remains available.
- The synchronizer is a separate process with an explicit list of runtime configurations.
  It takes exclusive locks for every source database and Project, preventing overlapping consumers.
- For an issue with attempts in several sources, the most recently admitted run owns the
  feature. Updating an older run does not steal ownership. The worker publishes the same
  selected feature view to every participating source.
  The source path breaks ties between copied admissions; version counters are compared
  only within the same source. Handoff holds the legacy per-work reconciliation locks
  and publishes pending ownership everywhere before any remote effect.
- The worker consumes committed changes, coalescing obsolete projections, with durable
  retry state. An acknowledged mirror requires remote readback of the exact status and
  assignee. A failed mirror remains pending and does not change the feature's status.
- Admitted issues, known PRs and Project drift are checked every five minutes by default. Their
  observation schedule survives restarts. No agents or product repairs are dispatched.
- PR state, head changes, Project drift, duplicate events, and worker restarts reconcile
  idempotently. Unknown remote observations retain the last known fact with an error and
  timestamp; they never imply successful synchronization.
- New deliveries acknowledge local tracking transitions without waiting for GitHub.
  Terminal publication still requires its PR custody readback before local acknowledgement.
  Historical run data is not rewritten to adopt new execution policies.
- Project Status options reuse case-insensitive matching names, so an existing
  `In Progress` option does not create a second `In progress` column. Creating missing
  options preserves existing IDs and values.
- A GitHub issue closed as completed supplies current completion evidence. Bound PRs
  observed merged produce `Merged`; completion without a complete merged-PR record
  produces `Done`. This never rewrites the old attempt, its checks or scope receipt.
  Reopening an issue removes that external completion evidence. A closed unmerged PR
  remains `PR closed`; an intervention needed by the current attempt remains `Blocked`.
- The dashboard contains admitted Devflow issues. The GitHub Project also contains
  backlog items and work executed elsewhere; absence from this installation is not
  evidence of completion.

Post-publication merge authorization, stack merge execution, repair-budget changes,
and learning retrospectives are separate changes. This component observes merge facts
but does not merge PRs or start deliveries.

## Running the independent worker

Create a private JSON configuration listing every runtime that can own the same
features. Do not run separate consumers with disjoint owner lists for the same Project.
The list is intentionally explicit; the worker never scans arbitrary local databases.

```json
{"version":1,"owners":["/absolute/runtime/service.json"],"pr_interval_seconds":300,"project_interval_seconds":300}
```

Use `devflow-project-sync --config /absolute/project-sync.json --once` for one
reconciliation or omit `--once` for the durable service. The service binds an owned
Unix datagram endpoint for every configured database before its initial catch-up,
then waits for notifications or the next durable retry/check deadline. It does not
poll local state on a two-second timer. Notifications carry no state or authority;
the service always reads committed SQLite state, coalescing duplicate notifications.
Periodic reconciliation also recovers a writer crash between commit and notification.
A stopped service catches up immediately on restart.

GitHub issue/PR observation and Project drift checks default to five minutes, independently
configurable from 300 to 86400 seconds. Newly recorded PRs and newly published heads
on existing PRs trigger an immediate observation. The consumed publication identity
is durable, so duplicate notifications and service restarts do not repeat that read.
An upgrade from the older observer establishes these identities with one initial
read of each recorded PR. Local status changes are mirrored immediately when the service is
available, regardless of these intervals. Configuration changes rebase existing
periodic deadlines once; pending Project retries keep their existing backoff.
Project write failures back off from 15 seconds to one hour without involving the
delivery controller or consuming repair cycles.

PR head observation checks which commit was validated. Updating a local checkout
does not validate an additional commit pushed after review. A changed published
head therefore requires validation, even when a local checkout has been updated.

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
Older immutable helper installations without the feature ownership guard must remain
activation-fenced or be upgraded before enabling this service; the new ownership view
cannot fence code that does not read it.
