# Worker deployment migration

New executions record `local-checks-before-review-v1` before local checks. Earlier
unmarked review-first executions replay with that marker absent. The intervening
unmarked checks-first implementation cannot replay on this worker: its next
activity differs before any distinguishing patch marker. An SDK-generated build
ID, issue number, or frozen request does not identify that order reliably.

Replay tests preserve both genuine recorded orders and require the retained
checks-first artifact to replay its own histories. They explicitly check that the
current worker rejects that cohort. The retained source archive is test data,
not a deployable native environment. The routing test exercises original workflow
code and fixture activities; it does not prove native preparation compatibility.

## Deployment options

`start` and `worker` accept optional `--deployment-name` and
`--deployment-build-id`. Both are required together. They configure the Python
SDK's `WorkerDeploymentConfig` with `VersioningBehavior.PINNED`. For a lifecycle
without a stored selection, registration is unchanged when flags are absent.
These options do not change admission specs,
configuration files, ownership hashes, repair budgets, or historical preparations.
The selection is stored at the top level of the existing process manifest before
any launch, separately from PIDs, and is also included in worker readiness. A
stop retains that selection with an empty process inventory. Failed and partial
starts preserve it; later implicit starts reuse it only when the recorded source
digest matches the package. Unknown or changed identity rejects startup and stop
before readiness, signalling or launching. Unversioned historical manifests keep
their original behavior. An explicit different version can replace a selection
only after the previous lifecycle has been drained and stopped to an empty
inventory. Selecting the same version never refreshes its source digest.

A version name denotes one immutable, replay-qualified source/dependency artifact.
Never reuse it for different bytes. These flags register the worker; they do not
change the server's current-version routing or override existing executions.

For an unchanged retained package, `scripts/run-versioned-worker.py` is an external
bootstrap adapter. Run it with that artifact's frozen interpreter/dependencies:

```sh
"$FROZEN_PYTHON" -B scripts/run-versioned-worker.py \
  --runtime-src "$RETAINED_RUNTIME_SRC" --config "$ORIGINAL_CONFIG" \
  --deployment-name "$DEPLOYMENT" --deployment-build-id "$RETAINED_VERSION"
```

The adapter supplies only the standard SDK Worker deployment option. It imports
the retained workflow and activity registrations and does not edit package files.
It uses the existing single `worker-ready.json` lifecycle; do not launch concurrent
adapters for one owner. An external supervisor must preserve process ownership.
It does not start an API, submit work, choose a cohort, migrate data, or change an
execution override.

## Operator steps before deploying

1. Quiesce admissions, dispatch and clients that automatically start the service.
   Inventory every open execution, including nested continuations and outstanding
   workflow/activity tasks. Preserve Temporal persistence and original packages,
   locked dependencies, interpreter paths and native preparation evidence.
2. Export complete histories through Temporal's public history API. Replay each
   against the current implementation and retained original artifacts. Classify
   by full successful replay, not phase, input text, marker fragments or SDK build
   IDs. Record incompatibilities without rewriting history. The included source
   archive demonstrates the intervening order; use the full retained deployment
   artifact for native execution.
3. Drain all workflow and native activity work before changing artifacts. The
   bundled lifecycle owns Temporal, the API and one worker together: `stop` stops
   all three, and there is no worker-only stop or simultaneous version support.
   Never run the retained adapter beside that worker; they share
   `worker-ready.json`. A whole-stack stop cannot keep the bundled Temporal server
   available to an adapter. The adapter is only usable with an independently
   supervised Temporal server and exclusive worker/readiness ownership supplied
   by the owner; this tooling does not provision that migration environment.
4. In such an owner-managed migration environment, register only the
   replay-qualified retained artifact. Verify both pollers and readiness ownership
   before setting a replay-qualified execution's explicit pinned override through
   Temporal's public workflow options API. Read back the override and observe a
   completed workflow task and actual progress; an accepted routing update alone
   proves no resumption. Frozen preparation/source/dependency guards still apply.
   If the retained artifact cannot resume under them, keep the incompatibility
   unresolved; do not refresh frozen inputs or waive checks.
5. Finish every retained execution and native task before retiring its artifact.
   After full drain, stop that entire owned lifecycle. Explicitly select the new
   artifact/version with `start --deployment-name ... --deployment-build-id ...`,
   verify its pollers/readiness, then set it current through Temporal's public
   deployment API and reopen admissions. Pinned mode requires this full drain
   again for subsequent upgrades with this single-worker tooling; it cannot keep
   old and new versions serving concurrently. Preserve persistence and retained
   artifacts until all executions are accounted for.

A history spanning incompatible unmarked implementations may replay on neither
artifact. None has been observed by these fixtures. If encountered, report the
complete failing history: pinning does not repair nondeterminism. Such executions
require a separate owner decision before the old artifact can be retired. This
PR performs no live deployment, override, reset, history edit or native recovery.

The installed Python SDK supports these deployment APIs. See Temporal's
[Worker Versioning documentation](https://docs.temporal.io/worker-versioning) and
[Python Worker deployment options](https://python.temporal.io/temporalio.worker.Worker.html).
