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
SDK's `WorkerDeploymentConfig` with `VersioningBehavior.PINNED`. Without the flags,
worker registration is unchanged. These options do not change admission specs,
configuration files, ownership hashes, repair budgets, or historical preparations.
The version appears in the process manifest and readiness record. Readiness still
requires both registered pollers and the owned process identity. Starting another
version while a worker is recorded fails without restarting it. A cold restart
reuses the explicit selection only when the recorded `runtime_payload_sha256`
matches the current package. Missing or changed source identity rejects startup
before any process is signalled or launched. This prevents silently registering
changed source under a pinned version or losing versioning after a crash.
Unversioned historical manifests keep their original behavior; they are not
upgraded by a restart. To change an artifact, the owner must first drain and stop
its existing lifecycle, then explicitly select the new version.

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
3. Drain native activities before changing a worker. The current service owns
   one worker/readiness lifecycle. Run compatible retained artifacts sequentially,
   preserving the original namespace, queue, configurations and transport. Wait
   for both pollers to register; a deployment override issued before registration
   is rejected. Confirm the deployment via Temporal's public readback.
4. For each execution whose complete history passed that artifact's replay, the
   owner can set an explicit pinned override with the public Temporal options API:

   ```sh
   temporal --address "$ADDRESS" --namespace "$NAMESPACE" workflow update-options \
     --workflow-id "$WORKFLOW_ID" --run-id "$RUN_ID" \
     --versioning-override-behavior pinned \
     --versioning-override-deployment-name "$DEPLOYMENT" \
     --versioning-override-build-id "$REPLAY_QUALIFIED_VERSION"
   ```

   Read back the override and observe a new completed workflow task and actual
   progress. An accepted routing update alone proves no resumption. Retain failure
   history. Existing native source, interpreter, dependency and resource guards
   still apply: a workflow replay pass cannot override a preparation mismatch.
   Do not refresh frozen inputs, waive checks, or adopt a different transport to
   make the migration pass.
5. Drain each retained cohort before switching this single worker lifecycle to
   another artifact. Only after historical executions are accounted for, register
   the current worker with an explicit deployment version, verify its two pollers,
   and set that version current through `temporal worker deployment
   set-current-version`. Then reopen admissions. New executions pin to the selected
   version; future deployments must retain workers for versions still in use.

A history spanning incompatible unmarked implementations may replay on neither
artifact. None has been observed by these fixtures. If encountered, report the
complete failing history: pinning does not repair nondeterminism. Such executions
require a separate owner decision before the old artifact can be retired. This
PR performs no live deployment, override, reset, history edit or native recovery.

The installed Python SDK supports these deployment APIs. See Temporal's
[Worker Versioning documentation](https://docs.temporal.io/worker-versioning) and
[Python Worker deployment options](https://python.temporal.io/temporalio.worker.Worker.html).
