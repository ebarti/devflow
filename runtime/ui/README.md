# Devflow local dashboard

This React/TypeScript dashboard is a static client for the local Devflow service. It never determines workflow completion; every state and gate is rendered from the service projection. `src/api.ts` is the only HTTP contract adapter.

## Build and serve

```sh
cd runtime/ui
npm ci
npm run build
npm test
```

The production output is `runtime/ui/dist/`. The Python service should serve `dist/index.html` at `/`, `/runs/{run_id}`, `/new`, `/settings`, and `/statistics`, with the hashed `dist/assets/*` files at `/assets/*`. A missing bundle is a service startup/build error, not an empty dashboard. All `/api/*` calls and the SSE stream stay on the same origin. The source and package lock are committed; `dist/` and `node_modules/` are generated and ignored.

For local UI development, run `npm run dev -- --port 5178` against the Python service through a same-origin proxy or serve the built output from Python. `tests/fixture-service.mjs` is a **test-only mock service**, never an application data source. It can be started with `node tests/fixture-service.mjs`. Its data has `fixture` labels and must never be copied into production responses.

## HTTP contract

Reads and SSE require no login. Before a command, the browser bootstraps or renews an anonymous HttpOnly, SameSite=Strict cookie through `POST /api/session` and receives `{csrf_token}`. The CSRF value stays in memory. Writes send `X-Devflow-CSRF`; the server enforces the session, loopback peer, Host and Origin. A stale session renews automatically; a 409 conflict refreshes the run and requires the user to review the new revision before sending a new command.

| Request | Response used by UI |
| --- | --- |
| `GET /api/runs?archived=false&limit=50&cursor=...` | `{runs:[{id, title?, goal?, repository?, issue?, phase?, execution_state?, archived?, updated_at?, revision?}],next_cursor:string\|null}`; `archived` defaults to false, `limit` defaults to 50 (1–100), and the first request omits `cursor` |
| `GET /api/runs/{id}` | `{run, events, evidence}`; `run` has `id`, nullable Temporal `revision`/`protocol_revision`, `projection_revision`, `iteration`, `sequence`, ordered `phase_gates`, `roles`, `capacity:{limit,active}`, boolean `queued`, top-level `cleanup`, `candidate`, `pull_request`, `checks`, `tracker`, role-keyed `usage`, and `decisions` with string `options` and optional blocking `blocker:{unknown,evidence_checked,why_no_safe_default}`, plus `question_notifications` with decision identity, sender `state` and optional acknowledgement/error `receipt` |
| `GET /api/runs/{id}/events?after=N` | SSE `event: update`, numeric `id`, optional JSON `sequence`; snapshot refetched on new event and reconnect |
| `GET /api/runs?archived=true` | The same paged response for archived tasks; continue with its `next_cursor` and the same archive filter |
| `GET /api/statistics` | All durable runs grouped by recorded release, revision, local source digest and provider; outcomes, repairs, duration and observed usage with coverage |
| `GET /api/service` | Service health, version, Temporal, capacity, and `policy` containing allowlisted `repositories` and role settings |
| `POST /api/runs` | Revisioned raw-goal command with `command_id`, `run_id`, `work_id`, `issue_url`, `repository_key`, `goal`, `base_ref`, `branch`, `authorized_endpoint: published_unmerged`, optional `plan_approval: automatic | required` (new requests default to automatic), legacy `accepted_plan` and `recovery_key`; returns `{run_id,dashboard_url,existing,phase}` |
| `POST /api/runs/{id}/decision` | `{command_id,expected_revision,decision_id,decision_revision,candidate_revision,answer}` |
| `POST /api/runs/{id}/cancel` | `{command_id,expected_revision,reason}` |
| `POST /api/runs/{id}/archive` | `{command_id,expected_revision,archived}`; reversible preference for stopped tasks, retaining evidence and metrics |
| `POST /api/runs/{id}/steer` | `{command_id,expected_revision,message}`; durable instructions for subsequent role launches within the frozen authority |

The service owns repository paths, role models, checks, authorization scope, and state transitions. The New run form only selects an allowlisted repository and recovery key from `/api/service`. Decision and cancellation commands use the observed `protocol_revision` and remain disabled before it exists. Archive and steering commands use `projection_revision`, including on a queued run before Temporal starts. Command IDs bind their exact body; after an uncertain transport result, the UI retries that same body and revision, rather than inventing a new mutation. The UI adapts `base_sha` and `content_sha256` for candidate display, sums only numeric role usage readings, and separates local checks from independent review and QA. It displays missing telemetry as unknown, pending tracker readback as pending, and a lost SSE or fetch connection as disconnected with the last observation retained.

## Board, steering and statistics

The default Runs page is a status board refreshed every five seconds with one recent page of 50 tasks. Runs are ordered by `updated_at` then run ID descending. **Load older tasks** requests one more page using the opaque `next_cursor`; `null` means no further page. Cursors belong to their archive filter. After paging starts, previously observed rows remain cached when they leave the recent page. Polling does not refetch older pages; detail reads and stream snapshots reconcile loaded rows, including archive/restore preferences. Switching collections clears the cache. If the entire recent page changes, a notice restarts explicit paging from the new boundary to recover potentially skipped rows while retaining loaded observations. Statistics continue to use complete durable history.

Cards follow authoritative workflow phases; selecting a card opens its evidence and controls. Refreshing or archiving retains the selected detail URL. The archive toggle switches collections without deleting a run. Only run detail shows the recent-runs rail; Settings, Statistics and New run use the full content area.

Steering appends up to 4000 characters per message, bounded to 16000 per run. It does not interrupt the active role or change permissions, required checks, accepted scope, models or the endpoint. Each role launch freezes the instructions it receives; retries reuse that snapshot. History distinguishes queued notes from notes included in a launch input, which is not proof the model followed them. Final QA closes steering to prevent instructions arriving after the final independent check. Existing plan decisions remain the control for approving or revising a pending plan.

Statistics include archived tasks and failures. New admissions record the running service's Git tag/revision and a digest of local source changes. Historical runs without that identity stay unknown; simulated providers have separate cohorts. Success rate uses all terminal runs, including failures and cancellations. First-pass delivery excludes repairs, recoveries and superseded predecessors. Elapsed time includes waits. Missing token and cost readings remain unknown with explicit observation coverage; the UI does not estimate prices. Cohort comparisons describe recorded samples and do not establish that a release caused an improvement.

## Visual and browser QA

The accepted design reference guided the dashboard's layout, typography, and palette. The built app was inspected in an in-app browser at 1536×1024 and 390×844 with the explicitly labelled test-only service. A screenshot was captured for visual comparison. This check is separate from service integration and the real JobCtrl run.

Fidelity comparison: (1) 224px pale navigation rail and 321px run rail, (2) 91px header and blue New run control, (3) six-node phase strip with blue completed connectors, (4) open facts grid and four-column role table, and (5) blue-dot chronological activity line all match the reference structure and palette. The timeline uses actual service timestamps. Reference issue numbers, dates, token counts and PRs are illustrative only; production code contains no seeded run. The mobile screen keeps the same hierarchy, moves navigation above the run list and preserves the table as a compact table without page overflow. The extra quality, operations, role provenance, usage, decision, and cancellation regions sit below the reference's first viewport because the product requires them. A stale connection banner appears on disconnect.

Blocking-question cards show the investigation justification and callback status. `queued` denotes a native queue acknowledgement only, not Desktop display or an answer; `unknown` effects are never blindly retried, and absent originating threads show `unavailable`. Notification events refresh the projection without changing the Temporal protocol revision used for answers.
