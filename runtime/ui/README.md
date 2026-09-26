# Devflow local dashboard

This React/TypeScript dashboard is a static client for the local Devflow service. It never determines workflow completion; every state and gate is rendered from the service projection. `src/api.ts` is the only HTTP contract adapter.

## Build and serve

```sh
cd runtime/ui
npm ci
npm run build
npm test
```

The production output is `runtime/ui/dist/`. The Python service should serve `dist/index.html` at `/` and `/runs/{run_id}`, `/new`, and `/settings`, with the hashed `dist/assets/*` files at `/assets/*`. A missing bundle is a service startup/build error, not an empty dashboard. All `/api/*` calls and the SSE stream stay on the same origin. The source and package lock are committed; `dist/` and `node_modules/` are generated and ignored.

For local UI development, run `npm run dev -- --port 5178` against the Python service through a same-origin proxy or serve the built output from Python. `tests/fixture-service.mjs` is a **test-only mock service**, never an application data source. It can be started with `node tests/fixture-service.mjs`; `DEVFLOW_FIXTURE_REQUIRE_LOGIN=1` enables the fixture sign-in path with its documented fixture token in the test script. Its data has `fixture` labels and must never be copied into production responses.

## HTTP contract

The browser starts with `GET /api/session`. If it returns `{authenticated:false}`, the sign-in screen sends the local service token once in the JSON body of `POST /api/session` and receives an HttpOnly cookie plus `{csrf_token}`. The token and CSRF value remain in memory; neither is put in a URL, persistent browser storage, rendered log, or activity record. Writes send `X-Devflow-CSRF`; the server enforces the session, Host and Origin. A 401 requires sign-in; a 409 decision conflict refreshes the run and requires the user to review the new revision before another answer.

| Request | Response used by UI |
| --- | --- |
| `GET /api/runs` | `{runs:[{id, title?, goal?, repository?, issue?, phase?, execution_state?, updated_at?, revision?}]}` |
| `GET /api/runs/{id}` | `{run, events, evidence}`; `run` has `id`, nullable Temporal `revision`/`protocol_revision`, `projection_revision`, `iteration`, `sequence`, ordered `phase_gates`, `roles`, `capacity:{limit,active}`, boolean `queued`, top-level `cleanup`, `candidate`, `pull_request`, `checks`, `tracker`, role-keyed `usage`, and `decisions` with string `options` |
| `GET /api/runs/{id}/events?after=N` | SSE `event: update`, numeric `id`, optional JSON `sequence`; snapshot refetched on new event and reconnect |
| `GET /api/service` | Service health, version, Temporal, capacity, and `policy` containing allowlisted `repositories` and role settings |
| `POST /api/runs` | Revisioned command with `command_id`, `run_id`, `work_id`, `issue_url`, `repository_key`, `goal`, `accepted_plan`, `base_ref`, `branch`, `authorized_endpoint: published_unmerged`, optional `recovery_key`; returns `{run_id,dashboard_url,existing,phase}` |
| `POST /api/runs/{id}/decision` | `{command_id,expected_revision,decision_id,decision_revision,candidate_revision,answer}` |
| `POST /api/runs/{id}/cancel` | `{command_id,expected_revision,reason}` |

The service owns repository paths, role models, checks, authorization scope, and state transitions. The New run form only selects an allowlisted repository and recovery key from `/api/service`. Decision and cancellation commands use the observed `protocol_revision` and remain disabled before it exists. The UI adapts `base_sha` and `content_sha256` for candidate display, sums only numeric role usage readings, and separates local checks from independent review and QA. It displays missing telemetry as unknown, pending tracker readback as pending, and a lost SSE or fetch connection as disconnected with the last observation retained.

## Visual and browser QA

The accepted design reference guided the dashboard's layout, typography, and palette. The built app was inspected in an in-app browser at 1536×1024 and 390×844 with the explicitly labelled test-only service. A screenshot was captured for visual comparison. This check is separate from service integration and the real JobCtrl run.

Fidelity comparison: (1) 224px pale navigation rail and 321px run rail, (2) 91px header and blue New run control, (3) six-node phase strip with blue completed connectors, (4) open facts grid and four-column role table, and (5) blue-dot chronological activity line all match the reference structure and palette. The timeline uses actual service timestamps. Reference issue numbers, dates, token counts and PRs are illustrative only; production code contains no seeded run. The mobile screen keeps the same hierarchy, moves navigation above the run list and preserves the table as a compact table without page overflow. The extra quality, operations, role provenance, usage, decision, and cancellation regions sit below the reference's first viewport because the product requires them. The explicit login screen and stale connection banner appear only in their corresponding states.
