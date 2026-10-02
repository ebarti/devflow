# Local Temporal delivery runtime

The runtime under `runtime/` is a single-host, local development service. It accepts a raw goal with an explicit, allowlisted issue and repository, runs a Temporal workflow, and exposes the same persisted run through the dashboard, CLI, and MCP. A read-only intake role investigates repository evidence, asks material questions, and proposes a scoped plan for explicit acceptance. The managed path then owns an isolated checkout, an early open pull request, independent review and verification roles, bounded repair in the implementation session, local and required CI checks, and issue reconciliation. Its endpoint is a **published, unmerged PR**. It does not merge, release, deploy, or update personal production data.

The older `devflow-temporal` CLI remains a disposable role-ordering demonstration. It does not publish a PR or reconcile an issue. Use the managed `devflow-delivery` service for an end-to-end delivery.

## Setup and policy

On macOS, install Python 3.12 or 3.13, `uv`, Git, GitHub CLI with access to the selected repository and Project, and a local Temporal CLI. Real runs execute natively on macOS; `execution_backend: "native-macos"` is the only supported execution policy. Docker is not required. Start from this checkout:

```sh
cd runtime
uv sync --frozen --python python3.12
cd ui
npm ci
npm run build
npm test
cd ..
```

The real provider declares `agent-runtime-kit[codex]>=0.5.3,<0.6`; the public-index `runtime/uv.lock` resolves kit 0.5.3 and SDK/CLI 0.160.0. Native preparation verifies the installed versions, frozen lock, runtime payload, Python and bundled SDK CLI identities. Set `codex_bin` to that installed `openai-codex-cli-bin` executable. A standalone CLI compatibility check does not establish managed kit execution or same-session resume.

The private service JSON owns absolute paths for `state_root`, `tracking_db`, `helpers_dir`, and `codex_bin`, fixed ports, explicit role models/efforts, and allowlisted repositories. A repository fixes its source/origin, Git base, Project/assignee, exact editable files, recovery manifest, check argv/cwd, optional browser fixture and required CI. Clients cannot supply these authorities. Native toolchain and browser read roots must be bounded canonical directories that do not overlap controller state or credentials. Native checks may use only their configured exact network domains; role commands have no network access. A service migration changes the execution policy and necessary tool locations with a private backup, digest guard and no live-run conflict; it never rewrites historical authority.

Valid raw requests are durable in `preparing` without caller-authored proof. Temporal's existing preparation activity measures deterministic parent/child filesystem and network denials through the actual named Codex command sandbox, including owned scratch writes, unrelated temp denials and nested provider attempts. The private cache records measured logs and runtime identity. Missing, changed or failed observations block execution. Each run separately freezes its repository, scope, branch, checkout/state paths, checks, model selection and endpoint binding. Cache reuse never grants a different run that binding. Preparation heartbeats every five seconds and may make up to three attempts within two hours after worker loss. Authority/probe conflicts fail explicitly; a published proof or frozen result can be recovered without manufacturing observations. Candidate-project and real managed model/resume checks remain separate evidence.

```json
{
  "command_id": "submit-001",
  "run_id": "local-001",
  "work_id": "issue-work-001",
  "issue_url": "https://github.com/OWNER/REPO/issues/123",
  "repository_key": "configured-repository",
  "goal": "Add the requested behavior to the configured repository",
  "base_ref": "main",
  "branch": "feat/issue-123",
  "authorized_endpoint": "published_unmerged"
}
```

Read-only intake investigates and asks material questions, then proposes a plan for explicit `proceed`, `change` or `cancel`, bound to its revision and digest. No model runs while waiting. Intake is limited to eight turns; other roles retain `max_repairs` and separately recorded finite operator grants. Roles and checks have bounded deadlines and shared durable capacity. Older requests may still carry an explicit `accepted_plan`. An identical submit reuses the run; changed inputs under that run ID conflict. The outbox records intent before Temporal start and verifies the remote memo on recovery.

Native role launch uses the supported kit adapter and named permissions. A trusted child waits behind a start gate until its PID/start identity is durably recorded. Child stdout/stderr use pipes drained by the controller into protected durable logs, so child descriptor metadata checks do not require access to controller files. The runtime journals observed descendants and the returned provider session/thread ID; a session ID identifies resumable conversation state, not an OS process. Implementation repair resumes the same session through the owning controller. The role's persisted Codex home is separate from its transient HOME/TMPDIR; shell commands cannot read copied provider credentials, controller tokens or mutable MCP authority. Project `.codex` configuration, Git hooks and Git metadata remain excluded. Review/verification receive controller-bound diffs and broker receipts.

The controller disables built-in agent spawning with `agents.enabled=false` and `features.multi_agent=false`, and rejects nested managed Devflow entry points before provider work. The supported kit adapter records the native SDK's parent thread ID before its turn and inspects raw typed collaboration items before the kit's filtered tool audit. Bounded before/after inventories include all source kinds and archived threads in that role's isolated Codex home. Collaboration, an extra child thread or incomplete observation blocks the role; an empty filtered audit cannot prove zero children. Measured command-sandbox probes deny normal-shell and explicit-path nested `codex exec` access to credentials and provider network, and require no provider thread/response observation. Harmless `codex --version` or help execution is allowed. This is a boundary against recursive agent/provider work, not a claim that macOS prevents every binary invocation. A prompt or hidden PATH is not the guard. The graph, depth, attempts, capacity and deadlines cannot extend themselves.

Native teardown stops only observed PID/start-identity matches, including observed detached descendants, and checks configured fixture ports. It does not claim Linux PID-namespace containment or prove every unseen detach/reparent race. An interrupted monitor, ownership gap or uncertain teardown remains `unknown`, prevents clean completion, and retains recovery material. Unrelated processes and ports are never stopped by name or recycled PID alone.

Temporary resource cleanup is embedded in terminal workflow finalization, including delivered, cancelled, blocked/error, timeout and preparation failure boundaries. Ownership is recorded before transient roots, disposable worktrees, dependencies and browser scratch are created. A heartbeating finalization activity makes up to three attempts within ten minutes, resumes a partially completed removal and verifies absence. The existing run checks/projection show separate process and resource cleanup statuses plus a hashed receipt: `removed`, `already_absent`, deliberately `retained` with reason, or `failed_unknown`. Required transient removal failures cannot produce a clean delivered result.

Finalization removes registered role/check/probe TMPDIRs, browser scratch, generated dependencies/artifacts and disposable gate worktrees. It preserves durable logs, receipts, decisions and sessions; dirty, untracked, unpushed or blocked recovery source remains with an explicit reason. A clean main worktree is removed through Git only after process/open-file checks and either no candidate change or exact published remote incorporation. Symlink/replaced roots fail closed; nested symlinks are unlinked without following them. No global temp sweep, shared cache removal, credential deletion or historical-run cleanup occurs.

For configured offline pnpm checks, trusted preparation copies only the admitted base's frozen lock, exact package-manager declaration and required sanitized workspace/patch inputs into a registered staging root. It validates registry/tarball targets as `registry.npmjs.org`, disables scripts and pnpmfile hooks, and fetches into an owned store using a fixed credential-free command. Candidate checks read that store with their existing offline argv and network policy. These are trusted-input/command controls; they do not claim OS-enforced DNS domain confinement. Workflow finalization removes staging, store and generated dependencies while retaining hashed fetch/check logs. Candidate lock drift and unsupported external sources are explicit failures.

For browser/API QA, policy fixes one argv/cwd, two exact owned local ports, permitted read roots, a positive test-count rule and bounded artifact directories. The credential-free Seatbelt fixture and its browser/services inherit those filesystem/network controls. The broker journals ownership, checks actual port listeners, test counts and candidate stability, preserves hashed logs/artifacts under durable evidence, and records an immutable receipt. The independent kit verifier inspects that evidence without claiming it ran the broker command. Completed native journal/receipt replay does not launch a duplicate effect.

`recovery_key` imports only a configured manifest. `supersedes_run_id` can transfer an explicitly identified blocked unpublished predecessor to a new branch after closed Temporal result, claim, candidate, session and manifest validation. No session exists before a role. Post-role continuation carries the exact accepted plan and session data, regenerates credentials/permissions, and preserves predecessor history. A clean transient generation may be recreated for a separately authorized finite continuation while its durable session stays intact.

Historical Docker run records and their evidence remain readable. Their execution and recovery paths are retired: mutation commands reject them before enqueueing work, and they are never silently resumed or migrated.

## Lifecycle and public interfaces

Use dedicated loopback ports that do not conflict with the target project. The runtime starts and owns a local Temporal dev server, one worker, and one dashboard/API process. It stores process identity in the private state root; status distinguishes a dead or replaced process. Build the UI before start. The Temporal dev server is for a local experiment and is not a production deployment.

CLI application commands and MCP tools automatically ensure that this stack is running before authentication. Concurrent callers and explicit `start`/`stop` serialize through one private state-root OS lock. A healthy stack retains its processes; stale or partial owned stacks recover without erasing state or killing foreign listeners. Readiness requires an authenticated owned API, current Temporal health, and the owned worker's workflow and activity pollers. `status` and `stop` never start the service. Startup has a 30-second deadline, configurable with positive `service_start_timeout` up to 120 seconds, and failures include log paths. Application HTTP errors do not trigger recovery and dispatched mutations are never automatically replayed. Startup, service reads, preparation, roles and checks require no Docker executable or daemon.

```sh
uv run --frozen devflow-delivery --config /absolute/private/config.json start
uv run --frozen devflow-delivery --config /absolute/private/config.json status
uv run --frozen devflow-delivery --config /absolute/private/config.json token
uv run --frozen devflow-delivery --config /absolute/private/config.json submit --request /absolute/private/submit.json
uv run --frozen devflow-delivery --config /absolute/private/config.json run --id local-001
uv run --frozen devflow-delivery --config /absolute/private/config.json stop
```

The token command prints the local service credential for dashboard sign-in; keep it private. The browser uses same-origin loopback requests, an HttpOnly session cookie, Origin/Host checks, and a CSRF header for writes. The dashboard serves `/`, `/new`, `/settings`, and `/runs/{id}` with the built static assets. It renders authoritative phase gates, roles, candidate and PR identity, checks, tracker status, usage unknowns, decisions, and durable events. SSE resumes from its event cursor and shows stale state on disconnect. No display refresh calls a model.

The CLI, HTTP API, and official MCP stdio server use the same service. To configure a local MCP client, run `uv run --frozen devflow-delivery-mcp --config /absolute/private/config.json` as its command. It exposes `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, and `cancel_run`. The MCP client authenticates to the loopback API with the private local token. HTTP has `GET /api/service`, `/api/runs`, `/api/runs/{id}`, indexed evidence and cursor-replay events; `POST /api/runs`, `/decision`, and `/cancel` require the session and CSRF value. Indexed evidence reads are contained to the run's owned state. The CLI request file is an alternative to browser writes.

A push can succeed before GitHub's PR-head readback updates. New runs keep the publication effect pending and retry the read-only head check on durable Temporal timers; they do not commit, push, create another PR, or start review until the owned branch and open regular PR show the expected commit. A run previously blocked at this exact boundary may use authenticated `POST /api/runs/{id}/recover-publication`, or `devflow-delivery recover-publication --id ID --request FILE`, with `command_id`, the observed `expected_revision` and `expected_candidate_id`, the pushed `expected_head`, and `expected_pr_number`. The service requires a closed Temporal predecessor, passed prepublication checks, confirmed role cleanup, the retained work claim, one bound publish effect that is pending or has a validated completed receipt, an unchanged checkout, and matching local, origin, and PR heads. It then records a single same-run continuation in the outbox and resumes at independent review and the remaining gates. The old execution and its failure remain visible in the timeline; a retry of the same command is idempotent. A changed branch, PR, candidate, claim, or ambiguous cleanup is a conflict requiring inspection.

After a published run exhausts its configured repairs on a specific failed review, broker check or required CI gate, an operator may grant one or two additional iterations with authenticated `POST /api/runs/{id}/continue-repair` or `devflow-delivery continue-repair --id ID --request FILE`. The request contains `command_id`, `expected_revision`, `expected_iteration`, `expected_candidate_id`, `expected_pr_number`, `expected_pr_head`, and `additional_iterations` (1 or 2). This initial grant is one-time and cannot exceed the original `max_repairs + 2`; it does not change the run's frozen policy. Admission reads the closed Temporal result, current failed-gate diagnostics (including a bounded, hashed failed CI job-log excerpt where needed), retained claim, confirmed native process and resource cleanup and exact open PR/remote head. It queues a new execution of the **same run** with the original implementer session and leaves prior failures visible. If GitHub or tracker readback is temporarily unavailable after the grant, the queued execution waits and retries under that same grant, including across worker restarts; no repair role starts until authority is confirmed. The resumed implementation must still pass prepublication checks, publication readback, independent review, final checks, browser QA when configured, verification, required CI and tracker reconciliation. A changed authority, uncertain cleanup or missing actionable failed-gate evidence is a conflict. No extension occurs automatically when the default budget is exhausted.

If that resumed implementer was blocked **before any provider process launch**, an operator can retry the same iteration once with authenticated `POST /api/runs/{id}/retry-prelaunch` or `devflow-delivery retry-prelaunch --id ID --request FILE`. The request contains `command_id`, `expected_revision`, `expected_iteration`, `expected_candidate_id`, `expected_pr_number`, and `expected_pr_head`. Admission requires a closed Temporal execution, the finished no-process attempt, confirmed cleanup, the unchanged claim/candidate/open PR and the original repair grant. It seals the preceding review findings together with any currently failed required-CI job diagnostics for that exact PR head; unavailable or incomplete CI readback prevents queuing. The retry uses the same implementer session and existing authorized iteration ceiling. It neither grants another iteration nor treats the CI excerpt as a passing check; all broker, review, verification and final CI gates still run after the role.

If an implementation-only role stops because a required tracked test file was omitted from the frozen edit list, an operator can issue one separate scope amendment with authenticated `POST /api/runs/{id}/amend-scope` or `devflow-delivery amend-scope --id ID --request FILE`. The request binds a unique `command_id`, expected protocol revision, iteration, post-role candidate ID, PR number/head, one or two sorted `added_paths`, and the absolute path and SHA-256 of a private amended service configuration. The amended configuration must differ from the original only in those file permissions; the original request and any earlier repair grant remain unchanged. The service reads the completed Temporal result, finished role receipt, retained claim, existing open PR, exact source candidate, sealed native ownership/finalization evidence and current-head CI diagnostics before queuing. The effective policy and predecessor are recorded separately and visible in run detail. A replay of the same command returns its receipt; a second amendment conflicts. The sole new turn resumes the original implementation session, then must pass every broker, independent role, CI and tracker gate. This command cannot convert a failed test or model assertion into a passing result, and it does not authorize arbitrary new runs or further repair turns.

The optional [Desktop entry point](../runtime/desktop/README.md) installs a narrow local skill and MCP command for submitting and opening these same runs. Its guarded installer requires the explicit runtime, private config and Codex-home paths and preserves unrelated entries. New valid requests prepare automatically through the service.

Each clarification and plan decision is a Temporal wait with a persisted ID and revision. A wrong/stale answer returns a conflict; a worker restart does not invoke a model while waiting. Cancellation stops at a role/check boundary. An accepted cancellation cannot later become a blocked or successful outcome because a check finishes concurrently. If a child or external effect cannot be proven stopped, cleanup is explicitly unknown. A completed role receipt is reused only for its bound request; an ambiguous in-flight role is quarantined rather than repeated.

## Verification and limits

```sh
cd runtime
uv run --frozen ruff check src tests
uv run --frozen pytest -q
cd ui
npm run build
npm test
```

Tests cover real Temporal restart/decision and repair gates, submission/claim conflicts, cancellation races, native command boundaries, and the local API. The dashboard suite exercises the real response shape as well as UI state changes. Live model availability, actual repository checks, GitHub effects, browser interaction against the real service, and independent review/verification require separate evidence for the **exact candidate**. A successful unit suite or fake provider run does not establish those effects. The service is local, single-host, and uses Temporal's development server and SQLite; interrupted external effects may need human reconciliation.

A reproducible live smoke uses the installed public CLI, a new private Git fixture and a separate local service. It submits two raw goals with no manually supplied proof, verifies real intake plus environment reuse, then cancels before plan acceptance and tracker writes. Supply an existing configured runtime and a new private evidence directory, such as a unique directory under `~/.local/state/devflow`:

```sh
runtime/.venv/bin/python runtime/scripts/smoke_preparation.py \
  --runtime-dir /absolute/path/to/runtime \
  --config /absolute/private/service-config.json \
  --output-dir /absolute/private/new-smoke-evidence \
  --execution-backend native-macos
```

The restart option crashes only the disposable service's identified worker after proof publication and before freeze, restarts through the public CLI, and verifies recovery from the same proof. Successful real intake requires the configured model to be available for the runtime's account. This proves the preparation/intake surface, not a full feature delivery or implementation-session resume. The latter is a separately labelled integration check.
