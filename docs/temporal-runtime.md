# Local Temporal delivery runtime

The runtime under `runtime/` is a single-host, local development service. It accepts an explicit, allowlisted issue and repository, runs a Temporal workflow, and exposes the same persisted run through the dashboard, CLI, and MCP. The managed path owns an isolated checkout, an early open pull request, independent review and verification roles, bounded repair in the implementation session, local and required CI checks, and issue reconciliation. Its endpoint is a **published, unmerged PR**. It does not merge, release, deploy, or update personal production data.

The older `devflow-temporal` CLI remains a disposable role-ordering demonstration. It does not publish a PR or reconcile an issue. Use the managed `devflow-delivery` service for an end-to-end delivery.

## Setup and policy

On macOS, install Python 3.12 or 3.13, `uv`, Git, GitHub CLI with access to the selected repository and Project, a local Temporal CLI, and Docker Desktop with a running daemon. Start from this checkout:

```sh
cd runtime
uv sync --locked --python python3.12
cd ui
npm ci
npm run build
npm test
cd ..
./docker/build.sh
```

The service configuration is a private JSON file outside the target checkout. It owns absolute paths for `state_root`, `tracking_db`, `helpers_dir`, and `codex_bin`, plus fixed ports and allowlisted repositories. A repository policy fixes its source, origin URL, GitHub repository, base ref and expected SHA, Project and assignee, exact allowed feature paths, recovery paths, prepublication and final check commands, an optional owned browser QA command, and required CI names. Roles have explicit model and effort selections. Clients cannot submit paths, check commands, model settings, or a broader endpoint. The public submit body is:

```json
{
  "command_id": "submit-001",
  "run_id": "local-001",
  "work_id": "issue-work-001",
  "issue_url": "https://github.com/OWNER/REPO/issues/123",
  "repository_key": "configured-repository",
  "goal": "Implement the accepted issue scope",
  "accepted_plan": "The reviewed implementation and verification contract",
  "base_ref": "main",
  "branch": "feat/issue-123",
  "authorized_endpoint": "published_unmerged"
}
```

`recovery_key` selects a server-configured source import. `supersedes_run_id` is allowed only for an explicitly named, terminal pre-role run whose external claim can be atomically transferred; the successor must use a new branch and run ID, and the previous checkout and history remain auditable. An identical submit retry reuses its run, while a changed request with the same run ID is rejected. The service records an outbox intent before Temporal start and compares the remote workflow memo on recovery.

The real provider uses the tested `openai-codex` SDK and `openai-codex-cli-bin` 0.157.1 with the pinned agent-runtime-kit revision. `runtime/docker/build.sh` builds from the pinned Playwright base image, verifies the Linux CLI and role-runner byte hashes, and prints the local image ID and platform. The operator's private `container` policy pins that image ID, platform, Docker executable hash, reviewed seccomp profile hash, role-runner hash, CLI path/hash and target repository's package-lock hash. The controller checks the image labels and actual Docker inspection at admission and at execution. The private `sandbox_attestation_path` binds these identities, the exact repository/workspace policy and every runtime Python source hash to positive and negative probes. A missing, stale or unsafe attestation blocks admission before a work claim is transferred.

All candidate-controlled execution uses one Docker lifecycle: the role, prepublication/final checks and browser QA each start in a distinct labelled container with a private PID/IPC namespace, non-root user, dropped capabilities, no privilege escalation, read-only root and the reviewed seccomp profile. The controller mounts only owned per-execution paths; controller state, host credentials and Docker socket stay outside. Container start is journalled before launch. Replay inspects the same container and receipt; disappearance or ambiguous daemon state leaves cleanup unknown. Only Docker's confirmed exited container with PID zero can produce `cleanup=confirmed`, including when a command spawned a detached child. The real provider has no native fallback that reports confirmed cleanup from a process-group sample.

The role container uses Codex's named native permission profile for its tools while trusted provider traffic uses Docker bridge networking. Role shell access to host credentials, controller files, unrelated files and network must be denied in both the direct tool and its children. Each role has a private persisted home; an implementation repair resumes the same kit session in a new contained execution. Check and browser containers instead use `--network none`; they receive a credential-free environment and a read-only, labelled dependency volume. The broker prefetches the admitted lockfile's package tarballs in a separate script-free preparation container, then candidate install/build/test commands run offline. A controller-produced, candidate-bound Git metadata snapshot is mounted read-only for Git-dependent checks; review and verification roles receive the immutable base-to-head patch. Project `.codex` configuration and Git hooks remain excluded from role authority.

For a repository needing a real browser/API/SQLite test, private `browser_qa` fixes one argv/cwd, two distinct internal loopback ports, a positive test-count rule and bounded artifact paths. Linux Landlock permits that QA command and its children to bind/connect only those ports inside their private network namespace; no port is published on the host. The broker owns the command, verifies test count, candidate unchanged, container exit and cleanup, and records the candidate/iteration/config/argv/ports, image/profile, log and artifact hashes in an immutable private receipt. The independent kit verify role inspects that receipt and source and returns its SHA-256 without claiming it executed the broker command. A completed receipt can reconcile an interrupted database update without launching a duplicate container.

The attestation requires an actual requested-model session, owned writes, same-session continuation, detached-child teardown, offline install and browser/API/SQLite evidence. Exact-configuration verification also exercises Git-dependent checks against the read-only metadata snapshot. Direct and child probes must deny protected credentials, controller state, outside files, Docker socket and unrelated network endpoints. A provider-observed model remains unknown unless the provider reports it; requested model and effort are recorded separately. This boundary is tested for the pinned local Docker/macOS configuration, not claimed portable or multi-tenant. New repositories or changed policy need new exact-configuration probes and attestation.

## Lifecycle and public interfaces

Use dedicated loopback ports that do not conflict with the target project. The runtime starts and owns a local Temporal dev server, one worker, and one dashboard/API process. It stores process identity in the private state root; status distinguishes a dead or replaced process. Build the UI before start. The Temporal dev server is for a local experiment and is not a production deployment.

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

The optional [Desktop entry point](../runtime/desktop/README.md) installs a narrow local skill and MCP command for submitting and opening these same runs. Its guarded installer requires the explicit runtime, private config and Codex-home paths; it neither starts an independent orchestrator nor broadens a submit request. Install it only after the local service and its exact policy have passed admission checks.

An optional managed decision is a Temporal wait with a persisted ID and candidate revision. A wrong/stale answer returns a conflict; a worker restart does not invoke a model while waiting. Cancellation stops at a role/check boundary. An accepted cancellation cannot later become a blocked or successful outcome because a check finishes concurrently. If a child or external effect cannot be proven stopped, cleanup is explicitly unknown. A completed role receipt is reused only for its bound request; an ambiguous in-flight role is quarantined rather than repeated.

## Verification and limits

```sh
cd runtime
uv run --frozen ruff check src tests
uv run --frozen pytest -q
cd ui
npm run build
npm test
```

Tests cover real Temporal restart/decision and repair gates, submission/claim conflicts, cancellation races, check containment, and the local API. The dashboard suite exercises the real response shape as well as UI state changes. Live model availability, actual repository checks, GitHub effects, browser interaction against the real service, and independent review/verification require separate evidence for the **exact candidate**. A successful unit suite or fake provider run does not establish those effects. The service is local, single-host, and uses Temporal's development server and SQLite; interrupted external effects may need human reconciliation.
