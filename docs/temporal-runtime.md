# Local Temporal delivery runtime

The runtime under `runtime/` is a single-host, local development service. It accepts an explicit, allowlisted issue and repository, runs a Temporal workflow, and exposes the same persisted run through the dashboard, CLI, and MCP. The managed path owns an isolated checkout, an early open pull request, independent review and verification roles, bounded repair in the implementation session, local and required CI checks, and issue reconciliation. Its endpoint is a **published, unmerged PR**. It does not merge, release, deploy, or update personal production data.

The older `devflow-temporal` CLI remains a disposable role-ordering demonstration. It does not publish a PR or reconcile an issue. Use the managed `devflow-delivery` service for an end-to-end delivery.

## Setup and policy

On macOS, install Python 3.12 or 3.13, `uv`, Git, GitHub CLI with access to the selected repository and Project, and a local Temporal CLI. Start from this checkout:

```sh
cd runtime
uv sync --locked --python python3.12
cd ui
npm ci
npm run build
npm test
cd ..
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

The real provider requires the tested `openai-codex` SDK and its installed `openai-codex-cli-bin` Mach-O at version 0.157.1, plus the pinned agent-runtime-kit revision in `runtime/pyproject.toml`. A wrapper or another executable path is rejected. The private `sandbox_attestation_path` binds the SDK/CLI versions, binary hash, repository and workspace policy, and every runtime Python source hash to local negative and positive boundary probes. A missing, stale, or unsafe attestation blocks admission before claiming work. The attestation records the requested model/effort and real session ID, command-tool and child-process denial of copied/host credentials, controller state, outside writes, both `/tmp` and `/private/tmp` read/write access, and loopback, plus an owned write; it also records separate broker-check and network-enabled install probes. If browser QA is configured, admission additionally requires an actual owned browser/API/SQLite pass and parent/child denial of protected files, temp aliases, unrelated hosts and ports. A check's `network_domains` may name exact public hosts only. Check commands receive a credential-free Codex home, an owned workspace, and the explicitly configured toolchain/cache. The model command tool uses a native named permission profile; provider authentication stays in the trusted CLI. This is a macOS local boundary, not a portable or multi-tenant security claim. Account acceptance of a requested model is established by an actual provider call; the current kit does not attest the provider-observed model, so reported model and effort remain unknown.

Only the operator's private policy file can grant check domains or toolchain roots. The check/role profiles reject tracked project `.codex` configuration, deny workspace `.git`, disable plugins and Git hooks, and keep broker GitHub credentials and controller state outside model and check command access. The broker produces a hash-bound base-to-head patch for each independent review and verification role, so those roles can inspect the exact candidate change without reading Git metadata. Candidate-controlled installation, build, and tests run through the native check profile, not as unrestricted broker subprocesses. Before running a new real repository, repeat the direct, child, credential, network, both temp-alias read/write, outside-write, owned-write, and actual toolchain probes for that configuration and bind the resulting evidence to its admission attestation.

For repositories that require a real local browser test, private `browser_qa` policy fixes one argv/cwd, two distinct loopback ports, the permitted browser cache, a positive test-count rule and bounded artifact paths. After a read-only review passes, the broker installs and checks the disposable gate checkout, then launches this command behind a **separate Seatbelt profile** inherited by its API, web server, browser and test children. Role shell networking stays disabled. The profile limits writes to the gate checkout and owned private scratch/home, denies other user files and controller state, and permits TCP bind/connect only on the two named ports plus Unix sockets inside the scratch directory. The broker rejects occupied ports before launch, observes listener PIDs as descendants of the recorded launch identity, and records child cleanup. A missing listener, stale candidate, failed or skipped tests, conflicting port, failed cleanup or missing receipt blocks the gate. The candidate/iteration/config/ports, argv, profile hash, listener identities, exit and test counts, log hash and artifact hashes are bound into a private receipt. The independent kit verify role reads that receipt and log, returns the receipt SHA-256, and assesses the source and observed behavior without claiming it ran the broker's browser command. A pending browser effect after interruption is quarantined and reconciled; it is never silently rerun.

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
