# Operating devflow

The package provides a CLI and a focused host skill. The coordinator invokes native subagent tools from journaled intents; the CLI does not start models or a background scheduler. Run `devflow --help` for input flags. Commands return JSON envelopes; `--json` is recommended for the coordinator. Amounts retain exact decimal JSON numbers.

Version 0.4.0 retains the agent-interpreted conversational request for work admission and adds subagent execution under the original conversation's coordinator. The agent records the request and calls the CLI; the user does not fill out JSON or use a separate approval channel. [Conversational intake](issue-trust.md) explains the scope and consistency boundary. Loading the `using-devflow` entry instructions creates no work, scans no backlog and resumes nothing; detailed workflow references load only for requested repository work.

## Build and verify

```sh
uv sync --frozen
uv run ruff check .
uv run pytest
uv build
```

Use a dedicated branch for workflow development. Repository profiles and source instructions are versioned alongside executable behavior. See [compatibility](compatibility.md) for release and installation boundaries.

## Install a reviewed release

Prepare a JSON request containing a clean source checkout, its full commit SHA, a separate installation root, explicitly owned target paths, a map of skill links, and any known shared consumers. `devflow install plan --request-file request.json --json` returns the before/after manifest. Review its exact scope and digest. `devflow install apply` and `install rollback` require the manifest plus independent `--approved-root`, repeatable `--approved-path`, and `--approved-plan-id` arguments. The manifest cannot grant its own authority.

Release files live under `<install-root>/releases/<revision>`; isolated Python environments live under `<install-root>/environments/<revision>`. The manifest returns the exact runtime command. Its skill-link map can install both `using-devflow` and `devflow` from the reviewed release; the tiny global loader and any command launcher are explicit managed installation targets, not startup actions. Installation does not rewrite model settings. Installing a skill entry and enrolling a product repository are separate operations. A shared global entry must retain a route for non-enrolled repositories and draining attempts before any legacy entry is replaced.

If a process stops during installation, rerun apply or rollback with the original approved plan. The private journal reconciles each actual target against its recorded before/after states. An intervening user edit blocks further writes. Rollback restores managed targets and preserves releases, runtime evidence, worktrees, and GitHub history.

## Repository profile and pin

Each enrolled repository commits `.devflow/repository.toml`, `checks.toml`, and `workflow.lock`. The repository table names a stable identity such as `github:owner/name` and its default branch. The lock pins a release version and full Git revision. Neither credentials nor active task IDs belong in these files.

Named check recipes contain a description, argv array, relative cwd, result kind (`static` or `junit`), and timeout. JUnit recipes use `{report_path}` in their command; the runner supplies a fresh owned report path. Minimum executed tests and maximum skipped tests are explicit. Optional `scenarios` names the predefined scenarios this recipe proves. Work-specific manual QA records additional actual scenario IDs.

`devflow profile inspect --repository checkout --json` validates the files. `doctor` also checks the pinned installation and local tools. Its `READY` status and `execution_enabled` describe local runtime readiness, not authorization or every external capability. The `capabilities` report lists readiness and missing tools separately for GitHub capture, the managed launcher, and usage collection. For example, a local runtime can be ready while GitHub capture lacks `gh`. Native task availability, GitHub authorization, and protected merge conformance remain explicit host/enrollment checks; an operation with a missing prerequisite cannot proceed.

Doctor reports the historical active-attempt pin before the repository lock and checks its installed content manifest. The current entry point never delegates commands to that older runtime: the current reader handles recovery, and further execution requires current admission. This security exception is explicit; historical snapshots remain unchanged. A source checkout is used for development and installation preparation; ordinary enrolled work runs the immutable release.

## Work and evidence

A substantive work request in an adopted GitHub repository first becomes one lightweight backlog issue, including work that starts immediately. A bug report/request to investigate or fix, a named issue, or a bounded selection such as the current P1 backlog also qualifies. Ordinary questions do not create work. The agent records the selected existing batch members under a shared request reference; future queue arrivals are not automatically selected. `devflow backlog capture --request-file request.json --json` wraps the existing GitHub CLI with a durable request/receipt journal. Supply a stable `work_id` and either `issue_number` to reuse an issue, or `title` and `body` with a short outcome, observable acceptance and necessary context. Link the returned issue node ID/URL to the work contract and PR. This adds no approval step or triage delay. Follow-up fixes and clarifications stay on that issue. Use a bound Project's In Progress field if available; an active linked PR suffices without a Project. Explicit local-only/no-publication instructions still govern, and public issue content cannot authorize execution.

The wrapper saves the exact sanitized request before sending, records dispatch before the one creation call, and independently reads the issue back before confirming. It uses an exact capture marker in paginated issue reads, including closed issues; it does not rely on title matching or search-index timing. Existing issue reuse performs only a read and preserves its body. Reusing a work ID with a different initial capture payload is a conflict; follow-ups use the existing issue. The journal appends namespaced operation facts to the existing schema, preserving compatibility with active pinned runtimes.

Use the records in [implementation contracts](implementation-contracts.md). A mutating request contains `operation_id`, `work_id`, and `expected_revision`. Reuse the same operation ID and identical request after an uncertain local response. Changed payloads require new IDs and current revisions. The tool returns the resulting revision and next actions.

The accepted endpoint includes an exact target: `local` uses the canonical absolute checkout path, `pr` uses the PR base branch, `merge` uses the target branch, and `release` uses an existing tag name. Remote branches/tags use their short names, such as `main` or `v0.1.0`, without `refs/` prefixes. The authority record independently binds the repository. A request cannot substitute a different path, branch, tag, or repository; actual endpoint readback must identify the accepted destination.

`work prepare` reports missing contract fields without creating state or inventing authority. `work ready` requires a contract and `user_request` with `reference`, `summary`, and `allowed_operations`; it derives and returns `admission_id`, `authority_id`, and `scope_hash`. `work amend` accepts the same request shape for an authorized scope revision without a separate `approved_delta` field. The agent supplies these fields from the conversation. Caller Authority records, labels and invented queue-policy references cannot substitute for a request. `snapshot capture` stores the actual used instruction bytes, profile, full package revision, and observed nonsecret settings. `work start` verifies the snapshot's executing revision, admitted profile and stored inputs before claiming an attempt and returning a workspace intent. A missing or mismatched pin is rejected before creating an active claim.

For example, the agent adds this request fragment to the existing contract/revision envelope and invokes `devflow work ready --request-file ready.json --json` (or `work amend` for an authorized revision):

```json
{
  "user_request": {
    "reference": "conversation:request-42",
    "summary": "Investigate and fix the reported parser crash; keep the result local.",
    "allowed_operations": ["edit", "check", "create_tasks"]
  }
}
```

This is an agent-prepared fragment, not a standalone complete work contract or a form for the user. The operations must match the requested endpoint; this local example grants no push, PR, merge or release permission.

The existing managed workspace/candidate lifecycle requires `edit`, including for an investigation contract. An explicitly read-only investigation can use capture and inspection commands while the agent performs the permitted analysis, but it cannot complete that managed lifecycle using only `check`. Do not widen a read-only request's permissions to bypass this limitation.

Register the dedicated checkout against that intent, verify it is clean, and record the actual workspace receipt. `candidate capture` reads real Git head/tree/base and ownership. `check run` executes an admitted recipe and records process and assertion outcomes separately. A later failed result supersedes an earlier passing result for that recipe. Named scenarios and acceptance must be covered by current passing evidence.

The check runner durably admits an execution before starting its process. An identical completed request returns the same recorded result without rerunning. An interrupted dispatched check remains unresolved until reconciled; it is never blindly repeated. Evidence and the confirmed receipt commit together. If another command changes the revision during execution, the private draft preserves the actual result while stale completion is rejected.

## Role execution

The original user conversation remains the coordinator in `owner_task_id`. New attempts use `execution_mode: subagent`; previously stored attempts without the field retain their native-thread behavior. Ordinary implementation and repairs require a bounded `implementation_worker`; review and QA use distinct independent identities when the risk tier requires them. Review-only and delivery-only entry do not invent a preceding implementation cycle. Editorial work still skips independent review/QA gates.

Before returning an executable native intent, the CLI checks that the invoking host's `CODEX_THREAD_ID` matches the recorded coordinator UUID. A missing or different coordinator can inspect recorded state and reconcile observations, but cannot dispatch into a different subagent tree using the old canonical path.

The coordinator resolves each role's explicit model and reasoning effort from user overrides, the selected role file, saved subagent defaults, then saved global defaults. Its active model overrides are not inherited by workers. Provenance retains only the winning settings and source references/hashes. Fixed custom agent roles and inherited full-history launches can override requested settings, so prepared launches use `agent_type: default`, `fork_turns: none`, and a self-contained brief with explicit ownership and acceptance.

1. `host assign` records the bounded role, settings policy, stable launch identity and prepared action. `host prepare` validates the active attempt, current revision, scope and candidate, then durably records `action.begin` before returning a native tool intent.
2. Invoke the returned `agents.spawn_agent` intent. The startup brief asks the child only to report its own session metadata location and wait. `host record` stores the canonical agent path from the receipt; it does not invent a native session UUID or treat requested settings as observed settings.
3. `host startup` reads the explicitly supplied child session file. It keeps only allowlisted identity/settings fields and verifies the parent UUID, canonical agent path, actual model and reasoning effort against the recorded assignment. Missing or mismatched facts block activation; an unavailable service tier remains unknown.
4. `host activate` prepares the bounded product-work follow-up. Use `host prepare` to begin that action durably, dispatch its `agents.followup_task` intent, then use `host record` with the actual agent inventory. The verified native UUID supports execution attribution; the canonical agent path is the target for native control.
5. `candidate capture` requires the verified running implementation assignment and its producer UUID. After capture, `host result` binds implementation completion to that output candidate before checks, gates or delivery can advance. Review and QA use `gate record` to return their candidate-bound independent results. The coordinator imports those results and cannot supply a substitute PASS.

Use `agents.send_message` for notifications, `agents.wait_agent` for updates, `agents.list_agents` for the actual canonical paths/statuses, and `agents.interrupt_agent` when stopping assigned work is required. Reuse the same available agent for related repairs. Follow-ups cannot change an agent's model: a changed role policy needs a recorded replacement that retains the old identity/history. Replacement after unavailability requires an actual observation. Inventory absence alone cannot prove that an uncertain spawn never ran, and does not authorize another launch. Canceled work, invalidated/failed actions, stale scopes/candidates and stale revisions cannot produce new executable intents; dispatched or uncertain operations expose recovery only.

Historical native-thread attempts retain their pending/final IDs, assignment markers and prompt/context reconciliation. When a final task ID arrives, the assignment no longer carries the pending setup ID; receipts retain that history. No historical attempt is relabeled as a subagent run.

Technical fix observations precede the final gate. `finding fix` requires candidate-contained Git readback; independent `fix record` verifies the regression. Publication and thread closure are separate recorded obligations. Every confirmed code finding is published when a PR exists within the authorized endpoint. A local-only outcome retains pending publication visibly.

## External actions and delivery

Persist `action prepare` before publication, then `action dispatch` for supported GitHub operations. Endpoint operations originate from `deliver` preparation, which validates current proof before creating their intent. The dispatcher marks an action dispatched before the external call and serializes one action writer. A restarted dispatched/ambiguous action performs reconciliation reads, not a blind second mutation. Unresolved work blockers pause new external/native dispatch and `action begin`; `next` lists only recovery and the blocker, while read-only reconciliation and eligible failed-action retries remain available.

A definite rejection with recorded proof that no mutation could have applied is `failed`. After correcting the prerequisite, `action retry` explicitly re-admits the same action against current authority, candidate and evidence while retaining its receipts. Merely choosing a new action ID does not bypass recovery. A successful or uncertain earlier write in a multi-step action prevents this retry route; read-only reconciliation remains required.

This covers deterministic preflight blockers such as pending required CI checks as well as definite HTTP rejections. A failed read during reconciliation cannot prove that an earlier interrupted write had no effect, so that action remains ambiguous.

Supported adapters publish findings, verify/resolve fixing replies, publish proof statuses, project tool-owned fields, create regular PRs, and publish releases against existing tags. Native task creation stays in the owner context. Direct protected merge requires current classic branch protection, strict required checks, no acting-account bypass, the exact proof binding, and integrated-tree readback. Unsupported merge queue, ruleset-only, and unverified atomic-stack modes block automatically.

An early PR publication can make findings reviewable before final acceptance. It cannot itself complete the work. Terminal PR delivery evaluates current proof in a fresh intent and independently reconciles the existing PR using its original publication identity, without creating another PR.

Record the adapter receipt and observation, then finalize the corresponding finding or delivery transition. Done requires the accepted endpoint, current gates, required publication/closure, and independently observed result. A queued or exposed-unverified result remains active with a blocker. See [contracts](implementation-contracts.md) for exact merge bindings and source/target race rules.

## Usage and outcomes

`usage collect` invokes `npx ccusage@latest` on an explicitly supplied data root, records its version, and rejects a version change during collection. `usage import` parses explicitly supplied native response JSONL with segment/allocation and price snapshots. It ignores cumulative counters and deduplicates response identities. Preserve price snapshots as private artifacts; never substitute session totals for per-response attribution.

The 20.0.20 collector was exercised with isolated synthetic session and daily inputs. Its report's input count is uncached; cache-read and cache-creation counts are separate. Comma-containing input roots are rejected because the collector interprets commas as multiple roots. That collector version omitted cache-write usage and noncompact JSONL in separate probes; reconciliation retains these discrepancies instead of trusting zero totals. Actual user-data compatibility and allocation coverage must still be established on explicitly selected inputs.

`usage record` stores normalized records in an attempt. `report usage` produces exact token partitions, known cost subtotals, unknown totals when prices are incomplete, and explicit unallocated usage. Reasoning tokens are included in output tokens, not added again. USD and estimated Codex credits are separate from an actual subscription invoice.

`outcome record` stores first-ready handoff, exposure, confirmed defect, and intervention observations, including those after delivery. Preserve origin, detector, and repair attribution separately. `report metrics` computes mature 30-day user-found rates, raw/unknown/late discoveries, distinct exposure views, unioned phase intervals, and execution lead time. It reports missing handoff/lead data rather than inventing timestamps. See the [metrics contract](metrics-contract.md) for denominator and attribution rules.

Private SQLite state, evidence, and backups stay under the configured state root. `work show`, `next`, and `work reconcile` support resumption; none of them deletes a checkout. The CLI is an audit/consistency boundary running as the user, not an OS security sandbox or a hard model-spending kill switch.

## Recover after a crash or power loss

When the user requests continuation or recovery, open the same repository and configured private state root. Run `backlog list` to discover saved intake requests and `work list` to discover admitted work. Resume intake with `backlog capture --work-id <id> --json`; the saved title/body and dispatch state are sufficient. Inspect an attempt with `work show` and `next`, retaining its historical package revision, checkout, evidence and role identities. The stored conversational admission supports continuation without another approval or verifier; legacy work first needs current request admission. The current reader never delegates to an older runtime. No original request file or owner conversation is needed for these recorded facts.

```text
saved request → saved dispatch → external action → observed result → saved confirmation
                    interrupted anywhere: read the saved state, then reconcile
```

An interrupted dispatched or ambiguous issue creation performs reads only. A unique marker must also match the pending creation's repository, creator, exact consumed content and any saved POST identity before confirmation, without another creation. If it cannot be uniquely found, retain the uncertainty for investigation; GitHub issue creation has no client idempotency guarantee, so absence alone cannot justify another write. A definite preflight/HTTP rejection with proof of no mutation is retryable with `backlog retry --work-id <id> --json`. Completed results replay from the journal. Concurrent owners on the same store serialize capture for the same work.

SQLite connections explicitly request `synchronous=EXTRA` and `fullfsync=ON`. Evidence, check-result drafts, installer journals/releases and directory entries are flushed in publication order; macOS files also request `F_FULLFSYNC`. Snapshots are validated and flushed before publication, and failed safety backups stop restore. Active claims or unresolved backlog writes block a state restore that would erase their recovery intent. Preserve SQLite's database, journal/WAL and evidence together; do not delete sidecars as crash cleanup. These choices follow [SQLite durability settings](https://www.sqlite.org/pragma.html#pragma_synchronous) and [fullfsync behavior](https://www.sqlite.org/pragma.html#pragma_fullfsync).

State-directory initialization requires the nearest existing directory in the selected path and its immediate parent to be openable and support flushing. For an existing state root, this means the root and its parent; higher existing ancestors are not opened for durability. Missing directories are created one at a time, flushing each directory and its parent before creating a child. A retry flushes the nearest existing directory and its parent because its creation may have been interrupted before publication finished. A denied or failed required flush stops initialization, including when creating a fresh root below an otherwise accessible directory whose parent is denied.

Recovery tests kill actual processes and reopen state, and inject commit/flush failures. They do not simulate a physical power cut. The guarantee covers acknowledged workflow records on a local filesystem whose OS/storage honors flush requests; it cannot preserve unsaved editor buffers or survive loss of the storage device. Python environments are derived caches that `uv` can rebuild from the retained immutable release and lock. No background restart scheduler or global installation is enabled by these recovery commands.
