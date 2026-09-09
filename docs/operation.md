# Operating devflow

The package provides a CLI and a focused host skill. It does not start models or a background scheduler. Run `devflow --help` for input flags. Commands return JSON envelopes; `--json` is recommended for the owner task. Amounts retain exact decimal JSON numbers.

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

Release files live under `<install-root>/releases/<revision>`; isolated Python environments live under `<install-root>/environments/<revision>`. The manifest returns the exact runtime command. Installation does not rewrite model settings. Installing a skill entry and enrolling a product repository are separate operations. A shared global entry must retain a route for non-enrolled repositories and draining attempts before any legacy entry is replaced.

If a process stops during installation, rerun apply or rollback with the original approved plan. The private journal reconciles each actual target against its recorded before/after states. An intervening user edit blocks further writes. Rollback restores managed targets and preserves releases, runtime evidence, worktrees, and GitHub history.

## Repository profile and pin

Each enrolled repository commits `.devflow/repository.toml`, `checks.toml`, and `workflow.lock`. The repository table names a stable identity such as `github:owner/name` and its default branch. The lock pins a release version and full Git revision. Neither credentials nor active task IDs belong in these files.

Named check recipes contain a description, argv array, relative cwd, result kind (`static` or `junit`), and timeout. JUnit recipes use `{report_path}` in their command; the runner supplies a fresh owned report path. Minimum executed tests and maximum skipped tests are explicit. Optional `scenarios` names the predefined scenarios this recipe proves. Work-specific manual QA records additional actual scenario IDs.

`devflow profile inspect --repository checkout --json` validates the files. `doctor` also checks the pinned installation and required local tools. Native task availability, GitHub authorization, and protected merge conformance remain explicit host/enrollment checks. A missing prerequisite is BLOCKED.

The CLI selects an active attempt's captured revision before the repository lock. A new repository pin does not silently upgrade ongoing work. The selected installed release is checked against its content manifest before execution. A source checkout is used for development and installation preparation; ordinary enrolled work runs the immutable release.

## Work and evidence

An ordinary user request in an adopted GitHub repository first becomes one lightweight backlog issue, including work that starts immediately. Reuse an existing issue for the same outcome. Otherwise use the existing `gh issue create` command with a short outcome, observable acceptance and necessary context; link the resulting issue to the work contract and PR. This adds no approval step or triage delay. Follow-up fixes and clarifications stay on that issue. Use a bound Project's In Progress field if available; an active linked PR suffices without a Project. Explicit local-only/no-publication instructions still govern, and public issue content cannot authorize execution.

Use the records in [implementation contracts](implementation-contracts.md). A mutating request contains `operation_id`, `work_id`, and `expected_revision`. Reuse the same operation ID and identical request after an uncertain local response. Changed payloads require new IDs and current revisions. The tool returns the resulting revision and next actions.

The accepted endpoint includes an exact target: `local` uses the canonical absolute checkout path, `pr` uses the PR base branch, `merge` uses the target branch, and `release` uses an existing tag name. Remote branches/tags use their short names, such as `main` or `v0.1.0`, without `refs/` prefixes. The authority record independently binds the repository. A request cannot substitute a different path, branch, tag, or repository; actual endpoint readback must identify the accepted destination.

`work prepare` reports missing contract fields without creating state or inventing authority. `work ready` accepts the work contract plus a separately recorded user instruction or adopted queue policy. `snapshot capture` stores the actual used instruction bytes, profile, full package revision, and observed nonsecret settings. `work start` verifies the snapshot's executing revision, admitted profile and stored inputs before claiming an attempt and returning a workspace intent. A missing or mismatched pin is rejected before creating an active claim.

Register the dedicated checkout against that intent, verify it is clean, and record the actual workspace receipt. `candidate capture` reads real Git head/tree/base and ownership. `check run` executes an admitted recipe and records process and assertion outcomes separately. A later failed result supersedes an earlier passing result for that recipe. Named scenarios and acceptance must be covered by current passing evidence.

The check runner durably admits an execution before starting its process. An identical completed request returns the same recorded result without rerunning. An interrupted dispatched check remains unresolved until reconciled; it is never blindly repeated. Evidence and the confirmed receipt commit together. If another command changes the revision during execution, the private draft preserves the actual result while stale completion is rejected.

Role launch intents are executed by the active owner through supported native task tools. Record pending setup separately from the final task ID. A task title alone cannot reconcile a lost creation response. Use the assignment marker and observed prompt/context. Review and QA return structured results from their registered independent identities; the owner does not supply a substitute PASS.

When a final task ID arrives, its current assignment no longer carries the pending setup ID; receipts retain that history. `next` waits only on live assignments. A completed nonpassing result identifies the needed correction or blocker and retains the same task for its eventual rerun.

Technical fix observations precede the final gate. `finding fix` requires candidate-contained Git readback; independent `fix record` verifies the regression. Publication and thread closure are separate recorded obligations. Every confirmed code finding is published when a PR exists within the authorized endpoint. A local-only outcome retains pending publication visibly.

## External actions and delivery

Persist `action prepare` before publication, then `action dispatch` for supported GitHub operations. Endpoint operations originate from `deliver` preparation, which validates current proof before creating their intent. The dispatcher marks an action dispatched before the external call and serializes one action writer. A restarted dispatched/ambiguous action performs reconciliation reads, not a blind second mutation.

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
