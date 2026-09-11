# Development workflow reset: target architecture

Historical design baseline: 2026-09-09, amended by the 2026-09-11 coordinator/subagent contract. The user selected a reusable workflow, with JobHunter/JobCtrl as the first adopter. This document owns the target design; the earlier assessment remains diagnosis and evidence. Implementation and activation evidence are tracked separately. The migration task's delegation limit is temporary and is not a setting proposed for the resulting workflow.

[Component and lifecycle diagrams](architecture-diagrams.md) · [Implementation contracts](implementation-contracts.md) · [Build and cutover plan](implementation-plan.md)

## 1. The reset

Replace the collection of independently authoritative instructions with one versioned development workflow package, provisionally named `devflow`. It combines a small executable tool, concise role instructions, and a repository-specific profile. GitHub remains the work and review surface. Codex remains the execution and task interface. Existing build/test commands and GitHub Actions remain the test execution infrastructure.

The main architectural change is that agents stop reconstructing process state from prose and conversation. The tool records the work contract, exact candidate, required evidence, unresolved findings, and next permissible actions. Agents make engineering judgments and execute the work. A model's statement that a task is complete does not change the recorded delivery state.

The preserved requirements are outcomes: zero user-found bugs as the goal; existing independent review/QA requirements; zero known Blocker/High defects at shipping; every confirmed code issue has a PR review-record obligation; verified fixes resolve their threads; private data and unrelated work remain protected. Legacy phases, skills, dispatch rules, duplicate documents, and backlog formats have no presumption of survival.

Model optimization is a later project. The implementation and stabilization baseline resolves the user's current defaults and explicit role overrides without embedding model/effort values in the reusable package. Every execution records the settings that actually ran. The future experiment duplicates the same features under two configurations after the workflow is stable.

## 2. Components and deployment boundaries

| Component | Concrete implementation | Owns | Does not own |
| --- | --- | --- | --- |
| Reusable package | Separate `developer-workflow` source repository; Python 3.12+ package and `devflow` executable; versioned JSON schemas and role references | Work-state transitions, evidence validity, finding lifecycle, receipts, adapters and installation | JobCtrl business code or model judgments |
| Repository profile | `.devflow/repository.toml`, `.devflow/checks.toml`, `.devflow/workflow.lock` | Repository identity, surface-to-check recipes, risk triggers, doc owners and pinned workflow version | Credentials, user model policy, live task IDs or usage data |
| Host workflow skill | Mandatory `using-devflow` entry and separately discoverable defining, planning, coordinating, implementing, reviewing, verifying and delivering skills | Engineering behavior and the bridge to supported Codex subagent tools | Duplicate state machine, token arithmetic or a second backlog |
| GitHub adapter | `gh api` REST/GraphQL, structured bodies and paginated reads | Journaled issue capture/reconciliation, issue/dependency reads, review comment publication/resolution, PR/check readback and Project projection | Model invocation or claims that a green check proves product correctness |
| Git adapter | Existing Git and `gh stack`; owned task worktrees and candidate snapshots | Ref verification, worktree ownership, stack serialization, expected-head delivery | Reimplementing Git or a new stacking algorithm |
| Local execution store | SQLite plus content-addressed evidence files under a private state directory | Attempts, assignments, scope snapshots, candidates, gates, actions, findings and accounting joins | A second authoritative issue description |
| Usage adapter | `npx ccusage@latest` plus the narrow version-tested response/phase attribution adapter | Tokens, versioned cost calculations and deterministic reports | A model-powered accounting assistant or an assumed hard spending kill switch |
| Existing CI and test tools | Repository scripts, pytest/Vitest/Playwright/Storybook and GitHub Actions | Running the actual checks and returning evidence | Work-item readiness or autonomous merge authority |

Python supplies SQLite, process execution, TOML parsing and the CLI without adopting JobCtrl's runtime or requiring its server to be running. JSON Schema validation is the single contract-validation dependency; development tests use pytest. Pin dependencies in the package lockfile during implementation. No new database server, workflow service, custom CI scheduler, web dashboard or generic plugin framework is required for this architecture.

Source modules are fixed in the implementation plan. `domain/` contains pure transition/evidence rules; `application/` coordinates commands and transactions; `adapters/` owns GitHub, Git, Codex host receipts and usage. Adapters exchange the records in the companion contract, rather than importing each other's implementation.

The package is installed as a pinned local tool and a managed skill. The installer changes only paths in its manifest. A repository opts in by committing its profile and lock. JobCtrl-specific recipes live in JobCtrl, so a second repository can adopt the same tool with different commands and invariants.

## 3. Who owns each fact

Current intake revision (0.3.0): [conversational intake](issue-trust.md) supersedes the 0.2.0 mandatory independent verifier. The agent interprets a direct work request, bug report/investigation, named issue, or bounded current backlog selection and records it through the CLI. Authority derives from an immutable request admission bound to repository/work/scope/source and allowed operations. This is workflow consistency, not independent human authentication. Source lineage survives issue projections; issue text, labels, comments and queue events cannot initiate or expand work.


| Fact | Source of truth | Other representations |
| --- | --- | --- |
| Requested behavior, acceptance and public bug evidence | GitHub issue, identified by host/repository/node ID | Immutable admitted revision in the private execution store |
| Maintainer priority and dependencies | Issue labels and native blocked-by links | Project view and local admission cache |
| Scope/endpoint authorization | Conversational user instruction, including a bounded selection of existing backlog items, interpreted by the agent | Private authority record tied to a scope revision; an issue label alone is insufficient |
| Current execution state | Local store for the active managed attempt | One tool-owned issue summary and Project Status |
| Code candidate | Git base/head/tree IDs and dependency/environment fingerprints | Gate and delivery records |
| Review finding and its discussion | GitHub PR review thread once published | Local finding identity, origin, obligation, fix and verification links |
| Product-check evidence | Actual check result/artifact and independent review/QA result | Private indexed summaries; public sanitized references |
| Model and instruction provenance | Effective host execution settings and the exact used policy bundle | Immutable private snapshots and hashes |
| Token usage | Unique recorded response usage, reconciled with ccusage | Deterministic work/phase/role/cost reports |

GitHub Project is a view, not another state authority. Its fields are Status and workflow-derived Risk, with issue Priority and labels displayed through native fields. Moving a card does not grant authority or mark delivery verified. A maintainer changes priority, dependencies or readiness through the issue/workflow intake; the tool then updates the view. This removes bidirectional status ambiguity.

Raw prompts, private logs, resumes, databases, session contents, model settings and cost reports stay outside the public repository. Public comments use allowlisted, sanitized evidence. The normalized private store retains hashes and retrievable evidence references, not an unfiltered transcript copy. Historical policy snapshots are private and scoped to instructions actually used; credentials are excluded.

## 4. Work, attempts and execution state

A work item is one independently valuable requested outcome. A stack can implement one work item. A role task, PR layer or retry does not create another outcome.

A work item has `backlog`, `ready`, `active`, `done` or `canceled` lifecycle state. An active attempt has `implement`, `verify` or `deliver` phase, plus an optional blocker that preserves the previous phase. Intake/definition time remains measurable before activation. Project Status is derived as Backlog, Ready, Implementing, Verifying, Ready to deliver, Blocked, Done or Canceled.

`ready` requires an accepted scope revision, observable acceptance criteria, sufficient context, a risk classification, resolved dependencies, a verification plan and a bounded authorized endpoint. A public issue submission cannot satisfy these conditions by assigning itself labels. Preparation can turn an underspecified request into a bounded investigation instead of guessing the implementation.

One active attempt may claim a work item. The local database enforces that uniqueness, and one enrolled execution host owns each repository's managed queue. All task worktrees on that host use the same store. A different host does not silently claim the same work: host transfer is an explicit stop/reconcile/reassign operation. Distributed autonomous scheduling is outside this version.

The original user conversation owns coordination, work admission, evidence integration and authorized delivery. It delegates implementation and repairs to a bounded `implementation_worker`; review and QA are independent subagents with distinct verified identities. Each receives explicit ownership and a self-contained brief. New work uses supported subagent tools, without creating visible peer tasks or moving coordination into a new task. Delegation stays under the original coordinator.

New attempts default to `execution_mode=subagent` and retain their actual `entry_phase`. Review-only and delivery-only work enter at their legitimate phase using existing valid evidence; they do not fabricate a completed implementation assignment. A subsequent required repair is delegated to an implementation worker. Historical attempts without `execution_mode` remain `native_thread`; their identities, receipts, instructions and evidence remain historical records.

The default stabilization concurrency is one active outcome per repository, with independent review and QA allowed to overlap when they use isolated fixtures. This is a workflow concurrency setting, not a model configuration. Record and version later changes to it.

## 5. The concrete happy path

1. **Capture and prepare.** The user requests work, reports a bug to investigate/fix, names an issue or selects a bounded existing batch. The agent captures/reuses issues and proceeds without a separate approval channel; a shared request reference can cover the selected members. The owner produces the self-contained issue contract, acceptance IDs, exact starting point, scope boundaries, risk and endpoint. Missing consequential decisions are resolved before Ready. The CLI validates the record and stores its hash.
2. **Claim and isolate.** `devflow work start` claims the issue atomically, verifies prerequisites, captures effective settings and pins the policy bundle. The host reuses the current dedicated task checkout or creates a task checkout from the verified base. It never cleans the canonical checkout to start work.
3. **Implement.** The coordinator prepares a bounded implementation assignment, resolves its role policy and journals launch before spawning. Product work starts only after startup identity and settings are observed and validated. The implementation worker makes the change and meaningful regression proof, using focused development probes. The coordinator first admits the captured candidate and implementation result; registered verification then uses the selected existing commands through `devflow check run`. The tool records the command, candidate, environment, result and evidence. An observed code issue is recorded immediately, including one caught before formal review. The coordinator records the worker's result before advancing the implementation lifecycle.
4. **Publish the candidate.** When publication is within the endpoint's authority, create/update a regular PR on the outcome branch. Its body states acceptance and pending verification accurately. PR existence does not mean readiness. A local-only request retains a local candidate and pending PR-publication obligations.
5. **Verify independently.** Freeze a clean candidate. For each required gate, the coordinator activates a verified review or QA subagent with the same candidate contract. Review inspects correctness/contracts; QA executes the affected product path. Each returns PASS, FAIL or BLOCKED and structured findings tied to exact evidence and its activation action. Every activated result is durably imported before repair or candidate replacement; late original results remain historical and cannot establish current readiness. Their identities are distinct from the coordinator, implementation worker and each other. They do not silently repair the product while judging it. Tier 0 requires static checks and skips independent gates.
6. **Repair and close findings.** The coordinator sends confirmed findings to the same available implementation worker, integrates its repair result, arranges the affected checks and push, and requests the failed gate's targeted rerun from the same available independent role. The GitHub adapter publishes all due findings, links fixing evidence, resolves verified fixed threads and reads their state back.
7. **Deliver.** The CLI recomputes completion conditions from the current candidate and remote state. Automatic merge enrollment requires strict target freshness and a required `devflow/verified` commit status, enforced for the acting account. Record every source head, target ref/SHA and expected integrated tree. The adapter publishes success only for the evaluated candidate/gate set, then uses the protected delivery mechanism. Read the actual resulting merge commit/tree and confirm its reachability from the target. Only matching proof and endpoint evidence can produce Done; a queued or unexpectedly integrated result cannot.
8. **Follow outcomes.** Record the delivery, cost and observed defects. A later bug links to the historical candidate and introducing attempt; the repair has its own linked record. The new workflow does not erase older evidence when an issue is reopened.

For a review-only, repair-only or merge-only request, start with the existing candidate/PR and its evidence. `next` calculates what is actually missing; it does not restart implementation. A merge-only request with stale QA gets the missing verification, not a new feature plan. Tier 0 editorial work keeps its bounded implementation assignment and static checks without independent review/QA gates.

## 6. Supported subagent integration

The coordinator uses `agents.spawn_agent`, `agents.followup_task`, `agents.send_message`, `agents.wait_agent`, `agents.list_agents` and `agents.interrupt_agent`. The CLI prepares and journals intents; the coordinator invokes the supported tool and records its actual receipt. These tools are neither shell commands nor invented HTTP endpoints. Missing required host capabilities block the action.

Role policy resolves each field from an explicit user role/session override, then the selected configured role file, then saved subagent defaults, then saved global defaults. The coordinator's active model or effort override is never an input. Workflow roles select `implementer`, `reviewer` and `qa` by default; an explicitly selected alternative such as `pr-reviewer` uses its configured file. Resolution honors `[agents.<role_name>].config_file` relative to the global configuration directory, otherwise checks adjacent `agents/<role_name>.toml`. `[agents].default_subagent_model` precedes the saved root model. Missing resolved settings are errors. Hash only used allowlisted settings and source references, excluding secrets and developer instructions.

Every spawn uses `agent_type=default`, explicit model and reasoning effort, and `fork_turns=none`. Prepared role instructions belong in the bounded brief. Fixed custom agent roles and full-history inheritance can defeat requested model overrides, so a configured role name selects policy without selecting a fixed native agent type.

Startup has two stages. Persist the assignment and action, then journal `action.begin` before launch. The initial spawn message only asks the child to report its own session metadata path and wait. Record the receipt without treating it as proof of a native UUID or effective settings: the spawn response supplies `task_name` and nickname, while `agents.list_agents` supplies canonical agent paths and status. The coordinator validates the explicitly supplied local session evidence, using allowlisted `session_meta` and `turn_context` fields to establish the parent UUID, exact canonical agent path and actual model/effort. Only then record the native UUID for attribution and activate the bounded product brief. Missing observed settings block activation; an unobserved service tier stays unknown. Session metadata and its source path are private evidence, never committed content.

Use the canonical `agent_name` for all subagent control. The verified native UUID is the attribution identity, not an invented control target. Reuse available implementation/review/QA agents for repairs and candidate changes. `followup_task` cannot override model settings: a policy change requires an explicitly recorded replacement with fresh startup observation. An unavailable agent's replacement preserves its prior identity, results and history. A lost spawn response requires reconciliation; inventory absence alone never proves that a spawn failed and never authorizes a duplicate.

The product handoff contains work/scope/assignment IDs, requested result, acceptance IDs, repository/base/head, owned paths, constraints, check recipe/evidence references, role responsibility and expected result. Review/QA receive candidate-bound inputs and isolated mutable fixtures. Agents sharing a checkout preserve one another's edits. Shared build caches are permitted; mutable application directories, browser profiles, ports and fixtures retain explicit ownership. A snapshot advances only after owned dirty state is accounted for.

Only significant transitions cause messages: assignment, a scope/candidate change, blocking information, a gate result or delivery. Use supported waits and bounded inventory observations. The coordinator communicates progress and consequential uncertainty to the user. The ledger retains durable facts; messages only notify. Loading entry instructions performs no launch, backlog scan, resume or scheduling. Ordinary questions and unused sessions create no work. Usage ingestion never needs a model.

Historical `native_thread` attempts retain their original visible-task control and receipt contract. This compatibility boundary does not create new visible peer tasks for subagent attempts or rewrite historical evidence. A future unattended host requires separate conformance evidence before replacing the supported bridge.

## 7. Evidence and quality enforcement

The repository profile selects existing command recipes from changed surfaces and declared risk. The owner adds scenario-specific acceptance checks. The selection is inspectable and may be increased; any omission requires an explicit recorded reason and cannot waive a mandatory gate. Unknown changed surfaces require classification. This is a command/risk router, not another CI scheduling engine.

Preserve the current tiers: editorial static checks; scoped work plus an independent reviewer; product work plus independent reviewer and QA; high-risk work plus the relevant full matrix and operational proof. Keep the final-stack rule for approved unreleased stacks, while active high-risk paths receive their normal gates immediately. Canonical docs precede cumulative final-stack QA.

A proof record includes candidate base/head/tree, scope hash, check/role version, environment and dependency fingerprint, command, actual assertions/scenarios, result, time and evidence hash. A zero exit code without required browser assertions is not a product PASS. Setup failure, blocked execution and unexecuted checks have separate outcomes.

A code or acceptance change invalidates potentially affected proof. A changed tree invalidates independent review and QA for that candidate; rerun the affected gate, reusing unaffected low-level checks only when their input fingerprints still match. A pure rebase with an unchanged tree and unchanged relevant inputs can retain applicable proof with an explicit equivalence record. CI checks and expected remote head are still verified on the current PR. Never silently treat patch similarity as tree equivalence.

The state tool verifies evidence completeness and consistency. Independent review/QA establish engineering conclusions. Because all local agents operate under the user's operating-system account, the CLI is not a security sandbox that prevents a privileged caller bypassing it. GitHub branch protections and actual host permissions remain external enforcement boundaries. Do not advertise database validation as proof that code is correct.

## 8. Findings are a first-class lifecycle

Each confirmed issue gets one stable finding ID with severity, violated invariant, candidate, source evidence and origin. Review, QA, the implementation worker, coordinator and user reports all feed the same lifecycle. Distinguish confirmed findings, hypotheses, duplicates, rejected allegations and accepted deferrals.

Every confirmed code finding carries a PR-review publication obligation. Publish to the relevant diff line, or use a file-level review thread when a line is not appropriate. Use a meaningful regression-test anchor when it demonstrates the affected behavior; do not invent an unrelated anchor. Already-fixed findings still get their record and verification history. GitHub supports file-level review comments without a line parameter. [Review-comment API](https://docs.github.com/en/rest/pulls/comments#create-a-review-comment-for-a-pull-request).

A finding without a relevant PR stays in its owning issue with an explicit pending-publication obligation. Once that PR exists, publication is due before that outcome completes. An unrelated discovery becomes its own self-contained backlog item; it does not silently expand the current implementation. If an in-scope finding cannot be anchored, its publication remains blocked rather than being replaced with an unresolvable timeline comment or counted as complete.

Technical repair and remote closure are separate. The independent role first records a fix-verification observation on the candidate; this can mark the finding technically verified and remove it from that candidate's blocking findings. Its final gate is evaluated after those observations are applied. Publication/closure obligations then require the pushed fix, public reply, resolution and independent readback. A local-only outcome can have a technically verified fix while PR publication remains explicitly pending. A response lost after posting or resolving enters reconciliation before retry; a fixed marker in prose or an outdated diff does not establish thread resolution.

Medium/Low findings may be explicitly deferred within authority, with an open review thread and a linked backlog record; they are not relabeled as fixed. A repository requiring all review threads resolved will consequently block that PR's merge, including JobCtrl under its currently observed rules. Fix it or obtain an explicit policy decision; do not falsely resolve a real deferral to get through GitHub. Blocker/High findings cannot pass the shipping gate. A rejected allegation can be resolved only with its recorded, evidenced non-defect disposition; it is excluded from fixed-thread coverage.

## 9. Recovery, synchronization and accounting

SQLite transactions update local state and enqueue external action intents together. An outbox executor makes the GitHub change, reads it back and records a receipt. External changes are not part of the database transaction. After interruption, reconcile uncertain actions against remote state before retrying. GitHub operations are retried with stable identities and expected revisions, not assumed exactly-once network delivery.

`work reconcile` checks registered tasks, worktrees, refs, pending actions and issue revisions. A lease or stale timestamp does not authorize killing another task or deleting its checkout. A missing capability, rate limit, offline host, changed issue scope or ambiguous remote mutation preserves the attempt and identifies the next required action. The companion contract defines each recovery outcome.

The store contains normalized current records and an append-only audit trail, not a general event-sourcing platform. Take SQLite-consistent backups before schema migration; migrations run transactionally. On an incompatible upgrade, stop new admissions and preserve old data. Downgrades restore a compatible snapshot explicitly; never discard newer history to make an old binary start.

ccusage performs counting and pricing through the user's requested `npx ccusage@latest` entry point; record the resolved version and price snapshot. The response adapter joins phase/role/work IDs and deduplicates cumulative/replayed usage. An attempt includes its owner, role tasks, retries, coordination and corrective work. Unknown attribution or missing prices remain visible and never become zero cost.

Report cost in tokens, API-equivalent USD and estimated Codex credits, with separate subscription allocation only if requested. An account-wide quota percentage is not an exact per-task bill. Admission and phase-boundary budget controls are supported initially; do not promise a hard mid-turn stop until the host exposes and passes that specific control test. The future model experiment must budget its duplicate work separately.

## 10. Reset and cutover

A clean switch is the goal. Install the new package under a pinned version, prepare the repository profile, migrate work records, verify the full contract, then activate one workflow owner. Do not leave old and new dispatch policy simultaneously active.

For enrolled repositories, replace current review/fix procedure definitions with thin routes to the shared package. A tiny global instruction loads the `using-devflow` entry skill; relevant detailed references and repository/attempt resolution are loaded for requested repository work only. A host-global compatibility entry then resolves repository identity and its active attempt/version: enrolled pinned work uses devflow; non-enrolled repositories use the frozen legacy package with their existing settings. For enrolled work, the retained old-pin exception blocks execution until current request admission. Keep global native role definitions available for those legacy consumers; devflow subagents use generic native agents with its role references and explicitly resolved user settings. This is one active authority per attempt, not a global switch imposed on every repository.

Retain the useful review rubric and regression knowledge as targeted references. The original user conversation coordinates each outcome through bounded implementation and independent verification subagents. Reduce enrolled repositories' AGENTS to project facts, data/authority invariants and routing. Convert QA documentation into a short router plus the detailed owning catalog, and the Markdown backlog into an index of canonical issues and preserved decision history.

Shared Codex/Claude definitions require a consumer-aware migration. Before changing a shared target, snapshot its current bytes and references. Non-migrated consumers remain bound to that frozen legacy package; Codex moves to the new package. Remove retired Codex entry points and duplicate active definitions, but do not break another consumer's symlink. Adoption by other repositories or hosts is explicit.

Reconcile legacy work and retain its historical snapshots. For enrolled work, the retained old-pin protection blocks further old-runtime execution and requires current request admission; it does not silently rewrite historical instructions. Rollback restores managed links/config and repository profile refs from the install manifest, preserves new work/evidence, and stops new admissions while compatibility is checked. Product commits and published review history are never undone as an installer rollback.

The implementation plan specifies package files, repository edits, migration accounting and acceptance tests. A completed design is not a verified runtime. JobHunter adoption must prove coordinator/subagent identity, observed role settings, supported-tool handoffs, finding closure, interrupted recovery, real product-path checks, deterministic accounting and instruction removal before ordinary work is called stabilized.

## 0.5 stage routing and resumable outputs

Method selection precedes execution authorization. The entry requires the relevant process skill for design or planning without creating work; an authorized request enters coordination once its contract is sufficient. Every stage has an independently discoverable trigger, inputs, exit evidence, failure/re-entry rule and named handoff. `next` maps already-computed actions to their instruction owners; it does not create another state machine. The old `devflow` name forwards to the entry and owns no competing workflow.

The 0.5 subagent snapshot contract prevents repair while an activated independent result is unimported. Each gate binds its assignment activation; original mismatched results remain retrievable history. Publication includes a journaled branch push with expected source and remote head. Real deferred findings require linked work, and delivery records actual accounting completeness or an explicit unavailable/unknown limit. These guards apply at the CLI boundary; instruction hashes prove captured bytes, not model compliance or shell interception.
