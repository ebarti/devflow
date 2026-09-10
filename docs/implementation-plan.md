# Development workflow reset: implementation and cutover plan

Design revision: 2026-09-09. Reusable package; JobHunter/JobCtrl first. This is a build specification and migration plan, not authorization to start implementation, create repositories/tasks, change credentials, publish PRs or activate automations. Existing scoped authorization remains effective; the implementation kickoff records the remaining concrete endpoints once rather than asking again at every step.

## 1. Decisions fixed by the design

| Decision | Choice | Why |
| --- | --- | --- |
| Packaging | Separate reusable `developer-workflow` repository, `devflow` Python CLI, one host skill and repository profiles | One versioned owner for executable rules and instructions; JobCtrl stays a consumer |
| Planning records | GitHub issues and native dependencies; one selected Project as a projection | A self-contained work record close to code and review history |
| Execution | Visible Codex owner/review/QA tasks; native task-tool bridge | Satisfies inspectability with currently available host capabilities |
| Local state | Private SQLite and content-addressed evidence, one enrolled execution host per repository | Durable recovery across task/worktree boundaries without a new service |
| Verification | Existing test tools/CI; explicit command recipes; candidate-bound independent gates | Preserve quality while removing reconstruction and speculative repetition |
| Automatic merge | Strict protected target freshness plus required per-head `devflow/verified` status and integrated-tree readback | Bind accepted proof to the code actually exposed despite remote writers |
| Findings | One lifecycle with mandatory due PR review publication and verified resolution | User-required audit trail across implementation, review, QA and repairs |
| Accounting | Deterministic `npx ccusage@latest` ingestion and versioned normalization | No model calls for counting or pricing |
| Models | Resolve and retain user defaults/overrides; log actual settings | Stabilize the workflow before changing configurations |
| Rollout | Versioned replacement, legacy work drained/reconciled, reversible managed installation | Avoid two active process authorities and preserve existing work |
| Later model experiment | Two configurations implement the same frozen cases independently | Direct comparison after workflow stabilization |

The temporary Astra/high cap requested for agents assisting with this architecture/implementation task is not a permanent workflow default.

## 2. Source layout to implement

```text
developer-workflow/
  pyproject.toml / uv.lock
  src/devflow/
    cli.py                         command parsing and JSON envelopes
    domain/
      work.py                      readiness, claims, scope amendments, phases
      evidence.py                  required evidence and invalidation rules
      findings.py                  publication and verified closure rules
      delivery.py                  completion predicate and expected endpoints
    application/
      commands.py                  transactions, action intents and receipts
      reconcile.py                 interruption and uncertain-action recovery
    adapters/
      sqlite_store.py              revisions, constraints, outbox and migrations
      github.py                    paginated REST/GraphQL and readback
      git.py                       owned worktrees, refs and gh-stack invocation
      codex_host.py                intent/receipt validation, no private app API
      usage.py                     ccusage invocation and normalized ingestion
      responses.py                 observed-format per-response attribution
    reporting.py                   deterministic queries, JSON/CSV/Markdown
    installation.py                manifests, managed links and rollback
  schemas/                         versioned contracts and migration fixtures
  skills/devflow/SKILL.md           short entry point and command routing
  skills/devflow/references/
    intake.md / implementation.md / review.md / qa.md / delivery.md
  tests/
    domain/ contract/ adapters/ acceptance/ migration/
  fixtures/repositories/           tiny distinct repository profiles
  docs/architecture.md / operation.md / compatibility.md
```

JobCtrl adds `.devflow/repository.toml`, `.devflow/checks.toml` and `.devflow/workflow.lock`. The profile contains commands and owners, not a copy of the Python state machine. A private enrollment file binds its GitHub Project/field IDs, host, authority source and user settings. Managed installation points the Codex skill entry at the pinned release; source checkouts are not mutable production installations.

Proposed core package dependencies: Python standard library plus JSON Schema validation; pytest and packaging tools for development. Use actual locked versions during implementation. The host continues using the existing subscription sign-in and GitHub authentication; this design introduces no model API key or second paid model service.

Set an explicit instruction-load acceptance target: the common devflow skill entry plus selected role instructions and repository process routing must fit within 2,000 words, with the QA router at most 600 words. These are chosen engineering limits, not benchmark-derived optima; product-specific references are loaded only when their owning behavior is relevant. Measure the actual removed/retained mandatory process text at cutover. This limit does not describe or constrain Codex's system prompt or native tool definitions.

## 3. JobCtrl replacement map

| Current owner/path | Change | Acceptance |
| --- | --- | --- |
| `AGENTS.md` and its `CLAUDE.md` link | Replace general process/dispatch duplication with project facts, safety boundaries and routing to the pinned workflow; retain precise JobCtrl requirements | One process authority; no contradictory gate/dispatch rule in applicable scoped instructions |
| `apps/web/AGENTS.md` | Keep frontend-specific architectural/test requirements; route shared lifecycle to devflow | Frontend requirements remain enforced without a second lifecycle |
| `CONTRIBUTING.md`, `docs/developer/README.md` | Define the supported contributor entry and public workflow summary once | Commands and described CI behavior match executable files |
| `docs/local-development.md` | Keep machine setup, supported runtimes and actual commands; remove blanket Verify policy | Scoped changes route to the appropriate recipe; Corepack usage is consistent |
| `docs/local-reliability-qa.md` | Replace the long chooser with a short risk/surface router | Agents can select applicable proof without reading an exhaustive checklist |
| `docs/developer/qa/regression-catalog.md`, `browser-smoke.md`, `frontend.md`, `complete-checklist.md` | Preserve valuable detailed scenarios at their owning catalog; remove repeated global workflow mandates | Every removed mandatory-reading section has an owner or an explicit retirement reason |
| `docs/backlog.md` | Migrate actionable items to issues with source/context links; retain an index and historical decisions | Every source item is migrated, already represented, explicitly deferred or retired; none silently lost |
| `.github/ISSUE_TEMPLATE/bug_report.yml`, `feature_request.yml`, `qa_regression.yml` | Separate public intake from Ready execution data; add accepted invariant/context/dependency fields where useful | An issue can become self-contained; public templates do not grant execution authority |
| `.github/workflows/issue-triage.yml` | Replace broad whole-body regex classification with exact template-field/declared-label mapping; retain an explicit triage fallback | Boilerplate mentioning tokens, data or releases does not mislabel every issue |
| `.github/workflows/python.yml`, `typescript.yml`, `repo-scripts.yml` and contributor CI prose | Choose executable workflows as CI truth; correct the current unsupported same-repo/top-of-stack claims in prose | A fixture for fork/same-repo/stack events proves the stated behavior; no new custom CI scheduler |
| GitHub main ruleset `Require pull requests for main` (observed ID 21876295) | Enrollment adds strict required status checks including `devflow/verified`, with no bypass; preserve owner-update and release-tag rules | Target/head races reject stale candidates; automatic delivery is disabled until enforcement is proven |
| `package.json` and existing scripts | Keep existing product/test commands; add only the minimal local workflow entry if needed | The aggregate is not advertised as including separate web unit/E2E/Storybook suites |
| `apps/web/e2e/fixtures/global-setup.ts`, `global-teardown.ts`, Playwright workspace configuration | Allocate unique owned test workspaces and require ownership on every destructive cleanup path | Ordinary, isolated and screenshot modes all preserve an unrelated sentinel directory |
| `apps/extension/src/chromium-extension.e2e.test.ts` | Remove the browser-launch failure path that returns without exercising assertions | Required browser launch failure fails or blocks the required check; it never reports a passing product test |
| `scripts/docs-screenshot-environment.mjs` and related tests | Extract/reuse fixture ownership logic where it can safely serve ordinary E2E too | One proven ownership primitive; screenshot behavior remains intact |

These source findings were inspected on `worktree/ee27`. The pre-existing `workers/automation/uv.lock` edit is unrelated and remains untouched. The harness hazards are source-confirmed; their replacement must reproduce and prove the behavior with fixtures before it is called fixed.

## 4. Shared skills and role cutover map

| Current surface | New ownership / disposition |
| --- | --- |
| `~/.agents/definitions/review-prs/workflow.md` | Move reusable engineering rules into devflow; remove independent dispatch/state ownership |
| `~/.agents/definitions/review-prs/reviewer.md` | Retain the valuable rubric as the devflow review reference, with one canonical copy |
| `~/.agents/skills/review-prs` and host-specific review skill adapters | Use a thin repository/attempt-aware router: enrolled pinned work uses devflow review; other repositories and draining attempts retain frozen legacy behavior |
| `~/.agents/skills/fix-pr-review-comments/SKILL.md` | The same router selects devflow repair/finding lifecycle for enrolled work, replacing mandatory hidden workers and loose resolution there |
| `~/.codex/agents/reviewer.toml`, `qa.toml`, `pr-fixer.toml`, `qa-fixer.toml`, `fixer.toml`, `implementer.toml` and configured PR-review role | Keep legacy native definitions for non-adopting consumers; devflow visible role tasks use package references and user-resolved settings. Do not globally rewrite another repository's role behavior |
| `~/.agents/skills/systematic-debugging-for-all/SKILL.md` | Keep diagnosis/invariant discipline; repository-aware routing uses devflow state/verification for enrolled work and preserves legacy instructions elsewhere |
| `~/.codex/skills/gh-stack/SKILL.md` | Keep the proven Git operation reference; it supplies commands, not an additional development lifecycle |
| Existing Claude adapters/links, including `~/.claude/commands/fix-prs.md` | Inventory and pin to a frozen legacy package before shared targets move; do not silently alter a non-adopting client |

The installer inventory follows symlinks and records every consumer and original target/hash, including other Codex repositories as well as Claude. It never writes through a symlink without accounting for those consumers. Only manifest-managed entries change. Resolution order is active attempt's pinned version, otherwise enrolled repository's pinned version, otherwise frozen legacy route. Missing/invalid enrollment is a diagnostic, not automatic migration. There is one active workflow authority per attempt, regardless of which compatibility skill triggered it.

## 5. Implementation work packages

Each row is a separately reviewable deliverable with explicit dependencies and proof. Implement within dedicated task branches/worktrees. Use regular PRs with Conventional Commit titles. New code findings from these packages follow the same publication/resolution obligation; this plan has not posted them to GitHub.

| ID | Outcome and ownership | Depends on | Required proof |
| --- | --- | --- | --- |
| W01 | Implement contracts, pure state rules and SQLite claims/revisions in the reusable package | Accepted design | Contract examples; duplicate-claim race; stale revision; scope amendment; Done rejection without delivery receipt; transactional migration/backup recovery |
| W02 | Implement host intents/receipts, owned workspace assignments, role briefs and `next` | W01 | Visible owner/review/QA tasks on a synthetic canary; pending client ID handling; lost launch response causes no duplicate; task result matches candidate; interrupt/resume keeps work |
| W03 | Implement GitHub/Git adapters, proof status and finding lifecycle | W01, W02 receipt contract | Paginated threads; idempotent publication/resolution; acyclic technical fix/gate/closure; protected target/head races; actual merged-tree mapping; stack backend per-head enforcement and recovery |
| W04 | Implement deterministic usage, provenance and reports | W01, W02 task identity | Replay import idempotency, mixed models/settings, one task reused across outcomes, partial prices, archived records, response allocation, linked repair and non-overlapping totals |
| W05 | Implement JobCtrl check profile and repair the two harness defects | Accepted design; can proceed independently of W02-W04 | Sentinel/symlink/forged-state cleanup fixtures; required extension launch failure is nonpassing; one actual isolated browser/product path; affected existing tests |
| W06 | Package the skill/references and managed installer; remove conflicting active instruction owners | W01-W05 interfaces | Fresh install/update; symlink inventory and enrolled/non-enrolled routing on one Codex host; user models preserved; rollback retains work/history; measured instruction reduction and link review |
| W07 | Enroll JobCtrl, configure approved merge protection and migrate work records/Project bindings | W03, W06 | Permission/rules preflight; required proof/freshness enforcement; source-to-issue mapping; idempotent rerun; complete dispositions; label/Project readback without overwriting user prose |
| W08 | Run cumulative workflow acceptance and cut over | W01-W07 | End-to-end feature/fix/review-repair/delivery scenarios; a second tiny repository profile; no missing independent gates; all serious findings fixed; current candidate/ref/readback proof |
| W09 | Stabilize through needed JobCtrl work using defaults | W08 | Operational evidence across representative paths; tracked interventions, defects, cost and missing data; stable workflow version; no known invalidating workflow/harness failure |

W01-W04 and W06 belong to the reusable repository; W05 and JobCtrl profile/doc changes belong to JobCtrl. Use dependent PRs where contracts build on one another. High-risk harness or credential/state effects receive their full required gates when exposed; intermediate contract-only work does not fabricate product QA. Canonical docs and cumulative QA precede the final activation.

## 6. Acceptance matrix that defines success

| ID | Scenario | Observable pass condition |
| --- | --- | --- |
| A01 | Feature from a Ready issue | Owner completes the original acceptance and endpoint; linked review/QA tasks are inspectable; final receipt identifies the delivered candidate |
| A02 | Bug reported by the user | Reproducer identifies the violated invariant; meaningful regression fails on the bad candidate and passes after repair; origin/detection/fix provenance remains distinct |
| A03 | Review/QA catches several issues | All severities receive distinct due PR review threads; confirmed fixes have commit/proof/reply/resolution/readback; no finding disappears in a summary |
| A04 | Retry after owner or host interruption | Same work/attempt resumes with current refs and existing proof; no duplicate owner, PR, comment or deleted checkout |
| A05 | Candidate changes while QA is running | Old PASS cannot authorize the new tree; the correct gate reruns on the new candidate |
| A06 | Merge-only request with valid existing proof | No implementation restart; only missing delivery checks run; expected head and merge result are independently verified |
| A07 | Stack restack or partially completed merge | Operations serialize; tree changes invalidate affected proof; completed layers are recognized; final target ancestry is verified |
| A08 | Public issue tries to mark itself Ready | No execution starts without a valid admitted contract and user/queue authority |
| A09 | Issue edit changes acceptance mid-run | Change is surfaced and versioned; previous proof cannot silently satisfy the new acceptance |
| A10 | API write succeeds but response is lost | Reconciliation confirms the existing remote object/state before any retry; exactly one visible finding record remains |
| A11 | More than one API page of findings/dependencies | All pages participate in readiness and completion; a blocker on a later page prevents delivery |
| A12 | E2E receives an unrelated directory or forged workspace file | Sentinel contents survive; cleanup refuses before deletion; ownership is mandatory in every mode |
| A13 | Extension browser cannot launch | Required suite produces FAIL/BLOCKED and zero false product PASS claims |
| A14 | Current price/usage format is unsupported | Tokens and coverage are reported honestly; cost unknown; no zero-cost fabricated success or budget-dependent admission |
| A15 | Rerun collector / archive a role task / link later repair | Totals remain idempotent and non-overlapping; history remains attributed to the original configuration/candidate |
| A16 | Installer meets an unowned file or shared Claude target | Installation stops with a concrete conflict or preserves that consumer via the manifest; no destructive overwrite |
| A17 | New package fails after cutover | Managed links/config restore; current code/evidence/GitHub history survive; admissions stop until compatible |
| A18 | Second repository with different commands | Same package completes a fixture workflow from its profile; no JobCtrl-specific branch in the generic engine |
| A19 | Ordinary small editorial request | Applicable static check and endpoint only; no invented unit tests, review/QA tasks or full-stack cycle |
| A20 | Model defaults changed by the user | Effective change is recorded as a new segment; no hidden package pin or experiment changes the user's selection |
| A21 | Another writer advances target or source during merge | Server rejects stale unverified integration, or a newly evaluated candidate is used; actual merge tree matches the recorded integrated tree; mismatches remain exposed-unverified |
| A22 | High finding is repaired, including a local-only request | Independent technical fix verification removes the finding's technical block before final gate evaluation; later remote publication/closure is due only at its authorized boundary |
| A23 | JobCtrl enrolled, another repository not enrolled, same Codex host | The first uses its pinned devflow version, the second retains frozen legacy instructions/settings; rollback preserves both routes and active-attempt versions |

Core tests use `uv run pytest tests/domain tests/contract tests/adapters tests/migration`. Acceptance tests use `uv run pytest tests/acceptance` with fake adapters for deterministic failures and a separately enabled synthetic host/GitHub canary for actual integration. These are planned package commands, not tests already run.

JobCtrl proof uses the relevant existing commands: `corepack pnpm api:check`, `api:test`, `web:check`, `web:test`, `web:e2e`, `web:test-d`, `web:storybook:test`, `extension:check`, `extension:test`, `extension:e2e`, Python focused pytest/Ruff and scripts tests as applicable. Each full command uses the `corepack pnpm` prefix. W05 chooses exact test files after extracting the ownership primitive; tests must exercise foreign paths and launch failures rather than assert implementation text. Run docs build for changed published navigation/links. The cumulative high-risk gate adds the actual impacted full matrix; it does not claim the root aggregate contains the separate web suites.

## 7. Enrollment, migration and activation procedure

1. **Inventory.** Resolve the reusable package destination and enrolled repository from its remote, not its folder name. Record active task/worktree IDs, dirty files, all applicable instructions/skills and symlink consumers. Export existing issue/Project IDs and backlog source entries with pagination. Capture user defaults and explicit overrides privately.
2. **Preflight.** Check native task tools, Git/gh/uv/npx/ccusage, authentication, Project permissions/IDs and the approved merge-protection profile. Current `gh project list --owner ebarti` lacks `read:project`; this does not mean no Project exists. Current main rules require PRs/thread resolution and restrict updates to the owner, but expose no strict required-status rule. The installation manifest must include the proposed additive required-check/freshness change and a no-bypass check; automatic merge remains disabled until its conformance test passes. Do not print or copy tokens. [GitHub Project authentication](https://docs.github.com/en/issues/planning-and-tracking-with-projects/automating-your-project/using-the-api-to-manage-projects#authentication).
3. **Build and validate in isolation.** Package contracts, adapters, fixtures and instruction changes; pass the acceptance matrix before switching ordinary work. Synthetic GitHub mutations need their declared test repository/PR scope. No production DB or application-submission path is used for workflow testing.
4. **Prepare installation and migration manifests.** Include before/after paths/hashes/consumers, package version, repository profile ref, each backlog source ID and its existing/new issue target, labels/dependencies/Project mapping, and rollback action. Deduplicate by stable identity plus reviewed scope; uncertain duplicates remain explicit.
5. **Perform one activation.** Drain legacy runs or explicitly transfer them; switch managed Codex entry points and the JobCtrl profile; migrate issue records; publish one canonical workflow entry. Remove obsolete active definitions and duplicate status descriptions. Leave historical plans immutable.
6. **Independent readback.** Re-enumerate installed skill/config routes separately from the install selection, compare backlog source counts/dispositions to live issues, check Project fields and finding states, and confirm no unrelated dirty work changed.
7. **Stabilize.** Use ordinary needed work, keep defaults, record workflow versions and corrections, and fix process failures. No model benchmark starts automatically.

Migration accounting is `source entries = migrated new issues + mapped existing issues + explicitly deferred/retired entries`, with every source ID assigned exactly once. Preserve original text/context in a private migration artifact and a safe source link in public issues. Issue creation alone does not mark it Ready. A rerun updates the same IDs and does not create duplicates.

## 8. What must be settled before implementation starts

The architecture selects the components, ownership, state model, host bridge, interfaces, failure behavior, package layout, replacement map, tests and cutover. These are design decisions, not deferred brainstorming.

Proposed deployment defaults are a private `developer-workflow` source repository checked out under `/Users/eloibarti/Github/developer-workflow`, the private roots named in the contract, and reuse of the selected existing GitHub Project after access permits its inventory. Repository creation/publication, Project identity and the additive merge-protection change are recorded in the reviewable installation manifest before activation. Exact Project/field IDs and granted credentials are enrollment values, not unresolved architecture. Do not infer publishing, rule-change or merge authority from this design review.

Runtime acceptance still has to be demonstrated by implementation. In particular, the actual host canary, GitHub comment/file-anchor behavior under the selected permissions, interrupted recovery, harness fixes and installed ccusage compatibility are implementation gates. A missing capability must produce its designed blocked state, never a silent substitute that drops inspectability or evidence.

Model comparison remains deferred until W09's workflow stability criteria are satisfied. The comparison plan and metrics contract retain same-case duplication and historical attribution; they do not determine model settings for the reset.

## Security admission revision (0.2.0)

The [verified intake contract](issue-trust.md) supersedes caller-supplied authority admission and old-pin execution retention. Package acceptance requires default CLI/application denial, constructor-injected synthetic protocol proof, exact source/scope/operation/expiry/revocation binding, unchanged replay, external lineage through owned projections, old-pin bypass regression, and capture correlation/recovery without duplicate writes. An independently authenticated real host intake/human-validation capability remains unavailable and mandatory before managed execution activation; synthetic tests cannot pass that deployment gate.
