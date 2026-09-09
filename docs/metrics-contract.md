Implementation design: [architecture](architecture.md), [contracts](implementation-contracts.md), and [build/cutover plan](implementation-plan.md) now own the proposed reset. The reusable package adopts JobCtrl first; model comparison remains deferred.

# Proposed JobCtrl workflow metrics contract

Status: assessment proposal, revised 9 September 2026. Collection and calculation use deterministic programs. No model calls are needed to read usage, calculate cost, measure durations, or synchronize status. Human or agent judgment is needed to identify a defect or explain a scope correction; its recorded classification becomes input to the calculations.

## Units and boundaries

- **Work item:** one requested, independently valuable outcome. Usually identified by a GitHub issue; an existing PR or stack can identify an operational request such as merging verified work. A stack is not automatically multiple outcomes.
- **Scope version:** the accepted request and observable acceptance criteria, with an explicit requested terminal action. Corrections of misunderstood instructions retain the original scope; new user requirements create a recorded scope amendment.
- **Attempt:** an execution of that work item under a recorded workflow version and model configuration. Resuming after an interruption remains the same attempt. A restart after abandonment or rejection is a new attempt linked to the same item.
- **Comparison pair:** later, after workflow stabilization, two independent experimental attempts of the same frozen case under different model/effort configurations, with identical starting code, scope, context, environment, and evaluation policy. Both share one case/pair ID and have separate run/arm IDs. They do not create two delivered product outcomes.
- **Task:** a Codex task participating in an attempt, with a named role and owner. Implementation, review, and QA tasks share the work ID. Their messages do not create additional work items.
- **Finding:** one distinct violated invariant and underlying cause. Duplicates share a finding ID, including reports made in multiple PRs. Severity and disposition remain explicit.
- **Response:** a uniquely identified model response, the smallest usage accounting unit. Replayed parent history and repeated cumulative counters must not create another charge.

Store UTC timestamps and display the user's timezone. A report always includes its cutoff, population, missing-data count, and observation window.

Initially collect these measures during workflow changes and stabilization using the user's defaults. Keep workflow versions distinct and record any user-requested configuration change. Different work completed in these periods provides operational evidence; it is not the proposed model comparison. That later comparison duplicates the same cases after the workflow is frozen.

`requested_at` is when the outcome was requested. `ready_at` is when its scope, acceptance, prerequisites, and authority are sufficient for execution. `started_at` is when an owner claims it and starts work. `handoff_at` is the first time the agent presents the candidate to the user as meeting its acceptance criteria. `delivered_at` is when the authorized terminal action and its independent readback actually succeed. An inaccurate completion claim does not establish delivery. `exposed_at` records when the relevant product change becomes available for use; it need not equal PR creation or merge.

User-found bugs during review count even if nothing has reached main. Post-release escapes are a narrower subset. Keep both views so the goal of zero user-found bugs cannot be improved by moving the reporting boundary.

## Defect type, detector, and stage

A defect is a confirmed violation of the accepted behavior or an existing invariant: functional behavior, required UX/accessibility, integration, persistence/restart behavior, compatibility, performance requirements, or safety. A new feature request, subjective preference without an agreed requirement, duplicate observation, or intentionally failing test during test-first development is not automatically a defect.

Record detector and stage independently. The fact that something was reported on a PR does not establish who found it or whether it escaped our checks.

| Situation | Classification | Included in the 30-day user-found-after-handoff numerator? |
| --- | --- | --- |
| Our reviewer finds a bug before we present the candidate as ready | Internal review catch | No; record review effectiveness, severity, and repair cost. |
| Our QA or CI finds a bug before that handoff | Internal QA/test catch | No; record interception and repair cost. |
| The user first finds a bug while reviewing a candidate already presented as ready, before merge | User-review escape | Yes, if attributed to that cohort and inside its window. |
| The user first finds a bug while using the exposed change | User-use escape | Yes, if inside the handoff window; also belongs to the post-exposure view. |
| Internal monitoring/QA first finds a bug after exposure | Internal post-exposure escape | No for user-first discovery; yes for the all-detector post-exposure metric. |

All confirmed bugs remain in the finding ledger. A user discovery before a ready claim is still a user discovery and may be a corrective intervention, even though it is outside the narrower after-handoff rate. If an internally known bug later reaches the user, record that user encounter and the prior-known state; do not hide it because the user was not the first detector. Keep these views separate from the first-discovery rate.

The 30 days is an observation window, not a review stage. Preserve the immediate pre-merge result, the 7-day early view, the mature 30-day view, and later discoveries. Do not postpone triage or repair until a window matures. Absence of a user report is weaker evidence when the affected path has not been used; retain known exposure/usage information.

Compute the mature after-handoff rate as:

`100 × distinct attributed first-user-discovered defects whose discovery is after the affected work's first ready handoff and within 30 days of it / implementation work items in that cohort whose full 30-day window has elapsed`

Group by workflow/configuration, work type, and risk. Do not dilute product-defect rates with editorial tasks or count every PR layer of one outcome as another denominator item. Report raw counts, unknown attribution, immature work, and late discoveries. One root-cause defect can have several observations; count its finding ID once in a given view.

## Attribution to historical workflow and model configurations

Save an immutable execution record before starting an item and append candidate, gate, and delivery records as work progresses. Read attribution from these records when a bug is discovered; never infer it from whichever configuration is current that day.

| Record | Required provenance |
| --- | --- |
| Work/scope | Work ID, original accepted scope version, acceptance/invariant IDs, and requested endpoint. |
| Experiment assignment | Only for the later comparison: experiment/cohort ID, frozen case/pair ID, assigned configuration/arm, execution order, assignment time, and experimental versus normal-delivery status. Both arms reference the same starting specification and base. |
| Workflow | Human-readable workflow version plus a content hash and retrievable snapshot of the effective policy bundle: applicable AGENTS instructions, used workflow/QA docs, skills, and relevant role/config values. Keep these private where necessary. |
| Model policy | Version/hash of model routing and escalation rules, separate from the actual execution settings. |
| Actual execution | Run and segment IDs; participating task and role; actual model ID, effort, service tier, timestamps, and fallback/escalation transitions. An intended default alone is insufficient. |
| Code and environment | Starting base, candidate/head/tree and final merge/release refs, relevant dependency/runtime/client versions, and the mapping across rebase or squash. |
| Verification | Exact candidate covered, reviewer/QA identity and effective configuration, tests/rubric version, claimed scope, findings, gate result, and evidence links. |
| Accounting | Unique response IDs, segment/work attribution, ccusage resolved version, token normalization version, price snapshot, and attribution completeness. |

A Git commit containing product changes is not by itself a workflow version. The policy bundle hash covers the effective workflow inputs; the candidate hash identifies the product code. Skills outside the repository need their own immutable snapshot or retrievable content, because a link to a mutable local file would lose the historical policy.

If a task switches model, changes workflow policy, or receives new scope, append a segment/amendment rather than rewrite the old record. Summarize an outcome as mixed when appropriate. Preserve the assigned experimental arm for the primary comparison, including its retries and recovery, while also reporting the models that actually ran. Otherwise a cheap candidate that needs an expensive rescue would misleadingly become a successful control task.

When a defect is confirmed:

1. Identify the code/artifact/version the user actually observed and its candidate or delivery record. Do not assume the current checkout is that version.
2. Link its finding ID, violated invariant, detection time, detector, stage, and evidence to that record.
3. Establish introducing provenance from the relevant code/history and the smallest useful before/after regression probe. Git blame is a lead, not causal proof. Label attribution confirmed, provisional, pre-existing, shared/interaction, or unknown.
4. Link gates that passed on the affected candidate and had the relevant invariant within their declared scope. Record a scoped missed detection, a missing required check, or a gap in coverage; these are different failures.
5. Link the repair run and fixing commit separately. Repair must not transfer the origin of the bug to the fixing model or current workflow version.

For example, an item implemented under workflow W3 by Sol/high, reviewed by Astra/max, and first reported by the user two weeks later under W4 still links to the W3 candidate. Its record identifies Sol's implementation segment, the actual reviewer and QA segments that covered it, the user's discovery, and the later repair configuration. It does not label the bug a W4/Astra implementation failure merely because Astra fixed it then.

Existing bugs first exposed by unrelated new work are not automatically attributed as newly introduced by that work. However, an item explicitly assigned to fix an existing bug fails acceptance if that bug remains; record failed remediation against that attempt even though the original defect predates it. An interaction can have multiple contributors; do not invent numerical blame shares.

A defect found during our review still links to the implementation configuration that produced the affected candidate and to the reviewing configuration that detected it. A user-found escape links to the historical delivered pipeline and its relevant passed gates. These associations support diagnosis. A controlled cohort comparison estimates the effect of configuration choice; provenance alone does not prove that one model caused an individual defect.

Backfill older work only when historical evidence supports it. Keep unknown or mixed attribution visible in overall defect counts and report coverage for configuration-specific rates. Do not silently assign missing history to the current default, and do not discard hard-to-attribute bugs to improve a score.

## Primary measures

| Metric | Exact calculation | Source and reporting rule |
| --- | --- | --- |
| User-found defects | Number of distinct confirmed defects first reported by the user after the agent presented the affected candidate as satisfying acceptance. | Linked bug issue/finding, affected candidate/version, discovery time, reporter, severity, introducing work when known. Show all severities separately; goal is zero. Feature requests and duplicates are excluded with an explicit reason. |
| User-found-after-handoff rate at 30 days | `100 × attributed first-user-discovered defects within 30 days after a ready handoff / comparable implementation items in that cohort with a complete 30-day window`. | Includes user review before merge and user use after exposure. Excludes our pre-handoff review/QA catches. Can exceed 100 because one item can introduce several bugs. Report counts, attribution coverage, immature items, and the distinct 7-day view. |
| Post-release escapes | Distinct confirmed defects first detected after the affected change's exposure time, split by user/internal detection and severity. | Includes defects discovered beyond 30 days in the all-time view. Attribute later discoveries back to their originating cohort without silently rewriting a previously frozen report. |
| Quality gate violations | Number of deliveries with an unresolved known Blocker/High defect or a missing required review/QA gate at delivery. | Read the finding and gate state at delivery, not its later corrected state. Required value: zero. Preserve the existing rule. |
| Tokens per work item | Sum unique attributed response usage across its owner, reviewers, QA, workers, retries, and coordination. | Report uncached input, cache reads, cache writes, output, and reasoning output separately. Reasoning is already included in output. Include failed and abandoned attempts. |
| Delivery cost | Sum the work item's attributed API-equivalent USD and estimated Codex credits through verified delivery. | Keep these as two separate values with explicit rate provenance. Neither is an observed subscription invoice. Show tool/CI charges separately where measured. |
| Cost through 30 days | Delivery cost plus attributable corrective work during the 30 days after delivery. | Repair tasks remain identifiable. Portfolio totals count each response once; do not sum overlapping original-work and repair-work reports. Show observation age. |
| Cost per delivered outcome | Total execution cost of every attempt in a predefined normal-work cohort, including failures and abandonments, divided by the number of distinct outcomes that met acceptance and reached the requested delivery endpoint by the cutoff. | If none delivered, report the total cost and zero deliveries; the ratio is undefined. Show unfinished and blocked items. Keep duplicate benchmark runs in the separate experiment account rather than inflating product deliveries. |
| Request lead time | `delivered_at − requested_at`. | Includes backlog and external waiting. Report queue time separately as `started_at − requested_at`. |
| Execution lead time | `delivered_at − started_at`. | Report p50 and p90 with sample count; include unfinished-item age and failure/abandonment counts. Do not infer active work from first/last session activity. |
| Phase elapsed time | Length of the union of recorded intervals assigned to Define, Implement, Verify, or Deliver for that work item. Revisits add intervals. | One owner-level phase at a time. Parallel review and QA occupy the same Verify interval rather than doubling elapsed time. Assign a shared boundary timestamp to close one phase and open the next. |
| Blocking wait time | Length of the union of explicit intervals in which the work item's next necessary step cannot proceed. | Record cause: user answer, required approval, CI, external service, dependency, or usage limit. If useful implementation continues, that interval is not entirely blocking. A parent awaiting an active reviewer remains active verification work. Overlapping waits are counted once. |
| First-pass verification | `first candidates passing every required gate without returning to implementation / all completed first-candidate verification attempts`. | Candidate means the first implementation handed to independent verification after focused local checks. A failed first candidate remains in the denominator even if later abandoned. Planned red/green test development is not a failed independent gate. |
| Autonomous completion | `eligible started items delivered without avoidable human intervention / all eligible items started in the stated cohort`. | Eligibility is recorded before execution from the work-item contract. Show delivered, in-progress, blocked, abandoned, and failed counts. Record necessary authorization separately. Also report a finished-attempt view; never hide pending items behind that denominator. |
| Corrective interventions | Distinct occasions when a human must restore the original scope, correct execution, point out missing proof, or rescue a stalled approach. | Several messages about the same unresolved cause count as one intervention episode. New requirements are scope amendments, not failures. A required credential/approval is separate; repeatedly asking for existing authorization is avoidable intervention. |
| Rework cycles and cost | Number and cost of returns from verification/delivery to implementation because acceptance or a required invariant was unmet. | Label cause: misunderstood requirement, product defect, integration, fixture/harness, infrastructure, or invalid evidence. A normal initial implementation loop is not a rework cycle. |
| Finding publication coverage | `distinct confirmed findings represented by a PR review thread / distinct confirmed findings requiring a PR record`. | Target 100%. Pre-PR findings remain in the issue and become due for publication when a relevant PR exists. An unavailable valid anchor or denied publication is an explicit incomplete record, never silently omitted. |
| Fixed-thread closure coverage | `verified fixed findings whose corresponding PR threads are resolved / verified fixed findings with PR threads`. | Target 100%. Check fix commit, verification, pushed head, reply, and final thread state. Dismissed/duplicate/deferred findings do not count as fixed. |

Classifications that require judgment retain evidence and a provisional/confirmed state. Counts are computed from those records; do not have another model repeatedly reclassify full conversations. A silent user is not evidence of affirmative acceptance. The 7/30-day defect views measure observed corrections and defects, with their exposure limits.

For the later paired experiment, report cost and elapsed time to the same verified candidate endpoint for each arm, including its review, QA, failures, retries, and repair. A run reaching a budget stop without acceptance remains incomplete/failed, not a cheap success. Show common setup/analysis overhead separately and include it in the total experiment cost; count each response once in portfolio totals. Benchmark completions are distinct from product deliveries. If one candidate is selected for exposure, subsequent user defects link to it; the unexposed alternative has no equivalent production observation window and cannot be compared through a production escape rate.

## Secondary diagnostics

Collect launch, handoff, follow-up, status-poll, verification-rerun, and failed-command counts as diagnostics. They are not productivity scores. Coordination cost can be measured only for usage explicitly assigned to coordination; the owner's entire cost is not a valid substitute. Keep mixed/unclassified usage visible and report attribution coverage.

For verification value, report confirmed unique defects by severity and verification cost, with false positives, duplicates, and unexecuted checks separately. Do not reward a reviewer for producing more comments. Evaluate reviewer recall on seeded, realistic defects and its precision on clean cases as part of a controlled benchmark.

## Token and dollar arithmetic

Normalize every source to disjoint input partitions:

`I = U + C + W`

`T = I + O`

Here `U` is uncached input, `C` is cache-read input, `W` is cache-write input, and `O` is total output. Reasoning output `R` must satisfy `0 ≤ R ≤ O` and is never added to `T` again. Reject inconsistent records rather than guessing their interpretation.

Native Codex `input_tokens` includes its cached input. In the tested ccusage 20.0.20 focused-session JSON, `inputTokens` excludes the separate `cacheReadTokens` and `cacheCreationTokens`. The six-session reconciliation exercised zero cache writes; nonzero cache-write normalization needs its own compatibility fixture before that path is called verified.

For each response:

`API-equivalent USD = (U × input_rate + C × cache_read_rate + W × cache_write_rate + O × output_rate) / 1,000,000`

Resolve the rate using the actual model, event time/rate version, service tier, context size, and applicable processing modifiers. For example, the current Standard short-context Astra rates are $10/$1/$12.50/$50, and Sol rates are $4/$0.40/$5/$20, respectively. Long-context and Fast rates differ. These are comparison prices, not a bill for subscription usage. [Official API pricing](https://developers.openai.com/api/docs/pricing).

Compute estimated Codex credits with the separate Codex rate card. Standard Astra uses 250/25/1,250 credits per million uncached-input/cached-input/output tokens, compared with Sol's 100/10/500. Astra Fast uses a 2.5× credit multiplier. An API Fast multiplier must not be reused as a subscription-credit multiplier. A missing rate or unsupported component produces an unknown/incomplete cost, never $0. [Official Codex rates](https://learn.chatgpt.com/docs/pricing#token-rates).

Use estimated Codex credits to compare subscription consumption and API-equivalent USD to provide a reproducible monetary comparison. If actual subscription allocation is useful for bookkeeping, define it explicitly: `attributable subscription charge for the billing month × work item's credit weight / all included work's credit weight for that month`. The attributable charge is an explicit input; do not allocate the whole ChatGPT subscription to JobCtrl by assumption. A partial denominator must be labeled partial. Keep purchased overage and tax/currency treatment separate. This allocation is an accounting convention, not marginal task spend.

When exact quota usage is unavailable per task, leave it unavailable. An account-wide percentage change amid concurrent activity cannot be converted into an exact task charge.

## Minimal collection design

1. **Usage engine:** invoke `npx --yes ccusage@latest` for deterministic Codex token/cost reports. Record the resolved ccusage version, command, source cutoff, and rate snapshot used. `latest` is the requested entry point; recording the actual version preserves auditability when it changes.
2. **Work/phase markers:** a small command appends a work ID, scope version, task ID/role, phase, timestamp, candidate ref, and event type to a private ledger. Reuse the same work ID across named review and QA tasks. No separate work item per phase.
3. **Hooks:** use session/turn lifecycle hooks only to enqueue a changed task ID for ingestion and close known activity intervals. A Stop hook records a turn ending; it must not assert that the work item is delivered. Coalesce repeated hooks and run collection outside the critical path. Do not add a model-powered automation to collect model usage.
4. **Attribution:** join aggregate ccusage usage to task ownership. Phase-specific token costs require a small version-tested adapter for unique response records plus phase markers, because ccusage session totals do not encode your development phases. The assessment's response-level collector provides a proven baseline for the observed schema; it is not a production collector yet. Shared response allocation weights must sum to one, and ambiguous attribution stays unallocated.
5. **GitHub:** read/update issues, PR findings, checks, and Project status at meaningful lifecycle transitions. Use structured batch reads with pagination and stable IDs. A review comment's resolved flag alone does not prove a verified fix; retain the finding disposition and evidence link.
6. **Reports:** produce SQLite queries/CSV/JSON and a small static report deterministically. No model invocation for counting, aggregation, rates, or routine status synchronization. Introduce OTel only if the lightweight path cannot supply a required measurement reliably.

The command surface should stay small: capture/validate an item, record a phase/candidate/outcome, collect usage, and show a report. These are proposed operations, not commands already installed in JobCtrl. One concise skill should describe when to use them; program code should enforce schemas, idempotency, accounting, and API interactions.

Store metrics privately outside the public repository. Allowlist metadata; omit prompts, reasoning text, command output, profile data, and secrets. Persist a stable owner/child relationship rather than relying on chat titles. A failed ingestion creates a visible data-quality record without fabricating a zero-cost successful run.

## Verification before adoption

Already demonstrated in this assessment: six complete related sessions reconcile to 1,037 unique native response records across input/cache/output/reasoning/total counters; current online pricing for their Standard short-context Astra usage matches the official API formula. The older offline probe returned zero cost despite nonzero usage; a subsequent online call did not make that offline path price Astra correctly. Do not adopt that offline path without a verified rate source. See `ccusage-latest-validation.json` and `ccusage-pricing-validation.json` beside this document.

Still required for an installed collector: replay idempotency, interruption/resumption, mixed-model and speed changes, nonzero cache writes, long context, phase boundaries, reused tasks containing multiple outcomes, moved/archived sessions, missing prices, and a correction linked back to an earlier delivery. Exercise only the formats the implementation claims to support, and report unsupported ones explicitly. Compare a deterministic repeated import to the first import: both must produce the same totals.

Use the [stabilization and deferred comparison plan](comparison-plan.md): stabilize the full workflow using the user's defaults, then run paired independent implementations of the same cases under two configurations with a fixed workflow and explicit duplicate-work budget. Retain unsuccessful attempts, publish sample sizes, and distinguish preliminary screening from evidence sufficient to change defaults.
