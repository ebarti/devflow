See [worker deployment migration](../docs/worker-versioning.md) for ordering
compatibility, optional pinned worker registration and required owner steps.

For new GitHub-owned feature admissions, see
[feature delivery and custody migration](../docs/github-feature-delivery.md).
The sections below describing per-run automatic retries and legacy public repair
grants apply to their original frozen versions. Feature mode uses one shared
execution claim, ten cumulative product repair cycles by default, and explicit
continuations of the same GitHub feature and stack. Version 2 child plans record
expected files separately from frozen execution authority and select admitted
checks for each chunk and its prerequisites. Required final acceptance remains.

A stopped feature can request a bounded plan correction with
`devflow-delivery --config /absolute/service.json revise-feature-plan --id RUN --request /absolute/revision.json`.
The request requires `command_id`, the observed `feature_plan.expected_revision`
(the local projection revision), and `reason` (1–4000 characters). Optional
`evidence` contains up to 16 existing owned `{path, sha256}` artifact references.
The authenticated HTTP route is `POST /api/runs/{id}/revise-feature-plan`; the
client and MCP operation are `revise_feature_plan`. The runtime crafts and
independently reviews the proposal. A caller cannot replace the plan or policy.
Exact retries return the recorded receipt; read the run after an uncertain
response and retain the command ID and request. Ordinary `continue_feature`
retains the adopted plan. Both flows preserve PR/session identity and repair use.
Readback includes the GitHub plan revision, child comment links, correction phase,
evidence, affected chunks, execution scope, expected files, and action eligibility.
Automatic correction requires a structured native planning defect tied to the
current plan, candidate and sealed evidence. Generic findings remain normal repair.
Reviewed future-work splits retain existing chunk IDs and acceptance, fixed started
dependencies and the published stack prefix, within the original authority. The
owned workflow performs this assessment without an external watcher.

After a genuine review rejection, an optional `reconsideration` object can request
fresh investigation and independent review under an authenticated changed controller
derivation context. It supplies `version: 1`, `kind: "derivation_context_change"`,
`rejected_revision_id`, the exact `proposal_sha256`, `expected_runtime_revision`, and
`expected_runtime_payload_sha256`. The service authenticates the completed earlier
native review, changed runtime context and retained worker authority before reserving
a successor. Missing, stale, unchanged or already consumed context refuses admission.
Every prior finding remains unresolved input to the fresh roles. The original
rejection and consumed cycles remain recorded; a new investigation uses the same
cumulative allowance. Only a fresh independent passing review and the existing
approval, custody and publication checks can adopt a correction. Keep the complete
request unchanged when recovering an uncertain response.

An authenticated stopped worker may expose its exact latest implementation findings
as an unpassed review subject, while preserving the original admission and terminal
candidate separately. Fresh roles receive its bounded original product findings as
unresolved evidence distinct from prior planning-review findings. A passing plan
review does not pass the product or reuse gates for changed content. After ordinary
approval and adoption, the original implementation session still requires normal
repair, fresh gates and product review.


### Automatic issue context

Every new native intake turn automatically receives the linked GitHub issue's
complete body and acceptance criteria, bounded comments, cross-referenced earlier
PRs and directly linked issue, PR and immutable file evidence. This read-only
controller retrieval also applies when feature decomposition is disabled. Each
snapshot records source identities, retrieval time and explicit unavailable or
truncated references; source text never expands the frozen execution authority.
Retries and reattachment preserve the exact snapshot of a launched turn.

Existing runs can record current context with
`devflow-delivery --config /absolute/service.json refresh-issue-context --id RUN --request /absolute/context.json`,
where the request contains a stable `command_id`. The HTTP route is
`POST /api/runs/{id}/issue-context`; the client and MCP operation are
`refresh_issue_context`. Indexed evidence exposes the snapshot. Refresh supplies
subsequent role launches and leaves completed role inputs, decisions, run outcomes,
claims, plans and limits unchanged. It does not answer a pending human decision or
restart a run.

### Native provider turn retries

New admissions freeze `provider_max_attempts` from the service configuration
(default 3; allowed range 1–3). A finalized typed provider rate limit, overload or
connection failure retries in the same owned session with backoff, within the
original role deadline. An explicitly rejected overload at turn start shares
that limit. Original failed turns, tool effects and token usage remain retained.
Unknown completion, lost transport, cancellation, authentication or budget errors,
collaboration conflicts and assessment findings never authorize another turn.
Existing frozen runs without this setting retain their one-attempt behavior.

### Fixed retry budgets

`max_attempts` bounds controller-created automatic retries. It defaults to 3 and
accepts integers from 1 to 10. The first budgeted admission freezes the issue's
automatic attempt ceiling; all earlier admissions, including archived and failed
runs, count against it. Changing run IDs, work IDs, or later configuration cannot
reset that automatic ceiling.

An explicit owner submission can start a new run under the current admission
policy after the earlier claim is released. Historical attempt counts do not
reject that request. The new admission preserves earlier runs and their frozen
policies; replaying an accepted command returns its original receipt.

Each budgeted run keeps its original `max_repairs` ceiling. A stopped resume may
spend unused original iterations, but public continuations and historical grant
rows cannot extend that ceiling. Policy recovery cannot grant positive iterations.
Already-admitted histories without the frozen budget version retain their original
behavior. Automatic attempt accounting is scoped to one authoritative tracking
database; separate databases cannot enforce a shared limit.

### Failure classification

New original runs freeze `automatic_retry_version: 1` at admission. When they
stop, `checks.failure` records a bounded reason, the controller phase and a
`terminal` or `transient` classification. The default is terminal. Only typed
activity transport failures/timeouts and recognized controller deadline results
are transient; model findings, invalid inputs, scope violations and failed or
changed-head CI remain terminal. Returned provider findings also remain terminal.

This is a diagnostic, not permission to execute again. Unknown cleanup and
publication effects retain their existing guards. The service may schedule one
fresh successor for the latest safely closed, unpublished issue attempt, within
its original total ceiling. Unknown observations wait with bounded backoff;
unresolved operator effects prevent another admission. Classification never
extends a budget. Scope amendments preserve the original version markers and
attempt ceiling. Existing workflows and historical continuation paths keep their
original payloads. Operator steps: deploy through the usual service update when
ready. No database schema migration is needed, but review issue attempt counts
before deploying: archived, failed and superseded prior admissions all count.
Choose `max_attempts` (1–10) before the first budgeted admission if automatic
retry headroom is needed. Three prior admissions exhaust the default automatic
ceiling; ten exhaust every supported setting. Once frozen, changing configuration
cannot raise it. This automatic retry limit does not reject a new explicit owner
submission. A stopped resume still requires unused original repair iterations;
it cannot grant extra turns or change an earlier run's frozen policy.

### QA findings require a passing assessment

New investigation assessment adjudication requests are rejected. A disposition
cannot turn failed QA into a delivered result; repair the findings and obtain a
passing assessment. Already consumed command responses remain readable without
creating another admission. Historical completed results are preserved for replay.

Before deploying, check every owning dashboard's run details for
`investigation_adjudication` and unfinished `investigation_adjudication_queued`,
`adjudication_preflight`, or `waiting_ci` executions. Finish or cancel those legacy
paths through the normal controls first. If an unfinished legacy tail nevertheless
reaches its terminal transition after upgrade, it blocks, retains its original
QA/findings, and performs normal cleanup and tracker reconciliation.

The two `delivery-adjudication*-history.json` fixtures were recorded from
`c04f00eb43eb225728b63c82026ffe97a41cafc2` on an isolated in-memory Temporal server using synthetic inputs
and a fixed fixture worker identity. They cover both completed and waiting-CI
histories; the legacy workflow body is retained solely for replay compatibility.

### Rerun gates after a runtime repair

A finalized native run that published a PR and stopped on review or QA can receive
one `published_gate_retry` command through `repair-admission-preflight` and
`continue-repair`. Use the current `protocol_revision`, iteration, candidate ID,
PR number and PR head; `additional_iterations` must be **0**. This resumes the
same run and PR, preserves its original failed results and resource receipts,
refreshes the measured runtime, and runs exact-head local checks before fresh review, QA, CI and
tracker readbacks. It grants no implementation turn, changes no source or repair
budget, and does not accept an old failed result as a pass. A stale head, unfinished
process, changed configuration or outstanding effect is rejected. Published-gate
retries remain limited to one admission.

Verification that explicitly names a JUnit recipe in tracked `scripts/checks.toml`
executes that recipe with `{report_path}` bound to an owned artifact path. The
controller retains the real report, metadata and plan hashes, and counts actual
test cases. Missing, malformed, empty, changed or failing reports cannot pass.
Existing configured checks continue to run unchanged.

Named JUnit and static recipes come from the admitted base commit in the source
repository. Candidate edits to `scripts/checks.toml` cannot change those commands;
a recipe update takes effect for runs admitted after it merges. Metadata hashes
refer to the exact base blob, while the checks still execute against the candidate.

A finalized published candidate whose passed local assessment omitted such a
requested report may receive one report assessment through `published_gate_retry`
after a measured runtime repair. Admission authenticates the old empty artifact
manifest and the omitted tracked recipe. It uses a separate report namespace,
retains prior admissions and source grants, and requires fresh local checks,
review and QA with no source iteration. A second report assessment is rejected.

The first published assessment after a finalized source repair is also supported.
It retains the original sealed repair grant and iteration ceiling, grants no new
source turn, and authenticates every historical admission during cleanup.

Accepted structured verification that names tracked pytest files also runs those
files in their tracked locked Python project. The controller owns the generated
environment and retains manager/lock/selector provenance and candidate-bound JUnit
artifacts. Failures before a check launches are preparation failures with concrete
diagnostics; uncertain native execution remains unknown.

When an accepted verification step requests focused regressions without naming
files, `published_gate_retry` can include `verification_test_paths`: at most 32
unique tracked Python or Vitest test paths. These are sealed execution selectors,
not a changed accepted plan or policy. Python projects require tracked metadata
and `uv.lock`; Vitest packages require the tracked pnpm workspace lock. Recipes
retain the plan hash, selector hash, lock metadata, executed count and JUnit report
for the exact candidate. Arbitrary commands, source changes and escaping paths
are rejected.

A finalized run that passed independent review, QA and local checks but stopped
on required CI can use one `published_ci_retry` with the same zero-iteration
request fields. It preserves candidate-bound passed assessments and the failed
CI history, observes required checks again on the same open PR/head, then performs
normal cleanup and tracker finalization. It executes no model role, source repair,
publication or repeated local check. Delivery still requires fresh successful CI.

An earlier published assessment that stopped before any fresh independent role
launched because the planned Python environment could not be registered may use
one `published_check_prelaunch_retry` with the same exact zero-iteration request
fields. It requires a repaired measured runtime, complete observed process
journals, no live owned process or port, a prepared profile but no planned process
launch or generated environment, and unchanged owned source and open PR head.
The old unknown receipt and prior admissions remain immutable; this resumes the
unassessed checks and gates without another source repair. A second retry is rejected.

A finalized run whose implementation passed but whose prepublication checks
failed can use `prepublication_gate_retry` through those same public commands.
Bind `expected_revision`, `expected_iteration`, `expected_candidate_id` and
`expected_candidate_head`, with `additional_iterations: 0`. The exact candidate
must still be owned and within scope, at its original base with no PR or remote
branch. Fresh prepublication checks must pass before the normal publication,
independent review, QA, CI, tracker and cleanup stages proceed. The historical
failure remains recorded and the delivery cannot count as a first-pass success.

A second prepublication admission is available only after the first one finalized
another failed gate and the installed native runtime payload differs from the
controller payloads that actually executed its gates. Queued preparation is not
treated as the consumed runtime after an owner source update. It
retains the first admission and check evidence in separate namespaces, keeps the
same feature candidate and grants zero implementation iterations. A third
admission, an unchanged runtime, or any changed feature authority is rejected.

For a failed controller publication that already committed and pushed the exact
checked content, `recover-publication` accepts `expected_pr_number: 0` with the
current revision, original candidate ID and actual pushed `expected_head`. It
completes the original pending publication effect, then runs independent review,
QA, CI and tracking; it creates no implementation turn or duplicate branch. Both
admission and execution require the owned remote head to remain unchanged.

Native admission freezes the GitHub publication branch separately from the Git
base commit. A full commit SHA resolves through symbolic `origin/HEAD` only when
that branch identifies the exact pinned commit. Tags, unbound commits and moved
defaults are rejected before accepting work. PR creation and readback use the same
branch binding. Legacy SHA-based publication recovery preserves the original Git
base and applies the same exact-match resolution.

Native heavyweight prepublication, local and browser check batches share one
service-wide host execution slot. Model roles remain parallel up to configured
capacity; queued checks leave the async worker available. Required commands,
test counts, timeouts and candidate scopes are unchanged.

If those fresh gates find a real source defect, the existing bounded repair
command can now continue a finalized native run: it authenticates the stopped
result and cleanup, reacquires the same issue, preserves historical evidence,
refreshes runtime preparation and resumes the original implementation session.
It keeps the existing PR and allows only the explicitly requested repair grant.
### Required CI waiting

New admissions freeze `ci_wait_seconds` from service configuration (default
10,800 seconds; range 60–43,200). Pending checks and unavailable GitHub readbacks
use bounded backoff while the activity heartbeats. GitHub reads run off the
worker loop, and required success must still match the exact PR head. A failed
check or changed head stops immediately; the deadline leaves CI pending.
Older frozen specifications keep their original twenty-minute waiting behavior.

### Durable control operations

`POST /api/operations` accepts an envelope with `operation_id`, `run_id` (null
for submit), `kind`, and the original `request` object. Operation IDs are a
separate namespace from domain command IDs; keep both and the complete request
unchanged after a timeout. The response is the operation record: pending or
running means the original work continues, not that its result is available.
The operation and per-run operation list are available from
`GET /api/operations/{id}` and `GET /api/runs/{id}/operations`. A different
request with the same operation ID is refused. For effectful commands, a second
operation ID carrying the same domain command and request returns the original
operation; a conflicting command ID is refused. Observations retain separate
identities so a fresh preflight can be followed by its distinct acknowledgement.

The service dispatches persisted pending work on restart, except pending resume
operations described below. A previously running
operation is unknown once its exclusive owner is gone; it is not launched
again. `POST /api/operations/{id}/reconcile` checks supported original domain
receipts without creating another command. Temporal updates record the exact
workflow, execution run, update ID, request and transport binding before sending;
reconciliation reads only that original update. If no exact receipt establishes
the outcome, the operation remains unknown and requires domain-specific recovery.
A recorded success is historical evidence and does not establish current
execution or merge eligibility. Original preflight, acknowledgement and
admission boundaries continue to check the current owner and authority.

CLI `operation-create --request envelope.json`, `operation --id ID`,
`operation-reconcile --id ID`, and `run-operations --id RUN` use an already
running service. The corresponding MCP tools and dashboard operation panel
expose the same state; none reissues an uncertain effect under a fresh ID.

### Queued pause controls

Pause currently applies only to explicitly admitted, untimed ordinary runs before
their workflow has started. `GET /api/runs/{id}/pause-control`, CLI
`pause-control --id RUN`, MCP `get_pause_control`, and the dashboard report the
same scope, persisted observed state and blockers. A `pause_requested` or
`pausing` state is not a completed pause. Running workflows, feature children,
native work and timed runs are not yet supported by this control.

The CLI `pause --id RUN --request FILE` and MCP `pause_run` accept an object
with caller-chosen stable `operation_id` and `command_id` values and the
`expected_revision` from pause control. `resume` and `resume_run` additionally
require its `epoch`; resume is an explicit
instruction. Both use the durable operation journal and require an already running
service. A resume operation still pending when the service restarts is held;
the user must explicitly resubmit the same operation to dispatch it. Preserve
the complete request after a timeout, inspect the operation and reconcile an
unknown result before attempting another command. A recovered
acknowledgement records the original intent; read pause control again for current
settlement or activation. The direct API and dashboard remain available with the
same command ID, revision and epoch semantics.

Routine CLI `run --id RUN` and MCP `get_run` read the versioned persisted
status projection. They do not reconstruct current execution authority. Use
CLI `run-detail --id RUN` or MCP `get_run_detail` for explicit full-detail
and evidence reconstruction.

The dashboard retains an uncertain submitted run, decision, or feature request
in tab-scoped session storage so reopening the form keeps the original command
and request. It clears that intent only after a durable success or known
refusal. The operation panel exposes the recorded status; a pending response
never creates a successor link or navigates to an undefined run.
