
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
another failed gate and the installed native runtime payload has changed. It
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
