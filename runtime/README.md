
### Rerun gates after a runtime repair

A finalized native run that published a PR and stopped on review or QA can receive
one `published_gate_retry` command through `repair-admission-preflight` and
`continue-repair`. Use the current `protocol_revision`, iteration, candidate ID,
PR number and PR head; `additional_iterations` must be **0**. This resumes the
same run and PR, preserves its original failed results and resource receipts,
refreshes the measured runtime, and runs fresh review, local checks, QA, CI and
tracker readbacks. It grants no implementation turn, changes no source or repair
budget, and does not accept an old failed result as a pass. A stale head, unfinished
process, changed configuration or outstanding effect is rejected. Published-gate
retries remain limited to one admission.

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

Native heavyweight prepublication, local and browser check batches share one
service-wide host execution slot. Model roles remain parallel up to configured
capacity; queued checks leave the async worker available. Required commands,
test counts, timeouts and candidate scopes are unchanged.

If those fresh gates find a real source defect, the existing bounded repair
command can now continue a finalized native run: it authenticates the stopped
result and cleanup, reacquires the same issue, preserves historical evidence,
refreshes runtime preparation and resumes the original implementation session.
It keeps the existing PR and allows only the explicitly requested repair grant.
