
### Rerun gates after a runtime repair

A finalized native run that published a PR and stopped on review or QA can receive
one `published_gate_retry` command through `repair-admission-preflight` and
`continue-repair`. Use the current `protocol_revision`, iteration, candidate ID,
PR number and PR head; `additional_iterations` must be **0**. This resumes the
same run and PR, preserves its original failed results and resource receipts,
refreshes the measured runtime, and runs fresh review, local checks, QA, CI and
tracker readbacks. It grants no implementation turn, changes no source or repair
budget, and does not accept an old failed result as a pass. A stale head, unfinished
process, changed configuration, outstanding effect or second retry is rejected.

A finalized run whose implementation passed but whose prepublication checks
failed can use `prepublication_gate_retry` through those same public commands.
Bind `expected_revision`, `expected_iteration`, `expected_candidate_id` and
`expected_candidate_head`, with `additional_iterations: 0`. The exact candidate
must still be owned and within scope, at its original base with no PR or remote
branch. Fresh prepublication checks must pass before the normal publication,
independent review, QA, CI, tracker and cleanup stages proceed. The historical
failure remains recorded and the delivery cannot count as a first-pass success.

If those fresh gates find a real source defect, the existing bounded repair
command can now continue a finalized native run: it authenticates the stopped
result and cleanup, reacquires the same issue, preserves historical evidence,
refreshes runtime preparation and resumes the original implementation session.
It keeps the existing PR and allows only the explicitly requested repair grant.
