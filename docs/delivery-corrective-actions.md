# Delivery corrective actions

New ordinary issues run as one issue and one PR when `feature_delivery_version`
is absent or null. Decomposition remains optional for large issues. Existing
accepted feature plans retain their original workers and custody.

`assessment_batch_version: 1` collects ordinary findings from automated checks,
browser QA, review and verification on the same unchanged candidate before one
repair with the original implementer. A changed candidate, unknown cleanup,
invalid session identity or planning defect still stops assessment. Passing a
later stage cannot erase an earlier finding.

`validation_schedule_version: 1` schedules identical broad prepublication and
postpublication commands once, at the mandatory postpublication gate. It keeps
Git/history checks and dependency preparation before publication. A build worker
without a later gate keeps its prepublication validation. Commands with different
arguments, directories, limits, selectors or other execution requirements remain
separate. Reviewers use the controller's passing receipts for covered broad suites
and independently exercise uncovered behavior with focused probes. This is
scheduling, with a real later command receipt required; it is not a fabricated
cache hit or permission to skip validation after source changes.

`merge_reconciliation_version: 1` reconciles main at merge using the existing
published PRs. GitHub performs a clean merge of the target into the expected head.
The controller authenticates the exact resulting parents and tree, and requires
fresh CI on that combined candidate. A lost update response gets readback, never
a second write. Chunk implementation and review workers are not restarted just
because main moved. Unresolvable or conflicting direction is surfaced with both
source versions retained; the user decides an opposing product direction.

`delivery_timeout_seconds` and `no_progress_timeout_seconds` are optional positive
integers up to 86,400. Admission freezes them and the original `delivery_admitted_at`
clock. Continuation, idempotent command replay and operational capacity changes do
not give a new elapsed-time allowance. Workflow timers cancel active owned
activities and join their cancellation before ordinary finalization. A changed
candidate, completed role or gate verdict is progress; a phase change, status poll
or another pending CI observation does not move the progress clock. Product repair
counts remain independent of infrastructure recovery.

At the limit, stop launching work, settle existing native and GitHub effects, and
retain dirty or unpublished source. The status includes the stopping reason,
original run/issue, candidate, PR, evidence directory and implementation session
references. A regular thread can reuse those references after cleanup and ownership
release are confirmed. There are no automatic new threads, database copies,
transcript archives, budget resets or concurrent owners. An unknown external write
keeps ownership until readback establishes its result.

Timing consists of actual start, finish and elapsed values for recorded stages and
commands, with parent elapsed time separate from summed overlapping work. Unknown
historical command timestamps remain unknown. CPU, memory, I/O and swap profiling
are outside this change.

The installed controller is excluded explicitly from temporary resource cleanup.
The production checkout stays Git-locked. Supported upgrades stop its owned stack
before changing imported source, retain the same persistence, start the qualified
worker and API, then independently read back readiness and configuration. Recovery
qualification covers the real maintenance, settlement and continuation paths with
original candidate, session and repair accounting retained.

Use bounded real issues for subsequent trials so completed and partial work is
useful. Keep the timing limits active, retain the same work on fallback, and assess
actual delivery outcomes. Do not resubmit work already transferred to a regular
thread or restart a paused trial without the user's instruction. Controlled local
Git and Temporal fixtures qualify failure handling; they do not replace real-work
trials or claim a product outcome shipped.
