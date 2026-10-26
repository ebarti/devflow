# Timer controls and terminal-only settlement

The original admitted policy, admission timestamp, accepted plan and budgets are
historical authority. An accepted `disable_delivery_limits` update is operational
control of the same logical delivery. A fresh execution inherits that exact
control; it does not acquire a fresh clock, policy or attempt allowance.

## Failure mechanisms and owning boundaries

The automatic unpublished successor used to preserve the original elapsed anchor
but initialize disabled state exclusively from `recovery.state`. Its fresh outbox
entry intentionally has no recovery state. Consequently a real accepted disable
was lost, and a transient stop after the old deadline immediately cancelled the
successor. The five causal steps are: the old deadline fired; the successor had no
control; only recovery state could carry controls; a fresh admission uses no
recovery state; immutable admission and mutable execution control had no durable
handoff contract. Successor admission and workflow initialization own that boundary.

`delivery_control_lineage` is an additive server-owned v1 carrier on new successor
specs. Ordinary caller admission rejects the field. The final admission transaction
rechecks the exact predecessor row, unchanged logical issue and original admission
time, its current accepted control, and the genuine recorded accepted-control
event. Automatic retry also requires agreement with the authenticated closed
Temporal result. The carrier binds the new run/request, immediate predecessor
request/control/lineage digests, original accepted event identity and exact receipt.
Repeated successors authenticate the original event without copying an unbounded
history. Missing or conflicting evidence refuses continuation. Historical specs
and events retain their original bytes. Scope rehydration preserves an already
validated carrier; it cannot mint one from caller input.

Terminal-only tracker reconciliation has a separate boundary. The original
terminal transition already closes product timers and freezes its verdict. A new
tracker-only execution formerly reconstructed the pending checkpoint without
restoring that clock mode. Its watcher or first preflight therefore reapplied an
expired product deadline and changed the displayed frozen verdict to cancellation.
The causal chain is: cancellation replaced the checkpoint verdict; the product
clock fired before preflight; settlement clock mode had not been restored; only
expired bounded admissions exposed the ordering; execution initialization treated
settlement as renewed product work. Timer initialization now restores terminal
settlement mode before starting the watcher or evaluating any activity. A Temporal
patch marker preserves replay of already recorded histories. The original clock
stays visible, and existing checkpoint, owner, history, candidate, effect and
preflight checks remain in force. The tracker-only successor retains its existing
finite tracker deadline. Ordinary active product work keeps its original deadline.

## Verification and limits

`test_delivery_control_lineage.py` checks one automatic successor under concurrent
retry, reconstruction of the store after accepted control and after admission,
original policy/clock/spec preservation, rejection of caller carriers and uncertain
control evidence, and authentication across multiple successors. Its public fixture
uses an owned Git repository, SQLite store, loopback Temporal service, authenticated
HTTP update, actual resource finalization and sealed closure readback. Provider
preparation is controlled, the fresh base is the owned fixture base, and a local
claim is released only after confirmed cleanup because the fake fixture has no
external tracker. It proves successor preparation runs beyond the original elapsed
deadline with the exact accepted control.

`test_delivery_terminal_clock.py` uses authentic closed terminal tails and public
tracker reconciliation on modern and historical tracker admissions, with blocked
and cancelled checkpoints. Tracker transport is controlled and the original
tracker deadline is compressed to eight seconds. The elapsed product deadline is
two seconds. It checks exact replay admits one successor, original verdict and
error remain frozen, and preflight plus tracker execute without product roles.
Delivered checkpoints still require independent real candidate/PR readback
qualification; these fixtures do not fabricate it. Existing active-product timer,
control response-loss, and history/candidate/ownership refusal tests remain
necessary adjacent coverage.

Parallel pytest invocations must use distinct `--basetemp` directories. This
repository removes test temporary roots aggressively; sharing the default root
can delete another process's active fixture and invalidate its evidence. Heavy
Temporal fixtures also use the shared two-slot probe limiter.
