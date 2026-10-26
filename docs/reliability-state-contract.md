# Delivery state ownership and compatibility

## Investigation

Issue #235 includes a reproduced same-iteration gate error: an earlier `ci_wait`
event remained in the history, a later `checks_retry_started` attempt ran a local
command that exited 7, and the public phase gate still reported `completed`.
The command result and workflow state correctly reported failure. The status
reducer selected historical completion before the newer failed observation.

The actors are the deterministic workflow, its asynchronously delivered SQLite
projections, the operator's accepted controls, and readers of persisted status.
The invariant is that a projection describes the latest admitted attempt and
cannot erase a newer control revision. Events are immutable history; they are
not unconditional current verdicts. This is a repeated-use failure: the initial
gate is correctly green, but a later attempt within the same iteration makes
the first completion obsolete.

The causal chain is: the current gate appeared complete because a historical
completion won; that history reached the surface because the reducer grouped
only by iteration; the reducer allowed it because it did not compare attempt
start and completion order; retries expose the error because they intentionally
reuse the candidate and iteration; the public contract still requires the new
attempt's real result. The fix belongs in the persisted gate reducer, rather
than the command runner or workflow result, because those already retain the
failed command accurately.

The projection writer has the corresponding ordering boundary: an activity
already in flight may arrive after a newer accepted control. A lower protocol
revision must be ignored as a stale observation, while equal-revision projections
remain usable for compatible cleanup and readback settlement. This preserves
the separate SQLite projection sequence and deterministic protocol revision.
The projection activity also captures its actual Temporal workflow ID. The store
compares it to a non-null current workflow owner under the write transaction, so
an old body that authenticated before a SQL wait cannot overwrite a newly queued
successor even at the same protocol revision. Historical initial rows without an
explicit successor owner retain their original projection behavior.

A separate owned combination probe found automatic unpublished retry still
compared capacity inside the full policy after service reconstruction. A live
capacity-only edit therefore produced `fresh attempt changed frozen execution
policy` while the equivalent feature continuation was compatible. The retry was
refused because policy equality included scheduling; that comparison lived at
the successor transaction; only a restarted service using the changed capacity
exposed it; authority and scheduling were expected to remain separate. The fix
belongs in that transaction's policy comparison: compare every field except
capacity, then retain the predecessor's exact full policy and digest. Current
capacity continues to come from `operational_config`; it grants no new scope,
command, role, timer or repair allowance.

## Ownership contract, version 1

| Fact | Authoritative owner | Read model and compatible transition |
| --- | --- | --- |
| Original scope, permissions, endpoint and product budgets | Immutable `delivery_runs.request_json`, its request/policy digests and frozen execution configuration | A status digest names those original bytes. Capacity is scheduling policy; it does not rewrite an admission. Scope/plan changes require their existing explicit authenticated amendment receipts. |
| Accepted plan | The admitted plan plus the durable accepted-plan record; feature plans retain their registry revision and plan digest | A status read distinguishes admitted and accepted plan digests. It cannot accept a plan or grant a new iteration. |
| Execution custody | The current workflow ID and protocol revision; feature deliveries additionally use the shared execution registry owner token/generation | Recorded ownership is labelled `recorded`. Launch and recovery still authenticate the current owner and frozen execution authority. |
| Environment binding | Frozen execution configuration, durable preparation, and acknowledged runtime installation/location receipts | Relocation and cleanup readback append their authenticated operational receipts. Original per-run source, scope, candidate and native-session custody remain independently checked. |
| External effects | Effect journal plus publication executor lease and its mutation-stage generation | A requested/pending effect is not completion. Lost mutation responses remain unknown until readback reconciles them. The activity waiter does not own the synchronous publication body. |
| Operational timer control | Accepted control event/receipt and server-admitted `delivery_control_lineage` v1 | New successor admission authenticates the original event and current predecessor under its transaction. The original clock and product budget remain unchanged. |
| Dashboard/tracker state | Derived projection, with independent projection revision and deterministic protocol revision | Lower protocol revisions cannot replace newer control state. Current gate attempt starts invalidate earlier same-iteration completions; current failed/unknown evidence remains failed/unknown. |
| Historical verdict and later cleanup/publication observation | Closed workflow/history and append-only events, respectively | Existing cleanup reconciliation records `original_checks` and its closed-result binding separately from the fresh observation. Neither status nor reconciliation rewrites original admission bytes. |

`state_contract.version = 1` describes these owners; it is a read model, not a new
grant or competing authorization database. The `admission_sha256` names the full
original input, including its historical environment; `policy_digest` names the
original frozen policy. The environment and execution fields explicitly report
whether proof was reconstructed. Digests alone never replace their existing
custody checks.

`GET /api/runs/{run_id}/status` returns a v1 `persisted_projection` envelope with
the current recorded decision, protocol and projection revisions, checks, effect
counts, candidate/publication and phase gates. `DeliveryStore.persisted_status()`
opens a query-only SQLite read transaction.
It reads the run, grouped effect states and a fixed set of indexed latest gate
events. It does not call preparation, runtime installation verification, Git,
Temporal, GitHub or effective-spec reconstruction. It remains available when an
execution proof is unavailable and labels that proof `not_reconstructed`.
It cannot acknowledge a remote effect or make a run executable. Detailed evidence
inspection and every mutation/launch retain their existing strict checks.

Writable store initialization adds gate indexes without rewriting historical
events or specifications. Existing admissions are exposed through the same v1
read model; no historical version markers are injected. Their execution remains
governed by the original markers. New successor controls are additive and bound
to authentic predecessor evidence. Current execution configuration already treats
capacity separately through `operational_config`, and runtime relocation already
retains the original per-run authority; this layer reuses those safeguards.

## Verification

The owned gate fixture executes the production CI-continuation controller in a
real loopback Temporal service. A real subprocess exits 7; the production
projection activity and public ASGI GET retain the failed local result and expose
the local phase gate as failed. Earlier completion is seeded through production
`project()`. Recovery admission readback is a controlled activity and the fixture
provider/tracker is local; it does not prove remote GitHub custody.

Separate fixtures exercise overlapping projection writers, a new gate attempt
that still carries its prior result, a reset outside the 200-event display window,
and persisted status when effective-spec or mutable-store proof access fails.
The capacity fixture proves the read model, original admission and retained
candidate do not change after a live capacity edit. Existing execution-config
tests additionally exercise real admitted dispatch/activity/projection across
capacity changes and reject conflicting transport/scope/command changes.

The runtime relocation, tracker transport/conflict, publication concurrency and
terminal-clock qualifications must be reported individually with their fixture
seams; a passing unit count does not establish their composition. The additive
persisted status method must be consumed by ordinary status interfaces; detailed
evidence reads intentionally retain stricter proof requirements.
