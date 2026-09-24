# Storage and metrics contract

The standard-library helper stores records in one SQLite database; its default location and the `--db` option are documented with the [command usage](../skills/devflow/references/state.md) installed alongside the skill, and [schema.sql](../skills/devflow/scripts/schema.sql) owns exact columns and constraints.

| Table | Stored facts |
| --- | --- |
| `works` | Stable outcome ID, title, repository/issue/branch/commit, status, stage, blocker, timing and context |
| `claims` | Unique work/issue reservation, actual owning task, private task locator and observation times |
| `runs` | Work, actual agent/role/model/effort, status, timing, duration and source reference |
| `results` | Work/run, kind, observed status, commit, evidence reference and summary |
| `findings` | Work, summary, severity, status, commit, evidence, fix and thread references |
| `usage` | Unique observation, run/agent/model, token counts, estimated dollars/credits and source |
| `usage_allocations` | Observation-to-work weights with a total of at most one |
| `history` | Change deltas and imported notes with source/context references |
| `imports` | Source import identity and report |
| `runtime_sessions` | Bound task identity, role/model, transcript cursor and cumulative token checkpoint |
| `runtime_scopes` | Explicit task-to-work associations; several scopes leave usage unallocated |
| `runtime_events` | Deduplicated turns/tools, observed lifecycle events, counter resets, skipped transcript lines and re-baselines after them, denied coordinator boundary calls, timing and command fingerprints |
| `reconcile_intents` | Revisioned desired transition, owner/lifecycle fence, attempts, retry/error and next action |
| `reconcile_cursor` | Durable position for bounded fair sweeps of managed records |

Fields are ordinary queryable columns; optional `details` holds extra JSON context. Missing facts remain SQL `NULL`. Creation records use caller-supplied stable IDs: replay matching stored facts is a no-op, conflicting reuse fails. Work/run/finding updates retain change history and support optional stable event IDs. A started run can be completed under the same ID. Related rows and history commit in one transaction. The helper records facts without enforcing stage order, authorization or a passing gate.

Schema 7 upgrades schemas 2 through 6 transactionally, adding claims, runtime and reconciliation tables and a monotonic runtime generation as needed. Claims are unique by canonical issue URL and work ID. A different owner cannot claim or release the same reservation; retries by its owner retain it. Changing a claimed work's issue/repository requires release first. Claim/release history is preserved. Claims coordinate one shared database and do not expire automatically or establish live host activity.

`works.details.github` retains the selected Project, Status mappings and any unresolved creation attempt. Creation records its attempt before calling GitHub and saves the issue URL before further updates. A missing result requires reconciliation, never an automatic second create. Project Status and assignment are read back before success; labels and other Project fields are untouched.

Before assignee or Project mutation, synchronization commits a desired intent. Replay reads GitHub first and writes only mismatched fields. Acknowledgment requires issue, assignee and Project option ID/name readback. Revisions and the claim/runtime generation fence stale owners. Network requests run outside SQLite transactions; failures retain a bounded retry or one explicit `needs_decision` state. The periodic service calls no model or agent.

Successful synchronization also stores `details.github.sync`: expected issue
state, assignee, Project/item/field/option IDs, selected Status name and readback time.
`github.py audit --work-id` opens SQLite read-only, compares that expectation
and local claim/runtime with the live issue and selected Project item, including
both the Status option ID and its current name. Active linked work with no claim
requires reconciliation. Stop does not start a repeated model reconciliation conversation; unresolved `create_pending` remains durable. Observed Interrupt/SessionEnd blocks the root's
claimed work locally, including local-only work, while retaining claims, and queues linked managed work. The service releases a claim only after terminal root and descendant evidence. A resumed same-ID owner starts a new runtime generation that fences old recovery. Hard crashes and missing hooks remain unknown. Audit
returns `consistent`, `unknown` or `reconciliation_required`. Legacy records
without the successful-sync metadata remain unknown. An observed root owner
SessionEnd with a retained claim requires reconciliation; no claim expires on
elapsed time. A tracked `details.github.await` Actions URL and follow-up are
read during audit, as is a legacy `details.release_run` URL. Terminal run state
signals review, not acceptance, closure or approval. The service records terminal Actions outcomes once and, after readback, moves success to configured In review or failure to configured Blocked with a concrete reason. An already-closed issue can converge to configured Done when no live owner remains, including a terminal interrupted owner whose retained claim is released after verified readback. API failure cannot pass. A repaired mapping can be retried by repeating the explicit transition; this creates a fresh revision rather than replaying a `needs_decision` intent.

Record timestamps are supplied automatically when omitted. Active and terminal work/run updates stamp missing start/end observations; explicit nulls remain unknown. Legacy imports retain unknown endpoints. Observation times accept timezone-aware ISO 8601 values. A run's duration derives from its start/end timestamps; unfinished runs have no inferred duration. Evidence references are locators, not copied or validated artifacts.

Metrics query stored rows directly. Global usage counts each unique observation once, including unallocated usage; a work report sums its weighted allocations. Hooks observe cumulative token deltas from the local transcript adapter. A cursor and counters commit with observations, making replay safe; partial lines wait for completion, counter resets record a gap, and a malformed line is recorded as a `transcript_gap` event and skipped rather than stalling the cursor; the next counter after it becomes a new baseline recorded as a `counter_baseline` gap, so a skipped line can hide usage from a work but never charge earlier usage to it. Hosted tools and absent hooks are outside runtime coverage. Prompts, arguments and outputs are never copied into telemetry.

Cached input and reasoning output are subsets of input/output totals. Dollar and credit values remain supplied estimates. Reports include coverage, not fabricated values for missing observations. Turn time includes tool waits, phase time includes waits within that phase, and repeated tools are not necessarily retries. First-recorded gate outcomes are explicitly labeled; complete first-pass success, defect escape, human intent and counterfactual overhead are not inferred.

`import-legacy SOURCE` records each canonical source path once in `imports` with its report, reads the previous database without changing it and preserves artifact references; its usage and limits are documented with the [command](../skills/devflow/references/state.md#recovery-and-import).
