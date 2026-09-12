# Storage and metrics contract

The standard-library helper uses SQLite at `$XDG_STATE_HOME/devflow/workflow.sqlite3`, or `~/.local/state/devflow/workflow.sqlite3` when unset. `--db` selects an explicit location. [Command usage](../skills/devflow/references/state.md) lives with the installed skill; [schema.sql](../skills/devflow/scripts/schema.sql) owns exact columns and constraints.

| Table | Stored facts |
| --- | --- |
| `works` | Stable outcome ID, title, repository/issue/branch/commit, status, stage, blocker, timing and context |
| `runs` | Work, actual agent/role/model/effort, status, timing, duration and source reference |
| `results` | Work/run, kind, observed status, commit, evidence reference and summary |
| `findings` | Work, summary, severity, status, commit, evidence, fix and thread references |
| `usage` | Unique observation, run/agent/model, token counts, estimated dollars/credits and source |
| `usage_allocations` | Observation-to-work weights with a total of at most one |
| `history` | Change deltas and imported notes with source/context references |
| `imports` | Source import identity and report |

Fields are ordinary queryable columns; optional `details` holds extra JSON context. Missing facts remain SQL `NULL`. Creation records use caller-supplied stable IDs: replay matching stored facts is a no-op, conflicting reuse fails. Work/run/finding updates retain change history and support optional stable event IDs. A started run can be completed under the same ID. Related rows and history commit in one transaction. The helper records facts without enforcing stage order, authorization or a passing gate.

Record timestamps are supplied automatically when omitted. Observation times accept timezone-aware ISO 8601 values. A run's duration can be derived from its supplied start/end timestamps; no elapsed duration is guessed for an unfinished run. Evidence references are locators, not copied or validated artifacts.

Metrics query stored rows directly. Global usage counts each unique observation once, including unallocated usage; a work report sums its weighted allocations. Cached input and reasoning output are subsets of input/output totals. Dollar and credit values are agent-supplied estimates, with no automatic collection or pricing lookup. Reports include missing-field counts for recorded observations; they cannot measure entirely missing observations. Summed run duration is agent time, not wall-clock completion time.

`import-legacy SOURCE` is a one-time import per canonical source path. It reads a previous database without changing it, normalizes useful history and preserves artifact references. Its report identifies skipped records by ID/count so the original archive remains the reference for those records. The old `state.sqlite3` remains separate; import does not execute or resume historical work.
