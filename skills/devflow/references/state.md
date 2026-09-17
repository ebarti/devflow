# Work records and metrics

Invoke this helper from the agent while following a Devflow skill. Resolve `scripts/state.py` relative to the installed `devflow` skill directory. For the default installation:

```sh
DEVFLOW_STATE="$HOME/.agents/skills/devflow/scripts/state.py"
python3.12 "$DEVFLOW_STATE" work create --id retry-fix --title "Fix retry behavior" --status active
python3.12 "$DEVFLOW_STATE" work show --id retry-fix
```

The database defaults to `$XDG_STATE_HOME/devflow/workflow.sqlite3`, or `~/.local/state/devflow/workflow.sqlite3` when unset. Place `--db /path/to/workflow.sqlite3` before the command to select another database. All output is JSON. The helper records supplied facts; it does not run the work, collect usage or decide completion.

## Commands

| Command | Required fields | Purpose |
| --- | --- | --- |
| `work create` | `--id`, `--title` | Record one requested outcome |
| `work update` | `--id`, changed fields | Save current progress, blocker or endpoint |
| `work list` | None | List records; optionally filter by `--status` or `--repository` |
| `work show` | `--id` | Read work with its runs, results, findings, allocated usage and history |
| `work claim` | `--id`, `--owner` | Atomically reserve the work/issue for one actual host task |
| `work release` | `--id`, `--owner` | Release that owner's claim, retaining its history |
| `record run` | `--id`, `--work-id` | Record an actual agent/role run |
| `run update` | `--id`, changed fields | Finish or correct the same run |
| `record result` | `--id`, `--work-id`, `--kind`, `--status` | Record check, review, QA or delivery observations |
| `record finding` | `--id`, `--work-id`, `--summary` | Retain a defect or unresolved concern |
| `finding update` | `--id`, changed fields | Link a fix, thread or verification outcome |
| `record usage` | `--id` | Retain a usage observation or estimate |
| `metrics` | None | Report all records or one `--work-id` |
| `import-legacy SOURCE` | Source database path | Explicitly import historical records |

Use stable IDs for created records. Retrying a creation with the same stored facts returns the existing record; different facts under that ID fail. After a work, run or finding has been updated, read its current state instead of replaying an outdated creation. Give each distinct run, result or usage observation its own ID. Work, runs and findings can be updated; updates retain change history. Finish a started run with `run update`, preserving its ID. Updating start/end timestamps recalculates duration unless you explicitly supply it. An optional stable `--event-id` makes an update retry explicit. Updating fields already at the requested values is a no-op.

`work list --claimed` shows issue owners, task locators and ownership observation times; `--owner` filters by task ID. `work show` includes the current claim. Use full GitHub issue URLs: claims normalize host/repository case and issue numbers, so another work ID cannot claim the same issue. Release before changing a claimed work's issue or repository. See [ownership and GitHub updates](ownership.md) for parallel work and interruption handling. Schema 2 records migrate transactionally to schema 3 by adding the claims table; existing records are preserved.

Writes accept `--file /path/to/record.json`, containing one JSON object with snake_case keys. Flags override fields in that file. Clear optional fields with JSON `null`, or a blocker with `--blocker ''`. Use `--help` on the specific command for its flags. Timestamps use ISO 8601 with a timezone. Optional facts remain unknown when omitted; the helper supplies record timestamps, not inferred status or model settings.

## Useful fields

- Work: `--repository`, `--issue`, `--branch`, `--commit`, `--status`, `--stage`, `--blocker`, `--started-at`, `--ended-at`.
- Run: actual `--agent`, `--role`, `--model`, `--effort`, `--status`, `--started-at`, `--ended-at`, `--duration-seconds`, `--source-ref`.
- Result: `--run-id`, `--commit`, `--evidence-ref`, `--summary`. Use a descriptive kind such as `check`, `review`, `qa` or `delivery`, and a status matching what was observed.
- Finding: `--severity`, `--status`, `--commit`, `--evidence-ref`, `--fix-ref`, `--thread-ref`. Updates preserve the original work association and recording time.
- Usage: `--run-id`, `--agent`, `--model`, token fields, `--estimated-cost-usd`, `--estimated-credits`, `--source-ref`, and the allocation described below.

All record types accept `--details` as JSON for concise extra context, such as acceptance conditions or a next action. Observation types accept `--recorded-at`. Keep evidence at a durable location and record its reference; the helper does not copy artifacts. Avoid storing credentials or unnecessary private content.

```sh
python3.12 "$DEVFLOW_STATE" record result --id retry-check-1 --work-id retry-fix \
  --kind check --status passed --summary "Original retry scenario passed" \
  --evidence-ref /path/to/retained-check-output.txt
python3.12 "$DEVFLOW_STATE" work update --id retry-fix --status done \
  --details '{"outcome":"Requested retry behavior verified"}'
python3.12 "$DEVFLOW_STATE" metrics --work-id retry-fix
```

Use real observed facts in place of these illustrative values. A recorded passing result or completed status does not itself prove the behavior.

## Usage

Record each unique observation once, with a source reference and estimation basis in `details`. Token fields are `input_tokens`, `cached_input_tokens`, `cache_write_tokens`, `output_tokens` and `reasoning_output_tokens` in JSON, or their kebab-case flags. Normalize cached input and reasoning output as subsets of the input/output totals; do not add them again when computing totals. Costs and credits are supplied estimates, with no built-in pricing.

`--work-id` allocates the full observation to one work. Alternatively, `--allocations` accepts a JSON array such as `[{"work_id":"retry-fix","weight":0.5}]`. Weights are between zero and one and sum to at most one; the remainder stays unallocated. Without either option, the observation is unallocated.

Metrics report work, ownership, roles/models, delivery, result/finding outcomes, phase transitions, recovery, timings and field coverage. Active and terminal work/run status updates stamp missing start/end times. Global usage counts each observation once; per-work usage applies its allocation weight. Unknown values remain unknown.

Installed hooks collect bound sessions' turns, tool attempts, permission requests, interruptions, compactions and cumulative token-counter deltas. Claims bind their coordinator automatically; child tasks inherit a single issue. For other tasks, run `telemetry.py bind --session-id SESSION_ID --work-id WORK_ID --role ROLE`. A session with several issues has unallocated usage; use separate worker sessions for issue attribution.

Worker completion also collects native tool-call records from its transcript when individual tool hooks are unavailable. Stable call IDs prevent duplicates; transcript completion alone means returned, not acceptance. Only structured exit codes or explicit errors establish tool success or failure.

Hooks store identifiers, counts, timestamps and command fingerprints, not prompts, commands or tool-output text. They neither call a model nor alter tool decisions. Transcript parsing is an adapter for the current local format; missing counters or a reset remain visible gaps, and a malformed line is recorded as a `transcript_gap` event and skipped so one bad line cannot stop collection. Replay and partial transcript lines do not add duplicate usage. Trust installed hooks using `/hooks`; untrusted hooks do not collect anything. A fresh task may be needed after installing them.

Collection starts at binding and closes when a released task's turn ends. Reclaiming or explicitly binding resumes collection; unrelated later conversations are ignored. Runtime runs represent observed turns, including tool waits. Shared coordinator usage is reported globally without an invented issue split.

Tool completion is separate from check/review acceptance. Record those outcomes and findings explicitly. Runtime turn duration includes tool waits; phase duration includes time spent in that recorded phase. Repeated calls are not necessarily retries. Complete overhead, counterfactual savings, defect escape rate and human intent are not inferred. Costs require supplied estimates and provenance.

## Recovery and import

On requested continuation, use `work list` and `work show`, then inspect the actual repository, artifacts and remote state before continuing. Reuse the work ID and retain earlier outcomes. The database is a record of observations, not a tool-execution journal or a substitute for checking whether an external action succeeded.

Legacy import is optional and explicit:

```sh
python3.12 "$DEVFLOW_STATE" import-legacy /path/to/old/state.sqlite3
```

Import is one-time per canonical source path, not a live sync. It reads the old database without changing it, normalizes useful history and retains artifact references. Its report identifies skipped records by ID/count; retain the original archive for those records. The new default database has a different filename; an existing `state.sqlite3` is never implicitly migrated. Imported historical results do not establish current checks or authorization.
