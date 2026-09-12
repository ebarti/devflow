# Work records and metrics

Invoke this helper from the agent while following a Devflow skill. Resolve `scripts/state.py` relative to the installed `devflow` skill directory. For the default installation:

```sh
DEVFLOW_STATE="$HOME/.agents/skills/devflow/scripts/state.py"
python3 "$DEVFLOW_STATE" work create --id retry-fix --title "Fix retry behavior" --status active
python3 "$DEVFLOW_STATE" work show --id retry-fix
```

The database defaults to `$XDG_STATE_HOME/devflow/workflow.sqlite3`, or `~/.local/state/devflow/workflow.sqlite3` when unset. Place `--db /path/to/workflow.sqlite3` before the command to select another database. All output is JSON. The helper records supplied facts; it does not run the work, collect usage or decide completion.

## Commands

| Command | Required fields | Purpose |
| --- | --- | --- |
| `work create` | `--id`, `--title` | Record one requested outcome |
| `work update` | `--id`, changed fields | Save current progress, blocker or endpoint |
| `work list` | None | List records; optionally filter by `--status` or `--repository` |
| `work show` | `--id` | Read work with its runs, results, findings, allocated usage and history |
| `record run` | `--id`, `--work-id` | Record an actual agent/role run |
| `run update` | `--id`, changed fields | Finish or correct the same run |
| `record result` | `--id`, `--work-id`, `--kind`, `--status` | Record check, review, QA or delivery observations |
| `record finding` | `--id`, `--work-id`, `--summary` | Retain a defect or unresolved concern |
| `finding update` | `--id`, changed fields | Link a fix, thread or verification outcome |
| `record usage` | `--id` | Retain a usage observation or estimate |
| `metrics` | None | Report all records or one `--work-id` |
| `import-legacy SOURCE` | Source database path | Explicitly import historical records |

Use stable IDs for created records. Retrying a creation with the same stored facts returns the existing record; different facts under that ID fail. After a work, run or finding has been updated, read its current state instead of replaying an outdated creation. Give each distinct run, result or usage observation its own ID. Work, runs and findings can be updated; updates retain change history. Finish a started run with `run update`, preserving its ID. Updating start/end timestamps recalculates duration unless you explicitly supply it. An optional stable `--event-id` makes an update retry explicit. Updating fields already at the requested values is a no-op.

Writes accept `--file /path/to/record.json`, containing one JSON object with snake_case keys. Flags override fields in that file. A JSON `null` clears an optional update field. Use `--help` on the specific command for its flags. Timestamps use ISO 8601 with a timezone. Optional facts remain unknown when omitted; the helper supplies record timestamps, not inferred status or model settings.

## Useful fields

- Work: `--repository`, `--issue`, `--branch`, `--commit`, `--status`, `--stage`, `--blocker`, `--started-at`, `--ended-at`.
- Run: actual `--agent`, `--role`, `--model`, `--effort`, `--status`, `--started-at`, `--ended-at`, `--duration-seconds`, `--source-ref`.
- Result: `--run-id`, `--commit`, `--evidence-ref`, `--summary`. Use a descriptive kind such as `check`, `review`, `qa` or `delivery`, and a status matching what was observed.
- Finding: `--severity`, `--status`, `--commit`, `--evidence-ref`, `--fix-ref`, `--thread-ref`. Updates preserve the original work association and recording time.
- Usage: `--run-id`, `--agent`, `--model`, token fields, `--estimated-cost-usd`, `--estimated-credits`, `--source-ref`, and the allocation described below.

All record types accept `--details` as JSON for concise extra context, such as acceptance conditions or a next action. Observation types accept `--recorded-at`. Keep evidence at a durable location and record its reference; the helper does not copy artifacts. Avoid storing credentials or unnecessary private content.

```sh
python3 "$DEVFLOW_STATE" record result --id retry-check-1 --work-id retry-fix \
  --kind check --status passed --summary "Original retry scenario passed" \
  --evidence-ref /path/to/retained-check-output.txt
python3 "$DEVFLOW_STATE" work update --id retry-fix --status done \
  --details '{"outcome":"Requested retry behavior verified"}'
python3 "$DEVFLOW_STATE" metrics --work-id retry-fix
```

Use real observed facts in place of these illustrative values. A recorded passing result or completed status does not itself prove the behavior.

## Usage

Record each unique observation once, with a source reference and estimation basis in `details`. Token fields are `input_tokens`, `cached_input_tokens`, `cache_write_tokens`, `output_tokens` and `reasoning_output_tokens` in JSON, or their kebab-case flags. Normalize cached input and reasoning output as subsets of the input/output totals; do not add them again when computing totals. Costs and credits are supplied estimates, with no built-in pricing.

`--work-id` allocates the full observation to one work. Alternatively, `--allocations` accepts a JSON array such as `[{"work_id":"retry-fix","weight":0.5}]`. Weights are between zero and one and sum to at most one; the remainder stays unallocated. Without either option, the observation is unallocated.

Metrics count work statuses, result kinds/statuses, and finding severities/statuses. They sum known run duration and usage values while reporting missing-field counts. Global usage includes each observation once; per-work usage applies its allocation weight. Summed run duration is agent time, not elapsed wall time. Missing observations cannot be counted, and unknown values are not zero.

## Recovery and import

On requested continuation, use `work list` and `work show`, then inspect the actual repository, artifacts and remote state before continuing. Reuse the work ID and retain earlier outcomes. The database is a record of observations, not a tool-execution journal or a substitute for checking whether an external action succeeded.

Legacy import is optional and explicit:

```sh
python3 "$DEVFLOW_STATE" import-legacy /path/to/old/state.sqlite3
```

Import is one-time per canonical source path, not a live sync. It reads the old database without changing it, normalizes useful history and retains artifact references. Its report identifies skipped records by ID/count; retain the original archive for those records. The new default database has a different filename; an existing `state.sqlite3` is never implicitly migrated. Imported historical results do not establish current checks or authorization.
