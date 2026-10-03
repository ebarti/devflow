# Local Desktop entry point

This directory installs one narrow Codex skill and one stdio MCP server. The MCP server is the existing `devflow-delivery-mcp` client of the authenticated local service; it does not run or poll the workflow. The installed skill does not replace the existing general Devflow skills.

Build the pinned runtime environment and dashboard before using the service. CLI and MCP operations start a stopped configured service automatically. Supply your own absolute runtime directory, service config file, and Codex home to the installer:

```sh
python3 runtime/desktop/install.py \
  --runtime-dir /absolute/path/to/runtime \
  --config /absolute/path/to/service-config.json \
  --codex-home /absolute/path/to/codex-home
```

`runtime/.venv/bin/devflow-delivery` and `devflow-delivery-mcp` must already exist. For a source checkout, run `uv sync --frozen` from the runtime directory, and `npm ci && npm run build` from its `ui` directory. The service config must already be prepared for the local runtime. The installer does not start or stop the service, alter tracker state, or read the service token.

The installer uses the supported `codex mcp add` command and copies `devflow-local-delivery/SKILL.md` into the specified Codex home. It reads back the registered entry. A same-name skill or MCP entry with different contents, command, or config is a conflict and is left untouched. Repeating an identical installation is safe. The inspected Codex CLI can overwrite a same-name MCP entry with `codex mcp add`, so use the guarded installer rather than an unchecked add command.

For an update to an inspected, Devflow-owned skill, supply `--replace-owned-skill-sha256` with the SHA-256 of its current bytes. The installer rechecks those bytes, retains a backup, and replaces only that skill. Different MCP commands/configs and disabled entries remain conflicts. This option does not authorize replacement of unrelated skills or service state.

With the runtime and config paths above, the service can be checked using the same public CLI:

```sh
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json status
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json runs
codex mcp get devflow-local-delivery --json
```

`status` and `stop` remain diagnostic commands and never start the stack. Explicit `start` uses the same serialized startup path as application calls. Healthy calls preserve process identities; partial owned stacks are recovered without taking over foreign ports. Authenticated API readiness, Temporal health, and worker poller registration are required before a request is sent. A delayed readiness response waits within the startup deadline and preserves a complete live stack if readiness remains unknown. New macOS execution is native and needs no Docker. Startup defaults to a 30-second deadline; `service_start_timeout` can set a positive value up to 120 seconds. Failures report the state root's service logs.

The CLI also supports `submit --request <json-file>`, `runs`, `run --id <run-id>`, `evidence --id <run-id> --evidence-id <id>`, `decision --id <run-id> --request <json-file>`, and `cancel --id <run-id> --request <json-file>`. The MCP tools are `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, and `cancel_run`; CLI and MCP use the same service API and authorization rules. A raw-goal submission returns a durable run ID and dashboard URL. New requests default to optional `plan_approval: "automatic"`: the service investigates, makes reasonable reversible assumptions and gathers only blocking answers, binds the exact plan to the run authorization, and continues implementation. Use `plan_approval: "required"` only for explicitly requested human plan review; historical stored inputs without the field retain their plan gate. Required review combined with an already supplied `accepted_plan` is contradictory and rejected. The Codex host may open the URL in its browser panel. Dashboard SSE supplies progress without model polling.

MCP submission binds the originating UUID from each call’s native top-level `threadId` or supported thread metadata, never `sessionId` or the daemon environment; the CLI can capture its current caller’s `CODEX_THREAD_ID`. An explicit `origin_thread_id` must match observed metadata. Blocking questions include the unknown, evidence checked and why a safe assumption cannot satisfy the goal. The service enqueues one callback per current question revision using the installed public `codex queue` command. The receiving skill freezes the presented run/decision/candidate identity before asking the actual user and rechecks it after the reply. If it changed, the old answer is discarded and any new question is presented separately; an answer is never rebound to new IDs. A match uses the refreshed protocol revision and the frozen decision/candidate revisions. A callback grants no authority. `queued` proves queue acknowledgement, not visible Desktop display or an answer. `unknown` effects are not retried; missing origin is `unavailable` and leaves the dashboard question accessible.

A configured real request needs no operator-authored per-run proof. Native runtime/command-boundary preparation runs after durable submission inside the existing Temporal activity. Its measured evidence uses a private reusable cache; each run freezes a separate binding to configured repository and paths. Terminal workflow finalization removes registered temporary roots and preserves durable sessions/evidence and necessary candidate recovery data. Process monitoring uncertainty and directory removal failures remain visible. Historical Docker runs remain readable and cannot resume.

Registration through the CLI and a successful MCP protocol handshake do not prove that an already-open Desktop thread has discovered the new tool. Check from a fresh Desktop thread or after restart, invoke a read-only MCP tool, and open an actual returned dashboard URL before claiming live Desktop integration. No private Codex database or undocumented IPC is used.
