# Local Desktop entry point

This directory installs one narrow Codex skill and one stdio MCP server. The MCP server is the existing `devflow-delivery-mcp` client of the authenticated local service; it does not run or poll the workflow. The installed skill does not replace the existing general Devflow skills.

Build the pinned runtime environment and dashboard before starting the service. Supply your own absolute runtime directory, service config file, and Codex home to the installer:

```sh
python3 runtime/desktop/install.py \
  --runtime-dir /absolute/path/to/runtime \
  --config /absolute/path/to/service-config.json \
  --codex-home /absolute/path/to/codex-home
```

`runtime/.venv/bin/devflow-delivery` and `devflow-delivery-mcp` must already exist. For a source checkout, run `uv sync --frozen` from the runtime directory, and `npm ci && npm run build` from its `ui` directory. The service config must already be prepared for the local runtime. The installer does not start or stop the service, alter tracker state, or read the service token.

The installer uses the supported `codex mcp add` command and copies `devflow-local-delivery/SKILL.md` into the specified Codex home. It reads back the registered entry. A same-name skill or MCP entry with different contents, command, or config is a conflict and is left untouched. Repeating an identical installation is safe. The inspected Codex CLI can overwrite a same-name MCP entry with `codex mcp add`, so use the guarded installer rather than an unchecked add command.

With the runtime and config paths above, the service can be checked using the same public CLI:

```sh
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json status
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json start
codex mcp get devflow-local-delivery --json
```

The CLI also supports `submit --request <json-file>`, `runs`, `run --id <run-id>`, `evidence --id <run-id> --evidence-id <id>`, `decision --id <run-id> --request <json-file>`, and `cancel --id <run-id> --request <json-file>`. The MCP tools are `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, and `cancel_run`; CLI and MCP use the same service API and authorization rules. A run submission returns a durable run ID and dashboard URL. The Codex host may open that URL in its browser panel. Dashboard SSE supplies progress without model polling.

Registration through the CLI and a successful MCP protocol handshake do not prove that an already-open Desktop thread has discovered the new tool. Check from a fresh Desktop thread or after restart, invoke a read-only MCP tool, and open an actual returned dashboard URL before claiming live Desktop integration. No private Codex database or undocumented IPC is used.
