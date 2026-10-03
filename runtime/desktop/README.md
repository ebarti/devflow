# Local Desktop entry point

For the portable plugin and additive local marketplace, see the [plugin guide](../../docs/local-delivery-plugin.md). `package_plugin.py` packages this same canonical skill and MCP executable without registering or changing the host. The direct installer below remains an alternative; the guarded primary-plugin switch removes its duplicate host registration and archives its skill for rollback.

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

For the explicit [trusted-local policy transition](../../docs/trusted-local-delivery.md),
the owned direct MCP entry must point to the new private configuration. Keep the
original frozen file unchanged and put the mode-only copy under the existing
service state root. Inspect the current entry using `codex mcp get
devflow-local-delivery --json`, then create an owned 0600 request containing:

```json
{
  "command_id": "stable-owned-trusted-upgrade",
  "expected_registration_sha256": "SHA256_OF_CANONICAL_PUBLIC_GET_JSON",
  "expected_config_path": "/absolute/original/frozen-service.json",
  "expected_config_sha256": "SHA256_OF_ORIGINAL_CONFIG_BYTES",
  "expected_skill_sha256": "SHA256_OF_INSPECTED_INSTALLED_LOCAL_DELIVERY_SKILL"
}
```

Canonical registration JSON uses sorted keys and compact `,` / `:` separators.
The guarded installer requires an enabled same-name stdio entry with the exact
runtime executable, original config argument and no transport environment/CWD
overrides. It rejects changed owned inputs, disabled/foreign entries, altered
models/capacity/scope, and linked inputs. It updates only the owned MCP pointer
through the supported public CLI and the one local-delivery skill, checking all
other MCP entries and host configuration semantics before and after. It does
not edit configuration TOML directly or change agents/models/plugins.

```sh
python3 runtime/desktop/install.py \
  --runtime-dir /absolute/stable/runtime \
  --config /absolute/private/state/trusted-local.json \
  --codex-home /absolute/codex-home \
  --repoint-owned-request /absolute/private/owned-upgrade.json
```

The JSON response includes a private `rollback_manifest` under the selected
Codex home's `.devflow-local-delivery-upgrades/COMMAND_ID/`. This intent and the
original skill backup are durable before the pointer changes. Repeating identical
inputs reads back the owned state; an interrupted successful pointer write is
observed instead of blindly repeated. A failed readback restores the owned old
pointer and skill when their identities still match; uncertain rollback remains
visible in the manifest. To explicitly restore:

```sh
python3 runtime/desktop/install.py \
  --runtime-dir /absolute/stable/runtime \
  --config /absolute/private/state/trusted-local.json \
  --codex-home /absolute/codex-home \
  --rollback-owned-manifest /absolute/codex-home/.devflow-local-delivery-upgrades/COMMAND_ID/manifest.json
```

Installation does not restart the service or refresh cached host clients. Stop
the owned service separately, apply the reviewed runtime/config/pointer change,
start through the new config, and verify discovery from a fresh client. An old
cached MCP client with the frozen config path must be refreshed before use or
it can select the old service policy. When using the plugin instead, package
and register its new resources with the same new config; inspect existing public
plugin registrations first and preserve any earlier package for rollback. The
direct pointer updater does not rewrite or remove plugin installations.

To make the plugin primary after that pointer update, package a fresh immutable
`devflow-local` marketplace under the service state root. Read back the public
MCP entry, `codex plugin list --json`, and `codex plugin marketplace list`. The
installed CLI reports an empty marketplace as `No plugin marketplaces in scope.`;
otherwise it reports a `MARKETPLACE ROOT` table. Create a private 0600 request:

```json
{
  "command_id": "stable-owned-primary-plugin-switch",
  "marketplace_root": "/absolute/private/state/fresh-marketplace",
  "expected_registration_sha256": "SHA256_OF_CURRENT_CANONICAL_PUBLIC_GET_JSON",
  "expected_config_sha256": "SHA256_OF_TRUSTED_CONFIG_BYTES",
  "expected_skill_sha256": "SHA256_OF_CURRENT_DIRECT_SKILL_BYTES",
  "expected_plugin_inventory_sha256": "SHA256_OF_CANONICAL_PLUGIN_LIST_JSON",
  "expected_marketplace_inventory_sha256": "SHA256_OF_CANONICAL_NAME_TO_ROOT_OBJECT"
}
```

All inventory hashes use sorted, compact JSON. The marketplace hash binds the
parsed name-to-root object, including `{}` for an empty inventory. The helper
`owned_plugin.snapshot(codex, codex_home)` returns these exact seals without
changing the host. A same-name plugin, marketplace or cached package already
present is a conflict. The package, installed cache, direct skill and selected
configuration must be owned regular files with the inspected bytes.

```sh
/absolute/runtime/.venv/bin/python runtime/desktop/install.py \
  --runtime-dir /absolute/runtime --config /absolute/private/state/trusted-local.json \
  --codex-home /absolute/codex-home \
  --activate-owned-plugin-request /absolute/private/state/primary-plugin.json
```

The installer journals its private intent before effects, registers the fresh
marketplace and enables `devflow@devflow-local` using public CLI commands, checks
the installed cache, removes only the exact direct MCP entry, and moves the exact
direct skill outside discovery into the returned manifest's `direct-skill`
archive. Unrelated host settings, models, agents, MCP entries and plugins are
checked before and after. Interrupted or uncertain commands retain their intent;
an identical request observes each completed effect before continuing. Changed
owned inputs conflict. To restore the inspected direct entry and skill:

```sh
/absolute/runtime/.venv/bin/python runtime/desktop/install.py \
  --runtime-dir /absolute/runtime --config /absolute/private/state/trusted-local.json \
  --codex-home /absolute/codex-home \
  --rollback-owned-plugin-manifest /absolute/codex-home/.devflow-local-delivery-upgrades/COMMAND_ID/manifest.json
```

Rollback uses public plugin/marketplace removal and MCP registration, then
restores the archived skill and verifies the original inventory. It rejects
modified owned bytes. A rolled-back command ID stays rolled back on replay.
Standalone CLI, direct stdio MCP invocation and the authenticated service API
remain available; this switch selects the global plugin as the host entry point.
Fresh host discovery must establish that entry point and its configuration.

With the runtime and config paths above, the service can be checked using the same public CLI:

```sh
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json status
/absolute/path/to/runtime/.venv/bin/devflow-delivery --config /absolute/path/to/service-config.json runs
codex mcp get devflow-local-delivery --json
```

`status` and `stop` remain diagnostic commands and never start the stack. Explicit `start` uses the same serialized startup path as application calls. Healthy calls preserve process identities; partial owned stacks are recovered without taking over foreign ports. Authenticated API readiness, Temporal health, and worker poller registration are required before a request is sent. A delayed readiness response waits within the startup deadline and preserves a complete live stack if readiness remains unknown. New macOS execution is native and needs no Docker. Startup defaults to a 30-second deadline; `service_start_timeout` can set a positive value up to 120 seconds. Failures report the state root's service logs.

The CLI also supports `submit --request <json-file>`, `runs`, `run --id <run-id>`, `evidence --id <run-id> --evidence-id <id>`, `decision --id <run-id> --request <json-file>`, and `cancel --id <run-id> --request <json-file>`. The MCP tools are `get_service`, `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, and `cancel_run`; CLI and MCP use the same service API and authorization rules. `get_service` reads public repository keys/base refs, role policy and service health through the normal self-start client. A raw-goal submission returns a durable run ID and dashboard URL. New requests default to optional `plan_approval: "automatic"`: the service investigates, makes reasonable reversible assumptions and gathers only blocking answers, binds the exact plan to the run authorization, and continues implementation. Use `plan_approval: "required"` only for explicitly requested human plan review; historical stored inputs without the field retain their plan gate. Required review combined with an already supplied `accepted_plan` is contradictory and rejected. The Codex host may open the URL in its browser panel. Dashboard SSE supplies progress without model polling.

MCP submission binds the originating UUID from each call’s native top-level `threadId` or supported thread metadata, never `sessionId` or the daemon environment; the CLI can capture its current caller’s `CODEX_THREAD_ID`. An explicit `origin_thread_id` must match observed metadata. Blocking questions include the unknown, evidence checked and why a safe assumption cannot satisfy the goal. The service enqueues one callback per current question revision using the installed public `codex queue` command. The receiving skill freezes the presented run/decision/candidate identity before asking the actual user and rechecks it after the reply. If it changed, the old answer is discarded and any new question is presented separately; an answer is never rebound to new IDs. A match uses the refreshed protocol revision and the frozen decision/candidate revisions. A callback grants no authority. `queued` proves queue acknowledgement, not visible Desktop display or an answer. `unknown` effects are not retried; missing origin is `unavailable` and leaves the dashboard question accessible.

A configured real request needs no operator-authored per-run proof. Native runtime/command-boundary preparation runs after durable submission inside the existing Temporal activity. Its measured evidence uses a private reusable cache; each run freezes a separate binding to configured repository and paths. Terminal workflow finalization removes registered temporary roots and preserves durable sessions/evidence and necessary candidate recovery data. Process monitoring uncertainty and directory removal failures remain visible. Historical Docker runs remain readable and cannot resume.

Registration through the CLI and a successful MCP protocol handshake do not prove that an already-open Desktop thread has discovered the new tool. Check from a fresh Desktop thread or after restart, invoke a read-only MCP tool, and open an actual returned dashboard URL before claiming live Desktop integration. No private Codex database or undocumented IPC is used.

For an exhausted `waiting_tracker` terminal checkpoint, the public CLI `reconcile-tracker --id RUN_ID --request PRIVATE_JSON` and MCP `reconcile_tracker` accept exactly a stable `command_id` and current `expected_revision`. They resume only three bounded tracker readback attempts in the original open workflow, preserving the candidate, gates and role budgets. A released owning intent is audited without another set; confirmed readback completes and refreshes the final projection. See the [terminal recovery contract](../../docs/trusted-local-delivery.md).
