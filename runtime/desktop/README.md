# Local Desktop entry point

For the portable plugin and additive local marketplace, see the [plugin guide](../../docs/local-delivery-plugin.md). `package_plugin.py` packages this same canonical skill and MCP executable without registering or changing the host. The direct installer below remains an alternative; the guarded primary-plugin switch removes its duplicate host registration and archives its skill for rollback.

This directory installs one narrow Codex skill and one stdio MCP server. The MCP server is the existing `devflow-delivery-mcp` client of the authenticated local service; it does not run or poll the workflow. The installed skill does not replace the existing general Devflow skills.

Build the pinned runtime environment and dashboard before using the service. CLI mutations and MCP write tools start a stopped configured service automatically. Read calls require an existing API and report unavailable if it is stopped. Supply your own absolute runtime directory, service config file, and Codex home to the installer:

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

Unrelated-state seals normalize only the public CLI's evidenced TOML
representations: stdio absent versus empty `args`, and finite integral
`startup_timeout_sec` integer/float values that represent exactly the same
seconds; MCP `enabled=true` versus absent only when one matching public inventory
entry confirms exact effective `enabled: true`. Both the direct/ambient
`other_mcp` and primary-plugin `mcp` snapshot shapes retain their original sealed
keys and full public inventory. Explicit false, non-boolean values, missing/ambiguous confirmation,
boolean/non-finite timeouts, changed values and every other field remain
conflicts. This enablement default is documented in the [MCP configuration
contract](https://learn.chatgpt.com/docs/extend/mcp?surface=cli) and reproduced by
the installed public CLI. Older journals retain their original raw digest; stable replay and
rollback reconstruct only those equivalent representations and must match that
exact digest. Compatibility search is bounded to twelve such fields; new
canonical journals need no search. No new baseline or private manifest rewrite
is accepted. Original rejection/rollback errors remain in a recovered journal.

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

An interrupted rollback may retain the original pointer with the newer skill.
After authenticating those original permitted states, repeat the same explicit
rollback manifest to finish restoring the old skill, then use the same original
update request to reapply. Preserve the original receipt, snapshots, sidecar and
errors; no replacement request or acknowledgement is needed for proven default
serialization. Unknown owned bytes or any effective foreign drift still refuse.

An interrupted `unknown` update can encounter actual ambient host changes. An
explicit `--acknowledge-owned-drift-request` operation records a separate immutable
receipt under that original command. It preserves current foreign state; it does
not normalize versions/settings, replace the historical seal, edit foreign files,
attribute their changes or claim a new human approval. Without that receipt, the
original unrelated-state checks still reject drift.

The owned 0600 request has exactly these fields (all paths are absolute):

```json
{
  "command_id": "ORIGINAL_OWNED_COMMAND_ID",
  "original_request": {"path": "/private/original-request.json", "sha256": "BYTE_SHA256"},
  "original_manifest_sha256": "ORIGINAL_UNKNOWN_JOURNAL_BYTE_SHA256",
  "original_unrelated_sha256": "ORIGINAL_HISTORICAL_SEAL",
  "authority": {"path": "/private/existing-installation-decision.json", "sha256": "BYTE_SHA256"},
  "prior_snapshot": {"path": "/private/retained-prior-snapshot.json", "sha256": "BYTE_SHA256"},
  "current_snapshot": {"path": "/private/fresh-current-snapshot.json", "sha256": "BYTE_SHA256"},
  "delta_sha256": "SHA256_OF_TYPED_DELTA_JSON",
  "content_indexes": [{"path": "/private/retained-content-index.json", "sha256": "BYTE_SHA256"}]
}
```

The authority reference is the existing main task installation decision, with
`decision_owner`, `authority_source` and `new_user_approval: false`; its exact
bytes are bound, rather than inventing an approval. Preserve earlier decisions
and rejection evidence. Requests, journal and private snapshots require owned
regular 0600 files. Hash-bound public authority/content indexes may also be 0644;
links, foreign ownership and group/world writes reject. Snapshot
JSON has exactly `settings`, `other_mcp`, `plugins` and `marketplaces` keys.
`owned_drift.foreign_snapshot(codex, home)` reads the current public inventory and
TOML without the owned MCP entry. Construct the prior snapshot from retained
original evidence: its settings/MCP component must authenticate the original
historical seal. `seal(owned_drift.delta(prior, current))` binds the complete
typed delta, including presence/absence. Neither an obsolete observation nor a
caller-supplied new baseline may replace the original journal.

`owned_drift.content_index(cache_root)` produces the complete retained file and
directory index, including content hashes, size, modes and ownership. Every
changed installed plugin requires its current cache index. Roots must belong to
currently observed foreign plugins in this Codex home's cache. Bounds are eight
indexes, 512 files/16 MiB per index, 8 MiB per file, 1024 directories, 1 MiB per
referenced JSON and 128 delta entries. Linked/special/foreign files reject.

```sh
python3 runtime/desktop/install.py \
  --runtime-dir /absolute/stable/runtime --config /absolute/private/state/trusted-local.json \
  --codex-home /absolute/codex-home \
  --acknowledge-owned-drift-request /absolute/private/ambient-acknowledgement.json
```

The response names `ambient-drift-acknowledgement.json`, separate from the original
pointer manifest/request. Its creation changes no pointer, skill or host setting.
Identical replay observes that same receipt; a different initial request conflicts.
The original pointer commands need no new ID or changed input: they automatically
validate the acknowledgement and every referenced original/authority/snapshot
and content proof. They preserve original errors/seal while checking the exact
acknowledged current foreign state before and after each pointer/skill effect,
including rollback and reapply. Any later setting, inventory, content, metadata
or owned-authority change refuses before further effects. Interrupted effects
remain observable through the same original command. A subsequent primary-plugin
request captures fresh then-current inventory normally.

If foreign state changes between separately authorized operations, the same flag
admits an **explicit successor**, with at most **four acknowledgements total**
(the first plus three successors). There is no automatic capture, retry loop or
renewal after exhaustion. Preserve the first receipt and every predecessor; a
successor appends `ambient-drift-acknowledgement-2.json` (then `-3`, `-4`) without
changing the original journal/request, historical seal or errors.

A successor retains `command_id`, `original_request`,
`original_manifest_sha256` and `original_unrelated_sha256` exactly from the first
request. It adds these fields to the same request shape:

```json
{
  "predecessor_sha256": "IMMEDIATE_PREDECESSOR_RECEIPT_BYTE_SHA256",
  "expected_manifest_sha256": "CURRENT_ORIGINAL_JOURNAL_BYTE_SHA256",
  "expected_manifest_state": "applied"
}
```

Its `prior_snapshot` must be the immediate predecessor request's exact
`current_snapshot` reference. Bind a new deciding existing-installation authority
receipt, a fresh complete current public snapshot, the full typed delta from the
predecessor snapshot, and fresh current content indexes. Retain indexes for every
previously indexed plugin still installed, as well as every newly changed
installed plugin. The operator freezes actual current state; no historical
setting value is inserted or presumed equivalent. Admission checks the complete
current snapshot/content twice, then rereads recognized live owned
pointer/skill/configuration/source and the exact current journal before appending.
Its immutable receipt retains the admitted journal bytes/state and immediate
predecessor hash. The original first authority remains authenticated in history.

Every replay, rollback or reapply validates the entire bounded chain and all
historical evidence bytes/hashes, then checks the latest acknowledged current
foreign state and live owned authority around each effect. Historical content
proofs remain authenticated without requiring obsolete plugin caches to exist.
An identical successor replay is idempotent even after authorized pointer state
changes or a lost response; a historical acknowledgement replay observes the current
chain. If interruption leaves the exclusive append's private temporary hardlink,
replay removes only that matching temporary inode and preserves the published
receipt bytes. Unrecognized receipt links refuse.
Wrong/missing predecessors, changed history/proof, unknown owned state, stale or
inconsistent current capture, further foreign drift and exhaustion refuse.
Responses include `chain_length` and `max_acknowledgements`. A rejected later
change requires a separate concrete authorization and explicit successor within
the bound; it is never silently adopted.

Each effect guard rereads the live owned MCP pointer, configuration hashes and
skill identity/bytes after the slower foreign inventory reads. It permits only
the original before/after states and requires the expected pointer/skill state
after each effect. An unrecognized concurrent owned change refuses without
overwriting it, including during unknown-command replay and rollback. Owning
journal nonstate fields, pointer readbacks and the mode-only configuration delta
use canonical typed seals: `true`, `1` and `1.0` remain distinct. Legitimate
journal state progression is allowed; foreign MCP default equivalence never
applies to owning history or authority.

The optional actual constrained-profile regressions use `DEVFLOW_CODEX_BIN` and
a real Python interpreter resolved inside the profile's existing `/opt/homebrew`
or `/usr/local` toolchain reads. `DEVFLOW_PROFILE_PYTHON` can select an exact such
interpreter. They execute the original sentinel/child/diff assertions; unavailable
interpreters fail an explicit opt-in rather than treating Apple SDK bootstrap
failure as denial evidence. Production profile permissions remain unchanged.

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
  "package_format": "codex",
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
For the pinned SDK CLI 0.160.0, generate that fresh package with
`package_plugin.py --format codex`; its declared compatibility manifest enables
automatic MCP discovery. The default portable package contract remains available,
but adding an overlay beside its root manifest does not establish MCP discovery
on this pinned host. The request's `package_format` binds exact selected bytes and
is retained for rollback. Legacy requests/manifests without the field continue to
mean `portable`. A wrong layout or altered cache conflicts before effects.
Rollback an older activated package with its original source before advancing
installed source/layout, preserving its package and immutable receipts. Then
activate a fresh selected package/request using the same service configuration.

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

`status` and `stop` remain diagnostic commands and never start the stack. Read-only application and preflight calls also never start or recover the stack. Explicit `start` uses the same serialized startup path as write calls. Healthy calls preserve process identities; partial owned stacks are recovered without taking over foreign ports. Authenticated API readiness, Temporal health, and worker poller registration are required before an intentional write is sent. A delayed readiness response waits within the startup deadline and preserves a complete live stack if readiness remains unknown. New macOS execution is native and needs no Docker. Startup defaults to a 30-second deadline; `service_start_timeout` can set a positive value up to 120 seconds. Failures report the state root's service logs.

The CLI also supports `submit --request <json-file>`, `runs`, `run --id <run-id>`, `evidence --id <run-id> --evidence-id <id>`, `decision --id <run-id> --request <json-file>`, and `cancel --id <run-id> --request <json-file>`. The MCP tools include `get_service`, `start_service`, `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, and `cancel_run`; CLI and MCP use the same service API and authorization rules. `get_service` reads public repository keys/base refs, role policy and service health through a non-starting client and reports unavailable if the API is stopped. For an authorized submission, answer, plan change or cancellation when a prerequisite read reports transport unavailable, call the write-annotated `start_service` tool and repeat the required reads before sending the mutation. It uses the registered config path; no out-of-band runtime/config paths are needed. Status/evidence requests and callbacks alone do not authorize startup. A raw-goal submission returns a durable run ID and dashboard URL. New requests default to optional `plan_approval: "automatic"`: the service investigates, makes reasonable reversible assumptions and gathers only blocking answers, binds the exact plan to the run authorization, and continues implementation. Use `plan_approval: "required"` only for explicitly requested human plan review; historical stored inputs without the field retain their plan gate. Required review combined with an already supplied `accepted_plan` is contradictory and rejected. The Codex host may open the URL in its browser panel. Dashboard SSE supplies progress without model polling.

MCP submission binds the originating UUID from each call’s native top-level `threadId` or supported thread metadata, never `sessionId` or the daemon environment; the CLI can capture its current caller’s `CODEX_THREAD_ID`. An explicit `origin_thread_id` must match observed metadata. Blocking questions include the unknown, evidence checked and why a safe assumption cannot satisfy the goal. The service enqueues one callback per current question revision using the installed public `codex queue` command. The receiving skill freezes the presented run/decision/candidate identity before asking the actual user and rechecks it after the reply. If it changed, the old answer is discarded and any new question is presented separately; an answer is never rebound to new IDs. A match uses the refreshed protocol revision and the frozen decision/candidate revisions. A callback grants no authority. `queued` proves queue acknowledgement, not visible Desktop display or an answer. `unknown` effects are not retried; missing origin is `unavailable` and leaves the dashboard question accessible.

A configured real request needs no operator-authored per-run proof. Native runtime/command-boundary preparation runs after durable submission inside the existing Temporal activity. Its measured evidence uses a private reusable cache; each run freezes a separate binding to configured repository and paths. Terminal workflow finalization removes registered temporary roots and preserves durable sessions/evidence and necessary candidate recovery data. Process monitoring uncertainty and directory removal failures remain visible. Historical Docker runs remain readable and cannot resume.

Registration through the CLI and a successful MCP protocol handshake do not prove that an already-open Desktop thread has discovered the new tool. Check tool discovery from a fresh Desktop thread or after restart. A read-only MCP call returns policy when running or transport unavailable when stopped, without starting anything. During an authorized handoff, use `start_service` if needed, repeat prerequisite reads, and open an actual returned dashboard URL before claiming live Desktop integration. No private Codex database or undocumented IPC is used.

For an exhausted `waiting_tracker` terminal checkpoint, the public CLI `reconcile-tracker --id RUN_ID --request PRIVATE_JSON` and MCP `reconcile_tracker` accept exactly a stable `command_id` and current `expected_revision`. They resume only three bounded tracker readback attempts in the original open workflow, preserving the candidate, gates and role budgets. A released owning intent is audited without another set; confirmed readback completes and refreshes the final projection. See the [terminal recovery contract](../../docs/trusted-local-delivery.md).
