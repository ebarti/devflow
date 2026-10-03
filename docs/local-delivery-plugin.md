# Local delivery plugin

The `devflow` plugin packages the existing local delivery MCP client and its canonical `devflow-local-delivery` skill. It hands a development goal to your configured macOS Temporal and agent-runtime-kit service, then exposes status, evidence and blocking decisions. The service investigates, plans automatically and delivers through an open, unmerged PR. The plugin adds no controller, provider, native thread hook or UI; the existing dashboard uses SSE for progress.

## Prepare and package

Use a stable checkout and a service configuration prepared according to the [runtime guide](temporal-runtime.md). Repository allowlists, role models, GitHub access and execution limits belong to that configuration. Build the locked runtime and dashboard first:

```sh
cd /absolute/path/to/devflow/runtime
uv sync --frozen
cd ui
npm ci
npm run build
```

From the source checkout, choose an explicit local marketplace root:

```sh
python3 runtime/desktop/package_plugin.py \
  --marketplace-root /absolute/path/to/local-marketplace \
  --runtime-dir /absolute/path/to/devflow/runtime \
  --config /absolute/path/to/service-config.json
```

The output is:

```text
local-marketplace/
├── .agents/plugins/marketplace.json
└── plugins/devflow/
    ├── plugin.json
    ├── mcp.json
    └── skills/devflow-local-delivery/SKILL.md
```

The skill is copied from `runtime/desktop/devflow-local-delivery/SKILL.md`. The generated MCP entry contains an absolute executable path and `--config` argument; it reads no configuration contents and embeds no credentials. The package contains all plugin resources, while the built runtime and service configuration remain external prerequisites. Generate it again for a different host or runtime location.

Packaging performs no host registration, service start, install or update. Identical repetition leaves existing files untouched. A changed same-name plugin or catalog entry is rejected before publishing files. Unrelated catalog entries and fields are retained. Symlink destinations are rejected. Use a new explicit root for a different candidate rather than overwriting an installed or modified package.

## Install on a local host

Register the chosen root using the supported CLI:

```sh
codex plugin marketplace add /absolute/path/to/local-marketplace
codex plugin marketplace list
```

In the desktop Plugins Directory, choose **Devflow Local**, inspect **Devflow Local Delivery**, and install it. Current CLI builds that expose `plugin add` also support:

```sh
codex plugin add devflow@devflow-local --json
codex plugin list --json
```

If adding to an existing catalog, use that catalog's `name` in place of `devflow-local`. Validate tool discovery in a fresh local conversation by invoking `get_service`. Registration and CLI installation do not establish GUI discovery in an already-open conversation. Refresh or restart the desktop app yourself if required by your host's marketplace discovery; packaging never restarts it.

The [direct MCP and skill installer](../runtime/desktop/README.md) remains available. For an inspected existing direct installation and a trusted-local configuration, its `--activate-owned-plugin-request` operation makes this plugin primary: a private request seals the exact prior registration, configuration, skill and public plugin/marketplace inventories. It registers a fresh owned package through the public CLI, removes the exact duplicate direct MCP entry, and archives its skill outside discovery. A durable rollback manifest restores them through supported commands. Identical requests observe completed effects; changed owned inputs conflict. The packager itself does not change host settings. Use `--rollback-owned-plugin-manifest` to switch back, and retain both manifests when a separate mode-only pointer update preceded activation.

This stdio package is for a host that can execute the local runtime, including local Codex Desktop, CLI and IDE MCP support. ChatGPT cloud cannot execute a Mac-local stdio process or reach that Mac's loopback API. A remote HTTPS service would be a different deployment; tunnels, cloud exposure and public marketplace submission are outside this package.

## Use

For an existing issue in an allowlisted repository, a realistic prompt is:

> Use Devflow local delivery for https://github.com/example/project/issues/123. Fix the issue within its scope and publish an unmerged PR. Do not merge, release or deploy.

The skill first reads `get_service`, matches the repository key and base ref, and generates stable unique routine IDs and a branch when absent. The issue URL and authorization through an unmerged PR must come from the user. It submits the original goal to `submit_run`, returns the run ID and dashboard URL, and ends the handoff without routine model polling. Calls start a stopped configured service using the existing bounded startup path. Service policy determines role models.

Automatic planning is the default. “Show me the plan for approval before implementing” sets `plan_approval: "required"`; only the user's explicit acceptance of the presented plan can release that gate. Blocking clarification still waits for the user under either policy. Broader endpoint or scope authority is never inferred from a service policy or a callback.

Follow-ups such as “What is the status of that run?” use `get_run`; “Show the failed check evidence” uses the indexed `read_evidence`. A blocking question callback is checked against the current run and its immutable decision/candidate identity before presentation. After the user answers, that same identity must still be pending before `answer_decision` sends the answer with refreshed protocol revision. A delayed answer to a replaced question is discarded, and the new question is presented separately. An uncertain mutation receipt is investigated using the stable run and command IDs before any retry.

Origin metadata comes from the individual MCP request. If the host does not supply an originating thread, blocking questions remain available in the dashboard and callback status is `unavailable`. `queued` proves queue acknowledgement, not desktop display or a user answer. The plugin does not add a native thread API or change these existing callback limits.

## Validation and evaluation

The package follows the official [portable plugin and local marketplace format](https://developers.openai.com/plugins/build/plugins): root `plugin.json`, explicit stdio transport in root `mcp.json`, auto-discovered `skills/`, and OpenAI presentation in `extensions.com.openai.interface`. The [MCP host guide](https://developers.openai.com/codex/mcp) describes local stdio support. [UI extensions](https://developers.openai.com/plugins/build/extensions) are optional, so this entry point reuses the existing dashboard.

Automated checks cover package discovery, identical repetition, conflicting files/catalogs, catalog preservation, symlink isolation, missing inputs, and rollback after a catalog write failure. The generated real MCP executable is exercised through the official stdio SDK against a disposable loopback API and real Temporal server, using the existing fake intake provider. It submits a raw goal, returns a blocking question, accepts its answer, binds the automatic plan, reaches implementation, and reads status/evidence. Only lifecycle startup is bypassed in that fixture; an implementation stub deliberately stops the run without publishing. Existing startup tests exercise the self-start path separately. No paid provider or live GitHub workflow is submitted.

Evaluate the skill with the following requests in a disposable environment, with mutations intercepted or fake providers configured:

| Case | Expected behavior |
| --- | --- |
| Direct: “Use Devflow for this linked issue; publish an unmerged PR” | Discover policy, generate routine IDs, submit one raw goal with automatic planning |
| Indirect: “Hand this linked issue to the configured local delivery service” | Select the delivery skill when the intent and endpoint are clear |
| Explicit plan review | Set required review and wait for explicit acceptance of the exact plan |
| Follow-up: “What is happening with that run?” | Read status/evidence without creating a new run or routine polling |
| Blocking answer after another question replaces it | Reject the old answer and present the current question separately |
| Negative: ordinary code explanation or unrelated edit | Keep the ordinary workflow; do not submit to delivery |
| Missing issue URL or unclear endpoint authority | Ask for the missing authority/input before submission |
| Cloud-only conversation | Explain the local host requirement; do not propose loopback access or invent a remote endpoint |

An isolated `CODEX_HOME` can verify CLI marketplace registration and plugin installation without modifying the user's installation. That check does not prove desktop GUI discovery, originating-thread metadata, callback display, paid-provider execution or live GitHub publication. Those remain explicit host/product trials, outside this package's simulated checks.
