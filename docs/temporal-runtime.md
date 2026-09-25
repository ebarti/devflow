# Experimental Temporal runtime

This optional Python package runs one local development task as a Temporal workflow. It does not change the existing Devflow skills, installer, tracking database, or GitHub issue state. The workflow starts with an explicit run ID, can wait for a decision, and then runs implement, independent review, and independent verification in order. A role must return a structured pass before the next role begins.

## Local setup

Use Python 3.12 or 3.13, `uv`, Git, and a local Temporal CLI. Run the commands below from `runtime/`. The dev server is for local experiments and is not a production Temporal deployment.

```sh
uv sync --locked --python python3.12
export DEVFLOW_TEMPORAL_DATA="$(mktemp -d)"
temporal server start-dev --ip 127.0.0.1 --port 17333 --ui-port 18333 \
  --db-filename "$DEVFLOW_TEMPORAL_DATA/temporal.db"
```

In another terminal, start the worker:

```sh
uv run --frozen devflow-temporal worker --address 127.0.0.1:17333
```

Prepare a **clean disposable Git copy** of the task repository. `start` requires `--disposable` because the implementer edits that copy. Keep the runtime state directory outside it. The deterministic fake provider gives a repeatable workflow demonstration and makes no model calls:

```sh
uv run --frozen devflow-temporal start --address 127.0.0.1:17333 \
  --id demo-1 --goal 'Add a demo marker' --repo /path/to/disposable-repo \
  --state-dir "$DEVFLOW_TEMPORAL_DATA/state" --provider fake --decision --disposable
uv run --frozen devflow-temporal status --address 127.0.0.1:17333 --id demo-1
uv run --frozen devflow-temporal decision --address 127.0.0.1:17333 \
  --id demo-1 --decision-id demo-1:start --revision 1 --answer proceed
uv run --frozen devflow-temporal status --address 127.0.0.1:17333 --id demo-1
```

The fake implementer writes `devflow-temporal-demo.txt`. `--fake-finding review` or `--fake-change review` demonstrates a blocked gate. Repeating `start` with the same ID and identical inputs returns the existing run; changing its inputs is rejected. `status` and waiting for a decision do not invoke a role. The Temporal UI is at `http://127.0.0.1:18333` for this example.

For a real provider, select an account-supported model and effort explicitly:

```sh
uv run --frozen devflow-temporal start --address 127.0.0.1:17333 \
  --id local-task-1 --goal 'Make a small local change' \
  --repo /path/to/disposable-repo --state-dir "$DEVFLOW_TEMPORAL_DATA/state" \
  --provider codex --model gpt-5.5 --effort low --disposable
```

This is an example model choice, not a guarantee that every account supports it. The start command checks the kit's declared task capabilities and bounded account readiness before scheduling. Provider-side model acceptance is established only by an actual call. Every real role call uses `agent-runtime-kit` 0.5.2 `AgentTask` and `AgentResult`; each role gets a separate task and session. Review requests the Codex read-only sandbox and strict approval mode. Implementation and verification request a workspace-write sandbox with strict approval mode; verification runs in a copy of the candidate. Network control and a portable tool allow-list are unsupported by this adapter, so this runtime does not claim to prevent arbitrary external side effects. Do not use it on production work or grant it credentials that permit unwanted writes.

## State and recovery

Temporal stores workflow transitions, decisions, and terminal outcomes in its configured persistence file. The separate private state directory holds candidate snapshots, role evidence, and a SQLite activity receipt store. The active Devflow tracking database is untouched. Status reports phase, outcome, candidate identity, findings, per-role usage, and the requested and provider-reported model, effort, and session. Unknown provider fields remain `null`. Fake evidence is labeled explicitly.

The candidate ID combines Git HEAD with the content and executable mode of Git-visible regular files: tracked files plus nonignored untracked files. The snapshot omits ignored files and `.git`, as well as `.venv`, `node_modules`, `__pycache__`, `.pytest_cache`, and `.ruff_cache`; symlinks and special files are rejected. Review and verification bind to the same snapshot. If their working copy changes during a gate, that gate blocks. This is a small-repository local snapshot design, not a general artifact service.

An activity receipt is keyed by run, role, iteration, and candidate. A finished receipt is reused if Temporal redelivers the activity. A still-running receipt after an interruption is treated as recovery-unknown and blocks, because the provider may already have acted. Activity and workflow retries are disabled for this path. It cannot resume a model mid-turn. Restarting a worker during the optional decision wait preserves the pending decision; answer with the exact ID and revision shown by `status`.

`cancel --id RUN --reason TEXT` requests a stop at the next role boundary. It does not roll back file or tool effects of an active role; status reports cleanup as unknown after activity work. This version has no production broker, multi-host receipt coordination, hosted UI, GitHub mutation, release, or global installation. The Codex adapter's output events are not a durable event journal and its SDK result arrives after the turn; use Temporal history and the local evidence files for the first version of observability.

## Checks

```sh
uv run --frozen ruff check src tests
uv run --frozen pytest -q
```

The integration tests start a real local Temporal dev server, exercise a worker restart, duplicate starts, decision revisions, candidate gates, findings, cancellation, and receipt reuse. The fake provider is deterministic; these tests do not prove live model quality. Real provider smoke evidence must be labeled separately.
