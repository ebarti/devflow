# Devflow

Devflow hands an authorized development goal to a configured local delivery
service, which investigates the repository, plans the work and publishes an
unmerged PR. The [Temporal runtime](docs/temporal-runtime.md) provides the
service, dashboard, CLI and MCP interface. Native macOS execution uses the
frozen agent-runtime-kit dependencies and measured command permissions.

The [local delivery plugin](docs/local-delivery-plugin.md) exposes the service
through one `devflow-local-delivery` skill. Packaging and normal installation
register entry points; service configuration, authentication and runtime startup
are separate operations.

## Prerequisites

Normal installation needs Python 3.12+, Git and a POSIX shell. Set
`DEVFLOW_PYTHON` to choose a supported interpreter; the installation helpers use
only the standard library. Delivery needs an existing configured local service,
an allowlisted repository and the user's linked issue and authority to publish
an unmerged PR. Follow the [runtime setup](docs/temporal-runtime.md) and
[desktop/plugin setup](docs/local-delivery-plugin.md) for those prerequisites.
Project tools and GitHub authentication retain their own requirements.

## Install

Use a trusted release checkout for setup. Set `RELEASE_TAG` to the
chosen [published release](https://github.com/ebarti/devflow/releases). `main`
and open PRs contain unreleased work.

```sh
git clone --branch "$RELEASE_TAG" https://github.com/ebarti/devflow.git
cd devflow
sh scripts/install.sh
```

The normal installer creates or verifies the independently owned checkout at
`~/.local/share/devflow/developer-workflow`, with its own Git objects and frozen
runtime environment (`uv` is required). Registrations point into that durable
checkout, so removing the setup checkout or allowing managed worktree cleanup
does not remove the installed code, dependencies, helpers or entry points.
The installer proves the actual imports and installed wheel contents before
exposing the sole `devflow-local-delivery` service skill
and copies four internal agent definitions as regular files. If a matching
installed local delivery plugin already provides that skill, installation
preserves it without creating a duplicate direct entry. The `devflow` path
contains script/reference helpers for retained work and has no `SKILL.md`.
The other direct-agent skills in the source checkout are not globally installed.

Defaults are `~/.agents/skills` and `$CODEX_HOME` (`~/.codex` when unset).
Use explicit locations for another host configuration:

```sh
sh scripts/install.sh /path/to/host/skills /path/to/codex-home
```

Normal installation does not configure MCP, install metrics
hooks, activate a launchd reconciler or change global model settings. Existing
foreign hooks, host configuration and user data are preserved. Internal agent
copies are tracked by `agents/.devflow-agent-manifest.json`; installation
refreshes unchanged owned copies and removes obsolete unchanged owned copies.
Modified or custom copies are preserved. `--force` retains the existing agent
installer's checkout-switch behavior; it does not bypass migration checks or
replace custom files.

Global delivery commands require the explicit operation described in
[the launcher installation contract](docs/global-delivery-launchers.md).
`scripts/install.sh --delivery-launchers` exposes the canonical
`devflow-delivery` and `devflow-delivery-mcp` environment and configuration.
It can migrate the recognized historical `devflow` command into an alias of
`devflow-delivery`, retaining the old immutable release and guarded rollback.
Normal installation does not migrate a CLI or activate a service.

For a source trial, explicitly select isolated destinations:

```sh
sh scripts/install.sh --development /path/to/trial/skills /path/to/trial/codex
```

`DEVFLOW_INSTALL_MODE=development` provides the same explicit mode for historical
updaters that cannot forward new flags. Development mode preserves source links
and refuses the user's production registration directories. It does not provide
a durable installation proof. Production launcher and supervisor registration
reject temporary/managed source, shared Git objects, indirect environment/import
dependencies, or temporary configuration and persistence roots.
Hosts with additional cleanup roots can supply `DEVFLOW_MANAGED_WORKTREE_ROOTS`
as a JSON array of absolute paths; the checks resolve configured aliases before
testing every dependency. Production source must use the fixed independently
owned installation root, including when launcher/service registration is invoked
from another checkout.

An interrupted installation retains its journal alongside the durable checkout.
Replay the same setup request to reconcile it, or run `sh scripts/install.sh
--rollback` to restore registrations and retain the unfinished owned checkout.
Rollback never deletes run state, configuration or histories. Existing native
execution grants and acknowledged runtime locations must use the supported
stopped `runtime-installation` migration; normal setup does not rewrite them.

## Upgrade

Between tasks, select a published release and update the durable owner:

```sh
sh scripts/update.sh "$RELEASE_TAG"
# For a custom installation, reuse its original locations:
sh scripts/update.sh "$RELEASE_TAG" /path/to/host/skills /path/to/codex-home
```

The production updater operates on the durable owner even when invoked from a
disposable setup checkout. It requires the worker and supervisor to be stopped,
fetches the tag, switches to its commit, synchronizes the frozen environment and
reconciles registrations. Source/environment recovery and registration snapshots
remain journaled until verification. Replay an interrupted update or use
`sh scripts/update.sh --rollback`; failed recovery retains its journal for retry.
Before changing dependencies, the updater verifies an independent complete copy
of the old environment. Rollback exchanges that copy back into place without
network access or dependency resolution. The journal also records a recovery
command using a separately proved base interpreter and retained controller;
use that command if the candidate has broken its own updater script.
Acknowledged native installations require their supported stopped authority
transition before changing source/environment. Existing guards remain enforced.
The ordinary updater currently supports fresh-owner fake-backend source updates.
Native source-successor acknowledgements and authenticated legacy-owner enrollment
remain tracked in [#237](https://github.com/ebarti/devflow/issues/237); those updates
currently refuse. This installation foundation does not complete those transitions.
New durable owners register every configuration loaded by the CLI/API, including
custom state roots, under the same installation lock. Missing databases and
unknown custody refuse an update. A legacy owner has no complete inventory merely
because a directory scan found nothing: it requires an authenticated stopped
cutover before ordinary updates can be enabled. Original native authority and
historical run records are retained throughout that transition.
Production console entries acquire a shared lifetime ownership lease through a
retained stdlib bootstrap before importing the mutable environment or source.
CLI/API/worker processes keep that lease until exit; the updater requires the
exclusive lease. Concurrent configuration readers use a separate short inventory
lock. An interrupted update keeps new operational starts blocked until its
retained recovery command reconciles the source and environment.
The installer prints immutable production command paths beside the runtime.
Global launcher and launchd registrations call those retained bootstraps directly;
they do not execute a mutable console script while `uv` replaces the environment.
The `.venv/bin` scripts are internal environment entries. Use the printed commands
or explicitly registered global launchers for production startup.
Use a supported `DEVFLOW_PYTHON`; a retired historical interpreter need not exist. Tracked source edits stop
an update. No database migration or service activation occurs in this normal
installation path.

Upgrades from previous direct-agent installations retire only unchanged owned
skill pointers and the recognized metrics hook registrations. Guarded installs
must have an owned `.devflow-install.json` pin whose hashes match the historical
Git inventory, and an unchanged `.devflow-hook.py`. The published v0.2.2 installer
predates these manifests: that case requires its exact verified historical Git
inventory, owned canonical source and skill symlinks, unchanged current source
files and all of its exact generated telemetry hook registrations. Arbitrary
unrecorded registrations are not migration candidates. The exact historical recipe
must use one consistently serialized canonical absolute interpreter prefix across
all generated events; its hook target, arguments, item fields and groups must
match. Historical installers supported Python 3.12+ and canonicalized the selected
executable without restricting its filename. The retired interpreter is never
executed. Old manifests did not record its value or version, so this recognizes
the generated recipe with a consistent historical interpreter; it cannot cryptographically prove that an unknown old interpreter
value was never edited. Inconsistent prefixes, quoting, arguments, events and
modified owned files still refuse before effects.

The migration atomically exchanges the `devflow` pointer for script-only
helpers under the skills directory, preserving source files and keeping helper
paths available. Other unchanged owned direct skills are removed from discovery.
Foreign hook entries and metadata remain. A modified, unowned or ambiguous
registration stops installation with its precise path and reason, even with
`--force`. Restore the recorded bytes/pointer or relocate the conflicting
registration and retry. Do not delete source helpers or user data to resolve a
registration conflict.

Private snapshots live in the owned Codex directory or its nearest owned protected
ancestor, outside the OS temporary-directory policy. A group-writable Codex
directory uses that ancestor without changing its permissions. Backup/target filesystems and
real native exchange are checked before destination effects. Snapshot file bytes,
mode and identity come from one opened-file observation;
a write during that read refuses capture before installation effects.
On preflight or zero-effect capture refusal, destinations are left untouched
and the authenticated previous checkout is restored, including updates begun
by the historical exec-based updater. After an installer effect,
rollback restores only matching recorded object identities; later changed or
foreign objects remain, with an actionable path and retained private backup.
Destination recovery failure still attempts authenticated source checkout recovery.
Recovery continues independent paths after object errors; helper cleanup waits
for pointer recovery. All conflicts and the backup remain until the operator
recovers them. Successful installation removes its rollback snapshot. Public refusals return
status 1; the installer never excludes other writers.
Owned destination pointers use native atomic exchange on Linux/macOS before
helper cleanup; unsupported hosts refuse without a replacement fallback.
Native exchange and capture require the private backup and target on the same
filesystem. Forward writes authenticate the displaced object in that backup;
retirement and rollback cleanup capture objects before authenticating and deleting
them. Concurrent drift refuses installation and retains foreign bytes and the
full backup at the reported paths. Upgrades begun
by the historical updater also restore its previous detached checkout when it
remains clean; newer updater failures restore their previous branch or commit.
SQLite, unrelated host files and background services remain outside this
operation. Retained work must finish before changing its installed source.

## Start

Install the [local delivery plugin](docs/local-delivery-plugin.md), connect it
to the configured service and use `devflow-local-delivery` for an authorized
request, such as fixing the retry bug in a linked repository issue. The service
reports its public policy and owns role selection, planning, implementation,
review and verification. Missing issue or delivery authority requires a blocking
decision. A submission authorizes publication through an unmerged PR;
merge, release and deployment require separate authority.

The [GitHub feature delivery model](docs/github-feature-delivery.md) uses the
parent issue as the feature, sub-issues as workstreams, and one stack for complete
incremental chunks. Feature mode coordinates parallel implementation, preserves
ownership and repair accounting across continuations, and supports reviewed plan
corrections within the original authority. Version 2 records keep detailed plans
on the child issues and a compact revision index on the parent. The public
`revise_feature_plan` operation requests native investigation and review of a
planning defect; it preserves the existing stack and cumulative repair budget.
An explicit merge instruction follows verification. Project updates are handled
by the independent event consumer.

Use the dashboard or the plugin's status/evidence operations to inspect a run.
Blocking decisions carry the exact run and candidate identity. A fresh host
task may be needed after registration for plugin discovery. The normal
installer supplies internal roles rather than independent global role skills.

## Retained helper workflows

Historical direct-agent skills, candidate trial tooling and standard-library
state/metrics helpers remain in the source tree for existing work and explicit
inspection. Their documentation describes those historical interfaces; normal
installation does not activate their hooks, collect their metrics or start their
reconciler. The [work records reference](skills/devflow/references/state.md) and
[storage contract](docs/implementation-contracts.md) describe retained helper
storage, separate from the delivery service's runtime configuration.

## Checks

```sh
sh scripts/check-install.sh
python3.12 -B -m unittest discover -s tests -v
```

Installation checks use isolated temporary skills and Codex-home directories.
They cover fresh installation, authentic historical upgrades, replay, refusal
and failure rollback through the public installer/updater. Updater tests create
tags only in their disposable local Git fixtures, because the public updater
requires a release tag fetched from its origin. CI fetches history for these
historical fixtures and separately checks the runtime and dashboard. These
checks do not perform live installation, model-driven delivery, target-project
checks, merges or deployment.

The runtime suite runs in two macOS jobs with two pytest parents per host.
`runtime/runtime_ci.py` collects the full suite in each parent and assigns whole
modules to four partitions using the measured weights in `runtime/ci_timings.json`;
new modules are included automatically. Host 0 runs partitions 0 and 2, and host 1
runs partitions 1 and 3. Cases within a module retain their order and run in one
parent. Host 0 completes the Mac source checks before starting its runtime parents.
Each job uploads its test inventory, result and JUnit timings. The required
`temporal-runtime-macos` check passes only when both Mac jobs and the portable
source job pass, and all four partition reports prove every collected test
finished exactly once at the same Git revision.
Update the timing weights from the uploaded results as the suite grows. From
`runtime/`, `uv run --frozen pytest` still runs the complete suite. To run one
partition locally:

```sh
uv run --frozen python runtime_ci.py --index 0 --count 4 --revision "$(git rev-parse HEAD)"
```

Keep future tests focused on high-value user, workflow, integrity and security
regressions or integration behavior. Do not add small isolated unit tests,
implementation-mirror assertions or trivial parameter expansions. Prefer extending
an existing meaningful scenario when it covers the risk. See [AGENTS.md](AGENTS.md)
for the repository contribution rules, including conventional PR titles.

[Architecture](docs/architecture.md)

Automatic retries keep the accepted plan and frozen per-issue attempt ceiling. Later cleanup and acknowledged Blocked/claim release can finish after that ceiling is exhausted, but cannot create another attempt. Cleanup maintenance preserves exact recorded resource evidence before updating canonical latest observations; interrupted maintenance reuses and authenticates the immutable archive. If the original finalizer returned no bound receipt, maintenance preserves that absence in the closed result and separately archives authenticated recovery-time observations. Any declared receipt hash remains mandatory. Completed provider journals without the original result binding remain unresolved rather than inferred clean. Ordinary closed unpublished deliveries also support public cleanup after exact accepted implementation custody is proven; cleanup records its own unpublished closure and releases only that run's claim. A passed implementation interrupted at prechecks can then resume those checks without another implementation turn. A provider turn with an unknown outcome can only seed an explicitly authorized new finite iteration from its exact supervisor-recorded partial output, in a fresh session/home; its original verdict stays unknown. A continuation stopped in preflight before its new iteration can reuse that exact unused iteration and existing ceiling with zero added iterations after public cleanup; it must retain the same source, inventory, session absence and unused home. Historical resource observation authenticates membership under the current admitted execution. These continuations require a worker with the corresponding recovery readers, so drain all open native workflows before a supported runtime update.

A closed ordinary implementation may resume with one or two explicitly authorized tracked files when its completed native assessment records blocked findings. The existing stopped-resume path authenticates the original SDK completion, private native receipts, session, retained partial source and exact amended configuration at admission and worker readback. It preserves the blocked result and spends only unused original iterations; every normal gate still applies. See the [scope-resume contract](docs/temporal-runtime.md).

A separate closed-run browser setup amendment can acknowledge only the reviewed `JOBCTRL_E2E_ISOLATED` transition from `1` to `0` for the same two selected scenarios. Fresh registration keeps its isolated default. The amendment binds private proposal/configuration bytes, original failed per-case evidence, reviewed owned fixture sources and an immutable acknowledgement, then requires both original cases to pass a fresh native gate. It preserves fake actions/transports, owned profile/data paths, all other recipe settings, normal gates and the original finite repair ceiling. See the [browser setup amendment contract](docs/temporal-runtime.md).

Private scope/browser amendments also bind the canonical live service scheduling limit independently of their frozen policy. A historical capacity `6` remains recorded as `6`; a live limit `2` stays `2` in the private amendment and native supervisor. Original submitted owner provenance, exact compatible owner bytes and repeated readback prevent a caller's private file from raising that limit.

Closed ordinary native deliveries can explicitly bind official pinned Temporal test prerequisites for one unchanged check through `stopped_delivery_resume.temporal_test_prerequisites`; the Python project/lock, provenance, private tool generation and two supported environment paths are authenticated through worker launch. This setup declaration grants no extra iteration and waives no test or gate. See [the runtime contract](docs/temporal-runtime.md).
