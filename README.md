# Developer Workflow

A reusable development workflow with a local `devflow` CLI, a focused host skill, repository profiles, and explicit execution evidence.

Version 0.4.0 keeps the user's initial conversation as the coordinator and delegates implementation, review, and QA to subagents. Each role receives explicit settings resolved from the user's configuration and overrides. The coordinator verifies the child's actual identity, model, and reasoning effort before releasing its bounded work. Role settings are independent of the coordinator's active model selection. See [role execution](docs/operation.md#role-execution).

Conversational intake accepts a requested change, bug investigation or fix, named issue, or bounded batch such as the current P1 backlog. The coordinator creates or reuses the issue and starts within the requested scope without a separate approval channel. Its persisted request supports workflow consistency; it does not independently authenticate a human. See [conversational intake](docs/issue-trust.md).

The initial runtime implements accepted work contracts, durable attempts, owned Git checkouts, candidate-bound checks and independent gates, finding publication/closure, delivery receipts, and deterministic usage/outcome reports. An interrupted external action is reconciled before another mutation. A changed candidate cannot inherit a previous candidate's passing gate.

Workflow code, skills, schemas, and operating documentation are released together. Consumers pin a release and full Git commit in `.devflow/workflow.lock`. Runtime records, credentials, machine bindings, and usage evidence stay outside this repository.

The happy path stays short: capture or reuse one backlog issue, delegate implementation, run applicable checks and independent gates, then deliver. Immediate implementation still gets an issue; corrections reuse the same record. Git, GitHub CLI, uv, ccusage and native subagent tools perform their existing jobs. Python owns the durable work/evidence state and cross-tool consistency rules, including candidate changes and uncertain-action recovery.

`backlog capture` journals the request before calling `gh`; a restart recovers its issue by a stable marker. When the user requests recovery, `backlog list` and `work list` rediscover unfinished requests and attempts without the original conversation. State, evidence and installer records use explicit disk flushes before acknowledgment. See [crash recovery](docs/operation.md#recover-after-a-crash-or-power-loss) for resumption and the storage boundary.

## Try the package

The development runtime requires Python 3.12+, Git, and uv on a local POSIX host. GitHub operations also use the authenticated GitHub CLI; usage collection uses npx. Role execution uses the coordinator's supported subagent tools and user-selected model settings, with readable child session metadata for startup verification.

```sh
git clone git@github.com:ebarti/devflow.git
cd devflow
uv sync --frozen
uv run devflow --help
uv run devflow validate record --request-file docs/design/work-contract.example.json --json
uv run pytest -q
```

For installation, prepare a manifest from a clean full commit, review its exact paths and digest, and apply it with separately supplied scope arguments. Enrolled repositories commit three small `.devflow` profile/lock files. Historical attempt pins remain recorded; legacy work requires current admission before further execution, and the current reader never delegates to an older runtime. See [operation](docs/operation.md) for installation, request structure, commands, recovery, and delivery.

A tiny global instruction routes new chats to the [using-devflow entry skill](skills/using-devflow/SKILL.md). This loads instructions only: ordinary questions and unused sessions create no work and do not scan or resume backlog. For requested repository work, the [host skill](skills/devflow/SKILL.md) loads the relevant intake, implementation, review, QA, or delivery reference. There are no hooks, automatic scheduling, or new service. The package introduces no model API keys and does not rewrite user settings. Subagent launches and their resolved settings are recorded explicitly. Private execution state stays outside the source checkout.

## Verification and adoption

Run `uv run ruff check .`, `uv run pytest -q`, `uv build`, and `git diff --check`. Tests include real temporary Git repositories, real subprocess checks, concurrent requests, actual process interruption, and private-state recovery. GitHub failure/race tests use controlled adapters; they do not establish live server protection. Synthetic subagent receipts do not establish actual host identity or model conformance.

This is the reusable package's initial implementation. [Implementation status](docs/implementation-status.md) distinguishes implemented behavior, demonstrated integration, and pending deployment acceptance. Native desktop canaries, protected merge/atomic-stack conformance, and JobCtrl enrollment/cutover remain explicit adoption gates. Ruleset-only merge protection, queues, and unverified atomic stacks currently block automatic delivery. See [compatibility](docs/compatibility.md) for supported paths and limits.

The [architecture](docs/architecture.md), [contracts](docs/implementation-contracts.md), and [implementation plan](docs/implementation-plan.md) retain the full target design. JobCtrl is the first planned adopter; creating this package does not change its runtime, profiles, issues, protection rules, or existing shared workflows.
