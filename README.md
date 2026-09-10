# Developer Workflow

A reusable development workflow with a local `devflow` CLI, a focused host skill, repository profiles, and explicit execution evidence.

Version 0.2.0 deliberately blocks managed execution: no authenticated intake or independent human-validation adapter is available in the CLI. `doctor` reports this as `BLOCKED`. Capture and safe recovery remain available. See [verified intake](docs/issue-trust.md) for the trust boundary and the security exception to old runtime pins.

The initial runtime implements accepted work contracts, durable attempts, owned Git checkouts, candidate-bound checks and independent gates, finding publication/closure, delivery receipts, and deterministic usage/outcome reports. An interrupted external action is reconciled before another mutation. A changed candidate cannot inherit a previous candidate's passing gate.

Workflow code, skills, schemas, and operating documentation are released together. Consumers pin a release and full Git commit in `.devflow/workflow.lock`. Runtime records, credentials, machine bindings, and usage evidence stay outside this repository.

The happy path stays short: capture or reuse one backlog issue, implement, run applicable checks and independent gates, then deliver. Immediate implementation still gets an issue; corrections reuse the same record. Git, GitHub CLI, uv, ccusage and native task tools perform their existing jobs. Python owns the durable work/evidence state and cross-tool consistency rules, including candidate changes and uncertain-action recovery.

`backlog capture` journals the request before calling `gh`; a restart recovers its issue by a stable marker. `backlog list` and `work list` rediscover unfinished requests and attempts without the original conversation. State, evidence and installer records use explicit disk flushes before acknowledgment. See [crash recovery](docs/operation.md#recover-after-a-crash-or-power-loss) for resumption and the storage boundary.

## Try the package

The development runtime requires Python 3.12+, Git, and uv on a local POSIX host. GitHub operations also use the authenticated GitHub CLI; usage collection uses npx. Native role tasks use the active owner's supported task tools and user-selected model settings.

```sh
git clone git@github.com:ebarti/developer-workflow.git
cd developer-workflow
uv sync --frozen
uv run devflow --help
uv run devflow validate record --request-file docs/design/work-contract.example.json --json
uv run pytest -q
```

For installation, prepare a manifest from a clean full commit, review its exact paths and digest, and apply it with separately supplied scope arguments. Enrolled repositories commit three small `.devflow` profile/lock files. Historical attempt pins remain recorded; 0.2.0 requires re-admission before further execution and never delegates to an older runtime. See [operation](docs/operation.md) for installation, request structure, commands, recovery, and delivery.

The [host skill](skills/devflow/SKILL.md) selects the relevant intake, implementation, review, QA, or delivery reference. It introduces no background scheduler, hidden model calls, API keys, or model override. Private execution state stays outside the source checkout.

## Verification and adoption

Run `uv run ruff check .`, `uv run pytest -q`, `uv build`, and `git diff --check`. Tests include real temporary Git repositories, real subprocess checks, concurrent requests, actual process interruption, and private-state recovery. GitHub failure/race tests use controlled adapters; they do not establish live server protection or desktop visibility.

This is the reusable package's initial implementation. [Implementation status](docs/implementation-status.md) distinguishes implemented behavior, demonstrated integration, and pending deployment acceptance. Native desktop canaries, protected merge/atomic-stack conformance, and JobCtrl enrollment/cutover remain explicit adoption gates. Ruleset-only merge protection, queues, and unverified atomic stacks currently block automatic delivery. See [compatibility](docs/compatibility.md) for supported paths and limits.

The [architecture](docs/architecture.md), [contracts](docs/implementation-contracts.md), and [implementation plan](docs/implementation-plan.md) retain the full target design. JobCtrl is the first planned adopter; creating this package does not change its runtime, profiles, issues, protection rules, or existing shared workflows.
