# Developer Workflow

A reusable development workflow with a local `devflow` CLI, a focused host skill, repository profiles, and explicit execution evidence.

The initial runtime implements accepted work contracts, durable attempts, owned Git checkouts, candidate-bound checks and independent gates, finding publication/closure, delivery receipts, and deterministic usage/outcome reports. An interrupted external action is reconciled before another mutation. A changed candidate cannot inherit a previous candidate's passing gate.

Workflow code, skills, schemas, and operating documentation are released together. Consumers pin a release and full Git commit in `.devflow/workflow.lock`. Runtime records, credentials, machine bindings, and usage evidence stay outside this repository.

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

For installation, prepare a manifest from a clean full commit, review its exact paths and digest, and apply it with separately supplied scope arguments. Enrolled repositories commit three small `.devflow` profile/lock files. Each active attempt retains its original package revision when a repository upgrades. See [operation](docs/operation.md) for installation, request structure, commands, recovery, and delivery.

The [host skill](skills/devflow/SKILL.md) selects the relevant intake, implementation, review, QA, or delivery reference. It introduces no background scheduler, hidden model calls, API keys, or model override. Private execution state stays outside the source checkout.

## Verification and adoption

Run `uv run ruff check .`, `uv run pytest -q`, `uv build`, and `git diff --check`. Tests include real temporary Git repositories, real subprocess checks, concurrent requests, actual process interruption, and private-state recovery. GitHub failure/race tests use controlled adapters; they do not establish live server protection or desktop visibility.

This is the reusable package's initial implementation. [Implementation status](docs/implementation-status.md) distinguishes implemented behavior, demonstrated integration, and pending deployment acceptance. Native desktop canaries, protected merge/atomic-stack conformance, and JobCtrl enrollment/cutover remain explicit adoption gates. Ruleset-only merge protection, queues, and unverified atomic stacks currently block automatic delivery. See [compatibility](docs/compatibility.md) for supported paths and limits.

The [architecture](docs/architecture.md), [contracts](docs/implementation-contracts.md), and [implementation plan](docs/implementation-plan.md) retain the full target design. JobCtrl is the first planned adopter; creating this package does not change its runtime, profiles, issues, protection rules, or existing shared workflows.
