# Developer Workflow

A reusable development workflow with a local `devflow` CLI, independently discoverable stage skills, repository profiles, and explicit execution evidence.

Version 0.5.0 requires the entry skill before task action, then selects definition, planning, coordination, implementation, review, verification or delivery. Each stage owns its inputs, outputs and next handoff. Design discussion uses the methodology without starting execution. The original conversation coordinates verified subagents; role settings remain independent of its active model. See [stage routing](docs/operation.md#stage-skills) and [role execution](docs/operation.md#role-execution).

Conversational intake accepts a requested change, bug investigation or fix, named issue, or bounded batch such as the current P1 backlog. The coordinator creates or reuses the issue and starts within the requested scope without a separate approval channel. Its persisted request supports workflow consistency; it does not independently authenticate a human. See [conversational intake](docs/issue-trust.md).

The runtime implements accepted work contracts, durable attempts, owned Git checkouts, candidate-bound checks and independent gates, finding publication/closure, delivery receipts, and deterministic usage/outcome reports. An interrupted external action is reconciled before another mutation. A changed candidate cannot inherit a previous candidate's passing gate. Every independent result is retained before repair; late results remain historical. Branch pushes are journaled with exact remote-ref expectations. Delivery requires linked real deferrals and an explicit accounting completeness record, including unavailable/unknown limits.

Workflow code, skills, schemas, and operating documentation are released together. Consumers pin a release and full Git commit in `.devflow/workflow.lock`. Runtime records, credentials, machine bindings, and usage evidence stay outside this repository.

The happy path stays short: capture or reuse one backlog issue, delegate implementation, run applicable checks and independent gates, then deliver. Immediate implementation still gets an issue; corrections reuse the same record. Git, GitHub CLI, uv, ccusage and native subagent tools perform their existing jobs. Python owns the durable work/evidence state and cross-tool consistency rules, including candidate changes and uncertain-action recovery.

`backlog capture` journals the request before calling `gh`; a restart recovers its issue by a stable marker. When the user requests recovery, `backlog list` and `work list` rediscover unfinished requests and attempts without the original conversation. State, evidence and installer records use explicit disk flushes before acknowledgment. See [crash recovery](docs/operation.md#recover-after-a-crash-or-power-loss) for resumption and the storage boundary.

Independent rounds retain their original activation through interruptions. If a completed producer's original gate omits required evidence IDs, `host recover-result` journals a correction by that producer using existing evidence, preserving its original judgement and rejected bytes. See [role execution](docs/operation.md#role-execution) for the exact boundary.

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

A small global instruction requires reading [using-devflow](skills/using-devflow/SKILL.md) before responding or acting and when intent changes. It creates no work or automatic backlog scan. `skill list` exposes the entry, seven stages and the forwarding `devflow` compatibility name; `skill resolve` returns the selected release’s actual file. `next` names each action’s owning skill. These instructions require routing; they do not intercept arbitrary shell calls or establish compliance from a read acknowledgement. No hooks or scheduler are installed.

## Verification and adoption

Run `uv run ruff check .`, `uv run pytest -q`, `uv build`, and `git diff --check`. Tests include real temporary Git repositories, real subprocess checks, concurrent requests, actual process interruption, and private-state recovery. GitHub failure/race tests use controlled adapters; they do not establish live server protection. Synthetic subagent receipts do not establish actual host identity or model conformance.

This is the reusable package's initial implementation. [Implementation status](docs/implementation-status.md) distinguishes implemented behavior, demonstrated integration, and pending deployment acceptance. Native desktop canaries, protected merge/atomic-stack conformance, and JobCtrl enrollment/cutover remain explicit adoption gates. Ruleset-only merge protection, queues, and unverified atomic stacks currently block automatic delivery. See [compatibility](docs/compatibility.md) for supported paths and limits.

The [architecture](docs/architecture.md), [contracts](docs/implementation-contracts.md), and [implementation plan](docs/implementation-plan.md) retain the full target design. JobCtrl is the first planned adopter; creating this package does not change its runtime, profiles, issues, protection rules, or existing shared workflows.
