# Architecture

The host discovers eight sibling skill directories. An agent selects the skill that fits the user's request, follows its procedure, and invokes existing tools directly. Delegated work runs as one of the five [Devflow agent types](../skills/devflow/references/agents.md) shipped in `agents/`, each pinning the workflow's default model and effort; the coordinator is the user's session and performs coordination, records, issue updates, authorized review comments and final merges. The implementer opens PRs early and pushes repairs. Sequential unmerged work forms one gh stack; [PR workflow](../skills/devflow/references/pr-workflow.md) owns the branch/publication rules. There is no delivery agent. The [implementation worker](../skills/devflow/references/implementation-worker.md) reference defines the worker's override, dispatch precondition, reuse and failure handling. The main [devflow skill](../skills/devflow/SKILL.md) owns the shared helper instructions. Skill references are canonical for agent-facing behavior; these docs describe components and storage and link to them instead of restating them.

`skills/devflow/scripts/state.py` uses Python's standard library to store local records, reserve work for an owning task, bind that task's runtime session and query metrics. Issue claims are atomic within a shared SQLite database. Independent work executes outside those short transactions.

`skills/devflow/scripts/github.py` uses the authenticated `gh` CLI to create or reuse issues, assign the responsible user and update the existing Project's Status; its creation, read-back and ownership guarantees are in the [storage contract](implementation-contracts.md). Neither helper starts agents or runs project checks.

`telemetry.py` installs lifecycle hooks, records bound sessions' runtime events and token-counter deltas, and denies recognized repository write commands from a claim-holding coordinator session; `measurements.py` derives timings, outcomes, recovery and coverage from the same database. Collection scope and limits are defined in [work records and metrics](../skills/devflow/references/state.md#usage).

The installer links skills and agent definitions from a release checkout and merges the collector into the host's hook configuration. Other agents and hooks are preserved; the host owns hook trust. Explicit upgrades select a release tag, refresh installation and remove obsolete owned links. Candidate sessions use separate configuration, skills, agent definitions and SQLite. Project policies and host model settings stay with their existing owners; the worker's model is pinned in its installed agent definition, never chosen at spawn time.

Agent prompts specify methods and report formats; input/output validation and enforced sequencing are deferred. Hook rules are a backstop rather than a complete enforcement boundary.

Helper behavior is covered by `tests/` (claims, replay, migration, transcript collection, issue creation) and by the installation smoke check in `scripts/check-install.sh`.

See the [README diagrams](../README.md#how-it-works), [storage contract](implementation-contracts.md) and [helper commands](../skills/devflow/references/state.md).
