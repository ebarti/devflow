# Implementation worker

Every implementation change runs in a dedicated implementation worker: features, fixes, review and QA repairs, and regression-test changes. The worker owns implementation, commits, early PR creation and subsequent pushes. The main task owns inspection and design; its execution coordinator owns dispatch, repair loops, records and the authorized merge. Neither edits the candidate. This reference defines the worker's configuration, brief, reuse and failure handling. The worker is one of the [Devflow agents](agents.md).

## The agent definition

The worker is the Codex agent type `devflow-implementer`, defined in `agents/devflow-implementer.toml` in the Devflow checkout and linked by `install.sh` into `$CODEX_HOME/agents/`. The definition pins **Sol / xhigh** (model `gpt-6-sol`, reasoning effort `xhigh`), a workspace-write sandbox and the worker's standing instructions. Codex applies an agent file's model and effort ahead of any spawn value, so spawning names the type and nothing else:

```json
{
  "agent_type": "devflow-implementer",
  "fork_turns": "none"
}
```

Add `task_name` and `message` (the brief). Do not pass `model` or `reasoning_effort`, do not use a generic `worker`, `implementer` or `fixer` role for implementation, and do not let a worker inherit the coordinator's model. `fork_turns: "none"` gives the worker no conversation history, so the brief must be self-contained.

## Override

The only override is a checked-in, project-scoped definition of the same agent in the target repository: `.codex/agents/devflow-implementer.toml`, which Codex loads for a trusted project in place of the installed definition. A request in the conversation, prose in agent instruction files and user-level settings are not overrides; the coordinator never chooses a model, so there is nothing for them to change. If the user wants another model for a repository, it goes into that repository's agent file first. The coordinator records the actual model and effort on the worker's run with `state.py record run --model MODEL --effort EFFORT` so metrics reflect what ran; workers write no Devflow records. The other roles run as their own Devflow agent types with the defaults in the [agents reference](agents.md).

## Prerequisites

Codex multi-agent tools enabled (they are on by default), the installed Devflow agent definition, and access to the pinned model. Devflow installs the definition; it does not change feature flags or verify model access.

## When to dispatch

Dispatch a worker only when the plan is sharp. The main task's [inspection](../../devflow-defining-work/SKILL.md) and [plan](../../devflow-planning/SKILL.md) supply the accepted outcome, design rationale, owned files, dependencies and exact checks with expected results. The execution coordinator turns each ready slice into a worker brief. Missing design decisions go back to the main task before dependent implementation. The worker implements against the brief and raises gaps to its coordinator rather than redefining the outcome or inventing verification.

## Reuse

Reuse is decided by the implementation, not by availability. When the next task is the same implementation or closely related to what an agent already did, in the same work ID and worktree, continue that agent: repairs go to the original implementer, rechecks to the original reviewer or verifier, preserving the independence the project requires. When the implementation is unrelated, spawn a new worker even if an idle one exists; stale context is not worth carrying. A reused implementer is a `devflow-implementer` agent like a new one. On Codex, continue a finished or idle agent with `agents.followup_task` using its existing ID or path, and send corrections to an active one with `agents.send_message`; other hosts use their equivalent continuation tools. Do not assign overlapping work. Spawn new workers for independent slices that must run concurrently.

## The brief

Each worker receives a concise brief containing:

- the work ID, repository/worktree and implementation assignment;
- one observable outcome;
- owned files or modules, and the note that other agents share the checkout and their edits must be preserved;
- inspection evidence, design rationale, dependencies, existing PR/stack, branch and base;
- publication scope, including any explicit local-only, no-commit or no-push limit;
- exact checks or manual steps with expected results and explicit limits such as no tests;
- the coordinator's reply target and relevant [implementation instructions](../../devflow-implementing/SKILL.md).

Missing or contradictory task inputs are returned to the coordinator before editing. The worker never spawns a coordinator or another worker. Report the exact candidate, PR/base/head and stack order, check results and remaining work. For no-commit scope, identify the complete dirty snapshot and its content hash instead of presenting HEAD as the candidate. See [PR workflow](pr-workflow.md).

## When spawning fails

Separate transient failures from configuration failures. For exhausted capacity, a rate limit or timeout, reuse a suitable worker or wait within the host's limits while independent work continues; do not repeatedly retry unchanged conditions. An unknown agent type, rejected model or permission error is a configuration blocker: return the exact failure and requested type to the main task, which handles any user decision. In neither case implement directly or substitute another agent type/model. Record the outcome and resume the same assignment once the prerequisite is fixed.
