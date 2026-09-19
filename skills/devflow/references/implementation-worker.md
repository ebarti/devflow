# Implementation worker

Every implementation change runs in a dedicated implementation worker: features, fixes, review and QA repairs, and regression-test changes. The coordinator owns scope, dispatch, verification and delivery and never edits the candidate itself. This reference is the single definition of the worker: its agent definition, the only override that can change it, when it is dispatched, the brief it receives, reuse, and failure handling. The role skills link here instead of restating it. The worker is one of the [Devflow agents](agents.md); it is the only one whose definition pins a model.

## The agent definition

The worker is the Codex agent type `devflow-implementer`, defined in `agents/devflow-implementer.toml` in the Devflow checkout and linked by `install.sh` into `$CODEX_HOME/agents/`. The definition pins **Sol / high** (model `gpt-5.6-sol`, reasoning effort `high`), a workspace-write sandbox and the worker's standing instructions. Codex applies an agent file's model and effort ahead of any spawn value, so spawning names the type and nothing else:

```json
{
  "agent_type": "devflow-implementer",
  "fork_turns": "none"
}
```

Add `task_name` and `message` (the brief). Do not pass `model` or `reasoning_effort`, do not use a generic `worker`, `implementer` or `fixer` role for implementation, and do not let a worker inherit the coordinator's model. `fork_turns: "none"` gives the worker no conversation history, so the brief must be self-contained.

## Override

The only override is a checked-in, project-scoped definition of the same agent in the target repository: `.codex/agents/devflow-implementer.toml`, which Codex loads for a trusted project in place of the installed definition. A request in the conversation, prose in agent instruction files and user-level settings are not overrides; the coordinator never chooses a model, so there is nothing for them to change. If the user wants another model for a repository, it goes into that repository's agent file first. Record the actual model and effort on the worker's run with `state.py record run --model MODEL --effort EFFORT` so metrics reflect what ran. Coordinator, review and verification roles keep their existing agent types and models.

## Prerequisites

Codex multi-agent tools enabled (they are on by default), the installed Devflow agent definition, and access to the pinned model. Devflow installs the definition; it does not change feature flags or verify model access.

## When to dispatch

Dispatch a worker only when the plan is sharp. Before spawning, the coordinator holds the accepted outcome, the slice with its owned files and dependencies, and exact checks or manual steps with expected results, produced through [defining work](../../devflow-defining-work/SKILL.md) and [planning](../../devflow-planning/SKILL.md) as the request required. If those cannot be written down yet, the work is not ready for a worker: sharpen the plan instead of delegating the ambiguity. The worker implements against the brief and raises gaps to the coordinator rather than redefining the outcome or inventing its own verification.

## Reuse

Reuse is decided by the implementation, not by availability. When the next task is the same implementation or closely related to what an agent already did, in the same work ID and worktree, continue that agent: repairs go to the original implementer, rechecks to the original reviewer or verifier, preserving the independence the project requires. When the implementation is unrelated, spawn a new worker even if an idle one exists; stale context is not worth carrying. A reused implementer is a `devflow-implementer` agent like a new one. On Codex, continue a finished or idle agent with `agents.followup_task` using its existing ID or path, and send corrections to an active one with `agents.send_message`; other hosts use their equivalent continuation tools. Do not assign overlapping work. Spawn new workers for independent slices that must run concurrently.

## The brief

Each worker receives a concise brief containing:

- the statement that it is the implementation worker for a named work ID, the model and effort of the installed definition, and that it must not delegate implementation;
- one observable outcome;
- owned files or modules, and the note that other agents share the checkout and their edits must be preserved;
- required context and dependencies;
- exact checks or manual steps with expected results;
- the relevant [implementation instructions](../../devflow-implementing/SKILL.md).

The worker relies on this brief rather than introspecting its own configuration. A worker without such a brief is not the implementation worker and follows [coordinating](../../devflow-coordinating/SKILL.md) instead.

## When spawning fails

Separate transient failures from configuration failures. Exhausted agent capacity, a rate limit or a timeout is transient: wait and retry within the host's capacity, or hand the task to an existing suitable worker under the reuse rules, keep other independent work moving meanwhile, and report only if the failure persists. An unknown or uninstalled `devflow-implementer` agent type, a rejected model, or a permission error is a configuration failure: stop, report the exact failure to the user, name the agent type that was requested, and ask what to fix, such as reinstalling Devflow, enabling multi-agent tools or correcting the repository's override. In neither case implement the change directly or spawn a different agent type or model for the work. Record the outcome in the work record and continue once the configuration is fixed.
