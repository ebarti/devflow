# Implementation worker

Every implementation change runs in a dedicated implementation worker: features, fixes, review and QA repairs, and regression-test changes. The coordinator owns scope, dispatch, verification and delivery and never edits the candidate itself. This reference is the single definition of the worker's model, precedence, spawn arguments, brief and failure handling; the role skills link here instead of restating it.

## Model and precedence

The default worker is **Sol / high**: model `gpt-5.6-sol`, reasoning effort `high`. Choose the worker's model and effort in this order:

1. An explicit instruction from the user for this task.
2. The target project's instructions, when they name a model or effort for implementation work.
3. The default above.

Silently inheriting the coordinator's model is never acceptable, and neither is substituting another model without an instruction from this list. Record the actual model and effort on the worker's run with `state.py record run --model MODEL --effort EFFORT` so metrics reflect what ran. Coordinator, review and verification roles keep their existing models.

## Prerequisites

On Codex, spawning needs agent support, a configurable `worker` role in the Codex configuration, and access to the selected model. Fixed roles such as `implementer`, `fixer` or a QA role may override the model, so they are not used for implementation. Devflow does not create the role or verify model access.

## Spawning on Codex

When no suitable agent exists, spawn a new worker with these literal arguments, adding `task_name` and `message`, and changing the model or effort only under the precedence above:

```json
{
  "agent_type": "worker",
  "model": "gpt-5.6-sol",
  "reasoning_effort": "high",
  "fork_turns": "none"
}
```

`fork_turns: "none"` gives the worker no conversation history, so the brief must be self-contained. The coordinator is responsible for the worker's configuration: it passed the arguments, so it verifies them. If it cannot confirm what a worker runs, it replaces the worker rather than asking the worker to guess.

## Reuse

Before spawning, inspect existing agents. Reuse an agent for related work in the same role, work ID and worktree; implementation reuse requires the same model and effort a fresh spawn would use. Prefer the original implementer for repairs and the original reviewer or verifier for rechecks while preserving the independence the project requires. On Codex, continue a finished or idle agent with `agents.followup_task` using its existing ID or path, and send corrections to an active one with `agents.send_message`; other hosts use their equivalent continuation tools. Do not assign overlapping work. Spawn only when no suitable agent is available or independent work must run concurrently.

## The brief

Each worker receives a concise brief containing:

- the statement that it is the implementation worker for a named work ID, the model and effort it was spawned with, and that it must not delegate implementation;
- one observable outcome;
- owned files or modules, and the note that other agents share the checkout and their edits must be preserved;
- required context and dependencies;
- exact checks or manual steps with expected results;
- the relevant [implementation instructions](../../devflow-implementing/SKILL.md).

The worker relies on this brief rather than introspecting its own model. A worker without such a brief is not the implementation worker and follows [coordinating](../../devflow-coordinating/SKILL.md) instead.

## When spawning fails

If the `worker` role is missing, the model is unavailable or rejected, or the spawn fails for any other reason, stop: report the exact failure to the user, name the model and effort that were requested, and ask which model and effort to use or what configuration to fix. Do not implement the change directly and do not substitute another model. The user's answer is an explicit instruction under the precedence above; record it in the work record and continue.
