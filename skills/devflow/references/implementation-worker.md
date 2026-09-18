# Implementation worker

Every implementation change runs in a dedicated implementation worker: features, fixes, review and QA repairs, and regression-test changes. The coordinator owns scope, dispatch, verification and delivery and never edits the candidate itself. This reference is the single definition of the worker's model, the only override that can change it, spawn arguments, brief and failure handling; the role skills link here instead of restating it.

## Model and override

The worker is **Sol / high**: model `gpt-5.6-sol`, reasoning effort `high`. The only thing that changes it is an explicit, checked-in configuration entry in the target repository that names the Devflow implementation worker's model and effort, for example a `[agents.worker]` table with `model` and `reasoning_effort` in the repository's `.codex/config.toml`. When such an entry exists, pass its values as the spawn arguments. A request in the conversation, prose in agent instruction files and user-level settings are not overrides; if the user wants another model, it goes into the repository's configuration first.

Silently inheriting the coordinator's model is never acceptable, and neither is substituting another model without such an entry. Record the actual model and effort on the worker's run with `state.py record run --model MODEL --effort EFFORT` so metrics reflect what ran. Coordinator, review and verification roles keep their existing models.

## Prerequisites

On Codex, spawning needs agent support, a configurable `worker` role in the Codex configuration, and access to the selected model. Fixed roles such as `implementer`, `fixer` or a QA role may override the model, so they are not used for implementation. Devflow does not create the role or verify model access.

## Spawning on Codex

When no suitable agent exists, spawn a new worker with these literal arguments, adding `task_name` and `message`, and changing the model or effort only when the repository configuration above says so:

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

If the `worker` role is missing, the model is unavailable or rejected, or the spawn fails for any other reason, stop: report the exact failure to the user, name the model and effort that were requested, and ask what configuration to fix. Do not implement the change directly and do not substitute another model. A different model takes effect only once it is written into the repository's configuration; a model named in the conversation is not an override. Record the outcome in the work record and continue once the configuration is fixed.
