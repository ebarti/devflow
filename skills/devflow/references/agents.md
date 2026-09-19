# Devflow agents

Every delegated action runs as a Devflow agent type, spawned by name. The definitions live in `agents/` in the Devflow checkout and are linked by `install.sh` into `$CODEX_HOME/agents/`. The coordinator is the session itself and has no agent type.

| Action | Skill | Agent type | Sandbox | Model |
| --- | --- | --- | --- | --- |
| Define the work | [defining work](../../devflow-defining-work/SKILL.md) | `devflow-definer` | read-only | session default |
| Plan the change | [planning](../../devflow-planning/SKILL.md) | `devflow-planner` | read-only | session default |
| Implement the change | [implementing](../../devflow-implementing/SKILL.md) | `devflow-implementer` | workspace-write | pinned: Sol / high |
| Review the candidate | [reviewing](../../devflow-reviewing/SKILL.md) | `devflow-reviewer` | read-only | session default |
| Verify the outcome | [verifying](../../devflow-verifying/SKILL.md) | `devflow-verifier` | workspace-write, evidence only | session default |
| Deliver the outcome | [delivering](../../devflow-delivering/SKILL.md) | `devflow-deliverer` | workspace-write | session default |

## Spawning

Spawn by type and nothing else, for example `{"agent_type": "devflow-reviewer", "fork_turns": "none"}` plus `task_name` and `message`. Never pass `model` or `reasoning_effort`. Codex applies an agent file's model ahead of any spawn value; the definitions that omit a model do so deliberately, so those roles follow the session's subagent default and keep the user's selection. Only the implementation worker pins its model; its dispatch precondition, brief, reuse and failure handling are in the [implementation worker reference](implementation-worker.md). The same reuse rule applies to every type: continue an agent when the next task is the same or closely related work in the same role, work ID and worktree, and spawn a new one otherwise. Every brief states the work ID, the agent's limits and the model and effort the coordinator expects, and agents record their runs with the [state helper](state.md) from that brief.

## Override

A repository changes a Devflow agent only by shipping its own project-scoped `.codex/agents/<name>.toml` with the same `name`, which Codex loads for a trusted project in place of the installed definition. Conversation requests, prose in agent instruction files and user-level settings are not overrides.

## Boundaries

The definer, planner and reviewer read only. The verifier runs commands and writes evidence but never edits application code or tests. The deliverer uses Git and `gh` within the authorized endpoint and never edits product code. Every repair returns to the coordinator for the implementation worker, and no Devflow agent spawns another agent.
