# Devflow agents

Every delegated action runs as a Devflow agent type, spawned by name. The definitions live in `agents/` in the Devflow checkout and are linked by `install.sh` into `$CODEX_HOME/agents/`. Each definition pins the workflow's default model and effort: `gpt-6-astra` where judgment matters and Sol / high where it does not. The coordinator is the user's session, has no agent type, and runs on the thread's own model; start feature threads on `gpt-5.6-sol` at high or xhigh.

| Action | Skill | Agent type | Sandbox | Model |
| --- | --- | --- | --- | --- |
| Define the work | [defining work](../../devflow-defining-work/SKILL.md) | `devflow-definer` | read-only | `gpt-6-astra` / high |
| Plan the change | [planning](../../devflow-planning/SKILL.md) | `devflow-planner` | read-only | `gpt-6-astra` / xhigh |
| Implement the change | [implementing](../../devflow-implementing/SKILL.md) | `devflow-implementer` | workspace-write | `gpt-5.6-sol` / high |
| Review the candidate | [reviewing](../../devflow-reviewing/SKILL.md) | `devflow-reviewer` | read-only | `gpt-6-astra` / xhigh |
| Verify the outcome | [verifying](../../devflow-verifying/SKILL.md) | `devflow-verifier` | workspace-write, evidence only | `gpt-6-astra` / xhigh |
| Deliver the outcome | [delivering](../../devflow-delivering/SKILL.md) | `devflow-deliverer` | workspace-write | `gpt-5.6-sol` / high |

## Spawning

Spawn by type and nothing else, for example `{"agent_type": "devflow-reviewer", "fork_turns": "none"}` plus `task_name` and `message`. Never pass `model` or `reasoning_effort`: Codex applies an agent file's model ahead of any spawn value, and every Devflow definition pins one. The implementation worker's dispatch precondition, brief, reuse and failure handling are in the [implementation worker reference](implementation-worker.md). The same reuse rule applies to every type: continue an agent when the next task is the same or closely related work in the same role, work ID and worktree, and spawn a new one otherwise. Every brief states the work ID, the agent's limits and the model and effort the coordinator expects, and agents record their runs with the [state helper](state.md) from that brief.

## Override

A repository changes a Devflow agent only by shipping its own project-scoped `.codex/agents/<name>.toml` with the same `name`, which Codex loads for a trusted project in place of the installed definition. Conversation requests, prose in agent instruction files and user-level settings are not overrides.

## Boundaries

The coordinator reads, runs the Devflow helpers, spawns and steers agents, and talks to the user; it never edits files, commits, pushes or runs product checks as evidence, and the installed hook denies repository mutations from a claim-holding coordinator session. The definer, planner and reviewer read only. The verifier runs commands and writes evidence but never edits application code or tests. The deliverer uses Git and `gh` within the authorized endpoint and never edits product code. Every repair returns to the coordinator for the implementation worker, and no Devflow agent spawns another agent.
