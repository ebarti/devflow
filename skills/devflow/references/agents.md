# Agent roles and handoffs

The installer links five agent definitions from `agents/` into `$CODEX_HOME/agents/`. Each selects its default model and effort. The coordinator uses the user's session, delegates scoped work, records results and performs the final authorized merge.

| Task | Skill | Agent type | Configured sandbox | Default model |
| --- | --- | --- | --- | --- |
| Investigate an unclear request | [defining work](../../devflow-defining-work/SKILL.md) | `devflow-definer` | read-only | `gpt-6-astra` / high |
| Design and slice a change | [planning](../../devflow-planning/SKILL.md) | `devflow-planner` | read-only | `gpt-6-astra` / xhigh |
| Implement, open PRs and push fixes | [implementing](../../devflow-implementing/SKILL.md) | `devflow-implementer` | workspace-write | `gpt-5.6-sol` / high |
| Review code and workflow contracts | [reviewing](../../devflow-reviewing/SKILL.md) | `devflow-reviewer` | read-only | `gpt-6-astra` / xhigh |
| Exercise candidate behavior | [verifying](../../devflow-verifying/SKILL.md) | `devflow-verifier` | workspace-write, evidence only by instruction | `gpt-6-astra` / xhigh |
| Merge a PR or stack | [merging](../../devflow-merging/SKILL.md) | coordinator directly | current session | current session |

## Dispatch and reuse

Spawn by type, for example `{"agent_type": "devflow-reviewer", "fork_turns": "none"}`, plus `task_name` and a self-contained `message`. Do not pass model or effort; the definition selects them. [Coordination](../../devflow-coordinating/SKILL.md) lists each role's required brief fields. The [worker reference](implementation-worker.md) covers implementation scope, publication, reuse and spawn failures.

Continue the original agent for related work in the same role, work ID and worktree. Repairs go to the original implementer and rechecks to the original reviewer/verifier, preserving required independence. Use a new agent for unrelated work. Avoid overlapping ownership and agents created merely to fill stages.

A trusted repository can supply `.codex/agents/<name>.toml` to override a role's defaults. Record the actual model and effort from the host when available; mark them unknown if unavailable rather than claiming the requested settings were observed.

## Responsibilities

Agents return reports; the coordinator writes work records, updates the issue, publishes authorized review comments and resolves verified findings. The implementer changes code and owns commits, PR creation and pushes. The definer, planner and reviewer inspect. The verifier runs checks and writes evidence without changing product code or tests. None delegates further. Merge is a bounded coordinator action, with no separate delivery agent.

These role boundaries are instructions and configuration defaults, not guaranteed isolation or enforced I/O contracts. The host controls effective permissions. The existing hook catches common coordinator writes; it does not enforce workflow sequencing.
