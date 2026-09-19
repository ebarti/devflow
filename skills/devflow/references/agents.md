# Agent roles and handoffs

Run the main task on **Astra/xhigh** for inspection, user questions, design and planning. A skill cannot change an existing task's model; select it in the host. Candidate trials configure that default. The installer leaves normal host model settings alone.

The installer links four definitions from `agents/` into `$CODEX_HOME/agents/`:

| Responsibility | Agent type | Default model | Permission configuration |
| --- | --- | --- | --- |
| Execute the plan, maintain records and tracker, manage workers and authorized merges | `devflow-coordinator` | `gpt-5.6-sol` / high | Inherit the main task's permissions |
| Implement, open PRs and push fixes | `devflow-implementer` | `gpt-5.6-sol` / high | workspace-write |
| Review code and workflow contracts | `devflow-reviewer` | `gpt-5.6-sol` / xhigh | read-only |
| Exercise candidate behavior | `devflow-verifier` | `gpt-5.6-sol` / xhigh | workspace-write; evidence only by instruction |

## One execution handoff

The main task performs [inspection](../../devflow-defining-work/SKILL.md) and [planning](../../devflow-planning/SKILL.md) itself. It supplies the accepted plan and evidence to one execution coordinator for the bounded request or batch. The coordinator creates the leaf workers, runs repair/recheck loops and returns a consolidated result. Only a missing decision, material design/scope change or an unresolved blocker returns to the main task. There are no definer or planner agents.

The [coordinating skill](../../devflow-coordinating/SKILL.md) owns the handoff fields and execution procedure. The main task holds the issue claim; the execution coordinator acts for that owner and is the sole writer of work records and tracker updates while running. It uses the supplied shared database and inherits existing permissions, without broadening them. Leaf workers return reports and do not write those records.

## Dispatch and reuse

Spawn by type, for example `{"agent_type": "devflow-coordinator", "fork_turns": "none"}`, plus `task_name` and a self-contained `message`. Do not pass model or effort; the definition selects them. Full-history forks inherit the parent's model, so use the explicit plan/evidence brief instead. The coordinator spawns only implementer, reviewer and verifier; those leaves never spawn another agent. This hierarchy needs native nested-agent tools and enough capacity for the main task, coordinator and at least one leaf. Serialize roles within the host's limits.

Continue the original agent for related work in the same role, work ID and worktree. Repairs go to the original implementer and rechecks to the original reviewer/verifier, preserving required independence. Reuse the coordinator for continuation of the same assignment. Use a new agent for unrelated work. The [worker reference](implementation-worker.md) covers implementation scope and failure handling.

A trusted repository can supply `.codex/agents/<name>.toml` to override a delegated role. Record actual model and effort when the host exposes them; otherwise leave them unknown. Configured defaults do not prove what ran.

## Questions and direct requests

The main task owns the user conversation. Leaves send questions to their coordinator; it answers from the accepted plan or escalates a material decision to the main task with `agents.send_message`. Only the main task asks the user. Continue independent work; return partial findings and the blocker when no independent work remains. Send answers to the same running agent or resume it with `agents.followup_task` when finished.

A standalone review or verification request needs only its leaf worker, dispatched directly by the main task with an inspected brief; the main task records that report and publishes authorized comments. A bounded merge-only request runs directly in the main task. Neither needs an execution coordinator just to add a layer. If implementation is requested, use the execution coordinator.

These are instruction boundaries and model defaults, not enforced I/O contracts or sequencing. The host controls effective permissions. The existing hook catches common repository writes from both the main claim holder and execution coordinator; it does not supervise the workflow.
