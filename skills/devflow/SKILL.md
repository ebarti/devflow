---
name: devflow
description: Use Devflow for a development request, role selection, local work records, or workflow metrics.
---

# Devflow

Follow the user's requested outcome and the target project's instructions. Start authorized work without an extra approval gate. Discussion alone creates no work; loading a skill creates no issues, agents or backlog activity. Use the host's tools, Git, `gh` and project commands. Every delegated action runs as its [Devflow agent type](references/agents.md), spawned by name, and each definition pins the workflow's default model and effort; implementation runs in the [implementation worker](references/implementation-worker.md). The coordinator is the user's session and only runs the workflow.

Select only the role needed:

| Intent | Skill | Delegated as |
| --- | --- | --- |
| Clarify a request or explore a design | [Defining work](../devflow-defining-work/SKILL.md) | `devflow-definer` |
| Plan architecture, slices or coverage | [Planning](../devflow-planning/SKILL.md) | `devflow-planner` |
| Carry out a defined request or continue actual work | [Coordinating](../devflow-coordinating/SKILL.md) | the session itself |
| Change or repair code | [Coordinating](../devflow-coordinating/SKILL.md); the dispatched worker uses [Implementing](../devflow-implementing/SKILL.md) | `devflow-implementer` |
| Review an existing candidate | [Reviewing](../devflow-reviewing/SKILL.md) | `devflow-reviewer` |
| Reproduce behavior or run project checks | [Verifying](../devflow-verifying/SKILL.md) | `devflow-verifier` |
| Publish, merge or reconcile delivery | [Delivering](../devflow-delivering/SKILL.md) | `devflow-deliverer` |

## Steps

1. **Pick one role** from the table for the request as stated; enter at the role that fits, not at the top of a pipeline.

2. **Keep a record for substantive work.** Use [the state helper](references/state.md) to retain a short outcome, relevant runs, results and findings, and useful recovery context. The script only records and queries data. Reuse the work ID for repairs and continuation. Record known usage or estimates with their provenance; leave unavailable values unknown. Do not create bookkeeping for ordinary questions.

3. **Own the issue for implementation or delivery.** Follow [issue ownership](references/ownership.md): the coordinating task creates or reuses the issue, claims the work and maintains its assignee and existing Project Status. Independent requested issues can run concurrently with separate work IDs and worktrees. Delegated workers reuse their coordinator's claim.

4. **Install only on request.** Run `bash scripts/install.sh [SKILLS_DIRECTORY] [CODEX_HOME]` from a release checkout, and upgrade with `bash scripts/update.sh TAG [SKILLS_DIRECTORY] [CODEX_HOME]`; keep development trials separate. Installation links sibling skills and agent definitions, removes obsolete links owned by that checkout and refreshes metrics hooks. Review changed hooks through the host's hook controls. Agent instructions and project tools remain with their existing owners.

## Example

"Use devflow to fix the retry bug in this repository" is a defined implementation request: coordinating claims the issue, dispatches the implementation worker, verifies the fix against the reported behavior and delivers within the requested scope. "How does the retry code work?" is a question: answer it without creating work records, issues or agents.
