---
name: devflow
description: Use Devflow for a development request, role selection, local work records, or workflow metrics.
---

# Devflow

Follow the user's requested outcome and the target project's instructions. Start authorized work without an extra approval gate. Discussion alone creates no work; loading a skill creates no issues, agents or backlog activity. Use the host's tools, Git, `gh` and project commands.

The main Astra task inspects, asks the user questions and plans. For implementation, hand the inspected plan to one Sol/high execution coordinator; it spawns the implementer (Sol/xhigh), reviewer (Sol/xhigh) and verifier (Sol/xhigh), handles repair loops and finishes at the authorized endpoint. Use the installed [agent types](references/agents.md) and [handoff procedure](../devflow-coordinating/SKILL.md). The main task handles material design decisions; routine execution stays with the coordinator.

Select only the role needed:

| Intent | Skill | Runs in |
| --- | --- | --- |
| Clarify a request or explore a design | [Defining work](../devflow-defining-work/SKILL.md) | Main task |
| Plan architecture, slices or coverage | [Planning](../devflow-planning/SKILL.md) | Main task |
| Carry out a defined request or continue actual work | [Coordinating](../devflow-coordinating/SKILL.md) | `devflow-coordinator` |
| Change or repair code | [Coordinating](../devflow-coordinating/SKILL.md); its worker uses [Implementing](../devflow-implementing/SKILL.md) | Coordinator → `devflow-implementer` |
| Review an existing candidate | [Reviewing](../devflow-reviewing/SKILL.md) | `devflow-reviewer` |
| Reproduce behavior or run project checks | [Verifying](../devflow-verifying/SKILL.md) | `devflow-verifier` |
| Commit, publish or update a PR/stack | [Implementing](../devflow-implementing/SKILL.md) | Coordinator → `devflow-implementer` |
| Merge a PR or stack | [Merging](../devflow-merging/SKILL.md) | Execution coordinator, or main task for a merge-only request |

## Steps

1. **Pick one role** from the table for the request as stated; enter at the role that fits, not at the top of a pipeline.

2. **Inspect and plan before execution.** Do both in the main task, keeping the plan proportional. Resolve material user decisions directly. Pass acceptance conditions, inspection evidence, design rationale, exact checks and explicit limits to the execution coordinator. A standalone review or verification request can go directly to its leaf worker with that scoped brief; a plan-only request stops with the plan.

3. **Retain work and ownership.** Use [the state helper](references/state.md) for substantive work and [issue ownership](references/ownership.md) for implementation or merging. The main task creates or reuses the record and claim; the execution coordinator maintains records, assignee and existing Project Status for that owner while executing. Audit linked issues before resume and closeout, including stopped owners and tracked external runs. Leaves report results without claiming again or writing records. Reuse work IDs for continuation. Record usage with provenance and leave unavailable values unknown. Ordinary questions need no bookkeeping.

4. **Install only on request.** Run `bash scripts/install.sh [SKILLS_DIRECTORY] [CODEX_HOME]` from a release checkout, and upgrade with `bash scripts/update.sh TAG [SKILLS_DIRECTORY] [CODEX_HOME]`; keep development trials separate. Installation links sibling skills, copies regular agent definitions, removes obsolete owned links and unchanged agent copies, refreshes metrics hooks and installs the deterministic launchd reconciler for the default macOS home. Custom homes stage its plist without activation. Preview managed records with `reconcile.py once --dry-run`; use `reconcile.py once` manually on other hosts. Review changed hooks through the host's hook controls. Agent instructions and project tools remain with their existing owners.

## Example

"Use devflow to fix the retry bug in this repository" starts with inspection and a short plan in the main task. It then claims the work and dispatches the execution coordinator, which runs implementation, early PR publication and the required review/verification and repairs. Merge only when requested. Sequential unmerged features join the same gh stack, even without a functional dependency; see [PR workflow](references/pr-workflow.md). "How does the retry code work?" is a question: answer it without creating work records, issues or agents.
