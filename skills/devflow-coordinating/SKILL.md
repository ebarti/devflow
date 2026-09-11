---
name: devflow-coordinating
description: Coordinate a defined repository request, named issue, bounded backlog batch or existing attempt. Own admission, durable stage handoffs, native subagent startup and recovery.
---

# Coordinate the outcome

Input: authorized scope, acceptance/check plan, endpoint and existing work ID. Output: durable assignments, imported producer results and the next stage. The original user conversation owns coordination.

Run the pinned launcher and `doctor`. For continuation, recover through `backlog list`/`work list`, then `work show` and `next`; reuse work, attempt, candidate, action and role identities. Read the skill named by each action. Do not start another work item to escape an unresolved operation.

If definition/check coverage is missing, hand off to [defining-work](../devflow-defining-work/SKILL.md) or [planning](../devflow-planning/SKILL.md). Admit the conversational request and snapshot through the [intake procedure](../devflow-defining-work/references/intake.md). Preserve the actual entry phase for review-only/delivery-only work.

Delegate implementation/repairs to a bounded worker and required review/QA to distinct independent agents. Read [host protocol](references/host-protocol.md) before launch, activation, replacement or native reconciliation. Journal dispatch, verify actual startup identity/settings, then activate the role skill and self-contained brief. Reuse available roles; preserve others' edits and isolate fixtures.

The worker returns a captured candidate and producer result. Persist `host result` before recorded `check run` or independent gates. Preliminary implementation probes do not replace candidate-bound registered verification.

For every activated review/QA round, import its actual PASS, FAIL or BLOCKED and findings before repair, candidate replacement or rerun. Bind the result to activation `assignment_action_id`; do not overwrite it when reusing an assignment. Save the producer’s bare gate-result JSON as an artifact; import exactly its parsed fields plus `producer_result_artifact_hash` (no hash inside the original JSON). Import independent fix observations before evaluating its gate. Completion messages notify after durable integration. Late results remain historical and cannot validate the current candidate.

On failure, preserve the cause, original response and state. Collect outstanding independent results before repair. If the user asks to stop at the first failure, stop active work through supported host control, observe stopped status, record the blocker and reconcile before resuming the same attempt. Use `host resume` for an observed interrupted round with unchanged inputs; preserve its activation ID. Before an amendment changes inputs, import its actual partial/BLOCKED output. Clearing a blocker does not establish a fix.

Handoff: checks/QA → [verifying](../devflow-verifying/SKILL.md); implementation/repair → [implementing](../devflow-implementing/SKILL.md); review → [reviewing](../devflow-reviewing/SKILL.md); endpoint → [delivering](../devflow-delivering/SKILL.md). Missing capability is a durable blocker, never a substitute PASS.
