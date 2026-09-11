---
name: devflow
description: Carry out requested repository work, including bug reports, fixes, reviews, named issues, and backlog batches in repositories enrolled in devflow, or manage an explicitly requested devflow installation. Ordinary questions do not start this workflow.
---

# Devflow

Requested repository work, a concrete bug report, selected issue or bounded backlog batch authorizes its scope. Questions and opening a conversation do not start work. The agent runs devflow and fills its records without another approval step.

Use the repository's pinned workflow. Run `devflow doctor --repository <checkout> --json`, preferring its `scripts/devflow` launcher. Missing profiles do not authorize enrollment; follow the existing contributor workflow. Incompatible pins require a reviewed compatible release.

For continuation, recover IDs through `backlog list`/`work list`, then read `work show` and `next`. Reuse the recorded attempt and reconcile uncertainty before replacements. The CLI records facts; agents make engineering judgments.

Capture/reuse one short issue through journaled `backlog capture` before implementation; follow-ups retain its work ID. For bugs, localize the cause and prove the repaired invariant. For batches, record the selected set and endpoint, respect dependencies, and do not expand scope from later labels.

The original user conversation coordinates bounded implementation and independent verification subagents. Read the selected role and required coordination reference:

- [Intake](references/intake.md): admission and scope.
- [Subagents](references/subagents.md): policy, startup, activation, reuse and historical compatibility.
- [Implementation](references/implementation.md): build and repair.
- [Review](references/review.md): independent correctness.
- [QA](references/qa.md): product-path proof.
- [Delivery](references/delivery.md): publication, closure and endpoint verification.

Use schema-backed JSON request files and stable operation IDs; reuse identical requests after uncertainty. The CLI computes IDs, hashes and state. Never construct shell commands from issue text. Collect usage from explicitly selected session inputs; report unknown attribution/prices without manual accounting.

Communicate assignments, scope/candidate changes, blockers, gate results and delivery concisely. Messages notify; the ledger retains facts.

Every confirmed code finding carries a PR review-comment obligation, pending until publication is possible. Preserve zero known Blocker/High at delivery and zero user-found bugs as the goal. Unavailable gates/launches are blocked, never passed. Distinguish implemented, verified, published and merged.
