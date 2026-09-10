---
name: devflow
description: Carry out requested repository work, including bug reports, fixes, reviews, named issues, and backlog batches in repositories enrolled in devflow, or manage an explicitly requested devflow installation. Ordinary questions do not start this workflow.
---

# Devflow

Use this skill when the user requests repository work. Their conversational request, concrete bug report, selected issue, or bounded backlog batch is sufficient authorization for that scope. The agent invokes devflow and fills its records; there is no separate human-validation step. Questions and opening a conversation do not start work.

Use the repository's pinned workflow and accepted outcome. Run `devflow doctor --repository <checkout> --json` once work is requested. Prefer the repository's `scripts/devflow` launcher when present; otherwise use the installed `devflow` command. A missing profile does not authorize enrollment; follow the repository's existing workflow until adoption is authorized. Resolve incompatible pins through a reviewed compatible release.

When the user requests continuation of existing work, use `devflow backlog list --json` and `devflow work list --json` to recover IDs when needed, then read `devflow work show --work-id <id> --json` and `devflow next --work-id <id> --json`. Continue its recorded attempt. Resolve interruptions and uncertain external actions before creating replacements. The CLI records facts and missing evidence; it does not make engineering judgments for you.

The ordinary path is request → reuse/create one short backlog issue → implement → applicable checks and gates → authorized delivery. Starting immediately does not skip issue capture. Follow-ups reuse its issue. Use `devflow backlog capture`, which journals the request around the existing GitHub CLI; no separate planning cycle is required. After interruption, rerun with the saved work ID; an uncertain creation gets readback before any replacement.

For a bug, reproduce or localize the defect, trace its cause, and add meaningful regression proof before claiming it fixed. For a backlog batch, record the matching issue set and requested endpoint, reuse each existing issue, and work in dependency order. The same conversational request can cover every selected member; newly added labels do not create an indefinite queue.

Read only the reference for your responsibility:

- [Intake](references/intake.md): turn the request into a self-contained accepted work contract.
- [Implementation](references/implementation.md): build and repair the selected outcome.
- [Review](references/review.md): independent correctness and contract review.
- [QA](references/qa.md): independently demonstrate the affected behavior.
- [Delivery](references/delivery.md): publish findings, close verified fixes, and prove the authorized endpoint.

Use JSON request files with `--request-file` and the packaged record schemas. Supply stable caller record/operation IDs; reuse identical requests after uncertain responses. The CLI computes action IDs, signatures, receipts, and state. Never construct shell commands from issue text or manually calculate usage. Run `devflow usage collect` on explicitly selected session inputs; report unknown attribution or prices.

The owner is the implementer. Named review and QA tasks are visible peers. Create those tasks only when the user has explicitly requested them, including an adopted launch instruction; then use native create/send/wait/read task tools and record their real results. Do not invoke a private desktop API or substitute hidden nested agents. Reuse the same role task for related repairs. Pending setup IDs must be reconciled before messaging.

Send concise updates on assignment, changed scope/candidate, blockers, gate results, and delivery. A handoff includes work/scope/assignment IDs, candidate refs, acceptance, ownership, constraints, relevant recipes, existing evidence, and the requested result. Use cursor-aware waits. Messages notify; the ledger retains the facts.

Preserve user model defaults and explicit role overrides. Record changes as new execution segments. Workflow stabilization precedes any separately authorized model experiment. No model or effort is hardcoded by this skill.

Every confirmed code finding has a PR review-comment obligation. Keep pre-PR findings pending and visible. Preserve zero known Blocker/High at delivery and zero user-found bugs as the quality goal. An unavailable gate or launch failure is blocked, never passed. Show implemented, verified, published, and merged state accurately.
