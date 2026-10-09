# GitHub-owned feature delivery

GitHub parent issues define features; sub-issues define sequential workstreams.
Complete chunks are published as layers of one feature-owned GitHub stack. One
coordinating run owns the feature, with parallel worker attempts where dependencies
allow them. A continuation retains the existing stack, work, evidence, and budget.

## Required invariants

- GitHub owns feature identity, issue hierarchy, accepted delivery plan and stack
  association. Local records are execution snapshots, references, and journals.
- The shared execution registry keys ownership by GitHub issue identity. At most
  one coordinator can own that identity across runtime instances.
- Known stack and PR identities are read directly. Missing or conflicting bindings
  block recovery instead of creating replacement publications.
- Workers own isolated workspaces. The broker serializes canonical stack effects
  and rejects stale ownership generations and changed candidate revisions.
- Each chunk is complete on its declared prerequisites, passes its checks and
  independent review, and contributes one PR. Integrated feature acceptance is
  required before the feature is ready to merge.
- Stop, cancellation, crashes and handoff preserve checkpoints and reconcile
  outstanding effects before transferring ownership.
- Product repair accounting is cumulative across continuations, with a configurable
  default of ten cycles. A passive learning flag does not perform a retrospective.
- Merge requires explicit authority and current candidate evidence. The GitHub
  stack is merged through gh-stack; actual PR state is read back before completion.
- Project updates remain event driven through the independent synchronizer.
  External PR and Project drift reconciliation remains on a 24-hour schedule.
- Historical specifications, outcomes and receipts remain immutable. Legacy
  publication ambiguity must be resolved explicitly, never by selecting the newest.

## Implementation sequence

1. GitHub delivery contract, explicit plan/publication references, and shared
   execution registry with recovery-safe ownership and publication fencing.
2. Feature orchestration, dependency scheduling, chunk execution, integration,
   continuation, cumulative repair accounting, and authorized stack merge.
3. Public API, CLI, MCP and dashboard controls; execution-based Project projection;
   compatibility, migration, installation and documentation.

## Verification and activation

Use pure database, Git, CLI-stub, protocol and dashboard checks locally. Use normal
hosted CI for native/Temporal/browser/process qualification. Obtain independent
review of the final candidate. Activate only after fresh runtime-idle evidence,
supported service stops, backups, installation, restart and independent readback.
Do not launch product deliveries or merge existing product PRs for qualification.
