Implementation design: [architecture](architecture.md), [contracts](implementation-contracts.md), and [build/cutover plan](implementation-plan.md) now own the proposed reset. The reusable package adopts JobCtrl first; model comparison remains deferred.

# Proposed autonomous work-item template

One issue owns the request and acceptance criteria. The Project organizes that issue. Code, PRs, test evidence, and task handoffs link back to it. A work item must be executable from this record and its specific evidence links without requiring the original conversation to be reread.

## Shared fields

- **Requested outcome:** preserve the user's intended result in concrete language and retain the original request or a durable link.
- **Acceptance examples:** observable success, the important failure/boundary case, and invariants that must remain true. Each must be verifiable.
- **Scope and constraints:** relevant surfaces, compatibility/data boundaries, and any explicit exclusions. Optional improvements become separate items.
- **Relevant context:** owning docs/code, current behavior, prior decisions that constrain the solution, and evidence links. Include a short summary of each decisive linked fact so a broken link does not erase the requirement.
- **Dependencies and readiness:** blockers, safe fixture/environment, needed credentials/access, and any prerequisite work. Mark a prerequisite verified only after checking it.
- **Verification:** the smallest meaningful checks and required review/QA gates. Name exact test paths when known; state the behavior to prove when investigation must locate them.
- **Authorized endpoint:** local change, published PR, merge, release, or another explicit action. Record which operations may proceed autonomously and any actual approval still needed.
- **Priority/risk/owner:** maintainer-set priority and risk, one implementation owner, and the linked task/PR once assigned. Keep private task metadata in the private ledger if appropriate.

## Additional bug fields

Record observed versus expected behavior, reproduction steps or a bounded failing fixture, affected version/environment, impact/severity, detection time and reporter, and the violated invariant. Label suspected causes as hypotheses. If a reproducer is unavailable, define the first investigation and the evidence needed to establish the cause; do not mark an unbounded implementation task Ready.

## Additional feature fields

Record the user problem and desired behavior with concrete examples, relevant product constraints, and the important acceptance boundary. Avoid prespecifying an implementation unless that choice is a requirement or an accepted architecture decision.

## Ready and execution rules

An authorized maintainer marks an item Ready only when an agent can begin without a consequential guess and its allowed terminal action is clear. A public issue submission or Project auto-add does not itself provide that authorization. Questions can be resolved in the issue before Ready; an investigation can be its own bounded Ready item.

At dispatch, validate the required fields and dependencies, claim one owner, and guard against duplicate execution of the same issue. Preserve scope changes as amendments. Execute within the recorded authority, continue independent work during a real blocker, and update state at claim, candidate ready, blocker, and verified completion. Reuse the original issue and PR through repair.

Every confirmed code finding becomes a distinct PR review thread, regardless of severity. Findings discovered before a PR exists stay in the issue and are published to the relevant PR during verification, including findings already repaired. A fixed finding records the fix commit and verification, then its review thread is resolved and read back. Genuine deferred findings retain an explicit open record; a reply is not resolution.

The independent review and QA tasks receive the work ID/scope version, acceptance criteria, exact candidate/base refs, existing proof, relevant constraints, their assigned responsibility, and the expected return format. They return findings with evidence and publication IDs, gate results, candidate refs, limitations, and any blocker requiring the owner. They do not create further hidden delegates by default.

The runner captures the historical workflow/configuration, actual model/effort segments, candidate refs, and gate records automatically in the private execution record. These are not additional fields the user must fill manually on every issue. Later defects link to that affected candidate, with introducing, detecting, missed-check, and fixing provenance kept separate.
