# Initial package implementation status

This page records the reusable package boundary as of 2026-09-09. The [implementation plan](implementation-plan.md) remains the target for the complete rollout. Package publication and repository adoption are separate steps; no JobCtrl cutover is implied.

## Implemented package surfaces

| Surface | Implementation and evidence | Remaining deployment proof |
| --- | --- | --- |
| Work/state contracts | JSON Schema validation, pure transition rules, immutable records, optimistic revisions, SQLite claims, backup/restore; contract, race and recovery tests | Real adopter work must meet its accepted endpoint |
| Git ownership and checks | Stable repository identity, dedicated clean checkout registration, candidate snapshots, process-group timeout, fresh JUnit evidence, durable execution admission, replay without rerun; temporary Git, real subprocess, concurrent request and SIGKILL tests | Product-specific recipes and actual product scenarios belong to each adopter |
| Native host bridge | Structured assignments, pending/final task IDs, correlation markers, result identity and cursor-aware wait inputs; adapter tests | Actual visible owner/review/QA task canary requires native launch authorization; no desktop visibility claim from fixtures |
| Findings and delivery | Candidate-bound independent results, technical fix verification before final gates, mandatory due publication/closure, PR/release/status/Project adapters and readback | Live endpoint canaries and permission/protection conformance remain separately observable gates |
| GitHub merging | Classic strict protected direct merge adapter, proof binding and actual merged-tree readback; controlled source/target race tests | No automatic merge until live enforcement is proven. Ruleset-only, queue and atomic-stack backends remain blocked |
| Usage and provenance | Per-response import, exact decimal accounting, price/unknown partitions, model segments, actual instruction-byte capture and outcome metrics | Real installed collector-format and attribution coverage must be established on explicitly selected native inputs |
| Installation and skill | Clean committed source archive, immutable revision/content check, independent scope/digest, journaled apply/rollback, legacy routing function, short role references; actual interrupted installer tests | Shared-host router cutover needs consumer inventory and adopter authorization; existing global entries are unchanged |

## Plan accounting

W01's package contracts and state engine are implemented. W02's Git and native receipt interfaces are implemented; native task visibility remains unverified. W03's ordinary GitHub endpoints are implemented, while atomic-stack and live protected-delivery conformance remain open. W04's deterministic package ingestion/reporting is implemented; installed native source compatibility needs its separate canary. W06's skill/installer/routing components are implemented; shared-host replacement is pending.

W05 (JobCtrl profile and harness repairs), W07 (enrollment, issues/Project and protection), W08's cumulative live cutover, and W09 stabilization have not been performed. Those rows remain in the plan and are not treated as passed by package tests.

## Acceptance evidence boundaries

Local/domain/adapter tests exercise A04/A05 interruption and candidate drift, A08/A09 authority and scope, A10/A11 reconciliation/pagination, A14/A15 incomplete price and replay accounting, A16/A17 installer conflicts/rollback, A19 editorial proportionality, and A22 technical repair ordering. A18 uses distinct synthetic repository profiles with real local command execution. Their assertions are evidence for the named package invariants, not blanket end-to-end passes for every production scenario in the matrix.

A01/A02/A03 still require a native task/GitHub/product workflow demonstration at the adopted boundary. A06/A07/A21 require the selected merge backend's live conformance. A12/A13 belong to JobCtrl's actual harness repair. A20 requires observing the user's host setting changes and A23 requires enrolled/non-enrolled consumers on the same host. Synthetic identities and simulated endpoints are labeled in tests and must not be recorded as real independent task results.

Independent package review and QA should report the exact candidate, reproduced findings, exercised paths and remaining limits. Gate results are retained in the implementation PR rather than hardcoded here as permanent claims about future revisions.
