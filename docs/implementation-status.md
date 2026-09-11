# Package implementation status

This page records the reusable package boundary through the 0.4.0 role-execution revision, with the initial 2026-09-10 rollout baseline retained below. The [implementation plan](implementation-plan.md) remains the target for the complete rollout. Package publication and repository adoption are separate steps; package tests alone do not establish JobCtrl cutover.

Conversational intake update 0.3.0 replaces the mandatory independent host verifier with agent-recorded user requests. Direct requests, bug reports/investigations, named issues and bounded current backlog selections can proceed without another approval channel. Immutable scope/source/operation bindings, capture/readback, old-pin protection and safe recovery remain. This is workflow consistency, not independently authenticated human approval. CLI lifecycle proof without a synthetic verifier is tracked separately from the native and protected-delivery gates below; those gates are not satisfied by admission alone. See [conversational intake](issue-trust.md).

Version 0.4.0 makes the original conversation the coordinator and delegates implementation and required independent gates to subagents. It resolves each role's settings explicitly, journals launch before native dispatch, and checks actual child identity/model/effort before product work. Historical native-thread attempts retain their original semantics. Role-policy fixtures, native startup proof, and final review/QA results have separate evidence boundaries; the implementation PR records the exact candidate and live host observations. See [role execution](operation.md#role-execution).

## Implemented package surfaces

| Surface | Implementation and evidence | Remaining deployment proof |
| --- | --- | --- |
| Work/state contracts | JSON Schema validation, pure transition rules, immutable records, optimistic revisions, SQLite claims, flushed backup/restore, pre-attempt issue capture journal and work discovery; contract, race, SIGKILL and storage-failure tests | Real adopter work must meet its accepted endpoint; physical power-cut and failed-device recovery are not claimed |
| Git ownership and checks | Stable repository identity, dedicated clean checkout registration, candidate snapshots, process-group timeout, fresh JUnit evidence, durable execution admission, replay without rerun; temporary Git, real subprocess, concurrent request and SIGKILL tests | Product-specific recipes and actual product scenarios belong to each adopter |
| Native host bridge | Original coordinator, bounded subagent assignments, explicit role-policy resolution, journaled dispatch, actual startup metadata checks, implementation results and candidate-bound gates; historical thread receipts remain supported | Actual child identity/model/effort requires a native canary; fixtures and requested launch settings cannot establish what ran |
| Findings and delivery | Candidate-bound independent results, technical fix verification before final gates, mandatory due publication/closure, PR/release/status/Project adapters and readback | Live endpoint canaries and permission/protection conformance remain separately observable gates |
| GitHub merging | Classic strict protected direct merge adapter, proof binding and actual merged-tree readback; controlled source/target race tests | No automatic merge until live enforcement is proven. Ruleset-only, queue and atomic-stack backends remain blocked |
| Usage and provenance | Per-response import, exact decimal accounting, price/unknown partitions, model segments, actual instruction-byte capture and outcome metrics; real 20.0.20 collector session/daily canary on isolated synthetic inputs | Actual user-data format and attribution coverage remain unverified; observed collector omissions stay explicit mismatches |
| Installation and skill | Clean committed source archive, immutable revision/content check, independent scope/digest, journaled apply/rollback, legacy routing function, short role references; actual interrupted installer tests | Each shared-host update needs a concrete consumer inventory and authorized manifest; consumer pins and host readback are separately verified |

## Initial plan accounting (2026-09-10)

At the initial package boundary, W01's contracts and state engine, W02's Git/native receipt interfaces, W03's ordinary GitHub endpoints, W04's deterministic ingestion/reporting and synthetic collector canary, and W06's skill/installer/router were implemented. Native host conformance, atomic-stack and live protected delivery, actual native-history coverage, and shared-host replacement were separate adoption gates. Version 0.4.0 replaces W02's new-work peer-thread design with the coordinator/subagent contract; earlier thread canaries do not prove that new path.

W05 (JobCtrl profile and harness repairs), W07 (enrollment, issues/Project and protection), W08's cumulative live cutover, and W09 stabilization were outside that initial package proof. Their current adopter evidence belongs to the corresponding work records and PRs; they are not treated as passed by package tests.

## Acceptance evidence boundaries

Local/domain/adapter tests exercise A04/A05 interruption and candidate drift, A08/A09 authority and scope, A10/A11 reconciliation/pagination, A14/A15 incomplete price and replay accounting, A16/A17 installer conflicts/rollback, A19 editorial proportionality, A20 role-policy selection, A22 technical repair ordering, and A24 subagent startup/recovery. A18 uses distinct synthetic repository profiles with real local command execution. Their assertions are evidence for the named package invariants, not blanket end-to-end passes for every production scenario in the matrix.

A01/A02/A03 still require a native task/GitHub/product workflow demonstration at the adopted boundary. A06/A07/A21 require the selected merge backend's live conformance. A12/A13 belong to JobCtrl's actual harness repair. A20 requires observing the user's host setting changes and A23 requires enrolled/non-enrolled consumers on the same host. Synthetic identities and simulated endpoints are labeled in tests and must not be recorded as real independent task results.

Independent package review and QA should report the exact candidate, reproduced findings, exercised paths and remaining limits. Gate results are retained in the implementation PR rather than hardcoded here as permanent claims about future revisions.
