Implementation design: [architecture](architecture.md), [contracts](implementation-contracts.md), and [build/cutover plan](implementation-plan.md) now own the proposed reset. The reusable package adopts JobCtrl first; model comparison remains deferred.

# JobCtrl workflow stabilization and deferred configuration comparison

Status: assessment proposal, revised 9 September 2026. Stabilize the entire development workflow using the user's defaults first. Model comparison is deferred. No workflow changes or experiments have been started by this assessment.

## First: stabilize the workflow using the user's defaults

The immediate unit of change is the development workflow: intake and Ready items, planning, implementation, skills, task visibility and communication, verification selection, independent gates, finding publication and resolution, delivery, and deterministic measurement. Keep the user's model/effort defaults and effective role settings as the baseline throughout this work; do not introduce experimental routing or lower effort as part of the revamp. Capture the actual configuration when work starts. A user-requested settings change creates a recorded configuration segment and a distinct baseline period.

1. Establish the smallest reliable execution record and deterministic usage collection through `npx ccusage@latest`. Capture the current workflow/configuration before changing it, reusing sufficiently complete recent evidence where possible. Ordinary needed work supplies the baseline; a benchmark campaign is unnecessary at this stage.
2. Apply the workflow changes as reviewable increments. Consolidate conflicting instructions and skills, shorten the QA router, make work items self-contained, provide visible tasks and useful handoffs, reuse applicable evidence, and carry every confirmed code finding through its PR review-comment lifecycle. Preserve the existing independent review/QA gates and zero-known-Blocker/High shipping rule.
3. Exercise the adopted workflow on actual needed work. Include the paths the workflow claims to support, such as a feature, a bug fix, a review repair, and a delivery/merge-only request; include a dependent stack or recovery path when those capabilities change. Use focused fixtures for a changed safety/recovery boundary when ordinary work has not exercised it. Do not manufacture extra feature work just to reach a sample count.
4. Fix workflow failures using the same defaults. Record which workflow version each outcome used, then freeze a candidate version for observation. A subsequent material workflow change creates a new observation segment; do not pool changing instructions as one stable treatment.

### Conditions for moving to model comparison

Use demonstrated behavior as the exit criterion, rather than declaring stability after an arbitrary number of days:

- A Ready item contains enough scope, acceptance, context, prerequisites, and authority to run without the user repeatedly reconstructing the request.
- Implementation, review, and QA responsibilities are inspectable, their handoffs preserve the exact candidate and evidence, and the relevant resumption/blocker paths work.
- Verification selects checks that prove the changed behavior; required independent gates pass. Known workflow or harness failures that invalidate a gate or require repeated avoidable intervention are resolved.
- Confirmed code issues have PR review threads; verified fixes have fixing evidence and resolved threads, checked by readback. Required publication and closure coverage are complete.
- The requested delivery endpoint is independently confirmed, with no known Blocker/High defect shipped. Zero user-found bugs remains the goal; user discoveries at review and after exposure remain attributable and feed repair.
- Tokens, cost, lead time, interventions, and defects can be joined to the actual workflow/configuration and candidate with stated coverage. The chosen representative paths have completed under the frozen workflow version, and observation limits remain visible.

This establishes operational readiness to compare configurations. It does not prove an absence of rare product defects. Review cost, delivery time, user corrections, and observed defect trends before advancing; a green checklist alone is insufficient. Leave the model experiment deferred while material workflow changes remain in flight.

## Later: run both configurations on the same features

The comparison unit is one frozen feature or bug-fix case executed twice, once per model/effort configuration. Both attempts use the stabilized workflow. Different backlog items, even if matched by estimated difficulty, do not satisfy this same-task comparison.

For each pair:

| Hold constant | Change or record |
| --- | --- |
| Exact starting commit, request, acceptance criteria, supplied context and fixtures | The selected model/effort configuration for the role being compared |
| Workflow/skill versions, tools, permissions, runtime and dependencies | Actual per-segment model, effort, speed and any deviation |
| Independent reviewer/QA configurations, rubric and required checks | Candidate findings, gate results, repair attempts and final acceptance |
| Allowed repair procedure and predeclared budget policy | Complete tokens/cost, time, interventions and completion status |

Use isolated worktrees and fresh task contexts. Neither implementation receives the other's code, findings, fixes, or completed solution; historical cases exclude the solution from both inputs. Review/QA receive each candidate in separate contexts, with model labels hidden where practical. Randomize execution order, avoid shared-runtime interference, and record cache/environment differences. Any new acceptance requirement discovered during evaluation applies consistently to both candidates and is recorded as a protocol amendment.

Begin with two configurations and a small, preselected set of representative cases. Choose the configurations after stabilization using the question that remains relevant then. The user's Astra/low versus Sol/high hypothesis remains a candidate question; no challenger is selected by this document. An owner-role comparison holds reviewer/QA settings fixed. Changing several role models at once would test that complete configuration package, with a correspondingly broader interpretation.

### Bound the duplicate work explicitly

Before starting, record the case set, both configurations, endpoint, common quality criteria, per-run and total token/credit allowances, API-equivalent USD estimates, stop behavior, and the decision the sample can support. Include both implementations, independent verification, retries, repairs, setup, and analysis. The second implementation is an explicit experiment cost. Do not carry forward the previous different-item pilot's zero-duplication budget, 12-item allocation, or 5% overhead assumption.

Use costs observed during stabilization to choose an affordable initial case set. Expand only if the first paired results leave a useful unanswered question and a new allowance is adopted. No automatic third arm, repeat campaign, or default change. `npx ccusage@latest` supplies deterministic accounting; record the resolved version and verified pricing path. It does not enforce spending by itself. Budget admission and supported stop behavior must be demonstrated, with an allowance for collection lag and in-flight work. Preserve stopped candidates and count failures rather than buying unlimited repairs until every run passes.

Compare acceptance and finding severity first, then total cost and elapsed time to the same verified endpoint. Include failed and incomplete runs; report each pair so averages cannot hide an acceptance failure. A small sample is an initial screen, not proof of equivalent rare-defect risk or a universal best model. Do not select a winner just because one used fewer tokens.

Only a candidate selected for normal delivery is exposed to users; duplicate attempts are experimental runs, not two delivered product outcomes. Normal review/finding rules and existing delivery authority still apply. Later user defects belong to the actually exposed candidate. The unexposed alternative has no equivalent production observation window, so production defect rates cannot establish a paired quality result for it.

## Provenance throughout both stages

Record work/scope IDs, workflow and model-policy versions, actual execution segments, candidate refs, gate evidence, and accounting versions from stabilization onward. Add experiment, case, pair and arm IDs only for the later comparison. Preserve each attempt's assigned arm and include its retries or rescue work; a fallback does not turn a failed challenger into a successful control run.

Defects link to the affected candidate and introducing work, with separate introducing, detecting, missed-gate, and fixing provenance. This supports diagnosis without claiming that one model alone caused an individual bug. See the [metrics contract](metrics-contract.md) for definitions and historical attribution.
