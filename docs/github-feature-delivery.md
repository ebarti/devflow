# GitHub-owned feature delivery

A GitHub parent issue is the feature and owns its business scope and acceptance.
Each direct sub-issue is a sequential workstream with its own acceptance. A chunk
is a complete, independently reviewable contribution on declared prerequisites.
Each chunk becomes one PR; all feature chunks share one GitHub stack. Independent
workstreams can build concurrently, while integration and publication follow the
plan's dependency order. Workers resolve conflicts against the current stack tip
and run normal checks, independent review, QA and CI on that integrated candidate.
The final chunk also carries the whole feature's acceptance criteria.

## Data ownership

| Owner | Data |
| --- | --- |
| GitHub | Parent and sub-issues, hierarchy, scope, acceptance, accepted chunk plan, owning stack ID, exact PR membership, branches, PR heads and actual merge state |
| Shared local execution registry | One coordinator claim per GitHub issue node ID, ownership generation, worker assignments, immutable input snapshots, checkpoints, effect intents and receipts, cumulative repairs and learning flag |
| Runtime database | Immutable admitted requests, workflow IDs and revisions, original role sessions and evidence, candidate/gate receipts, durable start outbox, derived status events and cached Project readbacks |
| Independent synchronizer | Event consumption, Project updates, retry receipts and the daily PR/Project drift schedule |

Version 2 publishes a compact `devflow-delivery:v2` index on the parent and
readable, immutable detailed plan comments on each exact child issue. The parent
index records the plan revision, resolved child identities, exact child-comment
references and digests, dependencies, and stack membership. Child plans contain
scope, steps, acceptance, verification, expected files, and chunk gate selections.
The issue's `devflow-plan-<comment-id>` label points directly to its parent index.
Human issue bodies are preserved. Existing v1 records remain readable until an
explicit supported revision adopts v2. Child records publish first; the parent
pointer commits the revision last. Every referenced child comment is authenticated
against its exact repository, issue, node ID and digest before adoption.

A local immutable snapshot records what an execution may do. Expected paths are
planning hints; frozen exact-file or versioned root/file restrictions, protected
paths, and resource authority remain execution limits. A plan correction cannot
expand that authority. Unrecorded GitHub changes stop the old execution from
publishing under changed scope. A missing or conflicting binding never selects a
newer issue, comment or PR.

All local runtimes use
`~/.local/state/devflow/execution-ownership/registry.sqlite3`. Claims have no expiry.
A stopped or disconnected process does not authorize takeover. Stop settles
workers, native resources and remote intents before releasing the generation.
The next coordinating run retains the existing stack and completed chunk evidence;
unfinished workers resume their original checkout, PR and implementation session.
A continuation must use the runtime that owns those worker artifacts. This is a
local execution system, not a cross-host artifact migration service.

An interrupted GitHub write keeps its original effect identity. The coordinator
performs readback while ownership is draining, and only releases the feature when
that outcome is known. A remote outage can leave it waiting for readback; it does
not authorize another creation or consume a product repair cycle.
The durable local status and Project projection show **Waiting for GitHub** during
that wait. A cancellation still settles the original operation before handoff.

## Controls and states

Enable new admissions with `feature_delivery_version: 1` after custody migration.
`max_repairs` defaults to 10 and is frozen for the feature across continuations.
At five product repair cycles `learning_required` becomes true and remains true
after success. Known Devflow defects can set that passive flag without adding a
product repair. The retrospective itself is separate work. Existing finite native
provider retries remain separate; no additional infrastructure-repair controller
is introduced.

The dashboard shows the GitHub plan, workers, complete chunks, repair use and
learning flag. **Continue this feature** submits
`POST /api/runs/{id}/continue-feature` with `command_id` and the current
`projection_revision` as `expected_revision`. The CLI equivalent is
`devflow-delivery continue-feature --id … --request …`; MCP exposes
`continue_feature`. The response identifies the successor coordinator. Retrying
the exact command returns the same result.

A stopped feature can request correction of a demonstrated planning defect with
`POST /api/runs/{id}/revise-feature-plan`, CLI `revise-feature-plan`, or MCP
`revise_feature_plan`. Supply a stable `command_id`, `expected_revision` from the
run's `feature_plan.expected_revision` (the local projection revision), and a
bounded `reason` of 1–4000 characters. Optional `evidence` lists at most 16 existing
owned `{path, sha256}` artifact references. Callers cannot supply a replacement
plan, command, policy, or authority override. An identical command returns its
original receipt; a reused ID with different input, a stale revision, or unsafe
custody is rejected before external mutations.

The configured intake role investigates sealed evidence and proposes the smallest
correction; independent review checks it against the original outcome and
acceptance. Replanning covers flawed assumptions, decomposition, dependencies or
gate placement. Automatic revision requires a structured native planning diagnostic
bound to the current plan, candidate and sealed evidence; generic findings retain
normal implementation repair. A reviewed proposal may split future work within an
existing child by adding chunks while retaining existing IDs and acceptance. Started
chunk dependencies and the published stack prefix remain fixed. A correction
shares the cumulative ten-cycle allowance, including bounded rejected attempts;
an already charged product cycle is not charged again. Adoption waits for known
worker/resource closure, settled external effects and exact GitHub readback.
Historical inputs, sessions and evidence remain immutable. Affected chunks and
transitive dependents are requalified under the new revision; unaffected completed
work, native sessions, child identities and the existing stack are retained. Human
plan review remains required when the original policy requests it. Broader outcome
or authority changes are outside autonomous revision and require new authorization.
Ordinary continuation preserves the adopted plan.

The run's `feature_plan` reports the adopted GitHub revision and digests, child
plan links, correction phase and reason, evidence, affected chunks, frozen source
authority, expected files, repair use and projected action eligibility. Eligibility
comes from local custody; the command rechecks native, Temporal and GitHub state.
Automatic assessment runs inside the owned workflow; it adds no external watcher.
These readbacks describe recorded evidence, not a new remote polling service.

After all chunks pass, the coordinator remains durably **Awaiting merge**.
**Merge this feature** answers its exact pending merge decision. MCP exposes
`merge_feature`; the existing decision API/CLI carries the same authority. The
request includes `command_id`, `expected_revision` (the protocol revision),
`decision_id`, `decision_revision`, `candidate_revision` and `answer: "merge"`.
An explicitly admitted `authorized_endpoint: "merged"` authorizes that transition
up front; ordinary publication does not.

The merge boundary rereads the recorded stack and PR IDs, verifies the exact
native evidence and required GitHub checks, and requires strict trunk protection
without bypass actors. It uses `gh stack merge <number> --yes --squash` once for a
stack, or a head-bound `gh pr merge` for one chunk. Unknown submission results are
read back without blind replay. A changed head or target requiring new integration
cannot reuse previous proof. Only confirmed merges whose trees match the verified
chunks and are incorporated into the target close the issues. An incomplete
feature with merged PRs is **Partially merged**, never **Merged**. Each child is
**Merged** only when every required chunk has a confirmed merge observation.
Closure intents identify that child's required chunks and exact merge receipts.

Every published PR records its chunk, workstream, exact child issue and parent,
and remains in the same native stack. Its body uses parent and child `Refs`
links. GitHub's native development links and closing keywords automatically close
linked issues on default-branch merge, so they cannot safely represent an early
chunk of a child with several required chunks. The runtime closes the child after
all its required chunks merge and closes the parent after the whole accepted
feature merges. See [GitHub's documented linking semantics](https://docs.github.com/en/issues/tracking-your-work-with-issues/using-issues/linking-a-pull-request-to-an-issue).

If the target advances, the coordinator creates a new integration pass and
rechecks every layer in dependency order. Unique local branches preserve earlier
worker artifacts. Each new candidate incorporates its existing PR head and the
updated preceding layer, then advances the same remote branch without a force
push. The stack ID and PR numbers remain unchanged. Earlier verification remains
immutable and only the new pass qualifies for merging. This consumes one repair
cycle for the integration pass, plus any product repairs its gates require. A
changed publication receives a fresh merge decision unless the admitted endpoint
already authorizes merging. Issue identities and parent links are checked again
before merge and closure. Confirmed PR observations update locally immediately.
An imported complete chunk may pass implementation without further edits when it
matches its authenticated preparation receipt; every normal verification gate
still runs. Stopped integration workers retain both their private local branch
and the exact original PR binding when they resume.

Feature and workstream status changes are projected through the existing
transactional outbox and independent Project synchronizer. A Project outage leaves
the mirror pending while delivery continues. External PR changes and Project
drift retain the configured **86,400-second** checks. Event-triggered updates and
merge-command readbacks do not wait for the daily check.

After an authorized external merge or closure, request immediate reconciliation
without changing that schedule or rewriting a historical execution receipt:

```sh
devflow-project-sync --config /absolute/path/to/synchronizer.json \
  --refresh-pr https://github.com/owner/repository/pull/123
```

Repeat `--refresh-pr` for additional recorded PRs. The command accepts only PRs
already bound to the configured deliveries. It durably queues a readback and
wakes the independent service; `queued` is not confirmation of remote state or
Project synchronization. If the service is unavailable, its next startup consumes
the request. Read the run's feature status and mirror receipt to confirm completion.
Each new feature revision requires a fresh Project readback, even when the desired
status is unchanged. For example, refresh after closing a merged issue to reconcile
a Project automation that changed its status to `Done`. Repeated consumer ticks
without a new revision retain the existing receipt and daily drift deadline.
It does not reopen execution, merge a PR, or change the product repair allowance.

Historical runs can include an unpublished retry that failed before feature work,
while an earlier run owns the issue's actual PR. Bind tracking to that exact known
publisher explicitly:

```sh
devflow-project-sync --config /absolute/path/to/synchronizer.json \
  --bind-legacy-run run-known-publication
```

This bridge requires an unambiguous stopped legacy publisher and a stopped,
unpublished selected attempt for the same issue in the same configured store.
All executions for that issue must be stopped legacy runs. It records a reference
to the exact publication receipt and triggers asynchronous readback. The feature
view names both the selected attempt and `legacy_publication_binding.run_id`;
original run inputs, outcomes, PR receipts and repair counts remain unchanged.
It neither searches GitHub for another PR nor grants continuation or native-stack
adoption authority. A resumed execution, changed publication receipt, or later
attempt invalidates use of the bridge. Native feature custody takes precedence.

## Upgrade and historical custody

Stop admission and establish that all participating runtime workflows are idle.
Back up their databases using SQLite backup, retain frozen configuration files,
and install the reviewed runtime through the supported installer. Import custody
from every participating runtime before enabling feature admissions:

```sh
python -m devflow_temporal.delivery_feature_migration \
  --config /absolute/path/to/first-config.json \
  --config /absolute/path/to/second-config.json
```

Use versioned configuration overlays for `feature_delivery_version: 1` and restart
the services through their supported controls. The importer refuses active legacy
runs, preserves all original execution records, and fences every issue that still
has a historical publication receipt. It does not pick a PR, merge competing PRs,
or reinterpret old whole-feature PRs as stack layers. Those cases require explicit
adoption after the owner resolves their publication identity.

Local verification uses SQLite, Git, transport stubs, protocol tests and DOM tests.
Normal hosted CI runs the native, Temporal, browser and process qualification.
Upgrade readback checks installed payloads, service ownership, feature policy,
Project consumer health and the unchanged daily schedule without starting product
workflows.
