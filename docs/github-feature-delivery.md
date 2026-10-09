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

The accepted plan and stack membership live in an issue comment with a
`devflow-delivery:v1` record. The issue's `devflow-plan-<comment-id>` label is the
direct pointer to that comment. Human issue bodies are preserved. A local plan
snapshot authorizes an execution; it is not a second editable feature definition.
Changing the GitHub plan after acceptance stops the old execution from publishing
under changed scope. A missing or conflicting binding never selects a newer PR.

All local runtimes use
`~/.local/state/devflow/execution-ownership/registry.sqlite3`. Claims have no expiry.
A stopped or disconnected process does not authorize takeover. Stop settles
workers, native resources and remote intents before releasing the generation.
The next coordinating run retains the existing stack and completed chunk evidence;
unfinished workers resume their original checkout, PR and implementation session.
A continuation must use the runtime that owns those worker artifacts. This is a
local execution system, not a cross-host artifact migration service.

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
feature with merged PRs is **Partially merged**, never **Merged**.

Feature and workstream status changes are projected through the existing
transactional outbox and independent Project synchronizer. A Project outage leaves
the mirror pending while delivery continues. External PR changes and Project
drift retain the configured **86,400-second** checks. Event-triggered updates and
merge-command readbacks do not wait for the daily check.

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
