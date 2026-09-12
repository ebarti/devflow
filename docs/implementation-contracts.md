# Development workflow implementation contracts

Historical design baseline: 2026-09-09. This document retains the original interface proposals with subsequent contract revisions, including the 0.3.0 conversational request contract and the 2026-09-11 coordinator/subagent contract. Those revisions supersede the mandatory independent verifier and owner-as-implementer/visible-peer launch rules for new work. See [operation](operation.md) and [implementation status](implementation-status.md) for shipped commands and unavailable host capabilities. Version every persisted record with `schema_version`; reject unknown major versions. Example records and JSON Schema accompany this document under `design/`.

[Record schemas](design/contracts.schema.json) · [Synthetic work contract](design/work-contract.example.json) · [Synthetic gate result](design/gate-result.example.json) · [Synthetic receipt](design/action-receipt.example.json)

## 1. Package and repository interfaces

The reusable package exposes a CLI with JSON input/output and readable output by default. Mutating commands accept `--request-file` for structured input; avoid interpolating issue text, paths or comment bodies into shell commands. Attempt mutations contain an `operation_id`, `work_id` and `expected_revision`. Pre-attempt backlog capture instead uses the stable work ID plus its initial issue request; the tool fills its append-only sequence. Success returns the new revision, durable IDs, pending external actions and observed receipts. An identical operation ID with identical payload returns the recorded result; reuse with a different payload is an error.

The initial commands are:

| Command | Required input | Result / state effect |
| --- | --- | --- |
| `devflow doctor` | Repository path, installed package and host capability report | Checks tool versions, profile, bindings, private store, permissions and required capabilities; no mutations |
| `devflow backlog capture/show/list/retry` | Stable work ID; first capture also needs existing issue number or sanitized title/body | Journals before gh creation; discovers saved requests; reconciles uncertainty; explicit retry only after proven non-mutation |
| `devflow work list` | Repository | Discovers recorded work IDs and lifecycle without the previous conversation |
| `devflow work prepare` | Proposed work contract | Reports schema errors and missing execution-source lineage/aggregate digest using the admission prerequisite; returns the unchanged valid record, creates no state/authority and never grants Ready |
| `devflow work ready` | Accepted contract with consumed source lineage and `user_request` (`reference`, `summary`, `allowed_operations`) | Derives immutable request admission/authority and verifies prerequisites, stores scope and marks Ready; missing request blocks admission |
| `devflow work start` | Ready work ID, host/original coordinator identity, expected revision and actual entry phase | Claims one attempt, defaults new execution to subagents, captures effective policy/configuration and schedules workspace/assignment actions |
| `devflow work amend` | New scope plus `user_request` recording the authorized amendment; current `workflow_snapshot` when an active attempt's pin/profile changed | Validates the snapshot, preserves historical records, appends scope revision and invalidates affected acceptance/proof and prepared actions |
| `devflow work reopen` | Verified completed PR, same work/issue/PR, fresh `user_request`, bounded contract and continuation entry/owner/ref observations; explicit current snapshot when changed | Appends continuation history and reacquires the claim; repair invalidates affected current proof, exact-input delivery preserves valid proof; no replacement PR or automatic merge authority |
| `devflow snapshot capture` | Effective workflow/profile/settings; optional `continuation_work_id` for an explicitly reviewed completed-PR upgrade | Captures actual inputs; the continuation exception retains the owned checkout's profile and binds its older pin and prior delivery for consumption only by `work reopen` |
| `devflow next` | Work/attempt ID | Reads current state and returns the next required action(s), missing evidence or blocker; never calls a model |
| `devflow action record` | Action ID plus native-host result or external readback | Binds observed identities and worktree/PR IDs; reconciles action state without inventing a host readback |
| `devflow host assign` | Bounded role/ownership/brief, coordinator identity and selected saved settings/explicit overrides | Resolves policy and journals the assignment and action |
| `devflow host prepare` | Journaled assignment and action | Journals action begin before returning the supported spawn or follow-up intent |
| `devflow host record` | Assignment/action plus actual supported-tool receipt | Records the receipt; a spawn receipt alone does not activate product work |
| `devflow host startup` | Assignment and explicit child session evidence path | Validates parent, canonical agent identity and actual model/effort before binding the native UUID |
| `devflow host activate` | Assignment with verified startup evidence | Releases the bounded product brief to the verified agent |
| `devflow host observe` | Assignment plus exact canonical agent path, fresh native status and source reference | Records running/interrupted/completed control state; interruption retains the verified identity for reuse and native completion does not supply implementation proof |
| `devflow host result` | Verified implementation assignment and completion evidence | Records implementation completion; independent review/QA continue through gate recording |
| `devflow candidate record` | Work/attempt ID and owned checkout | Captures clean base/head/tree plus input fingerprints; freezes a candidate |
| `devflow check run` | Candidate and selected recipe/scenario | Runs the existing command with isolated inputs and records status/assertions/evidence |
| `devflow gate record` | Registered role assignment, candidate and structured gate result | Validates identity, scope, producer, evidence links and findings; never converts missing evidence to PASS |
| `devflow host recover-result` | Completed producer assignment and stored original result artifact with missing/empty evidence IDs | Derives that schema rejection, preserves original judgement and activation, and journals a bounded correction using existing evidence without rerunning product work |
| `devflow finding record/publish/close` | Finding record or ID, candidate, relevant fix/proof | Registers publication obligations; performs idempotent publish/verified-close transitions |
| `devflow deliver` | Candidate, requested endpoint and expected remote refs | Evaluates all conditions, performs the authorized action and independent readback |
| `devflow work reconcile` | Attempt ID | Repairs stale projections and resolves ambiguous actions from live evidence; preserves work |
| `devflow usage collect` | Registered task/attempt IDs and cutoff | Imports/deduplicates ccusage and response usage; writes attribution/price completeness |
| `devflow report` | Work/cohort and observation window | Deterministic status, cost, quality, intervention and missing-data report |
| `devflow install plan/apply/rollback` | Version, consumer inventory and explicit installation scope | Previews or updates only manifest-owned files/links; records rollback material |

`next` chooses from a closed vocabulary: `prepare_scope`, `prepare_workspace`, `launch_role`, `activate_role`, `observe_role`, `resume_role`, `wait_roles`, `import_gate_result`, `request_user_action`, `resolve_findings`, `capture_candidate`, `implement`, `repair_findings`, `run_check`, `push_branch`, `publish_candidate`, `publish_findings`, `close_fixed_threads`, `record_accounting`, `deliver`, `reconcile_action`, `done`. It does not synthesize arbitrary shell commands. Engineering instructions come from the owning stage skill, role brief and repository recipe. A completed producer's `import_gate_result` action includes the bounded recovery command when applicable; valid original output still imports directly.

Users need not fill these records manually. They ask for work or select a Ready issue. The agent assembles the observed source and its aggregate digest and captures engineering judgments; the CLI derives the workflow identities and records timestamps, refs, task links and accounting.

## 2. Records and invariants

| Record | Required fields | Invariant |
| --- | --- | --- |
| Work contract | Work ID, kind, title, outcome, scope revision, acceptance IDs/text, paths/boundaries, decisive context, dependencies, risk/reason, verification plan, endpoint | Accepted behavior and endpoint are explicit; issue node identity survives renumbering/transfer |
| Intake admission | ID, repository/work/scope, exact source/lineage/digest, allowed operations, decision kind/reference, `user_request` | Default `user_request` decision is agent-interpreted conversational direction, stored immutably for consistency; optional legacy verifier decisions retain their expiry/revocation rules |
| Authority | Derived from immutable admission; ID, source user instruction including a bounded existing batch selection, allowed operations, repository/work/scope limits, source reference, revocation/expiry when applicable | A public issue, arbitrary comment, label or model-generated text cannot grant execution authority |
| Attempt | ID, work/scope IDs, active host/original coordinator, phase, `entry_phase`, `execution_mode`, optional blocker, policy/configuration snapshot IDs, start/stop/outcome | At most one active claim per work item; resumed work retains its attempt; missing historical `execution_mode` means `native_thread` |
| PR continuation | Previous verified delivery, original and new admission/scope/workflow links, same work/attempt/PR and coordinator, actual continuation entry phase | Fresh requested same-PR work reacquires its claim without rewriting the original entry phase, delivery, candidates, results or action history |
| Assignment | ID, action ID, role, `host_kind`, `coordinator_agent_name`, `task_name`, `agent_name`, `role_policy`, `brief`, `startup_observation`, optional `replaces_assignment_id`/`replacement_observation`, owned paths/workspace, input candidate and result | Product work requires observed parent/agent/settings; canonical agent path controls the subagent and verified native UUID provides attribution; historical native-thread IDs/receipts remain intact |
| Candidate | ID, attempt/scope, repository, base/head/tree IDs, clean-state result, dependency/environment fingerprints, creation time | Immutable; a code change creates a new candidate |
| Check evidence | ID, candidate/input signature, recipe/version, scenario/acceptance IDs, argv/cwd/environment profile, start/end, status, assertion counts or manual observations, evidence hash | PASS proves the named scenario ran; setup-only success is not execution proof |
| Gate | ID, role assignment, candidate/scope/policy hashes, PASS/FAIL/BLOCKED, required evidence IDs, finding IDs, limitations | Independent producer; complete required evidence; PASS has no unresolved gate-blocking finding |
| Fix verification | ID, finding/candidate, registered independent assignment, invariant/check evidence, verified/not-verified result | Technical fix status is established before final gate evaluation; remote thread closure is a separate obligation |
| Finding | ID, origin/candidate, invariant, severity, evidence, disposition, publication obligation, PR/thread/comment IDs, fix/proof links | One cause/invariant identity across duplicate reports; confirmed code issues remain auditable |
| Delivery | ID, work/attempt/candidate, endpoint, authority, gate set, named source heads and target ref/SHA, expected integrated tree, protection snapshot, actual merge commit/tree, action/receipt and result | Done requires verified status and a matching independently observed integrated tree/endpoint |
| Usage | Unique response identity, task/turn/segment, timestamp, disjoint token partitions, effective pricing inputs, allocation weights | Each response is counted once per portfolio; weights sum to at most one, remainder explicitly unallocated |
| External action | ID, command, payload hash, expected remote state, prepared/dispatched/confirmed/ambiguous/failed, receipts | An uncertain result is reconciled before repeating the mutation |

Endpoint target syntax is explicit: a `local` target is the canonical absolute checkout path; a `pr` target is its base branch; a `merge` target is its destination branch; a `release` target is its exact preexisting tag. Remote refs are short names without a `refs/` prefix. Repository identity comes from the admitted authority. Preparation, dispatch and readback must agree on this destination; copying the accepted endpoint into a receipt does not establish which destination was actually affected.

[Conversational intake](issue-trust.md) specifies the 0.3.0 request admission protocol, immutable source bindings, bounded batch selection and retained old-pin exception. The agent supplies the request from conversation; it is not independent human authentication. Legacy Authority and Source shapes remain readable, but they cannot substitute for a user request. Continuation validates the stored admission without a separate approval channel or synthetic verifier.

The JSON Schema validates record shape. Pure domain functions enforce cross-record rules and references. For example, JSON Schema cannot prove a fixing commit is contained in a remote PR; the Git/GitHub adapter supplies that observation and the transition rule requires it.

Source lineage remains optional for historical schema reads. Preparation and execution admission share the stronger prerequisite: nonempty `source.lineage` and `source.consumed_digest == devflow.validation.digest(lineage)`. Preparation names missing or incorrect fields in `missing`; admission still rejects incomplete source with `admission_source`. The agent uses the [capture-to-source recipe](../skills/devflow-defining-work/references/source-lineage.md) to preserve exact observed IDs, revisions, origins and issue content digests separately from the lineage aggregate. Neither the recipe nor preparation invents provenance or supplies conversational authority.

Scope identity is a SHA-256 hash of canonical normalized accepted fields, excluding mutable presentation, counters and timestamps. Workflow identity includes the installed package, used skill/reference bytes, applicable instructions and repository profile. Model-routing policy is hashed separately. Candidate identity uses actual Git refs/tree; it is not the workflow version.

A profile change in an unreviewed candidate cannot lower its own required checks. Execute under the admitted trusted profile. Evaluate a proposed new profile as a change, then activate it at cutover. If host-loaded instructions/settings drift during a running attempt, record the effective change, pause affected decisions and reconcile; a stored hash alone does not force the host to keep old instructions.

A completed-PR continuation may explicitly capture an executing release against the owned checkout's older pin. Its snapshot binds the prior delivery, prior snapshot and observed checkout pin, retaining that checkout's profile and custom recipes. Only reopening that completed work may consume the exception; ordinary start and amend still require matching provenance. Repair preserves the original subagent execution mode. Historical direct native-thread attempts support unchanged delivery-only reuse, not conversion into a subagent repair lifecycle.

Candidate capture, completed implementation results and current implementation readiness require the producer's workflow snapshot to match the candidate and attempt. Before changing scope or policy, retain the worker's actual partial output and observe stopped availability, then amend and reactivate that same worker under the new snapshot. A historical BLOCKED result may still be imported after inputs changed, but it cannot establish current completion. The earlier activation and output remain immutable evidence.

Reused implementation results bind `assignment_action_id` from their actual native handoff context. A late or unbound result cannot change a newer activation. Already imported historical results remain valid under their original contract; the new binding applies to new reuse, without rewriting old proof.

## 3. State transition table

| Current state | Command/event | Conditions | Result |
| --- | --- | --- | --- |
| Backlog | ready | Complete contract, authorized readiness, known dependencies/prerequisites | Ready with scope hash |
| Ready | start | No active claim; configured host; required capabilities and work authority | Active/Implement and claim |
| Active/Implement | candidate + focused checks | Clean captured candidate; local required checks completed | Active/Verify |
| Active/Verify | failed gate | Confirmed finding or unmet acceptance | Active/Implement; failed evidence remains history |
| Active/Verify | all gates pass | Candidate/scope match; due findings published; fixed-thread obligations complete | Active/Deliver |
| Active/Deliver | deliver | Current authority, remote refs and required checks match; no Blocker/High; readback succeeds | Done with delivery record |
| Active/* | external blocker | Missing access, dependency, user decision, unavailable gate or ambiguous action | Same phase with explicit blocker |
| Blocked attempt | reconcile/resume | Blocker cleared by evidence, no competing owner, state consistent | Previous phase, or earlier phase if proof invalidated |
| Active/* | accepted scope amendment | Recorded user delta | New scope; invalidate affected proof and resume correct phase |
| Any nonterminal state | cancel | Explicit cancellation authority | Canceled; stop only owned work, retain candidate/evidence |
| Done with verified PR | reopen | Fresh bounded request for the same PR, original coordinator, resolved actions/results, consistent refs and claim | Same attempt Active at the actual continuation phase; original delivered proof stays immutable and changed-input proof is invalidated |
| Done | new user-discovered defect | Confirmed affected version and invariant | Linked repair item/attempt; previous delivery record remains immutable |

PR creation, an AI final message, GitHub issue closure, a card moved to Done, test command exit 0 and an expired lease are not completion events.

Define/Implement/Verify/Deliver remain optional reporting labels; they do not mandate four separate tasks, plans or approvals. New attempts default to `execution_mode=subagent`; `entry_phase` preserves the actual entry. A review-only operation starts at Verify. A merge-only operation starts at Deliver after importing and validating existing proof. Neither fabricates an implementation assignment; a required repair activates a real implementation worker. Tier 0 skips independent gates while retaining applicable implementation and static-check requirements. A canceled operation is not a successfully delivered outcome.

## 4. Host action bridge and subagent communication

The original user conversation is the coordinator. It owns admission, assignments, evidence integration and authorized delivery. A bounded `implementation_worker` owns implementation and repairs; independent `review` and `qa` subagents own their required gates. The coordinator, implementation worker, reviewer and QA have distinct verified identities. Review-only/delivery-only entry preserves valid existing proof without inventing implementation. New subagent work does not create visible peer tasks.

The supported control surface is `agents.spawn_agent`, `agents.followup_task`, `agents.send_message`, `agents.wait_agent`, `agents.list_agents` and `agents.interrupt_agent`. The CLI provides durable prepared intents and receives actual observations. It does not call an undocumented desktop API or reinterpret native tools as shell commands. All control targets use the exact canonical `agent_name`; the native session UUID is separately observed for attribution.

### Role policy

Resolve each model/effort field in this order: explicit user role/session override; selected configured role file; saved subagent default; saved global default. The active coordinator's settings and opening-chat overrides are excluded. The default mapping is `implementation_worker` → `implementer`, `review` → `reviewer`, and `qa` → `qa`. An explicit alternative role name selects that configuration. Honor `[agents.<role_name>].config_file`, resolving relative paths from the global configuration directory; otherwise check adjacent `agents/<role_name>.toml`. `[agents].default_subagent_model` takes precedence over the root saved model.

The settings resolver requires a nonempty model and supported reasoning effort; missing or invalid resolved values are explicit errors. It stores normalized role, selected role name, model, reasoning effort, `agent_type=default`, policy hash and allowlisted source/hash/settings evidence. Source hashes cover only used settings and references, including a selected configured-file route. Full TOML files, secrets, environment contents and legacy developer instructions never enter policy provenance. An override record documents the supplied choice without claiming independent user authentication. Saved dispatch policy and actual execution observation are separate facts.

Launch the generic native agent with explicit model/effort and `fork_turns=none`. Load the selected independently discoverable role skill through the self-contained brief. A fixed custom native role or a full-history fork can override the requested model policy; selecting a configured role name therefore does not select that fixed native agent type.

### Journaled startup and activation

1. Journal the bounded assignment, resolved policy and launch action. `host prepare` records `action.begin` before returning a launch intent; a prepared action alone is insufficient to invoke the host.
2. Spawn with a bootstrap-only message asking the child to report its own session metadata path and wait. Do not include executable product work in this initial message. Record the actual spawn receipt. It supplies `task_name` and nickname, not a native UUID or proof of effective model settings.
3. Validate the explicitly supplied child session file through `host startup`. Read only allowlisted `session_meta` and `turn_context` evidence; verify parent UUID, exact canonical agent path and actual model/effort against the assignment and policy. Bind the observed native UUID only after this check. Missing settings block activation. Unknown service tier remains unknown.
4. Release the bounded product brief through `host activate` only after startup validation. Preserve work/scope/assignment identity, acceptance, repository/base/head, owned paths, constraints, check/evidence references, role responsibility and expected result. The implementation result is recorded through `host result`; review/QA return candidate-bound gate records.

The session source path, normalized observation and evidence hashes remain in private local evidence. Do not commit session metadata or copy unfiltered session contents. `agents.list_agents` returns canonical paths/status and cannot independently establish native UUIDs, parent UUIDs or model settings. A nickname, a claimed UUID in prose or a fabricated readback does not satisfy startup validation.

### Results, reruns and recovery

Each independent role result contains work/scope/candidate/assignment IDs, producer identity, technical fix-verification observations, gate status, evidence references, findings and limitations. Import independent fix-verification observations first, then evaluate the gate against that resulting technical state in the same transaction. Persist both before the completion notification. For 0.5 subagent activations, include `assignment_action_id` and preserve the bare producer JSON in `producer_result_artifact_hash`; the imported record must exactly match those original fields. An activated independent round must have its PASS/FAIL/BLOCKED imported before repair, replacement or rerun. Late results bind historical assignment snapshots and retain explicit gate-ingestion mismatch evidence; they never update current readiness. The coordinator imports results only from the registered verified producer; it cannot replace an unavailable QA result with its own PASS. Implementation completion is separate from independent gate completion.

Reuse the same available implementation agent for repairs and the same available review/QA agents for targeted candidate reruns. `followup_task` has no model-override parameter. Changing role policy requires an explicit recorded replacement and fresh startup verification, rather than relabeling an existing execution. An unavailable-agent replacement preserves the original identity, results and history, records `replaces_assignment_id` and the actual `replacement_observation`, and transfers only the bounded brief/evidence. Workstream reuse stays within the same outcome.

An interrupted unchanged round resumes through `host resume`, then `host prepare`, the native follow-up and `host record`. The continuation retains its original gate activation; it does not permit a candidate/scope/policy change before the result is imported. Assignment and candidate records retain their workflow snapshot so gates bind the producing policy across same-attempt amendments.

After `interrupt_agent`, obtain fresh `list_agents` inventory: the interrupt response contains the previous status. Pass the exact agent's status and source reference through `host observe`. `interrupted` retains its UUID and startup proof for the same assignment's follow-up. A native completed status permits control reuse but does not substitute for candidate-bound implementation completion. Actual target-error evidence is required to mark an agent unavailable; an inventory omission is insufficient.

A lost spawn receipt leaves an ambiguous action. Reconcile supported inventory and actual startup evidence before any replacement; inventory absence alone does not prove a failed spawn. Do not duplicate an agent because its response was lost or a single inventory observation omitted it. Preserve uncertainty when the host cannot uniquely establish the outcome.

Historical attempts missing `execution_mode` continue as `native_thread`, preserving original task IDs, pending client IDs, receipts and evidence. A historical pending client ID is not an executable native task ID. Do not rewrite those records as subagent observations or use historical visible-task instructions to launch new subagent work.

Shared runtime resources are reservations: unique test app directory, ports, database/profile paths and cleanup token per attempt/check. Each mutable resource has a recorded owner. Subagents sharing a checkout preserve others' edits; one agent cannot clean another's fixture because a conventional path name matches.

## 5. Evidence reuse and invalidation

An evidence input signature includes the scope/invariant subset, candidate tree or explicitly declared files, recipe definition, harness version, dependency lock fingerprint, runtime version and relevant nonsecret environment profile. Default to the whole tree when a smaller dependency boundary is not established.

- Same candidate and identical signature: reuse the evidence; no speculative rerun.
- Changed candidate tree: independent review and affected product QA require a new gate result. Unaffected command results may be reused only on an exact declared input-signature match.
- Scope/risk/acceptance change: recompute requirements and invalidate gates that no longer cover them.
- Pure rebase with identical tree and relevant inputs: attach an equivalence record; preserve applicable local proof and verify current remote CI/ref state.
- Stack parent change: rebase/integrate in dependency order, compute each resulting tree, and apply the same rules. The final candidate needs cumulative product proof under the approved stack policy.
- Harness unavailable or assertions skipped: BLOCKED/FAIL, never PASS. A manually accepted lack of evidence is an explicit limitation and does not satisfy a required gate.

A check result retains both process outcome and semantic execution outcome. For automated suites, identify executed/skipped/failed counts and the required selected tests. For manual product QA, record the exact synthetic setup, action, observed behavior and artifact. Screenshots alone do not establish the persistence or acquisition path when that is the invariant.

## 6. Finding publication and closure protocol

Publication state is `pending_pr`, `due`, `published` or `blocked_anchor`. Disposition is separate: `open`, `fix_pending`, `verified_fixed`, `deferred`, `duplicate` or `not_a_defect`. A resolved GitHub flag never changes disposition by itself.

Deduplicate against stable finding IDs and existing review threads, with semantic comparison of invariant/cause by the reviewer. Store the evidence for merges of duplicate reports. The deterministic publisher does not ask a model to rediscover all findings on every run.

Publication sequence: fetch current PR/base/head and all review-thread/comment pages; choose a validated line or file anchor; enqueue one action per finding; publish with a stable public correlation marker; locate the returned review thread; read back body/anchor/identity; mark Published. Several findings may share a submitted review, but they remain separate threads. Use file-comment endpoints where the chosen batch endpoint cannot represent a valid file anchor.

Technical sequence: run the fix/regression proof; the assigned independent role records a `FixVerification` observation for that finding and candidate; apply the applicable verified observations to set its technical disposition to `verified_fixed`; then evaluate the final gate's blocking finding set. This avoids requiring a final PASS before a finding can stop blocking that PASS. A new candidate must still satisfy the fix-evidence validity rules.

Remote close sequence: after technical verification and required final gates, verify the remote head contains the fix or recorded squash mapping; reply with sanitized proof; resolve; read back. Publication and closure state remain separate from the technical disposition. For a local-only endpoint, technical verification and local delivery can complete with `pending_pr` publication and closure `not_due`. When later publication is authorized and a PR exists, the obligation becomes due and must complete before that published outcome is reported complete.

A declined or disputed allegation requires an evidenced non-defect disposition; it is not a repaired defect. An accepted real deferral remains open with a linked issue. `finding.defer` records its actual follow-up reference and rationale. At 0.5 delivery, every accepted deferral, including Low, requires an observed linked follow-up. Unresolved Medium findings require an accepted deferral; Blocker/High findings still require independent fix verification. Low findings may remain open without a formal deferral. Required publication failures block the relevant outcome's completion even when code checks pass. Pending obligations for work without a PR remain visible in the overall report with the exact reason and owner.

## 7. GitHub and Git side effects

Use GitHub node IDs for stable identity and owner/name/number for request routing. Query all pages of issues, dependencies, reviews, review threads/comments and checks. Read native blocked-by relationships; detect cycles before Ready. Use global issue IDs where the dependency endpoint requires them. Project field and option IDs are resolved during enrollment and stored in private bindings; names are not assumed unique. [Issue dependency API](https://docs.github.com/en/rest/issues/issue-dependencies), [Projects API](https://docs.github.com/en/issues/planning-and-tracking-with-projects/automating-your-project/using-the-api-to-manage-projects).

Branch updates use a `push_branch` action bound to current candidate, owned branch and expected remote head (explicit null for creation); native Git leases and independent readback provide recovery without an unjournaled push. The tool has one writer per action stream. Persist intent before mutation and receipt after independent readback. On 429/temporary network failure, honor backoff and perform bounded retries. At most three transport attempts per invocation; a remaining failure becomes a persisted blocker. On 403/missing scope, do not retry as a transient error. On uncertain success, inspect remote state before retrying.

### Integrated-tree delivery policy

The first automatic-merge profile is protected direct merge, including GitHub's atomic stack merge. Enrollment requires strict up-to-date required checks, a required `devflow/verified` commit status for the evaluated head(s), and enforcement for the acting account without a bypass of those checks. Preserve existing owner-only updates and release-tag boundaries. Required gate status is a small commit-status publication, not a new CI scheduler. Its success binds the source head, scope, policy and applicable gate set; any invalidation returns it to pending/error. Existing CI requirements also remain enforced. [GitHub protected branches](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

Before merge, record named source PR/ref/head tuples, target ref and observed target SHA, the protection snapshot, merge method and the expected integrated tree. Ensure the target is included in the verified candidate ancestry. Ordinary PR merge also supplies the expected source SHA. For a tracked stack, use `gh stack merge <verified-selector> --yes --squash` or an explicitly selected supported method; the adapter validates the selector's exact member set and required per-head proof statuses first. Do not use a blind `gh pr merge` fallback for stacked PRs. Local serialization protects local operations; GitHub's enforced freshness and required per-head statuses protect against concurrent remote writers. Enrollment conformance must demonstrate that the installed stack backend enforces those conditions for every selected head before automatic stack delivery is enabled.

If the target advances before merge, the server must reject the stale integration or the tool must recompute/reverify it. Tests deliberately race another target update and a PR head update against merge. If server-side enforcement is unavailable, this profile cannot claim safe automatic merge: retain the verified PR and a `merge_policy_unavailable` blocker. Changing repository protection is a concrete enrollment change requiring its existing or newly granted authority.

Read the operation's actual resulting merge commit(s), not just the latest target tip. Compare the final merged tree to the evaluated integrated tree and confirm that commit is reachable from the current target. A later unrelated target commit does not falsify the recorded merge tree. Preserve per-layer squash/rebase mappings. A lost response is reconciled through actual PR merge metadata before retrying. Existing partly delivered stacks resume from observed state; an ordinary atomic direct stack merge is not assumed to expose partial success.

If a remote merge has occurred but its tree does not match evaluated proof, record `exposed_unverified`, stop further delivery admissions, notify the owner and reconcile/verify the actual candidate. Do not report Done, repeat the merge, or automatically reset/revert user work. Required corrective work stays linked to the attempt. Queue-enabled repositories need a separate merge-group proof adapter; the first profile reports unsupported queue delivery rather than treating queue admission as merge completion.

Issue/Project synchronization updates only the tool-owned summary and derived fields. Preserve the user's prose and labels. A changed accepted section triggers a scope conflict, not last-writer-wins overwrite. Project synchronization failure is visible; a failed review publication or merge readback blocks completion. A stale Project card alone does not invalidate already-proved product correctness, but the delivery report shows the unsynchronized projection until repaired. A real deferred finding remains open; if the repository requires thread resolution, its merge stays blocked until the issue is fixed or the owner explicitly changes that policy.

## 8. Local storage, retention and failure behavior

Proposed private root: `~/.local/state/devflow/`. The store contains `state.sqlite3`, `artifacts/<sha256>`, `policy/<sha256>`, `backups/` and `install-manifests/`. User bindings/model preferences live under `~/.config/devflow/`. Repositories contain no generated execution ledger or session data. Files are owner-readable/writable; paths are resolved and checked against the enrolled roots.

Registered check evidence hashes address retained JSON artifacts, containing the original command `output` and, when available, `junit` text plus `report_sha256`. The runner archives them before removing its temporary report directory. Review/QA handoffs supply the evidence ID, artifact hash and explicit state root; readers verify the hash and inspect that artifact. Expanded argv paths are historical execution metadata, not retained report locations. See [check evidence inspection](../skills/devflow-verifying/references/check-evidence.md).

SQLite uses transactional writes, explicit EXTRA/fullfsync durability, foreign keys and an active-attempt uniqueness constraint. The audit trail records old/new revision and event identity. Outbox actions and local transitions commit together. Evidence bytes and directory entries are flushed before references become committed. Installer releases/journals and check drafts use the same flush-before-acknowledgment boundary; snapshots validate before durable publication. Startup reports missing/corrupt evidence instead of silently treating it as a successful check. Crash tests cover actual process death and injected storage failures, not a physical power cut or failed storage hardware.

Backlog capture is a pre-attempt outbox: a schema-versioned private operation fact retains the stable repository/work identity, initial sanitized payload/hash, monotonic sequence, prepared/dispatched/ambiguous/failed/confirmed state and observed issue identity. Facts append to the existing operation table; no shared-store schema upgrade strands an older pinned attempt. Dispatch commits before calling gh. Confirmed capture replays. Creation journals authenticated repository/creator identities and content digest; a successful POST identity is saved before exact issue readback. Lost responses require a unique marker plus matching pending-operation identities/content. Reuse and legacy records are unknown without trusted provenance. A later failed read cannot erase historical mutation uncertainty. Capture never derives publication authority from issue content.

No automatic deletion of worktrees, evidence or historical records is part of the reset. Owned temporary fixtures may be removed after their cleanup token and real path are independently verified. Maintenance can later propose a retention policy with an explicit deletion scope. Installer rollback restores managed configuration; it does not erase user work or GitHub history.

| Failure | Required behavior |
| --- | --- |
| Duplicate start / two owners | One claim succeeds; the other receives the existing attempt and does not launch another implementation |
| Coordinator interrupted | Preserve phase/actions/candidate and verified role identities; resume/reconcile under the same attempt |
| Lost subagent spawn response | Reconcile supported inventory and startup evidence; absence alone does not permit a duplicate; block if ambiguous |
| Role policy changed / role unavailable | Record replacement reason and actual observation, preserve previous identities/evidence, verify the replacement startup before activation |
| Historical native-thread create response lost | Reconcile original task inventory and assignment marker under its retained host contract; block if ambiguous |
| GitHub write succeeds, response lost | Reconcile by action/finding identity and remote state before retry |
| Issue acceptance edited mid-run | Stop dependent delivery, append accepted amendment or retain old scope explicitly |
| Candidate changes during review | Reject mismatched gate for the new candidate; preserve it as historical evidence |
| QA runtime cannot start | Mark blocked/failed with setup evidence; do not report executed product assertions |
| Required price missing | Preserve token counts; cost unknown; suspend budget-dependent admission |
| Missing Project access | Report binding prerequisite; do not fabricate an existing Project or silently create another |
| Dirty owned or unrelated checkout | Preserve it, identify ownership and conflict; no implicit reset/delete |
| Corrupt/incompatible state schema | Stop new mutations, retain backup/current files and report recovery path |
| Unauthorized merge/release | Prepare the complete reviewable candidate; request only the remaining explicit authority |

## 9. Accounting and future comparison boundary

Use the precise formulas and defect definitions in the [metrics contract](metrics-contract.md). The new implementation adds stable joins, not new interpretations of those metrics. A late-discovered defect links to the historical introducing candidate/workflow/model segments and passed gates; its detecting and fixing configurations remain separate.

For 0.5 delivery, a candidate-bound `usage.account` record declares complete/partial/unknown/unavailable with source and limitations. Complete requires imported task/segment usage; incomplete data is explicit and cannot masquerade as zero. Every role/coordination response is attributed once. Shared work uses explicit allocation weights; unknown portions stay unallocated. Model retries and failed/abandoned attempts stay in cost totals. Ordinary delivery cost, later repair cost and experimental duplicate cost are distinguishable while portfolio totals remain non-overlapping.

The workflow implementation uses current user defaults. Only after stabilization will a separate experiment assign two independent runs of the same frozen case to two configurations. Case/pair/arm IDs attach to attempts, not to additional product deliveries. An unexposed duplicate has no production defect observation window. No comparison or automatic model-policy change is part of installing this package.
