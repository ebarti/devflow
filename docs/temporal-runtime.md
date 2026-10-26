# Local Temporal delivery runtime

New admissions retry an exact keyed local phase projection up to three times over
three minutes. A database lock or lost activity completion does not launch another
role, publication or tracker effect: the existing transaction deduplicates the
same event and key. Database work runs outside the shared worker event loop.
Malformed state, missing runs, changed configuration and database corruption are
terminal. Previously admitted inputs keep their recorded projection options;
scope amendments preserve that choice. No database migration or operator retry
is needed after an ordinary deployment.

Managed service startup authenticates the configured Temporal namespace and verifies
at least 90 days of closed workflow history retention before launching delivery
workers or the API. Direct worker/API startup uses the same check; an already
running owned service is checked without restarting its processes. A lower TTL is
raised through Temporal's supported `UpdateNamespace` API, with exact readback;
higher retention and other namespace settings are preserved. Validation, update
or readback failure blocks startup with a concrete error. This does not change
service configuration, recreate the namespace or enable archival.

Retention changes protect workflows that close after the update. Already closed
executions keep their previous cleanup timers, and expired history cannot be
restored by increasing retention. This provides a bounded recovery window, not
indefinite archival. See [Temporal's retention behavior](https://docs.temporal.io/encyclopedia/temporal-service/temporal-server).

Recovery no longer depends on that window for sealed executions. The exact original
history of a closed execution is stored compressed and content-addressed under
`state_root/closures/` (private files) and indexed in the tracking database. The API's
maintenance loop seals each run's current execution once it closes, and every recovery
read (stopped resume, gate retries, pending publication, feature continuation, failed
coordinator and terminal-tracker recovery) seals the execution it authenticates. When
Temporal no longer returns a closed execution, recovery rebuilds the identical closure
(workflow and run IDs, close time, memo digests and result) from the sealed history.
Sealing never writes to Temporal, keeps one execution per workflow ID and never
replaces a sealed history with a different one. An execution that closed and expired
while the service was not running cannot be sealed retroactively.

A closure that expired before it was sealed is refused with an explicit message.
If an authentic exported history exists, import it once:

```sh
devflow-delivery --config "$CONFIG" import-closure --id RUN_ID \
  --workflow-id WORKFLOW_ID --history exported-history.json
```

The import accepts only a complete, contiguous `DevflowDeliveryWorkflow` history
that closed COMPLETED or FAILED, whose workflow ID is the run's bound execution,
whose memo binds the run's request and recovery digests, whose inputs name the run
and its recovery, and whose terminal result agrees with the run's projected outcome.
It refuses while Temporal still retains the execution.

An original six-run permanent-installation cohort can instead be blocked by two
latest selected executions that closed while the controller was unavailable.
When the original helper and program location are gone and retained Temporal is
stopped, use the separate explicit local data-only owner:

```sh
devflow-delivery --config "$ORIGINAL_CONFIG" capture-retained-closures \
  --request "$PRIVATE_CAPTURE_REQUEST"
```

This narrow native macOS operation requires the unchanged original owner, a
private hash-bound six-member installation request, and exactly two current
workflow/execution selections. Each selection supplies `run_id`, `workflow_id`,
`execution_run_id`, `revision`, `protocol_revision`, `request_digest` and
`recovery_digest`. The top-level request contains `command_id`, `migration_path`,
`migration_sha256`, `captures`, `reader`, and `answer`. `answer` is the literal
`Capture only the two selected closed histories from an owned copy; change no delivery.`
The four other latest closures must already have valid authentic seals. The
selected rows must still be the exact stopped, unprojected native executions;
predecessor histories and terminal summaries cannot substitute.

The `reader` explicitly binds the unchanged configured
`/opt/homebrew/bin/temporal`, its resolved Homebrew CLI1.9.1/Server1.32.0
installation, current executable SHA256, and `historical_start_binary_sha256:
null`. Original startup version evidence and the fixed Homebrew installation
receipts are checked. This is current copy-reader authority; it never adds an
unrecorded historical executable hash. The original native Python3.12.13,
framework SDK1.33 and frozen dependency proof remain required. Browser-test SDK
and executable preparation receipts do not authorize this operation.

The owner holds the original exclusive lifecycle inode and installation recovery
lease, proves stopped service/native actors and complete original-data FD
visibility, then streams a stable raw DB plus optional WAL into a private
immutable master. It never opens the original Temporal SQLite connection or
creates/checkpoints its sidecars. SHM is retained as forensic evidence and rebuilt
locally in the working copy. A hot/unexpected journal, uncertain visibility,
foreign FD, replaced name, changed source generation or insufficient bounded
storage refuses capture. The pristine master admits exactly the original family
and its manifest; an added WAL cannot acquire recovery authority. Writable reader,
snapshot and working generations are exclusively allocated. Existing staging,
symlink or hard-link destinations are never adopted for copy or SQLite writes.
The disk preflight reserves all four retained copies plus headroom. Prepared
working-directory and database descriptors remain held through the child gate;
their names, identities and exact prepared bytes are checked before GO.

Only an identity-bound copy server runs, on fresh loopback endpoints with private
HOME/cwd/tmp and no inherited original lock/data descriptors. The pinned SDK
fetches both complete ALL_EVENT histories with archival explicitly skipped,
within total page/event/byte/time
bounds (512 pages, 65,536 events, 48 MiB encoded history and 90 seconds). Its
standalone SDK receive limit is fixed before first connection; compressed
payloads are expansion-checked before the normal codec, with 32 MiB per payload
and 64 MiB total decoded bounds. These are local copy-reader resource limits,
not changes to any delivery check or provider policy. Full start, memo, recovery, current execution, native and failed terminal
tracker/deadline authority must validate through the existing owners. Exceeding a
bound refuses the whole history. The copy server is cleaned up before one atomic
metadata acknowledgement appends both genuine `temporal_snapshot` closure seals.
Immutable provenance binds the snapshot, reader, namespace, explicit workflow and
execution IDs, raw exports and full canonical history hashes. It is not a new
execution transport or installation permission.

Complete stopped-owner authority can span more than one evidence file. Capture
intent/final receipts and installation receipts retain the existing bounded full
JSON form through16MiB. Larger documents use an explicit owner/document-scoped
immutable manifest: ordered private1MiB content-addressed parts, at most64parts
and64MiB of complete canonical authority. The small acknowledged root binds every
part's exact index, length and hash, the whole canonical hash/length and logical
root members. All parts and held file/directory generations are authenticated
before replay, historical/current validation or an effect can consume facts.
This preserves every run record, historical scope, journal hash, PID/start pair
and native input; it neither truncates evidence nor expands generic private-file
or history limits. Parts and a complete unacknowledged manifest remain inert
until the existing atomic capture or all-member installation acknowledgement.
Missing, changed, reordered, linked or excess parts refuse the entire document.
An aggregate above64MiB refuses before publishing a final root.
An interrupted publication retains one bounded sequential part prefix; a retry
cannot append a different hash at an occupied index or replace it with an inline
document. Atomic staging remains separate and grants no permission. The exact
root, part and directory descriptors stay held through canonical validation.
Every read validates the complete content freshly; no authority cache replaces
current source, claim, candidate or actor checks.

An earlier oversized immutable full intent remains evidence and is never
converted or overwritten. After proving that it reached no snapshot, reader,
export or acknowledgement, the operator may use a fresh explicit capture command
ID with the same original run/execution selections and fresh current checks.
This creates no replacement run or execution allowance.

The same exact command replays safely after an interrupted capture or lost
acknowledgement. A child without durable GO authorization is never signalable;
unknown actor/cwd/port ownership refuses replacement. A complete file-only
receipt is inert until the two seals and command/audit acknowledgement commit.
All `delivery_runs` fields, claims, provider outcomes, original unknowns, frozen
specifications, gates, models, capacity and allowances remain unchanged. The
expired-history import still requires genuine NOT_FOUND. After capture, rerun
the ordinary all-six installation preflight. Following installation, the existing
authentic closed-projection reconciliation, public cleanup and fresh finite
continuation preflight still apply; capture itself grants no retry or iteration.

The runtime under `runtime/` is a single-host, local development service. It accepts a raw goal with an explicit, allowlisted issue and repository, runs a Temporal workflow, and exposes the same persisted run through the dashboard, CLI, and MCP. A read-only intake role investigates repository evidence, asks only blocking questions, and records a scoped plan before autonomous implementation. The managed path then owns an isolated checkout, an early open pull request, independent review and verification roles, bounded repair in the implementation session, local and required CI checks, and issue reconciliation. The default endpoint is a **published, unmerged PR**. A fresh native run can instead explicitly authorize `merged`. That endpoint keeps all original checks, independent roles and required CI, then confirms owned cleanup before requesting a head-pinned squash merge. It independently verifies the actual merged commit/tree and closes only the admitted issue before reconciling Done and releasing the claim. A divergent base, uncertain effect or failed gate blocks completion; a lost response is reconciled through the original intent without blindly repeating the mutation. Existing unmerged histories retain their endpoint. Release, deployment and personal production data remain separate authority.

The older `devflow-temporal` CLI remains a disposable role-ordering demonstration. It does not publish a PR or reconcile an issue. Use the managed `devflow-delivery` service for an end-to-end delivery.

## Setup and policy

On macOS, install Python 3.12 or 3.13, `uv`, Git, GitHub CLI with access to the selected repository and Project, and a local Temporal CLI. Real runs execute natively on macOS; `execution_backend: "native-macos"` is the only supported execution policy. Docker is not required. Start from this checkout:

```sh
cd runtime
uv sync --frozen --python python3.12
cd ui
npm ci
npm run build
npm test
cd ..
```

The real provider declares `agent-runtime-kit[codex]>=0.5.3,<0.6`; the public-index `runtime/uv.lock` resolves kit 0.5.3 and SDK/CLI 0.160.0. Native preparation verifies the installed versions, frozen lock, runtime payload, Python and bundled SDK CLI identities. Set `codex_bin` to that installed `openai-codex-cli-bin` executable. A standalone CLI compatibility check does not establish managed kit execution or same-session resume.

The private service JSON owns absolute paths for `state_root`, `tracking_db`, `helpers_dir`, and `codex_bin`, fixed ports, explicit role models/efforts, and allowlisted repositories. A repository fixes its source/origin, Git base, Project/assignee, exact editable files, recovery manifest, check argv/cwd, optional browser fixture and required CI. Clients cannot supply these authorities. The default `execution_mode: "native-profile"` constrains toolchain/browser read roots to canonical directories outside controller state and credentials and checks to exact configured domains; role commands have no network access. A trusted single-user Mac can explicitly select `execution_mode: "trusted-local"` for noninteractive full host access without additional path or domain allowlists. This mode is an explicit trust tradeoff and retains controller budgets, ancestry/source checks and owned cleanup, not hostile-code isolation. See [trusted local execution and historical policy recovery](trusted-local-delivery.md). A service migration changes the execution policy and necessary tool locations with a private backup, digest guard and no live-run conflict; it never rewrites historical authority.

New admissions freeze `tracker_retry_seconds` (default 600; integer 60–3600). Recognized GitHub transport failures and local database lock timeouts retry the existing owned tracker intent on durable timers, without another feature turn or operator signal. Initial retries back off from two to thirty seconds; terminal reconciliation continues automatically after its first three attempts. Cancellation wakes initial waits. Ownership, mapping, permission and unknown errors remain conflicts; a deadline cannot make pending tracker state consistent or establish claim release. Previously admitted specs keep their original manual checkpoint behavior, including after a worker restart. Changing this setting affects only future admissions.

Valid raw requests are durable in `preparing` without caller-authored proof. Temporal's existing preparation activity measures deterministic parent/child filesystem and network denials through the actual named Codex command sandbox, including owned scratch writes, unrelated temp denials and nested provider attempts. The private cache records measured logs and runtime identity. Missing, changed or failed observations block execution. Each run separately freezes its repository, scope, branch, checkout/state paths, checks, model selection and endpoint binding. Cache reuse never grants a different run that binding. Preparation heartbeats every five seconds and may make up to three attempts within two hours after worker loss. Authority/probe conflicts fail explicitly; a published proof or frozen result can be recovered without manufacturing observations. Candidate-project and real managed model/resume checks remain separate evidence.

```json
{
  "command_id": "submit-001",
  "run_id": "local-001",
  "work_id": "issue-work-001",
  "issue_url": "https://github.com/OWNER/REPO/issues/123",
  "repository_key": "configured-repository",
  "goal": "Add the requested behavior to the configured repository",
  "publication_summary": "feat: add the requested repository behavior",
  "base_ref": "main",
  "branch": "feat/issue-123",
  "authorized_endpoint": "published_unmerged"
}
```

`goal` holds execution instructions and remains immutable. `publication_summary`
is a separate Conventional Commit subject describing the actual change, including
its type (for example, `docs: investigate source completeness` for a design
document). It must be one line and at most 120 characters, with no extra sentences
or control characters. The controller freezes it and uses it for both commits and
the PR title. A short single-sentence goal can supply the summary when this field
is omitted; detailed goals require an explicit summary before admission. Existing
frozen runs retain their original publication metadata behavior. This change does
not rename existing PRs or rewrite historical commits.

New admissions freeze optional `plan_approval: "automatic"` by default. Read-only intake investigates and uses reasonable reversible assumptions for routine choices and asks only unresolved blocking questions. Once those answers are complete, it records and binds the exact plan revision and digest with provenance from the run authorization, then starts implementation without a human `proceed` answer. Set `plan_approval: "required"` only when the caller explicitly requests human plan review; that mode waits for `proceed`, `change` or `cancel`, bound to the current plan revision and digest. Invalid values and a required-review request combined with `accepted_plan` are rejected before claiming work. Historical stored inputs without the field retain their required gate and original Temporal replay ordering. Neither policy permits broader paths, checks or endpoints. No model runs while waiting. Intake is limited to eight turns; other roles retain `max_repairs` and separately recorded finite operator grants. Roles and checks have bounded deadlines and shared durable capacity. Older requests may still carry an explicit `accepted_plan`. An identical submit reuses the run; changed inputs under that run ID conflict. The outbox records intent before Temporal start and verifies the remote memo on recovery.

New admissions freeze `blocking_questions_version: 1`. Every question must explain its unknown, checked evidence and why no reasonable safe default satisfies the authorized goal; malformed output blocks intake without creating a user question. Historical inputs retain the old schema and Temporal replay behavior. The justification is persisted with the current decision.

An optional canonical UUID `origin_thread_id` is bound at admission and preserved through preparation, scope amendments and continuations. MCP captures the native top-level `threadId` from each request’s metadata, or `x-codex-turn-metadata` (object or JSON string with `thread_id`) or `openai/threadId`/`openai/thread_id` metadata; conflicting or malformed destinations are rejected before a work claim. `sessionId` and the daemon environment are never reply destinations. CLI submission may use its caller’s `CODEX_THREAD_ID`. Role output cannot choose a target. Standalone submissions without a thread retain dashboard questions and an explicit `unavailable` callback status.

The question projection transaction creates one notification for its decision ID/revision and frozen destination. Services sharing a tracking database claim and recover only notifications bound to their frozen configuration path and state root. The deterministic local service serializes senders with an OS lock, commits `dispatching` before invoking the configured installed `codex queue --thread UUID --message TEXT` with separate argv, and commits `queued` only after an acknowledgement for that target. It rechecks pending question, answer/cancellation mutations and supersession immediately before dispatch. Duplicate projections, concurrent pumps and restarts do not resend; crash, timeout or uncertain output becomes visible `unknown` without blind retry. A failure before launching is `failed`; obsolete callbacks are `suppressed`. Sender events update projection revisions without altering Temporal protocol revisions.

Blocking-question callbacks use a fixed owner-message template. Repository/issue-derived agent text, including the agent-derived decision ID, appears only as single-line JSON strings in a delimited quoted block. Whitespace is flattened and hidden controls/formatting characters are rendered as visible Unicode code-point markers. Preview limits are 1000 characters for the question, 600 each for the unknown and why no safe default exists, 240 for each of up to eight evidence entries/options, and 384 for the derived decision ID. Truncated fields are marked; full question data stays unchanged on the dashboard. Controller-owned run/notification IDs, dashboard URL and decision/candidate revisions are preserved. The quoted preview is notification data; it cannot authorize an answer, plan or new run. Read the current run and present the genuine blocker to the user before responding. The local API remains tokenless with its existing loopback/origin/CSRF boundary.


A queued acknowledgement proves native enqueueing, not Desktop rendering, chat consumption or an answer. The receiving local-delivery skill freezes the presented run, decision ID/revision and candidate ID/revision before asking the actual user. After the reply, it re-reads the same run and requires that exact pending question identity. A changed or absent decision discards the delayed answer; any new question is presented separately. It never rebinds an old answer to new IDs. A matching answer uses the current protocol revision and frozen decision/candidate revisions. Callback data never approves a plan, changes authority or answers a question. No routine progress or completion callbacks run.

Native role launch uses the supported kit adapter and the selected mode-bound permissions. A trusted child waits behind a start gate until its PID/start identity is durably recorded. Child stdout/stderr use pipes drained by the controller into protected durable logs, so child descriptor metadata checks do not require access to controller files. The runtime journals observed descendants and the returned provider session/thread ID; a session ID identifies resumable conversation state, not an OS process. Implementation repair resumes the same session through the owning controller. The role's persisted Codex home is separate from its transient HOME/TMPDIR. In constrained mode, shell commands cannot read copied provider credentials, controller tokens or mutable MCP authority, and project `.codex` configuration, Git hooks and Git metadata remain excluded. Trusted-local execution has full host access; these are preparation/controller controls rather than non-bypassable host restrictions. Review/verification receive controller-bound diffs and broker receipts.

The controller disables built-in agent spawning with `agents.enabled=false` and `features.multi_agent=false`, and rejects nested managed Devflow entry points before provider work. The supported kit adapter records the native SDK's parent thread ID before its turn and inspects raw typed collaboration items before the kit's filtered tool audit. Bounded before/after inventories include all source kinds and archived threads in that role's isolated Codex home. Collaboration, an extra child thread or incomplete observation blocks the role; an empty filtered audit cannot prove zero children. Constrained-mode command-sandbox probes deny normal-shell and explicit-path nested `codex exec` access to credentials and provider network, and require no provider thread/response observation. Harmless `codex --version` or help execution is allowed. This is a boundary against recursive agent/provider work, not a claim that macOS prevents every binary invocation. A prompt or hidden PATH is not the guard. The graph, depth, product repair attempts and active-work deadlines cannot extend themselves.

For native roles carrying the provider retry policy, a confirmed model-capacity failure (`serverOverloaded`, `flexUnavailable`, a completed HTTP 503 failure, or an explicit SDK `ServerBusyError` rejection) retries in the same role and provider thread until success or cancellation. Backoff grows from one second to a maximum of sixty seconds; there is no capacity-attempt limit. Confirmed rejected-attempt time and its backoff do not consume the active role timeout or another product repair cycle. The native monitor authenticates the waiting process identity and still enforces active-work timeouts, cancellation and cleanup. Full capacity history remains in the private native observation, including before a later authenticated successful build. Account rate limits and other transient transport failures retain the configured bounded policy; authentication, quota, unknown accepted outcomes and permanent errors do not gain capacity retry authority. Historical requests without the provider retry policy retain their original execution contract.

Native teardown stops only observed PID/start-identity matches, including observed detached descendants, and checks configured fixture ports. It does not claim Linux PID-namespace containment or prove every unseen detach/reparent race. An interrupted monitor, ownership gap or uncertain teardown remains `unknown`, prevents clean completion, and retains recovery material. Unrelated processes and ports are never stopped by name or recycled PID alone.

Temporary resource cleanup is embedded in terminal workflow finalization, including delivered, cancelled, blocked/error, timeout and preparation failure boundaries. Ownership is recorded before transient roots, disposable worktrees, dependencies and browser scratch are created. A heartbeating finalization activity makes up to three attempts within ten minutes, resumes a partially completed removal and verifies absence. The existing run checks/projection show separate process and resource cleanup statuses plus a hashed receipt: `removed`, `already_absent`, deliberately `retained` with reason, or `failed_unknown`. Required transient removal failures cannot produce a clean delivered result.

Finalization removes registered role/check/probe TMPDIRs, browser scratch, generated dependencies/artifacts and disposable gate worktrees. It preserves durable logs, receipts, decisions and sessions; dirty, untracked, unpushed or blocked recovery source remains with an explicit reason. A clean main worktree is removed through Git only after process/open-file checks and either no candidate change or exact published remote incorporation. A historical gate can be removed only when its recorded source remains incorporated in the retained candidate. Symlink/replaced roots fail closed; nested symlinks are unlinked without following them. Cleanup does not sweep global temporary directories, shared caches or credentials.

Native checks register raw pytest output before execution and remove it after their original process and ports are confirmed stopped, including failed checks. Retained candidate-bound reports are inventoried before copying, with a 64 MiB aggregate limit for new manifests. Overflow remains a failed evidence check; mandatory JUnit reports cannot be silently dropped. Replaying a completed check validates its retained manifest without recreating raw fixture output. Generated output is removable only when declared by the frozen check policy; nested workspace dependencies additionally require unchanged tracked package declarations.

Owner maintenance also revisits bounded batches of closed native runs interrupted during checks, preparation or finalization, including a crash after files were removed but before cleanup status was saved. It authenticates the original Temporal execution, resource ownership, processes and configuration before completing cleanup. This performs no provider turn and spends no repair budget. Missing disposable scratch can be recreated only after its old native launches are confirmed closed; durable candidate and session state remains outside that scratch. The runtime's own pytest suite retains no temporary fixture sessions by default.

For configured offline pnpm checks, trusted preparation copies only the admitted base's frozen lock, exact package-manager declaration and required sanitized workspace/patch inputs into a registered staging root. It validates registry/tarball targets as `registry.npmjs.org`, disables scripts and pnpmfile hooks, and fetches into an owned store using a fixed credential-free command. Candidate checks read that store with their existing offline argv and network policy. These are trusted-input/command controls; they do not claim OS-enforced DNS domain confinement. Workflow finalization removes staging, store and generated dependencies while retaining hashed fetch/check logs. Candidate lock drift and unsupported external sources are explicit failures.

For browser/API QA, policy fixes one argv/cwd, two exact owned local ports, permitted read roots, a positive test-count rule and bounded artifact directories. The credential-free native fixture and its browser/services inherit the selected mode: constrained Seatbelt filesystem/network controls or explicitly trusted host access. The broker journals ownership, checks actual port listeners, test counts and candidate stability, preserves hashed logs/artifacts under durable evidence, and records an immutable receipt. The independent kit verifier inspects that evidence without claiming it ran the broker command. Completed native journal/receipt replay does not launch a duplicate effect.

Full PDF evidence may contain up to 2,048 files within a combined 50 MiB bound. Evidence collection errors fail the gate while preserving the native invocation's independently observed cleanup. They cannot turn completed process cleanup into an unknown process result.

For an ordinary published run stopped with `browser QA child cleanup is unknown` after a historical `ValueError`, the owner may use `devflow-delivery --config CONFIG reconcile-attempt --id RUN --role browser_qa --iteration N`. This requires the exact closed workflow, unchanged published candidate, one matching browser effect, and its finished native journal. It completes only retained evidence and the existing cleanup/claim transaction; it cannot launch another browser invocation or rewrite the historical workflow verdict. Read the run and repair-admission preflight again before continuing it.

For an ordinary published run whose native roles have finished but whose resource cleanup remains unknown, `reconcile-attempt --id RUN --role cleanup --iteration N` authenticates the exact closed workflow and re-observes its native resources before closing its claim. An unpublished run is also eligible after it has an accepted plan and an exact completed implementation/source checkpoint. Its closure is recorded as `unpublished`, with the closed execution identity, original checks/results, current source and work binding, attempt/effect inventory, native observation and tracker acknowledgement. It independently observes the configured origin and absence of the owned remote branch/PR before finalization and again before claim release. Pre-intake runs, a newer issue owner, changed work bindings, uncertain effects or live native custody prevent closure. Repeating the command re-observes its exact acknowledged custody without starting another role, admitting a successor or rewriting the historical verdict. Dirty retained source, publication state, failure classification and original repair allowance remain intact. Workspace dependency cleanup uses the declaration recorded when the runtime acquired custody, so permitted package script edits do not invalidate deletion of those same owned folders; replacement folders and symlinks remain protected.

After that closure, use the public `repair-admission-preflight --id RUN --request FILE` and `continue-repair --id RUN --request FILE` route with `continuation_kind: "stopped_delivery_resume"`, a stable unique `command_id`, the closed protocol `expected_revision`, `expected_iteration`, and current `expected_candidate_id`/`expected_candidate_head`. A passed implementation stopped with exact unknown prepublication cleanup resumes at the sealed iteration's checks with `additional_iterations: 0`. Its exact durable implementation result and unchanged candidate remain mandatory; all publication, independent review, QA, verification, required CI and tracker gates still run.

A provider monitor lost after launch remains an unknown verdict. For the narrow original unpublished implementation checkpoint, public cleanup can independently authenticate finished teardown without completing monitoring or inventing a provider session. The closed role, durable supervisor result, exact private request/journal hashes, input candidate, current partial output, provider identity and unknown outcome are bound separately. A later explicit `stopped_delivery_resume` must grant one or two unused original iterations (`additional_iterations: 1` or `2`), starts at the next iteration in a fresh implementation home/session, and receives no invented model finding from the old infrastructure failure. The old result, role and monitoring state remain unchanged. Subsequent normal resumes retain only those exact historical teardown receipts through the immutable admission chain; a different unknown role, changed output/evidence or current live custody still blocks them. Unknown external effects are never replayed.

Later published and unpublished cleanup rederive those exact historical teardown receipts from the retained admission chain. They check the same journal hashes and current actors/ports throughout observation, finalization, acknowledgement and replay, and record historical teardown provenance separately. Later completed roles and a published PR never turn an old unknown provider outcome into a completed assessment; a different unknown journal receives no exception.

When ordinary maintenance records a later compact cleanup projection, its complete contiguous chain retains the published anchor's exact historical teardown and native journal observation. Replay checks those original journal hashes against fresh custody and the latest cleanup receipt; a changed anchor, foreign write, revision gap, tracker change or unbound receipt cannot supply replacement authority.

Completed reservations can include a native role-preparation failure before any monitor or provider launched. Such a reservation intentionally has no registered process journal. Aggregate cleanup accepts this case only with the supervisor's original typed `prelaunch` result, exact private admitted request/job/input/workspace, no provider identity/session/usage, released existing owner and lifecycle leases, and absent journal/launch/start/result/cancellation evidence. It creates no missing evidence. The cleanup anchor separately binds the observed request/result/attempt and lease generations; finalization, claim acknowledgement and replay revalidate them, including later compact maintenance. A missing journal, null session or failure summary alone grants no exception. All registered processes, roots, live actors, ports and closed-execution guards remain required; the old blocked result and any separate finalizer exception remain unchanged.

If that admitted quarantined continuation closes during `repair_preflight` before entering its next iteration, public cleanup first closes that exact later execution. A fresh `stopped_delivery_resume` request then uses the later closed protocol revision and unchanged candidate/iteration with `additional_iterations: 0`. It retains the pending next iteration and maximum already sealed by the earlier admission, even when the workflow has not advanced its iteration counter yet. The immutable predecessor admission, unchanged roles/usage/attempts/effects/source, absent next-iteration attempt, absent session and exact unused implementation home must still match. A new role, source change or a request to enlarge that pending authority refuses reentry. The old failed preflight and original unknown verdict remain intact in their separate closed records.

An admitted ordinary native iteration can also stop before its provider turn when a configured dependency prerequisite launches and fails. After repairing setup, a fresh zero-additional stopped resume retains that same unused provider iteration and ceiling. It requires the one closed blocked preparation role, its unchanged role prefix/source/attempt/effect/session inventory and no provider attempt at that iteration. The last failed command must match an original configured non-test prerequisite; all retained helpers must have completed monitoring, drained output and confirmed teardown without interruption, cancellation, timeout or port conflict. Exact private journal, launch and output hashes are sealed with the pending turn and rechecked at admission and worker/resource readback. This bounded path accepts at most 64 helpers with each private evidence file at most 4 MiB. A failed product gate, unknown helper or changed receipt supplies no exception. Completing a command in a stopped checkout does not adopt newly created environments or rewrite its closed preparation role.

Those historical receipt proofs retain exact canonical workspace metadata after its checkout custody ends. They do not grant current source access: admission and worker readback still independently require the registered physical source and exact candidate, and refuse missing or replaced current checkouts.

An ordinary stopped file-scope amendment retains its original accepted base commit
when the configured named branch advances. The public request cannot override that
commit or acquire new main-branch metadata. Re-admission authenticates the same
original source, origin, repository and publication branch, requires the original
commit still to exist and to be an ancestor of the current named branch, and
derives policy from that original commit and the exact private configuration delta.
It never rebases the retained candidate or changes the two explicitly named files,
session, finite allowance or required gates. Preparation and current worker
readback recheck the branch/commit and registered physical checkout; rewritten
history, a missing commit, changed branch or replaced source refuses. Historical
custody remains readable without current source refs. Fresh submissions still
require the configured ref to match `expected_base_sha` exactly.
The ordinary recovery observer rejects Git replacement refs and legacy graft
files, and reads its frozen commit/tree with both replacement and graft views
disabled. A synthetic ancestry or substituted old tree cannot authorize recovery.
For an acknowledged combined scope/Temporal preparation, historical custody
reconstructs only the exact scope/configuration delta over the sealed original
spec and rebinds its original native proof. It retains every original Temporal
metadata, tool, evidence, command and transition check without re-admitting against
current branch refs. That historical receipt grants no source access: current
proposal and worker readback still enforce the physical graph and checkout.

Native predecessor observation resolves resources through the current acknowledged execution and requires the exact historical predecessor to be a member of its authenticated execution bindings. Both the bindings and current gate namespaces are rechecked after observation. Supplying an old spec as current authority, an unrecorded historical graft, changed configuration/admission/evidence, or authority drift during observation remains a refusal.

The local CLI can execute reviewed cleanup against already closed runs without starting or replacing the service. New continuation semantics must not be queued to an older worker: its custody/readback guards intentionally refuse them. The bundled lifecycle has no rolling-upgrade or launch-quiescence operation. Let every open native workflow finish all gates and cleanup, confirm no eligible automatic continuation or uncertain effect remains, then use the supported runtime update/start sequence before preflighting and admitting these continuations. A gap between native roles does not establish that drain.

A terminal finalizer exception records structured unknown cleanup without a receipt
path or hash. Later owner maintenance and the same public cleanup command preserve
that exact closed result. They archive the freshly authenticated current resource
bytes under `resources/cleanup-observations/<original-result-digest>/`, with an
`observed_resources` inventory and `producer_receipt_source: not_recorded`. These
are later observations; they do not reconstruct an original finalization receipt
or claim the file was historically absent. Replay authenticates the first saved
observation after canonical cleanup files advance. Partial/malformed declarations
are refused, and every declared hash retains its exact validation. Fresh cleanup
and acknowledged claim release still leave the original failure, CI verdict,
candidate, PR, policy and repair budget intact.

After a macOS reboot, legacy native/resource records may retain the same inode and
owner while their mount device number changes. The local public CLI exposes
`filesystem-custody-preflight --id RUN` and `adopt-filesystem-custody --id RUN --request FILE`,
both with an explicit `--config CONFIG`; neither starts a service. Preflight returns
`expected_revision` (the local projection revision), `expected_evidence_sha256`,
the exact affected objects and current workflow identity. An explicitly authorized
operator sends those two observed fields with a fresh stable `command_id` and
`answer: "adopt_current_filesystem"`. Keep the complete request unchanged after an
uncertain response.

This grants current-object custody only. Legacy records have no historical volume
UUID, so the receipt explicitly leaves historical volume continuity unproven. It
archives original bytes and commits one immutable exact-path identity overlay in
the existing command/event transaction. An unacknowledged file grants nothing.
The grant requires only device-number drift, unchanged inode/owner, canonical
owned paths, original registration kind/generation/source authority, acquired
resource/attempt/native leases, stopped recorded PID/start identities and clear
ports. An extant path whose prior receipt records removal is recreated and cannot
be adopted; an absent root is skipped, and a new generation uses its own current
identity. Fresh source, evidence, run/attempt/claim and uncertain-effect checks fence
publication. Current filesystem UUID and boot UUID must remain identical when
the grant is consumed; replacing a path, changing generation or inspection failure
cannot inherit it. New resource allocations still record real current identities.
Original policy, plan, claims, provider verdicts, repair ceilings and external effects
are untouched.

An open ordinary workflow is eligible only with durably quarantined unknown
attempts and no starting/running attempts or uncertain external effects. Adoption
itself neither completes nor repeats an attempt. Follow it with the existing
`reconcile-attempt --job-key KEY` for the exact quarantined attempt; its original
waiting activity can consume the independently observed cleanup and stop normally.
A lost provider monitor remains `recovery_unknown` with an unknown provider outcome,
never a fabricated pass or recovered session. Cancellation is not a prerequisite.
Read the resulting run and use only an admissible existing continuation; this
operation grants no provider retry. For a closed published run whose roles are
finished, the existing `reconcile-attempt --id RUN --role cleanup --iteration N`
continues to own finalization and claim closure, including default workflow IDs.

A confirmed removed dependency directory can bind a newly tracked declaration when the runtime recreates that directory. Renewal requires the old path to remain absent and its removal receipt to exist; live or replaced folders retain their original custody checks. A fixed-budget ordinary run that stopped before any provider attempt can continue its exact unstarted iteration with `additional_iterations: 0` after preflight authenticates unchanged source, session, effects and admission. An actual provider attempt makes the turn consumed, and this path cannot increase the original repair ceiling.

An owner scope amendment also supports versioned directory scopes. It grants only the one or two named exact files. A protected prefix can be removed to expose such a file only when no pre-existing allowed root or file would acquire access under that prefix. Every other execution setting stays frozen.

`recovery_key` imports only a configured manifest. `supersedes_run_id` can transfer an explicitly identified blocked unpublished predecessor to a new branch after closed Temporal result, claim, candidate, session and manifest validation. No session exists before a role. Post-role continuation carries the exact accepted plan and session data, regenerates credentials/permissions, and preserves predecessor history. A clean transient generation may be recreated for a separately authorized finite continuation while its durable session stays intact.

The owner maintenance sweep resumes interrupted `FAILED`, `TIMED_OUT` and `TERMINATED` controllers, including admissions from before the workflow failure boundary. It authenticates and seals the closed history, then uses Temporal's history reset to retry the interrupted controller task or failed activity. If the history proves that no workflow task ever started, it restarts those exact inputs instead. Every closed execution keeps its own sealed history, and readers select the latest authenticated recovery without substituting an older result. Completed activity results before a reset's resume point, original inputs, approval, native journals and application repair charges are preserved. This is not a new issue admission or a repair-budget extension. A lost reset response is resolved through the server's reset lineage, and never authorizes resetting an unrelated newer execution. Cancelled, retired, superseded or uncertain native executions remain fenced. Recovery is bounded to three attempts on unchanged runtime code; a persistent failure is recorded in the run's event history for correction. The original Temporal history must still exist on the server for a history reset; a saved closure alone does not recreate an expired server execution.

Historical Docker run records and their evidence remain readable. Their execution and recovery paths are retired: mutation commands reject them before enqueueing work, pending or unknown historical outbox entries stay untouched, and they are never silently resumed or migrated. The dashboard labels these runs read-only and keeps saved questions and plans visible without response or cancellation controls.

## Lifecycle and public interfaces

Use dedicated loopback ports that do not conflict with the target project. The runtime starts and owns a local Temporal dev server, one worker, and one dashboard/API process. It stores process identity in the private state root; status distinguishes a dead or replaced process. Build the UI before start. The Temporal dev server is for a local experiment and is not a production deployment.

A failed process-identity observation is unknown custody, not a dead process.
Lifecycle commands report that refusal and retain the process inventory and
worker registration until observation proves exit or a different generation.
`status` reports each known member normally and an unobservable member with
`running: null` and `observation: "unknown"`, then exits unsuccessfully. A mixed
`stop` still settles independently matched members and reports every member's
result; any `observation_unknown` result makes the command unsuccessful and
retains the inventory for reconciliation.
They never signal a PID whose recorded identity could not be matched. An
exited leader is reaped only after its matching identity and settled process
group are observed, preserving custody while its descendants remain alive. An
explicit `stop` records its intent before settlement, so periodic supervision
cannot restart a partially stopped stack while visibility recovers. A retained
`service-stopped.json` records that request, not proof that every member exited;
restore process visibility and repeat `stop` to finish settlement.

CLI mutations and MCP write tools automatically ensure that this stack is running before requests. Read-only service, run, evidence and preflight calls connect only to an existing API and report unavailable if it is stopped; they never start or recover the stack. CLI reads also leave an absent state root untouched. Concurrent callers and explicit `start`/`stop` serialize through one private state-root OS lock. A healthy stack retains its processes; stale or partial owned stacks recover without erasing state or killing foreign listeners. Readiness requires an owned loopback API, current Temporal health, and the owned worker's workflow and activity pollers. `status` and `stop` never start the service. Startup has a 30-second deadline, configurable with positive `service_start_timeout` up to 120 seconds, and failures include log paths. Application HTTP errors do not trigger recovery and dispatched mutations are never automatically replayed. Startup, service reads, preparation, roles and checks require no Docker executable or daemon.

```sh
uv run --frozen devflow-delivery --config /absolute/private/config.json start
uv run --frozen devflow-delivery --config /absolute/private/config.json status
uv run --frozen devflow-delivery --config /absolute/private/config.json submit --request /absolute/private/submit.json
uv run --frozen devflow-delivery --config /absolute/private/config.json run --id local-001
uv run --frozen devflow-delivery --config /absolute/private/config.json restart
uv run --frozen devflow-delivery --config /absolute/private/config.json stop
```

Upgrade the runtime in this order: `stop`, update the checkout (`sh scripts/update.sh TAG` or `git pull`), `uv sync --frozen --python python3.12` in `runtime/`, then `start`. The long-lived API and worker import modules lazily, so a checkout that changes underneath them would mix two versions in one process; `scripts/update.sh` refuses while a worker whose readiness marker names that checkout is alive (`scripts/delivery-workers.py --runtime runtime` reports them), and names the `stop` command. `stop` never requires the checkout to match the worker source recorded with a `--deployment-name`/`--deployment-build-id` selection: stopping a drifted stack is the safe action, each member is still proven owned before it is signalled, and the selection is retained for the next start. Ensuring the service compares the recorded worker source only when it must launch a member from the current source; a ready stack is served as it is. Member identities are recorded independent of the host's locale and time zone; manifests written earlier keep proving ownership in their original form.

An unversioned stopped stack with no retained worker deployment uses ordinary
`start` after the clean source update. If stopping retained a versioned deployment
selection, ordinary `start` preserves that selection and requires its old payload;
select the same deployment name and a new immutable build ID explicitly on
`start --deployment-name NAME --deployment-build-id BUILD` only after the process
inventory is empty. A same-root clean source upgrade retains its installed-location
acknowledgement and captured history. It does not repeat the location migration.

Public run reads authenticate the complete current installation acknowledgement
at every normal Store connection, including fresh original-row, cohort, owner and
exact two-name guard checks. Opening its read-only database connection does not
decode the immutable receipt twice. Retained-worktree readback derives admitted
gate roots once under its resource observation lease, then revalidates that
authority before returning; no observation survives the call. The run detail and
its evidence index share the same already observed detail within one GET.
Standalone evidence reads still observe fresh detail. Neither this reuse nor the
read endpoint changes authority, repair ceilings, history, or the ordinary
30-second client deadline.

Nothing restarts the stack by itself after a reboot or a crash: only CLI mutations and MCP write tools ensure it. `python3.12 scripts/delivery-service.py install --config /absolute/private/config.json` stages a per-configuration launchd agent (`com.ebarti.devflow.delivery.<digest>`) under the private state root (`launchd/`), where launchd never reads it; `--activate` copies it into `~/Library/LaunchAgents` and bootstraps it, restoring the previous agent if that fails; `uninstall` boots it out and removes both copies; `inspect` reports whether the activated copy lags the staged one. The agent runs `devflow-delivery --config … supervise` at login, once a minute (`--interval`) and again after a failure, in the standard process band (a background band would throttle Temporal, the worker, the API and every role and check they run). `supervise` is the idempotent start under the state-root lock — a ready stack is probed and left alone, a dead API beside a registered worker is relaunched alone, a dead stack is started — except while the owner's `stop` stands: `stop` records that intent in `service-stopped.json` in the state root, and the next explicit `start`, `restart` or write-triggered start clears it. The agent records the installing shell's `PATH` plus the `devflow-delivery` and Temporal CLI locations, the runtime checkout it supervises (derived from the canonical launcher's journal or wrapper, or from the entry's own `.venv`; `--runtime` names it explicitly), and logs to `launchd.log`/`launchd.error.log` in the state root. `scripts/update.sh` also refuses while an activated agent supervises a stack from the checkout it would change — by its recorded runtime or by the checkout its entry point execs now — or one whose checkout it cannot tell, or a plist under the Devflow label that Devflow does not own (`scripts/delivery-service.py active --runtime runtime` reports them): uninstall it, upgrade, then install and activate it again.

The API loads the configuration once; after an edit it refuses new admissions with a 409 that names `restart`. `restart` reloads only the API when the stack is ready and the running worker registered the same transport (tracking database, state root, Temporal address, namespace, task queue) that the edited file names, so roles in flight continue; a transport change, a worker that registered no transport, or an incomplete stack goes through the ordinary stop and start. The edited configuration is validated the way the API would start on it (address shape, a free port, dashboard bundle, store construction) before the running API is stopped, and when the API is the only missing member beside a worker registered on the same transport, `restart` brings back just the API. Ensuring the service never reloads a member on its own: a stale API keeps answering, a moved dashboard address is reported with the `restart` remedy, and an API that died beside a registered worker is relaunched alone only onto the transport that worker polls; otherwise the `restart` remedy is named instead. Admitted runs keep executing from their sealed configuration snapshot regardless of later edits; `start` seeds snapshots for runs admitted under the live configuration and names any run whose admitted configuration is no longer available.

The local dashboard, CLI and MCP require no login credential or token file. Reads and SSE work without cookies. Commands automatically bootstrap or renew an anonymous HttpOnly, SameSite=Strict cookie solely for CSRF protection; exact loopback peer, Host and Origin checks remain enforced. Cached clients may still POST the old session request, but its token has no authority. Existing token files are left untouched for cached callers. The dashboard serves `/`, `/new`, `/settings`, `/statistics`, and `/runs/{id}` with the built static assets. It renders authoritative phase gates, roles, candidate and PR identity, checks, tracker status, usage unknowns, decisions, and durable events. SSE resumes from its event cursor and shows stale state on disconnect. No display refresh calls a model.

The default Runs page is a Kanban board of authoritative phases, refreshed every five seconds. Settings and Statistics use the full content area without the recent-runs rail. `GET /api/runs?archived=true` lists archived tasks. Same-origin `POST /api/runs/{id}/archive` takes `{command_id,expected_revision,archived}` using the observed **projection revision** and allows reversible archiving only after the run and all role attempts stop. Evidence, claims and statistics remain intact.

Same-origin `POST /api/runs/{id}/steer` takes `{command_id,expected_revision,message}`, also using the projection revision. Messages are durable and bounded (4000 characters each, 16000 per run). They enter subsequent role launches within the accepted scope and frozen permissions, models, required checks and endpoint. An active role continues unchanged; each launch freezes its notes so replay and recovery cannot silently change its input. History distinguishes queued notes from inclusion in launch input, without claiming model compliance. Final QA and terminal work reject new steering; pending plan decisions use their existing answer controls. Dashboard commands retain the same exact command ID/body after an uncertain response and reject stale revisions.

`GET /api/statistics` includes every durable run, including archives, failures and cancellations. New admissions record Git release/revision and a local-change digest; historical unknown identities remain unknown. Providers form separate cohorts so simulations cannot stand in for native delivery. Cohorts show terminal sample size, success rate (delivered/all terminal), first-pass deliveries (no repairs, recovery or superseded predecessor), repair attempts, median elapsed time including waits, and observed tokens/cost with coverage. Missing telemetry remains unknown. Differences describe samples rather than causal improvement. `/api/service` derives its version from the running source identity instead of a fixed development label.

The CLI, HTTP API, and official MCP stdio server use the same service. To configure a local MCP client, run `uv run --frozen devflow-delivery-mcp --config /absolute/private/config.json` as its command. It exposes `get_service`, `start_service`, `submit_run`, `list_runs`, `get_run`, `read_evidence`, `answer_decision`, `cancel_run`, `reconcile_tracker`, `gates_only_preflight`, `admit_gates_only`, `repair_admission_preflight`, and `continue_repair`. The MCP client uses the same automatic anonymous CSRF bootstrap. HTTP has `GET /api/service`, `/api/runs`, `/api/runs/{id}`, indexed evidence and cursor-replay events; `POST /api/runs`, `/decision`, and `/cancel` require the session and CSRF value. Indexed evidence reads are contained to the run's owned state. The CLI request file is an alternative to browser writes.

Newly admitted runs freeze `publication_readback_version: 1` and the service's
`publication_readback_seconds` (default 900; integer 30–3600). If initial publication
completion is lost, the workflow inspects that existing publication effect and its
owned branch/PR; it never repeats commit, push, or PR creation. During reconciliation,
unavailable read-only queries and delayed PR-head projection wait on durable backoff
within this deadline. Cancellation wakes the wait. Observed ownership, candidate or
unexpected-head conflicts stop reconciliation. Before any remote mutation, validation
failures or unavailable prepublication queries stop immediately with a known no-effect
rejection; this does not establish a remote rejection. Query diagnostic text does not
determine whether an identity conflict occurred. A failed mutation process
cannot prove remote rejection from its exit status or stderr, so its original effect
is still read back without repeating the mutation. An exhausted deadline or cancelled unresolved publication
retains unknown cleanup, resources and the work claim for inspection. Scope amendments
preserve the original version/deadline, including their absence on historical runs.
Historical workflows retain their previous commands; this policy does not resubmit
issues or restart implementation against a fresh base.

A push can succeed before GitHub's PR-head readback updates. New runs keep the publication effect pending and retry the read-only head check on durable Temporal timers; they do not commit or push again, and they do not start review until the owned branch and open regular PR show the expected commit. When the branch is confirmed at the pushed head and no PR uses it, the reconciliation opens the PR for that exact head (GitHub refuses a second open PR for one head and base, which reads back as the same publication). Transport failures before the push (`gh`/`git` readback errors, timeouts) are confirmed pre-mutation and retried, first by the activity's bounded retry policy and then by the workflow within the publication readback window; a window that ends without a PR stops the run with the stable reason `publication unresolved: readback deadline exhausted` (or `remote unavailable before push`), classified as a transient `publication_deadline`. A run previously blocked at this exact boundary may use same-origin `POST /api/runs/{id}/recover-publication`, or `devflow-delivery recover-publication --id ID --request FILE`, with `command_id`, the observed `expected_revision` and `expected_candidate_id`, the pushed `expected_head`, and `expected_pr_number`. There are two shapes. With the PR number, the service requires a closed Temporal predecessor stopped at publication, passed prepublication checks, confirmed role cleanup, the retained work claim, one bound publish effect that is pending or has a validated completed receipt, an unchanged checkout, and matching local, origin, and PR heads; it records a single same-run continuation and resumes at independent review and the remaining gates. With `expected_pr_number: 0` (the pending-publication path, which the service loop also admits on its own after a classified `publication_deadline` stop), the run must be closed and blocked at publication with confirmed cleanup and a released claim, and its checkout must hold exactly the stopped iteration's passed implementation: still uncommitted, committed once on top of the head it was checked at, or pushed (`expected_head` is the checkout head). At most one pending publish effect may exist for that iteration, bound to that head and to the owned PR if one exists; origin must show the pushed head or still the head the iteration started from; and the owned PR, when the run already published one, must be that PR at either head (a first publication may show the PR a lost attempt created). It is admitted at most three times per run, finishes the publication from wherever it stopped (commit, push and PR creation are idempotent on the bound effect) and resumes at independent review without an implementation turn. The old execution and its failure remain visible in the timeline; a retry of the same command is idempotent. A changed branch, PR, candidate, claim, or ambiguous cleanup is a conflict requiring inspection.

After a published run exhausts its configured repairs on a specific failed review, broker check or required CI gate, an operator may grant one or two additional iterations with same-origin `POST /api/runs/{id}/continue-repair` or `devflow-delivery continue-repair --id ID --request FILE`. The request contains `command_id`, `expected_revision`, `expected_iteration`, `expected_candidate_id`, `expected_pr_number`, `expected_pr_head`, and `additional_iterations` (1 or 2). This initial grant is one-time and cannot exceed the original `max_repairs + 2`; it does not change the run's frozen policy. Admission reads the closed Temporal result, current failed-gate diagnostics (including a bounded, hashed failed CI job-log excerpt where needed), retained claim, confirmed native process and resource cleanup and exact open PR/remote head. It queues a new execution of the **same run** with the original implementer session and leaves prior failures visible. If GitHub or tracker readback is temporarily unavailable after the grant, the queued execution waits and retries under that same grant, including across worker restarts; no repair role starts until authority is confirmed. The resumed implementation must still pass prepublication checks, publication readback, independent review, final checks, browser QA when configured, verification, required CI and tracker reconciliation. A changed authority, uncertain cleanup or missing actionable failed-gate evidence is a conflict. No extension occurs automatically when the default budget is exhausted. A stop that the controller classified as transient (a lost gate activity, a controller failure, a CI observation deadline) is not a failed gate, so no repair iteration is spent on it: the service loop re-admits such a published run itself, on the unchanged candidate and PR head, as a `published_gate_retry` continuation (fresh local checks, review, QA, verification and CI; at most 2 per run, after a persisted backoff) or, for a CI deadline or a lost CI observation, as a `published_ci_retry` observation (at most 8 per run while the head is unchanged). The same admissions accept an operator's command. `stopped_delivery_resume` on such a stop never hands the transport or controller error to the implementer: when the PR already shows the stopped candidate (same head and same checked content) it re-runs the gates on it; when the candidate was never published (a repair whose publication was lost before its push) it resumes at prepublication checks and publishes the candidate into the existing PR before any gate, so review, verification and CI only ever run on the delivered head; an interrupted repair turn resumes with the recorded findings it was repairing. A stop that recorded unknown cleanup (a lost local-check activity, a controller failure) is first finalized again by the service loop once every native process is observed gone, which releases the claim and records a `resource_cleanup_reconciled` event bound to the closed result; only then is it admitted, and never when a newer run owns the issue. A lost CI observation keeps CI observation semantics (`published_ci_retry`), not fresh gates. A recorded gate verdict stands: when a controller failure interrupts the run after review, verification, a broker check or CI already recorded a failure, there is no fresh assessment of the same head; the recorded findings go to the repair turn the run would have taken next, with the turns an unmasked run would still have had (the service loop admits that resume itself; a failed required-CI verdict is repaired from its sealed job-log diagnostics, and without findings no repair turn starts), and once the budget is spent only the operator's one-time published gates retry applies, exactly as after an exhausted repair.

If that resumed implementer was blocked **before any provider process launch**, an operator can retry the same iteration once with same-origin `POST /api/runs/{id}/retry-prelaunch` or `devflow-delivery retry-prelaunch --id ID --request FILE`. The request contains `command_id`, `expected_revision`, `expected_iteration`, `expected_candidate_id`, `expected_pr_number`, and `expected_pr_head`. Admission requires a closed Temporal execution, the finished no-process attempt, confirmed cleanup, the unchanged claim/candidate/open PR and the original repair grant. It seals the preceding review findings together with any currently failed required-CI job diagnostics for that exact PR head; unavailable or incomplete CI readback prevents queuing. The retry uses the same implementer session and existing authorized iteration ceiling. It neither grants another iteration nor treats the CI excerpt as a passing check; all broker, review, verification and final CI gates still run after the role.

If an implementation-only role stops because a required tracked test file was omitted from the frozen edit list, an operator can issue one separate scope amendment with same-origin `POST /api/runs/{id}/amend-scope` or `devflow-delivery amend-scope --id ID --request FILE`. The request binds a unique `command_id`, expected protocol revision, iteration, post-role candidate ID, PR number/head, one or two sorted `added_paths`, and the absolute path and SHA-256 of a private amended service configuration. The amended configuration must differ from the original only in those file permissions; the original request and any earlier repair grant remain unchanged. The service reads the completed Temporal result, finished role receipt, retained claim, existing open PR, exact source candidate, sealed native ownership/finalization evidence and current-head CI diagnostics before queuing. The effective policy and predecessor are recorded separately and visible in run detail. A replay of the same command returns its receipt; a second amendment conflicts. The sole new turn resumes the original implementation session, then must pass every broker, independent role, CI and tracker gate. This command cannot convert a failed test or model assertion into a passing result, and it does not authorize arbitrary new runs or further repair turns.

For a finalized stopped implementation with a released claim, the existing `stopped_delivery_resume` request to `continue-repair` can instead include `added_paths`, `amended_config_path` and `amended_config_sha256`. This works before publication and spends only unused original repair iterations. It requires the authenticated original session's completed `findings` receipt or a completed native `blocked` assessment with nonempty findings, together with the unchanged partial implementation. A blocked assessment must retain an exact private request, raw result, process journal, launch/start records and typed SDK observation: the final turn completed without an error, its structured final assessment agrees with the enriched supervisor result, the input candidate and retained output are separately bound, and native teardown is confirmed. Missing, interrupted, unknown or mismatched evidence refuses the amendment. Admission records hashes of those original bytes and worker custody reauthenticates them; it preserves the original blocked verdict and session rather than rewriting the result. The exact operator file delta grants edit authority without classifying finding prose. One or two explicitly named files must already be tracked, remain unchanged, and have safe filesystem ancestry. The private amended configuration may differ only by appending those files to that repository's allowed paths or versioned exact-file source scope. Preflight validates without preparing or admitting work. Admission retains the original request, accepted plan, failed result, session, endpoint, models, checks and budget; it records the effective file scope separately in the recovery receipt, event and run detail. Runtime preparation and subsequent readback recheck the sealed configuration. The original session then repairs the finding through the normal implementation, review, QA, CI and delivery gates. This path does not waive a test failure or authorize a fresh issue admission.

The completed blocked assessment uses the same last typed `final_answer` item as the native turn producer. Earlier terminal-labelled stream items cannot replace it. A malformed or disagreeing last assessment refuses; the verifier never searches backwards for parseable JSON.

Later plain stopped resumes retain that scope amendment's original provenance in run detail. The amendment receipt describes its original admission; each continuation separately records its own remaining-iteration authority.

A closed ordinary trusted-local delivery may separately acknowledge one browser setup correction on an existing `stopped_delivery_resume` request. Its optional `browser_qa_amendment` object contains `expected_recipe_sha256` (the original canonical browser recipe digest), `recipe_path` and `recipe_sha256` (exact proposed JSON bytes), and `amended_config_path` and `amended_config_sha256` (exact private configuration bytes). Both input files must be owned regular mode-600 files inside the service state root. This amendment permits only `browser_qa.env.JOBCTRL_E2E_ISOLATED` changing from the string `1` to `0`. Fresh registration still requires mode `1`; the operation cannot change argv, selected scenarios, ports, timeouts, minimum counts, rejection rules, artifact paths, source scope, models, transport, or any other configuration. It uses only explicitly requested unused original iterations and cannot combine with a file-scope amendment or grant another allowance.

The original completed native browser failure must contain the two selected JUnit cases, one passed and one skipped, with the exact recipe, request, launch, process journal, log, retained artifacts and confirmed teardown. Every matching recorded failed browser effect, including earlier iterations, is authenticated and preserved separately from the new gate. The accepted command and immutable admission bind the original case identities, failed bytes, configuration and proposal hashes. Custody rechecks the original attempts/effects and authenticated lineage before worker readback, resource use and native browser launch. Later plain continuations inherit that same acknowledged recipe; they cannot graft another run's permission or substitute evidence.

Mode `0` selects the existing production API/RPC test backend while retaining the reviewed owned synthetic workspace and fake action/transport setup. The amendment pins the Playwright configuration, owned E2E fixture tree, API bootstrap/fake-dispatch owner and synthetic seed source bytes, and requires the same bytes in the gate checkout. The native launcher retains its existing credential-free owned HOME, XDG cache, temporary root, exact ports and filesystem/network profile. A new gate must retain passing JUnit entries for **both original selected cases**; aggregate `min_tests: 1`, collection output, or exit zero alone cannot pass it. Any missing, skipped, renamed or failed case fails the normal browser gate with truthful cleanup. The earlier failure remains failed, and independent review, verification, required CI, tracker and cleanup gates remain mandatory. Install the corresponding custody/launch readers after a full native drain before admitting this new receipt shape.

Ordinary scope and browser amendments keep scheduling capacity separate from frozen execution authority. For example, a run admitted with `policy.capacity: 6` retains that value and its original configuration snapshot after the canonical live service owner lowers the operational limit to `2`. A private amendment must contain that exact live value `2`; it cannot restore `6`, choose another value, or appoint its own file as the operational owner. The runtime derives the owner path/configuration from the original submitted run, requires the same transport and otherwise unchanged original owner authority, and records hashes of the current owner bytes in the immutable amendment. It rechecks that acknowledgement around preparation and during custody/worker readback. Missing, replaced, incompatible or changed owner evidence refuses the continuation. Native role scheduling resolves that original owner independently of the private execution path, while effective policy capacity remains `6`. This compatibility rule changes no global configuration and adds no model, repair, gate or delivery allowance.

A separate ordinary `stopped_delivery_resume` may declare `temporal_test_prerequisites` with exactly `check_id`, `python_project`, `provenance_path` and `provenance_sha256`. It may compose with either the explicitly acknowledged file-scope amendment or the exact browser-mode amendment in the same public continuation, sharing one immutable command and the same original finite allowance. File-scope and browser-mode amendments remain mutually exclusive; each individual authentication and delta guard still applies. It adds no eligibility, repair iterations or gate retries: the original closed execution, own released claim, candidate, session, external effects and unused finite allowance must satisfy the normal public preflight. The declared check must remain an exact frozen native test command, including its Node wrapper, cwd, selection, timeout, JUnit and minimum count. The explicit Python project is `workers/automation`; its tracked `pyproject.toml` and `uv.lock` must match the admitted Git base and the provenance's SDK1.26.0 metadata. The runtime never interprets JavaScript to discover authority.

The mode-600 provenance JSON must be an exact declared hash under the owned service state root. The supported initial tools are the official Temporal sdk-java1.32.0 macOS-arm64 test-server archive and CLI1.9.1 macOS-arm64 archive, with pinned publisher archive digests, exact safe members and executable hashes. Preparation validates those facts and every declared retained evidence file, records a distinct immutable admission/preparation lineage and rechecks it during worker/resource custody and before launch. It streams the two executables into a fresh owned check scratch generation and supplies only `JOBCTRL_TEMPORAL_TEST_SERVER_PATH` and `JOBCTRL_TEMPORAL_CLI_PATH`; caller environment variables and undocumented host caches cannot supply this declaration's paths. The existing resource owner seals each preparation under its admitted evidence/check key, registered Git subject and full candidate matching the current projected run candidate and check iteration, current own claim generation and canonical issue/repository binding, environment recipe and scratch generation. The immutable generated receipt also binds those complete owner inputs. It remains inert until that independent owner seal is recorded; an acknowledgement-loss replay cannot adopt a changed claim or resource generation. Launch rederives the subject and scratch from those owners, and refuses a rewritten receipt, replaced scratch generation, foreign working directory, candidate drift or replay into another check. Exact tool, scratch, metadata, frozen command, environment and native launch bindings remain mandatory. Original failed receipts remain failed. Bounded SDK setup probes and collected tests cannot pass a full worker gate; fresh retained JUnit and every unchanged count/rejection/review/QA/CI rule still apply. This affects only future eligible native launches after a closed continuation, and changes no global executable, PATH, cache, service environment, capacity, source scope, model or deadline.



### Permanent installation location recovery

An executable service belongs outside managed chat checkouts. Prepare an independent clean Git clone at `~/.local/share/devflow/developer-workflow`, with its own frozen environment, retained Git objects and the same original base Python, dependency lock, SDK and bundled CLI bytes. Its imports, entry points and dependency registrations must resolve to that permanent installation; the resolved original base interpreter remains pinned. Chat worktree cleanup can remain enabled.

The standalone public commands `runtime-location-preflight`, `adopt-runtime-location` and `rollback-runtime-location` run from that installed environment with the **original owner config file path**. They route before ordinary Store bootstrap, so the original executable and helper target may already be absent. `stop` also remains available without loading a missing helper. Stop and drain the original delivery actors and publicly uninstall its own delivery launchd agent before adoption. ProjectSync retains its separate process, registration and shared database.

The private request names `command_id`, `owner_before_sha256`, `owner_after_path`, `owner_after_sha256`, `destination` (exact imported `root`, `revision`, `tree`, `payload_sha256`), the complete named `runs` cohort, `host_home`, the canonical public plugin `host_cli` and `host_cli_sha256`, `baseline_revision`, and the explicit answer `Migrate this stopped installation; preserve every original run and execution allowance`. The proposed private owner JSON must live under the original state root's `operator-inputs/` and differ only in `codex_bin`, pointing to the actual permanent environment's bundled CLI. The original live capacity stays2; frozen historical capacity and every other source/model/gate/transport/budget field stay exact. `baseline_revision` authenticates the unchanged shared `state.py` and schema against the retained original source. Existing sealed COMPLETED histories, native journals, current source, claims, effects and every original run authority are observed before any pointer change.

```sh
~/.local/share/devflow/developer-workflow/runtime/.venv/bin/devflow-delivery \
  --config /absolute/original-owner.json runtime-location-preflight \
  --request /absolute/service-state/operator-inputs/runtime-location.json
# Add the returned expected_evidence_sha256 to the same private request.
~/.local/share/devflow/developer-workflow/runtime/.venv/bin/devflow-delivery \
  --config /absolute/original-owner.json adopt-runtime-location \
  --request /absolute/service-state/operator-inputs/runtime-location.json
```

Preflight does not publish config snapshots or migrate the database. Apply retains the exact original owner bytes and one immutable common receipt. Exact forward/reverse wrapper, physical helper-link and owner-file generations are sealed before the pending cohort transaction. File-only staging grants no execution authority. Pending intent fences new execution-critical code; existing old ProjectSync code can still write unrelated tracker data. Native/claim/source authority is revalidated under the native/resource/installation leases and final SQLite transaction. The original primary plugin changes only through its public management CLI with unrelated-state and exact-package checks. An ambiguous public plugin effect is observed before any further step and is never blindly repeated.

A previously installed package can legitimately retain an older canonical skill.
The migration does not upgrade that text. An explicit optional `plugin_origin`
witness uses schema `devflow.original-plugin-package/v1`, the full40-hex
`source_revision`, exact currently registered `marketplace`, `catalog_sha256`, and
`files_sha256` for all three Codex package members. The named source must be an
ancestor of the request's authorized `baseline_revision`, which must itself be an
ancestor of the current installation. Git data reads ignore replacement objects
and refuse graft interpretation; no old code runs. This bounded version requires
the same packaging/layout owner and manifest, with the exact historical skill
blob. Source provenance supplements protected complete catalog/package/cache
custody and exact public enabled/installed identity, version, source and original
stdio/configuration; it cannot bless an arbitrary skill edit or extra member.
Cache inventories include directories, so an unregistered empty directory also
refuses. The destination copies all authenticated original bytes and changes only
the MCP executable command to the derived permanent entry. Original bytes,
inventories, generations and witness are retained in the common receipt and
rechecked through public effects, replay and rollback. Current packaging templates
remain unchanged. Before each public plugin effect, the complete intended target
package and catalog must exist with the sealed bytes and exact member set; current
new registrations also revalidate that source independently of the cache. Pending
rollback needs only its original target when no forward package was published.
A fresh migration input may consume an already acknowledged
historical capture; it never rewrites that capture's archived input or recaptures
the same execution to adopt a different program source.

The common receipt identifies shared physical program, interpreter, dependency,
source and filesystem objects explicitly. Each run separately retains its full
measured native identity, including its original browser read roots, toolchain
roots and package-manager cache authority. A cohort may contain different frozen
read scopes; the installer neither merges them nor copies one run's grants to
another. Capture, migration acknowledgement/replay and late preparation,
execution and lifecycle readers revalidate the shared objects and each applicable
run binding. Native launch identity and complete policy/configuration checks
remain strict. Previously acknowledged full-identity receipts retain their
original strict comparison behavior.

Legacy delivery MCP adapters may still hold the original lifecycle implementation. Apply holds the original `service.lock` inode exclusively, commits the entire pending cohort, then durably creates one exact second name of that same inode. Fresh legacy lifecycle calls refuse its two links before changing the stopped marker. The installer then requires an authoritative inode-wide open-descriptor inventory with no foreign descriptor, including descriptors opened by its own process. Any warning, incomplete visibility or foreign descriptor refuses before program/config/registration effects. A refusal or crash before that zero-descriptor observation leaves an **unfinished migration** with unchanged relocation preimages and the guard retained; it does not prove the old service stays stopped, because a previously validated legacy descriptor can still proceed when the exclusive lock is released. Retry reacquires the original lock and revalidates the complete stopped/drained/source/cohort authority. After adoption, current lifecycle commands recognize only the acknowledged original inode and its exact two owned names; the guard stays across starts and rollback. Retiring it requires separate proof that old lifecycle authority can no longer act. A queued host plugin reload acknowledgement alone does not establish MCP readiness.

All named final commands and audit events commit atomically. Adoption changes **no `delivery_runs` column**, including revision, protocol, candidate, result, workflow and `updated_at`; it cannot make a stale running projection look closed or confirm unknown cleanup. For an authentic completed tracker-projection failure, the required order is adoption, normal closed-projection maintenance, public `reconcile-attempt --role cleanup`, then the existing finite continuation preflight. Old failed gates, provider uncertainty and sessions remain historical. Scope or browser amendments can compose with the existing Temporal test prerequisites in that same finite continuation. Strict native measurement and bundled-SDK equality remain required; a retained old proof is readable provenance, never launch permission. Future fresh native admissions use the permanent owner and their own new proof. Later clean committed source upgrades retain the existing native payload-update contract at the acknowledged physical installation.

Replaying the same apply recovers a lost acknowledgement without changing its cohort or object generations. Rollback uses the same private request and is bounded to before any subsequent native admission/attempt; it restores only the exact retained owned registrations and owner bytes, preserves historical acknowledgement and reports the absent legacy runtime honestly. A missing/changed original plugin package, foreign registration, source/config/claim/candidate drift, or unknown public effect refuses rather than replacing that evidence. Restage the delivery launchd agent afterwards through [the existing service installer](../scripts/delivery-service.py) using the permanent `--runtime` and `--devflow-delivery`; service start remains a separate public lifecycle action.

The optional [Desktop entry point](../runtime/desktop/README.md) installs a narrow local skill and MCP command for submitting and opening these same runs. Its guarded installer requires the explicit runtime, private config and Codex-home paths and preserves unrelated entries. New valid requests prepare automatically through the service.

New raw-goal admissions measure configured baseline checks on a separate clean checkout before intake. A fully observed test failure can enter intake as a repair finding only when source and feature identities remain unchanged, native process cleanup is confirmed, and every baseline recipe is retained exactly in both prepublication and final checks. Preparation failures, timeouts, cancellations, rejected output and uncertain monitoring still block before roles. The baseline failure remains visible; it is never reported as passed or used to waive a final check. Implementation must repair it within the configured scope and budget, and all required candidate checks, review, QA and CI must pass before delivery. Stored version-1 admissions and explicit legacy plans retain their original stop-on-baseline-failure behavior.

Local checks, prepublication checks, baseline checks and browser QA share a finite pool of OS-locked slots across workers using the same state root. Service configuration `check_concurrency` defaults to 2 and accepts integers from 1 through 32. Browser QA acquires sorted locks for its existing frozen port set before taking a generic slot, so runs sharing the same ports queue without changing their command or port assignments. Waiting activities heartbeat before admission; each native command starts its own runtime timeout only after admission. Activity cancellation or timeout signals the native monitor and waits for identity-bound process-tree teardown before releasing capacity. An unknown teardown retains its slot and any browser port locks while that worker remains alive. Physical worker death releases its OS locks even if native descendants survive; this change does not establish orphan reconciliation after a worker crash or restart. Operator deployment must resolve outstanding orphan checks through existing native journals and resource ownership before starting workers; these slots alone do not safely authorize deployment over unresolved orphan checks. New workflow histories use heartbeat-aware cancellation options under the `delivery-check-slots-v1` patch; recorded legacy histories retain their original activity commands.

Each clarification and explicitly required plan decision is a Temporal wait with a persisted ID and revision. A wrong/stale answer returns a conflict; a worker restart does not invoke a model while waiting. Cancellation stops at a role/check boundary. An accepted cancellation cannot later become a blocked or successful outcome because a check finishes concurrently. If a child or external effect cannot be proven stopped, cleanup is explicitly unknown. A completed role receipt is reused only for its bound request; an ambiguous in-flight role is quarantined rather than repeated.

## Verification and limits

```sh
cd runtime
uv run --frozen ruff check src tests
uv run --frozen pytest -q
cd ui
npm run build
npm test
```

Tests cover real Temporal restart/decision and repair gates, submission/claim conflicts, cancellation races, native command boundaries, and the local API. The dashboard suite exercises the real response shape as well as UI state changes. Live model availability, actual repository checks, GitHub effects, browser interaction against the real service, and independent review/verification require separate evidence for the **exact candidate**. A successful unit suite or fake provider run does not establish those effects. The service is local, single-host, and uses Temporal's development server and SQLite; interrupted external effects may need human reconciliation.

Runtime CI runs the locked package, Ruff and pytest on both Ubuntu and `macos-latest`. Both jobs supply Temporal CLI 1.9.1 through its pinned official setup action; macOS also supplies Python 3.12 and Node 22.21.1. Tests own their disposable Temporal servers and package-manager cache. Runtime jobs have finite bounds of 20 minutes on Linux and 30 minutes on macOS. A passing 723.78-second Linux suite exceeded the former 12-minute deadline and cancelled its job; the native macOS job took 17 minutes 39 seconds, leaving little margin under its former 20-minute bound. The new bounds reserve at least half again those observed whole-job durations, rounded up to five-minute bounds, for setup, finalization and runner variance. Local Temporal fixtures launch one owned CLI process and wait up to 15 seconds for real gRPC readiness using the supported SDK client, confirming the listener belongs to the recorded CLI PID/start identity. Port reservation enables socket reuse so Linux can immediately bind the reserved port. This avoids the SDK local-server launcher’s fixed five-second deadline. Startup failure reports the CLI exit status or readiness error and logs; timeout and cancellation stop recorded PID/start identities and observed children before returning. The tests require `temporal` on PATH and preserve SQLite restart databases; time-skipping fixtures retain the SDK launcher. Six-second delayed CLI and activity-entry regressions cover readiness separately from public cancellation’s unchanged three-second response bound. The public decision/restart regression also waits for an observed pending decision within its existing 15-second completion bound. Pytest reports macOS skip reasons, including fixtures that require local browser assets or an explicitly configured Codex binary. Hosted success does not establish live provider or browser-service behavior.

A reproducible live smoke uses the installed public CLI, a new private Git fixture and a separate local service. It submits two raw goals with no manually supplied proof, verifies real intake plus environment reuse, then cancels before plan acceptance and tracker writes. This disposable smoke explicitly selects `plan_approval: "required"` to keep that checkpoint. Supply an existing configured runtime and a new private evidence directory, such as a unique directory under `~/.local/state/devflow`:

```sh
runtime/.venv/bin/python runtime/scripts/smoke_preparation.py \
  --runtime-dir /absolute/path/to/runtime \
  --config /absolute/private/service-config.json \
  --output-dir /absolute/private/new-smoke-evidence \
  --execution-backend native-macos
```

The restart option crashes only the disposable service's identified worker after proof publication and before freeze, restarts through the public CLI, and verifies recovery from the same proof. Successful real intake requires the configured model to be available for the runtime's account. This proves the preparation/intake surface, not a full feature delivery or implementation-session resume. The latter is a separately labelled integration check.

Terminal tracker retries include unavailable PR queries and remote-branch queries,
including their timeouts. Successful conflicting PR/head observations still block
without releasing the claim; unavailable observations exhaust into recoverable
`waiting_tracker` at the frozen deadline. Existing unversioned inputs keep their
original activity result shapes.

Controller-failure boundary executions close `COMPLETED` even when their last
projection fails. The stop records which request failed: the terminal event, the
`tracker_deadline` that replaced it when the terminal tracker conflicted or
expired, any `tracker_pending` readback rejected inside that stop, or a failed
feature stop. Owner maintenance keeps its verdicts in `delivery_closed_projections`
per execution identity: settled executions are never re-read (also after a
service restart), open executions back off up to five minutes, unproven verdicts
up to one hour, SQLite work runs off the API event loop, and a reconciled row or a
replayed feature stop is bound to the authentic history bytes before any write.
The bounded-stop commands above are gated on the `delivery-liveness-v2` patch
marker, evaluated first in `DeliveryWorkflow.run()`. A patch marker is replay-safe
only for the exact change it shipped with, so every later workflow command change
defines its own marker next to it rather than reusing this one. Histories recorded
before a marker replay on their earlier command path, so workers roll forward only.
A reconciled expired deadline remains admissible by `terminal_tracker_recovery`:
the closed tail is the failed last projection bound by the completed result's
record, and the successor starts without that record.

Run lists return up to 50 observations by default (maximum 100). `/api/runs`, CLI `runs` and MCP `list_runs` accept `limit`, `cursor` and `archived`; CLI uses `--limit`, `--cursor` and `--archived`. Follow a returned `next_cursor` explicitly to read older history, keeping the same archive filter. Ordering uses last-update time and run ID. These are live observations, so concurrent updates can move rows between pages; cursors do not freeze history. Each page shares its read connection across compact rows and historical prepared-spec lookups, then rechecks the complete current installation, owner, cohort and guard before returning. No authority object is cached between operations, and page assembly holds no write transaction. The dashboard polls only the recent page every five seconds. **Load older tasks** retrieves one older page on demand and retains its rows without polling that page; toggling the archive collection resets older observations. Direct run URLs remain available, and statistics still include complete stored history.

### Retired published-metadata admissions

Fresh `metadata-preflight` and `reconcile-published-metadata` commands are no
longer exposed by the API, client, tools, CLI or store. Their commit-range rewrite,
force-with-lease and PR-title writer are removed. Retained metadata rows, immutable
evidence and existing `published_metadata_recovery` workflow inputs stay readable;
the workflow dispatch, `delivery_metadata_readback` activity, evidence applicability,
source/candidate validation and shared resource/identity helpers remain unchanged.

Operator steps before deployment: under the previous runtime, audit the retained
`delivery_metadata_recoveries` rows for `state = 'pending'` and the corresponding
saved commands. `metadata_reconciling` is a command response, not a run-detail
phase. Also inspect the indexed `metadata-reconciliation-intent` evidence for each
run and compare it with the saved recovery input and queued/validated metadata
rows. An intent can predate a row or appear under a later nested recovery; its
presence alone does not establish completion. Run detail alone cannot rule out
an interrupted prequeue write.

Include archived runs and metadata nested inside tracker, gates, technical, title
or other recovery inputs. Finish every pending write under the previous runtime;
finish or cancel all queued/running metadata executions, including nested ones,
and confirm owned resource cleanup before updating. Unknown effects require
readback on the previous runtime, never a fresh command or assumed completion.
These are owner deployment checks; the retained readers cannot finish an
interrupted writer. No rows or history are deleted and no schema migration is
required.

Fresh merged deliveries require an active GitHub ruleset enforcing their required
checks with an up-to-date base and no bypass actors. The runtime only reads these
rules and refuses a merge when enforcement is missing or ambiguous. It preserves
the original frozen base; if the actual squash parent advanced within the verified
head ancestry, it records that parent separately and verifies both ancestry ranges
and the exact tested whole tree before closing the issue. A divergent base requires
fresh integration evidence. Concurrent administrative changes to branch rules are
outside that server-enforcement guarantee. Unknown merge effects retain the claim;
read-only terminal reconciliation never schedules another merge.

### Ordinary published prerequisite recovery

The existing `published_gate_retry` zero-iteration request can admit one ordinary
published candidate whose planned locked Python prerequisite genuinely launched
and failed before any current independent or browser assessment. This requires
the original completed passing implementation and its raw final assessment,
session, separate input/output candidates, exact published source, closed
execution, complete published cleanup and released claim. The observer derives
the dependency command and project/lock/test-selection hashes from the frozen
plan and checks the exact native launch, journal, environment, runtime, log and
finished nonzero result. A name, failure summary, null session, missing test
report or cleanup flag grants no exception. Unknown monitoring, a test assertion,
browser failure or failed implementation remains outside this cause.

Admission preserves that launched failure and creates one fresh gate namespace.
It grants no implementation turn, changes no recipe or allowance, and cannot
repeat under another command ID. Worker readback reauthenticates the historical
receipts and current physical resource/source/claim authority. The same native
prerequisite must then complete and every normal required gate must run with its
original selection, deadline, JUnit and minimum counts. No diagnostic receipt is
an acceptance result, and no new browser, cache, environment or tool permission
is added by this recovery.

### Evidence-only frozen worker diagnostic

A stopped ordinary trusted-local delivery can expose a failed native worker
recipe after a genuinely completed blocked implementation. The public
`native-diagnostic-preflight` and `ack-native-diagnostic` commands preserve that
blocked assessment and every normal gate. They authorize one separate diagnostic
of the exact recorded `worker-regressions` recipe; they do not authorize a model
turn, change a repair ceiling, reopen a delivery or establish normal acceptance.
The first supported recipe retains its Node wrapper, cwd, 2700-second deadline,
xdist settings, selection, required JUnit and 1000-passing-test minimum.

Both commands require `--id` and a private JSON `--request`. The preflight body
contains `command_id`, `expected_revision`, `expected_projection_revision`,
`expected_iteration`, `expected_candidate_id`, `expected_candidate_head`,
`expected_content_sha256`, `expected_pr_number`, `expected_pr_head`, `check_id`
and `expected_check_sha256`. The acknowledgement adds the returned
`preflight_key` and the literal
`"acknowledgement": "diagnostic_only_no_delivery_acceptance"`. The service
rechecks the exact closed execution, completed terminal assessment, original
session/input/output, current source and publication, released claim, original
failed native log/journal, full resource cleanup and live operational owner.
A caller cannot provide a replacement command, cwd, environment, spec or result.
These commands connect only to an existing service; they do not start one.

The existing acknowledged `temporal_test_preparation` remains authoritative when
present. Otherwise the diagnostic acknowledgement may include the already
supported `temporal_test_prerequisites` declaration with `check_id`,
`python_project`, `provenance_path` and `provenance_sha256`. This first purpose
accepts only the existing official prerequisite receipt
`70e40d7284f61cfc4f3054ed9005028ec08394a6abb3ad30e10e2c59e991c0e1`.
The normal preparation owner authenticates its project/lock, SDK, archives,
executables and evidence. Its derived marker belongs to this diagnostic only;
the original admitted spec and policy are unchanged. No browser acquisition,
browser root, arbitrary environment or different tool authority is granted.

The durable diagnostic acknowledgement fences writes to that stopped execution
and claim, without changing any `delivery_runs` column or normal event history.
The API dispatches it through the existing cross-worker native check slots. Its
server-derived resource namespace contains an independent copy of the exact
candidate and an independent finalization generation. NativeProcess receives
the original admitted spec. Configured dependency preparation and native check
environment assembly use the existing owners, including the controller's
existing Playwright browser binding. Four fixed browser/Temporal readiness
controls run through that environment before the full unchanged recipe. Their
JUnit is separate readiness evidence and cannot satisfy the full gate.

Read status with `native-diagnostic --id RUN --diagnostic-command-id COMMAND`.
The immutable diagnostic intent, readiness and result receipts are also indexed
as diagnostic-only evidence. A completed diagnostic, including a passing one,
never becomes an implementation/review/verification/browser/CI verdict. Any
later original-within-ceiling assessment requires its own ordinary admissible
continuation and all normal acceptance gates.

A lost monitor is never retried by this purpose. A new command ID or a different
copy of the same tool declaration cannot mint a second invocation for the same
closed subject and recipe. Replay the original acknowledgement to observe the
one operation. After controller loss, replay can acknowledge an already saved
receipt or finalize only its own fully finished, freshly observed actor-free
resources. Missing or incomplete journals, live actors/ports, source or claim
drift and unconfirmed finalization remain unknown and fenced. Replay neither
launches a replacement nor kills an uncertain actor. An unacknowledged result
cannot claim a passing diagnostic verdict.

The acknowledgement also binds the clean current framework program and the
existing controller browser-cache binding. The framework Python/SDK and its
actual `-I` monitor stay on the installed framework lock; the declared SDK1.26
belongs only to the separately locked worker project. Subject, native-launch
plan and result files require their exact durable diagnostic phase hashes;
a file-only publication grants no new invocation or source generation.
`/api/service` reports `native_diagnostics.counts` separately from role capacity,
so zero active roles does not imply that a diagnostic is finished. An idle
service does not repeatedly read diagnostic authority: an acknowledgement wake
hint or startup recovery causes a fresh durable read, and every actual body
rechecks all authority. The hint contains no proof and grants no execution.

Native runtime measurement keeps its existing finite probe resources separate
from the canonical delivery registry. Each new probe records its exact parent
spec digest, private preparation intent and bounded attempt index. Resource
writes rederive that parent through the admitted configuration and original
run/database identity before querying the same diagnostic fence. Known retained
probe attempts can be finalized through that authenticated owning context
without rewriting their old specs. Unknown namespaces or self-declared flags
grant nothing. Measurement and finalization also hold the original registry's
existing exclusive lease in read-only mode; the diagnostic ACK's shared lease
therefore cannot overlap a separate preparation registry. Both owner checks
and FD/name authentication run after acquisition. The original configuration
loader, run rows, claims and canonical resource bytes remain unchanged.

### Acknowledged ordinary CI execution inventory

GitHub can omit genuine `workflow_dispatch` jobs from a PR's canonical status
rollup. A successful Actions run alone does not establish PR/ruleset or merge
eligibility. The ordinary published-unmerged endpoint supports one separately
reviewed, compiled-hash provenance profile for its exact original subject,
five existing attempt-1 runs and eleven frozen required names. Other subjects,
profiles, event formats or run selections refuse. It neither dispatches nor
reruns workflows and never changes the original policy or failed CI history.

Use `ci-inventory-preflight --id RUN --request FILE`, then explicitly acknowledge
its returned `preflight_key` with `ack-ci-inventory`. The HTTP equivalents are
`POST /api/runs/{id}/ci-inventory-preflight` and same-origin/CSRF-protected
`POST /api/runs/{id}/ack-ci-inventory`. The request contains `command_id`,
`expected_revision`, `expected_projection_revision`, `expected_iteration`,
`expected_candidate_id`, `expected_candidate_head`, `expected_content_sha256`,
`expected_pr_number`, `profile_path` and `expected_profile_sha256`. The private
profile is canonical JSON, mode 0600, single-link and bounded to 128 KiB; its
hash must equal the compiled reviewed fingerprint. ACK adds `preflight_key`
and the literal acknowledgement
`ordinary_execution_only_no_PR_ruleset_or_merge_eligibility`. No public input,
configuration, header or environment variable can override formats, matrix,
source pins, reports, minima, actor, selection or the profile fingerprint.

Preflight and ACK independently read the complete bounded current public
account/repository/PR/head, canonical rollup, selected run/suite/check/job
inventories and exact frozen source/action/main/post/dependency/selector graph.
They join actual checkout and source-framed commands, inputs, interpreters,
terminal reports and required uploaded artifact bytes. Required-job skips,
zero/partial/unknown reports, missing pages, duplicates, later attempts,
changed source/log bytes and current local authority drift refuse. Cases that
the original source permitted to skip or retry stay explicitly recorded as
skipped or flaky; build exits remain distinct from positive test reports.
Unuploaded runner-local reports are not invented.

Raw ANSI-bearing GitHub responses use the supported `gh api
--allow-escape-sequences` output option only inside bounded private capture.
Raw bytes are hashed before literal runner/report framing is parsed, and never
rendered to a terminal or evidence UI. The profile records complete retained
connector UTF-8 text with historical wire SHA unknown; a new successful exact
raw read is separately required, never inferred from an old connector result.
Transport failure or different bytes refuse without stripping or normalization
to make a match pass.

ACK appends only its own metadata/command/audit records. Every delivery-run
column, claim, ordinary event, source, role verdict/session, allowance, gate and
finalization remains unchanged. File-only profiles/receipts are inert. A new
command ID cannot mint another inventory. ACK replay freshly rechecks the same
selected IDs/bytes and current native/candidate/claim authority; it does not
return a stale success after drift. Once a normal execution ends or its retained
physical source is gone, use the separate data-only acknowledgement read, not an
ACK replay to obtain execution authority. Read the acknowledgement with
`ci-inventory --id RUN` or `GET /api/runs/{id}/ci-inventory`; historical hydration
checks immutable bytes and commands without requiring an ended checkout or a
new GitHub read.

The existing separately admitted `published_ci_retry` with zero additional
iterations is the only normal execution consumer. Its admission seals the exact
ACK and original checkpoint; worker and CI readback recheck current native
source/resources, owner, full original ledger and exact claim generation before
and after remote observation. The existing workflow alone records CI, tracker,
finalization and any delivered outcome. All independent local/browser/review/
verification gates remain required. The result always states
`PR_ruleset_eligibility: false` and `merge_context_verified: false`. Merge
readback keeps its independent pull-request event, current-base, hosted-tree,
ruleset and all other stronger predicates; this receipt never satisfies them.
