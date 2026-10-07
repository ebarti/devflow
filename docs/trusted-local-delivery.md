# Trusted local delivery

A service can explicitly select `"execution_mode": "trusted-local"` for a trusted
single-user Mac. The default remains `native-profile` for existing constrained
configurations. Admission freezes the selected mode. Historical workflow inputs
keep their recorded activity ordering and authority.

Trusted-local roles request kit 0.5.3 `PermissionMode.STRICT` and
`FilesystemAccess.FULL_ACCESS`, with no named profile. SDK 0.160.0 maps these to
`ApprovalMode.deny_all` and `Sandbox.full_access`: full host access without
interactive approval. The isolated role config also sets `approval_policy =
"never"`, `sandbox_mode = "danger-full-access"`, and disables plugins, built-in
agents and multi-agent tools. Broker dependency/check/browser commands use the
same trusted host through the owned native process launcher, with no filesystem
or domain allowlist. No extra compiler, SDK or browser redirect list is needed.

This is an explicit trust tradeoff. It does **not** isolate hostile source or
commands from host files, network or credentials. Private role homes and a
sanitized environment avoid accidental credential inheritance; they are not
non-bypassable restrictions. Controller ancestry guards prevent ordinary nested
Devflow calls. Source-scope checks, candidate identity, finite role/repair limits,
deadlines, owned process/port cleanup, synthetic fixture QA and the
`published_unmerged` endpoint remain controller contracts. Preparation records a
separate mode-bound proof and confirms the launcher and ancestry guard; it does
not claim constrained sandbox denials.

An owner-controlled update of the installed controller Python source may change
the current runtime payload hash for a run already frozen as `trusted-local`.
The replacement must match the committed payload of its clean installed Git tree;
dirty, untracked, ignored extra Python files and a non-installed copy are refused.
Every other native identity field must still match, including the interpreter,
OS/architecture, bundled CLI, locked dependencies, sandbox overrides and tool
roots. Verification authenticates the original private proof, evidence hashes,
measurements, fingerprint, policy and run binding against its frozen identity.
It preserves that specification and proof; the old observations remain historical
measurements rather than measurements of the replacement source. `native-profile`
retains its existing strict update checks.

The native launch journal and process result retain the actual controller revision
and payload at launch. Reattachment preserves that launch identity; later roles
and checks record their own controller. Run statistics cohorts still describe the
admission-time version, not every controller used during execution. Later gate
retries compare against authenticated consumed process identities, so installing
P2 while a P1-prepared retry is queued cannot earn another retry when P2 fails.
The existing two-generation and zero-implementation bounds remain unchanged.

Operator steps: before deployment, inspect queued and running executions through
public status and indexed evidence. Finish executions whose recorded activities have one attempt
and special legacy PID-bound recoveries, plus continuations that already consumed
a native preparation renewal, under their matching previous runtime
before upgrading. An update does not retrofit heartbeat or retry options into recorded
histories. Stop the original delivery worker through the existing `stop` control
before changing installed source. Confirm its recorded ready PID/start identity
has drained and the owned worker is stopped; do not update while it can launch roles
or gates. Install the clean committed replacement, then use the existing `start`
control and confirm the replacement worker is ready before dispatching.
Retry-capable ordinary trusted executions can reattach to the same
owned native invocation after worker replacement and start their remaining
authorized roles and gates. Changed configuration, source candidate, scope,
models, dependencies or interpreter require their existing authority checks;
unknown effects or cleanup still require inspection. Never rewrite a frozen
specification, proof or private state to force continuation.

New admissions freeze `publication_summary` separately from the unchanged execution
`goal`, and use the summary for commits and PR titles. Detailed or multi-sentence
goals require an explicit single-line Conventional Commit subject, at most 120
characters including its type, without extra sentences or control/bidi characters.
A short single-sentence goal may omit the field; a plain goal receives `chore:`.
Superseding submissions follow the same rule and preserve the predecessor's goal.
Historical frozen runs without the field keep their original goal-derived commit
subject and bounded title behavior. The controller uses `git commit
--signoff` with the existing configured Git identity; it requires the author and
sign-off identity to agree. Before staging, pushing or accepting a publication
receipt it checks every commit after the frozen base for a Conventional Commit
subject and the author's DCO trailer. A signed head cannot mask an unsigned
ancestor. GPG signing and a skipped owner-exempt DCO CI job do not satisfy this
controller check. Existing PR titles must also be conventional. Invalid already
published history is refused; the read-only publication recovery command cannot
rewrite it or reuse gate evidence for a different head.

Repositories can select `baseline_check_ids` from their existing
`prepublish_checks`, in recipe order, for shared project prerequisites such as
dependency installation, docs build and rendered browser regressions. New
admissions run these checks in an isolated clean checkout of the frozen base
before intake or implementation. A failed or unresolved baseline stops the run
with its original logs and base identity, without consuming feature repair
turns. Source-specific checks which require the proposed feature belong in the
normal candidate gates. Baseline success never replaces candidate checks:
every prepublication and verification recipe still runs on the feature result.
Historical inputs without the baseline marker keep their recorded behavior.
Resolve upstream defects through their owning change, then admit work against
the verified corrected base; never silently rewrite an existing frozen input.

New terminal runs synchronize tracker status and read back assignment, Project
and claim through the existing tracker helper. Blocked/cancelled outcomes select
Blocked; delivered outcomes select In review. Claims release only after proven
process and resource cleanup. Pending/failed readback replaces earlier tracker
success in the projection, and a pending final tracker cannot establish
successful delivery. Older inputs without `terminal_tracker_version` retain
legacy replay behavior.

The terminal tracker activity uses the owning `_tracker_sync` helper. Its terminal mode validates the owning helper's live issue/assignee/Project readback and atomically acknowledged intent before
claim release, rather than adding a release-breaking remote audit afterward.
A genuinely uncertain final In review result remains `waiting_tracker` with no
successful outcome; its desired transition stays In review. Its latest pending
readback replaces any earlier `consistent` projection. A cancellation accepted
during `tracker_started` still wins before the terminal transition is frozen.

Terminal reconciliation stays inside the original live Temporal workflow. It
makes three attempts, each with a three-minute activity bound and finite durable
backoff timers, then exposes an exhausted `waiting_tracker` checkpoint without
closing the execution immediately. The checkpoint has a ten-minute overall
deadline. On expiry it closes with the pending projection and no successful
outcome. Newly dispatched runs also have a finite 72-hour Temporal execution
timeout; a reconciliation-only successor has a fourteen-minute run timeout.
Its terminal outcome/status/release request is frozen;
new cancellation cannot change a transition that may already have released its
claim. A public `reconcile-tracker` update resumes one more three-attempt readback
cycle and grants no role, source edit, gate, model or capacity authority:

```sh
devflow-delivery --config /private/state/trusted-local.json reconcile-tracker --id RUN_ID --request /private/state/tracker-retry.json
```

The request contains exactly `command_id` and the current `expected_revision`.
HTTP uses same-origin, CSRF-protected
`POST /api/runs/{id}/reconcile-tracker`; MCP exposes `reconcile_tracker` through
that same client. Stable command replay returns the existing update receipt;
changed bytes conflict. An uncertain response requires inspection with the same
command ID. On each retry the activity first inspects the owning acknowledged
intent and claim. If the helper already released the claim, only a fresh
read-only audit is allowed; it never repeats `set` without ownership. Confirmed
readback restores the frozen terminal outcome and refreshes the final projection,
clearing the pending error and restoring any original blocked failure. The work,
acknowledged intent and live audit must all bind the frozen issue URL before any
helper mutation/audit and after readback. Reassignment conflicts rather than
following another issue. Background helper success therefore
remains safely recoverable through the public readback command.

If the execution is completed or timed out before confirmation, the same public
command admits a reconciliation-only successor. Admission reads the authentic
Temporal execution, memo and completed pending-projection activity from its
closed history; no caller-supplied checkpoint can authorize it. Admission
binds the authentic initial input to its immutable submitted/prepared admission
stage and the terminal projection to the effective prepared/accepted plan. A raw
automatic-planning submission need not equal its later accepted specification.
The durable grant binds that history hash, the exact projected candidate/PR and checks, every
finished attempt/effect, cleanup receipt, stopped process identities/ports and
current ownership. Delivered targets also revalidate the published remote head.
Retained source is rehashed against the frozen candidate. When cancellation
deliberately removed a clean base checkout, the owning cleanup intent and receipt
must carry the matching pre-removal content/head/base proof. Recovery authenticates
that proof and the removed root without recreating a checkout. Missing proof or
changed retained dirty source conflicts; no source authority is added.
An identical command returns its existing receipt, including across an uncertain
response and dispatch restart. The successor rechecks the seal before the first
tracker effect, preserves original role/session results and never runs roles,
candidate edits, preparation, gates, publication or cleanup again. Missing or
changed evidence leaves a recoverable conflict checkpoint. It can only confirm
the existing terminal transition. Each explicit continuation has the same finite
readback deadline.
The public run response exposes `terminal_tracker_recovery` with the predecessor
workflow/execution/status, closed-history/spec hashes and stopped-evidence seal.
It keeps the original policy/scope recovery summary and session/failure provenance
visible alongside that reconciliation-only record.

The dashboard hides cancellation once a terminal checkpoint is frozen and offers
`Reconcile tracker` only for an exhausted or closed checkpoint. Its retry keeps
the exact command ID and revision across streaming cycle updates until the API
acknowledges it. Authorized development continuations instead remove the active
terminal checkpoint before their first live projection, while retaining the
predecessor history; cancellation can then stop the newly authorized work.

## Preserved-candidate execution recovery

An operator can grant one explicit recovery for a stopped, unpublished native
run whose constrained implementation or prepublication gates exhausted their
budget. This supports multiple completed attempts with one original implementer
session. It retains the same work ID, run ID, branch, source bytes and provider
session. The original accepted plan, submitted configuration, closed Temporal
tail, attempt results, process logs and cleanup evidence remain unchanged.
Native attempt results are recorded before the role activity adds its controller
candidate envelope. Recovery authenticates the retained source against both the
frozen controller projection and that closed role envelope, including its input
candidate and session identity. A missing raw-result candidate is supported;
a present conflicting candidate or missing controller evidence is rejected.
Current GitHub issue requirements are read, hashed and supplied as requirements
data to the managed roles; they are not accepted results or new authority.

The public preflight binds the original specification, authentic closed Temporal
result/execution, every stopped attempt, current candidate, same-session state,
confirmed process/resource cleanup, released claim, completed effects, absence
of the branch/PR on the remote and original issue evidence. Any changed or
unavailable readback rejects admission. It observes PID/start identities and
ports; it never kills a process to manufacture stopped evidence. An unknown or
pending external effect cannot be retried through this operation.
Canonical work issue/repository and claim resource are bound to the frozen issue
before preparation, inside the atomic claim grant, during resume preflight and
before/after tracker-start effects and readback. Supported reassignment of a
released work item conflicts; it cannot redirect recovery to another issue. This
uses the existing owning helper contract without modifying installed helpers.

Create a private, owned configuration under the existing service state root by
copying the original JSON and changing only `execution_mode` to `trusted-local`.
The original file stays untouched. Model, effort, capacity, source scope, checks,
deadlines and all other raw configuration must match exactly. The new preparation
proof binds the actual installed trusted runtime/SDK/CLI and has a separate
identity; it cannot reuse the constrained proof. The same implementation
session's conversation/database state is copied into a new isolated role-home
generation; credentials and permission files are regenerated.

After separately authorized installation and chosen-mode verification:

```sh
devflow-delivery --config /private/state/trusted-local.json recovery-preflight --id RUN_ID
devflow-delivery --config /private/state/trusted-local.json recover-execution --id RUN_ID --request /private/state/recovery.json
devflow-delivery --config /private/state/trusted-local.json run --id RUN_ID
```

`recovery.json` is a private JSON object with exactly these fields:

```json
{
  "command_id": "stable-operator-command-id",
  "expected_precheck_sha256": "SHA256_FROM_THE_FRESH_PUBLIC_PREFLIGHT",
  "config_path": "/private/state/trusted-local.json",
  "config_sha256": "SHA256_OF_EXACT_PRIVATE_CONFIG_BYTES",
  "additional_iterations": 2
}
```

`additional_iterations: 1` authorizes only the preserved candidate's gates;
`2` also permits one same-session implementation repair after a newly observed
gate failure. The controller runs original prepublication checks first, then
normal publication, independent review, checks, browser QA when configured,
independent verification, required CI and terminal reconciliation. It does not
blindly call implementation or turn historical failures into a pass. The
endpoint remains `published_unmerged`.

A private command/preparation intent is durable before any new probe. Preparation
uses at most two separately owned resource generations, finalized independently
of the predecessor; neither a failed cache-miss probe nor unavailable subsequent
remote readback can rewrite its cleanup manifest or recreate its transient root.
A stable-ID retry observes interrupted probe ownership/cleanup before any new
probe, preserves failed logs, and reuses the new proof when already established.
Unknown probe cleanup blocks additional execution. The intent/failures are
publicly indexed, and prepared history is frozen in the eventual grant.

Admission atomically seals one durable grant, reacquires the released claim for
the same managed owner, and queues a new Temporal execution for the same run.
The original failure remains in the event timeline and recovery summary. A
repeat of identical command bytes returns the recorded receipt; changed bytes
under that ID or a second grant conflict. After an uncertain transport response,
read the run/receipt with the same command ID. Outbox dispatch inspects the exact
request/recovery memo before acknowledging an already started execution, without
starting a duplicate. No private database mutation or model-capacity retry is
part of this recovery.

HTTP exposes `GET /api/runs/{id}/recovery-preflight` and same-origin,
CSRF-protected `POST /api/runs/{id}/recover-execution`. The official MCP server
exposes `recovery_preflight` and `recover_execution` over that same client. The
dashboard status/evidence and existing plugin discovery, submission, status and
decision tools continue to read the shared service.

## Durable synthetic check evidence

Trusted test checks preserve their frozen argv and environment assignments. The
controller adds an owned `PYTEST_ADDOPTS=--basetemp=.../pytest-artifacts` so pytest
fixtures emit declared synthetic outputs within the run's durable evidence
directory, outside transient check TMPDIR. This does not enable an opt-in test
flag or relax executed/skipped/deselected acceptance. A mismatched source opt-in
contract must fail the gate and be corrected by the managed source repair.

Before final cleanup, the broker retains owned regular PDF, PNG, HTML, JSON,
XML and TXT outputs and writes a candidate-bound SHA-256 manifest. Limits are
4096 files, 50 MiB per file and 1 GiB per check; linked files and foreign or unsupported
file identities reject retention. Directory links, including pytest's numbered-fixture navigation links, are pruned without following them; owned
numbered directories and all their regular artifacts are visited directly. Failed checks also retain their outputs.
Independent verification receives these checked manifest references and must
inspect the required physical pages and measurements. Test exit status alone
does not establish visual QA.

The public evidence index includes each retained artifact after cleanup.
`GET /api/runs/{id}/evidence/{evidence_id}` returns its hash and text or
base64 bytes. The `/content` suffix returns hash-verified PDF/PNG bytes for
viewing; HTML is served as plain text, never executed. No global temporary
directory scan, manual copying race or shared cache cleanup is required.

Installation, service restart and managed recovery require the separately
assigned operational verification. This repair does not merge or deploy.

Published metadata reconciliation is retired for new commands. The API, client,
tools, CLI and store no longer expose `metadata-preflight` or
`reconcile-published-metadata`; the commit rewrite, force-with-lease and PR-title
writer are removed. Existing admitted metadata inputs and their evidence remain
readable. An interrupted write must finish under the previous runtime before
updating: replaying its old command on the new runtime cannot finish it. Follow
the [metadata deployment drain instructions](temporal-runtime.md#retired-published-metadata-admissions),
including pending commands, archived runs and nested recoveries.

Already-admitted metadata validation runs no implementation, review or QA provider
turn. It runs fresh native gates and retains explicit source-identical applicability for a
prior genuine independent PASS. A missing QA assessment remains incomplete.
A failed gate remains blocked with its fresh evidence. Neither an old CI result
nor an old head assessment is presented as a new head rerun.

A stopped investigation can use the separate `gates-only-preflight --id RUN`
and `admit-gates-only --id RUN --request REQUEST.json` commands only with an
explicit cause-specific authority and semantic receipt. The request contains
`command_id`, `precheck_sha256`, `authority_path`, `authority_sha256`,
`semantic_path`, and `semantic_sha256`. Admission authenticates both the frozen
input and the actual closed role after-candidate, raw result, session, source,
cleanup, work/claim and remote custody. It preserves the rejected historical
assessment and runs all mandatory gates at the same existing iteration, without
an implementation turn or larger repair budget. Any implementation-needed
failure stops blocked. Identical request replay is idempotent; another command
or changed preflight is refused. Historical native receipts may have the owning
read-only `attempts` container at 0755, while their run/attempt leaf remains
0700 and receipt remains owned 0600 with one link. Symlinks, writable containers,
public leaves and receipts, and changed receipt identity are rejected without
creating or changing permissions. Raw assessment bytes must match the frozen
assessment exactly except for the four documented supervisor additions:
`cleanup`, `process_cleanup`, `resource_cleanup` and `native_process`. The role
resource value is `pending_workflow_finalization`, distinct from the separately
confirmed terminal resource receipt. No assessment field is rewritten.

The semantic receipt's hash-bound accepted-plan readback must match the frozen
effective plan. `authority_readback(spec, seal, request)` authenticates those
receipt, plan and custody references without an admission effect. The new
successors retry custody readback at most three times within five minutes;
unknown custody stops the workflow before any provider turn.

A reviewed installed runtime payload change requires an explicit preparation
step in either stopped admission request: add `preparation_authority_path` and
`preparation_authority_sha256`. The hash-bound deciding receipt admits at most
two original runs and one immutable preparation generation per run, with zero
provider turns and no implementation or repair grant. Read-only preflight never
renews preparation. Metadata preflight validates the required renewal
authority/payload/proof before accepting a request; admission repeats those
read-only checks before freezing any immutable metadata intent or original
resource archive. A missing or bad authority therefore causes no command, claim,
ref or archive effect and allows a corrected request. Once a valid intent is
sealed, its exact request remains immutable. Missing old measurement ancestry is
observed without creating directories. Admission authenticates the old proof,
requires clean installed Git source and freezes its revision/import path, then measures the
new payload through the same native launcher. Only `runtime_payload_sha256`
may differ: protected installed command paths, dependencies, binaries, mode,
configuration, checks, plan, source, session and iteration authority stay exact.
The new specification must pass strict native bind/verify before execution.

The original proof and specification remain unchanged. Private immutable
authority/generation receipts and the bounded preparation journal are available
through public evidence. Measurement-copy JSON records encode the exact
original bytes as base64 with their original path/hash, including when a current
shared proof is reused; they do not claim a new measurement. Owned failed probe
logs and journals remain separately indexed. At most two owned probe attempts belong to that one
generation; an interrupted probe must first prove cleanup and retains its failed
logs. Stable command replay resumes the generation; a different command or
further identity change conflicts. The successor records explicit old-to-new
candidate/policy/proof lineage with identical feature source, including the
separate metadata commit mapping when applicable. Historical role inputs and
after-candidates remain historical.

These continuations retain original cleanup bytes and use distinct durable
check/browser evidence namespaces. Gate checkout allocation, controller diff and
browser receipt readback share the namespace authenticated by the durable
admission, sealed receipt and exact frozen execution specification. The default
run namespace remains valid for original gates; new continuation gates preserve
older checkouts and artifacts even when metadata changes the commit head at the
same iteration. Independent-role homes use that same authenticated namespace,
preserving retained workspace-bound configuration and original implementer homes.
Caller roots and symlink aliases refuse before allocation or
read-scope expansion. Generated-child cleanup and gate finalization use the same
owned namespace. Native process and lease roots remain in the original run.

### Technical-successor admission retirement

`continuation_kind: "accepted_technical_successor"` is unsupported by the current
`repair-admission-preflight` and `continue-repair` surfaces. This includes exact
request replay: the retired writer cannot finish a pending or orphan pre-workflow
intent. Historical technical-successor records, effective-spec and native
predecessor readers, integration readback, workflow bodies and activities remain
retained. That preservation does not authorize a new technical admission. See
[technical-successor retirement](technical-continuation-retirement.md).

Before deploying the changed runtime, cease new technical-successor requests and
keep the previous release available to finish its existing work:

1. Inventory both unarchived and archived runs (`GET /api/runs` and
   `GET /api/runs?archived=true`) and known original run IDs. Where accessible,
   `GET /api/runs/{id}` exposes the retained technical-successor projection and
   evidence index; the existing evidence read exposes indexed intent, closure,
   integration, native-generation, predecessor-resource and resume-actor files.
   Include nested recovery chains; a summary, missing projection or failed read
   does not establish that no technical successor exists.
2. The owner must also inspect the previous runtime's owned state read-only.
   Check `delivery_technical_successors` for pending rows and the original run
   roots for `technical-successor/intent.json`, closure/integration effects,
   native-generation journals and predecessor-resource archives. Include orphan
   intents without a committed row, archived runs, nested records and queued
   outbox dispatches. Public run/evidence reads are not a complete orphan scan.
   Use existing ownership/custody checks and preserve the original evidence;
   do not rewrite database rows, seals or cleanup results to bypass recovery.
3. Under the previous release, finish matching queued/running native executions
   or cancel them through its supported controls, and resolve pending/orphan
   pre-workflow intents through that release's supported procedure. An exact
   command resume belongs to the previous writer, not the retired current route.
   Confirm terminal execution and cleanup of actors, ports, leases and resources
   before deployment. An accepted cancellation alone does not prove cleanup.
   If any intent, dispatch or effect remains uncertain, retain the previous
   release and defer deployment rather than retrying the retired admission.

Native executions retain their runtime-source binding. Preserved outbox decoding
and replay of the three captured minimal c04 fixtures do not prove execution
across runtime hashes, every historical input, worker restart or lifecycle upgrade.

Resource roots stay under their original
registered ownership. For trusted pytest checks, the unchanged frozen argv runs
with controller-owned `PYTEST_ADDOPTS=--basetemp=<evidence>/<check-id>/pytest-artifacts`.
The controller retains PDF, PNG, HTML, JSON, XML and text files before transient
cleanup, with a candidate-bound hash manifest: at most 4096 files, 50 MiB per
file and 1 GiB total. A hash manifest establishes retained file integrity;
independent QA must still establish required case/render counts and inspect
every physical page. The pagination fixture's eight cases each produce six
PDFs, six source HTML files, six bbox HTML files, measurements and every page
image. Calibration PDFs are additional evidence. The expected new page count
comes from the actual rerun rather than a historical removed-file count.

The existing `continue-repair` command retains its ordinary seven-field contract.
The former browser-test-name correction variant, identified by additional
`authority_path` and `authority_sha256` fields, no longer admits new work through
preflight or continuation. An exact previously persisted command can still return
its saved response without effects. Previously admitted `title_constraint`
payloads and their nested lineage remain readable; their strict literal-only
source validation continues before and after the original provider turn.

Before deploying this retirement, finish or cancel queued/running title-constrained
repairs, including nested continuations, using the prior runtime. Also finish
pre-workflow admission effects and resolve any uncertain command acknowledgement
under that runtime. Generic continuation routes and the existing historical table
reads are unchanged. This retirement concerns browser test names, not PR titles.

Implementation admission measures the net file diff against the frozen base,
including untracked files. A committed, clean feature proceeds to the same
mandatory controller gates as uncommitted edits; an empty commit or reverted
feature does not establish a pass. Readiness does not certify later checks.

Dependency receipts label `input_hashes` as staged-byte hashes and include
`input_provenance`: original base Git-blob hashes and the exact manifest key
allowlists used for staging. Lockfiles and patches use the identity transform.
Independent verification can reconstruct the staging inputs without confusing
the sanitized manifests with repository bytes. Candidate setup stays excluded.

A repository may configure `project_statuses`, mapping logical work statuses
to existing GitHub Project Status options, for example
`{"blocked": "Needs validation"}`. The controller passes that mapping to the
owning tracker helper; the logical work remains blocked, and the helper still
requires a real matching option and live readback before releasing the claim.
Omitted mappings retain the helper's existing defaults.

Public terminal cleanup now derives confirmation from the exact hashed owning
finalization receipt, with process and resource confirmation and no unfinished
or unknown-cleanup attempt belonging to that run. Foreign active runs affect
global capacity, not the stopped run’s cleanup proof. `cleanup_recorded` exposes the historical stored value separately.
Old `none` is never broadly treated as confirmed or privately migrated; missing,
changed or unknown proof remains unconfirmed. Native failed gate feedback keeps
bounded structured regex matches and owned full-log provenance independently
of diagnostic truncation, while retaining the original log and reject policy.

Implementation and repair roles leave source changes uncommitted. The controller owns commit creation and validates author sign-off and Conventional Commit metadata before publication.

Implementer probe evidence has an explicit controller-owned allocation per attempt.
The prompt names that writable directory; complete probe source, synthetic inputs,
stdout/stderr, failed attempts, timestamps and provenance hashes can be retained
without expanding feature source paths. After process cleanup the controller seals
an immutable manifest and copies of the original files. Independent roles receive
read-only copies authenticated against the implementation content, and the public
evidence index exposes the sealed outputs after temporary resource cleanup.

Before a trusted implementer starts, the controller prepares only locked Python
projects selected by its accepted test plan and the frozen pnpm dependency recipe
needed by selected Vitest tests. A plan with no such prerequisite receives an
explicit successful no-op receipt. Preparation failures retain their real logs and
stop before a provider turn; they do not masquerade as unknown process ownership.
The role uses the prepared project interpreter for normally imported probes.

Repairs receive authenticated copies of earlier broker logs, receipts and artifact
files instead of instructions to read private controller state. Copies identify
original candidate/iteration provenance; earlier and baseline passes do not certify
an edited candidate. Original frozen scopes, checks and independent gates still
apply. Native-profile homes created before this permission contract require an
explicit fresh native preparation generation when resumed; immutable historical
profiles are not silently rewritten. Trusted-local homes retain their existing
permission mode.

For historical SHA-only inputs already published by the controller, recovery binds
the unchanged PR target to a completed owned publication, complete paginated GitHub
target-change history and the actual current target tip's ancestry. Advancing main
alone does not invalidate that published feature's immutable implementation base.
Retargeting, rewritten ancestry, mismatched ownership or incomplete readbacks stop
recovery. First publication still requires the original exact branch resolution.


Stopped implementation recovery uses the public `continue-repair` operation with
`continuation_kind: stopped_delivery_resume`. The command binds the stopped
protocol revision, iteration, actual retained candidate ID and head, and grants
one or two further implementation iterations. It may be used again after a later
verified stop: each distinct command is finite, idempotent, and retains the
original failed execution and all previous admissions. A consumed grant is never
reset or replaced. Unchanged-source gate retries remain separate operations.

Admission requires a closed Temporal result, released ownership, confirmed native
cleanup, an accepted plan, and a passed mandatory immutable baseline for runs
that were admitted with baseline checks. It authenticates partial implementation
source against the original supervisor receipt, preserves the original provider
session, and allows a first session only for an authenticated failure before any
provider launch. The source, configuration, attempt inventory and effects are
read again after runtime preparation and before launch. An unpublished feature
branch must still be absent remotely. Resumed work runs the ordinary publication,
independent review, verification, configured checks and exact-head CI gates.
