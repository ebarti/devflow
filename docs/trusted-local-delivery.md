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

Publication keeps a conventional goal subject, or prefixes a plain goal with
`chore:`, for both the commit and new PR title. The controller uses `git commit
--signoff` with the existing configured Git identity; it requires the author and
sign-off identity to agree. Before staging, pushing or accepting a publication
receipt it checks every commit after the frozen base for a Conventional Commit
subject and the author's DCO trailer. A signed head cannot mask an unsigned
ancestor. GPG signing and a skipped owner-exempt DCO CI job do not satisfy this
controller check. Existing PR titles must also be conventional. Invalid already
published history is refused; the read-only publication recovery command cannot
rewrite it or reuse gate evidence for a different head.

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

Stopped published metadata has a separate bounded reconciliation. The public
`metadata-preflight --id RUN --request REQUEST.json` command authenticates the
closed workflow, exact clean local/remote/PR head, controller publication range,
source scope, configured existing signer and resource cleanup. A gates-first
publication binds the exact retained implementation through its durable policy
recovery, controller-held input candidate and copied closed role envelope; it
does not require an implementation at that publication iteration or infer one
from proximity. The same request
is used by `reconcile-published-metadata`. Its fields are `command_id`,
`expected_revision`, `expected_candidate_id`, `expected_head`,
`expected_pr_number`, `expected_signer`, `authority_path`, and
`authority_sha256`. The authority receipt permits one command per admitted
original run and at most two admitted run IDs. The controller retains every old
commit object and preservation ref, each tree/author/author date, the original
receipts and an immutable old-to-new mapping. It changes commit subjects and
actual author sign-off, with an exact remote force-with-lease; a changed remote,
foreign commit, source edit, merged PR or unknown identity is refused. Replay
uses the same command and request after a known interrupted effect.

Metadata validation runs no implementation, review or QA provider turn. It runs
fresh native gates and retains explicit source-identical applicability for a
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

An already admitted original that stops at an authenticated controller boundary
can use the existing `repair-admission-preflight` and `continue-repair` JSON
surfaces with `continuation_kind: "accepted_technical_successor"` and
`additional_iterations: 0`. The complete request includes `command_id`,
`expected_revision` (the closed protocol revision), `expected_iteration`,
`expected_candidate_id`, `expected_pr_number`, `expected_pr_head`,
`expected_source_revision`, and the main-task `authority_path`/`authority_sha256`.
The explicitly authorized integration also supplies
`prospective_path`/`prospective_sha256`. Public preflight validates the whole
authority, exact closed failure, consumed admission and immediate native
generation, clean installed source/configuration, stopped actor/port/lease/root
inventory and owning claim before an immutable intent or preparation effect.
Missing, changed, aliased, foreign or stale inputs refuse first.

This technical successor spends no feature grant or implementation iteration.
It retains consumed admissions and generation journals, archives the original
resource bytes (including UNKNOWN), and appends a fresh observation and supported
cleanup closure. Its one child native generation has a separate journal and a
two-attempt ceiling. Only the installed runtime payload may change in native
identity. A failed borrowing of a released claim rolls back that owning claim;
a previously retained original claim stays bound. Exact request replay checks
actual partial effects and resumes their journals.

An exclusive intent whose SQLite transaction did not commit is recovered by the
same command only after fresh whole-request validation under its owning lock.
The original seal and controller identity remain byte-for-byte historical;
separate append-only observations bind each resume controller. Complete sealed
failure rows have a dedicated 4 MiB read limit with exact hash, run identity,
private ownership and no-follow custody; ordinary authority receipts retain
their smaller limit. A resumed workflow drops the inherited active terminal
checkpoint while preserving the frozen predecessor and its passed checks.
A newly initiated terminal transition still freezes cancellation.

The review launch boundary resumes review and QA of the unchanged published
candidate at iteration 4. It preserves prior source-applicable checks and never
repeats publication. The separately authorized integration computes the entire
prospective tree read-only from the frozen base, owned head and exact current
main. All six overlap outputs and every remaining entry must match; the sole
conflict preserves the coaching insertion bytes and accepts the main inventory
title/counts. The one signed merge preserves both parents and old history and
checks the exact owned remote before pushing. Its explicit base, candidate,
accepted-plan and preparation-input mapping is separate from native payload
renewal. Nested worker preparation inputs are compared as typed TOML alongside
all package manifests and locks. Fresh checks, browser QA, independent review
and current CI run at existing iteration 4. The existing title-only iteration 5
remains a separate admission after its fresh exact browser rejection; there is
no iteration 6. None of these source contracts authorizes an installation or
public continuation before independent review and current CI pass.
After a separate title admission changes the current head, replay of an older
technical command may refuse stale source identity rather than repeat effects.

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

The existing `continue-repair` command accepts its original seven fields. A
cause-specific title correction additionally requires `authority_path` and
`authority_sha256`, and can be inspected with `repair-admission-preflight` using
the same request. This narrow route requires the authenticated effective policy,
completed metadata reconciliation and a fresh failed browser receipt whose
frozen regex matches the exact title. It atomically reacquires a released claim
with grant/outbox persistence, rolling back acquisition on admission failure.
The admitted original session receives the bounded exact match, pattern and
owned log/hash provenance. The controller permits one title literal change
only; every surrounding byte, all five cases/assertions and other source files
must remain identical. This grant admits exactly iteration5 under an existing
max_repairs3 ceiling, with no iteration6 or automatic renewal. Metadata repair
must precede this source correction. No generic old failed run gains this
released-claim or effective-policy exception.

Public terminal cleanup now derives confirmation from the exact hashed owning
finalization receipt, with process and resource confirmation and no unfinished
or unknown-cleanup attempt belonging to that run. Foreign active runs affect
global capacity, not the stopped run’s cleanup proof. `cleanup_recorded` exposes the historical stored value separately.
Old `none` is never broadly treated as confirmed or privately migrated; missing,
changed or unknown proof remains unconfirmed. Native failed gate feedback keeps
bounded structured regex matches and owned full-log provenance independently
of diagnostic truncation, while retaining the original log and reject policy.
