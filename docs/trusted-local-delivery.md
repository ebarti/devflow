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
