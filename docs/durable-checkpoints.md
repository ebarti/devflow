# Durable delivery checkpoints

## Retained workspaces

An intact checkout is never deleted to make restoration proceed. The internal
`RetainedCheckpointCustody` context verifies the original acknowledged manifest,
all artifacts, independent Git ownership, worktree registration, branch and HEAD,
index tree, staged and unstaged patches, complete selected source paths and
bytes, executable bits, and exact retained state evidence and absences. It also
refuses newly selected evidence or unclassified ignored source. Source modes and
mtimes are not part of the existing source manifest; this operation makes no
stronger claim for them.

`POST /api/runs/{run_id}/checkpoints/{checkpoint_id}/verify-retained` accepts
`command_id` and `expected_revision` through the existing CSRF boundary. It records
a distinct `delivery_checkpoint_retained_verified` source-only receipt, without
restoring or activating anything. Replaying a command reacquires physical custody.
The context binds its original PID and thread; an inherited descriptor after a
fork is not the same owner.

The scope retains concrete checkpoint, resource and writer leases while preparing
its read-only comparison outside admission locks. A bounded raw tracking snapshot
is captured before full payload reads. The same live SQLite observer connection's
data version, schema and canonical pin are rechecked in the short ACK writer;
another connection's commit, including an ABA update, requires a fresh observation.
No full state materialization, JSON parsing or receipt hashing is needed there.

Pause readiness additionally requires the exact concrete `RetainedPauseCapability`
from the pause owner. Caller-provided fences, contexts and transition callbacks
are not accepted. The immutable transition ticket is part of both command and
receipt; a saved command never reapplies readiness. The capability prepares receipt
bytes before admission, verifies original and authorized post-state in the ACK
transaction, and can insert only its immutable readiness row. Source and budget
writes are denied by the ACK's SQL authorizer. Readiness is not activation, and
runtime work must stay gated until the context releases its physical leases. The
current pause custody producer still refuses unproven closure; that integration
remains unmounted.

The source-only route supports settled ordinary non-feature work
with no opaque native journals. Unproven native closure, queued attempt settlement
without its own concrete custody integration, feature projection, or a checkpoint
with unresolved effects remains unsupported. Local source equality cannot promote
an unobserved effect to a resumable result. No-copy acknowledgement preserves
the original source and budgets; it does not allocate or move a worktree, rewrite
authorization, run checks, publish, or start execution.

New source seals include a versioned index semantics baseline. Nonzero Git index
flags, including assume-unchanged, skip-worktree and intent-to-add, refuse sealing
without changing the index. Retained verification rechecks the baseline even if
candidate content is unchanged. Old acknowledgements lacking this proof remain
inspectable but cannot grant verified recovery; a fresh seal is required.

New native admissions own a private bare Git database in their durable run state
before admission is acknowledged or the outbox becomes dispatchable. Local source
copying happens before the short claim transaction; partial ownership publication
cannot produce an accepted delivery receipt.
The admitted `source_path` remains the original authorization provenance; operational
Git reads and worktree allocation use the independent owner. Removing the original
setup checkout, its common Git directory, or a managed chat worktree therefore does
not remove the delivery's admitted base, new commits, branch or Git ancestry.
New custody admissions refuse a state root or tracking database inside the product
source or another linked Git worktree, so the authoritative store and checkpoints
cannot inherit the same disposable lifetime through a custom configuration.
Historical admissions keep their existing source binding. Disposable fake-provider
fixtures can opt into the new behavior through server configuration
`"workspace_custody_version": 1`; native admissions always enable it.

The owning-layer failure was a lifetime mismatch: durable run state referred to a
candidate whose Git database belonged to a shorter-lived setup checkout. Why did
unfinished work disappear? Cleanup could remove that setup. Why did the retained
candidate become unusable? Its `.git` file pointed back into the setup's common Git
directory. Why was that directory external? Allocation used the configured source
as the worktree owner. Why did retaining evidence not repair it? Evidence did not
contain the dirty index, untracked source and complete Git ancestry. Why was the
run called recoverable? Its retained path was treated as custody of the necessary
bytes. Independent ownership and a verified checkpoint fix those boundaries.

## Public operations

The existing loopback API and CSRF boundary expose these operations:

- `POST /api/runs/{run_id}/checkpoints` accepts `command_id`,
  `expected_revision`, and optional `reason`.
- `GET /api/runs/{run_id}/checkpoints/{checkpoint_id}` verifies acknowledged
  manifest and artifact digests before returning its receipt.
- `POST /api/runs/{run_id}/checkpoints/{checkpoint_id}/restore` accepts a new
  `command_id` and `expected_revision`.

Creation requires a stopped run, reconciled external effects, closed original
launch and role leases, proven registered workspace ownership, and a fresh check
for open workspace actors. Registered native journals currently cause a typed
`native_checkpoint_custody_unknown` refusal before acknowledgement or restore:
the observed process identities do not establish complete descendant closure.
Previously acknowledged native checkpoints remain readable as evidence, with
their original acknowledgement distinguished from current resume eligibility.
No-launch and supported non-native paths remain available. A pause controller can
use the same internal API with its authenticated durable admission-fence validator;
HTTP callers cannot provide that validator or select arbitrary paths.
An effect marked complete without its recorded object-valued observation is
unknown. Feature effects marked complete or absent likewise need their recorded
result. A fenced checkpoint may retain such evidence with a resume blocker, but
restoration requires reconciliation.
New workspace admission journals its provisional Git allocation before creating
it. If its staging Git invocation is interrupted without confirmed helper closure,
the API returns `workspace_admission_git_custody_unknown` and retains that one
allocation and journal for reconciliation; retries do not delete or replace it.
Native stopped runs also require the exact closed Temporal workflow generation,
request/recovery memo and result; a blocked dashboard projection is insufficient.
An unavailable controller response remains unverified. The fenced pause path
preserves its existing open workflow and exact epoch instead.
The controller supplies `writer_context` and `admission_context` leases. Resource
custody, original native leases and publication/merge writer custody precede the
root admission mutex. Source and
process observation occur outside a database write transaction; a short final
transaction revalidates the unchanged authority and acknowledges the preserved bytes.
An SQL-only `on_ack` hook can bind the checkpoint to pause control in that same
transaction. A versioned owner validator can authenticate a queued, providerless
no-GO attempt without inventing a provider result or spending another attempt.
Feature projection reuses that exact fenced registry connection. Before any
admission lock, its captured registry facts must fit 512 rows, 2 MiB of text in
total and 256 KiB per cell; excess history refuses precisely. Checkpoint payloads
are decoded before the lock, and the original registry epochs are checked again
before using the prepared projection and committing its acknowledgement.
The observation checks row counts and encoded byte sizes using bounded scalar
metadata before fetching full payloads in the same read snapshot. Its initial
schema authentication captures SQLite's schema version. Writer-held validation
checks that fixed version, lifetime, three authority epochs and bounded current
claim through the exact existing connection, without rescanning trigger/index
definitions. This retains the supported-writer/cutover assumption; arbitrary
file edits, schema-version forgery and database backup restoration remain outside
that contract.
The trusted pause owner can supply `checkpoint_context`: its authenticated
SDK/history snapshot, cursors and budget/clock ledger are included verbatim with
an exact digest, rechecked at acknowledgement and restoration. HTTP callers cannot
supply this trust. Receipt replay repeats current epoch, writer and source-custody
checks rather than granting a later pause the earlier checkpoint's authority.
Public checkpoint operations use the service's trusted writer-context factory.
A fresh source-owner receipt binds a private run-wide writer witness before any
publication or merge body starts. Actual bodies hold shared custody, including
inline async activity state, every mutating pool-queue handoff and inherited
CLI/native-monitor descriptors. Checkpoint creation and
restoration take exclusive custody without waiting, then authenticate every
existing publication executor and the retained merge lease. A missing legacy
witness stays unknown; observation cannot create it as retrospective proof.
Trusted actual bodies retain an independent shared open-file description that
they never delegate to CLI children. The detached native monitor opens another
private shared description before provider GO. An inherited read-only descriptor
can still be unlocked by its child, so inherited descriptors alone are insufficient
proof. The monitor records `devflow-native-writer-proof-v2` against the exact
intent, journal, run, state directory and witness inode only after this independent
guard is held. Missing or older markers cannot prove that historical custody.
This carrier does not prove universal descendant closure. Native post-launch
checkpoint admission remains unavailable until an authenticated complete
descendant-closure capability exists.
Active merge controllers
additionally require their paused controller context. Retained merge requests are
included in the frozen state and unknown merge outcomes remain typed blockers.
An independently verified closed SDK controller also produces an append-only
writer cutoff in the checkpoint acknowledgement transaction. A delayed callback
from that exact workflow/run can no longer enter after exclusive custody is
released. Missing or foreign SDK identity grants no mutation authority; a selected
successor must prove its own original SDK start and current claim. Readback may
settle an existing effect but cannot originate a publication or merge mutation.
The default internal context is `store._checkpoint_writer_context(spec)`;
`CheckpointWriters(store, spec, additional_context=...)` composes an additional
non-overlapping trusted writer lease such as dispatcher custody. It enters that
context before the root admission mutex and delegates its SQL-only `verify(db)`.
Multiple participants can share one exact `SharedCheckpointMergeLease` object.
It authenticates the existing installation inode and retains its exclusive
descriptor while each participant independently holds its run's writer fence.
Read-only activity custody cannot be promoted to a mutating lease. Independently
authenticated settlement actors may observe a released claim; any publication
reconciliation in that scope forces `create_missing=False` and cannot commit,
push, create a PR, or issue a new generic GitHub mutation.
An authorized terminal tracker successor has a separate scoped settlement
capability: its current SDK binding and durable grant must name the exact sealed
status, release decision and reason. It may settle that original tracking intent
after claim release while general source/publication mutation remains forbidden.
The tracking-native renewal authority anchors are frozen too; rehydration cannot
recreate a deleted grant or roll it back to an older binding.
Only this fenced internal mode can preserve a known local closure with an unknown
remote effect. Its checkpoint has `resumable: false` and typed `resume_blockers`;
restoration still refuses until the effect has an exact, durable readback. A fresh
command can seal a new checkpoint under the same pause epoch after reconciliation.

The versioned manifest binds the effective authority, original issue/work/run
owner, exact run projection, accepted plan, cumulative usage and repair ledger,
candidate and index identity, checks, external-effect journal, native closure
observations, selected evidence, and required provider session files. Ignored
credentials, declared regenerable dependencies, credential profiles and complete
provider homes are excluded. Unclassified ignored work refuses preservation with
its exact path rather than silently disappearing. Accepted artifact references are
traversed and verified, including browser artifacts, XML/PDF reports and files
outside conventional evidence folders.
Staged and unstaged changes remain separate. Preservation never commits or
publishes source.

Files are privately staged, synchronized and atomically published before the exact
manifest digest is acknowledged in the command journal and event stream. An
unacknowledged directory remains a partial publication; reads and restoration
refuse it precisely. Retrying creation can reconcile a complete unacknowledged
publication only when current fenced source/state reproduces its exact manifest
and every artifact. Corrupt, incomplete, foreign or inconsistent bytes still refuse.
Resource pressure or an interrupted write cannot produce a
successful checkpoint receipt. Checkpoint directories live outside disposable
worktrees and have no automatic expiry or cleanup path.

Restoration validates every artifact and current execution authority, budget,
owner, original resource generation and native closure again. It reconstructs a
missing candidate in a newly allocated workspace in the original authorized
namespace. It verifies the candidate and index before recording a separate
restoration acknowledgement. Original admission, resource manifests, workflow
history, session lineage, usage, external effects and authorization remain intact.
The acknowledgement binds the new directory identity to the exact original
resource generation; ordinary custody checks recognize only that replacement.
Restoration does not start execution or replay any external operation.
Missing declared generated children can start a new generation only through the
completed restoration acknowledgement and exact archived parent/child registration.
The normal allocation and creation guards remain in force; an existing or unproven
output is never adopted. This lets locked dependencies regenerate after source
rehydration while retaining the original checkpoint evidence.
The restore command durably reserves its allocation pathname before creating it
and records its inode before reconstruction. An interruption before creation is
replayable; a directory created before its identity is recorded stays unknown.
Git mutations record an active phase before invocation. If the process is lost or
Git fails before confirming completion, the same command reports
`checkpoint_restore_custody_unknown` and retains the allocation and any locks
until the original Git operation and helpers are reconciled. Other commands cannot
allocate around unfinished work. A requested move is never reported as a verified
restoration. Source and evidence files use identity-bound staged writes and atomic
no-replace publication; a lost stage without an authenticated inode stays unknown.
Selected session files retain their original mode and nanosecond modification
timestamps, preserving the existing session-state digest and continuation lineage.

Missing or changed evidence, unknown writers/effects, advanced budgets, moved local
branches, changed authority and unacknowledged partial publications require explicit
reconciliation. An absent durable Git owner also refuses precisely rather than
falling back to a possibly unrelated setup checkout.

The writer-lifetime boundary has a separate failure chain. Why could a waiter
finish while source still changed? Cancelling `asyncio.to_thread` does not stop
its actual thread. Why could a closed-writer inventory miss that body? It may
still be queued and have no per-effect row yet. Why is enumerating and locking
existing rows insufficient? A later body can allocate a new row. Why can a shared
descriptor alone still admit a stale callback after a checkpoint? That callback
may not have reached lease acquisition before the exclusive seal. Why is a
projected stopped outcome insufficient to deny it? A projection does not identify
the closed SDK actor. The owning fixes are admission custody before queue
submission, actual-body/CLI lifetime custody, and an exact SDK closure cutoff
committed with acknowledgement. These retain the existing effect uncertainty,
authorization and native closure gates.
