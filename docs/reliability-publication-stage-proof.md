# Publication-stage recovery evidence

Fresh publication recovery accepts a typed `PublicationStopProof` v1 from its
own closure reader. Display text is never a new admission grant.

The previous compatibility helper accepted `error.startswith("publication
unresolved: ")`. That text reached fresh admission because both publication
constructors reused a workflow replay hint as a stage authenticator. Legacy
records made the ambiguity visible: they often lack the newer structured
failure, so text appeared to be the only classifier. The invariant is that the
owner must establish where the authentic execution stopped independently of
the wording displayed to a person. The fix belongs at the publication recovery
owner, not in a general error-string parser or a weakened cleanup gate.

Five causal steps explain the boundary: a matching error prefix selected the
publication route; fresh admission called a historical hint; old results lack
typed classification; neither constructor required independent stage evidence;
the route treated presentation as execution provenance. The proof observer now
separates those facts and preserves genuine old histories through explicit
authenticated observation.

The current controller's authenticated closed result can supply a fast proof
when its failure is a transient `publication_deadline` at `publishing` or
`publishing_pending`. `_stop` adds the stage around its typed classifier. The
proof binds logical run, exact workflow/execution, request and recovery digests,
closed time, iteration, checked candidate and the complete stopped-state digest.

For legacy results, the explicit owner operation reads the exact execution's
live history or content-addressed sealed copy. It validates complete history,
memo, reset identity, original recovery input, result and the publication
activity's exact execution spec, iteration and checked candidate. A bounded
scan selects the last activity outside the explicit settlement allowlist. It
must be publication or publication readback, with a failed, timed-out, cancelled
or still-pending result. A later check, role, CI stage or unknown future activity
cannot be classified as publication. A completed exact publication belongs to
its subsequent recovery route. No diagnostic text supplies these decisions.

Both constructors use the bounded initial closure reader before the general
closure/result decoder. Every start input must equal an independently persisted
submitted, prepared or effective admission; a recovery start must also equal the
current effective spec and the exact durable recovery record. A copied request
memo does not authorize changed scope, policy or recovery inputs. It bounds any indexed sealed candidate before connecting,
checks live exact execution identity, and selects retained evidence only on
Temporal NotFound. It reads live history in twenty-event pages with finite RPC
and operation deadlines. Execution alone may explicitly preserve a newly
authenticated history after observation; assessment never seals it. No ordinary
`handle.result()` or unbounded `sealed_closure()` precedes these checks.

Limits are 50,000 events, 64 MiB of event protobuf bytes, 32 MiB compressed sealed
history, 64 MiB expanded sealed history, 4 MiB expanded decoded payload bytes and
a thirty-second history operation deadline. Codec expansion is checked before
ordinary decoding. A missing, oversized, corrupt or unsupported history returns
a typed unknown with a precise evidence/import action. It does not become
permission or proof of absence. The stage observer does not seal histories or
mutate an executor journal. Its initial closure reader authenticates the closed
result and applies expansion bounds before decoding it.

Sealed reads require canonical existing ancestors, private owned root, a regular
0600 file with one link, a bounded descriptor read, unchanged descriptor/path
identity and metadata before and after reading, and the recorded content hash.
They do not heal permissions during assessment. Execution preservation uses the
same bounded reader for an existing unindexed orphan and for a file that races
with publication. It writes a private owned stage, atomically links the final
content address without replacement, validates the resulting artifact, and only
then inserts the closure index through its existing conflict/reset checks. A
corrupt or oversized orphan remains untouched; no generic unbounded sealing
helper is reached.

New admissions seal the proof alongside their existing source, candidate,
publication, effect, cleanup and budget evidence. Admission reobserves that
checkpoint under its existing custody and transaction. Existing accepted command
receipts remain exact replays. Old recorded recovery inputs lack the new field
and retain their original deterministic workflow branch; new proof-bearing
inputs require matching bound proof. Historical request/history bytes are not
rewritten. A proof describes a historical stop; it never renews eligibility.
The workflow matcher lives in the existing pure publication contract. It does
not import the history-reading owner, network client or filesystem observers.

The public fixtures use an actual loopback Temporal history from an owned
retained producer and real assessment/store boundaries. Their legacy preparation,
tracker and cleanup boundaries are controlled, and GitHub reads are local fixture
responses. They prove a genuine legacy stage remains eligible after display text
changes and that the old prefix after a check-stage stop is refused. Before/after
SQLite dumps and empty closure inventory establish read-only assessment. A
same-run workflow with a copied memo and altered start scope is refused even
when its closed result contains the modern typed publication failure. Public
execution controls retain a real live history while injecting a small
high-expansion unindexed orphan, both before preservation and at atomic
publication; both refuse before ordinary decoding, preserve the artifact and
leave the closure index unchanged. An artifact removed by the fixture between
initial stat and open produces unknown custody, with no replacement or index. Exact submitted/prepared/intake/recovery
constructor controls preserve those legitimate start variants. A
separate current-controller fixture confirms its actual typed stage field.
Older component tests explicitly control the stage-proof boundary and are not
counted as authentication qualification.

The no-proof recovery replay artifact was recorded by the unmodified
`_resume_publication` method from revision `8ed1339` in actual loopback Temporal,
with controlled remaining gate activities. Its original recorded recovery input
contains no proof field. Source-method and exact-history hashes are recorded
beside the fixture; replay uses the saved bytes without editing its input or
history. This specifically exercises the historical recovery branch.
