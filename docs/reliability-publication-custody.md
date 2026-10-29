# Publication execution and uncertain outcomes

Publication uses a descriptor lease for its entire synchronous broker body, not
for the lifetime of its asynchronous activity waiter. Each attempt opens a new
descriptor and takes a nonblocking exclusive file lock. A competing thread or
process returns `publication_execution_active`; it does not queue another body
behind a waiter that Temporal may repeatedly time out. Subprocesses inherit the
descriptor, so killing the Python owner does not release a still-running CLI's
lease. Replacing or unlinking the owned lock file cannot create a valid new
generation: its device/inode identity is also bound in the database.

The lease establishes local execution ownership. It is not evidence that a
remote request had no effect. A durable journal binds the exact publication
request, lock identity, owner token and generation, and records each commit,
push and PR-create launch. A replacement treats a recorded launch without its
completion as unknown. It first authenticates the checked content, signed
commit ancestry, admitted push destination, exact remote head and owned PR.
Only matching readback settles that mutation. A missing remote branch or PR,
a nonzero CLI exit, an HTTP 502 diagnostic, process exit, and elapsed time do
not establish that the earlier remote request had no effect.

An executable that was never launched (`FileNotFoundError` or `PermissionError`
from process creation) is recorded separately and can be retried. A PR query or
`ls-remote` transport failure before any remote mutation keeps the existing
confirmed pre-mutation retry path. A lost successful push acknowledgement can
converge through exact remote-head readback without another commit or push. An
uncertain PR creation converges when the exact owned PR is observed. If it
remains absent, the effect stays pending; operators must reconcile the remote
operation rather than treating absence as permission for another request.

Historical pending effects without this journal remain readable, but their
original executor and mutation state cannot be inferred. They may complete
through exact readback; they cannot acquire automatic permission for another
commit, push or creation. This deliberately preserves their unknown outcome.
Completed historical receipts still undergo the existing exact PR validation.

The workflow retains unknown cleanup custody when its bounded publication
readback deadline expires. A Temporal patch marker preserves previously
recorded finalization commands during replay. New executions cannot release
custody merely because a readback window exceeded the activity timeout.

A stopped run can use the existing bounded publication-recovery command after
the exact original PR appears. Admission joins the publication executor,
authenticates its original checked candidate and exact remote identity, and
independently observes the native actor, provider, port, root and lease inventory.
It seals this later observation without changing the closed workflow result or
claiming that an unknown provider succeeded. An active publication executor, an
absent PR or an unknown native/provider journal still refuses admission. The
recovery has zero implementation authority and rechecks the sealed observation
before downstream gates run.

Owned reproduction coverage includes cancellation of the actual publication
activity waiter while a real fixture CLI continues, killing a separate Python
publication owner while its CLI retains the lease, an unknown failed creation
that cannot be repeated after lock release, and lock-inode replacement. The
fixture CLI models the GitHub transport; it does not establish GitHub server
semantics or exercise a real GitHub mutation. The short cancellation case
tests the same thread/subprocess boundary as the 180-second production activity
timeout; qualification at the unchanged production deadline is recorded
separately.
