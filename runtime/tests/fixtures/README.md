# Delivery workflow replay fixtures

These are complete recorded Temporal histories using fake activities and an
isolated, in-memory test server. No production service or provider was used.

- `intake-required-history.json`: recorded before automatic plan approval; retains
  the original required human plan decision.
- `delivery-before-check-order-history.json`: recorded with `ffe733c`, before the
  local checks moved ahead of review. Includes review, local checks, browser QA,
  verification, CI, tracking and delivery.
- `delivery-after-check-order-history.json`: recorded from unmodified `c04f00e`,
  after the unversioned order change. Includes the existing
  `role-evidence-handoff-v1` marker and local checks before review.
- `delivery-published-checkpoint-history.json`: recorded from the unchanged workflow in
  `c04f00e` using its published-gate continuation. Skips implementation, so the
  existing handoff marker first occurs after the local checks.

`test_delivery_history_replay.py` tests the designated-artifact replay matrix.
The current marker-only workflow replays pre-change histories. Unmarked
checks-first histories explicitly fail against it and must pass the retained
original source artifact. This preserves the deployment incompatibility as a test
rather than changing historic events to claim universal replay.

`order/` adds byte-identical SDK captures of the same gates-only input recorded on
`ffe733c` and `c04f00e`. Their original SHA256 values are in `provenance.json`.
`c04-source.tar.gz` is an unmodified `git archive` of that commit's runtime package,
compressed with gzip timestamp zero. It is isolated test data, not a deployable
native environment. `retained-routing-probe.py` records a real unversioned execution
using that source, moves it to a pinned retained worker on a disposable server,
confirms delivery, and replays its untouched history. Activities are fixture
implementations, so this does not claim native preparation compatibility.

See [worker deployment migration](../../../docs/worker-versioning.md) before deploy.

The three delivery histories replace the recording machine hostname only in
worker identities and sticky task queue names. Event order, patch markers and
activity payloads are unchanged; the original captures are retained locally.
