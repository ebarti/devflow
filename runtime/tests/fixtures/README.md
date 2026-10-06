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

`test_delivery_history_replay.py` replays every JSON history in this directory
against the current workflow in the ordinary runtime test suite.

The three delivery histories replace the recording machine hostname only in
worker identities and sticky task queue names. Event order, patch markers and
activity payloads are unchanged; the original captures are retained locally.
