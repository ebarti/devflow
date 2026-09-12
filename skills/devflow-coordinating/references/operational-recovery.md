# Recover an operational stop before candidate capture

Use the existing work/attempt and current scoped edit authority. This route clears
an observed operational prerequisite failure during blocked delegated Implement
with no current candidate. It does not certify product correctness.

1. Preserve and import the original worker's actual partial BLOCKED `host result`,
   including its `assignment_action_id`. Resolve all prepared, dispatched,
   ambiguous or pending-setup actions and import outstanding producer/gate output.
2. Observe the same worker's actual completed turn with `host observe`. Its
   `agent_status` is the supported completed object and its source reference is
   the real native observation. The recorded control observation binds the current
   activation. Older unbound observations remain history; obtain a fresh observation
   through this release before recovery. An unavailable or running worker cannot
   use this route.
3. Verify the remedy for the recorded failure. Retain the actual remediation/probe
   bytes privately with `artifact put --file <private-remediation-file>` using the
   explicit repository and state root. Save the returned artifact hash.
4. Read complete current `work show` JSON after these observations. Build a typed
   `operational_recovery` record using the bindings below. Send `work reconcile`
   a normal operation/work/expected-revision request with `clear_blocker: true`
   and `operational_recovery: <record>`. Do not mix this record with `evidence_ids`.
5. Read back the preserved attempt and new immutable recovery record. Follow
   `next` through normal journaled assignment/preparation/receipt to reactivate
   the same available worker. Retain the old BLOCKED output and prior activation.

The record requires `schema_version: 1`, `record_type: "operational_recovery"`,
a unique `recovery_id`, actual `source_reference`, `summary`, `observed_at`, and
the stored remediation `artifact_hash`. Bind these fields from that current state:

| Field | Exact input |
| --- | --- |
| `work_id`, `attempt_id` | Current work and attempt |
| `blocked_revision` | Current blocked state revision after output/availability observations; also `expected_revision` |
| `blocker_hash`, `scope_hash` | `digest(state.blocker)` and current scope hash |
| `workflow_snapshot_id`, `workflow_snapshot_hash` | Current attempt snapshot ID and `digest` of its full immutable snapshot |
| `assignment_id`, `assignment_action_id`, `producer_task_id` | Original verified implementation assignment, current activation and native task UUID |
| `result_hash`, `availability_hash` | `digest` of that assignment's imported `implementation_result` and activation-bound `control_observation` |

Use the package's canonical `devflow.validation.digest` for object hashes, not
the displayed JSON's whitespace. Direct Python helpers must set
`sys.dont_write_bytecode = True` before importing the installed package (or start
Python with `-B`); preserve the immutable installed release.

The application verifies artifact bytes before committing recovery. A stale or
wrong binding, missing/corrupt artifact or unresolved operation leaves the state
unchanged. Replaying the identical acknowledged operation is idempotent; changed
payloads require a new current observation and operation identity. Candidate-bound
`evidence_ids` reconciliation remains the existing post-candidate path.

A title/revision-only amendment, or a snapshot differing only in identifier,
capture time or continuation metadata, retains the blocker. Actual authorized
scope/source or operative workflow changes retain normal amendment behavior;
do not manufacture a delta to bypass operational proof.
