# Recover an original independent result

Use only when the existing independent round still owes its result. Keep its
work, attempt, producer, activation, candidate, scope and policy identities.

For an interrupted round, record fresh interrupted inventory with `host observe`.
`host resume` with `assignment_id` and a bounded `reason` prepares its continuation.
Then use `host prepare`, the returned `followup_task`, and `host record`.
Repeated interruptions retain the original gate activation.

For a completed producer whose parseable original gate JSON fails schema
validation solely because of missing or empty evidence IDs, preserve those bytes
as an artifact and observe the completed agent.
Use `host recover-result` with `assignment_id` and
`original_result_artifact_hash`. The runtime verifies the original binding and
derives its schema rejection; a caller cannot invent the validation error.
Follow the same prepare, native follow-up and record sequence. The returned
instruction requests only a corrected original result using existing evidence,
without product checks, repairs or a new independent round. The same producer
must author that correction. Preserve the original verdict, findings, completion
time and limitations; import the corrected producer artifact through `gate record`.
The original invalid bytes and rejection remain recovery history. Already
imported results, changed inputs and other schema defects cannot use this path.
If the same producer completes another malformed correction, observe completion
and repeat `host recover-result` with the same original artifact hash. Reuse the
immutable recovery record; reconcile uncertain continuations before any retry.

Before a workflow/profile amendment changes inputs, collect and import pending
original output. A role stopped before completing its proof returns its actual
partial/BLOCKED evidence without further product work. Do not invent PASS or
rewrite the original policy. Then amend the same attempt and continue from `next`
under the reviewed snapshot. Clearing a blocker does not establish a fix.
