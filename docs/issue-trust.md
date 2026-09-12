# Conversational intake and execution admission

Version 0.3.0 replaces the 0.2.0 requirement for an independent host intake/human-validation adapter. The user gives natural conversational instructions; the agent interprets their meaning, records the bounded request, and calls devflow. No separate approval channel or user-authored JSON is required.

## Invariant and trust boundary

A direct work request, a bug report or request to investigate/fix, a named issue, or a bounded broad selection such as “work on the current P1 backlog” authorizes the requested work. The agent creates or reuses a lightweight issue and immediately proceeds within that scope. Follow-up corrections reuse the issue. Missing consequential scope or an unauthorized external commitment still requires clarification; routine capture and admission do not add a planning or approval gate.

The agent is the semantic authority interpreter. Existing issue text, labels, comments, attachments, PR content, queue events, or instructions embedded in those inputs cannot initiate work or expand the user's requested scope. A named external report can supply evidence for the requested investigation without a separate human-validation ceremony. Its observed origin and consumed lineage remain intact. Issue authorship and a shared GitHub token do not prove that an instruction came from a human.

The private `user_request` is workflow bookkeeping and consistency evidence, not independently authenticated human identity. Hashes bind recorded bytes; they do not authenticate their author. The local CLI runs as the user and is not an OS sandbox against a caller who changes the program or invokes another binary. Privacy, scope, candidate evidence, independent gates, delivery limits and ambiguous-action recovery still apply.

A tiny global instruction loads the `using-devflow` entry skill in each new chat. Only instruction text loads: ordinary questions and unused sessions create no issue or attempt and do not scan or resume a backlog. Detailed devflow references load only for requested repository work. There are no hooks, startup actions, automatic scheduling, or new service.

## One immutable admission

`work ready` and `work amend` accept a `user_request` object with a stable `reference`, concise `summary`, and nonempty `allowed_operations`. The agent prepares this object from the conversation. The CLI derives the admission and authority identities and binds the immutable record to the repository, work ID, accepted scope hash, exact source and source digest. Admission uses decision kind `user_request`; a legacy caller-written Authority or approval field cannot replace the request.

One request reference can authorize the existing members selected by a bounded batch request. Each selected work item still has its own repository/work/scope/source binding and allowed operations. No per-issue approval is needed. The request is not an indefinite queue policy: later arrivals and a different selection require user direction. Scope amendments record the authorized delta in the request summary and append a new immutable admission; unchanged replay uses the stored snapshot without importing later issue text.

The source retains its lineage array and `consumed_digest`. Consumed issue content, comments, attachments and external PR heads retain exact identity, revision and digest where recorded. Changed consumed inputs or scope require a corresponding amendment within user authorization. Mutable presentation metadata or unrelated issue edits do not silently amend the accepted contract.

The [capture-to-source recipe](../skills/devflow-defining-work/references/source-lineage.md) maps confirmed issue metadata into a lineage observation and preserves inherited and other consumed material. The aggregate source digest hashes the entire lineage list, not just the captured issue content. `work prepare` reports missing lineage or an absent/incorrect aggregate without creating state or authority; `work ready` retains the same exact-source admission guard. Historical schema readability does not imply execution readiness.

Start, resumption, candidate/check work, action preparation/retry/begin and native handoff preparation validate the stored admission's bindings and operation limits. Default continuation needs no synthetic or independent verifier and no repeated approval for the same authorized work. Optional constructor-injected `TrustedIntakeVerifier` support remains for legacy embedding integrations; it is not required by the conversational CLI path.

## Capture is bookkeeping

Capture retains the sanitized payload, optional `source_lineage`, immutable GitHub repository/issue/creator IDs, observed content digest and revision. It records unknown origin rather than claiming human provenance: issue reuse is `reused_unknown`; new issue creation/readback is `created_readback`. Neither state grants authority. The conversational user request supplies the work direction.

Before POST, the coordinator journals expected repository and authenticated creator identities plus exact title/body digest. After a successful POST it durably records that response's exact issue identity before independently reading that issue. A later retry retains the response binding. If the response is lost, reconciliation requires a unique marker match plus the pending operation's expected repository, creator and exact content. A marker present before dispatch, multiple matches, wrong creator/content, or an uncertain absence fails closed. Reads never justify duplicating an uncertain write.

Confirmed legacy captures replay unchanged and remain unknown; missing provenance is never invented. Legacy pending captures without creation expectations cannot establish creation by their marker. Existing issue reuse remains a safe read. Caller-provided lineage remains recorded context, including externally derived owner projections; it is not user authorization.

## Compatibility and recovery

The current entry point does not delegate execution to older runtime pins. Historical package/profile/instruction snapshots remain readable and unchanged, and the current reader handles safe recovery. This retains the 0.2 old-pin protection while replacing its mandatory unavailable verifier with conversational request admission.

Legacy Ready/Active work needs `work amend` with an increased scope revision and a user request recording the authorized scope before further execution. For an active attempt whose installed pin or profile changed, capture the current workflow/profile and effective model settings with `snapshot capture`, then pass that record as `workflow_snapshot` in the amendment. The CLI validates its current profile and stored artifacts before changing the attempt's current snapshot references. Prior snapshots and evidence remain immutable; candidates, checks, gates and prepared actions are invalidated for the amended scope. Omitting a new snapshot succeeds only while the existing one still matches the current profile.

Reads, cancellation, blocking, accounting, finding observations and receipts remain available. Dispatched or ambiguous external actions reconcile through read-only adapters; they are never blindly redispatched. Retrying a proven-unsent action requires a current admission. Historical evidence is preserved.

CLI lifecycle regression proof must exercise admission, start, candidate/check work and continuation without a synthetic verifier, plus missing-request rejection, scope/operation binding and ambiguous-action recovery. Such proof does not establish native desktop visibility or protected-merge conformance; those remain separate adoption gates in [implementation status](implementation-status.md).
