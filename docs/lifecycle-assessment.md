# Recovery transition assessments

`POST /api/runs/{id}/assess-transition` takes `{ "kind": ..., "request": ... }` and
performs the selected owner's explicit observation. It is not a grant, a lock,
or an instruction to execute. Admission reobserves its own evidence and retains
its existing command identity, ownership, scope and budget checks. A digest binds
an assessment to observed evidence; it does not authorize a subsequent admission.
Ordinary status reads do not perform these proofs.

The version 1 assessment has `operation`, `normalized_lifecycle`, `eligible`,
`observed_revision`, candidate and publication bindings, `proof_digest`,
`supported_next_actions`, and structured `blockers`. A blocker names its stable
`code`, owner, missing or unknown fact, next action, and whether new authorization
is required. An unavailable proof remains unknown. The lifecycle vocabulary is
`pending`, `running`, `stopping`, `stopped`, `recovering`, `terminal`, and `unknown`;
`paused` is reserved for the pause owner. These are owner observations, not inferred
from a free-form error or a dashboard phase.

| Owner and operation | Observation | Execution recheck |
| --- | --- | --- |
| Stopped delivery and published/unpublished gate retry | Exact stopped checkpoint, candidate and requested finite budget through the owner's preflight | Same typed checkpoint comparisons and a fresh full-seal comparison under the existing controller lock; existing admission transaction still owns the claim |
| Investigation gates-only | Stopped seal, requested authority and native readiness | Same seal comparison under the existing lock; original authority and custody validation remains in force |
| Merge follow-up | Delivered proof and merge command | Current proof and exclusive owner are reobserved under the existing admission fence |
| CI inventory and native diagnostic | Owner-specific preflight evidence | Acknowledgement rechecks the selected owner facts; execution authorization remains separate |
| Publication recovery, prelaunch retry and scope amendment | Where their owner can make a read-only proof, the existing admission guards are evaluated without issuing a command | The transaction rechecks owner, candidate, effects, cleanup and the original grant |
| Feature continuation and plan revision | Read-only complete settlement proof is not yet available; assessment reports unknown | Existing execution guards continue to apply |
| Other owners | Unsupported or unavailable proof reports unknown | Their existing recovery entry points and guards remain authoritative |

Some retained-history checks still report their original aggregate refusal. An
unknown assessment is not a determination that the operation is unsafe or safe;
inspect the named owner and its evidence. In particular, observational assessment
must not reconcile publication effects, stop native processes, reserve a successor,
or consume a one-use authorization merely to decide eligibility.
