# Minimal Temporal-backed Devflow runtime

Status: experimental implementation plan. The runtime is independently installable under `runtime/`; existing Devflow skills and installation remain in place.

## Outcome and scope

A user can start a local Temporal service, run a worker, submit an explicit local task, inspect its persisted state, answer an optional decision, and observe implement → independent review → independent verification → completed or blocked. Real model calls pass through agent-runtime-kit. There is no model polling, automatic GitHub write, merge, release, global installation, or production migration in this slice.

The package uses Python 3.12+, locked `uv` dependencies, `temporalio`, typed serializable contracts, and agent-runtime-kit 0.5.2. One `DevflowIssueWorkflow` owns deterministic transitions. Filesystem work and model calls run only in activities. Workflow-level and activity-level automatic retries are disabled for non-idempotent work.

## Runtime contract

- `start` returns a stable run ID immediately. Identical repeated inputs reuse it; changed inputs with the same ID fail. The chosen repository must be a clean disposable Git working copy, and state storage must be outside it.
- `status` returns phase, outcome, candidate, findings, per-role results and usage. A decision update requires the pending decision ID and revision. `cancel` requests a stop at the next role boundary with honest cleanup state.
- Implementation, review, and verification are distinct tasks and sessions. Typed role assessments must establish a pass. Findings and missing evidence block; a completed provider turn alone is insufficient.
- Candidate identity covers Git HEAD and actual working-tree content. The implementer produces a snapshot. Review and verification bind to that immutable candidate; a changed copy invalidates the gate.
- The kit bridge requests explicit provider, model, effort, working directory, output schema, and permission profile. It preflights supported task capabilities. Reviewer read-only behavior uses provider permissions. Unsupported controls are rejected rather than silently ignored.
- A separate single-host SQLite receipt store claims each activity before a model invocation. Finished results are reused on duplicate delivery. Ambiguous interrupted calls become recovery-unknown instead of launching a second model.
- Local role evidence retains run/candidate identity, requested and reported model/effort, session and usage when available. Unknown fields remain unknown. The fake demo is always labeled as fake.

## Verification

Use a real Temporal dev server for integration tests, not a mocked workflow scheduler. Cover ordered gates, independent identities, duplicate starts, mismatched inputs, findings and changed candidates, decision survival across worker restart, wrong ID/revision, receipt reuse and ambiguous inflight work, zero role invocations during status/wait, and cancellation/result races. Run a small, harmless actual agent-runtime-kit Codex smoke in a fresh disposable repository when supported authentication is available, and report its observed result separately from fake-provider checks. Keep the existing installation smoke check green.

## Limits

This is a local, single-host experiment. Temporal CLI's dev server is not a production service. The receipt store blocks ambiguous recovery rather than resuming a model mid-turn. Provider sandboxes do not provide a complete side-effect broker; requested permissions and prompts must not be described as a general guarantee against external writes. There is no React UI, FastAPI service, MCP server, Docker/Postgres production stack, or automatic backlog execution in this milestone.
