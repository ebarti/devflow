# Developer Workflow contributor instructions

Read `docs/architecture.md` and the relevant contract in `docs/implementation-contracts.md` before changing behavior. `docs/implementation-plan.md` defines acceptance; do not silently narrow it.

Work on a task branch, preserve unrelated changes, and use Conventional Commits. Keep executable rules, skill instructions, schemas, and operating docs consistent. Reuse standard Git, GitHub, and native host capabilities. Do not introduce hidden model calls for deterministic work.

Capture substantive user requests in one short GitHub issue even when starting immediately. Reuse the same issue for follow-ups; ordinary capture is bookkeeping, not another approval or planning gate. Use existing CLI capabilities directly where they suffice. Custom code must own a concrete cross-tool invariant or durable state requirement, rather than reproduce an existing tool's capability.

Use the journaled `devflow backlog capture` wrapper for new issue creation; it delegates to `gh`. Reuse the work ID after interruption and reconcile uncertain writes. Recover from the store with `backlog list`, `work list`, `work show`, and `next`; conversation history is not the sole copy of pending work. Acknowledged state/evidence must be flushed before dependent actions. Never erase a database journal or unresolved action to make recovery look complete.

Run focused regression tests for changed invariants and `git diff --check`. User-facing and high-risk paths require independent review and QA with no unresolved Blocker/High findings. Delegate bounded independent work when useful; preserve shared edits. Use the user's model settings and explicit session overrides.

Publish confirmed code findings as PR review comments within authorized publication scope; fix, verify, reply, resolve, and independently read back each fixed thread. Do not silently discard earlier findings.

Do not commit credentials, execution databases, logs, user artifacts, or machine bindings. Synthetic fixtures must be clearly synthetic. Install or enroll only within the user's authorized scope; always prepare a concrete manifest. Product repos retain their own profiles and safeguards.
