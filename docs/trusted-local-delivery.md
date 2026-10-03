# Trusted local delivery

A service can explicitly select `"execution_mode": "trusted-local"` for a trusted
single-user Mac. The default remains `native-profile` for existing constrained
configurations. Admission freezes the selected mode. Historical workflow inputs
keep their recorded activity ordering and authority.

Trusted-local roles request kit 0.5.3 `PermissionMode.STRICT` and
`FilesystemAccess.FULL_ACCESS`, with no named profile. SDK 0.160.0 maps these to
`ApprovalMode.deny_all` and `Sandbox.full_access`: full host access without
interactive approval. The isolated role config also sets `approval_policy =
"never"`, `sandbox_mode = "danger-full-access"`, and disables plugins, built-in
agents and multi-agent tools. Broker dependency/check/browser commands use the
same trusted host through the owned native process launcher, with no filesystem
or domain allowlist. No extra compiler, SDK or browser redirect list is needed.

This is an explicit trust tradeoff. It does **not** isolate hostile source or
commands from host files, network or credentials. Private role homes and a
sanitized environment avoid accidental credential inheritance; they are not
non-bypassable restrictions. Controller ancestry guards prevent ordinary nested
Devflow calls. Source-scope checks, candidate identity, finite role/repair limits,
deadlines, owned process/port cleanup, synthetic fixture QA and the
`published_unmerged` endpoint remain controller contracts. Preparation records a
separate mode-bound proof and confirms the launcher and ancestry guard; it does
not claim constrained sandbox denials.

New terminal runs synchronize tracker status and read back assignment, Project
and claim through the existing tracker helper. Blocked/cancelled outcomes select
Blocked; delivered outcomes select In review. Claims release only after proven
process and resource cleanup. Pending/failed readback replaces earlier tracker
success in the projection, and a pending delivered tracker cannot establish
successful delivery. Older inputs without `terminal_tracker_version` retain
legacy replay behavior.

Bounded preserved-candidate policy-change recovery is being implemented in this
repair. Installation, service restart, managed recovery, merging and deployment
require the separately assigned operational verification.
