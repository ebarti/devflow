# Contributor guide

Keep the skills short and independently discoverable. Put shared helper guidance under `skills/devflow/references/`; use relative links between sibling skills so installation preserves them. Keep command examples aligned with the Python helper and describe current behavior only.

Use existing host, Git, GitHub and project tools directly. Add supporting code only for a concrete storage or reporting need. Preserve user scope, settings and unrelated work.

The package's acceptance check is `bash scripts/check-install.sh`: installation into a temporary destination with expected skill links and entrypoints. CI runs only that smoke check. Do not add package test suites, QA/review gates or delivery automation. Projects using the skills keep their own verification policies.

[Architecture](architecture.md) · [Storage contract](implementation-contracts.md)
