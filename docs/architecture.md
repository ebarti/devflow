# Architecture

The host discovers nine sibling skill directories. An agent selects the skill that fits the user's request, applies its engineering method, and invokes existing tools directly. The main [devflow skill](../skills/devflow/SKILL.md) owns the shared helper instructions.

`skills/devflow/scripts/state.py` uses Python's standard library to store local records in SQLite and query metrics. The agent supplies observed outcomes and usage estimates. The helper runs no agents, Git commands, project checks or external operations.

The installer links skill directories into a supplied destination. Updating the source clone updates the linked skills. Project policies and user-selected model settings stay with their existing owners.

See the [README diagrams](../README.md#how-it-works), [storage contract](implementation-contracts.md) and [helper commands](../skills/devflow/references/state.md).
