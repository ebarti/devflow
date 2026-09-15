# Architecture

The host discovers eight sibling skill directories. An agent selects the skill that fits the user's request, applies its engineering method, and invokes existing tools directly. The main [devflow skill](../skills/devflow/SKILL.md) owns the shared helper instructions.

`skills/devflow/scripts/state.py` uses Python's standard library to store local records, reserve work for an owning task and query metrics. Issue claims are atomic within a shared SQLite database. Independent work executes outside those short transactions.

`skills/devflow/scripts/github.py` uses the existing authenticated `gh` CLI to assign issues and update status labels. It verifies ownership before writing and verifies the remote result before recording success or releasing the claim. The agent supplies outcomes and usage estimates; neither helper starts agents or runs project checks. Host task locators remain local. Claims have no automatic expiry, heartbeat or cross-host exclusion.

The installer links skill directories into a supplied destination. Updating the source clone updates the linked skills. Project policies and user-selected model settings stay with their existing owners.

See the [README diagrams](../README.md#how-it-works), [storage contract](implementation-contracts.md) and [helper commands](../skills/devflow/references/state.md).
