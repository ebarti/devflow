# Developer Workflow

A reusable development workflow with a local `devflow` CLI, a focused host skill, repository profiles, and explicit execution evidence.

The initial implementation follows the [architecture](docs/architecture.md), [contracts](docs/implementation-contracts.md), and [implementation plan](docs/implementation-plan.md). JobCtrl is the first planned adopter.

Workflow code, skills, schemas, and operating documentation are released together. Consumers pin a release and full Git commit in `.devflow/workflow.lock`. Runtime records, credentials, machine bindings, and usage evidence stay outside this repository.

Implementation and acceptance status will be recorded here as the package is built.
