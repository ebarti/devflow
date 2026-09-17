# Devflow

Small development skills for OpenAI Codex CLI agents, with Python helpers for work ownership, GitHub issue status and metrics. The agent follows the relevant skill and uses its host tools, Git, GitHub CLI and project commands directly.

## Prerequisites

Devflow targets Codex CLI: skills load from its skills directory, metrics hooks use its `hooks.json` and transcript format, and delegation uses its agent tools. Other hosts that discover `SKILL.md` directories can load the role skills, but hooks, metrics, candidate trials and worker spawning are Codex-specific. Supply Python 3.12+, a POSIX shell, Git and the tools required by your projects. Delegated implementation needs a configurable `worker` agent role and access to the worker model; see the [implementation worker reference](skills/devflow/references/implementation-worker.md). GitHub work requires authenticated `gh`; Project tracking also needs access to the selected existing Project (`project` scope for an OAuth token). Other tools need their usual authentication and permissions. Devflow does not check or install prerequisites or manage authentication; behavior with missing prerequisites is undefined.

## Install

Keep a release checkout at a stable location; installed skills are symlinks into it.

```sh
git clone --branch v0.1.0 https://github.com/ebarti/devflow.git
cd devflow
bash scripts/install.sh
```

The installer uses `python3.12`; set `DEVFLOW_PYTHON` to select another supported interpreter. Hooks record its resolved absolute executable path at installation, so later `PATH` changes do not switch Python. Reinstall to change the interpreter. The helpers use only the standard library; no pip dependencies are required.

The defaults are `~/.agents/skills` and `$CODEX_HOME/hooks.json` (`~/.codex/hooks.json` when unset). Supply custom locations when needed:

```sh
bash scripts/install.sh /path/to/host/skills /path/to/codex-home
```

To switch an existing installation to another checkout, run this from that checkout:

```sh
bash scripts/install.sh --force
```

`--force` replaces only bundled skill symlinks, including broken links. Regular files, directories and other hooks are preserved. Without it, conflicting paths stop installation. Review and trust the metrics hooks with `/hooks`; use a fresh task after installation. Agent instructions and target repositories are not modified. Automatic collection requires a host supporting the documented Codex hook interface.

## Upgrade

Choose a [release tag](https://github.com/ebarti/devflow/releases) and upgrade the existing checkout between tasks:

```sh
bash scripts/update.sh v0.1.0
```

The updater fetches that tag, checks out its commit and reruns installation. Reuse custom directory arguments and `DEVFLOW_PYTHON` when applicable. Tracked edits stop the upgrade. Obsolete skill links owned by this checkout are removed; other files and SQLite records are preserved. Supported database migrations run on the next helper use. Review changed hooks with `/hooks`, then start a fresh task. Updates are explicit; `main` contains unreleased work.

## Candidate trials

From a development worktree, use a new trial directory for each candidate and a separate target-project worktree:

```sh
python3.12 scripts/candidate.py /path/to/trial -C /path/to/project-worktree
```

The launcher isolates skills, hook configuration, sessions and SQLite, disables the normal Devflow skills in that session, and records the source commit in `candidate.json`. Its generated configuration belongs to the trial. Authenticate that session with `candidate.py /path/to/trial login`, then review its hooks with `/hooks`. `--prepare-only` prepares the directories without starting a session. Freeze the candidate while a trial runs and retain its metrics with the recorded commit.

Publish a new release tag after the installation smoke check and the selected product trial pass. Release tags remain fixed; normal installations advance only through an explicit upgrade.

## Start

Ask for the outcome you want, for example: “Use devflow to fix the retry bug in this repository.” Or invoke a role such as `$devflow-reviewing` for a specific review. Skills are independently discoverable; loading one does not create work, issues or agents, or resume a backlog.

Implementation changes, including small fixes and review or QA repairs, run in a dedicated implementation worker; the default is **Sol / high** (`gpt-5.6-sol`, effort `high`). An explicit user or project instruction naming a model or effort takes precedence, the coordinator never falls back to its own model, and when the worker cannot be spawned it stops and asks. Related follow-ups reuse the same agent within one role, issue and worktree while preserving required review and QA independence. Other roles keep their selected models. The [implementation worker reference](skills/devflow/references/implementation-worker.md) is the single definition.

| Skill | Use |
| --- | --- |
| [devflow](skills/devflow/SKILL.md) | Shared method, role selection and state helper |
| [devflow-defining-work](skills/devflow-defining-work/SKILL.md) | Clarify outcomes and investigate unclear requests |
| [devflow-planning](skills/devflow-planning/SKILL.md) | Plan consequential changes and coverage |
| [devflow-coordinating](skills/devflow-coordinating/SKILL.md) | Carry out a defined request and recover ongoing work |
| [devflow-implementing](skills/devflow-implementing/SKILL.md) | Implement or repair code |
| [devflow-reviewing](skills/devflow-reviewing/SKILL.md) | Review a candidate and verify findings |
| [devflow-verifying](skills/devflow-verifying/SKILL.md) | Reproduce behavior and run project checks |
| [devflow-delivering](skills/devflow-delivering/SKILL.md) | Publish, merge or reconcile requested delivery |

## How it works

```mermaid
flowchart LR
    User[User request] --> Agent[Agent follows skill]
    Agent --> Tools[Host tools, Git, gh, project commands]
    Agent --> Helper[Python state helper]
    Helper --> DB[(Local SQLite)]
    Agent --> GitHub[GitHub helper through gh]
    GitHub --> Issues[Issue assignee and Project Status]
    GitHub --> DB
```

Enter at the role that fits the request. A small fix needs no separate planning exercise; a review-only request starts with review. Project rules determine checks, independent review and delivery requirements.

```mermaid
flowchart LR
    Request[Implementation request] --> Claim[Claim issue and set in progress]
    Claim --> Work[Define or implement]
    Work --> Check[Check as project requires]
    Check -->|Repair needed| Work
    Check --> Deliver[Deliver within requested scope]
    Deliver --> Record[Update issue and release claim]
```

Independent issues follow this flow concurrently in separate worktrees, each with one owner and work ID. The GitHub helper creates or reuses the issue, assigns the accountable user and updates its existing Project Status. Ownership rules, concurrency and interruption handling are defined once in [issue ownership](skills/devflow/references/ownership.md).

The state helper stores work, claims, runs, results, findings and usage; installed hooks add content-free runtime observations for bound tasks. What is collected, how usage is attributed and what is deliberately not inferred are defined once in [work records and metrics](skills/devflow/references/state.md).

`state.py metrics` reports outcomes, roles and models, delivery, ownership, recovery, timing, usage and coverage; add `--work-id ID` for one issue.

The database location and helper commands are documented in [work records and metrics](skills/devflow/references/state.md); storage semantics are in the [storage contract](docs/implementation-contracts.md).

## Checks

```sh
bash scripts/check-install.sh
```

Devflow CI runs this installation smoke check and the helper unit tests in `tests/`:

```sh
python3.12 -m unittest discover -s tests
```

It does not run target projects' suites, review/QA gates or delivery automation; target projects retain their own check policies.

[Architecture](docs/architecture.md)
