---
name: devflow
description: Use Devflow for a development request, role selection, local work records, or workflow metrics.
---

# Devflow

Follow the user's requested outcome and the target project's instructions. Start authorized work without an extra approval gate. Discussion alone creates no work; loading a skill creates no issues, agents or backlog activity. Use the host's tools, Git, `gh` and project commands directly, with the user's model settings.

Select only the role needed:

| Intent | Skill |
| --- | --- |
| Clarify a request or explore a design | [Defining work](../devflow-defining-work/SKILL.md) |
| Plan architecture, slices or coverage | [Planning](../devflow-planning/SKILL.md) |
| Carry out a defined request or continue actual work | [Coordinating](../devflow-coordinating/SKILL.md) |
| Change or repair code | [Implementing](../devflow-implementing/SKILL.md) |
| Review an existing candidate | [Reviewing](../devflow-reviewing/SKILL.md) |
| Reproduce behavior or run project checks | [Verifying](../devflow-verifying/SKILL.md) |
| Publish, merge or reconcile delivery | [Delivering](../devflow-delivering/SKILL.md) |

For substantive work, use [the state helper](references/state.md) to retain a short outcome, relevant runs/results/findings and useful recovery context. Invoke the Python script while following the skill; it only records and queries data. Reuse the work ID for repairs and continuation. Record known usage or estimates with their provenance; leave unavailable values unknown. Do not create bookkeeping for ordinary questions.

For requested installation, run `bash scripts/install.sh [SKILLS_DIRECTORY]` from the source clone. It defaults to `~/.agents/skills`, links sibling skills and preserves conflicting paths. Update the source clone to update linked skills. Installation does not change global instructions or configure project tools.
