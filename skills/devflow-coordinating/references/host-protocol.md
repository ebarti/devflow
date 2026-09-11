# Subagent coordination

The original user conversation coordinates the outcome. Delegate implementation and repairs to an `implementation_worker`; delegate required review and QA to distinct independent agents. Keep bounded ownership, one outcome's durable attempt and the user's authorized endpoint. Tier 0 skips independent gates. Legitimate review-only or delivery-only entry uses existing valid evidence and does not fabricate an implementation assignment.

Use only the supported `agents.spawn_agent`, `agents.followup_task`, `agents.send_message`, `agents.wait_agent`, `agents.list_agents` and `agents.interrupt_agent` tools. The CLI prepares/journals intent; you perform the native call and record its actual outcome. Do not create visible peer tasks for new subagent work or use private desktop APIs.

## Resolve role policy

Resolve each setting from the explicit user role/session override, then the selected role file, then saved subagent defaults, then saved global defaults. Never pass the active coordinator's model/effort as a role default. Workflow roles map to `implementer`, `reviewer` and `qa`; an explicitly selected alternative role name uses that configured file.

Honor `[agents.<role_name>].config_file`, resolving relative paths beside the global configuration, or fall back to adjacent `agents/<role_name>.toml`. Saved `[agents].default_subagent_model` precedes the saved root model. A nonempty model and supported effort must resolve before dispatch. Use the resolver's allowlisted source/settings hashes; never copy full global TOML, secrets, environment or legacy developer instructions into provenance. Explicit override provenance records the choice, not independent user authentication.

Spawn `agent_type=default` with the resolved model and reasoning effort and `fork_turns=none`. A fixed custom agent type or full-history fork may override the requested settings. The configured role name selects settings; the activated brief supplies the workflow role instructions and bounded responsibility.

Record policy changes as new execution segments. No model or effort is hardcoded; a separately authorized model experiment follows workflow stabilization.

## Verify startup before product work

1. Use `host assign` to record the role, bounded ownership/brief, coordinator identity and resolved policy with a journaled action. Use `host prepare` to journal action begin before invoking the returned spawn intent.
2. Send only the bootstrap prompt: report the child's own session metadata path and wait. Do not ask it to inspect, edit, review or test the product before validation. Record the actual receipt through `host record`.
3. The spawn response supplies `task_name` and nickname, without a native UUID. Inventory supplies canonical paths and statuses. Use the returned exact canonical agent path as `agent_name`; do not infer a UUID, effective model or parent from a nickname or inventory.
4. Use `host startup` with the child's explicit local session path. It validates allowlisted `session_meta` and `turn_context` evidence: parent UUID, exact canonical agent path, actual model and reasoning effort. Bind the observed native UUID for attribution only after validation. Missing effective settings block activation; an unknown service tier stays unknown. Keep the source and normalized evidence private and uncommitted.
5. After startup passes, `host activate` prepares `send_role`; `host prepare` durably begins it and returns `followup_task`. Invoke that native follow-up, then `host record` with actual inventory to mark the agent running. The bounded brief includes work/scope/assignment identity, acceptance, repository/base/head, owned paths, constraints, check/evidence references, role skill and expected result. Preserve others' edits in shared checkouts and isolate mutable fixtures/resources.

For reuse after initial activation, `host assign` itself prepares `send_role`; follow it with `host prepare`, the returned native follow-up and `host record`. Do not call `host activate` a second time for that already-prepared action.

Capture the candidate using the verified running implementation assignment and producer identity. Then `host result` binds completion to that captured `output_candidate_id` before verification proceeds. Native metadata proves observed execution, not correctness. Review/QA submit every PASS/FAIL/BLOCKED through `gate record`, bound to the current activation `assignment_action_id`, before repair or replacement; the coordinator cannot substitute its own PASS. Independent producers must differ from the coordinator, implementation worker and one another.

## Reuse and reconcile

Control by canonical `agent_name`; native UUIDs provide attribution. Reuse available workers/reviewers/QA for repairs and reruns. After interruption, use fresh `list_agents` inventory with `host observe` before reassigning the same agent: `interrupt_agent` returns its previous status. Native completion is not implementation proof. Use bounded follow-ups and waits; preserve findings and evidence.

Follow-up has no model override. A changed policy requires an explicitly recorded replacement with fresh startup observation. An unavailable replacement records the prior assignment and actual replacement observation, preserving old identities and history. Transfer only the relevant bounded brief/evidence, not the entire coordinator conversation. Missing observation blocks replacement activation.

A lost or uncertain spawn must reconcile before another launch. A single inventory omission does not prove absence or a failed spawn; do not create duplicates from it. Never manufacture a readback or call an unobserved identity verified. Preserve the ambiguous action when supported observations cannot resolve it.

New attempts default to subagent execution. Historical attempts with missing `execution_mode` stay `native_thread`. Preserve their original IDs, receipts, evidence and control contract, including pending client IDs that were never executable task IDs. Do not rewrite historical task runs as subagents or use their visible-task launch instructions for new work.

For an interrupted round or malformed completed output, use the bounded
[result-recovery protocol](result-recovery.md). Preserve the original producer,
activation, bytes and rejected import before changing any workflow input.
