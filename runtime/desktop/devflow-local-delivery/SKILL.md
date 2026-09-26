---
name: devflow-local-delivery
description: Submit an already accepted and authorized Devflow managed run, or inspect a named local run or indexed evidence on request. Use for explicit local-runtime handoffs, not ordinary Devflow planning, review, or coding requests.
---

# Devflow local delivery

Use the registered `devflow-local-delivery` MCP server. Its tools call the same authenticated local service as `devflow-delivery`; the service, not this conversation, owns workflow progress, roles, gates, and tracker writes. If its tools are unavailable in this thread, check installation in a fresh Desktop thread; use the public CLI only with explicit runtime and config paths. Do not substitute native Codex agents for runtime roles or infer completion from a tool call.

Submit only a scope with an accepted plan and an already authorized `published_unmerged` endpoint. Supply `submit_run` with a JSON string containing exactly `command_id`, `run_id`, `work_id`, `issue_url`, `repository_key`, `goal`, `accepted_plan`, `base_ref`, `branch`, and `authorized_endpoint: "published_unmerged"`; add `recovery_key` only when the accepted scope uses an allowlisted recovery manifest. The repository key and base ref must come from the configured service policy. Do not supply a filesystem repository path, model choice, service credential, or broader endpoint. If an input or authorization is missing, resolve that through the existing Devflow planning/authorization flow before submitting.

Keep `command_id`, `run_id`, and the request body stable across a retry. If a response is uncertain, inspect the run before retrying; never create a second run merely because the first receipt was lost. Report the returned run ID, phase, and dashboard URL. If `open_in_codex` is available, open a returned loopback HTTP dashboard URL in its browser panel. Never put the service token in a URL or rendered output. End the turn after the handoff; the dashboard's SSE connection updates without model polling.

Use `list_runs`, `get_run`, or `read_evidence` when the user asks about status/evidence or a meaningful decision, failure, or completion needs interpretation. Do not schedule routine status calls. For an explicitly requested `answer_decision` or `cancel_run`, read the current run first and send a new command ID with the observed protocol revision and, for a decision, its decision and candidate revisions. Treat a stale/conflict response as a reason to refresh and ask for a new choice. Never merge, release, or deploy from this skill.
