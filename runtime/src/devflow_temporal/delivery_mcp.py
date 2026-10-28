"""Official MCP stdio transport over the authenticated local delivery API."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations

from .delivery_client import client, read_only_client
from .delivery_origin import bind_origin, metadata_origin


def build_server(config_path: Path) -> FastMCP:
    server = FastMCP(
        "Devflow local delivery",
        instructions=(
            "Discover configured repository keys and base refs with get_service before submission. "
            "The local service owns roles, planning and authorized GitHub delivery through an "
            "unmerged PR or one feature-owned stack. In feature mode GitHub owns the parent "
            "issue, sub-issues and accepted plan; continue_feature resumes stopped ownership "
            "without a replacement stack. revise_feature_plan requests a recorded correction "
            "of a demonstrated planning defect within the same feature authority "
            "and repair budget. "
            "Use merge_run or merge_feature only on an explicit merge instruction. "
            "Use status/evidence on request; dashboard SSE supplies progress. "
            "Preserve the original goal. Detailed or multi-sentence goals require a separate "
            "publication_summary describing the actual change: a single line, at most 120 "
            "characters including its Conventional Commit type, without extra sentences or "
            "control/bidi characters. Include it on superseding submissions too. "
            "Read tools require an already running service and never start it. "
            "For an authorized delivery or mutation, use start_service if a prerequisite "
            "read reports transport unavailable, then repeat the required reads. "
            "Status/evidence requests and callbacks alone do not authorize startup. "
            "Keep mutation IDs and the complete request stable after uncertain responses; "
            "inspect the run before retrying."
        ),
    )
    read = ToolAnnotations(
        readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
    )
    write = ToolAnnotations(
        readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=True
    )

    async def forward_write(method: str, *args) -> dict:
        # Startup performs synchronous Temporal readiness work using asyncio.run.
        # Both client creation and its one mutation belong outside the MCP loop.
        def invoke() -> dict:
            return getattr(client(config_path), method)(*args)

        try:
            return await asyncio.to_thread(invoke)
        except ValueError as exc:
            from .delivery_lifecycle import RecoveryRefused

            if isinstance(exc, RecoveryRefused):
                return {"state": "refused", "assessment": exc.assessment}
            raise

    @server.tool(annotations=write)
    async def create_operation(request_json: str) -> dict:
        """Submit an exact operation envelope; retain its ID and inspect pending results.

        Requires the service already running. A timeout does not cancel the operation.
        """
        request = json.loads(request_json)
        return await asyncio.to_thread(read_only_client(config_path).create_operation, request)

    @server.tool(annotations=read)
    def get_operation(operation_id: str) -> dict:
        """Read a durable operation without starting the service."""
        return read_only_client(config_path).operation(operation_id)

    @server.tool(annotations=read)
    def get_run_operations(run_id: str) -> dict:
        """Read the most recent durable operations for a run."""
        return read_only_client(config_path).run_operations(run_id)

    @server.tool(annotations=write)
    async def reconcile_operation(operation_id: str) -> dict:
        """Inspect the original domain receipt without repeating the effect."""
        return await asyncio.to_thread(
            read_only_client(config_path).reconcile_operation, operation_id
        )

    @server.tool(annotations=read)
    def get_service() -> dict:
        """Read local health and public policy; report unavailable if stopped."""
        return read_only_client(config_path).service()

    @server.tool(annotations=write)
    async def start_service() -> dict:
        """Start for an authorized delivery or mutation; may activate pending work."""
        return await forward_write("service")

    @server.tool(annotations=write)
    async def submit_run(request_json: str, ctx: Context) -> dict:
        """Submit a goal with publication_summary for detailed execution instructions.

        Preserve the original goal; detailed or multi-sentence instructions require
        publication_summary describing the actual change as a single line, at most
        120 characters including its Conventional Commit type, without extra
        sentences or control/bidi characters. Include it on superseding submissions
        too, preserving the predecessor's goal. A short single-sentence goal may
        omit it; the service prefixes a plain goal with chore:. Keep the whole request
        stable on retries. plan_approval=required opts into human plan review.
        """
        value = json.loads(request_json)
        if not isinstance(value, dict):
            raise ValueError("submit request must be a JSON object")
        meta = ctx.request_context.meta
        metadata = meta.model_dump(by_alias=True) if meta is not None else None
        return await forward_write("submit", bind_origin(value, metadata_origin(metadata)))

    @server.tool(annotations=read)
    def list_runs(limit: int = 50, cursor: str | None = None, archived: bool = False) -> dict:
        """Read one bounded run page; pass next_cursor to explicitly read older history."""
        return read_only_client(config_path).runs(limit=limit, cursor=cursor, archived=archived)

    @server.tool(annotations=read)
    def get_run(run_id: str) -> dict:
        """Read the bounded persisted run projection without fresh eligibility proof."""
        return read_only_client(config_path).status(run_id)

    @server.tool(annotations=read)
    def get_run_detail(run_id: str) -> dict:
        """Explicitly reconstruct run detail, evidence index and timeline."""
        return read_only_client(config_path).detail(run_id)

    @server.tool(annotations=read)
    def read_evidence(run_id: str, evidence_id: str) -> dict:
        """Read one indexed, contained evidence artifact for a run."""
        return read_only_client(config_path).evidence(run_id, evidence_id)

    @server.tool(annotations=write)
    async def answer_decision(run_id: str, request_json: str) -> dict:
        """Answer a question, review a plan, or authorize the recorded feature merge."""
        return await forward_write("decision", run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def refresh_issue_context(run_id: str, request_json: str) -> dict:
        """Fetch the linked issue, acceptance and evidence without changing run authority.

        Supply a stable command_id. Normal new intake turns fetch automatically;
        this command supplies context to existing runs without replaying roles.
        """
        return await forward_write("refresh_issue_context", run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def continue_feature(run_id: str, request_json: str) -> dict:
        """Continue a stopped feature's exact plan and stack under a successor run.

        Supply command_id and expected_revision from the current projection.
        Ownership must be stopped with workers/effects settled. Earlier repair
        cycles and published PRs remain attached to the same GitHub feature.
        A retained planning-defect build may add one or two unchanged tracked
        files with added_paths, amended_config_path and amended_config_sha256;
        the private config must change only that exact appended file list.
        """
        return await forward_write("continue_feature", run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def revise_feature_plan(run_id: str, request_json: str) -> dict:
        """Request native investigation and review of a stopped feature's planning defect.

        Supply command_id, expected_revision from feature_plan.expected_revision
        (the projection revision), and a bounded reason. Optional evidence is a
        list of existing owned artifact {path, sha256} references. The runtime
        crafts the proposal, preserves scope/acceptance/authority and stack,
        and shares the cumulative repair budget. No plan or policy overrides
        are accepted. Optional reconsideration selects a genuine rejected review
        after an authenticated derivation context change: {version: 1,
        kind: "derivation_context_change", rejected_revision_id, proposal_sha256,
        expected_runtime_revision, expected_runtime_payload_sha256}. It requests
        fresh investigation and independent review only; the original rejection
        and repair use remain. Unchanged, stale or already consumed context refuses
        before admission. Normal approval and adoption checks still apply.
        Keep the full request stable after an uncertain response.
        """
        return await forward_write("revise_feature_plan", run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def merge_feature(run_id: str, request_json: str) -> dict:
        """Merge on explicit user instruction using the pending merge decision's revisions.

        Read get_run and copy decision_id, decision_revision, candidate_revision,
        and protocol revision as expected_revision. Add command_id and answer='merge'.
        The service rechecks the recorded stack, native gates and GitHub checks.
        """
        request = json.loads(request_json)
        if request.get("answer") != "merge":
            raise ValueError("merge_feature requires the explicit merge decision")
        return await forward_write("decision", run_id, request)

    @server.tool(annotations=read)
    def merge_preflight(run_id: str) -> dict:
        """Check a delivered ordinary run's native merge evidence without granting a merge."""
        return read_only_client(config_path).merge_preflight(run_id)

    @server.tool(annotations=write)
    async def merge_run(run_id: str, request_json: str) -> dict:
        """Merge an ordinary delivered PR only on explicit user instruction.

        Read get_run. Supply command_id, expected_revision=protocol_revision,
        candidate_id, head and pr_number from merge, plus answer='merge'. This
        keeps the original run/PR and source evidence, reconciles current main,
        waits for current-target CI and records merge/issue/tracker readback.
        It does not need a feature merge decision. Keep the entire request
        stable after uncertainty; read status before retrying.
        """
        return await forward_write("merge", run_id, json.loads(request_json))

    @server.tool(annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=True
    ))
    async def cancel_run(run_id: str, request_json: str) -> dict:
        """Request a role-boundary cancellation with command ID and expected revision."""
        return await forward_write("cancel", run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def disable_delivery_limits(run_id: str, request_json: str) -> dict:
        """Disable an active run's delivery timers without stopping or restarting it.

        Read get_run first; supply command_id, expected_revision (protocol_revision),
        and a nonempty reason. Keep the complete request stable after uncertainty.
        Requires the API already running; never starts/reloads a service.
        """
        return await asyncio.to_thread(
            read_only_client(config_path).disable_delivery_limits,
            run_id, json.loads(request_json),
        )

    @server.tool(annotations=write)
    async def reconcile_tracker(run_id: str, request_json: str) -> dict:
        """Resume three terminal readback attempts; no candidate or model authority changes."""
        return await forward_write("reconcile_tracker", run_id, json.loads(request_json))


    @server.tool(annotations=read)
    def gates_only_preflight(run_id: str) -> dict:
        """Inspect authentic stopped investigation custody without admitting a role or effect."""
        return read_only_client(config_path).gates_only_preflight(run_id)

    @server.tool(annotations=write)
    async def admit_gates_only(run_id: str, request_json: str) -> dict:
        """One explicit investigation admission: same iteration, all gates, no implementation."""
        return await forward_write("admit_gates_only", run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def assess_transition(run_id: str, kind: str, request_json: str) -> dict:
        """Observe current recovery eligibility; the result grants no execution authority."""
        return read_only_client(config_path).assess_transition(
            run_id, kind, json.loads(request_json))

    @server.tool(annotations=read)
    def repair_admission_preflight(run_id: str, request_json: str) -> dict:
        """Read the exact failed gate, effective lineage and bounded repair authority."""
        return read_only_client(config_path).repair_admission_preflight(
            run_id, json.loads(request_json))

    @server.tool(annotations=write)
    async def continue_repair(run_id: str, request_json: str) -> dict:
        """Continue bounded repair or validate an existing PR's documentation update.

        For published_head_validation, provide command_id, expected_revision
        (protocol), expected_projection_revision, expected_candidate_id,
        expected_pr_number, expected_pr_head and additional_iterations=0.
        The current head must be an in-scope documentation-only fast-forward.
        Existing acceptance is rerun on the same PR; owner holds refuse admission.
        """
        return await forward_write("continue_repair", run_id, json.loads(request_json))

    return server


def main() -> None:
    from .delivery_native_guard import reject_nested_controller

    reject_nested_controller()
    parser = argparse.ArgumentParser(prog="devflow-delivery-mcp")
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    build_server(args.config.resolve(strict=True)).run(transport="stdio")


if __name__ == "__main__":
    main()
