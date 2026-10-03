"""Official MCP stdio transport over the authenticated local delivery API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations

from .delivery_client import client
from .delivery_origin import bind_origin, metadata_origin


def build_server(config_path: Path) -> FastMCP:
    server = FastMCP(
        "Devflow local delivery",
        instructions=(
            "Discover configured repository keys and base refs with get_service before submission. "
            "The local service owns roles, planning and authorized GitHub delivery through an "
            "unmerged PR. Use status/evidence on request; dashboard SSE supplies progress. "
            "Keep mutation IDs stable after uncertain responses; inspect the run before retrying."
        ),
    )
    read = ToolAnnotations(
        readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False
    )
    write = ToolAnnotations(
        readOnlyHint=False, destructiveHint=False, idempotentHint=True, openWorldHint=True
    )

    @server.tool(annotations=read)
    def get_service() -> dict:
        """Read local service health and public repository/role policy; starts it if stopped."""
        return client(config_path).service()

    @server.tool(annotations=write)
    def submit_run(request_json: str, ctx: Context) -> dict:
        """Submit a raw goal; plan_approval=required opts into human plan review."""
        value = json.loads(request_json)
        if not isinstance(value, dict):
            raise ValueError("submit request must be a JSON object")
        meta = ctx.request_context.meta
        metadata = meta.model_dump(by_alias=True) if meta is not None else None
        return client(config_path).submit(bind_origin(value, metadata_origin(metadata)))

    @server.tool(annotations=read)
    def list_runs() -> dict:
        """List compact, factual status for recent local delivery runs."""
        return client(config_path).runs()

    @server.tool(annotations=read)
    def get_run(run_id: str) -> dict:
        """Read a run's current state, evidence index, and durable event timeline."""
        return client(config_path).status(run_id)

    @server.tool(annotations=read)
    def read_evidence(run_id: str, evidence_id: str) -> dict:
        """Read one indexed, contained evidence artifact for a run."""
        return client(config_path).evidence(run_id, evidence_id)

    @server.tool(annotations=write)
    def answer_decision(run_id: str, request_json: str) -> dict:
        """Answer a question or accept/change a plan with command ID and revisions."""
        return client(config_path).decision(run_id, json.loads(request_json))

    @server.tool(annotations=ToolAnnotations(
        readOnlyHint=False, destructiveHint=True, idempotentHint=True, openWorldHint=True
    ))
    def cancel_run(run_id: str, request_json: str) -> dict:
        """Request a role-boundary cancellation with command ID and expected revision."""
        return client(config_path).cancel(run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def recovery_preflight(run_id: str) -> dict:
        """Inspect a stopped unpublished candidate and seal its policy recovery preconditions."""
        return client(config_path).recovery_preflight(run_id)

    @server.tool(annotations=write)
    def recover_execution(run_id: str, request_json: str) -> dict:
        """Grant one bounded trusted-local recovery using an explicit fresh preflight hash."""
        return client(config_path).recover_execution(run_id, json.loads(request_json))

    @server.tool(annotations=write)
    def reconcile_tracker(run_id: str, request_json: str) -> dict:
        """Resume three terminal readback attempts; no candidate or model authority changes."""
        return client(config_path).reconcile_tracker(run_id, json.loads(request_json))

    @server.tool(annotations=write)
    def reconcile_published_metadata(run_id: str, request_json: str) -> dict:
        """Explicit stopped-owned-range metadata correction, followed only by fresh gates."""
        return client(config_path).reconcile_published_metadata(run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def gates_only_preflight(run_id: str) -> dict:
        """Inspect authentic stopped investigation custody without admitting a role or effect."""
        return client(config_path).gates_only_preflight(run_id)

    @server.tool(annotations=write)
    def admit_gates_only(run_id: str, request_json: str) -> dict:
        """One explicit investigation admission: same iteration, all gates, no implementation."""
        return client(config_path).admit_gates_only(run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def metadata_preflight(run_id: str, request_json: str) -> dict:
        """Observe stopped owned publication authority before metadata reconciliation."""
        return client(config_path).metadata_preflight(run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def repair_admission_preflight(run_id: str, request_json: str) -> dict:
        """Read the exact failed gate, effective lineage and bounded repair authority."""
        return client(config_path).repair_admission_preflight(run_id, json.loads(request_json))

    @server.tool(annotations=write)
    def continue_repair(run_id: str, request_json: str) -> dict:
        """Admit the existing one-time stopped-run repair with its exact command identity."""
        return client(config_path).continue_repair(run_id, json.loads(request_json))

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
