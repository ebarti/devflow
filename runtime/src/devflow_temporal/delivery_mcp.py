"""Official MCP stdio transport over the authenticated local delivery API."""

from __future__ import annotations

import argparse
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
            "unmerged PR. Use status/evidence on request; dashboard SSE supplies progress. "
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

    @server.tool(annotations=read)
    def get_service() -> dict:
        """Read local health and public policy; report unavailable if stopped."""
        return read_only_client(config_path).service()

    @server.tool(annotations=write)
    def start_service() -> dict:
        """Start for an authorized delivery or mutation; may activate pending work."""
        return client(config_path).service()

    @server.tool(annotations=write)
    def submit_run(request_json: str, ctx: Context) -> dict:
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
        return client(config_path).submit(bind_origin(value, metadata_origin(metadata)))

    @server.tool(annotations=read)
    def list_runs(limit: int = 50, cursor: str | None = None, archived: bool = False) -> dict:
        """Read one bounded run page; pass next_cursor to explicitly read older history."""
        return read_only_client(config_path).runs(limit=limit, cursor=cursor, archived=archived)

    @server.tool(annotations=read)
    def get_run(run_id: str) -> dict:
        """Read a run's current state, evidence index, and durable event timeline."""
        return read_only_client(config_path).status(run_id)

    @server.tool(annotations=read)
    def read_evidence(run_id: str, evidence_id: str) -> dict:
        """Read one indexed, contained evidence artifact for a run."""
        return read_only_client(config_path).evidence(run_id, evidence_id)

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

    @server.tool(annotations=write)
    def reconcile_tracker(run_id: str, request_json: str) -> dict:
        """Resume three terminal readback attempts; no candidate or model authority changes."""
        return client(config_path).reconcile_tracker(run_id, json.loads(request_json))


    @server.tool(annotations=read)
    def gates_only_preflight(run_id: str) -> dict:
        """Inspect authentic stopped investigation custody without admitting a role or effect."""
        return read_only_client(config_path).gates_only_preflight(run_id)

    @server.tool(annotations=write)
    def admit_gates_only(run_id: str, request_json: str) -> dict:
        """One explicit investigation admission: same iteration, all gates, no implementation."""
        return client(config_path).admit_gates_only(run_id, json.loads(request_json))

    @server.tool(annotations=read)
    def repair_admission_preflight(run_id: str, request_json: str) -> dict:
        """Read the exact failed gate, effective lineage and bounded repair authority."""
        return read_only_client(config_path).repair_admission_preflight(
            run_id, json.loads(request_json))

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
