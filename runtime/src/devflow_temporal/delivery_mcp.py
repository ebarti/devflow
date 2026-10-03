"""Official MCP stdio transport over the authenticated local delivery API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mcp.server.fastmcp import Context, FastMCP

from .delivery_client import client
from .delivery_origin import bind_origin, metadata_origin


def build_server(config_path: Path) -> FastMCP:
    server = FastMCP("Devflow local delivery")

    @server.tool()
    def submit_run(request_json: str, ctx: Context) -> dict:
        """Submit a raw goal; plan_approval=required opts into human plan review."""
        value = json.loads(request_json)
        if not isinstance(value, dict):
            raise ValueError("submit request must be a JSON object")
        meta = ctx.request_context.meta
        metadata = meta.model_dump(by_alias=True) if meta is not None else None
        return client(config_path).submit(bind_origin(value, metadata_origin(metadata)))

    @server.tool()
    def list_runs() -> dict:
        """List compact, factual status for recent local delivery runs."""
        return client(config_path).runs()

    @server.tool()
    def get_run(run_id: str) -> dict:
        """Read a run's current state, evidence index, and durable event timeline."""
        return client(config_path).status(run_id)

    @server.tool()
    def read_evidence(run_id: str, evidence_id: str) -> dict:
        """Read one indexed, contained evidence artifact for a run."""
        return client(config_path).evidence(run_id, evidence_id)

    @server.tool()
    def answer_decision(run_id: str, request_json: str) -> dict:
        """Answer a question or accept/change a plan with command ID and revisions."""
        return client(config_path).decision(run_id, json.loads(request_json))

    @server.tool()
    def cancel_run(run_id: str, request_json: str) -> dict:
        """Request a role-boundary cancellation with command ID and expected revision."""
        return client(config_path).cancel(run_id, json.loads(request_json))

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
