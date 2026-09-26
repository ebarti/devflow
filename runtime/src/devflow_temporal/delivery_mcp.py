"""Official MCP stdio transport over the authenticated local delivery API."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .delivery_client import client


def build_server(config_path: Path) -> FastMCP:
    server = FastMCP("Devflow local delivery")

    @server.tool()
    def submit_run(request_json: str) -> dict:
        """Submit an authorized run as JSON; return its durable run ID and dashboard URL."""
        value = json.loads(request_json)
        if not isinstance(value, dict):
            raise ValueError("submit request must be a JSON object")
        return client(config_path).submit(value)

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
        """Answer a pending decision with command ID and revision checks."""
        return client(config_path).decision(run_id, json.loads(request_json))

    @server.tool()
    def cancel_run(run_id: str, request_json: str) -> dict:
        """Request a role-boundary cancellation with command ID and expected revision."""
        return client(config_path).cancel(run_id, json.loads(request_json))

    return server


def main() -> None:
    parser = argparse.ArgumentParser(prog="devflow-delivery-mcp")
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args()
    build_server(args.config.resolve(strict=True)).run(transport="stdio")


if __name__ == "__main__":
    main()
