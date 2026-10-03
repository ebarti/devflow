"""Blocking question admission and deterministic originating-thread callbacks."""

from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest
from mcp.server.fastmcp import Context
from mcp.server.fastmcp.exceptions import ToolError
from mcp.shared.context import RequestContext
from mcp.types import RequestParams
from test_delivery_intake import intake_fixture  # noqa: F401

from devflow_temporal.delivery_api import create_app
from devflow_temporal.delivery_control import main as delivery_main
from devflow_temporal.delivery_mcp import build_server
from devflow_temporal.delivery_origin import bind_origin, metadata_origin

THREAD = "01a0c8be-e849-7ef2-ad81-78ccdb4b4275"
OTHER = "01a100ac-efd3-7dd2-9f25-504381f0dcd9"


@pytest.mark.parametrize("invalid", [None, False, 5, {}, "thread-name", THREAD.upper(), "0" * 36])
def test_invalid_origin_rejected_before_claim(intake_fixture, invalid):
    path, request = intake_fixture
    store = create_app(path).state.delivery.store
    with pytest.raises(ValueError, match="canonical UUID"):
        store.submit({**request, "origin_thread_id": invalid})
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM delivery_runs").fetchone()[0] == 0
        assert store.state.row(db, "works", request["work_id"]) is None


def test_origin_is_optional_immutable_and_not_captured_from_service_env(intake_fixture, monkeypatch):
    path, request = intake_fixture
    monkeypatch.setenv("CODEX_THREAD_ID", OTHER)
    store = create_app(path).state.delivery.store
    receipt = store.submit({**request, "origin_thread_id": THREAD})
    assert store.spec("run-1")["origin_thread_id"] == THREAD
    assert store.spec("run-1")["blocking_questions_version"] == 1
    assert store.submit({**request, "origin_thread_id": THREAD}) == receipt
    with pytest.raises(ValueError, match="different inputs"):
        store.submit({**request, "origin_thread_id": OTHER})
    assert "origin_thread_id" not in bind_origin(request, None)
    with pytest.raises(ValueError, match="disagrees"):
        bind_origin({**request, "origin_thread_id": THREAD}, OTHER)


@pytest.mark.parametrize("metadata", [
    {"x-codex-turn-metadata": {"thread_id": THREAD}},
    {"x-codex-turn-metadata": json.dumps({"thread_id": THREAD})},
    {"openai/threadId": THREAD}, {"openai/thread_id": THREAD},
])
def test_metadata_origin_aliases(metadata):
    assert metadata_origin(metadata) == THREAD
    with pytest.raises(ValueError, match="disagrees"):
        metadata_origin({**metadata, "openai/thread_id": OTHER, "openai/threadId": THREAD})


@pytest.mark.asyncio
async def test_mcp_uses_each_actual_request_context_not_daemon_env(intake_fixture, monkeypatch):
    path, request = intake_fixture
    monkeypatch.setenv("CODEX_THREAD_ID", OTHER)
    seen = []
    monkeypatch.setattr("devflow_temporal.delivery_mcp.client", lambda _path: SimpleNamespace(
        submit=lambda value: seen.append(value) or value
    ))
    server = build_server(path)
    tool = server._tool_manager.get_tool("submit_run")
    for origin in (THREAD, OTHER, None):
        meta = RequestParams.Meta.model_validate(
            {"x-codex-turn-metadata": {"thread_id": origin}} if origin else {}
        )
        ctx = Context(request_context=RequestContext(
            request_id="request", meta=meta, session=None, lifespan_context=None,
        ), fastmcp=server)
        await tool.run({"request_json": json.dumps(request)}, context=ctx)
    assert [value.get("origin_thread_id") for value in seen] == [THREAD, OTHER, None]
    with pytest.raises(ToolError, match="disagrees"):
        await tool.run({"request_json": json.dumps({**request, "origin_thread_id": THREAD})},
                       context=Context(request_context=RequestContext(
                           request_id="request", meta=RequestParams.Meta.model_validate(
                               {"openai/threadId": OTHER}
                           ), session=None, lifespan_context=None,
                       ), fastmcp=server))
    assert len(seen) == 3


def test_cli_captures_caller_origin_before_public_submit(intake_fixture, monkeypatch, tmp_path):
    path, request = intake_fixture
    body = tmp_path / "request.json"
    body.write_text(json.dumps(request))
    monkeypatch.setenv("CODEX_THREAD_ID", THREAD)
    seen = []
    class Client:
        def submit(self, supplied):
            seen.append(supplied)
            return {"run_id": "run-1"}
        def __getattr__(self, _name):
            return lambda *_args: None
    monkeypatch.setattr("devflow_temporal.delivery_control.api_client", lambda _path: Client())
    monkeypatch.setattr(sys, "argv", ["devflow-delivery", "--config", str(path), "submit",
                                      "--request", str(body)])
    delivery_main()
    assert seen[0]["origin_thread_id"] == THREAD
    body.write_text(json.dumps({**request, "origin_thread_id": OTHER}))
    with pytest.raises(SystemExit) as failure:
        delivery_main()
    assert failure.value.code == 1
    assert len(seen) == 1
