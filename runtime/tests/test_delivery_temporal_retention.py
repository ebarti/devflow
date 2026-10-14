"""Namespace authority must survive a bounded closed-workflow recovery window."""

from __future__ import annotations

import argparse
import json
import time
from types import SimpleNamespace

import pytest
from google.protobuf.duration_pb2 import Duration
from temporalio.api.enums.v1 import ArchivalState, NamespaceState
from temporalio.api.namespace.v1 import NamespaceConfig, NamespaceInfo
from temporalio.api.replication.v1 import ClusterReplicationConfig, NamespaceReplicationConfig
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest, DescribeNamespaceResponse
from test_delivery_startup import _manifest
from test_delivery_startup import config as config

from devflow_temporal import delivery_control as control
from devflow_temporal.delivery_temporal_retention import (
    MINIMUM_RETENTION_SECONDS,
    ensure_namespace_retention,
)

REAL_STARTUP_CHECK = control._ensure_namespace_retention


class Namespace:
    def __init__(self, seconds=86400, *, failure=None, tamper=None):
        self.failure, self.tamper = failure, tamper
        self.calls = []
        self.value = DescribeNamespaceResponse(
            namespace_info=NamespaceInfo(
                name="default", id="original-namespace", description="owned",
                state=NamespaceState.NAMESPACE_STATE_REGISTERED, data={"owner": "delivery"}),
            config=NamespaceConfig(workflow_execution_retention_ttl=Duration(seconds=seconds),
                history_archival_state=ArchivalState.ARCHIVAL_STATE_DISABLED,
                visibility_archival_state=ArchivalState.ARCHIVAL_STATE_DISABLED,
                custom_search_attribute_aliases={"Keyword01": "existing-alias"}),
            replication_config=NamespaceReplicationConfig(active_cluster_name="active",
                clusters=[ClusterReplicationConfig(cluster_name="active")]),
        )
        self.client = SimpleNamespace(namespace="default", workflow_service=self)

    async def describe_namespace(self, request, **_kwargs):
        assert request == DescribeNamespaceRequest(namespace="default", weak_consistency=False)
        self.calls.append("describe")
        if self.failure == "describe" or self.failure == "readback" and "update" in self.calls:
            raise ConnectionError("namespace read unavailable")
        value = DescribeNamespaceResponse()
        value.CopyFrom(self.value)
        return value

    async def update_namespace(self, request, **_kwargs):
        self.calls.append("update")
        assert request.namespace == "default"
        assert [field.name for field, _ in request.ListFields()] == ["namespace", "config"]
        assert [field.name for field, _ in request.config.ListFields()] == [
            "workflow_execution_retention_ttl"]
        assert request.config.workflow_execution_retention_ttl.seconds == MINIMUM_RETENTION_SECONDS
        if self.failure == "update":
            raise PermissionError("namespace update denied")
        self.value.config.workflow_execution_retention_ttl.CopyFrom(
            request.config.workflow_execution_retention_ttl)
        if self.tamper:
            self.tamper(self.value)


async def test_exact_24_hour_floor_is_raised_and_repeated_start_is_idempotent():
    namespace = Namespace()
    original = DescribeNamespaceResponse()
    original.CopyFrom(namespace.value)
    await ensure_namespace_retention(namespace.client)
    assert namespace.value.config.workflow_execution_retention_ttl.seconds == 90 * 86400
    retained = DescribeNamespaceResponse()
    retained.CopyFrom(namespace.value)
    retained.config.workflow_execution_retention_ttl.CopyFrom(
        original.config.workflow_execution_retention_ttl)
    assert retained == original
    assert namespace.calls == ["describe", "update", "describe"]
    await ensure_namespace_retention(namespace.client)
    assert namespace.calls == ["describe", "update", "describe", "describe"]


@pytest.mark.parametrize("days", [90, 180])
async def test_existing_minimum_or_higher_retention_is_preserved(days):
    namespace = Namespace(days * 86400)
    original = namespace.value.SerializeToString(deterministic=True)
    await ensure_namespace_retention(namespace.client)
    assert namespace.calls == ["describe"]
    assert namespace.value.SerializeToString(deterministic=True) == original


@pytest.mark.parametrize("failure", ["describe", "readback"])
async def test_namespace_rpc_failure_is_explicit(failure):
    namespace = Namespace(failure=failure)
    with pytest.raises(RuntimeError, match="retention validation failed"):
        await ensure_namespace_retention(namespace.client)
    assert namespace.calls == {
        "describe": ["describe"], "update": ["describe", "update"],
        "readback": ["describe", "update", "describe"],
    }[failure]


@pytest.mark.parametrize(
    "tamper",
    [
        lambda value: setattr(value.namespace_info, "id", "replacement"),
        lambda value: setattr(value.replication_config, "active_cluster_name", "foreign"),
    ],
)
async def test_mismatched_readback_cannot_pass_startup(tamper):
    namespace = Namespace(tamper=tamper)
    with pytest.raises(RuntimeError, match="retention validation failed"):
        await ensure_namespace_retention(namespace.client)


@pytest.mark.parametrize(
    "field,value", [("name", "foreign"), ("state", NamespaceState.NAMESPACE_STATE_DEPRECATED)]
)
async def test_invalid_original_namespace_is_refused_before_update(field, value):
    namespace = Namespace()
    setattr(namespace.value.namespace_info, field, value)
    with pytest.raises(RuntimeError, match="namespace identity"):
        await ensure_namespace_retention(namespace.client)
    assert namespace.calls == ["describe"]


def connected(monkeypatch, namespace):
    async def connect(address, *, namespace: str, **_kwargs):
        assert address.startswith("127.0.0.1:") and namespace == "default"
        return selected.client

    selected = namespace
    monkeypatch.setattr(control.Client, "connect", connect)
    monkeypatch.setattr(control, "_ensure_namespace_retention", REAL_STARTUP_CHECK)


@pytest.mark.parametrize("failure", ["update", "readback"])
def test_cold_service_does_not_launch_worker_or_api_after_retention_failure(
    config, monkeypatch, failure,
):
    namespace = Namespace(failure=failure)
    connected(monkeypatch, namespace)
    config.raw["temporal_bin"] = "/usr/bin/true"
    config.path.write_text(json.dumps(config.raw))
    launches, stopped = [], []

    def launch(_config, name, _argv):
        launches.append(name)
        return {"pid": 999999, "identity": "fixture"}

    monkeypatch.setattr(control, "_free", lambda *_args: True)
    monkeypatch.setattr(control, "_wait_port", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control, "_launch", launch)
    monkeypatch.setattr(control, "_stop", lambda _config, manifest: stopped.append(manifest))
    with pytest.raises(RuntimeError, match="retention startup check failed"):
        control._start(config, time.monotonic() + 10)
    assert launches == ["temporal"]
    assert set(stopped[0]["processes"]) == {"temporal"}


def test_existing_owned_ready_stack_checks_retention_without_restarting(config, monkeypatch):
    namespace = Namespace()
    connected(monkeypatch, namespace)
    manifest = _manifest(config)
    original = control._manifest(config).read_bytes()
    monkeypatch.setattr(control, "_ready", lambda *_args: True)
    monkeypatch.setattr(control, "_stop", lambda *_args: pytest.fail("restarted owned stack"))
    monkeypatch.setattr(control, "_launch", lambda *_args: pytest.fail("launched another process"))
    result = control.ensure_service_running(config)
    assert result["processes"] == manifest["processes"]
    assert control._manifest(config).read_bytes() == original
    assert namespace.calls == ["describe", "update", "describe"]
    namespace.failure = "describe"
    with pytest.raises(ValueError, match="retention startup check failed"):
        control.ensure_service_running(config)
    assert control._manifest(config).read_bytes() == original


async def test_direct_worker_refuses_namespace_failure_before_constructing_worker(
    config, monkeypatch,
):
    namespace = Namespace(failure="update")
    connected(monkeypatch, namespace)
    monkeypatch.setattr(control, "Worker", lambda *_a, **_k: pytest.fail("worker accepted work"))
    with pytest.raises(RuntimeError, match="retention validation failed"):
        await control.worker(config)
    assert not (config.state_root / "worker-ready.json").exists()


def test_direct_api_refuses_namespace_failure_before_application_or_server(config, monkeypatch):
    namespace = Namespace(failure="update")
    connected(monkeypatch, namespace)
    monkeypatch.setattr(control, "create_app", lambda *_a: pytest.fail("API accepted work"))
    monkeypatch.setattr(control.uvicorn, "run", lambda *_a, **_k: pytest.fail("API served"))
    with pytest.raises(RuntimeError, match="retention startup check failed"):
        control._run(argparse.Namespace(command="api", config=str(config.path)),
                     argparse.ArgumentParser())


async def test_supported_update_and_readback_on_a_private_temporal_namespace():
    from temporal_test_server import local_temporal

    async with local_temporal() as environment:
        client = environment.client
        before = await client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=client.namespace, weak_consistency=False))
        assert before.config.workflow_execution_retention_ttl.seconds == 86400
        await ensure_namespace_retention(client)
        after = await client.workflow_service.describe_namespace(
            DescribeNamespaceRequest(namespace=client.namespace, weak_consistency=False))
        assert after.config.workflow_execution_retention_ttl.seconds == MINIMUM_RETENTION_SECONDS
        after.config.workflow_execution_retention_ttl.CopyFrom(
            before.config.workflow_execution_retention_ttl)
        assert after == before
        await ensure_namespace_retention(client)
