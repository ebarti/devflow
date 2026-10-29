"""Namespace authority must survive a bounded closed-workflow recovery window."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from google.protobuf.duration_pb2 import Duration
from temporalio.api.enums.v1 import ArchivalState, NamespaceState
from temporalio.api.namespace.v1 import NamespaceConfig, NamespaceInfo
from temporalio.api.replication.v1 import ClusterReplicationConfig, NamespaceReplicationConfig
from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest, DescribeNamespaceResponse
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
