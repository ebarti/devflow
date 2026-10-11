"""Retain closed Temporal authority for bounded delivery recovery."""

from __future__ import annotations

from datetime import timedelta

from google.protobuf.duration_pb2 import Duration
from temporalio.api.enums.v1 import NamespaceState
from temporalio.api.namespace.v1 import NamespaceConfig
from temporalio.api.workflowservice.v1 import (
    DescribeNamespaceRequest,
    DescribeNamespaceResponse,
    UpdateNamespaceRequest,
)
from temporalio.client import Client

MINIMUM_RETENTION_SECONDS = 90 * 24 * 60 * 60
RPC_TIMEOUT = timedelta(seconds=5)


def _authenticate(response: DescribeNamespaceResponse, namespace: str) -> None:
    info = response.namespace_info
    ttl = response.config.workflow_execution_retention_ttl
    if (info.name != namespace or not info.id
            or info.state != NamespaceState.NAMESPACE_STATE_REGISTERED):
        raise ValueError("configured namespace identity or registered state differs")
    if ttl.seconds <= 0 or not 0 <= ttl.nanos < 1_000_000_000:
        raise ValueError("namespace retention duration is invalid")


def _without_retention(response: DescribeNamespaceResponse) -> DescribeNamespaceResponse:
    result = DescribeNamespaceResponse()
    result.CopyFrom(response)
    result.config.ClearField("workflow_execution_retention_ttl")
    return result


async def ensure_namespace_retention(client: Client) -> None:
    """Raise only TTL, authenticate readback, and never recreate a namespace."""
    namespace = client.namespace
    if not isinstance(namespace, str) or not namespace or namespace != namespace.strip():
        raise ValueError("configured Temporal namespace is invalid")
    try:
        request = DescribeNamespaceRequest(namespace=namespace, weak_consistency=False)
        before = await client.workflow_service.describe_namespace(request, timeout=RPC_TIMEOUT)
        _authenticate(before, namespace)
        if before.config.workflow_execution_retention_ttl.seconds >= MINIMUM_RETENTION_SECONDS:
            return
        # UpdateNamespace treats omitted config fields as unchanged. Sending only
        # TTL avoids re-merging bad binaries, aliases, archival or replication.
        await client.workflow_service.update_namespace(
            UpdateNamespaceRequest(namespace=namespace, config=NamespaceConfig(
                workflow_execution_retention_ttl=Duration(seconds=MINIMUM_RETENTION_SECONDS))),
            timeout=RPC_TIMEOUT,
        )
        after = await client.workflow_service.describe_namespace(request, timeout=RPC_TIMEOUT)
        _authenticate(after, namespace)
        if (after.namespace_info.id != before.namespace_info.id
                or after.config.workflow_execution_retention_ttl.seconds
                < MINIMUM_RETENTION_SECONDS
                or _without_retention(after) != _without_retention(before)):
            raise ValueError("namespace retention readback differs or changed unrelated fields")
    except Exception as exc:
        raise RuntimeError(
            f"Temporal namespace {namespace!r} retention validation failed: {exc}") from exc
